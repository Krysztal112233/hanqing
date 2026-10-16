//! Incremental archiving from the live update stream.
//!
//! grammers resolves pts gaps via `updates.getDifference` (state persisted in
//! `session_update_state` / `session_channel_state`), so restarts resume
//! losslessly. The stream runs concurrently with backfill: both write through
//! the same idempotent upserts.

use std::time::{Duration, Instant};

use chrono::DateTime;
use grammers_client::Client;
use grammers_client::client::UpdateStream;
use grammers_client::message::Message;
use grammers_client::sender::UpdatesConfiguration;
use grammers_client::session::types::PeerId;
use grammers_client::session::updates::UpdatesLike;
use grammers_client::update::Update;
use hanqing_database::helper::prelude::*;
use hanqing_database::model::prelude::{Media, Messages};
use sea_orm::{DatabaseConnection, TransactionTrait as _};

use crate::peers::PeerRecorder;
use snafu::ResultExt;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use backon::RetryableWithContext;

use crate::backfill::map_message;
use crate::error::{DatabaseSnafu, DbSnafu, Error, InvokeSnafu, StreamSetupSnafu};
use hanqing_telegram::retry::{backoff, is_transient};

/// grammers persists update state only when asked (`sync_update_state`), so
/// the stream snapshots it periodically. On restart, catch_up replays from
/// the last snapshot; archive upserts make the replay idempotent.
const STATE_SYNC_INTERVAL: Duration = Duration::from_secs(60);

/// Follow the live update stream forever, archiving message events.
///
/// `enqueue_media` additionally dispatches every extracted media row into
/// the pgmq download queue (same transaction as the insert); the daemon
/// enables it when S3 storage is configured.
pub async fn run_update_stream(
    client: &Client,
    db: &DatabaseConnection,
    updates: mpsc::Receiver<UpdatesLike>,
    self_id: i64,
    enqueue_media: bool,
    recorder: &PeerRecorder,
) -> Result<(), Error> {
    // catch_up: replay updates received while we were offline.
    let configuration = UpdatesConfiguration {
        catch_up: true,
        ..Default::default()
    };
    let mut stream = client
        .stream_updates(updates, configuration)
        .await
        .map_err(|err| {
            StreamSetupSnafu {
                message: err.to_string(),
            }
            .build()
        })?;
    info!("update stream started");

    // Persist the post-catch-up state right away, then periodically.
    if let Err(err) = stream.sync_update_state().await {
        warn!(%err, "failed to sync update state");
    }
    let mut last_sync = Instant::now();

    loop {
        // The stream owns the connection state, so it moves through the
        // retry context rather than being rebuilt per attempt.
        let (stream_back, update) = (|mut stream: UpdateStream| async move {
            let update = stream.next().await;
            (stream, update)
        })
        .retry(backoff())
        .context(stream)
        .when(is_transient)
        .notify(|err, delay| warn!(%err, ?delay, "transient stream error; backing off"))
        .await;
        stream = stream_back;
        let update = match update {
            Ok(update) => update,
            Err(err) => return Err(err).context(InvokeSnafu),
        };

        if let Err(err) = handle_update(db, update, self_id, enqueue_media, recorder).await {
            warn!(%err, "failed to archive update; skipping");
        }
        if last_sync.elapsed() >= STATE_SYNC_INTERVAL {
            if let Err(err) = stream.sync_update_state().await {
                warn!(%err, "failed to sync update state");
            }
            last_sync = Instant::now();
        }
    }
}

async fn handle_update(
    db: &DatabaseConnection,
    update: Update,
    self_id: i64,
    enqueue_media: bool,
    recorder: &PeerRecorder,
) -> Result<(), Error> {
    let edited = matches!(&update, Update::MessageEdited(_));
    match update {
        Update::NewMessage(message) | Update::MessageEdited(message) => {
            let Some(chat_id) = chat_id_of(&message, self_id) else {
                return Ok(());
            };
            // Refresh the name cache for everyone involved (chat + sender).
            if let Some(peer) = message.peer() {
                recorder.record(db, peer).await;
            }
            if let Some(sender) = message.sender() {
                recorder.record(db, sender).await;
            }
            let (msg, media) = map_message(chat_id, &message, self_id)?;
            let mut outcome = None;
            if edited {
                outcome = Some(
                    Messages::apply_edit(db, msg, edit_date(&message))
                        .await
                        .context(DatabaseSnafu)?,
                );
            } else {
                Messages::save_all(db, vec![msg])
                    .await
                    .context(DatabaseSnafu)?;
            }
            if let Some(media) = media {
                // Media row + download task commit together: a task never
                // exists without its registry row (and vice versa the row
                // is the task's payload source).
                let file_id = media.file_id;
                let txn = db.begin().await.context(DbSnafu)?;
                Media::save_all(&txn, vec![media])
                    .await
                    .context(DatabaseSnafu)?;
                if enqueue_media {
                    hanqing_database::queue::send_batch(&txn, &[file_id])
                        .await
                        .context(DatabaseSnafu)?;
                }
                txn.commit().await.context(DbSnafu)?;
            }
            debug!(
                chat_id,
                msg_id = message.id(),
                edited,
                ?outcome,
                "message archived"
            );
        }
        Update::MessageDeleted(deletion) => {
            let msg_ids = deletion.messages();
            match deletion.channel_id() {
                Some(channel_id) => {
                    // Out-of-range channel ids have no valid chat mapping;
                    // surface the anomaly instead of writing to a dead id.
                    let Some(chat_id) =
                        PeerId::channel(channel_id).and_then(|p| p.bot_api_dialog_id())
                    else {
                        warn!(channel_id, "deletion for out-of-range channel id; skipping");
                        return Ok(());
                    };
                    let count = Messages::mark_deleted(db, chat_id, msg_ids)
                        .await
                        .context(DatabaseSnafu)?;
                    debug!(chat_id, count, "channel messages marked deleted");
                }
                // DeleteMessages carries no chat id; match across non-channel
                // chats by msg_id (documented as approximate).
                None => {
                    let count = Messages::mark_deleted_fuzzy(db, msg_ids)
                        .await
                        .context(DatabaseSnafu)?;
                    debug!(count, "non-channel messages marked deleted");
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn chat_id_of(message: &Message, self_id: i64) -> Option<i64> {
    // The self user is a sentinel without a real id; Saved Messages maps to
    // the account's own user id.
    message
        .peer()
        .map(|peer| peer.id().bot_api_dialog_id().unwrap_or(self_id))
}

fn edit_date(message: &Message) -> chrono::DateTime<chrono::FixedOffset> {
    let ts = match &message.raw {
        grammers_client::tl::enums::Message::Message(m) => m.edit_date,
        _ => None,
    }
    .unwrap_or_else(|| message.date().as_second() as i32);
    DateTime::from_timestamp(ts as i64, 0)
        .expect("edit timestamp out of range")
        .fixed_offset()
}
