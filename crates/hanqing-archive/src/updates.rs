//! Incremental archiving from the live update stream.
//!
//! grammers resolves pts gaps via `updates.getDifference` (state persisted in
//! `session_update_state` / `session_channel_state`), so restarts resume
//! losslessly. The stream runs concurrently with backfill: both write through
//! the same idempotent upserts.

use std::time::{Duration, Instant};

use chrono::DateTime;
use grammers_client::Client;
use grammers_client::message::Message;
use grammers_client::sender::{InvocationError, UpdatesConfiguration};
use grammers_client::session::types::PeerId;
use grammers_client::session::updates::UpdatesLike;
use grammers_client::update::Update;
use hanqing_database::helper::prelude::*;
use hanqing_database::model::prelude::{Media, Messages};
use sea_orm::DatabaseConnection;
use snafu::ResultExt;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::backfill::map_message;
use crate::error::{DatabaseSnafu, Error, InvokeSnafu, StreamSetupSnafu};

/// Consecutive IO errors tolerated before giving up; the sender pool
/// reconnects lazily, so brief network blips should just be slept off.
const MAX_IO_RETRIES: u32 = 10;

/// grammers persists update state only when asked (`sync_update_state`), so
/// the stream snapshots it periodically. On restart, catch_up replays from
/// the last snapshot; archive upserts make the replay idempotent.
const STATE_SYNC_INTERVAL: Duration = Duration::from_secs(60);

/// Follow the live update stream forever, archiving message events.
pub async fn run_update_stream(
    client: &Client,
    db: &DatabaseConnection,
    updates: mpsc::Receiver<UpdatesLike>,
    self_id: i64,
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

    let mut io_retries = 0u32;
    loop {
        match stream.next().await {
            Ok(update) => {
                io_retries = 0;
                if let Err(err) = handle_update(db, update, self_id).await {
                    warn!(%err, "failed to archive update; skipping");
                }
                if last_sync.elapsed() >= STATE_SYNC_INTERVAL {
                    if let Err(err) = stream.sync_update_state().await {
                        warn!(%err, "failed to sync update state");
                    }
                    last_sync = Instant::now();
                }
            }
            Err(InvocationError::Io(err)) => {
                io_retries += 1;
                if io_retries > MAX_IO_RETRIES {
                    return Err(InvocationError::Io(err)).context(InvokeSnafu);
                }
                let delay = 2u64.pow(io_retries).min(60);
                warn!(attempt = io_retries, delay, %err, "update stream io error; backing off");
                tokio::time::sleep(Duration::from_secs(delay)).await;
            }
            Err(err) => return Err(err).context(InvokeSnafu),
        }
    }
}

async fn handle_update(db: &DatabaseConnection, update: Update, self_id: i64) -> Result<(), Error> {
    match update {
        Update::NewMessage(message) => {
            let Some(chat_id) = chat_id_of(&message, self_id) else {
                return Ok(());
            };
            let (msg, media) = map_message(chat_id, &message, self_id)?;
            Messages::save_all(db, vec![msg])
                .await
                .context(DatabaseSnafu)?;
            if let Some(media) = media {
                Media::save_all(db, vec![media])
                    .await
                    .context(DatabaseSnafu)?;
            }
            debug!(chat_id, msg_id = message.id(), "new message archived");
        }
        Update::MessageEdited(message) => {
            let Some(chat_id) = chat_id_of(&message, self_id) else {
                return Ok(());
            };
            let edited_at = edit_date(&message);
            let (msg, media) = map_message(chat_id, &message, self_id)?;
            let outcome = Messages::apply_edit(db, msg, edited_at)
                .await
                .context(DatabaseSnafu)?;
            if let Some(media) = media {
                Media::save_all(db, vec![media])
                    .await
                    .context(DatabaseSnafu)?;
            }
            debug!(
                chat_id,
                msg_id = message.id(),
                ?outcome,
                "message edit archived"
            );
        }
        Update::MessageDeleted(deletion) => {
            let msg_ids = deletion.messages();
            match deletion.channel_id() {
                Some(channel_id) => {
                    let chat_id = PeerId::channel(channel_id)
                        .and_then(|p| p.bot_api_dialog_id())
                        .unwrap_or(channel_id);
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
