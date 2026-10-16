//! Resumable full-history backfill.
//!
//! Per chat, messages are fetched newest → oldest (`iter_messages` with
//! `offset_id` = watermark). The watermark (`sync_state.cursor`) is persisted
//! transactionally with every batch, so an interrupted run resumes where it
//! stopped. Chats are processed sequentially to stay gentle with rate limits;
//! `FLOOD_WAIT` errors pause the current chat (capped) instead of failing.

use std::collections::HashMap;
use std::time::Duration;

use chrono::DateTime;
use grammers_client::Client;
use grammers_client::client::MessageIter;
use grammers_client::message::Message;
use grammers_client::sender::InvocationError;
use grammers_client::session::types::PeerRef;
use hanqing_database::helper::media::NewMedia;
use hanqing_database::helper::message::NewMessage;
use hanqing_database::helper::prelude::*;
use hanqing_database::model::messages::{Column as MessagesColumn, Model as MessagesModel};
use hanqing_database::model::prelude::{Media, Messages, SyncState};
use sea_orm::sea_query::Expr;
use sea_orm::{
    DatabaseConnection, EntityTrait as _, QueryFilter as _, QueryOrder as _, QuerySelect as _,
    TransactionTrait as _, Value,
};
use snafu::ResultExt;
use tracing::{info, warn};

use crate::error::{
    DatabaseSnafu, DbSnafu, Error, InvokeSnafu, PeerNotCachedSnafu, PeerResolveSnafu,
    SerializeSnafu,
};
use crate::peers::PeerRecorder;
use backon::RetryableWithContext;

use hanqing_telegram::retry::{backoff, is_transient};

/// Messages per transactional insert + cursor checkpoint.
const BATCH_SIZE: usize = 200;

/// Flood waits longer than this pause the chat until the next run instead of
/// sleeping.
const MAX_FLOOD_WAIT_SECS: u32 = 3600;

#[derive(Debug, Default)]
pub struct BackfillStats {
    pub registered_chats: usize,
    pub completed_chats: usize,
    pub messages: u64,
}

/// Enumerate all dialogs into `sync_state`, then backfill every pending
/// (unfinished, enabled) chat.
///
/// `enqueue_media` additionally dispatches every extracted media row into
/// the pgmq download queue (same transaction as the insert); the daemon
/// enables it when S3 storage is configured.
pub async fn backfill_all(
    client: &Client,
    db: &DatabaseConnection,
    self_id: i64,
    enqueue_media: bool,
    recorder: &PeerRecorder,
) -> Result<BackfillStats, Error> {
    let peers = register_dialogs(client, db, self_id, recorder).await?;
    let ctx = BackfillCtx {
        client,
        db,
        self_id,
        enqueue_media,
        recorder,
    };
    let pending = SyncState::find_pending(db).await.context(DatabaseSnafu)?;

    let mut stats = BackfillStats {
        registered_chats: peers.len(),
        ..Default::default()
    };

    for state in pending {
        let Some(peer_ref) = peers.get(&state.chat_id) else {
            warn!(
                chat_id = state.chat_id,
                "chat pending but not in current dialogs; skipping"
            );
            continue;
        };

        let fetched = backfill_chat(&ctx, state.chat_id, peer_ref, state.cursor).await?;
        stats.completed_chats += 1;
        stats.messages += fetched;
    }

    Ok(stats)
}

/// Walk the dialog list and register unseen chats. Returns resolvable
/// `chat_id -> PeerRef` pairs for the current run.
async fn register_dialogs(
    client: &Client,
    db: &DatabaseConnection,
    self_id: i64,
    recorder: &PeerRecorder,
) -> Result<HashMap<i64, PeerRef>, Error> {
    let mut peers = HashMap::new();
    let mut dialogs = client.iter_dialogs();
    while let Some(dialog) = dialogs.next().await.context(InvokeSnafu)? {
        let peer = dialog.peer();
        recorder.record(db, peer).await;
        let chat_id = peer.id().bot_api_dialog_id().unwrap_or(self_id);
        let peer_ref = peer
            .to_ref()
            .await
            .context(PeerResolveSnafu)?
            .ok_or_else(|| PeerNotCachedSnafu { peer_id: chat_id }.build())?;
        peers.insert(chat_id, peer_ref);
    }
    info!(count = peers.len(), "dialogs enumerated");

    let chat_ids: Vec<i64> = peers.keys().copied().collect();
    SyncState::register_all(db, &chat_ids)
        .await
        .context(DatabaseSnafu)?;
    Ok(peers)
}

/// Shared context threaded through one backfill run.
struct BackfillCtx<'a> {
    client: &'a Client,
    db: &'a DatabaseConnection,
    self_id: i64,
    enqueue_media: bool,
    recorder: &'a PeerRecorder,
}

/// Backfill one chat to completion (or pausably far). Returns messages fetched.
async fn backfill_chat(
    ctx: &BackfillCtx<'_>,
    chat_id: i64,
    peer_ref: &PeerRef,
    cursor: Option<i64>,
) -> Result<u64, Error> {
    info!(chat_id, ?cursor, "backfilling chat");

    let mut oldest = cursor.unwrap_or(i64::MAX);
    // `oldest == i64::MAX` marks a fresh chat; guard every offset_id call,
    // because `i64::MAX as i32` truncates to -1.
    let rebuild_iter = |oldest: i64| {
        let mut iter = ctx.client.iter_messages(*peer_ref);
        if oldest != i64::MAX {
            iter = iter.offset_id(oldest as i32);
        }
        iter
    };
    let mut iter = rebuild_iter(oldest);

    let mut batch: Vec<(NewMessage, Option<NewMedia>)> = Vec::with_capacity(BATCH_SIZE);
    let mut total = 0u64;
    loop {
        // The iterator moves through the retry context; a transient failure
        // rebuilds it from the watermark for the next attempt.
        let (iter_back, next) = (|mut iter: MessageIter| async move {
            match iter.next().await {
                Err(err) if is_transient(&err) => (rebuild_iter(oldest), Err(err)),
                other => (iter, other),
            }
        })
        .retry(backoff())
        .context(iter)
        .when(is_transient)
        .notify(|err, delay| warn!(chat_id, %err, ?delay, "transient error; backing off"))
        .await;
        iter = iter_back;

        match next {
            Ok(Some(message)) => {
                oldest = oldest.min(message.id() as i64);
                if let Some(sender) = message.sender() {
                    ctx.recorder.record(ctx.db, sender).await;
                }
                batch.push(map_message(chat_id, &message, ctx.self_id)?);
                total += 1;
                if batch.len() >= BATCH_SIZE {
                    flush_batch(ctx.db, chat_id, &mut batch, oldest, ctx.enqueue_media).await?;
                }
            }
            Ok(None) => break,
            Err(InvocationError::Rpc(err)) if err.name == "FLOOD_WAIT" => {
                let wait = err.value.unwrap_or(30);
                flush_batch(ctx.db, chat_id, &mut batch, oldest, ctx.enqueue_media).await?;
                if wait > MAX_FLOOD_WAIT_SECS {
                    warn!(
                        chat_id,
                        wait, "flood wait exceeds cap; pausing chat until next run"
                    );
                    return Ok(total);
                }
                warn!(chat_id, wait, "flood wait; sleeping");
                tokio::time::sleep(Duration::from_secs(u64::from(wait) + 1)).await;
                // Rebuild the iterator from the persisted watermark rather
                // than relying on its internal state after an error.
                iter = rebuild_iter(oldest);
            }
            Err(err) => {
                flush_batch(ctx.db, chat_id, &mut batch, oldest, ctx.enqueue_media).await?;
                return Err(err).context(InvokeSnafu);
            }
        }
    }

    flush_batch(ctx.db, chat_id, &mut batch, oldest, ctx.enqueue_media).await?;
    SyncState::mark_done(ctx.db, chat_id)
        .await
        .context(DatabaseSnafu)?;
    info!(chat_id, total, "chat backfill complete");
    Ok(total)
}

/// Insert the batch (messages + media metadata) and advance the watermark in
/// one transaction. With `enqueue_media`, download tasks are dispatched into
/// the pgmq queue in the same transaction, so a task never exists without
/// its registry row.
async fn flush_batch(
    db: &DatabaseConnection,
    chat_id: i64,
    batch: &mut Vec<(NewMessage, Option<NewMedia>)>,
    oldest: i64,
    enqueue_media: bool,
) -> Result<(), Error> {
    if batch.is_empty() {
        return Ok(());
    }

    let (messages, media): (Vec<NewMessage>, Vec<Option<NewMedia>>) =
        std::mem::take(batch).into_iter().unzip();
    let media: Vec<NewMedia> = media.into_iter().flatten().collect();
    let file_ids: Vec<i64> = media.iter().map(|m| m.file_id).collect();

    let txn = db.begin().await.context(DbSnafu)?;
    let count = Messages::save_all(&txn, messages)
        .await
        .context(DatabaseSnafu)?;
    let media_count = Media::save_all(&txn, media).await.context(DatabaseSnafu)?;
    if enqueue_media {
        hanqing_database::queue::send_batch(&txn, &file_ids)
            .await
            .context(DatabaseSnafu)?;
    }
    SyncState::update_cursor(&txn, chat_id, oldest)
        .await
        .context(DatabaseSnafu)?;
    txn.commit().await.context(DbSnafu)?;

    info!(chat_id, count, media_count, oldest, "batch archived");
    Ok(())
}

pub(crate) fn map_message(
    chat_id: i64,
    message: &Message,
    self_id: i64,
) -> Result<(NewMessage, Option<NewMedia>), Error> {
    let raw = serde_json::to_value(&message.raw).context(SerializeSnafu)?;
    let date = DateTime::from_timestamp(
        message.date().as_second(),
        message.date().subsec_nanosecond() as u32,
    )
    .expect("message timestamp out of range")
    .fixed_offset();

    let media = Media::extract(chat_id, message.id() as i64, &raw);

    Ok((
        NewMessage {
            chat_id,
            msg_id: message.id() as i64,
            date,
            sender_id: message
                .sender_id()
                .map(|p| p.bot_api_dialog_id().unwrap_or(self_id)),
            text: message.text().to_string(),
            raw,
        },
        media,
    ))
}

/// One-shot scan: extract media metadata from already-archived messages
/// (`raw` JSONB), without any Telegram round-trip. Keyset-paginated;
/// idempotent via `file_id` upsert.
pub async fn scan_existing_media(db: &DatabaseConnection) -> Result<u64, Error> {
    const PAGE: u64 = 1000;

    let mut last: Option<(i64, i64)> = None;
    let mut scanned = 0u64;
    let mut extracted = 0u64;

    loop {
        let mut query = Messages::find()
            .filter(Expr::cust("raw->'Message' ? 'media'"))
            .order_by_asc(MessagesColumn::ChatId)
            .order_by_asc(MessagesColumn::MsgId)
            .limit(PAGE);

        if let Some((chat_id, msg_id)) = last {
            query = query.filter(Expr::cust_with_values(
                "(chat_id, msg_id) > ($1, $2)",
                [Value::from(chat_id), Value::from(msg_id)],
            ));
        }

        let rows: Vec<MessagesModel> = query.all(db).await.context(DbSnafu)?;
        if rows.is_empty() {
            break;
        }

        let media: Vec<NewMedia> = rows
            .iter()
            .filter_map(|m| Media::extract(m.chat_id, m.msg_id, &m.raw))
            .collect();

        let full_page = rows.len() as u64 == PAGE;
        if let Some(m) = rows.last() {
            last = Some((m.chat_id, m.msg_id));
        }
        scanned += rows.len() as u64;
        extracted += media.len() as u64;
        Media::save_all(db, media).await.context(DatabaseSnafu)?;

        if !full_page {
            break;
        }
    }

    info!(scanned, extracted, "media metadata scan complete");
    Ok(extracted)
}
