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

/// Messages per transactional insert + cursor checkpoint.
const BATCH_SIZE: usize = 200;

/// Consecutive IO errors tolerated before aborting the run. Telegram kills
/// idle connections routinely; reconnecting and resuming from the persisted
/// watermark is almost always the right response.
const MAX_IO_RETRIES: u32 = 10;

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
pub async fn backfill_all(
    client: &Client,
    db: &DatabaseConnection,
) -> Result<BackfillStats, Error> {
    let peers = register_dialogs(client, db).await?;
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

        let fetched = backfill_chat(client, db, state.chat_id, peer_ref, state.cursor).await?;
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
) -> Result<HashMap<i64, PeerRef>, Error> {
    let mut peers = HashMap::new();
    let mut dialogs = client.iter_dialogs();
    while let Some(dialog) = dialogs.next().await.context(InvokeSnafu)? {
        let peer = dialog.peer();
        let chat_id = peer.id().bot_api_dialog_id_unchecked();
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

/// Backfill one chat to completion (or pausably far). Returns messages fetched.
async fn backfill_chat(
    client: &Client,
    db: &DatabaseConnection,
    chat_id: i64,
    peer_ref: &PeerRef,
    cursor: Option<i64>,
) -> Result<u64, Error> {
    info!(chat_id, ?cursor, "backfilling chat");

    let mut oldest = cursor.unwrap_or(i64::MAX);
    let mut iter = client.iter_messages(*peer_ref);
    if let Some(cursor) = cursor {
        iter = iter.offset_id(cursor as i32);
    }

    let mut batch: Vec<(NewMessage, Option<NewMedia>)> = Vec::with_capacity(BATCH_SIZE);
    let mut total = 0u64;
    let mut io_retries = 0u32;

    loop {
        match iter.next().await {
            Ok(Some(message)) => {
                io_retries = 0;
                oldest = oldest.min(message.id() as i64);
                batch.push(map_message(chat_id, &message)?);
                total += 1;
                if batch.len() >= BATCH_SIZE {
                    flush_batch(db, chat_id, &mut batch, oldest).await?;
                }
            }
            Ok(None) => break,
            Err(InvocationError::Rpc(err)) if err.name == "FLOOD_WAIT" => {
                let wait = err.value.unwrap_or(30);
                flush_batch(db, chat_id, &mut batch, oldest).await?;
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
                iter = client.iter_messages(*peer_ref).offset_id(oldest as i32);
            }
            Err(InvocationError::Io(err)) => {
                flush_batch(db, chat_id, &mut batch, oldest).await?;
                io_retries += 1;
                if io_retries > MAX_IO_RETRIES {
                    return Err(InvocationError::Io(err)).context(InvokeSnafu);
                }
                let delay = 2u64.pow(io_retries).min(60);
                warn!(chat_id, attempt = io_retries, delay, %err, "io error; reconnecting");
                tokio::time::sleep(Duration::from_secs(delay)).await;
                // The sender pool reconnects lazily on the next request;
                // resume from the persisted watermark.
                iter = client.iter_messages(*peer_ref).offset_id(oldest as i32);
            }
            Err(err) => {
                flush_batch(db, chat_id, &mut batch, oldest).await?;
                return Err(err).context(InvokeSnafu);
            }
        }
    }

    flush_batch(db, chat_id, &mut batch, oldest).await?;
    SyncState::mark_done(db, chat_id)
        .await
        .context(DatabaseSnafu)?;
    info!(chat_id, total, "chat backfill complete");
    Ok(total)
}

/// Insert the batch (messages + media metadata) and advance the watermark in
/// one transaction.
async fn flush_batch(
    db: &DatabaseConnection,
    chat_id: i64,
    batch: &mut Vec<(NewMessage, Option<NewMedia>)>,
    oldest: i64,
) -> Result<(), Error> {
    if batch.is_empty() {
        return Ok(());
    }

    let drained = std::mem::take(batch);
    let media: Vec<NewMedia> = drained
        .iter()
        .filter_map(|(_, media)| media.clone())
        .collect();
    let messages: Vec<NewMessage> = drained.into_iter().map(|(msg, _)| msg).collect();

    let txn = db.begin().await.context(DbSnafu)?;
    let count = Messages::save_all(&txn, messages)
        .await
        .context(DatabaseSnafu)?;
    let media_count = Media::save_all(&txn, media).await.context(DatabaseSnafu)?;
    SyncState::update_cursor(&txn, chat_id, oldest)
        .await
        .context(DatabaseSnafu)?;
    txn.commit().await.context(DbSnafu)?;

    info!(chat_id, count, media_count, oldest, "batch archived");
    Ok(())
}

fn map_message(chat_id: i64, message: &Message) -> Result<(NewMessage, Option<NewMedia>), Error> {
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
            sender_id: message.sender_id().map(|p| p.bot_api_dialog_id_unchecked()),
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

        let page_last = rows.last().map(|m| (m.chat_id, m.msg_id));
        scanned += rows.len() as u64;
        extracted += media.len() as u64;
        Media::save_all(db, media).await.context(DatabaseSnafu)?;

        match page_last {
            Some(key) if rows.len() as u64 == PAGE => last = Some(key),
            _ => break,
        }
    }

    info!(scanned, extracted, "media metadata scan complete");
    Ok(extracted)
}
