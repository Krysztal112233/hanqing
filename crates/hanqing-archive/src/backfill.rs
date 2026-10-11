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
use hanqing_config::ArchiveConfig;
use hanqing_database::helper::message::{NewMessage, save_messages};
use hanqing_database::helper::sync_state as store;
use sea_orm::{DatabaseConnection, TransactionTrait as _};
use snafu::ResultExt;
use tracing::{info, warn};

use crate::error::{
    DatabaseSnafu, DbSnafu, Error, InvokeSnafu, PeerNotCachedSnafu, PeerResolveSnafu,
    SerializeSnafu,
};

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

/// Enumerate all dialogs into `sync_state`, then backfill every pending chat.
pub async fn backfill_all(
    client: &Client,
    db: &DatabaseConnection,
    config: &ArchiveConfig,
) -> Result<BackfillStats, Error> {
    let peers = register_dialogs(client, db, config).await?;
    let pending = store::pending_chats(db).await.context(DatabaseSnafu)?;

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
    config: &ArchiveConfig,
) -> Result<HashMap<i64, PeerRef>, Error> {
    let mut peers = HashMap::new();
    let mut dialogs = client.iter_dialogs();
    while let Some(dialog) = dialogs.next().await.context(InvokeSnafu)? {
        let peer = dialog.peer();
        let chat_id = peer.id().bot_api_dialog_id_unchecked();
        if !config.chat_ids.is_empty() && !config.chat_ids.contains(&chat_id) {
            continue;
        }
        let peer_ref = peer
            .to_ref()
            .await
            .context(PeerResolveSnafu)?
            .ok_or_else(|| PeerNotCachedSnafu { peer_id: chat_id }.build())?;
        peers.insert(chat_id, peer_ref);
    }
    info!(count = peers.len(), "dialogs enumerated");

    let chat_ids: Vec<i64> = peers.keys().copied().collect();
    store::register_chats(db, &chat_ids)
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

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut total = 0u64;

    loop {
        match iter.next().await {
            Ok(Some(message)) => {
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
            Err(err) => {
                flush_batch(db, chat_id, &mut batch, oldest).await?;
                return Err(err).context(InvokeSnafu);
            }
        }
    }

    flush_batch(db, chat_id, &mut batch, oldest).await?;
    store::mark_done(db, chat_id).await.context(DatabaseSnafu)?;
    info!(chat_id, total, "chat backfill complete");
    Ok(total)
}

/// Insert the batch and advance the watermark in one transaction.
async fn flush_batch(
    db: &DatabaseConnection,
    chat_id: i64,
    batch: &mut Vec<NewMessage>,
    oldest: i64,
) -> Result<(), Error> {
    if batch.is_empty() {
        return Ok(());
    }

    let txn = db.begin().await.context(DbSnafu)?;
    let count = save_messages(&txn, std::mem::take(batch))
        .await
        .context(DatabaseSnafu)?;
    store::update_cursor(&txn, chat_id, oldest)
        .await
        .context(DatabaseSnafu)?;
    txn.commit().await.context(DbSnafu)?;

    info!(chat_id, count, oldest, "batch archived");
    Ok(())
}

fn map_message(chat_id: i64, message: &Message) -> Result<NewMessage, Error> {
    let raw = serde_json::to_value(&message.raw).context(SerializeSnafu)?;
    let date = DateTime::from_timestamp(
        message.date().as_second(),
        message.date().subsec_nanosecond() as u32,
    )
    .expect("message timestamp out of range")
    .fixed_offset();

    Ok(NewMessage {
        chat_id,
        msg_id: message.id() as i64,
        date,
        sender_id: message.sender_id().map(|p| p.bot_api_dialog_id_unchecked()),
        text: message.text().to_string(),
        raw,
    })
}
