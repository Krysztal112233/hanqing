//! `hanqing-cli peers sync` — backfill the `peers` display-name cache.
//!
//! Two passes: (1) every dialog already carries its display name; (2) message
//! senders never seen as a dialog are resolved one by one through the session
//! peer cache (`resolve_peer` — one RPC each, so the second pass can take a
//! while on a large archive; flood waits are slept through).

use std::time::Duration;

use grammers_client::session::Session as _;
use grammers_client::session::types::PeerId;
use sea_orm::DatabaseConnection;
use snafu::ResultExt;
use tracing::{info, warn};

use crate::error::{DatabaseSnafu, Error, InvokeSnafu};
use hanqing_archive::peers::new_peer;
use hanqing_database::helper::prelude::*;
use hanqing_database::model::prelude::Peers;
use hanqing_telegram::Connection;

/// Flood waits longer than this abort the sender pass (dialogs are already
/// synced by then, so nothing is lost; rerun to continue).
const MAX_FLOOD_WAIT_SECS: u32 = 600;

pub async fn sync(
    db: &DatabaseConnection,
    connection: &Connection,
    self_id: i64,
) -> Result<(), Error> {
    sync_dialogs(db, connection, self_id).await?;
    sync_senders(db, connection, self_id).await
}

/// Pass 1: every dialog knows its own name.
async fn sync_dialogs(
    db: &DatabaseConnection,
    connection: &Connection,
    self_id: i64,
) -> Result<(), Error> {
    let mut dialogs = connection.client.iter_dialogs();
    let mut batch = Vec::new();
    let mut count = 0usize;
    while let Some(dialog) = dialogs.next().await.context(InvokeSnafu)? {
        batch.push(new_peer(dialog.peer(), self_id));
        count += 1;
        if batch.len() >= 500 {
            Peers::upsert(db, std::mem::take(&mut batch))
                .await
                .context(DatabaseSnafu)?;
        }
    }
    Peers::upsert(db, batch).await.context(DatabaseSnafu)?;
    info!(count, "dialog peers synced");
    Ok(())
}

/// Pass 2: message senders that never appeared as a dialog.
async fn sync_senders(
    db: &DatabaseConnection,
    connection: &Connection,
    self_id: i64,
) -> Result<(), Error> {
    let ids = Peers::find_unresolved_senders(db)
        .await
        .context(DatabaseSnafu)?;
    info!(count = ids.len(), "unresolved senders to look up");

    let mut resolved = 0usize;
    let mut skipped = 0usize;
    for id in ids {
        let Some(peer_id) = PeerId::from_bot_api_dialog_id(id) else {
            skipped += 1;
            continue;
        };
        let peer_ref = match connection.session.peer_ref(peer_id).await {
            Ok(Some(peer_ref)) => peer_ref,
            // No access hash cached for this peer; unresolvable.
            Ok(None) => {
                skipped += 1;
                continue;
            }
            Err(err) => {
                warn!(id, %err, "session lookup failed; skipping");
                skipped += 1;
                continue;
            }
        };
        match connection.client.resolve_peer(peer_ref).await {
            Ok(peer) => {
                Peers::upsert(db, vec![new_peer(&peer, self_id)])
                    .await
                    .context(DatabaseSnafu)?;
                resolved += 1;
            }
            Err(grammers_client::sender::InvocationError::Rpc(rpc)) if rpc.name == "FLOOD_WAIT" => {
                let wait = rpc.value.unwrap_or(30);
                if wait > MAX_FLOOD_WAIT_SECS {
                    warn!(
                        wait,
                        resolved, skipped, "flood wait too long; aborting sender pass"
                    );
                    break;
                }
                warn!(wait, "flood wait; sleeping");
                tokio::time::sleep(Duration::from_secs(u64::from(wait) + 1)).await;
            }
            Err(err) => {
                warn!(id, %err, "failed to resolve sender; skipping");
                skipped += 1;
            }
        }
        if (resolved + skipped).is_multiple_of(500) {
            info!(resolved, skipped, "sender sync progress");
        }
    }
    info!(resolved, skipped, "sender peers synced");
    Ok(())
}
