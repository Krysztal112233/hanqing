//! Peer display-name recording.
//!
//! Every peer sighted during archiving (dialogs, message senders) is upserted
//! into the `peers` cache so queries can render names instead of bare IDs.
//! Sightings are deduplicated in memory: one upsert per peer per daemon run
//! (a restart re-upserts, which is how name changes propagate).

use std::collections::HashSet;

use grammers_client::peer::Peer;
use hanqing_database::helper::peers::NewPeer;
use hanqing_database::helper::prelude::*;
use hanqing_database::model::prelude::Peers;
use hanqing_database::model::sea_orm_active_enums::PeerKind;
use sea_orm::DatabaseConnection;
use tokio::sync::Mutex;
use tracing::warn;

/// Extract the cache row for one peer. `self_id` is the fallback for peers
/// without a bot-API dialog id (the self "Saved Messages" chat).
pub fn new_peer(peer: &Peer, self_id: i64) -> NewPeer {
    let kind = match peer {
        Peer::User(_) => PeerKind::User,
        Peer::Group(_) => PeerKind::Group,
        Peer::Channel(_) => PeerKind::Channel,
        Peer::Community(_) => PeerKind::Community,
    };
    NewPeer {
        id: peer.id().bot_api_dialog_id().unwrap_or(self_id),
        kind,
        name: peer.name().map(str::to_owned),
        username: peer.username().map(str::to_owned),
    }
}

/// Dedup wrapper: upserts each peer id at most once per process run.
/// Recording is best-effort — failures are logged, never propagated.
#[derive(Debug)]
pub struct PeerRecorder {
    self_id: i64,
    seen: Mutex<HashSet<i64>>,
}

impl PeerRecorder {
    pub fn new(self_id: i64) -> Self {
        Self {
            self_id,
            seen: Mutex::new(HashSet::new()),
        }
    }

    pub async fn record(&self, db: &DatabaseConnection, peer: &Peer) {
        let row = new_peer(peer, self.self_id);
        if !self.seen.lock().await.insert(row.id) {
            return;
        }
        if let Err(err) = Peers::upsert(db, vec![row]).await {
            warn!(%err, "failed to record peer");
        }
    }
}
