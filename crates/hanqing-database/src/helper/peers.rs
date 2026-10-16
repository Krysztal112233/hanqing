//! Persistence helpers for the `peers` display-name cache.

use sea_orm::sea_query::OnConflict;
use sea_orm::{ActiveValue::Set, ConnectionTrait, EntityTrait as _};
use snafu::ResultExt;

use crate::error::{DatabaseSnafu, Error};
use crate::model::peers::{ActiveModel, Column};
use crate::model::prelude::Peers;
use crate::model::sea_orm_active_enums::PeerKind;

pub struct NewPeer {
    /// Bot-API dialog id (matches `messages.chat_id` / `sender_id`).
    pub id: i64,
    pub kind: PeerKind,
    /// Display name; `None` for inaccessible or deleted peers.
    pub name: Option<String>,
    /// Public @username without the '@'.
    pub username: Option<String>,
}

#[async_trait::async_trait]
pub trait PeersHelper {
    /// Upsert display names; a repeated sighting refreshes name/username and
    /// bumps `updated_at` (via the `EXCLUDED` row).
    async fn upsert(db: &impl ConnectionTrait, peers: Vec<NewPeer>) -> Result<(), Error> {
        if peers.is_empty() {
            return Ok(());
        }
        let models = peers.into_iter().map(|peer| ActiveModel {
            id: Set(peer.id),
            kind: Set(peer.kind),
            name: Set(peer.name),
            username: Set(peer.username),
            updated_at: Set(chrono::Utc::now().into()),
        });
        Peers::insert_many(models)
            .on_conflict(
                OnConflict::column(Column::Id)
                    .update_columns([
                        Column::Kind,
                        Column::Name,
                        Column::Username,
                        Column::UpdatedAt,
                    ])
                    .to_owned(),
            )
            .exec_without_returning(db)
            .await
            .context(DatabaseSnafu)?;
        Ok(())
    }
    /// Distinct message senders that have no `peers` row yet (candidates for
    /// one-shot resolution via `resolve_peer`).
    async fn find_unresolved_senders(db: &impl ConnectionTrait) -> Result<Vec<i64>, Error> {
        let rows = db
            .query_all_raw(sea_orm::Statement::from_string(
                sea_orm::DbBackend::Postgres,
                "SELECT DISTINCT sender_id FROM messages WHERE sender_id IS NOT NULL \
                 EXCEPT SELECT id FROM peers",
            ))
            .await
            .context(DatabaseSnafu)?;
        rows.iter()
            .map(|row| row.try_get("", "sender_id").context(DatabaseSnafu))
            .collect()
    }
}

impl PeersHelper for Peers {}
