//! PostgreSQL-backed [`grammers_session::Session`] storage.
//!
//! The project does not use SQLite; `PgSession` mirrors `SqliteSession`'s
//! semantics on the `session_*` tables: synchronous reads (`home_dc_id`,
//! `dc_option`) are served from an in-memory cache with write-through
/// updates, everything else hits PostgreSQL directly.
use std::collections::HashMap;
use std::sync::Mutex;

use grammers_session::types::{
    ChannelState, DcOption, PeerId, PeerInfo, UpdateState, UpdatesState,
};
use grammers_session::{BoxFuture, Session, SessionData};
use sea_orm::DatabaseConnection;
use snafu::ResultExt;
use tokio::sync::Mutex as AsyncMutex;

use hanqing_database::helper::session as store;

use crate::error::{DatabaseSnafu, PgSessionError, PoisonedSnafu, SerdeSnafu};

pub struct PgSession {
    db: DatabaseConnection,
    cache: Mutex<Cache>,
    /// Fallback datacenter options (statically known DCs) for entries not yet
    /// stored in the database.
    defaults: SessionData,
    /// Serializes `set_update_state` read-modify-write cycles.
    state_lock: AsyncMutex<()>,
}

struct Cache {
    home_dc: i32,
    dc_options: HashMap<i32, DcOption>,
}

impl PgSession {
    /// Load session state from PostgreSQL. Empty tables behave like a fresh
    /// session (`SessionData::default()` values).
    pub async fn open(db: DatabaseConnection) -> Result<Self, PgSessionError> {
        let defaults = SessionData::default();

        let home_dc = store::load_dc_home(&db)
            .await
            .context(DatabaseSnafu)?
            .unwrap_or(defaults.home_dc);

        let dc_options = store::load_dc_options(&db)
            .await
            .context(DatabaseSnafu)?
            .into_iter()
            .map(|(_, value)| {
                let option: DcOption = serde_json::from_value(value).context(SerdeSnafu)?;
                Ok((option.id, option))
            })
            .collect::<Result<HashMap<_, _>, PgSessionError>>()?;

        Ok(PgSession {
            db,
            cache: Mutex::new(Cache {
                home_dc,
                dc_options,
            }),
            defaults,
            state_lock: AsyncMutex::new(()),
        })
    }
}

impl Session for PgSession {
    type Error = PgSessionError;

    fn home_dc_id(&self) -> Result<i32, PgSessionError> {
        Ok(self
            .cache
            .lock()
            .map_err(|_| PoisonedSnafu.build())?
            .home_dc)
    }

    fn set_home_dc_id(&self, dc_id: i32) -> BoxFuture<'_, Result<(), PgSessionError>> {
        let ok = match self.cache.lock() {
            Err(_) => Err(PoisonedSnafu.build()),
            Ok(mut cache) => {
                cache.home_dc = dc_id;
                Ok(())
            }
        };
        Box::pin(async move {
            ok?;
            store::save_dc_home(&self.db, dc_id)
                .await
                .context(DatabaseSnafu)
        })
    }

    fn dc_option(&self, dc_id: i32) -> Result<Option<DcOption>, PgSessionError> {
        Ok(self
            .cache
            .lock()
            .map_err(|_| PoisonedSnafu.build())?
            .dc_options
            .get(&dc_id)
            .cloned()
            .or_else(|| self.defaults.dc_options.get(&dc_id).cloned()))
    }

    fn set_dc_option(&self, dc_option: &DcOption) -> BoxFuture<'_, Result<(), PgSessionError>> {
        let ok = match self.cache.lock() {
            Err(_) => Err(PoisonedSnafu.build()),
            Ok(mut cache) => {
                cache.dc_options.insert(dc_option.id, dc_option.clone());
                Ok(())
            }
        };
        let data = serde_json::to_value(dc_option);
        let dc_id = dc_option.id;
        Box::pin(async move {
            ok?;
            let data = data.context(SerdeSnafu)?;
            store::upsert_dc_option(&self.db, dc_id, data)
                .await
                .context(DatabaseSnafu)
        })
    }

    fn peer(&self, peer: PeerId) -> BoxFuture<'_, Result<Option<PeerInfo>, PgSessionError>> {
        Box::pin(async move {
            let found = match peer.bot_api_dialog_id() {
                Some(dialog_id) => store::load_peer(&self.db, dialog_id).await,
                // The self-user sentinel is not a valid storage key; the row
                // is found by its `is_self` flag inside the JSONB payload.
                None => store::load_self_peer(&self.db)
                    .await
                    .map(|row| row.map(|(_, data)| data)),
            }
            .context(DatabaseSnafu)?;

            found
                .map(serde_json::from_value)
                .transpose()
                .context(SerdeSnafu)
        })
    }

    fn cache_peer(&self, peer: PeerInfo) -> BoxFuture<'_, Result<(), PgSessionError>> {
        Box::pin(async move {
            // Merge into any existing entry: later info may only fill gaps.
            let peer = match self.peer(peer.id()).await? {
                Some(mut existing) => {
                    existing.extend_info(&peer);
                    existing
                }
                None => peer,
            };

            let peer_id = peer.id().bot_api_dialog_id_unchecked();
            let data = serde_json::to_value(&peer).context(SerdeSnafu)?;
            store::upsert_peer(&self.db, peer_id, data)
                .await
                .context(DatabaseSnafu)
        })
    }

    fn updates_state(&self) -> BoxFuture<'_, Result<UpdatesState, PgSessionError>> {
        Box::pin(async move {
            match store::load_updates_state(&self.db)
                .await
                .context(DatabaseSnafu)?
            {
                Some(value) => serde_json::from_value(value).context(SerdeSnafu),
                None => Ok(UpdatesState::default()),
            }
        })
    }

    fn set_update_state(&self, update: UpdateState) -> BoxFuture<'_, Result<(), PgSessionError>> {
        Box::pin(async move {
            let _guard = self.state_lock.lock().await;

            let mut state = match store::load_updates_state(&self.db)
                .await
                .context(DatabaseSnafu)?
            {
                Some(value) => serde_json::from_value::<UpdatesState>(value).context(SerdeSnafu)?,
                None => UpdatesState::default(),
            };

            match update {
                UpdateState::All(new_state) => state = new_state,
                UpdateState::Primary { pts, date, seq } => {
                    state.pts = pts;
                    state.date = date;
                    state.seq = seq;
                }
                UpdateState::Secondary { qts } => state.qts = qts,
                UpdateState::Channel { id, pts } => {
                    match state.channels.iter_mut().find(|c| c.id == id) {
                        Some(channel) => channel.pts = pts,
                        None => state.channels.push(ChannelState { id, pts }),
                    }
                }
            }

            let data = serde_json::to_value(&state).context(SerdeSnafu)?;
            store::save_updates_state(&self.db, data)
                .await
                .context(DatabaseSnafu)
        })
    }
}
