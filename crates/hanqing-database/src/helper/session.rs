//! Persistence helpers for the grammers session tables.
//!
//! Session values are opaque JSONB documents (grammers-session `serde`
//! feature); this module never names grammers types — (de)serialization is
//! the caller's job.

use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ActiveValue::Set, ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait as _,
    QueryOrder as _, Statement, TransactionTrait as _,
};
use serde_json::Value;
use snafu::ResultExt;

use crate::error::{DatabaseSnafu, Error};
use crate::model::prelude::{
    SessionChannelState, SessionDcHome, SessionDcOption, SessionPeer, SessionUpdateState,
};
use crate::model::session_channel_state::{
    ActiveModel as SessionChannelStateActiveModel, Column as SessionChannelStateColumn,
};
use crate::model::session_dc_home::ActiveModel as SessionDcHomeActiveModel;
use crate::model::session_dc_option::{
    ActiveModel as SessionDcOptionActiveModel, Column as SessionDcOptionColumn,
};
use crate::model::session_peer::{
    ActiveModel as SessionPeerActiveModel, Column as SessionPeerColumn,
};
use crate::model::session_update_state::{
    ActiveModel as SessionUpdateStateActiveModel, Column as SessionUpdateStateColumn,
};

const UPDATE_STATE_ROW_ID: i32 = 1;

#[async_trait::async_trait]
pub trait SessionDcHomeHelper {
    async fn load(db: &DatabaseConnection) -> Result<Option<i32>, Error> {
        let model = SessionDcHome::find().one(db).await.context(DatabaseSnafu)?;
        Ok(model.map(|m| m.dc_id))
    }

    /// Single-row table: replace any previous home datacenter.
    async fn save(db: &DatabaseConnection, dc_id: i32) -> Result<(), Error> {
        SessionDcHome::delete_many()
            .exec(db)
            .await
            .context(DatabaseSnafu)?;
        SessionDcHome::insert(SessionDcHomeActiveModel { dc_id: Set(dc_id) })
            .exec(db)
            .await
            .context(DatabaseSnafu)?;
        Ok(())
    }
}

impl SessionDcHomeHelper for SessionDcHome {}

#[async_trait::async_trait]
pub trait SessionDcOptionHelper {
    async fn load_all(db: &DatabaseConnection) -> Result<Vec<(i32, Value)>, Error> {
        let models = SessionDcOption::find()
            .all(db)
            .await
            .context(DatabaseSnafu)?;
        Ok(models.into_iter().map(|m| (m.dc_id, m.data)).collect())
    }

    async fn upsert(db: &DatabaseConnection, dc_id: i32, data: Value) -> Result<(), Error> {
        SessionDcOption::insert(SessionDcOptionActiveModel {
            dc_id: Set(dc_id),
            data: Set(data),
        })
        .on_conflict(
            OnConflict::column(SessionDcOptionColumn::DcId)
                .update_column(SessionDcOptionColumn::Data)
                .to_owned(),
        )
        .exec(db)
        .await
        .context(DatabaseSnafu)?;
        Ok(())
    }
}

impl SessionDcOptionHelper for SessionDcOption {}

#[async_trait::async_trait]
pub trait SessionPeerHelper {
    async fn load(db: &DatabaseConnection, peer_id: i64) -> Result<Option<Value>, Error> {
        let model = SessionPeer::find_by_id(peer_id)
            .one(db)
            .await
            .context(DatabaseSnafu)?;
        Ok(model.map(|m| m.data))
    }

    /// The self user is queried inside the JSONB payload
    /// (`{"User": {"is_self": true, ...}}`), not by primary key: its `PeerId`
    /// sentinel is not a valid storage key.
    async fn load_self(db: &DatabaseConnection) -> Result<Option<(i64, Value)>, Error> {
        let row = db
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT peer_id, data FROM session_peer WHERE data->'User'->>'is_self' = 'true' LIMIT 1",
            ))
            .await
            .context(DatabaseSnafu)?;

        row.map(|r| {
            let peer_id = r.try_get::<i64>("", "peer_id").context(DatabaseSnafu)?;
            let data = r.try_get::<Value>("", "data").context(DatabaseSnafu)?;
            Ok((peer_id, data))
        })
        .transpose()
    }

    async fn upsert(db: &DatabaseConnection, peer_id: i64, data: Value) -> Result<(), Error> {
        SessionPeer::insert(SessionPeerActiveModel {
            peer_id: Set(peer_id),
            data: Set(data),
        })
        .on_conflict(
            OnConflict::column(SessionPeerColumn::PeerId)
                .update_column(SessionPeerColumn::Data)
                .to_owned(),
        )
        .exec(db)
        .await
        .context(DatabaseSnafu)?;
        Ok(())
    }
}

impl SessionPeerHelper for SessionPeer {}

#[async_trait::async_trait]
pub trait SessionUpdateStateHelper {
    /// Assemble the `UpdatesState` JSON document from the split tables.
    /// Missing scalar values default to 0 (matching `UpdatesState::default`).
    async fn load(db: &DatabaseConnection) -> Result<Option<Value>, Error> {
        let row = SessionUpdateState::find_by_id(UPDATE_STATE_ROW_ID)
            .one(db)
            .await
            .context(DatabaseSnafu)?;
        let Some(row) = row else { return Ok(None) };

        let channels = SessionChannelState::find()
            .order_by_asc(SessionChannelStateColumn::ChannelId)
            .all(db)
            .await
            .context(DatabaseSnafu)?;

        Ok(Some(serde_json::json!({
            "pts": row.pts.unwrap_or(0),
            "qts": row.qts.unwrap_or(0),
            "date": row.date.unwrap_or(0),
            "seq": row.seq.unwrap_or(0),
            "channels": channels
                .into_iter()
                .map(|c| serde_json::json!({ "id": c.channel_id, "pts": c.pts }))
                .collect::<Vec<_>>(),
        })))
    }

    /// Replace the whole state (grammers `UpdateState::All`): both tables in
    /// one transaction.
    async fn save_all(db: &DatabaseConnection, data: Value) -> Result<(), Error> {
        let scalar = |key: &str| data.get(key).and_then(Value::as_i64).map(|v| v as i32);

        let txn = db.begin().await.context(DatabaseSnafu)?;

        SessionUpdateState::insert(SessionUpdateStateActiveModel {
            id: Set(UPDATE_STATE_ROW_ID),
            pts: Set(scalar("pts")),
            qts: Set(scalar("qts")),
            date: Set(scalar("date")),
            seq: Set(scalar("seq")),
        })
        .on_conflict(
            OnConflict::column(SessionUpdateStateColumn::Id)
                .update_columns([
                    SessionUpdateStateColumn::Pts,
                    SessionUpdateStateColumn::Qts,
                    SessionUpdateStateColumn::Date,
                    SessionUpdateStateColumn::Seq,
                ])
                .to_owned(),
        )
        .exec_without_returning(&txn)
        .await
        .context(DatabaseSnafu)?;

        SessionChannelState::delete_many()
            .exec(&txn)
            .await
            .context(DatabaseSnafu)?;

        let channels: Vec<SessionChannelStateActiveModel> = data
            .get("channels")
            .and_then(Value::as_array)
            .map(|channels| {
                channels
                    .iter()
                    .filter_map(|c| {
                        Some(SessionChannelStateActiveModel {
                            channel_id: Set(c.get("id")?.as_i64()?),
                            pts: Set(c.get("pts")?.as_i64()? as i32),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        if !channels.is_empty() {
            SessionChannelState::insert_many(channels)
                .on_conflict(
                    OnConflict::column(SessionChannelStateColumn::ChannelId)
                        .update_column(SessionChannelStateColumn::Pts)
                        .to_owned(),
                )
                .exec_without_returning(&txn)
                .await
                .context(DatabaseSnafu)?;
        }

        txn.commit().await.context(DatabaseSnafu)?;
        Ok(())
    }

    /// grammers `UpdateState::Primary`: direct column upsert, no
    /// read-modify-write.
    async fn save_primary(
        db: &DatabaseConnection,
        pts: i32,
        date: i32,
        seq: i32,
    ) -> Result<(), Error> {
        SessionUpdateState::insert(SessionUpdateStateActiveModel {
            id: Set(UPDATE_STATE_ROW_ID),
            pts: Set(Some(pts)),
            date: Set(Some(date)),
            seq: Set(Some(seq)),
            ..Default::default()
        })
        .on_conflict(
            OnConflict::column(SessionUpdateStateColumn::Id)
                .update_columns([
                    SessionUpdateStateColumn::Pts,
                    SessionUpdateStateColumn::Date,
                    SessionUpdateStateColumn::Seq,
                ])
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
        .context(DatabaseSnafu)?;
        Ok(())
    }

    /// grammers `UpdateState::Secondary`.
    async fn save_qts(db: &DatabaseConnection, qts: i32) -> Result<(), Error> {
        SessionUpdateState::insert(SessionUpdateStateActiveModel {
            id: Set(UPDATE_STATE_ROW_ID),
            qts: Set(Some(qts)),
            ..Default::default()
        })
        .on_conflict(
            OnConflict::column(SessionUpdateStateColumn::Id)
                .update_column(SessionUpdateStateColumn::Qts)
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
        .context(DatabaseSnafu)?;
        Ok(())
    }
}

impl SessionUpdateStateHelper for SessionUpdateState {}

#[async_trait::async_trait]
pub trait SessionChannelStateHelper {
    /// grammers `UpdateState::Channel`: per-channel pts upsert.
    async fn upsert(db: &DatabaseConnection, channel_id: i64, pts: i32) -> Result<(), Error> {
        SessionChannelState::insert(SessionChannelStateActiveModel {
            channel_id: Set(channel_id),
            pts: Set(pts),
        })
        .on_conflict(
            OnConflict::column(SessionChannelStateColumn::ChannelId)
                .update_column(SessionChannelStateColumn::Pts)
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
        .context(DatabaseSnafu)?;
        Ok(())
    }
}

impl SessionChannelStateHelper for SessionChannelState {}
