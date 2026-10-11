//! Persistence helpers for the grammers session tables.
//!
//! Session values are opaque JSONB documents (grammers-session `serde`
//! feature); this module never names grammers types — (de)serialization is
//! the caller's job.

use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ActiveValue::Set, ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait as _, Statement,
};
use serde_json::Value;
use snafu::ResultExt;

use crate::error::{DatabaseSnafu, Error};
use crate::model::prelude::{SessionDcHome, SessionDcOption, SessionPeer, SessionUpdateState};
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
    async fn load(db: &DatabaseConnection) -> Result<Option<Value>, Error> {
        let model = SessionUpdateState::find_by_id(UPDATE_STATE_ROW_ID)
            .one(db)
            .await
            .context(DatabaseSnafu)?;
        Ok(model.map(|m| m.data))
    }

    async fn save(db: &DatabaseConnection, data: Value) -> Result<(), Error> {
        SessionUpdateState::insert(SessionUpdateStateActiveModel {
            id: Set(UPDATE_STATE_ROW_ID),
            data: Set(data),
        })
        .on_conflict(
            OnConflict::column(SessionUpdateStateColumn::Id)
                .update_column(SessionUpdateStateColumn::Data)
                .to_owned(),
        )
        .exec(db)
        .await
        .context(DatabaseSnafu)?;
        Ok(())
    }
}

impl SessionUpdateStateHelper for SessionUpdateState {}
