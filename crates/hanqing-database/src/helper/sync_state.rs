//! Persistence helpers for `sync_state` (per-chat backfill progress and the
//! archive enable switch).

use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ActiveValue::Set, ColumnTrait as _, ConnectionTrait, DatabaseConnection, EntityTrait as _,
    QueryFilter as _, QueryOrder as _,
};
use snafu::ResultExt;

use crate::error::{DatabaseSnafu, Error};
use crate::model::prelude::SyncState;
use crate::model::sync_state::{ActiveModel, Column, Model};

#[async_trait::async_trait]
pub trait SyncStateHelper {
    /// Register chats for backfill; already-known chats keep their progress
    /// and enabled state.
    async fn register_all(db: &impl ConnectionTrait, chat_ids: &[i64]) -> Result<(), Error> {
        if chat_ids.is_empty() {
            return Ok(());
        }

        let models = chat_ids.iter().map(|&chat_id| ActiveModel {
            chat_id: Set(chat_id),
            ..Default::default()
        });

        SyncState::insert_many(models)
            .on_conflict(OnConflict::column(Column::ChatId).do_nothing().to_owned())
            .exec_without_returning(db)
            .await
            .context(DatabaseSnafu)?;
        Ok(())
    }

    /// Chats whose backfill has not reached the oldest message yet and are
    /// not excluded via `hanqing-cli chats disable`.
    async fn find_pending(db: &DatabaseConnection) -> Result<Vec<Model>, Error> {
        SyncState::find()
            .filter(Column::Done.eq(false))
            .filter(Column::Enabled.eq(true))
            .order_by_asc(Column::ChatId)
            .all(db)
            .await
            .context(DatabaseSnafu)
    }

    /// All registered chats, for `hanqing-cli chats ls`.
    async fn find_all(db: &DatabaseConnection) -> Result<Vec<Model>, Error> {
        SyncState::find()
            .order_by_asc(Column::ChatId)
            .all(db)
            .await
            .context(DatabaseSnafu)
    }

    /// Enable/disable archiving for a chat. Returns false when the chat is
    /// not registered (the daemon registers dialogs at the start of each
    /// run).
    async fn set_enabled(
        db: &DatabaseConnection,
        chat_id: i64,
        enabled: bool,
    ) -> Result<bool, Error> {
        let result = SyncState::update_many()
            .col_expr(Column::Enabled, Expr::value(enabled))
            .col_expr(Column::UpdatedAt, Expr::current_timestamp())
            .filter(Column::ChatId.eq(chat_id))
            .exec(db)
            .await
            .context(DatabaseSnafu)?;
        Ok(result.rows_affected > 0)
    }

    /// Advance the watermark: `cursor` is the oldest msg_id archived so far.
    async fn update_cursor(
        db: &impl ConnectionTrait,
        chat_id: i64,
        cursor: i64,
    ) -> Result<(), Error> {
        SyncState::update_many()
            .col_expr(Column::Cursor, Expr::value(cursor))
            .col_expr(Column::UpdatedAt, Expr::current_timestamp())
            .filter(Column::ChatId.eq(chat_id))
            .exec(db)
            .await
            .context(DatabaseSnafu)?;
        Ok(())
    }

    async fn mark_done(db: &DatabaseConnection, chat_id: i64) -> Result<(), Error> {
        SyncState::update_many()
            .col_expr(Column::Done, Expr::value(true))
            .col_expr(Column::UpdatedAt, Expr::current_timestamp())
            .filter(Column::ChatId.eq(chat_id))
            .exec(db)
            .await
            .context(DatabaseSnafu)?;
        Ok(())
    }
}

impl SyncStateHelper for SyncState {}
