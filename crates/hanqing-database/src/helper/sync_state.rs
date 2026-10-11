use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ActiveValue::Set, ColumnTrait as _, ConnectionTrait, DatabaseConnection, EntityTrait as _,
    QueryFilter as _, QueryOrder as _,
};
use snafu::ResultExt;

use crate::error::{DatabaseSnafu, Error};
use crate::model::sync_state::{
    ActiveModel as SyncStateActiveModel, Column as SyncStateColumn, Entity as SyncStateEntity,
    Model as SyncStateModel,
};

/// Register chats for backfill; already-known chats keep their progress.
pub async fn register_chats<C: ConnectionTrait>(db: &C, chat_ids: &[i64]) -> Result<(), Error> {
    if chat_ids.is_empty() {
        return Ok(());
    }

    let models = chat_ids.iter().map(|&chat_id| SyncStateActiveModel {
        chat_id: Set(chat_id),
        ..Default::default()
    });

    SyncStateEntity::insert_many(models)
        .on_conflict(
            OnConflict::column(SyncStateColumn::ChatId)
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
        .context(DatabaseSnafu)?;
    Ok(())
}

/// Chats whose backfill has not reached the oldest message yet.
pub async fn pending_chats(db: &DatabaseConnection) -> Result<Vec<SyncStateModel>, Error> {
    SyncStateEntity::find()
        .filter(SyncStateColumn::Done.eq(false))
        .order_by_asc(SyncStateColumn::ChatId)
        .all(db)
        .await
        .context(DatabaseSnafu)
}

/// Advance the watermark: `cursor` is the oldest msg_id archived so far.
pub async fn update_cursor<C: ConnectionTrait>(
    db: &C,
    chat_id: i64,
    cursor: i64,
) -> Result<(), Error> {
    SyncStateEntity::update_many()
        .col_expr(SyncStateColumn::Cursor, Expr::value(cursor))
        .col_expr(SyncStateColumn::UpdatedAt, Expr::current_timestamp())
        .filter(SyncStateColumn::ChatId.eq(chat_id))
        .exec(db)
        .await
        .context(DatabaseSnafu)?;
    Ok(())
}

pub async fn mark_done(db: &DatabaseConnection, chat_id: i64) -> Result<(), Error> {
    SyncStateEntity::update_many()
        .col_expr(SyncStateColumn::Done, Expr::value(true))
        .col_expr(SyncStateColumn::UpdatedAt, Expr::current_timestamp())
        .filter(SyncStateColumn::ChatId.eq(chat_id))
        .exec(db)
        .await
        .context(DatabaseSnafu)?;
    Ok(())
}
