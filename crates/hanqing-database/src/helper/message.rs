//! Persistence helpers for `messages`.

use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ActiveValue::Set, ColumnTrait as _, ConnectionTrait, DatabaseConnection, EntityTrait as _,
    QueryFilter as _, TransactionTrait as _,
};
use serde_json::Value;
use snafu::ResultExt;

use crate::error::{DatabaseSnafu, Error};
use crate::model::message_versions::ActiveModel as MessageVersionsActiveModel;
use crate::model::messages::{ActiveModel, Column};
use crate::model::prelude::{MessageVersions, Messages};

pub struct NewMessage {
    pub chat_id: i64,
    pub msg_id: i64,
    pub date: DateTimeWithTimeZone,
    pub sender_id: Option<i64>,
    pub text: String,
    /// Full Telegram TL payload (`grammers_tl_types::enums::Message`).
    pub raw: Value,
}

#[async_trait::async_trait]
pub trait MessagesHelper {
    /// Bulk-insert messages; re-archiving existing `(chat_id, msg_id)` rows
    /// is a no-op. Returns the number of messages attempted.
    async fn save_all(
        db: &impl ConnectionTrait,
        messages: Vec<NewMessage>,
    ) -> Result<usize, Error> {
        let count = messages.len();
        if count == 0 {
            return Ok(0);
        }

        let models = messages.into_iter().map(|msg| ActiveModel {
            chat_id: Set(msg.chat_id),
            msg_id: Set(msg.msg_id),
            date: Set(msg.date),
            sender_id: Set(msg.sender_id),
            text: Set(msg.text),
            raw: Set(msg.raw),
            ..Default::default()
        });

        Messages::insert_many(models)
            .on_conflict(
                OnConflict::columns([Column::ChatId, Column::MsgId])
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(db)
            .await
            .context(DatabaseSnafu)?;

        Ok(count)
    }

    /// Apply an edit: archive the current content into `message_versions`,
    /// then update the row to the latest version (messages always holds the
    /// latest; versions the superseded). Unknown messages are inserted as
    /// fresh rows (there is no prior version to keep).
    async fn apply_edit(
        db: &DatabaseConnection,
        msg: NewMessage,
        edited_at: DateTimeWithTimeZone,
    ) -> Result<EditOutcome, Error> {
        let (chat_id, msg_id) = (msg.chat_id, msg.msg_id);

        let txn = db.begin().await.context(DatabaseSnafu)?;
        let existing = Messages::find_by_id((chat_id, msg_id))
            .one(&txn)
            .await
            .context(DatabaseSnafu)?;

        let outcome = match existing {
            Some(old) => {
                MessageVersions::insert(MessageVersionsActiveModel {
                    chat_id: Set(old.chat_id),
                    msg_id: Set(old.msg_id),
                    text: Set(old.text),
                    raw: Set(old.raw),
                    ..Default::default()
                })
                .exec_without_returning(&txn)
                .await
                .context(DatabaseSnafu)?;

                Messages::update_many()
                    .col_expr(Column::Text, Expr::value(msg.text))
                    .col_expr(Column::Raw, Expr::value(msg.raw))
                    .col_expr(Column::EditedAt, Expr::value(edited_at))
                    .filter(Column::ChatId.eq(chat_id))
                    .filter(Column::MsgId.eq(msg_id))
                    .exec(&txn)
                    .await
                    .context(DatabaseSnafu)?;
                EditOutcome::Versioned
            }
            None => {
                Messages::insert(ActiveModel {
                    chat_id: Set(chat_id),
                    msg_id: Set(msg_id),
                    date: Set(msg.date),
                    sender_id: Set(msg.sender_id),
                    text: Set(msg.text),
                    raw: Set(msg.raw),
                    edited_at: Set(Some(edited_at)),
                    ..Default::default()
                })
                .exec_without_returning(&txn)
                .await
                .context(DatabaseSnafu)?;
                EditOutcome::InsertedFresh
            }
        };

        txn.commit().await.context(DatabaseSnafu)?;
        Ok(outcome)
    }

    /// Soft-delete messages in a known chat. Returns rows newly marked.
    async fn mark_deleted(
        db: &DatabaseConnection,
        chat_id: i64,
        msg_ids: &[i32],
    ) -> Result<u64, Error> {
        let result = Messages::update_many()
            .col_expr(Column::DeletedAt, Expr::current_timestamp())
            .filter(Column::ChatId.eq(chat_id))
            .filter(Column::MsgId.is_in(msg_ids.iter().copied()))
            .filter(Column::DeletedAt.is_null())
            .exec(db)
            .await
            .context(DatabaseSnafu)?;
        Ok(result.rows_affected)
    }

    /// Soft-delete for deletion events without a channel id (DMs and small
    /// groups): match by `msg_id` across non-channel chats only. msg_id
    /// collisions between non-channel chats may rarely over-mark — accepted.
    async fn mark_deleted_fuzzy(db: &DatabaseConnection, msg_ids: &[i32]) -> Result<u64, Error> {
        // Channel chat_ids are -(10^12 + channel_id), i.e. strictly below.
        const CHANNEL_BOUND: i64 = -1_000_000_000_000;
        let result = Messages::update_many()
            .col_expr(Column::DeletedAt, Expr::current_timestamp())
            .filter(Column::MsgId.is_in(msg_ids.iter().copied()))
            .filter(Column::ChatId.gt(CHANNEL_BOUND))
            .filter(Column::DeletedAt.is_null())
            .exec(db)
            .await
            .context(DatabaseSnafu)?;
        Ok(result.rows_affected)
    }
}

/// Outcome of [`MessagesHelper::apply_edit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EditOutcome {
    /// Previous content archived into `message_versions`, row updated.
    Versioned,
    /// Message unknown to the archive; inserted as a fresh row.
    InsertedFresh,
}

impl MessagesHelper for Messages {}
