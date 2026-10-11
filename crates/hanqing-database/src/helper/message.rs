use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ActiveValue::Set, DatabaseConnection, EntityTrait as _};
use serde_json::Value;
use snafu::ResultExt;

use crate::error::{DatabaseSnafu, Error};
use crate::model::messages::{
    ActiveModel as MessagesActiveModel, Column as MessagesColumn, Entity as MessagesEntity,
};

pub struct NewMessage {
    pub chat_id: i64,
    pub msg_id: i64,
    pub date: DateTimeWithTimeZone,
    pub sender_id: Option<i64>,
    pub text: String,
    /// Full Telegram TL payload (`grammers_tl_types::enums::Message`).
    pub raw: Value,
}

/// Insert a message; re-archiving the same `(chat_id, msg_id)` is a no-op.
pub async fn save_message(db: &DatabaseConnection, msg: NewMessage) -> Result<(), Error> {
    let model = MessagesActiveModel {
        chat_id: Set(msg.chat_id),
        msg_id: Set(msg.msg_id),
        date: Set(msg.date),
        sender_id: Set(msg.sender_id),
        text: Set(msg.text),
        raw: Set(msg.raw),
    };

    MessagesEntity::insert(model)
        .on_conflict(
            OnConflict::columns([MessagesColumn::ChatId, MessagesColumn::MsgId])
                .do_nothing()
                .to_owned(),
        )
        .exec(db)
        .await
        .context(DatabaseSnafu)?;

    Ok(())
}
