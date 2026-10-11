use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ActiveValue::Set, ConnectionTrait, EntityTrait as _};
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

/// Bulk-insert messages; re-archiving existing `(chat_id, msg_id)` rows is a
/// no-op. Returns the number of messages attempted.
pub async fn save_messages<C: ConnectionTrait>(
    db: &C,
    messages: Vec<NewMessage>,
) -> Result<usize, Error> {
    let count = messages.len();
    if count == 0 {
        return Ok(0);
    }

    let models = messages.into_iter().map(|msg| MessagesActiveModel {
        chat_id: Set(msg.chat_id),
        msg_id: Set(msg.msg_id),
        date: Set(msg.date),
        sender_id: Set(msg.sender_id),
        text: Set(msg.text),
        raw: Set(msg.raw),
    });

    MessagesEntity::insert_many(models)
        .on_conflict(
            OnConflict::columns([MessagesColumn::ChatId, MessagesColumn::MsgId])
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
        .context(DatabaseSnafu)?;

    Ok(count)
}
