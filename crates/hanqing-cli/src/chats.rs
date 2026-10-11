//! `hanqing-cli chats` — per-chat archive switches.

use hanqing_database::helper::prelude::*;
use hanqing_database::model::prelude::SyncState;
use sea_orm::DatabaseConnection;
use snafu::ResultExt;

use crate::error::{DatabaseSnafu, Error, UnknownChatSnafu};

pub async fn ls(db: &DatabaseConnection) -> Result<(), Error> {
    let chats = SyncState::find_all(db).await.context(DatabaseSnafu)?;

    println!("{:>15}  {:<7}  {:<5}  cursor", "chat_id", "enabled", "done");
    for chat in &chats {
        println!(
            "{:>15}  {:<7}  {:<5}  {}",
            chat.chat_id,
            chat.enabled,
            chat.done,
            chat.cursor
                .map(|c| c.to_string())
                .unwrap_or_else(|| "-".to_string()),
        );
    }
    println!("({} chats)", chats.len());
    Ok(())
}

pub async fn set_enabled(
    db: &DatabaseConnection,
    chat_id: i64,
    enabled: bool,
) -> Result<(), Error> {
    let updated = SyncState::set_enabled(db, chat_id, enabled)
        .await
        .context(DatabaseSnafu)?;
    if !updated {
        return UnknownChatSnafu { chat_id }.fail();
    }
    println!(
        "chat {chat_id} {}",
        if enabled { "enabled" } else { "disabled" }
    );
    Ok(())
}
