//! `hanqing-cli search` — full-text search over archived messages (pgroonga).

use sea_orm::DatabaseConnection;
use snafu::ResultExt;

use crate::error::{DatabaseSnafu, Error};
use hanqing_database::helper::prelude::*;
use hanqing_database::model::prelude::Messages;

pub async fn run(
    db: &DatabaseConnection,
    query: &str,
    chat: Option<i64>,
    limit: u64,
    offset: u64,
    chars: usize,
) -> Result<(), Error> {
    let hits = Messages::search(db, query, chat, limit, offset)
        .await
        .context(DatabaseSnafu)?;
    if hits.is_empty() {
        println!("no matches");
        return Ok(());
    }
    for hit in hits {
        let flat = hit.text.replace('\n', " ");
        let snippet: String = if chars == 0 {
            flat
        } else {
            flat.chars().take(chars).collect()
        };
        let chat = hit.chat_name.unwrap_or_else(|| hit.chat_id.to_string());
        let sender = hit
            .sender_name
            .or_else(|| hit.sender_id.map(|id| id.to_string()))
            .unwrap_or_default();
        println!(
            "{}  [{} #{}] {}: {}",
            hit.date.format("%Y-%m-%d %H:%M"),
            chat,
            hit.msg_id,
            sender,
            snippet
        );
    }
    Ok(())
}
