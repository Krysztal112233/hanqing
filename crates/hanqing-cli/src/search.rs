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
) -> Result<(), Error> {
    let hits = Messages::search(db, query, chat, limit, offset)
        .await
        .context(DatabaseSnafu)?;
    if hits.is_empty() {
        println!("no matches");
        return Ok(());
    }
    for hit in hits {
        let snippet: String = hit.text.replace('\n', " ").chars().take(100).collect();
        println!(
            "{} {:>15} {:>9} {:>12}  {}",
            hit.date.format("%Y-%m-%d %H:%M"),
            hit.chat_id,
            hit.msg_id,
            hit.sender_id.map(|s| s.to_string()).unwrap_or_default(),
            snippet
        );
    }
    Ok(())
}
