//! `hanqing-cli search` — full-text search over archived messages (pgroonga).

use sea_orm::DatabaseConnection;
use snafu::ResultExt;

use crate::error::{DatabaseSnafu, EmbedSnafu, Error};
use hanqing_database::helper::embedding as embed_store;
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
        print_hit(
            hit.date,
            hit.chat_name,
            hit.chat_id,
            hit.msg_id,
            hit.sender_name,
            hit.sender_id,
            &hit.text,
            None,
            chars,
        );
    }
    Ok(())
}

/// Semantic search: embed the query (with the retrieval instruction prompt)
/// and rank by cosine distance.
pub async fn run_semantic(
    db: &DatabaseConnection,
    embedding: &hanqing_config::EmbeddingConfig,
    query: &str,
    chat: Option<i64>,
    limit: u64,
    offset: u64,
    chars: usize,
) -> Result<(), Error> {
    let client = hanqing_embedding::EmbedClient::new(embedding).context(EmbedSnafu)?;
    let vector = client.embed_query(query).await.context(EmbedSnafu)?;
    let hits = embed_store::search_neighbors(db, &vector, &client.model, chat, limit, offset)
        .await
        .context(DatabaseSnafu)?;
    if hits.is_empty() {
        println!("no matches");
        return Ok(());
    }
    for hit in hits {
        print_hit(
            hit.date,
            hit.chat_name,
            hit.chat_id,
            hit.msg_id,
            hit.sender_name,
            hit.sender_id,
            &hit.text,
            Some(1.0 - hit.distance),
            chars,
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn print_hit(
    date: sea_orm::prelude::DateTimeWithTimeZone,
    chat_name: Option<String>,
    chat_id: i64,
    msg_id: i64,
    sender_name: Option<String>,
    sender_id: Option<i64>,
    text: &str,
    similarity: Option<f64>,
    chars: usize,
) {
    let flat = text.replace('\n', " ");
    let snippet: String = if chars == 0 {
        flat
    } else {
        flat.chars().take(chars).collect()
    };
    let chat = chat_name.unwrap_or_else(|| chat_id.to_string());
    let sender = sender_name
        .or_else(|| sender_id.map(|id| id.to_string()))
        .unwrap_or_default();
    let score = similarity.map(|s| format!("{s:.3} ")).unwrap_or_default();
    println!(
        "{} {} [{} #{}] {}: {}",
        date.format("%Y-%m-%d %H:%M"),
        score,
        chat,
        msg_id,
        sender,
        snippet
    );
}
