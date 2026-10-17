//! `hanqing-cli embed` — semantic-search index management.

use hanqing_database::helper::embedding as embed_store;
use hanqing_database::queue;
use sea_orm::DatabaseConnection;
use snafu::ResultExt;
use tracing::info;

use crate::error::{DatabaseSnafu, Error};

/// Messages scanned per page; tasks are sent in chunks of the same size.
const PAGE: u64 = 1000;

/// Enqueue every text-bearing message into the embed queue. Run once when
/// enabling semantic search on an existing archive; messages archived
/// afterwards are enqueued by the daemon itself. Duplicates are harmless:
/// embedding upserts are idempotent.
pub async fn enqueue_pending(db: &DatabaseConnection) -> Result<u64, Error> {
    let mut after = (i64::MIN, i64::MIN);
    let mut total = 0u64;
    loop {
        let keys = embed_store::find_embeddable_page(db, after, PAGE)
            .await
            .context(DatabaseSnafu)?;
        if keys.is_empty() {
            break;
        }
        after = *keys.last().expect("page is non-empty");
        queue::send_embed_batch(db, &keys)
            .await
            .context(DatabaseSnafu)?;
        total += keys.len() as u64;
        info!(total, "embed enqueue progress");
    }
    Ok(total)
}

/// Embedding coverage and queue metrics. With `model`, coverage counts only
/// rows for that model (a model swap starts at 0 instead of looking full).
pub async fn stats(db: &DatabaseConnection, model: Option<&str>) -> Result<(), Error> {
    let (embedded, embeddable) = embed_store::coverage(db, model)
        .await
        .context(DatabaseSnafu)?;
    let pct = if embeddable > 0 {
        embedded as f64 / embeddable as f64 * 100.0
    } else {
        0.0
    };
    match model {
        Some(model) => {
            println!("embeddings for `{model}`: {embedded} / {embeddable} messages ({pct:.1}%)")
        }
        None => println!("embeddings: {embedded} / {embeddable} messages ({pct:.1}%)"),
    }
    match queue::metrics(db, queue::TEXT_EMBED_QUEUE)
        .await
        .context(DatabaseSnafu)?
    {
        Some(m) => println!(
            "queue: {} in flight (oldest: {}s)",
            m.queue_length,
            m.oldest_msg_age_secs.unwrap_or(0)
        ),
        None => println!("queue missing (migration not applied?)"),
    }
    Ok(())
}
