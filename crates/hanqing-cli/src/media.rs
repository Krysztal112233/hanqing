//! Media download queue management: backlog enqueue and status overview.

use hanqing_database::helper::prelude::*;
use hanqing_database::model::prelude::Media;
use hanqing_database::queue;
use sea_orm::DatabaseConnection;
use snafu::ResultExt;
use tracing::info;

use crate::error::{DatabaseSnafu, Error};

/// Rows per enqueue page.
const PAGE: u64 = 1000;

/// Enqueue every `pending` media row into the pgmq download queue. Run once
/// when enabling downloads on an existing archive; media archived afterwards
/// is enqueued by the daemon itself. Duplicates are harmless: the consumer
/// re-checks row status before downloading.
pub async fn enqueue_pending(db: &DatabaseConnection) -> Result<u64, Error> {
    let mut after = i64::MIN;
    let mut total = 0u64;
    loop {
        let ids = Media::find_pending_ids(db, after, PAGE)
            .await
            .context(DatabaseSnafu)?;
        if ids.is_empty() {
            break;
        }
        after = *ids.last().expect("page is non-empty");
        queue::send_batch(db, &ids).await.context(DatabaseSnafu)?;
        total += ids.len() as u64;
        info!(total, "enqueue progress");
    }
    Ok(total)
}

/// Status counts from the registry plus pgmq queue metrics.
pub async fn stats(db: &DatabaseConnection) -> Result<(), Error> {
    println!("media rows by status:");
    for (status, count, bytes) in Media::status_counts(db).await.context(DatabaseSnafu)? {
        println!("  {status:<12} {count:>10} rows {bytes:>14} bytes");
    }

    match queue::metrics(db, queue::MEDIA_DOWNLOAD_QUEUE)
        .await
        .context(DatabaseSnafu)?
    {
        Some(metrics) => {
            let oldest = metrics
                .oldest_msg_age_secs
                .map_or_else(|| "-".to_string(), |secs| format!("{secs}s"));
            println!(
                "queue `{}`: {} queued ({} visible), {} total enqueued, oldest {oldest}",
                queue::MEDIA_DOWNLOAD_QUEUE,
                metrics.queue_length,
                metrics.visible_length,
                metrics.total_messages,
            );
        }
        None => println!(
            "queue `{}` missing; run `migration up` first",
            queue::MEDIA_DOWNLOAD_QUEUE
        ),
    }
    Ok(())
}
