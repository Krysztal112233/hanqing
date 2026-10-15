//! pgmq-backed dispatch queue for media downloads.
//!
//! The `media` table stays the registry and final state; pgmq carries the
//! in-flight tasks. Producers enqueue in the same transaction as the media
//! row insert, so a task never exists without its registry row. The consumer
//! reads with a visibility timeout (crash recovery) and deletes only after
//! the final state is persisted; `read_ct` doubles as the attempt counter.
//!
//! The queue itself is created by the `create_media_queue` migration.

use sea_orm::{ConnectionTrait, DbBackend, Statement, Value};
use snafu::ResultExt;

use crate::error::{DatabaseSnafu, Error};

/// The one and only queue; payload is `{"file_id": <media PK>}` — the
/// registry row carries everything else.
pub const MEDIA_DOWNLOAD_QUEUE: &str = "media_download";

/// One task read from the queue.
#[derive(Debug)]
pub struct QueueTask {
    /// pgmq message id; stable across visibility-timeout expiry, needed to
    /// delete or postpone the task.
    pub msg_id: i64,
    /// How many times the task has been delivered (1 on first read).
    pub read_ct: i32,
    /// `media` primary key of the file to download.
    pub file_id: i64,
}

/// Queue-level counters from `pgmq.metrics`; `None` when the queue does not
/// exist (migration not applied).
#[derive(Debug)]
pub struct QueueMetrics {
    pub queue_length: i64,
    pub visible_length: i64,
    pub oldest_msg_age_secs: Option<i32>,
    pub total_messages: i64,
}

/// Enqueue download tasks. Call in the same transaction as the media row
/// insert. Duplicate tasks (e.g. `file_id` deduped by the insert) are
/// harmless: the consumer re-checks row status before downloading.
pub async fn send_batch(db: &impl ConnectionTrait, file_ids: &[i64]) -> Result<(), Error> {
    if file_ids.is_empty() {
        return Ok(());
    }
    let payloads: Vec<serde_json::Value> = file_ids
        .iter()
        .map(|id| serde_json::json!({ "file_id": id }))
        .collect();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT pgmq.send_batch($1, ARRAY(SELECT jsonb_array_elements($2)))",
        vec![
            Value::from(MEDIA_DOWNLOAD_QUEUE),
            Value::from(serde_json::Value::Array(payloads)),
        ],
    ))
    .await
    .context(DatabaseSnafu)?;
    Ok(())
}

/// Read up to `qty` tasks, hiding each for `vt_secs` (visibility timeout).
/// Tasks not deleted within the VT become visible again (crash recovery).
pub async fn read(
    db: &impl ConnectionTrait,
    vt_secs: i32,
    qty: i32,
) -> Result<Vec<QueueTask>, Error> {
    let rows = db
        .query_all_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT msg_id, read_ct, message FROM pgmq.read($1, $2, $3)",
            vec![
                Value::from(MEDIA_DOWNLOAD_QUEUE),
                Value::from(vt_secs),
                Value::from(qty),
            ],
        ))
        .await
        .context(DatabaseSnafu)?;

    rows.iter()
        .map(|row| {
            let message: serde_json::Value = row.try_get("", "message").context(DatabaseSnafu)?;
            let file_id = message
                .get("file_id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| {
                    sea_orm::DbErr::Custom(format!(
                        "queue message {} lacks an integer file_id: {message}",
                        row.try_get::<i64>("", "msg_id").unwrap_or_default()
                    ))
                })
                .context(DatabaseSnafu)?;
            Ok(QueueTask {
                msg_id: row.try_get("", "msg_id").context(DatabaseSnafu)?,
                read_ct: row.try_get("", "read_ct").context(DatabaseSnafu)?,
                file_id,
            })
        })
        .collect()
}

/// Delete a task after its final state is persisted. Works regardless of
/// visibility state (`msg_id` is stable), so a task whose VT expired while
/// being processed is still removed.
pub async fn delete(db: &impl ConnectionTrait, msg_id: i64) -> Result<(), Error> {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT pgmq.delete($1, $2)",
        vec![Value::from(MEDIA_DOWNLOAD_QUEUE), Value::from(msg_id)],
    ))
    .await
    .context(DatabaseSnafu)?;
    Ok(())
}

/// Postpone a task: it stays in the queue but invisible for `vt_secs`. Used
/// to park a task behind a long flood wait without dropping it.
pub async fn set_vt(db: &impl ConnectionTrait, msg_id: i64, vt_secs: i32) -> Result<(), Error> {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT pgmq.set_vt($1, $2, $3)",
        vec![
            Value::from(MEDIA_DOWNLOAD_QUEUE),
            Value::from(msg_id),
            Value::from(vt_secs),
        ],
    ))
    .await
    .context(DatabaseSnafu)?;
    Ok(())
}

pub async fn metrics(db: &impl ConnectionTrait) -> Result<Option<QueueMetrics>, Error> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT queue_length, queue_visible_length, oldest_msg_age_sec, total_messages \
             FROM pgmq.metrics($1)",
            vec![Value::from(MEDIA_DOWNLOAD_QUEUE)],
        ))
        .await
        .context(DatabaseSnafu)?;

    row.map(|row| {
        Ok(QueueMetrics {
            queue_length: row.try_get("", "queue_length").context(DatabaseSnafu)?,
            visible_length: row
                .try_get("", "queue_visible_length")
                .context(DatabaseSnafu)?,
            oldest_msg_age_secs: row
                .try_get("", "oldest_msg_age_sec")
                .context(DatabaseSnafu)?,
            total_messages: row.try_get("", "total_messages").context(DatabaseSnafu)?,
        })
    })
    .transpose()
}
