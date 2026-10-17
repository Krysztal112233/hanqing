//! pgmq-backed task queues.
//!
//! `media_download` carries media download tasks: the `media` table is the
//! registry and final state; the queue carries in-flight work. Producers
//! enqueue in the same transaction as the media row insert, so a task never
//! exists without its registry row. The consumer reads with a visibility
//! timeout (crash recovery) and deletes only after the final state is
//! persisted; `read_ct` doubles as the attempt counter.
//!
//! `backfill_run` carries one-shot triggers: each message requests one
//! `backfill_all` pass (daemon startup enqueues one; `hanqing-cli backfill`
//! enqueues one manually). The consumer deletes the trigger before running —
//! a pass is far longer than any sensible visibility timeout, and a crash
//! mid-pass is covered by the next startup trigger.
//!
//! `text_embed` carries embedding tasks `(chat_id, msg_id)`; unlike media
//! there is no registry table — the `message_embeddings` row is the final
//! state and the task is the only in-flight state.
//!
//! All queues are created by migrations.

use sea_orm::{ConnectionTrait, DbBackend, Statement, Value};
use snafu::ResultExt;

use crate::error::{DatabaseSnafu, Error};

/// Media download tasks; payload is `{"file_id": <media PK>}`.
pub const MEDIA_DOWNLOAD_QUEUE: &str = "media_download";

/// Backfill triggers; payload is `{"requested_by": ...}` (informational).
pub const BACKFILL_RUN_QUEUE: &str = "backfill_run";

/// One media download task read from the queue.
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

/// One backfill trigger read from the queue.
#[derive(Debug)]
pub struct TriggerTask {
    /// pgmq message id.
    pub msg_id: i64,
    /// Informational `requested_by` field (e.g. "startup", "cli"), if set.
    pub requested_by: Option<String>,
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

/// Enqueue media download tasks. Call in the same transaction as the media
/// row insert. Duplicate tasks (e.g. `file_id` deduped by the insert) are
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

/// Request one backfill pass.
pub async fn send_backfill_trigger(
    db: &impl ConnectionTrait,
    requested_by: &str,
) -> Result<(), Error> {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT pgmq.send($1, $2)",
        vec![
            Value::from(BACKFILL_RUN_QUEUE),
            Value::from(serde_json::json!({ "requested_by": requested_by })),
        ],
    ))
    .await
    .context(DatabaseSnafu)?;
    Ok(())
}

/// Read up to `qty` media tasks, hiding each for `vt_secs` (visibility
/// timeout). Tasks not deleted within the VT become visible again (crash
/// recovery).
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

/// Read one backfill trigger (blocking the worker is the caller's job).
pub async fn read_backfill_trigger(
    db: &impl ConnectionTrait,
) -> Result<Option<TriggerTask>, Error> {
    let rows = db
        .query_all_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT msg_id, message FROM pgmq.read($1, 60, 1)",
            vec![Value::from(BACKFILL_RUN_QUEUE)],
        ))
        .await
        .context(DatabaseSnafu)?;
    rows.first()
        .map(|row| {
            let message: serde_json::Value = row.try_get("", "message").context(DatabaseSnafu)?;
            Ok(TriggerTask {
                msg_id: row.try_get("", "msg_id").context(DatabaseSnafu)?,
                requested_by: message
                    .get("requested_by")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
            })
        })
        .transpose()
}

/// Delete a media task after its final state is persisted. Works regardless
/// of visibility state (`msg_id` is stable), so a task whose VT expired
/// while being processed is still removed.
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

/// Delete a consumed backfill trigger.
pub async fn delete_backfill_trigger(db: &impl ConnectionTrait, msg_id: i64) -> Result<(), Error> {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT pgmq.delete($1, $2)",
        vec![Value::from(BACKFILL_RUN_QUEUE), Value::from(msg_id)],
    ))
    .await
    .context(DatabaseSnafu)?;
    Ok(())
}

/// Postpone a media task: it stays in the queue but invisible for `vt_secs`.
/// Used to park a task behind a long flood wait without dropping it.
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

pub async fn metrics(
    db: &impl ConnectionTrait,
    queue: &str,
) -> Result<Option<QueueMetrics>, Error> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT queue_length, queue_visible_length, oldest_msg_age_sec, total_messages \
             FROM pgmq.metrics($1)",
            vec![Value::from(queue)],
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

// ---- text_embed queue ----

/// Embedding tasks for the semantic-search pipeline.
pub const TEXT_EMBED_QUEUE: &str = "text_embed";

/// One `text_embed` task: embed the text of `(chat_id, msg_id)` into
/// `message_embeddings`. `task_id` is the pgmq message id (renamed to avoid
/// colliding with the archived message's own `msg_id` in the payload).
#[derive(Debug, Clone)]
pub struct EmbedTask {
    pub task_id: i64,
    pub chat_id: i64,
    pub msg_id: i64,
    pub read_ct: i32,
}

/// Enqueue embed tasks; duplicates are harmless (embedding upserts are
/// idempotent), so callers need no dedup.
pub async fn send_embed_batch(db: &impl ConnectionTrait, keys: &[(i64, i64)]) -> Result<(), Error> {
    if keys.is_empty() {
        return Ok(());
    }
    let payloads: Vec<serde_json::Value> = keys
        .iter()
        .map(|(chat_id, msg_id)| serde_json::json!({ "chat_id": chat_id, "msg_id": msg_id }))
        .collect();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT pgmq.send_batch($1, ARRAY(SELECT jsonb_array_elements($2)))",
        vec![
            Value::from(TEXT_EMBED_QUEUE),
            Value::from(serde_json::Value::Array(payloads)),
        ],
    ))
    .await
    .context(DatabaseSnafu)?;
    Ok(())
}

/// Run the server-side `enqueue_text_embed()` procedure, which walks the
/// text-bearing message backlog in committed batches and enqueues each batch.
/// One client round trip; progress is reported by the procedure's notices.
pub async fn call_enqueue_text_embed(db: &impl ConnectionTrait) -> Result<(), Error> {
    db.execute_raw(Statement::from_string(
        DbBackend::Postgres,
        "CALL enqueue_text_embed()",
    ))
    .await
    .context(DatabaseSnafu)?;
    Ok(())
}

/// Read up to `qty` embed tasks, hidden for `vt_secs`.
pub async fn read_embed(
    db: &impl ConnectionTrait,
    vt_secs: i32,
    qty: i32,
) -> Result<Vec<EmbedTask>, Error> {
    let rows = db
        .query_all_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT msg_id, read_ct, message FROM pgmq.read($1, $2, $3)",
            vec![
                Value::from(TEXT_EMBED_QUEUE),
                Value::from(vt_secs),
                Value::from(qty),
            ],
        ))
        .await
        .context(DatabaseSnafu)?;

    rows.iter()
        .map(|row| {
            let task_id: i64 = row.try_get("", "msg_id").context(DatabaseSnafu)?;
            let message: serde_json::Value = row.try_get("", "message").context(DatabaseSnafu)?;
            let get = |key: &str| {
                message
                    .get(key)
                    .and_then(serde_json::Value::as_i64)
                    .ok_or_else(|| {
                        sea_orm::DbErr::Custom(format!(
                            "embed task {task_id} lacks an integer {key}: {message}"
                        ))
                    })
                    .context(DatabaseSnafu)
            };
            Ok(EmbedTask {
                task_id,
                chat_id: get("chat_id")?,
                msg_id: get("msg_id")?,
                read_ct: row.try_get("", "read_ct").context(DatabaseSnafu)?,
            })
        })
        .collect()
}

pub async fn delete_embed(db: &impl ConnectionTrait, task_id: i64) -> Result<(), Error> {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT pgmq.delete($1, $2)",
        vec![Value::from(TEXT_EMBED_QUEUE), Value::from(task_id)],
    ))
    .await
    .context(DatabaseSnafu)?;
    Ok(())
}
