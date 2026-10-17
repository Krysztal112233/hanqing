//! Persistence for `message_embeddings`. Storage is ORM-native
//! (`PgVector`); distance queries cast both sides to `halfvec` so the
//! (deferred) HNSW expression index on `halfvec_cosine_ops` applies — the
//! same pattern fandwill settled on.

use std::collections::BTreeMap;

use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ActiveValue::Set, ColumnTrait as _, ConnectionTrait, DbBackend, EntityTrait as _,
    QueryFilter as _, Statement, Value,
};
use snafu::ResultExt;

use crate::error::{DatabaseSnafu, Error};
use crate::model::message_embeddings::{ActiveModel, Column};
use crate::model::prelude::MessageEmbeddings;

/// Vector dimensions; must match the column and the configured model.
pub const DIMENSIONS: u32 = hanqing_config::EMBEDDING_DIMENSIONS;

/// One semantic-search hit: message identifiers plus distance.
#[derive(Debug)]
pub struct EmbedHit {
    pub chat_id: i64,
    pub msg_id: i64,
    pub date: sea_orm::prelude::DateTimeWithTimeZone,
    pub sender_id: Option<i64>,
    pub text: String,
    /// Cosine distance (0 = identical).
    pub distance: f64,
    pub chat_name: Option<String>,
    pub sender_name: Option<String>,
}

/// Formats a vector in pgvector's text form `[0.1,-0.2,...]` for binding as
/// a `String` parameter cast via `$N::halfvec(N)`.
///
/// NOTE: workaround — SeaORM's `Value` has `Vector` but no `HalfVector`
/// variant, and `pgvector::HalfVector` has no `Display`. Drop this once
/// SeaORM gains native halfvec bindings.
pub fn vector_literal(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 8);
    s.push('[');
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&x.to_string());
    }
    s.push(']');
    s
}

/// Collapse duplicate `(chat_id, msg_id)` rows before one upsert statement.
/// Postgres rejects `ON CONFLICT DO UPDATE` when the same conflict key is
/// touched twice; duplicate queue tasks are expected (edits, re-enqueues),
/// so the last vector wins here.
fn dedupe_rows(rows: &[(i64, i64, Vec<f32>)]) -> Vec<((i64, i64), &Vec<f32>)> {
    let mut unique = BTreeMap::new();
    for (chat_id, msg_id, embedding) in rows {
        unique.insert((*chat_id, *msg_id), embedding);
    }
    unique.into_iter().collect()
}

/// Upsert embeddings; re-embedding a message replaces the old vector.
pub async fn save_all(
    db: &impl ConnectionTrait,
    rows: &[(i64, i64, Vec<f32>)],
    model: &str,
) -> Result<(), Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let models = dedupe_rows(rows)
        .into_iter()
        .map(|((chat_id, msg_id), embedding)| ActiveModel {
            chat_id: Set(chat_id),
            msg_id: Set(msg_id),
            embedding: Set(sea_orm::entity::prelude::PgVector::from(embedding.clone())),
            model: Set(model.to_owned()),
            ..Default::default()
        });
    MessageEmbeddings::insert_many(models)
        .on_conflict(
            OnConflict::columns([Column::ChatId, Column::MsgId])
                .update_columns([Column::Embedding, Column::Model, Column::EmbeddedAt])
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
        .context(DatabaseSnafu)?;
    Ok(())
}

/// Delete one message's embedding (for edits that empty the text).
pub async fn delete_one(db: &impl ConnectionTrait, chat_id: i64, msg_id: i64) -> Result<(), Error> {
    MessageEmbeddings::delete_many()
        .filter(Column::ChatId.eq(chat_id))
        .filter(Column::MsgId.eq(msg_id))
        .exec(db)
        .await
        .context(DatabaseSnafu)?;
    Ok(())
}

/// Cosine nearest neighbors among live, text-bearing messages embedded by
/// `model`. Optional chat filter; results ordered by distance ascending.
pub async fn search_neighbors(
    db: &impl ConnectionTrait,
    query: &[f32],
    model: &str,
    chat_id: Option<i64>,
    limit: u64,
    offset: u64,
) -> Result<Vec<EmbedHit>, Error> {
    let sql = format!(
        r#"SELECT m.chat_id, m.msg_id, m.date, m.sender_id, m.text,
                  (e.embedding::halfvec({DIMENSIONS}) <=> $1::halfvec({DIMENSIONS})) AS distance,
                  pc.name AS chat_name, ps.name AS sender_name
           FROM message_embeddings e
           JOIN messages m ON m.chat_id = e.chat_id AND m.msg_id = e.msg_id
           LEFT JOIN peers pc ON pc.id = m.chat_id
           LEFT JOIN peers ps ON ps.id = m.sender_id
           WHERE e.model = $2
             AND m.deleted_at IS NULL
             AND m.text <> ''
             AND ($3::bigint IS NULL OR m.chat_id = $3)
           ORDER BY e.embedding::halfvec({DIMENSIONS}) <=> $1::halfvec({DIMENSIONS})
           LIMIT $4 OFFSET $5"#
    );
    let rows = db
        .query_all_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            vec![
                Value::from(vector_literal(query)),
                Value::from(model.to_owned()),
                Value::from(chat_id),
                Value::from(limit),
                Value::from(offset),
            ],
        ))
        .await
        .context(DatabaseSnafu)?;
    rows.iter()
        .map(|row| {
            Ok(EmbedHit {
                chat_id: row.try_get("", "chat_id").context(DatabaseSnafu)?,
                msg_id: row.try_get("", "msg_id").context(DatabaseSnafu)?,
                date: row.try_get("", "date").context(DatabaseSnafu)?,
                sender_id: row.try_get("", "sender_id").context(DatabaseSnafu)?,
                text: row.try_get("", "text").context(DatabaseSnafu)?,
                distance: row.try_get("", "distance").context(DatabaseSnafu)?,
                chat_name: row.try_get("", "chat_name").context(DatabaseSnafu)?,
                sender_name: row.try_get("", "sender_name").context(DatabaseSnafu)?,
            })
        })
        .collect()
}

/// `message_refs -> text` lookup for a batch of embed tasks. Deleted messages
/// (or messages never archived) are simply absent from the map.
pub async fn fetch_texts(
    db: &impl ConnectionTrait,
    keys: &[(i64, i64)],
) -> Result<std::collections::HashMap<(i64, i64), String>, Error> {
    if keys.is_empty() {
        return Ok(Default::default());
    }
    let mut values = String::new();
    let mut params: Vec<Value> = Vec::with_capacity(keys.len() * 2);
    for (i, (chat_id, msg_id)) in keys.iter().enumerate() {
        if i > 0 {
            values.push(',');
        }
        values.push_str(&format!("(${}, ${})", i * 2 + 1, i * 2 + 2));
        params.push(Value::from(*chat_id));
        params.push(Value::from(*msg_id));
    }
    let rows = db
        .query_all_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            format!(
                "SELECT chat_id, msg_id, text FROM messages \
                 WHERE deleted_at IS NULL AND (chat_id, msg_id) IN ({values})"
            ),
            params,
        ))
        .await
        .context(DatabaseSnafu)?;
    Ok(rows
        .iter()
        .filter_map(|row| {
            Some((
                (
                    row.try_get("", "chat_id").ok()?,
                    row.try_get("", "msg_id").ok()?,
                ),
                row.try_get("", "text").ok()?,
            ))
        })
        .collect())
}

/// `(embedded live messages, embeddable messages)` for stats. With `model`,
/// only rows for that model count, so a model swap shows 0% instead of stale
/// coverage.
pub async fn coverage(db: &impl ConnectionTrait, model: Option<&str>) -> Result<(i64, i64), Error> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT \
               (SELECT count(*) FROM message_embeddings e \
                JOIN messages m ON m.chat_id = e.chat_id AND m.msg_id = e.msg_id \
                WHERE m.deleted_at IS NULL AND m.text <> '' \
                  AND ($1::text IS NULL OR e.model = $1)), \
               (SELECT count(*) FROM messages \
                WHERE deleted_at IS NULL AND text <> '')",
            vec![Value::String(model.map(str::to_owned))],
        ))
        .await
        .context(DatabaseSnafu)?
        .ok_or_else(|| sea_orm::DbErr::Custom("coverage query returned no row".into()))
        .context(DatabaseSnafu)?;
    Ok((
        row.try_get_by_index(0).context(DatabaseSnafu)?,
        row.try_get_by_index(1).context(DatabaseSnafu)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedupe_keeps_last_vector_per_message() {
        let rows = vec![(1, 2, vec![1.0]), (1, 2, vec![2.0]), (1, 3, vec![3.0])];
        let unique = dedupe_rows(&rows);
        assert_eq!(unique.len(), 2);
        assert_eq!(unique[0].0, (1, 2));
        assert_eq!(unique[0].1.as_slice(), &[2.0f32]);
        assert_eq!(unique[1].0, (1, 3));
    }

    #[test]
    fn vector_literal_uses_pgvector_text_form() {
        assert_eq!(vector_literal(&[1.0, -0.5]), "[1,-0.5]");
    }
}
