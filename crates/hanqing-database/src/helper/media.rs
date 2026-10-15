//! Media metadata extraction and persistence (`media` table).
//!
//! Metadata is extracted from the archived `messages.raw` JSONB (no Telegram
//! round-trip) and stored deduplicated by `file_id`. Downloading is a
//! separate concern: rows start as `pending` and a future downloader consumes
//! the queue.

use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ActiveModelTrait as _, ActiveValue::Set, ColumnTrait as _, ConnectionTrait, EntityTrait as _,
    QueryFilter as _, QueryOrder as _, QuerySelect as _,
};
use serde_json::Value;
use snafu::ResultExt;

use crate::error::{DatabaseSnafu, Error};
use crate::model::media::{ActiveModel, Column, Model};
use crate::model::prelude::Media;
use crate::model::sea_orm_active_enums::{MediaKind, MediaStatus};

#[derive(Debug, Clone)]
pub struct NewMedia {
    pub file_id: i64,
    pub chat_id: i64,
    pub msg_id: i64,
    pub kind: MediaKind,
    pub mime: Option<String>,
    pub size_bytes: Option<i64>,
    pub file_name: Option<String>,
}

#[async_trait::async_trait]
pub trait MediaHelper {
    /// Extract media metadata from a message's raw JSONB payload.
    ///
    /// Works on the serialized TL shape (externally tagged), so the same
    /// function serves both the live archive path and one-shot scans of
    /// stored rows. `WebPage`/`Poll`/`Story` media are skipped (nothing
    /// worth archiving).
    fn extract(chat_id: i64, msg_id: i64, raw: &Value) -> Option<NewMedia> {
        let media = raw.get("Message")?.get("media")?;
        if media.is_null() {
            return None;
        }

        if let Some(photo) = media.get("Photo") {
            let inner = photo.pointer("/photo/Photo")?;
            let file_id = inner.get("id")?.as_i64()?;

            // Largest available rendition: `Progressive` carries a size
            // ladder (last = full), `Size` carries a single `size`.
            let size_bytes = inner
                .get("sizes")
                .and_then(Value::as_array)
                .and_then(|sizes| {
                    sizes
                        .iter()
                        .filter_map(|s| {
                            if let Some(p) = s.get("Progressive") {
                                p.get("sizes")?
                                    .as_array()?
                                    .iter()
                                    .filter_map(Value::as_i64)
                                    .max()
                            } else {
                                s.get("Size")?.get("size")?.as_i64()
                            }
                        })
                        .max()
                });

            return Some(NewMedia {
                file_id,
                chat_id,
                msg_id,
                kind: MediaKind::Photo,
                mime: Some("image/jpeg".to_string()),
                size_bytes,
                file_name: None,
            });
        }

        if let Some(document) = media.get("Document") {
            let inner = document.pointer("/document/Document")?;
            let file_id = inner.get("id")?.as_i64()?;

            let size_bytes = inner.get("size").and_then(Value::as_i64);
            let mime = inner
                .get("mime_type")
                .and_then(Value::as_str)
                .map(str::to_owned);

            let attributes = inner.get("attributes").and_then(Value::as_array);
            let has_sticker = attributes
                .is_some_and(|attrs| attrs.iter().any(|attr| attr.get("Sticker").is_some()));
            let file_name = attributes.and_then(|attrs| {
                attrs.iter().find_map(|attr| {
                    attr.get("Filename")?
                        .get("file_name")?
                        .as_str()
                        .map(str::to_owned)
                })
            });

            let flag = |name: &str| document.get(name).and_then(Value::as_bool).unwrap_or(false);
            let kind = if flag("round") {
                MediaKind::Round
            } else if flag("voice") {
                MediaKind::Voice
            } else if flag("video") {
                MediaKind::Video
            } else if has_sticker {
                MediaKind::Sticker
            } else {
                MediaKind::Document
            };

            return Some(NewMedia {
                file_id,
                chat_id,
                msg_id,
                kind,
                mime,
                size_bytes,
                file_name,
            });
        }

        None
    }

    /// Bulk-insert media rows; the same `file_id` seen again is a no-op.
    async fn save_all(db: &impl ConnectionTrait, media: Vec<NewMedia>) -> Result<usize, Error> {
        let count = media.len();
        if count == 0 {
            return Ok(0);
        }

        let models = media.into_iter().map(|m| ActiveModel {
            file_id: Set(m.file_id),
            chat_id: Set(m.chat_id),
            msg_id: Set(m.msg_id),
            kind: Set(m.kind),
            mime: Set(m.mime),
            size_bytes: Set(m.size_bytes),
            file_name: Set(m.file_name),
            ..Default::default()
        });

        Media::insert_many(models)
            .on_conflict(OnConflict::column(Column::FileId).do_nothing().to_owned())
            .exec_without_returning(db)
            .await
            .context(DatabaseSnafu)?;

        Ok(count)
    }

    /// One media row by primary key.
    async fn find_one(db: &impl ConnectionTrait, file_id: i64) -> Result<Option<Model>, Error> {
        Media::find_by_id(file_id)
            .one(db)
            .await
            .context(DatabaseSnafu)
    }

    /// `file_id`s of `pending` rows after `after`, keyset-paginated. Backs
    /// the CLI backlog enqueue into the download queue.
    async fn find_pending_ids(
        db: &impl ConnectionTrait,
        after: i64,
        limit: u64,
    ) -> Result<Vec<i64>, Error> {
        Media::find()
            .select_only()
            .column(Column::FileId)
            .filter(Column::Status.eq(MediaStatus::Pending))
            .filter(Column::FileId.gt(after))
            .order_by_asc(Column::FileId)
            .limit(limit)
            .into_tuple::<i64>()
            .all(db)
            .await
            .context(DatabaseSnafu)
    }

    /// Per-status row and byte counts (unknown sizes excluded from bytes).
    async fn status_counts(db: &impl ConnectionTrait) -> Result<Vec<(String, i64, i64)>, Error> {
        let rows = db
            .query_all_raw(sea_orm::Statement::from_string(
                sea_orm::DbBackend::Postgres,
                "SELECT status::text, count(*)::bigint, \
                 coalesce(sum(size_bytes), 0)::bigint AS bytes \
                 FROM media GROUP BY status ORDER BY status",
            ))
            .await
            .context(DatabaseSnafu)?;
        rows.iter()
            .map(|row| {
                Ok((
                    row.try_get("", "status").context(DatabaseSnafu)?,
                    row.try_get("", "count").context(DatabaseSnafu)?,
                    row.try_get("", "bytes").context(DatabaseSnafu)?,
                ))
            })
            .collect()
    }

    /// Persist a successful download: final status plus the object key.
    async fn mark_done(db: &impl ConnectionTrait, file_id: i64, key: &str) -> Result<(), Error> {
        ActiveModel {
            file_id: Set(file_id),
            status: Set(MediaStatus::Done),
            path: Set(Some(key.to_string())),
            downloaded_at: Set(Some(chrono::Utc::now().fixed_offset())),
            ..Default::default()
        }
        .update(db)
        .await
        .context(DatabaseSnafu)?;
        Ok(())
    }

    /// Persist a terminal (`skipped`/`failed`) or in-flight (`downloading`)
    /// state.
    async fn mark_state(
        db: &impl ConnectionTrait,
        file_id: i64,
        status: MediaStatus,
    ) -> Result<(), Error> {
        ActiveModel {
            file_id: Set(file_id),
            status: Set(status),
            ..Default::default()
        }
        .update(db)
        .await
        .context(DatabaseSnafu)?;
        Ok(())
    }
}

impl MediaHelper for Media {}
