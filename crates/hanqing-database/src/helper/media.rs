//! Media metadata extraction and persistence.
//!
//! Metadata is extracted from the archived `messages.raw` JSONB (no Telegram
//! round-trip) and stored deduplicated by `file_id`. Downloading is a
//! separate concern: rows start as `pending` and a future downloader consumes
//! the queue.

use sea_orm::sea_query::OnConflict;
use sea_orm::{ActiveValue::Set, ConnectionTrait, EntityTrait as _};
use serde_json::Value;
use snafu::ResultExt;

use crate::error::{DatabaseSnafu, Error};
use crate::model::media::{
    ActiveModel as MediaActiveModel, Column as MediaColumn, Entity as MediaEntity,
};
use crate::model::sea_orm_active_enums::MediaKind;

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

/// Extract media metadata from a message's raw JSONB payload.
///
/// Works on the serialized TL shape (externally tagged), so the same function
/// serves both the live archive path and one-shot scans of stored rows.
/// `WebPage`/`Poll`/`Story` media are skipped (nothing worth archiving).
pub fn extract_media(chat_id: i64, msg_id: i64, raw: &Value) -> Option<NewMedia> {
    let media = raw.get("Message")?.get("media")?;
    if media.is_null() {
        return None;
    }

    if let Some(photo) = media.get("Photo") {
        let inner = photo.pointer("/photo/Photo")?;
        let file_id = inner.get("id")?.as_i64()?;

        // Largest available rendition: `Progressive` carries a size ladder
        // (last = full), `Size` carries a single `size`.
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
        let has_sticker =
            attributes.is_some_and(|attrs| attrs.iter().any(|attr| attr.get("Sticker").is_some()));
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
pub async fn save_media<C: ConnectionTrait>(db: &C, media: Vec<NewMedia>) -> Result<usize, Error> {
    let count = media.len();
    if count == 0 {
        return Ok(0);
    }

    let models = media.into_iter().map(|m| MediaActiveModel {
        file_id: Set(m.file_id),
        chat_id: Set(m.chat_id),
        msg_id: Set(m.msg_id),
        kind: Set(m.kind),
        mime: Set(m.mime),
        size_bytes: Set(m.size_bytes),
        file_name: Set(m.file_name),
        ..Default::default()
    });

    MediaEntity::insert_many(models)
        .on_conflict(
            OnConflict::column(MediaColumn::FileId)
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
        .context(DatabaseSnafu)?;

    Ok(count)
}
