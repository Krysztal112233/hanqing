//! Media download worker: consumes the pgmq `media_download` queue,
//! downloads from Telegram (reusing the daemon's MTProto session), stores
//! into S3, and updates the `media` registry row.
//!
//! Queue semantics:
//! - a task is deleted only after its final state (`done`/`skipped`/
//!   `failed`) is persisted; crashes and transient errors rely on the
//!   visibility timeout for redelivery;
//! - long flood waits postpone the task via `set_vt` instead of burning
//!   attempts busy-waiting;
//! - `read_ct` caps total attempts at [`MAX_ATTEMPTS`], after which the row
//!   is marked `failed`.
//!
//! The `file_reference` inside archived raw messages expires, so every
//! download re-fetches its message from Telegram (fresh reference) by
//! `(chat_id, msg_id)`. Messages that can no longer be fetched (deleted at
//! the source, or the account left the group) are marked `failed`.

use std::sync::Arc;
use std::time::Duration;

use grammers_client::Client;
use grammers_client::media::Media as TgMedia;
use grammers_client::sender::InvocationError;
use grammers_client::session::Session as _;
use grammers_client::session::types::PeerId;
use hanqing_config::S3Config;
use hanqing_database::helper::prelude::*;
use hanqing_database::model::media::Model as MediaModel;
use hanqing_database::model::prelude::{AccountSettings, Media};
use hanqing_database::model::sea_orm_active_enums::MediaStatus;
use hanqing_database::queue::{self, QueueTask};
use hanqing_telegram::PgSession;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use sea_orm::DatabaseConnection;
use snafu::ResultExt;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use backon::Retryable as _;

use crate::error::{CredentialsSnafu, DatabaseSnafu, Error, InvokeSnafu, S3Snafu, SessionSnafu};
use crate::{WORKERS, Worker, WorkerContext, distributed_slice};
use hanqing_telegram::retry::{backoff, is_transient};

/// Self-registration: the daemon's `hanqing_worker::spawn_all` picks this up
/// via the linkme distributed slice; no `main.rs` change needed.
#[distributed_slice(WORKERS)]
static MEDIA_DOWNLOADER: Worker = Worker {
    name: "media-downloader",
    spawn: spawn_media_downloader,
};

fn spawn_media_downloader(ctx: &WorkerContext) -> Option<JoinHandle<()>> {
    let s3 = ctx.config.s3.clone()?;
    let replicas = ctx.config.worker.media.download_workers;
    if replicas == 0 {
        info!("worker.media.download_workers = 0; downloader disabled");
        return None;
    }
    let mut handle = None;
    for replica in 0..replicas {
        match MediaDownloader::new(
            ctx.client.clone(),
            ctx.session.clone(),
            ctx.db.clone(),
            &s3,
            ctx.self_id,
        ) {
            Ok(downloader) => {
                let h = tokio::spawn(async move { downloader.run().await });
                handle.get_or_insert(h);
            }
            Err(err) => {
                error!(%err, replica, "media downloader replica disabled");
                return None;
            }
        }
    }
    handle
}

/// Tasks stay hidden this long per attempt; a crash redelivers afterwards.
/// One task per read, so the VT only ever has to cover a single item.
const VISIBILITY_TIMEOUT_SECS: i32 = 300;

/// Total deliveries before a task is marked `failed`. Generous on purpose:
/// attempts are only burned by transient errors, flood waits and crashes —
/// permanent failures are marked immediately without looking at `read_ct`.
const MAX_ATTEMPTS: i32 = 50;

/// Flood waits longer than this postpone the task (`set_vt`) instead of
/// sleeping inline.
const MAX_FLOOD_WAIT_SECS: u32 = 600;

/// Consecutive short flood waits tolerated before parking the task. A
/// persistent flood wait for one file must not hold the only media worker
/// (and therefore the whole queue) forever.
const MAX_INLINE_FLOOD_WAITS: u32 = 3;

/// Minimum visibility timeout when repeated short flood waits park a task.
const FLOOD_POSTPONE_SECS: u32 = 300;

/// Idle poll cadence when the queue is empty; when downloads are disabled
/// (`media.download_enabled = false`) the worker sleeps on this cadence too,
/// re-reading the settings document every cycle.
const IDLE_SLEEP: Duration = Duration::from_secs(30);

/// Outcome of fetching and downloading one media item.
enum Download {
    Done(Vec<u8>),
    /// Permanent failure (gone at the source, left the group, ...).
    Unavailable(String),
    Oversize,
    /// Flood wait beyond the inline-sleep cap, or repeated short flood waits:
    /// postpone the task by this many seconds.
    Postpone(i32),
}

/// Decide whether a flood wait should park the task instead of sleeping
/// inline. Long waits park immediately; short ones park after a run of
/// `MAX_INLINE_FLOOD_WAITS`, so one file cannot hold the whole queue.
fn flood_postpone(wait: u32, consecutive: u32) -> Option<i32> {
    if wait > MAX_FLOOD_WAIT_SECS || consecutive >= MAX_INLINE_FLOOD_WAITS {
        Some(wait.max(FLOOD_POSTPONE_SECS) as i32)
    } else {
        None
    }
}

/// Media downloader wired to the daemon's Telegram client and session.
pub struct MediaDownloader {
    client: Client,
    session: Arc<PgSession>,
    db: DatabaseConnection,
    bucket: Box<Bucket>,
    self_id: i64,
}

impl MediaDownloader {
    pub fn new(
        client: Client,
        session: Arc<PgSession>,
        db: DatabaseConnection,
        s3: &S3Config,
        self_id: i64,
    ) -> Result<Self, Error> {
        let region = Region::Custom {
            region: s3.region.clone(),
            endpoint: s3.endpoint.clone(),
        };
        let credentials = Credentials::new(
            Some(&s3.access_key_id),
            Some(&s3.secret_access_key),
            None,
            None,
            None,
        )
        .context(CredentialsSnafu)?;
        // Path style: self-hosted S3 (MinIO and friends) rarely maps bucket
        // subdomains.
        let bucket = Bucket::new(&s3.bucket, region, credentials)
            .context(S3Snafu)?
            .with_path_style();
        Ok(Self {
            client,
            session,
            db,
            bucket,
            self_id,
        })
    }

    /// Consume the queue forever. Behavior settings are re-read every cycle,
    /// so `hanqing-cli settings set media.*` applies without a restart.
    pub async fn run(&self) -> ! {
        loop {
            if let Err(err) = self.cycle().await {
                warn!(%err, "media downloader cycle failed; retrying");
                tokio::time::sleep(IDLE_SLEEP).await;
            }
        }
    }

    async fn cycle(&self) -> Result<(), Error> {
        let settings = AccountSettings::load_or_init(&self.db, self.self_id)
            .await
            .context(DatabaseSnafu)?;
        if !settings.media.download_enabled {
            debug!("media downloads disabled; idling");
            tokio::time::sleep(IDLE_SLEEP).await;
            return Ok(());
        }

        let tasks = queue::read(&self.db, VISIBILITY_TIMEOUT_SECS, 1)
            .await
            .context(DatabaseSnafu)?;
        let Some(task) = tasks.into_iter().next() else {
            tokio::time::sleep(IDLE_SLEEP).await;
            return Ok(());
        };

        self.process(&task, settings.media.max_size_bytes).await;
        Ok(())
    }

    /// Process one task; never fails fatally. Every path either deletes the
    /// task (final state persisted) or leaves it for VT redelivery.
    async fn process(&self, task: &QueueTask, max_size: u64) {
        let file_id = task.file_id;
        let row = match Media::find_one(&self.db, file_id).await {
            Ok(Some(row)) => row,
            // Registry row gone (message deleted at archive): drop the task.
            Ok(None) => return self.finish(task).await,
            Err(err) => return warn!(%err, file_id, "failed to load media row"),
        };

        // Also accept `downloading` rows: a crash mid-download leaves one,
        // and this delivery is its redelivery.
        if !matches!(row.status, MediaStatus::Pending | MediaStatus::Downloading) {
            debug!(file_id, status = %row.status, "already terminal; dropping task");
            return self.finish(task).await;
        }
        if task.read_ct > MAX_ATTEMPTS {
            warn!(
                file_id,
                read_ct = task.read_ct,
                "attempt cap reached; marking failed"
            );
            return self.finish_failed(task).await;
        }
        if row.size_bytes.is_some_and(|size| size as u64 > max_size) {
            debug!(
                file_id,
                size = row.size_bytes,
                max_size,
                "over size cap; skipping"
            );
            return self.finish_state(task, MediaStatus::Skipped).await;
        }

        if let Err(err) = Media::mark_state(&self.db, file_id, MediaStatus::Downloading).await {
            return warn!(%err, file_id, "failed to mark downloading");
        }

        match self.fetch_and_download(&row, max_size).await {
            Ok(Download::Done(bytes)) => self.store(task, &row, bytes).await,
            Ok(Download::Unavailable(reason)) => {
                info!(file_id, %reason, "media unavailable; marking failed");
                self.finish_failed(task).await;
            }
            Ok(Download::Oversize) => {
                debug!(file_id, max_size, "fresh size over cap; skipping");
                self.finish_state(task, MediaStatus::Skipped).await;
            }
            Ok(Download::Postpone(secs)) => {
                info!(file_id, secs, "flood wait; postponing task");
                if let Err(err) = queue::set_vt(&self.db, task.msg_id, secs).await {
                    warn!(%err, file_id, "failed to postpone task; VT redelivers");
                }
            }
            Err(err) => warn!(%err, file_id, "download failed; task redelivers"),
        }
    }

    /// Upload to S3, then persist `done` and delete the task.
    async fn store(&self, task: &QueueTask, row: &MediaModel, bytes: Vec<u8>) {
        let key = object_key(row);
        let mime = row.mime.as_deref().unwrap_or("application/octet-stream");
        if let Err(err) = self
            .bucket
            .put_object_with_content_type(&key, &bytes, mime)
            .await
        {
            return warn!(%err, file_id = row.file_id, key, "S3 upload failed; task redelivers");
        }
        match Media::mark_done(&self.db, row.file_id, &key).await {
            Ok(()) => {
                debug!(
                    file_id = row.file_id,
                    key,
                    bytes = bytes.len(),
                    "media stored"
                );
                self.finish(task).await;
            }
            Err(err) => warn!(%err, file_id = row.file_id, key, "failed to mark done"),
        }
    }

    /// Re-fetch the message (fresh `file_reference`) and download its media.
    async fn fetch_and_download(&self, row: &MediaModel, max_size: u64) -> Result<Download, Error> {
        let Some(peer_id) = PeerId::from_bot_api_dialog_id(row.chat_id) else {
            return Ok(Download::Unavailable("chat id out of range".into()));
        };
        let peer_ref = match self.session.peer_ref(peer_id).await {
            Ok(Some(peer_ref)) => peer_ref,
            Ok(None) => return Ok(Download::Unavailable("peer not in session cache".into())),
            Err(err) => return Err(Box::new(err)).context(SessionSnafu),
        };

        let Ok(msg_id) = i32::try_from(row.msg_id) else {
            return Ok(Download::Unavailable("msg id out of range".into()));
        };
        let messages = match self.client.get_messages_by_id(peer_ref, &[msg_id]).await {
            Ok(messages) => messages,
            // Long flood wait: park the task instead of burning attempts.
            Err(InvocationError::Rpc(rpc)) if rpc.name == "FLOOD_WAIT" => {
                return Ok(Download::Postpone(rpc.value.unwrap_or(300) as i32));
            }
            // 400/403 here means the peer or message is permanently
            // inaccessible (left the group, banned, invalid id): no amount
            // of redelivery will fix it.
            Err(InvocationError::Rpc(rpc)) if rpc.code == 400 || rpc.code == 403 => {
                return Ok(Download::Unavailable(format!(
                    "message fetch permanently denied ({} {})",
                    rpc.code, rpc.name
                )));
            }
            Err(err) => return Err(err).context(InvokeSnafu),
        };
        let Some(message) = messages.into_iter().next().flatten() else {
            return Ok(Download::Unavailable("message not retrievable".into()));
        };
        let Some(media) = message.media() else {
            return Ok(Download::Unavailable("message carries no media".into()));
        };
        if media.size().is_some_and(|size| size as u64 > max_size) {
            return Ok(Download::Oversize);
        }

        self.download(&media).await
    }

    /// Download fully into memory (bounded by `max_size_bytes`). Transient
    /// errors retry via the shared backon policy (a fresh iterator per
    /// attempt: grammers spends a failed `DownloadIter`, so retries restart
    /// from offset 0). Flood waits are slept through outside the retry
    /// budget; long ones, and a run of short ones, park the task instead.
    async fn download(&self, media: &TgMedia) -> Result<Download, Error> {
        let mut flood_waits = 0u32;
        loop {
            let result = (|| async {
                let mut iter = self.client.iter_download(media);
                let mut bytes = Vec::with_capacity(media.size().unwrap_or(0));
                while let Some(chunk) = iter.next().await? {
                    bytes.extend_from_slice(&chunk);
                }
                Ok(bytes)
            })
            .retry(backoff())
            .when(is_transient)
            .notify(|err, delay| warn!(%err, ?delay, "transient download error; restarting file"))
            .await;

            match result {
                Ok(bytes) => return Ok(Download::Done(bytes)),
                Err(InvocationError::Rpc(rpc)) if rpc.name == "FLOOD_WAIT" => {
                    let wait = rpc.value.unwrap_or(30);
                    flood_waits += 1;
                    if let Some(secs) = flood_postpone(wait, flood_waits) {
                        info!(wait, secs, "flood wait; postponing task");
                        return Ok(Download::Postpone(secs));
                    }
                    warn!(wait, "download flood wait; sleeping");
                    tokio::time::sleep(Duration::from_secs(u64::from(wait) + 1)).await;
                }
                // `fetch_and_download` already refreshed the message and its
                // file reference; a 400/403 here is a permanent per-file
                // failure (expired reference, revoked access, ...). Mark it
                // unavailable instead of burning redeliveries.
                Err(InvocationError::Rpc(rpc)) if rpc.code == 400 || rpc.code == 403 => {
                    return Ok(Download::Unavailable(format!(
                        "download permanently denied ({} {})",
                        rpc.code, rpc.name
                    )));
                }
                Err(err) => return Err(err).context(InvokeSnafu),
            }
        }
    }

    /// Delete the task after its final state is persisted.
    async fn finish(&self, task: &QueueTask) {
        if let Err(err) = queue::delete(&self.db, task.msg_id).await {
            warn!(%err, msg_id = task.msg_id, "failed to delete queue task");
        }
    }

    async fn finish_state(&self, task: &QueueTask, status: MediaStatus) {
        match Media::mark_state(&self.db, task.file_id, status).await {
            Ok(()) => self.finish(task).await,
            Err(err) => warn!(%err, file_id = task.file_id, "failed to mark media state"),
        }
    }

    async fn finish_failed(&self, task: &QueueTask) {
        self.finish_state(task, MediaStatus::Failed).await;
    }
}

/// S3 object key: `media/<kind>/<file_id>` — a pure identity, no extension.
/// The object carries Content-Type (from `media.mime`) and the row keeps the
/// original file name, so the extension would be decorative and its
/// derivation a maintenance cost. `kind` and `file_id` are fixed at row
/// insertion, so re-uploads overwrite the same key.
fn object_key(row: &MediaModel) -> String {
    format!("media/{}/{}", row.kind, row.file_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_flood_wait_parks_immediately() {
        assert_eq!(
            flood_postpone(MAX_FLOOD_WAIT_SECS + 1, 1),
            Some((MAX_FLOOD_WAIT_SECS + 1) as i32)
        );
    }

    #[test]
    fn short_flood_waits_sleep_until_the_cap() {
        assert_eq!(flood_postpone(8, 1), None);
        assert_eq!(flood_postpone(8, 2), None);
        assert_eq!(
            flood_postpone(8, MAX_INLINE_FLOOD_WAITS),
            Some(FLOOD_POSTPONE_SECS as i32)
        );
    }

    #[test]
    fn repeated_short_waits_keep_the_minimum_park() {
        assert_eq!(flood_postpone(600, MAX_INLINE_FLOOD_WAITS), Some(600));
    }
}
