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

use crate::error::{CredentialsSnafu, DatabaseSnafu, Error, InvokeSnafu, S3Snafu, SessionSnafu};
use crate::{WORKERS, Worker, WorkerContext, distributed_slice};
use hanqing_telegram::retry::is_transient;

/// Self-registration: the daemon's `hanqing_worker::spawn_all` picks this up
/// via the linkme distributed slice; no `main.rs` change needed.
#[distributed_slice(WORKERS)]
static MEDIA_DOWNLOADER: Worker = Worker {
    name: "media-downloader",
    spawn: spawn_media_downloader,
};

fn spawn_media_downloader(ctx: &WorkerContext) -> Option<JoinHandle<()>> {
    let s3 = ctx.config.s3.clone()?;
    match MediaDownloader::new(
        ctx.client.clone(),
        ctx.session.clone(),
        ctx.db.clone(),
        &s3,
        ctx.self_id,
    ) {
        Ok(downloader) => Some(tokio::spawn(async move { downloader.run().await })),
        Err(err) => {
            error!(%err, "media downloader disabled");
            None
        }
    }
}

/// Tasks stay hidden this long per attempt; a crash redelivers afterwards.
const VISIBILITY_TIMEOUT_SECS: i32 = 300;

/// Tasks consumed per `pgmq.read` (processed sequentially).
const BATCH_SIZE: i32 = 8;

/// Total deliveries before a task is marked `failed`. Generous on purpose:
/// attempts are only burned by transient errors, flood waits and crashes —
/// permanent failures are marked immediately without looking at `read_ct`.
const MAX_ATTEMPTS: i32 = 50;

/// Flood waits longer than this postpone the task (`set_vt`) instead of
/// sleeping inline.
const MAX_FLOOD_WAIT_SECS: u32 = 600;

/// Whole-file retries after a transient error (a failed `DownloadIter`
/// cannot resume, so each retry restarts from offset 0).
const MAX_DOWNLOAD_RETRIES: usize = 5;

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
    /// Flood wait beyond the inline-sleep cap; postpone by this many seconds.
    Postpone(i32),
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

        let tasks = queue::read(&self.db, VISIBILITY_TIMEOUT_SECS, BATCH_SIZE)
            .await
            .context(DatabaseSnafu)?;
        if tasks.is_empty() {
            tokio::time::sleep(IDLE_SLEEP).await;
            return Ok(());
        }

        for task in &tasks {
            self.process(task, settings.media.max_size_bytes).await;
        }
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
                info!(file_id, secs, "flood wait exceeds cap; postponing task");
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

    /// Download fully into memory (bounded by `max_size_bytes`). Flood waits
    /// are slept through; transient errors retry the whole file (grammers
    /// spends a failed `DownloadIter`, so partial resume is impossible).
    async fn download(&self, media: &TgMedia) -> Result<Download, Error> {
        let mut retries = 0;
        loop {
            let mut iter = self.client.iter_download(media);
            let mut bytes = Vec::with_capacity(media.size().unwrap_or(0));
            let result = loop {
                match iter.next().await {
                    Ok(Some(chunk)) => bytes.extend_from_slice(&chunk),
                    Ok(None) => break Ok(bytes),
                    Err(err) => break Err(err),
                }
            };

            match result {
                Ok(bytes) => return Ok(Download::Done(bytes)),
                Err(InvocationError::Rpc(rpc)) if rpc.name == "FLOOD_WAIT" => {
                    let wait = rpc.value.unwrap_or(30);
                    if wait > MAX_FLOOD_WAIT_SECS {
                        return Ok(Download::Postpone(wait as i32));
                    }
                    warn!(wait, "download flood wait; sleeping");
                    tokio::time::sleep(Duration::from_secs(u64::from(wait) + 1)).await;
                }
                Err(err) if is_transient(&err) && retries < MAX_DOWNLOAD_RETRIES => {
                    retries += 1;
                    let delay = Duration::from_secs(2 << retries.min(5));
                    warn!(%err, ?delay, retries, "transient download error; restarting file");
                    tokio::time::sleep(delay).await;
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
