//! Text embedder worker: consumes the pgmq `text_embed` queue, embeds
//! message text via the configured OpenAI-compatible service, and upserts
//! vectors into `message_embeddings`.
//!
//! Unlike the media pipeline there is no registry table: the queue task is
//! the only in-flight state, and the vector row itself is the final state.
//! Tasks pointing at missing/empty/deleted-text messages are dropped
//! (nothing to embed); poison tasks are dropped past `MAX_ATTEMPTS`
//! deliveries. Re-enqueueing is always possible via
//! `hanqing-cli embed enqueue-pending`.

use std::collections::BTreeMap;
use std::time::Duration;

use sea_orm::DatabaseConnection;
use snafu::ResultExt;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use hanqing_database::helper::embedding as embed_store;
use hanqing_database::helper::prelude::*;
use hanqing_database::model::prelude::AccountSettings;
use hanqing_database::queue::{self, EmbedTask};
use hanqing_embedding::EmbedClient;

use crate::error::DatabaseSnafu;
use crate::{WORKERS, Worker, WorkerContext, distributed_slice};

/// Self-registration: the daemon's `hanqing_worker::spawn_all` picks this up
/// via the linkme distributed slice; no `main.rs` change needed.
#[distributed_slice(WORKERS)]
static EMBEDDER: Worker = Worker {
    name: "embedder",
    spawn: spawn_embedder,
};

fn spawn_embedder(ctx: &WorkerContext) -> Option<JoinHandle<()>> {
    let config = ctx.config.embedding.clone()?;
    let client = match EmbedClient::new(&config) {
        Ok(client) => client,
        Err(err) => {
            error!(%err, "embedder disabled: invalid [embedding] config");
            return None;
        }
    };
    let embedder = Embedder {
        client,
        db: ctx.db.clone(),
        self_id: ctx.self_id,
    };
    Some(tokio::spawn(async move { embedder.run().await }))
}

/// Tasks embedded per API call (one read = one batched request).
const BATCH_SIZE: i32 = 32;

/// Tasks stay hidden this long per attempt; a crash redelivers afterwards.
/// Must exceed the client's request timeout so a slow call is not overtaken
/// by redelivery.
const VISIBILITY_TIMEOUT_SECS: i32 = 300;

/// Idle poll cadence; also the cadence when embedding is disabled.
const IDLE_SLEEP: Duration = Duration::from_secs(15);

/// Deliveries before a task is dropped as poison. Generous on purpose:
/// any service outage burns attempts, and there is no registry row to mark
/// a task failed, so prefer keeping work over silently dropping it.
const MAX_ATTEMPTS: i32 = 50;

struct Embedder {
    client: EmbedClient,
    db: DatabaseConnection,
    self_id: i64,
}

impl Embedder {
    async fn run(self) {
        info!(model = self.client.model, "embedder worker started");
        loop {
            if let Err(err) = self.cycle().await {
                warn!(%err, "embedder cycle failed; retrying");
                tokio::time::sleep(IDLE_SLEEP).await;
            }
        }
    }

    /// One read-embed-write cycle. Behavior settings are re-read every
    /// cycle, so `settings set embedding.enabled` applies without a restart.
    async fn cycle(&self) -> Result<(), crate::error::Error> {
        let settings = AccountSettings::load_or_init(&self.db, self.self_id)
            .await
            .context(DatabaseSnafu)?;
        if !settings.embedding.enabled {
            debug!("embedding disabled; idling");
            tokio::time::sleep(IDLE_SLEEP).await;
            return Ok(());
        }

        let tasks = queue::read_embed(&self.db, VISIBILITY_TIMEOUT_SECS, BATCH_SIZE)
            .await
            .context(DatabaseSnafu)?;
        if tasks.is_empty() {
            tokio::time::sleep(IDLE_SLEEP).await;
            return Ok(());
        }

        self.process(tasks).await;
        Ok(())
    }

    /// Process one batch; never fails fatally. Every task is either deleted
    /// (vector persisted / unembeddable / poison) or left for VT redelivery.
    async fn process(&self, tasks: Vec<EmbedTask>) {
        let (poison, tasks): (Vec<_>, Vec<_>) =
            tasks.into_iter().partition(|t| t.read_ct > MAX_ATTEMPTS);
        for task in &poison {
            warn!(
                chat_id = task.chat_id,
                msg_id = task.msg_id,
                read_ct = task.read_ct,
                "dropping poison embed task"
            );
            self.ack(task.task_id).await;
        }
        if tasks.is_empty() {
            return;
        }

        // Fetch the texts. Messages deleted since enqueueing (or with empty
        // text) have nothing to embed; their tasks are dropped after a
        // successful fetch/save.
        let keys: Vec<(i64, i64)> = tasks.iter().map(|t| (t.chat_id, t.msg_id)).collect();
        let texts = match embed_store::fetch_texts(&self.db, &keys).await {
            Ok(texts) => texts,
            Err(err) => {
                warn!(%err, "failed to fetch texts; tasks redelivered after VT");
                return;
            }
        };

        // Edits and repeated `enqueue-pending` runs can put duplicate
        // message keys in one read. Embed each key once; ack every task
        // after the vector is persisted.
        let mut embeddable: BTreeMap<(i64, i64), String> = BTreeMap::new();
        for task in &tasks {
            if let Some(text) = texts.get(&(task.chat_id, task.msg_id))
                && !text.is_empty()
            {
                embeddable.insert((task.chat_id, task.msg_id), text.clone());
            }
        }
        if embeddable.is_empty() {
            self.ack_all(&tasks).await;
            return;
        }

        let keys: Vec<(i64, i64)> = embeddable.keys().copied().collect();
        let input: Vec<String> = embeddable.values().cloned().collect();
        let vectors = match self.client.embed_documents(&input).await {
            Ok(vectors) => vectors,
            Err(err) => {
                warn!(%err, "embed request failed; tasks redelivered after VT");
                return;
            }
        };
        if vectors.len() != keys.len() {
            warn!(
                expected = keys.len(),
                got = vectors.len(),
                "embed response length mismatch; tasks redelivered after VT"
            );
            return;
        }
        let rows: Vec<(i64, i64, Vec<f32>)> = keys
            .into_iter()
            .zip(vectors)
            .map(|((chat_id, msg_id), vector)| (chat_id, msg_id, vector))
            .collect();
        if let Err(err) = embed_store::save_all(&self.db, &rows, &self.client.model).await {
            warn!(%err, "failed to persist embeddings; tasks redelivered after VT");
            return;
        }
        debug!(count = rows.len(), "embeddings stored");
        self.ack_all(&tasks).await;
    }

    /// Delete every task in a successfully handled batch.
    async fn ack_all(&self, tasks: &[EmbedTask]) {
        for task in tasks {
            self.ack(task.task_id).await;
        }
    }

    /// Delete a task whose outcome is final.
    async fn ack(&self, task_id: i64) {
        if let Err(err) = queue::delete_embed(&self.db, task_id).await {
            warn!(%err, task_id, "failed to delete embed task");
        }
    }
}
