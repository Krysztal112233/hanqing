//! Backfill worker: runs one `backfill_all` pass per trigger on the pgmq
//! `backfill_run` queue.
//!
//! The trigger is deleted as soon as it is received: a backfill pass far
//! outlives any sensible visibility timeout, and a crash mid-pass is covered
//! by the daemon enqueueing a fresh trigger on every startup. Triggers
//! arriving mid-run queue up and are consumed one pass at a time — a
//! redundant pass finds no pending chats and exits immediately.
//!
//! `HANQING_SKIP_BACKFILL=1` suppresses the startup trigger (daemon side),
//! not the worker itself: manual `hanqing-cli backfill` triggers still run.

use std::time::Duration;

use sea_orm::DatabaseConnection;
use tokio::task::JoinHandle;
use tracing::{error, info};

use grammers_client::Client;
use hanqing_archive::PeerRecorder;
use hanqing_database::queue;
use snafu::ResultExt;

use crate::error::DatabaseSnafu;
use crate::{WORKERS, Worker, WorkerContext, distributed_slice};

/// Self-registration: the daemon's `hanqing_worker::spawn_all` picks this up
/// via the linkme distributed slice; no `main.rs` change needed.
#[distributed_slice(WORKERS)]
static BACKFILL_WORKER: Worker = Worker {
    name: "backfill",
    spawn: spawn_backfill_worker,
};

fn spawn_backfill_worker(ctx: &WorkerContext) -> Option<JoinHandle<()>> {
    let worker = BackfillWorker {
        client: ctx.client.clone(),
        db: ctx.db.clone(),
        self_id: ctx.self_id,
        enqueue: hanqing_archive::Enqueue {
            media: ctx.config.s3.is_some(),
            embed: ctx.config.embedding.is_some(),
        },
    };
    Some(tokio::spawn(async move { worker.run().await }))
}

/// Poll interval while the queue is empty. A few seconds of trigger latency
/// is irrelevant for a job that runs for hours.
const IDLE_SLEEP: Duration = Duration::from_secs(10);

struct BackfillWorker {
    client: Client,
    db: DatabaseConnection,
    self_id: i64,
    enqueue: hanqing_archive::Enqueue,
}

impl BackfillWorker {
    async fn run(self) {
        info!("backfill worker started; waiting for triggers");
        loop {
            match self.run_once().await {
                Ok(true) => {} // a pass just finished; poll again immediately
                Ok(false) => tokio::time::sleep(IDLE_SLEEP).await,
                Err(err) => {
                    error!(%err, "backfill worker iteration failed");
                    tokio::time::sleep(IDLE_SLEEP).await;
                }
            }
        }
    }

    /// Poll one trigger; if present, consume it and run one backfill pass.
    /// Returns whether a pass ran.
    async fn run_once(&self) -> Result<bool, crate::error::Error> {
        let Some(trigger) = queue::read_backfill_trigger(&self.db)
            .await
            .context(DatabaseSnafu)?
        else {
            return Ok(false);
        };
        queue::delete_backfill_trigger(&self.db, trigger.msg_id)
            .await
            .context(DatabaseSnafu)?;

        info!(?trigger.requested_by, "backfill triggered");
        // Fresh recorder per pass: re-upserting every sighted peer is how
        // renames propagate into the cache.
        let recorder = PeerRecorder::new(self.self_id);
        match hanqing_archive::backfill_all(
            &self.client,
            &self.db,
            self.self_id,
            self.enqueue,
            &recorder,
        )
        .await
        {
            Ok(stats) => info!(
                chats = stats.registered_chats,
                completed = stats.completed_chats,
                messages = stats.messages,
                "backfill pass complete"
            ),
            // A failed pass keeps its watermarks; the next trigger resumes.
            Err(err) => error!(%err, "backfill pass failed"),
        }
        Ok(true)
    }
}
