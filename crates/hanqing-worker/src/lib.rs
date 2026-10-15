//! Pluggable worker registry for the daemon.
//!
//! Workers are resident background tasks beyond the archive core (update
//! stream + backfill): media downloader, future embedding/takeout jobs, ...
//! Each implementation crate registers itself into [`WORKERS`] via
//! `#[distributed_slice]`; the daemon simply calls [`spawn_all`] — adding a
//! worker never touches `main.rs`.
//!
//! A worker decides for itself whether it is enabled (e.g. the media
//! downloader requires `[s3]` config); its `spawn` returns `None` when
//! disabled. Workers are detached tasks: a worker failure must never take
//! down archiving, so fatal errors are logged, not propagated.

use std::sync::Arc;

use grammers_client::Client;
use hanqing_config::HanqingConfig;
use hanqing_telegram::PgSession;
use sea_orm::DatabaseConnection;
use tokio::task::JoinHandle;
use tracing::{debug, info};

pub use linkme::distributed_slice;

mod error;
mod media;

/// Everything a worker needs from the daemon.
pub struct WorkerContext {
    pub client: Client,
    pub session: Arc<PgSession>,
    pub db: DatabaseConnection,
    pub config: HanqingConfig,
    pub self_id: i64,
}

/// A registered worker.
pub struct Worker {
    pub name: &'static str,
    /// Start the worker, or return `None` when it is disabled by config.
    pub spawn: fn(&WorkerContext) -> Option<JoinHandle<()>>,
}

/// All workers linked into the daemon binary.
#[distributed_slice]
pub static WORKERS: [Worker];

/// Spawn every enabled worker; returns the names of those started.
pub fn spawn_all(ctx: &WorkerContext) -> Vec<&'static str> {
    let mut started = Vec::new();
    for worker in WORKERS {
        if (worker.spawn)(ctx).is_some() {
            info!(name = worker.name, "worker started");
            started.push(worker.name);
        } else {
            debug!(name = worker.name, "worker not enabled");
        }
    }
    started
}
