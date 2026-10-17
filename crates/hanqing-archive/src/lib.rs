pub mod backfill;

/// Which task queues a message insert dispatches into, derived from config:
/// `media` when S3 is configured, `embed` when `[embedding]` is configured.
/// Backfill commits message rows and tasks in one transaction. The live
/// update path commits the message first and enqueues in a following
/// transaction, so a crash there can leave a message without a task
/// (recoverable with `hanqing-cli embed enqueue-pending`); the reverse
/// (task without its source row) cannot happen.
#[derive(Debug, Clone, Copy, Default)]
pub struct Enqueue {
    pub media: bool,
    pub embed: bool,
}
pub mod error;
pub mod peers;
pub mod updates;

pub use backfill::*;
pub use peers::PeerRecorder;
pub use updates::run_update_stream;
