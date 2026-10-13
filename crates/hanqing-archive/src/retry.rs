//! Shared retry policy for Telegram I/O.
//!
//! Both the backfill walk and the live update stream retry transient errors
//! with the same policy; FLOOD_WAIT and other RPC errors are *not* transient
//! (flood waits are handled explicitly by the caller, everything else aborts).

use std::time::Duration;

use backon::ExponentialBuilder;
use grammers_client::sender::InvocationError;

/// Consecutive transient failures tolerated before giving up.
const MAX_RETRIES: usize = 10;

/// Exponential backoff: 2s, 4s, 8s, ... capped at 60s, up to [`MAX_RETRIES`].
pub(crate) fn backoff() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_secs(2))
        .with_max_delay(Duration::from_secs(60))
        .with_max_times(MAX_RETRIES)
}

/// IO errors and 5xx RPC errors (e.g. -503 Timeout) are transient; other RPC
/// errors (auth, flood, bad request) must abort.
pub(crate) fn is_transient(err: &InvocationError) -> bool {
    match err {
        InvocationError::Io(_) => true,
        InvocationError::Rpc(rpc) => rpc.code <= -500,
        _ => false,
    }
}
