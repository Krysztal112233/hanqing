use std::sync::Arc;

use grammers_client::Client;
use grammers_mtsender::SenderPool;
use grammers_session::storages::SqliteSession;
use hanqing_config::TelegramConfig;
use snafu::ResultExt;

use crate::error::{Error, SessionSnafu};

/// Connect to Telegram and return a client handle.
///
/// The sender-pool runner is spawned onto the current tokio runtime; the
/// session is persisted to `config.session_file` (SQLite). The caller is
/// responsible for the sign-in flow when `client.is_authorized()` is false.
pub async fn connect(config: &TelegramConfig) -> Result<Client, Error> {
    let session = Arc::new(
        SqliteSession::open(&config.session_file)
            .await
            .context(SessionSnafu)?,
    );

    let SenderPool { runner, handle, .. } = SenderPool::new(session, config.api_id);
    let client = Client::new(handle);
    let _runner = tokio::spawn(runner.run());

    Ok(client)
}
