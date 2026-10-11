use std::sync::Arc;

use grammers_client::Client;
use grammers_client::peer::User;
use grammers_mtsender::SenderPool;
use hanqing_config::TelegramConfig;
use sea_orm::DatabaseConnection;
use snafu::ResultExt;

use crate::error::{Error, InvokeSnafu, SessionStorageSnafu};
use crate::session::PgSession;

/// Connect to Telegram and return a client handle.
///
/// The session is stored in PostgreSQL (`session_*` tables via [`PgSession`]);
/// the sender-pool runner is spawned onto the current tokio runtime. This
/// crate never performs interactive sign-in: authorization is established by
/// `hanqing-cli login` and discovered here via [`authorized_user`].
pub async fn connect(config: &TelegramConfig, db: DatabaseConnection) -> Result<Client, Error> {
    let session = Arc::new(PgSession::open(db).await.context(SessionStorageSnafu)?);

    let SenderPool { runner, handle, .. } = SenderPool::new(session, config.api_id);
    let client = Client::new(handle);
    let _runner = tokio::spawn(runner.run());

    Ok(client)
}

/// The signed-in user when the stored session is authorized, else `None`.
///
/// `is_authorized` maps Telegram's 401 responses to `false`, so `None` covers
/// both "no session yet" and "session revoked". Callers (the daemon) should
/// direct the operator to `hanqing-cli login`.
pub async fn authorized_user(client: &Client) -> Result<Option<User>, Error> {
    if !client.is_authorized().await.context(InvokeSnafu)? {
        return Ok(None);
    }
    client.get_me().await.map(Some).context(InvokeSnafu)
}
