use std::sync::Arc;

use grammers_client::Client;
use grammers_client::peer::User;
use grammers_client::session::updates::UpdatesLike;
use grammers_mtsender::SenderPool;
use hanqing_config::TelegramConfig;
use sea_orm::DatabaseConnection;
use snafu::ResultExt;
use tokio::sync::mpsc;

use crate::error::{Error, InvokeSnafu, SessionStorageSnafu};
use crate::session::PgSession;

/// A connected client plus the raw update receiver for the update stream.
pub struct Connection {
    pub client: Client,
    /// Feed into [`Client::stream_updates`]; dropped receivers disable update
    /// reception (fine for short-lived commands like `login`).
    pub updates: mpsc::Receiver<UpdatesLike>,
}

/// Connect to Telegram.
///
/// The session is stored in PostgreSQL (`session_*` tables via [`PgSession`]);
/// the sender-pool runner is spawned onto the current tokio runtime. This
/// crate never performs interactive sign-in: authorization is established by
/// `hanqing-cli login` and discovered here via [`authorized_user`].
pub async fn connect(config: &TelegramConfig, db: DatabaseConnection) -> Result<Connection, Error> {
    let session = Arc::new(PgSession::open(db).await.context(SessionStorageSnafu)?);

    let SenderPool {
        runner,
        handle,
        updates,
        ..
    } = SenderPool::new(session, config.api_id.get());
    let client = Client::new(handle);
    let _runner = tokio::spawn(runner.run());

    Ok(Connection { client, updates })
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
