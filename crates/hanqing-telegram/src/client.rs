use std::io::{BufRead as _, Write as _};
use std::sync::Arc;

use grammers_client::{Client, SignInError};
use grammers_mtsender::SenderPool;
use hanqing_config::TelegramConfig;
use sea_orm::DatabaseConnection;
use snafu::ResultExt;
use tracing::info;

use crate::error::{Error, InvokeSnafu, PromptSnafu, SessionStorageSnafu, SignInSnafu};
use crate::session::PgSession;

/// Connect to Telegram and return a client handle.
///
/// The session is stored in PostgreSQL (`session_*` tables via [`PgSession`]);
/// the sender-pool runner is spawned onto the current tokio runtime. The
/// caller is responsible for the sign-in flow when `client.is_authorized()`
/// is false; see [`ensure_authorized`].
pub async fn connect(config: &TelegramConfig, db: DatabaseConnection) -> Result<Client, Error> {
    let session = Arc::new(PgSession::open(db).await.context(SessionStorageSnafu)?);

    let SenderPool { runner, handle, .. } = SenderPool::new(session, config.api_id);
    let client = Client::new(handle);
    let _runner = tokio::spawn(runner.run());

    Ok(client)
}

/// Sign in interactively when the session is not yet authorized.
///
/// The phone number comes from `config.phone` when set, otherwise from a
/// stdin prompt; the login code and optional 2FA password are always
/// prompted. Successful sign-in is persisted in the database, so this is a
/// one-time interactive step per account.
pub async fn ensure_authorized(client: &Client, config: &TelegramConfig) -> Result<(), Error> {
    if client.is_authorized().await.context(InvokeSnafu)? {
        return Ok(());
    }

    info!("session is not authorized; starting interactive sign-in");

    let phone = match &config.phone {
        Some(phone) => phone.clone(),
        None => prompt("Enter your phone number (international format): ")?,
    };

    let token = client
        .request_login_code(&phone, &config.api_hash)
        .await
        .context(InvokeSnafu)?;
    let code = prompt("Enter the code you received: ")?;

    match client.sign_in(&token, code.trim()).await {
        Ok(_) => info!("signed in"),
        Err(SignInError::PasswordRequired(password_token)) => {
            let hint = password_token.hint().unwrap_or("none");
            let password = prompt(&format!("Enter your 2FA password (hint: {hint}): "))?;
            client
                .check_password(password_token, password.trim())
                .await
                .context(SignInSnafu)?;
            info!("signed in with 2FA");
        }
        Err(e) => return Err(e).context(SignInSnafu),
    }

    Ok(())
}

fn prompt(message: &str) -> Result<String, Error> {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    stdout.write_all(message.as_bytes()).context(PromptSnafu)?;
    stdout.flush().context(PromptSnafu)?;

    let stdin = std::io::stdin();
    let mut line = String::new();
    stdin.lock().read_line(&mut line).context(PromptSnafu)?;
    Ok(line.trim().to_string())
}
