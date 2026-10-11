//! Interactive sign-in flow (the only place hanqing talks to a TTY).
//!
//! Successful sign-in persists the session in PostgreSQL via `PgSession`, so
//! this is a one-time step per account; the daemon never prompts.

use grammers_client::client::{LoginToken, PasswordToken};
use grammers_client::peer::User;
use grammers_client::{Client, SignInError};
use inquire::validator::Validation;
use inquire::{Password, PasswordDisplayMode, Text};
use snafu::ResultExt;
use tracing::info;

use crate::error::{
    Error, InvokeSnafu, PromptSnafu, SignInSnafu, TelegramSnafu, TooManyAttemptsSnafu,
};

/// Attempts allowed for each of code entry and 2FA password entry.
const MAX_ATTEMPTS: u32 = 3;

/// Sign in interactively unless the stored session is already authorized.
///
/// `phone` comes from `--phone`; prompted when absent. Codes and passwords
/// are always prompted, never taken from config or argv (they must not land
/// in shell history or committed files).
pub async fn login(client: &Client, api_hash: &str, phone: Option<String>) -> Result<(), Error> {
    if let Some(me) = hanqing_telegram::authorized_user(client)
        .await
        .context(TelegramSnafu)?
    {
        println!("already signed in as {}", describe(&me));
        return Ok(());
    }

    let phone = match phone {
        Some(phone) => phone,
        None => Text::new("Phone number (international format):")
            .with_validator(|input: &str| non_empty(input, "phone number required"))
            .prompt()
            .context(PromptSnafu)?,
    };

    let token = client
        .request_login_code(&phone, api_hash)
        .await
        .context(InvokeSnafu)?;

    let user = code_loop(client, &token).await?;
    println!("signed in as {}", describe(&user));
    info!("session persisted in database");
    Ok(())
}

async fn code_loop(client: &Client, token: &LoginToken) -> Result<User, Error> {
    for attempt in 1..=MAX_ATTEMPTS {
        let code = Text::new("Code you received:")
            .with_validator(|input: &str| {
                let input = input.trim();
                if !input.is_empty() && input.chars().all(|c| c.is_ascii_digit()) {
                    Ok(Validation::Valid)
                } else {
                    Ok(Validation::Invalid("numeric code required".into()))
                }
            })
            .prompt()
            .context(PromptSnafu)?;

        match client.sign_in(token, code.trim()).await {
            Ok(user) => return Ok(user),
            // PasswordRequired hands us the 2FA token; InvalidPassword hands
            // it back for the next attempt.
            Err(SignInError::PasswordRequired(password_token)) => {
                return password_loop(client, password_token).await;
            }
            Err(SignInError::InvalidCode) if attempt < MAX_ATTEMPTS => {
                eprintln!("invalid code ({attempt}/{MAX_ATTEMPTS}); try again");
            }
            Err(SignInError::InvalidCode) => return TooManyAttemptsSnafu.fail(),
            Err(err) => return Err(err).context(SignInSnafu),
        }
    }
    unreachable!("loop returns on success or final failure")
}

async fn password_loop(client: &Client, token: PasswordToken) -> Result<User, Error> {
    let hint = token.hint().unwrap_or("none").to_string();
    let mut token = token;

    for attempt in 1..=MAX_ATTEMPTS {
        // without_confirmation: this is a login, not a password change.
        let password = Password::new(&format!("2FA password (hint: {hint}):"))
            .with_display_mode(PasswordDisplayMode::Masked)
            .without_confirmation()
            .prompt()
            .context(PromptSnafu)?;

        match client.check_password(token, password.trim()).await {
            Ok(user) => return Ok(user),
            Err(SignInError::InvalidPassword(returned)) if attempt < MAX_ATTEMPTS => {
                eprintln!("invalid password ({attempt}/{MAX_ATTEMPTS}); try again");
                token = returned;
            }
            Err(SignInError::InvalidPassword(_)) => return TooManyAttemptsSnafu.fail(),
            Err(err) => return Err(err).context(SignInSnafu),
        }
    }
    unreachable!("loop returns on success or final failure")
}

fn describe(user: &User) -> String {
    match user.username() {
        Some(username) => format!("{} (@{username})", user.full_name()),
        None => user.full_name(),
    }
}

fn non_empty(input: &str, message: &str) -> Result<Validation, inquire::error::CustomUserError> {
    if input.trim().is_empty() {
        Ok(Validation::Invalid(message.into()))
    } else {
        Ok(Validation::Valid)
    }
}
