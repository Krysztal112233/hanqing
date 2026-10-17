//! `hanqing-cli settings` — view and adjust per-account behavior settings.

use hanqing_config::AccountSettings;
use hanqing_database::helper::prelude::*;
use hanqing_database::model::prelude::AccountSettings as AccountSettingsEntity;
use sea_orm::DatabaseConnection;
use snafu::{OptionExt as _, ResultExt};

use crate::error::{
    DatabaseSnafu, Error, InvalidValueSnafu, NoSessionSnafu, TelegramSnafu, UnknownSettingSnafu,
};

/// Keys accepted by `set`/`reset`.
const VALID_KEYS: &[&str] = &[
    "media.download_enabled",
    "media.max_size_bytes",
    "embedding.enabled",
];

fn valid_keys() -> String {
    VALID_KEYS.join(", ")
}

/// The account the stored session belongs to (settings are account-bound).
async fn session_account(
    db: &DatabaseConnection,
    config: &hanqing_config::TelegramConfig,
) -> Result<i64, Error> {
    let connection = hanqing_telegram::connect(config, db.clone())
        .await
        .context(TelegramSnafu)?;
    let me = hanqing_telegram::authorized_user(&connection.client)
        .await
        .context(TelegramSnafu)?
        .context(NoSessionSnafu)?;
    Ok(me.id().bot_api_dialog_id_unchecked())
}

pub async fn show(
    db: &DatabaseConnection,
    config: &hanqing_config::TelegramConfig,
) -> Result<(), Error> {
    let account_id = session_account(db, config).await?;
    let settings = AccountSettingsEntity::load_or_init(db, account_id)
        .await
        .context(DatabaseSnafu)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&settings).expect("settings serialize")
    );
    Ok(())
}

pub async fn set(
    db: &DatabaseConnection,
    config: &hanqing_config::TelegramConfig,
    key: &str,
    value: &str,
) -> Result<(), Error> {
    let account_id = session_account(db, config).await?;
    let mut settings = AccountSettingsEntity::load_or_init(db, account_id)
        .await
        .context(DatabaseSnafu)?;

    assign(&mut settings, key, value)?;
    AccountSettingsEntity::save(db, account_id, &settings)
        .await
        .context(DatabaseSnafu)?;

    println!("{key} = {}", current(&settings, key)?);
    Ok(())
}

pub async fn reset(
    db: &DatabaseConnection,
    config: &hanqing_config::TelegramConfig,
    key: Option<&str>,
) -> Result<(), Error> {
    let account_id = session_account(db, config).await?;
    let defaults = AccountSettings::default();
    let mut settings = AccountSettingsEntity::load_or_init(db, account_id)
        .await
        .context(DatabaseSnafu)?;

    match key {
        None => settings = defaults,
        Some(key) => {
            // Reuse `assign` by rendering the default value.
            let default_value = current(&defaults, key)?.to_string();
            assign(&mut settings, key, &default_value)?;
        }
    }

    AccountSettingsEntity::save(db, account_id, &settings)
        .await
        .context(DatabaseSnafu)?;
    println!("reset {}", key.unwrap_or("all settings"));
    Ok(())
}

fn assign(settings: &mut AccountSettings, key: &str, value: &str) -> Result<(), Error> {
    match key {
        "media.download_enabled" => {
            settings.media.download_enabled = value.parse().map_err(|_| {
                InvalidValueSnafu {
                    key,
                    value,
                    expected: "true or false",
                }
                .build()
            })?;
        }
        "media.max_size_bytes" => {
            settings.media.max_size_bytes = value.parse().map_err(|_| {
                InvalidValueSnafu {
                    key,
                    value,
                    expected: "non-negative integer (bytes)",
                }
                .build()
            })?;
        }
        "embedding.enabled" => {
            settings.embedding.enabled = value.parse().map_err(|_| {
                InvalidValueSnafu {
                    key,
                    value,
                    expected: "true or false",
                }
                .build()
            })?;
        }
        _ => {
            return UnknownSettingSnafu {
                key,
                valid: valid_keys(),
            }
            .fail();
        }
    }
    Ok(())
}

/// Current rendered value of a key (also used to source defaults on reset).
fn current(settings: &AccountSettings, key: &str) -> Result<impl std::fmt::Display, Error> {
    match key {
        "media.download_enabled" => Ok(settings.media.download_enabled.to_string()),
        "media.max_size_bytes" => Ok(settings.media.max_size_bytes.to_string()),
        "embedding.enabled" => Ok(settings.embedding.enabled.to_string()),
        _ => UnknownSettingSnafu {
            key,
            valid: valid_keys(),
        }
        .fail(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedding_enabled_is_settable_and_readable() {
        let mut settings = AccountSettings::default();
        assign(&mut settings, "embedding.enabled", "true").unwrap();
        assert!(settings.embedding.enabled);
        assert_eq!(
            current(&settings, "embedding.enabled").unwrap().to_string(),
            "true"
        );
        assert!(VALID_KEYS.contains(&"embedding.enabled"));
    }
}
