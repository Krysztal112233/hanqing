use std::path::PathBuf;

use crate::error::{ConfigSnafu, Error};
use config::Config;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HanqingConfig {
    pub telegram: TelegramConfig,
    pub database: DatabaseConfig,

    #[serde(default)]
    pub archive: ArchiveConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelegramConfig {
    /// From https://my.telegram.org/apps
    pub api_id: i32,
    /// Never commit a real value; override via `HANQING__TELEGRAM__API_HASH`.
    pub api_hash: String,
    #[serde(default = "default_session_file")]
    pub session_file: PathBuf,
}

fn default_session_file() -> PathBuf {
    PathBuf::from("hanqing.session")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    /// Optional in TOML; usually supplied via `DATABASE_URL` or
    /// `HANQING__DATABASE__URL`.
    pub url: Option<String>,
    pub max_connections: Option<u32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveConfig {
    /// Restrict archiving to these chat IDs; empty = all dialogs.
    #[serde(default)]
    pub chat_ids: Vec<i64>,
}

impl HanqingConfig {
    pub fn new() -> Result<Self, Error> {
        Self::new_at("hanqing.toml")
    }

    pub fn new_at(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        let config: Self = Config::builder()
            .add_source(config::File::from(path.as_ref()))
            .add_source(config::Environment::with_prefix("HANQING").separator("__"))
            .build()
            .context(ConfigSnafu)?
            .try_deserialize()
            .context(ConfigSnafu)?;

        Ok(config)
    }
}
