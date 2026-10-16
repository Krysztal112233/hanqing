use std::num::NonZero;

use crate::error::{ConfigSnafu, Error};
use config::Config;
use serde::{Deserialize, Deserializer, Serialize};
use snafu::ResultExt;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HanqingConfig {
    pub telegram: TelegramConfig,
    pub database: DatabaseConfig,
    /// Media object storage; absent until the downloader is configured.
    #[serde(default)]
    pub s3: Option<S3Config>,
    /// Process-level worker knobs, one sub-table per worker
    /// (`[worker.media]`, ...). Note the split from `AccountSettings` (the
    /// per-account DB document): concurrency and other spawn-time
    /// parameters live here; behavior toggles live in the DB so they apply
    /// without a restart.
    #[serde(default)]
    pub worker: WorkerConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct WorkerConfig {
    pub media: MediaWorkerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaWorkerConfig {
    /// Concurrent media downloader replicas (`pgmq.read` uses FOR UPDATE
    /// SKIP LOCKED, so replicas never see each other's tasks). `0` disables
    /// the downloader entirely.
    #[serde(default = "default_download_workers")]
    pub download_workers: usize,
}

impl Default for MediaWorkerConfig {
    fn default() -> Self {
        Self {
            download_workers: default_download_workers(),
        }
    }
}

fn default_download_workers() -> usize {
    3
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelegramConfig {
    /// From https://my.telegram.org/apps. Nonzero is enforced at the type
    /// level: `0` fails to parse, no runtime check needed.
    pub api_id: NonZero<i32>,
    /// Never commit a real value; override via `HANQING__TELEGRAM__API_HASH`.
    #[serde(deserialize_with = "non_empty_string")]
    pub api_hash: String,
}

fn non_empty_string<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let s = String::deserialize(deserializer)?;
    if s.is_empty() {
        return Err(serde::de::Error::custom("must not be empty"));
    }
    Ok(s)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    #[serde(deserialize_with = "non_empty_string")]
    pub endpoint: String,
    #[serde(deserialize_with = "non_empty_string")]
    pub bucket: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(deserialize_with = "non_empty_string")]
    pub access_key_id: String,
    #[serde(deserialize_with = "non_empty_string")]
    pub secret_access_key: String,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    /// Optional in TOML; usually supplied via `DATABASE_URL` or
    /// `HANQING__DATABASE__URL`.
    pub url: Option<String>,
    pub max_connections: Option<u32>,
}

/// Per-account behavior settings, stored as one JSONB document in
/// `account_settings` (this struct is the schema; `deny_unknown_fields`
/// keeps it strict like file config). Connectivity config stays in
/// `hanqing.toml`; everything about *what/how* to archive lives here.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AccountSettings {
    pub media: MediaSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MediaSettings {
    /// Download media files; metadata is always archived regardless.
    pub download_enabled: bool,
    /// Skip downloads larger than this many bytes.
    pub max_size_bytes: u64,
}

impl Default for MediaSettings {
    fn default() -> Self {
        Self {
            download_enabled: false,
            max_size_bytes: 50 * 1024 * 1024,
        }
    }
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

    /// Resolve the database URL: `database.url` from config, falling back to
    /// the `DATABASE_URL` environment variable.
    pub fn database_url(&self) -> String {
        self.database
            .url
            .clone()
            .or_else(|| std::env::var("DATABASE_URL").ok())
            .expect("database url missing: set `database.url` in hanqing.toml, `DATABASE_URL`, or `HANQING__DATABASE__URL`")
    }
}
