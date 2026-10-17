use std::num::NonZero;

use crate::error::{ConfigSnafu, Error};
use config::Config;
use serde::{Deserialize, Deserializer, Serialize};
use snafu::ResultExt;

/// Dimension of the `message_embeddings` vector column. The storage schema is
/// fixed; a model with a different dimension needs a migration, not just a
/// config change.
pub const EMBEDDING_DIMENSIONS: u32 = 1024;

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
    /// Embedding service for semantic search (OpenAI-compatible endpoint:
    /// TEI, llama.cpp, Bailian, ...); absent until configured.
    #[serde(default)]
    pub embedding: Option<EmbeddingConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingConfig {
    /// Base URL of an OpenAI-compatible embeddings API. The client appends
    /// `/embeddings`, so include the API version segment (for example
    /// `http://embed:8080/v1`). Parsed at config load.
    pub base_url: url::Url,
    /// Bearer token; local servers usually need none.
    pub api_key: Option<String>,
    #[serde(deserialize_with = "non_empty_string")]
    pub model: String,
    /// Vector dimensions; the storage schema only supports
    /// [`EMBEDDING_DIMENSIONS`], so other values are rejected at config load.
    #[serde(
        default = "default_dimensions",
        deserialize_with = "deserialize_dimensions"
    )]
    pub dimensions: u32,
}

fn default_dimensions() -> u32 {
    EMBEDDING_DIMENSIONS
}

fn deserialize_dimensions<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
    let dimensions = u32::deserialize(deserializer)?;
    if dimensions != EMBEDDING_DIMENSIONS {
        return Err(serde::de::Error::custom(format!(
            "unsupported embedding dimensions {dimensions}; the schema stores \
             {EMBEDDING_DIMENSIONS}-dim vectors (migrate the column first)"
        )));
    }
    Ok(dimensions)
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
    pub embedding: EmbeddingSettings,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EmbeddingSettings {
    /// Embed message text for semantic search; requires `[embedding]` in
    /// the file config for the daemon to actually run the embedder worker.
    pub enabled: bool,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn embedding(source: &str) -> Result<EmbeddingConfig, config::ConfigError> {
        Config::builder()
            .add_source(config::File::from_str(source, config::FileFormat::Toml))
            .build()?
            .try_deserialize()
    }

    #[test]
    fn embedding_defaults_to_schema_dimensions() {
        let config = embedding(
            r#"
base_url = "http://host:8080/v1"
model = "m"
"#,
        )
        .unwrap();
        assert_eq!(config.dimensions, EMBEDDING_DIMENSIONS);
    }

    #[test]
    fn embedding_rejects_other_dimensions() {
        let result = embedding(
            r#"
base_url = "http://host:8080/v1"
model = "m"
dimensions = 768
"#,
        );
        assert!(result.is_err());
    }
}
