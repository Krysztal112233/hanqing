use dotenvy::dotenv;
use mimalloc::MiMalloc;
use snafu::ResultExt;
use tracing::{info, warn};

use hanqing::error::{ConfigSnafu, DatabaseSnafu, Error, TelegramSnafu};
use hanqing_config::HanqingConfig;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let missing_dotenv = dotenv().is_err();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,hanqing=debug".into()),
        )
        .init();

    if missing_dotenv {
        warn!("no `.env` file found; relying on environment and hanqing.toml");
    }

    let config = HanqingConfig::new().context(ConfigSnafu)?;

    let database_url = config
        .database
        .url
        .clone()
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .expect("database url missing: set `database.url` in hanqing.toml, `DATABASE_URL`, or `HANQING__DATABASE__URL`");

    let db = hanqing_database::setup::connect(&database_url, config.database.max_connections)
        .await
        .context(DatabaseSnafu)?;
    info!("connected to database");

    let client = hanqing_telegram::connect(&config.telegram)
        .await
        .context(TelegramSnafu)?;
    info!("connected to telegram");

    // TODO(spike): sign-in flow when unauthorized, then iter_dialogs /
    // iter_messages and persist raw payloads into a JSONB column.
    let _ = db;
    let _ = client;

    Ok(())
}
