use dotenvy::dotenv;
use mimalloc::MiMalloc;
use snafu::{ResultExt, ensure};
use tracing::{error, info, warn};

use hanqing::error::MissingCredentialsSnafu;
use hanqing::error::{ArchiveSnafu, ConfigSnafu, DatabaseSnafu, Error, TelegramSnafu};
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

    ensure!(
        config.telegram.api_id != 0 && !config.telegram.api_hash.is_empty(),
        MissingCredentialsSnafu
    );

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

    let client = hanqing_telegram::connect(&config.telegram, db.clone())
        .await
        .context(TelegramSnafu)?;

    // Account-agnostic gate: the identity is whatever the stored session
    // carries (missing and revoked sessions both surface as None).
    let Some(me) = hanqing_telegram::authorized_user(&client)
        .await
        .context(TelegramSnafu)?
    else {
        error!("no authorized session; run `hanqing-cli login` first");
        std::process::exit(2);
    };
    info!(
        id = me.id().bot_api_dialog_id_unchecked(),
        name = me.full_name(),
        "running as"
    );

    let stats = hanqing_archive::backfill_all(&client, &db, &config.archive)
        .await
        .context(ArchiveSnafu)?;
    info!(
        chats = stats.registered_chats,
        completed = stats.completed_chats,
        messages = stats.messages,
        "backfill run complete"
    );

    Ok(())
}
