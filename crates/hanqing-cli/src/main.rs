use clap::{Parser, Subcommand};
use dotenvy::dotenv;
use snafu::ResultExt;
use tracing::{info, warn};

use hanqing_cli::error::{ArchiveSnafu, ConfigSnafu, DatabaseSnafu, Error, TelegramSnafu};
use hanqing_config::HanqingConfig;

#[derive(Parser)]
#[command(
    name = "hanqing-cli",
    version,
    about = "Management CLI for hanqing, the Telegram -> PostgreSQL archiver"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Sign in interactively; the session persists in PostgreSQL and the
    /// `hanqing` daemon reuses it from there.
    Login {
        /// Phone number in international format; prompted when omitted.
        #[arg(long)]
        phone: Option<String>,
    },

    /// Extract media metadata from already-archived messages (idempotent,
    /// no Telegram connection).
    ScanMedia,
}

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
    let db = hanqing_database::setup::connect(
        &config
            .database
            .url
            .clone()
            .or_else(|| std::env::var("DATABASE_URL").ok())
            .expect("database url missing: set `database.url` in hanqing.toml, `DATABASE_URL`, or `HANQING__DATABASE__URL`"),
        config.database.max_connections,
    )
    .await
    .context(DatabaseSnafu)?;

    match Cli::parse().command {
        Command::Login { phone } => {
            let client = hanqing_telegram::connect(&config.telegram, db)
                .await
                .context(TelegramSnafu)?;
            hanqing_cli::login::login(&client, &config.telegram.api_hash, phone).await?;
        }
        Command::ScanMedia => {
            let count = hanqing_archive::scan_existing_media(&db)
                .await
                .context(ArchiveSnafu)?;
            info!(count, "media scan done");
        }
    }

    Ok(())
}
