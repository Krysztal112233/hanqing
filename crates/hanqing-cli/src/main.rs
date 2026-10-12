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

    /// View and adjust per-account behavior settings (stored in PostgreSQL).
    Settings {
        #[command(subcommand)]
        command: SettingsCommand,
    },

    /// Per-chat archive switches.
    Chats {
        #[command(subcommand)]
        command: ChatsCommand,
    },
}

#[derive(Subcommand)]
enum SettingsCommand {
    /// Show the effective settings.
    Show,
    /// Set a key (e.g. `settings set media.max_size_bytes 10485760`).
    Set { key: String, value: String },
    /// Reset one key (or all) to defaults.
    Reset { key: Option<String> },
}

#[derive(Subcommand)]
enum ChatsCommand {
    /// List registered chats with enabled/done state and watermark.
    Ls,
    /// Include a chat in archiving.
    Enable { chat_id: i64 },
    /// Exclude a chat from archiving (progress is kept).
    Disable { chat_id: i64 },
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
    let db =
        hanqing_database::setup::connect(&config.database_url(), config.database.max_connections)
            .await
            .context(DatabaseSnafu)?;

    match Cli::parse().command {
        Command::Login { phone } => {
            // The updates receiver is dropped with the connection: login is
            // short-lived and follows no stream.
            let connection = hanqing_telegram::connect(&config.telegram, db)
                .await
                .context(TelegramSnafu)?;
            hanqing_cli::login::login(&connection.client, &config.telegram.api_hash, phone).await?;
        }
        Command::ScanMedia => {
            let count = hanqing_archive::scan_existing_media(&db)
                .await
                .context(ArchiveSnafu)?;
            info!(count, "media scan done");
        }
        Command::Settings { command } => match command {
            SettingsCommand::Show => hanqing_cli::settings::show(&db, &config.telegram).await?,
            SettingsCommand::Set { key, value } => {
                hanqing_cli::settings::set(&db, &config.telegram, &key, &value).await?;
            }
            SettingsCommand::Reset { key } => {
                hanqing_cli::settings::reset(&db, &config.telegram, key.as_deref()).await?;
            }
        },
        Command::Chats { command } => match command {
            ChatsCommand::Ls => hanqing_cli::chats::ls(&db).await?,
            ChatsCommand::Enable { chat_id } => {
                hanqing_cli::chats::set_enabled(&db, chat_id, true).await?;
            }
            ChatsCommand::Disable { chat_id } => {
                hanqing_cli::chats::set_enabled(&db, chat_id, false).await?;
            }
        },
    }

    Ok(())
}
