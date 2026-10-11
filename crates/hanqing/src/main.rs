use chrono::DateTime;
use dotenvy::dotenv;
use mimalloc::MiMalloc;
use snafu::{ResultExt, ensure};
use tracing::{info, warn};

use hanqing::error::{
    ConfigSnafu, DatabaseSnafu, Error, InvokeSnafu, MissingCredentialsSnafu, PeerResolveSnafu,
    SerializeSnafu, TelegramSnafu,
};
use hanqing_config::HanqingConfig;
use hanqing_database::helper::message::{NewMessage, save_message};

/// Spike scope: archive the newest messages of a single dialog to prove the
/// Telegram → PostgreSQL loop end to end.
const SPIKE_MESSAGE_LIMIT: u32 = 100;

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
    hanqing_telegram::ensure_authorized(&client, &config.telegram)
        .await
        .context(TelegramSnafu)?;
    info!("telegram session authorized");

    // Spike: archive the newest messages of the first dialog.
    let mut dialogs = client.iter_dialogs();
    let dialog = dialogs
        .next()
        .await
        .context(InvokeSnafu)?
        .expect("account has no dialogs to archive");
    let peer = dialog.peer();
    let chat_id = peer.id().bot_api_dialog_id_unchecked();
    let peer_ref = peer
        .to_ref()
        .await
        .context(PeerResolveSnafu)?
        .expect("dialog peer missing from session cache");
    info!(
        chat_id,
        name = peer.name().unwrap_or_default(),
        "archiving dialog"
    );

    let mut messages = client.iter_messages(peer_ref);
    let mut count = 0u32;
    while let Some(message) = messages.next().await.context(InvokeSnafu)? {
        let raw = serde_json::to_value(&message.raw).context(SerializeSnafu)?;
        let date = DateTime::from_timestamp(
            message.date().as_second(),
            message.date().subsec_nanosecond() as u32,
        )
        .expect("message timestamp out of range")
        .fixed_offset();

        save_message(
            &db,
            NewMessage {
                chat_id,
                msg_id: message.id() as i64,
                date,
                sender_id: message.sender_id().map(|p| p.bot_api_dialog_id_unchecked()),
                text: message.text().to_string(),
                raw,
            },
        )
        .await
        .context(DatabaseSnafu)?;

        count += 1;
        if count >= SPIKE_MESSAGE_LIMIT {
            break;
        }
    }
    info!(count, "spike archive complete");

    Ok(())
}
