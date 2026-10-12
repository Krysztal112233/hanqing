use dotenvy::dotenv;
use mimalloc::MiMalloc;
use snafu::{ResultExt, ensure};
use tracing::{error, info, warn};

use hanqing::error::MissingCredentialsSnafu;
use hanqing::error::{ArchiveSnafu, ConfigSnafu, DatabaseSnafu, Error, JoinSnafu, TelegramSnafu};
use hanqing_config::HanqingConfig;
use hanqing_database::helper::prelude::*;
use hanqing_database::model::prelude::AccountSettings;

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

    let connection = hanqing_telegram::connect(&config.telegram, db.clone())
        .await
        .context(TelegramSnafu)?;
    let client = connection.client;

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

    // Behavior settings live in the database (per account); first run
    // self-initializes defaults. Also the single-account guard: a session
    // belonging to a different account than the archive aborts here.
    let settings = AccountSettings::load_or_init(&db, me.id().bot_api_dialog_id_unchecked())
        .await
        .context(DatabaseSnafu)?;
    info!(?settings, "account settings loaded");

    let self_id = me
        .id()
        .bot_api_dialog_id()
        .expect("self user has a real id");

    // Live updates start before backfill so nothing arriving mid-run is
    // missed; both paths share the same idempotent upserts.
    let stream_db = db.clone();
    let stream_client = client.clone();
    let mut stream_task = tokio::spawn(async move {
        hanqing_archive::run_update_stream(&stream_client, &stream_db, connection.updates, self_id)
            .await
    });

    if std::env::var_os("HANQING_SKIP_BACKFILL").is_none() {
        let stats = hanqing_archive::backfill_all(&client, &db, self_id)
            .await
            .context(ArchiveSnafu)?;
        info!(
            chats = stats.registered_chats,
            completed = stats.completed_chats,
            messages = stats.messages,
            "backfill run complete"
        );
    } else {
        info!("HANQING_SKIP_BACKFILL set; skipping backfill");
    }

    info!("daemon resident; following live updates");
    tokio::select! {
        result = &mut stream_task => {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err).context(ArchiveSnafu),
                Err(err) => return Err(err).context(JoinSnafu),
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown requested; exiting");
        }
    }

    Ok(())
}
