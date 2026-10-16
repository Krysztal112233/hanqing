use dotenvy::dotenv;
use mimalloc::MiMalloc;
use snafu::ResultExt;
use tracing::{error, info, warn};

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

    let db =
        hanqing_database::setup::connect(&config.database_url(), config.database.max_connections)
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
    let self_id = me
        .id()
        .bot_api_dialog_id()
        .expect("self user has a real id");
    info!(id = self_id, name = me.full_name(), "running as");

    // Behavior settings live in the database (per account); first run
    // self-initializes defaults. Also the single-account guard: a session
    // belonging to a different account than the archive aborts here.
    let settings = AccountSettings::load_or_init(&db, self_id)
        .await
        .context(DatabaseSnafu)?;
    info!(?settings, "account settings loaded");

    // Live updates start before backfill so nothing arriving mid-run is
    // missed; both paths share the same idempotent upserts. Media download
    // tasks are enqueued at archive time when S3 storage is configured.
    let enqueue_media = config.s3.is_some();
    let peers = std::sync::Arc::new(hanqing_archive::PeerRecorder::new(self_id));
    let stream_db = db.clone();
    let stream_client = client.clone();
    let stream_peers = peers.clone();
    let mut stream_task = tokio::spawn(async move {
        hanqing_archive::run_update_stream(
            &stream_client,
            &stream_db,
            connection.updates,
            self_id,
            enqueue_media,
            &stream_peers,
        )
        .await
    });

    // Workers (media downloader, backfill, ...) self-register via linkme and
    // decide themselves whether config enables them; they run as detached
    // tasks — a worker failure must not take down archiving.
    let workers = hanqing_worker::spawn_all(&hanqing_worker::WorkerContext {
        client: client.clone(),
        session: connection.session.clone(),
        db: db.clone(),
        config: config.clone(),
        self_id,
    });
    info!(?workers, "workers spawned");

    // Backfill runs on demand: one trigger message per startup, consumed by
    // the backfill worker. `hanqing-cli backfill` enqueues the same trigger.
    if std::env::var_os("HANQING_SKIP_BACKFILL").is_none() {
        hanqing_database::queue::send_backfill_trigger(&db, "startup")
            .await
            .context(DatabaseSnafu)?;
        info!("startup backfill trigger enqueued");
    } else {
        info!("HANQING_SKIP_BACKFILL set; no startup backfill trigger");
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
