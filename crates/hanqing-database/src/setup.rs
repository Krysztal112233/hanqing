use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use snafu::ResultExt;
use tracing::log::LevelFilter;

use crate::error::{DatabaseSnafu, Error};

/// Queries slower than this are logged at WARN.
const SLOW_QUERY_THRESHOLD: std::time::Duration = std::time::Duration::from_millis(100);

pub async fn connect(url: &str, max_connections: Option<u32>) -> Result<DatabaseConnection, Error> {
    let mut opt = ConnectOptions::new(url);
    opt.sqlx_logging(false)
        .sqlx_slow_statements_logging_settings(LevelFilter::Warn, SLOW_QUERY_THRESHOLD);
    if let Some(n) = max_connections {
        opt.max_connections(n);
    }

    Database::connect(opt).await.context(DatabaseSnafu)
}
