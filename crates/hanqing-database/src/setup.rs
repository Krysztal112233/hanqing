use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use snafu::ResultExt;
use tracing::log::LevelFilter;

use crate::error::{DatabaseSnafu, Error};

pub async fn connect(url: &str, max_connections: Option<u32>) -> Result<DatabaseConnection, Error> {
    let mut opt = ConnectOptions::new(url);
    opt.sqlx_logging(true).sqlx_logging_level(LevelFilter::Info);
    if let Some(n) = max_connections {
        opt.max_connections(n);
    }

    Database::connect(opt).await.context(DatabaseSnafu)
}
