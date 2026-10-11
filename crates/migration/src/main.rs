use sea_orm_migration::prelude::*;

fn ensure_database_url() {
    if std::env::var("DATABASE_URL").is_ok() {
        return;
    }

    let url = config::Config::builder()
        .add_source(config::File::with_name("hanqing.toml"))
        .add_source(config::Environment::with_prefix("HANQING").separator("__"))
        .build()
        .ok()
        .and_then(|c| c.get::<String>("database.url").ok());

    if let Some(url) = url {
        unsafe { std::env::set_var("DATABASE_URL", &url) };
    }
}

#[tokio::main]
async fn main() {
    ensure_database_url();
    cli::run_cli(migration::Migrator).await;
}
