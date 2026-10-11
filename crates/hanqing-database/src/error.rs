use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("database error: {source}"))]
    Database {
        source: sea_orm::DbErr,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("settings document (de)serialization error: {source}"))]
    Json {
        source: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "session account {session} does not match archived account {archived}; \
         refusing to mix archives (reset with `just reset-session` to start over)"
    ))]
    AccountMismatch {
        session: i64,
        archived: i64,
        #[snafu(implicit)]
        location: Location,
    },
}
