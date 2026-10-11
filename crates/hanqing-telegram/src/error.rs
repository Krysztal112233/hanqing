use grammers_session::storages::SqliteSessionError;
use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("session error: {source}"))]
    Session {
        source: SqliteSessionError,
        #[snafu(implicit)]
        location: Location,
    },
}
