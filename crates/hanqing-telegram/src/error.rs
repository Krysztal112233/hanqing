use grammers_client::sender::InvocationError;
use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("session storage error: {source}"))]
    SessionStorage {
        source: PgSessionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("telegram rpc error: {source}"))]
    Invoke {
        source: InvocationError,
        #[snafu(implicit)]
        location: Location,
    },
}

/// Error type of [`crate::session::PgSession`], as required by
/// `grammers_session::Session::Error`.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum PgSessionError {
    #[snafu(display("session database error: {source}"))]
    Database {
        source: hanqing_database::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("session value (de)serialization error: {source}"))]
    Serde {
        source: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("session cache lock is poisoned"))]
    Poisoned {
        #[snafu(implicit)]
        location: Location,
    },
}
