use grammers_client::sender::InvocationError;
use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("config error: {source}"))]
    Config {
        source: hanqing_config::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "telegram credentials missing: set `HANQING__TELEGRAM__API_ID` and `HANQING__TELEGRAM__API_HASH`"
    ))]
    MissingCredentials {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("database error: {source}"))]
    Database {
        source: hanqing_database::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("telegram error: {source}"))]
    Telegram {
        source: hanqing_telegram::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("telegram rpc error: {source}"))]
    Invoke {
        source: InvocationError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("peer resolution error: {source}"))]
    PeerResolve {
        source: Box<dyn std::error::Error + Send + Sync>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("message serialization error: {source}"))]
    Serialize {
        source: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },
}
