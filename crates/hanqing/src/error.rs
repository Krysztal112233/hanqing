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

    #[snafu(display("archive error: {source}"))]
    Archive {
        source: hanqing_archive::error::Error,
        #[snafu(implicit)]
        location: Location,
    },
}
