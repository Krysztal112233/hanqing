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

    #[snafu(display("telegram rpc error: {source}"))]
    Invoke {
        source: grammers_client::InvocationError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("sign-in error: {source}"))]
    SignIn {
        source: grammers_client::SignInError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("interactive prompt error: {source}"))]
    Prompt {
        source: inquire::error::InquireError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("too many invalid attempts; giving up"))]
    TooManyAttempts {
        #[snafu(implicit)]
        location: Location,
    },
}
