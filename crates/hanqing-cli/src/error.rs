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

    #[snafu(display("embedding service error: {source}"))]
    Embed {
        source: hanqing_embedding::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("migration error: {source}"))]
    Migrate {
        source: sea_orm_migration::DbErr,
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

    #[snafu(display("no authorized session; run `hanqing-cli login` first"))]
    NoSession {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("unknown setting `{key}`; valid keys: {valid}"))]
    UnknownSetting {
        key: String,
        valid: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("invalid value `{value}` for `{key}`: expected {expected}"))]
    InvalidValue {
        key: String,
        value: String,
        expected: &'static str,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("chat {chat_id} is not registered; the daemon registers dialogs on each run"))]
    UnknownChat {
        chat_id: i64,
        #[snafu(implicit)]
        location: Location,
    },
}
