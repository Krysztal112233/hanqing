use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("database error: {source}"))]
    Database {
        source: hanqing_database::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("telegram rpc error: {source}"))]
    Invoke {
        source: grammers_client::sender::InvocationError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("session error: {source}"))]
    Session {
        source: Box<hanqing_telegram::error::PgSessionError>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("s3 credentials error: {source}"))]
    Credentials {
        source: s3::creds::error::CredentialsError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("s3 error: {source}"))]
    S3 {
        source: s3::error::S3Error,
        #[snafu(implicit)]
        location: Location,
    },
}
