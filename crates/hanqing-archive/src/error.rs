use grammers_client::sender::InvocationError;
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

    #[snafu(display("database error: {source}"))]
    Db {
        source: sea_orm::DbErr,
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

    #[snafu(display("peer {peer_id} missing from session cache"))]
    PeerNotCached {
        peer_id: i64,
        #[snafu(implicit)]
        location: Location,
    },
}
