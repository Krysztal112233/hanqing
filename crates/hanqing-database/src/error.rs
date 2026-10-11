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
}
