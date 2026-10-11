use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("config error: {source}"))]
    Config {
        source: config::ConfigError,
        #[snafu(implicit)]
        location: Location,
    },
}
