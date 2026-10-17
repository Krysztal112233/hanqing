use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("embedding request failed: {source}"))]
    Request {
        source: reqwest::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("embedding service returned {status}: {body}"))]
    Http {
        status: reqwest::StatusCode,
        body: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("invalid embedding api_key header: {source}"))]
    InvalidHeader {
        source: reqwest::header::InvalidHeaderValue,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("failed to build embedding http client: {source}"))]
    ClientBuild {
        source: reqwest::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("embedding service returned {got} vectors for {expected} inputs"))]
    LengthMismatch {
        expected: usize,
        got: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("embedding data has index {got}, expected {expected}"))]
    InvalidIndex {
        expected: usize,
        got: u32,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("embedding service returned {got}-dim vectors, expected {expected}"))]
    Dimensions {
        expected: usize,
        got: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("embedding service returned no vectors"))]
    EmptyResponse {
        #[snafu(implicit)]
        location: Location,
    },
}
