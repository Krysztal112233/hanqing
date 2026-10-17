//! Thin client for OpenAI-compatible embedding services (llama.cpp,
//! text-embeddings-inference, Bailian, ...). `base_url` points at the API
//! base including its version segment (for example `http://host:8080/v1`);
//! requests go to `{base_url}/embeddings`.

mod error;

pub use error::Error;

use std::time::Duration;

use hanqing_config::EmbeddingConfig;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use error::{
    ClientBuildSnafu, DimensionsSnafu, EmptyResponseSnafu, HttpSnafu, InvalidHeaderSnafu,
    InvalidIndexSnafu, LengthMismatchSnafu, RequestSnafu,
};

/// Per-request ceiling. The worker visibility timeout is 300 s, so a stuck
/// request must fail before its tasks become visible again.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(240);

/// Keep a broken endpoint from stalling worker startup for the OS timeout.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Error bodies are server-controlled; keep them out of huge log lines.
const MAX_ERROR_BODY_CHARS: usize = 200;

#[derive(Debug, Clone)]
pub struct EmbedClient {
    http: reqwest::Client,
    url: String,
    pub model: String,
    pub dimensions: u32,
}

#[derive(Serialize)]
struct EmbedRequest<'a> {
    model: &'a str,
    input: &'a [String],
}

/// Qwen3-Embedding's retrieval instruction, applied client-side (plain text
/// prefix) so any OpenAI-compatible endpoint works — not all servers
/// understand a `prompt_name` extension field.
const QUERY_INSTRUCTION: &str =
    "Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery: ";

#[derive(Deserialize)]
struct EmbedResponse {
    data: Vec<EmbedData>,
}

#[derive(Deserialize)]
struct EmbedData {
    index: u32,
    embedding: Vec<f32>,
}

impl EmbedClient {
    pub fn new(config: &EmbeddingConfig) -> Result<Self, Error> {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(key) = &config.api_key {
            let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {key}"))
                .context(InvalidHeaderSnafu)?;
            value.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .context(ClientBuildSnafu)?;
        Ok(Self {
            http,
            url: format!(
                "{}/embeddings",
                config.base_url.as_str().trim_end_matches('/')
            ),
            model: config.model.clone(),
            dimensions: config.dimensions,
        })
    }

    /// Embed documents (stored side). No instruction prompt.
    pub async fn embed_documents(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, Error> {
        self.embed(texts).await
    }

    /// Embed a search query with the model's retrieval instruction prompt.
    pub async fn embed_query(&self, text: &str) -> Result<Vec<f32>, Error> {
        let vectors = self.embed(&[format!("{QUERY_INSTRUCTION}{text}")]).await?;
        vectors
            .into_iter()
            .next()
            .ok_or_else(|| EmptyResponseSnafu.build())
    }

    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, Error> {
        let response = self
            .http
            .post(&self.url)
            .json(&EmbedRequest {
                model: &self.model,
                input: texts,
            })
            .send()
            .await
            .context(RequestSnafu)?;
        let status = response.status();
        if !status.is_success() {
            let body: String = response
                .text()
                .await
                .unwrap_or_default()
                .chars()
                .take(MAX_ERROR_BODY_CHARS)
                .collect();
            return HttpSnafu { status, body }.fail();
        }
        let parsed: EmbedResponse = response.json().await.context(RequestSnafu)?;
        ordered_vectors(parsed.data, texts.len(), self.dimensions)
    }
}

/// Put the service's `data` array back into input order and enforce the
/// response contract. OpenAI-compatible services are not required to return
/// entries in request order; `index` is the only reliable correlation.
fn ordered_vectors(
    mut data: Vec<EmbedData>,
    expected: usize,
    dimensions: u32,
) -> Result<Vec<Vec<f32>>, Error> {
    if data.len() != expected {
        return LengthMismatchSnafu {
            expected,
            got: data.len(),
        }
        .fail();
    }
    data.sort_by_key(|item| item.index);
    for (expected, item) in data.iter().enumerate() {
        if item.index as usize != expected {
            return InvalidIndexSnafu {
                expected,
                got: item.index,
            }
            .fail();
        }
    }
    let vectors: Vec<Vec<f32>> = data.into_iter().map(|item| item.embedding).collect();
    for vector in &vectors {
        if vector.len() != dimensions as usize {
            return DimensionsSnafu {
                expected: dimensions as usize,
                got: vector.len(),
            }
            .fail();
        }
    }
    Ok(vectors)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(base: &str) -> hanqing_config::EmbeddingConfig {
        hanqing_config::EmbeddingConfig {
            base_url: url::Url::parse(base).unwrap(),
            api_key: None,
            model: "m".into(),
            dimensions: hanqing_config::EMBEDDING_DIMENSIONS,
        }
    }

    #[test]
    fn endpoint_keeps_base_path() {
        for base in ["http://host:8123/v1", "http://host:8123/v1/"] {
            let client = EmbedClient::new(&config(base)).unwrap();
            assert_eq!(client.url, "http://host:8123/v1/embeddings");
        }
    }

    /// Why the endpoint is string-concatenated instead of `Url::join`:
    /// RFC 3986 relative resolution replaces the base's last path segment.
    #[test]
    fn url_join_drops_last_segment() {
        let base = url::Url::parse("http://host:8123/v1").unwrap();
        assert_eq!(
            base.join("embeddings").unwrap().as_str(),
            "http://host:8123/embeddings"
        );
    }

    #[test]
    fn ordered_vectors_sort_by_index() {
        let data = vec![
            EmbedData {
                index: 1,
                embedding: vec![2.0],
            },
            EmbedData {
                index: 0,
                embedding: vec![1.0],
            },
        ];
        let vectors = ordered_vectors(data, 2, 1).unwrap();
        assert_eq!(vectors, vec![vec![1.0], vec![2.0]]);
    }

    #[test]
    fn ordered_vectors_reject_short_response() {
        let data = vec![EmbedData {
            index: 0,
            embedding: vec![1.0],
        }];
        assert!(matches!(
            ordered_vectors(data, 2, 1),
            Err(Error::LengthMismatch {
                expected: 2,
                got: 1,
                ..
            })
        ));
    }

    #[test]
    fn ordered_vectors_reject_duplicate_index() {
        let data = vec![
            EmbedData {
                index: 0,
                embedding: vec![1.0],
            },
            EmbedData {
                index: 0,
                embedding: vec![2.0],
            },
        ];
        assert!(matches!(
            ordered_vectors(data, 2, 1),
            Err(Error::InvalidIndex {
                expected: 1,
                got: 0,
                ..
            })
        ));
    }

    #[test]
    fn ordered_vectors_reject_wrong_dimensions() {
        let data = vec![EmbedData {
            index: 0,
            embedding: vec![1.0, 2.0],
        }];
        assert!(matches!(
            ordered_vectors(data, 1, 1),
            Err(Error::Dimensions {
                expected: 1,
                got: 2,
                ..
            })
        ));
    }
}
