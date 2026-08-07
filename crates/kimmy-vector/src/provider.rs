//! Embedding providers.
//!
//! One trait, several implementations. The provider is the only part of the
//! vector pipeline that reaches outside the process, so it is also the only
//! part that can be slow, fail intermittently, or cost money — which is why
//! embedding runs off the write path entirely.

use async_trait::async_trait;
use kimmy_core::ProviderConfig;

use crate::error::{Result, VectorError};

/// Turns text into vectors.
///
/// Batched rather than one-at-a-time: every remote provider charges a
/// round-trip per call, and a document usually produces several chunks.
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Embed a batch of texts, returning one vector per input **in order**.
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;

    /// The width this provider produces.
    fn dim(&self) -> usize;

    fn name(&self) -> &'static str;
}

/// Build a provider from its configuration.
///
/// The `Byo` provider has no implementation here on purpose: it means the
/// client supplies vectors and the server never embeds, so there is nothing to
/// call. Callers check [`ProviderConfig::embeds_server_side`] first.
pub fn build(config: &ProviderConfig, dim: usize) -> Result<Box<dyn EmbeddingProvider>> {
    match config {
        ProviderConfig::Byo => Err(VectorError::NoProvider),

        ProviderConfig::OpenAi { model, endpoint, api_key_env } => {
            let base = endpoint.clone().unwrap_or_else(|| "https://api.openai.com".into());
            Ok(Box::new(HttpProvider::openai(base, model.clone(), api_key_env.clone(), dim)?))
        }
        ProviderConfig::Ollama { model, endpoint } => {
            Ok(Box::new(HttpProvider::ollama(endpoint.clone(), model.clone(), dim)))
        }
        ProviderConfig::CustomHttp { endpoint, api_key_env } => {
            Ok(Box::new(HttpProvider::custom(endpoint.clone(), api_key_env.clone(), dim)?))
        }

        ProviderConfig::Local { model } => local_provider(model, dim),
    }
}

#[cfg(feature = "local-embeddings")]
fn local_provider(model: &str, dim: usize) -> Result<Box<dyn EmbeddingProvider>> {
    Ok(Box::new(crate::local::LocalProvider::new(model, dim)?))
}

#[cfg(not(feature = "local-embeddings"))]
fn local_provider(_model: &str, _dim: usize) -> Result<Box<dyn EmbeddingProvider>> {
    // Configuration validation rejects this earlier; reaching here means a
    // config was written by a build that had the feature and is now being read
    // by one that does not.
    Err(VectorError::LocalUnavailable)
}

/// Which request and response shape a remote endpoint speaks.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Dialect {
    /// `{"input": [...], "model": "..."}` → `{"data": [{"embedding": [...]}]}`
    OpenAi,
    /// One text per request: `{"prompt": "...", "model": "..."}` →
    /// `{"embedding": [...]}`
    Ollama,
    /// `{"input": [...]}` → `{"embeddings": [[...]]}`
    Custom,
}

/// A provider that calls an HTTP endpoint.
pub struct HttpProvider {
    endpoint: String,
    model: String,
    dialect: Dialect,
    api_key: Option<String>,
    dim: usize,
}

impl HttpProvider {
    fn openai(base: String, model: String, key_env: String, dim: usize) -> Result<Self> {
        Ok(Self {
            endpoint: format!("{}/v1/embeddings", base.trim_end_matches('/')),
            model,
            dialect: Dialect::OpenAi,
            api_key: Some(read_key(&key_env)?),
            dim,
        })
    }

    fn ollama(endpoint: String, model: String, dim: usize) -> Self {
        Self {
            endpoint: format!("{}/api/embeddings", endpoint.trim_end_matches('/')),
            model,
            dialect: Dialect::Ollama,
            api_key: None,
            dim,
        }
    }

    fn custom(endpoint: String, key_env: Option<String>, dim: usize) -> Result<Self> {
        let api_key = match key_env {
            Some(var) => Some(read_key(&var)?),
            None => None,
        };
        Ok(Self { endpoint, model: String::new(), dialect: Dialect::Custom, api_key, dim })
    }

    /// The request body for a batch, in this provider's dialect.
    fn request_body(&self, texts: &[String]) -> serde_json::Value {
        match self.dialect {
            Dialect::OpenAi => serde_json::json!({ "input": texts, "model": self.model }),
            // Ollama embeds one text per call, so a batch is sent as separate
            // requests; this builds the body for a single one.
            Dialect::Ollama => {
                serde_json::json!({ "prompt": texts.first(), "model": self.model })
            }
            Dialect::Custom => serde_json::json!({ "input": texts }),
        }
    }

    /// Pull vectors out of a response body, in this provider's dialect.
    fn parse_response(&self, body: &serde_json::Value) -> Result<Vec<Vec<f32>>> {
        let malformed = || VectorError::MalformedResponse {
            provider: self.name(),
            detail: format!("unexpected shape: {body}"),
        };

        let rows: Vec<Vec<f32>> = match self.dialect {
            Dialect::OpenAi => body
                .get("data")
                .and_then(|d| d.as_array())
                .ok_or_else(malformed)?
                .iter()
                .map(|row| numbers(row.get("embedding")))
                .collect::<Option<_>>()
                .ok_or_else(malformed)?,
            Dialect::Ollama => {
                vec![numbers(body.get("embedding")).ok_or_else(malformed)?]
            }
            Dialect::Custom => body
                .get("embeddings")
                .and_then(|e| e.as_array())
                .ok_or_else(malformed)?
                .iter()
                .map(|row| numbers(Some(row)))
                .collect::<Option<_>>()
                .ok_or_else(malformed)?,
        };

        // A wrong width silently corrupts an index whose other vectors are a
        // different size, so it is checked here rather than discovered later.
        for row in &rows {
            if row.len() != self.dim {
                return Err(VectorError::DimensionMismatch {
                    expected: self.dim,
                    found: row.len(),
                });
            }
        }
        Ok(rows)
    }
}

fn numbers(value: Option<&serde_json::Value>) -> Option<Vec<f32>> {
    value?.as_array()?.iter().map(|n| n.as_f64().map(|f| f as f32)).collect()
}

/// Read an API key from the environment.
///
/// Keys live in the environment, never in collection metadata, which is
/// readable by anyone who can read the data directory.
fn read_key(var: &str) -> Result<String> {
    std::env::var(var).map_err(|_| VectorError::MissingApiKey { var: var.to_string() })
}

#[async_trait]
impl EmbeddingProvider for HttpProvider {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        // Ollama takes one text per request; the others take the whole batch.
        let batches: Vec<&[String]> = match self.dialect {
            Dialect::Ollama => texts.iter().map(std::slice::from_ref).collect(),
            _ => vec![texts],
        };

        let client = reqwest::Client::new();
        let mut out = Vec::with_capacity(texts.len());

        for batch in batches {
            let mut request = client.post(&self.endpoint).json(&self.request_body(batch));
            if let Some(key) = &self.api_key {
                request = request.bearer_auth(key);
            }

            let response = request.send().await.map_err(|e| VectorError::Transport {
                provider: self.name(),
                detail: e.to_string(),
            })?;

            let status = response.status();
            if !status.is_success() {
                // The body often explains the failure (bad model, quota); the
                // status alone rarely does.
                let detail = response.text().await.unwrap_or_default();
                return Err(VectorError::ProviderRejected {
                    provider: self.name(),
                    status: status.as_u16(),
                    detail: detail.chars().take(300).collect(),
                });
            }

            let body: serde_json::Value = response.json().await.map_err(|e| {
                VectorError::MalformedResponse { provider: self.name(), detail: e.to_string() }
            })?;
            out.extend(self.parse_response(&body)?);
        }

        if out.len() != texts.len() {
            return Err(VectorError::MalformedResponse {
                provider: self.name(),
                detail: format!("expected {} vectors, got {}", texts.len(), out.len()),
            });
        }
        Ok(out)
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn name(&self) -> &'static str {
        match self.dialect {
            Dialect::OpenAi => "openai",
            Dialect::Ollama => "ollama",
            Dialect::Custom => "custom_http",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider(dialect: Dialect, dim: usize) -> HttpProvider {
        HttpProvider {
            endpoint: "http://example.invalid".into(),
            model: "m".into(),
            dialect,
            api_key: None,
            dim,
        }
    }

    #[test]
    fn byo_has_no_provider_to_build() {
        // The client supplies vectors, so there is nothing to call.
        assert!(matches!(build(&ProviderConfig::Byo, 8).err(), Some(VectorError::NoProvider)));
    }

    #[test]
    fn request_bodies_match_each_dialect() {
        let texts = vec!["a".to_string(), "b".to_string()];

        let openai = provider(Dialect::OpenAi, 2).request_body(&texts);
        assert_eq!(openai["input"], serde_json::json!(["a", "b"]));
        assert_eq!(openai["model"], "m");

        // Ollama takes a single prompt per request.
        let ollama = provider(Dialect::Ollama, 2).request_body(&texts[..1]);
        assert_eq!(ollama["prompt"], "a");

        let custom = provider(Dialect::Custom, 2).request_body(&texts);
        assert_eq!(custom["input"], serde_json::json!(["a", "b"]));
    }

    #[test]
    fn responses_parse_in_each_dialect() {
        let openai = serde_json::json!({
            "data": [ { "embedding": [1.0, 2.0] }, { "embedding": [3.0, 4.0] } ]
        });
        assert_eq!(
            provider(Dialect::OpenAi, 2).parse_response(&openai).unwrap(),
            vec![vec![1.0, 2.0], vec![3.0, 4.0]]
        );

        let ollama = serde_json::json!({ "embedding": [1.0, 2.0] });
        assert_eq!(
            provider(Dialect::Ollama, 2).parse_response(&ollama).unwrap(),
            vec![vec![1.0, 2.0]]
        );

        let custom = serde_json::json!({ "embeddings": [[1.0, 2.0]] });
        assert_eq!(
            provider(Dialect::Custom, 2).parse_response(&custom).unwrap(),
            vec![vec![1.0, 2.0]]
        );
    }

    #[test]
    fn a_wrong_width_is_rejected_rather_than_stored() {
        // Mixing widths in one index is meaningless, and the failure would
        // otherwise surface far from its cause.
        let body = serde_json::json!({ "embeddings": [[1.0, 2.0, 3.0]] });
        let err = provider(Dialect::Custom, 2).parse_response(&body).unwrap_err();
        assert!(matches!(err, VectorError::DimensionMismatch { expected: 2, found: 3 }));
    }

    #[test]
    fn malformed_responses_are_reported_not_silently_empty() {
        for body in [
            serde_json::json!({}),
            serde_json::json!({ "data": "not an array" }),
            serde_json::json!({ "embeddings": [["not", "numbers"]] }),
        ] {
            assert!(
                provider(Dialect::Custom, 2).parse_response(&body).is_err()
                    || provider(Dialect::OpenAi, 2).parse_response(&body).is_err(),
                "should have rejected {body}"
            );
        }
    }

    #[tokio::test]
    async fn an_empty_batch_makes_no_request() {
        // Otherwise every document with no embeddable text costs a round trip.
        let result = provider(Dialect::Custom, 2).embed(&[]).await.unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn a_missing_api_key_is_reported_by_variable_name() {
        // Naming the variable is what makes this fixable without reading source.
        let config = ProviderConfig::OpenAi {
            model: "text-embedding-3-small".into(),
            endpoint: None,
            api_key_env: "KIMMY_TEST_KEY_DEFINITELY_UNSET".into(),
        };
        let Err(err) = build(&config, 1536) else {
            panic!("an unset key variable should not build a provider");
        };
        assert!(matches!(err, VectorError::MissingApiKey { ref var } if var.contains("UNSET")));
        assert!(err.to_string().contains("KIMMY_TEST_KEY_DEFINITELY_UNSET"));
    }

    #[test]
    fn endpoints_are_built_without_double_slashes() {
        let p = HttpProvider::ollama("http://localhost:11434/".into(), "m".into(), 8);
        assert_eq!(p.endpoint, "http://localhost:11434/api/embeddings");
    }

    #[cfg(not(feature = "local-embeddings"))]
    #[test]
    fn the_local_provider_is_unavailable_without_the_feature() {
        let config = ProviderConfig::Local { model: "bge-small-en-v1.5".into() };
        assert!(matches!(build(&config, 384).err(), Some(VectorError::LocalUnavailable)));
    }
}
