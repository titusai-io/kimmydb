//! In-process ONNX embedding.
//!
//! Compiled only with the `local-embeddings` feature. It pulls ONNX Runtime as
//! a **native** dependency, which undoes the pure-Rust property that motivated
//! choosing redb over RocksDB (ADR-001) and `rust_crypto` over `aws_lc_rs`
//! (ADR-016) — so the default build does without it, and this is opt-in.
//!
//! What the feature costs:
//!
//! - ONNX Runtime binaries downloaded **at build time**
//! - The model downloaded at **first use**, into the fastembed cache
//! - A container image roughly three times larger
//!
//! What it buys: embedding with no network at query time, no API key, and no
//! per-token cost.

use async_trait::async_trait;
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use tokio::sync::Mutex;

use crate::error::{Result, VectorError};
use crate::provider::EmbeddingProvider;

/// Embeds locally via ONNX Runtime.
pub struct LocalProvider {
    /// `TextEmbedding::embed` takes `&mut self`, and inference is CPU-bound
    /// anyway, so calls are serialized rather than run concurrently.
    model: Mutex<TextEmbedding>,
    model_name: String,
    dim: usize,
}

impl LocalProvider {
    pub fn new(model: &str, dim: usize) -> Result<Self> {
        let embedding_model = resolve_model(model)?;
        let inner = TextEmbedding::try_new(InitOptions::new(embedding_model)).map_err(|e| {
            VectorError::ModelUnavailable { model: model.to_string(), detail: e.to_string() }
        })?;

        Ok(Self { model: Mutex::new(inner), model_name: model.to_string(), dim })
    }

    /// The dimension a model produces, so a configuration can be checked
    /// against reality rather than trusted.
    pub fn expected_dim(model: &str) -> Option<usize> {
        match model {
            "bge-small-en-v1.5" | "all-MiniLM-L6-v2" => Some(384),
            "bge-base-en-v1.5" => Some(768),
            "bge-large-en-v1.5" => Some(1024),
            _ => None,
        }
    }
}

/// Map a configured model name onto a fastembed model.
///
/// An explicit list rather than a passthrough: an unknown name should fail at
/// configuration time with the supported set named, not download something
/// unexpected.
fn resolve_model(model: &str) -> Result<EmbeddingModel> {
    match model {
        "bge-small-en-v1.5" => Ok(EmbeddingModel::BGESmallENV15),
        "bge-base-en-v1.5" => Ok(EmbeddingModel::BGEBaseENV15),
        "bge-large-en-v1.5" => Ok(EmbeddingModel::BGELargeENV15),
        "all-MiniLM-L6-v2" => Ok(EmbeddingModel::AllMiniLML6V2),
        other => Err(VectorError::ModelUnavailable {
            model: other.to_string(),
            detail: "supported local models: bge-small-en-v1.5, bge-base-en-v1.5, \
                     bge-large-en-v1.5, all-MiniLM-L6-v2"
                .into(),
        }),
    }
}

#[async_trait]
impl EmbeddingProvider for LocalProvider {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let owned: Vec<String> = texts.to_vec();
        let mut model = self.model.lock().await;

        // Inference is CPU-bound and would otherwise stall the async runtime's
        // worker thread for the whole batch.
        let vectors = tokio::task::block_in_place(|| model.embed(owned, None)).map_err(|e| {
            VectorError::ModelUnavailable { model: self.model_name.clone(), detail: e.to_string() }
        })?;

        for row in &vectors {
            if row.len() != self.dim {
                return Err(VectorError::DimensionMismatch {
                    expected: self.dim,
                    found: row.len(),
                });
            }
        }
        Ok(vectors)
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn name(&self) -> &'static str {
        "local"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_models_resolve_and_unknown_ones_name_the_alternatives() {
        assert!(resolve_model("bge-small-en-v1.5").is_ok());

        let Err(err) = resolve_model("not-a-model") else {
            panic!("an unknown model should not resolve");
        };
        // Failing without saying what *is* supported forces a source dive.
        assert!(err.to_string().contains("bge-small-en-v1.5"), "unhelpful error: {err}");
    }

    #[test]
    fn expected_dimensions_are_known_for_supported_models() {
        assert_eq!(LocalProvider::expected_dim("bge-small-en-v1.5"), Some(384));
        assert_eq!(LocalProvider::expected_dim("bge-large-en-v1.5"), Some(1024));
        assert_eq!(LocalProvider::expected_dim("unknown"), None);
    }
}
