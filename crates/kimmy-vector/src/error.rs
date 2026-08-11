//! Vector pipeline errors.

use thiserror::Error;

pub type Result<T, E = VectorError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum VectorError {
    #[error(
        "this collection is configured for client-supplied vectors, so the server does not \
         embed; send a vector with the document, or configure an embedding provider"
    )]
    NoProvider,

    #[error(
        "the local embedding provider requires a build with the `local-embeddings` feature; \
         this build has no ONNX runtime"
    )]
    LocalUnavailable,

    #[error("environment variable {var} is not set, so the provider has no API key")]
    MissingApiKey { var: String },

    #[error("could not reach the {provider} embedding provider: {detail}")]
    Transport { provider: &'static str, detail: String },

    #[error("the {provider} embedding provider returned {status}: {detail}")]
    ProviderRejected { provider: &'static str, status: u16, detail: String },

    #[error("the {provider} embedding provider returned an unusable response: {detail}")]
    MalformedResponse { provider: &'static str, detail: String },

    #[error("expected a vector of {expected} dimensions, got {found}")]
    DimensionMismatch { expected: usize, found: usize },

    #[error("HNSW snapshot unusable: {0}")]
    Snapshot(String),

    #[error("embedding model {model:?} is not available: {detail}")]
    ModelUnavailable { model: String, detail: String },

    #[error(transparent)]
    Storage(#[from] kimmy_storage::StorageError),

    #[error(transparent)]
    Core(#[from] kimmy_core::Error),
}
