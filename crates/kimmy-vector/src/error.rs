//! Vector pipeline errors.

use thiserror::Error;

pub type Result<T, E = VectorError> = std::result::Result<T, E>;

/// What part of reaching a provider failed.
///
/// A connect failure, a timeout and a reset on an open connection are three
/// different operational problems (DNS or firewall, a slow or overloaded
/// provider, a load balancer closing idle connections), and the log line
/// that reports the failure is the only place an operator can tell them
/// apart. Also the label on `kimmy_embed_provider_errors_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    /// The connection could not be established: DNS, TCP or TLS.
    Connect,
    /// The client's own deadline passed before a response arrived.
    Timeout,
    /// The connection was established and then failed mid-request.
    Reset,
    /// Anything reqwest does not classify.
    Other,
}

impl TransportKind {
    pub const ALL: [TransportKind; 4] = [
        TransportKind::Connect,
        TransportKind::Timeout,
        TransportKind::Reset,
        TransportKind::Other,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            TransportKind::Connect => "connect",
            TransportKind::Timeout => "timeout",
            TransportKind::Reset => "reset",
            TransportKind::Other => "other",
        }
    }
}

impl std::fmt::Display for TransportKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

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

    #[error("could not reach the {provider} embedding provider ({kind}): {detail}")]
    Transport { provider: &'static str, kind: TransportKind, detail: String },

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
