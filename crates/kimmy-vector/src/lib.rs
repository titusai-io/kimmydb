//! Embeddings and vector search for KimmyDB.
//!
//! Providers turn text into vectors; the embedding worker keeps them in sync
//! with documents by consuming the oplog; the index makes them searchable.
//!
//! The provider is the only part that reaches outside the process, which is
//! why embedding runs off the write path — a write must never block on a
//! remote call or on model inference.

#![allow(dead_code)]

pub mod cache;
pub mod error;
pub mod index;
#[cfg(feature = "local-embeddings")]
pub mod local;
pub mod provider;
pub mod search;
pub mod worker;

pub use cache::{Access, IndexCache};
pub use error::{Result, VectorError};
pub use index::HnswIndex;
pub use provider::{EmbeddingProvider, build};
pub use search::{Hit, SearchOptions, keyword_search, reciprocal_rank_fusion, vector_search};
pub use worker::{EmbeddingWorker, Outcome};
