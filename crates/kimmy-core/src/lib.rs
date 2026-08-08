//! Core types for KimmyDB.
//!
//! This crate is deliberately I/O-free: it defines the vocabulary every other
//! crate speaks in. The most important type here is [`Hlc`], the hybrid logical
//! clock that drives last-writer-wins conflict resolution, oplog ordering, and
//! change-stream resume tokens alike.

pub mod cmp;
pub mod conflict;
pub mod error;
pub mod hlc;
pub mod ids;
pub mod index_meta;
pub mod keyenc;
pub mod oplog;
pub mod path;
pub mod record;
pub mod vector_meta;
pub mod vector_record;
pub mod version;

pub use cmp::canonical_cmp;
pub use conflict::UniqueViolationDetail;
pub use error::{Error, Result};
pub use hlc::{HLC_ENCODED_LEN, Hlc, HlcClock, Stamp};
pub use ids::{CollectionId, DocId, NodeId};
pub use index_meta::{Enforcement, IndexField, IndexMeta};
pub use oplog::{OpKind, OplogEntry, ResumeToken};
pub use record::DocRecord;
pub use vector_meta::{ChunkConfig, Metric, ProviderConfig, VectorConfig};
pub use vector_record::{VectorRecord, similarity};
pub use version::VersionVector;
