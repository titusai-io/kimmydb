//! Core types for KimmyDB.
//!
//! This crate is deliberately I/O-free: it defines the vocabulary every other
//! crate speaks in. The most important type here is [`Hlc`], the hybrid logical
//! clock that drives last-writer-wins conflict resolution, oplog ordering, and
//! change-stream resume tokens alike.

pub mod build;
pub mod cmp;
pub mod conflict;
pub mod cursor;
pub mod ddl;
pub mod decimal;
pub mod error;
pub mod hlc;
pub mod ids;
pub mod index_meta;
pub mod json_schema;
pub mod keyenc;
pub mod limits;
pub mod matching;
pub mod oplog;
pub mod partial;
pub mod path;
pub mod record;
pub mod regex;
pub mod type_names;
pub mod validator;
pub mod vector_meta;
pub mod vector_record;
pub mod version;

#[cfg(test)]
mod json_schema_tests;

pub use cmp::{canonical_cmp, holds_decimal128};
pub use conflict::{UniqueViolationDetail, UnkeyedDocumentDetail};
pub use cursor::Cursor;
pub use ddl::{
    CollectionCreate, CollectionRef, DATABASE_DROP_NAME, IndexCreate, IndexDrop, ValidatorSet,
    VectorSet, database_drop_id,
};
pub use error::{Error, Result};
pub use hlc::{HLC_ENCODED_LEN, Hlc, HlcClock, Stamp};
pub use ids::{CollectionId, DECIMAL128_ID_LEN, Decimal128Id, DocId, NodeId};
pub use index_meta::{Enforcement, IndexField, IndexMeta};
pub use oplog::{Issued, MAX_TOKEN_ORIGINS, OpKind, OplogEntry, ResumeToken};
pub use partial::{PartialFilter, PartialOp};
pub use record::DocRecord;
pub use validator::{ValidationAction, ValidationLevel, ValidatorSpec};
pub use vector_meta::{ChunkConfig, Metric, ProviderConfig, VectorConfig};
pub use vector_record::{ChunkSource, Unstamped, VectorRecord, similarity};
pub use version::VersionVector;
