//! redb-backed storage engine for KimmyDB.
//!
//! Owns the on-disk layout: collections, documents, secondary indexes, and the
//! oplog. Every mutation appends exactly one oplog entry *in the same
//! transaction* as the change itself, so the log can never disagree with the
//! data — which is what lets change streams, the embedding pipeline, and
//! cluster anti-entropy all read the same log and trust it.

#![allow(dead_code)]

pub mod codec;
pub mod docs;
pub mod engine;
pub mod error;
pub mod meta;
pub mod tables;
pub mod watch;

pub use docs::{ID_FIELD, WriteOutcome};
pub use engine::Engine;
pub use error::{Result, StorageError};
pub use meta::{CollectionMeta, DatabaseMeta, IndexField, IndexMeta};
pub use watch::{ChangeEvent, ChangeStream, InvalidateReason, WatchOptions, WatchScope};
