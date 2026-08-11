//! redb-backed storage engine for KimmyDB.
//!
//! Owns the on-disk layout: collections, documents, secondary indexes, and the
//! oplog. Every mutation appends exactly one oplog entry *in the same
//! transaction* as the change itself, so the log can never disagree with the
//! data — which is what lets change streams, the embedding pipeline, and
//! cluster anti-entropy all read the same log and trust it.

#![allow(dead_code)]

pub mod backup;
pub mod codec;
pub mod docs;
pub mod engine;
pub mod error;
pub mod gc;
pub mod index;
pub mod meta;
pub mod migrate;
pub mod rewind;
pub mod snapshot;
pub mod sync;
pub mod tables;
pub mod vectors;
pub mod watch;

pub use docs::{ID_FIELD, WriteOutcome};
pub use engine::Engine;
pub use engine::physical_now_ms;
pub use error::{Result, StorageError};
pub use gc::{GcOutcome, RetentionPolicy};
pub use meta::{CollectionMeta, DatabaseMeta, Enforcement, IndexField, IndexMeta, VectorConfig};
pub use snapshot::{CollectionState, SNAPSHOT_PAGE, SnapshotCursor, SnapshotDoc, SnapshotPage};
pub use sync::{SyncOutcome, lag_behind_ms};
pub use watch::{ChangeEvent, ChangeStream, InvalidateReason, WatchOptions, WatchScope};
