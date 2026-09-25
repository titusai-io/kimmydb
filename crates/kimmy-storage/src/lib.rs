//! redb-backed storage engine for KimmyDB.
//!
//! Owns the on-disk layout: collections, documents, secondary indexes, and the
//! oplog. Every mutation appends exactly one oplog entry *in the same
//! transaction* as the change itself, so the log can never disagree with the
//! data — which is what lets change streams, the embedding pipeline, and
//! cluster anti-entropy all read the same log and trust it.

#![allow(dead_code)]
// Tests build stores with redb directly; production opens them only after the
// check before the open (ADR-190, and `disallowed-methods` in clippy.toml).
#![cfg_attr(test, allow(clippy::disallowed_methods))]

pub mod backup;
pub mod codec;
pub mod divergence;
pub mod docs;
pub mod engine;
pub mod error;
pub mod expiry;
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub mod faults;
pub mod format;
pub mod gc;
pub mod health;
pub mod hold_meter;
pub mod index;
mod live_count;
pub mod meta;
pub mod migrate;
pub mod modify;
pub mod purge;
pub mod rewind;
pub mod snapshot;
mod store_lock;
pub mod sync;
pub mod tables;
pub mod vectors;
pub mod watch;

pub use divergence::{
    DivergenceTracker, Findings as DivergenceFindings, LocalState as DivergenceLocalState,
    PeerAnswer as DivergencePeerAnswer, compare as compare_divergence, next_probe,
};
pub use docs::{BulkInsertError, ID_FIELD, WriteOutcome, WriteScope};
pub use engine::physical_now_ms;
pub use engine::{
    CacheReading, DurabilityClass, Engine, WRITER_HOLD_BUCKETS_US, WRITER_HOLD_WARN,
    WRITER_WAIT_BUCKETS_US, WriterHoldSnapshot, WriterHolder, WriterWaitSnapshot, blocking,
    metered_writer_wait, with_write_wait_budget,
};
pub use error::{Result, StorageError};
pub use expiry::{ExpiryOutcome, MAX_EXPIRED_PER_PASS, ttl_indexes};
pub use gc::{GcOutcome, RetentionPolicy};
pub use hold_meter::{
    Component as HoldComponent, HoldDecomposition, Phase as HoldPhase, SERVE_WALK_BUCKETS_US,
    ServeSnapshot,
};
pub use index::{CandidateOrder, Dropped, IndexScan, IndexScanOutcome};
pub use meta::{CollectionMeta, DatabaseMeta, Enforcement, IndexField, IndexMeta, VectorConfig};
pub use modify::{Candidates, MAX_CANDIDATES, ModifyManyOutcome, ModifyOutcome, ModifySpec};
pub use purge::PurgeCounters;
pub use snapshot::{
    CollectionState, SNAPSHOT_PAGE, SnapshotApplied, SnapshotCursor, SnapshotDoc, SnapshotPage,
    SnapshotProgress, SnapshotTombstone,
};
pub use sync::{
    EntryWait, MarkedRange, PullTiming, SyncOutcome, UnknownCollection, WindowEnd, coverage_up_to,
    lacks_collected, lag_behind_ms, lag_beyond_horizon_ms,
};
pub use vectors::VectorWrite;
pub use watch::{
    ChangeEvent, ChangeStream, InvalidateReason, OplogWindow, WatchOptions, WatchScope,
};
