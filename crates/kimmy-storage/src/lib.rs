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
pub mod check;
pub mod class_step;
pub mod clock_bound;
pub mod codec;
mod database_tombstone;
pub mod definition_digest;
pub mod definition_probe;
mod definition_repair;
pub mod digest_probe;
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
pub mod own_clock;
// The field path of a write (insert, replace, delete) over a record's merge state.
#[cfg(test)]
mod conflict_event_tests;
mod conflict_winner;
mod field_write;
pub mod health;
pub mod hold_meter;
pub mod index;
mod join;
#[cfg(feature = "lab-hooks")]
pub mod lab_hooks;
mod late_drop;
mod live_count;
pub mod live_digest;
mod live_walk;
#[cfg(test)]
mod loss_order_tests;
mod loss_pending;
#[cfg(test)]
mod loss_stream_tests;
pub mod merge;
pub mod meta;
pub mod migrate;
mod missing;
pub mod modify;
pub mod open_progress;
pub mod purge;
pub mod refusal_hint;
mod register;
pub mod rewind;
pub mod snapshot;
mod store_lock;
pub mod sync;
pub mod tables;
mod tombstone_horizon;
pub mod validator;
pub mod vectors;
pub mod verified;
pub mod violations_table;
#[cfg(test)]
mod violations_table_tests;
pub mod walk;
pub use validator::{
    Compiled as CompiledValidator, VIOLATIONS_PAGE_MAX, VIOLATIONS_SCAN_PAGE, ValidationCount,
    ValidatorViolations, check_validator_spec,
};
pub use verified::VerifiedWalk;
pub use violations_table::{BackfillStep, ViolationsSnapshot};
pub use walk::WalkScope;
pub mod watch;
pub mod writer_hold;

#[cfg(test)]
mod decimal_cost_tests;
#[cfg(test)]
mod key_walk_tests;

#[cfg(test)]
mod database_drop_tests;

#[cfg(test)]
mod epoch_tests;

#[cfg(test)]
mod field_write_tests;

#[cfg(test)]
mod merge_policy_tests;

#[cfg(test)]
mod converge_tests;

#[cfg(test)]
mod converge_field_tests;

#[cfg(test)]
mod definition_repair_tests;

#[cfg(test)]
mod field_apply_tests;

#[cfg(test)]
mod field_digest_tests;

#[cfg(test)]
mod clock_bound_entry_tests;

#[cfg(test)]
mod clock_bound_document_tests;
#[cfg(test)]
mod clock_bound_tombstone_tests;

#[cfg(test)]
mod database_drop_stage_tests;

#[cfg(test)]
mod swap_kind_tests;

#[cfg(test)]
mod register_tests;

#[cfg(test)]
mod rule_b_tests;

#[cfg(test)]
mod release_effect_tests;

#[cfg(test)]
mod rule_s_tests;

#[cfg(test)]
mod ldrop_tests;

#[cfg(test)]
mod stage_name_tests;

#[cfg(test)]
mod resolve_tests;

#[cfg(test)]
mod burial_tests;

#[cfg(test)]
mod logical_drop_tests;

#[cfg(test)]
mod out_counter_tests;

#[cfg(test)]
mod join_tests;

#[cfg(test)]
mod shadow_burial_tests;

#[cfg(test)]
mod definition_floor_tests;

#[cfg(test)]
mod carried_drop_tests;

#[cfg(test)]
mod resolve_row_property_tests;

#[cfg(test)]
mod swap_routes_tests;

#[cfg(test)]
mod backup_register_tests;

#[cfg(test)]
mod definition_door_tests;
#[cfg(test)]
mod recreate_tests;
#[cfg(test)]
mod register_growth_bench;

#[cfg(test)]
mod d10_tests;

#[cfg(test)]
mod purge_defer_tests;

#[cfg(test)]
mod doomed_call_tests;

#[cfg(test)]
mod own_clock_tests;

#[cfg(test)]
mod clock_bound_state_tests;

#[cfg(test)]
mod clock_bound_frame_tests;

#[cfg(test)]
mod missing_tests;

#[cfg(test)]
mod clock_bound_claim_tests;

#[cfg(test)]
mod join_retention_tests;

#[cfg(test)]
mod field_snapshot_tests;

#[cfg(test)]
mod carried_delete_state_tests;

#[cfg(test)]
mod carried_delete_join_tests;

#[cfg(test)]
mod sweep_tombstone_tests;

#[cfg(test)]
mod rewind_field_tests;

#[cfg(test)]
mod floor_filter_tests;

#[cfg(test)]
mod sweep_rewrite_tests;

#[cfg(test)]
mod sweep_resume_tests;

#[cfg(test)]
mod state_hold_tests;

#[cfg(test)]
mod state_hold_rule_tests;

#[cfg(test)]
mod held_gap_tests;

#[cfg(test)]
mod tombstone_horizon_tests;

// The break-the-guard tests of the repair build's third review: the never-resurrect schedules
// and the random-history oracle.
#[cfg(test)]
mod revc_break_tests;

#[cfg(test)]
mod keep_later_writes_tests;

#[cfg(test)]
mod definition_digest_tests;

#[cfg(test)]
mod unkeyed_tests;

#[cfg(test)]
mod definition_probe_tests;

#[cfg(test)]
mod refusal_hint_tests;

#[cfg(test)]
mod snapshot_stamp_tests;

#[cfg(test)]
mod vector_width_tests;

#[cfg(test)]
mod life_supersession_tests;

#[cfg(test)]
mod convergence_model_tests;

#[cfg(test)]
mod later_life_stop_tests;

#[cfg(test)]
mod young_stamp_model_tests;

#[cfg(test)]
mod decimal_definition_tests;
#[cfg(test)]
mod size_cap_tests;
#[cfg(test)]
mod snapshot_pages_tests;
#[cfg(test)]
mod validator_tests;
#[cfg(test)]
mod writer_reread_tests;

#[cfg(test)]
mod log_capture;
#[cfg(feature = "test-hooks")]
pub use live_walk::stop_open_after_rows_for_test;

pub use clock_bound::{ClockBound, ClockRefusal, ClockStop, DEFAULT_MAX_CLOCK_SKEW_MS};
pub use conflict_winner::ConflictWinner;
pub use database_tombstone::{Buried, BuriedCollection, Swept};
pub use definition_digest::{DefinitionKey, DefinitionKind};
pub use definition_probe::{
    DefinitionAnswered, DefinitionAsk, DefinitionDefer, DefinitionEntry, DefinitionOutcome,
    DefinitionReply, DefinitionRound, DefinitionVerdict, WireDefinition, WireRefusal,
};
pub use definition_repair::{DEFINITION_RESULTS, DefinitionRestore};
pub use digest_probe::{
    ANSWER_KEY_BYTES, ASK_KEY_BYTES, Defer, DigestAnswered, DigestAsk, DigestEntry, DigestOutcome,
    DigestPlan, DigestReply, DigestRound, DigestVerdict, SCAN_BYTES, SCAN_WALL, ScanBudget,
    SelfCheck, Uncovered,
};
pub use divergence::{
    DivergenceTracker, Findings as DivergenceFindings, LocalState as DivergenceLocalState,
    PeerAnswer as DivergencePeerAnswer, compare as compare_divergence, next_probe,
};
pub use docs::{BulkInsertError, ID_FIELD, Seen, WriteOutcome, WriteScope};
pub use engine::{
    CacheReading, DurabilityClass, Engine, NotClosed, OpenOptions, RepairsAtOpen, Resolved,
    WRITER_HOLD_BUCKETS_US, WRITER_HOLD_WARN, WRITER_WAIT_BUCKETS_US, WriterHoldSnapshot,
    WriterHolder, WriterWaitSnapshot, blocking, metered_writer_wait, with_write_wait_budget,
};
pub use engine::{OUT_BURIAL_CAUSES, OUT_LOSS_KINDS, SNAPSHOT_SKIP_LABELS};
pub use engine::{parse_test_clock_offset_secs, physical_now_ms, set_test_clock_offset_secs};
pub use error::{Applied, RefusedKind, Result, StopReason, StorageError};
pub use expiry::{ExpiryOutcome, MAX_EXPIRED_PER_PASS, ttl_indexes};
pub use gc::{GcOutcome, RetentionPolicy};
pub use hold_meter::{
    Component as HoldComponent, HoldDecomposition, Phase as HoldPhase, SERVE_WALK_BUCKETS_US,
    ServeSnapshot, ServeWalk, WalkPath,
};
pub use index::{
    CandidateOrder, Dropped, IndexScan, IndexScanOutcome, KeyHold, ScanExtras, UnkeyedPage,
};
pub use loss_pending::LossRefusal;
pub use merge::{SinkCounts, SinkMode, SinkOutcome, WhenMatched};
pub use meta::{CollectionMeta, DatabaseMeta, Enforcement, IndexField, IndexMeta, VectorConfig};
pub use modify::{
    Candidates, MAX_CANDIDATES, MatchHold, ModifyManyOutcome, ModifyOutcome, ModifySpec,
};
pub use open_progress::{
    OpenPhase, OpenSnapshot, open_snapshot, open_stop_requested, request_open_stop, set_open_phase,
};
pub use own_clock::OwnStampRefusal;
pub use purge::PurgeCounters;
pub use refusal_hint::{HintKind, RefusalHint};
#[cfg(any(test, feature = "test-hooks"))]
pub use snapshot::SnapshotReaderHook;
pub use snapshot::{
    CollectionState, LEFT_IDS_NAMED, RegisterCarry, SNAPSHOT_PAGE, SNAPSHOT_PAGE_BYTES,
    ServeVerdict, SnapshotApplied, SnapshotCursor, SnapshotDoc, SnapshotPage, SnapshotPolicy,
    SnapshotProgress, SnapshotTombstone,
};
pub use sync::{
    EntryWait, MarkedRange, PeerWindow, PullTiming, SyncOutcome, UnknownCollection, WindowEnd,
    coverage_up_to, lacks_collected, lag_behind_ms, lag_beyond_horizon_ms,
};
pub use vectors::{VectorWrite, VectorsOff};
pub use watch::{
    ChangeEvent, ChangeStream, ExamineBudget, InvalidateReason, OplogWindow, WatchOptions,
    WatchScope, set_test_serve_walk_budget, set_test_serve_walk_linear,
};
#[cfg(test)]
mod loss_apply_tests;
#[cfg(test)]
mod loss_emission_tests;
#[cfg(test)]
mod loss_entry_tests;
#[cfg(test)]
mod loss_free_digest_tests;
#[cfg(test)]
mod loss_lifecycle_tests;
#[cfg(test)]
mod loss_limits_tests;
#[cfg(test)]
mod tombstone_id_tests;
