//! Storage-layer errors.

use thiserror::Error;

pub type Result<T, E = StorageError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    Core(#[from] kimmy_core::Error),

    #[error("database error: {0}")]
    Database(String),

    #[error("transaction error: {0}")]
    Transaction(String),

    #[error("corrupt record: {0}")]
    Corrupt(String),

    #[error(
        "on-disk format version {found} is not supported by this build (expected {expected}); \
         this data directory was written by a different version of KimmyDB"
    )]
    UnsupportedFormat { found: u8, expected: u8 },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Partial index definitions an earlier build stored that this build
    /// refuses to parse, found by the schema migration before it rebuilt
    /// anything (ADR-183, ADR-238).
    ///
    /// The migration rebuilds every index from its definition, and it cannot
    /// know what an index should hold from a filter it cannot read. Refusing
    /// at open confines the condition to files older than this build's schema,
    /// which is what lets ADR-181 call it transient and decline a metric for
    /// it. Nothing is written: the on-disk version is left as it was and no
    /// index entries were cleared.
    ///
    /// **What an operator can do depends on that version**, which is why the
    /// message asks `unparseable_filter_remedy` rather than stating one. Below
    /// this build's schema the previous build still opens the directory, so the
    /// index can be dropped there and the upgrade retried. At this build's
    /// schema -- a refusal while resuming an interrupted migration -- neither
    /// build opens it: that one refuses the schema and this one refuses the
    /// definition.
    #[error(
        "this build cannot parse {} stored partial index definition(s), so the schema \
         migration cannot know what those indexes should hold. This attempt changed nothing: \
         the on-disk version is still {found} and no index entries were cleared. Refused: {}. \
         {}",
        refused.len(),
        refused.join("; "),
        unparseable_filter_remedy(*found)
    )]
    UnparseablePartialFilter { found: u8, refused: Vec<String> },

    /// The storage schema migration needs more free space beside the store
    /// than the volume has (ADR-238, design 6.1 step 0). Refused before the
    /// sidecar is raised, so nothing the migration owns was written; `detail`
    /// says how `needed` is made up and what state the store is left in.
    #[error(
        "the storage schema migration needs {} MiB free and {} MiB is free, {detail}",
        needed >> 20,
        free >> 20
    )]
    NotEnoughSpace { needed: u64, free: u64, detail: String },

    /// A conditional write found a different version than the caller expected.
    ///
    /// `current` is the stamp the document holds now, or `None` when there is
    /// no live document — the caller expected a version and it is gone.
    /// Nothing was written, minted or published.
    #[error("the document is not at the expected version; re-read it and retry")]
    Stale { current: Option<kimmy_core::Stamp> },

    /// The single writer did not become free within the caller's budget
    /// (ADR-151). Nothing was written, minted or published; the caller may
    /// retry. Only a caller that set a budget can see this — one that did
    /// not waits for the writer however long it takes.
    #[error(
        "the write waited {} ms for the storage writer and gave up; the writer was held by \
         another transaction for the whole wait",
        waited.as_millis()
    )]
    WriterBusy { waited: std::time::Duration },

    /// A collection cannot be created under this name yet: a drop of an
    /// earlier collection of the same name still has rows under the id the
    /// name derives, and the drop purger is removing them (ADR-189). Nothing
    /// was written, and the purger has been asked to take this id next; the
    /// caller may retry. The creation does not remove the rows itself, because
    /// that takes as long as the dropped collection was large.
    #[error(
        "{db}.{name} was dropped and what it held is still being removed; it can be created again \
         once that finishes"
    )]
    CollectionPurging { db: String, name: String, id: kimmy_core::CollectionId },

    /// A write to a name was refused because this member knows of a register event that supersedes the physical the name answers
    /// from here, and has not made a newer event visible (ADR-272, decision 7; D10): a binding it holds in effect whose stage it
    /// cannot serve, or one it knows ahead of its position (`parked`), a logical drop or a recreate. Nothing was written, and the
    /// caller may retry: the refusal ends when the member makes the newer event visible.
    #[error(
        "writes to {logical} are refused on this member until it has made the newer replacement of the collection visible (a \
         `$out`, a drop or a recreate, stamped {event}, {known}); retry",
        event = event.hlc.wall_ms,
        known = if *parked { "learned ahead of its position" } else { "known" }
    )]
    NameSuperseded { logical: String, event: kimmy_core::Stamp, parked: bool },

    /// A vector configuration change refused because a replacing `$out` has swapped the name (ADR-272, decision 12; Q6.5-1): a name with
    /// a register event of its own, in effect or only known ahead of its position, keeps no vector configuration in this release.
    /// `change` is "configured on" or "removed from". Permanent for the name: the API answers 400, with this sentence and no prefix.
    #[error(
        "vectors cannot be {change} `{db}`.`{logical}`: a replacing `$out` has swapped this name, and a swapped name keeps no vector \
         configuration in this release (ADR-272, decision 12)"
    )]
    SwappedNameKeepsNoVectors { db: String, logical: String, change: &'static str },

    /// A vector configuration refused because a replacing `$out` of the name is in flight here: a stage of `logical` is held, and
    /// its effective deadline (`deadline`) has not passed on this member's clock (ADR-272, decision 12; Q6.5-1). Nothing was changed,
    /// and the caller may retry: the refusal ends when the swap completes or is abandoned, and a stage past its deadline is an
    /// orphan that blocks nothing.
    #[error(
        "`{db}`.`{logical}` has a replacing `$out` in progress (its staging collection is held here until {}); its vector \
         configuration cannot change until the swap completes or is abandoned: retry after it",
        rfc3339(.deadline)
    )]
    SwapInFlight { db: String, logical: String, deadline: kimmy_core::Hlc },

    /// A replicated entry or state of a `field` collection whose clock is more than the allowed
    /// skew ahead of this member's physical clock (U1). Nothing was written and nothing was
    /// witnessed: it applies once it is due. Not a fault of the peer or of this node, and never
    /// to be skipped: the apply of a batch ends at it and a snapshot page that meets it is
    /// rolled back whole (see [`crate::clock_bound`]).
    #[error(
        "refused until due: the {} of an entry from {} is {} ms past the allowed clock skew \
         ahead of this member's clock",
        .0.reason,
        .0.origin.node,
        .0.due_in_ms
    )]
    ClockRefused(Box<crate::clock_bound::ClockStop>),

    /// A schema change this member would make was refused before anything was written, because the stamps it
    /// would carry would be refused by the rest of the cluster until they are due (U1, [`crate::own_clock`]):
    /// this member's logical clock leads its physical clock by more than the allowed skew, or a peer has said it
    /// is refusing this member's entries for their clock. Retryable on another member.
    #[error("{0}")]
    OwnStampsRefused(crate::own_clock::OwnStampRefusal),

    /// The store was refused before anything opened it for writing, and
    /// nothing in it was changed (ADR-190): a newer build wrote it, its
    /// sidecar is unreadable, or it could not be read. `kind` says which of
    /// those it was, so `kimmyd check-store` can tell damage from a store
    /// that is only newer than this build (ADR-204); a serving start refuses
    /// every kind alike.
    #[error("{why}")]
    RefusedStore { kind: RefusedKind, why: String },

    /// Another process has the store open, so this one does not open it and
    /// writes nothing. Its own variant because a second start on a live data
    /// directory must leave that directory's lifecycle marker alone.
    #[error("{0}")]
    StoreInUse(String),

    /// A commit failed after its durability step had begun: a `sync_data`
    /// was attempted in it, or the barrier flush that was to make it durable
    /// failed. Whether it reached the disk is not known. After a failed fsync
    /// the pages may be there, and redb's repair on the next open keeps the
    /// commit; after a failure following a successful fsync, such as the
    /// shrink that follows the final sync, it is certainly there. So the
    /// write may have happened, and may replicate: it must not be reported as
    /// a write that failed (outcome unknown, ADR-057's `verify`).
    #[error("the write may or may not have been applied: {0}")]
    OutcomeUnknown(String),

    /// A write transaction was not begun because the node is stopping, for
    /// the reason given (ADR-192): a later transaction of a request that has
    /// already committed one, which then answers
    /// [`StorageError::PartiallyApplied`] with this as its cause; or any
    /// transaction once the engine is closed to writes at the end of a
    /// shutdown. Nothing of that transaction was written.
    #[error("the node is stopping: {0}")]
    Stopping(StopReason),

    /// A request that commits in more than one transaction failed after its
    /// first commit (ADR-192). What `applied` counts is committed, published
    /// and replicating; `cause` is why the request stopped there.
    #[error("the request was partly applied ({applied}) and then failed: {cause}")]
    PartiallyApplied { applied: Applied, cause: Box<StorageError> },

    /// A row of a snapshot page that does not fit a page holding nothing else
    /// (`snapshot::SNAPSHOT_PAGE_BYTES`): a document larger than a frame, which
    /// its oplog entry cannot cross in a window either; or a document whose `_id`
    /// is so large (over about 3 MiB of binary or 5 MiB of string) that the
    /// page's cursor naming it, whose key crosses as an array of integers, does
    /// not fit with it. It crosses on a page once it is smaller. The sender
    /// answers the page's request with this reason.
    #[error(
        "{what} is {bytes} bytes on a snapshot page with the cursor that names it, which \
         has {room} bytes for its rows: it cannot cross on a page until it is smaller"
    )]
    SnapshotRowTooLarge { what: String, bytes: usize, room: usize },

    /// The register a whole-database snapshot carries on its last page does not fit a page that holds nothing else
    /// (`snapshot::SNAPSHOT_PAGE_BYTES`, ADR-272, decisions 4 and 6; Q6.6, unit C2d). The carry is never truncated: a truncated register
    /// is a silently wrong one, so the sender answers the page's request with this reason, and the limit is in docs/operations.md.
    #[error(
        "the register a snapshot carries is {bytes} bytes, which does not fit the {cap} bytes a \
         page has for it: it cannot be sent until it is compacted"
    )]
    SnapshotRegisterCarryTooLarge { bytes: usize, cap: usize },
}

/// What kind of refusal a [`StorageError::RefusedStore`] is (ADR-204).
///
/// Only [`RefusedKind::Damaged`] says the store itself is damaged. A store a
/// newer or older build wrote is healthy, and telling an operator to wipe it would lose
/// it; a refusal for any other reason (an unreadable sidecar, a store that
/// changed while it was checked, a read that failed) says nothing either way.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefusedKind {
    /// The header, a commit slot or a root cannot be right, or redb refused
    /// the store as corrupted or panicked reading it.
    Damaged,
    /// A newer build wrote it: a newer schema, redb or file format.
    Newer,
    /// An older build wrote it: an older storage schema, which this build does
    /// not upgrade. The store is healthy and is left as it is.
    Older,
    /// Neither: the store may be healthy, and this refusal cannot say.
    Other,
}

/// Why a continuing request was stopped; see [`StorageError::Stopping`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopReason {
    /// The node's shutdown drain reached its deadline.
    DrainDeadline,
    /// The storage failed (ADR-188), and the process is stopping.
    StorageFailed,
    /// The node's stop signal has come, and a walk that serves no client
    /// ends at it ([`crate::WalkScope::Background`]).
    Shutdown,
}

impl std::fmt::Display for StopReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::DrainDeadline => "the node reached its shutdown deadline",
            Self::StorageFailed => "the node's storage failed, and it is stopping",
            Self::Shutdown => "the node is shutting down",
        })
    }
}

/// What a request that commits in more than one transaction had committed
/// when it failed (ADR-192).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Applied {
    /// A `multi` update or delete (ADR-086). Counts are of committed chunks
    /// only; `in_doubt` is the size of a chunk whose commit's outcome is
    /// unknown, which is not in the other counts.
    Modify { matched: u64, modified: u64, commits: u64, in_doubt: u64 },
    /// A database drop: the collections whose burial committed, and the one
    /// whose burial's outcome is unknown, if any.
    DropDatabase { dropped: Vec<String>, in_doubt: Option<String> },
    /// A `$merge` or `$out` write of a pipeline's output (ADR-255). Counts are
    /// of committed chunks only; `in_doubt` is the size of a chunk whose commit's
    /// outcome is unknown, which is not in the counts.
    Write { counts: crate::SinkCounts, commits: u64, in_doubt: u64 },
}

impl std::fmt::Display for Applied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Modify { matched, commits, in_doubt, .. } => {
                write!(f, "{matched} matched in {commits} commits, {in_doubt} in doubt")
            }
            Self::Write { counts, commits, in_doubt } => write!(
                f,
                "{} inserted, {} matched in {commits} commits, {in_doubt} in doubt",
                counts.inserted, counts.matched
            ),
            Self::DropDatabase { dropped, in_doubt } => {
                write!(f, "{} collections dropped", dropped.len())?;
                match in_doubt {
                    Some(name) => write!(f, ", {name} in doubt"),
                    None => Ok(()),
                }
            }
        }
    }
}

/// What an operator can actually do about a stored partial filter this build
/// refuses, which depends on the version the file is at.
///
/// Below this build's schema the previous build still opens the directory, so
/// the index can be dropped there and the upgrade retried. At this build's
/// schema it cannot: that build refuses the schema and this one refuses the
/// definition, so no build can serve the directory until the definition is
/// gone. That state needs a migration to have been interrupted *and* a filter
/// this build refuses, so it is close to unreachable -- but saying "use the
/// previous build" there would be advice that cannot be followed.
fn unparseable_filter_remedy(found: u8) -> &'static str {
    if found < crate::migrate::SCHEMA_VERSION {
        "To proceed, start this data directory with the previous build, drop each index named \
         above -- recreating it with a filter this build accepts -- and upgrade again"
    } else {
        "This directory cannot be opened by either build until that definition is gone: the \
         previous build refuses this schema, and this one refuses the definition. Wipe the data \
         directory and let the member catch up from its peers, or restore a backup taken \
         before the upgrade"
    }
}

// redb splits failures across several error types that all mean "the storage
// layer failed"; collapsing them here keeps call sites readable.
macro_rules! from_redb {
    ($($ty:ty => $variant:ident),* $(,)?) => {
        $(
            impl From<$ty> for StorageError {
                fn from(e: $ty) -> Self {
                    StorageError::$variant(e.to_string())
                }
            }
        )*
    };
}

from_redb! {
    redb::DatabaseError => Database,
    redb::StorageError => Database,
    redb::TableError => Database,
    redb::CommitError => Transaction,
    redb::TransactionError => Transaction,
    redb::Error => Database,
}

impl From<serde_json::Error> for StorageError {
    fn from(e: serde_json::Error) -> Self {
        StorageError::Corrupt(format!("metadata json: {e}"))
    }
}

impl From<bson::error::Error> for StorageError {
    fn from(e: bson::error::Error) -> Self {
        StorageError::Corrupt(format!("bson: {e}"))
    }
}

/// A deadline as the RFC 3339 text a client reads.
fn rfc3339(at: &kimmy_core::Hlc) -> String {
    bson::DateTime::from_millis(at.wall_ms as i64)
        .try_to_rfc3339_string()
        .unwrap_or_else(|_| format!("{} ms", at.wall_ms))
}
