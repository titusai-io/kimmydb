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
    /// refuses to parse, found by the schema 3 -> 4 migration before it
    /// rebuilt anything (ADR-183).
    ///
    /// The migration's whole job is to rebuild each partial index to hold what
    /// `find` with its filter selects, and it cannot know what an index should
    /// hold from a filter it cannot read. Refusing at open confines the
    /// condition to schema 3 files, which is what lets ADR-181 call it
    /// transient and decline a metric for it. Nothing is written: the on-disk
    /// version is left as it was and no index entries were cleared.
    ///
    /// **What an operator can do depends on that version**, which is why the
    /// message asks `unparseable_filter_remedy` rather than stating one. Below
    /// schema 4 the previous build still opens the directory, so the index can
    /// be dropped there and the upgrade retried. At schema 4 -- a refusal while
    /// resuming an interrupted migration -- neither build opens it: that one
    /// refuses the schema and this one refuses the definition.
    #[error(
        "this build cannot parse {} stored partial index definition(s), so the schema 3 to 4 \
         migration cannot know what those indexes should hold. This attempt changed nothing: \
         the on-disk version is still {found} and no index entries were cleared. Refused: {}. \
         {}",
        refused.len(),
        refused.join("; "),
        unparseable_filter_remedy(*found)
    )]
    UnparseablePartialFilter { found: u8, refused: Vec<String> },

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

    /// The store was refused before anything opened it for writing, and
    /// nothing in it was changed (ADR-190): a newer build wrote it, its
    /// sidecar is unreadable, or it could not be read.
    #[error("{0}")]
    RefusedStore(String),

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
}

/// What an operator can actually do about a stored partial filter this build
/// refuses, which depends on the version the file is at.
///
/// Below schema 4 the previous build still opens the directory, so the index can
/// be dropped there and the upgrade retried. At schema 4 it cannot: that build
/// refuses the schema and this one refuses the definition, so no build can serve
/// the directory until the definition is gone. That state needs a migration to
/// have been interrupted *and* a filter this build refuses, so it is close to
/// unreachable -- but saying "use the previous build" there would be advice that
/// cannot be followed.
fn unparseable_filter_remedy(found: u8) -> &'static str {
    if found < 4 {
        "To proceed, start this data directory with the previous build, drop each index named \
         above -- recreating it with a filter this build accepts -- and upgrade again"
    } else {
        "This directory cannot be opened by either build until that definition is gone: the \
         previous build refuses schema 4, and this one refuses the definition. Wipe the data \
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
