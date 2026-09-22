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
    /// version is still 3, no index entries were cleared, and the previous
    /// build still opens this directory.
    #[error(
        "this build cannot parse {} stored partial index definition(s), so the schema 3 to 4 \
         migration cannot know what those indexes should hold. Nothing was changed: the \
         on-disk version is still {found} and no index entries were cleared. Refused: {}. To \
         proceed, start this data directory with the previous build, drop each index named \
         above -- recreating it with a filter this build accepts -- and upgrade again",
        refused.len(),
        refused.join("; ")
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
