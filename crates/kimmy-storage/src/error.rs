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
