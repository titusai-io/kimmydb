//! Error types shared across KimmyDB.

use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("database {0:?} not found")]
    DatabaseNotFound(String),

    #[error("collection {db:?}.{collection:?} not found")]
    CollectionNotFound { db: String, collection: String },

    #[error("collection {db:?}.{collection:?} already exists")]
    CollectionExists { db: String, collection: String },

    #[error("document with _id {0} not found")]
    DocumentNotFound(String),

    #[error("duplicate key: document with _id {0} already exists")]
    DuplicateKey(String),

    #[error("invalid _id type {found:?}: must be an ObjectId, string, integer, or binary")]
    InvalidDocumentId { found: String },

    #[error("invalid name {name:?}: {reason}")]
    InvalidName { name: String, reason: &'static str },

    #[error("invalid query: {0}")]
    InvalidQuery(String),

    #[error("invalid update: {0}")]
    InvalidUpdate(String),

    #[error("unsupported operator {0:?}")]
    UnsupportedOperator(String),

    #[error("change stream resume token is no longer available; the oplog has advanced past it")]
    ResumeTokenExpired,

    #[error("malformed resume token")]
    MalformedResumeToken,

    #[error("bson error: {0}")]
    Bson(String),

    #[error("serialization error: {0}")]
    Serialization(String),
}

impl Error {
    /// Names are used verbatim in on-disk keys and URL paths, so they are
    /// validated once, here, rather than defensively at every call site.
    pub fn validate_name(name: &str) -> Result<()> {
        const MAX_NAME_LEN: usize = 120;
        let reason = if name.is_empty() {
            Some("must not be empty")
        } else if name.len() > MAX_NAME_LEN {
            Some("must be at most 120 bytes")
        } else if name.starts_with("__") {
            Some("the `__` prefix is reserved for system objects")
        } else if name.contains(['/', '\\', '\0', '$', ' ']) {
            Some("must not contain '/', '\\', '$', spaces, or NUL")
        } else if name == "." || name == ".." {
            Some("must not be '.' or '..'")
        } else {
            None
        };

        match reason {
            Some(reason) => Err(Error::InvalidName { name: name.to_string(), reason }),
            None => Ok(()),
        }
    }
}

impl From<bson::error::Error> for Error {
    fn from(e: bson::error::Error) -> Self {
        Error::Bson(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Serialization(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_names_are_accepted() {
        for name in ["orders", "user_events", "a", "col-1", "Ünïcödé"] {
            assert!(Error::validate_name(name).is_ok(), "{name} should be valid");
        }
    }

    #[test]
    fn invalid_names_are_rejected() {
        for name in ["", "__vectors", "a/b", "a$b", "with space", ".", "..", "a\0b"] {
            assert!(Error::validate_name(name).is_err(), "{name:?} should be rejected");
        }
        assert!(Error::validate_name(&"x".repeat(121)).is_err());
    }
}
