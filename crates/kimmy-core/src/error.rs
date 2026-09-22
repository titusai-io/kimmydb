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

    /// An index name is taken by a definition that is not the same one.
    ///
    /// Separate from [`Self::CollectionExists`], which this used to borrow.
    /// That produced a sentence built for a collection wrapped around an index
    /// — `collection "shop"."orders.item_qty (index already exists with
    /// different fields)" already exists` — and it named the wrong cause, since
    /// re-creating an index that differs only in its TTL also landed here.
    ///
    /// `differs` names what actually changed, because that is the whole
    /// question the reader has: the fix for a different TTL is not the fix for
    /// different fields.
    #[error(
        "index {index:?} on {db:?}.{collection:?} already exists with a different {differs}; \
         drop it first, or create this one under another name"
    )]
    IndexExists { db: String, collection: String, index: String, differs: String },

    #[error("document with _id {0} not found")]
    DocumentNotFound(String),

    #[error("duplicate key: document with _id {0} already exists")]
    DuplicateKey(String),

    #[error("unique index {index:?} violated: {detail}")]
    UniqueViolation { index: String, detail: String },

    #[error("{0}")]
    Unsupported(String),

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

    #[error("malformed cursor")]
    MalformedCursor,

    #[error("malformed stamp: pass back a stamp exactly as the server returned it")]
    MalformedStamp,

    #[error("bson error: {0}")]
    Bson(String),

    #[error("serialization error: {0}")]
    Serialization(String),
}

impl Error {
    /// Whether this refuses the *request* rather than reporting a failure of
    /// the *node*, which is the distinction replication settles a definition on
    /// (`kimmy_storage::sync::settle`).
    ///
    /// A refusal is a fact about what was asked, so the entry is skipped,
    /// counted and warned, and the round goes on. A failure of the node may
    /// succeed on retry, so it fails the round — because a round that quietly
    /// skips what it cannot understand is how corruption becomes convergence.
    ///
    /// **Exhaustive on purpose, with no wildcard.** `settle` used to carry a
    /// hand-written list of three variants, and `PartialFilter::parse` returns a
    /// fourth: `UnsupportedOperator`, which is not `Unsupported`. A definition a
    /// peer sent that this build refuses for its operator therefore failed the
    /// whole round, on every retry, instead of being skipped — latent until the
    /// first release that grows the partial-filter language, when the members
    /// not yet upgraded in a roll would each stall on it. A list cannot see a
    /// variant it does not name, so this is a match that will not compile until
    /// a new variant is classified.
    ///
    /// Every `false` below is the behaviour before this method existed, kept
    /// deliberately: widening any of them changes what replication skips and
    /// needs its own argument, not a drive-by.
    pub fn is_a_request_refusal(&self) -> bool {
        match self {
            // What was asked is not something this build can honour, and
            // retrying cannot change that.
            Error::InvalidQuery(_)
            | Error::UnsupportedOperator(_)
            | Error::Unsupported(_)
            | Error::IndexExists { .. } => true,

            // This node's own state answers the request differently, and
            // `settle` reads `CollectionNotFound` before asking here, as the
            // collection being gone is neither a refusal nor a failure.
            Error::CollectionNotFound { .. }
            | Error::DatabaseNotFound(_)
            | Error::CollectionExists { .. }
            | Error::DocumentNotFound(_) => false,

            // A conflict about data rather than about a definition. Reported
            // rather than refused, on the path that reports it (ADR-020).
            Error::DuplicateKey(_) | Error::UniqueViolation { .. } => false,

            // Malformed input on a path replication does not carry, so the
            // question does not arise; failing the round is the safe answer if
            // it ever does.
            Error::InvalidDocumentId { .. }
            | Error::InvalidName { .. }
            | Error::InvalidUpdate(_)
            | Error::ResumeTokenExpired
            | Error::MalformedResumeToken
            | Error::MalformedCursor
            | Error::MalformedStamp => false,

            // A value that will not decode or encode. Indistinguishable here
            // from corruption, which must never be skipped quietly.
            Error::Bson(_) | Error::Serialization(_) => false,
        }
    }

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
