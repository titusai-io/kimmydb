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

    /// A request went past a limit set for the whole request, such as the
    /// number of documents a pipeline stage may hold. Answered exactly as an
    /// [`Error::InvalidQuery`] is, the same `400` and the same words, and
    /// kept apart from it only so that nothing evaluating one value at a
    /// time can set it aside: a budget for the request is never deferred
    /// (ADR-211, [`Error::is_deferrable`]).
    #[error("invalid query: {0}")]
    Limit(String),

    #[error("invalid update: {0}")]
    InvalidUpdate(String),

    /// An operator this build does not have. `operator` is the token as the
    /// request spelled it, and nothing else: query-language.md's template,
    /// `unsupported operator "$typo"`, is filled with an operator, and a
    /// client matches or logs that slot. Why it is refused, when there is
    /// more to say, goes in `reason`, after it. A refusal that is not about
    /// an operator is an [`Error::InvalidQuery`].
    #[error(
        "unsupported operator {operator:?}{}",
        .reason.as_deref().map(|reason| format!(": {reason}")).unwrap_or_default()
    )]
    UnsupportedOperator { operator: String, reason: Option<String> },

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

    /// An invariant of this build does not hold: a bug, never something the
    /// request or the data could cause. Kept apart from
    /// [`Error::InvalidQuery`] so that nothing which sets an evaluation error
    /// aside while another argument may still decide (an expression's `$and`
    /// and `$or`, ADR-211) can set this one aside too, and show it for some
    /// documents and hide it for others. See [`Error::is_deferrable`].
    #[error("internal error: {0}")]
    Internal(String),
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
            | Error::Limit(_)
            | Error::UnsupportedOperator { .. }
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

            // A bug in this build. Failing the round is the safe answer.
            Error::Internal(_) => false,
        }
    }

    /// Whether an expression's `$and` or `$or` may hold this error while its
    /// other arguments are evaluated, and drop it when one of them decides
    /// (ADR-211).
    ///
    /// Only an error that says *this value has no answer* may wait: a type
    /// the operator cannot take, a division by zero, a `$switch` with no
    /// match, a `$range` past its size. Its argument is then "not known",
    /// and a `false` beside it in an `$and` (a `true` in an `$or`) is the
    /// answer whatever it would have been. Every evaluation-time error is an
    /// [`Error::InvalidQuery`] of that kind.
    ///
    /// Anything that is about the *request* rather than one value — a
    /// deadline, a cancellation, a memory budget for the whole request — and
    /// a broken invariant ([`Error::Internal`]) must stop the evaluation at
    /// once: deferring it would let an argument that happens to decide hide
    /// it on some documents and not on others. **Exhaustive on purpose, with
    /// no wildcard**, like [`Error::is_a_request_refusal`]: the first such
    /// error added later will not compile until it is classified here, and
    /// it belongs on the `false` side. It must not be spelled as an
    /// `InvalidQuery`.
    pub fn is_deferrable(&self) -> bool {
        match self {
            Error::InvalidQuery(_) => true,

            Error::Internal(_)
            | Error::Limit(_)
            | Error::DatabaseNotFound(_)
            | Error::CollectionNotFound { .. }
            | Error::CollectionExists { .. }
            | Error::IndexExists { .. }
            | Error::DocumentNotFound(_)
            | Error::DuplicateKey(_)
            | Error::UniqueViolation { .. }
            | Error::Unsupported(_)
            | Error::InvalidDocumentId { .. }
            | Error::InvalidName { .. }
            | Error::InvalidUpdate(_)
            | Error::UnsupportedOperator { .. }
            | Error::ResumeTokenExpired
            | Error::MalformedResumeToken
            | Error::MalformedCursor
            | Error::MalformedStamp
            | Error::Bson(_)
            | Error::Serialization(_) => false,
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
    fn only_a_value_error_may_wait_for_another_argument_to_decide() {
        assert!(Error::InvalidQuery("$divide by zero".into()).is_deferrable());
        assert!(!Error::Internal("variable $$x is not bound".into()).is_deferrable());
        assert!(!Error::Internal("x".into()).is_a_request_refusal());
        // A budget for the whole request is never deferred, though it reads
        // and is refused like a bad query.
        let limit = Error::Limit("$group produced 3 documents".into());
        assert!(!limit.is_deferrable());
        assert!(limit.is_a_request_refusal());
        assert_eq!(limit.to_string(), "invalid query: $group produced 3 documents");
    }

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
