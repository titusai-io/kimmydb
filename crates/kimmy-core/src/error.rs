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

    /// A locally originated creation that would persist a `Decimal128` where
    /// a member older than 0.51 can meet it and cannot read it, refused
    /// (ADR-238): a row whose stored `_id` is a `Decimal128`, or an index whose
    /// partial filter holds one. Such a member fails every frame that carries
    /// the row and never catches up, and refuses such a definition for good,
    /// so the creation waits until the operator has declared that every
    /// member can hold one (`cluster.decimal128`) and every live peer says so.
    /// Replicated, restored and repaired writes are never refused by it.
    #[error("{}", decimal128_refusal(*what, closed))]
    Decimal128Gated { what: Decimal128Use, closed: Decimal128Closed },
}

/// What a [`Error::Decimal128Gated`] refusal would have persisted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decimal128Use {
    /// A row whose stored `_id` has BSON type `Decimal128`, an integral one
    /// (whose `DocId` is an `Int64`) included: the body keeps the decimal.
    Id,
    /// An index whose `partialFilterExpression` holds a `Decimal128` operand.
    PartialFilterBound,
}

/// Why the gate on a [`Decimal128Use`] is closed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decimal128Closed {
    /// `cluster.decimal128` is false: the operator has not declared that
    /// every member runs 0.51 and has migrated.
    Undeclared,
    /// The key is set, and these live peers (node ids, sorted) sent a block
    /// that does not say they can hold one: a build older than 0.51. It lasts
    /// until they are upgraded.
    Peers(Vec<String>),
    /// The key is set, and this member cannot see what its peers say about
    /// themselves, for the reason given.
    Unseen(&'static str),
    /// The key is set and no live peer's block says it cannot hold one, but
    /// these live peers (node ids, sorted) have not been heard from yet: they
    /// came up a moment ago, or this member dropped their block on a SWIM
    /// rename or rejoin. **Transient**: a block arrives with the next contact,
    /// about `retry_after_secs` (one sync interval) later, so the request is
    /// answered `503` with `Retry-After` and is worth sending again as it was.
    Unheard { peers: Vec<String>, retry_after_secs: u64 },
}

impl Decimal128Closed {
    /// The seconds to wait before sending the request again, for the one cause
    /// that ends by itself ([`Self::Unheard`]); `None` for a refusal that lasts
    /// until the operator or the roll changes something.
    pub fn retry_after_secs(&self) -> Option<u64> {
        match self {
            Self::Unheard { retry_after_secs, .. } => Some(*retry_after_secs),
            Self::Undeclared | Self::Peers(_) | Self::Unseen(_) => None,
        }
    }
}

fn decimal128_refusal(what: Decimal128Use, closed: &Decimal128Closed) -> String {
    let what = match what {
        Decimal128Use::Id => "a document whose _id is a Decimal128 cannot be created",
        Decimal128Use::PartialFilterBound => {
            "an index whose partialFilterExpression holds a Decimal128 cannot be created"
        }
    };
    match closed {
        Decimal128Closed::Undeclared => format!(
            "{what} until cluster.decimal128 is set. Set it only once every member runs 0.51 \
             or later and has finished its storage migration, as a configuration change of \
             its own and never in the change that upgrades the binary: a member older than \
             0.51 that meets one never catches up"
        ),
        Decimal128Closed::Peers(peers) => format!(
            "{what}: cluster.decimal128 is set, but the live member(s) {} say they cannot hold \
             one (a build older than 0.51); it is accepted once every live member can",
            peers.join(", ")
        ),
        Decimal128Closed::Unseen(why) => {
            format!("{what}: cluster.decimal128 is set, but {why}")
        }
        Decimal128Closed::Unheard { peers, .. } => format!(
            "{what} yet: cluster.decimal128 is set, but this member has not yet heard from the \
             live member(s) {} whether they can hold one (a member that came up or rejoined a \
             moment ago). This is transient: nothing was written; send the same request again \
             after Retry-After",
            peers.join(", ")
        ),
    }
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

            // Raised on locally originated creations only; replication never
            // asks the gate (ADR-238). Failing the round is the safe answer if
            // it ever did: skipping would lose the write for good.
            Error::Decimal128Gated { .. } => false,

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
            | Error::Decimal128Gated { .. }
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
    fn a_decimal128_refusal_names_the_key_and_the_peers() {
        let undeclared = Error::Decimal128Gated {
            what: Decimal128Use::Id,
            closed: Decimal128Closed::Undeclared,
        };
        let text = undeclared.to_string();
        assert!(
            text.contains("_id is a Decimal128") && text.contains("cluster.decimal128"),
            "{text}"
        );
        let peers = Error::Decimal128Gated {
            what: Decimal128Use::PartialFilterBound,
            closed: Decimal128Closed::Peers(vec!["peer-a".into(), "peer-b".into()]),
        };
        let text = peers.to_string();
        assert!(
            text.contains("partialFilterExpression")
                && text.contains("cluster.decimal128")
                && text.contains("peer-a, peer-b"),
            "{text}"
        );
        // Never skipped by replication, never set aside by an expression.
        assert!(!peers.is_a_request_refusal());
        assert!(!peers.is_deferrable());
    }

    /// A peer not heard from yet is its own cause: transient, with the wait it
    /// carries, and said so; the three others carry none.
    #[test]
    fn a_decimal128_refusal_for_an_unheard_peer_is_transient_and_says_so() {
        let unheard =
            Decimal128Closed::Unheard { peers: vec!["peer-c".into()], retry_after_secs: 3 };
        assert_eq!(unheard.retry_after_secs(), Some(3));
        for lasting in [
            Decimal128Closed::Undeclared,
            Decimal128Closed::Peers(vec!["peer-c".into()]),
            Decimal128Closed::Unseen("membership is off"),
        ] {
            assert_eq!(lasting.retry_after_secs(), None, "{lasting:?}");
        }
        let text = Error::Decimal128Gated { what: Decimal128Use::Id, closed: unheard }.to_string();
        assert!(
            text.contains("peer-c")
                && text.contains("not yet heard")
                && text.contains("transient")
                && text.contains("Retry-After"),
            "{text}"
        );
        let text = Error::Decimal128Gated {
            what: Decimal128Use::Id,
            closed: Decimal128Closed::Peers(vec!["peer-c".into()]),
        }
        .to_string();
        assert!(!text.contains("heard") && !text.contains("transient"), "{text}");
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
