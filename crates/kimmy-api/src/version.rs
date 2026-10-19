//! What this node is, and what it can do.
//!
//! # Why a capability list rather than a version number
//!
//! A client is handed one or more addresses and fails over between them.
//! During a rolling upgrade the node that answers the next request may be
//! older than the one that answered the last, so the question a
//! client actually has is not "what version is this" but **"does this node
//! have the thing I am about to use"**. A number answers that only if the
//! client also carries a table mapping versions to features, which is the
//! table this endpoint exists to replace.
//!
//! The build version is reported too, because it is what an operator needs
//! when a cluster is half-upgraded and something is behaving oddly.
//!
//! # Unauthenticated, deliberately
//!
//! A client has to be able to negotiate *before* it holds a token — whether a
//! refresh route exists is exactly the sort of thing it needs to know while
//! logging in. `/readyz` already discloses the node id without credentials,
//! and the build version is on every release artifact, so this adds no fact an
//! observer could not already have. It names no database, collection or user:
//! the rule from `docs/security.md` is that an unauthenticated endpoint must
//! not leak the *schema*, and this does not.

use axum::extract::State;
use axum::{Json, response::IntoResponse};
use serde_json::{Value, json};

use crate::state::{AppState, SharedState};

/// The protocol major version this build serves.
///
/// The path carries it — every route below `/v1/` — so this and the route
/// prefixes cannot disagree without the contract test noticing.
pub const PROTOCOL: &str = "v1";

/// A named, client-visible capability.
///
/// An enum for the same reason `ErrorCode` is one (ADR-057): the set is public
/// surface, and a list assembled by hand drifts from the thing it describes.
/// Adding a capability here is what makes a new feature discoverable rather
/// than something a client finds out by being refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Capability {
    Aggregation,
    BulkInsert,
    Backup,
    ChangeStreams,
    ClientSuppliedVectors,
    CursorPaging,
    FindAndModify,
    HybridSearch,
    PartialIndexes,
    TokenRefresh,
    Topology,
    TtlIndexes,
    VectorSearch,
    Webhooks,
    /// `if_stamp` on writes, `stamps` on `find`, `ETag` on a read by id, and
    /// the `stale` error code (ADR-084).
    ConditionalWrites,
    /// In-process embedding. **The one that varies between builds**, and the
    /// reason this list is not a constant: a `local` provider is accepted on a
    /// node built with `local-embeddings` and refused on one without.
    LocalEmbeddings,
    /// `Idempotency-Key` on the document writes, the `Idempotency-Replayed`
    /// marker and the two key codes (B5). **The one that varies at run time**:
    /// listed only while this member takes the header, which is while the
    /// operator's switch is set and every live member carries records. A
    /// client that read it and then meets the gate's `400` has met a roll, a
    /// rollback or a change of the switch in between.
    IdempotencyKeys,
}

impl Capability {
    pub const ALL: [Capability; 17] = [
        Self::Aggregation,
        Self::BulkInsert,
        Self::Backup,
        Self::ChangeStreams,
        Self::ClientSuppliedVectors,
        Self::CursorPaging,
        Self::FindAndModify,
        Self::HybridSearch,
        Self::PartialIndexes,
        Self::TokenRefresh,
        Self::Topology,
        Self::TtlIndexes,
        Self::VectorSearch,
        Self::Webhooks,
        Self::ConditionalWrites,
        Self::LocalEmbeddings,
        Self::IdempotencyKeys,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Aggregation => "aggregation",
            Self::BulkInsert => "bulk-insert",
            Self::Backup => "backup",
            Self::ChangeStreams => "change-streams",
            Self::ClientSuppliedVectors => "client-supplied-vectors",
            Self::CursorPaging => "cursor-paging",
            Self::FindAndModify => "find-and-modify",
            Self::HybridSearch => "hybrid-search",
            Self::PartialIndexes => "partial-indexes",
            Self::TokenRefresh => "token-refresh",
            Self::Topology => "topology",
            Self::TtlIndexes => "ttl-indexes",
            Self::VectorSearch => "vector-search",
            Self::Webhooks => "webhooks",
            Self::ConditionalWrites => "conditional-writes",
            Self::LocalEmbeddings => "local-embeddings",
            Self::IdempotencyKeys => "idempotency-keys",
        }
    }

    /// Whether *this* node has it, now.
    ///
    /// Constant for everything a build always carries, a question about the
    /// build for the one that is feature-gated, and a question about this
    /// moment for the one that is gated at run time. A capability whose answer
    /// is always `true` still belongs here: it is `true` on this build, and the
    /// client asking is one that may also be talking to a node from before it
    /// existed.
    pub fn present(self, state: &AppState) -> bool {
        match self {
            Self::LocalEmbeddings => kimmy_vector::local_embeddings_available(),
            Self::IdempotencyKeys => state.idempotency_gate().is_ok(),
            _ => true,
        }
    }
}

/// The capabilities this node actually has now, in a stable order.
pub fn capabilities(state: &AppState) -> Vec<&'static str> {
    Capability::ALL.iter().filter(|c| c.present(state)).map(|c| c.as_str()).collect()
}

/// What this node is, and what it can do.
pub async fn version(State(state): State<SharedState>) -> impl IntoResponse {
    Json(json!({
        "protocol": PROTOCOL,
        // From the crate at the root of the graph rather than this crate's
        // own `CARGO_PKG_VERSION`, so this answer, the startup log and
        // `kimmy --version` are one fact, not three that happen to agree.
        "version": kimmy_core::build::VERSION,
        // The exact build, for the operator staring at a half-upgraded
        // cluster where two nodes claim the same version. `unknown` on a
        // build made without git — a tarball build is a supported build.
        "commit": kimmy_core::build::COMMIT,
        // The node that answered. During a rolling upgrade a client that fails
        // over lands on different builds, and this is what tells it which one
        // this answer came from.
        "node": state.engine.node_id().to_string(),
        "capabilities": capabilities(&state),
        // A fact about this node's configuration rather than its build, so
        // a field rather than a capability: the capability set is closed by
        // an enum, and "durable" is not a feature a client uses (ADR-088).
        "durability": state.engine.durability().as_str(),
        // How long this node remembers an `Idempotency-Key`, in seconds:
        // `storage.oplog_retention_secs`, a field for the reason `durability`
        // is one. The draft's §2.5.2 asks a server to publish its expiry
        // policy, and members configured differently remember keys for
        // different times, so it is this node's (B5).
        "idempotencyWindowSecs": state.idempotency_window_secs(),
    })) as Json<Value>
}

#[cfg(test)]
mod tests {
    /// One workspace version, one binary story (ADR-062): the version this
    /// endpoint reports is the workspace's, not privately this crate's.
    #[test]
    fn the_reported_version_is_the_workspace_version() {
        assert_eq!(env!("CARGO_PKG_VERSION"), kimmy_core::build::VERSION);
    }

    /// `idempotency-keys` is listed exactly while the gate is open, read at
    /// each answer (B5), and appended last: a client that reads the list as
    /// ordered finds every earlier capability where it was.
    #[test]
    fn idempotency_keys_is_listed_only_while_the_gate_is_open() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::tests::a_state(&dir);
        assert!(super::capabilities(&state).contains(&"idempotency-keys"), "no gate: open");
        assert_eq!(super::Capability::ALL.last(), Some(&super::Capability::IdempotencyKeys));

        let open = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gate = std::sync::Arc::clone(&open);
        state.set_idempotency_gate(std::sync::Arc::new(move || {
            if gate.load(std::sync::atomic::Ordering::SeqCst) {
                Ok(())
            } else {
                Err(kimmy_cluster::IdempotencyClosed::Undeclared)
            }
        }));
        let closed = super::capabilities(&state);
        assert!(!closed.contains(&"idempotency-keys"), "{closed:?}");
        assert!(closed.contains(&"conditional-writes"), "and nothing else moves: {closed:?}");
        open.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(super::capabilities(&state).contains(&"idempotency-keys"), "read live");
    }

    /// Closed for any cause is not listed: a live member that says it does not
    /// carry records, and one not heard from yet, whose refusal is a transient
    /// `503`, as much as the switch.
    #[test]
    fn idempotency_keys_is_not_listed_while_a_member_is_unheard_or_says_no() {
        use kimmy_cluster::IdempotencyClosed;
        for closed in [
            IdempotencyClosed::Peers(vec!["peer-c".into()]),
            IdempotencyClosed::Unheard { peers: vec!["peer-c".into()], retry_after_secs: 1 },
            IdempotencyClosed::Unseen("membership is off"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let state = crate::state::tests::a_state(&dir);
            let answer = closed.clone();
            state.set_idempotency_gate(std::sync::Arc::new(move || Err(answer.clone())));
            let listed = super::capabilities(&state);
            assert!(!listed.contains(&"idempotency-keys"), "{closed:?}: {listed:?}");
        }
    }

    /// The window is this node's `storage.oplog_retention_secs`, its default
    /// until the daemon says otherwise.
    #[test]
    fn the_window_is_the_retention_the_node_was_given() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::tests::a_state(&dir);
        assert_eq!(state.idempotency_window_secs(), 86_400);
        state.set_idempotency_window_secs(3_600);
        assert_eq!(state.idempotency_window_secs(), 3_600);
    }
}
