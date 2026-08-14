//! What this node is, and what it can do.
//!
//! # Why a capability list rather than a version number
//!
//! A client is handed one address and will, once task 5 lands, round-robin
//! across a cluster. During a rolling upgrade the node that answers the next
//! request may be older than the one that answered the last, so the question a
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

use crate::state::SharedState;

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
    TtlIndexes,
    VectorSearch,
    Webhooks,
    /// In-process embedding. **The one that varies between builds**, and the
    /// reason this list is not a constant: a `local` provider is accepted on a
    /// node built with `local-embeddings` and refused on one without.
    LocalEmbeddings,
}

impl Capability {
    pub const ALL: [Capability; 14] = [
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
        Self::TtlIndexes,
        Self::VectorSearch,
        Self::Webhooks,
        Self::LocalEmbeddings,
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
            Self::TtlIndexes => "ttl-indexes",
            Self::VectorSearch => "vector-search",
            Self::Webhooks => "webhooks",
            Self::LocalEmbeddings => "local-embeddings",
        }
    }

    /// Whether *this* node has it.
    ///
    /// Constant for everything a build always carries, and a real question for
    /// the one that is feature-gated. A capability whose answer is always
    /// `true` still belongs here: it is `true` on this build, and the client
    /// asking is one that may also be talking to a node from before it existed.
    pub fn present(self) -> bool {
        match self {
            Self::LocalEmbeddings => kimmy_vector::local_embeddings_available(),
            _ => true,
        }
    }
}

/// The capabilities this node actually has, in a stable order.
pub fn capabilities() -> Vec<&'static str> {
    Capability::ALL.iter().filter(|c| c.present()).map(|c| c.as_str()).collect()
}

/// What this node is, and what it can do.
pub async fn version(State(state): State<SharedState>) -> impl IntoResponse {
    Json(json!({
        "protocol": PROTOCOL,
        "version": env!("CARGO_PKG_VERSION"),
        // The node that answered. During a rolling upgrade a client may be
        // round-robining across builds, and this is what tells it which one
        // this answer came from.
        "node": state.engine.node_id().to_string(),
        "capabilities": capabilities(),
    })) as Json<Value>
}
