//! Which nodes a client can talk to.
//!
//! # Two sources, because neither one answers the question alone
//!
//! **Addresses come from a replicated registry.** Each node writes one record
//! naming itself and the endpoint it advertises, into `__kimmy.__nodes`, and
//! replication carries it everywhere — the same reason the webhook registry is
//! an ordinary collection (ADR-045): it replicates, it is in a backup, it comes
//! back on restore, and it needs no second transport.
//!
//! The obvious alternative was to read addresses out of SWIM. It cannot work
//! without changing what SWIM carries: `Member` is `{addr, incarnation, node}`
//! where `addr` is the *gossip* address, and it is encoded with postcard, which
//! is not self-describing — a new field there is a stop-the-cluster upgrade
//! (ADR-040, ADR-051, ADR-053 were each one). Inferring a client address from a
//! gossip address is a guess that breaks on separate interfaces, TLS
//! termination, container networking and proxies. See ADR-060.
//!
//! **Liveness comes from SWIM**, which is what it is for. So a record says
//! *where*, and membership says *whether* — and an entry nobody can vouch for
//! is reported as `unknown` rather than hidden, because a node whose gossip is
//! partitioned while its HTTP is fine is a real state in a leaderless cluster,
//! and hiding it removes an option exactly when a client needs one.
//!
//! # The registry is an address book, not a heartbeat
//!
//! A node writes its record at startup and only when the content would change.
//! A periodic rewrite would put an entry in the oplog every tick forever, on
//! every node, for information that changes at restart — the failure mode the
//! webhook dispatcher's heartbeat exists to avoid. Freshness of *liveness* is
//! SWIM's job, and it is a better source for it than a timestamp could be.

use axum::extract::State;
use bson::{Document, doc};
use serde_json::{Value, json};
use tracing::{info, warn};

use crate::error::ApiError;
use crate::state::{Auth, SharedState};

/// System database and collection holding node records.
pub const NODES_DB: &str = "__kimmy";
pub const NODES_COLLECTION: &str = "__nodes";

/// Record this node in the registry, so clients can be told about it.
///
/// Idempotent, and **silent when nothing changed**: the record is read first
/// and rewritten only if the endpoint or the build differs. A node that
/// restarts twice an hour should not append to a log every other node then
/// replicates.
///
/// Called at startup with the endpoint an operator says clients should use. It
/// cannot be inferred: a node bound to `0.0.0.0` has no single address, and the
/// address clients reach may belong to a proxy or a service rather than to this
/// process at all.
pub fn register(state: &SharedState, endpoint: &str) -> Result<(), ApiError> {
    let meta = registry(state)?;
    let me = state.engine.node_id().to_string();
    let id = kimmy_core::DocId::String(me.clone());
    let version = env!("CARGO_PKG_VERSION");

    let existing = state.engine.get(&meta, &id)?;
    let unchanged = existing.as_ref().is_some_and(|d| {
        d.get_str("endpoint").is_ok_and(|e| e == endpoint)
            && d.get_str("version").is_ok_and(|v| v == version)
    });
    if unchanged {
        return Ok(());
    }

    let record = doc! {
        "_id": me.clone(),
        "endpoint": endpoint,
        "version": version,
        "updatedMs": kimmy_storage::physical_now_ms() as i64,
    };
    state.engine.replace(&meta, &id, record, true)?;
    info!(node = %me, endpoint, "registered this node in the client topology");
    Ok(())
}

/// The nodes a client may use.
///
/// Authenticated, unlike `/v1/version`: a version is a fact about software,
/// and this is a map of where a deployment's data lives. A client that wants
/// to fail over already holds a token, so nothing needs it earlier.
pub async fn topology(
    State(state): State<SharedState>,
    auth: Auth,
) -> Result<axum::Json<Value>, ApiError> {
    let _ = auth;
    let me = state.engine.node_id();
    // Peers this node currently believes are alive. `Members` holds **peers
    // only** — it can never contain `me` — so this node is added below rather
    // than looked for here. An owner computed over the set alone can never be
    // this node, which is the bug that silently undelivered every clustered
    // webhook (ADR-051).
    let live = state.members().map(|m| m.node_ids()).unwrap_or_default();

    let meta = registry(&state)?;
    let mut nodes = Vec::new();
    let mut me_seen = false;

    state.engine.for_each_doc(&meta, |id, document| {
        let node = id.to_string();
        let is_me = node == me.to_string();
        me_seen |= is_me;
        nodes.push(entry(&document, &node, is_me, is_me || contains(&live, &node)));
        Ok(true)
    })?;

    // This node answers, so it is reachable whatever the registry says. A node
    // with no advertised endpoint is still named, because a client that
    // recognizes it has already reached it and needs no address for it.
    if !me_seen {
        nodes.push(json!({
            "node": me.to_string(),
            "endpoint": Value::Null,
            "version": env!("CARGO_PKG_VERSION"),
            "status": "live",
            "self": true,
        }));
    }

    // Stable order, with this node first: a client reading the list top-down
    // should not be told to move away from the node already answering it.
    nodes.sort_by_key(|n| {
        (!n["self"].as_bool().unwrap_or(false), n["node"].as_str().unwrap_or("").to_string())
    });

    Ok(axum::Json(json!({ "nodes": nodes, "count": nodes.len() })))
}

fn entry(document: &Document, node: &str, is_me: bool, live: bool) -> Value {
    json!({
        "node": node,
        "endpoint": document.get_str("endpoint").ok(),
        "version": document.get_str("version").ok(),
        // `unknown` rather than `down`: this node has not heard from it, which
        // is not the same as it being unreachable from the client.
        "status": if live { "live" } else { "unknown" },
        "self": is_me,
    })
}

fn contains(live: &std::collections::BTreeSet<kimmy_core::NodeId>, node: &str) -> bool {
    live.iter().any(|n| n.to_string() == node)
}

/// Ensure the registry collection exists, and hand back its metadata.
///
/// `create_system_collection`, like the webhook registry: the `__` prefix is
/// reserved and the ordinary path refuses it (ADR-017). Created on first use,
/// so a node that never registers never grows the collection.
fn registry(state: &SharedState) -> Result<kimmy_storage::CollectionMeta, ApiError> {
    match state.engine.get_collection(NODES_DB, NODES_COLLECTION) {
        Ok(meta) => Ok(meta),
        Err(_) => Ok(state.engine.create_system_collection(NODES_DB, NODES_COLLECTION)?),
    }
}

/// The endpoint to advertise, given what an operator configured and where the
/// server is bound.
///
/// Returns `None` when there is nothing honest to say — a wildcard bind with no
/// configured advertisement — because a node that guesses its own address puts
/// a wrong one in front of every client in the cluster.
pub fn advertised_endpoint(
    configured: Option<&str>,
    bind: std::net::SocketAddr,
    tls: bool,
) -> Option<String> {
    if let Some(explicit) = configured {
        return Some(explicit.to_string());
    }
    if bind.ip().is_unspecified() {
        warn!(
            %bind,
            "no server.advertise is set and the bind address is a wildcard, so this node will \
             not appear in /v1/topology; set server.advertise to the URL clients should use"
        );
        return None;
    }
    let scheme = if tls { "https" } else { "http" };
    Some(format!("{scheme}://{bind}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configured_endpoint_is_used_as_given() {
        let bind = "0.0.0.0:7878".parse().unwrap();
        assert_eq!(
            advertised_endpoint(Some("https://db.example.com"), bind, false).as_deref(),
            Some("https://db.example.com"),
            "an operator's answer wins, because only they know about proxies"
        );
    }

    #[test]
    fn a_concrete_bind_can_speak_for_itself() {
        let bind = "10.0.0.5:7878".parse().unwrap();
        assert_eq!(advertised_endpoint(None, bind, false).as_deref(), Some("http://10.0.0.5:7878"));
        assert_eq!(
            advertised_endpoint(None, bind, true).as_deref(),
            Some("https://10.0.0.5:7878"),
            "the scheme follows whether this node terminates TLS"
        );
    }

    #[test]
    fn a_wildcard_bind_advertises_nothing() {
        // Guessing here would publish a wrong address to every client in the
        // cluster, which is worse than publishing none.
        for bind in ["0.0.0.0:7878", "[::]:7878"] {
            assert_eq!(advertised_endpoint(None, bind.parse().unwrap(), false), None);
        }
    }
}
