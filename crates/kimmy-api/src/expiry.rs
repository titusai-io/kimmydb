//! The TTL expiry pass: which node expires a collection, and when.
//!
//! [`kimmy_storage::Engine::expire_documents`] decides *what* a pass removes.
//! This decides *whether this node should run one*, which is the half that
//! needs to know a cluster exists.
//!
//! # One owner per collection
//!
//! Every node runs its own timer, so if each expired independently one
//! document would produce N deletes — convergent under last-writer-wins, but
//! N-1 of them are superseded entries that still cost oplog space, replication
//! bandwidth and change-stream traffic. On a five-node cluster that is a 5×
//! amplification of a background workload nobody asked to pay for.
//!
//! So ownership is rendezvous-hashed per collection through [`crate::ownership`],
//! exactly as webhook subscriptions are (ADR-045, ADR-051). One node expires a
//! given collection and its deletes replicate as ordinary deletes.
//!
//! Two consequences worth stating, because both are deliberate:
//!
//! - **Expiry is best-effort.** If the owner is stopped or partitioned, that
//!   collection stops expiring until membership changes and ownership moves.
//!   Documents live past their TTL. MongoDB's own TTL is a background pass on
//!   an interval with no stronger promise, and the alternative — every node
//!   expiring — trades a bounded delay for permanent write amplification.
//! - **A brief double-delete is possible** while membership is settling, since
//!   a node SWIM has declared dead still counts itself a candidate. Two deletes
//!   of one document converge to the same tombstone under last-writer-wins, so
//!   this costs an extra oplog entry and nothing else.
//!
//! # An expiry is an ordinary delete
//!
//! Deliberately indistinguishable from a user delete on the wire and in a
//! change stream, which is also MongoDB's behaviour. `op_kind_from_tag` refuses
//! an unknown tag as *corruption*, so a dedicated `OpKind` would make every
//! upgrade a stop-the-cluster one, as ADR-040 and ADR-051 both were.

use std::collections::BTreeSet;
use std::time::Duration;

use kimmy_core::NodeId;
use kimmy_storage::{Engine, ExpiryOutcome, physical_now_ms, ttl_indexes};
use tracing::{debug, info, warn};

use crate::ownership;
use crate::state::SharedState;

/// How often a node looks for expired documents.
///
/// MongoDB uses sixty seconds and nothing here needs to be tighter: a TTL is a
/// retention policy, not a deadline. A shorter interval would multiply scans
/// across every collection with a policy for accuracy no caller can observe.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(60);

/// The ownership key for a collection's expiry.
///
/// `db.collection` rather than the numeric id: the id is derived from exactly
/// this string ([ADR-031](../../../docs/decisions.md)), so the two agree, and a
/// string is what [`ownership::owns`] hashes. Prefixed so an expiry key can
/// never collide with a webhook subscription id in the same hash space.
fn key(db: &str, collection: &str) -> String {
    format!("ttl\0{db}.{collection}")
}

/// Run one pass over every collection this node owns.
///
/// Returns the totals, for logging and for tests. Errors on one collection are
/// logged and the pass continues: a collection that cannot be scanned must not
/// stop every other collection from expiring.
pub fn pass(engine: &Engine, me: NodeId, members: &BTreeSet<NodeId>, now_ms: u64) -> ExpiryOutcome {
    let mut total = ExpiryOutcome::default();

    let databases = match engine.list_databases() {
        Ok(dbs) => dbs,
        Err(e) => {
            warn!(error = %e, "expiry pass could not list databases");
            return total;
        }
    };

    for db in databases {
        let collections = match engine.list_collections(&db.name) {
            Ok(cs) => cs,
            Err(e) => {
                warn!(db = %db.name, error = %e, "expiry pass could not list collections");
                continue;
            }
        };

        for coll in collections {
            // Checked before the scan, not after: a node that does not own a
            // collection should do no storage work for it at all.
            if !ownership::owns(&key(&coll.db, &coll.name), me, members) {
                continue;
            }

            for index in ttl_indexes(&coll) {
                match engine.expire_documents(&coll, index, now_ms) {
                    Ok(outcome) => {
                        if outcome.deleted > 0 || outcome.skipped > 0 {
                            info!(
                                db = %coll.db,
                                collection = %coll.name,
                                index = %index.name,
                                deleted = outcome.deleted,
                                skipped = outcome.skipped,
                                truncated = outcome.truncated,
                                "expired documents"
                            );
                        }
                        total.deleted += outcome.deleted;
                        total.skipped += outcome.skipped;
                        total.truncated |= outcome.truncated;
                    }
                    // Not fatal. The documents are still there and the next
                    // tick will find them, which is the same reasoning the
                    // retention collector uses.
                    Err(e) => warn!(
                        db = %coll.db,
                        collection = %coll.name,
                        index = %index.name,
                        error = %e,
                        "expiry pass failed for this index"
                    ),
                }
            }
        }
    }

    total
}

/// The expiry loop, run as a background task.
pub async fn run(
    state: SharedState,
    me: NodeId,
    members: Option<kimmy_cluster::Members>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    // The first tick fires immediately, which would expire during startup
    // before membership has formed — so a node that will not own a collection
    // once the cluster settles would expire it anyway. Skip it, exactly as the
    // retention collector does.
    ticker.tick().await;

    loop {
        ticker.tick().await;

        // Read per pass rather than once: membership changes under us, and an
        // ownership answer computed from a stale set is how a collection ends
        // up with no owner at all.
        let live: BTreeSet<NodeId> = members.as_ref().map(|m| m.node_ids()).unwrap_or_default();

        let outcome = pass(&state.engine, me, &live, physical_now_ms());
        // Recorded even though zero-valued calls are common, because summing
        // this across a cluster is how "one document, one delete" stays a
        // measured property rather than a claim in a comment.
        state.metrics.record_expiry(outcome.deleted, outcome.skipped);
        if outcome.truncated {
            debug!(
                deleted = outcome.deleted,
                "expiry pass hit its per-collection bound; the remainder drains next tick"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    #[test]
    fn exactly_one_node_owns_a_given_collection() {
        // The whole point of the ownership choice: one document, one delete.
        let members: BTreeSet<NodeId> = (1..=5).map(node).collect();
        let k = key("shop", "sessions");

        let owners: Vec<NodeId> =
            members.iter().copied().filter(|m| ownership::owns(&k, *m, &members)).collect();
        assert_eq!(owners.len(), 1, "one owner, or expiry amplifies: {owners:?}");
    }

    #[test]
    fn a_single_node_owns_every_collection() {
        // No cluster: the member set is empty and the union is just `me`, so
        // ownership needs no special case for the single-node deployment.
        let me = node(1);
        let none = BTreeSet::new();
        for name in ["a", "b", "sessions", "orders"] {
            assert!(ownership::owns(&key("shop", name), me, &none));
        }
    }

    #[test]
    fn collections_spread_across_the_cluster() {
        // Not a correctness requirement, but a policy pinned to one node would
        // make expiry a single-node workload on a cluster.
        let members: BTreeSet<NodeId> = (1..=3).map(node).collect();
        let mut seen = BTreeSet::new();
        for i in 0..50 {
            let k = key("shop", &format!("c{i}"));
            for m in &members {
                if ownership::owns(&k, *m, &members) {
                    seen.insert(*m);
                }
            }
        }
        assert!(seen.len() > 1, "every collection hashed to one node: {seen:?}");
    }

    #[test]
    fn the_expiry_key_cannot_collide_with_a_webhook_subscription() {
        // Both hash into the same space through `ownership::owns`, and a
        // collision would tie a subscription's owner to a collection's.
        assert_ne!(key("shop", "sessions"), "shop.sessions");
        assert!(key("shop", "sessions").starts_with("ttl\0"));
    }
}
