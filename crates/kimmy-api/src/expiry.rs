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
//! exactly as webhook subscriptions are (ADR-045, ADR-051), among the members
//! known to hold the collection's TTL index and able to expire (ADR-201). Once
//! the members' blocks have settled, one node expires a given collection and its
//! deletes replicate as ordinary deletes.
//!
//! Two consequences worth stating, because both are deliberate:
//!
//! - **Expiry is best-effort.** If the owner is stopped or partitioned, that
//!   collection stops expiring until membership changes and ownership moves.
//!   Documents live past their TTL. MongoDB's own TTL is a background pass on
//!   an interval with no stronger promise, and the alternative — every node
//!   expiring — trades a bounded delay for permanent write amplification.
//! - **A brief double-delete is possible** while membership is settling, since
//!   a node SWIM has declared dead still counts itself a candidate, and **for
//!   about one sync interval after a TTL index is created**: until the members
//!   have told each other they hold it, each holder counts only itself, so more
//!   than one holder may delete the same expired document (ADR-201). Two deletes
//!   of one document converge to the same tombstone under last-writer-wins, so
//!   this costs an extra oplog entry and nothing else.
//! - **Two double-deletes last as long as their cause.** A member whose sync
//!   contacts fail while SWIM keeps it up sees its peers' blocks go stale, counts
//!   only itself a holder, and owns every TTL collection it holds until its
//!   contacts recover. And a block lists at most 256 TTL collections
//!   (`kimmy_cluster::MAX_TTL_COLLECTIONS`), so for every collection past that
//!   each holder counts only itself and owns it, permanently: N deletes per
//!   document. Both show as `kimmy_ttl_collections{state="owned"}` summed across
//!   members exceeding one member's total.
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

use crate::ownership::{Owners, TtlState};
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
pub fn key(db: &str, collection: &str) -> String {
    format!("ttl\0{db}.{collection}")
}

/// Run one pass over every collection this node owns.
///
/// Returns the totals, for logging and for tests. Errors on one collection are
/// logged and the pass continues: a collection that cannot be scanned must not
/// stop every other collection from expiring.
pub fn pass(engine: &Engine, me: NodeId, members: &BTreeSet<NodeId>, now_ms: u64) -> ExpiryOutcome {
    pass_with(engine, &Owners::over_set(me, members.clone()), now_ms)
}

/// [`pass`], asking `owners` which collections this member may expire: the
/// members known to hold a collection's TTL index, not every live one
/// (ADR-201).
pub fn pass_with(engine: &Engine, owners: &Owners, now_ms: u64) -> ExpiryOutcome {
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
            // A collection with no TTL index has nothing to expire, whoever
            // owns it.
            if ttl_indexes(&coll).next().is_none() {
                continue;
            }
            // Checked before the scan, not after: a node that does not own a
            // collection should do no storage work for it at all.
            if !owners.owns_ttl(&coll) {
                continue;
            }

            for index in ttl_indexes(&coll) {
                match engine.expire_documents(&coll, index, now_ms) {
                    Ok(outcome) => {
                        if outcome.deleted > 0 || outcome.skipped > 0 || outcome.skipped_filter > 0
                        {
                            info!(
                                db = %coll.db,
                                collection = %coll.name,
                                index = %index.name,
                                deleted = outcome.deleted,
                                skipped = outcome.skipped,
                                skipped_filter = outcome.skipped_filter,
                                truncated = outcome.truncated,
                                "expired documents"
                            );
                        }
                        total.deleted += outcome.deleted;
                        total.skipped += outcome.skipped;
                        total.skipped_filter += outcome.skipped_filter;
                        total.truncated |= outcome.truncated;
                    }
                    // The node is stopping: the pass ends here, quietly, and
                    // the next start's pass finds what this one did not.
                    Err(kimmy_storage::StorageError::Stopping(reason)) => {
                        debug!(%reason, "expiry pass ended: this node is shutting down");
                        return total;
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
    let owners = Owners::over(me, members);
    let mut unowned = Unowned::default();
    let mut ticker = tokio::time::interval(interval);
    // The first tick fires immediately, which would expire during startup
    // before membership has formed — so a node that will not own a collection
    // once the cluster settles would expire it anyway. Skip it, exactly as the
    // retention collector does.
    ticker.tick().await;

    loop {
        ticker.tick().await;

        // Asked per pass rather than once: membership and what members say
        // change under us, and an ownership answer computed from a stale set is
        // how a collection ends up with no owner at all.

        // Off the async worker (ADR-199). The pass walks each owned TTL
        // index's expired range and deletes what it finds; inline, the worker
        // it held could not poll the runtime's I/O and timer driver, and the
        // stop signal reached the member owning the collections up to 0.74 s
        // late.
        let outcome =
            kimmy_storage::blocking(|| pass_with(&state.engine, &owners, physical_now_ms()));
        // A member with expiry on sees a collection unowned only while it is
        // itself catching up: otherwise it is a candidate for every collection
        // it holds. Checked on the pass all the same.
        unowned.check(&state, &owners, tokio::time::Instant::now());
        // Recorded even though zero-valued calls are common, because summing
        // this across a cluster is how "one document, one delete" stays a
        // measured property rather than a claim in a comment.
        state.metrics.record_expiry(outcome.deleted, outcome.skipped, outcome.skipped_filter);
        if outcome.truncated {
            debug!(
                deleted = outcome.deleted,
                "expiry pass hit its per-collection bound; the remainder drains next tick"
            );
        }
    }
}

/// How often a member with expiry switched off looks for collections nobody can
/// expire. The expiry pass's own default, so the two say it on the same cadence.
pub const UNOWNED_CHECK: Duration = DEFAULT_INTERVAL;

/// The least time between two warnings about the same collection.
pub const UNOWNED_WARN_EVERY: Duration = Duration::from_secs(30 * 60);

/// With expiry switched off here (`storage.ttl_interval_secs = 0`), what is
/// left of the expiry task: a check, every [`UNOWNED_CHECK`], that says which
/// collections this member holds a TTL index on and **no member can expire**
/// (ADR-201).
///
/// **Only a member that is not itself a candidate can see that.** A member with
/// expiry on that holds a collection's index is a candidate for it, so in its
/// own view the collection always has an owner; the member that can see nobody
/// is able is one with expiry off (or one that is catching up, which the pass
/// above checks). So without this, the members that could say it were exactly
/// the ones that never did.
pub async fn watch_unowned(
    state: SharedState,
    me: NodeId,
    members: Option<kimmy_cluster::Members>,
) {
    let owners = Owners::over(me, members).with_expiry_off(true);
    let mut unowned = Unowned::default();
    let mut ticker = tokio::time::interval(UNOWNED_CHECK);
    // As the pass does: not before membership has formed and the peers' blocks
    // have had a contact to arrive, or every collection would look unowned.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        unowned.check(&state, &owners, tokio::time::Instant::now());
    }
}

/// The collections nobody can expire, and when each was last said: a warning
/// per collection at most once per [`UNOWNED_WARN_EVERY`], by time rather than
/// by passes, so the rate does not follow `ttl_interval_secs`.
#[derive(Default)]
pub struct Unowned {
    warned: std::collections::HashMap<String, tokio::time::Instant>,
}

impl Unowned {
    /// Say, rate-limited per collection, that no member can expire a collection
    /// this member holds a TTL index on, and why (ADR-201). Silent expiry is the
    /// failure this whole design exists to make audible. The registry is read off
    /// the async worker.
    pub fn check(&mut self, state: &SharedState, owners: &Owners, now: tokio::time::Instant) {
        let collections = match kimmy_storage::blocking(|| state.engine.all_collections()) {
            Ok(collections) => collections,
            Err(e) => {
                debug!(error = %e, "could not list collections to check who owns their expiry");
                return;
            }
        };
        self.say(&owners.ttl_view(&collections), now);
    }

    /// Warn about each unowned collection in `view` that is due; answers how
    /// many were said.
    fn say(&mut self, view: &crate::ownership::TtlView, now: tokio::time::Instant) -> usize {
        let mut said = 0;
        // A collection that has an owner again is forgotten, so it is said at once
        // if it loses it again.
        self.warned.retain(|name, _| view.unowned.iter().any(|(unowned, _)| unowned == name));
        for (name, why) in &view.unowned {
            let due = self
                .warned
                .get(name)
                .is_none_or(|last| now.saturating_duration_since(*last) >= UNOWNED_WARN_EVERY);
            if !due {
                continue;
            }
            self.warned.insert(name.clone(), now);
            said += 1;
            match why {
                TtlState::UnownedCatchingUp => warn!(
                    collection = %name,
                    "no member can expire this collection: every member known to hold its TTL \
                     index is catching up, so expiry waits until one has caught up"
                ),
                _ => warn!(
                    collection = %name,
                    "no member can expire this collection: none is known to hold its TTL index \
                     and be able to expire (every holder has expiry switched off, or none is \
                     known)"
                ),
            }
        }
        said
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ownership;

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

    /// A collection nobody can expire is said at once, then not again for
    /// thirty minutes however often it is checked; one that finds an owner and
    /// loses it again is said at once.
    #[test]
    fn an_unowned_collection_is_said_once_per_thirty_minutes() {
        let t0 = tokio::time::Instant::now();
        let at = |secs: u64| t0 + Duration::from_secs(secs);
        let unowned = crate::ownership::TtlView {
            counts: [0, 0, 1, 0],
            unowned: vec![("app.sessions".into(), TtlState::UnownedNoHolder)],
        };
        let owned = crate::ownership::TtlView { counts: [1, 0, 0, 0], unowned: Vec::new() };
        let mut said = Unowned::default();
        assert_eq!(said.say(&unowned, at(0)), 1);
        for minute in 1..30 {
            assert_eq!(said.say(&unowned, at(minute * 60)), 0, "minute {minute}");
        }
        assert_eq!(said.say(&unowned, at(30 * 60)), 1, "due again after thirty minutes");
        assert_eq!(said.say(&owned, at(31 * 60)), 0);
        assert_eq!(said.say(&unowned, at(32 * 60)), 1, "owned in between, so said at once");
    }

    #[test]
    fn the_expiry_key_cannot_collide_with_a_webhook_subscription() {
        // Both hash into the same space through `ownership::owns`, and a
        // collision would tie a subscription's owner to a collection's.
        assert_ne!(key("shop", "sessions"), "shop.sessions");
        assert!(key("shop", "sessions").starts_with("ttl\0"));
    }
}
