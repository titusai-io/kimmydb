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
    use kimmy_storage::class_step;
    let mut total = ExpiryOutcome::default();
    // What this pass owned, and what it did with it: the class's heartbeat
    // (ADR-213). Every failure below used to be swallowed into a log line, so no
    // caller could see one; each is now counted, and a pass in which they rose and
    // no index succeeded is a bad cycle. One success masks failures in the same
    // pass: per-class yielding cannot fix a per-key fault, and the failures stay
    // visible in `kimmy_yield_faults_total{class="ttl",kind="local"}`.
    let mut owned: Vec<kimmy_core::CollectionId> = Vec::new();
    let mut succeeded = 0u64;
    // A pass that starts while this member is catching up owns nothing because it
    // knows nothing: its empty answer must not read as "owns nothing" later, when
    // ownership has reached the member and its stalled pass can no longer say so.
    let gated_at_start = owners.gated();

    let databases = match engine.list_databases() {
        Ok(dbs) => dbs,
        Err(e) => {
            warn!(error = %e, "expiry pass could not list databases");
            if !matches!(e, kimmy_storage::StorageError::Stopping(_)) {
                total.failed_lists += 1;
                class_step::local_fault();
                class_step::cycle(true);
            }
            return total;
        }
    };

    for db in databases {
        let collections = match engine.list_collections(&db.name) {
            Ok(cs) => cs,
            Err(e) => {
                warn!(db = %db.name, error = %e, "expiry pass could not list collections");
                if !matches!(e, kimmy_storage::StorageError::Stopping(_)) {
                    total.failed_lists += 1;
                    class_step::local_fault();
                }
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
            owned.push(coll.id);

            for index in ttl_indexes(&coll) {
                // The test switch fails the step here, **without calling the
                // backend**, so it can never trip ADR-188's latch.
                let expired = if class_step::test_fail_step(class_step::Class::Ttl) {
                    Err(kimmy_storage::StorageError::Transaction("test fail step".into()))
                } else {
                    engine.expire_documents(&coll, index, now_ms)
                };
                match expired {
                    Ok(outcome) => {
                        succeeded += 1;
                        class_step::ok();
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
                    Err(e) => {
                        total.failed_indexes += 1;
                        class_step::local_fault();
                        warn!(
                            db = %coll.db,
                            collection = %coll.name,
                            index = %index.name,
                            error = %e,
                            "expiry pass failed for this index"
                        );
                    }
                }
            }
        }
    }

    // The pass completed: what it owned, and whether it was a bad cycle.
    // Gated at its start or at its end, the pass did not see the whole answer, so
    // what it owned is unknown (`None`, as before the first pass) and the target
    // rule uses the holder list; an ungated pass that owned nothing owns nothing.
    let gated = gated_at_start || owners.gated();
    class_step::with_cell(|cell| {
        cell.set_owned(owned.len() as u64);
        if gated {
            cell.forget_owned_ttl();
        } else {
            cell.set_owned_ttl(owned);
        }
    });
    class_step::cycle(total.failed_indexes + total.failed_lists > 0 && succeeded == 0);
    total
}

/// The expiry loop, run as a background task.
///
/// `shutdown` is the stop's announcement, which a `KIMMY_TEST_KILL_TASK` stall
/// ends on. The class's heartbeat is the task-local its spawner set
/// (`kimmy_storage::class_step::scope`): `Waiting` while it waits for the next
/// tick, `Local` for the pass.
pub async fn run(
    state: SharedState,
    me: NodeId,
    members: Option<kimmy_cluster::Members>,
    interval: Duration,
    shutdown: kimmy_task::Shutdown,
) {
    use kimmy_storage::class_step::{self, Phase};
    let owners = Owners::over(me, members).gated_by(state.catch_up().cloned());
    let mut unowned = Unowned::default();
    let mut ticker = tokio::time::interval(interval);
    // The first tick fires immediately, which would expire during startup
    // before membership has formed — so a node that will not own a collection
    // once the cluster settles would expire it anyway. Skip it, exactly as the
    // retention collector does.
    class_step::phase(Phase::Waiting);
    ticker.tick().await;

    loop {
        // The stall point of the test switch: at the task's own `Waiting`
        // point, before the wait, so what the evaluator sees is a task that has
        // stopped beating in its waiting phase.
        class_step::phase(Phase::Waiting);
        kimmy_task::stall_point("ttl_expiry", &shutdown).await;
        ticker.tick().await;
        class_step::phase(Phase::Local);

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
        class_step::phase(Phase::Waiting);
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
    use kimmy_storage::class_step;
    let owners = Owners::over(me, members).with_expiry_off(true);
    let mut unowned = Unowned::default();
    let mut ticker = tokio::time::interval(UNOWNED_CHECK);
    // As the pass does: not before membership has formed and the peers' blocks
    // have had a contact to arrive, or every collection would look unowned.
    class_step::beat();
    ticker.tick().await;
    loop {
        // The class has no pass to beat here, so each check is the beat: the
        // `ttl_expiry` progress row reads this cell, and a member with expiry off
        // must not read as a writer that stopped (ADR-213).
        class_step::beat();
        ticker.tick().await;
        class_step::beat();
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
        // Kept across a spell with an owner, so a collection whose ownership
        // flaps is still said once per gap and not at every check; pruned once
        // the gap has passed, when it would be due anyway, so the map holds only
        // the collections said within the last gap.
        self.warned.retain(|_, last| now.saturating_duration_since(*last) < UNOWNED_WARN_EVERY);
        for (name, why) in &view.unowned {
            if self.warned.contains_key(name) {
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

    /// A TTL pass's heartbeat (ADR-213), read back from its cell: what it owned, a
    /// success per index, and the failures it used to swallow into a log line:
    /// counted, a pass of failures with no success a bad cycle, and a success in the
    /// same pass masking them. The failing step is the test switch's, which never
    /// calls the backend.
    #[tokio::test(flavor = "current_thread")]
    async fn a_pass_counts_what_it_owns_its_successes_and_the_failures_it_used_to_swallow() {
        use kimmy_storage::class_step::{self, ClassCell, MonotonicClock};
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let coll = engine.create_collection("app", "sessions").unwrap();
        engine
            .create_index_with(
                "app",
                "sessions",
                vec![kimmy_storage::IndexField::ascending("seen")],
                false,
                Default::default(),
                Some("ttl_seen".into()),
                Some(60),
                None,
            )
            .unwrap();
        let owners = Owners::over_set(node(1), BTreeSet::new());
        let cell = ClassCell::leak(std::sync::Arc::new(MonotonicClock::new()));
        let run = |fail: bool| {
            let (engine, owners) = (&engine, &owners);
            class_step::scope(cell, async move {
                kimmy_storage::blocking(|| {
                    class_step::test_fail_step_on_this_thread(class_step::Class::Ttl, fail);
                    let outcome = pass_with(engine, owners, 100_000);
                    class_step::test_fail_step_on_this_thread(class_step::Class::Ttl, false);
                    outcome
                })
            })
        };

        let outcome = run(true).await;
        assert_eq!(outcome.failed_indexes, 1, "the swallowed failure is counted");
        let r = cell.reading();
        assert_eq!((r.local_fault, r.local_ok), (1, 0), "{r:?}");
        assert_eq!(
            (r.cycles, r.cycles_bad, r.owned),
            (1, 1, 1),
            "a pass of failures is bad: {r:?}"
        );
        assert_eq!(
            cell.owned_ttl().unwrap().as_slice(),
            [coll.id],
            "what it owned, for the target rule"
        );

        let outcome = run(false).await;
        assert_eq!(outcome.failed_indexes, 0);
        let r = cell.reading();
        assert_eq!((r.local_fault, r.local_ok), (1, 1), "{r:?}");
        assert_eq!((r.cycles, r.cycles_bad), (2, 1), "a pass that succeeded is not bad: {r:?}");
    }

    /// What a pass records as owned, for the TTL target rule: a pass that ran while
    /// the member was catching up knows nothing and leaves `None` (the holder list
    /// then stands in, as before the first pass); an ungated pass that owned nothing
    /// leaves `Some([])`, which means the member owns nothing.
    #[tokio::test(flavor = "current_thread")]
    async fn a_gated_pass_records_nothing_owned_and_an_ungated_empty_one_records_none_owned() {
        use kimmy_cluster::CatchUpReason;
        use kimmy_cluster::catchup::CatchUp;
        use kimmy_storage::class_step::{self, ClassCell, MonotonicClock};
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let cell = ClassCell::leak(std::sync::Arc::new(MonotonicClock::new()));
        let pass_over = |owners: Owners| {
            let engine = &engine;
            class_step::scope(cell, async move {
                kimmy_storage::blocking(|| pass_with(engine, &owners, 100_000))
            })
        };

        // No collection with a TTL index: an ungated pass owns nothing, and says so.
        pass_over(Owners::over_set(node(1), BTreeSet::new())).await;
        assert_eq!(cell.owned_ttl().as_deref().map(Vec::as_slice), Some(&[][..]), "owns nothing");

        // The same member, its marker set: the pass knows nothing, so nothing is recorded.
        let marker = tempfile::tempdir().unwrap();
        let catch_up = CatchUp::open(marker.path(), std::time::Duration::from_secs(120));
        catch_up.mark(CatchUpReason::Restored).unwrap();
        let gated = Owners::over_set(node(1), BTreeSet::new()).gated_by(Some(catch_up));
        pass_over(gated).await;
        assert!(cell.owned_ttl().is_none(), "a gated pass is no knowledge");

        // Ungated again, the next pass records what it owns.
        pass_over(Owners::over_set(node(1), BTreeSet::new())).await;
        assert_eq!(cell.owned_ttl().as_deref().map(Vec::as_slice), Some(&[][..]));
    }

    /// The two reads of the catching-up marker each pin their own half: a pass that
    /// starts ungated and is gated by its end (the marker set mid-pass) and one that
    /// starts gated and is ungated by its end (the marker cleared mid-pass) both
    /// leave nothing owned, because in neither did the pass see one whole answer. A
    /// control pass with the marker never touched records what it owned.
    #[tokio::test(flavor = "current_thread")]
    async fn a_marker_that_changes_mid_pass_leaves_nothing_owned_either_way() {
        use kimmy_cluster::CatchUpReason;
        use kimmy_cluster::catchup::CatchUp;
        use kimmy_storage::class_step::{self, ClassCell, MonotonicClock};
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let coll = engine.create_collection("app", "sessions").unwrap();
        engine
            .create_index_with(
                "app",
                "sessions",
                vec![kimmy_storage::IndexField::ascending("seen")],
                false,
                Default::default(),
                Some("ttl_seen".into()),
                Some(60),
                None,
            )
            .unwrap();
        let cell = ClassCell::leak(std::sync::Arc::new(MonotonicClock::new()));
        let pass_over = |owners: Owners| {
            let engine = &engine;
            class_step::scope(cell, async move {
                kimmy_storage::blocking(|| pass_with(engine, &owners, 100_000))
            })
        };
        let marker = tempfile::tempdir().unwrap();
        let catch_up = CatchUp::open(marker.path(), std::time::Duration::from_secs(120));

        // Control: the marker never set, so the pass owns the collection and says so.
        let ungated = Owners::over_set(node(1), BTreeSet::new()).gated_by(Some(catch_up.clone()));
        pass_over(ungated).await;
        assert_eq!(cell.owned_ttl().unwrap().as_slice(), [coll.id], "control");

        // Set in the middle: ungated at the start, gated at the end.
        let flip = catch_up.clone();
        let sets = Owners::over_set(node(1), BTreeSet::new())
            .gated_by(Some(catch_up.clone()))
            .with_ask_hook(move || {
                if !flip.is_set() {
                    flip.mark(CatchUpReason::Restored).unwrap();
                }
            });
        pass_over(sets).await;
        assert!(catch_up.is_set(), "the hook set the marker during the pass");
        assert!(cell.owned_ttl().is_none(), "set mid-pass: the end read must forget");

        // The same marker, still set: gated at the start; the hook clears it in the
        // middle, so the pass ends ungated and owns the collection.
        let control = Owners::over_set(node(1), BTreeSet::new()).gated_by(Some(catch_up.clone()));
        pass_over(control).await;
        assert!(cell.owned_ttl().is_none(), "gated throughout");
        let flip = catch_up.clone();
        let clears = Owners::over_set(node(1), BTreeSet::new())
            .gated_by(Some(catch_up.clone()))
            .with_ask_hook(move || {
                if flip.is_set() {
                    flip.clear("test");
                }
            });
        // Seed a known list so a missing forget cannot hide as the same `None`.
        cell.set_owned_ttl(vec![coll.id]);
        pass_over(clears).await;
        assert!(!catch_up.is_set(), "the hook cleared the marker during the pass");
        assert!(cell.owned_ttl().is_none(), "cleared mid-pass: the start read must forget");
    }

    /// A pass that meets the node's stop is not a local fault
    /// (spec 6.3): it ends quietly, counts nothing and is no cycle. The control is
    /// the same pass with a document to remove before the stop, which succeeds.
    #[tokio::test(flavor = "current_thread")]
    async fn a_pass_the_stop_ends_counts_no_fault() {
        use kimmy_storage::class_step::{self, ClassCell, MonotonicClock};
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let coll = engine.create_collection("app", "sessions").unwrap();
        engine
            .create_index_with(
                "app",
                "sessions",
                vec![kimmy_storage::IndexField::ascending("seen")],
                false,
                Default::default(),
                Some("ttl_seen".into()),
                Some(60),
                None,
            )
            .unwrap();
        engine
            .insert(&coll, bson::doc! {"_id": 1, "seen": bson::DateTime::from_millis(0)})
            .unwrap();
        let owners = Owners::over_set(node(1), BTreeSet::new());
        let cell = ClassCell::leak(std::sync::Arc::new(MonotonicClock::new()));
        let run = || {
            let (engine, owners) = (&engine, &owners);
            class_step::scope(cell, async move {
                kimmy_storage::blocking(|| pass_with(engine, owners, 100_000))
            })
        };
        let before = run().await;
        assert_eq!(before.deleted, 1, "the control: a pass before the stop removes it");
        engine
            .insert(&coll, bson::doc! {"_id": 2, "seen": bson::DateTime::from_millis(0)})
            .unwrap();
        let faults = cell.reading().local_fault;
        engine.stop_walks();
        let during = run().await;
        let r = cell.reading();
        assert_eq!(during.failed_indexes + during.failed_lists, 0, "{during:?}");
        assert_eq!(r.local_fault, faults, "the stop is no local fault: {r:?}");
    }

    /// With expiry switched off the task is only the check for collections nobody
    /// can expire, and each check beats the class, or the `ttl_expiry` progress row
    /// would climb on a member that is doing exactly what it should.
    #[tokio::test(start_paused = true)]
    async fn the_expiry_off_watcher_beats_the_class_at_each_check() {
        use kimmy_storage::class_step::{self, ClassCell, StepClock};
        struct Counting(std::sync::atomic::AtomicU64);
        impl StepClock for Counting {
            fn now_ms(&self) -> u64 {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::tests::a_state(&dir);
        let clock = std::sync::Arc::new(Counting(Default::default()));
        let cell = ClassCell::leak(std::sync::Arc::clone(&clock) as std::sync::Arc<dyn StepClock>);
        let watcher = tokio::spawn(class_step::scope(
            cell,
            watch_unowned(std::sync::Arc::clone(&state), node(1), None),
        ));
        for _ in 0..4 {
            tokio::time::advance(UNOWNED_CHECK).await;
            tokio::task::yield_now().await;
        }
        let beats = clock.0.load(std::sync::atomic::Ordering::SeqCst);
        watcher.abort();
        assert!(beats >= 8, "a beat each side of every check, four checks: {beats}");
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
    /// thirty minutes however often it is checked, and however often it finds an
    /// owner and loses it again in between. What was said is forgotten once the
    /// gap has passed.
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
        // Flapping, owned one minute and not the next: still once per gap.
        for minute in 31..60 {
            let view = if minute % 2 == 1 { &owned } else { &unowned };
            assert_eq!(said.say(view, at(minute * 60)), 0, "minute {minute}");
        }
        assert_eq!(said.say(&unowned, at(60 * 60)), 1, "thirty minutes after the last");
        assert_eq!(said.say(&owned, at(90 * 60)), 0);
        assert!(said.warned.is_empty(), "forgotten once the gap has passed");
    }

    #[test]
    fn the_expiry_key_cannot_collide_with_a_webhook_subscription() {
        // Both hash into the same space through `ownership::owns`, and a
        // collision would tie a subscription's owner to a collection's.
        assert_ne!(key("shop", "sessions"), "shop.sessions");
        assert!(key("shop", "sessions").starts_with("ttl\0"));
    }
}
