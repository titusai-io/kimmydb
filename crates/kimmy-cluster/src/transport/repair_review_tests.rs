//! Tests from the independent review of the repair release build (cluster half): the
//! alternation across a repair boundary, the loop guard's episodes and pruning, the left ids
//! of the next repair's line, the count gate at promotion, and the stop path under background
//! writers.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use kimmy_core::{CollectionId, DocId, NodeId, VersionVector};
use kimmy_storage::{Engine, WriterHolder};

use super::*;
use crate::repair_ledger::{End, Judged, Key, LedgerKind, MayPlan, Params, RepairLedger};

fn node(n: u128) -> NodeId {
    NodeId::from_bytes(n.to_be_bytes())
}

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

// -- Starvation: the alternation (design 2.6) ---------------------------------------------

/// Design 2.6: "While a Converge repair is active the two alternate, so it cannot starve that
/// peer's ordinary replication", and the test plan's "an ordinary pull at least every second
/// round". With several collections queued, a Converge that finishes in its round is followed
/// on the very next (resting) contact by the next queued Converge: `repair_due` only rests a
/// Converge that is still *active*, and promotes a queued one without looking at `resting`.
/// RED on the build: every contact is a Converge pull.
#[test]
fn a_queued_converge_is_not_handed_out_on_the_resting_contact() {
    let mut stalls = PeerStalls::new();
    stalls.set_repair_clock(secs(60), secs(300));
    let p = node(1);
    for c in 1..=4u64 {
        assert_eq!(
            stalls.plan_digest_repair(p, CollectionId(c), VersionVector::default()),
            DigestPlan::Planned
        );
    }
    let mut handed = Vec::new();
    for _ in 0..4 {
        stalls.tick_opened();
        let due = stalls.repair_due(p);
        handed.push(due.map(|(c, r)| (c.0, r)));
        if due.is_some() {
            // A small collection: the Converge pull completes in its one round.
            stalls.repair_finished(p);
        }
    }
    assert_eq!(
        handed,
        [Some((1, Repair::Converge)), None, Some((2, Repair::Converge)), None],
        "an ordinary pull at least every second contact"
    );
}

/// The same, end to end over the wire: two collections each with a lost update on B, both
/// planned. The rounds' `repairing` should alternate true/false; on the build the second
/// Converge runs on the contact right after the first.
/// RED on the build.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_small_converge_repairs_leave_no_ordinary_round_between_them() {
    const SECRET: &str = "review-rrb-alternation";
    let a_dir = tempfile::tempdir().unwrap();
    let a = Arc::new(Engine::open(&a_dir.path().join("kimmy.redb")).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(Arc::clone(&a), listener, SECRET.into()));
    let c1 = a.create_collection("shop", "c1").unwrap();
    let c2 = a.create_collection("shop", "c2").unwrap();
    for coll in [&c1, &c2] {
        a.insert(coll, bson::doc! { "_id": 1, "v": 1 }).unwrap();
    }
    let b_dir = tempfile::tempdir().unwrap();
    let b = Engine::open(&b_dir.path().join("kimmy.redb")).unwrap();
    let mut ordinary = PeerStalls::new();
    for _ in 0..3 {
        ordinary.tick_opened();
        sync_once_with(&b, addr, SECRET, None, &mut ordinary).await.unwrap();
    }
    std::thread::sleep(Duration::from_millis(5));
    for coll in [&c1, &c2] {
        a.replace(coll, &DocId::Int64(1), bson::doc! { "_id": 1, "v": 2 }, false).unwrap();
    }
    b.absorb_witnessed(&a.witnessed_vector().unwrap()).unwrap();

    let mut stalls = PeerStalls::new();
    stalls.set_repair_clock(secs(60), secs(300));
    assert_eq!(
        stalls.plan_digest_repair(a.node_id(), c1.id, VersionVector::default()),
        DigestPlan::Planned
    );
    assert_eq!(
        stalls.plan_digest_repair(a.node_id(), c2.id, VersionVector::default()),
        DigestPlan::Planned
    );
    let mut repairing = Vec::new();
    for _ in 0..6 {
        stalls.tick_opened();
        let outcome = sync_once_with(&b, addr, SECRET, None, &mut stalls).await.unwrap();
        repairing.push(outcome.repairing);
        if !stalls.repairing(a.node_id()) {
            break;
        }
    }
    assert!(!stalls.repairing(a.node_id()), "both repairs finished: {repairing:?}");
    assert_eq!(repairing, [true, false, true], "an ordinary round between two Converge rounds");
}

// -- Loops: the loop guard's R4 and R7 ------------------------------------------------------

/// R4's de-duplication window is three rotation periods, and the loop sets the rotation to
/// `sync_interval * held collections`. The build de-duplicates against the last counted
/// episode of *any* peer, the same peer's included (the design de-duplicates "episodes of
/// different peers"). On a member holding 100 collections at the default 5 s interval the
/// window is 25 minutes, so at most three episodes fit in a rolling hour and a loop that
/// clears and recurs every two minutes, from one peer, is never stopped.
/// RED on the build.
#[test]
fn r4_stops_a_one_peer_loop_on_a_member_with_many_collections() {
    let mut l = RepairLedger::new(Params { rotation: secs(5) * 100 });
    let k = Key { peer: node(1), collection: CollectionId(3), kind: LedgerKind::Digest };
    let mut stopped = false;
    let mut episodes = 0;
    let mut t = secs(0);
    // Two hours of: repair, judged effective, the bug re-diverges two minutes later.
    while t < secs(2 * 3600) {
        assert_eq!(l.may_plan(k, t), MayPlan::Yes, "at {t:?}");
        l.started(k, t);
        l.ended(k, t, End::Finished { progress: true });
        l.judge(k, t + secs(10), Judged::Effective);
        let c = l.confirmed(k, t + secs(120));
        episodes += usize::from(c.episode);
        stopped |= c.newly_stopped;
        if stopped {
            break;
        }
        t += secs(130);
    }
    assert!(stopped, "55 clear-and-recur cycles in two hours, {episodes} counted, never stopped");
}

/// The ledger's `armed` map and its per-collection R4 state carry no kind: a definition
/// finding that stands (a TTL index difference, which the repair never closes) consumes the
/// arm an effective *digest* repair set, in the same contact, and counts an episode with no
/// recurrence of the digest finding at all.
/// RED on the build.
#[test]
fn r4_an_effective_digest_repair_is_no_episode_of_a_standing_definition_finding() {
    let mut l = RepairLedger::new(Params { rotation: secs(5) });
    let digest = Key { peer: node(1), collection: CollectionId(3), kind: LedgerKind::Digest };
    let definition = Key { kind: LedgerKind::Definition, ..digest };
    l.started(digest, secs(0));
    l.ended(digest, secs(0), End::Finished { progress: true });
    l.judge(digest, secs(5), Judged::Effective);
    // The same contact's planner: the definition finding stands and is confirmed.
    let c = l.confirmed(definition, secs(5));
    assert!(!c.episode, "a digest repair that cleared its finding counted as a recurrence");
}

/// R7 prunes the ledger "by the cluster's member list", and the tick passes
/// `Members::node_ids()`, which is the *live* set: SWIM's `MemberDown` removes a member from
/// it (membership.rs `Collector::notify`). A peer down across one tick start (a restart, a
/// six-second cut) loses its whole ledger entry: the level, the back-off and the R4 arm. On
/// its return the standing finding re-confirms in two contacts and is repaired at level 0.
/// RED on the build.
#[test]
fn a_peer_down_for_one_tick_keeps_its_back_off() {
    let mut stalls = PeerStalls::new();
    let p = node(1);
    let c = CollectionId(3);
    let held = BTreeSet::from([c]);
    let mut t = secs(0);
    // Every tick lists the live members before it plans anything.
    stalls.set_repair_clock(t, secs(5));
    stalls.retain_ledger(&held, Some(&BTreeSet::from([p])));
    // Four ineffective repairs: level 4, backing off 2 h 15 min.
    for i in 0..4 {
        stalls.set_repair_clock(t, secs(5));
        assert_eq!(stalls.plan_digest_repair(p, c, VersionVector::default()), DigestPlan::Planned);
        // A Converge pull rests the contact after it: take the contacts it needs.
        let due = (0..3).find_map(|_| {
            stalls.tick_opened();
            stalls.repair_due(p)
        });
        assert_eq!(due, Some((c, Repair::Converge)));
        stalls.repair_finished(p);
        stalls.judge_digest_repair(p, c, Judged::Ineffective);
        stalls.retain_digest(p, &BTreeSet::new());
        if i < 3 {
            t += secs(3 * 3600);
        }
    }
    // An hour into the 2 h 15 min back-off.
    t += secs(3600);
    stalls.set_repair_clock(t, secs(5));
    assert!(matches!(
        stalls.plan_digest_repair(p, c, VersionVector::default()),
        DigestPlan::Held(MayPlan::BackOff(_))
    ));
    // The peer is marked down for one tick, then comes back.
    stalls.retain_ledger(&held, Some(&BTreeSet::new()));
    stalls.retain_ledger(&held, Some(&BTreeSet::from([p])));
    assert!(
        matches!(
            stalls.plan_digest_repair(p, c, VersionVector::default()),
            DigestPlan::Held(MayPlan::BackOff(_))
        ),
        "a peer that was down for one tick reset the loop guard"
    );
}

// -- The stop path (design 6 test 11, the standing rule for stop-path changes) -------------

/// What the background writers saw: how many writes, and the longest one.
struct Writers {
    stop: Arc<AtomicBool>,
    writes: Arc<AtomicU64>,
    longest_us: Arc<AtomicU64>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

fn background_writers(b: &Arc<Engine>, n: usize) -> Writers {
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicU64::new(0));
    let longest_us = Arc::new(AtomicU64::new(0));
    let coll = b.create_collection("shop", "writes").unwrap();
    let threads = (0..n)
        .map(|w| {
            let (b, stop, writes, longest_us, coll) = (
                Arc::clone(b),
                Arc::clone(&stop),
                Arc::clone(&writes),
                Arc::clone(&longest_us),
                coll.clone(),
            );
            std::thread::spawn(move || {
                let mut i = 0i64;
                while !stop.load(Ordering::SeqCst) {
                    let started = Instant::now();
                    if b.insert(&coll, bson::doc! { "_id": format!("{w}-{i}"), "v": i }).is_err() {
                        break;
                    }
                    let us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
                    longest_us.fetch_max(us, Ordering::SeqCst);
                    writes.fetch_add(1, Ordering::SeqCst);
                    i += 1;
                }
            })
        })
        .collect();
    Writers { stop, writes, longest_us, threads }
}

impl Writers {
    fn end(self) -> (u64, Duration) {
        self.stop.store(true, Ordering::SeqCst);
        for t in self.threads {
            t.join().unwrap();
        }
        (
            self.writes.load(Ordering::SeqCst),
            Duration::from_micros(self.longest_us.load(Ordering::SeqCst)),
        )
    }
}

/// A Converge repair of a collection of many pages, under background writers on the
/// requester, stopped part-way the way a node stops its replication loop: the walks are
/// stopped and the task aborted (`kimmy_task::supervised`). Timed: the task must end
/// promptly, no write transaction may be left held (a write right after the stop lands at
/// once), and the writers are never held for long by a page.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_mid_converge_with_background_writers_is_prompt_and_holds_nothing() {
    const SECRET: &str = "review-rrb-stop-converge";
    let a_dir = tempfile::tempdir().unwrap();
    let a = Arc::new(Engine::open(&a_dir.path().join("kimmy.redb")).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(Arc::clone(&a), listener, SECRET.into()));
    let orders = a.create_collection("shop", "orders").unwrap();
    let docs = 40 * kimmy_storage::SNAPSHOT_PAGE as i64;
    for chunk in (0..docs).collect::<Vec<_>>().chunks(2_000) {
        a.insert_many(&orders, chunk.iter().map(|i| bson::doc! { "_id": *i, "v": 1 }).collect())
            .unwrap();
    }
    let b_dir = tempfile::tempdir().unwrap();
    let b = Arc::new(Engine::open(&b_dir.path().join("kimmy.redb")).unwrap());
    let mut ordinary = PeerStalls::new();
    for _ in 0..80 {
        ordinary.tick_opened();
        let outcome = sync_once_with(&b, addr, SECRET, None, &mut ordinary).await.unwrap();
        if outcome.exhausted {
            break;
        }
    }
    std::thread::sleep(Duration::from_millis(5));
    for chunk in (0..docs).collect::<Vec<_>>().chunks(2_000) {
        a.write_batch(WriterHolder::Bulk, |scope| {
            for i in chunk {
                scope.replace(
                    &orders,
                    &DocId::Int64(*i),
                    bson::doc! { "_id": *i, "v": 2 },
                    false,
                )?;
            }
            Ok(())
        })
        .unwrap();
    }
    b.absorb_witnessed(&a.witnessed_vector().unwrap()).unwrap();
    let meta = b.get_collection("shop", "orders").unwrap();
    let version = |id: i64| b.get(&meta, &DocId::Int64(id)).unwrap().unwrap().get_i32("v").unwrap();
    assert_eq!(version(0), 1, "premise");

    let writers = background_writers(&b, 4);
    let mut stalls = PeerStalls::new();
    stalls.set_repair_clock(secs(60), secs(300));
    assert_eq!(
        stalls.plan_digest_repair(a.node_id(), orders.id, VersionVector::default()),
        DigestPlan::Planned
    );
    let looping = tokio::spawn({
        let b = Arc::clone(&b);
        async move {
            loop {
                stalls.tick_opened();
                let _ = sync_once_with(&b, addr, SECRET, None, &mut stalls).await;
            }
        }
    });
    // Wait until the repair is part-way: its first page landed, its last has not.
    let waited = Instant::now();
    while version(0) == 1 && waited.elapsed() < secs(60) {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(version(0), 2, "the repair never landed a page");
    assert_eq!(version(docs - 1), 1, "the repair finished before the stop; make it larger");

    let stopping = Instant::now();
    b.stop_walks();
    looping.abort();
    let _ = looping.await;
    let stopped_in = stopping.elapsed();
    let write = Instant::now();
    let probe = b.create_collection("shop", "after").unwrap();
    b.insert(&probe, bson::doc! { "_id": 1 }).unwrap();
    let write_after = write.elapsed();
    let (writes, longest) = writers.end();
    eprintln!(
        "review_rrb stop mid-converge: task ended {stopped_in:?} after the stop; a write after \
         it took {write_after:?}; background writers made {writes} writes, longest {longest:?}"
    );
    assert!(stopped_in < secs(2), "the stop waited {stopped_in:?}");
    assert!(write_after < secs(1), "a write after the stop waited {write_after:?}");
    assert!(writes > 0);
}

/// A definition repair building an index over a large collection, under background writers,
/// stopped part-way as a node stops: the build is a background walk, which ends at
/// `stop_walks`, so the stop is prompt and the writer is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_mid_definition_build_with_background_writers_is_prompt() {
    const SECRET: &str = "review-rrb-stop-definition";
    let a_dir = tempfile::tempdir().unwrap();
    let a = Arc::new(Engine::open(&a_dir.path().join("kimmy.redb")).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(Arc::clone(&a), listener, SECRET.into()));
    let orders = a.create_collection("shop", "orders").unwrap();
    let docs = 120_000i64;
    for chunk in (0..docs).collect::<Vec<_>>().chunks(5_000) {
        a.insert_many(&orders, chunk.iter().map(|i| bson::doc! { "_id": *i, "v": *i }).collect())
            .unwrap();
    }
    let b_dir = tempfile::tempdir().unwrap();
    let b = Arc::new(Engine::open(&b_dir.path().join("kimmy.redb")).unwrap());
    let mut ordinary = PeerStalls::new();
    for _ in 0..400 {
        ordinary.tick_opened();
        let outcome = sync_once_with(&b, addr, SECRET, None, &mut ordinary).await.unwrap();
        if outcome.exhausted {
            break;
        }
    }
    let meta = b.get_collection("shop", "orders").unwrap();
    assert_eq!(b.count_by_id(meta.id).unwrap(), Some(docs as u64), "premise");
    std::thread::sleep(Duration::from_millis(5));
    a.create_index(
        "shop",
        "orders",
        vec![kimmy_storage::IndexField::ascending("v")],
        false,
        Some("by_v".into()),
    )
    .unwrap();
    b.absorb_witnessed(&a.witnessed_vector().unwrap()).unwrap();

    let writers = background_writers(&b, 2);
    let mut stalls = PeerStalls::new();
    stalls.set_repair_clock(secs(60), secs(300));
    assert_eq!(
        stalls.plan_definition_repair(a.node_id(), orders.id, 1_000_000),
        DigestPlan::Planned
    );
    let started = Instant::now();
    let looping = tokio::spawn({
        let b = Arc::clone(&b);
        async move {
            loop {
                stalls.tick_opened();
                let _ = sync_once_with(&b, addr, SECRET, None, &mut stalls).await;
            }
        }
    });
    // The build is under way once the writer is held by an index build; give it time to
    // start, then stop.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let stopping = Instant::now();
    b.stop_walks();
    looping.abort();
    let _ = looping.await;
    let stopped_in = stopping.elapsed();
    let write = Instant::now();
    let probe = b.create_collection("shop", "after").unwrap();
    let wrote = b.insert(&probe, bson::doc! { "_id": 1 });
    let write_after = write.elapsed();
    let built = b.list_indexes("shop", "orders").unwrap().len();
    let (writes, longest) = writers.end();
    eprintln!(
        "review_rrb stop mid-definition-build: {:?} from the round's start to the stop; the \
         task ended {stopped_in:?} after it; index built: {built}; a write after it took \
         {write_after:?} ({}); writers made {writes} writes, longest {longest:?}",
        stopping - started,
        if wrote.is_ok() { "ok" } else { "refused" }
    );
    assert!(stopped_in < secs(2), "the stop waited {stopped_in:?} for the build");
    assert!(write_after < secs(1), "a write after the stop waited {write_after:?}");
}

// -- Observability: the end line of the next repair ----------------------------------------

/// `left_ids` (the first ten ids a document repair left, for its end line and WARN) is
/// cleared only in `repair_ended`. A repair that ends any other way (the `REPAIR_ATTEMPTS`
/// abandon in `repair_due`, `whole_database_takes_over`) leaves its ids behind, and the next
/// repair against the peer, of another collection, reports them as its own: and since only
/// ten are kept, its own are never named.
/// RED on the build.
#[test]
fn an_abandoned_repairs_left_ids_do_not_reach_the_next_repairs_line() {
    let mut stalls = PeerStalls::new();
    stalls.set_repair_clock(secs(60), secs(300));
    let p = node(1);
    let (c1, c2) = (CollectionId(1), CollectionId(2));
    assert_eq!(stalls.plan_digest_repair(p, c1, VersionVector::default()), DigestPlan::Planned);
    stalls.tick_opened();
    assert_eq!(stalls.repair_due(p), Some((c1, Repair::Converge)));
    let left_in_c1 = kimmy_storage::SnapshotApplied {
        left: 1,
        left_ids: vec![DocId::Int64(111)],
        ..Default::default()
    };
    stalls.converge_page_applied(p, &left_in_c1);
    stalls.snapshot_advanced(p);
    // Three Converge rounds that land nothing: abandoned.
    while stalls.repairing(p) {
        stalls.tick_opened();
        stalls.repair_due(p);
    }
    assert_eq!(stalls.plan_digest_repair(p, c2, VersionVector::default()), DigestPlan::Planned);
    loop {
        stalls.tick_opened();
        if stalls.repair_due(p).is_some() {
            break;
        }
    }
    assert_eq!(stalls.repairs.get(&p).unwrap().active, Some((c2, Repair::Converge)));
    assert!(
        stalls.repairs.get(&p).unwrap().left_ids.is_empty(),
        "c2's repair starts holding c1's left ids: {:?}",
        stalls.repairs.get(&p).unwrap().left_ids
    );
}

// -- The count gate at apply time ------------------------------------------------------------

/// Design 3: "A digest repair is not planned while a count repair is queued, active or
/// cooling for the collection". The gate is at plan time only: a Converge queued first, and a
/// count repair of the same collection planned after it (which the count half may do), runs
/// the count repair first (the key order) and then the Converge while the count repair cools.
/// RED on the build.
#[test]
fn a_queued_converge_does_not_run_while_the_count_repair_of_its_collection_cools() {
    let mut stalls = PeerStalls::new();
    stalls.set_repair_clock(secs(60), secs(300));
    let p = node(1);
    let c = CollectionId(7);
    assert_eq!(stalls.plan_digest_repair(p, c, VersionVector::default()), DigestPlan::Planned);
    assert!(stalls.plan_repair(p, c, Repair::Snapshot), "a count repair is not refused");
    stalls.tick_opened();
    assert_eq!(stalls.repair_due(p), Some((c, Repair::Snapshot)));
    stalls.repair_finished(p);
    assert!(stalls.count_repair_pending(p, c), "premise: the count repair is cooling");
    stalls.tick_opened();
    assert_eq!(stalls.repair_due(p), None, "a Converge ran beside a cooling count repair");
}

/// R7 with the grace: a member absent from the live list for less than `MEMBER_GRACE` keeps its
/// whole ledger entry; one absent for longer is removed, with its entry. The tracker is pruned
/// by the same returned set.
#[test]
fn a_member_is_removed_only_after_the_grace_and_the_tracker_follows() {
    let mut stalls = PeerStalls::new();
    let (p, q) = (node(1), node(2));
    let c = CollectionId(3);
    let held = BTreeSet::from([c]);
    stalls.set_repair_clock(secs(0), secs(5));
    stalls.retain_ledger(&held, Some(&BTreeSet::from([p, q])));
    for peer in [p, q] {
        assert_eq!(
            stalls.plan_digest_repair(peer, c, VersionVector::default()),
            DigestPlan::Planned
        );
    }
    // Only `q` is live from here on.
    let down = BTreeSet::from([q]);
    stalls.set_repair_clock(secs(60), secs(5));
    let known = stalls.retain_ledger(&held, Some(&down)).unwrap();
    assert!(known.contains(&p), "down for a minute is not removed");
    assert_eq!(
        stalls.plan_digest_repair(p, c, VersionVector::default()),
        DigestPlan::Held(MayPlan::Busy),
        "its queued repair and entry are kept"
    );
    stalls.set_repair_clock(MEMBER_GRACE + secs(120), secs(5));
    let known = stalls.retain_ledger(&held, Some(&down)).unwrap();
    assert!(!known.contains(&p) && known.contains(&q), "{known:?}");
    assert_eq!(
        stalls.plan_digest_repair(p, c, VersionVector::default()),
        DigestPlan::Planned,
        "removed beyond the grace: a fresh entry"
    );
    assert_eq!(stalls.retain_ledger(&held, None), None, "no member list, nothing judged");
}

/// A plan gate counts a standing finding once, not at every contact it stands; the count
/// starts again when the finding has gone.
#[test]
fn a_refused_plan_is_counted_once_per_standing_finding() {
    let mut stalls = PeerStalls::new();
    let (p, c) = (node(1), CollectionId(3));
    for _ in 0..5 {
        stalls.digest_repair_refused(p, c, DigestRepairOutcome::TooLarge);
        stalls.definition_repair_refused(p, c, DigestRepairOutcome::Disabled);
    }
    stalls.digest_repair_refused(p, c, DigestRepairOutcome::Disabled);
    let counts = stalls.take_repair_counts();
    assert_eq!(counts.digest_repairs[DigestRepairOutcome::TooLarge.index()], 1);
    assert_eq!(counts.digest_repairs[DigestRepairOutcome::Disabled.index()], 1, "its own label");
    assert_eq!(counts.definition_repairs[DigestRepairOutcome::Disabled.index()], 1);
    // The finding clears and comes back: refused (and counted) again.
    stalls.retain_digest(p, &BTreeSet::new());
    stalls.digest_repair_refused(p, c, DigestRepairOutcome::TooLarge);
    stalls.digest_repair_refused(p, c, DigestRepairOutcome::TooLarge);
    assert_eq!(
        stalls.take_repair_counts().digest_repairs[DigestRepairOutcome::TooLarge.index()],
        1
    );
    // And a count repair's skip is counted the same way.
    assert!(stalls.plan_repair(p, c, Repair::Snapshot));
    for _ in 0..3 {
        stalls.plan_digest_repair(p, c, VersionVector::default());
    }
    let skipped =
        stalls.take_repair_counts().digest_repairs[DigestRepairOutcome::SkippedCountRepair.index()];
    assert_eq!(skipped, 1);
}
