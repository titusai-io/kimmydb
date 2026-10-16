//! The count half's repair of a finding the gate can no longer re-examine.
//!
//! A count finding is confirmed by two compared probes that mismatch and cleared only by a
//! compared probe that matches (`DivergenceTracker::observe`). The gate defers the probe on
//! every contact with a peer that trails this member and moves (ADR-168), which is every
//! contact while this member takes writes. A finding confirmed while the peer stood still
//! therefore outlives the difference it reported once the peer moves again, and the loop
//! planned the same repair from it every `REPAIR_COOLDOWN_ROUNDS` contacts for as long as the
//! writes went on: a replay that lands nothing, and against a peer that holds its history only
//! as a snapshot's state (ADR-224), a whole-database snapshot.
//!
//! Real rounds over TLS between two real engines; only the loop around them is the test's.

use std::net::SocketAddr;
use std::sync::Arc;

use bson::doc;
use kimmy_core::{Hlc, NodeId};
use kimmy_storage::{DivergenceTracker, Engine, WalkScope};

use super::*;
use crate::catchup::CatchUp;
use crate::transport::{FROZEN_CONTACTS, REPAIR_COOLDOWN_ROUNDS, serve_with};

const SECRET: &str = "count-finding-secret";

struct Side {
    engine: Arc<Engine>,
    addr: SocketAddr,
    node: NodeId,
    stalls: PeerStalls,
    divergence: DivergenceTracker,
    members: Members,
    tick: u64,
    /// This side's count probes: compared, and deferred by the gate.
    compared: usize,
    deferred: usize,
    /// This side's catching-up marker, which the block it serves says (ADR-201). Its own
    /// rounds read it only once [`hold_own_marker`] hands it to them, as the loop does.
    catch_up: Arc<CatchUp>,
    own_marker: Option<Arc<CatchUp>>,
    _dir: tempfile::TempDir,
    _marker_dir: tempfile::TempDir,
}

async fn side() -> Side {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let marker_dir = tempfile::tempdir().unwrap();
    let catch_up = CatchUp::open(marker_dir.path(), Duration::from_secs(120));
    let members = Members::default();
    let said = Arc::clone(&catch_up);
    members.set_facts_source(Arc::new(move || crate::facts::Facts {
        boot: vec![1; 16],
        catching_up: said.is_set(),
        catching_up_reason: said.reason(),
        ..crate::facts::Facts::default()
    }));
    let tls = Arc::new(crate::tls::ClusterTls::new().unwrap());
    tokio::spawn(serve_with(
        Arc::clone(&engine),
        listener,
        SECRET.into(),
        None,
        None,
        None,
        tls,
        Some(members.clone()),
    ));
    Side {
        node: engine.node_id(),
        engine,
        addr,
        stalls: PeerStalls::new(),
        divergence: DivergenceTracker::new(),
        members,
        tick: 0,
        compared: 0,
        deferred: 0,
        catch_up,
        own_marker: None,
        _dir: dir,
        _marker_dir: marker_dir,
    }
}

/// Hand this side's marker to its own rounds, as the loop hands `config.catch_up` to them.
fn hold_own_marker(side: &mut Side) {
    side.own_marker = Some(Arc::clone(&side.catch_up));
    side.stalls.set_catch_up(side.own_marker.clone());
}

/// What the loop asks before it plans a repair from `node` (`CatchingUp::of`): this side's own
/// marker, when its rounds hold it, and the peer's, from the block this contact carried
/// (`said`) or the one membership holds for `node`.
fn catching_up(side: &Side, node: NodeId, said: bool) -> CatchingUp {
    let mut config = ReplicationConfig::new(Vec::new(), SECRET.into(), side.addr);
    config.members = Some(side.members.clone());
    config.catch_up = side.own_marker.clone();
    CatchingUp::of(&config, node, said)
}

fn pull(into: &Engine, from: &Engine) {
    let entries = from.entries_for_peer(Hlc::ZERO, 100_000, WalkScope::Background).unwrap().entries;
    into.apply_batch(&entries).unwrap();
}

fn live_count(side: &Side) -> Option<u64> {
    side.engine.count_by_id(kimmy_core::CollectionId::derive("shop", "live")).unwrap()
}

/// One tick of the loop's count half, in its order: the probe read once for the tick, each
/// listed peer contacted (drained while a pull is truncated), the contact folded in and the
/// repairs planned from it. Returns how many repairs the tick planned.
async fn tick(side: &mut Side) -> usize {
    side.tick += 1;
    let engine = Arc::clone(&side.engine);
    let held = engine.all_collection_ids().unwrap();
    let probe = advance_probe_on(&mut side.divergence, Ok::<_, ()>(held)).map(|id| {
        let (vector, mine_count) = engine.count_probe_reading(id).unwrap();
        DivergenceProbe { id, mine_count, mine_at: Some(vector) }
    });
    side.stalls.begin_tick(side.tick);
    side.stalls.set_members(Some(side.members.clone()));
    side.stalls.tick_opened();
    let mut planned = 0;
    for (addr, _) in side.members.entries() {
        side.stalls.allow_horizon_deferral(false);
        for _ in 0..8 {
            let mut outcome =
                sync_once_with(&engine, addr, SECRET, probe.clone(), &mut side.stalls)
                    .await
                    .unwrap();
            apply_facts_read(&side.members, &mut side.stalls);
            if outcome.truncated {
                continue;
            }
            if let Some(node) = outcome.peer
                && let Some(existence) = outcome.divergent.take()
            {
                let count = outcome.count_probe.take();
                side.compared += usize::from(count.is_some());
                side.deferred += usize::from(outcome.count_probe_deferred);
                fold_check(
                    &mut side.divergence,
                    node,
                    existence,
                    count,
                    outcome.count_probe_deferred,
                    probe.as_ref(),
                );
                let catching_up = catching_up(side, node, outcome.peer_catching_up);
                planned += plan_count_repairs(
                    &engine,
                    &side.divergence,
                    &mut side.stalls,
                    node,
                    addr,
                    catching_up,
                );
            }
            break;
        }
    }
    planned
}

/// A pair, `live` on both, and B standing still behind five writes of A's: B does not pull,
/// A contacts it every tick. After `FROZEN_CONTACTS` unchanged sightings A compares B's count,
/// confirms the difference on the next probe and plans one repair, which runs and lands
/// nothing (the difference is B's to take). Returns the repairs planned.
async fn a_still_peer_behind_five_writes(a: &mut Side, b: &mut Side) -> usize {
    a.members.insert_for_test(b.addr, b.node);
    b.members.insert_for_test(a.addr, a.node);
    let live = a.engine.create_collection("shop", "live").unwrap();
    pull(&b.engine, &a.engine);
    for i in 0..5i64 {
        a.engine.insert(&live, doc! { "_id": i }).unwrap();
    }
    let mut planned = 0;
    for _ in 0..FROZEN_CONTACTS + 4 {
        planned += tick(a).await;
    }
    planned
}

/// The loop of the whole-database storm, in two members: the finding was right when it was
/// confirmed (B was behind and not pulling), B then took what it lacked, and A's every later
/// probe of B was deferred because A kept writing. Nothing re-examined the finding, so A planned
/// the same repair every cooldown, for as long as the writes went on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_count_finding_nothing_has_compared_since_its_repair_is_not_repaired_again() {
    let (mut a, mut b) = (side().await, side().await);
    assert_eq!(
        a_still_peer_behind_five_writes(&mut a, &mut b).await,
        1,
        "premise: the still peer's count was compared, confirmed and repaired once"
    );
    // B pulls again and takes what it lacked.
    tick(&mut b).await;
    assert_eq!(live_count(&a), live_count(&b), "premise: B holds what A holds");
    // A takes a write every tick, before its own contact, so every contact finds B behind and
    // moving: the probe is deferred, every time.
    let live = a.engine.get_collection("shop", "live").unwrap();
    let mut again = 0;
    for i in 0..3 * i64::from(REPAIR_COOLDOWN_ROUNDS) {
        a.engine.insert(&live, doc! { "_id": 1_000 + i }).unwrap();
        again += tick(&mut a).await;
        tick(&mut b).await;
    }
    assert_eq!(live_count(&a), live_count(&b), "B holds what A holds: there was nothing to repair");
    assert_eq!(
        again, 0,
        "a count finding no probe compared since its repair was repaired {again} more times, \
         one per cooldown of {REPAIR_COOLDOWN_ROUNDS} contacts"
    );
    assert_eq!(
        a.divergence.confirmed_count(),
        1,
        "still reported while the writes go on: a deferred probe compared nothing"
    );
    // The writes stop, the next probe is compared, and it matches: the finding was stale.
    tick(&mut a).await;
    assert_eq!(a.divergence.confirmed_count(), 0, "the first compared probe clears it");
}

/// And a finding the probes keep renewing is still repaired once per cooldown, against a peer
/// that stops and moves in turn: B takes a write of A's before every fifth contact, so A's probe
/// is deferred while B moves and compared once B has stood still for `FROZEN_CONTACTS`
/// contacts. The difference is a document B witnessed and never applied, which no pull brings,
/// so the finding is real throughout. A suspended finding keeps the cooldown of the repair it
/// planned: forgetting that repair at the suspension would repair B every time it stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_count_finding_renewed_between_deferrals_is_still_repaired_once_per_cooldown() {
    let (mut a, mut b) = (side().await, side().await);
    a.members.insert_for_test(b.addr, b.node);
    b.members.insert_for_test(a.addr, a.node);
    let live = a.engine.create_collection("shop", "live").unwrap();
    pull(&b.engine, &a.engine);
    a.engine.insert(&live, doc! { "_id": "z" }).unwrap();
    b.engine.absorb_witnessed(&a.engine.witnessed_vector().unwrap()).unwrap();
    let mut planned = 0;
    let mut contacts = 0;
    let mut i = 0i64;
    while contacts < 2 * REPAIR_COOLDOWN_ROUNDS + 10 {
        // B moves: it takes a write of A's, and A writes again, so B trails A.
        a.engine.insert(&live, doc! { "_id": i }).unwrap();
        tick(&mut b).await;
        a.engine.insert(&live, doc! { "_id": -1 - i }).unwrap();
        i += 1;
        for _ in 0..FROZEN_CONTACTS + 2 {
            planned += tick(&mut a).await;
            contacts += 1;
        }
    }
    assert!(
        a.deferred > 0 && a.compared > 0,
        "premise: deferred {} compared {}",
        a.deferred,
        a.compared
    );
    tick(&mut b).await;
    assert_eq!(live_count(&a), live_count(&b).map(|n| n + 1), "premise: B lacks z, and only z");
    assert!(
        (2..=3).contains(&planned),
        "{planned} repairs in {contacts} contacts: one per cooldown of {REPAIR_COOLDOWN_ROUNDS} \
         contacts while the probes keep renewing the finding"
    );
    assert_eq!(a.divergence.confirmed_count(), 1, "and the finding stands");
}

/// One tick of A's loop with the cut digest folded in, in `replicate`'s order: the count
/// check (`fold_check`), then the cut digest (`fold_digest_round`, where F17 part 3 clears a
/// suspended count finding), then the count planner (`plan_count_repairs`).
async fn tick_with_digest(side: &mut Side, digest: &mut DigestState) -> usize {
    side.tick += 1;
    let engine = Arc::clone(&side.engine);
    let held = engine.all_collection_ids().unwrap();
    digest.prune(&held);
    let probe = advance_probe_on(&mut side.divergence, Ok::<_, ()>(held)).map(|id| {
        let (vector, mine_count) = engine.count_probe_reading(id).unwrap();
        DivergenceProbe { id, mine_count, mine_at: Some(vector) }
    });
    side.stalls.begin_tick(side.tick);
    side.stalls.set_members(Some(side.members.clone()));
    side.stalls.tick_opened();
    let mut planned = 0;
    for (addr, _) in side.members.entries() {
        side.stalls.allow_horizon_deferral(false);
        for _ in 0..8 {
            let mut outcome =
                sync_once_with(&engine, addr, SECRET, probe.clone(), &mut side.stalls)
                    .await
                    .unwrap();
            apply_facts_read(&side.members, &mut side.stalls);
            if outcome.truncated {
                continue;
            }
            if let Some(node) = outcome.peer
                && let Some(existence) = outcome.divergent.take()
            {
                let count = outcome.count_probe.take();
                side.compared += usize::from(count.is_some());
                side.deferred += usize::from(outcome.count_probe_deferred);
                fold_check(
                    &mut side.divergence,
                    node,
                    existence,
                    count,
                    outcome.count_probe_deferred,
                    probe.as_ref(),
                );
                if let Some(round) = outcome.digest.take() {
                    let mut report = RoundReport::default();
                    fold_digest_round(
                        &engine,
                        &mut side.divergence,
                        digest,
                        &mut side.stalls,
                        node,
                        addr,
                        round,
                        &mut report,
                    );
                }
                let catching_up = catching_up(side, node, outcome.peer_catching_up);
                planned += plan_count_repairs(
                    &engine,
                    &side.divergence,
                    &mut side.stalls,
                    node,
                    addr,
                    catching_up,
                );
            }
            break;
        }
    }
    planned
}

/// A peer that stops and moves in turn, B, trailing A only by A's latest write. Each time B
/// stands still for `FROZEN_CONTACTS` contacts A compares B's count, which is one short, and the
/// finding is confirmed and repaired; while B moves the probe is deferred and the finding
/// suspended, keeping its cooldown (ADR-236 item 10: "so a peer that stops and moves in turn is
/// not repaired every time it stops"). The cut digest of the same contact compares equal (A's
/// newest write is above the cut, so both sides set it aside) and quiets the suspended finding:
/// off the gauge, planning nothing, but still reported, so `retain_repaired` keeps the cooldown.
/// Expected: one repair per cooldown, as `a_count_finding_renewed_between_deferrals_...` pins
/// for a real difference.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_compared_equal_digest_keeps_the_count_cooldown_of_a_peer_that_stops_and_moves() {
    let (mut a, mut b) = (side().await, side().await);
    a.members.insert_for_test(b.addr, b.node);
    b.members.insert_for_test(a.addr, a.node);
    let live = a.engine.create_collection("shop", "live").unwrap();
    pull(&b.engine, &a.engine);
    let mut digest = DigestState::default();
    let mut planned = 0;
    let mut contacts = 0;
    let mut i = 0i64;
    while contacts < 2 * REPAIR_COOLDOWN_ROUNDS + 10 {
        // B moves: it takes A's writes so far, and A writes again, so B trails A by one.
        a.engine.insert(&live, doc! { "_id": i }).unwrap();
        tick(&mut b).await;
        a.engine.insert(&live, doc! { "_id": -1 - i }).unwrap();
        i += 1;
        for _ in 0..FROZEN_CONTACTS + 2 {
            planned += tick_with_digest(&mut a, &mut digest).await;
            contacts += 1;
        }
    }
    assert!(
        a.deferred > 0 && a.compared > 0,
        "premise: deferred {} compared {}",
        a.deferred,
        a.compared
    );
    tick(&mut b).await;
    assert_eq!(live_count(&a), live_count(&b), "premise: B only trailed; it holds what A holds");
    assert!(
        planned <= 3,
        "{planned} count repairs in {contacts} contacts against a peer that only trails: the \
         compared-equal digest cleared the suspended finding and with it the cooldown, so every \
         stop of the peer was repaired (expected one per cooldown of {REPAIR_COOLDOWN_ROUNDS})"
    );
}

/// A quieted finding is not held off for good. B stops and moves in turn,
/// trailing A only, until A's finding is quieted (off the gauge, still reported); then B
/// witnesses an insert of A's without applying it (a real difference) and stands still for
/// good. A must confirm the finding again and repair it within a cooldown, and the gauge must
/// light.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quieted_finding_is_repaired_again_once_the_peer_stands_with_a_real_difference() {
    let (mut a, mut b) = (side().await, side().await);
    a.members.insert_for_test(b.addr, b.node);
    b.members.insert_for_test(a.addr, a.node);
    let live = a.engine.create_collection("shop", "live").unwrap();
    let id = kimmy_core::CollectionId::derive("shop", "live");
    pull(&b.engine, &a.engine);
    let mut digest = DigestState::default();
    let mut i = 0i64;
    for _ in 0..4 {
        a.engine.insert(&live, doc! { "_id": i }).unwrap();
        tick(&mut b).await;
        a.engine.insert(&live, doc! { "_id": -1 - i }).unwrap();
        i += 1;
        for _ in 0..FROZEN_CONTACTS + 2 {
            tick_with_digest(&mut a, &mut digest).await;
        }
    }
    // One more move, and one contact: the probe is deferred and the digest quiets the finding.
    a.engine.insert(&live, doc! { "_id": i }).unwrap();
    tick(&mut b).await;
    a.engine.insert(&live, doc! { "_id": -1 - i }).unwrap();
    tick_with_digest(&mut a, &mut digest).await;
    assert_eq!(a.divergence.confirmed_count(), 0, "premise: quieted, off the gauge");
    assert!(a.divergence.reported_against(b.node).contains(&id), "premise: still reported");

    // A real difference: B witnesses z and never applies it, and stands still from now on.
    a.engine.insert(&live, doc! { "_id": "z" }).unwrap();
    b.engine.absorb_witnessed(&a.engine.witnessed_vector().unwrap()).unwrap();
    let mut planned = 0;
    for _ in 0..REPAIR_COOLDOWN_ROUNDS + FROZEN_CONTACTS + 5 {
        planned += tick_with_digest(&mut a, &mut digest).await;
    }
    assert!(a.divergence.confirmed_against(b.node).contains(&id), "confirmed again");
    assert!(a.divergence.confirmed_count() >= 1, "the gauge is lit");
    assert!(planned >= 1, "repaired again within a cooldown, not held off: {planned}");
}

// A member catching up (ADR-237's addendum): its count is about to change however still its
// vector stands, so neither side of a contact with it compares a count or plans a repair.

/// How long a member's served block may be reused before it is built again (`facts`'
/// `REBUILD_AFTER`), and a margin: a marker cleared now is in its block after this.
const BLOCK_REBUILT: Duration = Duration::from_millis(1_200);

/// B fresh, with the first page of A's whole-database snapshot applied and the rest to come, as
/// a member behind A's retention horizon is while it takes the snapshot: it holds part of A's
/// documents as state its vector does not name, its vector does not move until the last page
/// lands, and its marker is set. B does not pull, so it stands still behind A.
fn b_part_way_through_a_snapshot_of(a: &Side, b: &Side) {
    a.members.insert_for_test(b.addr, b.node);
    b.members.insert_for_test(a.addr, a.node);
    let live = a.engine.create_collection("shop", "live").unwrap();
    let docs: Vec<_> =
        (0..kimmy_storage::SNAPSHOT_PAGE as i64 + 20).map(|i| doc! { "_id": i }).collect();
    a.engine.insert_many(&live, docs).unwrap();
    let mut progress = kimmy_storage::SnapshotProgress::whole_database();
    let page = a.engine.snapshot_page(None, None).unwrap();
    b.engine.apply_snapshot_page(a.node, &mut progress, &page).unwrap();
    assert!(!progress.is_complete(), "premise: part-way through the snapshot");
    b.catch_up.mark(crate::CatchUpReason::Snapshot).unwrap();
    assert!(live_count(b) < live_count(a), "premise: B holds part of what A holds");
}

/// Lab row 7 of 0.50.0: B, part-way through a whole-database snapshot, stood still behind A and
/// C, which read it as frozen after `FROZEN_CONTACTS` contacts, compared its part-way count,
/// confirmed a divergence and pulled a repair from it. A member whose snapshot is in progress is
/// probed and nothing comes of it: no count compared, no finding, no repair.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_member_part_way_through_a_snapshot_is_probed_without_a_finding_or_a_repair() {
    let (mut a, b) = (side().await, side().await);
    b_part_way_through_a_snapshot_of(&a, &b);
    let mut planned = 0;
    for _ in 0..2 * FROZEN_CONTACTS + 4 {
        planned += tick(&mut a).await;
    }
    assert!(a.deferred > 0, "premise: the rotation named the collection and the check ran");
    assert_eq!(a.compared, 0, "a part-way count was compared ({} deferred)", a.deferred);
    assert_eq!(a.divergence.confirmed_count(), 0, "a finding against a member catching up");
    assert!(a.divergence.reported_against(b.node).is_empty(), "nor one standing suspended");
    assert_eq!(planned, 0, "a repair planned from a member catching up");
}

/// The check that follows a pull defers too. A pulls a write from B on every contact, and its
/// probe carries no reading of its own vector (`mine_at`), so B's side alone judges the position,
/// as on a contact that ran a repair: B stands still behind A and is catching up, so the probe
/// is deferred, and once B's marker clears it is compared.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_check_after_a_pull_defers_against_a_member_catching_up() {
    let (a, b) = (side().await, side().await);
    b_part_way_through_a_snapshot_of(&a, &b);
    let own = b.engine.create_collection("shop", "own").unwrap();
    let live = kimmy_core::CollectionId::derive("shop", "live");
    let mut stalls = PeerStalls::new();
    async fn contact(
        a: &Side,
        b: &Side,
        own: &kimmy_storage::CollectionMeta,
        i: i64,
        stalls: &mut PeerStalls,
    ) -> kimmy_storage::SyncOutcome {
        b.engine.insert(own, doc! { "_id": i }).unwrap();
        stalls.tick_opened();
        let live = kimmy_core::CollectionId::derive("shop", "live");
        let mine_count = a.engine.count_by_id(live).unwrap();
        let probe = DivergenceProbe { id: live, mine_count, mine_at: None };
        sync_once_with(&a.engine, b.addr, SECRET, Some(probe), stalls).await.unwrap()
    }
    for i in 0..2 * i64::from(FROZEN_CONTACTS) + 2 {
        let outcome = contact(&a, &b, &own, i, &mut stalls).await;
        assert!(outcome.applied > 0 && outcome.exhausted, "premise {i}: a pull, then the check");
        assert_eq!(outcome.count_probe, None, "contact {i}: compared a member catching up");
        assert!(outcome.count_probe_deferred, "contact {i}: and counted deferred");
        assert!(outcome.peer_catching_up, "contact {i}: and the round says why");
    }
    b.catch_up.clear("the test");
    tokio::time::sleep(BLOCK_REBUILT).await;
    let outcome = contact(&a, &b, &own, 1_000, &mut stalls).await;
    assert_eq!(outcome.count_probe, Some((live, true)), "B still, marker clear: compared");
    assert!(!outcome.peer_catching_up, "and the round says B is not catching up");
}

/// The deferral ends with the marker. B's snapshot stops where it stood and its marker clears,
/// so B now holds part of A's documents for good: a real divergence, which A compares, confirms
/// and plans the repair of, as for any still peer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_real_count_divergence_is_found_and_repaired_once_the_marker_clears() {
    let (mut a, b) = (side().await, side().await);
    b_part_way_through_a_snapshot_of(&a, &b);
    for _ in 0..FROZEN_CONTACTS + 2 {
        assert_eq!(tick(&mut a).await, 0, "premise: nothing planned while B catches up");
    }
    assert_eq!(a.compared, 0, "premise: deferred while B catches up");

    b.catch_up.clear("the test");
    tokio::time::sleep(BLOCK_REBUILT).await;
    let mut planned = 0;
    for _ in 0..FROZEN_CONTACTS + 2 {
        planned += tick(&mut a).await;
    }
    let live = kimmy_core::CollectionId::derive("shop", "live");
    assert!(a.compared >= 2, "compared once the marker cleared: {}", a.compared);
    assert!(a.divergence.reported_against(b.node).contains(&live), "the finding is confirmed");
    assert_eq!(planned, 1, "and repaired from B once, within the cooldown");
}

/// The other side of the contact: A is the member catching up, and B stands still behind it with
/// fewer documents. A's own count is the one about to change, so it compares nothing against B
/// and plans nothing from it; the same pair with no marker is compared and repaired once
/// (`a_count_finding_nothing_has_compared_since_its_repair_is_not_repaired_again`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_member_catching_up_compares_no_count_and_plans_no_repair_itself() {
    let (mut a, mut b) = (side().await, side().await);
    hold_own_marker(&mut a);
    a.catch_up.mark(crate::CatchUpReason::Snapshot).unwrap();
    assert_eq!(a_still_peer_behind_five_writes(&mut a, &mut b).await, 0, "a repair was planned");
    assert!(a.deferred > 0, "premise: the check ran");
    assert_eq!(a.compared, 0, "a count compared by a member catching up");
    assert_eq!(a.divergence.confirmed_count(), 0, "a finding by a member catching up");
}

/// The repair's own gate, in the loop itself (`replicate`): B holds a collection A does not,
/// only as state part of a snapshot gave it (no entry of it that a pull could bring), and its
/// marker is set. The existence half finds the collection on B, and nothing is pulled from B
/// while it is catching up; once its marker clears, the collection is repaired from B. Both with
/// membership, which holds B's block, and without it (static seeds), where the block this
/// contact carried is all the loop has to go on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_loop_repairs_nothing_from_a_peer_until_its_marker_clears() {
    for membership in [true, false] {
        let (a, b) = (side().await, side().await);
        a.members.insert_for_test(b.addr, b.node);
        let _source = b_holds_part_of_a_snapshot_of_extra(&b);
        b.catch_up.mark(crate::CatchUpReason::Snapshot).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
        let seeds = vec![SeedSource::Static(vec![b.addr])];
        let mut config = ReplicationConfig::new(seeds, SECRET.into(), a.addr);
        config.sync_interval = Duration::from_millis(100);
        config.discovery_interval = Duration::from_millis(100);
        config.members = membership.then(|| a.members.clone());
        config.on_round = Some(Arc::new(move |report| {
            let _ = tx.send(report);
        }));
        let looping = tokio::spawn(replicate(Arc::clone(&a.engine), config));
        let held = || a.engine.get_collection("shop", "extra").is_ok();

        let (mut checks, mut repairs) = (0, 0);
        while checks < 10 {
            let Ok(Some(report)) = tokio::time::timeout(Duration::from_secs(10), rx.recv()).await
            else {
                break;
            };
            checks += report.divergence_checks;
            repairs += report.repair_rounds;
        }
        assert!(checks >= 10, "premise (membership {membership}): A checked B: {checks}");
        assert_eq!(
            repairs, 0,
            "membership {membership}: a repair round against a peer catching up"
        );
        assert!(!held(), "membership {membership}: a collection repaired from a peer catching up");

        b.catch_up.clear("the test");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        while !held() && tokio::time::Instant::now() < deadline {
            let _ = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await;
        }
        looping.abort();
        assert!(held(), "membership {membership}: repaired from B once its marker cleared");
    }
}

/// B holds a collection A does not, only as the state part of a snapshot gave it, and its marker
/// is set. A's membership still lists B's address under the id B had before it was wiped, as
/// SWIM does until the new id is gossiped, so what membership holds says nothing of B; the
/// block B sent on this contact says it is catching up, and nothing is planned from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_listed_under_another_id_is_not_repaired_from_while_its_block_says_it_catches_up() {
    let (mut a, b) = (side().await, side().await);
    a.members.insert_for_test(b.addr, NodeId::from_bytes([7; 16]));
    b_holds_part_of_a_snapshot_of_extra(&b);
    b.catch_up.mark(crate::CatchUpReason::Snapshot).unwrap();
    let mut planned = 0;
    for _ in 0..4 {
        planned += tick(&mut a).await;
    }
    assert!(b.catch_up.is_set(), "premise: B is still catching up");
    let held = a.engine.get_collection("shop", "extra").is_ok();
    assert!(
        planned == 0 && !held,
        "repaired from a member whose block on this contact says it is catching up: \
         {planned} planned, collection taken: {held}"
    );
}

/// B, as the state part of a snapshot gave it: a collection, `extra`, with no entry of it that a
/// pull could bring.
fn b_holds_part_of_a_snapshot_of_extra(b: &Side) -> tempfile::TempDir {
    let source_dir = tempfile::tempdir().unwrap();
    let source = Engine::open(&source_dir.path().join("kimmy.redb")).unwrap();
    let extra = source.create_collection("shop", "extra").unwrap();
    let docs: Vec<_> =
        (0..kimmy_storage::SNAPSHOT_PAGE as i64 + 20).map(|i| doc! { "_id": i }).collect();
    source.insert_many(&extra, docs).unwrap();
    let mut progress = kimmy_storage::SnapshotProgress::whole_database();
    let page = source.snapshot_page(None, None).unwrap();
    b.engine.apply_snapshot_page(source.node_id(), &mut progress, &page).unwrap();
    source_dir
}

/// A count repair planned from B while B was not catching up waits once B's block says it is:
/// the next contact runs no count repair against B, and the repair is still owed, and runs, once
/// B's marker has cleared.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_count_repair_planned_before_the_peer_marks_waits_until_its_marker_clears() {
    let (mut a, b) = (side().await, side().await);
    a.members.insert_for_test(b.addr, b.node);
    b.members.insert_for_test(a.addr, a.node);
    let live = a.engine.create_collection("shop", "live").unwrap();
    pull(&b.engine, &a.engine);
    for i in 0..5i64 {
        a.engine.insert(&live, doc! { "_id": i }).unwrap();
    }
    let mut planned = 0;
    for _ in 0..FROZEN_CONTACTS + 4 {
        planned += tick(&mut a).await;
        if planned > 0 {
            break;
        }
    }
    assert_eq!(
        planned, 1,
        "premise: a count repair was planned from B while B was not catching up"
    );
    b.catch_up.mark(crate::CatchUpReason::Snapshot).unwrap();
    tokio::time::sleep(BLOCK_REBUILT).await;
    async fn contact(a: &mut Side, b: &Side) -> kimmy_storage::SyncOutcome {
        a.tick += 1;
        a.stalls.begin_tick(a.tick);
        a.stalls.set_members(Some(a.members.clone()));
        a.stalls.tick_opened();
        a.stalls.allow_horizon_deferral(false);
        sync_once_with(&a.engine, b.addr, SECRET, None, &mut a.stalls).await.unwrap()
    }
    for round in 0..crate::transport::REPAIR_ATTEMPTS + 2 {
        let outcome = contact(&mut a, &b).await;
        assert!(outcome.peer_catching_up, "premise {round}: B's block says it is catching up");
        assert!(!outcome.count_repair, "round {round}: a count repair ran against B");
    }
    b.catch_up.clear("the test");
    tokio::time::sleep(BLOCK_REBUILT).await;
    let outcome = contact(&mut a, &b).await;
    assert!(outcome.count_repair, "the repair is still owed, and runs once B has caught up");
}

/// The other way round, for the existence half: A is catching up and lacks a collection B holds
/// (B is not catching up). A's own marker holds no repair of a collection it lacks, since that
/// repair is what brings a collection its own catch-up may not; it is planned and A takes it.
/// A count finding of A's would still be held (`a_member_catching_up_compares_no_count_...`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_member_catching_up_still_repairs_a_collection_it_lacks_from_a_peer_that_is_not() {
    let (mut a, b) = (side().await, side().await);
    a.members.insert_for_test(b.addr, b.node);
    hold_own_marker(&mut a);
    a.catch_up.mark(crate::CatchUpReason::Restored).unwrap();
    let _source = b_holds_part_of_a_snapshot_of_extra(&b);
    let mut planned = 0;
    for _ in 0..4 {
        planned += tick(&mut a).await;
    }
    assert!(a.catch_up.is_set(), "premise: A is still catching up");
    assert_eq!(planned, 1, "the collection A lacks is planned from B");
    assert!(a.engine.get_collection("shop", "extra").is_ok(), "and A takes it");
}

/// A pull from B stops at a collection A has no record of (A witnessed its creation without
/// applying it), and the stop plans the collection's snapshot from B. B is catching up, and the
/// snapshot is pulled all the same: it only adds what A lacks, and without it every pull from B
/// stops at the same entry. Only a count repair's replay waits for a peer catching up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pull_that_stops_at_a_collection_it_lacks_takes_its_snapshot_from_a_peer_catching_up() {
    let (mut a, b) = (side().await, side().await);
    a.members.insert_for_test(b.addr, b.node);
    let x = b.engine.create_collection("shop", "x").unwrap();
    a.engine.absorb_witnessed(&b.engine.witnessed_vector().unwrap()).unwrap();
    b.engine.insert(&x, doc! { "_id": 1 }).unwrap();
    b.catch_up.mark(crate::CatchUpReason::Snapshot).unwrap();
    for _ in 0..4 {
        tick(&mut a).await;
    }
    assert!(b.catch_up.is_set(), "premise: B is still catching up");
    assert!(a.engine.get_collection("shop", "x").is_ok(), "the stop's snapshot was held");
}
