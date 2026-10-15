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
use crate::transport::{FROZEN_CONTACTS, REPAIR_COOLDOWN_ROUNDS, serve};

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
    _dir: tempfile::TempDir,
}

async fn side() -> Side {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(Arc::clone(&engine), listener, SECRET.into()));
    Side {
        node: engine.node_id(),
        engine,
        addr,
        stalls: PeerStalls::new(),
        divergence: DivergenceTracker::new(),
        members: Members::default(),
        tick: 0,
        compared: 0,
        deferred: 0,
        _dir: dir,
    }
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
                planned +=
                    plan_count_repairs(&engine, &side.divergence, &mut side.stalls, node, addr);
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
                planned +=
                    plan_count_repairs(&engine, &side.divergence, &mut side.stalls, node, addr);
            }
            break;
        }
    }
    planned
}

/// A peer that stops and moves in turn, B, trailing A
/// only by A's latest write. Each time B stands still for `FROZEN_CONTACTS` contacts A compares
/// B's count, which is one short, and the finding is confirmed and repaired; while B moves the
/// probe is deferred and the finding suspended, keeping its cooldown (ADR-236 item 10: "so a
/// peer that stops and moves in turn is not repaired every time it stops"). The cut digest of
/// the same contact compares equal (A's newest write is above the cut, so both sides set it
/// aside), and since 77b1e6a0 that clears the suspended finding, so `retain_repaired` reads it
/// as cleared and forgets the cooldown: the next stop plans the repair again at once.
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
