//! Break-the-guard review C of the repair build: a real-loop storm of the document repair
//! over **simulated time**.
//!
//! The loop guard judges time by `tick_started - loop_started` (real time), and its
//! back-off is minutes to hours, so `replicate` itself cannot be run for a simulated day.
//! This module runs the same tick, in the same order, through the same functions the loop
//! calls (`advance_probe_on`, `sync_once_with` against a real `serve` over TLS,
//! `fold_digest_round`, `fold_definition_round`, the count half's planning,
//! `plan_digest_repairs`, `plan_definition_repairs`, the `retain_*` calls and the ledger's
//! clock, expiry and pruning), with the clock handed to `set_repair_clock` advanced by the
//! sync interval each tick. Only the clock is simulated; every round is real.
//!
//! The forged divergence is the lab's row 5 crossing pair, which neither half can close:
//! A holds `x`, B holds `y`, C holds `x`; each member witnessed the other's insert without
//! applying it, the counts are equal, and every member's horizon is above both stamps
//! (a sentinel tombstone collected after them), so neither id is young.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bson::doc;
use kimmy_core::{DocId, Hlc, NodeId};
use kimmy_storage::{DivergenceFindings, DivergenceTracker, Engine, WalkScope};

use super::*;
use crate::transport::{DigestRepairOutcome, DivergenceProbe, Judgement, PeerStalls, serve};

const SECRET: &str = "revc-storm-secret";
const INTERVAL: Duration = Duration::from_secs(5);

struct Member {
    name: &'static str,
    engine: Arc<Engine>,
    addr: SocketAddr,
    node: NodeId,
    stalls: PeerStalls,
    divergence: DivergenceTracker,
    digest_state: DigestState,
    definition_state: DefinitionState,
    config: ReplicationConfig,
    tick: u64,
    _dir: tempfile::TempDir,
}

/// What one requester saw of one peer's document repairs.
#[derive(Debug, Default, Clone)]
struct PairLog {
    /// Simulated minutes at which a document repair against the peer ended, and how.
    ends: Vec<(f64, &'static str)>,
    judged: BTreeMap<&'static str, usize>,
    left: usize,
    applied: usize,
    created: usize,
}

#[derive(Debug, Default)]
struct Storm {
    pairs: BTreeMap<(&'static str, &'static str), PairLog>,
    errors: usize,
    error_kinds: BTreeMap<String, usize>,
    digest_compared: usize,
    digest_deferred: usize,
    count_repairs_planned: usize,
    skipped_count_repair: usize,
    ticks: u64,
}

async fn member(name: &'static str) -> Member {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(Arc::clone(&engine), listener, SECRET.into()));
    let mut config = ReplicationConfig::new(Vec::new(), SECRET.into(), addr);
    config.sync_interval = INTERVAL;
    config.members = Some(Members::default());
    let mut digest_state = DigestState::default();
    let mut definition_state = DefinitionState::default();
    digest_state.repair_planned = config.divergence_repair;
    definition_state.repair_planned = config.divergence_repair;
    let mut stalls = PeerStalls::new();
    stalls.set_catch_up(None);
    Member {
        name,
        node: engine.node_id(),
        engine,
        addr,
        stalls,
        divergence: DivergenceTracker::new(),
        digest_state,
        definition_state,
        config,
        tick: 0,
        _dir: dir,
    }
}

fn pull(into: &Engine, from: &Engine) {
    let entries = from.entries_for_peer(Hlc::ZERO, 100_000, WalkScope::Background).unwrap().entries;
    into.apply_batch(&entries).unwrap();
}

/// One tick of `replicate` for `m` at simulated time `now`, contacting each peer `m`'s
/// membership lists once, in the loop's order.
async fn tick(
    m: &mut Member,
    now: Duration,
    names: &BTreeMap<NodeId, &'static str>,
    storm: &mut Storm,
) {
    m.tick += 1;
    tracing::info!(sim_min = now.as_secs_f64() / 60.0, member = m.name, "REVC tick");
    let engine = Arc::clone(&m.engine);
    let held = engine.all_collection_ids().unwrap();
    m.digest_state.prune(&held);
    m.definition_state.begin_tick();
    let members = m.config.members.as_ref().map(Members::node_ids);
    m.definition_state.prune(&held, members.as_ref());
    m.stalls.retain_definition_detail(members.as_ref(), &held);
    m.stalls.set_repair_clock(
        now,
        m.config.sync_interval * u32::try_from(held.len().max(1)).unwrap_or(u32::MAX),
    );
    for (key, verdict) in m.stalls.expire_repairs() {
        log_repair_verdict(key.peer, key.collection, verdict);
        // Unjudged ends, attributed to their pair here.
        let peer = names.get(&key.peer).copied().unwrap_or("?");
        *storm.pairs.entry((m.name, peer)).or_default().judged.entry("unjudged").or_default() += 1;
    }
    let _ = m.stalls.take_repair_counts();
    m.stalls.retain_ledger(&held, members.as_ref());
    m.divergence.retain_members(members.as_ref());
    let probe =
        advance_probe_on(&mut m.divergence, Ok::<_, ()>(held.clone())).map(|id| {
            match engine.count_probe_reading(id) {
                Ok((vector, mine_count)) => {
                    DivergenceProbe { id, mine_count, mine_at: Some(vector) }
                }
                Err(_) => DivergenceProbe { id, mine_count: None, mine_at: None },
            }
        });
    m.stalls.begin_tick(m.tick);
    m.stalls.set_members(m.config.members.clone());
    m.stalls.tick_opened();
    let peers: Vec<(SocketAddr, NodeId)> = m.config.members.as_ref().unwrap().entries();
    for (addr, _) in peers {
        m.stalls.allow_horizon_deferral(false);
        let mut pulls = 0;
        loop {
            pulls += 1;
            let pulled = sync_once_with(&engine, addr, SECRET, probe.clone(), &mut m.stalls).await;
            let _ = m.stalls.take_vectors_read();
            let _ = m.stalls.take_facts_undecodable();
            let _ = m.stalls.take_facts_read();
            let _ = m.stalls.take_pull();
            let _ = m.stalls.take_applied();
            let _ = m.stalls.take_reached_after(&pulled);
            match pulled {
                Ok(outcome) if outcome.horizon_deferred => break,
                Ok(mut outcome) => {
                    if outcome.truncated && pulls < 8 {
                        continue;
                    }
                    if let Some(node) = outcome.peer
                        && let Some(existence) = outcome.divergent.take()
                    {
                        let mut report = RoundReport::default();
                        let count = outcome.count_probe.take();
                        m.divergence.observe(node, DivergenceFindings { existence, count });
                        if let Some(round) = outcome.digest.take() {
                            fold_digest_round(
                                &engine,
                                &mut m.divergence,
                                &mut m.digest_state,
                                &mut m.stalls,
                                node,
                                addr,
                                round,
                                &mut report,
                            );
                        }
                        if let Some(round) = outcome.definitions.take() {
                            fold_definition_round(
                                &engine,
                                &mut m.divergence,
                                &mut m.definition_state,
                                &mut m.stalls,
                                node,
                                addr,
                                round,
                                &mut report,
                            );
                        }
                        let confirmed = m.divergence.confirmed_against(node);
                        for collection in &confirmed {
                            let repair = match engine.collection_by_id(*collection) {
                                Ok(Some(meta)) => Repair::Replay {
                                    from: meta
                                        .incarnation_floor
                                        .map_or(meta.created, |floor| floor.min(meta.created)),
                                },
                                _ => Repair::Snapshot,
                            };
                            if m.stalls.plan_repair(node, *collection, repair) {
                                storm.count_repairs_planned += 1;
                            }
                        }
                        m.stalls.retain_repaired(node, &confirmed);
                        let digest_confirmed = m.divergence.digest_confirmed_against(node);
                        plan_digest_repairs(
                            &engine,
                            &m.config,
                            &mut m.stalls,
                            &m.digest_state,
                            node,
                            addr,
                            &digest_confirmed,
                        );
                        m.stalls.retain_digest(node, &digest_confirmed);
                        let definition_confirmed = m.divergence.definition_confirmed_against(node);
                        plan_definition_repairs(
                            &engine,
                            &m.config,
                            &mut m.stalls,
                            node,
                            addr,
                            &definition_confirmed,
                        );
                        m.stalls.retain_definition(node, &definition_confirmed);
                        storm.digest_compared += report.digest_compared;
                        storm.digest_deferred += report.digest_deferred.iter().sum::<usize>();
                    }
                    let counts = m.stalls.take_repair_counts();
                    let peer = outcome.peer.and_then(|n| names.get(&n).copied());
                    record(storm, m.name, peer, now, &counts);
                    break;
                }
                Err(e) => {
                    storm.errors += 1;
                    let text: String = e.to_string().chars().take(90).collect();
                    *storm.error_kinds.entry(text).or_default() += 1;
                    let counts = m.stalls.take_repair_counts();
                    record(storm, m.name, Some(name_of_addr(names, addr)), now, &counts);
                    break;
                }
            }
        }
    }
}

thread_local! {
    static ADDRS: std::cell::RefCell<BTreeMap<SocketAddr, &'static str>> = const { std::cell::RefCell::new(BTreeMap::new()) };
}

fn name_of_addr(_names: &BTreeMap<NodeId, &'static str>, addr: SocketAddr) -> &'static str {
    ADDRS.with(|a| a.borrow().get(&addr).copied().unwrap_or("?"))
}

fn record(
    storm: &mut Storm,
    me: &'static str,
    peer: Option<&'static str>,
    now: Duration,
    counts: &crate::transport::RepairCounts,
) {
    let minutes = now.as_secs_f64() / 60.0;
    let pair = storm.pairs.entry((me, peer.unwrap_or("*"))).or_default();
    for outcome in DigestRepairOutcome::ALL {
        for _ in 0..counts.digest_repairs[outcome.index()] {
            if matches!(
                outcome,
                DigestRepairOutcome::Applied
                    | DigestRepairOutcome::Transient
                    | DigestRepairOutcome::Abandoned
            ) {
                pair.ends.push((minutes, outcome.label()));
            }
        }
    }
    storm.skipped_count_repair +=
        counts.digest_repairs[DigestRepairOutcome::SkippedCountRepair.index()];
    for judgement in Judgement::ALL {
        if judgement == Judgement::Unjudged {
            continue;
        }
        *pair.judged.entry(judgement.label()).or_default() +=
            counts.digest_judgements[judgement.index()];
    }
    pair.applied += counts.digest_rows[crate::transport::RowResult::Applied.index()];
    pair.left += counts.digest_rows[crate::transport::RowResult::Left.index()];
    pair.created += counts.digest_rows[crate::transport::RowResult::Created.index()];
}

/// The knobs of one storm.
#[derive(Clone, Copy, Debug)]
struct Knobs {
    hours: u64,
    /// Writes a tick on another collection (about 2 a second at 10 a tick), with deletes.
    other_writes: usize,
    /// Writes a tick on the pair's own collection, on ids other than the pair's.
    same_writes: usize,
    /// Every this many ticks, C leaves every other member's live set for one tick (a
    /// SWIM down and up), as an unstable link or a stalled member does.
    flap_every: Option<u64>,
    /// At this tick, B drops and recreates the pair's collection (all of it).
    drop_recreate_at: Option<u64>,
    /// C's copy of the pair's collection holds a record that does not decode (ADR-187's
    /// survivable corruption) behind this many good documents: every scoped snapshot C
    /// serves of it fails at that page, while its digest and its entries answer as ever.
    corrupt_c_after: Option<usize>,
    /// From this tick B stops contacting A for `away` ticks while A writes and collects its
    /// oplog, so B's next ordinary pull from A is answered `BeyondHorizon` and becomes a
    /// whole-database pull competing with B's document repair against A.
    whole_db_at: Option<(u64, u64)>,
}

impl Default for Knobs {
    fn default() -> Self {
        let hours = std::env::var("REVC_HOURS").ok().and_then(|v| v.parse().ok()).unwrap_or(8);
        Knobs {
            hours,
            other_writes: 10,
            same_writes: 0,
            flap_every: None,
            drop_recreate_at: None,
            corrupt_c_after: None,
            whole_db_at: None,
        }
    }
}

const YEAR: u64 = 365 * 24 * 3600;

async fn storm(knobs: Knobs) -> Storm {
    if let Ok(path) = std::env::var("REVC_LOG") {
        let file = std::sync::Mutex::new(std::fs::File::create(path).unwrap());
        let subscriber = tracing_subscriber::fmt()
            .with_writer(file)
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    }
    let mut ms = vec![member("A").await, member("B").await, member("C").await];
    if std::env::var("REVC_REPAIR_OFF").is_ok() {
        for m in ms.iter_mut() {
            m.config.divergence_repair = false;
            m.digest_state.repair_planned = false;
            m.definition_state.repair_planned = false;
        }
    }
    let names: BTreeMap<NodeId, &'static str> = ms.iter().map(|m| (m.node, m.name)).collect();
    ADDRS.with(|a| {
        let mut a = a.borrow_mut();
        for m in &ms {
            a.insert(m.addr, m.name);
        }
    });
    let peers_of = |i: usize, ms: &[Member]| -> Vec<(SocketAddr, NodeId)> {
        ms.iter().enumerate().filter(|(j, _)| *j != i).map(|(_, m)| (m.addr, m.node)).collect()
    };
    for i in 0..3 {
        for (addr, node) in peers_of(i, &ms) {
            ms[i].config.members.as_ref().unwrap().insert_for_test(addr, node);
        }
    }
    // The collections, everywhere.
    let a = Arc::clone(&ms[0].engine);
    let b = Arc::clone(&ms[1].engine);
    let c = Arc::clone(&ms[2].engine);
    let orders = a.create_collection("shop", "orders").unwrap();
    a.create_collection("shop", "live").unwrap();
    pull(&b, &a);
    pull(&c, &a);
    // A sentinel in another collection, deleted and replicated first: its tombstone is
    // what every member's retention pass collects after the pair, so every horizon passes
    // the pair's stamps and neither id is young.
    let live = a.get_collection("shop", "live").unwrap();
    a.insert(&live, doc! { "_id": "sentinel" }).unwrap();
    a.delete(&live, &DocId::String("sentinel".into())).unwrap();
    pull(&b, &a);
    pull(&c, &a);
    std::thread::sleep(Duration::from_millis(5));
    if let Some(good) = knobs.corrupt_c_after
        && good > 0
    {
        // Good documents on all three, before the pair, so they are no difference of their own.
        let docs: Vec<_> = (0..good).map(|i| doc! { "_id": format!("g{i:06}") }).collect();
        a.insert_many(&orders, docs).unwrap();
        pull(&b, &a);
        pull(&c, &a);
        std::thread::sleep(Duration::from_millis(5));
    }
    // The crossing pair: A writes x, B writes y; C takes x; nobody applies the other's.
    a.insert(&orders, doc! { "_id": "x", "v": 1 }).unwrap();
    b.insert(&b.get_collection("shop", "orders").unwrap(), doc! { "_id": "y", "v": 1 }).unwrap();
    pull(&c, &a);
    let (wa, wb, wc) = (
        a.witnessed_vector().unwrap(),
        b.witnessed_vector().unwrap(),
        c.witnessed_vector().unwrap(),
    );
    for e in [&a, &b, &c] {
        e.absorb_witnessed(&wa).unwrap();
        e.absorb_witnessed(&wb).unwrap();
        e.absorb_witnessed(&wc).unwrap();
    }
    std::thread::sleep(Duration::from_millis(5));
    let cutoff = kimmy_storage::physical_now_ms();
    for e in [&a, &b, &c] {
        let removed = e
            .collect_garbage_at(cutoff + 1_000, kimmy_storage::gc::RetentionPolicy::new(YEAR, 1))
            .unwrap()
            .tombstones_removed;
        assert!(removed >= 1, "premise: the sentinel tombstone was collected");
    }
    let held = |e: &Engine, id: &str| {
        e.get(&e.get_collection("shop", "orders").unwrap(), &DocId::String(id.into()))
            .unwrap()
            .is_some()
    };
    assert!(held(&a, "x") && !held(&a, "y"), "premise: A holds x only");
    assert!(held(&b, "y") && !held(&b, "x"), "premise: B holds y only");
    assert!(held(&c, "x") && !held(&c, "y"), "premise: C holds x only");
    if knobs.corrupt_c_after.is_some() {
        // A key that sorts after every good one.
        c.store_undecodable_doc_for_test(&c.get_collection("shop", "orders").unwrap(), &[0xff; 24]);
    }
    let mut out = Storm::default();
    let ticks = knobs.hours * 3600 / INTERVAL.as_secs();
    let mut written = 0u64;
    for t in 0..ticks {
        let now = INTERVAL * u32::try_from(t).unwrap();
        // Live writes, by A.
        let live = a.get_collection("shop", "live").unwrap();
        for _ in 0..knobs.other_writes {
            written += 1;
            a.insert(&live, doc! { "_id": format!("w{written}") }).unwrap();
            if written % 2 == 0 {
                a.delete(&live, &DocId::String(format!("w{}", written - 1))).unwrap();
            }
        }
        if knobs.same_writes > 0
            && let Ok(on) = a.get_collection("shop", "orders")
        {
            for _ in 0..knobs.same_writes {
                written += 1;
                a.insert(&on, doc! { "_id": format!("o{written}") }).unwrap();
                if written % 2 == 0 {
                    a.delete(&on, &DocId::String(format!("o{}", written - 1))).unwrap();
                }
            }
        }
        if knobs.drop_recreate_at == Some(t) {
            assert!(b.drop_collection("shop", "orders").unwrap());
            b.finish_purges_now().unwrap();
            b.create_collection("shop", "orders").unwrap();
        }
        if let Some((at, away)) = knobs.whole_db_at {
            if t == at {
                ms[1].config.members.as_ref().unwrap().remove_for_test(&ms[0].addr);
                ms[2].config.members.as_ref().unwrap().remove_for_test(&ms[0].addr);
            }
            if t == at + away / 2 {
                // A writes a batch nobody has, and collects its own oplog under it.
                let bulk = a.create_collection("shop", "bulk").unwrap();
                let docs: Vec<_> = (0..3_000).map(|i| doc! { "_id": i }).collect();
                a.insert_many(&bulk, docs).unwrap();
                std::thread::sleep(Duration::from_millis(5));
                a.collect_garbage_at(
                    kimmy_storage::physical_now_ms() + 1_000,
                    kimmy_storage::gc::RetentionPolicy::new(1, YEAR),
                )
                .unwrap();
            }
            if t == at + away {
                let (addr, node) = (ms[0].addr, ms[0].node);
                ms[1].config.members.as_ref().unwrap().insert_for_test(addr, node);
                ms[2].config.members.as_ref().unwrap().insert_for_test(addr, node);
            }
        }
        // C flaps out of A's and B's live sets for one tick.
        let flapping = knobs.flap_every.is_some_and(|every| t > 0 && t % every == 0);
        if flapping {
            for i in 0..2 {
                ms[i].config.members.as_ref().unwrap().remove_for_test(&ms[2].addr);
            }
        }
        for m in ms.iter_mut() {
            // The drop purger runs in the background on every real member.
            m.engine.finish_purges_now().unwrap();
            tick(m, now, &names, &mut out).await;
        }
        if flapping {
            let (addr, node) = (ms[2].addr, ms[2].node);
            for i in 0..2 {
                ms[i].config.members.as_ref().unwrap().insert_for_test(addr, node);
            }
        }
        out.ticks += 1;
    }
    out
}

fn summary(label: &str, storm: &Storm) -> String {
    let mut s = format!(
        "REVC STORM {label}: ticks={} errors={} {:?} digest_compared={} digest_deferred={} count_repairs_planned={} skipped_count_repair={}\n",
        storm.ticks,
        storm.errors,
        storm.error_kinds,
        storm.digest_compared,
        storm.digest_deferred,
        storm.count_repairs_planned,
        storm.skipped_count_repair
    );
    for ((me, peer), log) in &storm.pairs {
        if log.ends.is_empty() && log.judged.values().all(|n| *n == 0) {
            continue;
        }
        let at: Vec<String> = log
            .ends
            .iter()
            .map(
                |(m, how)| {
                    if *how == "applied" { format!("{m:.1}") } else { format!("{m:.1}:{how}") }
                },
            )
            .collect();
        s.push_str(&format!(
            "REVC STORM {label}:   {me}<-{peer}: {} ends at min [{}] judged {:?} rows applied={} left={} created={}\n",
            log.ends.len(),
            at.join(", "),
            log.judged,
            log.applied,
            log.left,
            log.created
        ));
    }
    s
}

/// The back-off bound per (requester, peer) over `hours`: repairs at about 0, 5, 20, 65 and
/// 200 minutes, then one per 6 hours, each start delayed by up to a cooldown and a rotation.
/// Generous: the bound plus two.
fn bound(hours: u64) -> usize {
    let minutes = hours as f64 * 60.0;
    let mut at = 0.0;
    let mut n = 0;
    for step in [5.0, 15.0, 45.0, 135.0] {
        if at > minutes {
            break;
        }
        n += 1;
        at += step;
    }
    while at <= minutes {
        n += 1;
        at += 360.0;
    }
    n + 2
}

fn assert_bounded(label: &str, storm: &Storm, hours: u64) {
    let limit = bound(hours);
    let mut worst = 0;
    for ((me, peer), log) in &storm.pairs {
        if *peer == "*" {
            continue;
        }
        worst = worst.max(log.ends.len());
        assert!(
            log.ends.len() <= limit,
            "{label}: {me}<-{peer} ran {} document repairs in {hours} h; the back-off bounds it near {}\n{}",
            log.ends.len(),
            limit - 2,
            summary(label, storm)
        );
    }
    assert!(
        worst >= 3,
        "{label}: the storm never formed (worst pair {worst})\n{}",
        summary(label, storm)
    );
}

/// The baseline: the crossing pair, live writes with deletes on another collection, 8
/// simulated hours.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revc_storm_the_crossing_pair_backs_off_by_the_schedule() {
    let knobs = Knobs::default();
    let storm = storm(knobs).await;
    println!("{}", summary("baseline", &storm));
    assert_bounded("baseline", &storm, knobs.hours);
}

/// Live writes on the pair's own collection (lab row 5's variant b).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revc_storm_writes_on_the_pairs_collection_back_off_by_the_schedule() {
    let knobs = Knobs { same_writes: 10, other_writes: 0, ..Knobs::default() };
    let storm = storm(knobs).await;
    println!("{}", summary("same-collection", &storm));
    assert_bounded("same-collection", &storm, knobs.hours);
}

/// C leaves A's and B's live sets for one tick every two minutes (a SWIM down and up).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revc_storm_a_flapping_member_keeps_the_back_off() {
    let knobs = Knobs { flap_every: Some(24), ..Knobs::default() };
    let storm = storm(knobs).await;
    println!("{}", summary("flapping", &storm));
    assert_bounded("flapping", &storm, knobs.hours);
}

/// B drops and recreates the pair's collection an hour in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revc_storm_a_drop_and_recreate_mid_storm_stays_bounded() {
    let knobs = Knobs { drop_recreate_at: Some(720), ..Knobs::default() };
    let storm = storm(knobs).await;
    println!("{}", summary("drop-recreate", &storm));
    for ((me, peer), log) in &storm.pairs {
        assert!(
            log.ends.len() <= bound(knobs.hours) * 2,
            "drop-recreate: {me}<-{peer} {}\n{}",
            log.ends.len(),
            summary("drop-recreate", &storm)
        );
    }
}

/// C's copy of the pair's collection carries a record that does not decode, behind 3,000 good
/// documents: every Converge pull B makes from C fails on the page that reaches it, after
/// pulling the pages before it. C's digest still answers, so B's finding against C stands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revc_storm_a_sender_whose_pages_fail_keeps_the_back_off() {
    let knobs = Knobs { corrupt_c_after: Some(3_000), ..Knobs::default() };
    let storm = storm(knobs).await;
    println!("{}", summary("error-peer", &storm));
    assert_bounded("error-peer", &storm, knobs.hours);
}

/// B is away from A for ten minutes while A writes and collects its oplog: B's next pull from
/// A is a whole-database pull, competing with B's document repair against A.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revc_storm_a_whole_database_pull_competing_for_the_peer_stays_bounded() {
    let knobs = Knobs { whole_db_at: Some((600, 120)), ..Knobs::default() };
    let storm = storm(knobs).await;
    println!("{}", summary("whole-db", &storm));
    assert_bounded("whole-db", &storm, knobs.hours);
}

/// The planner and the ledger alone, over a simulated day, for a peer whose every Converge
/// round fails (an error, a reset, a frame that never arrives) while its ordinary rounds and
/// its digest answer: the finding stays confirmed, each repair is abandoned after
/// `REPAIR_ATTEMPTS` rounds as **transient** (R1), and a transient end never climbs. The
/// repair is planned again every cooldown, for ever.
#[test]
fn revc_a_peer_whose_converge_rounds_always_fail_is_repaired_every_cooldown() {
    use crate::transport::DigestPlan;
    let peer = NodeId::from_bytes([7; 16]);
    let collection = kimmy_core::CollectionId(77);
    let mut stalls = PeerStalls::new();
    let mut handed = 0usize;
    let mut abandoned = 0usize;
    let mut planned = 0usize;
    let contacts = 24 * 3600 / INTERVAL.as_secs();
    let mut last_active: Option<(kimmy_core::CollectionId, Repair)> = None;
    for contact in 0..contacts {
        let now = INTERVAL * u32::try_from(contact).unwrap();
        stalls.set_repair_clock(now, INTERVAL);
        let _ = stalls.expire_repairs();
        stalls.tick_opened();
        // The round: a Converge round if one is due, which fails (no page lands); otherwise
        // an ordinary round, which succeeds and runs the check.
        match stalls.repair_due(peer) {
            Some(due) => {
                if last_active != Some(due) || !stalls.repairing(peer) {
                    handed += 1;
                }
                last_active = Some(due);
            }
            None => {
                // The check ran: the finding stands, and the planner is asked.
                stalls.digest_finding_confirmed(peer, collection);
                if stalls.plan_digest_repair(peer, collection, kimmy_core::VersionVector::new())
                    == DigestPlan::Planned
                {
                    planned += 1;
                    last_active = None;
                }
                stalls.retain_digest(peer, &std::collections::BTreeSet::from([collection]));
            }
        }
        let counts = stalls.take_repair_counts();
        abandoned += counts.digest_repairs[DigestRepairOutcome::Abandoned.index()];
    }
    println!(
        "REVC ERROR-PEER over 24 simulated hours: planned={planned} handed_out_rounds~{handed} abandoned={abandoned} (the back-off bound is about 7)"
    );
    assert!(
        planned <= bound(24),
        "a peer whose Converge rounds always fail was repaired {planned} times in a day; the \
         back-off bounds a repair that changes nothing near {} (abandoned {abandoned})",
        bound(24) - 2
    );
}
