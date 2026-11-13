//! The random run (`swapmodel5.py` 1304-1420) and the grid (`swapmodel5.py` 1608-1745): the action mix, the
//! quiescence loop, the batch summary. Phase A has no snapshot joins (phase B adds the `snap` and `jpage` actions),
//! so `Cfg::snapshots` is false and those two actions carry weight 0.

use std::collections::BTreeMap;

use crate::entry::*;
use crate::flags::flags_for;
use crate::member::{DocOp, Member};
use crate::oracle::{Res, finish};
use crate::relay::{pair, take_quiesce};
use crate::rng::Rng;
use crate::snapshot::{land_page_in, snapshot_begin_in};
use crate::world::{Policy, World};

#[derive(Clone, Copy, Debug)]
pub struct Cfg {
    pub members: usize,
    pub keys: usize,
    pub outs: usize,
    pub steps: Option<usize>,
    pub delete_bias: bool,
    pub snapshots: bool,
    pub policy: Policy,
    /// `field`: mint Q7's loss records as entries (default on)
    pub q7_entries: bool,
    /// `field`: `K` records per document, both families together (off by default)
    pub k: Option<usize>,
    /// `field`: updates alternate `$set` and `$inc` by the clock's parity (default); off, every update is a `$set`
    pub field_counters: bool,
    /// the `document` policy witnesses page versions' stamps too (the metamorphic runs; see `World::witness_pages`)
    pub witness_pages: bool,
    /// the route probes (`Q6_F`, `Q6_ROUTES`): a share of the window steps are a route step instead (the model's hold and repair
    /// routes); register events then travel out of position by construction, so the in-order liveness check is off
    pub routes: Option<Routes>,
    /// a workload on which last-writer-wins and the join agree: no delete, and every document written by one member only
    pub lww_workload: Option<Writers>,
    /// the oracle's HB-2 exclusion (`World::hb2_exclusion`; on by default, off only to show what it clears)
    pub hb2_exclusion: bool,
    /// B2: the members' coverage is the product's per-origin vector (`World::vector_cov`)
    pub vector_cov: bool,
    /// M5, the abandon-and-restart mode (Rust only; the model never abandons a join): a join whose sender has vanished lands no
    /// further page (the product reads pages from the sender); every live member with such a join restarts it with `supersede_join`
    /// from a random live eligible peer, after each action and during the end-of-run delivery
    pub restart_joins: bool,
}

/// The route probes of `swapmodel6a.py` (default off): `f` is the probability, per window step, of a route step.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Routes {
    pub f: f64,
    /// `hold`: a non-document entry of the source's held set is delivered out of position
    pub hold: bool,
    /// `repair`: the source's register events are handed over as information only
    pub repair: bool,
    /// the product's routes (no model counterpart; register events stay in position): `doc_hold`: a DOCUMENT entry of the source's
    /// held set is delivered out of position (held as state, released in position); `doc_repair`: a scoped repair, the source's
    /// document versions handed over as pages of a snapshot that grants no coverage and carries no register events
    pub doc_hold: bool,
    pub doc_repair: bool,
}

/// Who writes a document in an `lww_workload`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Writers {
    /// the document's owner, member `key % members` (the write is skipped when the owner has vanished)
    OwnerByKey,
    /// member 0 writes everything
    OneWriter,
}

impl Cfg {
    pub fn new(members: usize, outs: usize, keys: usize) -> Self {
        Self {
            members,
            keys,
            outs,
            steps: None,
            delete_bias: false,
            snapshots: true,
            policy: Policy::Document,
            q7_entries: true,
            k: None,
            field_counters: true,
            witness_pages: false,
            routes: None,
            lww_workload: None,
            hb2_exclusion: true,
            vector_cov: false,
            restart_joins: false,
        }
    }
}

/// M5: every live member whose join's sender has vanished restarts it from a random live eligible peer (not itself, not the old
/// sender); a member with no eligible peer waits (counted `dead_sender_join_waiting`) and is retried at the next call.
fn restart_dead_joins(ms: &mut [Member], w: &mut World) {
    for d in 0..ms.len() {
        if ms[d].vanished {
            continue;
        }
        let Some(old) = ms[d].join.as_ref().map(|j| j.src) else { continue };
        if !ms[old].vanished {
            continue;
        }
        let cands: Vec<usize> = (0..ms.len())
            .filter(|&i| i != d && i != old && !ms[i].vanished && ms[i].eligible_source())
            .collect();
        if cands.is_empty() {
            w.stats.bump("dead_sender_join_waiting");
            continue;
        }
        let s = cands[w.rng.below(cands.len())];
        let lazy = w.rng.f64() < 0.5;
        let (dm, sm) = pair(ms, d, s);
        if dm.supersede_join(w, sm, lazy) {
            w.stats.bump("join_restarted_dead_sender");
        }
    }
}

/// Land `i`'s pages until its join completes. With `restart` a join whose sender has vanished is restarted (`restart_dead_joins`)
/// instead of landed; one that cannot be restarted stays open (`dead_sender_join_open_at_end`).
fn finish_joins(ms: &mut [Member], i: usize, w: &mut World, restart: bool) {
    while ms[i].join.is_some() {
        if restart {
            restart_dead_joins(ms, w);
            if ms[i].join.as_ref().is_some_and(|j| ms[j.src].vanished) {
                w.stats.bump("dead_sender_join_open_at_end");
                return;
            }
        }
        land_page_in(ms, i, w);
    }
}

/// The model's D10 state counters: member-runs whose served physical is superseded by a known register event (any cause), when the
/// steps end (`at_end` false) and at the end of the run, and those where the event is held as information.
pub fn count_open_refusals(w: &mut World, ms: &[Member], at_end: bool) {
    let (all, info) = if at_end {
        ("refusal_open_at_run_end", "refusal_open_at_run_end_info")
    } else {
        ("refusal_open_at_steps_end", "refusal_open_at_steps_end_info")
    };
    for m in ms.iter().filter(|m| !m.vanished) {
        if let Some(s) = m.serving() {
            let inf = m.info_superseding(w, &s).is_some();
            if m.first_superseding(w, &s).is_some() || inf {
                w.stats.bump(all);
            }
            if inf {
                w.stats.bump(info);
            }
        }
    }
}

fn choices(rng: &mut Rng, weights: &[f64]) -> usize {
    let total: f64 = weights.iter().sum();
    let mut r = rng.f64() * total;
    for (i, w) in weights.iter().enumerate() {
        if r < *w {
            return i;
        }
        r -= w;
    }
    weights.iter().rposition(|w| *w > 0.0).unwrap_or(0)
}

fn randint(rng: &mut Rng, a: i64, b: i64) -> i64 {
    a + rng.below((b - a + 1) as usize) as i64
}

/// `random.sample(range(n), k)`.
fn sample(rng: &mut Rng, n: usize, k: usize) -> Vec<u32> {
    let mut v: Vec<u32> = (0..n as u32).collect();
    for i in 0..k {
        let j = i + rng.below(n - i);
        v.swap(i, j);
    }
    v.truncate(k);
    v
}

pub struct RunOut {
    pub res: Res,
    pub w: World,
    pub ms: Vec<Member>,
}

pub fn run(mode: &str, base: &str, seed: u64, cfg: Cfg, trace: bool) -> RunOut {
    let flags = flags_for(mode, base);
    let mut rng = Rng::new(seed);
    let names: Vec<String> =
        (0..cfg.members).map(|i| ((b'A' + i as u8) as char).to_string()).collect();
    let d = [cfg.keys as i64 + 8, cfg.keys as i64 + 25, cfg.keys as i64 + 60][rng.below(3)];
    let skews: Vec<i64> = (0..cfg.members).map(|_| [-6, -3, 0, 0, 3, 6][rng.below(6)]).collect();
    let mut w = World::new(names.clone(), flags, rng, d, trace);
    let mut ms: Vec<Member> =
        names.iter().enumerate().map(|(i, n)| Member::new(i, n, skews[i])).collect();
    w.policy = cfg.policy;
    w.q7_entries = cfg.q7_entries;
    w.k = cfg.k;
    w.in_order = cfg.routes.is_none_or(|rt| !rt.hold && !rt.repair);
    w.reg_in_position_check = w.in_order;
    w.held_uncovered = cfg.routes.is_some_and(|rt| rt.doc_hold || rt.doc_repair);
    w.hb2_exclusion = cfg.hb2_exclusion;
    w.vector_cov = cfg.vector_cov;
    w.field_counters = cfg.field_counters;
    w.witness_pages = cfg.witness_pages;
    let steps = cfg.steps.unwrap_or(120 + 40 * cfg.outs);
    for k in 0..cfg.keys as u32 {
        ms[0].local_doc(&mut w, k, DocOp::Insert);
    }
    for m in ms.iter_mut() {
        for id in 0..w.all.len() {
            let e = w.ent_rc(id);
            m.take(&mut w, &e);
        }
    }
    let mut jobs: Vec<Job> = Vec::new();
    let mut outs_left = cfg.outs as i64;
    let ldrop_p = [0.0, 0.0, 0.5, 1.2][w.rng.below(4)];
    for step in 0..steps as i64 {
        w.step = step;
        w.clock += 1;
        let due: Vec<Job> = jobs.iter().filter(|j| j.bind_step <= step).cloned().collect();
        jobs.retain(|j| j.bind_step > step);
        for mut j in due {
            let live = ms.iter().filter(|m| !m.vanished).count();
            ms[j.writer].do_bind(&mut w, &mut j, live);
        }
        let weights = [
            26.0,
            if outs_left > 0 { 5.0 } else { 0.0 },
            ldrop_p,
            8.0,
            if cfg.snapshots { 4.0 } else { 0.0 },
            if cfg.snapshots { 4.0 } else { 0.0 },
            2.0,
            52.0,
        ];
        let mut act = choices(&mut w.rng, &weights);
        let active: Vec<usize> = (0..ms.len()).filter(|&i| !ms[i].vanished).collect();
        if active.is_empty() && act <= 3 {
            act = 7;
        }
        match act {
            0 => {
                let m = active[w.rng.below(active.len())];
                let key = w.rng.below(cfg.keys + 2) as u32;
                let ops: [DocOp; 4] = if cfg.delete_bias {
                    [DocOp::Insert, DocOp::Update, DocOp::Delete, DocOp::Delete]
                } else {
                    [DocOp::Insert, DocOp::Update, DocOp::Update, DocOp::Delete]
                };
                let op = ops[w.rng.below(4)];
                // the draws above are the same for every policy; an `lww_workload` only reshapes what they do
                let (m, op) = match cfg.lww_workload {
                    None => (Some(m), op),
                    Some(who) => {
                        // (the owner is by the key as the model draws it: `key % members` below is of the mapped key)
                        let key = key % cfg.keys as u32;
                        let owner = match who {
                            Writers::OwnerByKey => key as usize % cfg.members,
                            Writers::OneWriter => 0,
                        };
                        let op = DocOp::Update; // no delete, and no later insert (see below)
                        (Some(owner).filter(|&o| !ms[o].vanished), op)
                    }
                };
                // updates only (after the setup's inserts): a lost insert is two records under `field` (the document-level write and
                // its field cell) and one under `document`, and each record ticks its minter's clock, so the two policies'
                // stamps would part; an update is one cell, one record
                let key = if cfg.lww_workload.is_some() { key % cfg.keys as u32 } else { key };
                if let Some(m) = m {
                    ms[m].local_doc(&mut w, key, op);
                }
            }
            1 => {
                outs_left -= 1;
                let m = active[w.rng.below(active.len())];
                let kind = [JobKind::Good, JobKind::Late, JobKind::CrashAbandon, JobKind::Vanish]
                    [choices(&mut w.rng, &[62.0, 12.0, 16.0, 10.0])];
                let delay = if kind == JobKind::Late {
                    w.d + randint(&mut w.rng, 4, 25)
                } else {
                    randint(&mut w.rng, 1, w.d.min(30))
                };
                let k = randint(&mut w.rng, 1, cfg.keys as i64) as usize;
                let keys = sample(&mut w.rng, cfg.keys + 2, k);
                let job = ms[m].start_out(&mut w, keys, kind, step + delay);
                jobs.push(job);
            }
            2 => {
                let m = active[w.rng.below(active.len())];
                ms[m].local_ldrop(&mut w);
            }
            3 => {
                let absent: Vec<usize> =
                    active.iter().copied().filter(|&i| ms[i].serving().is_none()).collect();
                if !absent.is_empty() {
                    let m = absent[w.rng.below(absent.len())];
                    ms[m].local_lcreate(&mut w);
                }
            }
            4 => {
                if active.is_empty() {
                    continue;
                }
                let dst = active[w.rng.below(active.len())];
                let cands: Vec<usize> = (0..ms.len())
                    .filter(|&i| i != dst && !ms[i].vanished && ms[i].eligible_source())
                    .collect();
                let post: Vec<usize> = cands
                    .iter()
                    .copied()
                    .filter(|&i| {
                        ms[i].cur.as_ref().is_some_and(|c| {
                            c.kind == Kind::Bind
                                && ms[i]
                                    .phys
                                    .get(&c.phys)
                                    .unwrap()
                                    .docs
                                    .values()
                                    .any(|v| v.stamp > c.stamp)
                        })
                    })
                    .collect();
                if !cands.is_empty() {
                    let src = if !post.is_empty() && w.rng.f64() < 0.6 {
                        post[w.rng.below(post.len())]
                    } else {
                        cands[w.rng.below(cands.len())]
                    };
                    let lazy = w.rng.f64() < 0.5;
                    snapshot_begin_in(&mut ms, dst, src, &mut w, lazy);
                }
            }
            5 => {
                let joining: Vec<usize> = (0..ms.len())
                    .filter(|&i| {
                        !ms[i].vanished
                            && ms[i]
                                .join
                                .as_ref()
                                .is_some_and(|j| !(cfg.restart_joins && ms[j.src].vanished))
                    })
                    .collect();
                if !joining.is_empty() {
                    let m = joining[w.rng.below(joining.len())];
                    land_page_in(&mut ms, m, &mut w);
                }
            }
            6 => {
                if active.len() < 2 {
                    continue;
                }
                let (a, b) = w.rng.sample2(active.len());
                let (src, dst) = (active[a], active[b]);
                let (d, s) = pair(&mut ms, dst, src);
                d.relay_tomb(&mut w, s);
            }
            _ => {
                if active.len() < 2 {
                    continue;
                }
                let (a, b) = w.rng.sample2(active.len());
                let (src, dst) = (active[a], active[b]);
                // a window from the receiver's single threshold: the lowest-stamp entry the source APPENDED and the receiver lacks
                let (d, s) = pair(&mut ms, dst, src);
                let cand = d.next_window_entry(&w, s);
                // route probes (off by default; the draw is made only when `f > 0`, as in the model)
                let r = match cfg.routes {
                    Some(rt) if rt.f > 0.0 => w.rng.f64(),
                    _ => 1.0,
                };
                if let Some(rt) = cfg.routes.filter(|rt| rt.f > 0.0 && r < rt.f) {
                    let kinds: Vec<u8> = [rt.hold, rt.repair, rt.doc_hold, rt.doc_repair]
                        .iter()
                        .enumerate()
                        .filter(|(_, on)| **on)
                        .map(|(i, _)| i as u8)
                        .collect();
                    match kinds[w.rng.below(kinds.len())] {
                        0 => {
                            // the hold route: a non-document entry the source holds (any stamp, origin or kind) is delivered out of position
                            let nd: Vec<EntryRef> = w
                                .all
                                .iter()
                                .filter(|e| {
                                    e.kind != Kind::Doc && s.holds(e.idx) && !d.holds(e.idx)
                                })
                                .cloned()
                                .collect();
                            if !nd.is_empty() {
                                let e = nd[w.rng.below(nd.len())].clone();
                                w.note(format!("HOLD {} <- {}: {}", d.name, s.name, e));
                                w.stats.bump("hold_taken");
                                d.take_hold(&mut w, &e);
                            }
                        }
                        1 => {
                            // the repair route: the source's register events are handed over as information (a scoped repair that carries
                            // the register and grants no coverage): no entry, no coverage, no clock witness
                            let new: Vec<EntryId> =
                                s.evl.iter().copied().filter(|i| !d.events.contains(i)).collect();
                            if !new.is_empty() {
                                w.note(format!(
                                    "REPAIR {} <- {}: register events {:?}",
                                    d.name, s.name, new
                                ));
                                for &i in &new {
                                    let e = w.ent_rc(i);
                                    d.add_event(&mut w, &e);
                                }
                                w.stats.add("repair_events", new.len() as i64);
                                d.settle(&mut w);
                            }
                        }
                        2 => {
                            // the product's held document entries (ADR-160/169): a document entry the source holds, delivered out of
                            // position: applied, not appended, released once every earlier entry of its origin is held.
                            // `m-register-ahead` lets a register event be picked as well (invariant (ii) broken on purpose)
                            let ahead = w.f.fm_register_ahead;
                            let q6 = w.f.fm_doc_hold_q6;
                            let nd: Vec<EntryRef> = w
                                .all
                                .iter()
                                .filter(|e| {
                                    (e.kind == Kind::Doc
                                        || (ahead && e.kind.is_register_event())
                                        || (q6 && e.is_q6_kind()))
                                        && s.holds(e.idx)
                                        && !d.holds(e.idx)
                                })
                                .cloned()
                                .collect();
                            if !nd.is_empty() {
                                let e = nd[w.rng.below(nd.len())].clone();
                                w.note(format!("DOC-HOLD {} <- {}: {}", d.name, s.name, e));
                                w.stats.bump("doc_hold_taken");
                                if e.is_q6_kind() {
                                    w.stats.bump("route_picked_q6_kind"); // counted only: the check below is on the consequence
                                }
                                // R1 (M2 of the harness review): the hold relog carries no Q6 kind, judged by its CONSEQUENCE on the
                                // receiver's state, as the scoped repair's: the Q6 entries it knows (stage creates, abandons, register
                                // events in effect or held as information, records held as information) did not grow. (Not the whole
                                // Q6 surface: a held document entry legitimately triggers burials and flips through `settle`.)
                                let before = d.q6_known();
                                d.take_hold(&mut w, &e);
                                if d.q6_known_grew_from(&before) {
                                    w.stats.bump("route_carries_q6_kind");
                                    d.inv.insert("route_carries_q6_kind");
                                }
                            }
                        }
                        _ => {
                            // the product's scoped repair: document versions as state, no grant, no register events
                            w.note(format!("DOC-REPAIR {} <- {}", d.name, s.name));
                            if d.scoped_repair(&mut w, s) {
                                w.stats.bump("doc_repair");
                            }
                        }
                    }
                } else if let Some(e) = cand {
                    d.take_from(&mut w, s, &e, false);
                }
            }
        }
        if cfg.restart_joins {
            restart_dead_joins(&mut ms, &mut w);
        }
        for m in ms.iter_mut() {
            m.check_inv(&w);
        }
    }
    // D10 measured without side effects when the steps end (member-runs still refusing, every cause)
    count_open_refusals(&mut w, &ms, false);
    // quiescence: pending $out jobs run; entries held by no live member are lost with the vanished members; every
    // other entry is delivered to every live member in stamp order
    w.step += 1;
    w.steps_end = w.step;
    w.quiescing = true;
    for mut j in jobs {
        let live = ms.iter().filter(|m| !m.vanished).count();
        ms[j.writer].do_bind(&mut w, &mut j, live);
    }
    let live: Vec<usize> = (0..ms.len()).filter(|&i| !ms[i].vanished).collect();
    for &i in &live {
        finish_joins(&mut ms, i, &mut w, cfg.restart_joins);
    }
    w.lost =
        w.all.iter().filter(|e| !live.iter().any(|&i| ms[i].holds(e.idx))).map(|e| e.idx).collect();
    for _ in 0..12 {
        for &i in &live {
            finish_joins(&mut ms, i, &mut w, cfg.restart_joins);
        }
        for &i in &live {
            let mut order: Vec<EntryRef> =
                w.all.iter().filter(|e| !w.lost.contains(&e.idx)).cloned().collect();
            order.sort_by_key(|e| e.stamp);
            for e in &order {
                take_quiesce(&mut ms, i, &mut w, e);
            }
        }
        for &i in &live {
            ms[i].settle(&mut w);
        }
        if live.iter().all(|&i| w.all.iter().all(|e| w.lost.contains(&e.idx) || ms[i].holds(e.idx)))
        {
            break;
        }
    }
    w.k_final = true;
    for &i in &live {
        ms[i].settle(&mut w);
        ms[i].check_inv(&w);
    }
    count_open_refusals(&mut w, &ms, true); // ... and at the end of the run, after the end-of-run delivery
    let res = finish(&mut w, &mut ms);
    RunOut { res, w, ms }
}

/// A configuration of the product routes (`doc_hold`, `doc_repair`; the model's routes off): the B1 classification's seeds are named
/// this way (`<policy> m/o/k doc_hold doc_repair f seed`).
pub fn product_cfg(
    policy: Policy,
    (members, outs, keys): (usize, usize, usize),
    doc_hold: bool,
    doc_repair: bool,
    f: f64,
) -> Cfg {
    let mut c = Cfg::new(members, outs, keys);
    c.policy = policy;
    c.routes = Some(Routes { f, hold: false, repair: false, doc_hold, doc_repair });
    c
}

/// One run of the product routes at one configuration.
#[allow(clippy::too_many_arguments)]
pub fn run_product(
    mode: &str,
    policy: Policy,
    mok: (usize, usize, usize),
    doc_hold: bool,
    doc_repair: bool,
    f: f64,
    seed: u64,
    trace: bool,
) -> RunOut {
    run(mode, "v6", seed, product_cfg(policy, mok, doc_hold, doc_repair, f), trace)
}

// ----------------------------------------------------------------------------- batch

/// Counters summed over runs (the model's `SUMS`); every other key counts the runs where it is non-zero.
pub const SUMS: [&str; 22] = [
    "cnt_register_events_above_grant",
    "cnt_register_events_not_carried_above_grant",
    "cnt_register_events_read_above_grant",
    "cnt_bind_between_first_and_last_page",
    "heads_required",
    "true_late_writes_allowed",
    "nonheads_unreported",
    "nonheads_unreported_vanished_origin",
    "nonheads_reported",
    "cnt_appended_in_position",
    "cnt_witnessed_without_receiving",
    "cnt_state_held_refusals",
    "cnt_snap_deletes_carried_with_id",
    "cnt_windows_served_by_a_join_in_progress",
    "cnt_lww_losers_not_kept",
    "cnt_witnessed_at_quiescence",
    "cnt_snap_versions_as_entries",
    "vr_replicated_late_writes",
    "vr_unreplicated_late_writes",
    "late_write_count",
    "refused_count",
    "pending_acked_lost",
];

#[derive(Default, Clone, Debug)]
pub struct Summary {
    pub runs: u64,
    pub tot: BTreeMap<String, i64>,
    pub first: BTreeMap<String, u64>,
    /// runs where an unreported non-head late write did NOT belong to a vanished origin
    pub nonhead_not_vanished: i64,
}

impl Summary {
    pub fn add(&mut self, seed: u64, res: &Res) {
        self.runs += 1;
        for (k, v) in res {
            if SUMS.contains(&k.as_str())
                || k.starts_with("cnt_")
                || k.starts_with("accepted_")
                || k.starts_with("refused_")
            {
                *self.tot.entry(k.clone()).or_insert(0) += v;
            } else if *v != 0 {
                *self.tot.entry(k.clone()).or_insert(0) += 1;
                self.first.entry(k.clone()).or_insert(seed);
            }
        }
        if res.get("nonheads_unreported") != res.get("nonheads_unreported_vanished_origin") {
            self.nonhead_not_vanished += 1;
        }
    }
    pub fn merge(&mut self, o: &Summary) {
        self.runs += o.runs;
        for (k, v) in &o.tot {
            *self.tot.entry(k.clone()).or_insert(0) += v;
        }
        for (k, s) in &o.first {
            let e = self.first.entry(k.clone()).or_insert(*s);
            *e = (*e).min(*s);
        }
        self.nonhead_not_vanished += o.nonhead_not_vanished;
    }
    pub fn get(&self, k: &str) -> i64 {
        self.tot.get(k).copied().unwrap_or(0)
    }
    /// The checks that must be zero on the base: name -> number of runs (or the count) that violated it.
    pub fn problems(&self) -> BTreeMap<String, (i64, u64)> {
        let mut p = BTreeMap::new();
        let keys = [
            "diverged",
            "div_state",
            "div_loss",
            "div_replay",
            "div_replay_interleaved",
            "q7_records_diverged",
            "oracle_state",
            "served_mismatch",
            "alive_mismatch",
            "loss_mismatch",
            "loss_missing",
            "loss_extra",
            "loss_body_mismatch",
            "loss_missing_unexplained",
            "k_exceeded",
            "invariant",
        ];
        for k in keys {
            if self.get(k) != 0 {
                p.insert(k.to_string(), (self.get(k), self.first.get(k).copied().unwrap_or(0)));
            }
        }
        for (k, v) in &self.tot {
            if k.starts_with("inv_") {
                p.insert(k.clone(), (*v, self.first.get(k).copied().unwrap_or(0)));
            }
        }
        if self.nonhead_not_vanished != 0 {
            p.insert(
                "nonhead_unreported_of_a_live_origin".to_string(),
                (self.nonhead_not_vanished, 0),
            );
        }
        p
    }
}

/// members x outs x keys, the model's `full` grid.
pub fn grid(members: &[usize], outs: &[usize], keys: &[usize]) -> Vec<Cfg> {
    let mut v = Vec::new();
    for &m in members {
        for &o in outs {
            for &k in keys {
                v.push(Cfg::new(m, o, k));
            }
        }
    }
    v
}

/// Run `seeds` seeds of every configuration on `threads` threads.
pub fn run_grid(
    mode: &str,
    base: &str,
    cfgs: &[Cfg],
    seed0: u64,
    seeds: u64,
    threads: usize,
) -> Summary {
    let jobs: Vec<(Cfg, u64)> =
        cfgs.iter().flat_map(|c| (seed0..seed0 + seeds).map(move |s| (*c, s))).collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let out = std::sync::Mutex::new(Summary::default());
    std::thread::scope(|sc| {
        for _ in 0..threads.max(1) {
            sc.spawn(|| {
                let mut local = Summary::default();
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if i >= jobs.len() {
                        break;
                    }
                    let (c, s) = jobs[i];
                    let r = run(mode, base, s, c, false);
                    local.add(s, &r.res);
                }
                out.lock().unwrap().merge(&local);
            });
        }
    });
    out.into_inner().unwrap()
}

/// The product-route grid with every run classified (`oracle::product_classes`): the summary, the runs per named class, and every
/// run with a problem no class explains (`<cfg> seed <s>: <residual>`).
pub fn run_grid_classified(
    mode: &str,
    cfgs: &[Cfg],
    seed0: u64,
    seeds: u64,
    threads: usize,
) -> (Summary, BTreeMap<String, i64>, Vec<String>) {
    let jobs: Vec<(Cfg, u64)> =
        cfgs.iter().flat_map(|c| (seed0..seed0 + seeds).map(move |s| (*c, s))).collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let out = std::sync::Mutex::new((Summary::default(), BTreeMap::new(), Vec::new()));
    std::thread::scope(|sc| {
        for _ in 0..threads.max(1) {
            sc.spawn(|| {
                let mut local =
                    (Summary::default(), BTreeMap::<String, i64>::new(), Vec::<String>::new());
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if i >= jobs.len() {
                        break;
                    }
                    let (c, s) = jobs[i];
                    let r = run(mode, "v6", s, c, false);
                    local.0.add(s, &r.res);
                    let (classes, residual) =
                        crate::oracle::product_classes(&r.res, c.policy == Policy::Field);
                    for k in classes {
                        *local.1.entry(k.to_string()).or_insert(0) += 1;
                    }
                    if !residual.is_empty() {
                        let rt = c.routes.unwrap_or(Routes {
                            f: 0.0,
                            hold: false,
                            repair: false,
                            doc_hold: false,
                            doc_repair: false,
                        });
                        local.2.push(format!(
                            "{:?} {}/{}/{} doc_hold {} doc_repair {} f {} seed {s}: {residual:?}",
                            c.policy, c.members, c.outs, c.keys, rt.doc_hold, rt.doc_repair, rt.f
                        ));
                    }
                }
                let mut o = out.lock().unwrap();
                o.0.merge(&local.0);
                for (k, v) in local.1 {
                    *o.1.entry(k).or_insert(0) += v;
                }
                o.2.extend(local.2);
            });
        }
    });
    let (s, c, mut r) = out.into_inner().unwrap();
    r.sort();
    (s, c, r)
}
