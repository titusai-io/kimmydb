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
                ms[m].local_doc(&mut w, key, op);
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
                let joining: Vec<usize> =
                    (0..ms.len()).filter(|&i| ms[i].join.is_some() && !ms[i].vanished).collect();
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
                if let Some(e) = d.next_window_entry(&w, s) {
                    d.take_from(&mut w, s, &e, false);
                }
            }
        }
        for m in ms.iter_mut() {
            m.check_inv();
        }
    }
    // quiescence: pending $out jobs run; entries held by no live member are lost with the vanished members; every
    // other entry is delivered to every live member in stamp order
    w.step += 1;
    for mut j in jobs {
        let live = ms.iter().filter(|m| !m.vanished).count();
        ms[j.writer].do_bind(&mut w, &mut j, live);
    }
    let live: Vec<usize> = (0..ms.len()).filter(|&i| !ms[i].vanished).collect();
    for &i in &live {
        while ms[i].join.is_some() {
            land_page_in(&mut ms, i, &mut w);
        }
    }
    w.lost =
        w.all.iter().filter(|e| !live.iter().any(|&i| ms[i].holds(e.idx))).map(|e| e.idx).collect();
    for _ in 0..12 {
        for &i in &live {
            while ms[i].join.is_some() {
                land_page_in(&mut ms, i, &mut w);
            }
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
    for &i in &live {
        ms[i].settle(&mut w);
        ms[i].check_inv();
    }
    let res = finish(&mut w, &mut ms);
    RunOut { res, w, ms }
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
            "oracle_state",
            "served_mismatch",
            "alive_mismatch",
            "loss_mismatch",
            "loss_missing",
            "loss_extra",
            "loss_body_mismatch",
            "loss_missing_unexplained",
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
