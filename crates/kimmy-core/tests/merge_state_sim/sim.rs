//! The cluster pull protocol (pullmodel5.py): nodes with an oplog, servable and
//! witnessed vectors keyed by (node, epoch), ADR-224 holes, threshold pulls with
//! truncated windows, snapshots, backup and restore, fixpoint, redeliver and
//! settle, and the random run.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use kimmy_core::hlc::Hlc;
use kimmy_core::ids::NodeId;
use kimmy_core::merge_state::{
    Epoch, JoinOptions, LifeKind, LossKey, LossKind, LossValue, MStamp, MergeState, Path, Received,
    Value, Writer, WriterId, Written, join,
};

use crate::oracle::Log;

/// A one-segment path: the sim's fields are flat.
pub fn p(x: &str) -> Path {
    Path::flat(x).expect("a valid field name")
}
/// The fields of a flat document as paths.
pub fn fl(f: &[(&str, Value)]) -> Vec<(Path, Value)> {
    f.iter().map(|(k, v)| (p(k), v.clone())).collect()
}
use crate::rng::Rng;

/// A vector keyed by origin: `(node, epoch)`, or `(node, 0)` when the
/// coverage is keyed by node only (the V3M2 mutant).
pub type Vec2 = BTreeMap<WriterId, Hlc>;

/// Mutation switches of the protocol and of the loss reporting that sit
/// outside `JoinOptions`. The default is the design.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SimFlags {
    /// V3M2: the oplog origin and the coverage vectors are (node, epoch).
    pub epoch_keyed: bool,
    /// ADR-224 holes on a snapshot grant (V3M4).
    pub holes: bool,
    /// D10: a snapshot join that detects a loss appends its entry (V3m1).
    pub snapshot_loss_entries: bool,
    /// Mutant for V4M2: the epoch is a counter kept in the store, which the backup carries.
    pub epoch_from_backup: bool,
    /// Mutant: no join appends a loss entry.
    pub no_loss_entries: bool,
    /// Mutant: losses of this kind are never detected.
    pub drop_kind: Option<LossKind>,
    /// Mutant V7m4: a superseded and killed first insert is reported neither way.
    pub report_neither: bool,
    /// Mutant V4M1: hidden increments are reported as once-only replicated records
    /// again (v4's behaviour). `LossKind` has no hidden kind, so the stand-in is
    /// `RetiredWriter`, which P0 never produces.
    pub hidden_records: bool,
}

impl Default for SimFlags {
    fn default() -> Self {
        Self {
            epoch_keyed: true,
            holes: true,
            snapshot_loss_entries: true,
            epoch_from_backup: false,
            no_loss_entries: false,
            drop_kind: None,
            report_neither: false,
            hidden_records: false,
        }
    }
}

/// Everything a run needs to know about the cluster's configuration.
#[derive(Clone, Debug)]
pub struct Cfg {
    pub opts: JoinOptions,
    pub flags: SimFlags,
}

impl Cfg {
    pub fn new(retention: u64) -> Self {
        Cfg {
            opts: JoinOptions::new(retention, i64::MAX).expect("a valid bound"),
            flags: SimFlags::default(),
        }
    }
    pub fn default_ret() -> Self {
        Self::new(1_000_000_000)
    }
}

pub fn nid(c: char) -> NodeId {
    NodeId::from_bytes([c as u8; 16])
}

/// An oplog entry: a writer entry carries the writer's full state (and the
/// oracle-only set of contained writes), a loss entry one loss record.
pub enum Ent {
    W { st: MStamp, state: MergeState, contained: BTreeSet<MStamp> },
    L { st: MStamp, key: LossKey, val: LossValue },
}

impl Ent {
    fn st(&self) -> MStamp {
        match self {
            Ent::W { st, .. } | Ent::L { st, .. } => *st,
        }
    }
}

struct Saved {
    state: MergeState,
    contained: BTreeSet<MStamp>,
    oplog: BTreeMap<MStamp, Rc<Ent>>,
    servable: Vec2,
    hole: Vec2,
    epoch_ctr: u64,
}

pub struct Node {
    pub name: char,
    pub w: Writer,
    /// ORACLE ONLY: the writes contained in the state (`State.contained`).
    pub contained: BTreeSet<MStamp>,
    pub oplog: BTreeMap<MStamp, Rc<Ent>>,
    pub servable: Vec2,
    pub witnessed: Vec2,
    pub hole: Vec2,
    pub snaps: u32,
    pub events: Vec<LossKey>,
    pub restored: bool,
    pub flags: SimFlags,
    epoch_ctr: u64,
    epoch_rng: Rng,
    saved: Option<Saved>,
}

fn raise(vec: &mut Vec2, o: WriterId, h: Hlc) {
    if h > vec.get(&o).copied().unwrap_or(Hlc::ZERO) {
        vec.insert(o, h);
    }
}

fn get(vec: &Vec2, o: &WriterId) -> Hlc {
    vec.get(o).copied().unwrap_or(Hlc::ZERO)
}

impl Node {
    pub fn new(name: char, cfg: &Cfg) -> Self {
        let id = WriterId { node: nid(name), epoch: Epoch::from_u64(0) };
        Node {
            name,
            w: Writer::new(id, cfg.opts.clone()),
            contained: BTreeSet::new(),
            oplog: BTreeMap::new(),
            servable: Vec2::new(),
            witnessed: Vec2::new(),
            hole: Vec2::new(),
            snaps: 0,
            events: Vec::new(),
            restored: false,
            flags: cfg.flags,
            epoch_ctr: 0,
            epoch_rng: Rng::new(20_261_031 ^ u64::from(name as u32)),
            saved: None,
        }
    }

    /// `node(st)`: the origin a stamp counts under.
    fn origin(&self, st: MStamp) -> WriterId {
        if self.flags.epoch_keyed {
            st.writer
        } else {
            WriterId { node: st.writer.node, epoch: Epoch::default() }
        }
    }

    pub fn ret(&self) -> u64 {
        self.w.opts.retention
    }

    pub fn bound(&self) -> i64 {
        self.w.opts.int_bound()
    }

    pub fn state(&self) -> &MergeState {
        &self.w.state
    }

    pub fn canon(&self) -> Vec<u8> {
        self.w.state.canonical_bytes()
    }

    fn append(&mut self, ent: Rc<Ent>) {
        let st = ent.st();
        self.oplog.insert(st, ent);
        let o = self.origin(st);
        raise(&mut self.servable, o, st.hlc);
        raise(&mut self.witnessed, o, st.hlc);
    }

    /// The model's `finish` tail: the writer already holds the normalised state;
    /// log the write with its causal past and append its entry.
    pub fn logged(&mut self, wr: Option<Written>, log: &mut Log) -> Option<MStamp> {
        let wr = wr?;
        let st = wr.stamp;
        log.insert(st, (self.contained.clone(), wr.record));
        self.contained.insert(st);
        let ent = Ent::W { st, state: self.w.state.clone(), contained: self.contained.clone() };
        self.append(Rc::new(ent));
        Some(st)
    }

    /// The join as this node's cluster runs it (a mutant may wrap it).
    fn receive(&mut self, other: &MergeState) -> Received {
        let f = self.flags;
        if f.drop_kind.is_none() && !f.report_neither && !f.hidden_records {
            return self.w.receive_report(other);
        }
        let ret = self.ret();
        let (mut merged, mut fresh) = join(&self.w.state, other, &self.w.opts);
        if let Some(kind) = f.drop_kind {
            // no <kind> detection
            let gone: Vec<LossKey> = merged
                .losses
                .keys()
                .filter(|k| {
                    k.kind == kind
                        && !self.w.state.losses.contains_key(*k)
                        && !other.losses.contains_key(*k)
                })
                .cloned()
                .collect();
            for k in gone {
                merged.losses.remove(&k);
            }
            fresh.retain(|k| k.kind != kind);
        }
        if f.report_neither {
            // V7m4
            let first: Vec<MStamp> = merged
                .lives
                .iter()
                .filter(|(_, v)| v.kind == LifeKind::Insert && v.seen.is_empty())
                .map(|(l, _)| *l)
                .collect();
            let newest = first.iter().max().copied();
            let killed: BTreeSet<MStamp> = first
                .into_iter()
                .filter(|l| Some(*l) != newest)
                .filter(|l| {
                    merged.deletes.keys().any(|d| {
                        !merged.lives[l].seen.contains(d)
                            && i128::from(d.hlc.wall_ms)
                                >= i128::from(l.hlc.wall_ms) - i128::from(ret)
                    })
                })
                .collect();
            let gone: Vec<LossKey> = merged
                .losses
                .keys()
                .filter(|k| {
                    killed.contains(&k.stamp)
                        && matches!(k.kind, LossKind::InsertLost | LossKind::DeleteWon)
                })
                .cloned()
                .collect();
            for k in gone {
                merged.losses.remove(&k);
                fresh.remove(&k);
            }
        }
        if f.hidden_records {
            let h = merged.horizon(&self.w.opts);
            let al: BTreeSet<MStamp> = merged.alive(&self.w.opts).into_iter().collect();
            let mut add = Vec::new();
            for src in [&self.w.state, other] {
                for ((l, p, _, gn), v) in &src.slots {
                    if !al.contains(l) {
                        continue;
                    }
                    let Some(g) = merged.gen_of(p, &self.w.opts) else { continue };
                    let older = *gn != Some(g.stamp) && gn.is_none_or(|x| x < g.stamp);
                    let covered =
                        g.vv.get(&v.stamp.writer).copied().unwrap_or(Hlc::ZERO) >= v.stamp.hlc;
                    let key =
                        LossKey { kind: LossKind::RetiredWriter, path: p.clone(), stamp: v.stamp };
                    if older && !covered && key.stamp.hlc >= h && !merged.losses.contains_key(&key)
                    {
                        add.push((key, LossValue::Total(v.total)));
                    }
                }
            }
            for (key, val) in add {
                if !self.w.state.losses.contains_key(&key) {
                    fresh.insert(key.clone());
                }
                merged.losses.insert(key, val);
            }
        }
        self.w.clock = merged.vv.values().copied().chain([self.w.clock]).max().unwrap_or(Hlc::ZERO);
        let changed = merged.canonical_bytes() != self.w.state.canonical_bytes();
        self.w.state = merged;
        Received { changed, fresh: fresh.into_iter().collect() }
    }

    fn absorb(
        &mut self,
        other: &MergeState,
        other_contained: &BTreeSet<MStamp>,
        rec: bool,
    ) -> bool {
        let r = self.receive(other);
        self.contained.extend(other_contained.iter().copied());
        let record = rec && !self.flags.no_loss_entries;
        for k in r.fresh {
            self.events.push(k.clone());
            if record {
                let st = self.w.tick(0);
                let val = self.w.state.losses[&k].clone();
                self.append(Rc::new(Ent::L { st, key: k, val }));
            }
        }
        r.changed
    }

    pub fn receive_entry(&mut self, ent: &Rc<Ent>) {
        match &**ent {
            Ent::W { st, state, contained } => {
                if self.absorb(state, contained, true) && !self.oplog.contains_key(st) {
                    self.append(Rc::clone(ent));
                }
            }
            Ent::L { key, val, .. } => {
                if self.w.receive_loss(key.clone(), val.clone()) {
                    self.events.push(key.clone());
                    self.append(Rc::clone(ent));
                }
            }
        }
        let st = ent.st();
        let o = self.origin(st);
        raise(&mut self.witnessed, o, st.hlc);
    }

    pub fn snapshot_from(&mut self, src: &Node) {
        self.snaps += 1;
        let rec = self.flags.snapshot_loss_entries;
        self.absorb(&src.w.state, &src.contained, rec);
        for (o, h) in &src.servable {
            if *h > get(&self.servable, o) {
                if self.flags.holes {
                    raise(&mut self.hole, *o, *h);
                }
                self.servable.insert(*o, *h);
            }
            raise(&mut self.witnessed, *o, *h);
        }
    }

    pub fn backup(&mut self) {
        self.saved = Some(Saved {
            state: self.w.state.clone(),
            contained: self.contained.clone(),
            oplog: self.oplog.clone(),
            servable: self.servable.clone(),
            hole: self.hole.clone(),
            epoch_ctr: self.epoch_ctr,
        });
    }

    pub fn restore(&mut self) -> bool {
        let Some(s) = self.saved.take() else { return false };
        self.w.state = s.state.clone();
        self.contained = s.contained.clone();
        self.oplog = s.oplog.clone();
        self.servable = s.servable.clone();
        self.hole = s.hole.clone();
        self.witnessed = s.servable.clone();
        let saved_ctr = s.epoch_ctr;
        self.saved = Some(s);
        let epoch = if self.flags.epoch_from_backup {
            // the mutant: a counter kept in the store, which a backup carries
            self.epoch_ctr = saved_ctr + 1;
            Epoch::from_u64(self.epoch_ctr)
        } else {
            // V4M2: the restore's start HLC plus a random 64 bits, never read from the store
            Epoch::new(self.w.clock, self.epoch_rng.next_u64().to_be_bytes())
        };
        self.w.id = WriterId { node: self.w.id.node, epoch };
        self.restored = true;
        true
    }

    // ---- local writes, logged ----------------------------------------------

    pub fn insert(&mut self, f: &[(&str, Value)], log: &mut Log, skew: u64) -> Option<MStamp> {
        let r = self.w.insert(&fl(f), skew);
        self.logged(r, log)
    }
    pub fn upsert_inc(&mut self, path: &str, n: i64, log: &mut Log, skew: u64) -> Option<MStamp> {
        let r = self.w.upsert_inc(&p(path), n, skew);
        self.logged(r, log)
    }
    /// A `$setOnInsert` upsert: two sequential writes by one writer (the creation
    /// and the increment), each logged with its own causal past and appended as
    /// its own oplog entry carrying the state right after it.
    pub fn upsert_soi(
        &mut self,
        fields: &[(&str, Value)],
        inc_path: &str,
        n: i64,
        sets: &[(&str, Value)],
        log: &mut Log,
        skew: u64,
    ) -> Option<MStamp> {
        let inc_path = p(inc_path);
        let created = self.w.upsert_soi_create(&fl(fields), &inc_path, &fl(sets), skew);
        let s1 = self.logged(created, log)?;
        let incd = self.w.upsert_inc_on(s1, &inc_path, n, skew);
        self.logged(incd, log)
    }
    pub fn set(&mut self, path: &str, v: Value, log: &mut Log, skew: u64) -> Option<MStamp> {
        let r = self.w.write_set(&p(path), v, skew);
        self.logged(r, log)
    }
    pub fn unset(&mut self, path: &str, log: &mut Log, skew: u64) -> Option<MStamp> {
        let r = self.w.write_unset(&p(path), skew);
        self.logged(r, log)
    }
    pub fn inc(&mut self, path: &str, n: i64, log: &mut Log, skew: u64) -> Option<MStamp> {
        let r = self.w.write_inc(&p(path), n, skew);
        self.logged(r, log)
    }
    pub fn replace(&mut self, d: &[(&str, Value)], log: &mut Log, skew: u64) -> Option<MStamp> {
        let r = self.w.write_replace(&fl(d), skew);
        self.logged(r, log)
    }
    pub fn delete(&mut self, log: &mut Log, skew: u64) -> Option<MStamp> {
        let r = self.w.write_delete(skew);
        self.logged(r, log)
    }
    pub fn visible(&self) -> Option<BTreeMap<Path, Value>> {
        self.w.visible()
    }
}

/// `(p, r)` as one mutable and one shared node.
fn two(ms: &mut [Node], p: usize, r: usize) -> (&mut Node, &Node) {
    assert_ne!(p, r);
    if p < r {
        let (a, b) = ms.split_at_mut(r);
        (&mut a[p], &b[0])
    } else {
        let (a, b) = ms.split_at_mut(p);
        (&mut b[0], &a[r])
    }
}

/// `p` pulls from `r`. True if anything was applied or witnessed.
pub fn pull(ms: &mut [Node], p: usize, r: usize, limit: Option<usize>) -> bool {
    if p == r {
        return false; // nothing is behind oneself
    }
    let (p, r) = two(ms, p, r);
    let behind: Vec<WriterId> =
        r.servable.iter().filter(|(o, h)| **h > get(&p.witnessed, o)).map(|(o, _)| *o).collect();
    if behind.is_empty() {
        return false;
    }
    let thr = behind.iter().map(|o| get(&p.witnessed, o)).min().unwrap();
    if r.servable
        .iter()
        .any(|(o, h)| get(&p.witnessed, o) < *h && get(&p.witnessed, o) < get(&r.hole, o))
    {
        p.snapshot_from(r); // BeyondHorizon (a hole): take the snapshot
        return true;
    }
    let mut window: Vec<(MStamp, Rc<Ent>)> =
        r.oplog.iter().filter(|(k, _)| k.hlc > thr).map(|(k, e)| (*k, Rc::clone(e))).collect();
    let exhausted = limit.is_none_or(|l| window.len() <= l);
    if !exhausted {
        window.truncate(limit.unwrap());
    }
    for (_, e) in &window {
        p.receive_entry(e);
    }
    if exhausted {
        for (o, h) in &r.servable {
            raise(&mut p.witnessed, *o, *h);
        }
    } else if let Some((end, _)) = window.last() {
        let eo = p.origin(*end);
        for (o, h) in &r.servable {
            let reach =
                if *o <= eo { end.hlc } else { Hlc::new(end.hlc.wall_ms.saturating_sub(1), 0) };
            raise(&mut p.witnessed, *o, (*h).min(reach));
        }
    }
    true
}

/// Overlapping windows re-send entries the receiver already holds (routine in
/// the cluster), in any order.
pub fn redeliver(ms: &mut [Node], order: i32) {
    for p in 0..ms.len() {
        for r in 0..ms.len() {
            if p != r {
                let mut es: Vec<Rc<Ent>> = ms[r].oplog.values().cloned().collect();
                if order < 0 {
                    es.reverse();
                }
                for e in &es {
                    ms[p].receive_entry(e);
                }
            }
        }
    }
}

fn canons(ms: &[Node]) -> Vec<Vec<u8>> {
    ms.iter().map(Node::canon).collect()
}

fn all_equal(ms: &[Node]) -> bool {
    let c = canons(ms);
    c.iter().all(|x| *x == c[0])
}

/// Beyond-retention regime (ADR-085): redeliver in both orders, repair by
/// snapshot where members differ, repeat. Returns the rounds needed, or `None`
/// when the state is still moving (a flip-flop).
pub fn settle(ms: &mut [Node], rounds: usize) -> Option<usize> {
    for n in 0..rounds {
        redeliver(ms, 1);
        let mid = canons(ms);
        redeliver(ms, -1);
        fixpoint(ms, 40);
        if !all_equal(ms) {
            for p in 0..ms.len() {
                for r in 0..ms.len() {
                    if p != r {
                        let (a, b) = two(ms, p, r);
                        a.snapshot_from(b);
                    }
                }
            }
            fixpoint(ms, 40);
        }
        if all_equal(ms) && canons(ms) == mid {
            return Some(n);
        }
    }
    None
}

/// What a fixpoint round compares: every state and every witnessed vector.
pub type FixKey = Vec<(Vec<u8>, Vec<(WriterId, Hlc)>)>;

pub fn fix_key(ms: &[Node]) -> FixKey {
    ms.iter().map(|m| (m.canon(), m.witnessed.iter().map(|(o, h)| (*o, *h)).collect())).collect()
}

pub fn fixpoint(ms: &mut [Node], rounds: usize) -> bool {
    for _ in 0..rounds {
        let before = fix_key(ms);
        for p in 0..ms.len() {
            for r in 0..ms.len() {
                if p != r {
                    pull(ms, p, r, None);
                }
            }
        }
        if fix_key(ms) == before {
            return true;
        }
    }
    false
}

#[derive(Clone, Debug)]
pub struct RunConfig {
    pub steps: usize,
    pub snap_rate: f64,
    pub restore: bool,
    /// `None`: the model's default (10^9, far beyond any horizon).
    pub retention: Option<u64>,
    pub sequential: bool,
    /// `None`: the real Int64 bound. A small bound exercises the oracle's clamp.
    pub int_bound: Option<i64>,
}

impl Default for RunConfig {
    fn default() -> Self {
        RunConfig {
            steps: 18,
            snap_rate: 0.06,
            restore: true,
            retention: None,
            sequential: false,
            int_bound: None,
        }
    }
}

pub struct RunOut {
    pub nodes: Vec<Node>,
    pub log: Log,
    /// The final fixpoint was reached.
    pub settled: bool,
}

const OPS: [&str; 14] = [
    "inc",
    "inc",
    "inc",
    "inc_zero_sum",
    "set",
    "set",
    "unset",
    "delete",
    "insert",
    "replace",
    "upsert",
    "upsert_soi",
    "upsert_soi",
    "upsert",
];

/// A value from the wide pool (M3): small and large integers, the Int64 bounds, strings
/// (also empty and non-ASCII) and doubles (also `2.0`, `-0.0`, `0.0`, a huge one).
fn pool_value(rnd: &mut Rng) -> Value {
    match rnd.below(16) {
        0 => Value::Int64(0),
        1 => Value::Int64(7),
        2 => Value::Int64(8),
        3 => Value::Int64(500),
        4 => Value::Int64(-7),
        5 => Value::Int64(1 << 62),
        6 => Value::Int64(-(1 << 62)),
        7 => Value::Int64(i64::MAX),
        8 => Value::Int64(i64::MIN),
        9 => Value::String("s".into()),
        10 => Value::String(String::new()),
        11 => Value::String("\u{fc}".into()),
        12 => Value::Double(2.5),
        13 => Value::Double(2.0),
        14 => Value::Double(-0.0),
        _ => Value::Double(1e300),
    }
}

/// An increment from the wide pool: small, zero, negative, and near the bounds.
fn pool_inc(rnd: &mut Rng) -> i64 {
    match rnd.below(14) {
        0 => 1,
        1 => 2,
        2 => 5,
        3 => 20,
        4 => -3,
        5 => -1,
        6 => 0,
        7 => 1 << 62,
        8 => -(1 << 62),
        9 => i64::MAX,
        10 => i64::MIN,
        11 => 900,
        12 => 100,
        _ => -100,
    }
}

/// One to three distinct fields with pool values.
fn pool_fields<'a>(rnd: &mut Rng, names: &[&'a str]) -> Vec<(&'a str, Value)> {
    let n = 1 + rnd.below(3);
    let mut out: Vec<(&str, Value)> = Vec::new();
    for _ in 0..n {
        let k = rnd.choice(names);
        if !out.iter().any(|(q, _)| *q == k) {
            out.push((k, pool_value(rnd)));
        }
    }
    out
}

/// One random write on `m` (the op table of `run`).
fn random_write(rnd: &mut Rng, m: &mut Node, log: &mut Log, skew: u64) {
    let op = rnd.choice(&OPS);
    let p = rnd.choice(&["x", "n", "y"]);
    match op {
        "insert" => {
            let f = pool_fields(rnd, &["x", "y", "n"]);
            m.insert(&f, log, skew);
        }
        "upsert" => {
            let k = rnd.choice(&["x", "y", "n"]);
            m.upsert_inc(k, pool_inc(rnd), log, skew);
        }
        "upsert_soi" => {
            let f = pool_fields(rnd, &["y", "z", "n"]);
            let n = pool_inc(rnd);
            // `$set`, `$setOnInsert` and `$inc` on one path conflict (the update is refused
            // up front), so the three path lists are disjoint.
            let mut sets: Vec<(&str, Value)> =
                if rnd.below(2) == 0 { vec![] } else { pool_fields(rnd, &["n", "x", "w"]) };
            sets.retain(|(q, _)| !f.iter().any(|(g, _)| g == q));
            let free: Vec<&str> = ["x", "y", "n", "z", "u"]
                .into_iter()
                .filter(|q| !f.iter().chain(sets.iter()).any(|(g, _)| g == q))
                .collect();
            let ip = rnd.choice(&free);
            m.upsert_soi(&f, ip, n, &sets, log, skew);
        }
        "replace" => {
            let d = pool_fields(rnd, &["x", "n", "y"]);
            m.replace(&d, log, skew);
        }
        "inc" => {
            m.inc(p, pool_inc(rnd), log, skew);
        }
        "inc_zero_sum" => {
            // +n then -n on one writer: a counter that returns to its start.
            let n = pool_inc(rnd);
            if m.inc(p, n, log, skew).is_some() && n != i64::MIN {
                m.inc(p, -n, log, skew);
            }
        }
        "set" => {
            let v = pool_value(rnd);
            m.set(p, v, log, skew);
        }
        _ => {
            if op == "unset" {
                m.unset(p, log, skew);
            } else {
                m.delete(log, skew);
            }
        }
    }
}

/// One random pull (or a snapshot standing in for retention's BeyondHorizon).
fn random_pull(rnd: &mut Rng, ms: &mut [Node], snap_rate: f64) {
    let (p, r) = rnd.sample2(ms.len());
    if rnd.f64() < snap_rate {
        let (a, b) = two(ms, p, r);
        a.snapshot_from(b);
    } else {
        let limit = rnd.choice(&[None, None, Some(1), Some(2)]);
        pull(ms, p, r, limit);
    }
}

pub fn new_cluster(rnd: &mut Rng, cfg: &Cfg, log: &mut Log) -> Vec<Node> {
    let n = 3 + rnd.below(2);
    let mut ms: Vec<Node> = "ABCD".chars().take(n).map(|c| Node::new(c, cfg)).collect();
    ms[0].insert(&[("x", Value::Int64(100)), ("n", Value::String("a".into()))], log, 0);
    for m in &mut ms {
        m.backup();
    }
    ms
}

/// The restore step: a peer pulls the target first (its writes exist somewhere
/// before the restore), then the target is restored.
fn random_restore(rnd: &mut Rng, ms: &mut [Node]) {
    let tgt = rnd.below(ms.len());
    let mut other = rnd.below(ms.len() - 1);
    if other >= tgt {
        other += 1;
    }
    pull(ms, other, tgt, None);
    ms[tgt].restore();
}

pub fn run(seed: u64, rc: &RunConfig, cfg: &Cfg) -> RunOut {
    let mut rnd = Rng::new(seed);
    let mut cfg = cfg.clone();
    cfg.opts.retention = rc.retention.unwrap_or(1_000_000_000);
    if let Some(b) = rc.int_bound {
        cfg.opts.set_int_bound(b).expect("a valid bound");
    }
    let mut log = Log::new();
    let mut ms = new_cluster(&mut rnd, &cfg, &mut log);
    let mut did_restore = false;
    for i in 0..rc.steps {
        let m = rnd.below(ms.len());
        let skew = if rc.sequential { 0 } else { rnd.choice(&[0, 0, 0, 4]) };
        let x = if rc.sequential { 1.0 } else { rnd.f64() };
        if rc.sequential {
            fixpoint(&mut ms, 40);
        }
        if x < 0.45 {
            random_pull(&mut rnd, &mut ms, rc.snap_rate);
            continue;
        }
        if rc.restore && !rc.sequential && !did_restore && i > rc.steps / 2 && rnd.f64() < 0.15 {
            random_restore(&mut rnd, &mut ms);
            did_restore = true;
            continue;
        }
        random_write(&mut rnd, &mut ms[m], &mut log, skew);
    }
    let settled = fixpoint(&mut ms, 40);
    RunOut { nodes: ms, log, settled }
}

/// V4m3: truly inside retention: every member pulls to a fixpoint every
/// `sync_every` steps, so nobody trails by more than the clock moves between two
/// fixpoints. Returns the run and the largest clock advance between fixpoints.
pub fn run_bounded(
    seed: u64,
    cfg: &Cfg,
    ret: u64,
    sync_every: usize,
    steps: usize,
) -> (RunOut, u64) {
    let mut rnd = Rng::new(seed);
    let mut cfg = cfg.clone();
    cfg.opts.retention = ret;
    let mut log = Log::new();
    let mut ms = new_cluster(&mut rnd, &cfg, &mut log);
    let (mut did, mut gap, mut last) = (false, 0u64, 0u64);
    for i in 0..steps {
        if i % sync_every == 0 {
            fixpoint(&mut ms, 40);
            let top = ms.iter().map(|m| m.w.clock.wall_ms).max().unwrap();
            gap = gap.max(top - last);
            last = top;
        }
        let m = rnd.below(ms.len());
        let skew = rnd.choice(&[0, 0, 0, 4]);
        if rnd.f64() < 0.45 {
            random_pull(&mut rnd, &mut ms, 0.06);
            continue;
        }
        if !did && i > steps / 2 && rnd.f64() < 0.15 {
            random_restore(&mut rnd, &mut ms);
            did = true;
            continue;
        }
        random_write(&mut rnd, &mut ms[m], &mut log, skew);
    }
    let settled = fixpoint(&mut ms, 40);
    (RunOut { nodes: ms, log, settled }, gap)
}
