//! Unit tests: the model's named probes (probes5.py, model5.py), on writer
//! entries delivered through a small oplog harness that mirrors the model's
//! `Member`.
//!
//! The pull-protocol probes (`probe_V4M2_epoch_twice`, targeted5.py) belong to
//! the protocol harness and are not ported here. The model's oracle checks
//! (`oracle`, `loss_oracle`, `hidden_oracle`, `stale_records`) are asserted by
//! their documented expected values instead.

use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::hlc::Hlc;
use crate::ids::NodeId;

fn nid(c: char) -> NodeId {
    NodeId::from_bytes([c as u8; 16])
}
fn wid(c: char, epoch: u64) -> WriterId {
    WriterId { node: nid(c), epoch: Epoch::from_u64(epoch) }
}
fn h(n: u64) -> Hlc {
    Hlc::new(n, 0)
}
fn i(n: i64) -> Value {
    Value::Int64(n)
}
fn s(x: &str) -> Value {
    Value::String(x.to_string())
}
fn f(x: f64) -> Value {
    Value::Double(x)
}
/// A one-segment path.
fn p(x: &str) -> Path {
    Path::flat(x).unwrap()
}
/// The fields of a flat document as paths.
fn fl(f: &[(&str, Value)]) -> Vec<(Path, Value)> {
    f.iter().map(|(k, v)| (p(k), v.clone())).collect()
}
fn opts() -> JoinOptions {
    JoinOptions::default()
}
fn ret(r: u64) -> JoinOptions {
    JoinOptions::new(r, i64::MAX).unwrap()
}
fn doc(kv: &[(&str, Value)]) -> Option<BTreeMap<Path, Value>> {
    Some(kv.iter().map(|(k, v)| (p(k), v.clone())).collect())
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum OKey {
    W(MStamp),
    L(LossKey),
}

#[derive(Clone)]
enum Ent {
    W(MStamp, MergeState),
    L(LossKey, LossValue),
}

/// The model's `Member`: a writer with an oplog of entries and an event list.
struct Mem {
    name: char,
    epoch: u64,
    w: Writer,
    oplog: BTreeMap<OKey, Ent>,
    events: Vec<LossKey>,
    saved: Vec<(MergeState, BTreeMap<OKey, Ent>)>,
}

impl Mem {
    fn new(name: char, o: &JoinOptions) -> Self {
        Mem {
            name,
            epoch: 0,
            w: Writer::new(wid(name, 0), o.clone()),
            oplog: BTreeMap::new(),
            events: Vec::new(),
            saved: Vec::new(),
        }
    }
    fn log(&mut self, wr: Option<Written>) -> Option<MStamp> {
        let wr = wr?;
        self.oplog.insert(OKey::W(wr.stamp), Ent::W(wr.stamp, self.w.state.clone()));
        Some(wr.stamp)
    }
    fn ent(&self, st: MStamp) -> Ent {
        self.oplog[&OKey::W(st)].clone()
    }
    fn ins(&mut self, f: &[(&str, Value)], sk: u64) -> Option<MStamp> {
        let r = self.w.insert(&fl(f), sk);
        self.log(r)
    }
    fn upsert_inc(&mut self, path: &str, n: i64) -> Option<MStamp> {
        let r = self.w.upsert_inc(&p(path), n, 0);
        self.log(r)
    }
    /// Returns `(life, inc stamp)`: the creation write and the increment write.
    fn upsert_soi(
        &mut self,
        f: &[(&str, Value)],
        path: &str,
        n: i64,
        sets: &[(&str, Value)],
    ) -> Option<(MStamp, MStamp)> {
        let (c, i) = self.w.upsert_soi(&fl(f), &p(path), n, &fl(sets), 0)?;
        let life = c.stamp;
        let inc = i.stamp;
        self.log(Some(c));
        self.log(Some(i));
        Some((life, inc))
    }
    fn set(&mut self, path: &str, v: Value, sk: u64) -> Option<MStamp> {
        let r = self.w.write_set(&p(path), v, sk);
        self.log(r)
    }
    fn inc(&mut self, path: &str, n: i64, sk: u64) -> Option<MStamp> {
        let r = self.w.write_inc(&p(path), n, sk);
        self.log(r)
    }
    fn del(&mut self) -> Option<MStamp> {
        let r = self.w.write_delete(0);
        self.log(r)
    }
    fn absorb(&mut self, other: &MergeState, record: bool) -> bool {
        let r = self.w.receive_report(other);
        for k in r.fresh {
            self.events.push(k.clone());
            if record {
                let v = self.w.state.losses[&k].clone();
                self.oplog.entry(OKey::L(k.clone())).or_insert(Ent::L(k, v));
            }
        }
        r.changed
    }
    fn receive_entry(&mut self, e: &Ent) {
        match e {
            Ent::W(st, state) => {
                if self.absorb(state, true) {
                    self.oplog.entry(OKey::W(*st)).or_insert_with(|| e.clone());
                }
            }
            Ent::L(k, v) => {
                if self.w.receive_loss(k.clone(), v.clone()) {
                    self.events.push(k.clone());
                    self.oplog.entry(OKey::L(k.clone())).or_insert_with(|| e.clone());
                }
            }
        }
    }
    fn receive_snapshot(&mut self, other: &MergeState) {
        self.absorb(other, false);
    }
    fn restore(&mut self) {
        let (st, ol) = self.saved[0].clone();
        self.w.state = st;
        self.oplog = ol;
        self.epoch += 1;
        self.w.id = wid(self.name, self.epoch);
    }
    fn visible(&self) -> Option<BTreeMap<Path, Value>> {
        self.w.visible()
    }
    fn canon(&self) -> Vec<u8> {
        self.w.state.canonical_bytes()
    }
    fn loss_set(&self) -> BTreeSet<(LossKind, Path, MStamp)> {
        self.w.state.losses.keys().map(|k| (k.kind, k.path.clone(), k.stamp)).collect()
    }
}

fn fresh_observer(o: &JoinOptions, seed: &[Ent]) -> Mem {
    let mut m = Mem::new('O', o);
    for e in seed {
        m.receive_entry(e);
    }
    m
}

fn perms(n: usize) -> Vec<Vec<usize>> {
    fn go(cur: &mut Vec<usize>, used: &mut Vec<bool>, out: &mut Vec<Vec<usize>>) {
        if cur.len() == used.len() {
            out.push(cur.clone());
            return;
        }
        for k in 0..used.len() {
            if !used[k] {
                used[k] = true;
                cur.push(k);
                go(cur, used, out);
                cur.pop();
                used[k] = false;
            }
        }
    }
    let mut out = Vec::new();
    go(&mut Vec::new(), &mut vec![false; n], &mut out);
    out
}

/// Every delivery order of `ents` into a fresh observer seeded with `seed`.
fn all_orders(o: &JoinOptions, ents: &[Ent], seed: &[Ent]) -> Vec<(Vec<usize>, Mem)> {
    perms(ents.len())
        .into_iter()
        .map(|p| {
            let mut m = fresh_observer(o, seed);
            for &k in &p {
                m.receive_entry(&ents[k]);
            }
            (p, m)
        })
        .collect()
}

fn sync(ms: &mut [Mem]) {
    for _ in 0..12 {
        let before: Vec<_> = ms.iter().map(Mem::canon).collect();
        for r in 0..ms.len() {
            for s_ in 0..ms.len() {
                if r != s_ {
                    let es: Vec<Ent> = ms[s_].oplog.values().cloned().collect();
                    for e in &es {
                        ms[r].receive_entry(e);
                    }
                }
            }
        }
        if ms.iter().map(Mem::canon).collect::<Vec<_>>() == before {
            return;
        }
    }
}

fn pull_all(obs: &mut [Mem]) {
    for a in 0..obs.len() {
        for b in 0..obs.len() {
            let es: Vec<Ent> = obs[b].oplog.values().cloned().collect();
            for e in &es {
                obs[a].receive_entry(e);
            }
        }
    }
}

// ---- model5.py probes --------------------------------------------------------

#[test]
fn probe_p1_reinsert_same_field() {
    let o = opts();
    let (mut ii, mut a, mut b) = (Mem::new('I', &o), Mem::new('A', &o), Mem::new('B', &o));
    let i0 = ii.ins(&[("x", i(1))], 0).unwrap();
    a.receive_entry(&ii.ent(i0));
    b.receive_entry(&ii.ent(i0));
    let d = a.del().unwrap();
    let r = a.ins(&[("x", i(2))], 0).unwrap();
    b.w.clock = h(5);
    let z = b.set("x", i(3), 0).unwrap();
    for (ord, m) in all_orders(&o, &[a.ent(d), a.ent(r), b.ent(z)], &[ii.ent(i0)]) {
        assert_eq!(m.visible(), doc(&[("x", i(2))]), "{ord:?}");
    }
}

#[test]
fn probe_p1b_disjoint_fields() {
    let o = opts();
    let (mut ii, mut a, mut b) = (Mem::new('I', &o), Mem::new('A', &o), Mem::new('B', &o));
    let i0 = ii.ins(&[("x", i(1))], 0).unwrap();
    a.receive_entry(&ii.ent(i0));
    b.receive_entry(&ii.ent(i0));
    let d = a.del().unwrap();
    let r = a.ins(&[("y", i(2))], 0).unwrap();
    b.w.clock = h(5);
    let z = b.set("z", i(3), 0).unwrap();
    for (ord, m) in all_orders(&o, &[a.ent(d), a.ent(r), b.ent(z)], &[ii.ent(i0)]) {
        assert_eq!(m.visible(), doc(&[("y", i(2))]), "{ord:?}");
    }
}

#[test]
fn probe_p2_no_genesis_reuse_after_gc() {
    let o = ret(6);
    let (mut a, mut c) = (Mem::new('A', &o), Mem::new('C', &o));
    let i1 = a.ins(&[("x", i(1))], 0).unwrap();
    c.receive_entry(&a.ent(i1));
    let d = a.del().unwrap();
    c.receive_entry(&a.ent(d));
    assert_eq!(c.visible(), None);
    // A's retention pass: a join with a far-future state of its own prunes the delete and the dead life.
    let mut far = a.w.state.clone();
    far.vv.insert(wid('A', 0), h(40));
    a.absorb(&far, true);
    assert!(a.w.state.deletes.is_empty());
    a.w.clock = h(40);
    let n = a.ins(&[("x", i(9))], 0).unwrap();
    assert_eq!(a.visible(), doc(&[("x", i(9))]));
    // C still holds the delete (it has not run its retention yet).
    c.receive_entry(&a.ent(n));
    assert_eq!(c.visible(), doc(&[("x", i(9))]));
}

#[test]
fn probe_p4_delete_vs_inc() {
    let o = opts();
    let (mut a, mut b) = (Mem::new('A', &o), Mem::new('B', &o));
    let i0 = a.ins(&[("x", i(100))], 0).unwrap();
    b.receive_entry(&a.ent(i0));
    let d = a.del().unwrap();
    let inc = b.inc("x", 20, 0).unwrap();
    let mut res = BTreeSet::new();
    for (_, m) in all_orders(&o, &[a.ent(d), b.ent(inc)], &[a.ent(i0)]) {
        res.insert((
            format!("{:?}", m.visible()),
            m.w.state.losses.keys().cloned().collect::<Vec<_>>(),
        ));
    }
    assert_eq!(res.len(), 1, "{res:?}");
    let (vis, losses) = res.into_iter().next().unwrap();
    assert_eq!(vis, "None");
    assert!(losses.iter().any(|k| k.kind == LossKind::DeleteWon && k.path == p("x")));
}

#[test]
fn probe_p5_record_set_is_order_independent() {
    let o = opts();
    let (mut ii, mut a, mut b, mut c) =
        (Mem::new('I', &o), Mem::new('A', &o), Mem::new('B', &o), Mem::new('C', &o));
    let i0 = ii.ins(&[("x", i(0))], 0).unwrap();
    for m in [&mut a, &mut b, &mut c] {
        m.receive_entry(&ii.ent(i0));
    }
    let bb = b.set("x", s("b"), 0).unwrap();
    c.receive_entry(&b.ent(bb));
    let cc = c.set("x", s("c"), 0).unwrap();
    a.w.clock = h(5);
    let aa = a.set("x", s("a"), 0).unwrap();
    let mut obs: Vec<Mem> = all_orders(&o, &[b.ent(bb), c.ent(cc), a.ent(aa)], &[ii.ent(i0)])
        .into_iter()
        .map(|(_, m)| m)
        .collect();
    // Observers also pull the loss entries that the other observers created.
    pull_all(&mut obs);
    let fin: BTreeSet<_> = obs.iter().map(Mem::canon).collect();
    assert_eq!(fin.len(), 1, "record sets still differ after pulling loss entries");
}

#[test]
fn probe3_concurrent_lives_then_delete() {
    let o = opts();
    let (mut ii, mut a, mut b, mut c) =
        (Mem::new('I', &o), Mem::new('A', &o), Mem::new('B', &o), Mem::new('C', &o));
    let i0 = ii.ins(&[("q", i(0))], 0).unwrap();
    for m in [&mut a, &mut b, &mut c] {
        m.receive_entry(&ii.ent(i0));
    }
    let d0 = a.del().unwrap();
    b.receive_entry(&a.ent(d0));
    c.receive_entry(&a.ent(d0));
    let la = a.ins(&[("a", i(10))], 0).unwrap();
    b.w.clock = h(7);
    let lb = b.ins(&[("b", i(20))], 0).unwrap();
    c.receive_entry(&b.ent(lb));
    let d1 = c.del().unwrap();
    for (ord, m) in all_orders(&o, &[a.ent(d0), a.ent(la), b.ent(lb), c.ent(d1)], &[ii.ent(i0)]) {
        assert_eq!(m.visible(), None, "{ord:?}");
    }
}

#[test]
fn probe_oplogfaithful() {
    let o = opts();
    let names = ['A', 'B', 'C'];
    let mut cases = 0;
    for order in perms(3) {
        for seen_bits in 0..4u32 {
            for sk in 0..8u32 {
                let skews = [
                    u64::from(sk & 1) * 5,
                    u64::from((sk >> 1) & 1) * 5,
                    u64::from((sk >> 2) & 1) * 5,
                ];
                let mut ii = Mem::new('I', &o);
                let mut mem: Vec<Mem> = names.iter().map(|n| Mem::new(*n, &o)).collect();
                let i0 = ii.ins(&[("x", i(0))], 0).unwrap();
                for m in &mut mem {
                    m.receive_entry(&ii.ent(i0));
                }
                let mut stamps = Vec::new();
                for (idx, &n) in order.iter().enumerate() {
                    for j in 0..idx {
                        if (seen_bits >> j) & 1 == 1 {
                            let e = mem[order[j]].ent(stamps[j]);
                            mem[n].receive_entry(&e);
                        }
                    }
                    let v = s(&format!("{}{idx}", names[n]));
                    stamps.push(mem[n].set("x", v, skews[idx]).unwrap());
                }
                let ents: Vec<Ent> =
                    order.iter().enumerate().map(|(k, &n)| mem[n].ent(stamps[k])).collect();
                let mut obs: Vec<Mem> =
                    all_orders(&o, &ents, &[ii.ent(i0)]).into_iter().map(|(_, m)| m).collect();
                pull_all(&mut obs);
                let canons: BTreeSet<_> = obs.iter().map(Mem::canon).collect();
                assert_eq!(canons.len(), 1, "order {order:?} bits {seen_bits} skews {skews:?}");
                cases += 1;
            }
        }
    }
    assert_eq!(cases, 192);
}

#[test]
fn probe_gc_collected_record_does_not_return() {
    let o = ret(6);
    let mut ii = Mem::new('I', &o);
    let (mut a, mut b, mut c, mut d) =
        (Mem::new('A', &o), Mem::new('B', &o), Mem::new('C', &o), Mem::new('D', &o));
    let i0 = ii.ins(&[("x", i(0))], 0).unwrap();
    for m in [&mut a, &mut b, &mut c, &mut d] {
        m.receive_entry(&ii.ent(i0));
    }
    let aa = a.set("x", s("a"), 0).unwrap();
    let bb = b.set("x", s("b"), 2).unwrap();
    d.receive_entry(&a.ent(aa));
    d.receive_entry(&b.ent(bb));
    let dd = d.set("y", i(1), 0).unwrap(); // D's entry carries the loss record in its state
    assert!(!d.w.state.losses.is_empty());
    c.receive_entry(&a.ent(aa));
    c.receive_entry(&b.ent(bb));
    assert!(!c.w.state.losses.is_empty() && c.events.len() == 1);
    for n in 0..5 {
        // time passes: C advances far past retention
        c.set("z", i(n), 10);
    }
    let far = c.w.state.clone();
    c.absorb(&far, true);
    assert!(c.w.state.losses.is_empty(), "the record should be collected by the horizon");
    c.receive_entry(&d.ent(dd)); // an old entry carrying the record is resent
    assert!(c.events.len() == 1 && c.w.state.losses.is_empty(), "{:?}", c.events);
}

#[test]
fn probe_restore_conservation() {
    for epochs in [true, false] {
        let o = opts();
        let (mut a, mut b, mut c) = (Mem::new('A', &o), Mem::new('B', &o), Mem::new('C', &o));
        let i0 = a.ins(&[("x", i(100))], 0).unwrap();
        b.receive_entry(&a.ent(i0));
        c.receive_entry(&a.ent(i0));
        b.saved.push((b.w.state.clone(), b.oplog.clone()));
        let inc = b.inc("x", 30, 0).unwrap();
        a.receive_entry(&b.ent(inc));
        c.receive_entry(&b.ent(inc));
        b.restore();
        if !epochs {
            b.epoch = 0; // identity kept, no new epoch
            b.w.id = wid('B', 0);
        }
        b.inc("x", 5, 0).unwrap();
        let mut ms = [a, b, c];
        sync(&mut ms);
        let v = ms[0].w.state.value(&p("x"), &o);
        assert_eq!(v, Some(i(if epochs { 135 } else { 105 })), "epochs={epochs}"); // 105: the silent loss epochs prevent
    }
}

// ---- probes5.py ---------------------------------------------------------------

fn flipflop(horizon_first: bool) -> Vec<String> {
    // The model5 `Member` does not normalise its writes; the mutant is only visible on such states.
    let mut o = ret(4);
    o.set_horizon_first(horizon_first);
    o.set_normalize_writes(false);
    let (mut ii, mut a, mut c) = (Mem::new('I', &o), Mem::new('A', &o), Mem::new('C', &o));
    let i0 = ii.ins(&[("x", i(1))], 0).unwrap();
    a.receive_entry(&ii.ent(i0));
    c.receive_entry(&ii.ent(i0));
    let d = a.del().unwrap();
    c.w.clock = h(6);
    let c7 = c.set("y", i(5), 0).unwrap();
    a.w.clock = h(7);
    let a8 = a.ins(&[("z", i(1))], 0).unwrap();
    let mut obs = fresh_observer(&o, &[ii.ent(i0)]);
    let mut seq = Vec::new();
    for e in [a.ent(d), a.ent(a8), c.ent(c7), a.ent(a8), c.ent(c7), a.ent(a8)] {
        obs.receive_entry(&e);
        seq.push(format!("{:?}", obs.visible()));
    }
    seq
}

#[test]
fn probe_v3m1() {
    let ok = flipflop(true);
    let bad = flipflop(false);
    assert_eq!(ok[2..].iter().collect::<BTreeSet<_>>().len(), 1, "{ok:?}"); // settles after the stale write, stays
    assert!(bad[2..].iter().collect::<BTreeSet<_>>().len() > 1, "{bad:?}"); // the v3 order alternates
}

fn gen_scenario(b_clock: u64, a_skew: u64) -> BTreeSet<(String, Vec<LossKind>)> {
    let o = opts();
    let (mut ii, mut a, mut b) = (Mem::new('I', &o), Mem::new('A', &o), Mem::new('B', &o));
    let i0 = ii.ins(&[("x", i(100))], 0).unwrap();
    a.receive_entry(&ii.ent(i0));
    b.receive_entry(&ii.ent(i0));
    let aa = a.set("x", s("s"), a_skew).unwrap();
    b.w.clock = h(b_clock);
    let b1 = b.set("x", i(7), 0).unwrap();
    let b2 = b.inc("x", 1, 0).unwrap();
    all_orders(&o, &[a.ent(aa), b.ent(b1), b.ent(b2)], &[ii.ent(i0)])
        .into_iter()
        .map(|(_, m)| {
            let mut kinds: Vec<_> = m.w.state.losses.keys().map(|k| k.kind).collect();
            kinds.sort();
            (format!("{:?}", m.visible()), kinds)
        })
        .collect()
}

#[test]
fn probe_v3m3() {
    // B's +1 was made on top of the winning 7: it counts.
    let outs = gen_scenario(5, 0);
    let want = BTreeSet::from([(format!("{:?}", doc(&[("x", i(8))])), vec![LossKind::Clash])]);
    assert_eq!(outs, want);
    // A's "s" is the newest base: it overwrites B's +1 (the hidden figure is state, not a record).
    let outs2 = gen_scenario(0, 8);
    let want2 = BTreeSet::from([(format!("{:?}", doc(&[("x", s("s"))])), vec![LossKind::Clash])]);
    assert_eq!(outs2, want2);
}

#[test]
fn probe_v3m3_newer_generation_kept() {
    // A: $set x "s", $set x 10, $inc +3 (generation of "s"). B, concurrent and newer: $set x 7.
    // B's 7 wins; A's +3 sits in a NEWER generation whose starter lost: kept (D6), so 7 + 3 = 10.
    let o = opts();
    let (mut ii, mut a, mut b) = (Mem::new('I', &o), Mem::new('A', &o), Mem::new('B', &o));
    let i0 = ii.ins(&[("x", i(100))], 0).unwrap();
    a.receive_entry(&ii.ent(i0));
    b.receive_entry(&ii.ent(i0));
    let a1 = a.set("x", s("s"), 0).unwrap();
    let a2 = a.set("x", i(10), 0).unwrap();
    let a3 = a.inc("x", 3, 0).unwrap();
    b.w.clock = h(9);
    let b1 = b.set("x", i(7), 0).unwrap();
    for (ord, m) in all_orders(&o, &[a.ent(a1), a.ent(a2), a.ent(a3), b.ent(b1)], &[ii.ent(i0)]) {
        assert_eq!(m.visible(), doc(&[("x", i(10))]), "{ord:?}");
    }
}

#[test]
fn probe_double_base() {
    // Nm5: $inc on a double base is a register write that starts a generation.
    let o = opts();
    let (mut ii, mut a, mut b) = (Mem::new('I', &o), Mem::new('A', &o), Mem::new('B', &o));
    let i0 = ii.ins(&[("x", f(2.5))], 0).unwrap();
    a.receive_entry(&ii.ent(i0));
    b.receive_entry(&ii.ent(i0));
    let aa = a.inc("x", 1, 0).unwrap(); // 3.5, a register write
    let bb = b.inc("x", 2, 3).unwrap(); // 4.5, newer: wins
    assert_eq!(a.visible(), doc(&[("x", f(3.5))]));
    let obs = fresh_observer(&o, &[ii.ent(i0), a.ent(aa), b.ent(bb)]);
    assert_eq!(obs.visible(), doc(&[("x", f(4.5))]));
}

#[test]
fn probe_p2_faithful() {
    // P2 with the collection done the way gc.rs does it, then a new insert, a stale member that
    // still holds the delete, and a repair from it.
    let o = ret(6);
    let (mut a, mut c) = (Mem::new('A', &o), Mem::new('C', &o));
    let i1 = a.ins(&[("x", i(1))], 0).unwrap();
    c.receive_entry(&a.ent(i1));
    let d = a.del().unwrap();
    c.receive_entry(&a.ent(d));
    a.w.state = MergeState::default();
    a.w.clock = h(40);
    let n = a.ins(&[("x", i(9))], 0).unwrap();
    c.receive_entry(&a.ent(n));
    let on_c = c.visible();
    let snap = c.w.state.clone();
    a.receive_snapshot(&snap);
    assert_eq!(on_c, doc(&[("x", i(9))]));
    assert_eq!(a.visible(), doc(&[("x", i(9))]));
}

#[test]
fn probe_v4m1_revive() {
    // x=100. C +1 (C@2). A, unseen: $set "s", $set 10. B, newer, unseen: $set 7. Every delivery
    // order: reads 8, and the hidden STATE is 0 once B's 7 wins.
    let o = opts();
    let (mut ii, mut a, mut b, mut c) =
        (Mem::new('I', &o), Mem::new('A', &o), Mem::new('B', &o), Mem::new('C', &o));
    let i0 = ii.ins(&[("x", i(100))], 0).unwrap();
    for m in [&mut a, &mut b, &mut c] {
        m.receive_entry(&ii.ent(i0));
    }
    let cc = c.inc("x", 1, 0).unwrap();
    let a1 = a.set("x", s("s"), 0).unwrap();
    let a2 = a.set("x", i(10), 0).unwrap();
    b.w.clock = h(8);
    let bb = b.set("x", i(7), 0).unwrap();
    for (ord, m) in all_orders(&o, &[c.ent(cc), a.ent(a1), a.ent(a2), b.ent(bb)], &[ii.ent(i0)]) {
        assert_eq!(m.visible(), doc(&[("x", i(8))]), "{ord:?}");
        assert!(m.w.hidden_all().is_empty(), "{ord:?}");
    }
}

#[test]
fn probe_v5m1_hidden_part() {
    // The hidden figure is the part the starter of the winning generation had not seen, as a net
    // signed sum. B +5; A sees it; B +2; A (newer) sets "s": 2, not 7. C +3 and D -3 unseen by A:
    // nets to 0. A lone -3: -3.
    let o = opts();
    let mut out = BTreeMap::new();
    for (label, others) in
        [("saw 5 not 2", None), ("+3 and -3", Some(vec![3, -3])), ("lone -3", Some(vec![-3]))]
    {
        let (mut ii, mut a, mut b, mut c, mut d) = (
            Mem::new('I', &o),
            Mem::new('A', &o),
            Mem::new('B', &o),
            Mem::new('C', &o),
            Mem::new('D', &o),
        );
        let i0 = ii.ins(&[("x", i(100))], 0).unwrap();
        for m in [&mut a, &mut b, &mut c, &mut d] {
            m.receive_entry(&ii.ent(i0));
        }
        let mut ents = Vec::new();
        match others {
            None => {
                let b1 = b.inc("x", 5, 0).unwrap();
                a.receive_entry(&b.ent(b1));
                let b2 = b.inc("x", 2, 0).unwrap();
                ents = vec![b.ent(b1), b.ent(b2)];
            }
            Some(ns) => {
                for (m, n) in [&mut c, &mut d].into_iter().zip(ns) {
                    let st = m.inc("x", n, 0).unwrap();
                    ents.push(m.ent(st));
                }
            }
        }
        a.w.clock = h(20);
        let aa = a.set("x", s("s"), 0).unwrap();
        let mut seed = vec![ii.ent(i0)];
        seed.extend(ents);
        seed.push(a.ent(aa));
        let obs = fresh_observer(&o, &seed);
        out.insert(label, obs.w.state.hidden(&p("x"), &o));
    }
    assert_eq!(out, BTreeMap::from([("saw 5 not 2", 2), ("+3 and -3", 0), ("lone -3", -3)]));
}

#[test]
fn probe_q2_first_inserts() {
    // Concurrent FIRST inserts of one _id: the newest whole document wins, the other is reported
    // (insertLost) and its fields do not merge in.
    let o = opts();
    let (mut a, mut b) = (Mem::new('A', &o), Mem::new('B', &o));
    let aa = a.ins(&[("x", i(1)), ("y", i(1))], 0).unwrap();
    b.w.clock = h(5);
    let bb = b.ins(&[("x", i(2)), ("z", i(3))], 0).unwrap();
    for (ord, m) in all_orders(&o, &[a.ent(aa), b.ent(bb)], &[]) {
        assert_eq!(m.visible(), doc(&[("x", i(2)), ("z", i(3))]), "{ord:?}");
        let kinds: Vec<_> = m.w.state.losses.keys().map(|k| (k.kind, k.path.clone())).collect();
        assert_eq!(kinds, vec![(LossKind::InsertLost, p("_"))], "{ord:?}");
    }
}

#[test]
fn probe_q2_upsert_still_merges() {
    // Concurrent $inc upserts are not first inserts of a document: they still merge (views = 2).
    let o = opts();
    let (mut a, mut b) = (Mem::new('A', &o), Mem::new('B', &o));
    let aa = a.upsert_inc("views", 1).unwrap();
    let bb = b.upsert_inc("views", 1).unwrap();
    let obs = fresh_observer(&o, &[a.ent(aa), b.ent(bb)]);
    assert_eq!(obs.visible(), doc(&[("views", i(2))]));
}

#[test]
fn probe_q11_overflow() {
    // A local $inc past the Int64 bound is refused; two members each within the bound that
    // together cross it saturate at the bound after the merge, on every member.
    let o = JoinOptions::new(1_000_000_000, 1000).unwrap();
    let (mut a, mut b) = (Mem::new('A', &o), Mem::new('B', &o));
    let i0 = a.ins(&[("x", i(900))], 0).unwrap();
    b.receive_entry(&a.ent(i0));
    assert!(a.inc("x", 200, 0).is_none(), "a local $inc past the bound must be refused");
    let aa = a.inc("x", 60, 0).unwrap();
    let bb = b.inc("x", 60, 0).unwrap();
    let obs = fresh_observer(&o, &[a.ent(i0), a.ent(aa), b.ent(bb)]);
    assert_eq!(obs.visible(), doc(&[("x", i(1000))]));
}

#[test]
fn inc_landing_exactly_on_a_bound_is_accepted_one_past_is_refused() {
    // The Int64 bound: -2^63 and 2^63-1 are legal totals, -2^63-1 and 2^63 are not.
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&fl(&[("x", i(0)), ("y", i(0)), ("n", s("a"))]), 0).unwrap();
    assert!(w.write_inc(&p("x"), i64::MAX, 0).is_some());
    assert_eq!(w.visible().unwrap()[&p("x")], i(i64::MAX));
    assert!(w.write_inc(&p("x"), 1, 0).is_none(), "2^63 is one past the upper bound");
    assert!(w.write_inc(&p("x"), 0, 0).is_some(), "still on the bound");
    assert!(w.write_inc(&p("y"), i64::MIN, 0).is_some());
    assert_eq!(w.visible().unwrap()[&p("y")], i(i64::MIN));
    assert!(w.write_inc(&p("y"), -1, 0).is_none(), "-2^63-1 is one past the lower bound");
    assert!(w.write_inc(&p("y"), 1, 0).is_some());
    // A field that does not exist yet starts from 0.
    assert!(w.write_inc(&p("a"), i64::MAX, 0).is_some());
    assert!(w.write_inc(&p("b"), i64::MIN, 0).is_some());
    assert_eq!(w.visible().unwrap()[&p("a")], i(i64::MAX));
    assert_eq!(w.visible().unwrap()[&p("b")], i(i64::MIN));
    // The total is a base plus counted slots: 10 + (MAX - 10) lands on the bound.
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&fl(&[("x", i(10))]), 0).unwrap();
    assert!(w.write_inc(&p("x"), i64::MAX - 9, 0).is_none(), "one past");
    assert!(w.write_inc(&p("x"), i64::MAX - 10, 0).is_some(), "exactly on");
    assert!(w.write_inc(&p("x"), 1, 0).is_none());
    assert!(w.write_inc(&p("x"), i64::MIN, 0).is_some(), "back down across the whole range");
    assert_eq!(w.visible().unwrap()[&p("x")], i(-1));
}

#[test]
fn inc_landing_exactly_on_a_lowered_bound() {
    let o = JoinOptions::new(1_000_000_000, 1000).unwrap();
    let mut w = Writer::new(wid('A', 0), o);
    w.insert(&fl(&[("x", i(0)), ("y", i(0))]), 0).unwrap();
    assert!(w.write_inc(&p("x"), 1001, 0).is_none());
    assert!(w.write_inc(&p("x"), 1000, 0).is_some());
    assert!(w.write_inc(&p("x"), 1, 0).is_none());
    assert!(w.write_inc(&p("y"), -1002, 0).is_none());
    assert!(w.write_inc(&p("y"), -1001, 0).is_some());
    assert!(w.write_inc(&p("y"), -1, 0).is_none());
    assert_eq!(w.visible().unwrap()[&p("y")], i(-1001));
}

#[test]
fn probe_v6m1_losing_writes() {
    // A inserts {x:1}; C writes $set note and $inc views into it (acknowledged); B, newer, never held
    // it, inserts {y:2}. Every delivery order: {y:2}, and C's two writes are reported.
    let o = opts();
    let (mut a, mut b, mut c) = (Mem::new('A', &o), Mem::new('B', &o), Mem::new('C', &o));
    let aa = a.ins(&[("x", i(1))], 0).unwrap();
    c.receive_entry(&a.ent(aa));
    let c1 = c.set("note", s("c"), 0).unwrap();
    let c2 = c.inc("views", 1, 0).unwrap();
    b.w.clock = h(8);
    let bb = b.ins(&[("y", i(2))], 0).unwrap();
    let ents = [a.ent(aa), c.ent(c1), c.ent(c2), b.ent(bb)];
    let orders = all_orders(&o, &ents, &[]);
    assert_eq!(orders.len(), 24);
    for (ord, m) in orders {
        assert_eq!(m.visible(), doc(&[("y", i(2))]));
        let have: BTreeSet<(Path, MStamp)> =
            m.w.state.losses.keys().map(|k| (k.path.clone(), k.stamp)).collect();
        assert!(have.contains(&(p("note"), c1)), "{ord:?}");
        assert!(have.contains(&(p("views"), c2)), "{ord:?}");
        assert!(have.contains(&(Path::whole_document(), aa)), "{ord:?}");
    }
}

#[test]
fn probe_v6m1_killed_not_lost() {
    // I inserts, A (saw it) deletes and re-inserts, B (never held it) inserts later: the deleted
    // original is not reported as lost to B's insert (it is dead); B's field is deleteWon.
    let o = opts();
    let (mut ii, mut a, mut b) = (Mem::new('I', &o), Mem::new('A', &o), Mem::new('B', &o));
    let i0 = ii.ins(&[("x", i(1))], 0).unwrap();
    a.receive_entry(&ii.ent(i0));
    let d = a.del().unwrap();
    let r = a.ins(&[("x", i(2))], 0).unwrap();
    b.w.clock = h(9);
    let bb = b.ins(&[("y", i(5))], 0).unwrap();
    let ents = [ii.ent(i0), a.ent(d), a.ent(r), b.ent(bb)];
    // The delete known before the two first inserts meet: the deleted original is dead, not lost.
    let mut m = fresh_observer(&o, &[]);
    for e in &ents {
        m.receive_entry(e);
    }
    assert!(!m.loss_set().contains(&(LossKind::InsertLost, p("_"), i0)), "{:?}", m.loss_set());
    // Other orders may also hold the (true, sound) insertLost if the two first inserts met before the
    // delete did: an extra record, allowed by Q7; the document and the deleteWon are the same.
    for (ord, m) in all_orders(&o, &ents, &[]) {
        assert_eq!(m.visible(), doc(&[("x", i(2))]), "{ord:?}");
        assert!(
            m.loss_set().contains(&(LossKind::DeleteWon, p("y"), bb)),
            "{ord:?} {:?}",
            m.loss_set()
        );
    }
}

#[test]
fn probe_v6m2_insert_vs_upsert() {
    // An explicit insert racing an upsert-creation of one _id: the insert's document is the base, the
    // upsert's $inc applies on top, its $setOnInsert fields are dropped and reported as insertLost.
    let o = opts();
    let (mut a, mut b) = (Mem::new('A', &o), Mem::new('B', &o));
    let aa = a.ins(&[("name", s("A")), ("role", s("admin"))], 0).unwrap();
    b.w.clock = h(6);
    let (bl, bb) = b.upsert_soi(&[("theme", s("dark"))], "views", 1, &[]).unwrap();
    for (ord, m) in all_orders(&o, &[a.ent(aa), b.ent(bb)], &[]) {
        assert_eq!(
            m.visible(),
            doc(&[("name", s("A")), ("role", s("admin")), ("views", i(1))]),
            "{ord:?}"
        );
        assert!(m.loss_set().contains(&(LossKind::InsertLost, p("theme"), bl)), "{ord:?}");
    }
    // The same, the upsert older.
    let (mut a, mut b) = (Mem::new('A', &o), Mem::new('B', &o));
    let (_, bb) = b.upsert_soi(&[("theme", s("dark"))], "views", 1, &[]).unwrap();
    a.w.clock = h(6);
    let aa = a.ins(&[("name", s("A"))], 0).unwrap();
    for (ord, m) in all_orders(&o, &[a.ent(aa), b.ent(bb)], &[]) {
        assert_eq!(m.visible(), doc(&[("name", s("A")), ("views", i(1))]), "{ord:?}");
    }
    // Two upserts merge.
    let (mut a, mut b) = (Mem::new('A', &o), Mem::new('B', &o));
    let (_, x) = a.upsert_soi(&[("theme", s("dark"))], "views", 1, &[]).unwrap();
    let y = b.upsert_inc("views", 1).unwrap();
    let obs = fresh_observer(&o, &[a.ent(x), b.ent(y)]);
    assert_eq!(obs.visible().unwrap()[&p("views")], i(2));
}

#[test]
fn probe_v6m3_lower_bound() {
    // Q11 symmetric: a local $inc below -2^63 is refused; a merge crossing it saturates.
    let o = JoinOptions::new(1_000_000_000, 1000).unwrap();
    let (mut a, mut b) = (Mem::new('A', &o), Mem::new('B', &o));
    let i0 = a.ins(&[("x", i(-900))], 0).unwrap();
    b.receive_entry(&a.ent(i0));
    assert!(a.inc("x", -200, 0).is_none(), "a local $inc below the lower bound must be refused");
    let aa = a.inc("x", -60, 0).unwrap();
    let bb = b.inc("x", -60, 0).unwrap();
    let obs = fresh_observer(&o, &[a.ent(i0), a.ent(aa), b.ent(bb)]);
    assert_eq!(obs.visible(), doc(&[("x", i(-1001))]));
}

#[test]
fn probe_v7m1_v7m2_reinsert_vs_upsert() {
    // An explicit RE-INSERT after a delete racing an upsert-creation also makes the insert the base
    // ($setOnInsert dropped and reported), and an upsert carrying BOTH $setOnInsert and $set keeps
    // its $set fields on top.
    for after_delete in [false, true] {
        let o = opts();
        let (mut ii, mut a, mut b) = (Mem::new('I', &o), Mem::new('A', &o), Mem::new('B', &o));
        let mut seed = Vec::new();
        if after_delete {
            let i0 = ii.ins(&[("x", i(1))], 0).unwrap();
            a.receive_entry(&ii.ent(i0));
            b.receive_entry(&ii.ent(i0));
            let d = ii.del().unwrap();
            a.receive_entry(&ii.ent(d));
            b.receive_entry(&ii.ent(d));
            seed = vec![ii.ent(i0), ii.ent(d)];
        }
        let aa = a.ins(&[("name", s("A"))], 0).unwrap();
        b.w.clock = h(6);
        let (bl, bb) =
            b.upsert_soi(&[("theme", s("dark"))], "views", 1, &[("lang", s("en"))]).unwrap();
        for (ord, m) in all_orders(&o, &[a.ent(aa), b.ent(bb)], &seed) {
            let want = doc(&[("name", s("A")), ("lang", s("en")), ("views", i(1))]);
            assert_eq!(m.visible(), want, "{after_delete} {ord:?}");
            assert!(
                m.loss_set().contains(&(LossKind::InsertLost, p("theme"), bl)),
                "{after_delete} {ord:?}"
            );
            assert!(!m.w.state.losses.keys().any(|k| k.path == p("lang")));
        }
    }
}

#[test]
fn upsert_soi_is_two_writes() {
    let o = opts();
    let id = wid('B', 0);
    let mut w = Writer::new(id, o);
    let (c, n) = w
        .upsert_soi(&fl(&[("theme", s("dark"))]), &p("views"), 3, &fl(&[("lang", s("en"))]), 0)
        .unwrap();
    assert!(c.stamp < n.stamp);
    assert_eq!(c.record.kind, WriteKind::Insert);
    assert!(c.record.upsert && c.record.insert);
    assert_eq!(c.record.life, Some(c.stamp));
    assert_eq!(c.record.paths, vec![p("theme"), p("lang")]);
    assert_eq!(c.record.soi, vec![p("theme")]);
    assert_eq!(c.record.vals[&p("lang")], s("en"));
    assert_eq!(n.record.kind, WriteKind::Inc);
    assert!(!n.record.upsert && !n.record.insert);
    assert_eq!(n.record.life, Some(c.stamp));
    assert_eq!(n.record.paths, vec![p("views")]);
    assert_eq!(n.record.n, Some(3));
    let slot = &w.state.slots[&(c.stamp, p("views"), id, None)];
    assert_eq!(slot.stamp, n.stamp);
    assert_eq!(w.state.vv[&id], n.stamp.hlc);
    // no other write sets `upsert`
    let x = w.write_set(&p("lang"), s("fr"), 0).unwrap();
    assert!(!x.record.upsert);
    assert_eq!(w.visible(), doc(&[("theme", s("dark")), ("lang", s("fr")), ("views", i(3))]));
    assert!(w.upsert_soi(&[], &p("v"), 1, &[], 0).is_none());
}

// ---- golden vectors -------------------------------------------------------------

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub(super) fn golden_empty() -> MergeState {
    MergeState::default()
}

/// x=100 inserted by A, then +20 by B and +50 by C, all joined: x reads 170.
pub(super) fn golden_170() -> MergeState {
    let o = opts();
    let (mut a, mut b, mut c) = (Mem::new('A', &o), Mem::new('B', &o), Mem::new('C', &o));
    let i0 = a.ins(&[("x", i(100))], 0).unwrap();
    b.receive_entry(&a.ent(i0));
    c.receive_entry(&a.ent(i0));
    let bb = b.inc("x", 20, 0).unwrap();
    let cc = c.inc("x", 50, 0).unwrap();
    a.receive_entry(&b.ent(bb));
    a.receive_entry(&c.ent(cc));
    assert_eq!(a.visible(), doc(&[("x", i(170))]));
    a.w.state.clone()
}

/// A superseded first insert (insertLost on the document), an upsert with $setOnInsert whose
/// cell is dropped for the alive insert (insertLost on the cell), and a clash record.
pub(super) fn golden_mixed() -> MergeState {
    let o = opts();
    let (mut a, mut b, mut c) = (Mem::new('A', &o), Mem::new('B', &o), Mem::new('C', &o));
    let aa = a.ins(&[("x", i(1)), ("n", s("a"))], 0).unwrap();
    b.w.clock = h(5);
    let bb = b.ins(&[("y", i(2)), ("n", s("b"))], 0).unwrap();
    let (_, cc) = c.upsert_soi(&[("theme", s("dark"))], "views", 1, &[("lang", f(1.5))]).unwrap();
    let mut obs = fresh_observer(&o, &[]);
    for e in [a.ent(aa), b.ent(bb), c.ent(cc)] {
        obs.receive_entry(&e);
    }
    let kinds: BTreeSet<LossKind> = obs.w.state.losses.keys().map(|k| k.kind).collect();
    assert!(kinds.contains(&LossKind::InsertLost));
    obs.w.state.clone()
}

/// A delete and its `deleteWon` records: A inserts and then deletes; B and C took the insert
/// first and write into it without having seen the delete (a $set and an $inc), so the document
/// is dead and both writes are reported as lost to the delete.
pub(super) fn golden_delete() -> MergeState {
    let o = opts();
    let (mut a, mut b, mut c) = (Mem::new('A', &o), Mem::new('B', &o), Mem::new('C', &o));
    let i0 = a.ins(&[("x", i(1)), ("n", s("a"))], 0).unwrap();
    b.receive_entry(&a.ent(i0));
    c.receive_entry(&a.ent(i0));
    let dd = a.del().unwrap();
    let bb = b.set("x", i(5), 0).unwrap();
    let cc = c.inc("y", 2, 0).unwrap();
    let mut obs = fresh_observer(&o, &[]);
    for e in [a.ent(i0), a.ent(dd), b.ent(bb), c.ent(cc)] {
        obs.receive_entry(&e);
    }
    let st = obs.w.state.clone();
    assert!(!st.deletes.is_empty() && obs.visible().is_none());
    let kinds: BTreeSet<(LossKind, Path)> =
        st.losses.keys().map(|k| (k.kind, k.path.clone())).collect();
    assert_eq!(
        kinds,
        BTreeSet::from([(LossKind::DeleteWon, p("x")), (LossKind::DeleteWon, p("y"))])
    );
    st
}

/// A base written after counted increments: A inserts {x, y}; B, concurrently, adds 7 to y; A adds 3
/// to x and then sets x to 20 (the register's `obs` holds x's slot with its stamp and total), adds
/// 2 to y and then sets y to a string (the generation it starts has `seen` = A's y slot, and B's 7
/// is the unseen part, hidden). Pins the layout of `Reg::obs` and `Gen::seen`.
pub(super) fn golden_obs() -> MergeState {
    let o = opts();
    let (mut a, mut b) = (Mem::new('A', &o), Mem::new('B', &o));
    let i0 = a.ins(&[("x", i(10)), ("y", i(5))], 0).unwrap();
    b.receive_entry(&a.ent(i0));
    let bb = b.inc("y", 7, 0).unwrap();
    a.inc("x", 3, 0).unwrap();
    a.set("x", i(20), 0).unwrap();
    a.inc("y", 2, 0).unwrap();
    let aa = a.set("y", s("t"), 0).unwrap();
    let mut obs = fresh_observer(&o, &[]);
    for e in [a.ent(i0), b.ent(bb), a.ent(aa)] {
        obs.receive_entry(&e);
    }
    let st = obs.w.state.clone();
    let rx = &st.regs[&(i0, p("x"))];
    assert_eq!(rx.obs.len(), 1, "x's base observed its one slot");
    let (_, t) = rx.obs.values().next().unwrap();
    assert_eq!(*t, 3);
    let gy = st.regs[&(i0, p("y"))].generation.as_ref().unwrap();
    assert_eq!(gy.seen.values().copied().collect::<Vec<_>>(), vec![2]);
    assert_eq!(obs.visible(), doc(&[("x", i(20)), ("y", s("t"))]));
    assert_eq!(obs.w.hidden_all(), BTreeMap::from([(p("y"), 7)]));
    st
}

pub(super) fn golden_small() -> MergeState {
    let mut w = Writer::new(wid('A', 7), opts());
    w.insert(&fl(&[("x", i(1)), ("s", s("t"))]), 0).unwrap();
    w.state.clone()
}

/// GOLDEN VECTORS. The canonical bytes, the `encode()` bytes and the state hash are a persisted,
/// cross-release, cross-architecture format. Changing any value below is a FORMAT CHANGE (a wipe
/// release) and must be deliberate: do not "fix" these by regenerating them.
///
/// Each entry is (state hash hex, canonical length, first 64 canonical bytes hex).
///
/// History: `mixed` was regenerated on purpose twice. The M1 fix (a non-numeric upsert
/// creation value starts a generation): f40dd813..de19 / 1349 became 928f2042..5f82 / 1557. The
/// removal of the starter's version vector from a generation, which only the V4M1 mutant read
/// (`Gen::vv`; three registers of this state start a generation, 52 bytes each): 928f2042..5f82 /
/// 1557 became 34ce6696..f39c / 1401. `empty`, `170` and `delete` hold no generation and did not
/// change. `obs` was added later: no other vector has a register `obs` or a generation `seen`
/// that is not empty, so reordering either layout left the other hashes unchanged. `golden_encode_bytes_round_trip` lost the same vector and its `gens` section.
fn check_golden(name: &str, st: &MergeState, want: (&str, usize, &str)) {
    let cb = st.canonical_bytes();
    assert_eq!(hex(&st.state_hash()), want.0, "{name}: state hash");
    assert_eq!(cb.len(), want.1, "{name}: canonical length");
    assert_eq!(hex(&cb[..cb.len().min(64)]), want.2, "{name}: first 64 canonical bytes");
}

#[test]
fn golden_vectors_empty() {
    check_golden(
        "empty",
        &golden_empty(),
        (
            "f8d0ff0ab1a622e0a611a762fe2b45fb",
            56,
            "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
        ),
    );
}

#[test]
fn golden_vectors_170() {
    check_golden(
        "170",
        &golden_170(),
        (
            "42b0c20d1b0c26903625be81f0a105a0",
            670,
            "00000000000000010000000000000001000041414141414141414141414141414141000000000000000000000000000000000000000000000000000000000000",
        ),
    );
}

#[test]
fn golden_vectors_mixed() {
    check_golden(
        "mixed",
        &golden_mixed(),
        (
            "34ce6696c01c0c70255c54445944f39c",
            1401,
            "00000000000000030000000000000001000041414141414141414141414141414141000000000000000000000000000000000000000000000000000000000000",
        ),
    );
}

#[test]
fn golden_vectors_delete() {
    check_golden(
        "delete",
        &golden_delete(),
        (
            "0b4d37ac9b577c417603e7c9bce83933",
            542,
            "00000000000000010000000000000001000041414141414141414141414141414141000000000000000000000000000000000000000000000000000000000000",
        ),
    );
}

#[test]
fn golden_vectors_obs() {
    check_golden(
        "obs",
        &golden_obs(),
        (
            "f96f244c2b3198f724fcea0eafec32c7",
            1325,
            "00000000000000010000000000000001000041414141414141414141414141414141000000000000000000000000000000000000000000000000000000000000",
        ),
    );
}

#[test]
fn golden_encode_bytes_round_trip() {
    const ENC: &str = "514d53010000000000000001000000000000000100004141414141414141414141414141414100000000000000000000000000000000000700000000000000000000000000000002000000000000000178000000000000000173000000000000000000000000000000000000000000000000020000000000000001000041414141414141414141414141414141000000000000000000000000000000000007000000000000000173000000000000000100004141414141414141414141414141414100000000000000000000000000000000000703000000000000000174000000000000000001000000000000000100004141414141414141414141414141414100000000000000000000000000000000000700000000000000000000000000000000010000414141414141414141414141414141410000000000000000000000000000000000070000000000000001780000000000000001000041414141414141414141414141414141000000000000000000000000000000000007010000000000000001000000000000000000000000000000000000000000000000000141414141414141414141414141414141000000000000000000000000000000000007000000000000000100000000000000000000";
    let st = golden_small();
    assert_eq!(hex(&st.encode()), ENC);
    let bytes: Vec<u8> = (0..ENC.len() / 2)
        .map(|k| u8::from_str_radix(&ENC[2 * k..2 * k + 2], 16).unwrap())
        .collect();
    // The header: magic "QMS" and the format version byte.
    assert_eq!(&bytes[..4], b"QMS\x01");
    let back = MergeState::decode(&bytes).unwrap();
    assert_eq!(back, st);
    assert_eq!(back.encode(), bytes);
    // Lengths are big-endian u64: the first count (lives = 1) follows the header.
    assert_eq!(&bytes[4..12], &1u64.to_be_bytes());
}

// ---- Q4: the write diff compares value and type ------------------------------------

#[test]
fn q4_same_helper() {
    assert!(Value::same(None, &Value::Unset), "missing is Unset");
    assert!(Value::same(Some(&Value::Unset), &Value::Unset));
    assert!(!Value::same(None, &i(0)));
    assert!(!Value::same(Some(&i(2)), &f(2.0)), "2 is not 2.0");
    assert!(!Value::same(Some(&f(2.0)), &i(2)));
    assert!(Value::same(Some(&f(2.0)), &f(2.0)));
    assert!(Value::same(Some(&f(0.0)), &f(-0.0)), "doubles compare as numbers");
    assert!(!Value::same(Some(&f(f64::NAN)), &f(f64::NAN)));
    assert!(!Value::same(Some(&s("2")), &i(2)));
}

#[test]
fn q4_set_compares_value_and_type() {
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&fl(&[("x", f(2.0)), ("k", i(1))]), 0).unwrap();
    assert!(w.write_set(&p("x"), i(2), 0).is_some(), "$set 2 over a stored 2.0 is a write");
    assert_eq!(w.visible().unwrap()[&p("x")], i(2));
    assert!(w.write_set(&p("x"), i(2), 0).is_none(), "same type and value is a no-op");
    assert!(w.write_set(&p("x"), f(2.0), 0).is_some(), "and back");
    assert!(w.write_set(&p("k"), s("1"), 0).is_some());
    // Unset versus missing: unsetting a missing path is a no-op, a stored value is a write.
    assert!(w.write_set(&p("absent"), Value::Unset, 0).is_none());
    assert!(w.write_set(&p("k"), Value::Unset, 0).is_some());
    assert!(!w.visible().unwrap().contains_key(&p("k")));
    assert!(w.write_set(&p("k"), Value::Unset, 0).is_none(), "now missing again");
    assert!(w.write_unset(&p("k"), 0).is_none());
}

#[test]
fn q4_replace_compares_value_and_type() {
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&fl(&[("x", f(2.0)), ("y", s("t"))]), 0).unwrap();
    assert!(w.write_replace(&fl(&[("x", f(2.0)), ("y", s("t"))]), 0).is_none());
    let r = w.write_replace(&fl(&[("x", i(2)), ("y", s("t"))]), 0).unwrap();
    assert_eq!(r.record.paths, vec![p("x")], "only the retyped field changes");
    // An unset field in the new document over a missing path is not a change; over a stored one it is.
    assert!(w.write_replace(&fl(&[("x", i(2)), ("y", s("t")), ("z", Value::Unset)]), 0).is_none());
    let r = w.write_replace(&fl(&[("x", i(2)), ("y", Value::Unset)]), 0).unwrap();
    assert_eq!(r.record.paths, vec![p("y")]);
    assert_eq!(w.visible(), doc(&[("x", i(2))]));
    // Fields absent from the new document are dropped.
    let r = w.write_replace(&fl(&[("q", i(1))]), 0).unwrap();
    assert_eq!(r.record.paths, vec![p("q"), p("x")]);
    assert_eq!(w.visible(), doc(&[("q", i(1))]));
}

#[test]
fn upsert_soi_is_create_then_inc() {
    let fields = fl(&[("theme", s("dark"))]);
    let sets = fl(&[("lang", s("en"))]);
    let mut whole = Writer::new(wid('B', 0), opts());
    let (c1, n1) = whole.upsert_soi(&fields, &p("views"), 3, &sets, 2).unwrap();
    let mut parts = Writer::new(wid('B', 0), opts());
    let c2 = parts.upsert_soi_create(&fields, &p("views"), &sets, 2).unwrap();
    // Between the two writes: the life and the cells, no slot, nothing visible yet but the cells.
    assert!(parts.state.slots.is_empty());
    assert!(parts.state.lives.contains_key(&c2.stamp));
    assert_eq!(parts.state.regs.len(), 2);
    assert_eq!(parts.visible(), doc(&[("theme", s("dark")), ("lang", s("en"))]));
    // The increment needs an alive life.
    assert!(
        parts
            .upsert_inc_on(MStamp { hlc: h(99), writer: wid('Z', 0) }, &p("views"), 3, 2)
            .is_none()
    );
    let n2 = parts.upsert_inc_on(c2.stamp, &p("views"), 3, 2).unwrap();
    assert_eq!((c1, n1), (c2, n2));
    assert_eq!(whole.state, parts.state);
    assert_eq!(whole.clock, parts.clock);
    // Not while a life is alive.
    assert!(parts.upsert_soi_create(&fields, &p("views"), &sets, 0).is_none());
}

#[test]
fn upsert_with_conflicting_paths_is_refused_whole() {
    // `$setOnInsert`, `$set` and `$inc` on one path conflict: nothing is written.
    type Case<'a> = (&'a [(&'a str, Value)], &'a str, &'a [(&'a str, Value)]);
    let cases: [Case; 5] = [
        (&[("x", s("a"))], "x", &[]),            // $setOnInsert vs $inc
        (&[], "x", &[("x", i(1))]),              // $set vs $inc
        (&[("y", s("a"))], "x", &[("y", i(1))]), // $setOnInsert vs $set
        // the repeated path is not next to its twin in the order the lists are given
        (&[("a", s("a")), ("b", s("b"))], "a", &[]),
        (&[("a", s("a")), ("b", s("b"))], "c", &[("a", i(1))]),
    ];
    for (fields, inc, sets) in cases {
        // The one-shot form, and the two-step form given the whole update: each refuses before
        // it writes anything or moves the clock.
        let mut w = Writer::new(wid('A', 0), opts());
        assert!(w.upsert_soi(&fl(fields), &p(inc), 1, &fl(sets), 0).is_none());
        let untouched = Writer::new(wid('A', 0), opts());
        assert!(w.state == untouched.state && w.state.regs.is_empty());
        assert_eq!(w.clock, Hlc::ZERO);
        assert!(w.upsert_soi_create(&fl(fields), &p(inc), &fl(sets), 3).is_none());
        assert!(w.state == untouched.state && w.state.lives.is_empty());
        assert_eq!(w.clock, Hlc::ZERO, "a refused creation does not tick");
        // and nothing is left behind that a later update could trip over
        assert!(w.upsert_soi(&fl(&[("q", i(1))]), &p("r"), 1, &[], 0).is_some());
    }
    // Two-step, the other order: the increment step alone refuses a path the creation wrote
    // (the creation was valid for the `$inc` it was told about), with no write and no tick.
    let mut w = Writer::new(wid('A', 0), opts());
    let c = w.upsert_soi_create(&fl(&[("x", s("a"))]), &p("z"), &[], 0).unwrap();
    let (state, clock) = (w.state.clone(), w.clock);
    assert!(w.upsert_inc_on(c.stamp, &p("x"), 1, 0).is_none());
    assert!(w.state == state && w.clock == clock);
    assert!(w.upsert_inc_on(c.stamp, &p("z"), 1, 0).is_some());
}

// ---- B1: a counter that returns to exactly 0 stays in the document ---------------------

#[test]
fn zero_sum_counter_vanishes() {
    // insert {n:"a"}; $inc views +1; $inc views -1  =>  {n:"a", views:0}
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&fl(&[("n", s("a"))]), 0).unwrap();
    w.write_inc(&p("views"), 1, 0).unwrap();
    w.write_inc(&p("views"), -1, 0).unwrap();
    assert_eq!(w.visible(), doc(&[("n", s("a")), ("views", i(0))]));

    // upsert $inc views +1; $inc views -1  =>  {views:0}
    let mut w = Writer::new(wid('A', 0), opts());
    w.upsert_inc(&p("views"), 1, 0).unwrap();
    w.write_inc(&p("views"), -1, 0).unwrap();
    assert_eq!(w.visible(), doc(&[("views", i(0))]));

    // $inc 0 on a missing field creates it at 0.
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&fl(&[("n", s("a"))]), 0).unwrap();
    w.write_inc(&p("x"), 0, 0).unwrap();
    assert_eq!(w.visible(), doc(&[("n", s("a")), ("x", i(0))]));

    // {x:100}; $unset x; $inc x +5; $inc x -5  =>  {x:0}
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&fl(&[("x", i(100))]), 0).unwrap();
    w.write_unset(&p("x"), 0).unwrap();
    w.write_inc(&p("x"), 5, 0).unwrap();
    w.write_inc(&p("x"), -5, 0).unwrap();
    assert_eq!(w.visible(), doc(&[("x", i(0))]));
}

#[test]
fn zero_sum_counter_survives_the_join() {
    // The same sequence on a replica that only receives the states, in either order.
    let mut a = Writer::new(wid('A', 0), opts());
    a.upsert_inc(&p("views"), 1, 0).unwrap();
    let mid = a.state.clone();
    a.write_inc(&p("views"), -1, 0).unwrap();
    let mut b = Writer::new(wid('B', 0), opts());
    b.receive(&a.state);
    b.receive(&mid);
    assert_eq!(b.visible(), doc(&[("views", i(0))]));
    assert_eq!(b.state.state_hash(), a.state.state_hash());
}

#[test]
fn register_obs_keeps_the_slot_stamp() {
    // A base that observed a slot at total T, after which the slot moved and came back to T,
    // still counts the field present, and the figure is T - T = 0 on top of the base.
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&fl(&[("x", i(10))]), 0).unwrap();
    w.write_inc(&p("x"), 3, 0).unwrap();
    w.write_set(&p("x"), i(20), 0).unwrap();
    let r = w.state.regs.values().find(|r| r.value == i(20)).unwrap();
    assert!(!r.obs.is_empty() && r.obs.values().all(|(st, t)| *t == 3 && *st < r.stamp));
    w.write_inc(&p("x"), 4, 0).unwrap();
    w.write_inc(&p("x"), -4, 0).unwrap();
    assert_eq!(w.visible(), doc(&[("x", i(20))]));
}

#[test]
fn b1_mutant_is_visible_to_the_lib_tests() {
    // With the slot stamp ignored the zero-sum counter is absent again: the mutant is killed.
    let mut o = opts();
    o.set_value_ignores_slot_stamp(true);
    let mut w = Writer::new(wid('A', 0), o);
    w.insert(&fl(&[("n", s("a"))]), 0).unwrap();
    w.write_inc(&p("views"), 1, 0).unwrap();
    w.write_inc(&p("views"), -1, 0).unwrap();
    assert_eq!(w.visible(), doc(&[("n", s("a"))]));
}

// ---- M1: a non-numeric upsert creation value starts a generation ----------------------

fn run_upsert_string_gen(o: &JoinOptions, soi: bool) -> Writer {
    // No document. A: upsert {$set|$setOnInsert: {n: "s"}, $inc: {x: 1}}. B, concurrent:
    // upsert {$inc: {n: 5}}. C pulls A only, then $set n 7. Then everyone joins everything.
    let (mut a, mut b, mut c) = (
        Writer::new(wid('A', 0), o.clone()),
        Writer::new(wid('B', 0), o.clone()),
        Writer::new(wid('C', 0), o.clone()),
    );
    let sv = [("n", s("s"))];
    if soi {
        a.upsert_soi(&fl(&sv), &p("x"), 1, &[], 0).unwrap();
    } else {
        a.upsert_soi(&[], &p("x"), 1, &fl(&sv), 0).unwrap();
    }
    b.upsert_inc(&p("n"), 5, 0).unwrap();
    c.receive(&a.state);
    c.write_set(&p("n"), i(7), 0).unwrap();
    c.receive(&a.state);
    c.receive(&b.state);
    a.receive(&c.state);
    b.receive(&c.state);
    assert_eq!(a.state.state_hash(), c.state.state_hash());
    assert_eq!(b.state.state_hash(), c.state.state_hash());
    c
}

#[test]
fn upsert_string_gen() {
    for soi in [false, true] {
        let c = run_upsert_string_gen(&opts(), soi);
        let want = doc(&[("n", i(7)), ("x", i(1))]);
        assert_eq!(c.visible(), want, "soi={soi}");
        // B's +5 belongs to an older generation than C's set, which had not seen it.
        assert_eq!(c.hidden_all(), BTreeMap::from([(p("n"), 5)]), "soi={soi}");
    }
}

#[test]
fn m1_mutant_is_visible_to_the_lib_tests() {
    let mut o = opts();
    o.set_creation_without_generation(true);
    for soi in [false, true] {
        let c = run_upsert_string_gen(&o, soi);
        assert_eq!(c.visible(), doc(&[("n", i(12)), ("x", i(1))]), "soi={soi}");
        assert!(c.hidden_all().is_empty());
    }
}

#[test]
fn upsert_creation_generation_is_recorded_like_insert() {
    let mut w = Writer::new(wid('A', 0), opts());
    let (c, _) = w
        .upsert_soi(&fl(&[("t", s("dark"))]), &p("v"), 1, &fl(&[("l", f(1.5)), ("k", i(3))]), 0)
        .unwrap();
    let st = c.stamp;
    for name in ["t", "l"] {
        let p = p(name);
        let r = &w.state.regs[&(st, p.clone())];
        let g = r.generation.as_ref().expect("non-numeric creation value starts a generation");
        assert_eq!(g.stamp, st);
        assert_eq!(g.vv, BTreeMap::from([(st.writer, st.hlc)]));
        assert!(g.seen.is_empty());
        assert_eq!(w.state.gens[&(st, p.clone())], (st, g.vv.clone()));
    }
    assert!(w.state.regs[&(st, p("k"))].generation.is_none());
    assert!(!w.state.gens.contains_key(&(st, p("k"))));
}

// ---- m1: counter totals cannot overflow the read side ------------------------------------

/// A state with one upserted life and a slot on `x` for writers B and C, set to `total` each.
fn state_with_big_slots(total: i128) -> (MergeState, MStamp) {
    let mut w = Writer::new(wid('A', 0), opts());
    w.upsert_inc(&p("x"), 1, 0).unwrap();
    let mut st = w.state.clone();
    let life = *st.lives.keys().next().unwrap();
    let stamp = MStamp { hlc: h(1), writer: wid('B', 0) };
    for c in ['B', 'C'] {
        st.slots.insert((life, p("x"), wid(c, 0), None), Slot { stamp, total });
    }
    (st, life)
}

#[test]
fn decode_refuses_totals_past_the_bound() {
    // Two slots at i128::MAX would overflow a plain sum: the decoder refuses them.
    let (st, _) = state_with_big_slots(i128::MAX);
    assert_eq!(MergeState::decode(&st.encode()), Err(DecodeError::TotalOutOfRange));
    let (st, _) = state_with_big_slots(i128::MIN);
    assert_eq!(MergeState::decode(&st.encode()), Err(DecodeError::TotalOutOfRange));
    // The bound itself is accepted, one past it (either sign) is not.
    let (st, _) = state_with_big_slots(MAX_TOTAL);
    assert_eq!(MergeState::decode(&st.encode()).unwrap(), st);
    let (st, _) = state_with_big_slots(-MAX_TOTAL);
    assert_eq!(MergeState::decode(&st.encode()).unwrap(), st);
    let (st, _) = state_with_big_slots(MAX_TOTAL + 1);
    assert_eq!(MergeState::decode(&st.encode()), Err(DecodeError::TotalOutOfRange));
    let (st, _) = state_with_big_slots(-MAX_TOTAL - 1);
    assert_eq!(MergeState::decode(&st.encode()), Err(DecodeError::TotalOutOfRange));
    // The same copies elsewhere: a register's `obs`, a generation's `seen`, a loss record.
    let o = opts();
    let mut w = Writer::new(wid('A', 0), o.clone());
    w.insert(&fl(&[("x", i(1))]), 0).unwrap();
    w.write_inc(&p("x"), 2, 0).unwrap();
    w.write_set(&p("x"), s("t"), 0).unwrap();
    let mut good = w.state.clone();
    good.losses.insert(
        LossKey { kind: LossKind::Clash, path: p("x"), stamp: *good.lives.keys().next().unwrap() },
        LossValue::Total(5),
    );
    assert_eq!(MergeState::decode(&good.encode()).unwrap(), good);
    let big = MAX_TOTAL + 1;
    let mut bad = good.clone();
    for r in bad.regs.values_mut() {
        for v in r.obs.values_mut() {
            v.1 = big;
        }
    }
    let mut hit = 0;
    let mut try_state = |st: &MergeState| {
        hit += 1;
        assert_eq!(MergeState::decode(&st.encode()), Err(DecodeError::TotalOutOfRange));
    };
    if bad.regs.values().any(|r| !r.obs.is_empty()) {
        try_state(&bad);
    }
    let mut bad = good.clone();
    for r in bad.regs.values_mut() {
        if let Some(g) = r.generation.as_mut() {
            g.seen.insert((g.stamp, wid('A', 0), None), big);
        }
    }
    try_state(&bad);
    let mut bad = good.clone();
    for v in bad.losses.values_mut() {
        *v = LossValue::Total(big);
    }
    try_state(&bad);
    assert_eq!(hit, 3, "each copy of a total was exercised");
}

#[test]
fn read_side_cannot_overflow_on_a_hand_built_state() {
    // The decoder keeps such totals out, but the fields are public: the read side must not
    // panic (debug) or wrap (release) on a state assembled by hand.
    let (st, _) = state_with_big_slots(i128::MAX);
    let o = opts();
    assert_eq!(st.value(&p("x"), &o), Some(i(i64::MAX)), "saturates at the bound");
    assert_eq!(st.visible(&o).unwrap()[&p("x")], i(i64::MAX));
    let _ = st.hidden_all(&o);
    let (st, _) = state_with_big_slots(i128::MIN);
    assert_eq!(st.value(&p("x"), &o), Some(i(i64::MIN)));
    let mut w = Writer::from_state(wid('A', 0), st, o);
    assert!(w.write_inc(&p("x"), -1, 0).is_none(), "one past the lower bound");
    assert!(w.write_inc(&p("x"), 1, 0).is_some());
}

#[test]
fn write_inc_refuses_a_slot_past_the_total_bound() {
    // A's own slot sits just under the bound, balanced by B's: the visible value is small, so
    // the Int64 check passes, but the slot itself may not pass MAX_TOTAL.
    let mut w = Writer::new(wid('A', 0), opts());
    w.upsert_inc(&p("x"), 1, 0).unwrap();
    let life = *w.state.lives.keys().next().unwrap();
    let stamp = MStamp { hlc: h(1), writer: wid('B', 0) };
    w.state.slots.insert(
        (life, p("x"), wid('A', 0), None),
        Slot { stamp: w.state.slots.values().next().unwrap().stamp, total: MAX_TOTAL - 2 },
    );
    w.state
        .slots
        .insert((life, p("x"), wid('B', 0), None), Slot { stamp, total: -(MAX_TOTAL - 2) });
    assert_eq!(w.visible().unwrap()[&p("x")], i(0));
    let clock = w.clock;
    assert!(w.write_inc(&p("x"), 3, 0).is_none(), "MAX_TOTAL + 1");
    assert_eq!(w.clock, clock, "a refused write does not tick");
    assert!(w.write_inc(&p("x"), 2, 0).is_some(), "exactly MAX_TOTAL");
    assert!(w.write_inc(&p("x"), 1, 0).is_none());
    assert!(w.write_inc(&p("x"), -1, 0).is_some());
}

// ---- m3: mutant memory is outside the format ------------------------------------------------

#[test]
fn generation_vv_and_gens_are_outside_bytes_hash_and_equality() {
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&fl(&[("x", i(1)), ("t", s("a"))]), 0).unwrap();
    w.write_set(&p("t"), s("b"), 0).unwrap();
    let st = w.state.clone();
    assert!(!st.gens.is_empty());
    assert!(st.regs.values().filter_map(|r| r.generation.as_ref()).any(|g| !g.vv.is_empty()));
    let mut bare = st.clone();
    bare.gens.clear();
    for r in bare.regs.values_mut() {
        if let Some(g) = r.generation.as_mut() {
            g.vv.clear();
        }
    }
    assert_eq!(bare, st);
    assert_eq!(bare.canonical_bytes(), st.canonical_bytes());
    assert_eq!(bare.encode(), st.encode());
    assert_eq!(bare.state_hash(), st.state_hash());
    // A decoded state has none of it, and equals the original.
    let back = MergeState::decode(&st.encode()).unwrap();
    assert!(back.gens.is_empty());
    assert_eq!(back, st);
}

// ---- m5/m6: the options -----------------------------------------------------------------

#[test]
fn a_negative_int_bound_is_refused_and_zero_is_usable() {
    assert_eq!(JoinOptions::new(5, -1).unwrap_err(), InvalidIntBound(-1));
    assert!(JoinOptions::new(5, i64::MIN).is_err());
    let mut o = JoinOptions::new(5, 7).unwrap();
    assert_eq!((o.retention, o.int_bound()), (5, 7));
    assert_eq!(o.set_int_bound(-3), Err(InvalidIntBound(-3)));
    assert_eq!(o.int_bound(), 7, "a refused bound changes nothing");
    // The smallest valid bound clamps to [-1, 0] without panicking.
    let o = JoinOptions::new(5, 0).unwrap();
    let mut w = Writer::new(wid('A', 0), o);
    w.insert(&fl(&[("x", i(0))]), 0).unwrap();
    assert!(w.write_inc(&p("x"), 1, 0).is_none());
    assert!(w.write_inc(&p("x"), -1, 0).is_some());
    assert_eq!(w.visible(), doc(&[("x", i(-1))]));
    assert!(w.write_inc(&p("x"), -1, 0).is_none());
}

// ---- m-c: the total bound is theoretical, and a member never stores what it cannot decode ----

#[test]
fn max_total_is_far_beyond_reach_and_sums_for_64_writers() {
    // 2^119: each increment moves a slot by under 2^63, so reaching it takes over 2^56 of them.
    assert_eq!(MAX_TOTAL, 1i128 << 119);
    assert!(MAX_TOTAL / i128::from(i64::MAX) > 1i128 << 55);
    // 64 writers' slots, all at the bound on one path: the exact sum fits in i128 (the terms are
    // under 2^120, so 64 of them sum to under 2^126), and the visible value saturates.
    let o = opts();
    let mut w = Writer::new(wid('A', 0), o.clone());
    w.upsert_inc(&p("x"), 1, 0).unwrap();
    let mut st = w.state.clone();
    let life = *st.lives.keys().next().unwrap();
    st.slots.clear();
    let stamp = MStamp { hlc: h(1), writer: wid('B', 0) };
    for k in 0..64u8 {
        let wr = WriterId { node: nid((b'a' + k) as char), epoch: Epoch::from_u64(0) };
        st.slots.insert((life, p("x"), wr, None), Slot { stamp, total: MAX_TOTAL });
    }
    assert_eq!(st.value(&p("x"), &o), Some(i(i64::MAX)));
    assert_eq!(MergeState::decode(&st.encode()).unwrap(), st, "a state at the bound decodes");
    // Half at +bound, half at -bound: the exact sum is 0, which only exact arithmetic gives.
    for (k, slot) in st.slots.values_mut().enumerate() {
        slot.total = if k % 2 == 0 { MAX_TOTAL } else { -MAX_TOTAL };
    }
    assert_eq!(st.value(&p("x"), &o), Some(i(0)));
    // The same terms through the hidden figure: a generation that saw nothing, over 64 slots of
    // an older generation at the bound.
    let gen_stamp = MStamp { hlc: h(5), writer: wid('B', 0) };
    let mut st = st.clone();
    for slot in st.slots.values_mut() {
        slot.total = MAX_TOTAL;
    }
    st.regs.insert(
        (life, p("x")),
        Reg {
            stamp: gen_stamp,
            value: s("t"),
            obs: BTreeMap::new(),
            generation: Some(Gen::start(gen_stamp, &BTreeMap::new(), BTreeMap::new())),
            soi: false,
        },
    );
    assert_eq!(st.hidden(&p("x"), &o), 64 * MAX_TOTAL, "exact, no saturation");
}

#[test]
fn receive_loss_refuses_a_total_past_the_bound() {
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&fl(&[("x", i(1))]), 0).unwrap();
    let st = w.state.clone();
    let life = *st.lives.keys().next().unwrap();
    let key = |path: &str| LossKey { kind: LossKind::DeleteWon, path: p(path), stamp: life };
    for (p, t, added) in [
        ("a", MAX_TOTAL, true),
        ("b", -MAX_TOTAL, true),
        ("c", MAX_TOTAL + 1, false),
        ("d", -MAX_TOTAL - 1, false),
        ("e", i128::MAX, false),
        ("f", i128::MIN, false),
    ] {
        assert_eq!(w.receive_loss(key(p), LossValue::Total(t)), added, "{p}");
    }
    // Whatever was stored decodes.
    assert_eq!(MergeState::decode(&w.state.encode()).unwrap(), w.state);
    // Other loss values are unaffected.
    assert!(w.receive_loss(key("g"), LossValue::Value(i(i64::MAX))));
}

#[test]
fn hidden_figure_adds_with_saturation_on_a_hand_built_state() {
    // A generation over slots whose totals would overflow a plain i128 sum or difference: the
    // figure saturates instead of panicking (the decoder keeps such totals out, the fields are
    // public).
    let o = opts();
    let mut w = Writer::new(wid('A', 0), o.clone());
    w.insert(&fl(&[("x", i(1))]), 0).unwrap();
    let mut st = w.state.clone();
    let life = *st.lives.keys().next().unwrap();
    let gen_stamp = MStamp { hlc: h(9), writer: wid('A', 0) };
    let slot_stamp = MStamp { hlc: h(2), writer: wid('B', 0) };
    for c in ['B', 'C'] {
        st.slots
            .insert((life, p("x"), wid(c, 0), None), Slot { stamp: slot_stamp, total: i128::MAX });
    }
    let mut g = Gen::start(gen_stamp, &BTreeMap::new(), BTreeMap::new());
    // B's slot was seen at -1: i128::MAX - (-1) overflows a plain subtraction
    g.seen.insert((life, wid('B', 0), None), -1);
    st.regs.insert(
        (life, p("x")),
        Reg {
            stamp: gen_stamp,
            value: s("t"),
            obs: BTreeMap::new(),
            generation: Some(g),
            soi: false,
        },
    );
    assert_eq!(st.hidden(&p("x"), &o), i128::MAX);
    assert_eq!(st.hidden_all(&o), BTreeMap::from([(p("x"), i128::MAX)]));
}

// ---- the BSON value set ----------------------------------------------------------------

/// One value of every type, the array holding a nested array.
fn every_type() -> Vec<(&'static str, Value)> {
    vec![
        ("unset", Value::Unset),
        ("null", Value::Null),
        ("i32", Value::Int32(-7)),
        ("i64", Value::Int64(1 << 40)),
        ("dbl", Value::Double(2.5)),
        ("dec", Value::Decimal128([0x30; 16])),
        ("str", s("héllo")),
        ("bool", Value::Bool(true)),
        ("date", Value::Date(-1)),
        ("oid", Value::ObjectId([7; 12])),
        ("bin", Value::Binary { subtype: 0x80, bytes: vec![0, 1, 255] }),
        ("ts", Value::Timestamp { time: u32::MAX, increment: 3 }),
        ("re", Value::Regex { pattern: "^a.*".into(), options: "ix".into() }),
        (
            "arr",
            Value::Array(vec![i(1), Value::Array(vec![s("x"), Value::Null, Value::Unset]), f(0.5)]),
        ),
    ]
}

#[test]
fn every_bson_type_is_stored_read_back_and_round_trips() {
    let mut w = Writer::new(wid('A', 0), opts());
    let fields = every_type();
    let fields: Vec<(&str, Value)> = fields.into_iter().filter(|(k, _)| *k != "unset").collect();
    w.insert(&fl(&fields), 0).unwrap();
    let vis = w.visible().unwrap();
    for (k, v) in &fields {
        assert_eq!(vis.get(&p(k)), Some(v), "{k}");
    }
    let enc = w.state.encode();
    let back = MergeState::decode(&enc).unwrap();
    assert_eq!(back, w.state);
    assert_eq!(back.encode(), enc);
    assert_eq!(back.state_hash(), w.state.state_hash());
    // A loss record keeps any value too.
    for (_, v) in &fields {
        let mut st = MergeState::default();
        let key = LossKey {
            kind: LossKind::Clash,
            path: p("p"),
            stamp: MStamp { hlc: h(1), writer: wid('A', 0) },
        };
        st.losses.insert(key, LossValue::Value(v.clone()));
        assert_eq!(MergeState::decode(&st.encode()).unwrap(), st);
    }
}

#[test]
fn the_bson_types_are_never_equal_to_one_another() {
    let all = every_type();
    for (a, (ka, va)) in all.iter().enumerate() {
        for (b, (kb, vb)) in all.iter().enumerate() {
            assert_eq!(va == vb, a == b, "{ka} against {kb}");
            assert_eq!(Value::same(Some(va), vb), a == b, "same: {ka} against {kb}");
        }
    }
    // The same number in four numeric types.
    let nums = [Value::Int32(2), Value::Int64(2), Value::Double(2.0), Value::Decimal128([2; 16])];
    for (a, x) in nums.iter().enumerate() {
        for (b, y) in nums.iter().enumerate() {
            assert_eq!(x == y, a == b);
            assert_eq!(Value::same(Some(x), y), a == b);
        }
    }
    // Only the payload tells two binaries, timestamps, regexes apart.
    let bin = |t, b: &[u8]| Value::Binary { subtype: t, bytes: b.to_vec() };
    assert_ne!(bin(0, b"a"), bin(4, b"a"));
    assert_ne!(bin(0, b"a"), bin(0, b"b"));
    let ts = |t, c| Value::Timestamp { time: t, increment: c };
    assert_ne!(ts(1, 2), ts(2, 1));
    let re = |p: &str, o: &str| Value::Regex { pattern: p.into(), options: o.into() };
    assert_ne!(re("a", "i"), re("a", ""));
    assert_ne!(re("a", "i"), re("b", "i"));
}

#[test]
fn arrays_compare_element_by_element_and_type_for_type() {
    let a = Value::Array(vec![i(1), f(0.0)]);
    assert_eq!(a, Value::Array(vec![i(1), f(0.0)]));
    assert_ne!(a, Value::Array(vec![i(1)]));
    assert_ne!(a, Value::Array(vec![Value::Int32(1), f(0.0)]));
    // Doubles inside an array: equality is by bits, `same` follows the number.
    let z = Value::Array(vec![f(-0.0)]);
    assert_ne!(Value::Array(vec![f(0.0)]), z);
    assert!(Value::same(Some(&Value::Array(vec![f(0.0)])), &z));
    let nan = Value::Array(vec![f(f64::NAN)]);
    assert_eq!(nan, nan.clone());
    assert!(!Value::same(Some(&nan), &nan));
    // An array is not a scalar and not an empty array.
    assert_ne!(Value::Array(vec![]), Value::Null);
    assert!(!Value::same(Some(&Value::Array(vec![])), &Value::Null));
}

#[test]
fn only_null_and_int64_are_numeric() {
    for (k, v) in every_type() {
        assert_eq!(v.is_numeric(), k == "unset" || k == "i64", "{k}");
    }
}

#[test]
fn a_write_across_integer_widths_is_a_write() {
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&fl(&[("n", i(5))]), 0).unwrap();
    assert!(w.write_set(&p("n"), i(5), 0).is_none(), "same type, same number: a no-op");
    assert!(w.write_set(&p("n"), Value::Int32(5), 0).is_some(), "Int32 over Int64 is a write");
    assert!(w.write_set(&p("n"), Value::Int32(5), 0).is_none());
    assert!(w.write_set(&p("n"), f(5.0), 0).is_some(), "a double over an Int32 is a write");
    assert!(w.write_set(&p("n"), Value::Decimal128([1; 16]), 0).is_some());
    assert_eq!(w.visible().unwrap()[&p("n")], Value::Decimal128([1; 16]));
    // A string or an Int32 base refuses `$inc` like any non-counter.
    assert!(w.write_inc(&p("n"), 1, 0).is_none());
}

#[test]
fn a_concurrent_write_of_two_types_resolves_by_stamp_and_reports_the_loser() {
    let mut a = Writer::new(wid('A', 0), opts());
    let mut b = Writer::new(wid('B', 0), opts());
    a.insert(&fl(&[("x", i(0))]), 0).unwrap();
    b.receive(&a.state);
    a.write_set(&p("x"), Value::Int32(1), 0).unwrap();
    b.write_set(&p("x"), Value::Date(2), 5).unwrap();
    let (la, lb) = (a.receive(&b.state), b.receive(&a.state));
    assert_eq!(a.state.state_hash(), b.state.state_hash());
    assert_eq!(a.visible().unwrap()[&p("x")], Value::Date(2));
    assert_eq!((la.len(), lb.len()), (1, 1));
    assert_eq!(a.state.losses.values().next(), Some(&LossValue::Value(Value::Int32(1))));
}

#[test]
fn decode_refuses_a_bad_bool_a_deep_array_and_a_cut_value() {
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&fl(&[("b", Value::Bool(true))]), 0).unwrap();
    let enc = w.state.encode();
    // The bool's payload byte is the last value byte: find `[9, 1]` and set it to 2.
    let pos = enc.windows(2).rposition(|p| p == [9, 1]).unwrap();
    let mut bad = enc.clone();
    bad[pos + 1] = 2;
    assert_eq!(MergeState::decode(&bad), Err(DecodeError::BadTag(2)));

    let nest = |depth: usize| {
        let mut v = Value::Null;
        for _ in 0..depth {
            v = Value::Array(vec![v]);
        }
        v
    };
    let ok = nest(super::codec::MAX_VALUE_DEPTH);
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&fl(&[("a", ok)]), 0).unwrap();
    assert!(MergeState::decode(&w.state.encode()).is_ok());
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&fl(&[("a", nest(super::codec::MAX_VALUE_DEPTH + 1))]), 0).unwrap();
    assert_eq!(MergeState::decode(&w.state.encode()), Err(DecodeError::TooDeep));
    // Every proper prefix of a state with every type is refused as truncated or bad.
    let mut w = Writer::new(wid('A', 0), opts());
    let fields: Vec<(&str, Value)> =
        every_type().into_iter().filter(|(k, _)| *k != "unset").collect();
    w.insert(&fl(&fields), 0).unwrap();
    let enc = w.state.encode();
    for n in 0..enc.len() {
        assert!(MergeState::decode(&enc[..n]).is_err(), "prefix {n}");
    }
}

// ---- paths as segment lists --------------------------------------------------------------

#[test]
fn a_path_is_a_nonempty_list_of_nul_free_segments() {
    assert_eq!(Path::new(Vec::<String>::new()), Err(InvalidPath("no segments")));
    assert_eq!(Path::new(["a\0b"]), Err(InvalidPath("a NUL byte in a segment")));
    // The engine stores empty, dotted and `$`-prefixed field names: all are segments.
    for name in ["", "$set", "$", "a.b", ".", "..", "é"] {
        assert_eq!(Path::flat(name).unwrap().segments(), [name]);
    }
    assert_eq!(Path::new(["a", ""]).unwrap().segments(), ["a", ""]);
    // A dot is part of a name, not a separator.
    let dotted = Path::flat("a.b").unwrap();
    assert_eq!(dotted.segments(), ["a.b"]);
    assert_ne!(dotted, Path::new(["a", "b"]).unwrap());
    assert_eq!(Path::new(["a", "b"]).unwrap().segments(), ["a", "b"]);
    assert_eq!(Path::new(["a", "b"]).unwrap().to_string(), "a.b");
    assert_eq!(Path::whole_document(), p("_"));
}

#[test]
fn paths_order_by_segments_and_a_prefix_sorts_first() {
    let pp = |s: &[&str]| Path::new(s.iter().copied()).unwrap();
    let sorted = [
        pp(&["a"]),
        pp(&["a", "b"]),
        pp(&["a", "b", "c"]),
        pp(&["a", "c"]),
        pp(&["a\u{1}"]),
        pp(&["a.b"]),
        pp(&["b"]),
        pp(&["é"]),
    ];
    for (x, y) in sorted.iter().zip(sorted.iter().skip(1)) {
        assert!(x < y, "{x:?} < {y:?}");
    }
    let mut shuffled = [3, 7, 1, 0, 6, 2, 5, 4].map(|k| sorted[k].clone());
    shuffled.sort();
    assert_eq!(shuffled, sorted);
}

#[test]
fn dotted_names_and_nested_paths_are_different_cells_and_round_trip() {
    let ab = Path::new(["a", "b"]).unwrap();
    let a_b = Path::flat("a.b").unwrap();
    let abc = Path::new(["a", "b", "c"]).unwrap();
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&[(a_b.clone(), i(1)), (ab.clone(), i(2)), (abc.clone(), s("x"))], 0).unwrap();
    let vis = w.visible().unwrap();
    assert_eq!(vis.len(), 3);
    assert_eq!((&vis[&a_b], &vis[&ab], &vis[&abc]), (&i(1), &i(2), &s("x")));
    assert!(w.write_set(&ab, i(3), 0).is_some());
    assert!(w.write_inc(&a_b, 4, 0).is_some());
    assert!(w.write_unset(&abc, 0).is_some());
    let vis = w.visible().unwrap();
    assert_eq!((&vis[&a_b], &vis[&ab], vis.get(&abc)), (&i(5), &i(3), None));
    // The state encodes, decodes and hashes with the paths intact.
    let enc = w.state.encode();
    let back = MergeState::decode(&enc).unwrap();
    assert_eq!(back, w.state);
    assert_eq!(back.encode(), enc);
    // ... and the two spellings of "a.b" encode differently.
    let mut x = Writer::new(wid('A', 0), opts());
    let mut y = Writer::new(wid('A', 0), opts());
    x.insert(&[(a_b, i(1))], 0).unwrap();
    y.insert(&[(ab, i(1))], 0).unwrap();
    assert_ne!(x.state.canonical_bytes(), y.state.canonical_bytes());
    assert_ne!(x.state.state_hash(), y.state.state_hash());
    // Two writers merge a nested path and its dotted twin without mixing them.
    let mut b = Writer::new(wid('B', 0), opts());
    b.receive(&x.state);
    let ab = Path::new(["a", "b"]).unwrap();
    b.write_set(&ab, s("nested"), 0).unwrap();
    x.receive(&b.state);
    let vis = x.visible().unwrap();
    assert_eq!(
        (vis.get(&Path::flat("a.b").unwrap()), vis.get(&ab)),
        (Some(&i(1)), Some(&s("nested")))
    );
}

#[test]
fn odd_field_names_are_distinct_cells_and_round_trip() {
    // Empty, dotted, `$`-prefixed and nested names, each a different path, in the order the
    // segments give: a prefix first, then the byte order of the segments.
    let paths: Vec<Path> =
        [&[""][..], &["", ""], &["$set"], &["a"], &["a", ""], &["a", "b"], &["a\u{1}"], &["a.b"]]
            .iter()
            .map(|s| Path::new(s.iter().copied()).unwrap())
            .collect();
    let mut w = Writer::new(wid('A', 0), opts());
    let fields: Vec<(Path, Value)> =
        paths.iter().enumerate().map(|(k, q)| (q.clone(), i(k as i64))).collect();
    w.insert(&fields, 0).unwrap();
    let vis = w.visible().unwrap();
    assert_eq!(vis.keys().cloned().collect::<Vec<_>>(), paths);
    let enc = w.state.encode();
    let back = MergeState::decode(&enc).unwrap();
    assert_eq!(back, w.state);
    assert_eq!(back.encode(), enc);
    assert_eq!(back.visible(&opts()).unwrap(), vis);
}

// ---- stamps minted outside the writer ---------------------------------------------------

/// The stamp `Writer::tick(skew)` would mint next, built from outside as the engine would.
fn next_stamp_of(w: &Writer, skew: u64) -> MStamp {
    let top = w.state.vv.values().copied().chain([w.clock]).max().unwrap_or(Hlc::ZERO);
    MStamp { hlc: Hlc::new(top.wall_ms + 1 + skew, 0), writer: w.id }
}

/// Each external-stamp entry point does what its self-ticking twin does when it is handed the
/// stamp the twin would have minted: same stamps, same records, same state, same clock.
#[test]
fn stamped_writes_equal_self_ticking_writes() {
    let mut own = Writer::new(wid('A', 0), opts());
    let mut ext = Writer::new(wid('A', 0), opts());
    // Both sides run the same step; `$e` is the self-ticking call, `$x` the stamped one given
    // `st` (the stamp of `tick`) through a closure.
    macro_rules! step {
        ($skew:expr, $own:expr, $ext:expr) => {{
            let st = next_stamp_of(&ext, $skew);
            let a = $own(&mut own);
            let mut calls = 0;
            let b = $ext(&mut ext, &mut || {
                calls += 1;
                st
            });
            assert_eq!(a.is_some(), b.as_ref().unwrap().is_some());
            assert_eq!(calls, usize::from(a.is_some()), "one stamp per accepted write");
            assert_eq!(a.as_ref().map(|w: &Written| w.stamp), b.unwrap().map(|w| w.stamp));
            assert_eq!(own.state, ext.state);
            assert_eq!(own.clock, ext.clock);
            assert_eq!(own.state.state_hash(), ext.state.state_hash());
        }};
    }
    let (x, y, n) = (p("x"), p("y"), p("n"));
    let f = fl(&[("x", i(1)), ("y", s("a"))]);
    step!(0, |w: &mut Writer| w.insert(&f, 0), |w: &mut Writer, st| w.insert_stamped(&f, st));
    step!(0, |w: &mut Writer| w.insert(&f, 0), |w: &mut Writer, st| w.insert_stamped(&f, st)); // refused: alive
    step!(3, |w: &mut Writer| w.write_set(&x, Value::Int32(7), 3), |w: &mut Writer, st| w
        .write_set_stamped(&x, Value::Int32(7), st));
    step!(
        0,
        |w: &mut Writer| w.write_set(&x, Value::Int32(7), 0), // unchanged: no stamp is asked for
        |w: &mut Writer, st| w.write_set_stamped(&x, Value::Int32(7), st)
    );
    step!(1, |w: &mut Writer| w.write_inc(&n, 5, 1), |w: &mut Writer, st| w
        .write_inc_stamped(&n, 5, st));
    step!(0, |w: &mut Writer| w.write_inc(&n, 5, 0), |w: &mut Writer, st| w
        .write_inc_stamped(&n, 5, st));
    step!(0, |w: &mut Writer| w.write_unset(&y, 0), |w: &mut Writer, st| w
        .write_unset_stamped(&y, st));
    let d = fl(&[("x", Value::Int32(9)), ("z", i(0))]);
    step!(2, |w: &mut Writer| w.write_replace(&d, 2), |w: &mut Writer, st| w
        .write_replace_stamped(&d, st));
    step!(0, |w: &mut Writer| w.write_delete(0), |w: &mut Writer, st| w.write_delete_stamped(st));
    step!(0, |w: &mut Writer| w.write_delete(0), |w: &mut Writer, st| w.write_delete_stamped(st));
    step!(0, |w: &mut Writer| w.upsert_inc(&n, 4, 0), |w: &mut Writer, st| w
        .upsert_inc_stamped(&n, 4, st));
    step!(0, |w: &mut Writer| w.write_delete(0), |w: &mut Writer, st| w.write_delete_stamped(st));
    // The two-step upsert, one stamp at a time.
    let (sf, sets) = (fl(&[("t", s("dark"))]), fl(&[("l", s("en"))]));
    let v = p("v");
    let c1 = own.upsert_soi_create(&sf, &v, &sets, 1).unwrap();
    let st = next_stamp_of(&ext, 1);
    let c2 = ext.upsert_soi_create_stamped(&sf, &v, &sets, &mut || st).unwrap().unwrap();
    assert_eq!((c1.stamp, &own.state), (c2.stamp, &ext.state));
    let i1 = own.upsert_inc_on(c1.stamp, &v, 2, 1).unwrap();
    let st = next_stamp_of(&ext, 1);
    let i2 = ext.upsert_inc_on_stamped(c2.stamp, &v, 2, &mut || st).unwrap().unwrap();
    assert_eq!((i1.stamp, &own.state), (i2.stamp, &ext.state));
    // The whole upsert: two stamps, in order.
    let (mut o2, mut e2) = (Writer::new(wid('B', 0), opts()), Writer::new(wid('B', 0), opts()));
    let (a1, a2) = o2.upsert_soi(&sf, &v, 2, &sets, 4).unwrap();
    // skew 4 on a fresh writer: wall 0 + 1 + 4 = 5, then 5 + 1 + 4 = 10
    let mut mint = [5, 10].map(|wall| MStamp { hlc: Hlc::new(wall, 0), writer: e2.id }).into_iter();
    let (b1, b2) =
        e2.upsert_soi_stamped(&sf, &v, 2, &sets, &mut || mint.next().unwrap()).unwrap().unwrap();
    assert_eq!((a1.stamp, a2.stamp), (b1.stamp, b2.stamp));
    assert_eq!(o2.state, e2.state);
}

#[test]
fn an_engine_shaped_stamp_with_a_counter_converges_like_any_other() {
    // Stamps with HLC counters and a wall clock well ahead, as `Engine::next_stamp` gives them.
    let mut a = Writer::new(wid('A', 3), opts());
    let mut b = Writer::new(wid('B', 0), opts());
    let st = |w: &Writer, wall, c| MStamp { hlc: Hlc::new(wall, c), writer: w.id };
    let s1 = st(&a, 1_700_000_000_000, 4);
    a.insert_stamped(&fl(&[("x", i(1))]), &mut || s1).unwrap().unwrap();
    b.receive(&a.state);
    let s2 = st(&a, 1_700_000_000_000, 5);
    a.write_set_stamped(&p("x"), i(2), &mut || s2).unwrap().unwrap();
    let s3 = st(&b, 1_700_000_000_000, 5);
    assert!(s3 > s2, "same instant and counter: the node breaks the tie");
    b.write_set_stamped(&p("x"), i(3), &mut || s3).unwrap().unwrap();
    let (ab, ba) = (a.receive(&b.state), b.receive(&a.state));
    assert_eq!(a.state.state_hash(), b.state.state_hash());
    assert_eq!(a.visible(), doc(&[("x", i(3))]));
    assert_eq!((ab.len(), ba.len()), (1, 1), "the loser s2 is reported on both sides");
    // The self-ticking clock then continues above the supplied stamps.
    let next = a.tick(0);
    assert!(next.hlc > s3.hlc);
    assert_eq!(a.state.state_hash(), a.state.join(&MergeState::default(), &a.opts).0.state_hash());
}

#[test]
fn an_external_stamp_must_be_this_writers_and_above_everything_it_has_seen() {
    let mut a = Writer::new(wid('A', 7), opts());
    let mut b = Writer::new(wid('B', 0), opts());
    a.insert(&fl(&[("x", i(1))]), 0).unwrap(); // stamp 1
    b.receive(&a.state);
    b.write_set(&p("x"), i(2), 10).unwrap(); // stamp 12 by B
    a.receive(&b.state); // A has seen 12
    let snapshot = (a.state.clone(), a.clock);
    let unchanged = |a: &Writer| (a.state.clone(), a.clock) == snapshot;
    let asked = std::cell::Cell::new(0);
    let try_set = |a: &mut Writer, st: MStamp, v: i64| {
        a.write_set_stamped(&p("x"), i(v), &mut || {
            asked.set(asked.get() + 1);
            st
        })
    };
    let mine = |wall| MStamp { hlc: Hlc::new(wall, 0), writer: a.id };
    // not above the writer's clock, nor the newest vv entry (B's 12), nor equal to it
    let top = Hlc::new(12, 0);
    for bad in [mine(0), mine(1), mine(11), mine(12)] {
        assert_eq!(
            try_set(&mut a, bad, 3),
            Err(StampError::NotAbove { top, stamp: bad.hlc }),
            "{bad:?}"
        );
        assert!(unchanged(&a));
    }
    // a counter above the same instant is above
    let ok = MStamp { hlc: Hlc::new(12, 1), writer: a.id };
    // another node, and the same node under another epoch
    let other_node = MStamp { writer: wid('B', 0), ..ok };
    let other_epoch = MStamp { writer: wid('A', 8), ..ok };
    for bad in [other_node, other_epoch] {
        assert_eq!(
            try_set(&mut a, bad, 3),
            Err(StampError::OtherWriter { writer: a.id, stamp: bad.writer })
        );
        assert!(unchanged(&a));
    }
    assert_eq!(asked.get(), 6, "each refused stamp was asked for once");
    assert!(try_set(&mut a, ok, 3).unwrap().is_some());
    assert_eq!(a.clock, ok.hlc);
    // and a second write must go above the first
    assert!(matches!(try_set(&mut a, ok, 4), Err(StampError::NotAbove { .. })));
    // `tick_to` is the same check on its own
    let mut c = Writer::new(wid('C', 0), opts());
    assert!(c.tick_to(MStamp { hlc: Hlc::ZERO, writer: c.id }).is_err());
    assert_eq!(c.clock, Hlc::ZERO);
    let ok = MStamp { hlc: Hlc::new(0, 1), writer: c.id };
    assert_eq!(c.tick_to(ok), Ok(ok));
    assert_eq!(c.clock, ok.hlc);
}

#[test]
fn a_refused_stamp_changes_nothing_for_every_entry_point() {
    let mut w = Writer::new(wid('A', 0), opts());
    let (f, sets, x, n) =
        (fl(&[("x", i(1)), ("y", s("a"))]), fl(&[("l", s("en"))]), p("x"), p("n"));
    w.insert(&f, 5).unwrap();
    let before = (w.state.clone(), w.clock);
    let bad = MStamp { hlc: Hlc::new(1, 0), writer: w.id };
    macro_rules! refused {
        ($e:expr) => {{
            let r = $e;
            assert!(matches!(r, Err(StampError::NotAbove { .. })), "{r:?}");
            assert_eq!((w.state.clone(), w.clock), before);
        }};
    }
    refused!(w.write_set_stamped(&x, i(9), &mut || bad));
    refused!(w.write_unset_stamped(&x, &mut || bad));
    refused!(w.write_inc_stamped(&x, 1, &mut || bad));
    refused!(w.write_replace_stamped(&fl(&[("x", i(9))]), &mut || bad));
    refused!(w.write_delete_stamped(&mut || bad));
    // the creating entry points need no alive life: a fresh writer
    let mut v = Writer::new(wid('A', 1), opts());
    v.tick_to(MStamp { hlc: Hlc::new(50, 0), writer: v.id }).unwrap();
    let bad = MStamp { hlc: Hlc::new(50, 0), writer: v.id };
    let before = (v.state.clone(), v.clock);
    for r in [
        v.insert_stamped(&f, &mut || bad),
        v.upsert_inc_stamped(&n, 1, &mut || bad),
        v.upsert_soi_create_stamped(&f, &n, &sets, &mut || bad),
    ] {
        assert!(matches!(r, Err(StampError::NotAbove { .. })), "{r:?}");
        assert_eq!((v.state.clone(), v.clock), before);
    }
    let r = v.upsert_soi_stamped(&f, &n, 1, &sets, &mut || bad);
    assert!(matches!(r, Err(StampError::NotAbove { .. })), "{r:?}");
    assert_eq!((v.state.clone(), v.clock), before);
    // the increment step of an upsert on a missing life asks for no stamp
    let mut asked = 0;
    let life = MStamp { hlc: Hlc::new(99, 0), writer: v.id };
    let r = v.upsert_inc_on_stamped(life, &n, 1, &mut || {
        asked += 1;
        bad
    });
    assert_eq!((r, asked), (Ok(None), 0));
}

#[test]
fn a_two_stamp_upsert_whose_second_stamp_is_refused_is_taken_back() {
    let mut w = Writer::new(wid('A', 0), opts());
    let (f, sets, n) = (fl(&[("t", s("dark"))]), fl(&[("l", s("en"))]), p("v"));
    let first = MStamp { hlc: Hlc::new(10, 0), writer: w.id };
    let mut mint = [first, first].into_iter(); // the second is not above the first
    let r = w.upsert_soi_stamped(&f, &n, 3, &sets, &mut || mint.next().unwrap());
    assert!(matches!(r, Err(StampError::NotAbove { top, .. }) if top == first.hlc), "{r:?}");
    assert_eq!(w.state, MergeState::default());
    assert_eq!(w.clock, Hlc::ZERO);
    // and the same call with a second stamp above the first goes through
    let second = MStamp { hlc: Hlc::new(11, 0), writer: w.id };
    let mut mint = [first, second].into_iter();
    let (c, i) =
        w.upsert_soi_stamped(&f, &n, 3, &sets, &mut || mint.next().unwrap()).unwrap().unwrap();
    assert_eq!((c.stamp, i.stamp), (first, second));
}

// ---- a BSON null is a value, an unset is not --------------------------------------------

#[test]
fn a_null_is_a_present_value_and_an_unset_field_is_absent() {
    let mut w = Writer::new(wid('A', 0), opts());
    let (x, y) = (p("x"), p("y"));
    w.insert(&fl(&[("x", Value::Null), ("y", i(1))]), 0).unwrap();
    assert_eq!(w.visible(), doc(&[("x", Value::Null), ("y", i(1))]), "insert stores a null");
    assert!(w.write_set(&x, Value::Null, 0).is_none(), "null over null: no change");
    assert!(w.write_set(&x, Value::Unset, 0).is_some(), "unsetting a stored null is a write");
    assert_eq!(w.visible(), doc(&[("y", i(1))]));
    assert!(w.write_unset(&x, 0).is_none(), "already absent");
    assert!(w.write_set(&x, Value::Null, 0).is_some(), "null over an absent field is a write");
    assert_eq!(w.visible().unwrap()[&x], Value::Null);
    assert!(w.write_set(&y, Value::Null, 0).is_some());
    assert_eq!(w.visible(), doc(&[("x", Value::Null), ("y", Value::Null)]));
    // $unset removes it
    assert!(w.write_unset(&y, 0).is_some());
    assert_eq!(w.visible(), doc(&[("x", Value::Null)]));
    // a null is not a counter, an absent field is
    assert!(w.write_inc(&x, 1, 0).is_none(), "$inc on a null is refused");
    assert!(w.write_inc(&y, 1, 0).is_some(), "$inc on an absent field creates it");
    // a replace keeps a null field, writes one over a missing field, and drops by absence
    assert!(w.write_replace(&fl(&[("x", Value::Null), ("y", i(1))]), 0).is_none());
    let r =
        w.write_replace(&fl(&[("x", Value::Null), ("y", i(1)), ("z", Value::Null)]), 0).unwrap();
    assert_eq!(r.record.paths, vec![p("z")]);
    let r = w.write_replace(&fl(&[("y", i(1)), ("z", Value::Null)]), 0).unwrap();
    assert_eq!(r.record.paths, vec![x.clone()]);
    assert_eq!(w.visible(), doc(&[("y", i(1)), ("z", Value::Null)]));
}

#[test]
fn a_null_and_an_unset_merge_and_encode_as_different_values() {
    let mut a = Writer::new(wid('A', 0), opts());
    let mut b = Writer::new(wid('B', 0), opts());
    a.insert(&fl(&[("x", i(1))]), 0).unwrap();
    b.receive(&a.state);
    a.write_set(&p("x"), Value::Null, 0).unwrap();
    b.write_unset(&p("x"), 5).unwrap(); // the later stamp
    a.receive(&b.state);
    b.receive(&a.state);
    assert_eq!(a.state.state_hash(), b.state.state_hash());
    assert_eq!(a.visible(), doc(&[]), "the unset wins by its stamp");
    assert_eq!(
        a.state.losses.values().collect::<Vec<_>>(),
        vec![&LossValue::Value(Value::Null)],
        "and the null is the reported loser"
    );
    // The other way round: the null is later and stays.
    let (mut c, mut d) = (Writer::new(wid('C', 0), opts()), Writer::new(wid('D', 0), opts()));
    c.insert(&fl(&[("x", i(1))]), 0).unwrap();
    d.receive(&c.state);
    c.write_unset(&p("x"), 0).unwrap();
    d.write_set(&p("x"), Value::Null, 5).unwrap();
    c.receive(&d.state);
    assert_eq!(c.visible(), doc(&[("x", Value::Null)]));
    assert_eq!(c.state.losses.values().collect::<Vec<_>>(), vec![&LossValue::Value(Value::Unset)]);
    // The two states encode differently and round trip.
    let (mut n, mut u) = (Writer::new(wid('A', 0), opts()), Writer::new(wid('A', 0), opts()));
    n.insert(&fl(&[("x", Value::Null)]), 0).unwrap();
    u.insert(&fl(&[("x", Value::Unset)]), 0).unwrap();
    assert_ne!(n.state.canonical_bytes(), u.state.canonical_bytes());
    for w in [&n, &u] {
        assert_eq!(MergeState::decode(&w.state.encode()).unwrap(), w.state);
    }
}
