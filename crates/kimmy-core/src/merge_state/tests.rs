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
    Value::Int(n)
}
fn s(x: &str) -> Value {
    Value::Str(x.to_string())
}
fn f(x: f64) -> Value {
    Value::Double(x)
}
fn opts() -> JoinOptions {
    JoinOptions::default()
}
fn ret(r: u64) -> JoinOptions {
    JoinOptions { retention: r, ..JoinOptions::default() }
}
fn doc(kv: &[(&str, Value)]) -> Option<BTreeMap<String, Value>> {
    Some(kv.iter().map(|(k, v)| (k.to_string(), v.clone())).collect())
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
        let r = self.w.insert(f, sk);
        self.log(r)
    }
    fn upsert_inc(&mut self, p: &str, n: i64) -> Option<MStamp> {
        let r = self.w.upsert_inc(p, n, 0);
        self.log(r)
    }
    /// Returns `(life, inc stamp)`: the creation write and the increment write.
    fn upsert_soi(
        &mut self,
        f: &[(&str, Value)],
        p: &str,
        n: i64,
        sets: &[(&str, Value)],
    ) -> Option<(MStamp, MStamp)> {
        let (c, i) = self.w.upsert_soi(f, p, n, sets, 0)?;
        let life = c.stamp;
        let inc = i.stamp;
        self.log(Some(c));
        self.log(Some(i));
        Some((life, inc))
    }
    fn set(&mut self, p: &str, v: Value, sk: u64) -> Option<MStamp> {
        let r = self.w.write_set(p, v, sk);
        self.log(r)
    }
    fn inc(&mut self, p: &str, n: i64, sk: u64) -> Option<MStamp> {
        let r = self.w.write_inc(p, n, sk);
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
    fn visible(&self) -> Option<BTreeMap<String, Value>> {
        self.w.visible()
    }
    fn canon(&self) -> Vec<u8> {
        self.w.state.canonical_bytes()
    }
    fn loss_set(&self) -> BTreeSet<(LossKind, String, MStamp)> {
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
    for (p, m) in all_orders(&o, &[a.ent(d), a.ent(r), b.ent(z)], &[ii.ent(i0)]) {
        assert_eq!(m.visible(), doc(&[("x", i(2))]), "{p:?}");
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
    for (p, m) in all_orders(&o, &[a.ent(d), a.ent(r), b.ent(z)], &[ii.ent(i0)]) {
        assert_eq!(m.visible(), doc(&[("y", i(2))]), "{p:?}");
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
    assert!(losses.iter().any(|k| k.kind == LossKind::DeleteWon && k.path == "x"));
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
    for (p, m) in all_orders(&o, &[a.ent(d0), a.ent(la), b.ent(lb), c.ent(d1)], &[ii.ent(i0)]) {
        assert_eq!(m.visible(), None, "{p:?}");
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
        let v = ms[0].w.state.value("x", &o);
        assert_eq!(v, Some(i(if epochs { 135 } else { 105 })), "epochs={epochs}"); // 105: the silent loss epochs prevent
    }
}

// ---- probes5.py ---------------------------------------------------------------

fn flipflop(horizon_first: bool) -> Vec<String> {
    // The model5 `Member` does not normalise its writes; the mutant is only visible on such states.
    let o = JoinOptions {
        retention: 4,
        horizon_first,
        normalize_writes: false,
        ..JoinOptions::default()
    };
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
    for (p, m) in all_orders(&o, &[a.ent(a1), a.ent(a2), a.ent(a3), b.ent(b1)], &[ii.ent(i0)]) {
        assert_eq!(m.visible(), doc(&[("x", i(10))]), "{p:?}");
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
    for (p, m) in all_orders(&o, &[c.ent(cc), a.ent(a1), a.ent(a2), b.ent(bb)], &[ii.ent(i0)]) {
        assert_eq!(m.visible(), doc(&[("x", i(8))]), "{p:?}");
        assert!(m.w.hidden_all().is_empty(), "{p:?}");
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
        out.insert(label, obs.w.state.hidden("x", &o));
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
    for (p, m) in all_orders(&o, &[a.ent(aa), b.ent(bb)], &[]) {
        assert_eq!(m.visible(), doc(&[("x", i(2)), ("z", i(3))]), "{p:?}");
        let kinds: Vec<_> = m.w.state.losses.keys().map(|k| (k.kind, k.path.clone())).collect();
        assert_eq!(kinds, vec![(LossKind::InsertLost, "_".to_string())], "{p:?}");
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
    let o = JoinOptions { int_bound: 1000, ..JoinOptions::default() };
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
    w.insert(&[("x", i(0)), ("y", i(0)), ("n", s("a"))], 0).unwrap();
    assert!(w.write_inc("x", i64::MAX, 0).is_some());
    assert_eq!(w.visible().unwrap()["x"], i(i64::MAX));
    assert!(w.write_inc("x", 1, 0).is_none(), "2^63 is one past the upper bound");
    assert!(w.write_inc("x", 0, 0).is_some(), "still on the bound");
    assert!(w.write_inc("y", i64::MIN, 0).is_some());
    assert_eq!(w.visible().unwrap()["y"], i(i64::MIN));
    assert!(w.write_inc("y", -1, 0).is_none(), "-2^63-1 is one past the lower bound");
    assert!(w.write_inc("y", 1, 0).is_some());
    // A field that does not exist yet starts from 0.
    assert!(w.write_inc("a", i64::MAX, 0).is_some());
    assert!(w.write_inc("b", i64::MIN, 0).is_some());
    assert_eq!(w.visible().unwrap()["a"], i(i64::MAX));
    assert_eq!(w.visible().unwrap()["b"], i(i64::MIN));
    // The total is a base plus counted slots: 10 + (MAX - 10) lands on the bound.
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&[("x", i(10))], 0).unwrap();
    assert!(w.write_inc("x", i64::MAX - 9, 0).is_none(), "one past");
    assert!(w.write_inc("x", i64::MAX - 10, 0).is_some(), "exactly on");
    assert!(w.write_inc("x", 1, 0).is_none());
    assert!(w.write_inc("x", i64::MIN, 0).is_some(), "back down across the whole range");
    assert_eq!(w.visible().unwrap()["x"], i(-1));
}

#[test]
fn inc_landing_exactly_on_a_lowered_bound() {
    let o = JoinOptions { int_bound: 1000, ..opts() };
    let mut w = Writer::new(wid('A', 0), o);
    w.insert(&[("x", i(0)), ("y", i(0))], 0).unwrap();
    assert!(w.write_inc("x", 1001, 0).is_none());
    assert!(w.write_inc("x", 1000, 0).is_some());
    assert!(w.write_inc("x", 1, 0).is_none());
    assert!(w.write_inc("y", -1002, 0).is_none());
    assert!(w.write_inc("y", -1001, 0).is_some());
    assert!(w.write_inc("y", -1, 0).is_none());
    assert_eq!(w.visible().unwrap()["y"], i(-1001));
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
    for (p, m) in orders {
        assert_eq!(m.visible(), doc(&[("y", i(2))]));
        let have: BTreeSet<(String, MStamp)> =
            m.w.state.losses.keys().map(|k| (k.path.clone(), k.stamp)).collect();
        assert!(have.contains(&("note".to_string(), c1)), "{p:?}");
        assert!(have.contains(&("views".to_string(), c2)), "{p:?}");
        assert!(have.contains(&("_".to_string(), aa)), "{p:?}");
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
    assert!(
        !m.loss_set().contains(&(LossKind::InsertLost, "_".to_string(), i0)),
        "{:?}",
        m.loss_set()
    );
    // Other orders may also hold the (true, sound) insertLost if the two first inserts met before the
    // delete did: an extra record, allowed by Q7; the document and the deleteWon are the same.
    for (p, m) in all_orders(&o, &ents, &[]) {
        assert_eq!(m.visible(), doc(&[("x", i(2))]), "{p:?}");
        assert!(
            m.loss_set().contains(&(LossKind::DeleteWon, "y".to_string(), bb)),
            "{p:?} {:?}",
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
    for (p, m) in all_orders(&o, &[a.ent(aa), b.ent(bb)], &[]) {
        assert_eq!(
            m.visible(),
            doc(&[("name", s("A")), ("role", s("admin")), ("views", i(1))]),
            "{p:?}"
        );
        assert!(m.loss_set().contains(&(LossKind::InsertLost, "theme".to_string(), bl)), "{p:?}");
    }
    // The same, the upsert older.
    let (mut a, mut b) = (Mem::new('A', &o), Mem::new('B', &o));
    let (_, bb) = b.upsert_soi(&[("theme", s("dark"))], "views", 1, &[]).unwrap();
    a.w.clock = h(6);
    let aa = a.ins(&[("name", s("A"))], 0).unwrap();
    for (p, m) in all_orders(&o, &[a.ent(aa), b.ent(bb)], &[]) {
        assert_eq!(m.visible(), doc(&[("name", s("A")), ("views", i(1))]), "{p:?}");
    }
    // Two upserts merge.
    let (mut a, mut b) = (Mem::new('A', &o), Mem::new('B', &o));
    let (_, x) = a.upsert_soi(&[("theme", s("dark"))], "views", 1, &[]).unwrap();
    let y = b.upsert_inc("views", 1).unwrap();
    let obs = fresh_observer(&o, &[a.ent(x), b.ent(y)]);
    assert_eq!(obs.visible().unwrap()["views"], i(2));
}

#[test]
fn probe_v6m3_lower_bound() {
    // Q11 symmetric: a local $inc below -2^63 is refused; a merge crossing it saturates.
    let o = JoinOptions { int_bound: 1000, ..JoinOptions::default() };
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
        for (p, m) in all_orders(&o, &[a.ent(aa), b.ent(bb)], &seed) {
            let want = doc(&[("name", s("A")), ("lang", s("en")), ("views", i(1))]);
            assert_eq!(m.visible(), want, "{after_delete} {p:?}");
            assert!(
                m.loss_set().contains(&(LossKind::InsertLost, "theme".to_string(), bl)),
                "{after_delete} {p:?}"
            );
            assert!(!m.w.state.losses.keys().any(|k| k.path == "lang"));
        }
    }
}

#[test]
fn upsert_soi_is_two_writes() {
    let o = opts();
    let id = wid('B', 0);
    let mut w = Writer::new(id, o);
    let (c, n) =
        w.upsert_soi(&[("theme", s("dark"))], "views", 3, &[("lang", s("en"))], 0).unwrap();
    assert!(c.stamp < n.stamp);
    assert_eq!(c.record.kind, WriteKind::Insert);
    assert!(c.record.upsert && c.record.insert);
    assert_eq!(c.record.life, Some(c.stamp));
    assert_eq!(c.record.paths, vec!["theme".to_string(), "lang".to_string()]);
    assert_eq!(c.record.soi, vec!["theme".to_string()]);
    assert_eq!(c.record.vals["lang"], s("en"));
    assert_eq!(n.record.kind, WriteKind::Inc);
    assert!(!n.record.upsert && !n.record.insert);
    assert_eq!(n.record.life, Some(c.stamp));
    assert_eq!(n.record.paths, vec!["views".to_string()]);
    assert_eq!(n.record.n, Some(3));
    let slot = &w.state.slots[&(c.stamp, "views".to_string(), id, None)];
    assert_eq!(slot.stamp, n.stamp);
    assert_eq!(w.state.vv[&id], n.stamp.hlc);
    // no other write sets `upsert`
    let x = w.write_set("lang", s("fr"), 0).unwrap();
    assert!(!x.record.upsert);
    assert_eq!(w.visible(), doc(&[("theme", s("dark")), ("lang", s("fr")), ("views", i(3))]));
    assert!(w.upsert_soi(&[], "v", 1, &[], 0).is_none());
}

// ---- golden vectors -------------------------------------------------------------

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn golden_empty() -> MergeState {
    MergeState::default()
}

/// x=100 inserted by A, then +20 by B and +50 by C, all joined: x reads 170.
fn golden_170() -> MergeState {
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
fn golden_mixed() -> MergeState {
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
fn golden_delete() -> MergeState {
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
    let kinds: BTreeSet<(LossKind, String)> =
        st.losses.keys().map(|k| (k.kind, k.path.clone())).collect();
    assert_eq!(
        kinds,
        BTreeSet::from([
            (LossKind::DeleteWon, "x".to_string()),
            (LossKind::DeleteWon, "y".to_string())
        ])
    );
    st
}

fn golden_small() -> MergeState {
    let mut w = Writer::new(wid('A', 7), opts());
    w.insert(&[("x", i(1)), ("s", s("t"))], 0).unwrap();
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
/// change. `golden_encode_bytes_round_trip` lost the same vector and its `gens` section.
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
    assert!(Value::same(None, &Value::Null), "missing is Null");
    assert!(Value::same(Some(&Value::Null), &Value::Null));
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
    w.insert(&[("x", f(2.0)), ("k", i(1))], 0).unwrap();
    assert!(w.write_set("x", i(2), 0).is_some(), "$set 2 over a stored 2.0 is a write");
    assert_eq!(w.visible().unwrap()["x"], i(2));
    assert!(w.write_set("x", i(2), 0).is_none(), "same type and value is a no-op");
    assert!(w.write_set("x", f(2.0), 0).is_some(), "and back");
    assert!(w.write_set("k", s("1"), 0).is_some());
    // Null versus missing: setting a missing path to Null is a no-op, a stored value to Null is a write.
    assert!(w.write_set("absent", Value::Null, 0).is_none());
    assert!(w.write_set("k", Value::Null, 0).is_some());
    assert!(!w.visible().unwrap().contains_key("k"));
    assert!(w.write_set("k", Value::Null, 0).is_none(), "now missing again");
    assert!(w.write_unset("k", 0).is_none());
}

#[test]
fn q4_replace_compares_value_and_type() {
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&[("x", f(2.0)), ("y", s("t"))], 0).unwrap();
    assert!(w.write_replace(&[("x", f(2.0)), ("y", s("t"))], 0).is_none());
    let r = w.write_replace(&[("x", i(2)), ("y", s("t"))], 0).unwrap();
    assert_eq!(r.record.paths, vec!["x".to_string()], "only the retyped field changes");
    // A Null in the new document over a missing path is not a change; over a stored one it is.
    assert!(w.write_replace(&[("x", i(2)), ("y", s("t")), ("z", Value::Null)], 0).is_none());
    let r = w.write_replace(&[("x", i(2)), ("y", Value::Null)], 0).unwrap();
    assert_eq!(r.record.paths, vec!["y".to_string()]);
    assert_eq!(w.visible(), doc(&[("x", i(2))]));
    // Fields absent from the new document are dropped.
    let r = w.write_replace(&[("q", i(1))], 0).unwrap();
    assert_eq!(r.record.paths, vec!["q".to_string(), "x".to_string()]);
    assert_eq!(w.visible(), doc(&[("q", i(1))]));
}

#[test]
fn upsert_soi_is_create_then_inc() {
    let fields = [("theme", s("dark"))];
    let sets = [("lang", s("en"))];
    let mut whole = Writer::new(wid('B', 0), opts());
    let (c1, n1) = whole.upsert_soi(&fields, "views", 3, &sets, 2).unwrap();
    let mut parts = Writer::new(wid('B', 0), opts());
    let c2 = parts.upsert_soi_create(&fields, &sets, 2).unwrap();
    // Between the two writes: the life and the cells, no slot, nothing visible yet but the cells.
    assert!(parts.state.slots.is_empty());
    assert!(parts.state.lives.contains_key(&c2.stamp));
    assert_eq!(parts.state.regs.len(), 2);
    assert_eq!(parts.visible(), doc(&[("theme", s("dark")), ("lang", s("en"))]));
    // The increment needs an alive life.
    assert!(
        parts.upsert_inc_on(MStamp { hlc: h(99), writer: wid('Z', 0) }, "views", 3, 2).is_none()
    );
    let n2 = parts.upsert_inc_on(c2.stamp, "views", 3, 2).unwrap();
    assert_eq!((c1, n1), (c2, n2));
    assert_eq!(whole.state, parts.state);
    assert_eq!(whole.clock, parts.clock);
    // Not while a life is alive.
    assert!(parts.upsert_soi_create(&fields, &sets, 0).is_none());
}

#[test]
fn upsert_with_conflicting_paths_is_refused_whole() {
    // `$setOnInsert`, `$set` and `$inc` on one path conflict: nothing is written.
    type Case<'a> = (&'a [(&'a str, Value)], &'a str, &'a [(&'a str, Value)]);
    let cases: [Case; 3] = [
        (&[("x", s("a"))], "x", &[]),            // $setOnInsert vs $inc
        (&[], "x", &[("x", i(1))]),              // $set vs $inc
        (&[("y", s("a"))], "x", &[("y", i(1))]), // $setOnInsert vs $set
    ];
    for (fields, inc, sets) in cases {
        let mut w = Writer::new(wid('A', 0), opts());
        assert!(w.upsert_soi(fields, inc, 1, sets, 0).is_none());
        assert!(w.state.lives.is_empty() && w.state.regs.is_empty() && w.state.slots.is_empty());
        assert_eq!(w.clock, Hlc::ZERO);
    }
    // The increment step alone also refuses a path the creation wrote.
    let mut w = Writer::new(wid('A', 0), opts());
    let c = w.upsert_soi_create(&[("x", s("a"))], &[], 0).unwrap();
    assert!(w.upsert_inc_on(c.stamp, "x", 1, 0).is_none());
    assert!(w.upsert_inc_on(c.stamp, "z", 1, 0).is_some());
}

// ---- B1: a counter that returns to exactly 0 stays in the document ---------------------

#[test]
fn zero_sum_counter_vanishes() {
    // insert {n:"a"}; $inc views +1; $inc views -1  =>  {n:"a", views:0}
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&[("n", s("a"))], 0).unwrap();
    w.write_inc("views", 1, 0).unwrap();
    w.write_inc("views", -1, 0).unwrap();
    assert_eq!(w.visible(), doc(&[("n", s("a")), ("views", i(0))]));

    // upsert $inc views +1; $inc views -1  =>  {views:0}
    let mut w = Writer::new(wid('A', 0), opts());
    w.upsert_inc("views", 1, 0).unwrap();
    w.write_inc("views", -1, 0).unwrap();
    assert_eq!(w.visible(), doc(&[("views", i(0))]));

    // $inc 0 on a missing field creates it at 0.
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&[("n", s("a"))], 0).unwrap();
    w.write_inc("x", 0, 0).unwrap();
    assert_eq!(w.visible(), doc(&[("n", s("a")), ("x", i(0))]));

    // {x:100}; $unset x; $inc x +5; $inc x -5  =>  {x:0}
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&[("x", i(100))], 0).unwrap();
    w.write_unset("x", 0).unwrap();
    w.write_inc("x", 5, 0).unwrap();
    w.write_inc("x", -5, 0).unwrap();
    assert_eq!(w.visible(), doc(&[("x", i(0))]));
}

#[test]
fn zero_sum_counter_survives_the_join() {
    // The same sequence on a replica that only receives the states, in either order.
    let mut a = Writer::new(wid('A', 0), opts());
    a.upsert_inc("views", 1, 0).unwrap();
    let mid = a.state.clone();
    a.write_inc("views", -1, 0).unwrap();
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
    w.insert(&[("x", i(10))], 0).unwrap();
    w.write_inc("x", 3, 0).unwrap();
    w.write_set("x", i(20), 0).unwrap();
    let r = w.state.regs.values().find(|r| r.value == i(20)).unwrap();
    assert!(!r.obs.is_empty() && r.obs.values().all(|(st, t)| *t == 3 && *st < r.stamp));
    w.write_inc("x", 4, 0).unwrap();
    w.write_inc("x", -4, 0).unwrap();
    assert_eq!(w.visible(), doc(&[("x", i(20))]));
}

#[test]
fn b1_mutant_is_visible_to_the_lib_tests() {
    // With the slot stamp ignored the zero-sum counter is absent again: the mutant is killed.
    let o = JoinOptions { value_ignores_slot_stamp: true, ..opts() };
    let mut w = Writer::new(wid('A', 0), o);
    w.insert(&[("n", s("a"))], 0).unwrap();
    w.write_inc("views", 1, 0).unwrap();
    w.write_inc("views", -1, 0).unwrap();
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
        a.upsert_soi(&sv, "x", 1, &[], 0).unwrap();
    } else {
        a.upsert_soi(&[], "x", 1, &sv, 0).unwrap();
    }
    b.upsert_inc("n", 5, 0).unwrap();
    c.receive(&a.state);
    c.write_set("n", i(7), 0).unwrap();
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
        assert_eq!(c.hidden_all(), BTreeMap::from([("n".to_string(), 5)]), "soi={soi}");
    }
}

#[test]
fn m1_mutant_is_visible_to_the_lib_tests() {
    let o = JoinOptions { creation_without_generation: true, ..opts() };
    for soi in [false, true] {
        let c = run_upsert_string_gen(&o, soi);
        assert_eq!(c.visible(), doc(&[("n", i(12)), ("x", i(1))]), "soi={soi}");
        assert!(c.hidden_all().is_empty());
    }
}

#[test]
fn upsert_creation_generation_is_recorded_like_insert() {
    let mut w = Writer::new(wid('A', 0), opts());
    let (c, _) =
        w.upsert_soi(&[("t", s("dark"))], "v", 1, &[("l", f(1.5)), ("k", i(3))], 0).unwrap();
    let st = c.stamp;
    for p in ["t", "l"] {
        let r = &w.state.regs[&(st, p.to_string())];
        let g = r.generation.as_ref().expect("non-numeric creation value starts a generation");
        assert_eq!(g.stamp, st);
        assert_eq!(g.vv, BTreeMap::from([(st.writer, st.hlc)]));
        assert!(g.seen.is_empty());
        assert_eq!(w.state.gens[&(st, p.to_string())], (st, g.vv.clone()));
    }
    assert!(w.state.regs[&(st, "k".to_string())].generation.is_none());
    assert!(!w.state.gens.contains_key(&(st, "k".to_string())));
}

// ---- m1: counter totals cannot overflow the read side ------------------------------------

/// A state with one upserted life and a slot on `x` for writers B and C, set to `total` each.
fn state_with_big_slots(total: i128) -> (MergeState, MStamp) {
    let mut w = Writer::new(wid('A', 0), opts());
    w.upsert_inc("x", 1, 0).unwrap();
    let mut st = w.state.clone();
    let life = *st.lives.keys().next().unwrap();
    let stamp = MStamp { hlc: h(1), writer: wid('B', 0) };
    for c in ['B', 'C'] {
        st.slots.insert((life, "x".into(), wid(c, 0), None), Slot { stamp, total });
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
    w.insert(&[("x", i(1))], 0).unwrap();
    w.write_inc("x", 2, 0).unwrap();
    w.write_set("x", s("t"), 0).unwrap();
    let mut good = w.state.clone();
    good.losses.insert(
        LossKey {
            kind: LossKind::Clash,
            path: "x".into(),
            stamp: *good.lives.keys().next().unwrap(),
        },
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
    assert_eq!(st.value("x", &o), Some(i(i64::MAX)), "saturates at the bound");
    assert_eq!(st.visible(&o).unwrap()["x"], i(i64::MAX));
    let _ = st.hidden_all(&o);
    let (st, _) = state_with_big_slots(i128::MIN);
    assert_eq!(st.value("x", &o), Some(i(i64::MIN)));
    let mut w = Writer::from_state(wid('A', 0), st, o);
    assert!(w.write_inc("x", -1, 0).is_none(), "one past the lower bound");
    assert!(w.write_inc("x", 1, 0).is_some());
}

#[test]
fn write_inc_refuses_a_slot_past_the_total_bound() {
    // A's own slot sits just under the bound, balanced by B's: the visible value is small, so
    // the Int64 check passes, but the slot itself may not pass MAX_TOTAL.
    let mut w = Writer::new(wid('A', 0), opts());
    w.upsert_inc("x", 1, 0).unwrap();
    let life = *w.state.lives.keys().next().unwrap();
    let stamp = MStamp { hlc: h(1), writer: wid('B', 0) };
    w.state.slots.insert(
        (life, "x".into(), wid('A', 0), None),
        Slot { stamp: w.state.slots.values().next().unwrap().stamp, total: MAX_TOTAL - 2 },
    );
    w.state
        .slots
        .insert((life, "x".into(), wid('B', 0), None), Slot { stamp, total: -(MAX_TOTAL - 2) });
    assert_eq!(w.visible().unwrap()["x"], i(0));
    let clock = w.clock;
    assert!(w.write_inc("x", 3, 0).is_none(), "MAX_TOTAL + 1");
    assert_eq!(w.clock, clock, "a refused write does not tick");
    assert!(w.write_inc("x", 2, 0).is_some(), "exactly MAX_TOTAL");
    assert!(w.write_inc("x", 1, 0).is_none());
    assert!(w.write_inc("x", -1, 0).is_some());
}

// ---- m3: mutant memory is outside the format ------------------------------------------------

#[test]
fn generation_vv_and_gens_are_outside_bytes_hash_and_equality() {
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&[("x", i(1)), ("t", s("a"))], 0).unwrap();
    w.write_set("t", s("b"), 0).unwrap();
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
