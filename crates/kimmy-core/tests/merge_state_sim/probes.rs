//! The probes that need the pull protocol: probes5.py `probe_V4M2_epoch_twice`
//! and targeted5.py (`restore_inc_scenario`, `snapshot_loss_scenario`). The
//! state-level probes live in `merge_state/tests.rs`.

use std::collections::BTreeMap;

use kimmy_core::hlc::Hlc;
use kimmy_core::merge_state::{MStamp, Path, Value};

use crate::grade::grade;
use crate::oracle::Log;
use crate::sim::{Cfg, Ent, Node, fix_key, fixpoint, p, pull};

fn x_of(n: &Node) -> Value {
    n.visible().and_then(|d| d.get(&p("x")).cloned()).expect("x is visible")
}

/// Restoring the same backup twice. The epoch from the restore's start HLC plus
/// 64 random bits: 125 on every member. The mutant (a counter kept in the
/// store, which the backup carries): 105 with no record. Returns the value of
/// `x` on every member.
pub fn v4m2_epoch_twice(cfg: &Cfg) -> BTreeMap<char, Value> {
    let mut log = Log::new();
    let mut ms: Vec<Node> = "ABC".chars().map(|c| Node::new(c, cfg)).collect();
    ms[0].insert(&[("x", Value::Int64(100))], &mut log, 0);
    pull(&mut ms, 1, 0, None);
    pull(&mut ms, 2, 0, None);
    ms[0].backup();
    ms[0].restore();
    ms[0].inc("x", 20, &mut log, 0);
    pull(&mut ms, 1, 0, None);
    pull(&mut ms, 2, 0, None);
    ms[0].restore();
    ms[0].inc("x", 5, &mut log, 0);
    fixpoint(&mut ms, 40);
    ms.iter().map(|m| (m.name, x_of(m))).collect()
}

/// True when the design gives 125 everywhere and the mutant 105 everywhere.
pub fn v4m2_check(design: &Cfg, mutant: &Cfg) -> bool {
    let d = v4m2_epoch_twice(design);
    let m = v4m2_epoch_twice(mutant);
    d.values().all(|v| *v == Value::Int64(125)) && m.values().all(|v| *v == Value::Int64(105))
}

type Scenario = fn(&Cfg) -> (Vec<Node>, Log);

/// x = 100, A is backed up; A: $inc +20 (A@2, acknowledged); C pulls it. A is
/// restored (new epoch) and serves $inc +5. B pulls from A first. Then
/// everyone pulls to a fixpoint. Expected everywhere: 125. (Increments, so no
/// member detects a loss: a loss entry from C would raise C's own origin, and
/// the low threshold of that pull would over-fetch the lost write by luck.)
pub fn restore_inc_scenario(cfg: &Cfg) -> (Vec<Node>, Log) {
    let mut log = Log::new();
    let mut ms: Vec<Node> = "ABC".chars().map(|c| Node::new(c, cfg)).collect();
    ms[0].insert(&[("x", Value::Int64(100))], &mut log, 0);
    pull(&mut ms, 1, 0, None);
    pull(&mut ms, 2, 0, None);
    ms[0].backup();
    ms[0].inc("x", 20, &mut log, 0);
    pull(&mut ms, 2, 0, None);
    ms[0].restore();
    ms[0].inc("x", 5, &mut log, 0);
    pull(&mut ms, 1, 0, None);
    (ms, log)
}

/// The record clash(B@2) is detected only in a snapshot join, on S, and only S
/// ever holds it. I inserts x=0 everywhere. B: $set x "b" (B@2). C pulls B, then
/// $set x "c" (C@3; it saw B@2). A, clock ahead: $set x "a" (A@6). S pulls B
/// only, then takes a SNAPSHOT from A (stands in for a BeyondHorizon): that join
/// compares B@2 with A@6 and records clash(B@2). S then pulls C: clash(C@3). O,
/// B and C pull C and then A, so they compare only C@3 with A@6. A then leaves
/// the cluster (a dead origin: the design says a loss reaches every member
/// whether or not its origin is alive). Survivors: I, B, C, S, O.
pub fn snapshot_loss_scenario(cfg: &Cfg) -> (Vec<Node>, Log) {
    let mut log = Log::new();
    // indices: I0 A1 B2 C3 S4 O5
    let mut ms: Vec<Node> = "IABCSO".chars().map(|c| Node::new(c, cfg)).collect();
    ms[0].insert(&[("x", Value::Int64(0))], &mut log, 0);
    for m in 1..6 {
        pull(&mut ms, m, 0, None);
    }
    ms[2].set("x", Value::String("b".into()), &mut log, 0);
    pull(&mut ms, 3, 2, None);
    ms[3].set("x", Value::String("c".into()), &mut log, 0);
    ms[1].w.clock = Hlc::new(5, 0);
    ms[1].set("x", Value::String("a".into()), &mut log, 0);
    pull(&mut ms, 4, 2, None);
    {
        let (a, b) = ms.split_at_mut(4);
        b[0].snapshot_from(&a[1]);
    }
    pull(&mut ms, 4, 3, None);
    for m in [5, 2, 3] {
        pull(&mut ms, m, 3, None);
        pull(&mut ms, m, 1, None);
    }
    ms.remove(1); // A leaves: [I, B, C, S, O]
    (ms, log)
}

fn fixpoint_in(ms: &mut [Node], order: &[(usize, usize)]) {
    for _ in 0..40 {
        let before = fix_key(ms);
        for &(p, r) in order {
            pull(ms, p, r, None);
        }
        if fix_key(ms) == before {
            return;
        }
    }
}

fn permutations(items: &[(usize, usize)]) -> Vec<Vec<(usize, usize)>> {
    fn go(
        items: &[(usize, usize)],
        used: &mut Vec<bool>,
        cur: &mut Vec<(usize, usize)>,
        out: &mut Vec<Vec<(usize, usize)>>,
    ) {
        if cur.len() == items.len() {
            out.push(cur.clone());
            return;
        }
        for i in 0..items.len() {
            if !used[i] {
                used[i] = true;
                cur.push(items[i]);
                go(items, used, cur, out);
                cur.pop();
                used[i] = false;
            }
        }
    }
    let mut out = Vec::new();
    go(items, &mut vec![false; items.len()], &mut Vec::new(), &mut out);
    out
}

pub struct Targeted {
    pub label: &'static str,
    pub orders: usize,
    /// Orders in which the design did NOT converge to a sound state (must be 0).
    pub design_bad: usize,
    /// Orders in which the mutant did NOT fail (must be 0).
    pub mutant_ok: usize,
}

impl Targeted {
    pub fn pass(&self) -> bool {
        self.design_bad == 0 && self.mutant_ok == 0
    }
}

/// Every order of the final pulls (all 720 for 3 members; two orders for more),
/// for the design and for the mutant: the design must be sound in every order,
/// the mutant must fail in every order.
fn targeted(label: &'static str, scenario: Scenario, design: &Cfg, mutant: &Cfg) -> Targeted {
    let n = scenario(design).0.len();
    let pairs: Vec<(usize, usize)> =
        (0..n).flat_map(|p| (0..n).filter(move |r| *r != p).map(move |r| (p, r))).collect();
    let orders = if pairs.len() <= 6 {
        permutations(&pairs)
    } else {
        let mut rev = pairs.clone();
        rev.reverse();
        vec![pairs, rev]
    };
    let mut t = Targeted { label, orders: orders.len(), design_bad: 0, mutant_ok: 0 };
    for order in &orders {
        let (mut ms, log) = scenario(design);
        fixpoint_in(&mut ms, order);
        if !grade(&ms, &log).is_empty() {
            t.design_bad += 1;
        }
        let (mut ms, log) = scenario(mutant);
        fixpoint_in(&mut ms, order);
        if grade(&ms, &log).is_empty() {
            t.mutant_ok += 1;
        }
    }
    t
}

/// V3M2: coverage keyed by node, not (node, epoch).
pub fn targeted_v3m2(design: &Cfg, mutant: &Cfg) -> Targeted {
    targeted("V3M2 restore, increments", restore_inc_scenario, design, mutant)
}

/// V3m1: snapshot joins append no loss entries.
pub fn targeted_v3m1(design: &Cfg, mutant: &Cfg) -> Targeted {
    targeted("V3m1 snapshot-join loss entry", snapshot_loss_scenario, design, mutant)
}

/// Run a scenario on a cluster and grade it against the oracle.
fn graded(cfg: &Cfg, names: &str, f: impl FnOnce(&mut Vec<Node>, &mut Log)) -> Vec<String> {
    let mut log = Log::new();
    let mut ms: Vec<Node> = names.chars().map(|c| Node::new(c, cfg)).collect();
    f(&mut ms, &mut log);
    fixpoint(&mut ms, 40);
    grade(&ms, &log)
}

/// The B1 and M1 scenarios (a counter that nets to zero; a non-numeric upsert creation value),
/// each with the problems the oracle finds.
pub fn scenarios() -> Vec<(String, Vec<String>)> {
    scenarios_with(&Cfg::default_ret())
}

/// [`scenarios`] on the given configuration: a mutant is killed by them when they find a problem.
pub fn scenarios_with(cfg: &Cfg) -> Vec<(String, Vec<String>)> {
    let cfg = cfg.clone();
    let s = |x: &str| Value::String(x.into());
    let mut out = Vec::new();
    out.push((
        "zero sum: insert {n:a}; +1; -1".to_string(),
        graded(&cfg, "A", |ms, log| {
            ms[0].insert(&[("n", s("a"))], log, 0);
            ms[0].inc("views", 1, log, 0);
            ms[0].inc("views", -1, log, 0);
        }),
    ));
    out.push((
        "zero sum: upsert +1; -1".to_string(),
        graded(&cfg, "A", |ms, log| {
            ms[0].upsert_inc("views", 1, log, 0);
            ms[0].inc("views", -1, log, 0);
        }),
    ));
    out.push((
        "zero sum: $inc 0 on a missing field".to_string(),
        graded(&cfg, "A", |ms, log| {
            ms[0].insert(&[("n", s("a"))], log, 0);
            ms[0].inc("views", 0, log, 0);
        }),
    ));
    out.push((
        "zero sum: {x:100}; $unset x; +5; -5".to_string(),
        graded(&cfg, "A", |ms, log| {
            ms[0].insert(&[("x", Value::Int64(100))], log, 0);
            ms[0].unset("x", log, 0);
            ms[0].inc("x", 5, log, 0);
            ms[0].inc("x", -5, log, 0);
        }),
    ));
    for soi in [false, true] {
        out.push((
            format!("upsert string base ({})", if soi { "$setOnInsert" } else { "$set" }),
            graded(&cfg, "ABC", |ms, log| {
                let sv = [("n", s("s"))];
                if soi {
                    ms[0].upsert_soi(&sv, "x", 1, &[], log, 0);
                } else {
                    ms[0].upsert_soi(&[], "x", 1, &sv, log, 0);
                }
                ms[1].upsert_inc("n", 5, log, 0);
                pull(ms, 2, 0, None);
                ms[2].set("n", Value::Int64(7), log, 0);
            }),
        ));
    }
    out
}

// ---- a write lost with no record under the covered-entry skip ---------------------------------------------------------

/// A lost write must not be left with no record when its map cell is dropped by a join that reports nothing for it (a map that
/// loses to a map, or an implicit marker that loses, each dropped without a record) and a later delete then wins over the
/// write: under the covered-entry skip nobody joins the write's own entry again, so no record would be made at all. The
/// model's `sync` hides it (it re-joins covered entries); the product's skip does not. Each scenario is the same schedule
/// (the review's `s1_repro.py`):
/// A inserts `{k: 0}`; C deletes, concurrently with everything below; A writes `a1`; B, concurrent and newer (skew 4),
/// writes `b`; A and B take each other's entry; A writes `a2` in sequence after `a1` (`a2_before_b`: right after `a1`, and
/// then `k = 1` is A's last write); C takes A's last entry; B takes it too (`b_sees_last`: so that no member still holds A's
/// first write below `a` when the delete reaches it); A and B take the delete; then every member takes every entry of every
/// member, three times. The skip is ON (the design). Graded against the oracle: the problems found; none is the answer.
///
/// The design (an explicit map that loses to a map is a clash; an implicit marker is never a head of its own) leaves every
/// head with a record at or below it. A join that drops the clash of a map that loses to a map fails the first of the five.
pub fn s1_scenarios_with(cfg: &Cfg) -> Vec<(String, Vec<String>)> {
    let mut cfg = cfg.clone();
    cfg.flags.covered_skip = true;
    let int = Value::Int64;
    let map = |f: &[(&str, Value)]| {
        Value::Document(f.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect())
    };
    // the sim's `set(path)` takes ONE field name (a name may hold a dot): these writes go to nested paths
    let np = |dotted: &str| Path::new(dotted.split('.')).unwrap();
    type W = (&'static str, Value);
    // (name, a1, b, a2, a2 before b)
    let cases: Vec<(&str, W, W, W, bool)> = vec![
        ("S1b'", ("a", map(&[])), ("a.c", int(2)), ("k", int(1)), false),
        ("S1c'", ("a", map(&[("x", int(1))])), ("a.c", int(2)), ("a.x", int(3)), false),
        (
            "S1e'",
            ("a", map(&[("x", int(1))])),
            ("a", map(&[("y", int(2))])),
            ("a.x", int(3)),
            false,
        ),
        ("S1m'", ("a.x", int(1)), ("a", map(&[])), ("a.x", int(3)), false),
        ("S1s", ("a.x", int(1)), ("a", int(7)), ("a.x", int(3)), true),
    ];
    let mut out = Vec::new();
    for (name, a1, b, a2, a2_before_b) in cases {
        let mut log = Log::new();
        let mut ms: Vec<Node> = "ABC".chars().map(|c| Node::new(c, &cfg)).collect();
        let ent = |n: &Node, st: Option<MStamp>| {
            std::rc::Rc::clone(&n.oplog[&st.expect("the write was accepted")])
        };
        let i0 = ms[0].insert(&[("k", int(0))], &mut log, 0);
        let e0 = ent(&ms[0], i0);
        ms[1].receive_entry(&e0);
        ms[2].receive_entry(&e0);
        let dc = ms[2].delete(&mut log, 0);
        let ec = ent(&ms[2], dc);
        let s_a1 = ms[0].set_at(&np(a1.0), a1.1.clone(), &mut log, 0);
        let mut ea = ent(&ms[0], s_a1);
        if a2_before_b {
            let s_a2 = ms[0].set_at(&np(a2.0), a2.1.clone(), &mut log, 0);
            ea = ent(&ms[0], s_a2);
        }
        let s_b = ms[1].set_at(&np(b.0), b.1.clone(), &mut log, 4);
        let eb = ent(&ms[1], s_b);
        ms[0].receive_entry(&eb);
        ms[1].receive_entry(&ea);
        let last = if a2_before_b {
            ms[0].set("k", int(1), &mut log, 0)
        } else {
            ms[0].set_at(&np(a2.0), a2.1.clone(), &mut log, 0)
        };
        let el = ent(&ms[0], last);
        ms[2].receive_entry(&el);
        ms[1].receive_entry(&el);
        ms[0].receive_entry(&ec);
        ms[1].receive_entry(&ec);
        for _ in 0..3 {
            for p in 0..ms.len() {
                for r in 0..ms.len() {
                    let es: Vec<std::rc::Rc<Ent>> = ms[r].oplog.values().cloned().collect();
                    for e in &es {
                        ms[p].receive_entry(e);
                    }
                }
            }
        }
        out.push((format!("S1 {name} under the covered-entry skip"), grade(&ms, &log)));
    }
    out
}

// ---- the counter cell's fold (review item M2) ----------------------------------------------

/// Every member's value of `path` after the fixpoint, against `want`: the problems found.
fn all_read(ms: &[Node], path: &str, want: i64) -> Vec<String> {
    ms.iter()
        .filter_map(|n| {
            let got = n.visible().and_then(|d| d.get(&p(path)).cloned());
            (got != Some(Value::Int64(want)))
                .then(|| format!("{} reads {got:?}, not {want}", n.name))
        })
        .collect()
}

/// The fold scenarios of the counter cell, read by value (not through the oracle): a base write must
/// absorb every slot its writer holds in an alive life, of any generation, not only the slots it counts.
/// The four-step case of the review (M2): x = 100; B adds 5; C, which saw only the insert, sets x to a
/// string and receives the 5 (an older generation: hidden), then sets x to 7; D, which saw only the
/// insert, sets 50 with the newest stamp. Serial orders read 7 or 50, never 55. And EG2, the double-base
/// form: A adds 1, B sets a to 2.5 (a generation that hides A's add), A receives it and sets 5, C (saw
/// only the insert) sets 5 with the newest stamp: 5, never 6. A mutant that folds only the counted slots
/// reads 55 and 6. And the marker scenario: `$set a {b: 1}` over a counter at `a` folds its slot (the
/// mutant that folds nothing at a map marker leaves the slot without its pair). And the cross-life
/// scenario: a base in the newest alive life folds a slot of another alive life (the mutant that folds
/// only the write's own life leaves that slot without its pair).
pub fn fold_scenarios_with(cfg: &Cfg) -> Vec<(String, Vec<String>)> {
    let s = |x: &str| Value::String(x.into());
    let mut out = Vec::new();
    {
        let mut log = Log::new();
        let mut ms: Vec<Node> = "ABCD".chars().map(|c| Node::new(c, cfg)).collect();
        ms[0].insert(&[("x", Value::Int64(100))], &mut log, 0);
        pull(&mut ms, 1, 0, None);
        ms[1].inc("x", 5, &mut log, 0);
        pull(&mut ms, 2, 0, None);
        ms[2].set("x", s("s"), &mut log, 0);
        pull(&mut ms, 2, 1, None);
        ms[2].set("x", Value::Int64(7), &mut log, 0);
        pull(&mut ms, 3, 0, None);
        ms[3].set("x", Value::Int64(50), &mut log, 100);
        fixpoint(&mut ms, 40);
        out.push(("fold: the four-step case reads 50".to_string(), all_read(&ms, "x", 50)));
    }
    {
        let mut log = Log::new();
        let mut ms: Vec<Node> = "ABC".chars().map(|c| Node::new(c, cfg)).collect();
        ms[0].insert(&[("a", Value::Int64(0))], &mut log, 0);
        pull(&mut ms, 1, 0, None);
        pull(&mut ms, 2, 0, None);
        ms[0].inc("a", 1, &mut log, 0);
        ms[1].set("a", Value::Double(2.5), &mut log, 0);
        pull(&mut ms, 0, 1, None);
        ms[0].set("a", Value::Int64(5), &mut log, 0);
        ms[2].set("a", Value::Int64(5), &mut log, 100);
        fixpoint(&mut ms, 40);
        out.push((
            "fold: EG2, a double base `$set a 2.5`, reads 5".to_string(),
            all_read(&ms, "a", 5),
        ));
    }
    {
        // A map-marker base over a counter folds the counter's slots: `$set a {b: 1}` over a counter at `a`
        // writes a marker as a register of `a` (a base) and a leaf below it, and the slot of `a` takes the
        // pair the marker held. Read through the state, not by value (what the pair changes is what a later
        // base counts, and the mutant that folds nothing at a marker leaves the slot without a pair).
        let mut log = Log::new();
        let mut ms: Vec<Node> = "A".chars().map(|c| Node::new(c, cfg)).collect();
        ms[0].insert(&[("a", Value::Int64(1))], &mut log, 0);
        ms[0].inc("a", 5, &mut log, 0);
        ms[0].set("a", Value::Document(vec![("b".to_string(), Value::Int64(1))]), &mut log, 0);
        let slots: Vec<_> = ms[0].state().slots.values().collect();
        let problems = match slots.as_slice() {
            [sl] if sl.pair() == Some((sl.stamp, sl.total)) => Vec::new(),
            [sl] => vec![format!("the marker write left the slot's pair at {:?}", sl.absorbed)],
            _ => vec![format!("{} slots, not one", slots.len())],
        };
        out.push(("fold: a map-marker base over a counter folds its slots".to_string(), problems));
    }
    {
        // A base write in the NEWEST alive life folds a slot of ANOTHER alive life (the shape of seed 792 of
        // the nested differential): C inserts `c` and adds 5 (a slot in C's insert life); D upserts a counter
        // at `y` (an upsert life beside the insert; D's life is the newer, same HLC and the greater writer);
        // C pulls it and sets `c` to a double, a base written in D's life. The slot of C's life takes the
        // pair the base held; the mutant that folds only the slots of the write's own life leaves it bare.
        let mut log = Log::new();
        let mut ms: Vec<Node> = "CD".chars().map(|c| Node::new(c, cfg)).collect();
        ms[0].insert(&[("c", Value::Int64(0))], &mut log, 0);
        ms[0].inc("c", 5, &mut log, 0);
        ms[1].upsert_inc("y", 1, &mut log, 0);
        pull(&mut ms, 0, 1, None);
        let alive = ms[0].w.alive();
        ms[0].set("c", Value::Double(2.5), &mut log, 0);
        let slots: Vec<_> = ms[0].state().slots.iter().filter(|(k, _)| k.1 == p("c")).collect();
        let problems = match slots.as_slice() {
            _ if alive.len() != 2 => vec![format!("{} alive lives, not two", alive.len())],
            [(k, _)] if Some(&k.0) == alive.last() => {
                vec!["the slot is in the write's own life, not in another".to_string()]
            }
            [(_, sl)] if sl.pair() == Some((sl.stamp, sl.total)) => Vec::new(),
            [(_, sl)] => vec![format!("the base left the other life's slot at {:?}", sl.absorbed)],
            _ => vec![format!("{} slots at c, not one", slots.len())],
        };
        out.push((
            "fold: a base in the newest alive life folds a slot of another alive life".to_string(),
            problems,
        ));
    }
    // ---- the absorbed pair belongs to the life of the base write that made it (note q7-absorbed-by-writing-life, section 7.1) ----
    {
        // (1) a base made into a life that is then superseded absorbs nothing (the shape of rich seeds 943 and 2570): A upserts `$inc c 1`
        // (life U); B inserts `{k: 1}` (P, newer than U by writer name); A receives P and unsets `c`, a base INTO P that folds U's slot; C
        // inserts `{k: 2}` with the newest stamp: P is superseded and the write into it is dropped. `c` reads 1 on every member.
        let mut log = Log::new();
        let mut ms: Vec<Node> = "ABC".chars().map(|c| Node::new(c, cfg)).collect();
        ms[0].upsert_inc("c", 1, &mut log, 0);
        ms[1].insert(&[("k", Value::Int64(1))], &mut log, 0);
        pull(&mut ms, 0, 1, None);
        ms[0].unset("c", &mut log, 0);
        ms[2].insert(&[("k", Value::Int64(2))], &mut log, 100);
        fixpoint(&mut ms, 40);
        out.push((
            "fold: a base made into a superseded life absorbs nothing".to_string(),
            all_read(&ms, "c", 1),
        ));
    }
    {
        // (2) case_two_writing_lives: ONE slot absorbed by two bases in two writing lives, the higher pair in the life that loses. A upserts
        // `+1` (U); D pulls it; A adds `+1` (the slot reads 2); B inserts P; A pulls P and unsets `c` INTO P (the pair of P is 2); D, holding
        // only U, sets `c` to 5 INTO U (the pair of U is 1); C inserts Q, newer: P is superseded. The value is 6. The only case that kills a
        // single-pair-per-slot layout. A fresh node E pulls A, then D, then C BEFORE the fixpoint and must read 6 on its own: the fixpoint's
        // member order alone is not enough (a change of the pull order, the skip or the stamps could make the members converge on 6 under
        // the mutant and the probe would pass). E reads 6 with the design and 7 under `single_pair`.
        let mut log = Log::new();
        let mut ms: Vec<Node> = "ABCD".chars().map(|c| Node::new(c, cfg)).collect();
        ms[0].upsert_inc("c", 1, &mut log, 0);
        pull(&mut ms, 3, 0, None);
        ms[0].inc("c", 1, &mut log, 0);
        ms[1].insert(&[("k", Value::Int64(1))], &mut log, 0);
        pull(&mut ms, 0, 1, None);
        ms[0].unset("c", &mut log, 0);
        ms[3].set("c", Value::Int64(5), &mut log, 3);
        ms[2].insert(&[("k", Value::Int64(2))], &mut log, 9);
        ms.push(Node::new('E', cfg));
        pull(&mut ms, 4, 0, None);
        pull(&mut ms, 4, 3, None);
        pull(&mut ms, 4, 2, None);
        let mut problems = all_read(&ms[4..5], "c", 6);
        fixpoint(&mut ms, 40);
        problems.extend(all_read(&ms, "c", 6));
        out.push((
            "fold: two writing lives absorb one slot at different stamps".to_string(),
            problems,
        ));
    }
    {
        // (3) case_unset_several_lives: one base (`$unset c`) absorbs the slots of TWO other lives (two upserts of `c`, U1 and U2) from P; P
        // is superseded. Both counters keep their increments: 1 + 2 = 3.
        let mut log = Log::new();
        let mut ms: Vec<Node> = "ABCD".chars().map(|c| Node::new(c, cfg)).collect();
        ms[0].upsert_inc("c", 1, &mut log, 0);
        ms[3].upsert_inc("c", 2, &mut log, 0);
        ms[1].insert(&[("k", Value::Int64(1))], &mut log, 1);
        pull(&mut ms, 0, 3, None);
        pull(&mut ms, 0, 1, None);
        ms[0].unset("c", &mut log, 0);
        ms[2].insert(&[("k", Value::Int64(2))], &mut log, 9);
        fixpoint(&mut ms, 40);
        out.push((
            "fold: one base into a lost life absorbs the slots of two other lives".to_string(),
            all_read(&ms, "c", 3),
        ));
    }
    {
        // (4) case_upsert_writes_absorb_insert_slot: the writing life is the UPSERT life U (newer than the first insert P) and the absorbed
        // slot is P's; P is then superseded. Reads 7. And NO PAIR is left without its slot or with a dead writing life, on any member.
        let mut log = Log::new();
        let mut ms: Vec<Node> = "ABC".chars().map(|c| Node::new(c, cfg)).collect();
        ms[1].insert(&[("k", Value::Int64(1))], &mut log, 0);
        ms[1].inc("c", 4, &mut log, 0);
        ms[0].upsert_inc("c", 1, &mut log, 3);
        pull(&mut ms, 0, 1, None);
        ms[0].set("c", Value::Int64(7), &mut log, 0);
        ms[2].insert(&[("k", Value::Int64(2))], &mut log, 9);
        fixpoint(&mut ms, 40);
        let mut problems = all_read(&ms, "c", 7);
        for n in &ms {
            let alive = n.w.alive();
            for (k, sl) in n.state().slots.iter() {
                if !alive.contains(&k.0) {
                    problems.push(format!("{}: a slot of the dead life {:?}", n.name, k.0));
                }
                for pr in &sl.absorbed {
                    if !alive.contains(&pr.life) {
                        problems.push(format!(
                            "{}: a pair of the dead writing life {:?}",
                            n.name, pr.life
                        ));
                    }
                }
            }
        }
        out.push(("fold: no pair outlives its slot or its writing life".to_string(), problems));
    }
    {
        // (5) case_inc_after_absorb_in_lost_life: after the absorbing unset, A adds `+2` (a new slot in P); P is superseded: reads 1.
        let mut log = Log::new();
        let mut ms: Vec<Node> = "ABC".chars().map(|c| Node::new(c, cfg)).collect();
        ms[0].upsert_inc("c", 1, &mut log, 0);
        ms[1].insert(&[("k", Value::Int64(1))], &mut log, 0);
        pull(&mut ms, 0, 1, None);
        ms[0].unset("c", &mut log, 0);
        ms[0].inc("c", 2, &mut log, 0);
        ms[2].insert(&[("k", Value::Int64(2))], &mut log, 9);
        fixpoint(&mut ms, 40);
        out.push((
            "fold: an increment after the absorb, into the lost life, is lost with it".to_string(),
            all_read(&ms, "c", 1),
        ));
    }
    out
}

// ---- Q3: a collected life must not come back (probes_q3.py of the model) -------------------

/// What a Q3 script found: A's document right after the pull that could bring a collected life back,
/// every member's document after the fixpoint, how many entries A was handed by that pull, and what
/// the oracle says of the finished run.
pub struct Q3Out {
    pub a_after: Option<BTreeMap<kimmy_core::merge_state::Path, Value>>,
    pub finals: Vec<Option<BTreeMap<kimmy_core::merge_state::Path, Value>>>,
    pub delivered: usize,
    pub graded: Vec<String>,
}

impl Q3Out {
    /// Did the collected life (`old`) come back at A?
    pub fn resurrected(&self) -> bool {
        self.a_after.as_ref().is_some_and(|d| d.contains_key(&p("old")))
    }

    /// Clean: no `old` at A, `y` written, every member alike, and the oracle finds nothing.
    pub fn clean(&self) -> bool {
        let y = self.a_after.as_ref().and_then(|d| d.get(&p("y")));
        !self.resurrected()
            && y == Some(&Value::Int64(1))
            && self.finals.iter().all(|f| *f == self.finals[0])
            && self.finals[0].as_ref().is_some_and(|d| !d.contains_key(&p("old")))
            && self.graded.is_empty()
    }
}

/// The retention of the Q3 scripts (R = 10), with the flags of the configuration under test.
fn q3_cfg(cfg: &Cfg) -> Cfg {
    let mut c = Cfg::new(10);
    c.flags = cfg.flags;
    c
}

fn q3_old_life_collected(a: &Node) {
    assert!(
        !a.visible().is_some_and(|d| d.contains_key(&p("old"))),
        "setup: the old life is collected"
    );
}

/// The review's case: R = 10, A inserts {x, old}, deletes, re-inserts, writes 15 times, fixpoint; B takes a
/// backup and restores at once and writes y; A pulls from B. B's pulls for its new epoch start at the
/// epoch's start HLC (the start-HLC rule) or at 0 (a replay of B's whole oplog); the covered-entry skip
/// then only witnesses what A holds. Either rule alone keeps the life dead; with both off it comes back.
pub fn q3_restore(cfg: &Cfg) -> Q3Out {
    let cfg = q3_cfg(cfg);
    let mut log = Log::new();
    let mut ms: Vec<Node> = "AB".chars().map(|c| Node::new(c, &cfg)).collect();
    ms[0].insert(&[("x", Value::Int64(1)), ("old", Value::Int64(1))], &mut log, 0);
    ms[0].delete(&mut log, 0);
    ms[0].insert(&[("x", Value::Int64(2))], &mut log, 0);
    for i in 0..15 {
        ms[0].set("x", Value::Int64(10 + i), &mut log, 0);
    }
    fixpoint(&mut ms, 40);
    q3_old_life_collected(&ms[0]);
    ms[1].backup();
    ms[1].restore();
    ms[1].set("y", Value::Int64(1), &mut log, 0);
    let before = ms[0].entries_received;
    pull(&mut ms, 0, 1, None);
    let delivered = ms[0].entries_received - before;
    let a_after = ms[0].visible();
    fixpoint(&mut ms, 40);
    Q3Out {
        a_after,
        finals: ms.iter().map(Node::visible).collect(),
        delivered,
        graded: grade(&ms, &log),
    }
}

/// An idle origin (the review's M1: no restore at all). C inserts and is silent; A kills and re-creates the
/// document twice and writes 15 times; the fixpoint collects life 1 everywhere. C then writes y and A pulls
/// from C: the threshold is A's witnessed C, one old write, so C re-serves every entry it appended above
/// it, among them life 1's insert. The start-HLC rule cannot help (C's epoch is a first epoch): only the
/// covered-entry skip keeps the life dead.
pub fn q3_idle_origin(cfg: &Cfg) -> Q3Out {
    let cfg = q3_cfg(cfg);
    let mut log = Log::new();
    let mut ms: Vec<Node> = "ABC".chars().map(|c| Node::new(c, &cfg)).collect();
    ms[2].insert(&[("x", Value::Int64(0))], &mut log, 0);
    fixpoint(&mut ms, 40);
    ms[0].delete(&mut log, 0);
    ms[0].insert(&[("x", Value::Int64(1)), ("old", Value::Int64(1))], &mut log, 0);
    fixpoint(&mut ms, 40);
    ms[0].delete(&mut log, 0);
    ms[0].insert(&[("x", Value::Int64(2))], &mut log, 0);
    for i in 0..15 {
        ms[0].set("x", Value::Int64(10 + i), &mut log, 0);
    }
    fixpoint(&mut ms, 40);
    q3_old_life_collected(&ms[0]);
    ms[2].set("y", Value::Int64(1), &mut log, 0);
    let before = ms[0].entries_received;
    pull(&mut ms, 0, 2, None);
    let delivered = ms[0].entries_received - before;
    let a_after = ms[0].visible();
    fixpoint(&mut ms, 40);
    Q3Out {
        a_after,
        finals: ms.iter().map(Node::visible).collect(),
        delivered,
        graded: grade(&ms, &log),
    }
}

/// A late member. D joins after the fact (a first epoch nobody has witnessed), pulls everything one entry at
/// a time, writes y; A pulls from D: D's origin starts at 0 under either rule, so D re-serves the old entries.
/// Only the covered-entry skip keeps the collected life dead.
pub fn q3_late_member(cfg: &Cfg) -> Q3Out {
    let cfg = q3_cfg(cfg);
    let mut log = Log::new();
    let mut ms: Vec<Node> = "AB".chars().map(|c| Node::new(c, &cfg)).collect();
    ms[0].insert(&[("x", Value::Int64(1)), ("old", Value::Int64(1))], &mut log, 0);
    fixpoint(&mut ms, 40);
    ms[0].delete(&mut log, 0);
    ms[0].insert(&[("x", Value::Int64(2))], &mut log, 0);
    for i in 0..15 {
        ms[0].set("x", Value::Int64(10 + i), &mut log, 0);
    }
    fixpoint(&mut ms, 40);
    q3_old_life_collected(&ms[0]);
    ms.push(Node::new('D', &cfg));
    while pull(&mut ms, 2, 1, Some(1)) {
        let (b, d) = (&ms[1], &ms[2]);
        if b.servable.iter().all(|(o, h)| *h <= d.witnessed.get(o).copied().unwrap_or(Hlc::ZERO)) {
            break;
        }
    }
    ms[2].set("y", Value::Int64(1), &mut log, 0);
    let before = ms[0].entries_received;
    pull(&mut ms, 0, 2, None);
    let delivered = ms[0].entries_received - before;
    let a_after = ms[0].visible();
    fixpoint(&mut ms, 40);
    Q3Out {
        a_after,
        finals: ms.iter().map(Node::visible).collect(),
        delivered,
        graded: grade(&ms, &log),
    }
}

/// The Q3 scripts on the flags of the given configuration, named.
pub fn q3_scripts(cfg: &Cfg) -> Vec<(&'static str, Q3Out)> {
    vec![
        ("restore", q3_restore(cfg)),
        ("idle origin", q3_idle_origin(cfg)),
        ("late member", q3_late_member(cfg)),
    ]
}

/// The design is clean on all three scripts; with the covered-entry skip off the idle origin and the late
/// member bring the collected life back (the start-HLC rule cannot help them); with both rules off the
/// restore does too, and either rule alone clears it. Returns the problems found (empty when all hold).
pub fn q3_check() -> Vec<String> {
    let flags = |skip: bool, start: bool| {
        let mut c = Cfg::default_ret();
        c.flags.covered_skip = skip;
        c.flags.start_hlc = start;
        c
    };
    let mut bad = Vec::new();
    for (skip, start) in [(true, true), (true, false), (false, true)] {
        let r = q3_restore(&flags(skip, start));
        if !r.clean() {
            bad.push(format!("restore, skip {skip}, start {start}: not clean"));
        }
    }
    if !q3_restore(&flags(false, false)).resurrected() {
        bad.push("restore with both rules off should resurrect the old life".into());
    }
    for (name, f) in [
        ("idle origin", q3_idle_origin as fn(&Cfg) -> Q3Out),
        ("late member", q3_late_member as fn(&Cfg) -> Q3Out),
    ] {
        for start in [true, false] {
            if !f(&flags(true, start)).clean() {
                bad.push(format!("{name}, skip on, start {start}: not clean"));
            }
            if !f(&flags(false, start)).resurrected() {
                bad.push(format!("{name}, skip off, start {start}: the old life should come back"));
            }
        }
    }
    if !v3m1_order_independent(&Cfg::default_ret()) {
        bad.push("the design should hold one state whatever the order of the entries after a long partition".into());
    }
    if !q3_whole_vector(&Cfg::default_ret()) {
        bad.push(
            "the design should apply an entry whose stamp is covered but whose vector is not"
                .into(),
        );
    }
    // the start-HLC rule changes what is delivered, not the states: A is handed fewer entries by the
    // restored member's pull when a never-witnessed origin starts at its epoch's start HLC
    let (with, without) = (q3_restore(&flags(true, true)), q3_restore(&flags(true, false)));
    if with.delivered >= without.delivered {
        bad.push(format!(
            "the start-HLC rule should deliver fewer entries ({} against {})",
            with.delivered, without.delivered
        ));
    }
    bad
}

/// The covered-entry skip looks at the WHOLE vector of the entry (the model's rule, Step 3's), not at its
/// stamp: an entry whose stamp the receiver covers but whose state carries a write the receiver lacks must
/// be applied. In an honest run the two rules agree (a version vector is causally closed: covering a
/// writer's stamp means holding that writer's whole state at it), so only a hand-made entry tells them
/// apart: C holds A's insert; B writes y after receiving it; C is handed an entry stamped by A's insert
/// carrying B's state. Returns whether C learned B's write.
pub fn q3_whole_vector(cfg: &Cfg) -> bool {
    let mut log = Log::new();
    let mut ms: Vec<Node> = "ABC".chars().map(|c| Node::new(c, cfg)).collect();
    let a1 = ms[0].insert(&[("x", Value::Int64(1))], &mut log, 0).expect("inserted");
    pull(&mut ms, 1, 0, None);
    pull(&mut ms, 2, 0, None);
    ms[1].set("y", Value::Int64(2), &mut log, 0);
    let ent = std::rc::Rc::new(Ent::W {
        st: a1,
        state: ms[1].state().clone(),
        contained: ms[1].contained.clone(),
    });
    ms[2].receive_entry(&ent);
    ms[2].visible().is_some_and(|d| d.get(&p("y")) == Some(&Value::Int64(2)))
}

/// V3M1 under the covered-entry skip (the default protocol): a partition longer than retention
/// (ADR-085). C inserts and is silent from A, who kills the life; C then writes twice with its clock far
/// ahead (`u` and `u2`), so the horizon of any join with C's entries is past the dead life and its delete.
/// Two members that apply the SAME entries in different orders must agree: `W` applies A's delete `s`, then
/// `u`, then `u2`; `V` applies `s`, then `u2` (then `u`, which the skip witnesses and does not apply).
/// None of the entries is covered by the receiver's vector when it arrives, so the skip never applies:
/// this is the case where the order of horizon and aliveness still matters in the default protocol.
/// The design (the horizon of the joined vectors first) takes the dead life out BEFORE the union and
/// both members hold C's life; applied after aliveness, `W` collects the life at `u` and C's `u2`
/// brings it back, while `V` never lets it in. Returns whether `W` and `V` hold the same state.
pub fn v3m1_order_independent(cfg: &Cfg) -> bool {
    let mut cfg = cfg.clone();
    cfg.opts.retention = 10;
    let mut log = Log::new();
    let mut ms: Vec<Node> = "AC".chars().map(|c| Node::new(c, &cfg)).collect();
    ms[1].insert(&[("x", Value::Int64(1))], &mut log, 0);
    pull(&mut ms, 0, 1, None);
    ms[0].delete(&mut log, 0);
    ms[1].set("x", Value::Int64(2), &mut log, 1000);
    ms[1].set("x", Value::Int64(3), &mut log, 1000);
    let entry = |n: &Node, i: usize| std::rc::Rc::clone(n.oplog.values().nth(i).expect("an entry"));
    let s = entry(&ms[0], ms[0].oplog.len() - 1);
    let (u, u2) = (entry(&ms[1], 1), entry(&ms[1], 2));
    let mut w = Node::new('W', &cfg);
    let mut v = Node::new('V', &cfg);
    for e in [&s, &u, &u2] {
        w.receive_entry(e);
    }
    for e in [&s, &u2, &u] {
        v.receive_entry(e);
    }
    w.canon() == v.canon()
}

/// Step 3, ruling 9: the required case (two documents, three members; a single-document run cannot show it).
/// A sets `X.a` at s1; M, not having it, sets `X.b` at s2 > s1; M pulls A (the entry is joined and must be
/// APPENDED although M's newest stamp out-stamps it); A writes another document Y at s3 and M pulls it, which
/// raises M's servable[A] past s1; P, empty, pulls only M. P must read `X = {a, b}` with M's state, with no
/// snapshot and nothing left to move on a following P-to-A round. Returns what P read, whether P and M hold
/// the same state, and the snapshots taken anywhere.
pub struct OmittedOut {
    pub p_reads_a_and_b: bool,
    pub same_state: bool,
    pub snapshots: u32,
    pub follow_up_moved: bool,
}

pub fn omitted_lower_stamp_entry(cfg: &Cfg) -> OmittedOut {
    let mut log = Log::new();
    // indices: I0 A1 M2 P3; I inserts the document, A and M hold it, P is empty
    let mut ms: Vec<Node> = "IAMP".chars().map(|c| Node::new(c, cfg)).collect();
    ms[0].insert(&[("x", Value::Int64(0))], &mut log, 0).expect("I inserts X");
    pull(&mut ms, 1, 0, None);
    pull(&mut ms, 2, 0, None);
    ms[1].set("a", Value::Int64(1), &mut log, 0).expect("A writes X.a at s1");
    ms[2].w.clock = Hlc::new(5, 0);
    ms[2].set("b", Value::Int64(2), &mut log, 0).expect("M writes X.b at s2 above s1");
    pull(&mut ms, 2, 1, None);
    ms[1].other_document_write();
    pull(&mut ms, 2, 1, None);
    pull(&mut ms, 3, 2, None);
    let read = ms[3].visible();
    let p_reads_a_and_b = read.is_some_and(|d| {
        d.get(&p("a")) == Some(&Value::Int64(1)) && d.get(&p("b")) == Some(&Value::Int64(2))
    });
    let same_state = ms[3].canon() == ms[2].canon();
    let before = fix_key(&ms[3..]);
    pull(&mut ms, 3, 1, None);
    let follow_up_moved = fix_key(&ms[3..]) != before;
    OmittedOut {
        p_reads_a_and_b,
        same_state,
        snapshots: ms.iter().map(|n| n.snaps).sum(),
        follow_up_moved,
    }
}
