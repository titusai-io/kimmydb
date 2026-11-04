//! The probes that need the pull protocol: probes5.py `probe_V4M2_epoch_twice`
//! and targeted5.py (`restore_inc_scenario`, `snapshot_loss_scenario`). The
//! state-level probes live in `merge_state/tests.rs`.

use std::collections::BTreeMap;

use kimmy_core::hlc::Hlc;
use kimmy_core::merge_state::Value;

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
/// reads 55 and 6.
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
