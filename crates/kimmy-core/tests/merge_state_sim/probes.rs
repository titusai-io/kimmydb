//! The probes that need the pull protocol: probes5.py `probe_V4M2_epoch_twice`
//! and targeted5.py (`restore_inc_scenario`, `snapshot_loss_scenario`). The
//! state-level probes live in `merge_state/tests.rs`.

use std::collections::BTreeMap;

use kimmy_core::hlc::Hlc;
use kimmy_core::merge_state::Value;

use crate::grade::grade;
use crate::oracle::Log;
use crate::sim::{Cfg, Node, fix_key, fixpoint, pull};

fn x_of(n: &Node) -> Value {
    n.visible().and_then(|d| d.get("x").cloned()).expect("x is visible")
}

/// Restoring the same backup twice. The epoch from the restore's start HLC plus
/// 64 random bits: 125 on every member. The mutant (a counter kept in the
/// store, which the backup carries): 105 with no record. Returns the value of
/// `x` on every member.
pub fn v4m2_epoch_twice(cfg: &Cfg) -> BTreeMap<char, Value> {
    let mut log = Log::new();
    let mut ms: Vec<Node> = "ABC".chars().map(|c| Node::new(c, cfg)).collect();
    ms[0].insert(&[("x", Value::Int(100))], &mut log, 0);
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
    d.values().all(|v| *v == Value::Int(125)) && m.values().all(|v| *v == Value::Int(105))
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
    ms[0].insert(&[("x", Value::Int(100))], &mut log, 0);
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
    ms[0].insert(&[("x", Value::Int(0))], &mut log, 0);
    for m in 1..6 {
        pull(&mut ms, m, 0, None);
    }
    ms[2].set("x", Value::Str("b".into()), &mut log, 0);
    pull(&mut ms, 3, 2, None);
    ms[3].set("x", Value::Str("c".into()), &mut log, 0);
    ms[1].w.clock = Hlc::new(5, 0);
    ms[1].set("x", Value::Str("a".into()), &mut log, 0);
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
