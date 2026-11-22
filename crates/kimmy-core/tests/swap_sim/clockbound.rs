//! U1 and its guard, in the swap harness (the bug plan "a far-future document entry drags every member's clock", ruled 2026-11-22):
//! every replicated stamp is bounded before it is witnessed, and a coordinator whose own stamps will be refused does not bind.
//!
//! The harness had no clock bound (`coverage.rs`: "far-future clock checks ... NOT FROZEN"). `World::clock_bound` is it: a window
//! refuses an entry stamped past the receiver's clock (the world clock plus its skew) plus the bound, before anything is witnessed
//! or applied, whatever the kind; the entry applies once the receiver's clock has caught up. A member whose skew is far past the
//! bound is the bad clock. Clock units are model steps (the product's seconds). Scripted scenarios only: no grid sets the bound.

use crate::entry::*;
use crate::member::DocOp;
use crate::scenario::Sc;

/// The bound, in clock units, and the bad clock's skew (far past it).
const BOUND: i64 = 50;
const FAR: i64 = 1000;

fn far_cluster(mode: &str) -> Sc {
    let mut sc = Sc::new(mode, &[("A", 0), ("B", 0), ("X", FAR)], 20);
    sc.w.clock_bound = Some(BOUND);
    sc
}

/// X, the member whose clock is far ahead, mints one entry of every kind a member writes: a document write, a stage's create and
/// its staged row, a bind, an abandon, a logical drop and a recreate. Returns them in stamp order.
fn x_mints_every_kind(sc: &mut Sc) -> Vec<EntryRef> {
    let x = sc.idx("X");
    sc.advance(1);
    sc.ms[x].local_doc(&mut sc.w, 1, DocOp::Insert).expect("a write");
    sc.advance(2);
    let mut job = sc.ms[x].start_out(&mut sc.w, vec![0], JobKind::Good, 3);
    sc.advance(3);
    sc.ms[x].do_bind(&mut sc.w, &mut job, 3).expect("a bind");
    sc.advance(4);
    let mut job2 = sc.ms[x].start_out(&mut sc.w, vec![1], JobKind::CrashAbandon, 5);
    sc.advance(5);
    assert!(sc.ms[x].do_bind(&mut sc.w, &mut job2, 3).is_none(), "an abandon, not a bind");
    sc.advance(6);
    sc.ms[x].local_ldrop(&mut sc.w).expect("a logical drop");
    sc.advance(7);
    sc.ms[x].local_lcreate(&mut sc.w).expect("a recreate");
    let mut entries: Vec<EntryRef> = sc.ms[x].appended.iter().map(|&i| sc.w.ent_rc(i)).collect();
    entries.sort_by_key(|e| e.stamp);
    entries
}

/// Every kind from the bad clock is refused by an honest member, not witnessed, and applies once it is due. The problems found.
fn every_kind_is_bounded(mode: &str) -> Vec<String> {
    let mut sc = far_cluster(mode);
    let entries = x_mints_every_kind(&mut sc);
    let mut problems = Vec::new();
    for kind in [Kind::Doc, Kind::Create, Kind::Bind, Kind::Abandon, Kind::Ldrop, Kind::Lcreate] {
        if !entries.iter().any(|e| e.kind == kind) {
            problems.push(format!("premise: X minted no {kind:?}"));
        }
    }
    let (a, b) = (sc.idx("A"), sc.idx("B"));
    let hlc_before = [sc.ms[a].hlc, sc.ms[b].hlc];
    for e in &entries {
        for honest in ["A", "B"] {
            sc.window(honest, "X", e);
            let m = &sc.ms[sc.idx(honest)];
            if m.holds(e.idx) {
                problems.push(format!(
                    "{honest} took {e} from a clock {FAR} ahead (past the bound {BOUND})"
                ));
            }
        }
    }
    // The invariant: an honest member's clock never leads its own by more than the bound through replication.
    for (i, honest) in [a, b].into_iter().enumerate() {
        let hlc = sc.ms[honest].hlc;
        if hlc != hlc_before[i] || hlc > sc.w.clock + sc.ms[honest].skew + BOUND {
            problems.push(format!(
                "{} witnessed a refused stamp: hlc {} (was {})",
                sc.ms[honest].name, hlc, hlc_before[i]
            ));
        }
    }
    if sc.w.stats.get("clock_refused") == 0 {
        problems.push("nothing was counted as clock refused".into());
    }
    // Due: the honest members' clocks catch up to within the bound of the newest stamp, and the window applies.
    let newest = entries.iter().map(|e| e.hlc).max().unwrap();
    sc.advance(newest - BOUND);
    for honest in ["A", "B"] {
        sc.pull(honest, "X");
        let m = &sc.ms[sc.idx(honest)];
        if entries.iter().any(|e| !m.holds(e.idx)) {
            problems.push(format!("{honest} did not take every entry once it was due"));
        }
    }
    problems
}

/// A coordinator whose clock is far ahead stages a replacing `$out`; an honest peer refuses its stage create; the coordinator's guard
/// (b) keeps it from binding. Returns the binds X minted and the binds the guard refused.
fn coordinator_ahead(mode: &str) -> (usize, i64) {
    let mut sc = far_cluster(mode);
    let x = sc.idx("X");
    sc.advance(1);
    let mut job = sc.ms[x].start_out(&mut sc.w, vec![0], JobKind::Good, 3);
    sc.advance(2);
    sc.pull("A", "X"); // A refuses the stage's create: X reads it in the reply
    sc.advance(3);
    sc.guarded_bind("X", &mut job, 3);
    let binds = sc.w.all.iter().filter(|e| e.kind == Kind::Bind && e.origin == x).count();
    (binds, sc.w.stats.get("bind_refused_by_guard"))
}

/// With the guard off, the bad clock's bind goes out and the honest members, which have not taken it (it is refused until due),
/// acknowledge writes to the old physical meanwhile. When the bind is due they flip, and those writes, stamped below the bind, are
/// buried with it, with no record. Returns (the writes acknowledged at A, how many are still in A's served state at the end).
fn acknowledged_writes_after_a_far_bind(mode: &str) -> (usize, usize) {
    let mut sc = far_cluster(mode);
    let (x, a) = (sc.idx("X"), sc.idx("A"));
    sc.advance(1);
    let mut job = sc.ms[x].start_out(&mut sc.w, vec![0], JobKind::Good, 3);
    sc.advance(2);
    sc.pull("A", "X"); // refused until due: X learns it
    sc.advance(3);
    sc.guarded_bind("X", &mut job, 3); // the guard refuses it; with the guard off it is minted
    let keys = [5u32, 6];
    let mut acked = 0;
    for (i, key) in keys.into_iter().enumerate() {
        sc.advance(4 + i as i64);
        if sc.ms[a].local_doc(&mut sc.w, key, DocOp::Insert).is_some() {
            acked += 1;
        }
    }
    // the bind is due: A's clock reaches within the bound of it
    let newest = sc.ms[x].appended.iter().map(|&i| sc.w.ent(i).hlc).max().unwrap();
    sc.advance(newest - BOUND);
    sc.pull("A", "X");
    sc.pull("A", "X");
    let m = &sc.ms[a];
    let served = m.serving().and_then(|s| m.phys.get(&s));
    let present = keys
        .into_iter()
        .filter(|k| served.is_some_and(|p| p.docs.get(k).is_some_and(|v| v.body.is_some())))
        .count();
    (acked, present)
}

/// A member alone in its cluster has no peer to refuse it and no clock lead of its own: the guard has no signal, and its bind goes
/// out. The residual, with its exact count: one bind per attempt, none refused.
fn lone_member() -> (usize, i64) {
    let mut sc = Sc::new("v6", &[("X", FAR)], 20);
    sc.w.clock_bound = Some(BOUND);
    sc.advance(1);
    let mut job = sc.ms[0].start_out(&mut sc.w, vec![0], JobKind::Good, 3);
    sc.advance(2);
    sc.guarded_bind("X", &mut job, 1);
    let binds = sc.w.all.iter().filter(|e| e.kind == Kind::Bind).count();
    (binds, sc.w.stats.get("bind_refused_by_guard"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_stamped_past_the_bound_is_refused_not_witnessed_and_applies_once_due() {
        let problems = every_kind_is_bounded("v6");
        assert!(problems.is_empty(), "{problems:#?}");
    }

    #[test]
    fn the_mutant_without_the_clock_bound_is_killed() {
        let problems = every_kind_is_bounded("m-no-clock-bound");
        assert!(
            problems.iter().any(|p| p.contains("from a clock"))
                && problems.iter().any(|p| p.contains("witnessed a refused stamp")),
            "the unbounded window takes the entry and witnesses its stamp: {problems:#?}"
        );
    }

    #[test]
    fn a_coordinator_whose_stage_create_was_refused_mints_no_bind() {
        assert_eq!(coordinator_ahead("v6"), (0, 1), "no bind, one bind refused by the guard");
    }

    #[test]
    fn the_mutant_without_the_guard_binds_although_a_peer_refused_it() {
        assert_eq!(
            coordinator_ahead("m-no-own-guard"),
            (1, 0),
            "the guard off: the bind is minted"
        );
    }

    #[test]
    fn with_the_guard_off_acknowledged_writes_are_buried_by_the_far_bind_and_with_it_on_none_are() {
        let (acked, present) = acknowledged_writes_after_a_far_bind("v6");
        assert_eq!(
            (acked, present),
            (2, 2),
            "the guard on: nothing was bound, every acknowledged write stays"
        );
        let (acked, present) = acknowledged_writes_after_a_far_bind("m-no-own-guard");
        assert_eq!(acked, 2);
        assert_eq!(
            acked - present,
            2,
            "the guard off: both acknowledged writes were buried as pre-flip, with no record"
        );
    }

    #[test]
    fn a_lone_member_far_ahead_binds_once_the_residual_with_its_exact_count() {
        assert_eq!(
            lone_member(),
            (1, 0),
            "no peer and no lead of its own: the guard has no signal, the bind is minted"
        );
    }
}
