//! Scripted scenarios of the snapshot routes (ports of `probes4.py`, `probes5.py`, `probes6.py`, `probes6a.py`, and the
//! design-level sender-dies-mid-join scenario). Each runs on the v6 base and on the mutant it is aimed at.

use crate::entry::*;
use crate::member::DocOp;
use crate::oracle::{Res, finish};
use crate::scenario::Sc;

/// The checks that must be 0 on the base.
const MUST_BE_ZERO: [&str; 12] = [
    "diverged",
    "oracle_state",
    "loss_mismatch",
    "loss_missing",
    "loss_extra",
    "invariant",
    "head_not_exactly_one_record_and_event",
    "div_replay",
    "div_replay_interleaved",
    "loss_body_mismatch",
    "div_state",
    "div_loss",
];

fn nonzero(res: &Res) -> Vec<(String, i64)> {
    res.iter()
        .filter(|(k, v)| **v != 0 && (MUST_BE_ZERO.contains(&k.as_str()) || k.starts_with("inv_")))
        .map(|(k, v)| (k.clone(), *v))
        .collect()
}

fn has_record(sc: &Sc, member: &str, key: u32, stamp: Stamp) -> bool {
    let m = &sc.ms[sc.idx(member)];
    m.loss.keys().any(|k| k.key == key && k.stamp == Some(stamp))
}

/// `probes6.py` `seed885_shape`: a late write held only by C; D binds (burying L) and takes C's page of L as a snapshot,
/// the grant names the entry; C vanishes; A and B take snapshots from D. With the seed-885 rule the page versions of the
/// buried physical are retained and evaluated by D11 (at once and on A's later bind): the head is reported on A, B and D.
/// Returns (the late write's stamp, per-member record presence, the run's problems).
fn seed885(mode: &str) -> (Stamp, Vec<bool>, Vec<(String, i64)>) {
    let mut sc = Sc::new(mode, &[("A", 0), ("B", 0), ("C", 0), ("D", 0)], 60);
    sc.advance(1);
    let seed = sc.ms[0].local_doc(&mut sc.w, 6, DocOp::Insert).unwrap();
    for m in ["B", "C", "D"] {
        sc.window(m, "A", &seed);
    }
    sc.advance(10);
    let mut job_a = sc.ms[0].start_out(&mut sc.w, vec![0], JobKind::Good, 12);
    sc.advance(12);
    sc.ms[0].do_bind(&mut sc.w, &mut job_a, 4); // A's bind: C and D do not know it
    sc.advance(20);
    let late = sc.ms[2].local_doc(&mut sc.w, 6, DocOp::Update).unwrap(); // late by A's bind; only C holds the entry
    sc.advance(30);
    let mut job_d = sc.ms[3].start_out(&mut sc.w, vec![1], JobKind::Good, 31);
    sc.advance(31);
    sc.ms[3].do_bind(&mut sc.w, &mut job_d, 4); // D binds (buries L), still without A's bind
    sc.advance(32);
    sc.snapshot("D", "C", false); // D takes C's state
    sc.advance(40);
    sc.vanish(&["C"]);
    sc.advance(41);
    sc.snapshot("A", "D", false);
    sc.advance(42);
    sc.snapshot("B", "D", false);
    for _ in 0..3 {
        sc.pull("D", "A");
        sc.pull("B", "A");
        sc.pull("A", "B");
    }
    sc.quiesce();
    let recs: Vec<bool> =
        ["A", "B", "D"].iter().map(|m| has_record(&sc, m, 6, late.stamp)).collect();
    let res = finish(&mut sc.w, &mut sc.ms);
    (late.stamp, recs, nonzero(&res))
}

/// Seed 885's shape under `field`, with two late writes by C on two paths (an even clock sets `v`, an odd clock increments `n`):
/// D takes C's state, so the older write's cell (`v`) is held only inside the page state whose version is the newer write's
/// (`n`). Returns, per surviving member, whether it holds the records of both late writes, and the failing checks.
type TwoPathRecords = Vec<(bool, bool)>;

fn seed885_two_paths(mode: &str) -> (TwoPathRecords, Vec<(String, i64)>) {
    let mut sc = Sc::new(mode, &[("A", 0), ("B", 0), ("C", 0), ("D", 0)], 60);
    sc.advance(1);
    let seed = sc.ms[0].local_doc(&mut sc.w, 6, DocOp::Insert).unwrap();
    for m in ["B", "C", "D"] {
        sc.window(m, "A", &seed);
    }
    sc.advance(10);
    let mut job_a = sc.ms[0].start_out(&mut sc.w, vec![0], JobKind::Good, 12);
    sc.advance(12);
    sc.ms[0].do_bind(&mut sc.w, &mut job_a, 4);
    sc.advance(20);
    let late_v = sc.ms[2].local_doc(&mut sc.w, 6, DocOp::Update).unwrap(); // clock 20: a set of v
    sc.advance(21);
    let late_n = sc.ms[2].local_doc(&mut sc.w, 6, DocOp::Update).unwrap(); // clock 21: an increment of n
    sc.advance(30);
    let mut job_d = sc.ms[3].start_out(&mut sc.w, vec![1], JobKind::Good, 31);
    sc.advance(31);
    sc.ms[3].do_bind(&mut sc.w, &mut job_d, 4);
    sc.advance(32);
    sc.snapshot("D", "C", false);
    sc.advance(40);
    sc.vanish(&["C"]);
    sc.advance(41);
    sc.snapshot("A", "D", false);
    sc.advance(42);
    sc.snapshot("B", "D", false);
    for _ in 0..3 {
        sc.pull("D", "A");
        sc.pull("B", "A");
        sc.pull("A", "B");
    }
    sc.quiesce();
    let recs: Vec<(bool, bool)> = ["A", "B", "D"]
        .iter()
        .map(|m| (has_record(&sc, m, 6, late_v.stamp), has_record(&sc, m, 6, late_n.stamp)))
        .collect();
    let res = finish(&mut sc.w, &mut sc.ms);
    (recs, nonzero(&res))
}

/// `probes6a.py` `snapshot-record-sole-carrier`: B holds C's late write as history and mints the record; D takes B's
/// window and only witnesses the write (held as granted state); B and C vanish; E joins by a snapshot from D, whose
/// grant names both the write and the record entry. The snapshot's loss-record set is the sole carrier of the record.
fn sole_carrier(mode: &str) -> (bool, Vec<(String, i64)>) {
    let mut sc = Sc::new(mode, &[("A", 0), ("B", 0), ("C", 0), ("D", 0), ("E", 0)], 60);
    sc.advance(1);
    let seed = sc.ms[0].local_doc(&mut sc.w, 6, DocOp::Insert).unwrap();
    for m in ["B", "C", "D"] {
        sc.window(m, "A", &seed);
    }
    sc.advance(10);
    let mut job_a = sc.ms[0].start_out(&mut sc.w, vec![0], JobKind::Good, 12);
    sc.advance(12);
    sc.ms[0].do_bind(&mut sc.w, &mut job_a, 5);
    sc.advance(14);
    sc.pull("B", "A");
    sc.pull("D", "A"); // B's and D's L are buried by A's bind; C has not heard of it
    sc.advance(20);
    let late = sc.ms[2].local_doc(&mut sc.w, 6, DocOp::Update).unwrap(); // the late write; only C holds the entry
    sc.advance(22);
    sc.window("B", "C", &late); // history at B: B mints the record
    sc.advance(25);
    sc.ms[2].start_out(&mut sc.w, vec![1], JobKind::Good, 99); // C's later entries: a create and its staged rows
    sc.advance(30);
    sc.pull("B", "C");
    sc.advance(31);
    sc.pull("D", "B"); // D gets B's record; the window skips C's history write: D only witnesses it
    sc.advance(40);
    sc.vanish(&["B", "C"]);
    sc.advance(41);
    sc.snapshot("E", "D", false); // E joins by a snapshot from D
    sc.quiesce();
    let e_has = has_record(&sc, "E", 6, late.stamp);
    let res = finish(&mut sc.w, &mut sc.ms);
    (e_has, nonzero(&res))
}

/// `probes4.py` `probe_gap2`: O's late delete is held by S; N joins by a snapshot from S and holds the delete only as
/// state; O and S vanish; a logical drop reaches N first, then the binding. The head (a delete) must have exactly one
/// record and one event on W, Y and N.
fn late_delete_only_as_state(mode: &str) -> (Vec<bool>, Vec<(String, i64)>) {
    let mut sc = Sc::new(mode, &[("O", 50), ("S", 0), ("W", 0), ("Y", 60), ("N", 0)], 60);
    sc.advance(1);
    let seed = sc.ms[2].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
    for m in ["O", "S", "Y", "N"] {
        sc.window(m, "W", &seed);
    }
    sc.advance(10);
    let mut jw = sc.ms[2].start_out(&mut sc.w, vec![1], JobKind::Good, 0);
    sc.advance(12);
    sc.ms[2].do_bind(&mut sc.w, &mut jw, 5); // only W holds the binding
    sc.advance(14);
    let d = sc.ms[0].local_doc(&mut sc.w, 0, DocOp::Delete).unwrap(); // a late DELETE by O, which has not heard of the binding
    sc.window("S", "O", &d);
    sc.snapshot("N", "S", false); // N holds the delete as state (an entry with its _id)
    for dead in ["O", "S"] {
        let i = sc.idx(dead);
        let mut jd = sc.ms[i].start_out(&mut sc.w, vec![2], JobKind::Vanish, 0);
        sc.ms[i].do_bind(&mut sc.w, &mut jd, 3);
    }
    sc.advance(20);
    let g = sc.ms[3].local_ldrop(&mut sc.w).unwrap(); // a logical drop stamped after the delete
    sc.window("N", "Y", &g); // N buries L because of the drop; the earliest superseding event is still unknown to it
    sc.pull("N", "W"); // the binding arrives
    sc.quiesce();
    let recs: Vec<bool> = ["W", "Y", "N"]
        .iter()
        .map(|m| {
            let mm = &sc.ms[sc.idx(m)];
            let key = (0u32, None, Some(d.stamp));
            mm.loss.keys().filter(|k| k.head() == key).count() == 1
                && mm.events_emitted.get(&key).copied() == Some(1)
        })
        .collect();
    let res = finish(&mut sc.w, &mut sc.ms);
    (recs, nonzero(&res))
}

/// `probes4.py` `probe_gap1`: C is mid-join and holds the page record `k@(30,S)` out of position; W's staged `k@(11,W)`
/// arrives in position. The staged entry is appended in position and servable, so A, served by C's window, flips WITH k.
/// Returns (the staged entry is appended at C, A flipped without k).
fn append_in_position(mode: &str) -> (bool, bool, bool) {
    let mut sc = Sc::new(mode, &[("W", 0), ("W2", 0), ("S", 0), ("C", 0), ("A", 0)], 60);
    sc.advance(1);
    let seed = sc.ms[0].local_doc(&mut sc.w, 5, DocOp::Insert).unwrap();
    for m in ["W2", "S", "C", "A"] {
        sc.pull(m, "W");
    }
    sc.advance(10);
    let mut jw = sc.ms[0].start_out(&mut sc.w, vec![0, 1], JobKind::Good, 0);
    sc.advance(12);
    sc.ms[0].do_bind(&mut sc.w, &mut jw, 5);
    sc.advance(13);
    let _jw2 = sc.ms[1].start_out(&mut sc.w, vec![2], JobKind::Good, 0);
    sc.pull("S", "W");
    sc.pull("S", "W2");
    sc.advance(30);
    let kw = sc.ms[2].local_doc(&mut sc.w, 0, DocOp::Update).unwrap(); // a post-flip write into the output
    assert!(kw.stamp > jw.digest[0].1);
    sc.snapshot("C", "S", true);
    // the page of W's stage lands first (pages pop from the end)
    let stage = jw.stage.clone();
    {
        let j = sc.ms[3].join.as_mut().unwrap();
        j.pages.retain(|n| *n != stage);
        j.pages.push(stage.clone());
    }
    sc.land("C");
    sc.pull_origins("C", "W", &["W"]);
    let ek =
        sc.w.all
            .iter()
            .find(|e| e.kind == Kind::Doc && e.staging && e.phys == stage && e.key == 0)
            .unwrap()
            .clone();
    let appended = sc.ms[3].appended.contains(&ek.idx);
    sc.window("A", "W", &seed);
    // windows served by the member that is mid-join (a join in progress is not protected)
    let mut pending: Vec<EntryRef> = sc
        .w
        .all
        .iter()
        .filter(|e| e.origin == 0 && !sc.ms[4].holds(e.idx) && sc.ms[3].appended.contains(&e.idx))
        .cloned()
        .collect();
    pending.sort_by_key(|e| e.stamp);
    for e in pending {
        sc.window("A", "C", &e);
    }
    let partial = sc.ms[4].inv.contains("partial_flip");
    let serving = sc.ms[4].serving().unwrap_or_default();
    (appended, partial, serving == stage)
}

/// Review F3 (the shape 6e found): a write by ANOTHER origin, stamped above the stage's valid binding, addressed to the stage physical,
/// arriving in position at a member that knows no binding and is superseded on arrival by a newer version held out of position.
/// S-bind (no origin clause) appends it; S-bind-origin must not. Returns (the write is appended at X, X holds the newer version).
fn other_origin_write_above_the_binding(mode: &str) -> (bool, bool) {
    let mut sc = Sc::new(mode, &[("W", 0), ("S", 0), ("X", 0)], 60);
    sc.advance(1);
    let seed = sc.ms[0].local_doc(&mut sc.w, 5, DocOp::Insert).unwrap();
    for m in ["S", "X"] {
        sc.pull(m, "W");
    }
    sc.advance(10);
    let mut jw = sc.ms[0].start_out(&mut sc.w, vec![0], JobKind::Good, 0);
    let stage = jw.stage.clone();
    // X learns the stage's creation (and nothing of the binding): W's entries up to the creation only
    let creates: Vec<EntryRef> =
        sc.w.all.iter().filter(|e| e.kind == Kind::Create && e.phys == stage).cloned().collect();
    sc.advance(12);
    sc.ms[0].do_bind(&mut sc.w, &mut jw, 5);
    sc.advance(13);
    sc.pull("S", "W"); // S knows the binding and serves the stage
    sc.advance(30);
    let k1 = sc.ms[1].local_doc(&mut sc.w, 0, DocOp::Update).unwrap(); // a post-flip write into the output, above the binding
    sc.advance(31);
    let k2 = sc.ms[1].local_doc(&mut sc.w, 0, DocOp::Update).unwrap(); // a newer version of the same key
    assert!(k1.stamp > jw.digest[0].1 && k2.stamp > k1.stamp);
    for c in &creates {
        sc.take("X", c); // X knows the stage's creation, no binding
    }
    assert!(sc.ms[2].phys.contains_key(&stage));
    sc.take("X", &k2); // the newer version, held out of position
    sc.window("X", "S", &k1); // the older write arrives in position: superseded on arrival
    let _ = seed;
    (sc.ms[2].appended.contains(&k1.idx), sc.ms[2].holds(k2.idx))
}

/// Item R / build item (a), seed 1251's shape: A holds B's bind as information (route 3, a parked bind); B's rows arrive in position,
/// so the bind is RELEASED (covered, served); a snapshot R takes from A right then. With `m-release-without-effect` the bind takes
/// effect at A only at a later settle, so the snapshot carries the coverage without the event, and R covers the writer past the bind
/// and never learns it. Returns the run's problems.
fn release_effect_twin(mode: &str) -> Vec<(String, i64)> {
    let mut sc = Sc::new(mode, &[("A", 0), ("B", 0), ("C", 0), ("R", 0)], 60);
    sc.w.reg_in_position_check = false; // the bind is delivered out of position on purpose (rule B's shape)
    sc.advance(1);
    let seed = sc.ms[0].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
    for m in ["B", "C"] {
        sc.window(m, "A", &seed);
    }
    sc.advance(10);
    let mut job = sc.ms[1].start_out(&mut sc.w, vec![1, 2], JobKind::Good, 0);
    sc.advance(12);
    let bind = sc.ms[1].do_bind(&mut sc.w, &mut job, 3).unwrap();
    let create =
        sc.w.all.iter().find(|e| e.kind == Kind::Create && e.phys == job.stage).unwrap().clone();
    sc.window("A", "B", &create);
    let a = sc.idx("A");
    sc.ms[a].take_hold(&mut sc.w, &bind); // held out of position (OPLOG_HELD mark), before the rows: a parked bind
    sc.advance(14);
    sc.pull("A", "B"); // the rows arrive in position: the bind is released
    assert!(sc.snapshot("R", "A", false), "R joins from A between release and effect");
    step(&mut sc);
    sc.advance(16);
    sc.quiesce();
    step(&mut sc);
    let res = finish(&mut sc.w, &mut sc.ms);
    nonzero(&res)
}

/// A join resumed only from its own sending peer: N is mid-join from X, a begin from Y is refused with no side effect,
/// the register read when the last page lands is X's. Returns (the refused begin changed nothing, N serves the output,
/// the run's problems).
fn join_from_another_peer(mode: &str) -> (bool, bool, Vec<(String, i64)>) {
    let mut sc = Sc::new(mode, &[("W", 0), ("X", 0), ("Y", 0), ("N", 0)], 60);
    sc.advance(1);
    let seed = sc.ms[0].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
    for m in ["X", "Y"] {
        sc.window(m, "W", &seed);
    }
    sc.advance(10);
    let mut job = sc.ms[0].start_out(&mut sc.w, vec![1], JobKind::Good, 0);
    sc.advance(12);
    sc.ms[0].do_bind(&mut sc.w, &mut job, 4);
    sc.pull("X", "W"); // X holds the whole stage and the binding; Y holds the seed only
    assert!(sc.snapshot("N", "X", true));
    let n = sc.idx("N");
    let (pages, held) = {
        let j = sc.ms[n].join.as_ref().unwrap();
        (j.pages.clone(), sc.ms[n].cv.count())
    };
    let refused = !sc.snapshot("N", "Y", true);
    let unchanged = {
        let j = sc.ms[n].join.as_ref().unwrap();
        j.src == sc.idx("X") && j.pages == pages && sc.ms[n].cv.count() == held && !j.granted
    };
    while sc.land("N") {
        if sc.ms[n].join.is_none() {
            break;
        }
    }
    let serves = sc.ms[n].serving().as_deref() == Some(job.stage.as_str());
    sc.quiesce();
    let res = finish(&mut sc.w, &mut sc.ms);
    (refused && unchanged, serves, nonzero(&res))
}

/// Rule B: a bind held out of position (route 3) is information at A: no burial of the stage by A's own later logical
/// drop, so the stage's rows arrive in position and are appended and C flips whole. Returns (parked at A, A refused a
/// write while holding it, the bind took effect later, C partially flipped, the run's problems).
fn rule_b_shape(mode: &str) -> (bool, bool, bool, bool, Vec<(String, i64)>) {
    let mut sc = Sc::new(mode, &[("A", 0), ("B", 0), ("C", 0)], 60);
    sc.w.reg_in_position_check = false; // the scenario delivers a bind out of position on purpose (rule B's shape)
    sc.advance(1);
    let seed = sc.ms[0].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
    for m in ["B", "C"] {
        sc.window(m, "A", &seed);
    }
    sc.advance(10);
    let mut job = sc.ms[1].start_out(&mut sc.w, vec![1, 2], JobKind::Good, 0);
    sc.advance(12);
    let bind = sc.ms[1].do_bind(&mut sc.w, &mut job, 3).unwrap();
    let create =
        sc.w.all.iter().find(|e| e.kind == Kind::Create && e.phys == job.stage).unwrap().clone();
    sc.window("A", "B", &create);
    sc.take("A", &bind); // route 3: held out of position, before the rows
    let parked = sc.ms[0].info.contains_key(&bind.idx);
    sc.advance(14);
    let refused = sc.ms[0].local_doc(&mut sc.w, 0, DocOp::Update).is_none();
    sc.advance(30);
    sc.ms[0].local_ldrop(&mut sc.w).unwrap();
    sc.pull("A", "B"); // the rows arrive
    let took_effect = sc.ms[0].events.contains(&bind.idx) && !sc.ms[0].info.contains_key(&bind.idx);
    sc.pull("C", "A");
    let partial = sc.ms[2].inv.contains("partial_flip");
    sc.quiesce();
    let res = finish(&mut sc.w, &mut sc.ms);
    (parked, refused, took_effect, partial, nonzero(&res))
}

/// ADR-272 decision 7, the build requirement: N is mid-join from S; the stage is a placeholder page of that join; the
/// binding is covered at N (windows from P) but cannot be made visible, so N refuses writes. S vanishes. A fresh pull
/// from P supersedes the unfinished join: the placeholder is re-filled and completed by the new grant, the binding
/// becomes visible, the refusal is released, no placeholder or held record is left.
struct Supersede {
    refused_before: bool,
    join_open_before: bool,
    placeholder_before: bool,
    refused_still_after_sender_dies: bool,
    superseded: bool,
    join_after: bool,
    placeholders_after: usize,
    held_records_after: usize,
    serving_stage_after: bool,
    write_accepted_after: bool,
    problems: Vec<(String, i64)>,
}

fn sender_dies_mid_join(mode: &str) -> Supersede {
    let mut sc = Sc::new(mode, &[("W", 0), ("W2", 0), ("S", 0), ("P", 0), ("N", 0)], 60);
    let n = sc.idx("N");
    sc.advance(1);
    let seed = sc.ms[0].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
    for m in ["W2", "S", "P"] {
        sc.window(m, "W", &seed);
    }
    sc.advance(10);
    let mut job = sc.ms[0].start_out(&mut sc.w, vec![1, 2], JobKind::Good, 0);
    sc.advance(12);
    sc.ms[0].do_bind(&mut sc.w, &mut job, 5);
    sc.advance(13);
    let job2 = sc.ms[1].start_out(&mut sc.w, vec![3], JobKind::Good, 0); // an in-flight stage nobody has bound
    sc.pull("S", "W");
    sc.pull("S", "W2"); // S holds the output and the in-flight stage (two alive pages)
    sc.pull("P", "W"); // P holds the output only
    assert!(sc.snapshot("N", "S", true));
    // the output's page lands first (pages pop from the end), the in-flight stage's page stays
    {
        let stage = job.stage.clone();
        let j = sc.ms[n].join.as_mut().unwrap();
        j.pages.retain(|p| *p != stage);
        j.pages.push(stage);
        assert_eq!(j.pages.len(), 2, "two alive pages: {:?} and {}", job.stage, job2.stage);
    }
    sc.land("N");
    let placeholder_before = sc.ms[n].phys.get(&job.stage).is_some_and(|p| p.missing);
    sc.pull("N", "P"); // windows: the stage's entries and the binding; N covers W at the binding
    sc.advance(20);
    let refused_before = sc.ms[n].local_doc(&mut sc.w, 0, DocOp::Update).is_none();
    let join_open_before = sc.ms[n].join.is_some();
    sc.vanish(&["S"]);
    sc.advance(21);
    let refused_still = sc.ms[n].local_doc(&mut sc.w, 0, DocOp::Update).is_none();
    let p = sc.idx("P");
    let superseded = {
        let (d, s) = crate::relay::pair(&mut sc.ms, n, p);
        d.supersede_join(&mut sc.w, s, false)
    };
    sc.advance(22);
    let write_accepted_after = sc.ms[n].local_doc(&mut sc.w, 1, DocOp::Update).is_some();
    let join_after = sc.ms[n].join.is_some();
    let placeholders_after = sc.ms[n].phys.values().filter(|p| p.missing).count();
    let held_records_after = sc.ms[n].held_records.len();
    let serving_stage_after = sc.ms[n].serving().as_deref() == Some(job.stage.as_str());
    sc.quiesce();
    let res = finish(&mut sc.w, &mut sc.ms);
    Supersede {
        refused_before,
        join_open_before,
        placeholder_before,
        refused_still_after_sender_dies: refused_still,
        superseded,
        join_after,
        placeholders_after,
        held_records_after,
        serving_stage_after,
        write_accepted_after,
        problems: nonzero(&res),
    }
}

/// The step check after an action, on every member (the scripted twins check as the random run does, after each step).
fn step(sc: &mut Sc) {
    for i in 0..sc.ms.len() {
        sc.ms[i].check_inv(&sc.w);
    }
}

fn loss_entries(sc: &Sc, origin: &str, key: u32, stamp: Stamp) -> Vec<EntryRef> {
    let o = sc.idx(origin);
    sc.w.all
        .iter()
        .filter(|e| {
            e.kind == Kind::Loss
                && e.origin == o
                && e.lkey.is_some_and(|k| k.key == key && k.stamp == Some(stamp))
        })
        .cloned()
        .collect()
}

/// What a twin reports: the record for the lost write at E (the member that pulls only live members), whether the step check
/// fired on any member, and the run's problems.
struct Twin {
    step_check: bool,
    e_has_record: bool,
    problems: Vec<(String, i64)>,
}

/// `probe1434b.py` (the seed-1434 shape): a LOSS entry handed out of position by the hold route (C has neither x nor y),
/// then x by a window; B vanishes with y and r_y, so r_x is never in position anywhere. Rule B for records: r_x is
/// information at C, C mints its own record when it finds x late, and E gets that by a window.
fn seed_1434_twin(mode: &str) -> Twin {
    let mut sc = Sc::new(mode, &[("A", 0), ("B", 0), ("C", 0), ("E", 0)], 60);
    sc.w.reg_in_position_check = false; // the hold route hands a loss entry out of position on purpose (the seed-1434 shape)
    sc.advance(1);
    let s6 = sc.ms[0].local_doc(&mut sc.w, 6, DocOp::Insert).unwrap();
    let s7 = sc.ms[0].local_doc(&mut sc.w, 7, DocOp::Insert).unwrap();
    for m in ["B", "C", "E"] {
        sc.window(m, "A", &s6);
        sc.window(m, "A", &s7);
    }
    sc.advance(10);
    let mut job = sc.ms[0].start_out(&mut sc.w, vec![0], JobKind::Good, 0);
    sc.advance(12);
    sc.ms[0].do_bind(&mut sc.w, &mut job, 4);
    sc.advance(14);
    sc.pull("C", "A");
    sc.pull("E", "A");
    sc.advance(20);
    let x = sc.ms[1].local_doc(&mut sc.w, 6, DocOp::Update).unwrap();
    sc.advance(22);
    sc.ms[1].local_doc(&mut sc.w, 7, DocOp::Update).unwrap();
    sc.advance(24);
    sc.pull("B", "A"); // B learns the bind: L buried at B, B mints r_x and r_y
    let rx = loss_entries(&sc, "B", x.key, x.stamp);
    assert!(!rx.is_empty(), "B did not mint r_x");
    step(&mut sc);
    sc.advance(26);
    let c = sc.idx("C");
    sc.ms[c].take_hold(&mut sc.w, &rx[0]); // the hold route: r_x out of position at C
    step(&mut sc);
    sc.advance(28);
    sc.window("C", "B", &x); // x by a window, in position at C: history, retained, late
    step(&mut sc);
    sc.advance(30);
    sc.vanish(&["B"]);
    sc.advance(32);
    sc.pull("E", "C");
    sc.w.step += 1;
    step(&mut sc);
    sc.quiesce();
    step(&mut sc);
    let res = finish(&mut sc.w, &mut sc.ms);
    let e = sc.idx("E");
    Twin {
        step_check: sc.ms.iter().any(|m| m.step_inv.contains("unbacked_state_record")),
        e_has_record: sc.ms[e].loss.keys().any(|k| k.key == x.key && k.stamp == Some(x.stamp)),
        problems: nonzero(&res),
    }
}

/// `probes7.py` (companion clause (3), the paged join): R begins a LAZY snapshot join from C; the first page grants neither x
/// nor r_x; C then finds x late and mints r_C above the grant; the last page lands at R. Without the clause R reads C's
/// records at the last page and holds a record for x with no entry of it: R does not mint, and when B and C vanish nobody
/// serves the record.
fn paged_join_twin(mode: &str) -> Twin {
    let mut sc = Sc::new(mode, &[("A", 0), ("B", 0), ("C", 0), ("R", 0), ("E", 0)], 60);
    sc.advance(1);
    let s6 = sc.ms[0].local_doc(&mut sc.w, 6, DocOp::Insert).unwrap();
    for m in ["B", "C", "E"] {
        sc.window(m, "A", &s6);
    }
    sc.advance(10);
    let mut job = sc.ms[0].start_out(&mut sc.w, vec![0], JobKind::Good, 0);
    sc.advance(12);
    sc.ms[0].do_bind(&mut sc.w, &mut job, 5);
    sc.advance(14);
    sc.pull("C", "A");
    sc.pull("E", "A");
    sc.advance(20);
    let x = sc.ms[1].local_doc(&mut sc.w, 6, DocOp::Update).unwrap();
    sc.advance(24);
    sc.pull("B", "A"); // B learns the bind and mints r_x
    sc.advance(26);
    assert!(sc.snapshot("R", "C", true), "R begins a paged join");
    let r = sc.idx("R");
    assert!(
        sc.ms[r].join.as_ref().is_some_and(|j| !j.pages.is_empty()),
        "the join is paged: pages remain after the grant"
    );
    step(&mut sc);
    sc.advance(28);
    sc.window("C", "B", &x); // history at C: C mints r_C, above R's grant
    assert!(!loss_entries(&sc, "C", x.key, x.stamp).is_empty(), "C did not mint r_C");
    step(&mut sc);
    sc.advance(30);
    while sc.land("R") {
        if sc.ms[r].join.is_none() {
            break;
        }
    }
    step(&mut sc);
    sc.advance(32);
    sc.window("R", "B", &x); // x in position at R: history, found late
    step(&mut sc);
    sc.advance(34);
    sc.vanish(&["B", "C"]);
    sc.advance(36);
    sc.pull("E", "R");
    sc.pull("E", "A");
    sc.w.step += 1;
    sc.quiesce();
    step(&mut sc);
    let res = finish(&mut sc.w, &mut sc.ms);
    let e = sc.idx("E");
    Twin {
        step_check: sc.ms.iter().any(|m| m.step_inv.contains("unbacked_state_record")),
        e_has_record: sc.ms[e].loss.keys().any(|k| k.key == x.key && k.stamp == Some(x.stamp)),
        problems: nonzero(&res),
    }
}

/// M3, clause (3) NOT granted by construction (within the grant): C finds x late and mints r_C BEFORE R begins its join from C, so
/// the record is within the grant; B and C vanish; the third member E pulls ONLY by window from R (never from A, whose bind it
/// knows). E has the record only if R made the carried record a served entry (build item clause (3), ASSUMED). The mutant
/// `m-snap-record-not-served` carries the record as state alone: the step check names it unbacked and E never gets it.
fn within_grant_join_twin(mode: &str) -> Twin {
    let mut sc = Sc::new(mode, &[("A", 0), ("B", 0), ("C", 0), ("R", 0), ("E", 0)], 60);
    sc.advance(1);
    let s6 = sc.ms[0].local_doc(&mut sc.w, 6, DocOp::Insert).unwrap();
    for m in ["B", "C", "E"] {
        sc.window(m, "A", &s6);
    }
    sc.advance(10);
    let mut job = sc.ms[0].start_out(&mut sc.w, vec![0], JobKind::Good, 0);
    sc.advance(12);
    sc.ms[0].do_bind(&mut sc.w, &mut job, 5);
    sc.advance(14);
    sc.pull("C", "A");
    sc.pull("E", "A");
    sc.advance(20);
    let x = sc.ms[1].local_doc(&mut sc.w, 6, DocOp::Update).unwrap();
    sc.advance(24);
    sc.pull("B", "A"); // B learns the bind and mints r_x
    sc.advance(26);
    sc.window("C", "B", &x); // history at C: C mints r_C
    assert!(!loss_entries(&sc, "C", x.key, x.stamp).is_empty(), "C did not mint r_C");
    step(&mut sc);
    sc.advance(28);
    assert!(
        sc.snapshot("R", "C", false),
        "R joins from C, whole, with the record within the grant"
    );
    step(&mut sc);
    sc.advance(30);
    sc.vanish(&["B", "C"]);
    sc.advance(32);
    sc.pull("E", "R"); // the only route: a window from the joiner
    sc.w.step += 1;
    sc.quiesce();
    step(&mut sc);
    let res = finish(&mut sc.w, &mut sc.ms);
    let e = sc.idx("E");
    Twin {
        step_check: sc.ms.iter().any(|m| m.step_inv.contains("unbacked_state_record")),
        e_has_record: sc.ms[e].loss.keys().any(|k| k.key == x.key && k.stamp == Some(x.stamp)),
        problems: nonzero(&res),
    }
}

/// The step check's own mutant: B receives A's record entry by a window; with `m-loss-received-not-appended` it holds the
/// record in its state without an entry it serves onward. Nothing but the step check sees it on this script.
fn received_record_twin(mode: &str) -> Twin {
    let mut sc = Sc::new(mode, &[("A", 0), ("B", 0), ("C", 0)], 60);
    sc.advance(1);
    let s6 = sc.ms[0].local_doc(&mut sc.w, 6, DocOp::Insert).unwrap();
    for m in ["B", "C"] {
        sc.window(m, "A", &s6);
    }
    sc.advance(10);
    let mut job = sc.ms[2].start_out(&mut sc.w, vec![0], JobKind::Good, 0);
    sc.advance(12);
    sc.ms[2].do_bind(&mut sc.w, &mut job, 3);
    sc.advance(20);
    let x = sc.ms[0].local_doc(&mut sc.w, 6, DocOp::Update).unwrap(); // A, not having heard, writes a late update
    sc.advance(24);
    sc.pull("A", "C"); // A learns the bind: x is history, A mints r_x
    assert!(!loss_entries(&sc, "A", x.key, x.stamp).is_empty(), "A did not mint r_x");
    step(&mut sc);
    sc.advance(26);
    sc.pull_origins("B", "A", &["A"]); // B, not having heard of the bind, takes x (current at B), then r_x (deferred: L is not buried at B)
    step(&mut sc);
    let b = sc.idx("B");
    let has = (sc.ms[b].loss.keys().chain(sc.ms[b].loss_pending.keys()))
        .any(|k| k.key == x.key && k.stamp == Some(x.stamp));
    sc.quiesce();
    step(&mut sc);
    let res = finish(&mut sc.w, &mut sc.ms);
    Twin {
        step_check: sc.ms.iter().any(|m| m.step_inv.contains("unbacked_state_record")),
        e_has_record: has,
        problems: nonzero(&res),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed_885_shape_the_page_of_a_buried_physical_is_retained_and_the_head_reported_under(
        pre: &str,
    ) {
        let (_, recs, problems) = seed885(&format!("{pre}v6"));
        assert_eq!(recs, vec![true, true, true], "the late head has a record on A, B and D");
        assert!(problems.is_empty(), "{problems:?}");
        // the seed-885 rule off: every live member holds the write as granted state only and nobody mints the record
        let (_, recs, problems) = seed885(&format!("{pre}m-dead-page-dropped"));
        assert_eq!(recs, vec![false, false, false]);
        let names: Vec<&str> = problems.iter().map(|(k, _)| k.as_str()).collect();
        assert!(names.contains(&"loss_missing"), "{problems:?}");
        assert!(names.contains(&"inv_head_not_exactly_one_record_and_event"), "{problems:?}");
    }

    fn the_snapshot_record_set_is_the_sole_carrier_of_a_record_under(pre: &str) {
        let (e_has, problems) = sole_carrier(&format!("{pre}v6"));
        assert!(e_has && problems.is_empty(), "{problems:?}");
        for mutant in ["m-snap-drop-loss", "m-snap-no-loss-records"] {
            let (e_has, problems) = sole_carrier(mutant);
            assert!(!e_has, "{mutant}: E must hold the head with no record");
            let names: Vec<&str> = problems.iter().map(|(k, _)| k.as_str()).collect();
            assert!(names.contains(&"loss_missing"), "{mutant}: {problems:?}");
            assert!(
                names.contains(&"inv_head_not_exactly_one_record_and_event"),
                "{mutant}: {problems:?}"
            );
        }
    }

    /// The Rust-only mutants of the field apply and loss path that no random run of the metamorphic comparison or the field grid
    /// sees are killed here by the scenarios built for the shapes they break: a retained page version's late cells (seed 885's
    /// shape: the write is held only as a snapshot's state).
    #[test]
    fn a_field_mutant_the_random_runs_cannot_see_is_killed_by_the_scenario_of_its_shape() {
        let (recs, problems) = seed885_two_paths("field:v6");
        assert_eq!(recs, vec![(true, true); 3], "both late writes are reported at A, B and D");
        assert!(problems.is_empty(), "{problems:?}");
        let (recs, problems) = seed885_two_paths("field:v6+m-field-no-page-loss");
        eprintln!("m-field-no-page-loss on the two-path seed 885: {recs:?} {problems:?}");
        assert!(
            recs != vec![(true, true); 3] && !problems.is_empty(),
            "m-field-no-page-loss survived the scenario of its shape"
        );
    }

    /// The hold route (`swapmodel6a.py` `take_hold`, `promote_oop`): a non-document entry taken out of position is known to the member
    /// (its register sees it) but is not appended, so not served onward, until every earlier entry of its origin is held; a
    /// document entry is taken as usual. The repair route hands a register event over as information only.
    #[test]
    fn a_held_entry_is_known_but_served_only_once_it_is_in_position() {
        let mut sc = Sc::new("v6", &[("W", 0), ("D", 0), ("R", 0)], 60);
        let (w_, d, r) = (sc.idx("W"), sc.idx("D"), sc.idx("R"));
        sc.advance(1);
        let seed = sc.ms[w_].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
        sc.advance(10);
        let job = sc.ms[w_].start_out(&mut sc.w, vec![0], JobKind::Good, 0);
        let create =
            sc.w.all.iter().find(|e| e.kind == Kind::Create).cloned().expect("the stage's create");
        assert!(create.hlc > seed.hlc);
        // D takes the create ahead of its position: the seed (an earlier entry of W) is not held at D
        sc.ms[d].take_hold(&mut sc.w, &create);
        assert!(sc.ms[d].holds(create.idx) && sc.ms[d].held_oop.contains(&create.idx));
        assert!(!sc.ms[d].appended.contains(&create.idx), "held out of position: not appended");
        // the seed arrives: the create is in position and servable
        sc.ms[d].take(&mut sc.w, &seed);
        sc.ms[d].settle(&mut sc.w);
        assert!(sc.ms[d].appended.contains(&create.idx) && sc.ms[d].held_oop.is_empty());
        // the repair route: W binds, R is handed W's register events as information
        sc.advance(12);
        let mut job = job;
        let bind = sc.ms[w_].do_bind(&mut sc.w, &mut job, 3).expect("the bind");
        let evs: Vec<EntryId> = sc.ms[w_].evl.clone();
        for i in evs {
            let e = sc.w.ent_rc(i);
            sc.ms[r].add_event(&mut sc.w, &e);
        }
        assert!(sc.ms[r].info.contains_key(&bind.idx), "ahead of its position: information");
        assert!(!sc.ms[r].holds(bind.idx) && !sc.ms[r].events.contains(&bind.idx));
        assert_eq!(sc.w.stats.get("info_parked"), 1);
    }

    fn a_late_delete_held_only_as_snapshot_state_is_reported_once_everywhere_under(pre: &str) {
        let (recs, problems) = late_delete_only_as_state(&format!("{pre}v6"));
        assert_eq!(recs, vec![true, true, true], "one record and one event on W, Y and N");
        assert!(problems.is_empty(), "{problems:?}");
        // ADR-228's bare tombstone: no _id, no entry, no member mints the record
        let (recs, problems) = late_delete_only_as_state(&format!("{pre}m-snap-delete-bare"));
        assert_eq!(recs, vec![false, false, false]);
        assert!(!problems.is_empty());
    }

    /// Review F3: the negative case that tells S-bind from S-bind-origin. The shape is built with the REJECTED trigger as the control (it
    /// appends the write, so the scenario can see the difference) and the ruled trigger must not.
    #[test]
    fn rule_s_does_not_append_another_origins_write_above_the_binding_at_a_member_that_knows_no_binding()
     {
        let (appended, held) = other_origin_write_above_the_binding("v6");
        assert!(held, "the setup: X holds the newer version");
        assert!(!appended, "S-bind-origin appended another origin's write to a stage physical");
        let (appended, held) = other_origin_write_above_the_binding("m-s-bind-any-origin");
        assert!(held, "the setup: X holds the newer version");
        assert!(
            appended,
            "the scenario does not distinguish S-bind from S-bind-origin: it is vacuous"
        );
        // the same on the FIELD policy (rule S there is an unmodelled extension; the negative case is still asserted)
        let (appended, held) = other_origin_write_above_the_binding("field:v6");
        assert!(held, "field: the setup: X holds the newer version");
        assert!(!appended, "field: S-bind-origin appended another origin's write to a stage physical");
        let (appended, held) = other_origin_write_above_the_binding("field:m-s-bind-any-origin");
        assert!(held, "field: the setup: X holds the newer version");
        assert!(appended, "field: the scenario does not distinguish S-bind from S-bind-origin: it is vacuous");
    }

    fn the_mid_join_append_in_position_rule_under(pre: &str) {
        let (appended, partial, flipped) = append_in_position(&format!("{pre}v6"));
        assert!(appended, "the staged entry is appended in position and servable");
        assert!(!partial, "A flips with k");
        assert!(flipped);
        let (appended, partial, _) = append_in_position(&format!("{pre}m-no-append-in-position"));
        {
            // RULE S (v2.6): on both policies a staged row arriving in position is appended whatever gap 1 does, so gap 1's
            // mutant alone is EQUIVALENT here (the model's verdict change, REPORT8): the staged-row half is subsumed
            assert!(appended, "rule S appends the staged row although gap 1 is off");
            assert!(!partial);
            let (appended, partial, _) = append_in_position(&format!("{pre}m-staged-row-off"));
            assert!(appended, "gap 1 appends it although rule S is off");
            assert!(!partial);
            // the REQUIRED kill: both off
            let (appended, partial, _) =
                append_in_position(&format!("{pre}m-no-append-in-position+m-staged-row-off"));
            assert!(!appended);
            assert!(
                partial,
                "with gap 1 and rule S both off A witnesses the entry without receiving it and flips without k"
            );
        }
    }

    fn a_join_is_resumed_only_from_its_own_sending_peer_under(pre: &str) {
        let (unchanged, serves, problems) = join_from_another_peer(&format!("{pre}v6"));
        assert!(
            unchanged,
            "a begin from another peer while a join is in progress is refused with no side effect"
        );
        assert!(serves, "N serves the output after its join completes: the register came from X");
        assert!(problems.is_empty(), "{problems:?}");
        // the reviewer's source-overwrite defect: the last page reads the register of the member of the last begin call
        let (_, serves, problems) = join_from_another_peer(&format!("{pre}m-stale-src"));
        assert!(!serves, "the register was read from a member that is not the sender");
        let names: Vec<&str> = problems.iter().map(|(k, _)| k.as_str()).collect();
        assert!(
            names.contains(&"inv_register_read_from_a_member_that_is_not_the_sender"),
            "{problems:?}"
        );
    }

    /// Item R: a release that takes effect one settle late (`m-release-without-effect`) lets a snapshot carry the coverage without the
    /// event; build item (a) (the release takes effect in the releasing transaction) keeps the run clean.
    #[test]
    fn a_snapshot_between_release_and_effect_needs_build_item_a() {
        for pre in ["", "field:"] {
            let p = release_effect_twin(&format!("{pre}v6"));
            assert!(p.is_empty(), "{pre}v6: {p:?}");
            let m = release_effect_twin(&format!("{pre}m-release-without-effect"));
            eprintln!("{pre}m-release-without-effect: {m:?}");
            assert!(!m.is_empty(), "{pre}: the mutant must be killed by the scripted scenario");
        }
    }

    fn rule_b_a_held_bind_is_information_until_covered_under(pre: &str) {
        let (parked, refused, took_effect, partial, problems) = rule_b_shape(&format!("{pre}v6"));
        assert!(parked && refused, "the held bind is information and D10 refuses writes on it");
        assert!(took_effect, "it takes effect once covered");
        assert!(!partial, "C flips whole");
        assert!(problems.is_empty(), "{problems:?}");
        // rule B off: the known bind sets the stage's birth, A's own drop buries the stage before its rows arrive, the
        // rows are history there and never served, and C flips to a stage without them
        let (parked, refused, _, partial, _) = rule_b_shape(&format!("{pre}m-rule-b-off"));
        assert!(!parked && refused);
        assert!(
            partial,
            "without rule B C covers the writer at the bind through a window that skipped the rows"
        );
        // M6/N6: D10 on an event parked as information: with `m-d10-ignores-info` the write is accepted while the parked bind
        // supersedes the served physical, and the invariant names it
        let (parked, refused, _, _, problems) = rule_b_shape(&format!("{pre}m-d10-ignores-info"));
        assert!(parked && !refused, "the mutant accepts a write while the bind is information");
        assert!(
            problems.iter().any(|(k, _)| k == "inv_write_accepted_while_info_superseded"),
            "{problems:?}"
        );
    }

    fn a_refusal_on_an_unfinished_join_is_released_by_a_fresh_pull_after_its_sender_dies_under(
        pre: &str,
    ) {
        let s = sender_dies_mid_join(&format!("{pre}v6"));
        assert!(
            s.placeholder_before && s.join_open_before,
            "the stage is a placeholder page of an unfinished join"
        );
        assert!(
            s.refused_before,
            "the member refuses writes: the binding is covered but cannot be made visible"
        );
        assert!(
            s.refused_still_after_sender_dies,
            "the refusal stays open while the dead sender's join is unfinished"
        );
        assert!(s.superseded);
        assert!(!s.join_after, "the fresh pull completed");
        assert_eq!(s.placeholders_after, 0, "no stale placeholder is left");
        assert_eq!(s.held_records_after, 0, "no held record is left");
        assert!(s.serving_stage_after, "the binding is visible");
        assert!(s.write_accepted_after, "the refusal is released");
        assert!(s.problems.is_empty(), "{:?}", s.problems);
    }

    /// Under both policies: the `document` policy (the model) and the `field` policy (`merge_state`).
    #[test]
    fn seed_885_shape_the_page_of_a_buried_physical_is_retained_and_the_head_reported() {
        for pre in ["", "field:"] {
            seed_885_shape_the_page_of_a_buried_physical_is_retained_and_the_head_reported_under(
                pre,
            );
        }
    }

    /// Under both policies: the `document` policy (the model) and the `field` policy (`merge_state`).
    #[test]
    fn the_snapshot_record_set_is_the_sole_carrier_of_a_record() {
        for pre in ["", "field:"] {
            the_snapshot_record_set_is_the_sole_carrier_of_a_record_under(pre);
        }
    }

    /// Under both policies: the `document` policy (the model) and the `field` policy (`merge_state`).
    #[test]
    fn a_late_delete_held_only_as_snapshot_state_is_reported_once_everywhere() {
        for pre in ["", "field:"] {
            a_late_delete_held_only_as_snapshot_state_is_reported_once_everywhere_under(pre);
        }
    }

    /// Under both policies: the `document` policy (the model) and the `field` policy (`merge_state`).
    #[test]
    fn the_mid_join_append_in_position_rule() {
        for pre in ["", "field:"] {
            the_mid_join_append_in_position_rule_under(pre);
        }
    }

    /// Under both policies: the `document` policy (the model) and the `field` policy (`merge_state`).
    #[test]
    fn a_join_is_resumed_only_from_its_own_sending_peer() {
        for pre in ["", "field:"] {
            a_join_is_resumed_only_from_its_own_sending_peer_under(pre);
        }
    }

    /// Under both policies: the `document` policy (the model) and the `field` policy (`merge_state`).
    #[test]
    fn rule_b_a_held_bind_is_information_until_covered() {
        for pre in ["", "field:"] {
            rule_b_a_held_bind_is_information_until_covered_under(pre);
        }
    }

    /// Under both policies: the `document` policy (the model) and the `field` policy (`merge_state`).
    #[test]
    fn a_refusal_on_an_unfinished_join_is_released_by_a_fresh_pull_after_its_sender_dies() {
        for pre in ["", "field:"] {
            a_refusal_on_an_unfinished_join_is_released_by_a_fresh_pull_after_its_sender_dies_under(
                pre,
            );
        }
    }

    fn names(t: &Twin) -> Vec<&str> {
        t.problems.iter().map(|(k, _)| k.as_str()).collect()
    }

    /// Rule B for loss records (the seed-1434 shape): a record out of position is information; off, C holds r_x in its state
    /// with no entry it serves, never mints, and E never gets a record for x.
    #[test]
    fn seed_1434_shape_a_loss_record_ahead_of_its_position_is_information() {
        for pre in ["", "field:"] {
            let t = seed_1434_twin(&format!("{pre}v6"));
            assert!(t.problems.is_empty(), "{:?}", t.problems);
            assert!(!t.step_check && t.e_has_record, "E gets a record for x by a window");
            assert!(t.problems.is_empty());
            let m = seed_1434_twin(&format!("{pre}m-loss-out-of-position"));
            eprintln!(
                "m-loss-out-of-position: step check {} E record {} {:?}",
                m.step_check, m.e_has_record, m.problems
            );
            assert!(!m.e_has_record, "the mutant: E has no record for x");
            assert!(m.step_check, "the step check sees the unbacked record at C");
            assert!(names(&m).contains(&"loss_missing"), "{:?}", m.problems);
        }
    }

    /// Clause (3) and the paged read (the scripted paged join): the sender's records are read at the last page, a record is
    /// carried only if an entry of it is within the grant.
    #[test]
    fn a_paged_join_carries_a_record_only_if_an_entry_of_it_is_within_the_grant() {
        for pre in ["", "field:"] {
            for mode in ["v6", "v6-paged-loss"] {
                let t = paged_join_twin(&format!("{pre}{mode}"));
                assert!(t.problems.is_empty(), "{mode}: {:?}", t.problems);
                assert!(t.e_has_record && !t.step_check, "{mode}: E gets R's record by a window");
            }
            let m = paged_join_twin(&format!("{pre}m-snap-record-no-grant-filter"));
            eprintln!(
                "m-snap-record-no-grant-filter: step check {} E record {} {:?}",
                m.step_check, m.e_has_record, m.problems
            );
            assert!(!m.e_has_record, "the mutant: R holds the record alone, E never gets one");
            assert!(m.step_check, "the step check sees the record R holds without an entry of it");
            let n = names(&m);
            assert!(n.contains(&"loss_missing") && n.contains(&"div_loss"), "{:?}", m.problems);
        }
    }

    /// M3: a record carried WITHIN the grant becomes a served entry at the joiner (clause (3)), shown with a third member that
    /// pulls only by window from the joiner. The mutant carries it as state alone.
    #[test]
    fn a_record_carried_within_the_grant_is_a_served_entry_a_third_member_gets_by_window() {
        for pre in ["", "field:"] {
            let t = within_grant_join_twin(&format!("{pre}v6"));
            assert!(t.problems.is_empty(), "{:?}", t.problems);
            assert!(t.e_has_record && !t.step_check, "E gets the record from R by a window");
            let m = within_grant_join_twin(&format!("{pre}m-snap-record-not-served"));
            eprintln!(
                "m-snap-record-not-served: step check {} E record {} {:?}",
                m.step_check, m.e_has_record, m.problems
            );
            assert!(m.step_check, "the step check sees the carried record without a served entry");
            assert!(!m.e_has_record, "the mutant: E never gets the record");
        }
    }

    /// The step check's own mutant: a received record entry that is not appended. Only the step check sees it here.
    #[test]
    fn the_step_check_sees_a_record_in_the_state_without_an_entry_the_member_serves() {
        for pre in ["", "field:"] {
            let t = received_record_twin(&format!("{pre}v6"));
            assert!(t.e_has_record && !t.step_check && t.problems.is_empty(), "{:?}", t.problems);
            let m = received_record_twin(&format!("{pre}m-loss-received-not-appended"));
            assert!(
                m.e_has_record && m.step_check,
                "the record is in B state, unbacked: {} {} {:?}",
                m.e_has_record,
                m.step_check,
                m.problems
            );
            eprintln!("m-loss-received-not-appended: {:?}", m.problems);
            assert_eq!(
                names(&m),
                vec!["inv_unbacked_state_record", "invariant"],
                "{:?}",
                m.problems
            );
        }
    }
}
