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

    fn a_late_delete_held_only_as_snapshot_state_is_reported_once_everywhere_under(pre: &str) {
        let (recs, problems) = late_delete_only_as_state(&format!("{pre}v6"));
        assert_eq!(recs, vec![true, true, true], "one record and one event on W, Y and N");
        assert!(problems.is_empty(), "{problems:?}");
        // ADR-228's bare tombstone: no _id, no entry, no member mints the record
        let (recs, problems) = late_delete_only_as_state(&format!("{pre}m-snap-delete-bare"));
        assert_eq!(recs, vec![false, false, false]);
        assert!(!problems.is_empty());
    }

    fn the_mid_join_append_in_position_rule_under(pre: &str) {
        let (appended, partial, flipped) = append_in_position(&format!("{pre}v6"));
        assert!(appended, "the staged entry is appended in position and servable");
        assert!(!partial, "A flips with k");
        assert!(flipped);
        let (appended, partial, _) = append_in_position(&format!("{pre}m-no-append-in-position"));
        assert!(!appended);
        assert!(
            partial,
            "without the rule A witnesses the entry without receiving it and flips without k"
        );
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
}
