//! Tests of the `field` policy (phase F): the apply decisions, the loss record per lost head, Q7's head rule, the 64 B cut.

use crate::entry::*;
use crate::field::*;
use crate::member::DocOp;
use crate::oracle::finish;
use crate::scenario::Sc;
use crate::world::Policy;

fn opts() -> kimmy_core::merge_state::JoinOptions {
    kimmy_core::merge_state::JoinOptions::new(1_000_000_000, i64::MAX).unwrap()
}

#[test]
fn a_value_is_cut_at_64_bytes_on_a_character_boundary_with_a_truncated_flag() {
    let (s, t) = cut("short", 64);
    assert_eq!((s.as_str(), t), ("short", false));
    let long = "é".repeat(40); // 80 bytes
    let (s, t) = cut(&long, 64);
    assert!(
        t && s.len() <= 64 && s.len() % 2 == 0 && long.starts_with(&s),
        "cut at a boundary: {} bytes",
        s.len()
    );
    assert_eq!(s.len(), 64);
    let odd = format!("a{}", "é".repeat(40)); // the 64th byte is inside a character
    let (s, _) = cut(&odd, 64);
    assert_eq!(s.len(), 63, "the cut backs off to the boundary");
    // the record body: the cut value, and a second line only when it was cut
    assert_eq!(record_body("x").unwrap().as_ref(), &vec!["x".to_string()]);
    let b = record_body(&"y".repeat(100)).unwrap();
    assert_eq!(b.len(), 2);
    assert_eq!(b[0].len(), 64);
    assert_eq!(b[1], "truncated");
}

#[test]
fn the_late_cells_of_a_state_are_its_registers_slots_lives_deletes_and_recorded_losers() {
    let o = opts();
    let a = local_write(0, None, DocOp::Insert, 5, "m5", &o, false).unwrap().unwrap();
    let b = local_write(0, Some(&a), DocOp::Update, 9, "m9", &o, false).unwrap().unwrap(); // set v
    let c = local_write(0, Some(&b), DocOp::Update, 11, "m11", &o, true).unwrap().unwrap(); // inc n
    let d = local_write(0, Some(&c), DocOp::Delete, 13, "m13", &o, false).unwrap().unwrap();
    // an entry's own write: the cells stamped with its stamp
    let own = late_cells(&b, neg(), Some(stamp(9, 0)));
    assert_eq!(own.len(), 1);
    assert_eq!(own[0].path.as_ref().map(|p| p.segments().join(".")).as_deref(), Some("v"));
    let own = late_cells(&c, neg(), Some(stamp(11, 0)));
    assert_eq!(own.len(), 1, "an increment is one slot");
    assert_eq!(own[0].path.as_ref().unwrap().segments(), ["n"]);
    // an insert is a write of the field and of the whole document
    let ins = late_cells(&a, neg(), Some(stamp(5, 0)));
    assert_eq!(ins.len(), 2);
    assert!(ins.iter().any(|c| c.path.is_none()) && ins.iter().any(|c| c.path.is_some()));
    // a delete is the document-level write, and the dead life's cells are gone from the state
    let del = late_cells(&d, neg(), Some(stamp(13, 0)));
    assert_eq!(del.len(), 1);
    assert_eq!((del[0].path.clone(), del[0].value.as_str()), (None, "deleted"));
    // a page version: every cell after the event
    let all = late_cells(&c, stamp(6, 0), None);
    assert!(all.iter().all(|c| c.stamp > stamp(6, 0)) && all.len() == 2, "{all:?}");
}

/// Q7 D11 and the covered-entry skip, as the Q7 sim's `receive_entry`: an entry whose whole version vector the receiver
/// covers is witnessed only; an entry that changes the joined state is appended; one that changes nothing is not.
#[test]
fn a_field_entry_is_appended_iff_it_changes_the_state_and_a_covered_one_is_only_witnessed() {
    let mut sc = Sc::new("field:v6", &[("A", 0), ("B", 0), ("C", 0)], 60);
    let (a, b, c) = (sc.idx("A"), sc.idx("B"), sc.idx("C"));
    sc.advance(1);
    let a0 = sc.ms[a].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
    sc.window("B", "A", &a0);
    sc.advance(2);
    let b1 = sc.ms[b].local_doc(&mut sc.w, 0, DocOp::Update).unwrap(); // its state holds a0's insert
    // C receives the later entry first: it changes C's (empty) state, so it is appended
    sc.window("C", "B", &b1);
    assert!(sc.ms[c].appended.contains(&b1.idx));
    // then a0: C's version vector already covers it, so it is witnessed only and not appended
    let skips = sc.w.stats.get("covered_skips");
    sc.window("C", "A", &a0);
    assert_eq!(sc.w.stats.get("covered_skips"), skips + 1);
    assert!(sc.ms[c].holds(a0.idx) && !sc.ms[c].appended.contains(&a0.idx));
    // A receives b1: it changes A's state (b1's set), appended
    sc.window("A", "B", &b1);
    assert!(sc.ms[a].appended.contains(&b1.idx));
    // the same entry again changes nothing: held already, nothing happens
    sc.window("A", "B", &b1);
    let res = {
        sc.quiesce();
        finish(&mut sc.w, &mut sc.ms)
    };
    for k in ["diverged", "oracle_state", "loss_mismatch", "invariant"] {
        assert_eq!(res[k], 0, "{k}");
    }
}

/// Builds the swap and a set of late writes by `O` (skew 50: stamped after the binding, accepted before it hears of it).
fn swapped(mode: &str) -> (Sc, Job) {
    let mut sc = Sc::new(mode, &[("W", 0), ("X", 0), ("O", 50)], 60);
    let (w_, x, o) = (sc.idx("W"), sc.idx("X"), sc.idx("O"));
    sc.advance(1);
    sc.ms[w_].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
    for m in ["X", "O"] {
        sc.pull(m, "W");
    }
    sc.advance(10);
    let mut job = sc.ms[w_].start_out(&mut sc.w, vec![1], JobKind::Good, 0);
    sc.advance(12);
    sc.ms[w_].do_bind(&mut sc.w, &mut job, 3).unwrap();
    let _ = (x, o);
    (sc, job)
}

fn records(sc: &Sc, member: &str, key: u32) -> Vec<(String, i64, Body)> {
    let m = &sc.ms[sc.idx(member)];
    let mut v: Vec<(String, i64, Body)> = m
        .loss
        .iter()
        .filter(|(k, _)| k.key == key)
        .map(|(k, b)| {
            (
                k.path.map_or("<document>".to_string(), |p| sc.w.path_label(p)),
                hlc_of(&k.stamp.unwrap()),
                b.clone(),
            )
        })
        .collect();
    v.sort_by(|a, b| (a.1, &a.0).cmp(&(b.1, &b.0)));
    v
}

/// A late `field` write is reported per lost head: one record per path and loser stamp, the value cut at 64 B per value
/// with a truncated marker, the same on every member, one event each.
#[test]
fn a_late_field_write_is_reported_per_lost_head_with_the_value_cut_at_64_bytes() {
    let (mut sc, _job) = swapped("field:v6");
    let o = sc.idx("O");
    // twenty sets of `v` (even clocks are sets for member O) make the register longer than 64 bytes, then one `$inc` of `n`
    for c in (14..=52).step_by(2) {
        sc.advance(c);
        sc.ms[o].local_doc(&mut sc.w, 0, DocOp::Update).expect("O has not heard of the binding");
    }
    sc.advance(55);
    sc.ms[o].local_doc(&mut sc.w, 0, DocOp::Update).expect("an increment");
    sc.quiesce();
    for m in ["W", "X", "O"] {
        let r = records(&sc, m, 0);
        let v: Vec<_> = r.iter().filter(|(p, _, _)| p == "v").collect();
        let n: Vec<_> = r.iter().filter(|(p, _, _)| p == "n").collect();
        assert_eq!(v.len(), 20, "a record per late set of v at {m}");
        assert_eq!(n.len(), 1, "one record for the late increment at {m}");
        let head = v.last().unwrap();
        let body = head.2.as_ref().unwrap();
        assert!(
            body[0].len() <= 64 && body.last().map(String::as_str) == Some("truncated"),
            "the newest set is cut: {body:?}"
        );
        let first = v.first().unwrap().2.as_ref().unwrap();
        assert_eq!(first.len(), 1, "a short value is not cut: {first:?}");
        let mm = &sc.ms[sc.idx(m)];
        assert!(mm.events_emitted.values().all(|c| *c == 1), "one event per record at {m}");
    }
    let res = finish(&mut sc.w, &mut sc.ms);
    for k in [
        "diverged",
        "oracle_state",
        "loss_mismatch",
        "loss_missing",
        "loss_extra",
        "loss_body_mismatch",
        "invariant",
    ] {
        assert_eq!(res[k], 0, "{k}");
    }
    assert_eq!(
        res["heads_required"], 2,
        "one head on v (the last set) and one on n (the increment)"
    );
}

/// Q7's head: concurrent late writes on one path are all heads; the stamp-newest wording of the `document` policy
/// would require only one of them.
#[test]
fn concurrent_late_writes_on_one_path_are_all_heads_under_field_and_one_under_document() {
    for (mode, heads) in [("field:v6", 2), ("v6", 1)] {
        let (mut sc, _) = swapped(mode);
        let (x, o) = (sc.idx("X"), sc.idx("O"));
        sc.advance(14);
        sc.ms[o].local_doc(&mut sc.w, 0, DocOp::Update).expect("O writes"); // a set of v (field), an update (document)
        sc.advance(15);
        sc.ms[x]
            .local_doc(&mut sc.w, 0, DocOp::Update)
            .expect("X writes, concurrently: it has not seen O's entry");
        sc.quiesce();
        let res = finish(&mut sc.w, &mut sc.ms);
        for k in
            ["diverged", "oracle_state", "loss_mismatch", "loss_missing", "loss_extra", "invariant"]
        {
            assert_eq!(res[k], 0, "{mode}: {k}");
        }
        assert_eq!(res["heads_required"], heads, "{mode}: heads required");
        assert_eq!(res["true_late_writes_allowed"], 2, "{mode}: both writes are late");
    }
}

/// A late write that a later late writer had seen is not a head (its writer's state covers it); the later one is.
#[test]
fn a_late_write_the_next_late_writer_had_seen_is_not_a_head() {
    let (mut sc, _) = swapped("field:v6");
    let (x, o) = (sc.idx("X"), sc.idx("O"));
    sc.advance(14);
    let eo = sc.ms[o].local_doc(&mut sc.w, 0, DocOp::Update).expect("O writes");
    sc.window("X", "O", &eo); // X sees O's late write before writing its own
    sc.advance(15); // a set of v for member X (odd clock)
    let ex =
        sc.ms[x].local_doc(&mut sc.w, 0, DocOp::Update).expect("X writes after seeing O's entry");
    assert!(
        ex.fstate.as_ref().unwrap().vv.get(&eo.stamp.writer).is_some_and(|h| *h >= eo.stamp.hlc)
    );
    sc.quiesce();
    let res = finish(&mut sc.w, &mut sc.ms);
    for k in
        ["diverged", "oracle_state", "loss_mismatch", "loss_missing", "loss_extra", "invariant"]
    {
        assert_eq!(res[k], 0, "{k}");
    }
    assert_eq!(res["heads_required"], 1, "X's write only");
    assert_eq!(res["true_late_writes_allowed"], 2, "O's is allowed (and reported)");
    let _ = Policy::Field;
}

// ---------------------------------------------------------------------------------------------------------------- point 2

/// The HLC rule (design note Q-F6, the manager's ruling): the product witnesses every applied entry's stamp (raise only) and a
/// local write ticks strictly above; a joined page state's version vector is witnessed into the clock. The primitive first: a
/// `Writer` refuses a stamp that is not above the state's version vector.
#[test]
fn a_write_below_the_merged_version_vector_is_refused_by_the_writer() {
    let o = opts();
    let a = local_write(2, None, DocOp::Insert, 55, "m55", &o, false).unwrap().unwrap();
    let r = local_write(0, Some(&a), DocOp::Update, 7, "m7", &o, false);
    assert!(r.is_err(), "a stamp below the vv (55) is a StampError::NotAbove");
}

/// A member in the middle of a join holds a page state whose version vector is far above its clock; its next local write
/// must tick above it and proceed (no refusal, no delay).
#[test]
fn a_member_mid_join_writes_above_the_page_states_version_vector() {
    let mut sc = Sc::new("field:v6", &[("S", 0), ("W2", 0), ("O", 50), ("N", 0)], 60);
    let (s, o, n) = (sc.idx("S"), sc.idx("O"), sc.idx("N"));
    sc.advance(1);
    let seed = sc.ms[s].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
    for m in ["W2", "O", "N"] {
        sc.window(m, "S", &seed);
    }
    sc.advance(5);
    let eo = sc.ms[o]
        .local_doc(&mut sc.w, 0, DocOp::Update)
        .expect("O writes, its stamp is far ahead (skew 50)");
    assert!(hlc_of(&eo.stamp) >= 55);
    sc.window("S", "O", &eo);
    // a second alive physical keeps S's snapshot at two pages, so N's join stays unfinished after the page of L lands
    let job2 = {
        let w2 = sc.idx("W2");
        sc.ms[w2].start_out(&mut sc.w, vec![1], JobKind::Good, 0)
    };
    sc.pull("S", "W2");
    assert!(sc.snapshot("N", "S", true));
    {
        let _ = &job2;
        let j = sc.ms[n].join.as_mut().unwrap();
        j.pages.retain(|p| p != L);
        j.pages.push(L.to_string()); // pages pop from the end: L lands first
    }
    assert!(sc.ms[n].hlc < 55, "before the page lands N's clock is small ({})", sc.ms[n].hlc);
    sc.land("N");
    assert!(sc.ms[n].join.is_some(), "the join is unfinished: the grant has not landed");
    assert!(
        sc.ms[n].hlc >= 55,
        "the page state's version vector was witnessed into the clock ({})",
        sc.ms[n].hlc
    );
    sc.advance(6);
    let w = sc.ms[n].local_doc(&mut sc.w, 0, DocOp::Update).expect("the write proceeds");
    assert!(hlc_of(&w.stamp) > 55, "it ticks above the merged vector ({})", hlc_of(&w.stamp));
    assert_eq!(sc.w.stats.get("field_stamp_refused"), 0);
}

/// Q7's loss records are entries of the relay (V6M2): a member that detects a clash appends its own loss entry under its own
/// origin; every member ends with the same record set for the document; a member mints at most one entry per record.
#[test]
fn a_detected_q7_record_is_minted_as_an_entry_and_every_member_ends_with_it() {
    let mut sc = Sc::new("field:v6", &[("W", 0), ("X", 0), ("Y", 0)], 60);
    let (w_, x, y) = (sc.idx("W"), sc.idx("X"), sc.idx("Y"));
    sc.advance(1);
    sc.ms[w_].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
    for m in ["X", "Y"] {
        sc.pull(m, "W");
    }
    // concurrent sets of the same register (even clocks are sets for X and Y: (clock + id) even)
    sc.advance(5);
    let ex = sc.ms[x].local_doc(&mut sc.w, 0, DocOp::Update).expect("X sets v"); // (5 + 1) even
    sc.advance(6);
    let ey = sc.ms[y].local_doc(&mut sc.w, 0, DocOp::Update).expect("Y sets v"); // (6 + 2) even
    assert!(
        !ey.fstate.as_ref().unwrap().vv.contains_key(&ex.stamp.writer),
        "concurrent: Y has not seen X's entry"
    );
    sc.quiesce();
    let minted = sc.w.all.iter().filter(|e| e.kind == Kind::LossQ7).count();
    assert!(
        (1..=3).contains(&minted),
        "the clash is minted by the members that detect it: {minted}"
    );
    let sets: Vec<_> = [w_, x, y]
        .iter()
        .map(|&i| {
            sc.ms[i]
                .phys
                .get("L")
                .unwrap()
                .docs
                .get(&0)
                .unwrap()
                .state
                .as_ref()
                .unwrap()
                .losses
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(sets[0].len(), 1, "one clash: {sets:?}");
    assert!(sets.iter().all(|s| *s == sets[0]), "every member holds the same record: {sets:?}");
    let res = finish(&mut sc.w, &mut sc.ms);
    for k in ["diverged", "oracle_state", "loss_mismatch", "invariant", "q7_records_diverged"] {
        assert_eq!(res[k], 0, "{k}");
    }
}

/// The mutant that desynchronises the members' record sets: the loss entries are not minted. The end-of-run check
/// (`finish`, `q7_records_diverged`: the Q7 records of each document compared across the live members) fires on the grid.
#[test]
fn without_the_q7_loss_entries_the_record_sets_of_the_members_diverge() {
    use crate::run::{Cfg, grid, run_grid};
    let seeds = 60;
    let mut cfgs = grid(&[3, 4], &[1, 2], &[3, 4]);
    for c in cfgs.iter_mut() {
        c.policy = Policy::Field;
    }
    let on = run_grid("v6", "v6", &cfgs, 0, seeds, 4);
    assert_eq!(on.get("q7_records_diverged"), 0, "with the entries: {:?}", on.problems());
    for c in cfgs.iter_mut() {
        c.q7_entries = false;
    }
    let off = run_grid("v6", "v6", &cfgs, 0, seeds, 4);
    assert!(
        off.get("q7_records_diverged") > 0,
        "without the entries the record sets diverge in {} runs",
        off.runs
    );
    let _ = Cfg::new(2, 1, 3);
}

/// Seed 406 of the field grid (4 members, 3 outs, 3 keys): an origin vanished and the only holders keep the version as page
/// state. The whole-insert record, key `(_id 3, the document level, loser stamp 28)`, is what Q7 D10 (1) reports for the insert,
/// and it is held by every live member; the per-field cell `(3, v, 28)` is allowed but, with the insert's life superseded,
/// no longer derivable from any state, so the required set does not ask for it (`REQUIRE_INSERT_FIELD_CELLS`).
#[test]
fn seed_406_passes_because_the_whole_insert_record_covers_the_insert() {
    use crate::run::{Cfg, run};
    let mut cfg = Cfg::new(4, 3, 3);
    cfg.policy = Policy::Field;
    let mut r = run("v6", "v6", 406, cfg, false);
    for k in ["loss_missing", "loss_mismatch", "diverged", "invariant", "loss_extra"] {
        assert_eq!(r.res[k], 0, "{k}");
    }
    let o = crate::oracle::oracle(&mut r.w);
    let hit = o.allowed.iter().find(|(k, p, s)| *k == 3 && p.is_none() && hlc_of(s) == 28).cloned();
    assert!(
        hit.is_some(),
        "the whole-insert write is late and allowed: {:?}",
        o.allowed.iter().filter(|(k, _, _)| *k == 3).collect::<Vec<_>>()
    );
    let cell =
        o.allowed.iter().find(|(k, p, s)| *k == 3 && p.is_some() && hlc_of(s) == 28).cloned();
    assert!(cell.is_some(), "the per-field cell is allowed");
    assert!(
        !o.loss.keys().any(|(k, p, s)| *k == 3 && p.is_some() && hlc_of(s) == 28),
        "the per-field cell of the insert is not required"
    );
    let live: Vec<_> = r.ms.iter().filter(|m| !m.vanished).collect();
    assert!(live.iter().all(|m| {
        m.loss
            .keys()
            .any(|k| k.key == 3 && k.path.is_none() && k.stamp.is_some_and(|s| hlc_of(&s) == 28))
    }));
}
