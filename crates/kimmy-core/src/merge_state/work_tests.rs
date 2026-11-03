//! The work of the merge state, counted rather than timed: how many registers a join or a local
//! write clones, how many cells a read visits, how often a write walks the whole state. The
//! bounds are the complexity the code is meant to have (linear in the cells, nothing per path
//! times cells); behaviour is pinned elsewhere (the fingerprints of the sim and the
//! differentials, the unit tests).

use super::tests::{fl, i, opts, p, s, wid};
use super::work::count;
use super::*;

/// A writer holding a document of `n` string fields `f000`.. and two counters.
fn wide(n: usize) -> Writer {
    let names: Vec<String> = (0..n).map(|k| format!("f{k:03}")).collect();
    let mut fields: Vec<(&str, Value)> = names.iter().map(|k| (k.as_str(), s("v"))).collect();
    fields.push(("c1", i(0)));
    fields.push(("c2", i(0)));
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&fl(&fields), 0).unwrap();
    w
}

#[test]
fn a_writer_that_has_written_does_not_walk_the_state_for_the_writer_cap() {
    let mut a = wide(20);
    let (_, c) = count(|| {
        for k in 0..5 {
            a.write_set(&p("f003"), s(&format!("x{k}")), 0).unwrap();
        }
        a.write_inc(&p("c1"), 1, 0).unwrap();
    });
    assert_eq!(c.writer_scans, 0);
    // a writer the document does not mention yet is checked against the cap, once
    let mut b = Writer::new(wid('B', 0), opts());
    b.receive(&a.state);
    let (_, c) = count(|| b.write_set(&p("f004"), s("y"), 0).unwrap());
    assert_eq!(c.writer_scans, 1);
    let (_, c) = count(|| b.write_set(&p("f005"), s("y"), 0).unwrap());
    assert_eq!(c.writer_scans, 0);
}

/// Two writers' copies of a document of `n` fields, where the second then `$set`s `changed` of
/// them (and so differs from the first by that many registers).
fn two(n: usize, changed: usize) -> (Writer, Writer, usize) {
    let a = wide(n);
    let mut b = Writer::new(wid('B', 0), opts());
    b.receive(&a.state);
    for k in 0..changed {
        b.write_set(&p(&format!("f{k:03}")), s("new"), 0).unwrap();
    }
    let regs = a.state.regs.len();
    (a, b, regs)
}

#[test]
fn a_join_copies_the_first_state_once_and_the_registers_that_win() {
    for n in [20, 200] {
        let (a, b, regs) = two(n, n / 10);
        let ((j, _), c) = count(|| a.state.join(&b.state, &opts()));
        assert_eq!(j.regs.len(), regs);
        // the copy of `a` (the result is built in it) and the `n / 10` registers of `b` that
        // replace the ones in it; not a copy of each side before the union and another after
        assert_eq!(c.reg_clones, regs + n / 10, "{n} fields");
        // the joined-in state is the receiver's: one copy of it and the winners
        let mut r = a.clone();
        let (rec, c) = count(|| r.receive_report(&b.state));
        assert!(rec.changed);
        assert_eq!(c.reg_clones, regs + n / 10, "receive, {n} fields");
    }
}

#[test]
fn a_local_write_copies_no_register() {
    for n in [20, 200] {
        let mut w = wide(n);
        w.write_set(&p("f000"), s("a"), 0).unwrap();
        let (_, c) = count(|| w.write_set(&p("f001"), s("b"), 0).unwrap());
        assert_eq!(c.reg_clones, 0, "$set, {n} fields");
        let (_, c) = count(|| w.write_inc(&p("c1"), 1, 0).unwrap());
        assert_eq!(c.reg_clones, 0, "$inc, {n} fields");
        let (_, c) = count(|| w.write_unset(&p("f002"), 0).unwrap());
        assert_eq!(c.reg_clones, 0, "$unset, {n} fields");
    }
}

#[test]
fn the_clash_search_of_a_join_is_linear_in_the_cells() {
    let visits = |n: usize| {
        let (a, mut b, regs) = two(n, 0);
        // every field is changed by both: n concurrent registers
        let mut a = a;
        for k in 0..n {
            a.write_set(&p(&format!("f{k:03}")), s("from a"), 0).unwrap();
            b.write_set(&p(&format!("f{k:03}")), s("from b"), 0).unwrap();
        }
        let ((j, fresh), c) = count(|| a.state.join(&b.state, &opts()));
        assert_eq!(fresh.len(), n, "a clash for every field");
        assert_eq!(j.regs.len(), regs);
        c.visits
    };
    let (v1, v2) = (visits(100), visits(200));
    // the registers of both sides, once each (not once per path)
    assert_eq!(v1, 2 * 102);
    assert_eq!(v2, 2 * 202);
}

#[test]
fn a_read_of_every_path_visits_each_cell_once_not_once_per_path() {
    for n in [100, 200, 400] {
        let mut w = wide(n);
        w.write_inc(&p("c1"), 1, 0).unwrap();
        let o = opts();
        let cells = w.state.regs.len() + w.state.slots.len();
        let (v, c) = count(|| w.state.visible(&o));
        assert_eq!(v.unwrap().len(), n + 2);
        assert_eq!(c.visits, cells, "visible, {n} fields");
        let (_, c) = count(|| w.state.hidden_all(&o));
        assert_eq!(c.visits, cells, "hidden_all, {n} fields");
        let (_, c) = count(|| w.state.excess(&o));
        assert_eq!(c.visits, cells, "excess, {n} fields");
        let (_, c) = count(|| w.state.over_caps(&o));
        assert_eq!(c.visits, cells, "over_caps, {n} fields");
    }
}

#[test]
fn a_read_of_one_path_looks_at_the_alive_lives_not_at_every_register() {
    for n in [20, 400] {
        let mut w = wide(n);
        w.write_inc(&p("c1"), 1, 0).unwrap();
        let o = opts();
        let (v, c) = count(|| w.state.value(&p("c1"), &o));
        assert_eq!(v, Some(Value::Int64(1)));
        // one life: one lookup of its register and one range of its slots
        assert_eq!(c.visits, 2, "{n} fields");
        let (_, c) = count(|| w.state.hidden(&p("f001"), &o));
        assert_eq!(c.visits, 2, "{n} fields");
    }
}

#[test]
fn a_local_write_is_linear_in_the_cells() {
    // the visits of a write at n fields are a n + b: doubling the document doubles the
    // difference, where the pass per path of the model would make them grow as n squared
    let on = |n: usize, write: &dyn Fn(&mut Writer)| {
        let mut w = wide(n);
        w.write_inc(&p("c1"), 1, 0).unwrap();
        count(|| write(&mut w)).1.visits
    };
    let ops: [(&str, &dyn Fn(&mut Writer)); 4] = [
        ("$set", &|w| drop(w.write_set(&p("f001"), s("z"), 0).unwrap())),
        ("$inc", &|w| drop(w.write_inc(&p("c1"), 1, 0).unwrap())),
        ("$unset", &|w| drop(w.write_unset(&p("f001"), 0).unwrap())),
        ("replace", &|w| {
            let doc: Vec<(Path, Value)> = w
                .visible()
                .unwrap()
                .into_iter()
                .map(|(k, v)| if k == p("f002") { (k, s("other")) } else { (k, v) })
                .collect();
            drop(w.write_replace(&doc, 0).unwrap());
        }),
    ];
    for (name, write) in ops {
        let (v100, v200, v400) = (on(100, write), on(200, write), on(400, write));
        assert_eq!(v400 - v200, 2 * (v200 - v100), "{name}: {v100} {v200} {v400}");
    }
}
