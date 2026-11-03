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
