//! Tests of the reads of a state by stamp ([`MergeState::alive_lives`], [`MergeState::write_stamps`]).

use std::collections::BTreeSet;

use super::prop_tests::opts;
use super::tests::{fl, i, p, wid};
use super::*;

fn writer(c: char, o: &JoinOptions) -> Writer {
    Writer::new(wid(c, 0), o.clone())
}

#[test]
fn a_life_the_state_marks_dead_is_not_an_alive_life() {
    // A made X, deleted it and made it again: the state holds two lives, the first dead.
    let o = opts(1_000_000_000);
    let mut a = writer('A', &o);
    let dead = a.insert(&fl(&[("x", i(1))]), 0).unwrap().stamp;
    a.write_delete(0).unwrap();
    let alive = a.insert(&fl(&[("x", i(2))]), 0).unwrap().stamp;
    assert!(a.state.dead.contains(&dead), "premise: the life A deleted is dead");
    assert_eq!(
        a.state.alive_lives().collect::<Vec<_>>(),
        vec![alive],
        "only the life that is not dead is alive"
    );
}

#[test]
fn a_state_with_nothing_has_no_alive_life() {
    assert_eq!(MergeState::default().alive_lives().count(), 0);
}

#[test]
fn a_state_names_the_stamp_of_every_write_it_records() {
    let o = opts(1_000_000_000);
    let mut a = writer('A', &o);
    let insert = a.insert(&fl(&[("x", i(1))]), 0).unwrap().stamp;
    let set = a.write_set(&p("y"), i(2), 0).unwrap().stamp;
    let inc = a.write_inc(&p("n"), 3, 0).unwrap().stamp;
    assert_eq!(
        a.state.write_stamps().collect::<BTreeSet<_>>(),
        BTreeSet::from([insert, set, inc]),
        "a life, a register and a counter slot each name their write"
    );
    // A delete is a write the state records; the cells of the life it killed are dropped, the life stays.
    let delete = a.write_delete(0).unwrap().stamp;
    assert_eq!(
        a.state.write_stamps().collect::<BTreeSet<_>>(),
        BTreeSet::from([insert, delete]),
        "a delete names its write, and the life it killed keeps its own"
    );
}

#[test]
fn the_version_vector_is_not_a_write_the_state_holds() {
    let o = opts(1_000_000_000);
    let mut a = writer('A', &o);
    let first = a.insert(&fl(&[("x", i(1))]), 0).unwrap().stamp;
    let mut st = a.state.clone();
    assert!(st.drop_at_or_below(first.hlc));
    assert!(!st.vv.is_empty(), "premise: the state still says what it has seen");
    assert_eq!(st.write_stamps().count(), 0, "and holds no write");
}
