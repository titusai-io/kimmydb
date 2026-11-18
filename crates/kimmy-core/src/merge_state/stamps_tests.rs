//! Tests of the reads of a state by stamp ([`MergeState::alive_lives`]).

use super::prop_tests::opts;
use super::tests::{fl, i, wid};
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
