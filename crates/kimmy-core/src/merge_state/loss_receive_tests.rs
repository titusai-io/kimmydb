//! `MergeState::receive_loss`: a loss-record entry joined into a state, the rule `Writer::receive_loss` applies
//! and the rewind's fold applies at an entry's key position, with no writer to hold it.

use super::counter::Num;
use super::prop_tests::opts;
use super::state::MAX_TOTAL;
use super::tests::{fl, h, i, p, wid};
use super::*;

const R: u64 = 10;

fn key(field: &str, wall: u64) -> LossKey {
    LossKey::field(LossKind::Clash, p(field), MStamp { hlc: h(wall), writer: wid('A', 0) })
}

fn value(n: i64) -> LossValue {
    LossValue::Value(i(n))
}

/// A state whose newest write is at `newest`, so its horizon is `newest - R`.
fn state_at(newest: u64) -> MergeState {
    let mut state = MergeState::default();
    state.vv.insert(wid('B', 0), h(newest));
    state
}

#[test]
fn a_new_record_is_kept_and_the_version_vector_does_not_move() {
    let mut state = state_at(100);
    let vv = state.vv.clone();

    assert!(state.receive_loss(key("x", 95), value(1), &opts(R)));

    assert_eq!(state.losses.get(&key("x", 95)), Some(&value(1)));
    assert_eq!(state.vv, vv, "a loss entry names a write the vector already covers, or none");
}

#[test]
fn a_known_key_is_refused_and_keeps_its_value() {
    let mut state = state_at(100);
    assert!(state.receive_loss(key("x", 95), value(1), &opts(R)));

    assert!(!state.receive_loss(key("x", 95), value(2), &opts(R)), "a known key is refused");

    assert_eq!(
        state.losses.get(&key("x", 95)),
        Some(&value(1)),
        "the first to name a key keeps it"
    );
}

#[test]
fn a_record_below_the_horizon_is_refused_and_one_at_it_is_kept() {
    let mut state = state_at(100);
    let horizon = state.horizon(&opts(R));
    assert_eq!(horizon, h(90), "premise");

    assert!(
        !state.receive_loss(key("x", 89), value(1), &opts(R)),
        "a record below the horizon is refused"
    );
    assert!(
        state.receive_loss(key("y", 90), value(1), &opts(R)),
        "a record at the horizon is kept"
    );

    assert_eq!(state.losses.len(), 1);
}

#[test]
fn a_total_past_the_most_a_state_can_hold_is_refused_and_one_at_it_is_kept() {
    let mut state = state_at(100);

    let total = |n: i128| LossValue::Total(Num::Int64(n));
    assert!(
        !state.receive_loss(key("x", 95), total(MAX_TOTAL + 1), &opts(R)),
        "a total past the bound is refused"
    );
    assert!(
        !state.receive_loss(key("x", 95), total(-MAX_TOTAL - 1), &opts(R)),
        "a total past the bound is refused"
    );
    assert!(
        state.receive_loss(key("x", 95), total(MAX_TOTAL), &opts(R)),
        "a total at the bound is kept"
    );
}

#[test]
fn the_writer_and_the_state_apply_the_same_rule_to_the_same_entries() {
    // The writer's method delegates, so the two cannot drift: the same sequence of entries, in the same order, leaves
    // the same state and answers the same.
    let mut writer = Writer::new(wid('A', 0), opts(R));
    writer.insert(&fl(&[("x", i(1))]), 90).unwrap();
    let mut state = writer.state.clone();
    let newest = writer.state.max_vv().wall_ms;
    let entries = [
        (key("x", newest), value(1)),
        (key("x", newest), value(2)),
        (key("y", newest - R - 1), value(3)),
        (key("z", newest - R), value(4)),
        (key("w", newest), LossValue::Total(Num::Int64(MAX_TOTAL + 1))),
    ];
    for (k, v) in entries {
        let by_writer = writer.receive_loss(k.clone(), v.clone());
        let by_state = state.receive_loss(k, v, &opts(R));
        assert_eq!(by_writer, by_state, "the writer and the state answer alike");
    }
    assert_eq!(writer.state, state, "the writer and the state hold alike");
    assert_eq!(state.losses.len(), 2, "the two that are new, inside the horizon and in range");
}
