//! The state of a document at a point in time ([`state_at`]): the fold of the states its entries carry,
//! in ascending stamp order, through the incarnation floor.

use super::prop_tests::opts;
use super::tests::{fl, i, p};
use super::*;
use crate::hlc::Hlc;

use super::tests::wid;

const R: u64 = 1_000_000_000;

fn writer(c: char) -> Writer {
    Writer::new(wid(c, 0), opts(R))
}

type Entries = Vec<(MStamp, MergeState)>;

/// The entry a write leaves in the oplog: its stamp and the writer's whole state after it.
fn entry(entries: &mut Entries, w: &Writer, written: Option<Written>) -> MStamp {
    let stamp = written.expect("the write is made").stamp;
    entries.push((stamp, w.state.clone()));
    stamp
}

fn fold(entries: &Entries, target: Hlc, floor: Option<Hlc>) -> MergeState {
    state_at(entries.iter().map(|(stamp, meta)| (*stamp, meta)), target, floor, &opts(R))
}

fn read(state: &MergeState) -> Option<std::collections::BTreeMap<Path, Value>> {
    state.visible(&opts(R))
}

#[test]
fn one_writers_state_at_each_of_its_writes_is_the_state_that_write_left() {
    let mut a = writer('A');
    let mut entries = Entries::new();
    let w1 = a.insert(&fl(&[("x", i(1))]), 0);
    entry(&mut entries, &a, w1);
    let w2 = a.write_set(&p("y"), i(2), 0);
    entry(&mut entries, &a, w2);
    let w3 = a.write_set(&p("x"), i(3), 0);
    entry(&mut entries, &a, w3);

    for (stamp, meta) in &entries {
        assert_eq!(&fold(&entries, stamp.hlc, None), meta, "the state at {stamp:?}");
    }
    let first = fold(&entries, entries[0].0.hlc, None);
    assert_eq!(read(&first).unwrap().get(&p("x")), Some(&i(1)), "a later write is not in it");
    assert_eq!(read(&first).unwrap().get(&p("y")), None);
}

/// Two writers that write concurrently, and receive each other's state between some of the writes.
fn two_writers() -> Entries {
    let (mut a, mut b) = (writer('A'), writer('B'));
    let mut entries = Entries::new();
    let w = a.insert(&fl(&[("x", i(1)), ("n", i(0))]), 0);
    entry(&mut entries, &a, w);
    b.receive(&a.state);
    let w = a.write_set(&p("x"), i(10), 0);
    entry(&mut entries, &a, w);
    let w = b.write_set(&p("x"), i(20), 3);
    entry(&mut entries, &b, w);
    let w = b.write_inc(&p("n"), 4, 3);
    entry(&mut entries, &b, w);
    a.receive(&b.state);
    let w = a.write_inc(&p("n"), 1, 0);
    entry(&mut entries, &a, w);
    let w = b.write_set(&p("z"), i(7), 0);
    entry(&mut entries, &b, w);
    entries
}

#[test]
fn the_state_does_not_depend_on_the_order_the_entries_are_given_in() {
    let entries = two_writers();
    let last = entries.iter().map(|(s, _)| s.hlc).max().unwrap();
    for target in entries.iter().map(|(s, _)| s.hlc).chain([last]) {
        let canonical = fold(&entries, target, None);
        let mut reversed = entries.clone();
        reversed.reverse();
        assert_eq!(fold(&reversed, target, None), canonical, "reversed, at {target:?}");
        let mut rotated = entries.clone();
        rotated.rotate_left(2);
        assert_eq!(fold(&rotated, target, None), canonical, "rotated, at {target:?}");
        let mut doubled = entries.clone();
        doubled.extend(entries.clone());
        assert_eq!(fold(&doubled, target, None), canonical, "every entry listed twice");
    }
}

#[test]
fn nothing_of_the_state_is_stamped_above_the_target() {
    let entries = two_writers();
    for target in entries.iter().map(|(s, _)| s.hlc) {
        let state = fold(&entries, target, None);
        assert!(
            state.write_stamps().all(|stamp| stamp.hlc <= target),
            "a cell above {target:?}: {:?}",
            state.write_stamps().collect::<Vec<_>>()
        );
        assert!(state.vv.values().all(|seen| *seen <= target), "a vv entry above {target:?}");
    }
}

#[test]
fn a_counter_has_each_writers_total_as_of_the_target() {
    let (mut a, mut b) = (writer('A'), writer('B'));
    let mut entries = Entries::new();
    let w = a.insert(&fl(&[("n", i(0))]), 0);
    let inserted = entry(&mut entries, &a, w);
    b.receive(&a.state);
    let w = a.write_inc(&p("n"), 5, 0);
    let a5 = entry(&mut entries, &a, w);
    let w = b.write_inc(&p("n"), 3, 2);
    let b3 = entry(&mut entries, &b, w);
    let w = a.write_inc(&p("n"), 2, 4);
    let a2 = entry(&mut entries, &a, w);

    let n = |target: Hlc| read(&fold(&entries, target, None)).unwrap().get(&p("n")).cloned();
    assert_eq!(n(inserted.hlc), Some(i(0)));
    assert_eq!(n(a5.hlc.min(b3.hlc)), Some(i(if a5 < b3 { 5 } else { 3 })), "only the earlier");
    assert_eq!(n(a5.hlc.max(b3.hlc)), Some(i(8)), "both writers' totals");
    assert_eq!(n(a2.hlc), Some(i(10)), "and A's later total replaces its earlier one");
}

#[test]
fn a_document_is_alive_when_a_life_is_and_not_after_its_delete() {
    let mut a = writer('A');
    let mut entries = Entries::new();
    let w = a.insert(&fl(&[("x", i(1))]), 0);
    let born = entry(&mut entries, &a, w);
    let w = a.write_delete(0);
    let deleted = entry(&mut entries, &a, w);
    let w = a.insert(&fl(&[("y", i(2))]), 0);
    let reborn = entry(&mut entries, &a, w);

    let at = |target: Hlc| read(&fold(&entries, target, None));
    assert_eq!(at(born.hlc).unwrap().get(&p("x")), Some(&i(1)));
    assert_eq!(at(deleted.hlc), None, "deleted at the delete");
    let again = at(reborn.hlc).unwrap();
    assert_eq!(again.get(&p("y")), Some(&i(2)), "the new life");
    assert_eq!(again.get(&p("x")), None, "and not the one the delete killed");
}

#[test]
fn the_floor_drops_what_an_entry_carries_of_a_life_at_or_below_it() {
    let mut a = writer('A');
    let mut entries = Entries::new();
    let w = a.insert(&fl(&[("x", i(1))]), 0);
    let first = entry(&mut entries, &a, w);
    let w = a.write_set(&p("y"), i(2), 0);
    let second = entry(&mut entries, &a, w);

    let floored = fold(&entries, second.hlc, Some(first.hlc));

    let seen = read(&floored).expect("the life a write after the floor reached stays");
    assert_eq!(seen.get(&p("y")), Some(&i(2)));
    assert_eq!(seen.get(&p("x")), None, "a cell at or below the floor is not in the state");
    assert_eq!(read(&fold(&entries, second.hlc, None)).unwrap().get(&p("x")), Some(&i(1)));
}

#[test]
fn no_entry_at_or_below_the_target_is_the_empty_state() {
    let mut a = writer('A');
    let mut entries = Entries::new();
    let w = a.insert(&fl(&[("x", i(1))]), 0);
    entry(&mut entries, &a, w);
    assert_eq!(fold(&entries, Hlc::ZERO, None), MergeState::default());
    assert_eq!(fold(&Entries::new(), Hlc::MAX, None), MergeState::default());
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "not causally closed")]
fn an_entry_the_fold_already_covers_is_a_history_that_is_not_causally_closed() {
    let mut a = writer('A');
    let mut entries = Entries::new();
    let w = a.insert(&fl(&[("x", i(1))]), 0);
    let first = entry(&mut entries, &a, w);
    // The same state again under a later stamp: its writer could not have seen that stamp.
    let later = MStamp { hlc: Hlc::new(first.hlc.wall_ms + 5, 0), writer: first.writer };
    entries.push((later, entries[0].1.clone()));
    fold(&entries, Hlc::MAX, None);
}

/// A history whose spread passes the retention. Under `retention` 6, A inserts `x` (wall 1), deletes the document
/// (wall 2) and inserts `y` far later (wall 43). The last entry's state has dropped the dead life of `x` and its
/// delete, since both are older than the horizon, and the first entry's state still lists the life as alive.
fn spread_past_the_retention(retention: u64) -> Entries {
    let mut a = Writer::new(wid('A', 0), opts(retention));
    let mut entries = Entries::new();
    let w = a.insert(&fl(&[("x", i(1))]), 0);
    entry(&mut entries, &a, w);
    let w = a.write_delete(0);
    entry(&mut entries, &a, w);
    let w = a.insert(&fl(&[("y", i(2))]), 40);
    entry(&mut entries, &a, w);
    assert!(
        entries[2].0.hlc.wall_ms - entries[0].0.hlc.wall_ms > retention,
        "the history spreads past R"
    );
    entries
}

/// The fold in ascending stamp order with a plain `join`, which is the definition `state_at` follows.
fn ascending_fold(entries: &Entries, o: &JoinOptions) -> MergeState {
    let mut ascending = entries.clone();
    ascending.sort_by_key(|(stamp, _)| *stamp);
    ascending.iter().fold(MergeState::default(), |acc, (_, state)| join(&acc, state, o).0)
}

/// The join is not associative once the spread passes `R`, so what `state_at` returns must not follow the order the
/// entries are given in: an early state that still lists a life the horizon dropped, joined late, brings the life back.
#[test]
fn the_state_does_not_depend_on_the_order_when_the_history_spreads_past_the_retention() {
    let o = opts(6);
    let entries = spread_past_the_retention(6);
    let last = entries.iter().map(|(s, _)| s.hlc).max().unwrap();
    let canonical = ascending_fold(&entries, &o);
    assert_eq!(
        read(&canonical).unwrap().into_iter().collect::<Vec<_>>(),
        vec![(p("y"), i(2))],
        "in ascending order the life of `x` is gone and only `y` is there"
    );

    let mut reversed = entries.clone();
    reversed.reverse();
    let mut rotated_once = entries.clone();
    rotated_once.rotate_left(1);
    let mut rotated_twice = entries.clone();
    rotated_twice.rotate_left(2);
    for (name, given) in
        [("reversed", reversed), ("rotated by 1", rotated_once), ("rotated by 2", rotated_twice)]
    {
        // The control: a plain join in this arrival order gives another state, the dropped life returns. Without
        // it a fold that ignored the order could not be told from one that kept it.
        let arrival =
            given.iter().fold(MergeState::default(), |acc, (_, state)| join(&acc, state, &o).0);
        assert_ne!(
            arrival, canonical,
            "the control: a plain join of the entries {name} must differ"
        );
        assert_eq!(
            read(&arrival).unwrap().keys().cloned().collect::<Vec<_>>(),
            vec![p("x"), p("y")],
            "the control: the life of `x` returns when it is joined {name}"
        );

        let at = state_at(given.iter().map(|(stamp, state)| (*stamp, state)), last, None, &o);
        assert_eq!(at, canonical, "state_at over the entries {name} is the ascending-order fold");
    }
}
