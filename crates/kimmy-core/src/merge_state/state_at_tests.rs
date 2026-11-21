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
    state_at(entries.iter().map(|(stamp, meta)| (*stamp, meta.into())), target, floor, &opts(R))
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

        let at =
            state_at(given.iter().map(|(stamp, state)| (*stamp, state.into())), last, None, &o);
        assert_eq!(at, canonical, "state_at over the entries {name} is the ascending-order fold");
    }
}

// ---- the absorbed pair belongs to its writing life (q7-absorbed) ------------------------------------------

/// The shape of rich seeds 943, 2570 and 13787 as entries: A upserts `+1` (life U); B inserts P (newer than U); A receives P and unsets
/// `c`, a base made into P that absorbs U's slot; C inserts Q, newer than P, which supersedes P. Returns the entries and the stamps of the
/// unset and of Q.
fn base_into_a_life_that_is_then_superseded() -> (Entries, MStamp, MStamp) {
    let (mut a, mut b, mut c) = (writer('A'), writer('B'), writer('C'));
    let mut entries = Entries::new();
    let w = a.upsert_inc(&p("c"), 1, 0);
    entry(&mut entries, &a, w);
    let w = b.insert(&fl(&[("k", i(1))]), 0);
    entry(&mut entries, &b, w);
    a.receive(&b.state);
    let w = a.write_unset(&p("c"), 0);
    let unset = entry(&mut entries, &a, w);
    let w = c.insert(&fl(&[("k", i(2))]), 100);
    let q = entry(&mut entries, &c, w);
    (entries, unset, q)
}

/// At the unset P is the newest alive life and the write is into it: `c` is gone. At Q's entry P is superseded and its cells are dropped
/// with the write: the upsert's increment stands, `c` reads 1 (the old keying read 0, the pair outliving P with U's slot).
#[test]
fn a_base_into_a_life_that_is_later_superseded_absorbs_nothing_at_the_later_target() {
    let (entries, unset, q) = base_into_a_life_that_is_then_superseded();
    let at_unset = read(&fold(&entries, unset.hlc, None)).unwrap();
    assert_eq!(at_unset.get(&p("c")), None, "P is alive and the unset is applied");
    assert_eq!(at_unset.get(&p("k")), Some(&i(1)));
    let at_q = read(&fold(&entries, q.hlc, None)).unwrap();
    assert_eq!(
        at_q.get(&p("c")),
        Some(&i(1)),
        "P is superseded: the unset into it is dropped, the increment stands"
    );
    assert_eq!(at_q.get(&p("k")), Some(&i(2)));
    let state = fold(&entries, q.hlc, None);
    let alive = state.alive(&opts(R));
    assert!(
        state.slots.values().all(|s| s.absorbed.iter().all(|pr| alive.contains(&pr.life))),
        "no pair of the life that is gone"
    );
}

/// The F4 history through the fold with the floor argument (`state_at` floors each entry first). The oplog holds a writer's state at its
/// WRITES only. Where an entry carries both the slot D moved above the floor and the pair of P that absorbed it (A writes after receiving
/// D's `+1`), the floor re-homes the pair onto U and `c` reads 1. Where no entry does (D's entry holds the slot and no pair; A's entry holds
/// the pair and a slot the floor drops with it), the fold reads 3: the same transient as a member that met only the thinned states, settled
/// by a repair that joins a state which holds both (note v2.1 section 3, route dependence). The two histories differ in one write by A.
fn f4_entries(a_writes_after_d: bool) -> (Entries, Hlc, MStamp) {
    let (mut a, mut b, mut d) = (writer('A'), writer('B'), writer('D'));
    let mut entries = Entries::new();
    let w = a.upsert_inc(&p("c"), 1, 0);
    entry(&mut entries, &a, w);
    d.receive(&a.state);
    let w = d.write_inc(&p("c"), 2, 0);
    entry(&mut entries, &d, w);
    a.receive(&d.state);
    let w = b.insert(&fl(&[("k", i(1))]), 1);
    entry(&mut entries, &b, w);
    a.receive(&b.state);
    let w = a.write_set(&p("c"), i(0), 0);
    let floor = entry(&mut entries, &a, w);
    let w = d.write_inc(&p("c"), 1, 1_000);
    let mut last = entry(&mut entries, &d, w);
    if a_writes_after_d {
        a.receive(&d.state);
        let w = a.write_set(&p("y"), i(1), 2_000);
        last = entry(&mut entries, &a, w);
    }
    (entries, floor.hlc, last)
}

#[test]
fn the_fold_with_a_floor_reads_the_counter_after_the_rehome_when_an_entry_carries_the_pair_and_the_slot()
 {
    let (entries, floor, last) = f4_entries(true);
    let got = read(&fold(&entries, last.hlc, Some(floor))).unwrap();
    assert_eq!(got.get(&p("c")), Some(&i(1)), "+1, +2, set 0, the floor, +1");
}

#[test]
fn the_fold_with_a_floor_reads_the_pre_floor_increments_back_when_no_entry_carries_both() {
    let (entries, floor, last) = f4_entries(false);
    let got = read(&fold(&entries, last.hlc, Some(floor))).unwrap();
    assert_eq!(
        got.get(&p("c")),
        Some(&i(3)),
        "the pair is in A's entry with a slot the floor drops, the slot above the floor is in D's entry without it"
    );
}

// ---- loss entries (C7: the rewind side of loss records) ------------------------------------------------------

/// The record of a clash at `field`, lost by a write of `A` stamped at `wall`.
fn lkey(field: &str, wall: u64) -> LossKey {
    LossKey::field(
        LossKind::Clash,
        p(field),
        MStamp { hlc: Hlc::new(wall, 0), writer: wid('A', 0) },
    )
}

fn lval(n: i64) -> LossValue {
    LossValue::Value(i(n))
}

/// What the oplog holds of one document: states and loss records, each at its own stamp.
type Items = Vec<(MStamp, Item)>;

#[derive(Clone)]
enum Item {
    Meta(MergeState),
    Loss(LossKey, LossValue),
}

fn items_of(entries: &Entries) -> Items {
    entries.iter().map(|(stamp, meta)| (*stamp, Item::Meta(meta.clone()))).collect()
}

/// A loss entry stamped just above `after` (by a member `L`, whose own writes are in no entry here).
fn loss_after(after: MStamp, key: LossKey, val: LossValue) -> (MStamp, Item) {
    let hlc = Hlc::new(after.hlc.wall_ms, after.hlc.counter + 1);
    (MStamp { hlc, writer: wid('L', 0) }, Item::Loss(key, val))
}

fn fold_items(items: &Items, target: Hlc, floor: Option<Hlc>, o: &JoinOptions) -> MergeState {
    state_at(
        items.iter().map(|(stamp, item)| {
            let entry = match item {
                Item::Meta(meta) => StateAtEntry::Meta(meta),
                Item::Loss(key, val) => StateAtEntry::Loss(key.clone(), val.clone()),
            };
            (*stamp, entry)
        }),
        target,
        floor,
        o,
    )
}

/// The live apply of the same entries in key order: a member that receives each state with `Writer::receive` and
/// each record with `Writer::receive_loss`, the member's own rules, with no knowledge of `state_at`.
fn live_apply(items: &Items, target: Hlc, o: &JoinOptions) -> MergeState {
    let mut sorted: Vec<&(MStamp, Item)> = items.iter().filter(|(s, _)| s.hlc <= target).collect();
    sorted.sort_by_key(|(stamp, item)| (*stamp, matches!(item, Item::Loss(..))));
    let mut member = Writer::new(wid('M', 0), o.clone());
    for (_, item) in sorted {
        match item {
            Item::Meta(meta) => {
                member.receive(meta);
            }
            Item::Loss(key, val) => {
                member.receive_loss(key.clone(), val.clone());
            }
        }
    }
    member.state
}

#[test]
fn a_loss_entry_is_applied_at_its_key_position_and_equals_the_live_apply_at_every_target() {
    let entries = two_writers();
    let mut items = items_of(&entries);
    // Records stamped between, at the end of and before the writes of the history.
    let stamps: Vec<MStamp> = entries.iter().map(|(s, _)| *s).collect();
    items.push(loss_after(stamps[1], lkey("x", 5), lval(1)));
    items.push(loss_after(stamps[3], lkey("n", 6), lval(2)));
    items.push(loss_after(*stamps.last().unwrap(), lkey("z", 7), lval(3)));
    let last = stamps.iter().map(|s| s.hlc).max().unwrap();
    let o = opts(R);
    let mut seen_a_record = false;
    for (stamp, _) in &items {
        for target in
            [Hlc::new(stamp.hlc.wall_ms, stamp.hlc.counter.saturating_sub(1)), stamp.hlc, last]
        {
            let folded = fold_items(&items, target, None, &o);
            assert_eq!(
                folded,
                live_apply(&items, target, &o),
                "state_at is the live apply, at {target:?}"
            );
            seen_a_record |= !folded.losses.is_empty();
        }
    }
    assert!(seen_a_record, "the premise: a record is in a state at some target");
    // The record of an entry above the target is not read, and the one at it is.
    let at_first = fold_items(&items, items[1].0.hlc, None, &o);
    assert!(at_first.losses.is_empty(), "no record entry is stamped at or below the second write");
    let (loss_stamp, _) = items.iter().find(|(_, item)| matches!(item, Item::Loss(..))).unwrap();
    let at_loss = fold_items(&items, loss_stamp.hlc, None, &o);
    assert_eq!(
        at_loss.losses.get(&lkey("x", 5)),
        Some(&lval(1)),
        "a record entry at the target is read"
    );
    assert!(!at_loss.losses.contains_key(&lkey("n", 6)), "and the one above it is not");
}

#[test]
fn a_state_and_a_loss_entry_that_share_a_stamp_are_both_read_whatever_order_they_are_given_in() {
    let entries = two_writers();
    let shared = entries[2].0;
    let o = opts(R);
    let mut items = items_of(&entries[..3].to_vec());
    items.push((shared, Item::Loss(lkey("x", 5), lval(9))));
    let forward = fold_items(&items, shared.hlc, None, &o);
    items.reverse();
    let backward = fold_items(&items, shared.hlc, None, &o);
    assert_eq!(forward, backward, "the order given does not matter");
    assert_eq!(
        forward.losses.get(&lkey("x", 5)),
        Some(&lval(9)),
        "the record is read beside the state of its stamp"
    );
    assert_eq!(
        forward.vv,
        fold(&entries[..3].to_vec(), shared.hlc, None).vv,
        "and so is the state"
    );
}

#[test]
fn a_record_relayed_by_several_members_is_applied_once() {
    let entries = two_writers();
    let o = opts(R);
    let stamps: Vec<MStamp> = entries.iter().map(|(s, _)| *s).collect();
    let mut once = items_of(&entries);
    once.push(loss_after(stamps[2], lkey("x", 5), lval(1)));
    // The same record under the stamp of a second member's entry, and the first entry listed twice.
    let mut thrice = once.clone();
    let (second, item) = loss_after(stamps[3], lkey("x", 5), lval(1));
    thrice.push((MStamp { writer: wid('K', 0), ..second }, item));
    thrice.push(once.last().unwrap().clone());
    let last = stamps.last().unwrap().hlc;
    assert_eq!(fold_items(&thrice, last, None, &o), fold_items(&once, last, None, &o));
    let derived = fold(&entries, last, None).losses.len();
    assert_eq!(
        fold_items(&once, last, None, &o).losses.len(),
        derived + 1,
        "the joins' own record, and the relayed one once"
    );
}

#[test]
fn a_record_at_or_below_the_floor_is_refused_as_the_live_apply_refuses_it() {
    let entries = two_writers();
    let o = opts(R);
    let mut items = items_of(&entries);
    items.push(loss_after(entries[1].0, lkey("x", 5), lval(1)));
    let last = entries.iter().map(|(s, _)| s.hlc).max().unwrap();
    assert!(fold_items(&items, last, Some(Hlc::new(4, 0)), &o).losses.contains_key(&lkey("x", 5)));
    assert!(
        fold_items(&items, last, Some(Hlc::new(5, 0)), &o).losses.is_empty(),
        "a record of a life the floor ended"
    );
    let mut state = MergeState::default();
    assert_eq!(
        state.receive_loss_floored(lkey("x", 5), lval(1), Some(Hlc::new(5, 0)), &o).0,
        LossReceive::Floor
    );
    assert!(state.losses.is_empty());
}

#[test]
fn a_record_that_meets_no_row_is_parked_and_applied_after_the_next_state_gives_one() {
    let entries = two_writers();
    let o = opts(R);
    let first = entries[0].0;
    // A record stamped below the first write of the document: no life and no delete to hold it yet.
    let early =
        MStamp { hlc: Hlc::new(first.hlc.wall_ms.saturating_sub(1), 0), writer: wid('L', 0) };
    let mut items = items_of(&entries);
    items.push((early, Item::Loss(lkey("x", 5), lval(1))));
    let last = entries.iter().map(|(s, _)| s.hlc).max().unwrap();
    let folded = fold_items(&items, last, None, &o);
    assert_eq!(folded.losses.get(&lkey("x", 5)), Some(&lval(1)), "applied once a row exists");
    assert_eq!(folded, live_apply(&items, last, &o), "as the live apply parks and folds it");
    // With no state at all, nothing is built from a record alone.
    let only = vec![(early, Item::Loss(lkey("x", 5), lval(1)))];
    assert_eq!(fold_items(&only, last, None, &o), MergeState::default());
}

#[test]
fn the_fold_keeps_the_greatest_k_records_of_the_options_it_is_given_and_not_of_the_defaults() {
    let entries = two_writers();
    let capped = opts(R).with_loss_limits(4, 64).unwrap();
    let default = opts(R);
    assert!(
        capped.loss_cap() < default.loss_cap(),
        "premise: the collection's K is below the join's default"
    );
    let mut items = items_of(&entries);
    let anchor = entries.last().unwrap().0;
    for n in 0..6u64 {
        let (stamp, item) = loss_after(anchor, lkey("x", 10 + n), lval(n as i64));
        items.push((
            MStamp { hlc: Hlc::new(stamp.hlc.wall_ms, stamp.hlc.counter + n as u16), ..stamp },
            item,
        ));
    }
    let last = items.iter().map(|(s, _)| s.hlc).max().unwrap();
    let with_cap = fold_items(&items, last, None, &capped);
    assert_eq!(with_cap.losses.len(), 4, "K greatest by rank stay");
    assert_eq!(with_cap, live_apply(&items, last, &capped));
    let derived = fold(&entries, last, None).losses.len();
    assert_eq!(
        fold_items(&items, last, None, &default).losses.len(),
        derived + 6,
        "the default K would keep the joins' own record and all six"
    );
}
