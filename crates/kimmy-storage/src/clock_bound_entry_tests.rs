//! Step 3, package 3.0 (U1): an entry or state of a `field` collection whose clock is more than
//! `max_clock_skew_secs` ahead of this member's physical clock is refused until due, by name, not
//! applied and not witnessed; the window ends at it and it applies once it is due.
//!
//! The receiver's clock is moved back (`set_clock_offset_for_tests`, per engine) so that entries made
//! from the real clock are far-future to it, and forward again to make them due: no test waits.

use std::time::Duration;

use bson::doc;
use kimmy_core::{DocId, Epoch, Hlc, IndexField, MergePolicy, OplogEntry, Origin, Stamp};
use redb::{ReadableDatabase, ReadableTableMetadata};

use crate::{ClockRefusal, Engine, StorageError, WalkScope, physical_now_ms, tables};

const SKEW_MS: u64 = 300_000;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

/// A donor with a collection `shop.orders` of the given policy and a receiver that has taken its creation.
struct Fixture {
    a: Engine,
    b: Engine,
    ca: crate::meta::CollectionMeta,
    cb: crate::meta::CollectionMeta,
    creation: Vec<OplogEntry>,
    cut: Stamp,
    _dirs: [tempfile::TempDir; 2],
}

fn fixture(policy: MergePolicy) -> Fixture {
    fixture_with(policy, false)
}

/// With an index on `n`, so that a write that reaches the record also reaches the index.
fn fixture_with(policy: MergePolicy, indexed: bool) -> Fixture {
    let (a, da) = engine();
    let (b, db) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", policy, None).unwrap();
    if indexed {
        a.create_index("shop", "orders", vec![IndexField::ascending("n")], false, None).unwrap();
    }
    let creation = everything(&a);
    b.apply_batch(&creation).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    let cut = creation.last().unwrap().stamp;
    Fixture { a, b, ca, cb, creation, cut, _dirs: [da, db] }
}

impl Fixture {
    /// The entries A has written since the creation, in stamp order.
    fn written(&self) -> Vec<OplogEntry> {
        everything(&self.a).into_iter().filter(|e| e.stamp > self.cut).collect()
    }

    /// One more document written on A, and its entry.
    fn write(&self, id: i64) -> OplogEntry {
        std::thread::sleep(Duration::from_millis(3));
        self.a.insert(&self.ca, doc! { "_id": id, "n": id }).unwrap();
        self.written().pop().unwrap()
    }

    /// B's clock as the check reads it, moved back far enough that everything A writes now is
    /// `by_ms` past the bound.
    fn make_far_future(&self, by_ms: u64) {
        self.b.set_clock_offset_for_tests(-((SKEW_MS + by_ms) as i64));
    }

    fn witnessed(&self, entry: &OplogEntry) -> Hlc {
        self.b.witnessed_vector().unwrap().get(entry.stamp.origin())
    }
}

fn index_entries(engine: &Engine) -> usize {
    let txn = engine.db().begin_read().unwrap();
    // The count the table keeps in its root: no walk of the store.
    usize::try_from(txn.open_table(tables::INDEX_ENTRIES).unwrap().len().unwrap()).unwrap()
}

fn far(wall_ms: u64, like: &Stamp) -> Stamp {
    Stamp::new(Hlc::new(wall_ms, 0), like.node, like.epoch)
}

#[test]
fn a_field_entry_whose_state_is_past_the_bound_is_refused_by_name_and_not_witnessed() {
    let f = fixture(MergePolicy::Field);
    let insert = f.write(1);
    f.make_far_future(10_000);
    let before = f.witnessed(&insert);

    let outcome = f.b.apply_batch(std::slice::from_ref(&insert)).unwrap();

    let stop = outcome
        .clock_refused_at
        .as_ref()
        .expect("refused by name: vv, but the entry was not refused");
    assert_eq!(stop.reason, ClockRefusal::Vv, "refused by name: vv");
    assert_eq!(stop.stamp, insert.stamp, "the stop names the entry");
    assert!((9_000..=10_000).contains(&stop.due_in_ms), "due in about ten seconds: {stop:?}");
    assert_eq!((outcome.clock_refused, outcome.applied), (1, 0), "{outcome:?}");
    assert_eq!(f.b.get(&f.cb, &DocId::Int64(1)).unwrap(), None, "nothing was written");
    assert_eq!(f.witnessed(&insert), before, "not witnessed: the vector did not move");
}

#[test]
fn a_field_entry_whose_stamp_is_past_the_bound_is_refused_by_name() {
    let f = fixture(MergePolicy::Field);
    let mut insert = f.write(1);
    // The state it carries is honest; only the stamp is from the future.
    insert.stamp = far(physical_now_ms() + 3_600_000, &insert.stamp);

    let outcome = f.b.apply_batch(std::slice::from_ref(&insert)).unwrap();

    let stop = outcome
        .clock_refused_at
        .as_ref()
        .expect("refused by name: stamp, but the entry was not refused");
    assert_eq!(stop.reason, ClockRefusal::Stamp, "refused by name: stamp");
    assert_eq!(outcome.applied, 0);
}

#[test]
fn an_entry_whose_epoch_starts_past_the_bound_is_refused_by_name() {
    let f = fixture(MergePolicy::Field);
    let mut insert = f.write(1);
    let minted = Epoch::new(Hlc::new(physical_now_ms() + 3_600_000, 0), [5; 8]);
    insert.stamp = Stamp::new(insert.stamp.hlc, insert.stamp.node, minted);

    let outcome = f.b.apply_batch(std::slice::from_ref(&insert)).unwrap();

    let stop = outcome
        .clock_refused_at
        .as_ref()
        .expect("refused by name: epoch, but the entry was not refused");
    assert_eq!(stop.reason, ClockRefusal::Epoch, "refused by name: epoch");
}

#[test]
fn the_bound_applies_inside_and_refuses_past_it() {
    let f = fixture(MergePolicy::Field);
    let inside = f.write(1);
    // Two seconds inside the bound, then two seconds past it. Exactness at the edge is the unit test's: the
    // clock moves while this runs.
    f.b.set_clock_offset_for_tests(-((SKEW_MS - 2_000) as i64));
    let outcome = f.b.apply_batch(std::slice::from_ref(&inside)).unwrap();
    assert_eq!((outcome.applied, outcome.clock_refused), (1, 0), "within the bound applies");

    let past = f.write(2);
    f.b.set_clock_offset_for_tests(-((SKEW_MS + 2_000) as i64));
    let outcome = f.b.apply_batch(std::slice::from_ref(&past)).unwrap();
    assert_eq!(outcome.clock_refused, 1, "past the bound is refused: {outcome:?}");
}

#[test]
fn a_refused_entry_applies_once_it_is_due() {
    let f = fixture(MergePolicy::Field);
    let insert = f.write(1);
    f.make_far_future(10_000);
    let refused = f.b.apply_batch(std::slice::from_ref(&insert)).unwrap();
    assert_eq!(refused.clock_refused, 1, "applies once due: it was not refused first: {refused:?}");

    // The receiver's clock catches up; the same entry is served again.
    f.b.set_clock_offset_for_tests(0);
    let outcome = f.b.apply_batch(std::slice::from_ref(&insert)).unwrap();

    assert_eq!((outcome.applied, outcome.clock_refused), (1, 0), "applies once due: {outcome:?}");
    assert_eq!(f.b.get(&f.cb, &DocId::Int64(1)).unwrap(), Some(doc! { "_id": 1i64, "n": 1i64 }));
    assert_eq!(f.witnessed(&insert), insert.stamp.hlc, "and is witnessed then");
}

#[test]
fn the_window_stops_at_the_refused_entry_and_the_witnessed_vector_stays_below_it() {
    let f = fixture_with(MergePolicy::Field, true);
    let first = f.write(1);
    let second = f.write(2);
    let third = f.write(3);
    // The second is from the future; the first and third are honest. The window is in stamp order, so the
    // third is stamped after the second: it must wait with it.
    let mut second_far = second.clone();
    second_far.stamp = far(physical_now_ms() + 3_600_000, &second.stamp);
    let mut third_after = third.clone();
    third_after.stamp = far(second_far.stamp.hlc.wall_ms + 1, &third.stamp);

    let outcome =
        f.b.apply_batch(&[first.clone(), second_far.clone(), third_after.clone()]).unwrap();

    assert_eq!(outcome.applied, 1, "the entry below the refused one applied: {outcome:?}");
    assert_eq!(f.b.get(&f.cb, &DocId::Int64(1)).unwrap(), Some(doc! { "_id": 1i64, "n": 1i64 }));
    assert_eq!(f.b.get(&f.cb, &DocId::Int64(3)).unwrap(), None, "the entry above it was left");
    assert_eq!(
        outcome.stopped_at(),
        Some(second_far.stamp),
        "the window stops at the refused entry, not past it"
    );
    let witnessed = f.b.witnessed_vector().unwrap().get(first.stamp.origin());
    assert_eq!(witnessed, first.stamp.hlc, "the witnessed vector stops before the refused stamp");

    // The refusal came before ANY write for the refused entry in the batch's shared transaction: the refused
    // document has no record, no index key and no digest term, exactly as on a member that applied the first
    // entry alone.
    assert_eq!(
        f.b.get(&f.cb, &DocId::Int64(2)).unwrap(),
        None,
        "no record for the refused document"
    );
    let (c, _dc) = engine();
    c.apply_batch(&f.creation).unwrap();
    c.apply_batch(std::slice::from_ref(&first)).unwrap();
    assert_eq!(
        index_entries(&f.b),
        index_entries(&c),
        "the index has no key for the refused document"
    );
    assert_eq!(
        f.b.digest_row(f.cb.id).unwrap(),
        c.digest_row(f.cb.id).unwrap(),
        "its digest term is unchanged: nothing of the refused entry reached the record"
    );
}

#[test]
fn a_clock_stopped_outcome_has_zero_total_and_is_stopped() {
    let f = fixture(MergePolicy::Field);
    let insert = f.write(1);
    f.make_far_future(10_000);

    let outcome = f.b.apply_batch(std::slice::from_ref(&insert)).unwrap();

    assert_eq!(outcome.total(), 0, "total() is 0: a stop decides nothing");
    assert!(outcome.stopped(), "and stopped() is true: it is what carries the stop");
    assert_eq!((outcome.unknown_collection, outcome.purge_pending), (0, 0));
}

#[test]
fn a_state_only_apply_of_a_far_future_state_writes_nothing() {
    let f = fixture(MergePolicy::Field);
    let insert = f.write(1);
    f.make_far_future(10_000);
    let digest_before = f.b.digest_row(f.cb.id).unwrap();

    let refused = f.b.apply_held_for_test(&f.cb, &insert);

    match refused {
        Err(StorageError::ClockRefused(stop)) => {
            assert_eq!(stop.reason, ClockRefusal::Vv, "state refused: vv");
        }
        other => panic!("state refused: vv, not {other:?}"),
    }
    assert_eq!(f.b.get(&f.cb, &DocId::Int64(1)).unwrap(), None, "no record, no digest move");
    assert_eq!(f.b.digest_row(f.cb.id).unwrap(), digest_before, "no record, no digest move");
}

#[test]
fn the_refusal_is_counted_per_origin_and_capped_at_64() {
    let f = fixture(MergePolicy::Field);
    let insert = f.write(1);
    f.make_far_future(10_000);
    f.b.apply_batch(std::slice::from_ref(&insert)).unwrap();

    let counts = f.b.clock_refusals();
    assert_eq!(counts, vec![(insert.stamp.node.to_string(), 1)], "counted per origin");

    for n in 0..70u8 {
        let origin =
            Origin { node: kimmy_core::NodeId::from_bytes([n + 1; 16]), epoch: Epoch::ZERO };
        f.b.count_clock_refused(&origin, 1);
    }
    let counts = f.b.clock_refusals();
    assert_eq!(counts.len(), 65, "64 origins named, the rest under one label");
    assert_eq!(counts.last().unwrap().0, "other");
}

/// Not failing-first: it is green over the parent too. The guard that U1 bounds `field` collections only.
#[test]
fn a_document_collection_is_not_bounded() {
    let f = fixture(MergePolicy::Document);
    let mut insert = f.write(1);
    insert.stamp = far(physical_now_ms() + 3_600_000, &insert.stamp);

    let outcome = f.b.apply_batch(std::slice::from_ref(&insert)).unwrap();

    assert_eq!(
        (outcome.applied, outcome.clock_refused),
        (1, 0),
        "a document collection is not bounded by U1"
    );
}
