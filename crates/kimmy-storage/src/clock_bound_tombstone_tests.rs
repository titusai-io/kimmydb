//! The clock bound covers the entries judged history as well as the ones applied (ADR-272, decision 14, ruling (c); U1).
//!
//! `judge_document` answers an entry addressed to a collection this member holds only a tombstone for with `Taken`, so that waiting for a
//! collection that never comes does not stop the batch. A taken entry is witnessed into the batch's vector, and the logical clock has no
//! clamp: the bound at the top of `apply_remote_in_txn` is never reached on this path. An entry a day ahead, for a collection this member
//! dropped, was witnessed durably (`OPLOG_WITNESSED`), and the next open resumed its logical clock from it.

use bson::doc;
use kimmy_core::{Hlc, MergePolicy, NodeId, OplogEntry, Stamp};

use crate::{ClockRefusal, Engine, WalkScope, physical_now_ms};

const SKEW_MS: u64 = 300_000;
const A_DAY_MS: u64 = 86_400_000;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

/// A write of `shop.notes` that a member whose clock is a day ahead made, and a member that created that collection and dropped it
/// (it holds only the tombstone).
fn a_dropped_collection_and_a_far_entry_for_it() -> (Engine, tempfile::TempDir, OplogEntry) {
    let (m, dir) = engine();
    m.create_collection_with_policy("shop", "notes", MergePolicy::Document, None).unwrap();
    assert!(m.drop_collection("shop", "notes").unwrap(), "premise: the collection is dropped");
    let (x, _dx) = engine();
    x.create_collection_with_policy("shop", "notes", MergePolicy::Document, None).unwrap();
    let notes = x.get_collection("shop", "notes").unwrap();
    x.insert(&notes, doc! { "_id": 1, "text": "from a clock a day ahead" }).unwrap();
    let mut entry = x
        .entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background)
        .unwrap()
        .entries
        .pop()
        .expect("the insert");
    entry.stamp = Stamp::new(
        Hlc::new(physical_now_ms() + A_DAY_MS, 0),
        NodeId::from_bytes([9; 16]),
        entry.stamp.epoch,
    );
    (m, dir, entry)
}

#[test]
fn a_far_future_entry_for_a_dropped_collection_is_refused_by_name_and_not_witnessed() {
    let (m, _d, entry) = a_dropped_collection_and_a_far_entry_for_it();
    let outcome = m.apply_batch(std::slice::from_ref(&entry)).unwrap();
    let stop = outcome.clock_refused_at.as_ref().expect(
        "an entry a day ahead is refused by name, whatever collection it is addressed to: stamp",
    );
    assert_eq!(stop.reason, ClockRefusal::Stamp);
    assert_eq!(outcome.clock_refused, 1, "{outcome:?}");
    assert_eq!(
        m.witnessed_vector().unwrap().get(entry.stamp.origin()),
        Hlc::ZERO,
        "not witnessed: a taken entry is observed into the vector, and the clock reads it"
    );
    let lead = m.clock_last().wall_ms.saturating_sub(physical_now_ms());
    assert!(
        lead <= SKEW_MS,
        "the logical clock leads the physical clock by {lead} ms, more than the bound"
    );
}

#[test]
fn the_clock_is_unchanged_after_a_reopen_by_a_far_future_entry_for_a_dropped_collection() {
    let (m, d, entry) = a_dropped_collection_and_a_far_entry_for_it();
    m.apply_batch(std::slice::from_ref(&entry)).unwrap();
    assert!(m.close().is_ok(), "the engine closed");
    let reopened = Engine::open(&d.path().join("kimmy.redb")).unwrap();
    let lead = reopened.clock_last().wall_ms.saturating_sub(physical_now_ms());
    assert!(
        lead <= SKEW_MS,
        "after a restart the logical clock leads the physical clock by {lead} ms: resumed from a witnessed far-future stamp"
    );
}

/// Unit P: an honest entry stamped ABOVE the tombstone of a collection this member kept the row of is a fact above its floor, so it makes the
/// collection again and is applied (it was judged history and not applied before); the bound is the same, and it is witnessed.
#[test]
fn an_honest_entry_above_a_dropped_collections_tombstone_makes_it_again_and_is_taken() {
    let (m, _d, mut entry) = a_dropped_collection_and_a_far_entry_for_it();
    entry.stamp = Stamp::new(
        Hlc::new(physical_now_ms() + 1_000, 0),
        NodeId::from_bytes([9; 16]),
        entry.stamp.epoch,
    );
    let outcome = m.apply_batch(std::slice::from_ref(&entry)).unwrap();
    assert_eq!((outcome.clock_refused, outcome.applied), (0, 1), "{outcome:?}");
    assert!(
        m.witnessed_vector().unwrap().get(entry.stamp.origin()) > Hlc::ZERO,
        "a write inside the bound that made the collection again is taken and witnessed"
    );
}
