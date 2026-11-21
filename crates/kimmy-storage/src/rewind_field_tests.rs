//! The rewind of a `field` collection: a document's state at the target is the fold of the states its entries
//! carry (design q7-step3-rewind-design.md), not the post-image of one entry, so concurrent writes of different
//! fields are both put back; a document whose missing history may reach the target is refused, never guessed.

use std::time::Duration;

use bson::doc;
use kimmy_core::{DocId, Hlc, MergePolicy, OpKind, OplogEntry};

use crate::{CollectionMeta, Engine, RetentionPolicy, SnapshotProgress, WalkScope};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn later() {
    std::thread::sleep(Duration::from_millis(20));
}

fn entries(engine: &Engine) -> Vec<OplogEntry> {
    engine.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn newest(engine: &Engine) -> Hlc {
    entries(engine).iter().map(|entry| entry.stamp.hlc).max().unwrap()
}

fn field_collection(engine: &Engine) -> CollectionMeta {
    engine.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap()
}

/// A document as its fields by name: the order of a merged document's fields is the merge state's, not the test's.
type Fields = Option<std::collections::BTreeMap<String, bson::Bson>>;

fn fields(document: Option<bson::Document>) -> Fields {
    document.map(|document| document.into_iter().collect())
}

fn read(engine: &Engine, coll: &CollectionMeta, id: i64) -> Fields {
    fields(engine.get(coll, &DocId::Int64(id)).unwrap())
}

fn want(document: bson::Document) -> Fields {
    fields(Some(document))
}

/// A takes B's entries and B takes A's: both then hold every write.
fn exchange(a: &Engine, b: &Engine) {
    a.apply_batch(&entries(b)).unwrap();
    b.apply_batch(&entries(a)).unwrap();
}

#[test]
fn a_rewind_puts_back_both_fields_two_members_wrote_concurrently() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1 }).unwrap();
    later();
    b.apply_batch(&entries(&a)).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 1 }, false).unwrap();
    b.replace(&cb, &DocId::Int64(1), doc! { "_id": 1, "y": 2 }, false).unwrap();
    later();
    exchange(&a, &b);
    assert_eq!(read(&a, &ca, 1), want(doc! { "_id": 1, "x": 1, "y": 2 }), "premise: both fields");
    let target = newest(&a);
    later();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 9, "y": 2 }, false).unwrap();

    let outcome = a.rewind_to(target).unwrap();

    assert_eq!(outcome.reverted, 1);
    assert_eq!(
        read(&a, &ca, 1),
        want(doc! { "_id": 1, "x": 1, "y": 2 }),
        "the state at the target is the join of both members' writes, not one entry's image"
    );
}

#[test]
fn a_rewind_removes_a_field_document_created_after_the_target_and_leaves_no_tombstone() {
    let (a, _da) = engine();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1 }).unwrap();
    let target = newest(&a);
    later();
    a.insert(&ca, doc! { "_id": 2 }).unwrap();

    let outcome = a.rewind_to(target).unwrap();

    assert_eq!(outcome.removed, 1);
    assert_eq!(read(&a, &ca, 2), None);
    assert_eq!(
        crate::docs::record_stamp(&a, &ca, &DocId::Int64(2)).unwrap(),
        None,
        "no row, and no tombstone to outrank a write of it a peer holds"
    );
    assert!(read(&a, &ca, 1).is_some(), "a document that did not change is left alone");
}

#[test]
fn a_rewind_puts_back_a_field_document_a_later_delete_removed() {
    let (a, _da) = engine();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    let target = newest(&a);
    later();
    assert!(a.delete(&ca, &DocId::Int64(1)).unwrap());

    a.rewind_to(target).unwrap();

    assert_eq!(read(&a, &ca, 1), want(doc! { "_id": 1, "x": 1 }));
}

#[test]
fn a_document_deleted_at_the_target_stays_deleted_and_keeps_its_state() {
    let (a, _da) = engine();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1 }).unwrap();
    later();
    assert!(a.delete(&ca, &DocId::Int64(1)).unwrap());
    let target = newest(&a);
    later();
    a.insert(&ca, doc! { "_id": 1, "again": true }).unwrap();

    a.rewind_to(target).unwrap();

    assert_eq!(read(&a, &ca, 1), None, "deleted at the target");
    assert!(
        crate::docs::record_stamp(&a, &ca, &DocId::Int64(1)).unwrap().is_some(),
        "a tombstone that still carries the state, so the delete's context is kept"
    );
}

/// A inserts document 1, B edits it having seen A's creation, and A's own insert is then collected on A: the write
/// at the target that A no longer holds as an entry is covered by B's later state, which carries A's whole state.
#[test]
fn a_collected_write_that_a_retained_entry_covers_is_no_reason_to_refuse() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let ca = field_collection(&a);
    let first = a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    let inserted = entries(&a).into_iter().find(|e| e.kind == OpKind::Insert).unwrap();
    later();
    b.apply_batch(&entries(&a)).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    b.replace(&cb, &first, doc! { "_id": 1, "x": 1, "y": 2 }, false).unwrap();
    later();
    a.apply_batch(&entries(&b)).unwrap();
    a.collect_garbage_at(inserted.stamp.hlc.wall_ms + 10, RetentionPolicy::new(0, u64::MAX))
        .unwrap();
    assert!(
        !a.missing_through(&ca, &first).unwrap().is_empty(),
        "premise: A's insert was collected and is recorded"
    );
    let target = newest(&a);
    later();
    a.replace(&ca, &first, doc! { "_id": 1, "x": 9, "y": 2 }, false).unwrap();

    a.rewind_to(target).unwrap();

    assert_eq!(
        read(&a, &ca, 1),
        want(doc! { "_id": 1, "x": 1, "y": 2 }),
        "restored from B's state, which carries A's insert"
    );
}

#[test]
fn a_document_whose_write_at_the_target_was_collected_is_refused_and_nothing_is_written() {
    let (a, _da) = engine();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    let inserted = entries(&a).into_iter().find(|e| e.kind == OpKind::Insert).unwrap();
    later();
    a.insert(&ca, doc! { "_id": 2 }).unwrap();
    a.collect_garbage_at(inserted.stamp.hlc.wall_ms + 10, RetentionPolicy::new(0, u64::MAX))
        .unwrap();
    let target = newest(&a);
    later();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 9 }, false).unwrap();
    let before = entries(&a);

    let err = a.rewind_to(target).expect_err("the value at the target is not recoverable");

    assert!(err.to_string().contains("not in this member's oplog"), "{err}");
    assert_eq!(read(&a, &ca, 1), want(doc! { "_id": 1, "x": 9 }), "nothing was written");
    assert_eq!(entries(&a), before, "and no entry was discarded");
}

/// Only the check on the collected writes can refuse here: the document has an entry at the target (so there is
/// something to fold, and it is not a document held only as a state), and its state is of a life that stands. What it
/// lacks is B's write, collected, which the retained entry (written before A had seen it) does not cover.
#[test]
fn a_collected_write_of_another_writer_that_no_entry_covers_is_refused_and_nothing_is_written() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let ca = field_collection(&a);
    let id = a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    later();
    b.apply_batch(&entries(&a)).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    b.replace(&cb, &id, doc! { "_id": 1, "x": 1, "y": 2 }, false).unwrap();
    let written_by_b = entries(&b).into_iter().find(|e| e.stamp.node == b.node_id()).unwrap();
    later();
    // A writes again before it has seen B's write, so no state A's entries carry covers it.
    a.replace(&ca, &id, doc! { "_id": 1, "x": 5 }, false).unwrap();
    later();
    a.apply_batch(&entries(&b)).unwrap();
    a.collect_garbage_at(written_by_b.stamp.hlc.wall_ms + 10, RetentionPolicy::new(0, u64::MAX))
        .unwrap();
    let missing = a.missing_through(&ca, &id).unwrap();
    assert!(
        missing.iter().any(|(origin, _)| origin.node != a.node_id()),
        "premise: B's write was collected and is recorded: {missing:?}"
    );
    let target = newest(&a);
    assert!(
        entries(&a).iter().any(|e| e.kind == OpKind::Replace && e.stamp.hlc <= target),
        "premise: an entry of the document is retained at or below the target"
    );
    later();
    a.replace(&ca, &id, doc! { "_id": 1, "x": 9 }, false).unwrap();
    let before = entries(&a);

    let err = a.rewind_to(target).expect_err("B's write at the target is not recoverable");

    assert!(err.to_string().contains("not in this member's oplog"), "{err}");
    assert_eq!(
        read(&a, &ca, 1).unwrap().get("x"),
        Some(&bson::Bson::Int32(9)),
        "nothing was written"
    );
    assert_eq!(entries(&a), before, "and no entry was discarded");
}

#[test]
fn a_document_held_only_as_a_state_with_writes_above_the_target_is_refused() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1 }).unwrap();
    let page = a.snapshot_page(None, None).unwrap();
    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();

    let err = b.rewind_to(Hlc::new(1, 0)).expect_err("nothing here can undo a state");

    assert!(err.to_string().contains("only as a snapshot page's state"), "{err}");
    assert!(read(&b, &cb, 1).is_some(), "nothing was written");
}

/// The rewound state is the stored state in every table that follows a record: the counts and the cut digest (moved by
/// the one way a record is written) and an index, as for any write.
#[test]
fn the_counts_the_digest_and_the_indexes_follow_a_rewound_field_document() {
    use crate::live_count::test_support::{assert_counts_exact, assert_digests_exact};

    let (a, _da) = engine();
    let ca = field_collection(&a);
    a.create_index("shop", "orders", vec![kimmy_core::IndexField::ascending("x")], false, None)
        .unwrap();
    a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    let target = newest(&a);
    later();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 2 }, false).unwrap();
    a.insert(&ca, doc! { "_id": 2, "x": 3 }).unwrap();

    a.rewind_to(target).unwrap();

    assert_counts_exact(&a, "after the rewind");
    assert_digests_exact(&a, "after the rewind");
    let index_entries = {
        use redb::{ReadableDatabase, ReadableTableMetadata};
        let txn = a.db().begin_read().unwrap();
        txn.open_table(crate::tables::INDEX_ENTRIES).unwrap().len().unwrap()
    };
    assert_eq!(
        index_entries, 1,
        "one entry, for the document at the target: the future's are gone"
    );
}

/// The member that rewound mints a fresh epoch at its next open, above the one it had and inside the clock bound, and a
/// second rewind mints another above that (L26: restore twice).
#[test]
fn the_epoch_after_a_field_rewind_is_above_the_last_and_inside_the_clock_bound() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let first = {
        let a = Engine::open(&path).unwrap();
        let ca = field_collection(&a);
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        let target = newest(&a);
        later();
        a.insert(&ca, doc! { "_id": 2 }).unwrap();
        let before = a.epoch();
        a.rewind_to(target).unwrap();
        a.close().expect("closes");
        before
    };
    let second = {
        let a = Engine::open(&path).unwrap();
        let after = a.epoch();
        assert!(after > first, "a fresh epoch above the last: {first:?} then {after:?}");
        let gap = after.start_hlc().wall_ms.saturating_sub(crate::physical_now_ms());
        assert!(
            gap <= crate::DEFAULT_MAX_CLOCK_SKEW_MS,
            "the epoch starts inside the clock bound, or peers hold the origin until due: {gap} ms ahead"
        );
        let ca = a.get_collection("shop", "orders").unwrap();
        let target = newest(&a);
        later();
        a.insert(&ca, doc! { "_id": 3 }).unwrap();
        a.rewind_to(target).unwrap();
        a.close().expect("closes");
        after
    };
    let third = Engine::open(&path).unwrap().epoch();
    assert!(
        third > second,
        "and a second rewind mints another above that: {second:?} then {third:?}"
    );
}

#[test]
fn an_epoch_that_starts_past_the_default_clock_bound_is_named_and_one_inside_it_is_not() {
    use crate::engine::epoch_start_gap_ms;

    let now = 1_000_000_000;
    let start = |ahead: u64| Hlc::new(now + ahead, 0);
    assert_eq!(epoch_start_gap_ms(start(0), now), None);
    assert_eq!(
        epoch_start_gap_ms(start(crate::DEFAULT_MAX_CLOCK_SKEW_MS), now),
        None,
        "the bound itself"
    );
    assert_eq!(
        epoch_start_gap_ms(start(crate::DEFAULT_MAX_CLOCK_SKEW_MS + 1), now),
        Some(crate::DEFAULT_MAX_CLOCK_SKEW_MS + 1)
    );
    assert_eq!(epoch_start_gap_ms(Hlc::new(5, 0), now), None, "behind the clock is no gap");
}

/// A stream opened after a rewind reads the retained entries only: an event is the stored entry itself, and the
/// rewind removed the discarded ones with their arrival rows.
#[tokio::test]
async fn a_stream_opened_after_a_field_rewind_sees_only_the_retained_entries() {
    let (a, _da) = engine();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    let target = newest(&a);
    later();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 2 }, false).unwrap();
    a.rewind_to(target).unwrap();

    let options = crate::WatchOptions { start_at: Some(Hlc::ZERO), ..Default::default() };
    let mut stream = a.watch(crate::WatchScope::Collection(ca.id), options).unwrap();
    let mut seen = Vec::new();
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(300), stream.next(&a)).await
    {
        if let crate::ChangeEvent::Change { entry, .. } = event
            && entry.kind.is_document()
        {
            seen.push(entry.kind);
        }
    }

    assert_eq!(seen, vec![OpKind::Insert], "the undone replace is not delivered: {seen:?}");
}

// ---- loss records (C7: the rewind side) ------------------------------------------------------------------------

use kimmy_core::merge_state::{
    LossKey, LossKind, LossValue, MStamp, MergeState, Path, Value, WriterId,
};

/// The loss records in the stored state of the one document of `from`, with the entry's key.
fn stored_losses(from: &Engine) -> std::collections::BTreeMap<LossKey, LossValue> {
    let page =
        from.snapshot_page(None, Some(kimmy_core::CollectionId::derive("shop", "orders"))).unwrap();
    assert_eq!(page.documents.len(), 1, "premise: one row");
    crate::field_write::load(page.documents[0].meta.as_deref()).unwrap().losses
}

/// The record a member could have detected for the document `1`, stamped a little in the future of `from`'s clock.
fn a_record(from: &Engine, field: &str, wall_ahead: u64) -> (LossKey, OplogEntry) {
    let writer = WriterId { node: from.node_id(), epoch: from.epoch() };
    let loser = MStamp { hlc: Hlc::new(crate::physical_now_ms(), 0), writer };
    let key = LossKey::field(LossKind::Clash, Path::flat(field).unwrap(), loser);
    let value = LossValue::Value(Value::Int64(7));
    let entry = OplogEntry {
        stamp: kimmy_core::Stamp::new(
            Hlc::new(crate::physical_now_ms() + wall_ahead, 0),
            from.node_id(),
            from.epoch(),
        ),
        kind: OpKind::LossRecord,
        collection: kimmy_core::CollectionId::derive("shop", "orders"),
        doc_id: Some(DocId::Int64(1)),
        body: Some(MergeState::encode_loss_record(&key, &value)),
        meta: None,
    };
    (key, entry)
}

fn loss_entries(engine: &Engine) -> Vec<OplogEntry> {
    entries(engine).into_iter().filter(|e| e.kind == OpKind::LossRecord).collect()
}

#[test]
fn a_rewind_keeps_a_record_whose_entry_is_at_or_below_the_target() {
    let (a, _da) = engine();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    later();
    let (key, entry) = a_record(&a, "x", 1);
    a.apply_batch(std::slice::from_ref(&entry)).unwrap();
    assert!(stored_losses(&a).contains_key(&key), "premise: the record is in the stored state");
    let target = newest(&a);
    later();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 2 }, false).unwrap();

    a.rewind_to(target).unwrap();

    assert_eq!(read(&a, &ca, 1), want(doc! { "_id": 1, "x": 1 }), "the document is put back");
    assert!(
        stored_losses(&a).contains_key(&key),
        "and the record of an entry at or below the target stays"
    );
}

#[test]
fn a_document_changed_after_the_target_only_by_a_loss_entry_is_rewound_to_the_state_without_its_record()
 {
    let (a, _da) = engine();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    let target = newest(&a);
    later();
    let (key, entry) = a_record(&a, "x", 1);
    a.apply_batch(std::slice::from_ref(&entry)).unwrap();
    assert!(
        stored_losses(&a).contains_key(&key),
        "premise: the late record is in the stored state"
    );
    assert!(!loss_entries(&a).is_empty(), "premise: its entry is above the target");

    a.rewind_to(target).unwrap();

    assert!(
        !stored_losses(&a).contains_key(&key),
        "the state at the target has no record that entered after it"
    );
    assert!(loss_entries(&a).is_empty(), "and its entry was discarded with the rest of the future");
    assert_eq!(read(&a, &ca, 1), want(doc! { "_id": 1, "x": 1 }));
}

#[test]
fn a_rewind_mints_no_loss_entry_and_a_second_rewind_to_the_same_target_gives_the_same_records() {
    let (a, _da) = engine();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    later();
    let (key, entry) = a_record(&a, "x", 1);
    a.apply_batch(std::slice::from_ref(&entry)).unwrap();
    let target = newest(&a);
    let at_target = loss_entries(&a);
    later();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 2 }, false).unwrap();

    a.rewind_to(target).unwrap();
    let once = stored_losses(&a);
    assert_eq!(
        loss_entries(&a),
        at_target,
        "no loss entry minted by the rewind, none left above the target"
    );
    later();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 3 }, false).unwrap();
    a.rewind_to(target).unwrap();

    assert_eq!(stored_losses(&a), once, "restore twice (L26): the same records");
    assert!(once.contains_key(&key));
    assert_eq!(loss_entries(&a), at_target);
}

/// A record the stored state holds, whose loss entries retention has collected and that no retained state entry carries, is
/// dropped by the rewind (Q2, ruled by the manager 2026-11-20): the rewound state holds the records its retained history
/// derives, and a record is an at-least-once extra (must <= have <= allowed). The record here has no loss behind it, so it
/// is an extra. A REQUIRED head is never in this position: the two tests below show it re-derived or the document refused.
/// Retention collects a loss entry without a trace (`gc.rs` raises the missing-through row only for entries that carry a state),
/// so the rewind cannot tell.
#[test]
fn a_record_that_only_collected_loss_entries_explain_is_dropped_by_a_rewind_as_an_allowed_extra() {
    let (m, _dm) = engine();
    let (a, _da) = engine();
    let cm = field_collection(&m);
    a.apply_batch(&entries(&m)).unwrap();
    let ca = a.get_collection("shop", "orders").unwrap();
    a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    later();
    m.apply_batch(&entries(&a)).unwrap();
    later();
    // `m` receives a record (its own entry for it is appended), and then a write of `a` that has not seen it.
    let (key, entry) = a_record(&a, "x", 1);
    m.apply_batch(std::slice::from_ref(&entry)).unwrap();
    later();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 1, "y": 2 }, false).unwrap();
    later();
    m.apply_batch(&entries(&a)).unwrap();
    let carried_by_a_state_entry = entries(&m).iter().any(|e| {
        e.kind != OpKind::LossRecord
            && e.meta.as_deref().is_some_and(|meta| {
                crate::field_write::load(Some(meta)).unwrap().losses.contains_key(&key)
            })
    });
    assert!(
        !carried_by_a_state_entry,
        "premise: no state entry of the document carries the record"
    );
    let target = newest(&m);
    later();
    m.replace(&cm, &DocId::Int64(1), doc! { "_id": 1, "x": 1, "y": 3 }, false).unwrap();
    assert!(stored_losses(&m).contains_key(&key), "premise: the stored state holds the record");
    // Retention collects the loss entries (and what is older than them), not the write of `a` after them.
    let cutoff = loss_entries(&m).iter().map(|e| e.stamp.hlc.wall_ms).max().unwrap() + 5;
    m.collect_garbage_at(cutoff, RetentionPolicy::new(0, u64::MAX)).unwrap();
    let left = entries(&m);
    assert_eq!(
        left.iter().filter(|e| e.kind == OpKind::LossRecord && e.stamp.hlc <= target).count(),
        0,
        "premise: retention collected every loss entry at or below the target"
    );
    assert!(
        left.iter().any(|e| e.kind != OpKind::LossRecord && e.stamp.hlc <= target),
        "premise: a state entry of the document is retained at or below the target"
    );

    m.rewind_to(target).expect("not refused: no write at or below the target is missing");

    assert_eq!(
        read(&m, &cm, 1),
        want(doc! { "_id": 1, "x": 1, "y": 2 }),
        "the document is put back"
    );
    assert!(
        !stored_losses(&m).contains_key(&key),
        "the record no retained entry explains is dropped: the rewound state holds what its retained history derives"
    );
}

/// Q2 with a REAL required head. `m` rewinds. `a` and `c` write field `x` of document 1 concurrently, `m` receives both and
/// derives the record of the lost write (and appends its own entry for it). Retention then collects every entry older
/// than a later write of `a` (`a_sees_c`: whether `a` had received `c`'s write before it wrote, so that its state
/// carries the record), and `m` rewinds to the newest stamp. A required head is never silently lost: the rewind refuses
/// the document or the record is in the rewound state. Returns the refusal, if any, and whether the records are kept.
fn q2_real_clash(a_sees_c: bool) -> (Option<String>, bool) {
    let (m, _dm) = engine();
    let (a, _da) = engine();
    let (c, _dc) = engine();
    let cm = field_collection(&m);
    a.apply_batch(&entries(&m)).unwrap();
    c.apply_batch(&entries(&m)).unwrap();
    let ca = a.get_collection("shop", "orders").unwrap();
    let cc = c.get_collection("shop", "orders").unwrap();
    a.insert(&ca, doc! { "_id": 1, "x": 0 }).unwrap();
    later();
    m.apply_batch(&entries(&a)).unwrap();
    c.apply_batch(&entries(&a)).unwrap();
    later();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 1 }, false).unwrap();
    c.replace(&cc, &DocId::Int64(1), doc! { "_id": 1, "x": 2 }, false).unwrap();
    later();
    m.apply_batch(&entries(&a)).unwrap();
    m.apply_batch(&entries(&c)).unwrap();
    let clash = stored_losses(&m);
    assert!(!clash.is_empty(), "premise: m derived the record of the lost write: {clash:?}");
    later();
    if a_sees_c {
        a.apply_batch(&entries(&c)).unwrap();
        later();
    }
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 1, "y": 5 }, false).unwrap();
    let y_wall = entries(&a)
        .iter()
        .filter(|e| e.kind == OpKind::Replace)
        .map(|e| e.stamp.hlc.wall_ms)
        .max()
        .unwrap();
    later();
    m.apply_batch(&entries(&a)).unwrap();
    let target = newest(&m);
    later();
    m.replace(&cm, &DocId::Int64(1), doc! { "_id": 1, "x": 1, "y": 6 }, false).unwrap();
    m.collect_garbage_at(y_wall - 1, RetentionPolicy::new(0, u64::MAX)).unwrap();
    let left: Vec<_> = entries(&m).into_iter().filter(|e| e.stamp.hlc <= target).collect();
    assert_eq!(
        left.iter().map(|e| e.kind).collect::<Vec<_>>(),
        vec![OpKind::Replace],
        "premise: only a's later write is retained at or below the target"
    );
    match m.rewind_to(target) {
        Err(err) => (Some(err.to_string()), false),
        Ok(_) => (None, clash.keys().all(|k| stored_losses(&m).contains_key(k))),
    }
}

/// A required head whose later write never saw the other side: the write of the other side is collected and no retained
/// state covers it, so the document is REFUSED (missing.rs), and nothing is written.
#[test]
fn a_required_head_whose_other_write_is_collected_and_covered_by_no_retained_state_is_refused() {
    let (refused, kept) = q2_real_clash(false);
    let err = refused.expect("refused: the write of the other side is not in this member's oplog");
    assert!(err.contains("not in this member's oplog"), "{err}");
    assert!(!kept);
}

/// A required head whose later write DID see the other side: that write's retained state carries the record (any state
/// that saw both writes derived it), so the rewind succeeds and the record is KEPT.
#[test]
fn a_required_head_is_kept_through_a_retained_state_that_saw_both_writes() {
    let (refused, kept) = q2_real_clash(true);
    assert_eq!(refused, None, "not refused");
    assert!(kept, "the record of the lost write is in the rewound state");
}

/// The fold takes its cap and cut from the collection (`field_writer(..).options()`), the options the live apply uses, and
/// not from the defaults: a member whose collection keeps `K = 4` records holds four after a rewind as before it, though
/// the oplog holds the entries of all six records it was sent (and its own entry for each it accepted).
#[test]
fn the_rewind_folds_with_the_collections_own_loss_limits() {
    let (a, _da) = engine();
    a.set_default_loss_limits(kimmy_core::LossLimits { cap: 4, cut_bytes: 4096 }).unwrap();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    later();
    for (n, field) in ["f0", "f1", "f2", "f3", "f4", "f5"].into_iter().enumerate() {
        let (_, entry) = a_record(&a, field, 1 + n as u64);
        a.apply_batch(std::slice::from_ref(&entry)).unwrap();
    }
    let live = stored_losses(&a);
    assert_eq!(live.len(), 4, "premise: the collection keeps the K = 4 greatest records");
    assert!(
        loss_entries(&a).len() >= 6,
        "premise: the entries of all six records are kept, and the member's own for each"
    );
    let target = newest(&a);
    later();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 2 }, false).unwrap();

    a.rewind_to(target).unwrap();

    assert_eq!(
        stored_losses(&a),
        live,
        "the rewound state holds the records the live apply held: K = 4 of them"
    );
}

/// The lives a stored document state lists, by their stamps: what a state that outlived the retention still remembers.
fn stored_lives(from: &Engine) -> usize {
    let page =
        from.snapshot_page(None, Some(kimmy_core::CollectionId::derive("shop", "orders"))).unwrap();
    assert_eq!(page.documents.len(), 1, "premise: one row");
    crate::field_write::load(page.documents[0].meta.as_deref()).unwrap().lives.len()
}

/// The fold is made with the collection's own retention `R`, the options the live apply uses, and not the defaults': the join is not
/// associative once a history spreads past `R`. A deletes the document, a pause longer than `R` passes, A inserts it again, and B,
/// which never saw the delete, writes the first life afterwards: in stamp order the horizon has dropped the dead first life when B's
/// state, which still lists it, is joined, so it returns. A fold with the default retention (a day and more) never drops it, keeps it
/// dead, and the state it gives lists one life where the live apply's lists two.
#[test]
fn the_rewind_folds_with_the_collections_own_retention() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.set_default_join_retention(Duration::from_millis(400));
    let ca = field_collection(&a);
    b.apply_batch(&entries(&a)).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    b.apply_batch(&entries(&a)).unwrap();
    later();
    assert!(a.delete(&ca, &DocId::Int64(1)).unwrap());
    std::thread::sleep(Duration::from_millis(500));
    a.insert(&ca, doc! { "_id": 1, "y": 2 }).unwrap();
    later();
    b.replace(&cb, &DocId::Int64(1), doc! { "_id": 1, "x": 1, "z": 3 }, false).unwrap();
    a.apply_batch(&entries(&b)).unwrap();
    let live = stored_lives(&a);
    assert_eq!(live, 2, "premise: the first life returned when B's state was joined");
    let target = newest(&a);
    later();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "y": 5 }, false).unwrap();

    a.rewind_to(target).unwrap();

    assert_eq!(
        stored_lives(&a),
        live,
        "the state at the target is the fold under the collection's own R"
    );
}
