//! The `conflictResolved` side of the storage stream and the counter behind `kimmy_conflicts_resolved_total` (Q7 Step 4, C8): a member
//! tells a loss record once, by the loss entry of its OWN that it minted when the record entered its state; no other loss entry reaches a
//! client; the resume skip exempts an own loss entry only for a token another member issued; the counter follows the commit.

use std::sync::Arc;
use std::time::Duration;

use bson::doc;
use kimmy_core::merge_state::{
    LossKey, LossKind, LossValue, MStamp, MergeState, Path, Value, WriterId,
};
use kimmy_core::{DocId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp};

use crate::{
    ChangeEvent, CollectionMeta, Engine, SnapshotProgress, WalkScope, WatchOptions, WatchScope,
};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn later() {
    std::thread::sleep(Duration::from_millis(20));
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn pair() -> (Engine, Engine, CollectionMeta, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    let coll = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    (a, b, coll, [da, db])
}

/// `a` and `b` write field `x` of document 1 at once and take each other's entries: each derives the record of the write that lost
/// and mints an entry of its own for it, and each holds the other's entry too.
fn a_clash(a: &Engine, b: &Engine, ca: &CollectionMeta) {
    a.insert(ca, doc! { "_id": 1, "x": 0 }).unwrap();
    b.apply_batch(&everything(a)).unwrap();
    later();
    let cb = b.get_collection("shop", "orders").unwrap();
    a.replace(ca, &DocId::Int64(1), doc! { "_id": 1, "x": 1 }, false).unwrap();
    b.replace(&cb, &DocId::Int64(1), doc! { "_id": 1, "x": 2 }, false).unwrap();
    later();
    a.apply_batch(&everything(b)).unwrap();
    b.apply_batch(&everything(a)).unwrap();
    a.apply_batch(&everything(b)).unwrap();
}

fn loss_entries(e: &Engine) -> Vec<OplogEntry> {
    everything(e).into_iter().filter(|x| x.kind == OpKind::LossRecord).collect()
}

fn own_loss_entries(e: &Engine) -> Vec<OplogEntry> {
    loss_entries(e).into_iter().filter(|x| x.stamp.node == e.node_id()).collect()
}

fn total(e: &Engine) -> u64 {
    e.conflicts_resolved().iter().map(|(_, n)| n).sum()
}

/// Every event the stream has now: the replay, then what the idle stream times out on.
async fn drain(e: &Engine, stream: &mut crate::ChangeStream) -> Vec<ChangeEvent> {
    let mut out = Vec::new();
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(400), stream.next(e)).await
    {
        out.push(event);
    }
    out
}

fn loss_events(events: &[ChangeEvent]) -> Vec<Arc<OplogEntry>> {
    events
        .iter()
        .filter_map(|e| match e {
            ChangeEvent::Change { entry, .. } if entry.kind == OpKind::LossRecord => {
                Some(Arc::clone(entry))
            }
            _ => None,
        })
        .collect()
}

fn from_start(e: &Engine, coll: &CollectionMeta) -> crate::ChangeStream {
    e.watch(
        WatchScope::Collection(coll.id),
        WatchOptions { start_at: Some(Hlc::ZERO), ..Default::default() },
    )
    .unwrap()
}

#[tokio::test]
async fn a_member_tells_a_loss_by_the_entry_of_its_own_and_by_no_other() {
    let (a, b, ca, _d) = pair();
    a_clash(&a, &b, &ca);
    let own = own_loss_entries(&a);
    assert!(!own.is_empty(), "premise: `a` minted an entry of its own for the record");
    assert!(
        loss_entries(&a).iter().any(|e| e.stamp.node == b.node_id()),
        "premise: `a` also holds the entry `b` minted (kept to be served onward)"
    );

    let mut stream = from_start(&a, &ca);
    let told = loss_events(&drain(&a, &mut stream).await);

    let mut want: Vec<_> = own.iter().map(|e| e.stamp).collect();
    let mut got: Vec<_> = told.iter().map(|e| e.stamp).collect();
    want.sort();
    got.sort();
    assert_eq!(got, want, "exactly the entries `a` minted, each once");
    assert!(told.iter().all(|e| e.stamp.node == a.node_id()), "and none of another member's");
}

#[tokio::test]
async fn the_counter_is_the_number_of_events_the_member_made() {
    let (a, b, ca, _d) = pair();
    assert_eq!(total(&a), 0, "premise");
    a_clash(&a, &b, &ca);
    let own = own_loss_entries(&a);
    assert_eq!(total(&a), own.len() as u64, "one count for each entry of its own");
    let by_kind: std::collections::BTreeMap<_, _> = a.conflicts_resolved().into_iter().collect();
    assert_eq!(by_kind.len(), 6, "every kind has its sample");
    assert_eq!(by_kind["retiredWriter"], 0, "reserved");
    assert!(by_kind["clash"] + by_kind["insertLost"] + by_kind["deleteWon"] > 0, "{by_kind:?}");
    let kinds: Vec<_> = a.conflicts_resolved().iter().map(|(k, _)| *k).collect();
    assert_eq!(
        kinds,
        ["clash", "deleteWon", "insertLost", "retiredWriter", "typeClash", "arrayClash"]
    );
}

/// A loss record for a document this member has no row for is parked, and a refused or parked record tells nothing and counts nothing.
#[tokio::test]
async fn a_record_that_was_parked_tells_nothing_and_counts_nothing() {
    let (a, b, ca, _d) = pair();
    let writer = WriterId { node: b.node_id(), epoch: b.epoch() };
    let loser = MStamp { hlc: Hlc::new(crate::physical_now_ms(), 0), writer };
    let key = LossKey::field(LossKind::Clash, Path::flat("x").unwrap(), loser);
    let entry = OplogEntry {
        stamp: Stamp::new(Hlc::new(crate::physical_now_ms() + 1, 0), b.node_id(), b.epoch()),
        kind: OpKind::LossRecord,
        collection: ca.id,
        doc_id: Some(DocId::Int64(9)),
        body: Some(MergeState::encode_loss_record(&key, &LossValue::Value(Value::Int64(1)))),
        meta: None,
    };
    a.apply_batch(std::slice::from_ref(&entry)).unwrap();
    assert!(loss_entries(&a).iter().any(|e| e.stamp == entry.stamp), "premise: the entry is kept");

    let mut stream = from_start(&a, &ca);
    assert!(loss_events(&drain(&a, &mut stream).await).is_empty(), "a relayed entry is no event");
    assert_eq!(total(&a), 0);
    assert!(
        own_loss_entries(&a).is_empty(),
        "and `a` minted none: the record did not enter its state"
    );
}

#[tokio::test]
async fn a_rewind_tells_nothing_new_and_counts_nothing() {
    let (a, b, ca, _d) = pair();
    a.insert(&ca, doc! { "_id": 1, "x": 0 }).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    later();
    let target = everything(&a).iter().map(|e| e.stamp.hlc).max().unwrap();
    later();
    let cb = b.get_collection("shop", "orders").unwrap();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 1 }, false).unwrap();
    b.replace(&cb, &DocId::Int64(1), doc! { "_id": 1, "x": 2 }, false).unwrap();
    later();
    a.apply_batch(&everything(&b)).unwrap();
    let made = total(&a);
    assert!(made > 0, "premise: a record entered `a`'s state");

    a.rewind_to(target).unwrap();

    assert_eq!(total(&a), made, "a rewind counts nothing");
    assert!(
        own_loss_entries(&a).is_empty(),
        "the entries above the target went with the future, and none was minted"
    );
    let mut stream = from_start(&a, &ca);
    assert!(loss_events(&drain(&a, &mut stream).await).is_empty(), "and nothing is told");
}

/// A route that is state and not history (a snapshot page) still mints the member's own entry, so the member tells it.
#[tokio::test]
async fn a_member_that_installs_a_state_carrying_a_record_it_did_not_hold_tells_it_once() {
    let (a, b, ca, _d) = pair();
    a_clash(&a, &b, &ca);
    assert!(total(&a) > 0, "premise: `a` holds a record");
    let (c, _dc) = engine();
    c.apply_batch(&everything_definitions(&a)).unwrap();
    let cc = c.get_collection("shop", "orders").unwrap();
    assert_eq!(total(&c), 0, "premise");

    let page = a.snapshot_page(None, None).unwrap();
    c.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();

    let records = crate::field_write::load(
        c.snapshot_page(None, Some(ca.id)).unwrap().documents[0].meta.as_deref(),
    )
    .unwrap()
    .losses;
    assert!(!records.is_empty(), "premise: the state `c` installed carries a record");
    assert_eq!(
        own_loss_entries(&c).len(),
        records.len(),
        "one entry of its own for each record that entered"
    );
    assert_eq!(total(&c), records.len() as u64, "and one count");
    let mut stream = from_start(&c, &cc);
    assert_eq!(loss_events(&drain(&c, &mut stream).await).len(), records.len(), "and one event");
}

/// The definitions of the collection only (what a member with no documents holds).
fn everything_definitions(from: &Engine) -> Vec<OplogEntry> {
    everything(from)
        .into_iter()
        .filter(|e| !e.kind.is_document() && !e.kind.is_loss_record())
        .collect()
}

/// A batch whose later entry fails is one transaction that is dropped: the counter is unchanged and no entry of the member's own is left.
#[tokio::test]
async fn a_batch_that_rolls_back_counts_nothing_and_leaves_no_entry_of_its_own() {
    let (a, b, ca, _d) = pair();
    a.insert(&ca, doc! { "_id": 1, "x": 0 }).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    later();
    let cb = b.get_collection("shop", "orders").unwrap();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 1 }, false).unwrap();
    let written_by_a = everything(&a);
    later();
    b.replace(&cb, &DocId::Int64(1), doc! { "_id": 1, "x": 2 }, false).unwrap();
    // `a` is to take `b`'s write (a record enters its state) and then an entry that cannot be applied.
    let before = everything(&a);
    let mut batch: Vec<OplogEntry> = everything(&b)
        .into_iter()
        .filter(|e| e.stamp.node == b.node_id() && e.kind.is_document())
        .collect();
    let (corrupt_stamp, _) =
        (Stamp::new(Hlc::new(crate::physical_now_ms() + 5, 0), b.node_id(), b.epoch()), ());
    batch.push(OplogEntry {
        stamp: corrupt_stamp,
        kind: OpKind::Insert,
        collection: ca.id,
        doc_id: Some(DocId::Int64(2)),
        body: Some(bson::serialize_to_vec(&doc! { "_id": 2 }).unwrap()),
        meta: Some(vec![0xff, 0xff, 0xff]),
    });
    let counted = total(&a);

    let result = a.apply_batch(&batch);

    assert!(result.is_err(), "premise: the batch fails ({result:?})");
    assert_eq!(total(&a), counted, "a transaction that did not commit counts nothing");
    assert!(own_loss_entries(&a).is_empty(), "and left no entry of its own");
    assert_eq!(everything(&a).len(), before.len(), "nothing of the batch is in the oplog");
    drop(written_by_a);

    // The control: the same entries without the bad one are applied, a record enters, and the member counts it.
    batch.pop();
    a.apply_batch(&batch).unwrap();
    assert!(
        total(&a) > counted,
        "control: the entries the aborted batch carried do mint an entry and count it"
    );
    assert!(!own_loss_entries(&a).is_empty(), "control: and leave an entry of the member's own");
}

/// `a` and `b` write field `x` of one document in turn, and then `a` takes `b`'s entries with its clock `ahead_ms` ahead of the real one. The
/// record of the write that lost enters `a`'s state either way.
///
/// The join retention is ten seconds, so the record cannot be refused as below the state's horizon (the retention under the newest version
/// the state has seen) unless ten seconds of a loaded machine pass between the two writes. Whether the record is inside the emission window
/// (the retention after the lost write's own time, by this member's clock) is set by moving the clock and not by waiting for it, so what the
/// machine's load does to the gap between the two writes cannot move the answer.
fn clash_met_with_the_clock_ahead_by(
    ahead_ms: i64,
) -> (Engine, CollectionMeta, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    a.set_default_join_retention(Duration::from_secs(10));
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    a.insert(&ca, doc! { "_id": 1, "x": 0 }).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    later();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 1 }, false).unwrap();
    b.replace(&cb, &DocId::Int64(1), doc! { "_id": 1, "x": 2 }, false).unwrap();
    a.set_clock_offset_for_tests(ahead_ms);

    a.apply_batch(&everything(&b)).unwrap();
    (a, ca, [da, db])
}

fn losses_in_the_state_of(a: &Engine, coll: &CollectionMeta) -> usize {
    crate::field_write::load(
        a.snapshot_page(None, Some(coll.id)).unwrap().documents[0].meta.as_deref(),
    )
    .unwrap()
    .losses
    .len()
}

/// The emission window: a record older than the retention time by this member's clock is in the state, is not reported, and so is neither
/// told nor counted. The event, the entry and the counter are one decision.
#[tokio::test]
async fn a_record_older_than_the_retention_time_enters_the_state_but_is_neither_told_nor_counted() {
    // The control: with the clock as it is, the lost write is young and the record is reported. The same scenario, so the clock is the only
    // difference between the two halves of the test.
    let (a, ca, _dirs) = clash_met_with_the_clock_ahead_by(0);
    assert!(losses_in_the_state_of(&a, &ca) > 0, "control premise: the record is in the state");
    assert!(!own_loss_entries(&a).is_empty(), "control: a young lost write is told by an entry");
    assert!(total(&a) > 0, "control: and counted");

    // A clock a minute ahead of the lost write, which the retention (ten seconds) has long passed by it.
    let (a, ca, _dirs) = clash_met_with_the_clock_ahead_by(60_000);
    assert!(losses_in_the_state_of(&a, &ca) > 0, "premise: the record is in the state");
    assert!(own_loss_entries(&a).is_empty(), "no entry of its own for a record past the window");
    assert_eq!(total(&a), 0, "and no count");
    let mut stream = from_start(&a, &ca);
    assert!(loss_events(&drain(&a, &mut stream).await).is_empty(), "and no event");
}

/// The member whose own write wins: the record enters its state and the document does not change, which is another branch of the apply
/// (the merged state moved and the document did not). It makes its event all the same.
#[tokio::test]
async fn the_member_whose_write_wins_tells_the_loss_although_its_document_did_not_change() {
    let (a, b, ca, _d) = pair();
    a_clash(&a, &b, &ca);
    // `b` wrote x = 2 after `a` wrote x = 1 (a_clash), so `b`'s write wins on both members: `b`'s document did not change when it met `a`'s.
    let cb = b.get_collection("shop", "orders").unwrap();
    assert_eq!(
        b.get(&cb, &DocId::Int64(1)).unwrap().unwrap().get_i32("x").unwrap(),
        2,
        "premise: `b`'s write won"
    );
    let own = own_loss_entries(&b);
    assert!(
        !own.is_empty(),
        "`b` minted an entry of its own for the record that entered its state"
    );
    assert_eq!(total(&b), own.len() as u64, "and counted it");
    let mut stream = from_start(&b, &cb);
    assert_eq!(loss_events(&drain(&b, &mut stream).await).len(), own.len(), "and tells it");
}
