//! Step 3, item (3.3): a state applied as state (a page document, a carried delete of a `field` collection) has
//! no entry behind it, so every write it holds of every writer is a hole in what this member can serve: the
//! hole is recorded, by the writer's node and epoch, in the transaction that applies it, and a peer behind it
//! is sent to a snapshot. This member's own live origin is left out.

use bson::doc;
use kimmy_core::{Epoch, Hlc, MergePolicy, OplogEntry, Origin, Stamp};

use crate::engine::WriterHolder;
use crate::{CollectionMeta, Engine, ServeVerdict, SnapshotProgress, WalkScope};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn last_stamp(from: &Engine) -> Stamp {
    everything(from).last().unwrap().stamp
}

/// `a` makes a field collection and a document X in it, and `m` takes both by entries.
fn shared_document() -> (Engine, Engine, CollectionMeta, kimmy_core::DocId, [tempfile::TempDir; 2])
{
    let (a, da) = engine();
    let (m, dm) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let creation = everything(&a);
    m.apply_batch(&creation).unwrap();
    later();
    let x = a.insert(&ca, doc! { "_id": 1 }).unwrap();
    let after: Vec<_> =
        everything(&a).into_iter().filter(|e| e.stamp > creation.last().unwrap().stamp).collect();
    m.apply_batch(&after).unwrap();
    (a, m, ca, x, [da, dm])
}

/// The sender's whole scoped snapshot of the collection, applied to `into` under `Converge`.
fn converge(into: &Engine, from: &Engine, coll: &CollectionMeta) {
    let mut progress =
        SnapshotProgress::of_collection_converging(coll.id, kimmy_core::VersionVector::new());
    while !progress.is_complete() {
        let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
    }
}

/// A repair page that carries a document leaves a hole over what the document holds, and a peer behind it is
/// sent to a snapshot: the oplog here has no entry for the writes the page brought.
#[test]
fn a_repair_page_with_a_field_document_leaves_a_hole_and_a_peer_behind_it_is_sent_to_a_snapshot() {
    let (a, m, ca, x, _dirs) = shared_document();
    later();
    a.replace(&ca, &x, doc! { "_id": 1, "a": 1 }, false).unwrap();
    let write = last_stamp(&a);
    assert!(m.state_held().unwrap().is_empty(), "premise: no hole before the repair");

    converge(&m, &a, &ca);

    let held = m.state_held().unwrap().get(a.origin());
    assert!(held >= write.hlc, "the hole reaches the repaired document's last write: {held:?}");
    assert_eq!(
        m.serve_verdict_from(Hlc::ZERO).unwrap(),
        ServeVerdict::StateHeld,
        "a peer behind the repaired document is sent to a snapshot"
    );
}

/// The writes a state holds can be ones the servable vector already claims; there is no entry for them either
/// way, so the hole is recorded all the same.
#[test]
fn a_hole_is_left_over_writes_the_servable_vector_already_claims() {
    let (a, m, ca, x, _dirs) = shared_document();
    later();
    a.replace(&ca, &x, doc! { "_id": 1, "a": 1 }, false).unwrap();
    let write = last_stamp(&a);
    {
        let txn = m.begin_write(WriterHolder::Replication).unwrap();
        crate::engine::raise_version(&txn, crate::tables::OPLOG_VERSIONS, &write).unwrap();
        txn.commit().unwrap();
    }

    converge(&m, &a, &ca);

    let held = m.state_held().unwrap().get(a.origin());
    assert!(held >= write.hlc, "the hole is left over writes the servable vector claims: {held:?}");
}

/// A whole-database snapshot grants its coverage as of its first page, but a document on a later page can hold
/// a write made after it. The grant does not cover that write, and the entry that later brings it arrives
/// covered and is not appended, so the hole has to reach the document's own last write.
#[test]
fn a_page_document_with_writes_above_the_first_page_grant_leaves_a_hole_above_it() {
    let (a, _da) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    for i in 0..6i64 {
        a.insert(&ca, doc! { "_id": i, "pad": "x".repeat(200) }).unwrap();
    }
    // A page budget that splits the six documents into three pages or more, found as the multi-page
    // snapshot test of 3.1 finds it.
    let bytes = (0..64usize)
        .find_map(|step| {
            let bytes = 500 + step * 250;
            a.set_snapshot_page_bytes_for_test(Some(bytes));
            let mut pages = 0;
            let mut after = None;
            loop {
                let page = a.snapshot_page(after.take(), None).ok()?;
                after = page.next.clone();
                pages += 1;
                if after.is_none() {
                    break;
                }
                if pages > 50 {
                    return None;
                }
            }
            (pages >= 3).then_some(bytes)
        })
        .expect("a page budget that splits six documents into three pages or more");
    a.set_snapshot_page_bytes_for_test(Some(bytes));

    let (m, _dm) = engine();
    let mut progress = SnapshotProgress::whole_database();
    let mut after = None;
    let mut write = None;
    for n in 0..50 {
        let page = a.snapshot_page(after.take(), None).unwrap();
        if n == 0 {
            // After the first page, and so above its grant: the last document is written again.
            later();
            let last = kimmy_core::DocId::Int64(5);
            a.replace(&ca, &last, doc! { "_id": 5, "pad": "y", "again": 1 }, false).unwrap();
            write = Some(last_stamp(&a));
        }
        m.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
        after = page.next.clone();
        if after.is_none() {
            break;
        }
    }
    assert!(progress.is_complete(), "the snapshot ended");
    let write = write.expect("the write after the first page");

    let held = m.state_held().unwrap().get(a.origin());
    assert!(
        held >= write.hlc,
        "the hole reaches the page document's last write, above the first page's grant: {held:?}"
    );
}

/// A carried delete of a field document is a state with no entry too.
#[test]
fn a_carried_delete_leaves_a_hole_over_the_state_it_joined() {
    let (a, m, ca, x, _dirs) = shared_document();
    later();
    a.delete(&ca, &x).unwrap();
    let delete = last_stamp(&a);
    assert!(m.state_held().unwrap().is_empty(), "premise: no hole before the repair");

    converge(&m, &a, &ca);

    let held = m.state_held().unwrap().get(a.origin());
    assert!(held >= delete.hlc, "the hole reaches the carried delete: {held:?}");
    assert_eq!(m.serve_verdict_from(Hlc::ZERO).unwrap(), ServeVerdict::StateHeld);
}

/// The hole is set for every writer the state names, by the node AND the epoch it names: a document written by
/// A and then by B, taken by a member that holds only A's write, leaves a row for each, and none for B's node
/// with no epoch.
#[test]
fn a_hole_is_set_for_each_node_and_epoch_the_state_names() {
    let (a, m, ca, x, _dirs) = shared_document();
    let (b, _db) = engine();
    b.apply_batch(&everything(&a)).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    later();
    b.replace(&cb, &x, doc! { "_id": 1, "b": 2 }, false).unwrap();
    let b_write = last_stamp(&b);
    let a_insert = everything(&a).last().unwrap().stamp;
    assert_ne!(b.epoch(), Epoch::ZERO, "premise: B's epoch is not the zero epoch");

    converge(&m, &b, &ca);

    let held = m.state_held().unwrap();
    assert!(held.get(b.origin()) >= b_write.hlc, "a row for B's node and epoch: {held:?}");
    assert!(held.get(a.origin()) >= a_insert.hlc, "and one for A's, which B's state also names");
    assert_eq!(
        held.get(Origin::new(b.node_id(), Epoch::ZERO)),
        Hlc::ZERO,
        "none for B's node with no epoch"
    );
}

/// A field entry that arrives by a pull is history: it leaves no hole.
#[test]
fn a_field_entry_applied_by_a_pull_leaves_no_hole() {
    let (a, m, ca, x, _dirs) = shared_document();
    later();
    a.replace(&ca, &x, doc! { "_id": 1, "a": 1 }, false).unwrap();
    let applied = m.apply_batch(&everything(&a)).unwrap();
    assert!(applied.applied > 0, "premise: the write was applied: {applied:?}");
    assert!(m.state_held().unwrap().is_empty(), "an entry is history, not state");
}

/// A page that is refused, and so rolled back whole, leaves no hole for the documents it had applied.
#[test]
fn a_page_that_is_rolled_back_leaves_no_hole() {
    let (a, _da) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    a.insert(&ca, doc! { "_id": 0 }).unwrap();
    // The second document is stamped far past the bound of the member that takes the page.
    let now = last_stamp(&a);
    a.advance_clock_past(&Stamp::new(
        Hlc::new(now.hlc.wall_ms + 400_000, 0),
        a.node_id(),
        a.epoch(),
    ));
    a.insert(&ca, doc! { "_id": 1 }).unwrap();
    let page = a.snapshot_page(None, None).unwrap();
    assert_eq!(page.documents.len(), 2, "premise: one page carries both");

    let (m, _dm) = engine();
    let applied =
        m.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    assert!(applied.clock_refused.is_some(), "premise: the page was refused: {applied:?}");
    assert!(m.state_held().unwrap().is_empty(), "a rolled back page leaves no hole");
}

/// This member's own live origin is never a hole: every write of its own epoch is in its own oplog until
/// retention collects it. A state that carries the member's own writes must not send its peers to a snapshot
/// over writes it can serve.
#[test]
fn a_state_that_carries_the_receivers_own_writes_leaves_no_hole_on_its_own_origin() {
    let (a, m, ca, x, _dirs) = shared_document();
    let cm = m.get_collection("shop", "orders").unwrap();
    later();
    m.replace(&cm, &x, doc! { "_id": 1, "m": 1 }, false).unwrap();
    a.apply_batch(&everything(&m)).unwrap();
    later();
    a.replace(&ca, &x, doc! { "_id": 1, "m": 1, "a": 2 }, false).unwrap();

    converge(&m, &a, &ca);

    assert_eq!(
        m.state_held().unwrap().get(m.origin()),
        Hlc::ZERO,
        "no hole on the receiver's own live origin"
    );
}
