//! Unit P: the page route makes a collection that is gone here again from its row when a fact the page brings is stamped above its floor
//! (`Engine::remake_in_txn`), at the three places a page's fact meets a collection that is not held: a document, a delete, and a definition
//! of the collection's state. The carry's own merge is pinned by `plain_life_page_rows_tests` (pb1 to pb3, pe1).
//!
//! The world: `s` holds `orders` with document 1 and has not heard of its drop; `z` took the collection and dropped it (D); `m` took the
//! collection, then D, and purged: it holds the collection's tombstone and the row N kept, and no collection. Then `s` writes, deletes or
//! defines something stamped above D, and `m` takes `s`'s whole-database page. Each ends with the collection made again, dated just above D
//! under its row's options, the life re-dated, holding only what is above D.

use bson::doc;
use kimmy_core::{CollectionId, DocId, Hlc, IndexField, OpKind, OplogEntry, VersionVector};

use crate::{Engine, PeerWindow, SnapshotProgress, WalkScope};

const DB: &str = "app";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn window(b: &Engine, entries: &[OplogEntry]) {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = b.witnessed_vector().unwrap();
    let mut outcome = crate::SyncOutcome::default();
    b.apply_peer_window_into(
        &theirs,
        Some(&mine),
        entries,
        PeerWindow {
            scanned_to: entries.last().map_or(Hlc::ZERO, |e| e.stamp.hlc),
            exhausted: true,
            passed_through: None,
            asked_partial: false,
        },
        &mut outcome,
    )
    .unwrap();
}

fn pause() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn id() -> CollectionId {
    CollectionId::derive(DB, "orders")
}

/// `s` (holding `orders` and document 1, not having heard of the drop), the drop D, and `m` (which took the collection and D, purged, and
/// holds the tombstone and the row only).
fn the_world() -> (Engine, Engine, OplogEntry, [tempfile::TempDir; 3]) {
    let (s, ds) = engine();
    let orders = s.create_collection(DB, "orders").unwrap();
    s.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    let (z, dz) = engine();
    window(&z, &everything(&s));
    pause();
    assert!(z.drop_collection(DB, "orders").unwrap());
    let d = everything(&z)
        .into_iter()
        .find(|e| e.kind == OpKind::DropCollection && e.collection == id())
        .expect("the drop");
    let (m, dm) = engine();
    window(&m, &everything(&s));
    window(&m, std::slice::from_ref(&d));
    let _ = m.purge_dropped_collection(id());
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the collection is gone on m");
    assert_eq!(
        m.collection_dropped_at(id()).unwrap(),
        Some(d.stamp),
        "premise: with its tombstone"
    );
    pause();
    (s, m, d, [ds, dz, dm])
}

fn pull(from: &Engine, into: &Engine) {
    let page = from.snapshot_page(None, None).unwrap();
    assert!(page.next.is_none(), "premise: the database is one page");
    into.apply_snapshot_page(from.node_id(), &mut SnapshotProgress::whole_database(), &page)
        .expect("the page applies");
}

/// What the collection made again looks like on `m` after a page: standing, dated just above D, re-dated, under the row's options.
fn made_again(m: &Engine, d: &OplogEntry, how: &str, row_at: Hlc) {
    let held = m.get_collection(DB, "orders");
    assert!(held.is_ok(), "{how}: a fact above the floor makes the collection again");
    let held = held.unwrap();
    assert_eq!(held.created, d.stamp.hlc.successor(), "{how}: dated just above the floor");
    assert_eq!(held.incarnation_floor, Some(d.stamp.hlc), "{how}: with the drop as its floor");
    assert!(held.is_redated(), "{how}: the life is re-dated, as the other order reaches it");
    assert_eq!(
        held.merge_policy_at,
        Some(row_at),
        "{how}: the policy stamp is the row's `at` (the creation the row keeps), not the life's `created`"
    );
    assert_eq!(
        m.collection_dropped_at(id()).unwrap(),
        Some(d.stamp),
        "{how}: its tombstone is D's"
    );
}

/// A document of the page stamped above D makes the collection again, and document 1, below D, does not come back. Mutant: the remake at
/// the page's documents is not made (the write is judged history by the tombstone).
#[test]
fn a_page_document_above_the_floor_makes_the_gone_collection_again_from_its_row() {
    let (s, m, d, _dirs) = the_world();
    let orders = s.get_collection(DB, "orders").unwrap();
    s.insert(&orders, doc! { "_id": 7, "v": "after the drop" }).unwrap();
    pull(&s, &m);
    made_again(&m, &d, "a document", orders.created);
    let held = m.get_collection(DB, "orders").unwrap();
    assert_eq!(m.count(&held, WalkScope::Background).unwrap(), 1, "with the write alone");
    assert!(m.get(&held, &DocId::Int64(7)).unwrap().is_some(), "document 7 is there");
    assert!(m.get(&held, &DocId::Int64(1)).unwrap().is_none(), "and document 1 did not come back");
}

/// A delete of the page stamped above D makes the collection again (it is a fact above the floor), holding nothing. Mutant: the remake at
/// the page's deletes is not made.
#[test]
fn a_page_delete_above_the_floor_makes_the_gone_collection_again_from_its_row() {
    let (s, m, d, _dirs) = the_world();
    let orders = s.get_collection(DB, "orders").unwrap();
    assert!(s.delete(&orders, &DocId::Int64(1)).unwrap());
    let page = s.snapshot_page(None, None).unwrap();
    assert!(!page.deleted_documents.is_empty(), "premise: the page carries the delete");
    m.apply_snapshot_page(s.node_id(), &mut SnapshotProgress::whole_database(), &page)
        .expect("the page applies");
    made_again(&m, &d, "a delete", orders.created);
    let held = m.get_collection(DB, "orders").unwrap();
    assert_eq!(m.count(&held, WalkScope::Background).unwrap(), 0, "holding no document");
}

/// A definition of the page's state stamped above D (an index) makes the collection again and is restored into it. Mutant: the remake at
/// the state's refusal as history is not made.
#[test]
fn a_page_definition_above_the_floor_makes_the_gone_collection_again_from_its_row() {
    let (s, m, d, _dirs) = the_world();
    let orders = s.get_collection(DB, "orders").unwrap();
    s.create_index(DB, "orders", vec![IndexField::ascending("v")], false, Some("x".into()))
        .unwrap();
    pull(&s, &m);
    made_again(&m, &d, "a definition", orders.created);
    let held = m.get_collection(DB, "orders").unwrap();
    assert!(held.index("x").is_some(), "the index above the drop is restored into it");
    assert_eq!(m.count(&held, WalkScope::Background).unwrap(), 0, "and no document came back");
}
