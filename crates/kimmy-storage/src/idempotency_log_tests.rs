//! The change log of idempotency records across an identity reset of a store
//! that holds no record (ADR-240, "Snapshots carry records").
//!
//! An open begins the log again over the records a store holds when no log of
//! this node's identity has begun (`begin_log_over_held_records`); a store that
//! holds none is left to its first change, which begins the log again when the
//! beginning it finds is another identity's (`log_change`). This is that second
//! case, mutation row P:M12r of `docs/adr-240-mutation-rows.md`: the open's
//! beginning covers the reset of a store with records, so only a store without
//! any shows whether the first change begins the log again.

use bson::doc;
use kimmy_core::{DocId, Fingerprint, ScopeHash};

use crate::idempotency::{Keyed, KeyedWrite};
use crate::{
    CollectionMeta, Engine, SNAPSHOT_PAGE, SnapshotCursor, SnapshotPage, SnapshotProgress,
};

fn key(n: u8) -> KeyedWrite {
    KeyedWrite::new(ScopeHash([n; 32]), Fingerprint([n; 32]))
}

/// A collection whose documents take three pages.
fn three_pages(y: &Engine) -> CollectionMeta {
    let cy = y.create_collection("shop", "orders").unwrap();
    y.insert_many(
        &cy,
        (1..(3 * SNAPSHOT_PAGE as i64)).map(|i| doc! { "_id": i, "v": i }).collect(),
    )
    .unwrap();
    cy
}

/// The page restarted its collection: its records phase, `records` of them,
/// and the documents from the first.
fn restarted(page: &SnapshotPage, records: usize) -> bool {
    page.records.len() == records && page.documents.first().map(|d| &d.id) == Some(&DocId::Int64(1))
}

fn epoch(cursor: &SnapshotCursor) -> Option<i64> {
    cursor.records_base.and_then(|base| base.epoch)
}

/// A sender whose log began and whose records are all gone (retention collected
/// them) has its identity reset. The open begins no log (there is no record to
/// begin it over), and the log its earlier identity began reads as none: a base
/// cut before the reset restarts its collection. Its first keyed write begins
/// the log again, under this identity and with a new epoch, which its pages
/// name; a base cut before the reset still restarts its collection.
#[test]
fn an_identity_reset_over_a_store_without_records_begins_the_log_again_at_its_first_change() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("y.redb");
    let (y, x) = (Engine::open(&path).unwrap(), Engine::open(&dir.path().join("x.redb")).unwrap());
    let cy = three_pages(&y);
    let began = crate::idempotency::begin_log_for_test(&y).unwrap();
    let mut progress = SnapshotProgress::of_collection(cy.id);
    let page = y.snapshot_page(None, Some(cy.id)).unwrap();
    x.apply_snapshot_page(y.node_id(), &mut progress, &page).unwrap();
    let before = progress.after().cloned().unwrap();
    assert_eq!(epoch(&before), Some(began.epoch), "premise: the base names the log y began");
    assert!(y.idempotency_record(cy.id, &key(9).scope).unwrap().is_none(), "premise: no record");
    let old = y.node_id();
    y.close().unwrap();
    {
        let db = redb::Database::open(&path).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(crate::tables::META).unwrap().remove(crate::tables::META_NODE_ID).unwrap();
        txn.commit().unwrap();
    }
    let y = Engine::open(&path).unwrap();
    assert_ne!(y.node_id(), old, "premise: the identity was reset");
    let page = y.snapshot_page(Some(before.clone()), Some(cy.id)).unwrap();
    assert_eq!(page.records_epoch, None, "the log another identity began reads as none");
    assert!(restarted(&page, 0), "a base of that log restarts its collection: {:?}", page.next);
    match y.insert_keyed(&cy, doc! { "_id": 1_000_000i64 }, &key(9)).unwrap() {
        Keyed::Fresh(_) => {}
        other => panic!("expected a fresh write, got {other:?}"),
    }
    let page = y.snapshot_page(Some(before.clone()), Some(cy.id)).unwrap();
    assert!(
        restarted(&page, 1),
        "a base cut before the reset restarts its collection: {:?}",
        page.next
    );
    let renewed = page.records_epoch;
    assert!(
        renewed.is_some() && renewed != Some(began.epoch),
        "the first change began the log again under this identity: {renewed:?}"
    );
    assert_eq!(page.next.as_ref().map(epoch), Some(renewed), "and the cursor's base names it");
}
