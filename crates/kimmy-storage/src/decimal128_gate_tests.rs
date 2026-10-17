//! The `Decimal128` gate and its marker (ADR-238): the three local chokepoints
//! refuse while the installed predicate is closed, every replicated, restored and
//! repaired route applies whatever it says, and the marker is written by the first
//! decimal `_id` a member writes or applies, keyed on the body's BSON type.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bson::{Bson, Document, doc};
use kimmy_core::{CollectionId, Decimal128Closed, Decimal128Use, DocId, Error as CoreError, Hlc};

use crate::{
    CollectionMeta, Engine, IndexField, SnapshotProgress, StorageError, WalkScope, WriterHolder,
};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn dec(text: &str) -> Bson {
    Bson::Decimal128(text.parse().unwrap())
}

fn orders() -> CollectionId {
    CollectionId::derive("shop", "orders")
}

fn coll(e: &Engine) -> CollectionMeta {
    e.get_collection("shop", "orders").unwrap()
}

/// An engine with the collection, and a gate that is closed the way a clustered
/// member's is before `cluster.decimal128` is set.
fn closed() -> (Engine, tempfile::TempDir) {
    let (e, d) = engine();
    e.create_collection("shop", "orders").unwrap();
    close(&e);
    (e, d)
}

fn close(e: &Engine) {
    e.set_decimal128_gate(Arc::new(|| Err(Decimal128Closed::Undeclared)));
}

fn open(e: &Engine) {
    e.set_decimal128_gate(Arc::new(|| Ok(())));
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn pull(into: &Engine, from: &Engine) {
    let entries = from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    into.apply_batch(&entries).unwrap();
}

/// A sender whose gate is open and a requester whose gate is closed, which have
/// replicated everything so far.
fn pair() -> (Engine, Engine, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    a.create_collection("shop", "orders").unwrap();
    pull(&b, &a);
    open(&a);
    close(&b);
    (a, b, [da, db])
}

fn gated(e: StorageError, what: Decimal128Use) {
    match e {
        StorageError::Core(CoreError::Decimal128Gated { what: found, .. }) if found == what => {}
        other => panic!("expected the {what:?} gate, found {other:?}"),
    }
}

fn marked(e: &Engine) -> bool {
    e.decimal128_id_present().unwrap()
}

fn count(e: &Engine) -> u64 {
    e.count(&coll(e), WalkScope::Request).unwrap()
}

// -- The first chokepoint: check_insert ----------------------------------------------

#[test]
fn an_insert_of_a_decimal_id_is_refused_while_the_gate_is_closed() {
    let (e, _d) = closed();
    let refused = e.insert(&coll(&e), doc! { "_id": dec("1.5"), "v": 1 }).unwrap_err();
    gated(refused, Decimal128Use::Id);
    assert_eq!(count(&e), 0, "nothing was written");
    assert!(!marked(&e), "a refused insert sets no marker");

    open(&e);
    let id = e.insert(&coll(&e), doc! { "_id": dec("1.5"), "v": 1 }).unwrap();
    assert!(matches!(id, DocId::Decimal128(_)));
    assert!(marked(&e), "the first decimal row sets the marker in its own commit");
}

#[test]
fn an_integral_decimal_id_is_an_int64_and_is_still_gated_and_marked() {
    // `Decimal128("5")` is the document `_id: 5` as far as the key goes, but the body
    // keeps the decimal, which an older member cannot read: the BSON type decides.
    let (e, _d) = closed();
    for text in ["5", "0", "-0", "5.000"] {
        let refused = e.insert(&coll(&e), doc! { "_id": dec(text) }).unwrap_err();
        gated(refused, Decimal128Use::Id);
    }
    assert!(!marked(&e));
    open(&e);
    let id = e.insert(&coll(&e), doc! { "_id": dec("5") }).unwrap();
    assert_eq!(id, DocId::Int64(5), "normalised to the integer's id");
    let body = e.get(&coll(&e), &DocId::Int64(5)).unwrap().unwrap();
    assert_eq!(body.get("_id"), Some(&dec("5")), "the body keeps the decimal as written");
    assert!(marked(&e), "marked by the body's type, not the id's variant");
}

#[test]
fn the_gate_is_asked_about_decimal_ids_only() {
    let (e, _d) = engine();
    e.create_collection("shop", "orders").unwrap();
    let asked = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&asked);
    e.set_decimal128_gate(Arc::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
        Err(Decimal128Closed::Undeclared)
    }));
    let c = coll(&e);
    e.insert(&c, doc! { "_id": 1, "d": dec("1.5") }).unwrap();
    e.insert(&c, doc! { "_id": "s" }).unwrap();
    e.insert(&c, doc! { "v": 1 }).unwrap();
    e.replace(&c, &DocId::Int64(2), doc! { "d": dec("2.5") }, true).unwrap();
    assert_eq!(asked.load(Ordering::SeqCst), 0, "a decimal value elsewhere is not an id");
    assert!(!marked(&e), "and sets no marker");
}

#[test]
fn insert_many_names_the_decimal_document_and_writes_nothing() {
    let (e, _d) = closed();
    let refused = e
        .insert_many(
            &coll(&e),
            vec![doc! { "_id": 1 }, doc! { "_id": dec("2.5") }, doc! { "_id": 3 }],
        )
        .unwrap_err();
    assert_eq!(refused.index, Some(1), "the bulk insert names the document");
    gated(refused.source, Decimal128Use::Id);
    assert_eq!(count(&e), 0, "all or nothing");
    assert!(!marked(&e));

    open(&e);
    e.insert_many(&coll(&e), vec![doc! { "_id": 1 }, doc! { "_id": dec("2.5") }]).unwrap();
    assert!(marked(&e));
}

#[test]
fn a_scope_insert_and_a_scope_replace_are_gated() {
    let (e, _d) = closed();
    let c = coll(&e);
    let refused = e
        .write_batch(WriterHolder::Write, |scope| scope.insert(&c, doc! { "_id": dec("1.5") }))
        .unwrap_err();
    gated(refused, Decimal128Use::Id);
    let id = DocId::try_from_bson(&dec("2.5")).unwrap();
    let refused = e
        .write_batch(WriterHolder::Write, |scope| scope.replace(&c, &id, doc! { "v": 1 }, true))
        .unwrap_err();
    gated(refused, Decimal128Use::Id);
    assert_eq!(count(&e), 0);
    assert!(!marked(&e));
}

/// An upsert as `find_and_modify` builds one when nothing matches: the seed the
/// filter's equalities imply, with `$setOnInsert` and the update applied by the
/// query layer, exactly as the HTTP edge does it.
struct Upsert(Document);

impl crate::ModifySpec for Upsert {
    fn matches(&self, _: &Document) -> kimmy_core::Result<bool> {
        Ok(false)
    }
    fn compare(&self, _: &Document, _: &Document) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
        Ok(Some(doc.clone()))
    }
    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        Some(Ok(self.0.clone()))
    }
}

fn upsert_seed(id: Bson, update: Document) -> Document {
    let update = kimmy_query::update::parse(&update).unwrap();
    let mut seed = doc! { "_id": id };
    kimmy_query::update::apply_on_insert(&update, &mut seed, 0).unwrap();
    seed
}

#[test]
fn an_upsert_and_a_set_on_insert_that_create_a_decimal_id_are_refused() {
    let (e, _d) = closed();
    for seed in [
        upsert_seed(dec("3.5"), doc! { "$set": { "v": 1 } }),
        upsert_seed(dec("4"), doc! { "$setOnInsert": { "created_at": 1 }, "$inc": { "n": 1 } }),
    ] {
        let refused = e.find_and_modify(&coll(&e), &crate::Candidates::Scan, &Upsert(seed.clone()));
        gated(refused.unwrap_err(), Decimal128Use::Id);
    }
    assert_eq!(count(&e), 0);
    assert!(!marked(&e));

    open(&e);
    let seed = upsert_seed(dec("4"), doc! { "$setOnInsert": { "created_at": 1 } });
    let done = e.find_and_modify(&coll(&e), &crate::Candidates::Scan, &Upsert(seed)).unwrap();
    assert_eq!(done.upserted, Some(DocId::Int64(4)));
    assert!(marked(&e), "the upsert's commit carries the marker");
}

#[test]
fn a_duplicate_decimal_id_is_a_duplicate_key_while_the_gate_is_closed() {
    // A duplicate creates nothing, so the gate has nothing to answer: it is
    // `DuplicateKey`, as for any id, and as a replace over the same live row is
    // never gated. Nothing is written and no marker moves.
    let (e, _d) = engine();
    e.create_collection("shop", "orders").unwrap();
    let c = coll(&e);
    e.insert(&c, doc! { "_id": dec("1.5") }).unwrap();
    e.insert(&c, doc! { "_id": 5 }).unwrap();
    close(&e);
    let duplicate = |result: crate::Result<DocId>, what: &str| match result {
        Err(StorageError::Core(CoreError::DuplicateKey(_))) => {}
        other => panic!("{what}: expected DuplicateKey, found {other:?}"),
    };
    duplicate(e.insert(&c, doc! { "_id": dec("1.5") }), "the same decimal");
    duplicate(e.insert(&c, doc! { "_id": dec("1.50") }), "the same value in another cohort");
    duplicate(e.insert(&c, doc! { "_id": dec("5") }), "an integral decimal over the integer");
    let batch = e.insert_many(&c, vec![doc! { "_id": 9 }, doc! { "_id": dec("1.5") }]).unwrap_err();
    assert_eq!(batch.index, Some(1));
    assert!(matches!(batch.source, StorageError::Core(CoreError::DuplicateKey(_))), "{batch:?}");
    assert_eq!(count(&e), 2, "nothing was written");
    // A new decimal id is still refused, in a batch with a duplicate of itself too:
    // the first creates, and is the gate's.
    gated(e.insert(&c, doc! { "_id": dec("2.5") }).unwrap_err(), Decimal128Use::Id);
    let batch = e.insert_many(&c, vec![doc! { "_id": dec("3.5") }, doc! { "_id": dec("3.5") }]);
    let batch = batch.unwrap_err();
    assert_eq!(batch.index, Some(0));
    gated(batch.source, Decimal128Use::Id);
}

// -- The second chokepoint: replace_in_txn ------------------------------------------

#[test]
fn a_replace_that_creates_a_decimal_row_is_refused() {
    let (e, _d) = closed();
    let c = coll(&e);
    let id = DocId::try_from_bson(&dec("1.5")).unwrap();
    gated(e.replace(&c, &id, doc! { "v": 1 }, true).unwrap_err(), Decimal128Use::Id);
    // An integral one through the body's `_id`, which the replace keeps.
    let refused = e.replace(&c, &DocId::Int64(7), doc! { "_id": dec("7"), "v": 1 }, true);
    gated(refused.unwrap_err(), Decimal128Use::Id);
    // Without upsert nothing is created and nothing is refused.
    let missed = e.replace(&c, &id, doc! { "v": 1 }, false).unwrap();
    assert!(!missed.matched && !missed.upserted);
    assert_eq!(count(&e), 0);
    assert!(!marked(&e));
}

#[test]
fn a_replace_over_a_deleted_decimal_row_creates_and_is_refused() {
    let (e, _d) = engine();
    e.create_collection("shop", "orders").unwrap();
    let c = coll(&e);
    let id = e.insert(&c, doc! { "_id": dec("1.5") }).unwrap();
    e.delete(&c, &id).unwrap();
    close(&e);
    // Only a tombstone stands: the replace creates the row again.
    gated(e.replace(&c, &id, doc! { "v": 2 }, true).unwrap_err(), Decimal128Use::Id);
}

#[test]
fn a_replace_over_an_existing_decimal_id_is_not_refused() {
    // A document replicated from a member whose key is set can still be replaced on a
    // member whose key is not: the replace keeps the stored `_id` and creates nothing.
    let (e, _d) = engine();
    e.create_collection("shop", "orders").unwrap();
    let c = coll(&e);
    let fraction = e.insert(&c, doc! { "_id": dec("1.5"), "v": 1 }).unwrap();
    let integral = e.insert(&c, doc! { "_id": dec("5"), "v": 1 }).unwrap();
    close(&e);
    for upsert in [false, true] {
        let done = e.replace(&c, &fraction, doc! { "v": 2 }, upsert).unwrap();
        assert!(done.matched && !done.upserted, "upsert {upsert}");
        // The body names the same id as an `Int32`; the stored decimal is kept.
        let done = e.replace(&c, &integral, doc! { "_id": 5, "v": 2 }, upsert).unwrap();
        assert!(done.matched && !done.upserted, "upsert {upsert}");
    }
    let kept = e.get(&c, &integral).unwrap().unwrap();
    assert_eq!(kept.get("_id"), Some(&dec("5")), "the stored type is kept");
}

#[test]
fn a_replace_naming_a_decimal_over_a_stored_integer_keeps_the_integer() {
    // The reverse: the stored `_id: 5` is kept, so no decimal is created or stored.
    let (e, _d) = closed();
    let c = coll(&e);
    e.insert(&c, doc! { "_id": 5, "v": 1 }).unwrap();
    let done = e.replace(&c, &DocId::Int64(5), doc! { "_id": dec("5"), "v": 2 }, true).unwrap();
    assert!(done.matched);
    assert_eq!(e.get(&c, &DocId::Int64(5)).unwrap().unwrap().get("_id"), Some(&Bson::Int32(5)));
    assert!(!marked(&e));
}

// -- The marker on every replicated route --------------------------------------------

#[test]
fn an_applied_replicated_decimal_document_is_never_refused_and_marks() {
    let (a, b, _d) = pair();
    let id = a.insert(&coll(&a), doc! { "_id": dec("1.5"), "v": 1 }).unwrap();
    assert!(!marked(&b));
    pull(&b, &a);
    assert!(b.get(&coll(&b), &id).unwrap().is_some(), "applied whatever the gate says");
    assert!(marked(&b), "the apply sets the marker");
}

#[test]
fn an_applied_int64_normalised_document_with_a_decimal_body_marks() {
    let (a, b, _d) = pair();
    let id = a.insert(&coll(&a), doc! { "_id": dec("7"), "v": 1 }).unwrap();
    assert_eq!(id, DocId::Int64(7), "the entry carries an Int64 id");
    pull(&b, &a);
    assert!(marked(&b), "keyed on the body's BSON type, not the entry's id variant");
}

#[test]
fn an_applied_delete_of_a_decimal_id_marks() {
    // A member that meets only the delete still holds an entry naming the decimal id.
    let (a, b, _d) = pair();
    let c = coll(&a);
    let id = a.insert(&c, doc! { "_id": dec("1.5") }).unwrap();
    a.delete(&c, &id).unwrap();
    let entries = a.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    let deletes: Vec<_> =
        entries.into_iter().filter(|e| e.kind == kimmy_core::OpKind::Delete).collect();
    assert_eq!(deletes.len(), 1);
    b.apply_batch(&deletes).unwrap();
    assert!(marked(&b));
}

#[test]
fn a_whole_database_snapshot_marks() {
    let (a, b, _d) = pair();
    a.insert(&coll(&a), doc! { "_id": dec("7"), "v": 1 }).unwrap();
    let mut progress = SnapshotProgress::whole_database();
    while !progress.is_complete() {
        let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    }
    assert_eq!(count(&b), 1);
    assert!(marked(&b));
}

#[test]
fn a_converge_row_applied_over_a_held_row_marks() {
    // A member that holds the row without the marker (it met it before it ran 0.51)
    // is marked by the repair that writes it again.
    let (a, b, _d) = pair();
    let c = coll(&a);
    let id = a.insert(&c, doc! { "_id": dec("1.5"), "v": 1 }).unwrap();
    pull(&b, &a);
    b.test_remove_decimal128_marker();
    later();
    a.replace(&c, &id, doc! { "v": 2 }, false).unwrap();
    let mut progress =
        SnapshotProgress::of_collection_converging(orders(), kimmy_core::VersionVector::new());
    let mut applied = 0;
    while !progress.is_complete() {
        let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        applied += b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap().applied;
    }
    assert_eq!(applied, 1, "the newer row was applied");
    assert!(marked(&b));
}

#[test]
fn a_backup_carries_the_marker() {
    let (e, _d) = engine();
    e.create_collection("shop", "orders").unwrap();
    e.insert(&coll(&e), doc! { "_id": dec("1.5") }).unwrap();
    let mut buf = Vec::new();
    e.backup_to(&mut buf, WalkScope::Request).unwrap();
    let dest = tempfile::tempdir().unwrap();
    let path = dest.path().join("restored.redb");
    crate::backup::restore(&path, &mut buf.as_slice()).unwrap();
    let restored = Engine::open(&path).unwrap();
    assert!(restored.decimal128_id_present().unwrap(), "the restore carries the marker");

    // And a store with no decimal row has none to carry.
    let (plain, _p) = engine();
    plain.create_collection("shop", "orders").unwrap();
    plain.insert(&coll(&plain), doc! { "_id": 1, "d": dec("1.5") }).unwrap();
    let mut buf = Vec::new();
    plain.backup_to(&mut buf, WalkScope::Request).unwrap();
    let path = dest.path().join("plain.redb");
    crate::backup::restore(&path, &mut buf.as_slice()).unwrap();
    assert!(!Engine::open(&path).unwrap().decimal128_id_present().unwrap());
}

// -- The third chokepoint: a local index whose partial filter holds a decimal -------

fn bounded(e: &Engine, filter: Document) -> crate::Result<crate::IndexMeta> {
    e.create_index_with(
        "shop",
        "orders",
        vec![IndexField::ascending("email")],
        false,
        crate::Enforcement::Local,
        Some("positive".into()),
        None,
        Some(filter),
    )
}

fn names(e: &Engine) -> Vec<String> {
    e.list_indexes("shop", "orders").unwrap().into_iter().map(|i| i.name).collect()
}

#[test]
fn a_local_decimal_bound_index_is_refused_while_the_gate_is_closed() {
    let (e, _d) = closed();
    for filter in [
        doc! { "balance": { "$gt": dec("0") } },
        doc! { "balance": dec("1.5") },
        doc! { "ok": { "$exists": true }, "balance": { "$eq": { "n": dec("1") } } },
    ] {
        gated(bounded(&e, filter).unwrap_err(), Decimal128Use::PartialFilterBound);
    }
    assert!(!names(&e).contains(&"positive".to_string()), "nothing was created");
    // A filter without a decimal is not asked.
    bounded(&e, doc! { "balance": { "$gt": 0 } }).unwrap();
    e.drop_index("shop", "orders", "positive").unwrap();

    open(&e);
    let index = bounded(&e, doc! { "balance": { "$gt": dec("0") } }).unwrap();
    assert_eq!(index.partial_filter, Some(doc! { "balance": { "$gt": dec("0") } }));
}

#[test]
fn a_replicated_decimal_bound_definition_is_applied_with_the_gate_closed() {
    let (a, b, _d) = pair();
    a.insert(&coll(&a), doc! { "_id": 1, "email": "x", "balance": dec("10") }).unwrap();
    bounded(&a, doc! { "balance": { "$gt": dec("0") } }).unwrap();
    pull(&b, &a);
    assert!(names(&b).contains(&"positive".to_string()), "the peer's definition is built");
}

#[test]
fn a_restored_decimal_bound_definition_is_applied_with_the_gate_closed() {
    let (a, b, _d) = pair();
    bounded(&a, doc! { "balance": { "$gt": dec("0") } }).unwrap();
    let mut progress = SnapshotProgress::whole_database();
    while !progress.is_complete() {
        let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    }
    assert!(names(&b).contains(&"positive".to_string()), "a snapshot's definition is built");
}

#[test]
fn a_repaired_decimal_bound_definition_is_built_with_the_gate_closed() {
    let (a, b, _d) = pair();
    a.insert(&coll(&a), doc! { "_id": 1, "email": "x", "balance": dec("10") }).unwrap();
    pull(&b, &a);
    later();
    bounded(&a, doc! { "balance": { "$gt": dec("0") } }).unwrap();
    let mut progress = SnapshotProgress::of_collection_definitions(orders(), 1_000_000);
    let page = a.snapshot_page(None, progress.scope()).unwrap();
    let done = b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    assert_eq!(done.definitions.built, 1, "{done:?}");
    assert!(names(&b).contains(&"positive".to_string()), "the repair built it");
}

#[test]
fn an_identical_recreate_of_a_replicated_decimal_bound_index_stands_with_the_gate_closed() {
    // A client that creates its indexes on every boot, on a member whose key is not
    // set, re-creates a definition the member already holds by replication: the
    // idempotent answer, as for any index, and nothing minted.
    let (a, b, _d) = pair();
    let held = bounded(&a, doc! { "balance": { "$gt": dec("0") } }).unwrap();
    pull(&b, &a);
    let before = b.oplog_entries().unwrap();
    let again = bounded(&b, doc! { "balance": { "$gt": dec("0") } }).unwrap();
    assert_eq!(again.created, held.created, "the standing definition, not a new one");
    assert_eq!(b.oplog_entries().unwrap(), before, "nothing minted");
    // A different definition under the name is `IndexExists`, as for any index:
    // nothing would be minted either.
    match bounded(&b, doc! { "balance": { "$gt": dec("1") } }) {
        Err(StorageError::Core(CoreError::IndexExists { .. })) => {}
        other => panic!("expected IndexExists, found {other:?}"),
    }
    // A new decimal-bound definition under another name is still the gate's.
    let refused = b.create_index_with(
        "shop",
        "orders",
        vec![IndexField::ascending("email")],
        false,
        crate::Enforcement::Local,
        Some("other".into()),
        None,
        Some(doc! { "balance": { "$lt": dec("0") } }),
    );
    gated(refused.unwrap_err(), Decimal128Use::PartialFilterBound);
}

#[test]
fn the_restamp_of_an_inherited_decimal_bound_index_is_the_gates() {
    // An identical definition inherited from the collection's earlier life is
    // re-stamped by a local create: a new creation, replicated as one, so it waits
    // for the gate like a new definition.
    let (a, da) = engine();
    let (b, db) = engine();
    let _dirs = (da, db);
    let on_a = a.create_collection("shop", "orders").unwrap();
    open(&a);
    bounded(&a, doc! { "balance": { "$gt": dec("0") } }).unwrap();
    a.insert(&on_a, doc! { "_id": 1i64, "balance": 1 }).unwrap();
    assert!(a.drop_collection("shop", "orders").unwrap());
    a.finish_purges_now().unwrap();
    later();
    let on_b = b.create_collection("shop", "orders").unwrap();
    b.insert(&on_b, doc! { "_id": 2i64 }).unwrap();
    let start = b.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
    let entries = a.entries_for_peer(start, 10_000, WalkScope::Background).unwrap().entries;
    let drop_at = entries.iter().position(|e| e.kind == kimmy_core::OpKind::DropCollection);
    b.apply_batch(&entries[..drop_at.expect("the drop is in the window")]).unwrap();
    let inherited = b.get_collection("shop", "orders").unwrap();
    assert!(inherited.indexes[0].created.unwrap().hlc < inherited.created, "premise: inherited");

    close(&b);
    let refused = bounded(&b, doc! { "balance": { "$gt": dec("0") } });
    gated(refused.unwrap_err(), Decimal128Use::PartialFilterBound);
    let after = b.get_collection("shop", "orders").unwrap();
    assert_eq!(after.indexes[0].created, inherited.indexes[0].created, "not re-stamped");

    open(&b);
    bounded(&b, doc! { "balance": { "$gt": dec("0") } }).unwrap();
    let after = b.get_collection("shop", "orders").unwrap();
    assert!(after.indexes[0].created > inherited.indexes[0].created, "re-stamped once allowed");
}
