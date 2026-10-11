//! A database drop stays dropped on every member (ADR-229).
//!
//! Each test builds the sequence the 0.26.0 field case had: one member drops a
//! database while a peer holds a collection the dropper never received. They
//! use only what the engine offers, so a test here that is red names a
//! behaviour, not a missing function.

use std::sync::Arc;
use std::time::Duration;

use bson::doc;
use kimmy_core::{CollectionId, CollectionRef, Hlc, NodeId, OpKind, Stamp};

use crate::{Engine, SnapshotProgress, SyncOutcome};

const BATCH: usize = 1024;

/// The reserved collection a database's tombstone is filed under.
const RESERVED: &str = "__dropped_database";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    (engine, dir)
}

/// One direction of an anti-entropy round: pull into `into` from `from`.
fn pull(into: &Engine, from: &Engine) -> SyncOutcome {
    let mine = into.version_vector().unwrap();
    let theirs = from.version_vector().unwrap();
    match mine.behind(&theirs) {
        Some(start) => {
            let entries =
                from.entries_for_peer(start, BATCH, crate::WalkScope::Background).unwrap().entries;
            into.apply_batch(&entries).unwrap()
        }
        None => SyncOutcome::default(),
    }
}

fn sync(a: &Engine, b: &Engine) {
    pull(a, b);
    pull(b, a);
}

/// Long enough that the next stamp is on a later millisecond than any minted
/// before it, so "created after the drop" is a fact about stamps and not luck.
fn later() {
    std::thread::sleep(Duration::from_millis(5));
}

fn holds(engine: &Engine, db: &str, name: &str) -> bool {
    engine.get_collection(db, name).is_ok()
}

fn vector_config() -> kimmy_core::VectorConfig {
    kimmy_core::VectorConfig {
        fields: vec!["text".into()],
        provider: kimmy_core::ProviderConfig::Byo {},
        dim: 4,
        metric: Default::default(),
        document_prefix: None,
        query_prefix: None,
        chunk: Default::default(),
    }
}

/// A database dropped on `a`, where `b` holds `shop.late`, which `a` never
/// received: `a` and `b` share `shop.orders`, `b` then creates `late` and
/// writes a document into it, and only then does `a` drop the database.
fn the_field_sequence() -> (Engine, Engine, tempfile::TempDir, tempfile::TempDir, CollectionId) {
    let (a, da) = engine();
    let (b, db) = engine();
    a.create_collection("shop", "orders").unwrap();
    sync(&a, &b);
    let late = b.create_collection("shop", "late").unwrap();
    b.insert(&late, doc! { "_id": "x" }).unwrap();
    later();
    assert!(a.drop_database("shop").unwrap());
    (a, b, da, db, late.id)
}

#[test]
fn a_database_drop_is_not_undone_by_a_collection_the_dropper_never_held() {
    let (a, b, _da, _db, _late) = the_field_sequence();
    sync(&a, &b);
    assert!(!a.database_exists("shop").unwrap(), "the dropper listed {:?}", a.list_databases());
    assert!(!holds(&a, "shop", "late"));
    assert!(!b.database_exists("shop").unwrap(), "the peer listed {:?}", b.list_databases());
}

#[test]
fn a_database_drop_buries_what_a_peer_holds_that_was_created_before_it() {
    let (a, b, _da, _db, late) = the_field_sequence();
    let (c, _dc) = engine();
    // A third member that holds both, and hears the drop last.
    pull(&c, &b);
    assert!(holds(&c, "shop", "late"));
    sync(&a, &b);
    sync(&a, &c);
    sync(&b, &c);
    for (name, member) in [("a", &a), ("b", &b), ("c", &c)] {
        assert!(!holds(member, "shop", "late"), "{name} still holds shop.late");
        assert!(!member.database_exists("shop").unwrap(), "{name} still lists shop");
        assert!(member.collection_dropped_at(late).unwrap().is_some(), "{name} holds no tombstone");
    }
}

/// Control: a collection created after the drop stands and replicates. The
/// pair with the two tests above is what tells the fix from "refuse every
/// collection of a database that was ever dropped".
#[test]
fn a_collection_created_after_a_database_drop_stands_and_replicates() {
    let (a, b, _da, _db, _late) = the_field_sequence();
    sync(&a, &b);
    later();
    let fresh = b.create_collection("shop", "fresh").unwrap();
    b.insert(&fresh, doc! { "_id": "y" }).unwrap();
    sync(&a, &b);
    for member in [&a, &b] {
        assert!(holds(member, "shop", "fresh"));
        assert!(member.database_exists("shop").unwrap());
    }
}

#[test]
fn a_create_older_than_a_database_drop_is_history_and_its_documents_are_superseded() {
    let (a, b, _da, _db, late) = the_field_sequence();
    let (c, _dc) = engine();
    // `c` hears the drop first and never held anything. Then the collection
    // the dropper never held arrives, create first and documents after.
    pull(&c, &a);
    let outcome = pull(&c, &b);
    assert!(!holds(&c, "shop", "late"), "an older create recreated the collection");
    assert!(!c.database_exists("shop").unwrap());
    assert!(
        c.collection_dropped_at(late).unwrap().is_some(),
        "the refusal must leave the collection's own tombstone, or its documents stop the batch"
    );
    assert_eq!(outcome.unknown_collection, 0, "{:?}", outcome.unknown);
    // A creation judged history is not served onward, as one older than the
    // collection's own tombstone is not.
    let created = b
        .entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .find(|e| e.kind == OpKind::CreateCollection && e.collection == late)
        .expect("b holds the creation");
    assert!(c.oplog_entry(&created.stamp).unwrap().is_none(), "a refused creation was kept");

    // And more of the old life, after the create was refused, is history, not a stop.
    let bl = b.get_collection("shop", "late").unwrap();
    b.insert(&bl, doc! { "_id": "z" }).unwrap();
    let outcome = pull(&c, &b);
    assert_eq!(outcome.unknown_collection, 0, "{:?}", outcome.unknown);
    assert!(!holds(&c, "shop", "late"));
}

#[test]
fn a_collection_recreated_under_a_dropped_database_is_floored_at_the_drop() {
    let (a, b, _da, _db, late) = the_field_sequence();
    let (c, _dc) = engine();
    pull(&c, &a);
    later();
    // The operator creates the database again on `c`, then the old life's
    // documents, stamped before the drop, arrive from `b`.
    let fresh = c.create_collection("shop", "late").unwrap();
    c.insert(&fresh, doc! { "_id": "mine" }).unwrap();
    pull(&c, &b);
    assert_eq!(
        c.count(&c.get_collection("shop", "late").unwrap(), crate::WalkScope::Request).unwrap(),
        1
    );
    assert_eq!(late, fresh.id, "the same name derives the same id");
}

/// Control: a drop that reaches a member after it has made a newer life of the
/// name does not take that life.
#[test]
fn a_drop_arriving_after_a_newer_life_does_not_take_it() {
    let (a, _da) = engine();
    let (c, _dc) = engine();
    a.create_collection("shop", "orders").unwrap();
    sync(&a, &c);
    assert!(a.drop_database("shop").unwrap());
    later();
    let again = c.create_collection("shop", "orders");
    // `c` still holds the first life (it has not heard the drop), so the
    // create is refused as existing; drop it locally first to make a newer life.
    assert!(again.is_err());
    assert!(c.drop_collection("shop", "orders").unwrap());
    later();
    let second = c.create_collection("shop", "orders").unwrap();
    c.insert(&second, doc! { "_id": "kept" }).unwrap();
    sync(&a, &c);
    assert!(holds(&c, "shop", "orders"), "a drop of an earlier life took the newer one");
    assert_eq!(
        c.count(&c.get_collection("shop", "orders").unwrap(), crate::WalkScope::Request).unwrap(),
        1
    );
}

/// The head of a database drop is committed before the first collection is
/// buried, so a peer's create that lands after the last burial and before the
/// database row goes is refused. (At the burials themselves the drop's second
/// listing buries whatever stands, which hides the gap.)
#[test]
fn a_database_drop_records_its_tombstone_before_the_first_burial() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let a = Arc::new(a);
    a.create_collection("shop", "orders").unwrap();
    sync(&a, &b);
    let late = b.create_collection("shop", "late").unwrap();
    b.insert(&late, doc! { "_id": "x" }).unwrap();
    later();
    let theirs = b.version_vector().unwrap();
    let entries =
        b.entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background).unwrap().entries;
    let _ = theirs;
    let landing = Arc::clone(&a);
    let (dropped, applied) = crate::sync::race_hooks::race(
        crate::sync::race_hooks::Race::BeforeRowRemoval,
        move || landing.apply_batch(&entries),
        || a.drop_database("shop"),
    );
    assert!(dropped.unwrap());
    applied.unwrap();
    assert!(!holds(&a, "shop", "late"), "a create between the head and the burial stood");
    assert!(!a.database_exists("shop").unwrap());
}

/// Control: a database the member does not hold is not dropped by it, and
/// nothing is written.
#[test]
fn a_drop_of_a_database_the_member_does_not_hold_mints_nothing() {
    let (a, _da) = engine();
    let before = a.version_vector().unwrap();
    assert!(!a.drop_database("nothing").unwrap());
    assert_eq!(a.version_vector().unwrap(), before);
}

/// The database tombstone rides a snapshot page like any collection tombstone,
/// and the receiver drops what it covers before it restores definitions.
#[test]
fn a_whole_database_page_buries_what_the_senders_tombstone_covers() {
    let (a, b, _da, _db, _late) = the_field_sequence();
    let (c, _dc) = engine();
    pull(&c, &b);
    assert!(holds(&c, "shop", "late") && holds(&c, "shop", "orders"));
    let page = a.snapshot_page(None, None).unwrap();
    c.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    assert!(!holds(&c, "shop", "late"), "the page left a collection created before the drop");
    assert!(!c.database_exists("shop").unwrap());
}

#[test]
fn a_definition_older_than_the_tombstone_is_not_restored_and_its_entries_are_history() {
    let (a, b, _da, _db, late) = the_field_sequence();
    let page = b.snapshot_page(None, None).unwrap();
    assert!(page.collections.iter().any(|c| c.name == "late"), "premise: the page defines late");
    a.apply_snapshot_page(b.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    assert!(!holds(&a, "shop", "late"), "a snapshot recreated a collection the drop covers");
    assert!(
        a.collection_dropped_at(late).unwrap().is_some(),
        "the refusal must leave the collection's own tombstone"
    );
    // Later entries for the refused collection are history, not a stop.
    let bl = b.get_collection("shop", "late").unwrap();
    b.insert(&bl, doc! { "_id": "more" }).unwrap();
    let outcome = pull(&a, &b);
    assert_eq!(outcome.unknown_collection, 0, "{:?}", outcome.unknown);
    assert!(!holds(&a, "shop", "late"));
}

/// The pre-existing defect the same change fixes: a never-held collection with
/// vectors on stopped a batch at its shadow's first chunk, because the drop
/// that arrived first recorded only the parent's id.
#[test]
fn a_never_held_vector_configured_collections_shadow_inserts_are_superseded_after_its_drop() {
    let (b, _db) = engine();
    let (d, _dd) = engine();
    let (a, _da) = engine();
    b.create_collection("shop", "orders").unwrap();
    b.configure_vectors("shop", "orders", vector_config()).unwrap();
    let shadow = b.vector_collection("shop", "orders").unwrap().unwrap();
    let source = kimmy_core::DocId::Int64(1);
    b.put_vectors(
        &shadow,
        &source,
        &[kimmy_core::VectorRecord {
            source: source.clone(),
            chunk: 0,
            source_hlc: Hlc::new(1, 0),
            vector: vec![1.0, 0.0, 0.0, 0.0],
            text: "t".into(),
            config: None,
        }],
    )
    .unwrap();
    pull(&d, &b);
    later();
    assert!(d.drop_collection("shop", "orders").unwrap());
    // `a` hears the drop of a collection it never held, then the collection's
    // own history.
    let drops: Vec<_> = d
        .entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .filter(|e| e.kind == OpKind::DropCollection)
        .collect();
    assert!(!drops.is_empty());
    a.apply_batch(&drops).unwrap();
    let history =
        b.entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background).unwrap().entries;
    let outcome = a.apply_batch(&history).unwrap();
    assert_eq!(
        outcome.unknown_collection, 0,
        "the shadow's chunk stopped the batch: {:?}",
        outcome.unknown
    );
}

#[test]
fn a_backup_restores_the_database_tombstone_and_the_restored_member_refuses_the_late_create() {
    let (a, b, da, _db, _late) = the_field_sequence();
    let mut bytes = Vec::new();
    a.backup_to(&mut bytes, crate::WalkScope::Request).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("restored.redb");
    crate::backup::restore(&path, &mut &bytes[..]).unwrap();
    drop(da);
    let restored = Engine::open(&path).unwrap();
    pull(&restored, &b);
    assert!(!holds(&restored, "shop", "late"), "a restored member recreated the dropped database");
    assert!(!restored.database_exists("shop").unwrap());
}

/// Control, and the compatibility pin: the entry a 0.47.0 member sends for a
/// database drop is an ordinary `DropCollection` of a collection named by a
/// reserved name, which a 0.46.0 member applies as the drop of a collection it
/// never held. Written against what 0.46.0 has, so it runs unchanged there: the
/// pull request records the result of copying it into a 0.46.0 worktree.
#[test]
fn the_sentinel_is_an_ordinary_drop_collection_entry() {
    let (b, _db) = engine();
    let (c, _dc) = engine();
    let orders = b.create_collection("shop", "orders").unwrap();
    b.insert(&orders, doc! { "_id": "1" }).unwrap();
    let origin = NodeId::from_bytes([9; 16]);
    // Older than the collection, so a member that sweeps under it spares the
    // collection and the assertions below hold on either build.
    let hlc = Hlc::new(1_000, 0);
    let id = CollectionId::derive("shop", RESERVED);
    let entry = crate::engine::ddl_entry(
        Stamp::new(hlc, origin),
        OpKind::DropCollection,
        id,
        &CollectionRef::new("shop", RESERVED),
    )
    .unwrap();
    let encoded = crate::codec::encode_oplog_entry(&entry);
    assert_eq!(crate::codec::decode_oplog_entry(&encoded).unwrap(), entry);

    b.apply_batch(std::slice::from_ref(&entry)).unwrap();
    assert_eq!(b.collection_dropped_at(id).unwrap(), Some(entry.stamp));
    assert!(
        holds(&b, "shop", "orders"),
        "the collections of the database are not the entry's to drop"
    );

    let window = b.entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background).unwrap();
    assert!(window.entries.iter().any(|e| e.collection == id), "served onward");
    c.apply_batch(&window.entries).unwrap();
    assert_eq!(c.collection_dropped_at(id).unwrap(), Some(entry.stamp));
}

// ---------------------------------------------------------------------------
// The sweep, its retries, its refusals and its ties
// ---------------------------------------------------------------------------

fn stamp_of(hlc: u64, node: u8) -> Stamp {
    Stamp::new(Hlc::new(hlc, 0), NodeId::from_bytes([node; 16]))
}

fn create_entry(db: &str, name: &str, at: Stamp) -> kimmy_core::OplogEntry {
    crate::engine::ddl_entry(
        at,
        OpKind::CreateCollection,
        CollectionId::derive(db, name),
        &CollectionRef::new(db, name),
    )
    .unwrap()
}

fn drop_database_entry(db: &str, at: Stamp) -> kimmy_core::OplogEntry {
    crate::engine::ddl_entry(
        at,
        OpKind::DropCollection,
        kimmy_core::database_drop_id(db),
        &CollectionRef::new(db, RESERVED),
    )
    .unwrap()
}

/// Review M2. Three members and a fourth that refused the creation: a
/// collection created after the drop, on a member that had not heard of it,
/// refuses a straggler of the life before it exactly as the member that
/// refused the creation first and accepted the new one later does.
#[test]
fn a_survivor_of_the_sweep_refuses_a_pre_drop_straggler_like_every_other_member() {
    let (z, _dz) = engine();
    let (a, _da) = engine();
    let (y, _dy) = engine();
    let (m, _dm) = engine();
    let zl = z.create_collection("shop", "late").unwrap();
    z.insert(&zl, doc! { "_id": "d1" }).unwrap();
    a.create_collection("shop", "orders").unwrap();
    later();
    assert!(a.drop_database("shop").unwrap());
    later();
    // `y` makes the name again before it has heard of the drop.
    let yl = y.create_collection("shop", "late").unwrap();
    y.insert(&yl, doc! { "_id": "y1" }).unwrap();

    pull(&m, &a);
    pull(&m, &z);
    later();
    pull(&m, &y);

    pull(&y, &a);
    pull(&y, &z);

    let has = |e: &Engine, id: &str| -> bool {
        let meta = e.get_collection("shop", "late").unwrap();
        e.get(&meta, &kimmy_core::DocId::String(id.into())).unwrap().is_some()
    };
    assert!(has(&m, "y1") && !has(&m, "d1"), "the member that refused the creation");
    assert!(has(&y, "y1"), "the survivor lost its own document");
    assert!(!has(&y, "d1"), "the survivor took a straggler of the life before it");
}

/// Reviews m1 and F5: a creation and a drop stamped with the same `Hlc` agree
/// on every member whichever arrives first, whichever node sorts higher.
#[test]
fn a_create_and_a_database_drop_with_equal_hlc_agree_in_both_arrival_orders() {
    for (create_node, drop_node) in [(2u8, 1u8), (1u8, 2u8)] {
        let create = create_entry("shop", "late", stamp_of(5_000, create_node));
        let drop = drop_database_entry("shop", stamp_of(5_000, drop_node));
        for create_first in [true, false] {
            let (e, _d) = engine();
            let order = if create_first { [&create, &drop] } else { [&drop, &create] };
            for entry in order {
                e.apply_batch(std::slice::from_ref(entry)).unwrap();
            }
            assert!(
                !holds(&e, "shop", "late"),
                "create node {create_node}, drop node {drop_node}, create first: {create_first}"
            );
            assert!(!e.database_exists("shop").unwrap());
        }
    }
}

#[test]
fn a_replicated_create_of_the_reserved_name_is_refused() {
    let (e, _d) = engine();
    let entry = create_entry("shop", RESERVED, stamp_of(5_000, 3));
    e.apply_batch(std::slice::from_ref(&entry)).unwrap();
    assert!(!holds(&e, "shop", RESERVED));
    assert!(!e.database_exists("shop").unwrap());
}

#[test]
fn the_floor_of_a_recreated_collection_is_the_larger_of_the_two_tombstones() {
    let (a, b, _da, _db, _late) = the_field_sequence();
    let (c, _dc) = engine();
    pull(&c, &a);
    later();
    let first = c.create_collection("shop", "late").unwrap();
    let database = c.database_dropped_at("shop").unwrap().unwrap();
    assert_eq!(first.incarnation_floor, Some(database.hlc));
    later();
    assert!(c.drop_collection("shop", "late").unwrap());
    let collection = c.collection_dropped_at(first.id).unwrap().unwrap();
    assert!(collection > database);
    later();
    let second = c.create_collection("shop", "late").unwrap();
    assert_eq!(second.incarnation_floor, Some(collection.hlc), "the newer tombstone is the floor");
    drop(b);
}

/// The drop's tombstone committed and the burials did not: the next start
/// finishes them.
#[test]
fn a_crash_between_the_head_and_the_burials_is_finished_by_the_reconcile() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let engine = Arc::new(Engine::open(&path).unwrap());
        for name in ["a", "b"] {
            engine.create_collection("shop", name).unwrap();
        }
        let stopper = Arc::clone(&engine);
        crate::sync::race_hooks::at(crate::sync::race_hooks::Race::Burial, move || {
            stopper.set_stopping();
        });
        let failed = engine.drop_database("shop").unwrap_err();
        assert!(matches!(failed, crate::StorageError::PartiallyApplied { .. }), "{failed:?}");
    }
    let engine = Engine::open(&path).unwrap();
    assert!(engine.database_dropped_at("shop").unwrap().is_some(), "the head is durable");
    assert!(holds(&engine, "shop", "a") && holds(&engine, "shop", "b"), "no burial landed");
    let swept = engine.reconcile_database_tombstones().unwrap();
    assert_eq!(swept.buried.len(), 2);
    assert!(!engine.database_exists("shop").unwrap());
}

/// What a 0.46.0 member leaves: the tombstone, recorded as a plain drop of a
/// collection it never held, over collections it still holds.
#[test]
fn a_member_that_took_the_drop_as_a_plain_collection_drop_buries_under_it_on_reconcile() {
    let (e, _d) = engine();
    e.create_collection("shop", "orders").unwrap();
    later();
    let after =
        Stamp::new(Hlc::new(crate::physical_now_ms() + 1_000, 0), NodeId::from_bytes([7; 16]));
    e.record_collection_drop(kimmy_core::database_drop_id("shop"), after).unwrap();
    assert!(holds(&e, "shop", "orders"), "nothing has acted on the tombstone yet");
    let swept = e.reconcile_database_tombstones().unwrap();
    assert_eq!(swept.buried.len(), 1);
    assert!(!holds(&e, "shop", "orders"));
    assert!(!e.database_exists("shop").unwrap());
}

/// Review M1: the page that recorded the tombstone is retried after a sweep
/// that failed; it carries nothing new, and must still bury.
#[test]
fn a_retried_page_still_buries_under_a_tombstone_an_earlier_attempt_recorded() {
    let (a, b, _da, _db, _late) = the_field_sequence();
    let (c, _dc) = engine();
    pull(&c, &b);
    assert!(holds(&c, "shop", "late"));
    let page = a.snapshot_page(None, None).unwrap();
    let drop = a.database_dropped_at("shop").unwrap().unwrap();
    // The first attempt: the tombstone committed, the sweep did not.
    c.record_collection_drop(kimmy_core::database_drop_id("shop"), drop).unwrap();
    assert!(holds(&c, "shop", "late"));
    c.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    assert!(!holds(&c, "shop", "late"), "a retried page left what the tombstone covers");
}

#[test]
fn a_tombstone_the_senders_coverage_does_not_name_buries_nothing() {
    let (a, b, _da, _db, _late) = the_field_sequence();
    let (c, _dc) = engine();
    pull(&c, &b);
    let mut page = a.snapshot_page(None, None).unwrap();
    page.versions = kimmy_core::VersionVector::new();
    c.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    assert!(
        holds(&c, "shop", "late"),
        "a denial above the sender's coverage destroyed a collection"
    );
    assert!(c.database_dropped_at("shop").unwrap().is_none());
}

#[test]
fn a_page_carries_the_database_tombstone() {
    let (a, _b, _da, _db, _late) = the_field_sequence();
    let page = a.snapshot_page(None, None).unwrap();
    let id = kimmy_core::database_drop_id("shop");
    assert!(page.dropped_collections.iter().any(|(held, _)| *held == id), "{page:?}");
}

/// The sweep is skipped when nothing it reads has changed, and not memoised
/// when it fails.
#[test]
fn a_sweep_of_an_unchanged_database_does_not_walk_its_collections_again() {
    let (a, _da) = engine();
    let (c, _dc) = engine();
    a.create_collection("shop", "orders").unwrap();
    later();
    assert!(a.drop_database("shop").unwrap());
    later();
    let fresh = c.create_collection("shop", "late").unwrap();
    c.insert(&fresh, doc! { "_id": 1 }).unwrap();
    pull(&c, &a);
    // A sweep's own record moves the generation, so the first sweep after it
    // walks once more, finds nothing to write and is memoised; from then on a
    // sweep walks nothing.
    let settled = (0..3).find(|_| c.bury_under_database_tombstones().unwrap().spared == 0);
    assert!(settled.is_some(), "the sweep never settled");
    assert_eq!(c.bury_under_database_tombstones().unwrap().spared, 0);
    // A creation moves the generation, so the next sweep walks again and finds
    // both survivors, then settles again.
    later();
    c.create_collection("shop", "newer").unwrap();
    assert_eq!(c.bury_under_database_tombstones().unwrap().spared, 2);
    let settled = (0..3).find(|_| c.bury_under_database_tombstones().unwrap().spared == 0);
    assert!(settled.is_some());
}

#[test]
fn a_failed_sweep_is_not_memoised() {
    let (a, _da) = engine();
    let (c, _dc) = engine();
    a.create_collection("shop", "orders").unwrap();
    later();
    assert!(a.drop_database("shop").unwrap());
    later();
    c.create_collection("shop", "late").unwrap();
    // The tombstone is here and the sweep stops at its first checkpoint.
    c.record_collection_drop(
        kimmy_core::database_drop_id("shop"),
        a.database_dropped_at("shop").unwrap().unwrap(),
    )
    .unwrap();
    // Past the first checkpoint, so the failure comes after the walk has begun.
    let calls = std::cell::Cell::new(0);
    let stopped = c.sweep(&|| {
        calls.set(calls.get() + 1);
        if calls.get() > 1 {
            return Err(crate::StorageError::Stopping(crate::StopReason::Shutdown));
        }
        Ok(())
    });
    assert!(stopped.is_err());
    assert_eq!(c.bury_under_database_tombstones().unwrap().spared, 1, "the retry walked");
}

#[test]
fn a_burial_that_loses_documents_logs_their_number() {
    let (a, b, _da, _db, _late) = the_field_sequence();
    let (c, _dc) = engine();
    pull(&c, &b);
    let late = c.get_collection("shop", "late").unwrap();
    later();
    // Written into the collection after the drop was stamped.
    c.insert(&late, doc! { "_id": "after" }).unwrap();
    let logs = crate::log_capture::logs_of(|| {
        pull(&c, &a);
    });
    assert!(logs.contains("a database drop buried a collection that holds documents"), "{logs}");
    assert!(logs.contains("written_after_the_drop=1"), "{logs}");
    assert!(logs.contains("documents=2"), "{logs}");
}

// ---------------------------------------------------------------------------
// The head of the drop
// ---------------------------------------------------------------------------

fn three_collections() -> (Arc<Engine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(&dir.path().join("k.redb")).unwrap();
    for name in ["a", "b", "c"] {
        engine.create_collection("shop", name).unwrap();
    }
    (Arc::new(engine), dir)
}

#[test]
fn a_drop_of_an_empty_database_row_mints_the_sentinel() {
    let (e, _d) = engine();
    e.create_database("empty").unwrap();
    let before = e.version_vector().unwrap();
    assert!(e.drop_database("empty").unwrap());
    assert!(e.database_dropped_at("empty").unwrap().is_some());
    assert_ne!(e.version_vector().unwrap(), before);
}

/// The burial after the head continues the request (ADR-192): a writer held
/// past the budget between the head and the first burial is waited out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_database_drop_whose_first_burial_waits_for_the_writer_after_the_head() {
    let (engine, _dir) = three_collections();
    let budget = Duration::from_millis(100);
    let dropped = {
        let engine = Arc::clone(&engine);
        crate::with_write_wait_budget(budget, async move {
            let holder = Arc::clone(&engine);
            crate::sync::race_hooks::at(crate::sync::race_hooks::Race::Burial, move || {
                let (held, is_held) = std::sync::mpsc::channel();
                std::thread::spawn(move || {
                    let guard = holder.hold_writer(crate::WriterHolder::Bulk);
                    held.send(()).unwrap();
                    std::thread::sleep(budget * 3);
                    drop(guard);
                });
                is_held.recv().unwrap();
            });
            engine.drop_database("shop")
        })
        .await
    };
    assert!(dropped.expect("the first burial waits for the writer"));
    assert!(!engine.database_exists("shop").unwrap());
    assert_eq!(engine.writer_wait_timeouts(), 0);
}

#[test]
fn a_failed_burial_after_the_head_is_partially_applied_with_an_empty_dropped() {
    let (engine, _dir) = three_collections();
    let stopper = Arc::clone(&engine);
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::Burial, move || {
        stopper.set_stopping();
    });
    let failed = engine.drop_database("shop").unwrap_err();
    let crate::StorageError::PartiallyApplied { applied, cause } = failed else {
        panic!("a stop after the head is partly applied: {failed:?}");
    };
    assert_eq!(applied, crate::Applied::DropDatabase { dropped: Vec::new(), in_doubt: None });
    assert!(matches!(*cause, crate::StorageError::Stopping(_)), "{cause:?}");
    assert!(engine.database_dropped_at("shop").unwrap().is_some());
}

#[test]
fn a_head_whose_commit_outcome_is_unknown_is_a_plain_outcome_unknown_and_buries_nothing() {
    let (engine, _dir) = three_collections();
    assert!(engine.arm_test_storage_failure("sync_data"));
    let failed = engine.drop_database("shop").unwrap_err();
    assert!(matches!(failed, crate::StorageError::OutcomeUnknown(_)), "{failed:?}");
}

#[test]
fn gc_collects_a_database_tombstone_with_the_collection_tombstones() {
    let (a, _b, _da, _db, _late) = the_field_sequence();
    // The drop's rows are the purger's; none is owed under the reserved id.
    for meta in a.all_collections().unwrap() {
        a.purge_dropped_collection(meta.id).unwrap();
    }
    assert!(a.database_dropped_at("shop").unwrap().is_some());
    let day = 24 * 60 * 60;
    let later = crate::physical_now_ms() + 365 * day * 1_000;
    a.collect_garbage_at(later, crate::gc::RetentionPolicy::new(day, day)).unwrap();
    assert!(a.database_dropped_at("shop").unwrap().is_none(), "the tombstone outlived retention");
}

// ---------------------------------------------------------------------------
// Streams: told of a burial that minted no entry, and resumable on a survivor
// ---------------------------------------------------------------------------

use crate::watch::{ChangeEvent, ChangeStream, InvalidateReason, WatchOptions, WatchScope};

async fn next(stream: &mut ChangeStream, engine: &Engine) -> Option<ChangeEvent> {
    tokio::time::timeout(Duration::from_secs(5), stream.next(engine))
        .await
        .expect("the stream answered within five seconds")
}

/// The next document change of `stream`, past the schema changes it carries.
async fn next_document(stream: &mut ChangeStream, engine: &Engine) -> kimmy_core::OplogEntry {
    loop {
        match next(stream, engine).await {
            Some(ChangeEvent::Change { entry, .. }) if entry.kind.is_document() => {
                return (*entry).clone();
            }
            Some(ChangeEvent::Change { .. }) => {}
            other => panic!("expected a document change: {other:?}"),
        }
    }
}

fn dropped(event: Option<ChangeEvent>) -> bool {
    matches!(event, Some(ChangeEvent::Invalidate { reason: InvalidateReason::CollectionDropped }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_collection_stream_on_a_collection_the_sweep_buries_ends_with_collection_dropped() {
    let (a, b, _da, _db, late) = the_field_sequence();
    let (c, _dc) = engine();
    pull(&c, &b);
    // Opened before the sweep. `late` has no entry of its own to end it: the
    // dropper never held it, so nothing it minted names it.
    let mut stream = c.watch(WatchScope::Collection(late), WatchOptions::default()).unwrap();
    pull(&c, &a);
    assert!(!holds(&c, "shop", "late"));
    assert!(dropped(next(&mut stream, &c).await));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_collection_stream_opened_during_the_sweep_ends_too() {
    let (a, b, _da, _db, late) = the_field_sequence();
    let (c, _dc) = engine();
    let c = Arc::new(c);
    pull(&c, &b);
    let opener = Arc::clone(&c);
    let ((), stream) = crate::sync::race_hooks::race(
        crate::sync::race_hooks::Race::Burial,
        move || opener.watch(WatchScope::Collection(late), WatchOptions::default()).unwrap(),
        || {
            pull(&c, &a);
        },
    );
    let mut stream = stream;
    assert!(dropped(next(&mut stream, &c).await));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_collection_stream_ends_on_a_lagged_notice_channel_when_the_collection_is_gone() {
    let (a, b, _da, _db, late) = the_field_sequence();
    let (c, _dc) = engine();
    pull(&c, &b);
    let mut stream = c.watch(WatchScope::Collection(late), WatchOptions::default()).unwrap();
    // More notices about other collections than the channel holds, so the one
    // that names this collection is lost with them.
    for i in 0..2_000u64 {
        c.send_buried(vec![crate::BuriedCollection {
            db: "other".into(),
            name: format!("c{i}"),
            ids: vec![CollectionId(1_000_000 + i)],
        }]);
    }
    pull(&c, &a);
    assert!(dropped(next(&mut stream, &c).await));
}

/// Review N6: a sweep of more collections than the channel holds is one notice.
#[test]
fn a_sweep_of_more_collections_than_the_buffer_holds_does_not_lag_a_subscriber() {
    let (a, _da) = engine();
    let (c, _dc) = engine();
    let mut rx = c.subscribe_buried();
    for i in 0..1_100 {
        a.create_collection("shop", &format!("c{i}")).unwrap();
    }
    // A window carries a thousand entries.
    for _ in 0..3 {
        pull(&c, &a);
    }
    assert!(holds(&c, "shop", "c1099"), "premise: the member holds them");
    later();
    assert!(a.drop_database("shop").unwrap());
    pull(&c, &a);
    assert!(!holds(&c, "shop", "c0") && !holds(&c, "shop", "c1099"));
    let mut named = 0;
    loop {
        match rx.try_recv() {
            Ok(notice) => named += notice.collections.len(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            Err(e) => panic!("a subscriber lagged: {e:?}"),
        }
    }
    assert!(named >= 1_100, "{named}");
}

/// A survivor learned of the drop after its own entries arrived, so a position
/// taken from the drop's entry would skip them or refuse a token for them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_token_issued_before_the_sentinel_arrives_still_resumes_on_a_survivor() {
    let (a, _da) = engine();
    a.create_collection("shop", "orders").unwrap();
    later();
    assert!(a.drop_database("shop").unwrap());
    later();
    let (c, _dc) = engine();
    let late = c.create_collection("shop", "late").unwrap();
    c.insert(&late, doc! { "_id": 1 }).unwrap();
    let from_start = WatchOptions { resume_after: None, start_at: Some(Hlc::ZERO) };
    let mut stream = c.watch(WatchScope::Collection(late.id), from_start).unwrap();
    let first = next_document(&mut stream, &c).await;
    assert_eq!(first.doc_id, Some(kimmy_core::DocId::Int64(1)));
    let token = stream.resume_token().unwrap();

    pull(&c, &a);
    assert!(holds(&c, "shop", "late"), "premise: the collection stands");
    assert!(c.collection_dropped_at(late.id).unwrap().is_some(), "premise: it holds the tombstone");

    let options = WatchOptions { resume_after: Some(token), start_at: None };
    let mut resumed = c
        .watch(WatchScope::Collection(late.id), options)
        .expect("a token for the survivor's own entries was refused");
    c.insert(&c.get_collection("shop", "late").unwrap(), doc! { "_id": 2 }).unwrap();
    let second = next_document(&mut resumed, &c).await;
    assert_eq!(second.doc_id, Some(kimmy_core::DocId::Int64(2)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tokenless_stream_on_a_survivor_keeps_the_entries_that_arrived_before_the_sentinel() {
    let (a, _da) = engine();
    a.create_collection("shop", "orders").unwrap();
    later();
    assert!(a.drop_database("shop").unwrap());
    later();
    let (c, _dc) = engine();
    let late = c.create_collection("shop", "late").unwrap();
    c.insert(&late, doc! { "_id": 1 }).unwrap();
    pull(&c, &a);
    let from_start = WatchOptions { resume_after: None, start_at: Some(Hlc::ZERO) };
    let mut stream = c.watch(WatchScope::Collection(late.id), from_start).unwrap();
    let first = next_document(&mut stream, &c).await;
    assert_eq!(
        first.doc_id,
        Some(kimmy_core::DocId::Int64(1)),
        "the survivor's first write was skipped"
    );
}

/// Control: on a collection dropped and recreated on this member, a token of
/// the life before is still refused, and a replay starts at the new life.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_token_from_the_previous_life_of_a_recreated_collection_still_expires() {
    let (e, _d) = engine();
    let first = e.create_collection("shop", "x").unwrap();
    e.insert(&first, doc! { "_id": "old" }).unwrap();
    let from_start = WatchOptions { resume_after: None, start_at: Some(Hlc::ZERO) };
    let mut stream = e.watch(WatchScope::Collection(first.id), from_start).unwrap();
    let _ = next_document(&mut stream, &e).await;
    let token = stream.resume_token().unwrap();
    assert!(e.drop_collection("shop", "x").unwrap());
    e.purge_dropped_collection(first.id).unwrap();
    later();
    let second = e.create_collection("shop", "x").unwrap();
    e.insert(&second, doc! { "_id": "new" }).unwrap();

    let refused = e.watch(
        WatchScope::Collection(second.id),
        WatchOptions { resume_after: Some(token), start_at: None },
    );
    assert!(
        matches!(refused, Err(crate::StorageError::Core(kimmy_core::Error::ResumeTokenExpired))),
        "a token of the life before resumed on the new one"
    );
    let from_start = WatchOptions { resume_after: None, start_at: Some(Hlc::ZERO) };
    let mut fresh = e.watch(WatchScope::Collection(second.id), from_start).unwrap();
    let only = next_document(&mut fresh, &e).await;
    assert_eq!(
        only.doc_id,
        Some(kimmy_core::DocId::String("new".into())),
        "the replay began in the life before"
    );
}

// ---------------------------------------------------------------------------
// The reconcile at a start
// ---------------------------------------------------------------------------

/// A store holding a tombstone over two collections it still holds.
fn tombstoned_store() -> (Arc<Engine>, tempfile::TempDir) {
    let (engine, dir) = three_collections();
    engine
        .record_collection_drop(
            kimmy_core::database_drop_id("shop"),
            Stamp::new(Hlc::new(crate::physical_now_ms() + 1_000, 0), NodeId::from_bytes([7; 16])),
        )
        .unwrap();
    (engine, dir)
}

#[test]
fn the_reconcile_stops_at_a_safe_point_when_a_stop_is_asked() {
    use crate::open_progress::tests::THIS_THREAD_STOPS;
    let (engine, _dir) = tombstoned_store();
    // The stop comes while the first collection is being buried; the next
    // checkpoint ends the reconcile, and what is left is the next start's.
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::Burial, || {
        THIS_THREAD_STOPS.with(|stops| stops.set(true));
    });
    let stopped = engine.reconcile_database_tombstones();
    THIS_THREAD_STOPS.with(|stops| stops.set(false));
    assert!(
        matches!(stopped, Err(crate::StorageError::Stopping(_))),
        "a stop did not end the reconcile"
    );
    let left: Vec<String> =
        engine.list_collections("shop").unwrap().into_iter().map(|c| c.name).collect();
    assert_eq!(left, vec!["b".to_string(), "c".to_string()], "one burial landed whole");
    assert_eq!(engine.reconcile_database_tombstones().unwrap().buried.len(), 2);
    assert!(!engine.database_exists("shop").unwrap());
}

#[test]
fn the_reconcile_says_what_it_is_doing_on_the_readiness_probe() {
    use crate::open_progress::{OpenPhase, open_snapshot};
    let (engine, _dir) = tombstoned_store();
    let seen = std::rc::Rc::new(std::cell::Cell::new(None));
    let into = std::rc::Rc::clone(&seen);
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::Burial, move || {
        let now = open_snapshot();
        into.set(Some((now.phase, now.total)));
    });
    let before = open_snapshot().phase;
    engine.reconcile_database_tombstones().unwrap();
    assert_eq!(seen.get(), Some((OpenPhase::Reconciling, 1)));
    assert_eq!(open_snapshot().phase, before, "the phase it found is the phase it leaves");
}

/// One measurement, not a guard: what a stream opened with no token on a
/// survivor pays for the stamp-based start. The walk reads the arrival index
/// from the stream's start to the first entry stamped after the tombstone, so
/// it is the retained prefix older than the drop and no more: here the worst
/// case, the whole oplog older than the drop.
/// `cargo test --release -p kimmy-storage --lib the_stamp_based_start -- --ignored --nocapture`
#[test]
#[ignore = "a measurement"]
fn the_stamp_based_start_of_a_stream_on_a_survivor_costs_one_pass_over_the_older_prefix() {
    let (c, _d) = engine();
    let bulk = c.create_collection("other", "bulk").unwrap();
    let rows: usize =
        std::env::var("KIMMY_MEASURE_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(500_000);
    let started = std::time::Instant::now();
    for chunk in 0..(rows / 10_000) {
        let docs = (0..10_000).map(|i| doc! { "_id": format!("d{chunk}-{i}") }).collect();
        c.insert_many(&bulk, docs).unwrap();
    }
    eprintln!("MEASURE wrote {rows} entries in {:?}", started.elapsed());
    later();
    let dropped = Stamp::new(Hlc::new(crate::physical_now_ms(), 0), NodeId::from_bytes([7; 16]));
    c.record_collection_drop(kimmy_core::database_drop_id("shop"), dropped).unwrap();
    c.record_collection_drop(CollectionId::derive("shop", "late"), dropped).unwrap();
    later();
    let late = c.create_collection("shop", "late").unwrap();
    c.insert(&late, doc! { "_id": 1 }).unwrap();
    let from_start = WatchOptions { resume_after: None, start_at: Some(Hlc::ZERO) };
    let began = std::time::Instant::now();
    let _stream = c.watch(WatchScope::Collection(late.id), from_start).unwrap();
    eprintln!(
        "MEASURE opened a stream with no token over {rows} older entries in {:?}",
        began.elapsed()
    );
}

// -- The position branch of the incarnation clamp, which two cases still reach --

/// A collection dropped and not recreated: there is no live life to read stamps
/// from, so a stream starts after the drop's entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_of_a_dropped_collection_starts_after_its_drop() {
    let (e, _d) = engine();
    let x = e.create_collection("shop", "x").unwrap();
    e.insert(&x, doc! { "_id": "old" }).unwrap();
    assert!(e.drop_collection("shop", "x").unwrap());
    let from_start = WatchOptions { resume_after: None, start_at: Some(Hlc::ZERO) };
    let mut stream = e.watch(WatchScope::Collection(x.id), from_start).unwrap();
    let heard = tokio::time::timeout(Duration::from_millis(300), stream.next(&e)).await;
    assert!(
        heard.is_err(),
        "the stream of a dropped collection replayed the life before: {heard:?}"
    );
}

/// A creation stamped at the tombstone's own `Hlc` (the node breaks the tie in
/// its favour), so the live life did not begin *after* the tombstone and the
/// stamp-based branch does not apply: the position the drop's entry arrived at
/// still bounds the incarnation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_creation_stamped_at_the_drops_hlc_is_placed_by_where_the_drop_arrived() {
    let (e, _d) = engine();
    let x = CollectionId::derive("shop", "x");
    let insert = |at: Stamp, id: &str| kimmy_core::OplogEntry {
        stamp: at,
        kind: OpKind::Insert,
        collection: x,
        doc_id: Some(kimmy_core::DocId::String(id.into())),
        body: Some(bson::serialize_to_vec(&doc! { "_id": id }).unwrap()),
    };
    let drop = crate::engine::ddl_entry(
        stamp_of(5_000, 1),
        OpKind::DropCollection,
        x,
        &CollectionRef::new("shop", "x"),
    )
    .unwrap();
    for entry in
        [create_entry("shop", "x", stamp_of(4_900, 1)), insert(stamp_of(4_950, 1), "old"), drop]
    {
        e.apply_batch(std::slice::from_ref(&entry)).unwrap();
    }
    // What the drop held is the purger's before the name can be made again.
    e.finish_purges_now().unwrap();
    for entry in [
        // The same `Hlc` as the drop, from a node that sorts above it: not history.
        create_entry("shop", "x", stamp_of(5_000, 2)),
        insert(stamp_of(5_100, 2), "new"),
    ] {
        e.apply_batch(std::slice::from_ref(&entry)).unwrap();
    }
    let live = e.get_collection("shop", "x").unwrap();
    assert_eq!(live.created, Hlc::new(5_000, 0), "premise: the creation is at the tombstone's Hlc");
    let from_start = WatchOptions { resume_after: None, start_at: Some(Hlc::ZERO) };
    let mut stream = e.watch(WatchScope::Collection(x), from_start).unwrap();
    let only = next_document(&mut stream, &e).await;
    assert_eq!(
        only.doc_id,
        Some(kimmy_core::DocId::String("new".into())),
        "the life before was replayed"
    );
}

// -- The races a drop can lose, and the shapes it must keep ------------------------

/// The drop lands between a replicated creation's check and its writer: the
/// creation is judged again under the writer, and refused, and the collection's
/// own tombstone is left.
#[test]
fn a_database_drop_that_lands_while_a_creation_applies_still_refuses_it() {
    let (a, b, _da, _db, late) = the_field_sequence();
    let (c, _dc) = engine();
    let c = Arc::new(c);
    let theirs: Vec<_> = b
        .entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .filter(|e| e.collection == late)
        .collect();
    assert!(theirs.iter().any(|e| e.kind == OpKind::CreateCollection), "premise");
    let drop = a.entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background).unwrap().entries;
    let landing = Arc::clone(&c);
    let (applied, ()) = crate::sync::race_hooks::race(
        crate::sync::race_hooks::Race::ReplicatedCreate,
        move || {
            landing.apply_batch(&drop).unwrap();
        },
        || c.apply_batch(&theirs),
    );
    let outcome = applied.unwrap();
    assert_eq!(outcome.unknown_collection, 0, "{:?}", outcome.unknown);
    assert!(!holds(&c, "shop", "late"), "a creation older than the drop stood");
    assert!(c.collection_dropped_at(late).unwrap().is_some(), "and left no tombstone of its own");
}

/// The same for a snapshot's definition.
#[test]
fn a_database_drop_that_lands_while_a_definition_is_restored_still_refuses_it() {
    let (a, b, _da, _db, late) = the_field_sequence();
    let (c, _dc) = engine();
    let c = Arc::new(c);
    // `c` holds `orders`, so `late` is the first definition it has to create.
    let orders =
        a.entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background).unwrap().entries;
    let orders_create: Vec<_> =
        orders.iter().filter(|e| e.kind == OpKind::CreateCollection).cloned().collect();
    c.apply_batch(&orders_create).unwrap();
    assert!(holds(&c, "shop", "orders"));
    let page = b.snapshot_page(None, None).unwrap();
    let landing = Arc::clone(&c);
    let (restored, ()) = crate::sync::race_hooks::race(
        crate::sync::race_hooks::Race::Restore,
        move || {
            landing.apply_batch(&orders).unwrap();
        },
        || c.apply_snapshot_page(b.node_id(), &mut SnapshotProgress::whole_database(), &page),
    );
    restored.unwrap();
    assert!(!holds(&c, "shop", "late"), "a definition older than the drop was restored");
    assert!(c.collection_dropped_at(late).unwrap().is_some(), "and left no tombstone of its own");
}

/// A refused collection with vectors leaves its shadow's tombstone too: the
/// chunks that follow are judged by the shadow's id.
#[test]
fn a_database_drop_covers_a_never_held_vector_configured_collection_and_its_chunks() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let (c, _dc) = engine();
    a.create_collection("shop", "orders").unwrap();
    sync(&a, &b);
    b.create_collection("shop", "late").unwrap();
    b.configure_vectors("shop", "late", vector_config()).unwrap();
    let shadow = b.vector_collection("shop", "late").unwrap().unwrap();
    let put = |n: i64| {
        let source = kimmy_core::DocId::Int64(n);
        b.put_vectors(
            &shadow,
            &source,
            &[kimmy_core::VectorRecord {
                source: source.clone(),
                chunk: 0,
                source_hlc: Hlc::new(1, 0),
                vector: vec![1.0, 0.0, 0.0, 0.0],
                text: "t".into(),
                config: None,
            }],
        )
        .unwrap();
    };
    put(1);
    later();
    assert!(a.drop_database("shop").unwrap());
    pull(&c, &a);
    let outcome = pull(&c, &b);
    assert_eq!(
        outcome.unknown_collection, 0,
        "the first chunk stopped the batch: {:?}",
        outcome.unknown
    );
    put(2);
    let outcome = pull(&c, &b);
    assert_eq!(outcome.unknown_collection, 0, "{:?}", outcome.unknown);
    assert!(!holds(&c, "shop", "late"));
}

/// What the head mints is an ordinary `DropCollection` that names the reserved
/// collection, in the shape a build that predates the rule applies (ADR-229).
#[test]
fn the_head_of_a_database_drop_mints_an_ordinary_drop_collection_entry() {
    let (a, _da) = engine();
    a.create_collection("shop", "orders").unwrap();
    assert!(a.drop_database("shop").unwrap());
    let id = kimmy_core::database_drop_id("shop");
    let entries =
        a.entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background).unwrap().entries;
    let head: Vec<_> = entries.iter().filter(|e| e.collection == id).collect();
    assert_eq!(head.len(), 1, "one head entry: {head:?}");
    assert_eq!(head[0].kind, OpKind::DropCollection);
    let body: CollectionRef = bson::deserialize_from_slice(head[0].body.as_ref().unwrap()).unwrap();
    assert_eq!(body, CollectionRef::new("shop", RESERVED));
    assert_eq!(a.database_dropped_at("shop").unwrap(), Some(head[0].stamp));
    // And it is stamped before the burial of the collection it covers.
    let burial = entries
        .iter()
        .find(|e| {
            e.kind == OpKind::DropCollection
                && e.collection == CollectionId::derive("shop", "orders")
        })
        .unwrap();
    assert!(head[0].stamp < burial.stamp);
}

/// A stream subscribes to the buried notices before it resolves where it starts:
/// a sweep that runs *during* the resolve is heard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_opened_while_its_start_resolves_hears_a_sweep_that_runs_then() {
    let (a, b, _da, _db, late) = the_field_sequence();
    let (c, _dc) = engine();
    let (a, c) = (Arc::new(a), Arc::new(c));
    pull(&c, &b);
    let (sweeping, from) = (Arc::clone(&c), Arc::clone(&a));
    c.during_next_watch_resolve(move || {
        pull(&sweeping, &from);
    });
    let mut stream = c.watch(WatchScope::Collection(late), WatchOptions::default()).unwrap();
    assert!(!holds(&c, "shop", "late"), "premise: the sweep ran during the resolve");
    assert!(dropped(next(&mut stream, &c).await), "the notice sent during the resolve was missed");
}
