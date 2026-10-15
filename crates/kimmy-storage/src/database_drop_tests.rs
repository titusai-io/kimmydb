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

/// A creation the database's drop refuses is held and served onward (ADR-236), as
/// an ignored collection drop is: the collection's tombstone is the database's drop
/// expanded on meeting the creation, and a member that pulls only the relay never met
/// it, while the relay's vector named the creation's origin past it. The two ended
/// with different tombstone tables.
#[test]
fn a_creation_the_database_refuses_is_relayed_and_its_tombstone_follows() {
    let (a, b, _da, _db, late) = the_field_sequence();
    let (c, _dc) = engine();
    pull(&a, &b);
    assert!(!holds(&a, "shop", "late"), "premise: A refused it");
    let expanded = a.collection_dropped_at(late).unwrap();
    assert!(expanded.is_some(), "premise: A expanded the database's drop");
    pull(&c, &a);
    assert!(!holds(&c, "shop", "late"));
    assert_eq!(c.collection_dropped_at(late).unwrap(), expanded, "the tombstone did not follow");
    assert_eq!(pull(&c, &a), SyncOutcome::default(), "and nothing is left to serve");
}

/// A definition for a collection this member does not hold, stamped after the
/// collection's own tombstone and at or before its database's, is history and does
/// not stop the batch (ADR-236): no life of the name can begin at or before the
/// database's drop, and the drop takes what a life it covers made before it. Judged
/// against the collection's tombstone alone it stopped the batch for a life that can
/// never come.
#[test]
fn a_definition_below_the_databases_tombstone_is_history_for_an_absent_collection() {
    let (a, _da) = engine();
    let (e, _de) = engine();
    a.create_collection("shop", "orders").unwrap();
    let field = vec![kimmy_core::IndexField::ascending("k")];
    a.create_index("shop", "orders", field, false, Some("by_k".into())).unwrap();
    let created = a
        .entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| entry.kind == OpKind::CreateIndex)
        .expect("the index's entry");
    let orders = CollectionId::derive("shop", "orders");
    let at = |wall: u64| Stamp::new(Hlc::new(wall, 0), NodeId::from_bytes([7; 16]));
    e.record_collection_drop(orders, at(created.stamp.hlc.wall_ms - 1)).unwrap();
    e.record_database_tombstone_for_test(
        kimmy_core::database_drop_id("shop"),
        at(created.stamp.hlc.wall_ms + 1),
    )
    .unwrap();
    let outcome = e.apply_batch(std::slice::from_ref(&created)).unwrap();
    assert_eq!(outcome.unknown_collection, 0, "{:?}", outcome.unknown);
    assert_eq!(outcome.superseded, 1, "{outcome:?}");
}

/// A life created over its database's tombstone takes a tombstone of its own at the
/// database's drop, and so does its vector shadow's id (ADR-236), as the database's
/// sweep gives a life it leaves standing: the same table on the member that created it
/// and on one that applied the creation after the drop.
#[test]
fn a_life_created_over_its_databases_tombstone_takes_its_own_and_its_shadows() {
    let (a, b, _da, _db, _late) = the_field_sequence();
    later();
    let fresh = a.create_collection("shop", "fresh").unwrap();
    let dropped = a.database_dropped_at("shop").unwrap().expect("the database's tombstone");
    let shadow = CollectionId::derive("shop", &kimmy_core::vector_meta::shadow_name("fresh"));
    pull(&b, &a);
    for (who, e) in [("the creator", &a), ("a member that applied it", &b)] {
        assert_eq!(e.collection_dropped_at(fresh.id).unwrap(), Some(dropped), "{who}");
        assert_eq!(e.collection_dropped_at(shadow).unwrap(), Some(dropped), "{who}: the shadow");
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
    // A creation the database's drop refuses is held and served onward (ADR-236):
    // a member that meets it only through this one records the collection's
    // tombstone from it, as this one did.
    let created = b
        .entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .find(|e| e.kind == OpKind::CreateCollection && e.collection == late)
        .expect("b holds the creation");
    assert!(c.oplog_entry(&created.stamp).unwrap().is_some(), "a refused creation was withheld");

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

/// Three members and a fourth that refused the creation: a
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
    assert!(
        y.collection_dropped_at(yl.id).unwrap().is_some(),
        "the sweep left the survivor no tombstone of its own"
    );
    pull(&y, &z);

    let has = |e: &Engine, id: &str| -> bool {
        let meta = e.get_collection("shop", "late").unwrap();
        e.get(&meta, &kimmy_core::DocId::String(id.into())).unwrap().is_some()
    };
    assert!(has(&m, "y1") && !has(&m, "d1"), "the member that refused the creation");
    assert!(has(&y, "y1"), "the survivor lost its own document");
    assert!(!has(&y, "d1"), "the survivor took a straggler of the life before it");
}

/// A creation and a drop stamped with the same `Hlc` agree
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

/// The page that recorded the tombstone is retried after a sweep
/// that failed; it carries nothing new, and must still bury.
#[test]
fn a_retried_page_still_buries_under_a_tombstone_an_earlier_attempt_recorded() {
    let (a, b, _da, _db, _late) = the_field_sequence();
    let (c, _dc) = engine();
    pull(&c, &b);
    assert!(holds(&c, "shop", "late"));
    let page = a.snapshot_page(None, None).unwrap();
    let drop = a.database_dropped_at("shop").unwrap().unwrap();
    // The first attempt: every tombstone the page carries committed (the
    // collection's, and the database's), the sweep did not. The retry has
    // nothing new to record.
    let orders = CollectionId::derive("shop", "orders");
    c.record_collection_drop(orders, a.collection_dropped_at(orders).unwrap().unwrap()).unwrap();
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

/// A document written into the collection after the drop was stamped, on a
/// member that had not yet received the drop, is a write acknowledged to a
/// client: the database's drop keeps it, as a collection's drop does, in the
/// life re-dated just above the drop, and removes what was written before it
/// (ADR-236). It used to bury it with the collection and warn of the loss.
#[test]
fn a_database_drop_keeps_a_write_made_after_it_and_says_so() {
    let (a, b, _da, _db, late) = the_field_sequence();
    let (c, _dc) = engine();
    pull(&c, &b);
    let held = c.get_collection("shop", "late").unwrap();
    later();
    // Written into the collection after the drop was stamped.
    c.insert(&held, doc! { "_id": "after" }).unwrap();
    let logs = crate::log_capture::logs_of(|| {
        pull(&c, &a);
    });
    assert!(logs.contains("kept them") && logs.contains("kept=true"), "{logs}");
    let kept = c.get_collection("shop", "late").expect("the life keeps the later write");
    let dropped = a.database_dropped_at("shop").unwrap().expect("the database's tombstone");
    assert!(kept.is_redated() && kept.created > dropped.hlc, "re-dated above it: {kept:?}");
    assert_eq!(kept.incarnation_floor, Some(dropped.hlc), "floored at the database's drop");
    assert_eq!(c.collection_dropped_at(late).unwrap(), Some(dropped), "with its tombstone");
    let ids: Vec<String> = c
        .snapshot_page(None, Some(late))
        .unwrap()
        .documents
        .iter()
        .map(|d| format!("{:?}", d.id))
        .collect();
    assert_eq!(ids, vec![format!("{:?}", kimmy_core::DocId::String("after".into()))], "{ids:?}");
    assert!(
        !logs.lines().any(|line| line.contains("WARN") && line.contains("buried a collection")),
        "a kept write was reported as lost: {logs}"
    );
}

/// An ordinary clean drop of a collection that holds documents warns of nothing:
/// every document it holds was written before the drop, which is what the drop
/// is for, and the burial is already logged.
#[test]
fn a_clean_database_drop_with_documents_buries_without_a_warning() {
    let (a, b, _da, _db, late) = the_field_sequence();
    let (c, _dc) = engine();
    pull(&c, &b);
    let held = c.get_collection("shop", "late").unwrap();
    assert_eq!(
        c.count(&held, crate::WalkScope::Request).unwrap(),
        1,
        "premise: it holds a document"
    );
    let logs = crate::log_capture::logs_of(|| {
        pull(&c, &a);
    });
    assert!(!holds(&c, "shop", "late"), "premise: the sweep buried it");
    assert!(c.collection_dropped_at(late).unwrap().is_some());
    assert!(
        !logs.lines().any(|line| line.contains("WARN") && line.contains("buried a collection")),
        "a clean drop warned: {logs}"
    );
}

/// A document deleted after the drop is not a lost write: its record is a
/// tombstone, stamped after the drop, beside a live document that predates it.
#[test]
fn a_document_deleted_after_the_drop_is_not_a_lost_write() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let (c, _dc) = engine();
    a.create_collection("shop", "orders").unwrap();
    sync(&a, &b);
    let late = b.create_collection("shop", "late").unwrap();
    b.insert(&late, doc! { "_id": "keep" }).unwrap();
    b.insert(&late, doc! { "_id": "gone" }).unwrap();
    later();
    assert!(a.drop_database("shop").unwrap());
    pull(&c, &b);
    let held = c.get_collection("shop", "late").unwrap();
    later();
    c.delete(&held, &kimmy_core::DocId::String("gone".into())).unwrap();
    assert_eq!(
        c.count(&held, crate::WalkScope::Request).unwrap(),
        1,
        "premise: one live document is left"
    );
    let logs = crate::log_capture::logs_of(|| {
        pull(&c, &a);
    });
    assert!(!holds(&c, "shop", "late"), "premise: the sweep buried it");
    assert!(
        !logs.lines().any(|line| line.contains("WARN") && line.contains("buried a collection")),
        "a deletion after the drop was reported as a lost write: {logs}"
    );
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
    pull(&c, &a);
    // More notices about other collections than the channel holds, after the one
    // that names this collection, so that one is lost with the rest.
    for i in 0..2_000u64 {
        c.send_buried(vec![crate::BuriedCollection {
            db: "other".into(),
            name: format!("c{i}"),
            ids: vec![CollectionId(1_000_000 + i)],
        }]);
    }
    assert!(dropped(next(&mut stream, &c).await));
}

/// A sweep of more collections than the channel holds is one notice.
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

/// A collection created here from a peer's entry, and a document of the peer's
/// for it applied between the creation's commit and its entry's, is in the
/// replay of a stream opened with no token once the sentinel has arrived: the
/// document arrived before the creation's entry, and only the stamps place it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_document_applied_while_a_replicated_creation_is_recorded_is_not_skipped_by_a_replay() {
    let (a, _da) = engine();
    a.create_collection("shop", "orders").unwrap();
    later();
    assert!(a.drop_database("shop").unwrap());
    later();
    let (p, _dp) = engine();
    let late = p.create_collection("shop", "late").unwrap();
    p.insert(&late, doc! { "_id": 1 }).unwrap();
    let theirs =
        p.entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background).unwrap().entries;
    let create = theirs.iter().find(|e| e.kind == OpKind::CreateCollection).unwrap().clone();
    let insert = theirs.iter().find(|e| e.kind == OpKind::Insert).unwrap().clone();

    let (c, _dc) = engine();
    let c = Arc::new(c);
    let competing = Arc::clone(&c);
    let arriving = insert.clone();
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::DdlAppend, move || {
        competing.apply_batch(std::slice::from_ref(&arriving)).unwrap();
    });
    c.apply_batch(std::slice::from_ref(&create)).unwrap();
    // Reached when the creation's entry is appended after the creation commits:
    // the document arrives between the two. The hook is drained either way.
    crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::DdlAppend);
    pull(&c, &a);
    assert!(holds(&c, "shop", "late"), "premise: the collection survives the sentinel");
    assert!(c.collection_dropped_at(late.id).unwrap().is_some(), "premise: it holds the tombstone");

    let from_start = WatchOptions { resume_after: None, start_at: Some(Hlc::ZERO) };
    let mut stream = c.watch(WatchScope::Collection(late.id), from_start).unwrap();
    let first = next_document(&mut stream, &c).await;
    assert_eq!(
        first.doc_id,
        Some(kimmy_core::DocId::Int64(1)),
        "a replay skipped a document that arrived while the creation was recorded"
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
    use crate::open_progress::{OpenPhase, tests::THIS_THREAD_PHASE};
    let (engine, _dir) = tombstoned_store();
    let seen = std::rc::Rc::new(std::cell::Cell::new(None));
    let into = std::rc::Rc::clone(&seen);
    THIS_THREAD_PHASE.with(|phase| phase.set(None));
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::Burial, move || {
        // This thread's own record of the phase: the probe's is the process's,
        // and an open running in another test moves it.
        into.set(THIS_THREAD_PHASE.with(std::cell::Cell::get));
    });
    engine.reconcile_database_tombstones().unwrap();
    assert_eq!(seen.get(), Some((OpenPhase::Reconciling, 1)));
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

/// A creation stamped at the tombstone's own `Hlc` (the node broke the tie in
/// its favour, which a build before ADR-236 allowed, and a store it wrote still
/// holds) is placed by the tombstone's stamp like any other life that began
/// after it: the replay starts at the new life. A tie is the drop's now, so the
/// creation is applied one tick later and the stored `created` written back to
/// the tie, the state such a build left.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_creation_stamped_at_the_drops_hlc_is_placed_by_the_tombstones_stamp() {
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
    // The same `Hlc` as the drop, from a node that sorts above it: history (ADR-236).
    e.apply_batch(&[create_entry("shop", "x", stamp_of(5_000, 2))]).unwrap();
    assert!(e.get_collection("shop", "x").is_err(), "a tie is the drop's");
    for entry in [create_entry("shop", "x", stamp_of(5_001, 2)), insert(stamp_of(5_100, 2), "new")]
    {
        e.apply_batch(std::slice::from_ref(&entry)).unwrap();
    }
    let mut legacy = e.get_collection("shop", "x").unwrap();
    legacy.created = Hlc::new(5_000, 0);
    let txn = e.begin_write(crate::WriterHolder::Ddl).unwrap();
    Engine::put_collection_meta(&txn, &legacy).unwrap();
    txn.commit().unwrap();
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
    // The shadow's own creation entry, if `b` logged one, would leave its
    // tombstone by itself. Retention can have collected it while the chunks
    // remain, and then the parent's refusal is all there is.
    let shadow_id = CollectionId::derive("shop", &kimmy_core::vector_meta::shadow_name("late"));
    let entries: Vec<_> = b
        .entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .filter(|e| !(e.kind == OpKind::CreateCollection && e.collection == shadow_id))
        .collect();
    let outcome = c.apply_batch(&entries).unwrap();
    assert_eq!(
        outcome.unknown_collection, 0,
        "the first chunk stopped the batch: {:?}",
        outcome.unknown
    );
    put(2);
    let more: Vec<_> = b
        .entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .filter(|e| e.collection == shadow_id && e.kind != OpKind::CreateCollection)
        .collect();
    let outcome = c.apply_batch(&more).unwrap();
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

/// A survivor holds, applied before it learned of the drop, documents of the
/// life before it; a stream of it passes over them by the floor, however late
/// they arrived. (They stay in the collection: a tombstone refuses what comes
/// after it, it does not unwrite. ADR-229 names that as a residual.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_of_a_survivor_passes_over_entries_of_the_life_before_that_arrived_after_its_start()
 {
    let (z, _dz) = engine();
    let (a, _da) = engine();
    let (y, _dy) = engine();
    let zl = z.create_collection("shop", "late").unwrap();
    z.insert(&zl, doc! { "_id": "old" }).unwrap();
    a.create_collection("shop", "orders").unwrap();
    later();
    assert!(a.drop_database("shop").unwrap());
    later();
    let yl = y.create_collection("shop", "late").unwrap();
    y.insert(&yl, doc! { "_id": "mine" }).unwrap();
    // `y` takes the life before's document while it knows nothing of the drop,
    // and learns of the drop afterwards.
    pull(&y, &z);
    pull(&y, &a);
    assert!(y.collection_dropped_at(yl.id).unwrap().is_some(), "premise: y holds the tombstone");

    let from_start = WatchOptions { resume_after: None, start_at: Some(Hlc::ZERO) };
    let mut stream = y.watch(WatchScope::Collection(yl.id), from_start).unwrap();
    let mut seen = Vec::new();
    while let Ok(Some(ChangeEvent::Change { entry, .. })) =
        tokio::time::timeout(Duration::from_millis(300), stream.next(&y)).await
    {
        if entry.kind.is_document() {
            seen.push(entry.doc_id.clone().unwrap());
        }
    }
    assert_eq!(seen, vec![kimmy_core::DocId::String("mine".into())], "{seen:?}");
}

/// A survivor that took the other life's creation before it learned of the drop
/// has no creation entry left to be refused when the documents arrive after it:
/// the tombstone the sweep gave it is what refuses them.
#[test]
fn a_survivor_refuses_a_straggler_that_arrives_after_the_drop_though_its_creation_arrived_before() {
    let (z, _dz) = engine();
    let (a, _da) = engine();
    let (y, _dy) = engine();
    let zl = z.create_collection("shop", "late").unwrap();
    z.insert(&zl, doc! { "_id": "old" }).unwrap();
    a.create_collection("shop", "orders").unwrap();
    later();
    assert!(a.drop_database("shop").unwrap());
    later();
    let yl = y.create_collection("shop", "late").unwrap();
    y.insert(&yl, doc! { "_id": "mine" }).unwrap();

    let entries =
        z.entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background).unwrap().entries;
    let (creation, documents): (Vec<_>, Vec<_>) =
        entries.into_iter().partition(|e| e.kind == OpKind::CreateCollection);
    y.apply_batch(&creation).unwrap();
    pull(&y, &a);
    y.apply_batch(&documents).unwrap();
    let meta = y.get_collection("shop", "late").unwrap();
    assert!(
        y.get(&meta, &kimmy_core::DocId::String("old".into())).unwrap().is_none(),
        "the survivor took a document of the life before it, stamped before the drop"
    );
    assert!(y.get(&meta, &kimmy_core::DocId::String("mine".into())).unwrap().is_some());
}

/// A replicated drop's tombstones and its entry are one commit.
#[test]
fn a_replicated_drop_records_its_tombstones_in_the_commit_that_appends_its_entry() {
    let (e, _d) = engine();
    let drop = crate::engine::ddl_entry(
        stamp_of(5_000, 3),
        OpKind::DropCollection,
        CollectionId::derive("shop", "ghost"),
        &CollectionRef::new("shop", "ghost"),
    )
    .unwrap();
    let before = e.commits();
    e.apply_batch(std::slice::from_ref(&drop)).unwrap();
    let spent = e.commits() - before;
    let ghost = CollectionId::derive("shop", "ghost");
    let shadow = CollectionId::derive("shop", &kimmy_core::vector_meta::shadow_name("ghost"));
    // Both tombstones, for a collection this member never held...
    assert_eq!(e.collection_dropped_at(ghost).unwrap(), Some(drop.stamp));
    assert_eq!(e.collection_dropped_at(shadow).unwrap(), Some(drop.stamp));
    // ...and the entry, in the one commit (the batch's own bookkeeping is the other).
    assert!(e.oplog_entry(&drop.stamp).unwrap().is_some());
    assert_eq!(spent, 2, "the tombstones and the entry took {spent} commits");
    // Delivered again, it costs the batch's bookkeeping and nothing of its own.
    let before = e.commits();
    e.apply_batch(std::slice::from_ref(&drop)).unwrap();
    assert!(e.commits() - before <= 1, "a drop already held cost {} commits", e.commits() - before);
}

/// An entry already held as sent still leaves its tombstones: a store that holds
/// the entry and lost, or never had, the tombstones (a restore, an older build)
/// gets them back when the entry is delivered again.
#[test]
fn a_drop_already_held_as_sent_still_records_its_tombstones() {
    let (e, _d) = engine();
    let ghost = CollectionId::derive("shop", "ghost");
    let drop = crate::engine::ddl_entry(
        stamp_of(5_000, 3),
        OpKind::DropCollection,
        ghost,
        &CollectionRef::new("shop", "ghost"),
    )
    .unwrap();
    e.apply_batch(std::slice::from_ref(&drop)).unwrap();
    {
        let txn = e.begin_write(crate::WriterHolder::Ddl).unwrap();
        txn.open_table(crate::tables::COLLECTIONS_DROPPED).unwrap().remove(ghost.0).unwrap();
        txn.commit().unwrap();
    }
    assert!(e.collection_dropped_at(ghost).unwrap().is_none(), "premise");
    e.apply_batch(std::slice::from_ref(&drop)).unwrap();
    assert_eq!(e.collection_dropped_at(ghost).unwrap(), Some(drop.stamp));
}

/// The position branch is for a collection that is absent: an entry of the life
/// before, stamped after the drop (a write on a member whose clock ran ahead)
/// and arrived before it, is still the life before's, and only where the drop's
/// entry arrived says so. No drop leaves that state any more, since every drop
/// keeps such a write and the collection with it (ADR-231, extended; ADR-236 for
/// a database's), and that has its own test beside the sweep; a store written by
/// 0.49.0, whose database drop buried the collection with what was written into
/// it afterwards, still holds it. So the burial here is that build's: the
/// collection buried whole at the database's stamp, after the write arrived.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_collections_stream_skips_an_old_entry_stamped_after_its_drop() {
    let (e, _d) = engine();
    let x = CollectionId::derive("shop", "x");
    let insert = |at: Stamp, id: &str| kimmy_core::OplogEntry {
        stamp: at,
        kind: OpKind::Insert,
        collection: x,
        doc_id: Some(kimmy_core::DocId::String(id.into())),
        body: Some(bson::serialize_to_vec(&doc! { "_id": id }).unwrap()),
    };
    for entry in [create_entry("shop", "x", stamp_of(4_900, 1)), insert(stamp_of(6_000, 3), "o")] {
        e.apply_batch(std::slice::from_ref(&entry)).unwrap();
    }
    // What 0.49.0 did with the database's drop: its tombstone, its entry, and the
    // collection buried whole at its stamp.
    let sentinel = drop_database_entry("shop", stamp_of(5_000, 1));
    e.record_database_tombstone_for_test(sentinel.collection, sentinel.stamp).unwrap();
    {
        let txn = e.begin_write(crate::WriterHolder::Replication).unwrap();
        crate::engine::append_oplog(&txn, &sentinel).unwrap();
        txn.commit().unwrap();
    }
    assert!(e.drop_collection_inner("shop", "x", Some(sentinel.stamp)).unwrap());
    assert!(e.get_collection("shop", "x").is_err(), "premise: x is gone");
    assert!(e.collection_dropped_at(x).unwrap().is_some(), "premise: x holds a tombstone");
    let from_start = WatchOptions { resume_after: None, start_at: Some(Hlc::ZERO) };
    let mut stream = e.watch(WatchScope::Collection(x), from_start).unwrap();
    let heard = tokio::time::timeout(Duration::from_millis(300), stream.next(&e)).await;
    assert!(
        heard.is_err(),
        "the stream of a dropped collection replayed the life before: {heard:?}"
    );
}

// -- What a refusal costs, and what it reads as ------------------------------

/// A definition the database rule refuses is decided before the writer is
/// taken; the one hold it costs is the record it leaves.
#[test]
fn a_definition_the_database_refuses_is_decided_before_the_writer() {
    let (a, b, _da, _db, _late) = the_field_sequence();
    let page = b.snapshot_page(None, None).unwrap();
    let slot = crate::engine::WriterHolder::Ddl.slot();
    let before = a.writer_hold().count[slot];
    a.apply_snapshot_page(b.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    let after = a.writer_hold().count[slot];
    assert_eq!(after - before, 1, "a refused definition took more than the one record");
}

/// A page's definition the database rule refuses does not wait on the purge of
/// an earlier life of its name that the collection's own tombstone does not make
/// history.
#[test]
fn a_definition_the_database_refuses_does_not_wait_on_an_older_lifes_purge() {
    let (r, _dr) = engine();
    let (s, _ds) = engine();
    let (d, _dd) = engine();
    let x = s.create_collection("shop", "x").unwrap();
    s.insert(&x, doc! { "_id": "old" }).unwrap();
    pull(&r, &s);
    assert!(holds(&r, "shop", "x"));
    later();
    // The receiver drops `x`; the purge of its rows is still owed.
    assert!(r.drop_collection("shop", "x").unwrap());
    pull(&s, &r);
    assert!(!holds(&s, "shop", "x"));
    s.finish_purges_now().unwrap();
    later();
    // The sender makes the name again, and then a database drop covers it.
    let x2 = s.create_collection("shop", "x").unwrap();
    s.insert(&x2, doc! { "_id": "new" }).unwrap();
    later();
    d.create_collection("shop", "other").unwrap();
    later();
    assert!(d.drop_database("shop").unwrap());
    pull(&r, &d);
    assert!(r.database_dropped_at("shop").unwrap().is_some(), "premise: r holds the tombstone");
    assert!(r.purge_pending(x.id).unwrap(), "premise: r still owes the purge of the old life");
    let page = s.snapshot_page(None, None).unwrap();
    assert!(page.collections.iter().any(|c| c.name == "x"), "premise: the page defines x");
    let applied =
        r.apply_snapshot_page(s.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    assert!(
        applied.purge_pending.is_none(),
        "the page waited on a purge for a definition it refuses"
    );
    assert!(!holds(&r, "shop", "x"));
}

/// A creation naming no `created` (a page from a sender before the field) reads
/// as older than the drop.
#[test]
fn a_definition_naming_no_creation_reads_as_older_than_the_drop() {
    let (a, _b, _da, _db, _late) = the_field_sequence();
    let (f, _df) = engine();
    later();
    f.create_collection("shop", "fresh").unwrap();
    let mut page = f.snapshot_page(None, None).unwrap();
    for c in page.collections.iter_mut() {
        c.created = None;
    }
    a.apply_snapshot_page(f.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    assert!(!holds(&a, "shop", "fresh"), "a definition with no creation stamp was read as newer");
}

/// Recording a refusal already held takes no writer (ADR-152).
#[test]
fn recording_a_refusal_already_held_takes_no_writer() {
    let (a, _b, _da, _db, _late) = the_field_sequence();
    a.record_database_refusal("shop", "late", Some(Hlc::new(1, 0))).unwrap();
    let slot = crate::engine::WriterHolder::Ddl.slot();
    let before = a.writer_hold().count[slot];
    a.record_database_refusal("shop", "late", Some(Hlc::new(1, 0))).unwrap();
    assert_eq!(a.writer_hold().count[slot], before, "a held refusal took the writer");
}

// -- The sweep and a stop -----------------------------------------------------

#[test]
fn a_stop_ends_a_sweep_between_two_burials() {
    let (engine, _dir) = tombstoned_store();
    let stopper = Arc::clone(&engine);
    // The stop comes while the first collection is being buried: that burial
    // lands whole, and the next checkpoint ends the sweep.
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::Burial, move || {
        stopper.stop_walks();
    });
    let stopped = engine.bury_under_database_tombstones();
    assert!(
        matches!(stopped, Err(crate::StorageError::Stopping(_))),
        "a stop did not end the sweep"
    );
    let left: Vec<String> =
        engine.list_collections("shop").unwrap().into_iter().map(|c| c.name).collect();
    assert_eq!(left, vec!["b".to_string(), "c".to_string()], "one burial landed whole");
    engine.lift_stops_for_test();
    assert_eq!(engine.bury_under_database_tombstones().unwrap().buried.len(), 2);
}

/// A stop ahead of a drop's entry leaves the entry unappended, so the next
/// start redoes it.
#[test]
fn a_stop_before_a_sentinel_is_applied_leaves_the_entry_to_be_redone() {
    let (e, _d) = engine();
    e.create_collection("shop", "orders").unwrap();
    later();
    let sentinel = drop_database_entry("shop", stamp_of(crate::physical_now_ms() + 1_000, 7));
    e.stop_walks();
    let stopped = e.apply_batch(std::slice::from_ref(&sentinel));
    assert!(matches!(stopped, Err(crate::StorageError::Stopping(_))), "{:?}", stopped.map(|_| ()));
    assert!(e.oplog_entry(&sentinel.stamp).unwrap().is_none(), "the entry was appended");
    e.lift_stops_for_test();
    e.apply_batch(std::slice::from_ref(&sentinel)).unwrap();
    assert!(!holds(&e, "shop", "orders"));
    assert!(e.oplog_entry(&sentinel.stamp).unwrap().is_some());
}

/// The final removal of the database's row continues the request, too: a writer
/// held past the budget there is waited out, whether or not a burial landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_removal_of_an_empty_databases_row_waits_for_the_writer_after_the_head() {
    let (engine, _dir) = engine();
    let engine = Arc::new(engine);
    engine.create_database("empty").unwrap();
    let budget = Duration::from_millis(100);
    let dropped = {
        let engine = Arc::clone(&engine);
        crate::with_write_wait_budget(budget, async move {
            let holder = Arc::clone(&engine);
            crate::sync::race_hooks::at(
                crate::sync::race_hooks::Race::BeforeRowRemoval,
                move || {
                    let (held, is_held) = std::sync::mpsc::channel();
                    std::thread::spawn(move || {
                        let guard = holder.hold_writer(crate::WriterHolder::Bulk);
                        held.send(()).unwrap();
                        std::thread::sleep(budget * 3);
                        drop(guard);
                    });
                    is_held.recv().unwrap();
                },
            );
            engine.drop_database("empty")
        })
        .await
    };
    assert!(dropped.expect("the removal waits for the writer"));
    assert!(!engine.database_exists("empty").unwrap());
    assert_eq!(engine.writer_wait_timeouts(), 0);
}

/// The sweep's memo holds a database only while it holds a tombstone.
#[test]
fn the_sweep_memo_is_pruned_when_the_database_or_its_tombstone_goes() {
    let (a, _da) = engine();
    let (c, _dc) = engine();
    a.create_collection("shop", "orders").unwrap();
    later();
    assert!(a.drop_database("shop").unwrap());
    later();
    c.create_collection("shop", "late").unwrap();
    pull(&c, &a);
    c.bury_under_database_tombstones().unwrap();
    assert_eq!(c.swept.lock().len(), 1, "premise: the survivor's database is memoised");

    // The tombstone is collected while the database stands.
    let day = 24 * 60 * 60;
    let later_ms = crate::physical_now_ms() + 365 * day * 1_000;
    c.collect_garbage_at(later_ms, crate::gc::RetentionPolicy::new(day, day)).unwrap();
    assert!(c.database_dropped_at("shop").unwrap().is_none(), "premise: the tombstone is gone");
    c.bury_under_database_tombstones().unwrap();
    assert_eq!(c.swept.lock().len(), 0, "a collected tombstone left its memo behind");

    // And the database goes.
    let (e, _de) = engine();
    e.create_collection("gone", "x").unwrap();
    assert!(e.drop_database("gone").unwrap());
    e.swept.lock().insert("gone".into(), (stamp_of(1, 1), 0));
    e.bury_under_database_tombstones().unwrap();
    assert_eq!(e.swept.lock().len(), 0, "a database that is gone left its memo behind");
}

/// A stop that comes with the last burial ends the sweep before it walks the
/// database's collections again, as it would have ended it before the next
/// burial.
#[test]
fn a_stop_that_comes_with_the_last_burial_ends_the_sweep_before_its_second_walk() {
    let (engine, _dir) = engine();
    let engine = Arc::new(engine);
    engine.create_collection("shop", "only").unwrap();
    engine
        .record_collection_drop(
            kimmy_core::database_drop_id("shop"),
            Stamp::new(Hlc::new(crate::physical_now_ms() + 1_000, 0), NodeId::from_bytes([7; 16])),
        )
        .unwrap();
    let stopper = Arc::clone(&engine);
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::Burial, move || {
        stopper.stop_walks();
    });
    let stopped = engine.bury_under_database_tombstones();
    assert!(
        matches!(stopped, Err(crate::StorageError::Stopping(_))),
        "the sweep went on to walk the database again after a stop"
    );
    assert!(!holds(&engine, "shop", "only"), "the burial that was under way landed whole");
}

// -- A collection held without its shadow ------------------------------------

/// A member holds a collection but not yet its vector configuration, which
/// arrives from another origin after the drop: burying the parent writes no
/// shadow tombstone, because there is no shadow to bury, and the shadow's first
/// chunk must still be history and not a stop.
#[test]
fn a_held_collection_without_its_shadow_still_leaves_the_shadows_tombstone() {
    let (b, _db) = engine();
    let (d, _dd) = engine();
    let (a, _da) = engine();
    b.create_collection("shop", "orders").unwrap();
    pull(&a, &b);
    assert!(holds(&a, "shop", "orders"));
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
    let drops: Vec<_> = d
        .entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .filter(|e| e.kind == OpKind::DropCollection)
        .collect();
    a.apply_batch(&drops).unwrap();
    let history =
        b.entries_for_peer(Hlc::ZERO, BATCH, crate::WalkScope::Background).unwrap().entries;
    let outcome = a.apply_batch(&history).unwrap();
    assert_eq!(outcome.unknown_collection, 0, "stalled: {:?}", outcome.unknown);
    assert!(!holds(&a, "shop", "orders"));
    assert!(
        !holds(&a, "shop", &kimmy_core::vector_meta::shadow_name("orders")),
        "the shadow was created over the drop"
    );
}

// -- The stop checkpoint before each shadow burial ---------------------------

/// What is left after the parents are buried is orphan shadows; a stop that
/// comes with the first ends the sweep before the second.
#[test]
fn a_stop_ends_a_sweep_between_two_shadow_burials() {
    let (engine, _dir) = engine();
    let engine = Arc::new(engine);
    for parent in ["x", "y"] {
        engine
            .create_system_collection("shop", &kimmy_core::vector_meta::shadow_name(parent))
            .unwrap();
    }
    engine
        .record_collection_drop(
            kimmy_core::database_drop_id("shop"),
            Stamp::new(Hlc::new(crate::physical_now_ms() + 1_000, 0), NodeId::from_bytes([7; 16])),
        )
        .unwrap();
    let stopper = Arc::clone(&engine);
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::Burial, move || {
        stopper.stop_walks();
    });
    let stopped = engine.bury_under_database_tombstones();
    assert!(
        matches!(stopped, Err(crate::StorageError::Stopping(_))),
        "a stop did not end the sweep"
    );
    assert_eq!(
        engine.list_collections("shop").unwrap().len(),
        1,
        "one shadow's burial landed whole and the next did not begin"
    );
}

// -- The tombstones owed with an append -------------------------------------

/// A drop already held, delivered again, takes no writer to record what is held.
#[test]
fn a_drop_already_held_records_nothing_and_takes_no_writer() {
    let (e, _d) = engine();
    let drop = crate::engine::ddl_entry(
        stamp_of(5_000, 3),
        OpKind::DropCollection,
        CollectionId::derive("shop", "ghost"),
        &CollectionRef::new("shop", "ghost"),
    )
    .unwrap();
    e.apply_batch(std::slice::from_ref(&drop)).unwrap();
    let slot = crate::engine::WriterHolder::Ddl.slot();
    let before = e.writer_hold().count[slot];
    e.apply_batch(std::slice::from_ref(&drop)).unwrap();
    assert_eq!(
        e.writer_hold().count[slot],
        before,
        "a held drop took the writer to record nothing"
    );
}

/// The entry is appended by another path between the check and the writer: the
/// tombstones it owed are recorded all the same.
#[test]
fn a_drop_appended_by_another_path_while_it_applies_still_leaves_its_tombstones() {
    let (e, _d) = engine();
    let e = Arc::new(e);
    let ghost = CollectionId::derive("shop", "ghost");
    let drop = crate::engine::ddl_entry(
        stamp_of(5_000, 3),
        OpKind::DropCollection,
        ghost,
        &CollectionRef::new("shop", "ghost"),
    )
    .unwrap();
    let competing = Arc::clone(&e);
    let appended = drop.clone();
    let (applied, ()) = crate::sync::race_hooks::race(
        crate::sync::race_hooks::Race::DdlAppend,
        move || {
            // The entry alone, with none of the tombstones it owes.
            let txn = competing.begin_write(crate::WriterHolder::Replication).unwrap();
            crate::engine::append_oplog(&txn, &appended).unwrap();
            txn.commit().unwrap();
        },
        || e.apply_batch(std::slice::from_ref(&drop)),
    );
    applied.unwrap();
    assert!(e.oplog_entry(&drop.stamp).unwrap().is_some());
    assert_eq!(e.collection_dropped_at(ghost).unwrap(), Some(drop.stamp), "the tombstone was lost");
}
