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
