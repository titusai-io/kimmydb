//! The definition digest's probe (B3 half 2, design 3.3 to 3.5): what is
//! compared, what is deferred and why, the pinned transaction, and the
//! explanation of a refusal. One test per clause, each of which fails when its
//! own clause is mutated and not a neighbour's.

use bson::doc;
use kimmy_core::{CollectionId, Hlc, IndexField, NodeId, Stamp, VersionVector};
use serde_bytes::ByteBuf;

use crate::definition_digest::{DefinitionKey, definition_list, index_hash};
use crate::definition_probe::{
    DefinitionAsk, DefinitionDefer, DefinitionEntry, DefinitionOutcome, DefinitionVerdict,
    test_hooks,
};
use crate::meta::CollectionMeta;
use crate::refusal_hint::{HintKind, RefusalHint};
use crate::{Engine, WalkScope};

const BATCH: usize = 1024;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn pull(into: &Engine, from: &Engine) {
    let mine = into.version_vector().unwrap();
    let theirs = from.version_vector().unwrap();
    if let Some(start) = mine.behind(&theirs) {
        let entries = from.entries_for_peer(start, BATCH, WalkScope::Background).unwrap().entries;
        into.apply_batch(&entries).unwrap();
    }
}

fn sync(a: &Engine, b: &Engine) {
    pull(a, b);
    pull(b, a);
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn fields(path: &str) -> Vec<IndexField> {
    vec![IndexField::ascending(path)]
}

/// Two members holding `shop.orders` and `shop.items`, converged, with nothing
/// else.
fn converged() -> (Engine, Engine, CollectionId, CollectionId, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    let orders = a.create_collection("shop", "orders").unwrap();
    let items = a.create_collection("shop", "items").unwrap();
    a.insert(&orders, doc! { "_id": 1 }).unwrap();
    sync(&a, &b);
    (a, b, orders.id, items.id, [da, db])
}

/// The cut both members have processed.
fn cut_of(a: &Engine, b: &Engine) -> VersionVector {
    a.witnessed_vector().unwrap().meet(&b.witnessed_vector().unwrap())
}

/// A requester `a` probing `b` at their cut, all the way through.
fn exchange(a: &Engine, b: &Engine, detail: &[CollectionId]) -> Vec<DefinitionVerdict> {
    exchange_at(a, b, cut_of(a, b), detail)
}

fn exchange_at(
    a: &Engine,
    b: &Engine,
    cut: VersionVector,
    detail: &[CollectionId],
) -> Vec<DefinitionVerdict> {
    let ask = DefinitionAsk::new(cut, detail.iter().copied());
    let reply = b.definition_answer(&ask).unwrap();
    a.definition_compare(&ask, &reply).unwrap()
}

fn outcome_of(verdicts: &[DefinitionVerdict], id: CollectionId) -> Option<DefinitionOutcome> {
    verdicts.iter().find(|v| v.collection == id).map(|v| v.outcome.clone())
}

/// Whether the collection was compared, and found equal.
fn compared(verdicts: &[DefinitionVerdict], id: CollectionId) -> Option<bool> {
    match outcome_of(verdicts, id) {
        Some(DefinitionOutcome::Compared { equal, .. }) => Some(equal),
        _ => None,
    }
}

fn deferred(verdicts: &[DefinitionVerdict], id: CollectionId) -> Option<DefinitionDefer> {
    match outcome_of(verdicts, id) {
        Some(DefinitionOutcome::Deferred(reason)) => Some(reason),
        _ => None,
    }
}

/// Rewrite the stored definition of `shop.orders` on `e`: how a test makes a
/// divergence the replication would not have.
fn edit(e: &Engine, name: &str, change: impl FnOnce(&mut CollectionMeta)) {
    let mut meta = e.get_collection("shop", name).unwrap();
    change(&mut meta);
    let txn = e.begin_write(crate::WriterHolder::Ddl).unwrap();
    Engine::put_collection_meta(&txn, &meta).unwrap();
    txn.commit().unwrap();
}

/// Both members hold index `name` on `shop.orders`, covered by the cut.
fn converged_with_index(name: &str) -> (Engine, Engine, CollectionId, [tempfile::TempDir; 2]) {
    let (a, b, orders, _items, dirs) = converged();
    a.create_index("shop", "orders", fields(name), false, Some(name.into())).unwrap();
    sync(&a, &b);
    (a, b, orders, dirs)
}

fn hash_of(e: &Engine, collection: &str, index: &str) -> u128 {
    index_hash(e.get_collection("shop", collection).unwrap().index(index).unwrap())
}

// -- What is compared -----------------------------------------------------------

#[test]
fn members_holding_the_same_definitions_compare_equal_in_both_directions() {
    let (a, b, orders, dirs) = converged_with_index("by_a");
    let _dirs = dirs;
    for (x, y) in [(&a, &b), (&b, &a)] {
        let verdicts = exchange(x, y, &[]);
        assert_eq!(compared(&verdicts, orders), Some(true), "{verdicts:?}");
    }
}

#[test]
fn a_covered_difference_is_a_mismatch_that_names_nothing_until_the_detail_is_asked() {
    let (a, b, orders, _dirs) = converged_with_index("by_a");
    edit(&b, "orders", |meta| meta.indexes.clear());
    let verdicts = exchange(&a, &b, &[]);
    match outcome_of(&verdicts, orders) {
        Some(DefinitionOutcome::Compared { equal: false, differing, .. }) => {
            assert!(differing.is_empty(), "names are asked for, not volunteered: {differing:?}");
        }
        other => panic!("expected a mismatch, got {other:?}"),
    }
    let verdicts = exchange(&a, &b, &[orders]);
    match outcome_of(&verdicts, orders) {
        Some(DefinitionOutcome::Compared { equal: false, differing, .. }) => {
            assert_eq!(differing, vec![DefinitionKey::index("by_a")]);
        }
        other => panic!("expected a mismatch with its name, got {other:?}"),
    }
}

#[test]
fn a_difference_in_one_part_of_one_definition_names_that_definition() {
    let (a, b, orders, _dirs) = converged_with_index("ttl");
    edit(&b, "orders", |meta| meta.indexes[0].expire_after_secs = Some(60));
    let verdicts = exchange(&a, &b, &[orders]);
    match outcome_of(&verdicts, orders) {
        Some(DefinitionOutcome::Compared { equal: false, differing, .. }) => {
            assert_eq!(differing, vec![DefinitionKey::index("ttl")]);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_vector_configuration_one_side_lacks_is_named_by_its_reserved_name() {
    let (a, b, orders, _items, _dirs) = converged();
    a.configure_vectors("shop", "orders", crate::definition_probe_tests::byo()).unwrap();
    sync(&a, &b);
    edit(&b, "orders", |meta| meta.vector = None);
    let verdicts = exchange(&a, &b, &[orders]);
    match outcome_of(&verdicts, orders) {
        Some(DefinitionOutcome::Compared { equal: false, differing, .. }) => {
            assert_eq!(differing, vec![DefinitionKey::vector()]);
        }
        other => panic!("{other:?}"),
    }
}

pub(crate) fn byo() -> kimmy_core::VectorConfig {
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

#[test]
fn a_collection_only_one_side_holds_is_the_existence_halfs_business() {
    let (a, b, orders, _items, _dirs) = converged();
    let extra = a.create_collection("shop", "extra").unwrap();
    let verdicts = exchange(&a, &b, &[]);
    assert!(outcome_of(&verdicts, extra.id).is_none(), "{verdicts:?}");
    assert_eq!(compared(&verdicts, orders), Some(true));
}

#[test]
fn a_vector_shadow_is_not_a_collection_to_compare() {
    let (a, b, orders, _items, _dirs) = converged();
    a.configure_vectors("shop", "orders", byo()).unwrap();
    sync(&a, &b);
    let shadow = CollectionId::derive("shop", &kimmy_core::vector_meta::shadow_name("orders"));
    let verdicts = exchange(&a, &b, &[]);
    assert!(outcome_of(&verdicts, shadow).is_none(), "{verdicts:?}");
    assert_eq!(compared(&verdicts, orders), Some(true));
}

// -- The stamp the cut must cover ---------------------------------------------------

/// The cut where `node`'s component is `at` and everything else is the pair's.
fn cut_with(base: &VersionVector, node: NodeId, at: Hlc) -> VersionVector {
    let mut cut = base.clone();
    cut.insert(node, at);
    cut
}

#[test]
fn an_index_stamped_above_the_cut_defers_the_collection_and_one_at_the_cut_does_not() {
    let (a, b, orders, _dirs) = converged_with_index("by_a");
    let stamp = a.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap();
    let base = cut_of(&a, &b);
    // At the cut: processed by both, compared.
    let verdicts = exchange_at(&a, &b, cut_with(&base, stamp.node, stamp.hlc), &[]);
    assert_eq!(compared(&verdicts, orders), Some(true), "a stamp at the cut: {verdicts:?}");
    // One below the stamp: not yet covered, on both sides.
    let below = Hlc::new(stamp.hlc.wall_ms - 1, 0);
    let verdicts = exchange_at(&a, &b, cut_with(&base, stamp.node, below), &[]);
    assert_eq!(
        deferred(&verdicts, orders),
        Some(DefinitionDefer::DefinitionAboveCut),
        "a stamp above the cut: {verdicts:?}"
    );
}

#[test]
fn a_definition_stamped_above_the_cut_on_the_responders_side_alone_defers_too() {
    let (a, b, orders, _dirs) = converged_with_index("by_a");
    later();
    // B drops the index: a stamp above the cut that A has not witnessed.
    b.drop_index("shop", "orders", "by_a").unwrap();
    let verdicts = exchange_at(&a, &b, cut_of(&a, &b), &[]);
    assert_eq!(
        deferred(&verdicts, orders),
        Some(DefinitionDefer::DefinitionAboveCut),
        "{verdicts:?}"
    );
}

#[test]
fn a_dropped_indexs_tombstone_stamp_counts_as_a_definition_stamp() {
    let (a, b, orders, _dirs) = converged_with_index("by_a");
    later();
    a.drop_index("shop", "orders", "by_a").unwrap();
    // The drop is above B's witnessed vector, so above the cut.
    let verdicts = exchange(&a, &b, &[]);
    assert_eq!(
        deferred(&verdicts, orders),
        Some(DefinitionDefer::DefinitionAboveCut),
        "{verdicts:?}"
    );
    // And once both have it, the collections compare equal.
    sync(&a, &b);
    let verdicts = exchange(&a, &b, &[]);
    assert_eq!(compared(&verdicts, orders), Some(true), "{verdicts:?}");
}

#[test]
fn a_vector_configurations_stamp_counts_as_a_definition_stamp() {
    let (a, b, orders, _items, _dirs) = converged();
    later();
    a.configure_vectors("shop", "orders", byo()).unwrap();
    let verdicts = exchange(&a, &b, &[]);
    assert_eq!(
        deferred(&verdicts, orders),
        Some(DefinitionDefer::DefinitionAboveCut),
        "{verdicts:?}"
    );
    sync(&a, &b);
    let verdicts = exchange(&a, &b, &[]);
    assert_eq!(compared(&verdicts, orders), Some(true));
}

#[test]
fn an_index_with_no_creation_stamp_cannot_defer_and_cannot_be_hidden() {
    let (a, b, orders, _dirs) = converged_with_index("by_a");
    edit(&a, "orders", |meta| meta.indexes[0].created = None);
    edit(&b, "orders", |meta| meta.indexes[0].created = None);
    let verdicts = exchange(&a, &b, &[]);
    assert_eq!(compared(&verdicts, orders), Some(true), "{verdicts:?}");
    edit(&b, "orders", |meta| meta.indexes[0].unique = true);
    let verdicts = exchange(&a, &b, &[]);
    assert_eq!(
        compared(&verdicts, orders),
        Some(false),
        "a difference is still found: {verdicts:?}"
    );
}

// -- Horizon -------------------------------------------------------------------------

#[test]
fn a_cut_below_what_retention_removed_defers_every_collection_on_the_horizon() {
    let (a, b, orders, items, _dirs) = converged();
    a.insert(&a.get_collection("shop", "orders").unwrap(), doc! { "_id": 100 }).unwrap();
    a.insert(&a.get_collection("shop", "orders").unwrap(), doc! { "_id": 101 }).unwrap();
    later();
    a.collect_garbage(crate::RetentionPolicy::new(0, 3_600)).unwrap();
    // B's cut is below what A collected: the requester's view of the cut says
    // so on its own side, and the responder's on its.
    let verdicts = exchange(&a, &b, &[]);
    for id in [orders, items] {
        assert_eq!(deferred(&verdicts, id), Some(DefinitionDefer::Horizon), "{verdicts:?}");
    }
    let verdicts = exchange(&b, &a, &[]);
    for id in [orders, items] {
        assert_eq!(deferred(&verdicts, id), Some(DefinitionDefer::Horizon), "{verdicts:?}");
    }
}

// -- The drop tombstones: the shared rule --------------------------------------------

#[test]
fn a_collection_tombstone_defers_while_the_drop_is_above_the_cut_or_the_life_is_not_newer() {
    let (a, b, orders, _items, _dirs) = converged();
    let created = a.get_collection("shop", "orders").unwrap().created;
    // A drop stamped exactly at the creation of the life standing: the life is
    // not newer than the drop.
    let tie = Stamp::new(created, a.node_id());
    a.record_collection_drop(orders, tie).unwrap();
    let verdicts = exchange(&a, &b, &[]);
    assert_eq!(deferred(&verdicts, orders), Some(DefinitionDefer::Incarnation), "{verdicts:?}");
}

#[test]
fn a_covered_drop_over_a_newer_life_does_not_defer() {
    let (a, b, orders, _items, _dirs) = converged();
    let created = a.get_collection("shop", "orders").unwrap().created;
    let before = Stamp::new(Hlc::new(created.wall_ms - 1, 0), a.node_id());
    a.record_collection_drop(orders, before).unwrap();
    // The drop arrived after the life, so the life owes ADR-231's sweep; done here,
    // as an apply does it, before the drop is judged covered (the state a member
    // leaves standing is swept; the one that owes it has its own test).
    a.sweep_late_drop("shop", "orders", before).unwrap();
    // The cut covers a drop stamped before everything either wrote.
    let verdicts = exchange(&a, &b, &[]);
    assert_eq!(compared(&verdicts, orders), Some(true), "{verdicts:?}");
    // One above the cut: not covered.
    let future = Stamp::new(Hlc::new(crate::physical_now_ms() + 60_000, 0), a.node_id());
    a.record_collection_drop(orders, future).unwrap();
    // Swept too, so that what defers it is the cut and not the sweep it owes.
    a.sweep_late_drop("shop", "orders", future).unwrap();
    let verdicts = exchange(&a, &b, &[]);
    assert_eq!(deferred(&verdicts, orders), Some(DefinitionDefer::Incarnation), "{verdicts:?}");
}

#[test]
fn a_database_tombstone_defers_its_collections_by_the_same_rule() {
    let (a, b, orders, items, _dirs) = converged();
    let db_drop = kimmy_core::database_drop_id("shop");
    let future = Stamp::new(Hlc::new(crate::physical_now_ms() + 60_000, 0), a.node_id());
    a.record_database_tombstone_for_test(db_drop, future).unwrap();
    let verdicts = exchange(&a, &b, &[]);
    for id in [orders, items] {
        assert_eq!(
            deferred(&verdicts, id),
            Some(DefinitionDefer::DatabaseTombstone),
            "{verdicts:?}"
        );
    }
}

/// A life that owes ADR-231's sweep for a covered drop (created above the drop,
/// floor below it) still holds what the life before put in it: the definition
/// digest defers it as the cut digest does, until the floor is at the drop.
#[test]
fn a_covered_drop_defers_a_life_that_owes_the_sweep_until_its_floor_is_at_the_drop() {
    let (e, f, _orders, items, _dirs) = converged();
    let origin = NodeId::generate();
    let at = Hlc::new(crate::physical_now_ms() - 5_000, 0);
    for node in [&e, &f] {
        let entry = kimmy_core::OplogEntry {
            stamp: Stamp::new(at, origin),
            kind: kimmy_core::OpKind::Insert,
            collection: items,
            doc_id: Some(kimmy_core::DocId::Int64(8_000)),
            body: Some(bson::serialize_to_vec(&doc! { "_id": 8000i64 }).unwrap()),
        };
        let meta = node.get_collection("shop", "items").unwrap();
        node.apply_remote(&meta, &entry).unwrap();
    }
    let fresh = e.create_collection("shop", "fresh").unwrap();
    e.insert(&fresh, doc! { "_id": 1i64 }).unwrap();
    sync(&e, &f);
    assert!(fresh.created > at, "premise: the life is newer than the drop");
    e.record_collection_drop(fresh.id, Stamp::new(at, origin)).unwrap();
    let floored = |floor: Option<Hlc>| {
        let mut meta = e.get_collection("shop", "fresh").unwrap();
        meta.incarnation_floor = floor;
        let txn = e.begin_write(crate::WriterHolder::Ddl).unwrap();
        Engine::put_collection_meta(&txn, &meta).unwrap();
        txn.commit().unwrap();
    };
    // The cut covers the drop, and the life owes its sweep: not compared.
    for owing in [None, Some(Hlc::new(at.wall_ms - 1, 0))] {
        floored(owing);
        let verdicts = exchange(&e, &f, &[]);
        assert_eq!(
            deferred(&verdicts, fresh.id),
            Some(DefinitionDefer::Incarnation),
            "floor {owing:?}: {verdicts:?}"
        );
    }
    // The sweep done: the floor at the drop, and above it.
    for done in [at, Hlc::new(at.wall_ms + 1, 0)] {
        floored(Some(done));
        let verdicts = exchange(&e, &f, &[]);
        assert_eq!(compared(&verdicts, fresh.id), Some(true), "floor {done:?}: {verdicts:?}");
    }
}

// -- The single pinned transaction ------------------------------------------------------

/// Every predicate is read through the one transaction a side opens. A commit
/// that lands after the transaction opened is not seen by any of them: here an
/// index created (a stamp above the cut, a changed digest) between the open and
/// the reads. A predicate read in a transaction of its own would see it and
/// defer, or compare a digest the rest of the answer does not describe.
#[test]
fn a_commit_after_the_transaction_opened_is_seen_by_no_predicate_on_the_responder() {
    let (a, b, orders, _dirs) = converged_with_index("by_a");
    let b = std::sync::Arc::new(b);
    let writer = std::sync::Arc::clone(&b);
    test_hooks::after_the_transaction(move |_| {
        writer.drop_index("shop", "orders", "by_a").unwrap();
    });
    let ask = DefinitionAsk::new(cut_of(&a, &b), []);
    let reply = b.definition_answer(&ask).unwrap();
    let verdicts = a.definition_compare(&ask, &reply).unwrap();
    assert_eq!(
        compared(&verdicts, orders),
        Some(true),
        "the answer describes the snapshot it was read from: {verdicts:?}"
    );
}

#[test]
fn a_commit_after_the_transaction_opened_is_seen_by_no_predicate_on_the_requester() {
    let (a, b, orders, _dirs) = converged_with_index("by_a");
    let a = std::sync::Arc::new(a);
    let ask = DefinitionAsk::new(cut_of(&a, &b), []);
    let reply = b.definition_answer(&ask).unwrap();
    let writer = std::sync::Arc::clone(&a);
    test_hooks::after_the_transaction(move |_| {
        writer.drop_index("shop", "orders", "by_a").unwrap();
    });
    let verdicts = a.definition_compare(&ask, &reply).unwrap();
    assert_eq!(compared(&verdicts, orders), Some(true), "{verdicts:?}");
}

/// The drop tombstone is read through the same transaction: one recorded after it
/// opened defers nothing in this answer.
#[test]
fn a_tombstone_recorded_after_the_transaction_opened_is_seen_by_no_predicate() {
    let (a, b, orders, _dirs) = converged_with_index("by_a");
    let b = std::sync::Arc::new(b);
    let writer = std::sync::Arc::clone(&b);
    test_hooks::after_the_transaction(move |_| {
        let future = Stamp::new(Hlc::new(crate::physical_now_ms() + 60_000, 0), writer.node_id());
        writer.record_collection_drop(orders, future).unwrap();
    });
    let ask = DefinitionAsk::new(cut_of(&a, &b), []);
    let reply = b.definition_answer(&ask).unwrap();
    let verdicts = a.definition_compare(&ask, &reply).unwrap();
    assert_eq!(compared(&verdicts, orders), Some(true), "{verdicts:?}");
}

/// A refusal hint recorded after the transaction opened explains nothing in this
/// comparison: the hints are read through the pinned transaction too.
#[test]
fn a_hint_recorded_after_the_transaction_opened_explains_nothing() {
    let (a, b, orders, _dirs) = converged_with_index("ttl");
    let hash = hash_of(&a, "orders", "ttl");
    let stamp = a.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap();
    edit(&a, "orders", |meta| meta.indexes.clear());
    let a = std::sync::Arc::new(a);
    let ask = DefinitionAsk::new(cut_of(&a, &b), [orders]);
    let reply = b.definition_answer(&ask).unwrap();
    // The hook fires in the requester's comparison, after its transaction opened.
    let writer = std::sync::Arc::clone(&a);
    test_hooks::after_the_transaction(move |_| {
        writer
            .record_refusal_hint(&RefusalHint::new(orders, HintKind::Index, "ttl", stamp, hash))
            .unwrap();
    });
    let verdicts = a.definition_compare(&ask, &reply).unwrap();
    assert_eq!(
        compared(&verdicts, orders),
        Some(false),
        "the hint was not there yet: {verdicts:?}"
    );
}

/// The serve verdict (the horizon) is read through the pinned transaction: a
/// retention pass that lands after it opened does not turn this answer into a
/// deferral.
#[test]
fn a_retention_pass_after_the_transaction_opened_does_not_move_the_horizon() {
    let (a, b, orders, _items, _dirs) = converged();
    let a = std::sync::Arc::new(a);
    let on_a = a.get_collection("shop", "orders").unwrap();
    a.insert(&on_a, doc! { "_id": 100 }).unwrap();
    a.insert(&on_a, doc! { "_id": 101 }).unwrap();
    later();
    let collector = std::sync::Arc::clone(&a);
    test_hooks::after_the_transaction(move |_| {
        collector.collect_garbage(crate::RetentionPolicy::new(0, 3_600)).unwrap();
    });
    // B asks A at B's cut, which A has entries above: the answer reads A at the
    // snapshot, before the collection.
    let ask = DefinitionAsk::new(cut_of(&b, &a), []);
    let reply = a.definition_answer(&ask).unwrap();
    let entry = reply.entries.iter().find(|e| e.collection == orders).unwrap();
    assert_eq!(entry.deferred, None, "the snapshot was not collected: {entry:?}");
}

// -- The answer's own faults --------------------------------------------------------------

#[test]
fn a_digest_of_the_wrong_width_is_malformed_and_never_a_difference() {
    let (a, b, orders, _dirs) = converged_with_index("by_a");
    let ask = DefinitionAsk::new(cut_of(&a, &b), []);
    let mut reply = b.definition_answer(&ask).unwrap();
    for entry in &mut reply.entries {
        entry.digest = ByteBuf::from(vec![1, 2, 3]);
    }
    let verdicts = a.definition_compare(&ask, &reply).unwrap();
    assert_eq!(deferred(&verdicts, orders), Some(DefinitionDefer::Malformed), "{verdicts:?}");
}

#[test]
fn an_entry_the_peer_deferred_is_deferred_here_with_the_peers_reason() {
    let (a, b, orders, _dirs) = converged_with_index("by_a");
    let ask = DefinitionAsk::new(cut_of(&a, &b), []);
    let mut reply = b.definition_answer(&ask).unwrap();
    reply.entries = vec![DefinitionEntry {
        collection: orders,
        digest: ByteBuf::from(vec![0; 16]),
        deferred: Some(DefinitionDefer::DefinitionAboveCut),
        above: None,
        definitions: Vec::new(),
        refused: Vec::new(),
    }];
    let verdicts = a.definition_compare(&ask, &reply).unwrap();
    assert_eq!(
        deferred(&verdicts, orders),
        Some(DefinitionDefer::DefinitionAboveCut),
        "{verdicts:?}"
    );
}

// -- Refusal hints: a hint, never a mask ---------------------------------------------------

/// B holds `ttl` and A refused it: A lacks the index and holds a hint with the
/// hash of what it refused. Covered by the cut on both sides.
fn a_refused_what_b_holds() -> (Engine, Engine, CollectionId, [tempfile::TempDir; 2]) {
    let (a, b, orders, dirs) = converged_with_index("ttl");
    let hash = hash_of(&a, "orders", "ttl");
    let stamp = a.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap();
    edit(&a, "orders", |meta| meta.indexes.clear());
    a.record_refusal_hint(&RefusalHint::new(orders, HintKind::Index, "ttl", stamp, hash)).unwrap();
    (a, b, orders, dirs)
}

#[test]
fn a_mismatch_the_requesters_own_refusal_explains_is_explained_and_not_compared() {
    let (a, b, orders, _dirs) = a_refused_what_b_holds();
    // Without the detail, the first contact is a plain mismatch.
    let verdicts = exchange(&a, &b, &[]);
    assert_eq!(compared(&verdicts, orders), Some(false), "{verdicts:?}");
    // With it, the requester's own hint covers the one differing name.
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(
        outcome_of(&verdicts, orders),
        Some(DefinitionOutcome::Explained { differing: vec![DefinitionKey::index("ttl")] }),
        "{verdicts:?}"
    );
}

#[test]
fn a_mismatch_the_responders_refusal_explains_is_explained_too() {
    // B refused A's definition: B lacks it and holds the hint; A is the
    // requester and holds what B refused.
    let (a, b, orders, _dirs) = converged_with_index("ttl");
    let hash = hash_of(&a, "orders", "ttl");
    let stamp = a.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap();
    edit(&b, "orders", |meta| meta.indexes.clear());
    b.record_refusal_hint(&RefusalHint::new(orders, HintKind::Index, "ttl", stamp, hash)).unwrap();
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(
        outcome_of(&verdicts, orders),
        Some(DefinitionOutcome::Explained { differing: vec![DefinitionKey::index("ttl")] }),
        "{verdicts:?}"
    );
}

#[test]
fn a_hint_never_masks_an_unrelated_difference_on_the_same_collection() {
    let (a, b, orders, _dirs) = a_refused_what_b_holds();
    a.create_index("shop", "orders", fields("other"), false, Some("other".into())).unwrap();
    sync(&a, &b);
    edit(&b, "orders", |meta| meta.indexes.retain(|i| i.name != "other"));
    let verdicts = exchange(&a, &b, &[orders]);
    match outcome_of(&verdicts, orders) {
        Some(DefinitionOutcome::Compared { equal: false, differing, .. }) => {
            assert_eq!(differing, vec![DefinitionKey::index("other"), DefinitionKey::index("ttl")]);
        }
        other => panic!("one hint explains one name, not both: {other:?}"),
    }
}

#[test]
fn a_hint_is_valid_only_while_the_peers_hash_is_the_one_it_stored() {
    let (a, b, orders, _dirs) = a_refused_what_b_holds();
    // B replaces its definition under the same name: the hint is about a
    // definition B no longer holds.
    edit(&b, "orders", |meta| meta.indexes[0].expire_after_secs = Some(30));
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(compared(&verdicts, orders), Some(false), "{verdicts:?}");
}

#[test]
fn a_hint_does_not_explain_a_name_the_member_holds_in_the_same_form() {
    let (a, b, orders, _dirs) = a_refused_what_b_holds();
    // A can apply it now (a rollback to a build that never clears rows, or a
    // fix): the hint is stale and the digests are equal.
    let ttl = b.get_collection("shop", "orders").unwrap().indexes[0].clone();
    edit(&a, "orders", |meta| meta.indexes.push(ttl));
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(compared(&verdicts, orders), Some(true), "{verdicts:?}");
}

#[test]
fn a_hint_at_or_below_the_standing_lifes_floor_explains_nothing() {
    let (a, b, orders, _dirs) = a_refused_what_b_holds();
    let hint = a.refusal_hints(orders).unwrap().remove(0);
    edit(&a, "orders", |meta| meta.incarnation_floor = Some(hint.stamp.hlc));
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(compared(&verdicts, orders), Some(false), "a floor at the stamp: {verdicts:?}");
    edit(&a, "orders", |meta| {
        meta.incarnation_floor = Some(Hlc::new(hint.stamp.hlc.wall_ms - 1, 0))
    });
    let verdicts = exchange(&a, &b, &[orders]);
    assert!(
        matches!(outcome_of(&verdicts, orders), Some(DefinitionOutcome::Explained { .. })),
        "a floor below it: {verdicts:?}"
    );
}

/// h2-N3: the requester cannot evaluate the peer's floor, so the responder
/// applies the floor clause before it puts a pair in `refused`.
#[test]
fn the_responder_applies_the_floor_clause_before_it_sends_a_pair() {
    let (a, b, orders, _dirs) = converged_with_index("ttl");
    let hash = hash_of(&a, "orders", "ttl");
    let stamp = a.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap();
    edit(&b, "orders", |meta| meta.indexes.clear());
    b.record_refusal_hint(&RefusalHint::new(orders, HintKind::Index, "ttl", stamp, hash)).unwrap();
    let ask = DefinitionAsk::new(cut_of(&a, &b), [orders]);
    let reply = b.definition_answer(&ask).unwrap();
    let entry = reply.entries.iter().find(|e| e.collection == orders).unwrap();
    assert_eq!(entry.refused.len(), 1, "above the floor: sent");
    edit(&b, "orders", |meta| meta.incarnation_floor = Some(stamp.hlc));
    let reply = b.definition_answer(&ask).unwrap();
    let entry = reply.entries.iter().find(|e| e.collection == orders).unwrap();
    assert!(entry.refused.is_empty(), "at the floor: not sent");
    let reply_not_asked = b.definition_answer(&DefinitionAsk::new(cut_of(&a, &b), [])).unwrap();
    assert!(
        reply_not_asked.entries.iter().all(|e| e.refused.is_empty() && e.definitions.is_empty()),
        "detail only for the collections asked for"
    );
}

/// The opposite shape: this member holds an index the peer dropped and cannot
/// drop. Its hint covers the difference on the side that holds it, and is valid
/// only while the index it holds is the one whose hash it stored.
#[test]
fn a_refused_drop_is_explained_on_the_side_that_holds_the_index() {
    let (a, b, orders, _dirs) = converged_with_index("held");
    let hash = hash_of(&a, "orders", "held");
    let stamp = a.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap();
    // B dropped it (it lacks it); A holds it and refused the drop.
    edit(&b, "orders", |meta| meta.indexes.clear());
    a.record_refusal_hint(&RefusalHint::new(orders, HintKind::DropIndex, "held", stamp, hash))
        .unwrap();
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(
        outcome_of(&verdicts, orders),
        Some(DefinitionOutcome::Explained { differing: vec![DefinitionKey::index("held")] }),
        "{verdicts:?}"
    );
    // The held index is replaced by a different definition: the hint is about
    // an index this member no longer holds, and masks nothing.
    edit(&a, "orders", |meta| meta.indexes[0].unique = true);
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(compared(&verdicts, orders), Some(false), "{verdicts:?}");
}

#[test]
fn a_refusal_nobody_recorded_is_not_explained() {
    let (a, b, orders, _dirs) = converged_with_index("ttl");
    edit(&a, "orders", |meta| meta.indexes.clear());
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(compared(&verdicts, orders), Some(false), "no hint, no hold: {verdicts:?}");
}

// -- The hint rows -------------------------------------------------------------------------

#[test]
fn a_hint_round_trips_and_a_name_with_colons_is_kept_whole() {
    let (a, _b, orders, _items, _dirs) = converged();
    let stamp = Stamp::new(Hlc::new(50, 3), NodeId::from_bytes([4; 16]));
    let hint = RefusalHint::new(orders, HintKind::Index, "a:b:c", stamp, 0xabcd);
    a.record_refusal_hint(&hint).unwrap();
    assert_eq!(a.refusal_hints(orders).unwrap(), vec![hint.clone()]);
    // An older refusal never replaces a newer one for the same key.
    let older = RefusalHint::new(
        orders,
        HintKind::Index,
        "a:b:c",
        Stamp::new(Hlc::new(40, 0), stamp.node),
        0x1111,
    );
    a.record_refusal_hint(&older).unwrap();
    assert_eq!(a.refusal_hints(orders).unwrap(), vec![hint]);
    // Another collection's hints are not this one's.
    let items = CollectionId::derive("shop", "items");
    assert!(a.refusal_hints(items).unwrap().is_empty());
}

#[test]
fn the_hints_of_a_collection_are_cleared_with_a_prefix_range() {
    let (a, _b, orders, items, _dirs) = converged();
    let stamp = Stamp::new(Hlc::new(50, 0), NodeId::from_bytes([4; 16]));
    for (collection, name) in [(orders, "x"), (orders, "y"), (items, "x")] {
        a.record_refusal_hint(&RefusalHint::new(collection, HintKind::Index, name, stamp, 1))
            .unwrap();
    }
    let txn = a.begin_write(crate::WriterHolder::Ddl).unwrap();
    assert_eq!(Engine::clear_refusal_hints_in(&txn, orders).unwrap(), 2);
    txn.commit().unwrap();
    assert!(a.refusal_hints(orders).unwrap().is_empty());
    assert_eq!(a.refusal_hints(items).unwrap().len(), 1, "the neighbour's survives");
}

#[test]
fn every_defer_reason_has_its_own_place_and_label() {
    for (n, reason) in DefinitionDefer::ALL.into_iter().enumerate() {
        assert_eq!(reason.index(), n);
        assert_eq!(DefinitionDefer::ALL.iter().filter(|r| r.label() == reason.label()).count(), 1);
    }
}

#[test]
fn a_definition_lists_hashes_are_what_the_answer_carries_for_the_detail() {
    let (a, b, orders, _dirs) = converged_with_index("by_a");
    let ask = DefinitionAsk::new(cut_of(&a, &b), [orders]);
    let reply = b.definition_answer(&ask).unwrap();
    let entry = reply.entries.iter().find(|e| e.collection == orders).unwrap();
    let list = definition_list(&b.get_collection("shop", "orders").unwrap());
    let carried: Vec<(DefinitionKey, u128)> = entry
        .definitions
        .iter()
        .map(|d| {
            (
                DefinitionKey { kind: d.kind, name: d.name.clone() },
                u128::from_be_bytes(d.hash.as_slice().try_into().expect("16 bytes")),
            )
        })
        .collect();
    assert_eq!(carried, list);
}

// -- Clauses the first mutation run found unpinned ------------------------------------

/// A drop hint covers a name the peer *lacks*. A peer that holds the name in
/// another form is a different difference, and the hint masks nothing.
#[test]
fn a_refused_drop_does_not_explain_a_name_the_peer_holds_in_another_form() {
    let (a, b, orders, _dirs) = converged_with_index("held");
    let hash = hash_of(&a, "orders", "held");
    let stamp = a.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap();
    a.record_refusal_hint(&RefusalHint::new(orders, HintKind::DropIndex, "held", stamp, hash))
        .unwrap();
    edit(&b, "orders", |meta| meta.indexes[0].unique = true);
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(compared(&verdicts, orders), Some(false), "{verdicts:?}");
}

/// The responder's pair says "I hold this hash and could not drop it". It
/// explains a name the requester lacks, and not one the requester holds in
/// another form.
#[test]
fn a_pair_for_a_held_index_does_not_explain_a_name_the_requester_holds_in_another_form() {
    let (a, b, orders, _dirs) = converged_with_index("held");
    let hash = hash_of(&b, "orders", "held");
    let stamp = b.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap();
    b.record_refusal_hint(&RefusalHint::new(orders, HintKind::DropIndex, "held", stamp, hash))
        .unwrap();
    // Control: A lacks the name, which the pair explains.
    let held = a.get_collection("shop", "orders").unwrap().indexes[0].clone();
    edit(&a, "orders", |meta| meta.indexes.clear());
    let verdicts = exchange(&a, &b, &[orders]);
    assert!(
        matches!(outcome_of(&verdicts, orders), Some(DefinitionOutcome::Explained { .. })),
        "control: {verdicts:?}"
    );
    // A holds the name, in another form.
    edit(&a, "orders", |meta| {
        let mut other = held.clone();
        other.unique = true;
        meta.indexes.push(other);
    });
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(compared(&verdicts, orders), Some(false), "{verdicts:?}");
}

/// The responder's pair says "I refused this hash". It explains a requester that
/// holds exactly that hash, and not one that holds another definition under the name.
#[test]
fn a_pair_for_a_refused_definition_does_not_explain_another_definition_under_the_name() {
    let (a, b, orders, _dirs) = converged_with_index("ttl");
    let hash = hash_of(&a, "orders", "ttl");
    let stamp = a.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap();
    edit(&b, "orders", |meta| meta.indexes.clear());
    b.record_refusal_hint(&RefusalHint::new(orders, HintKind::Index, "ttl", stamp, hash)).unwrap();
    edit(&a, "orders", |meta| meta.indexes[0].expire_after_secs = Some(30));
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(compared(&verdicts, orders), Some(false), "{verdicts:?}");
}

/// A digest that differs while both lists are the same is a peer that is not
/// consistent with itself: a mismatch nothing explains, never an explanation
/// of nothing.
#[test]
fn a_differing_digest_over_equal_lists_is_not_explained() {
    let (a, b, orders, _dirs) = converged_with_index("ttl");
    let ask = DefinitionAsk::new(cut_of(&a, &b), [orders]);
    let mut reply = b.definition_answer(&ask).unwrap();
    for entry in &mut reply.entries {
        if entry.collection == orders {
            let mut bytes = entry.digest.to_vec();
            bytes[0] ^= 1;
            entry.digest = ByteBuf::from(bytes);
        }
    }
    let verdicts = a.definition_compare(&ask, &reply).unwrap();
    match outcome_of(&verdicts, orders) {
        Some(DefinitionOutcome::Compared { equal: false, differing, .. }) => {
            assert!(differing.is_empty(), "{differing:?}");
        }
        other => panic!("nothing differs by name, so nothing is explained: {other:?}"),
    }
}

/// The responder reads the hints through its pinned transaction: one recorded
/// after it opened is not in the pairs of this answer.
#[test]
fn a_hint_recorded_after_the_responders_transaction_opened_is_not_in_its_pairs() {
    let (a, b, orders, _dirs) = converged_with_index("ttl");
    let hash = hash_of(&a, "orders", "ttl");
    let stamp = a.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap();
    edit(&b, "orders", |meta| meta.indexes.clear());
    let b = std::sync::Arc::new(b);
    let writer = std::sync::Arc::clone(&b);
    test_hooks::after_the_transaction(move |_| {
        writer
            .record_refusal_hint(&RefusalHint::new(orders, HintKind::Index, "ttl", stamp, hash))
            .unwrap();
    });
    let ask = DefinitionAsk::new(cut_of(&a, &b), [orders]);
    let reply = b.definition_answer(&ask).unwrap();
    let entry = reply.entries.iter().find(|e| e.collection == orders).unwrap();
    assert!(entry.refused.is_empty(), "the hint was not there yet: {entry:?}");
    // And it is there for the next answer.
    let reply = b.definition_answer(&ask).unwrap();
    let entry = reply.entries.iter().find(|e| e.collection == orders).unwrap();
    assert_eq!(entry.refused.len(), 1);
}

/// The database's drop tombstone is read through the pinned transaction too.
#[test]
fn a_database_tombstone_recorded_after_the_transaction_opened_is_seen_by_no_predicate() {
    let (a, b, orders, items, _dirs) = converged();
    let b = std::sync::Arc::new(b);
    let writer = std::sync::Arc::clone(&b);
    test_hooks::after_the_transaction(move |_| {
        let future = Stamp::new(Hlc::new(crate::physical_now_ms() + 60_000, 0), writer.node_id());
        writer
            .record_database_tombstone_for_test(kimmy_core::database_drop_id("shop"), future)
            .unwrap();
    });
    let ask = DefinitionAsk::new(cut_of(&a, &b), []);
    let reply = b.definition_answer(&ask).unwrap();
    let verdicts = a.definition_compare(&ask, &reply).unwrap();
    for id in [orders, items] {
        assert_eq!(compared(&verdicts, id), Some(true), "{verdicts:?}");
    }
}

// -- Review of the hint layer: kinds on the wire, keys by kind, a hint is mended --------

/// An `Index`-kind hint on the member that *now holds* the definition is stale:
/// the responder sends no pair for it, so a real loss on the requester is a
/// finding and not "explained".
#[test]
fn a_stale_refusal_on_the_member_that_holds_the_definition_explains_no_loss() {
    let (a, b, orders, _dirs) = converged_with_index("ttl");
    let hash = hash_of(&b, "orders", "ttl");
    let stamp = b.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap();
    // B once refused it (the hint), and holds it now.
    b.record_refusal_hint(&RefusalHint::new(orders, HintKind::Index, "ttl", stamp, hash)).unwrap();
    // A has lost the same definition for an unrelated reason.
    edit(&a, "orders", |meta| meta.indexes.clear());
    let ask = DefinitionAsk::new(cut_of(&a, &b), [orders]);
    let reply = b.definition_answer(&ask).unwrap();
    let entry = reply.entries.iter().find(|e| e.collection == orders).unwrap();
    assert!(entry.refused.is_empty(), "no pair for a refusal B has since mended: {entry:?}");
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(compared(&verdicts, orders), Some(false), "a finding: {verdicts:?}");
}

/// The opposite shape on the responder: a held-drop pair is sent only while the
/// responder holds exactly the hash it stored.
#[test]
fn a_held_drop_pair_is_sent_only_while_the_responder_holds_what_it_stored() {
    let (a, b, orders, _dirs) = converged_with_index("held");
    let hash = hash_of(&b, "orders", "held");
    let stamp = b.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap();
    b.record_refusal_hint(&RefusalHint::new(orders, HintKind::DropIndex, "held", stamp, hash))
        .unwrap();
    let ask = DefinitionAsk::new(cut_of(&a, &b), [orders]);
    let sent = |b: &Engine| {
        let reply = b.definition_answer(&ask).unwrap();
        reply.entries.iter().find(|e| e.collection == orders).unwrap().refused.clone()
    };
    let pairs = sent(&b);
    assert_eq!(pairs.len(), 1, "it holds what it stored: {pairs:?}");
    assert_eq!((pairs[0].hint, pairs[0].name.as_str()), (HintKind::DropIndex, "held"));
    edit(&b, "orders", |meta| meta.indexes[0].unique = true);
    assert!(sent(&b).is_empty(), "it holds another definition now");
    edit(&b, "orders", |meta| meta.indexes.clear());
    assert!(sent(&b).is_empty(), "it holds nothing now");
}

/// A refused vector configuration is explained, on the requester's own hint and
/// on the responder's pair, and is read by its own kind (not as a drop).
#[test]
fn a_refused_vector_configuration_is_explained_from_either_side() {
    let (a, b, orders, _items, _dirs) = converged();
    a.configure_vectors("shop", "orders", byo()).unwrap();
    sync(&a, &b);
    let held = b.get_collection("shop", "orders").unwrap();
    let stamp = held.vector_changed.unwrap();
    let hash = crate::definition_digest::vector_hash(held.vector.as_ref().unwrap());
    // A refused it (it lacks it and says so): A's own hint, A the requester.
    edit(&a, "orders", |meta| meta.vector = None);
    a.record_refusal_hint(&RefusalHint::new(orders, HintKind::Vector, "", stamp, hash)).unwrap();
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(
        outcome_of(&verdicts, orders),
        Some(DefinitionOutcome::Explained { differing: vec![DefinitionKey::vector()] }),
        "the requester's own: {verdicts:?}"
    );
    // The same from the responder's side: B is asked, and A's pair travels.
    let verdicts = exchange(&b, &a, &[orders]);
    assert!(
        matches!(outcome_of(&verdicts, orders), Some(DefinitionOutcome::Explained { .. })),
        "the responder's pair: {verdicts:?}"
    );
}

/// The turning-off of a vector configuration this member holds and could not
/// apply is explained on its own kind, and a hint of the wrong kind is not it.
#[test]
fn a_refused_turn_off_of_the_vector_configuration_is_explained_by_its_own_kind() {
    let (a, b, orders, _items, _dirs) = converged();
    a.configure_vectors("shop", "orders", byo()).unwrap();
    sync(&a, &b);
    let held = a.get_collection("shop", "orders").unwrap();
    let stamp = held.vector_changed.unwrap();
    let hash = crate::definition_digest::vector_hash(held.vector.as_ref().unwrap());
    // B turned it off; A holds it and could not.
    edit(&b, "orders", |meta| meta.vector = None);
    a.record_refusal_hint(&RefusalHint::new(orders, HintKind::DropVector, "", stamp, hash))
        .unwrap();
    let verdicts = exchange(&a, &b, &[orders]);
    assert!(
        matches!(outcome_of(&verdicts, orders), Some(DefinitionOutcome::Explained { .. })),
        "{verdicts:?}"
    );
    // The same row of the refused-definition kind is another shape: nothing here
    // is a definition B refused, so it explains nothing.
    let txn = a.begin_write(crate::WriterHolder::Ddl).unwrap();
    Engine::clear_refusal_hints_for_in(&txn, orders, &DefinitionKey::vector()).unwrap();
    txn.commit().unwrap();
    a.record_refusal_hint(&RefusalHint::new(orders, HintKind::Vector, "", stamp, hash)).unwrap();
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(compared(&verdicts, orders), Some(false), "wrong kind: {verdicts:?}");
}

/// An index can be named `__vector`: it is not the vector configuration, in the
/// list, in the digest, in what differs, and in which hint covers it.
#[test]
fn an_index_named_like_the_old_reserved_name_is_not_the_vector_configuration() {
    let (a, b, orders, _items, _dirs) = converged();
    a.configure_vectors("shop", "orders", byo()).unwrap();
    sync(&a, &b);
    a.create_index("shop", "orders", fields("zz"), false, Some("__vector".into())).unwrap();
    sync(&a, &b);
    let list = definition_list(&a.get_collection("shop", "orders").unwrap());
    assert_eq!(
        list.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>(),
        vec![DefinitionKey::index("__vector"), DefinitionKey::vector()],
        "two entries, each its own"
    );
    // A loses the index: what differs is the index, not the configuration.
    edit(&a, "orders", |meta| meta.indexes.clear());
    let verdicts = exchange(&a, &b, &[orders]);
    match outcome_of(&verdicts, orders) {
        Some(DefinitionOutcome::Compared { equal: false, differing, .. }) => {
            assert_eq!(differing, vec![DefinitionKey::index("__vector")]);
        }
        other => panic!("{other:?}"),
    }
    // A hint about the vector configuration does not cover the index.
    let held = b.get_collection("shop", "orders").unwrap();
    let hash = crate::definition_digest::vector_hash(held.vector.as_ref().unwrap());
    a.record_refusal_hint(&RefusalHint::new(
        orders,
        HintKind::Vector,
        "__vector",
        held.vector_changed.unwrap(),
        hash,
    ))
    .unwrap();
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(compared(&verdicts, orders), Some(false), "{verdicts:?}");
}

/// A refusal mended here does not outlive the fix: the hint explained the
/// difference, the operator created the definition on the member that lacked it
/// (the `operations.md` TTL sequence), and a later loss of the same definition is
/// a finding again.
#[test]
fn a_hint_does_not_outlive_the_fix_and_a_later_loss_is_a_finding() {
    let (a, b, orders, _dirs) = a_refused_what_b_holds();
    let verdicts = exchange(&a, &b, &[orders]);
    assert!(
        matches!(outcome_of(&verdicts, orders), Some(DefinitionOutcome::Explained { .. })),
        "premise: refused and explained: {verdicts:?}"
    );
    let held = b.get_collection("shop", "orders").unwrap().indexes[0].clone();
    // The operator creates it on the member that lacks it.
    a.create_index_with(
        "shop",
        "orders",
        held.fields.clone(),
        held.unique,
        held.enforcement,
        Some(held.name.clone()),
        held.expire_after_secs,
        held.partial_filter.clone(),
    )
    .unwrap();
    sync(&a, &b);
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(compared(&verdicts, orders), Some(true), "fixed: {verdicts:?}");
    assert!(a.refusal_hints(orders).unwrap().is_empty(), "the mended refusal is forgotten");
    edit(&a, "orders", |meta| meta.indexes.clear());
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(compared(&verdicts, orders), Some(false), "lost again: {verdicts:?}");
    // And two compared mismatches running confirm it (the tracker's rule).
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(compared(&verdicts, orders), Some(false), "{verdicts:?}");
}

/// A digest of another format is never compared, on any collection: the answer
/// says its format, and a different one (or none) defers with its own reason.
#[test]
fn an_answer_of_another_format_defers_every_collection() {
    let (a, b, orders, items, _dirs) = converged();
    let ask = DefinitionAsk::new(cut_of(&a, &b), []);
    let reply = b.definition_answer(&ask).unwrap();
    assert_eq!(reply.format, crate::definition_digest::FORMAT, "the responder says its format");
    let verdicts = a.definition_compare(&ask, &reply).unwrap();
    assert_eq!(compared(&verdicts, orders), Some(true), "the same format compares");
    for other in [crate::definition_digest::FORMAT + 1, 0] {
        let mut reply = reply.clone();
        reply.format = other;
        let verdicts = a.definition_compare(&ask, &reply).unwrap();
        for id in [orders, items] {
            assert_eq!(
                deferred(&verdicts, id),
                Some(DefinitionDefer::Format),
                "format {other}: {verdicts:?}"
            );
        }
    }
}

/// A hint row this build cannot read (a kind from a later build, the store rolled
/// back) is skipped: the answer, the comparison and the next refusal's write all
/// go on, and a hint written under the same key replaces it.
#[test]
fn an_unreadable_hint_row_costs_that_row_and_nothing_else() {
    let (a, b, orders, _dirs) = a_refused_what_b_holds();
    let put = |e: &Engine, key: String, value: Vec<u8>| {
        let txn = e.begin_write(crate::WriterHolder::Ddl).unwrap();
        {
            let mut meta = txn.open_table(crate::tables::META).unwrap();
            meta.insert(key.as_str(), value.as_slice()).unwrap();
        }
        txn.commit().unwrap();
    };
    for e in [&a, &b] {
        put(e, format!("ddl_refused:{}:future:x", orders.0), vec![1, 2, 3]);
        put(e, format!("ddl_refused:{}:index:short", orders.0), vec![1]);
    }
    // The readable hint still explains; the answer and the comparison go on.
    let verdicts = exchange(&a, &b, &[orders]);
    assert!(
        matches!(outcome_of(&verdicts, orders), Some(DefinitionOutcome::Explained { .. })),
        "{verdicts:?}"
    );
    assert_eq!(a.refusal_hints(orders).unwrap().len(), 1, "only the readable one");
    // A refusal's write is not failed by the rows beside it.
    let hash = hash_of(&b, "orders", "ttl");
    let stamp = b.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap();
    a.record_refusal_hint(&RefusalHint::new(orders, HintKind::Index, "other", stamp, hash))
        .unwrap();
    // And a hint under the unreadable row's own key replaces it.
    put(&a, format!("ddl_refused:{}:index:mine", orders.0), vec![9]);
    a.record_refusal_hint(&RefusalHint::new(orders, HintKind::Index, "mine", stamp, hash)).unwrap();
    let names: Vec<_> = a.refusal_hints(orders).unwrap().into_iter().map(|h| h.name).collect();
    assert_eq!(names, vec!["mine".to_string(), "other".to_string(), "ttl".to_string()]);
}

/// Every dropped-index row of a collection is judged against the cut, not the first
/// one the range reads.
#[test]
fn every_dropped_index_row_is_judged_against_the_cut() {
    for order in [["p", "q"], ["q", "p"]] {
        let (a, b, orders, _items, _dirs) = converged();
        for name in ["p", "q"] {
            a.create_index("shop", "orders", fields(name), false, Some(name.into())).unwrap();
        }
        sync(&a, &b);
        later();
        a.drop_index("shop", "orders", order[0]).unwrap();
        later();
        a.drop_index("shop", "orders", order[1]).unwrap();
        sync(&a, &b);
        let stamps: Vec<Stamp> = {
            use redb::ReadableDatabase;
            let txn = a.db().begin_read().unwrap();
            let table = txn.open_table(crate::tables::INDEXES_DROPPED).unwrap();
            table
                .range((orders.0, 0)..=(orders.0, u32::MAX))
                .unwrap()
                .map(|row| crate::codec::decode_oplog_key(row.unwrap().1.value()).unwrap())
                .collect()
        };
        assert_eq!(stamps.len(), 2, "premise");
        // The cut covers the earlier drop and not the later one.
        let earliest = stamps.iter().map(|stamp| stamp.hlc).min().unwrap();
        let mut cut = VersionVector::new();
        cut.insert(a.node_id(), earliest);
        let verdicts = exchange_at(&a, &b, cut, &[]);
        assert_eq!(
            deferred(&verdicts, orders),
            Some(DefinitionDefer::DefinitionAboveCut),
            "order {order:?}: {verdicts:?}"
        );
    }
}

/// A stamp is judged against the cut at its own origin. A cut that is far ahead
/// for one origin and has seen nothing of another does not cover the other's
/// stamp.
#[test]
fn a_stamp_is_judged_against_the_cut_at_its_own_origin() {
    let (a, b, orders, _items, _dirs) = converged();
    b.create_index("shop", "orders", fields("by_b"), false, Some("by_b".into())).unwrap();
    sync(&a, &b);
    let mut cut = VersionVector::new();
    cut.insert(a.node_id(), Hlc::new(crate::physical_now_ms() + 60_000, 0));
    let verdicts = exchange_at(&a, &b, cut.clone(), &[]);
    assert_eq!(
        deferred(&verdicts, orders),
        Some(DefinitionDefer::DefinitionAboveCut),
        "B's stamp is above the cut at B: {verdicts:?}"
    );
    // And covered once the cut has B's own component.
    cut.insert(b.node_id(), Hlc::new(crate::physical_now_ms() + 60_000, 0));
    let verdicts = exchange_at(&a, &b, cut, &[]);
    assert_eq!(compared(&verdicts, orders), Some(true), "{verdicts:?}");
}

/// A collection the responder cannot compare carries no detail: nothing to
/// explain, and nothing to spend the frame on.
#[test]
fn a_deferred_collection_carries_no_detail() {
    let (a, b, orders, _dirs) = converged_with_index("ttl");
    let hash = hash_of(&a, "orders", "ttl");
    let stamp = a.get_collection("shop", "orders").unwrap().indexes[0].created.unwrap();
    b.record_refusal_hint(&RefusalHint::new(orders, HintKind::Index, "ttl", stamp, hash)).unwrap();
    let future = Stamp::new(Hlc::new(crate::physical_now_ms() + 60_000, 0), b.node_id());
    b.record_collection_drop(orders, future).unwrap();
    let ask = DefinitionAsk::new(cut_of(&a, &b), [orders]);
    let reply = b.definition_answer(&ask).unwrap();
    let entry = reply.entries.iter().find(|e| e.collection == orders).unwrap();
    assert_eq!(entry.deferred, Some(DefinitionDefer::Incarnation));
    assert!(entry.definitions.is_empty() && entry.refused.is_empty(), "{entry:?}");
}

// -- The cost ------------------------------------------------------------------------

/// The figure the ADR records: what one answer and one comparison cost over
/// 1,000 collections of three indexes each (one with a partial filter), in a
/// release build. Run by hand: `cargo test -p kimmy-storage --release --lib
/// the_cost_of_a_definition_probe -- --ignored --nocapture`.
#[test]
#[ignore = "a measurement, not a test"]
fn the_cost_of_a_definition_probe_over_a_thousand_collections() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    for i in 0..1_000 {
        let name = format!("c{i}");
        a.create_collection("shop", &name).unwrap();
        a.create_index("shop", &name, fields("a"), false, Some("a_1".into())).unwrap();
        a.create_index("shop", &name, fields("b"), true, Some("b_1".into())).unwrap();
        a.create_index_with(
            "shop",
            &name,
            fields("n"),
            false,
            kimmy_core::Enforcement::Local,
            Some("filtered".into()),
            None,
            Some(doc! { "n": { "$gte": 5_i64 }, "k": { "a": 1, "b": "x" } }),
        )
        .unwrap();
    }
    // A thousand collections are four thousand entries: more than one batch.
    for _ in 0..8 {
        sync(&a, &b);
    }
    let ask = DefinitionAsk::new(cut_of(&a, &b), []);
    let started = std::time::Instant::now();
    let reply = b.definition_answer(&ask).unwrap();
    let answered = started.elapsed();
    let started = std::time::Instant::now();
    let verdicts = a.definition_compare(&ask, &reply).unwrap();
    let compared = started.elapsed();
    assert_eq!(verdicts.len(), 1_000);
    eprintln!("1,000 collections x 3 indexes: answer {answered:?}, compare {compared:?}");
}
