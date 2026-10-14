//! The definition digest's probe (B3 half 2, design 3.3 to 3.5): what is
//! compared, what is deferred and why, the pinned transaction, and the
//! explanation of a refusal. One test per clause, each of which fails when its
//! own clause is mutated and not a neighbour's.

use bson::doc;
use kimmy_core::{CollectionId, Hlc, IndexField, NodeId, Stamp, VersionVector};
use serde_bytes::ByteBuf;

use crate::definition_digest::{VECTOR_NAME, definition_list, index_hash};
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
            assert_eq!(differing, vec!["by_a".to_string()]);
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
            assert_eq!(differing, vec!["ttl".to_string()]);
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
            assert_eq!(differing, vec![VECTOR_NAME.to_string()]);
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
    // The cut covers a drop stamped before everything either wrote.
    let verdicts = exchange(&a, &b, &[]);
    assert_eq!(compared(&verdicts, orders), Some(true), "{verdicts:?}");
    // One above the cut: not covered.
    let future = Stamp::new(Hlc::new(crate::physical_now_ms() + 60_000, 0), a.node_id());
    a.record_collection_drop(orders, future).unwrap();
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
        indexes: Vec::new(),
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
        Some(DefinitionOutcome::Explained { differing: vec!["ttl".into()] }),
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
        Some(DefinitionOutcome::Explained { differing: vec!["ttl".into()] }),
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
            assert_eq!(differing, vec!["other".to_string(), "ttl".to_string()]);
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
        reply_not_asked.entries.iter().all(|e| e.refused.is_empty() && e.indexes.is_empty()),
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
    a.record_refusal_hint(&RefusalHint::new(orders, HintKind::Drop, "held", stamp, hash)).unwrap();
    let verdicts = exchange(&a, &b, &[orders]);
    assert_eq!(
        outcome_of(&verdicts, orders),
        Some(DefinitionOutcome::Explained { differing: vec!["held".into()] }),
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
    let carried: Vec<(String, u128)> = entry
        .indexes
        .iter()
        .map(|(name, hash)| {
            (name.clone(), u128::from_be_bytes(hash.as_slice().try_into().expect("16 bytes")))
        })
        .collect();
    assert_eq!(carried, list);
}
