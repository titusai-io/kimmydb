//! The definition doors address the LOGICAL name (ADR-272, decision 6; Q6.5-1). A schema change (an index, its drop, a TTL index, a
//! validator) made to a name that has been swapped resolves through the register to the physical the name answers from, the way a
//! read does, and mints its entry addressed to the logical name (rule 1). Before this unit each door looked the name up with
//! `get_collection`, which finds a physical by its own name, so after a flip (the identity buried, the stage serving) every door
//! answered `CollectionNotFound`.
//!
//! The fixture holds TWO stages of `orders`: the unbound one is named to sort BEFORE the bound one, so a lookup that takes the first
//! stage of the name finds the wrong one where the other order would hide it.

use std::sync::Arc;

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, Enforcement, Hlc, IndexField, MergePolicy, OpKind, OplogEntry,
    Stamp, ValidatorSpec, VersionVector,
};

use crate::{Engine, PeerWindow, SyncOutcome, WalkScope, physical_now_ms};

const DB: &str = "app";
/// A stage that is never bound: its name sorts before the bound one's.
const UNBOUND: &str = "orders.__stg.a0";
const BOUND: &str = "orders.__stg.a1";

fn engine() -> (Arc<Engine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap()), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn window(b: &Engine, entries: &[OplogEntry]) -> SyncOutcome {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = b.witnessed_vector().unwrap();
    let mut outcome = SyncOutcome::default();
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
    outcome
}

fn bind_of(by: &Engine, wall_ms: u64, physical: &str) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        kind: OpKind::BindCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: physical.into(),
            })
            .unwrap(),
        ),
        meta: None,
    }
}

/// A member holding `orders` (with a document) and the creations of both stages, the binding of `BOUND` not yet known to it.
struct Swap {
    m: Arc<Engine>,
    _dm: tempfile::TempDir,
    bind: OplogEntry,
    _author: (Engine, tempfile::TempDir),
}

fn before_the_flip() -> Swap {
    let (m, dm) = engine();
    let orders = m.create_collection(DB, "orders").unwrap();
    m.insert(&orders, doc! { "_id": 1, "v": 1, "t": 1 }).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let author = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    author.create_stage(DB, UNBOUND, MergePolicy::Document, deadline).unwrap();
    author.create_stage(DB, BOUND, MergePolicy::Document, deadline).unwrap();
    let creations: Vec<OplogEntry> = everything(&author)
        .into_iter()
        .filter(|e| e.kind == OpKind::CreateCollection)
        .filter(|e| {
            e.collection == CollectionId::derive(DB, UNBOUND)
                || e.collection == CollectionId::derive(DB, BOUND)
        })
        .collect();
    assert_eq!(creations.len(), 2, "premise: both stages' creations");
    m.apply_batch(&creations).unwrap();
    let wall =
        creations.iter().map(|e| e.stamp.hlc.wall_ms).max().unwrap().max(physical_now_ms()) + 1;
    let bind = bind_of(&author, wall, BOUND);
    Swap { m, _dm: dm, bind, _author: (author, dir) }
}

/// The same member after the binding of `BOUND` took effect: the identity is buried, the bound stage serves, the other stage is held.
fn flipped() -> Swap {
    let s = before_the_flip();
    window(&s.m, std::slice::from_ref(&s.bind));
    assert_eq!(s.m.resolve(DB, "orders").unwrap().physical, BOUND, "premise: the flip is visible");
    assert!(s.m.get_collection(DB, "orders").is_err(), "premise: the identity is buried");
    assert!(s.m.get_collection(DB, UNBOUND).is_ok(), "premise: the other stage is held, unbound");
    s
}

fn not_found<T: std::fmt::Debug>(result: crate::Result<T>) -> bool {
    matches!(result, Err(crate::StorageError::Core(kimmy_core::Error::CollectionNotFound { .. })))
}

fn validator() -> ValidatorSpec {
    ValidatorSpec {
        schema: doc! { "bsonType": "object" },
        action: Default::default(),
        level: Default::default(),
    }
}

/// The entries of `kind` this member holds, oldest first.
fn entries_of(e: &Engine, kind: OpKind) -> Vec<OplogEntry> {
    everything(e).into_iter().filter(|entry| entry.kind == kind).collect()
}

fn on_the_bound_stage(s: &Swap, index: &str) -> bool {
    s.m.get_collection(DB, BOUND).unwrap().index(index).is_some()
}

fn on_the_unbound_stage(s: &Swap, index: &str) -> bool {
    s.m.get_collection(DB, UNBOUND).unwrap().index(index).is_some()
}

#[test]
fn an_index_made_to_a_swapped_name_lands_on_the_physical_the_name_answers_from() {
    let s = flipped();
    let made =
        s.m.create_index(DB, "orders", vec![IndexField::ascending("v")], false, None)
            .expect("the door finds the name through the register");
    assert!(on_the_bound_stage(&s, &made.name), "the index is on the bound stage");
    assert!(!on_the_unbound_stage(&s, &made.name), "an unbound stage is never a target");
}

#[test]
fn a_ttl_index_made_to_a_swapped_name_lands_on_the_bound_physical() {
    let s = flipped();
    let made =
        s.m.create_index_with(
            DB,
            "orders",
            vec![IndexField::ascending("t")],
            false,
            Enforcement::Local,
            None,
            Some(3600),
            None,
        )
        .expect("the TTL form goes through the same door");
    let on = s.m.get_collection(DB, BOUND).unwrap();
    assert_eq!(on.index(&made.name).and_then(|i| i.expire_after_secs), Some(3600));
    assert!(!on_the_unbound_stage(&s, &made.name));
}

#[test]
fn an_index_dropped_by_the_swapped_name_is_dropped_from_the_bound_physical() {
    let s = flipped();
    let made =
        s.m.create_index(DB, "orders", vec![IndexField::ascending("v")], false, None)
            .expect("the door finds the name");
    assert!(on_the_bound_stage(&s, &made.name), "premise: it is there");
    assert!(
        s.m.drop_index(DB, "orders", &made.name).expect("the drop finds the name too"),
        "the index existed on the physical the name answers from"
    );
    assert!(!on_the_bound_stage(&s, &made.name), "and it is gone from it");
}

#[test]
fn a_validator_set_on_a_swapped_name_lands_on_the_bound_physical() {
    let s = flipped();
    s.m.set_validator(DB, "orders", Some(validator())).expect("the door finds the name");
    assert!(s.m.get_collection(DB, BOUND).unwrap().validator.is_some(), "the bound stage has it");
    assert!(
        s.m.get_collection(DB, UNBOUND).unwrap().validator.is_none(),
        "the unbound one does not"
    );
}

#[test]
fn the_indexes_listed_under_a_swapped_name_are_the_bound_physicals() {
    let s = flipped();
    let made =
        s.m.create_index(DB, "orders", vec![IndexField::ascending("v")], false, None)
            .expect("the door finds the name");
    let listed: Vec<String> =
        s.m.list_indexes(DB, "orders")
            .expect("the read finds it too")
            .into_iter()
            .map(|i| i.name)
            .collect();
    assert!(listed.contains(&made.name), "{listed:?}");
}

#[test]
fn the_entry_a_door_mints_for_a_swapped_name_is_addressed_to_the_logical_name() {
    // Rule 1: both the entry's `collection` id and the name in its body are the logical name's. Either alone passes a half-done rule.
    let s = flipped();
    let logical = CollectionId::derive(DB, "orders");
    let made =
        s.m.create_index(DB, "orders", vec![IndexField::ascending("v")], false, None)
            .expect("the index door finds the name");
    s.m.drop_index(DB, "orders", &made.name).expect("the drop door finds the name");
    s.m.set_validator(DB, "orders", Some(validator())).expect("the validator door finds the name");

    let created = entries_of(&s.m, OpKind::CreateIndex);
    assert_eq!(created.len(), 1, "one creation entry");
    assert_eq!(created[0].collection, logical, "CreateIndex: the entry's collection id");
    let body: kimmy_core::IndexCreate =
        bson::deserialize_from_slice(created[0].body.as_ref().unwrap()).unwrap();
    assert_eq!(body.collection, "orders", "CreateIndex: the name in the body");

    let dropped = entries_of(&s.m, OpKind::DropIndex);
    assert_eq!(dropped.len(), 1, "one drop entry");
    assert_eq!(dropped[0].collection, logical, "DropIndex: the entry's collection id");
    let body: kimmy_core::IndexDrop =
        bson::deserialize_from_slice(dropped[0].body.as_ref().unwrap()).unwrap();
    assert_eq!(body.collection, "orders", "DropIndex: the name in the body");

    let set = entries_of(&s.m, OpKind::ConfigureValidator);
    assert_eq!(set.len(), 1, "one validator entry");
    assert_eq!(set[0].collection, logical, "ConfigureValidator: the entry's collection id");
    let body: kimmy_core::ValidatorSet =
        bson::deserialize_from_slice(set[0].body.as_ref().unwrap()).unwrap();
    assert_eq!(body.collection, "orders", "ConfigureValidator: the name in the body");
}

#[test]
fn a_plain_name_behaves_as_it_always_did() {
    // One control per door: a never-swapped name, the entry addressed to it, the index on its own physical.
    let (m, _d) = engine();
    let orders = m.create_collection(DB, "orders").unwrap();
    m.insert(&orders, doc! { "_id": 1, "v": 1 }).unwrap();
    let made = m.create_index(DB, "orders", vec![IndexField::ascending("v")], false, None).unwrap();
    assert!(m.get_collection(DB, "orders").unwrap().index(&made.name).is_some());
    assert_eq!(m.list_indexes(DB, "orders").unwrap().len(), 1);
    m.set_validator(DB, "orders", Some(validator())).unwrap();
    assert!(m.get_collection(DB, "orders").unwrap().validator.is_some());
    assert!(m.drop_index(DB, "orders", &made.name).unwrap());
    let logical = CollectionId::derive(DB, "orders");
    for kind in [OpKind::CreateIndex, OpKind::DropIndex, OpKind::ConfigureValidator] {
        let entries = entries_of(&m, kind);
        assert_eq!(entries.len(), 1, "{kind:?}");
        assert_eq!(entries[0].collection, logical, "{kind:?}");
    }
}

#[test]
fn a_name_that_reads_absent_answers_not_found_at_every_door_and_mints_nothing() {
    let s = flipped();
    assert!(s.m.drop_collection(DB, "orders").unwrap(), "premise: a logical drop of the name");
    assert!(not_found(s.m.resolve(DB, "orders")), "premise: the name reads absent");
    let before = everything(&s.m).len();
    assert!(not_found(s.m.create_index(
        DB,
        "orders",
        vec![IndexField::ascending("v")],
        false,
        None
    )));
    assert!(not_found(s.m.drop_index(DB, "orders", "v_1")));
    assert!(not_found(s.m.set_validator(DB, "orders", Some(validator()))));
    assert!(not_found(s.m.list_indexes(DB, "orders")));
    assert_eq!(everything(&s.m).len(), before, "no entry was minted");
}

#[test]
fn a_flip_between_the_doors_read_and_its_write_ends_on_the_new_physical() {
    // The door reads the definition (the identity, before the flip), the binding takes effect, and the door takes the writer: the
    // retry must resolve the name again and land on the stage, never on the buried identity.
    let s = before_the_flip();
    let (m, bind) = (Arc::clone(&s.m), s.bind.clone());
    let (result, ()) = crate::sync::race_hooks::race(
        crate::sync::race_hooks::Race::IndexCreation,
        move || {
            window(&m, std::slice::from_ref(&bind));
        },
        || s.m.create_index(DB, "orders", vec![IndexField::ascending("v")], false, None),
    );
    let made = result.expect("the door was retried and found the name where it answers now");
    assert_eq!(s.m.resolve(DB, "orders").unwrap().physical, BOUND, "premise: the flip took effect");
    assert!(
        on_the_bound_stage(&s, &made.name),
        "the index is on the physical the name answers from"
    );
}
