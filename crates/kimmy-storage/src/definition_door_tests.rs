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
    /// The deadline both stages were made with.
    deadline: Hlc,
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
    Swap { m, _dm: dm, bind, deadline, _author: (author, dir) }
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

#[test]
fn a_flip_between_the_validators_read_and_its_write_ends_on_the_new_physical() {
    // The validator door reads the definition (the identity, before the flip), the binding takes effect, and the door takes the
    // writer: the retry resolves the name again and the validator lands on the stage, never on the buried identity.
    let s = before_the_flip();
    let (m, bind) = (Arc::clone(&s.m), s.bind.clone());
    let (result, ()) = crate::sync::race_hooks::race(
        crate::sync::race_hooks::Race::Validator,
        move || {
            window(&m, std::slice::from_ref(&bind));
        },
        || s.m.set_validator(DB, "orders", Some(validator())),
    );
    result.expect("the door was retried and found the name where it answers now");
    assert_eq!(s.m.resolve(DB, "orders").unwrap().physical, BOUND, "premise: the flip took effect");
    assert!(
        s.m.get_collection(DB, BOUND).unwrap().validator.is_some(),
        "the validator is on the bound stage"
    );
}

// -- The replicated arms (Q6.5-1, rule 2) -------------------------------------------------------------------------------------------------
//
// A definition entry minted by a member that made it to a swapped name is addressed to the logical name. The member that receives it
// resolves the name as the door does, so the entry lands on the physical the name answers from here, and does not stop the batch as an
// entry for a collection this member has no record of. (What each member ends with when the entry arrives BEFORE the binding is the
// join's, Q6.5-2; the tombstones read under the target's id are rule 4's, Q6.5-4. These tests are about members that have flipped.)

/// Two members that both hold the swap (the same two stages, the same binding, each its own `orders` made independently), the binding
/// of `BOUND` visible on both.
struct Pair {
    p: Arc<Engine>,
    q: Arc<Engine>,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
    _author: (Engine, tempfile::TempDir),
}

fn two_flipped_members() -> Pair {
    flipped_pair(true)
}

/// Two members that have made the same binding visible. `rows`: whether the identity each held held a document. A buried identity's rows
/// are the drop purger's to remove, and a creation of the same name is refused while any remain (`create_collection_marked`), so a member
/// whose identity held none takes such a creation at once.
fn flipped_pair(rows: bool) -> Pair {
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
    let wall =
        creations.iter().map(|e| e.stamp.hlc.wall_ms).max().unwrap().max(physical_now_ms()) + 1;
    let bind = bind_of(&author, wall, BOUND);
    let member = || {
        let (m, d) = engine();
        let orders = m.create_collection(DB, "orders").unwrap();
        if rows {
            m.insert(&orders, doc! { "_id": 1, "v": 1, "t": 1 }).unwrap();
        }
        m.apply_batch(&creations).unwrap();
        window(&m, std::slice::from_ref(&bind));
        assert_eq!(
            m.resolve(DB, "orders").unwrap().physical,
            BOUND,
            "premise: the flip is visible"
        );
        (m, d)
    };
    let (p, dp) = member();
    let (q, dq) = member();
    Pair { p, q, _dirs: (dp, dq), _author: (author, dir) }
}

#[test]
fn a_replicated_index_creation_addressed_to_a_swapped_name_lands_on_the_bound_physical() {
    let pair = two_flipped_members();
    let made = pair
        .p
        .create_index(DB, "orders", vec![IndexField::ascending("v")], false, None)
        .expect("premise: the door of the member that minted it finds the name");
    let entries = entries_of(&pair.p, OpKind::CreateIndex);
    assert_eq!(entries.len(), 1, "premise: the one entry the door minted");
    let outcome = window(&pair.q, &entries);
    assert!(
        pair.q.get_collection(DB, BOUND).unwrap().index(&made.name).is_some(),
        "the entry landed on the bound physical of the member that received it ({outcome:?})"
    );
    assert!(
        pair.q.get_collection(DB, UNBOUND).unwrap().index(&made.name).is_none(),
        "and not on the unbound stage"
    );
}

#[test]
fn a_replicated_index_drop_addressed_to_a_swapped_name_removes_it_from_the_bound_physical() {
    // The receiving member's index is made through its own local door (fixed in the commit before), BEFORE the other member makes
    // and drops its own: so the drop is newer than the index it names and applies, and only the replicated drop arm is under test.
    let pair = two_flipped_members();
    let held = pair
        .q
        .create_index(DB, "orders", vec![IndexField::ascending("v")], false, None)
        .expect("premise: the receiving member's own door finds the name");
    assert!(
        pair.q.get_collection(DB, BOUND).unwrap().index(&held.name).is_some(),
        "premise: the index is on the bound physical, so the drop has something to remove"
    );
    std::thread::sleep(std::time::Duration::from_millis(5));
    pair.p
        .create_index(DB, "orders", vec![IndexField::ascending("v")], false, None)
        .expect("premise: the door of the member that drops it finds the name");
    pair.p.drop_index(DB, "orders", &held.name).expect("premise: the drop door finds it too");
    let dropped = entries_of(&pair.p, OpKind::DropIndex);
    assert_eq!(dropped.len(), 1, "premise: the one drop entry");
    assert!(
        dropped[0].stamp.hlc > held.created.expect("a stamped index").hlc,
        "premise: the drop is newer than the index it names"
    );
    window(&pair.q, &dropped);
    let on = pair.q.get_collection(DB, BOUND).unwrap();
    assert!(
        on.index(&held.name).is_none(),
        "the drop applied to the bound physical: {:?}",
        on.indexes
    );
    assert!(
        pair.q
            .index_dropped_at(on.id, kimmy_core::IndexMeta::derive_id(&held.name))
            .unwrap()
            .is_some(),
        "and left its tombstone under the target's id"
    );
}

#[test]
fn a_replicated_validator_addressed_to_a_swapped_name_lands_on_the_bound_physical() {
    let pair = two_flipped_members();
    pair.p
        .set_validator(DB, "orders", Some(validator()))
        .expect("premise: the door finds the name");
    let entries = entries_of(&pair.p, OpKind::ConfigureValidator);
    assert_eq!(entries.len(), 1, "premise: the one entry the door minted");
    window(&pair.q, &entries);
    assert!(
        pair.q.get_collection(DB, BOUND).unwrap().validator.is_some(),
        "the bound physical has it"
    );
    assert!(
        pair.q.get_collection(DB, UNBOUND).unwrap().validator.is_none(),
        "the unbound one does not"
    );
}

#[test]
fn a_replicated_index_creation_addressed_to_a_stage_lands_on_that_stage() {
    // The writer's own copy of a definition into the stage it is filling (Q6.7) names the stage's physical: a name with no register
    // event of its own, found as a physical is. Built by hand here, as nothing mints it yet.
    let pair = two_flipped_members();
    let index = kimmy_core::IndexMeta {
        id: kimmy_core::IndexMeta::derive_id("v_1"),
        name: "v_1".into(),
        fields: vec![IndexField::ascending("v")],
        unique: false,
        enforcement: Enforcement::Local,
        multikey: false,
        expire_after_secs: None,
        partial_filter: None,
        created: None,
    };
    let stamp = Stamp::new(Hlc::new(physical_now_ms() + 10, 0), pair.p.node_id(), pair.p.epoch());
    let entry = crate::engine::ddl_entry(
        stamp,
        OpKind::CreateIndex,
        CollectionId::derive(DB, UNBOUND),
        &kimmy_core::IndexCreate { db: DB.into(), collection: UNBOUND.into(), index },
    )
    .unwrap();
    window(&pair.q, std::slice::from_ref(&entry));
    assert!(
        pair.q.get_collection(DB, UNBOUND).unwrap().index("v_1").is_some(),
        "the definition addressed to the stage landed on the stage"
    );
    assert!(pair.q.get_collection(DB, BOUND).unwrap().index("v_1").is_none(), "and on no other");
}

#[test]
fn a_replicated_index_drop_older_than_the_index_it_names_is_declined_on_a_swapped_name() {
    // The receiving member made its own index AFTER the drop that arrives was minted: the drop belongs to the index of an earlier life,
    // and does not remove the one standing (ADR-132). The arm reads the standing index on the physical the name answers from here.
    let pair = two_flipped_members();
    pair.p
        .create_index(DB, "orders", vec![IndexField::ascending("v")], false, None)
        .expect("premise: the door of the member that drops it finds the name");
    pair.p.drop_index(DB, "orders", "v_1").expect("premise: the drop door finds it too");
    let dropped = entries_of(&pair.p, OpKind::DropIndex);
    assert_eq!(dropped.len(), 1, "premise: the one drop entry");
    std::thread::sleep(std::time::Duration::from_millis(5));
    let held = pair
        .q
        .create_index(DB, "orders", vec![IndexField::ascending("v")], false, None)
        .expect("premise: the receiving member's own door finds the name");
    assert_eq!(held.name, "v_1", "premise: the same index name, so the same derived id");
    assert!(
        held.created.expect("a stamped index").hlc > dropped[0].stamp.hlc,
        "premise: the index was made after the drop was minted"
    );
    // The arm declines on its own read of the standing index, before the writer, and the transaction judges again under the writer
    // (`drop_index_inner`), so the index standing proves nothing about which guard stopped the drop: the writer's would stop it too. The
    // race hook at the point between them tells them apart: it is reached only by a drop the arm let through, and the writer's decline
    // is then recorded as absorbed.
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::ReplicatedIndexDrop, || {});
    let outcome = window(&pair.q, &dropped);
    assert!(
        pair.q.get_collection(DB, BOUND).unwrap().index("v_1").is_some(),
        "the drop is older than the index it names: declined, the index stands ({outcome:?})"
    );
    assert!(
        !crate::sync::race_hooks::was_absorbed(crate::sync::race_hooks::Race::ReplicatedIndexDrop),
        "the drop was declined by the arm's own read of the index on the physical the name answers from, and not left to the writer's \
         second judgement ({outcome:?})"
    );
    // the hook, armed for the point the arm never reached, is spent here so that it cannot leak into a later test on this thread
    crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::ReplicatedIndexDrop);
}

#[test]
fn a_validator_carried_by_a_creation_names_the_collection_that_creation_made_not_the_swapped_name()
{
    // A creation of `orders` by a member that never heard of the swap carries a validator and arrives after the flip. The validator is
    // the new collection's own (a creation names the physical it made), never a definition addressed to the logical name: it must not
    // land on the stage the name answers from. (What becomes of the creation itself is Q6.5's later units.)
    // The identity holds no rows: a buried identity's rows are the purger's to remove, and a creation of its name waits for them
    // (`CollectionPurging`, the batch stops), so with a row the first delivery below would make nothing.
    let pair = flipped_pair(false);
    let (x, _dx) = engine();
    std::thread::sleep(std::time::Duration::from_millis(20));
    x.create_collection_with_validator(DB, "orders", validator()).expect(
        "premise: the member that never heard of the swap creates the name with a validator",
    );
    let creation: Vec<OplogEntry> = entries_of(&x, OpKind::CreateCollection)
        .into_iter()
        .filter(|e| e.collection == CollectionId::derive(DB, "orders"))
        .collect();
    assert_eq!(creation.len(), 1, "premise: its one creation entry");
    // The first delivery finds no collection of that name (the identity is buried) and makes one, validator and all, in one transaction.
    // That never takes the path under test. The creation met again finds the name held, and applies the validator it carries as a change
    // to the collection that creation made, by name: it must not land on the stage the name answers from.
    window(&pair.q, &creation);
    let made = pair
        .q
        .get_collection(DB, "orders")
        .expect("premise: the first delivery made the creation's collection, which the name does not answer from");
    assert!(made.validator.is_some(), "premise: it holds the validator the creation carried");
    assert_eq!(
        pair.q.resolve(DB, "orders").expect("premise: the name still resolves").physical,
        BOUND,
        "premise: the name still answers from the stage"
    );
    window(&pair.q, &creation);
    assert!(
        pair.q.get_collection(DB, BOUND).expect("the stage is held").validator.is_none(),
        "the creation's validator did not land on the stage the name answers from"
    );
}

// -- The vector door and the swap (Q6.5-1; ADR-272, decision 12, ruled 2026-11-23) -------------------------------------------------------
//
// A name with a register event of its own, in effect or only known ahead of its position, keeps no vector configuration in this release:
// both doors refuse it with a 400. A name that only has a stage held, inside its deadline on this member's clock, is mid-`$out`: both
// doors refuse it with a retryable 503. A held stage past its deadline is an orphan and must not block the name for ever. The register
// event decides before the held stage is asked.

fn a_vector_config() -> kimmy_core::VectorConfig {
    kimmy_core::VectorConfig {
        fields: vec!["body".into()],
        provider: kimmy_core::ProviderConfig::Byo {},
        dim: 8,
        metric: Default::default(),
        document_prefix: None,
        query_prefix: None,
        chunk: Default::default(),
    }
}

fn the_400(verb: &str) -> String {
    format!(
        "vectors cannot be {verb} `app`.`orders`: a replacing `$out` has swapped this name, and a swapped name keeps no vector \
         configuration in this release (ADR-272, decision 12)"
    )
}

fn refused_400(result: crate::Result<impl std::fmt::Debug>, verb: &str, door: &str) {
    // The sentence is the whole of the refusal (the API gives it a 400 of its own, without a prefix; that is pinned where the class
    // is, in kimmy-api's error tests): it is neither the 503 of a swap in flight nor a failure of anything else.
    match result {
        Err(error) => {
            assert_eq!(error.to_string(), the_400(verb), "{door}: the sentence");
            assert!(
                !error.to_string().contains("in progress"),
                "{door}: a register event is not the retryable refusal"
            );
        }
        other => panic!("{door}: expected the 400 refusal of a swapped name, got {other:?}"),
    }
}

#[test]
fn both_vector_doors_refuse_a_name_with_a_register_event_in_effect_with_the_400() {
    // `flipped()` also holds an unbound stage inside its deadline: the register event decides before the held stage is asked, so the
    // answer is the 400 and never the 503 (the precedence).
    let s = flipped();
    refused_400(
        s.m.configure_vectors(DB, "orders", a_vector_config()),
        "configured on",
        "configure",
    );
    refused_400(s.m.disable_vectors(DB, "orders", false), "removed from", "disable");
}

#[test]
fn both_vector_doors_refuse_a_name_whose_only_register_event_is_known_ahead_of_its_position_with_the_400()
 {
    // The binding is known and not in effect (no window behind it): `is_swapped` counts it, on purpose, and a stage is held inside its
    // deadline as well, so this is the precedence again.
    let s = before_the_flip();
    s.m.apply_batch(std::slice::from_ref(&s.bind)).unwrap();
    assert_eq!(
        s.m.resolve(DB, "orders").unwrap().physical,
        "orders",
        "premise: the binding is not in effect, the name still answers from the identity"
    );
    refused_400(
        s.m.configure_vectors(DB, "orders", a_vector_config()),
        "configured on",
        "configure",
    );
    refused_400(s.m.disable_vectors(DB, "orders", false), "removed from", "disable");
}

fn the_503_text(deadline_ms: u64) -> String {
    let at = bson::DateTime::from_millis(deadline_ms as i64).try_to_rfc3339_string().unwrap();
    format!(
        "`app`.`orders` has a replacing `$out` in progress (its staging collection is held here until {at}); its vector configuration \
         cannot change until the swap completes or is abandoned: retry after it"
    )
}

#[test]
fn both_vector_doors_answer_the_retryable_503_for_a_stage_held_inside_its_deadline() {
    let s = before_the_flip();
    assert!(s.m.get_collection(DB, BOUND).is_ok(), "premise: a stage of the name is held");
    let expected = the_503_text(s.deadline.wall_ms);
    for (door, result) in [
        ("configure", s.m.configure_vectors(DB, "orders", a_vector_config()).map(|_| ())),
        ("disable", s.m.disable_vectors(DB, "orders", false).map(|_| ())),
    ] {
        let error = result.expect_err("a stage held inside its deadline refuses");
        assert!(
            !matches!(error, crate::StorageError::Core(_)),
            "{door}: the retryable refusal is not a 400: {error:?}"
        );
        assert_eq!(error.to_string(), expected, "{door}: the sentence");
    }
}

#[test]
fn a_stage_held_past_its_deadline_does_not_block_the_vector_doors() {
    // An orphan: its writer vanished, and it stays held until a later event of the name removes it. A deadline before its own creation
    // reads as the creation's, which is past on this member's clock a moment later.
    let (m, _dm) = engine();
    let orders = m.create_collection(DB, "orders").unwrap();
    m.insert(&orders, doc! { "_id": 1, "body": "text" }).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let author = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    author.create_stage(DB, BOUND, MergePolicy::Document, Hlc::new(1, 0)).unwrap();
    let creation: Vec<OplogEntry> = everything(&author)
        .into_iter()
        .filter(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, BOUND)
        })
        .collect();
    m.apply_batch(&creation).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(m.get_collection(DB, BOUND).is_ok(), "premise: the stage is held");
    assert!(
        creation[0].stamp.hlc.wall_ms < physical_now_ms(),
        "premise: its effective deadline (its creation, the deadline being before it) is below this member's clock"
    );
    m.configure_vectors(DB, "orders", a_vector_config()).expect("an orphaned stage blocks nothing");
    m.disable_vectors(DB, "orders", false).expect("nor the disable");
}

#[test]
fn a_plain_name_configures_and_disables_vectors_as_it_always_did() {
    let (m, _d) = engine();
    let orders = m.create_collection(DB, "orders").unwrap();
    m.insert(&orders, doc! { "_id": 1, "body": "text" }).unwrap();
    m.configure_vectors(DB, "orders", a_vector_config()).expect("the control");
    assert!(m.disable_vectors(DB, "orders", false).expect("the control"), "it was on");
}
