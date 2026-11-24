//! A schema change and its reads through the API, on a name that has been swapped (ADR-272, decision 6; Q6.5-1). The engine's doors
//! resolve the logical name through the register (the storage tests are in `kimmy-storage`'s `definition_door_tests`); these are the
//! API callers that used to ask for a physical by the client's name: the answer after an index is built, the listing of indexes,
//! the removal of a validator, and the TTL sweep, which expires the documents of the physical the name answers from and of no stage that
//! is not one.

use std::collections::BTreeSet;

use axum::Json;
use axum::extract::{Path, State};
use kimmy_core::{CollectionId, Hlc, NodeId, OpKind, OplogEntry, Stamp};

use super::resolve_memo_tests::{flip, state_with_orders};
use super::*;

fn root() -> Auth {
    Auth(kimmy_auth::Principal::superuser("test"))
}

fn field(path: &str) -> IndexFieldSpec {
    IndexFieldSpec { path: path.into(), descending: false }
}

#[test]
fn an_index_created_through_the_api_on_a_swapped_name_is_built_on_the_bound_physical() {
    // The two physicals hold different documents on purpose. The identity, which the flip buries, holds five documents that share `k`,
    // which a unique index cannot be built over; the bound stage holds three with distinct `k`, which it can. The create succeeds only if
    // the build, and the answer's read of what it built, went to the physical the name answers from now. A buried physical cannot be
    // read back by name, so "the identity gained nothing" is shown by that success, and by the entry being addressed to the logical name.
    let (state, _dir) = state_with_orders();
    let identity = state
        .engine
        .get_collection("app", "orders")
        .expect("premise: the identity is held before the flip");
    for id in 2..=5 {
        state
            .engine
            .insert(&identity, bson::doc! { "_id": id, "k": 1 })
            .expect("premise: the identity takes a document that shares `k`");
    }
    let stage = flip(&state);
    let bound = state
        .engine
        .get_collection("app", &stage)
        .expect("premise: the bound stage is held after the flip");
    for id in 1..=3 {
        state
            .engine
            .insert(&bound, bson::doc! { "_id": id, "k": id })
            .expect("premise: the bound stage takes a document with its own `k`");
    }
    assert!(
        bound.indexes.is_empty(),
        "premise: nothing stands on the bound stage yet: {:?}",
        bound.indexes
    );
    assert!(
        state.engine.get_collection("app", "orders").is_err(),
        "premise: the identity is buried, so nothing can read it back by its name"
    );
    let spec = IndexSpec { fields: vec![field("k")], unique: true, ..Default::default() };
    let answer = create_index(&state, &root(), "app", "orders", spec).expect(
        "the unique index is built over the three distinct documents the name answers from; the identity's five would refuse it",
    );
    assert_eq!(answer["unique"], serde_json::json!(true), "{answer}");
    assert_eq!(
        answer["unkeyed"],
        serde_json::json!(0),
        "the answer reads the index it built: {answer}"
    );
    let on_stage = state
        .engine
        .get_collection("app", &stage)
        .expect("the bound stage is still held after the create");
    assert_eq!(on_stage.indexes.len(), 1, "the index stands on the physical the name answers from");
    let minted = state
        .engine
        .entries_for_peer(Hlc::ZERO, 10_000, kimmy_storage::WalkScope::Background)
        .expect("the oplog is readable")
        .entries
        .into_iter()
        .filter(|e| e.kind == OpKind::CreateIndex)
        .collect::<Vec<_>>();
    assert_eq!(minted.len(), 1, "the one entry the door minted");
    assert_eq!(
        minted[0].collection,
        CollectionId::derive("app", "orders"),
        "the entry is addressed to the logical name, not to the stage"
    );
}

#[test]
fn the_indexes_listed_through_the_api_under_a_swapped_name_are_the_bound_physicals() {
    let (state, _dir) = state_with_orders();
    flip(&state);
    state
        .engine
        .create_index("app", "orders", vec![kimmy_storage::IndexField::ascending("v")], false, None)
        .expect("premise: the engine's door puts an index on the bound physical");
    let listed = list_indexes(&state, &root(), "app", "orders")
        .expect("the listing resolves the name through the register");
    assert_eq!(listed["indexes"].as_array().map(Vec::len), Some(1), "{listed}");
}

#[tokio::test(flavor = "current_thread")]
async fn removing_the_validator_of_a_swapped_name_reports_that_it_removed_one() {
    let (state, _dir) = state_with_orders();
    flip(&state);
    let spec = kimmy_core::ValidatorSpec {
        schema: bson::doc! { "bsonType": "object" },
        action: Default::default(),
        level: Default::default(),
    };
    state
        .engine
        .set_validator("app", "orders", Some(spec))
        .expect("premise: the engine's door puts a validator on the bound physical");
    let Json(answer) = crate::validator::remove_validator(
        State(state.clone()),
        root(),
        Path(("app".to_string(), "orders".to_string())),
    )
    .await
    .expect("the removal finds the name through the register");
    assert_eq!(answer["removed"], serde_json::json!(true), "it had one: {answer}");
}

/// The creation of a TTL index on `stage`, as a peer's entry would carry it (nothing mints one for a stage until the executor copies the
/// target's definitions, Q6.7): addressed to the stage by its own name.
fn a_ttl_index_entry_for(stage: &str, by: &kimmy_storage::Engine) -> OplogEntry {
    let index = kimmy_storage::IndexMeta {
        id: kimmy_storage::IndexMeta::derive_id("ttl_t"),
        name: "ttl_t".into(),
        fields: vec![kimmy_storage::IndexField::ascending("t")],
        unique: false,
        enforcement: kimmy_storage::Enforcement::Local,
        multikey: false,
        expire_after_secs: Some(60),
        partial_filter: None,
        created: None,
    };
    OplogEntry {
        stamp: Stamp::new(
            Hlc::new(kimmy_storage::physical_now_ms() + 10, 0),
            by.node_id(),
            by.epoch(),
        ),
        kind: OpKind::CreateIndex,
        collection: CollectionId::derive("app", stage),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&kimmy_core::IndexCreate {
                db: "app".into(),
                collection: stage.into(),
                index,
            })
            .expect("the creation body encodes"),
        ),
        meta: None,
    }
}

#[test]
fn the_ttl_sweep_expires_the_bound_physical_and_a_plain_collection_and_leaves_an_unbound_stage() {
    let (state, _dir) = state_with_orders();
    let bound = flip(&state);
    let engine = &state.engine;
    let old = bson::DateTime::from_millis(0);
    // the bound physical: a TTL index through the engine's door (it lands where the name answers) and an expired document
    engine
        .create_index_with(
            "app",
            "orders",
            vec![kimmy_storage::IndexField::ascending("t")],
            false,
            Default::default(),
            Some("ttl_t".into()),
            Some(60),
            None,
        )
        .expect("premise: the TTL index lands on the bound physical");
    let bound_meta =
        engine.get_collection("app", &bound).expect("premise: the bound stage is held");
    engine
        .insert(&bound_meta, bson::doc! { "_id": 1, "t": old })
        .expect("premise: the bound stage takes an expired document");
    // an unbound stage of the same name, with a TTL index delivered as a peer's entry would and an expired document
    let other = "orders.__stg.other";
    engine
        .create_stage(
            "app",
            other,
            kimmy_core::MergePolicy::Document,
            Hlc::new(kimmy_storage::physical_now_ms() + 60_000, 0),
        )
        .expect("premise: a second stage is created and left unbound");
    engine
        .apply_batch(&[a_ttl_index_entry_for(other, engine)])
        .expect("premise: the peer's TTL index entry applies to the unbound stage");
    let other_meta =
        engine.get_collection("app", other).expect("premise: the unbound stage is held");
    assert!(other_meta.index("ttl_t").is_some(), "premise: the unbound stage holds a TTL index");
    engine
        .insert(&other_meta, bson::doc! { "_id": 1, "t": old })
        .expect("premise: the unbound stage takes an expired document");
    // a collection that was never swapped, the control that shows the sweep ran at all
    state
        .engine
        .create_collection("app", "sessions")
        .expect("premise: the control collection is created");
    engine
        .create_index_with(
            "app",
            "sessions",
            vec![kimmy_storage::IndexField::ascending("t")],
            false,
            Default::default(),
            Some("ttl_t".into()),
            Some(60),
            None,
        )
        .expect("premise: the control collection gets a TTL index");
    let sessions = engine.get_collection("app", "sessions").expect("premise: the control is held");
    engine
        .insert(&sessions, bson::doc! { "_id": 1, "t": old })
        .expect("premise: the control takes an expired document");

    let me = NodeId::from_bytes([1; 16]);
    crate::expiry::pass(engine, me, &BTreeSet::new(), 100_000_000_000);

    let count = |name: &str| {
        let meta = engine.get_collection("app", name).expect("the physical named is held");
        engine
            .count(&meta, kimmy_storage::WalkScope::Background)
            .expect("the physical's documents are counted")
    };
    assert_eq!(count("sessions"), 0, "the control: a plain collection's expired document went");
    assert_eq!(count(&bound), 0, "the physical the name answers from is swept");
    assert_eq!(count(other), 1, "an unbound stage is left alone");
}
