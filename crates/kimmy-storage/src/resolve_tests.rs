//! `Engine::resolve`: a logical name resolved through the register (ADR-272, decisions 5 and 7; Q6.4 item 0). With no event in effect
//! it is exactly `get_collection` (every existing path is the identity row); with a binding in effect the name resolves to the bound
//! physical; a binding whose physical is not alive here is skipped for the next older visible event; a logical drop in effect makes
//! the name absent.

use kimmy_core::{
    CollectionId, Error as CoreError, Hlc, MergePolicy, RegisterEvent, StageInfo, StageMeta, Stamp,
};

use crate::{Engine, StorageError, WriterHolder, physical_now_ms};

const DB: &str = "app";
const STAGE: &str = "orders.__stg.a1";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn stamp_after(e: &Engine, wall_ms: u64) -> Stamp {
    Stamp::new(Hlc::new(wall_ms, 0), e.node_id(), e.epoch())
}

/// Put `event` in effect on `name`, as the Effect arm does: through the one door, in a transaction of its own.
fn in_effect(e: &Engine, name: &str, stamp: Stamp, event: RegisterEvent) {
    let txn = e.begin_write(WriterHolder::Ddl).unwrap();
    let put = crate::register::put(&txn, DB, name, stamp, event).unwrap();
    assert_eq!(
        put,
        kimmy_core::register_state::Put::Stored,
        "premise: the event is valid and stored"
    );
    txn.commit().unwrap();
}

/// A stage made here, its creation stamp, and the stamp a binding of it may carry.
fn a_stage(e: &Engine) -> Stamp {
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    let meta = e.create_stage(DB, STAGE, MergePolicy::Document, deadline).unwrap();
    stamp_after(e, meta.created.wall_ms + 1)
}

#[test]
fn a_name_with_no_event_in_effect_resolves_to_the_collection_of_that_name() {
    let (e, _d) = engine();
    let orders = e.create_collection(DB, "orders").unwrap();
    let resolved = e.resolve(DB, "orders").unwrap();
    assert_eq!(resolved.meta.id, orders.id);
    assert_eq!((resolved.physical.as_str(), resolved.visible), ("orders", None));
}

#[test]
fn a_name_that_was_never_made_is_absent_as_get_collection_says() {
    let (e, _d) = engine();
    match (e.resolve(DB, "ghost"), e.get_collection(DB, "ghost")) {
        (
            Err(StorageError::Core(CoreError::CollectionNotFound { collection: a, .. })),
            Err(StorageError::Core(CoreError::CollectionNotFound { collection: b, .. })),
        ) => assert_eq!(a, b),
        other => panic!("both answer absent: {other:?}"),
    }
}

#[test]
fn a_binding_in_effect_makes_the_bound_physical_what_the_name_resolves_to() {
    let (e, _d) = engine();
    e.create_collection(DB, "orders").unwrap();
    let bind = a_stage(&e);
    in_effect(&e, "orders", bind, RegisterEvent::Bind { physical: STAGE.into() });
    let resolved = e.resolve(DB, "orders").unwrap();
    assert_eq!(resolved.physical, STAGE, "the bound physical");
    assert_eq!(resolved.meta.name, STAGE);
    assert_eq!(resolved.visible, Some(bind), "made visible by the binding's stamp");
}

#[test]
fn a_binding_whose_physical_is_not_alive_here_is_skipped_for_the_next_older_visible_event() {
    let (e, _d) = engine();
    let orders = e.create_collection(DB, "orders").unwrap();
    // The register knows the stage (its creation row outlives the collection) but no collection stands under it here.
    let ghost = "orders.__stg.ghost";
    let created = Stamp::new(Hlc::new(physical_now_ms(), 0), e.node_id(), e.epoch());
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    let txn = e.begin_write(WriterHolder::Ddl).unwrap();
    crate::register::write_stage_row(
        &txn,
        CollectionId::derive(DB, ghost),
        &StageMeta { created, info: StageInfo { deadline } },
    )
    .unwrap();
    txn.commit().unwrap();
    in_effect(
        &e,
        "orders",
        stamp_after(&e, created.hlc.wall_ms + 1),
        RegisterEvent::Bind { physical: ghost.into() },
    );
    let resolved = e.resolve(DB, "orders").unwrap();
    assert_eq!(resolved.meta.id, orders.id, "the identity stays what the name resolves to");
    assert_eq!(resolved.visible, None);
}

#[test]
fn a_logical_drop_in_effect_makes_the_name_absent() {
    let (e, _d) = engine();
    e.create_collection(DB, "orders").unwrap();
    in_effect(&e, "orders", stamp_after(&e, physical_now_ms() + 1), RegisterEvent::LogicalDrop);
    match e.resolve(DB, "orders") {
        Err(StorageError::Core(CoreError::CollectionNotFound { .. })) => {}
        other => panic!("a name the register says was dropped answers as absent: {other:?}"),
    }
}

#[test]
fn a_recreation_in_effect_resolves_to_the_recreated_physical() {
    let (e, _d) = engine();
    e.create_collection(DB, "orders").unwrap();
    e.create_collection(DB, "orders.rc1").unwrap();
    in_effect(
        &e,
        "orders",
        stamp_after(&e, physical_now_ms() + 1),
        RegisterEvent::Create { physical: "orders.rc1".into() },
    );
    let resolved = e.resolve(DB, "orders").unwrap();
    assert_eq!(resolved.physical, "orders.rc1");
}
