//! The purge of a physical the register buried is deferred past the requests in flight (ADR-272, decision 5; Q6.4 item 2). A
//! burial by the register is a drop with a deferral: the physical is gone to everything that asks the name, and its ROWS stay for
//! `purge_not_before` = this member's wall clock at the burial + `server.request_timeout_secs`, so a request that resolved the name
//! just before the flip can finish reading what it resolved. The instant is local to the member and is never replicated. A drop a
//! client makes is not deferred.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp, VersionVector,
};

use crate::{Engine, PeerWindow, SyncOutcome, WalkScope, physical_now_ms};

const DB: &str = "app";
const STAGE: &str = "orders.__stg.a1";

fn engine_at(path: &std::path::Path) -> Engine {
    Engine::open(path).unwrap()
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

/// A member holding `orders` (with documents) and the creation of a stage, and the binding it has not applied yet.
fn world(path: &std::path::Path) -> (Engine, OplogEntry, tempfile::TempDir) {
    world_with_binding_ahead(path, 1)
}

/// [`world`], the binding stamped `ahead_ms` above the stage's creation.
fn world_with_binding_ahead(
    path: &std::path::Path,
    ahead_ms: u64,
) -> (Engine, OplogEntry, tempfile::TempDir) {
    let da = tempfile::tempdir().unwrap();
    let a = engine_at(&da.path().join("a.redb"));
    a.create_stage(DB, STAGE, MergePolicy::Document, Hlc::new(physical_now_ms() + 600_000, 0))
        .unwrap();
    let creation = everything(&a)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, STAGE)
        })
        .unwrap();
    let bind = OplogEntry {
        stamp: Stamp::new(
            Hlc::new(creation.stamp.hlc.wall_ms + ahead_ms, 0),
            a.node_id(),
            a.epoch(),
        ),
        kind: OpKind::BindCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: STAGE.into(),
            })
            .unwrap(),
        ),
        meta: None,
    };
    let m = engine_at(path);
    let orders = m.create_collection(DB, "orders").unwrap();
    for i in 0..3 {
        m.insert(&orders, doc! { "_id": i, "v": "old" }).unwrap();
    }
    m.apply_batch(std::slice::from_ref(&creation)).unwrap();
    (m, bind, da)
}

fn orders_id() -> CollectionId {
    CollectionId::derive(DB, "orders")
}

#[test]
fn the_rows_of_a_physical_the_register_buried_stay_until_the_deferral_has_passed() {
    let dir = tempfile::tempdir().unwrap();
    let (m, bind, _da) = world(&dir.path().join("m.redb"));
    m.set_purge_deferral(Duration::from_secs(60));
    window(&m, std::slice::from_ref(&bind)); // the flip buries `orders`
    assert!(
        m.purge_pending(orders_id()).unwrap(),
        "premise: the rows of the buried physical are owed to the purger"
    );
    assert_eq!(
        m.purge_chunk(orders_id()).unwrap(),
        0,
        "a chunk before the instant removes nothing"
    );
    assert!(m.rows_under(orders_id()).unwrap() > 0, "the rows are still there");
    m.set_clock_offset_for_tests(61_000); // the member's clock passes the instant
    assert!(m.purge_chunk(orders_id()).unwrap() > 0, "after it the purger takes them");
    assert_eq!(m.rows_under(orders_id()).unwrap(), 0);
}

#[test]
fn a_request_that_resolved_the_name_before_the_flip_still_reads_what_it_resolved() {
    let dir = tempfile::tempdir().unwrap();
    let (m, bind, _da) = world(&dir.path().join("m.redb"));
    m.set_purge_deferral(Duration::from_secs(60));
    let resolved = m.resolve(DB, "orders").unwrap().meta; // the request resolves the name
    window(&m, std::slice::from_ref(&bind)); // the swap lands: `orders` is buried and the name answers from the stage
    m.finish_purges_now().unwrap(); // everything the purger would do now
    assert!(
        m.get(&resolved, &kimmy_core::DocId::Int64(1)).unwrap().is_some(),
        "the physical the request resolved is gone to the name and its rows are still readable by the request that has it"
    );
    m.set_clock_offset_for_tests(61_000);
    m.finish_purges_now().unwrap();
    assert!(
        m.get(&resolved, &kimmy_core::DocId::Int64(1)).unwrap().is_none(),
        "after the instant the rows are gone"
    );
}

#[test]
fn the_instant_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.redb");
    let (m, bind, _da) = world(&path);
    m.set_purge_deferral(Duration::from_secs(600));
    window(&m, std::slice::from_ref(&bind));
    assert!(m.close().is_ok(), "the engine closed");
    let reopened = engine_at(&path);
    // not told the deferral again: the instant is a row, not a setting
    assert!(reopened.purge_pending(orders_id()).unwrap(), "premise: the rows are owed");
    assert_eq!(
        reopened.purge_chunk(orders_id()).unwrap(),
        0,
        "the reopened member still leaves them alone until the instant it recorded"
    );
}

#[test]
fn a_drop_a_client_makes_is_not_deferred_and_a_zero_deferral_defers_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (m, bind, _da) = world(&dir.path().join("m.redb"));
    m.set_purge_deferral(Duration::from_secs(60));
    let plain = m.create_collection(DB, "plain").unwrap();
    m.insert(&plain, doc! { "_id": 1 }).unwrap();
    assert!(m.drop_collection(DB, "plain").unwrap());
    assert!(
        m.purge_chunk(CollectionId::derive(DB, "plain")).unwrap() > 0,
        "a client's drop is purged at once"
    );
    // and with no deferral configured the register's burial is purged at once too
    m.set_purge_deferral(Duration::ZERO);
    window(&m, std::slice::from_ref(&bind));
    assert!(m.purge_chunk(orders_id()).unwrap() > 0, "zero defers nothing");
}

#[test]
fn the_instant_is_this_members_own_clock_and_not_the_stamp_of_the_event() {
    // The binding is stamped four minutes ahead (inside the bound U1 allows). The instant is the member's wall clock at the burial
    // plus the deferral, so a clock that has moved past it by a little more than the deferral lets the purger in; an instant
    // computed from the event's stamp would sit four minutes further.
    let dir = tempfile::tempdir().unwrap();
    let (m, bind, _da) = world_with_binding_ahead(&dir.path().join("m.redb"), 240_000);
    m.set_purge_deferral(Duration::from_secs(60));
    window(&m, std::slice::from_ref(&bind));
    assert_eq!(m.purge_chunk(orders_id()).unwrap(), 0, "premise: deferred");
    m.set_clock_offset_for_tests(61_000);
    assert!(
        m.purge_chunk(orders_id()).unwrap() > 0,
        "the instant is this member's own clock plus the deferral, not the stamp of the event that buried the physical"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_purger_takes_a_deferred_physical_at_its_instant_and_not_at_its_next_idle_turn() {
    let dir = tempfile::tempdir().unwrap();
    let (m, bind, _da) = world(&dir.path().join("m.redb"));
    let m = Arc::new(m);
    m.set_purge_deferral(Duration::from_millis(1_500));
    let purger = tokio::spawn(Arc::clone(&m).run_drop_purger());
    tokio::time::sleep(Duration::from_millis(100)).await; // the purger is idle
    window(&m, std::slice::from_ref(&bind));
    let burial = Instant::now();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(m.rows_under(orders_id()).unwrap() > 0, "inside the deferral the rows stay");
    while m.rows_under(orders_id()).unwrap() > 0 && burial.elapsed() < Duration::from_secs(4) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        m.rows_under(orders_id()).unwrap(),
        0,
        "the purger took the rows at their instant, not at its next idle turn five seconds on"
    );
    purger.abort();
}
