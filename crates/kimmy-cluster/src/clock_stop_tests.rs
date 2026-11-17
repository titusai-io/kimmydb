//! U1 at the sites that decide "stop, or move on" (package 3.0): a window that ended at an entry whose
//! clock is not due yet is a stop, and every site that reads a stop treats it as one.
//!
//! The receiver's clock is moved back per engine (`set_clock_offset_for_tests`) so that what the donor
//! wrote just now is far-future to it, and forward again to make it due: no test waits.

use std::net::SocketAddr;
use std::sync::Arc;

use bson::doc;
use kimmy_core::{DocId, MergePolicy};
use kimmy_storage::Engine;

use crate::transport::{serve, sync_once};

const SECRET: &str = "a-clock-stop-test-secret";
const SKEW_MS: i64 = 300_000;

fn engine() -> (Arc<Engine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap()), dir)
}

/// A donor serving replication, holding a field collection `shop.orders` with document 1.
async fn donor() -> (Arc<Engine>, SocketAddr, tempfile::TempDir) {
    let (a, dir) = engine();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(Arc::clone(&a), listener, SECRET.into()));
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    a.insert(&ca, doc! { "_id": 1 }).unwrap();
    (a, addr, dir)
}

/// A pull that stopped at a refused entry is not the tail, and the next pull starts below the refused stamp.
#[tokio::test(flavor = "multi_thread")]
async fn a_pull_stopped_at_a_clock_refused_entry_is_not_exhausted_and_the_next_pull_starts_below_it()
 {
    let (a, a_addr, _a_dir) = donor().await;
    let (b, _b_dir) = engine();
    b.set_clock_offset_for_tests(-(SKEW_MS + 10_000));

    let outcome = sync_once(&b, a_addr, SECRET, None).await.unwrap();

    assert!(!outcome.exhausted, "the window is not marked exhausted: {outcome:?}");
    assert_eq!(outcome.clock_refused, 1, "the insert was refused: {outcome:?}");
    assert!(outcome.stopped(), "{outcome:?}");
    let stop = outcome.clock_refused_at.clone().expect("the stop names the entry");
    let witnessed = b.witnessed_vector().unwrap().get(stop.stamp.origin());
    assert!(
        witnessed < stop.stamp.hlc,
        "the next pull starts below the refused stamp: witnessed {witnessed:?}, refused {:?}",
        stop.stamp
    );

    // Due: the next pull applies it, from the same place.
    b.set_clock_offset_for_tests(0);
    let again = sync_once(&b, a_addr, SECRET, None).await.unwrap();
    assert_eq!(again.clock_refused, 0, "{again:?}");
    let cb = b.get_collection("shop", "orders").unwrap();
    assert_eq!(b.get(&cb, &DocId::Int64(1)).unwrap(), Some(doc! { "_id": 1 }), "applied once due");
    drop(a);
}

/// A member that can only be caught up by snapshot takes a page with a far-future document: it neither holds the
/// document nor marks its origin covered, the snapshot does not complete (a stop, never a skip), and a later sync
/// brings the document, with the donor's version identity, once it is due.
#[tokio::test(flavor = "multi_thread")]
async fn a_far_future_field_document_in_a_snapshot_page_is_not_held_or_covered_and_arrives_once_due()
 {
    let (a, a_addr, _a_dir) = donor().await;
    // A's history is gone, so B can only be caught up by snapshot.
    a.collect_garbage_at(
        kimmy_storage::physical_now_ms() + 1_000_000_000,
        kimmy_storage::RetentionPolicy::new(0, u64::MAX),
    )
    .unwrap();
    let (b, _b_dir) = engine();
    b.set_clock_offset_for_tests(-(SKEW_MS + 10_000));

    let outcome = sync_once(&b, a_addr, SECRET, None).await.expect(
        "B does not hold the far-future document: the sync must end in a stop, not an error",
    );

    assert_eq!(
        outcome.clock_refused, 1,
        "B does not hold the far-future document: the stop was not reported: {outcome:?}"
    );
    let held =
        b.get_collection("shop", "orders").ok().and_then(|c| b.get(&c, &DocId::Int64(1)).unwrap());
    assert_eq!(held, None, "B does not hold the far-future document");
    assert_eq!(
        b.witnessed_vector().unwrap().get(a.origin()),
        kimmy_core::Hlc::ZERO,
        "B did not mark its origin covered"
    );

    // Due: the snapshot resumes from where it stopped and the document arrives.
    b.set_clock_offset_for_tests(0);
    let again = sync_once(&b, a_addr, SECRET, None).await.unwrap();
    assert_eq!(again.clock_refused, 0, "{again:?}");
    let cb = b.get_collection("shop", "orders").unwrap();
    let ca = a.get_collection("shop", "orders").unwrap();
    let (vb, db) = b.get_versioned(&cb, &DocId::Int64(1)).unwrap().expect("arrives once due");
    let (va, _) = a.get_versioned(&ca, &DocId::Int64(1)).unwrap().unwrap();
    assert_eq!(db, doc! { "_id": 1 });
    assert_eq!(vb, va, "with the donor's version identity");
}
