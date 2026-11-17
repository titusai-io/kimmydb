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
