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

/// The round loop reports a refused window in `entries_skipped_clock_refused` (the metric's source), and the engine
/// names the origin: the two things an operator reads on `/metrics`.
#[tokio::test(flavor = "multi_thread")]
async fn the_round_report_and_the_engine_count_a_refused_window_by_origin() {
    use crate::discovery::SeedSource;
    use crate::peers::{ReplicationConfig, RoundReport, replicate};

    let (a, a_addr, _a_dir) = donor().await;
    let (b, _b_dir) = engine();
    b.set_clock_offset_for_tests(-(SKEW_MS + 10_000));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let b_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let mut config =
        ReplicationConfig::new(vec![SeedSource::Static(vec![a_addr])], SECRET.into(), b_addr);
    config.sync_interval = std::time::Duration::from_millis(100);
    config.discovery_interval = std::time::Duration::from_millis(100);
    config.on_round = Some(std::sync::Arc::new(move |report| {
        let _ = tx.send(report);
    }));
    let looping = tokio::spawn(replicate(std::sync::Arc::clone(&b), config));

    let reported = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let report = rx.recv().await.expect("the loop keeps reporting");
            if report.entries_skipped_clock_refused > 0 {
                return report;
            }
        }
    })
    .await
    .expect("a round reported the refused window");
    looping.abort();

    assert_eq!(reported.failed, 0, "a clock stop is no failed round: {reported:?}");
    let counts = b.clock_refusals();
    assert_eq!(counts.len(), 1, "one origin named: {counts:?}");
    assert_eq!(counts[0].0, a.node_id().to_string(), "the donor's node id");
    assert!(counts[0].1 >= 1);
}
