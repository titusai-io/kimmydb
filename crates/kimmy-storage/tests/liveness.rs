//! Storage commits must not starve the async runtime.
//!
//! One runtime worker, four tasks hammering the single-writer engine with
//! durable commits, and a fifth task that only wants a 20 ms timer to fire
//! on time. Before `Engine::begin_write` and `WriteTxn::commit` yielded the
//! worker, the writers held it for their whole loop and the timer fired
//! seconds late — which on a real node is a peer's TLS handshake timing out
//! and SWIM marking the member down.

use std::sync::Arc;
use std::time::{Duration, Instant};

use kimmy_storage::Engine;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn durable_commits_do_not_starve_the_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let coll = engine.create_collection("app", "docs").unwrap();

    // Writers: sync engine calls from inside async tasks, no `.await` between
    // them — the shape of every request handler and of anti-entropy apply.
    let mut writers = Vec::new();
    for w in 0..4u32 {
        let engine = Arc::clone(&engine);
        let coll = coll.clone();
        writers.push(tokio::spawn(async move {
            for batch in 0..15u32 {
                let docs: Vec<_> = (0..40u32)
                    .map(|i| bson::doc! { "_id": format!("w{w}-b{batch}-{i}"), "n": i })
                    .collect();
                engine.insert_many(&coll, docs).unwrap();
            }
        }));
    }

    // The probe: how late does a short timer fire while the writers run?
    let mut worst = Duration::ZERO;
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(3) {
        let t = Instant::now();
        tokio::time::sleep(Duration::from_millis(20)).await;
        worst = worst.max(t.elapsed().saturating_sub(Duration::from_millis(20)));
        if writers.iter().all(|h| h.is_finished()) {
            break;
        }
    }
    for h in writers {
        h.await.unwrap();
    }
    assert_eq!(engine.count(&coll).unwrap(), 4 * 15 * 40);
    assert!(
        worst < Duration::from_millis(250),
        "a timer on the runtime fired {worst:?} late while storage was committing"
    );
}
