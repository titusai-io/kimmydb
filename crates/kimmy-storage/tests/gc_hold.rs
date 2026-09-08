//! How long a retention pass that collects nothing holds the single writer.
//!
//! Diagnostic for the 0.25.1 round's write-throughput collapse. Not part of
//! the suite: it seeds a few hundred thousand documents and takes minutes.
//! Run with:
//!
//! ```text
//! GC_HOLD_DOCS=300000 cargo test -p kimmy-storage --release --test gc_hold -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bson::doc;
use kimmy_storage::{Engine, RetentionPolicy};

#[test]
#[ignore]
fn a_no_op_retention_pass_measured_against_a_concurrent_writer() {
    let docs: usize =
        std::env::var("GC_HOLD_DOCS").ok().and_then(|v| v.parse().ok()).unwrap_or(100_000);
    let cache: usize =
        std::env::var("GC_HOLD_CACHE_BYTES").ok().and_then(|v| v.parse().ok()).unwrap_or(8 << 20);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let engine = Arc::new(Engine::open_with_cache(&path, Some(cache)).unwrap());
    let coll = engine.create_collection("bench", "load_writes").unwrap();

    // ~1 KiB documents, in the 1,000-document batches the bulk route uses.
    let filler = "x".repeat(900);
    let seed_started = Instant::now();
    for batch in 0..(docs / 1_000) {
        let batch_docs = (0..1_000)
            .map(|i| doc! { "n": (batch * 1_000 + i) as i64, "body": filler.clone() })
            .collect();
        engine.insert_many(&coll, batch_docs).unwrap();
    }
    let file_bytes = std::fs::metadata(&path).unwrap().len();
    eprintln!(
        "seeded {docs} documents in {:.1?}; file {:.1} MiB; cache {} MiB",
        seed_started.elapsed(),
        file_bytes as f64 / 1048576.0,
        cache >> 20
    );

    // A writer that keeps trying, timing every wait for the writer, while the
    // pass runs. It is what a client `PUT` looks like from the engine's side.
    let stop = Arc::new(AtomicBool::new(false));
    let probe = {
        let engine = Arc::clone(&engine);
        let coll = coll.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut waits: Vec<Duration> = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                let t = Instant::now();
                engine.insert(&coll, doc! { "probe": true }).unwrap();
                waits.push(t.elapsed());
                std::thread::sleep(Duration::from_millis(200));
            }
            waits
        })
    };
    std::thread::sleep(Duration::from_secs(2));

    // Nothing is older than a day, so nothing is collected: this is the pass
    // the deployment runs every 600 s at 86,400 s retention.
    let policy = RetentionPolicy::new(86_400, 86_400);
    let pass_started = Instant::now();
    let outcome = engine.collect_garbage(policy).unwrap();
    let pass = pass_started.elapsed();
    stop.store(true, Ordering::Relaxed);
    let mut waits = probe.join().unwrap();
    waits.sort();

    let max = waits.last().copied().unwrap_or_default();
    let p50 = waits.get(waits.len() / 2).copied().unwrap_or_default();
    eprintln!(
        "retention pass over {docs} documents: {pass:.2?} (removed oplog {} tombstones {}); \
         {:.1} us/doc; probe writes {} — p50 wait {p50:.2?}, max wait {max:.2?}",
        outcome.oplog_removed,
        outcome.tombstones_removed,
        pass.as_micros() as f64 / docs as f64,
        waits.len(),
    );
    eprintln!("commits {} fsyncs {}", engine.commits(), engine.fsyncs());
    assert_eq!(outcome.oplog_removed, 0);
    assert_eq!(outcome.tombstones_removed, 0);
}
