//! The TTL pass gives its worker up while it walks.
//!
//! [ADR-199](../../../docs/decisions.md): the expiry loop ran its pass inline
//! on a tokio worker. The pass walks each TTL index's expired range and
//! deletes what it finds, and while it ran that worker polled nothing else:
//! not a task, and not the runtime's I/O and timer driver, so a timer due
//! during the pass fired when it ended. On a member that owned the TTL
//! collections the stop signal was measured arriving up to 0.74 s late.
//!
//! One worker, on purpose, as in `scan_yields.rs`: the smallest runtime on
//! which "the timer fired during the pass" and "the timer fired after it" are
//! different observations. The test body runs on the thread that called
//! `block_on`, which is not a worker and drives no timer.
//!
//! The walk is made slow with the storage crate's per-row test delay, which is
//! process-wide; this file holds one test so that nothing else runs under it.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bson::{Document, doc};
use kimmy_storage::Engine;

/// Expired documents in the fixture: all of them within one pass's bound.
const N: u64 = 200;
/// Each row of the pass's walk, so that the walk alone takes N × this.
const ROW: Duration = Duration::from_millis(5);
/// The probe's timer, and how late it may fire.
const TICK: Duration = Duration::from_millis(10);
const LATE: Duration = Duration::from_millis(200);

/// A live state over a fresh database holding `N` documents a TTL index says
/// expired long ago.
fn fixture(dir: &tempfile::TempDir) -> kimmy_api::SharedState {
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let tokens =
        kimmy_auth::TokenIssuer::new("an-adequately-long-test-secret-for-hs256", 3600).unwrap();
    let state = kimmy_api::state(engine, tokens, false, kimmy_api::RateLimits::disabled()).unwrap();
    state.engine.create_collection("app", "sessions").unwrap();
    state
        .engine
        .create_index_with(
            "app",
            "sessions",
            vec![kimmy_storage::IndexField::ascending("seen")],
            false,
            Default::default(),
            Some("ttl_seen".into()),
            Some(60),
            None,
        )
        .unwrap();
    let meta = state.engine.get_collection("app", "sessions").unwrap();
    let docs: Vec<Document> =
        (0..N as i64).map(|i| doc! { "_id": i, "seen": bson::DateTime::from_millis(i) }).collect();
    state.engine.insert_many(&meta, docs).unwrap();
    state
}

#[test]
fn a_timer_fires_on_time_while_the_ttl_pass_walks() {
    let dir = tempfile::tempdir().unwrap();
    let state = fixture(&dir);
    let runtime =
        tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();

    // Slowed only now: the fixture's index build walks too.
    kimmy_storage::walk::set_test_walk_row_delay(ROW);
    let (lateness, took) = runtime.block_on(async {
        let me = state.engine.node_id();
        // The first tick is skipped, so the first pass starts one interval in.
        let expiry = tokio::spawn(kimmy_api::expiry::run(
            state.clone(),
            me,
            None,
            Duration::from_millis(50),
        ));

        // A task that wants nothing but its timer: how late the timer fires
        // is how long the driver went unpolled.
        let done = Arc::new(AtomicBool::new(false));
        let worst = Arc::new(AtomicU64::new(0));
        let probe = tokio::spawn({
            let (done, worst) = (Arc::clone(&done), Arc::clone(&worst));
            async move {
                while !done.load(Ordering::SeqCst) {
                    let asked = Instant::now();
                    tokio::time::sleep(TICK).await;
                    let late = asked.elapsed().saturating_sub(TICK);
                    worst.fetch_max(late.as_micros() as u64, Ordering::SeqCst);
                }
            }
        });

        // Watched from this thread, which is not the worker, so that the
        // watching needs neither the worker nor a timer.
        let began = Instant::now();
        while state.metrics.snapshot().ttl_expired < N {
            assert!(began.elapsed() < Duration::from_secs(60), "the pass never finished");
            std::thread::sleep(Duration::from_millis(5));
        }
        let took = began.elapsed();
        done.store(true, Ordering::SeqCst);
        probe.await.unwrap();
        // Awaited, so that no pass is still running when the runtime drops.
        expiry.abort();
        let _ = expiry.await;
        (Duration::from_micros(worst.load(Ordering::SeqCst)), took)
    });
    kimmy_storage::walk::set_test_walk_row_delay(Duration::ZERO);

    // The walk alone is N rows at ROW each, so the pass was long enough for
    // an inline one to have starved the probe several times over.
    assert!(took >= ROW * N as u32, "the pass took {took:?}: the walk was not slowed");
    assert!(
        lateness < LATE,
        "a {TICK:?} timer fired {lateness:?} late during a {took:?} pass: the pass held the \
         only worker, and the timer driver with it"
    );
}
