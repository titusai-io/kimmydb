//! A read that walks a collection gives its worker up while it walks.
//!
//! [ADR-153](../../../docs/decisions.md): `find`, `count` and `aggregate`
//! run their scan synchronously, and a scan that runs inline on a tokio worker
//! holds that worker for as long as the collection is. With as many scans in
//! flight as the runtime has workers, nothing else runs — not a scrape of
//! `/metrics`, not `/v1/version`, not a one-document `find`. Under
//! `kimmy_storage::blocking` the worker hands its queue to another thread
//! before the walk begins, so a task spawned while the walk runs is polled
//! while it runs rather than after it.
//!
//! One worker, on purpose: the smallest runtime on which "the other task ran
//! during the scan" and "the other task ran after the scan" are different
//! observations. The test body itself runs on the thread that called
//! `block_on`, not on the worker, so it can spawn and wait without taking the
//! worker's place.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use bson::{Document, doc};
use kimmy_api::exec::{self, FindParams};
use kimmy_api::state::Auth;
use kimmy_storage::Engine;

const N: usize = 30_000;

/// A live state over a fresh database holding `N` small documents — enough
/// that a count of them takes longer than handing a worker's queue to another
/// thread, by orders of magnitude.
fn fixture(dir: &tempfile::TempDir) -> kimmy_api::SharedState {
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let tokens =
        kimmy_auth::TokenIssuer::new("an-adequately-long-test-secret-for-hs256", 3600).unwrap();
    let state = kimmy_api::state(engine, tokens, false, kimmy_api::RateLimits::disabled()).unwrap();
    state.engine.create_collection("app", "docs").unwrap();
    let meta = state.engine.get_collection("app", "docs").unwrap();
    for batch in 0..(N / 1_000) {
        let docs: Vec<Document> = (0..1_000)
            .map(|i| doc! { "n": (batch * 1_000 + i) as i64, "body": "x".repeat(64) })
            .collect();
        state.engine.insert_many(&meta, docs).unwrap();
    }
    state
}

fn root() -> Auth {
    Auth(kimmy_auth::Principal::insecure_root())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn a_task_spawned_during_a_count_is_polled_before_the_count_finishes() {
    let dir = tempfile::tempdir().unwrap();
    let state = fixture(&dir);

    // The count, on the one worker. `started` is raised on the worker just
    // before the scan begins and nothing between the two yields, so once it
    // is seen the worker is inside the scan.
    let started = Arc::new(AtomicBool::new(false));
    let counting = tokio::spawn({
        let state = state.clone();
        let started = Arc::clone(&started);
        async move {
            started.store(true, Ordering::SeqCst);
            let body = exec::count(&state, &root(), "app", "docs", FindParams::default()).unwrap();
            let finished = Instant::now();
            assert_eq!(body["count"], N as u64);
            finished
        }
    });
    while !started.load(Ordering::SeqCst) {
        std::thread::yield_now();
    }

    // A task that wants nothing but a worker to be polled on. Before ADR-153
    // it got one when the count let go; now the count let go before it began.
    let bystander = tokio::spawn(async { Instant::now() });

    let polled = bystander.await.unwrap();
    let finished = counting.await.unwrap();
    assert!(
        polled < finished,
        "the bystander was polled {:?} after the count finished: the scan held the only worker",
        polled - finished
    );
}
