//! A filtered vector search whose filter pins `_id` (`{_id: {$in: [...]}}`) reads its ids
//! inline, not in a walk, and charges the ids it admits to a lease that waits for a full pool.
//! That wait must be on a thread the runtime has handed its queue to (`kimmy_storage::blocking`),
//! not on the async worker (ADR-153, ADR-199): one per worker is every worker gone.
//!
//! The runtime has one worker. A search that is the older holder waits in the pool, a task
//! that only sleeps for a few milliseconds is spawned beside it, and the task has to run while
//! the search is still waiting. On a worker held by the wait it could not run until the search
//! ended, which is at its deadline.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bson::{Document, doc};
use kimmy_api::AggregateMemory;
use kimmy_api::limits::within_request_deadline_of;
use kimmy_api::state::Auth;
use kimmy_api::vectors::{SearchRequest, run_vector_search};
use kimmy_core::{ChunkConfig, DocId, Metric, ProviderConfig, VectorConfig, VectorRecord};
use kimmy_storage::Engine;

#[test]
fn an_id_pinned_filter_waiting_in_the_pool_does_not_hold_an_async_worker() {
    // 11,500 documents and a filter that names all their `_id`s: the search is charged for its
    // ids from the 10,001st, as the older holder. Another request then takes all that is left
    // of the pool and never lets go, so the search waits at its next id. (Each document is
    // matched against the whole list, so the list is no longer than it has to be.)
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let tokens =
        kimmy_auth::TokenIssuer::new("an-adequately-long-test-secret-for-hs256", 3600).unwrap();
    let state = kimmy_api::state(engine, tokens, false, kimmy_api::RateLimits::disabled()).unwrap();
    state.set_aggregate_memory(AggregateMemory::new(64 << 20, 64 << 20));
    state.engine.create_collection("app", "orders").unwrap();
    let config = VectorConfig {
        fields: vec!["text".into()],
        provider: ProviderConfig::Byo {},
        dim: 2,
        metric: Metric::Cosine,
        document_prefix: None,
        query_prefix: None,
        chunk: ChunkConfig::default(),
    };
    state.engine.configure_vectors("app", "orders", config).unwrap();
    let meta = state.engine.get_collection("app", "orders").unwrap();
    let shadow = state.engine.vector_collection("app", "orders").unwrap().unwrap();
    let docs: Vec<Document> = (0..11_500_i64).map(|i| doc! { "_id": i }).collect();
    state.engine.insert_many(&meta, docs).unwrap();
    let record = VectorRecord {
        source: DocId::Int64(0),
        chunk: 0,
        source_hlc: kimmy_core::Hlc::new(1, 0),
        vector: vec![1.0, 0.0],
        text: "d0".into(),
        config: None,
    };
    state.engine.put_vectors(&shadow, &DocId::Int64(0), &[record]).unwrap();
    let memory = state.aggregate_memory();
    let pool = memory.pool().clone();

    let runtime =
        tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
    let started = Instant::now();
    // The search's own request deadline, and what every wait of the test below is bounded by.
    // Reading its first 10,001 ids is the slow part: 19 to 28 seconds in the gates' debug build, and
    // more when other work is on the machine. A search that reaches a shorter deadline first ends
    // before it holds the pool, which is not what this test is about, so the deadline is far
    // past that and the wait for the pool below notices a search that ended early, whatever
    // the deadline. A worker held by the pool's wait still fails the test: the probe task
    // below does not run, however long the deadline is.
    let deadline = Duration::from_secs(300);
    let search = {
        let state = Arc::clone(&state);
        runtime.spawn(async move {
            let auth = Auth(kimmy_auth::Principal::insecure_root());
            let ids: Vec<i64> = (0..11_500).collect();
            let body = SearchRequest {
                vector: Some(vec![1.0, 0.0]),
                filter: Some(serde_json::json!({ "_id": { "$in": ids } })),
                k: Some(3),
                ..Default::default()
            };
            within_request_deadline_of(
                deadline,
                deadline,
                run_vector_search(&state, &auth, "app", "orders", &body),
            )
            .await
        })
    };
    // The search holds more than its free ids: it has a lease, and is not about to end.
    let past_the_window = 10_001 * kimmy_query::budget::held_doc_id(&DocId::Int64(1));
    while pool.used() < past_the_window {
        if search.is_finished() {
            let ended = match runtime.block_on(search) {
                Ok(Ok(_)) => "it answered".to_string(),
                Ok(Err(e)) => format!("it ended with {e:?}"),
                Err(e) => format!("its task failed: {e}"),
            };
            panic!(
                "the search ended before it held the pool ({} of the {past_the_window} bytes of its ids after {:?}): {ended}",
                pool.used(),
                started.elapsed()
            );
        }
        assert!(
            started.elapsed() < deadline + Duration::from_secs(30),
            "the search neither held the pool nor ended within its deadline of {deadline:?}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    // A younger request takes the rest of the pool: retried if the search grew in between.
    let other = memory.limits();
    let young = loop {
        let mut lease = other.buffer.lease("another request");
        if lease.charge(pool.cap() - pool.used()).is_ok() {
            break lease;
        }
    };
    let refused = pool.pool_refusals();

    // The search waits for the pool, and a task beside it still runs.
    let ran = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&ran);
    runtime.spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        flag.store(true, Ordering::SeqCst);
    });
    let probed = Instant::now();
    while !ran.load(Ordering::SeqCst) && probed.elapsed() < Duration::from_secs(5) {
        std::thread::sleep(Duration::from_millis(5));
    }
    let waiting = !search.is_finished();
    let ran = ran.load(Ordering::SeqCst);

    // Let the search go on, however the checks came out, so the runtime can end.
    drop(young);
    let answered = runtime.block_on(search).unwrap();
    assert!(waiting, "the older holder waits for the pool, as a read does");
    assert!(
        ran,
        "a task spawned beside a search waiting in the pool did not run: the wait is on the worker"
    );
    assert_eq!(pool.pool_refusals(), refused);
    let _ = answered.expect("answered once the pool has room");
    drop(other);
    assert_eq!((pool.used(), pool.holders()), (0, 0));
}
