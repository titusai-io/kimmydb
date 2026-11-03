//! A filtered vector search waiting in the aggregation pool for the ids its filter admits is
//! ended by the node's stop, and answered as the shutdown answers any request it cut (ADR-266,
//! ADR-269, ADR-270): `503 node_stopping`, with the pool at 0 afterwards.
//!
//! The stop is a flag for the whole process that nothing clears, so this is a binary of its
//! own with one test.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bson::{Document, doc};
use kimmy_api::AggregateMemory;
use kimmy_api::error::ErrorCode;
use kimmy_api::limits::within_request_deadline_of;
use kimmy_api::state::Auth;
use kimmy_api::vectors::{SearchRequest, run_vector_search};
use kimmy_core::{
    ChunkConfig, DocId, Metric, ProviderConfig, VectorConfig, VectorRecord, deadline,
};
use kimmy_storage::Engine;
use kimmy_storage::walk::set_test_walk_row_delay;

#[test]
fn a_filtered_search_waiting_in_the_pool_for_its_ids_is_ended_by_the_stop() {
    // 50,000 documents and a filter that admits them all: the search is charged for its ids
    // from the 10,001st, as the older holder. Another request then takes all that is left of
    // the pool and never lets go, so the search waits at its next id, until the node stops.
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
    for start in (0..50_000_i64).step_by(10_000) {
        let docs: Vec<Document> = (start..start + 10_000).map(|i| doc! { "_id": i }).collect();
        state.engine.insert_many(&meta, docs).unwrap();
    }
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

    let started = Instant::now();
    let search = {
        let state = Arc::clone(&state);
        std::thread::spawn(move || {
            let auth = Auth(kimmy_auth::Principal::insecure_root());
            let body = SearchRequest {
                vector: Some(vec![1.0, 0.0]),
                filter: Some(serde_json::json!({ "_id": { "$gte": 0 } })),
                k: Some(3),
                ..Default::default()
            };
            let hour = Duration::from_secs(3600);
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap()
                .block_on(within_request_deadline_of(
                    hour,
                    hour,
                    run_vector_search(&state, &auth, "app", "orders", &body),
                ))
                .unwrap()
        })
    };
    // The search holds more than its free ids: it has a lease, and is not about to end.
    let past_the_window = 11_000 * kimmy_query::budget::held_doc_id(&DocId::Int64(1));
    while pool.used() < past_the_window {
        assert!(started.elapsed() < Duration::from_secs(120), "the search never held the pool");
        std::thread::sleep(Duration::from_millis(1));
    }
    // The rows it has left take a millisecond each, so that the request below has all the time
    // it needs to take the rest of the pool before the search can have finished its ids.
    set_test_walk_row_delay(Duration::from_millis(1));
    // A younger request takes the rest of the pool: retried if the search grew in between.
    let other = memory.limits();
    let young = loop {
        let mut lease = other.buffer.lease("another request");
        if lease.charge(pool.cap() - pool.used()).is_ok() {
            break lease;
        }
    };

    let refused = pool.pool_refusals();
    std::thread::sleep(Duration::from_millis(300));
    assert!(!search.is_finished(), "the older holder waits for the pool, as a read does");
    assert_eq!(pool.pool_refusals(), refused);

    deadline::stop_requests();
    let answered = search.join().unwrap().expect_err("the stop ended the wait");
    assert_eq!(answered.status.as_u16(), 503);
    assert_eq!(answered.code, ErrorCode::NodeStopping);
    assert!(answered.message.contains("shutting down"), "{}", answered.message);
    assert!(started.elapsed() < Duration::from_secs(60), "ended by the stop");
    assert_eq!(pool.pool_refusals(), refused, "a stop is not a refusal of the pool");
    assert_eq!(pool.used(), young.bytes(), "the search gave back what it held");
    drop(young);
    drop(other);
    assert_eq!((pool.used(), pool.holders()), (0, 0));
}
