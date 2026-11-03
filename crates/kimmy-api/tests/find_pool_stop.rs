//! A `find` waiting in the aggregation pool for the keys of an index range is ended by the
//! node's stop, and answered as the shutdown answers any request it cut (ADR-266, ADR-269,
//! ADR-270): `503 node_stopping`, with the pool at 0 afterwards.
//!
//! The stop is a flag for the whole process that nothing clears, so this is a binary of its
//! own with one test.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bson::{Document, doc};
use kimmy_api::AggregateMemory;
use kimmy_api::error::ErrorCode;
use kimmy_api::exec::{self, FindParams};
use kimmy_api::state::Auth;
use kimmy_core::deadline;
use kimmy_storage::{Engine, IndexField};

#[test]
fn a_find_waiting_in_the_pool_for_its_keys_is_ended_by_the_stop_and_answered_node_stopping() {
    // A range of 50,000 entries and a filter that rejects them all: the find goes from
    // pass to pass, doubling, and from its second pass holds keys that are charged to the
    // pool. It is the older holder; another request then takes all that is left of the pool
    // and never lets go, so the find waits at its next key, until the node stops.
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let tokens =
        kimmy_auth::TokenIssuer::new("an-adequately-long-test-secret-for-hs256", 3600).unwrap();
    let state = kimmy_api::state(engine, tokens, false, kimmy_api::RateLimits::disabled()).unwrap();
    state.set_aggregate_memory(AggregateMemory::new(64 << 20, 64 << 20));
    state.engine.create_collection("app", "orders").unwrap();
    let index = vec![IndexField::ascending("t")];
    state.engine.create_index("app", "orders", index, false, None).unwrap();
    let meta = state.engine.get_collection("app", "orders").unwrap();
    let docs: Vec<Document> = (0..50_000_i64).map(|i| doc! { "_id": i, "t": i }).collect();
    state.engine.insert_many(&meta, docs).unwrap();
    let memory = state.aggregate_memory();
    let pool = memory.pool().clone();

    let started = Instant::now();
    let find = {
        let state = Arc::clone(&state);
        std::thread::spawn(move || {
            let auth = Auth(kimmy_auth::Principal::insecure_root());
            let params = FindParams {
                filter: Some(serde_json::json!({ "t": { "$gte": 0 }, "missing": 1 })),
                ..Default::default()
            };
            exec::find(&state, &auth, "app", "orders", params)
        })
    };
    // The find holds 1,000 keys of the pool, in a pass that wants more: past its window, and
    // not about to end.
    let thousand_keys = 1_000 * kimmy_query::budget::held_set_key(5);
    while pool.used() < thousand_keys {
        assert!(started.elapsed() < Duration::from_secs(120), "the find never held the pool");
        std::thread::sleep(Duration::from_millis(1));
    }
    // A younger request takes the rest of the pool: retried if the find grew in between.
    let other = memory.limits();
    let young = loop {
        let mut lease = other.buffer.lease("another request");
        if lease.charge(pool.cap() - pool.used()).is_ok() {
            break lease;
        }
    };

    // (The other request may have been refused once or twice before it got in.)
    let refused = pool.pool_refusals();
    std::thread::sleep(Duration::from_millis(300));
    assert!(!find.is_finished(), "the older holder waits for the pool");
    assert_eq!(pool.pool_refusals(), refused);

    deadline::stop_requests();
    let answered = find.join().unwrap().expect_err("the stop ended the wait");
    assert_eq!(answered.status.as_u16(), 503);
    assert_eq!(answered.code, ErrorCode::NodeStopping);
    assert!(answered.message.contains("shutting down"), "{}", answered.message);
    assert!(started.elapsed() < Duration::from_secs(60), "ended by the stop");
    assert_eq!(pool.pool_refusals(), refused, "a stop is not a refusal of the pool");
    assert_eq!(pool.used(), young.bytes(), "the find gave back what it held");
    drop(young);
    drop(other);
    assert_eq!((pool.used(), pool.holders()), (0, 0));
}
