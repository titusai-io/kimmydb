//! The gather of the keys of an index range that an `update`, a `delete` or a
//! `find_and_modify` makes under the writer is ended by the node's stop, and answered as the
//! shutdown answers any request it cut (ADR-266, ADR-270): `503 node_stopping`, with nothing
//! written and the pool at 0 afterwards. Before the gather ticked, the stop reached a write
//! only at its first candidate document, after the whole range was collected.
//!
//! The gather is made the long part of the request, so that a stop that only reached the first
//! candidate would not do: every index entry takes a millisecond to read
//! (`set_test_walk_row_delay`), the point reads behind them take none.
//!
//! The stop is a flag for the whole process that nothing clears, so this is a binary of its
//! own with one test.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bson::{Document, doc};
use kimmy_api::AggregateMemory;
use kimmy_api::error::{ApiError, ErrorCode};
use kimmy_api::exec::{self, WriteParams};
use kimmy_api::limits::within_request_deadline_of;
use kimmy_api::state::Auth;
use kimmy_core::deadline;
use kimmy_storage::walk::set_test_walk_row_delay;
use kimmy_storage::{Engine, IndexField};

type State = kimmy_api::state::SharedState;

/// The update of every document of a range that its filter rejects, as a request with an hour
/// to run: a write that matches nothing, so the range is all there is to gather.
fn rejecting_update(state: &State) -> Result<serde_json::Value, ApiError> {
    let auth = Auth(kimmy_auth::Principal::insecure_root());
    let params = WriteParams {
        filter: Some(serde_json::json!({ "t": { "$gte": 0 }, "missing": 1 })),
        multi: true,
        ..Default::default()
    };
    let hour = Duration::from_secs(3600);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(within_request_deadline_of(hour, hour, async {
            exec::update(
                state,
                &auth,
                "app",
                "orders",
                &serde_json::json!({ "$set": { "x": 1 } }),
                params,
            )
        }))
        .unwrap()
}

#[test]
fn a_write_gathering_a_range_is_ended_by_the_stop_and_answered_node_stopping() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let tokens =
        kimmy_auth::TokenIssuer::new("an-adequately-long-test-secret-for-hs256", 3600).unwrap();
    let state = kimmy_api::state(engine, tokens, false, kimmy_api::RateLimits::disabled()).unwrap();
    state.set_aggregate_memory(AggregateMemory::new(1 << 30, 1 << 30));
    state.engine.create_collection("app", "orders").unwrap();
    let index = vec![IndexField::ascending("t")];
    state.engine.create_index("app", "orders", index, false, None).unwrap();
    let meta = state.engine.get_collection("app", "orders").unwrap();
    // Within the 10,000 keys a write may gather unpooled: the stop is what ends the gather.
    let docs: Vec<Document> = (0..3_000_i64).map(|i| doc! { "_id": i, "t": i }).collect();
    state.engine.insert_many(&meta, docs).unwrap();
    set_test_walk_row_delay(Duration::from_millis(1));

    // How long the whole gather and the reads behind it take when nothing stops it.
    let started = Instant::now();
    assert_eq!(rejecting_update(&state).unwrap()["matched"], 0);
    let whole = started.elapsed();

    // The node stops a quarter of the way through the same request.
    let stopper = std::thread::spawn(move || {
        std::thread::sleep(whole / 4);
        deadline::stop_requests();
    });
    let started = Instant::now();
    let answered = rejecting_update(&state).expect_err("the stop ended the write");
    let took = started.elapsed();
    stopper.join().unwrap();
    assert_eq!(answered.status.as_u16(), 503);
    assert_eq!(answered.code, ErrorCode::NodeStopping);
    assert!(answered.message.contains("shutting down"), "{}", answered.message);
    assert!(took < whole * 3 / 4, "ended at the stop, not at the end: {took:?} of {whole:?}");
    set_test_walk_row_delay(Duration::ZERO);

    let pool = state.aggregate_memory().pool();
    assert_eq!((pool.used(), pool.holders()), (0, 0));
    assert_eq!(
        state.engine.get(&meta, &kimmy_core::DocId::Int64(0)).unwrap().unwrap().get("x"),
        None,
        "nothing was written"
    );
}
