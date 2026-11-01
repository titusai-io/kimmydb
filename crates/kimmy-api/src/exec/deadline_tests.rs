//! The request's deadline inside the synchronous paths (ADR-266): each entry path that
//! sets the deadline answers `503 timeout` once it has passed and works as before when it
//! has not, and the deadline set for a call is gone when the call returns.
//!
//! "Passed" here is a request whose deadline is `Instant::now()` (`as_a_request`), so the
//! path under test reads no time left and its first check fails.

use std::time::{Duration, Instant};

use bson::doc;
use kimmy_core::deadline::Deadline;
use serde_json::{Value, json};

use super::tests::{live_state, seed_docs};
use super::*;
use crate::error::ErrorCode;

fn root() -> Auth {
    Auth(kimmy_auth::Principal::superuser("test"))
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().build().unwrap()
}

/// Run synchronous `work` as a handler of a request with `left` of its time left.
fn in_a_request<T>(left: Duration, work: impl FnOnce() -> T) -> T {
    runtime().block_on(crate::limits::as_a_request(
        Instant::now() + left,
        Duration::from_secs(30),
        async { work() },
    ))
}

fn aggregate_in_a_request(
    state: &SharedState,
    pipeline: &Value,
    left: Duration,
) -> Result<Value, ApiError> {
    runtime().block_on(crate::limits::as_a_request(
        Instant::now() + left,
        Duration::from_secs(30),
        aggregate_with_limits(state, &root(), "app", "orders", pipeline, Default::default()),
    ))
}

fn is_the_timeout<T: std::fmt::Debug>(result: &Result<T, ApiError>) -> bool {
    matches!(result, Err(e) if e.status == axum::http::StatusCode::SERVICE_UNAVAILABLE
        && e.code == ErrorCode::Timeout)
}

const PAST: Duration = Duration::ZERO;
const PLENTY: Duration = Duration::from_secs(3600);

fn state_with_data(dir: &tempfile::TempDir) -> SharedState {
    let state = live_state(dir);
    seed_docs(
        &state,
        "orders",
        (0..6_i64).map(|i| doc! { "_id": i, "k": i % 3, "v": i, "tags": ["a", "b"] }).collect(),
    );
    seed_docs(
        &state,
        "items",
        (0..6_i64).map(|i| doc! { "_id": i, "k": i % 3, "next": (i + 1) % 6 }).collect(),
    );
    state
}

#[test]
fn an_aggregation_reading_its_source_is_cut_at_a_passed_deadline_and_runs_with_time_left() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    let pipelines = [
        json!([{ "$group": { "_id": "$k", "n": { "$sum": 1 } } }]),
        json!([{ "$sort": { "v": -1 } }]),
        json!([{ "$match": { "k": 1 } }]),
        json!([{ "$lookup": { "from": "items", "localField": "k", "foreignField": "k",
                               "as": "got" } }]),
        json!([{ "$lookup": { "from": "items", "let": { "k": "$k" },
                               "pipeline": [{ "$match": { "$expr": { "$eq": ["$k", "$$k"] } } }],
                               "as": "got" } }]),
        json!([{ "$graphLookup": { "from": "items", "startWith": "$next", "connectFromField":
                                    "next", "connectToField": "_id", "maxDepth": 3,
                                    "as": "g" } }]),
        json!([{ "$facet": { "a": [{ "$limit": 1 }] } }]),
    ];
    for pipeline in &pipelines {
        let late = aggregate_in_a_request(&state, pipeline, PAST);
        assert!(is_the_timeout(&late), "{pipeline}: {late:?}");
        let on_time = aggregate_in_a_request(&state, pipeline, PLENTY);
        assert!(on_time.is_ok(), "{pipeline}: {on_time:?}");
    }
    assert!(!Deadline::is_set(), "no deadline outlives the call that set it");
}

#[test]
fn an_aggregation_with_nothing_to_read_is_still_cut_by_the_deadline_its_stages_run_under() {
    // The leading `$match` names no `_id` (a primary-key read of nothing), so the source
    // has no row to tick on and the stages after it are the first thing to see the
    // deadline: it is set for them, by the aggregation itself.
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    for stage in [
        json!({ "$sort": { "v": 1 } }),
        json!({ "$group": { "_id": null, "n": { "$sum": 1 } } }),
        json!({ "$lookup": { "from": "items", "localField": "k", "foreignField": "k",
                              "as": "got" } }),
    ] {
        let pipeline = json!([{ "$match": { "_id": { "$in": [] } } }, stage]);
        let late = aggregate_in_a_request(&state, &pipeline, PAST);
        assert!(is_the_timeout(&late), "{pipeline}: {late:?}");
        assert!(aggregate_in_a_request(&state, &pipeline, PLENTY).is_ok(), "{pipeline}");
    }
}

#[test]
fn outside_a_request_there_is_no_deadline_and_nothing_is_cut() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    let out = runtime()
        .block_on(aggregate_with_limits(
            &state,
            &root(),
            "app",
            "orders",
            &json!([{ "$sort": { "v": -1 } }]),
            Default::default(),
        ))
        .unwrap();
    assert_eq!(out["count"], 6);
}

#[test]
fn a_write_stage_is_cut_before_it_creates_or_writes_anything() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    for last in [json!({ "$out": "dest" }), json!({ "$merge": { "into": "dest" } })] {
        let pipeline = json!([{ "$match": { "k": 1 } }, last]);
        let name = if pipeline[1].get("$out").is_some() { "$out" } else { "$merge" };
        let stages = parse_pipeline(&pipeline).unwrap();
        let plan = plan_sink(&state, &root(), "app", "orders", &stages).unwrap().unwrap();
        // `write_sink` itself, with the deadline already passed: the pipeline's output is
        // in hand, so this is the stage's own check, not the source's.
        let docs = vec![doc! { "_id": 1, "n": 1 }, doc! { "_id": 2, "n": 2 }];
        let cut = {
            let _deadline = Deadline::scope(Some(PAST));
            write_sink(&state, plan, stages.last().unwrap(), docs)
        };
        assert!(is_the_timeout(&cut), "{name}: {cut:?}");
        assert!(
            state.engine.get_collection("app", "dest").is_err(),
            "{name}: the target was created by a write that was cut"
        );
    }
}

#[test]
fn find_and_count_are_cut_at_a_passed_deadline_and_work_with_time_left() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    let params = || FindParams { filter: Some(json!({ "k": 1 })), ..Default::default() };
    let sorted = || FindParams { sort: Some(json!({ "v": -1 })), ..Default::default() };
    let late = in_a_request(PAST, || find(&state, &root(), "app", "orders", params()));
    assert!(is_the_timeout(&late), "{late:?}");
    let late = in_a_request(PAST, || find(&state, &root(), "app", "orders", sorted()));
    assert!(is_the_timeout(&late), "{late:?}");
    let late = in_a_request(PAST, || count(&state, &root(), "app", "orders", params()));
    assert!(is_the_timeout(&late), "{late:?}");
    let found = in_a_request(PLENTY, || find(&state, &root(), "app", "orders", sorted())).unwrap();
    assert_eq!(found["count"], 6);
    let counted = in_a_request(PLENTY, || count(&state, &root(), "app", "orders", params()));
    assert_eq!(counted.unwrap()["count"], 2);
}

#[test]
fn a_primary_key_read_of_many_ids_is_cut_too() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    let ids: Vec<i64> = (0..50).collect();
    let params =
        FindParams { filter: Some(json!({ "_id": { "$in": ids } })), ..Default::default() };
    let late = in_a_request(PAST, || find(&state, &root(), "app", "orders", params));
    assert!(is_the_timeout(&late), "{late:?}");
}

#[test]
fn update_delete_and_find_and_modify_are_cut_before_they_write() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    let write =
        |multi: bool| WriteParams { filter: Some(json!({ "k": 1 })), multi, ..Default::default() };
    let set = json!({ "$set": { "touched": true } });
    for multi in [false, true] {
        let cut =
            in_a_request(PAST, || update(&state, &root(), "app", "orders", &set, write(multi)));
        assert!(is_the_timeout(&cut), "update multi={multi}: {cut:?}");
        let cut = in_a_request(PAST, || delete(&state, &root(), "app", "orders", write(multi)));
        assert!(is_the_timeout(&cut), "delete multi={multi}: {cut:?}");
    }
    let spec = || FindAndModifySpec {
        filter: Some(json!({ "k": 1 })),
        sort: Some(json!({ "v": 1 })),
        update: Some(set.clone()),
        remove: false,
        upsert: false,
        return_document: ReturnDocument::After,
        projection: None,
        if_stamp: None,
        array_filters: Vec::new(),
    };
    let cut = in_a_request(PAST, || find_and_modify(&state, &root(), "app", "orders", spec()));
    assert!(is_the_timeout(&cut), "find_and_modify: {cut:?}");

    let meta = state.engine.get_collection("app", "orders").unwrap();
    for id in 0..6 {
        let stored = state.engine.get(&meta, &DocId::Int64(id)).unwrap().expect("not deleted");
        assert!(!stored.contains_key("touched"), "{id}: written by a request that was cut");
    }

    // With time left the same requests do their work.
    let done = in_a_request(PLENTY, || update(&state, &root(), "app", "orders", &set, write(true)));
    assert_eq!(done.unwrap()["modified"], 2);
    let done = in_a_request(PLENTY, || delete(&state, &root(), "app", "orders", write(true)));
    assert_eq!(done.unwrap()["deleted"], 2);
}

#[test]
fn the_deadline_a_call_sets_is_removed_when_it_returns_even_by_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    let _ = in_a_request(PAST, || find(&state, &root(), "app", "orders", FindParams::default()));
    assert!(!Deadline::is_set());
    let _ = in_a_request(PAST, || {
        update(
            &state,
            &root(),
            "app",
            "orders",
            &json!({ "$set": { "a": 1 } }),
            WriteParams::default(),
        )
    });
    assert!(!Deadline::is_set());
    // And a call outside any request, on the same thread, is not cut.
    assert!(find(&state, &root(), "app", "orders", FindParams::default()).is_ok());
}
