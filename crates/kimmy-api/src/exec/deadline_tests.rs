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
        // Stages that stream take the scan into the same scope (ADR-270).
        json!([{ "$addFields": { "w": 1 } }, { "$limit": 5 }]),
        json!([{ "$project": { "v": 1 } }]),
        json!([{ "$unwind": "$tags" }, { "$match": { "tags": "a" } }]),
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
fn the_stages_after_the_source_run_under_the_deadline_the_aggregation_sets() {
    // `finish_pipeline` is called with the documents in hand, so no read of the source
    // can be the thing that sees the deadline: the scope the aggregation sets for what
    // follows its source is, and nothing else here sets one.
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    for stage in [
        json!({ "$sort": { "v": 1 } }),
        json!({ "$group": { "_id": null, "n": { "$sum": 1 } } }),
        json!({ "$lookup": { "from": "items", "localField": "k", "foreignField": "k",
                              "as": "got" } }),
        json!({ "$unionWith": "absent" }),
    ] {
        let stages = parse_pipeline(&json!([stage])).unwrap();
        // No input document for `$unionWith`: with none, no loop after the stage has a row
        // to tick on (the answer is built from what it appended), so only the stage's own
        // looks at the deadline can cut it.
        let docs = if stage.get("$unionWith").is_some() {
            Vec::new()
        } else {
            vec![doc! { "_id": 1, "k": 1, "v": 1 }]
        };
        let run = |left: Duration| {
            in_a_request(left, || {
                finish_pipeline(
                    &state,
                    &root(),
                    "app",
                    &stages,
                    docs.clone(),
                    &aggregate::Limits::default(),
                    None,
                    None,
                    Shape::Value,
                )
                .map(|(answer, _, _)| match answer {
                    Answer::Json(body) => body,
                    Answer::Body(_) => unreachable!("Shape::Value answers a value"),
                })
            })
        };
        let late = run(PAST);
        assert!(is_the_timeout(&late), "{stage}: {late:?}");
        assert!(run(PLENTY).is_ok(), "{stage}");
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
        // An empty output has no row to tick on, so only the look immediately before the
        // target is created can cut it: it is cut there, and creates nothing.
        let plan = plan_sink(&state, &root(), "app", "orders", &stages).unwrap().unwrap();
        let cut = {
            let _deadline = Deadline::scope(Some(PAST));
            write_sink(&state, plan, stages.last().unwrap(), Vec::new())
        };
        assert!(is_the_timeout(&cut), "{name}, empty: {cut:?}");
        assert!(state.engine.get_collection("app", "dest").is_err(), "{name}, empty");
    }
}

#[test]
fn a_write_stage_that_is_not_cut_before_its_target_exists_goes_to_its_end() {
    // The deadline is set aside from the look before the target is created: a write that
    // has begun is not cut (ADR-192, ADR-266). Here the deadline has not passed, and the
    // stage writes whole, leaving the deadline as the caller had it.
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    let stages = parse_pipeline(&json!([{ "$out": "dest" }])).unwrap();
    let plan = plan_sink(&state, &root(), "app", "orders", &stages).unwrap().unwrap();
    let docs = vec![doc! { "_id": 1 }, doc! { "_id": 2 }];
    let _deadline = Deadline::scope(Some(PLENTY));
    let written = write_sink(&state, plan, stages.last().unwrap(), docs).unwrap();
    assert_eq!(written["out"]["inserted"], 2);
    assert!(Deadline::is_set(), "the caller's deadline is back");
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
fn describe_collection_is_cut_at_a_passed_deadline_and_works_with_time_left() {
    // The sample and the document count both walk the collection (review q8d3, m2).
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    let describe =
        || crate::schema::describe_collection(&state, &root(), "app", "orders", None, false);
    let late = in_a_request(PAST, describe);
    assert!(is_the_timeout(&late), "{late:?}");
    let body = in_a_request(PLENTY, describe).unwrap();
    assert_eq!(body["documentCount"], 6);
    assert!(!Deadline::is_set(), "no deadline outlives the call that set it");
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

#[test]
fn union_with_is_cut_at_a_passed_deadline_with_its_collection_present_or_missing() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    for from in ["items", "absent"] {
        // A leading `$match` in the sub-pipeline is applied as the foreign side is read.
        for sub in [json!([]), json!([{ "$match": { "k": 1 } }, { "$limit": 5 }])] {
            let pipeline = json!([{ "$match": { "_id": { "$in": [] } } },
                                  { "$unionWith": { "coll": from, "pipeline": sub } }]);
            let late = aggregate_in_a_request(&state, &pipeline, PAST);
            assert!(is_the_timeout(&late), "{pipeline}: {late:?}");
            let on_time = aggregate_in_a_request(&state, &pipeline, PLENTY);
            assert!(on_time.is_ok(), "{pipeline}: {on_time:?}");
        }
    }
    // With the source read too.
    let pipeline = json!([{ "$unionWith": "items" }, { "$sample": { "size": 3 } },
                          { "$redact": "$$DESCEND" }, { "$sortByCount": "$k" }]);
    assert!(is_the_timeout(&aggregate_in_a_request(&state, &pipeline, PAST)));
    assert!(aggregate_in_a_request(&state, &pipeline, PLENTY).is_ok());
}

#[test]
fn a_pipeline_update_is_cut_before_it_writes_and_runs_with_time_left() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    let set = json!([{ "$set": { "touched": { "$add": ["$v", 1] } } }]);
    let params =
        |multi: bool| WriteParams { filter: Some(json!({ "k": 1 })), multi, ..Default::default() };
    for multi in [false, true] {
        let cut =
            in_a_request(PAST, || update(&state, &root(), "app", "orders", &set, params(multi)));
        assert!(is_the_timeout(&cut), "multi={multi}: {cut:?}");
    }
    let spec = FindAndModifySpec {
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
    let cut = in_a_request(PAST, || find_and_modify(&state, &root(), "app", "orders", spec));
    assert!(is_the_timeout(&cut), "find_and_modify: {cut:?}");
    let meta = state.engine.get_collection("app", "orders").unwrap();
    for id in 0..6 {
        let stored = state.engine.get(&meta, &DocId::Int64(id)).unwrap().unwrap();
        assert!(!stored.contains_key("touched"), "{id}: written by a request that was cut");
    }
    let done =
        in_a_request(PLENTY, || update(&state, &root(), "app", "orders", &set, params(true)));
    assert_eq!(done.unwrap()["modified"], 2);
    let stored = state.engine.get(&meta, &DocId::Int64(1)).unwrap().unwrap();
    assert_eq!(stored.get_i64("touched").unwrap(), 2);
}

#[test]
fn a_find_and_modify_that_has_committed_answers_whatever_the_deadline_says() {
    // ADR-192: after the first commit nothing may answer `503 timeout`. A positional
    // projection re-evaluates the filter for the answer, and an `$expr` over an array
    // ticks the deadline, which passes between the commit and the answer here.
    let dir = tempfile::tempdir().unwrap();
    let state = live_state(&dir);
    seed_docs(
        &state,
        "orders",
        vec![doc! { "_id": 1, "n": 0, "items": [{ "q": 0 }, { "q": 5 }, { "q": 7 }] }],
    );
    let filter = json!({
        "items.q": { "$gt": 2 },
        "$expr": { "$gt": [{ "$size": { "$filter": {
            "input": "$items", "as": "i", "cond": { "$gt": ["$$i.q", 0] } } } }, 0] },
    });
    let spec = || FindAndModifySpec {
        filter: Some(filter.clone()),
        sort: None,
        update: Some(json!({ "$inc": { "n": 1 } })),
        remove: false,
        upsert: false,
        return_document: ReturnDocument::After,
        projection: Some(json!({ "items.$": 1 })),
        if_stamp: None,
        array_filters: Vec::new(),
    };
    AFTER_ENGINE.set(Some(Box::new(|| {
        std::mem::forget(Deadline::scope(Some(PAST)));
    })));
    let answered =
        in_a_request(PLENTY, || find_and_modify(&state, &root(), "app", "orders", spec()));
    let body = answered.expect("a committed write is answered, not timed out");
    assert_eq!(body["modified"], 1);
    assert_eq!(body["document"]["items"], json!([{ "q": 5 }]));
    let meta = state.engine.get_collection("app", "orders").unwrap();
    let stored = state.engine.get(&meta, &DocId::Int64(1)).unwrap().unwrap();
    assert_eq!(stored.get_i32("n").unwrap(), 1, "what the answer says landed");
}

// ---- Loops that tick, held by their count of looks (review q8d3, m5) ----

/// The looks at the clock `work` makes when every tick reads it: its ticks and checks, exactly.
fn looks<T>(work: impl FnOnce() -> T) -> u64 {
    kimmy_core::deadline::looks_of(work).1
}

/// A state with `orders` orders `{_id, k, v, next}` and `items` items `{_id, k, next}` that
/// chain to the following item.
fn sized_state(dir: &tempfile::TempDir, orders: i64, items: i64) -> SharedState {
    let state = live_state(dir);
    seed_docs(
        &state,
        "orders",
        (0..orders).map(|i| doc! { "_id": i, "k": i % 3, "v": i, "next": 0 }).collect(),
    );
    seed_docs(
        &state,
        "items",
        (0..items)
            .map(|i| doc! { "_id": i, "k": i % 3, "next": if i + 1 < items { Some(i + 1) } else { None } })
            .collect(),
    );
    state
}

fn aggregate_looks(state: &SharedState, pipeline: &Value) -> u64 {
    looks(|| aggregate_in_a_request(state, pipeline, PLENTY).unwrap())
}

#[test]
fn a_lookup_ticks_in_its_walk_its_attach_loop_its_copy_loop_and_the_answer() {
    // Four looks for each input document: the source walk's row, the loop that finds what each
    // document joins, the loop that attaches it, and the loop that builds the answer.
    let (small, big) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (a, b) = (sized_state(&small, 10, 5), sized_state(&big, 50, 5));
    let equality = json!([{ "$lookup": { "from": "items", "localField": "k",
                                          "foreignField": "k", "as": "got" } }]);
    let diff = aggregate_looks(&b, &equality) - aggregate_looks(&a, &equality);
    assert_eq!(diff, 4 * 40, "{diff} looks for forty more orders");
}

#[test]
fn a_lookup_with_a_pipeline_ticks_for_each_input_document_it_runs_the_pipeline_for() {
    // Three looks for each input document: the source walk's row, the pass over the foreign
    // side that the document costs, and the answer.
    let (small, big) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (a, b) = (sized_state(&small, 10, 5), sized_state(&big, 50, 5));
    let piped = json!([{ "$lookup": { "from": "items", "let": { "k": "$k" },
        "pipeline": [{ "$match": { "$expr": { "$eq": ["$k", "$$k"] } } }], "as": "got" } }]);
    let diff = aggregate_looks(&b, &piped) - aggregate_looks(&a, &piped);
    assert_eq!(diff, 3 * 40, "{diff} looks for forty more orders");
}

#[test]
fn a_union_with_ticks_for_each_document_it_reads_measures_and_answers() {
    // Four looks for each document appended: the foreign walk's row, the loop that measures
    // what is appended, and the two that carry it on to the answer.
    let (small, big) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (a, b) = (sized_state(&small, 1, 10), sized_state(&big, 1, 50));
    let union = json!([{ "$unionWith": "items" }]);
    let diff = aggregate_looks(&b, &union) - aggregate_looks(&a, &union);
    assert_eq!(diff, 4 * 40, "{diff} looks for forty more documents");
}

#[test]
fn a_graph_lookup_ticks_for_each_foreign_document_and_each_step_of_the_walk() {
    let (small, big) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (a, b) = (sized_state(&small, 1, 10), sized_state(&big, 1, 50));
    let graph = |depth: i64| {
        json!([{ "$graphLookup": { "from": "items", "startWith": "$next",
            "connectFromField": "next", "connectToField": "_id", "maxDepth": depth, "as": "g" } }])
    };
    // No steps beyond the first: what grows is the foreign side, whose row the walk ticks and
    // whose entry in the table is ticked again.
    let diff = aggregate_looks(&b, &graph(0)) - aggregate_looks(&a, &graph(0));
    assert_eq!(diff, 2 * 40, "{diff} looks for forty more foreign documents");
    // The same foreign side, a longer walk down a chain: each step is one look, and each
    // document it reaches is another.
    let diff = aggregate_looks(&b, &graph(40)) - aggregate_looks(&b, &graph(10));
    assert_eq!(diff, 2 * 30, "{diff} looks for thirty more steps");
}

#[test]
fn the_answer_of_an_aggregation_ticks_for_each_document() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    let reads = |n: i64| {
        let docs: Vec<bson::Document> = (0..n).map(|i| doc! { "_id": i }).collect();
        looks(|| {
            finish_pipeline(
                &state,
                &root(),
                "app",
                &[],
                docs,
                &aggregate::Limits::default(),
                None,
                None,
                Shape::Value,
            )
            .unwrap()
        })
    };
    let diff = reads(70) - reads(10);
    assert_eq!(diff, 60, "{diff} looks for sixty more documents");
}

#[test]
fn a_write_stage_sets_the_deadline_aside_for_the_whole_of_its_write() {
    // The checks of the output's `_id`s before the target is created tick for each row (two
    // looks each); from the look before the target is created no tick of the write reads the
    // clock, so the write's own checks and chunks add none. Without the set-aside they do.
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    let mut next = 0;
    let mut reads = |n: i64| {
        next += 1;
        let stages = parse_pipeline(&json!([{ "$out": format!("dest{next}") }])).unwrap();
        let plan = plan_sink(&state, &root(), "app", "orders", &stages).unwrap().unwrap();
        let docs: Vec<bson::Document> = (0..n).map(|i| doc! { "_id": i }).collect();
        looks(|| write_sink(&state, plan, stages.last().unwrap(), docs).unwrap())
    };
    let (few, many) = (reads(5), reads(60));
    assert_eq!(many - few, 2 * 55, "{few} looks for 5 documents, {many} for 60");
}

#[test]
fn violations_is_cut_at_a_passed_deadline_and_runs_with_time_left() {
    // Two members' documents that collide on a unique key: the report reads the records of the
    // collision, a walk that ticks the request's deadline.
    use kimmy_core::{DocId, Hlc, NodeId, OpKind, OplogEntry, Stamp};
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    state.engine.create_collection("app", "people").unwrap();
    state
        .engine
        .create_index(
            "app",
            "people",
            vec![kimmy_storage::meta::IndexField { path: "email".into(), descending: false }],
            true,
            None,
        )
        .unwrap();
    let meta = state.engine.get_collection("app", "people").unwrap();
    state.engine.insert(&meta, doc! { "_id": "local", "email": "clash@x" }).unwrap();
    let remote = OplogEntry {
        stamp: Stamp::new(Hlc::new(kimmy_storage::physical_now_ms(), 0), NodeId::generate()),
        kind: OpKind::Insert,
        collection: meta.id,
        doc_id: Some(DocId::String("remote".into())),
        body: Some(bson::serialize_to_vec(&doc! { "_id": "remote", "email": "clash@x" }).unwrap()),
    };
    state.engine.apply_remote(&meta, &remote).unwrap();

    let report = || violations(&state, &root(), "app", "people", None);
    let late = in_a_request(PAST, report);
    assert!(is_the_timeout(&late), "{late:?}");
    let found = in_a_request(PLENTY, report).unwrap();
    assert_eq!(found["count"], 1, "{found}");
    assert!(!Deadline::is_set());
}

#[test]
fn an_upsert_with_no_time_left_inserts_nothing_and_answers_503_in_both_forms() {
    // The upsert of a primary key that is not there examines no document, so no tick of a scan
    // answers: the look before the engine's transaction does, for an operator update (which
    // would otherwise insert and answer `200`) and for a pipeline update (whose seed is cut
    // by the deadline, and which answered `400 invalid_update` when only the engine looked).
    let dir = tempfile::tempdir().unwrap();
    let state = state_with_data(&dir);
    let meta = state.engine.get_collection("app", "orders").unwrap();
    let spec = |update: Value, id: i64| FindAndModifySpec {
        filter: Some(json!({ "_id": id })),
        sort: None,
        update: Some(update),
        remove: false,
        upsert: true,
        return_document: ReturnDocument::After,
        projection: None,
        if_stamp: None,
        array_filters: Vec::new(),
    };
    for (n, update) in
        [json!({ "$set": { "a": 1 } }), json!([{ "$set": { "a": { "$add": [1, 1] } } }])]
            .into_iter()
            .enumerate()
    {
        let id = 900 + n as i64;
        let cut = in_a_request(PAST, || {
            find_and_modify(&state, &root(), "app", "orders", spec(update.clone(), id))
        });
        assert!(is_the_timeout(&cut), "{update}: {cut:?}");
        assert!(
            state.engine.get(&meta, &DocId::Int64(id)).unwrap().is_none(),
            "{update}: inserted by a request with no time left"
        );
        // With time left it inserts.
        let done = in_a_request(PLENTY, || {
            find_and_modify(&state, &root(), "app", "orders", spec(update.clone(), id))
        })
        .unwrap();
        assert_eq!(done["upserted"], true, "{update}: {done}");
        assert!(state.engine.get(&meta, &DocId::Int64(id)).unwrap().is_some());
    }
}

#[test]
fn an_index_range_collected_in_id_order_ticks_for_each_entry_it_reads() {
    // `[$match range, $limit 1]` offers one document, but a range delivered in `_id` order is
    // read whole to find its smallest key (ADR-270). The walk of the index looks at the
    // deadline for each entry, so the collecting of the keys is cut by it, and a passed
    // deadline ends it before the first document.
    let state_of = |dir: &tempfile::TempDir, n: i64| {
        let state = live_state(dir);
        state.engine.create_collection("app", "orders").unwrap();
        state
            .engine
            .create_index(
                "app",
                "orders",
                vec![kimmy_storage::IndexField::ascending("t")],
                false,
                None,
            )
            .unwrap();
        let meta = state.engine.get_collection("app", "orders").unwrap();
        state
            .engine
            .insert_many(&meta, (0..n).map(|i| doc! { "_id": i, "t": i }).collect())
            .unwrap();
        state
    };
    let (small, big) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (a, b) = (state_of(&small, 100), state_of(&big, 1_100));
    let pipeline = json!([{ "$match": { "t": { "$gte": 0 } } }, { "$limit": 1 }]);
    let diff = aggregate_looks(&b, &pipeline) - aggregate_looks(&a, &pipeline);
    assert_eq!(diff, 1_000, "{diff} looks for a thousand more entries");
    let late = aggregate_in_a_request(&b, &pipeline, PAST);
    assert!(is_the_timeout(&late), "{late:?}");
}
