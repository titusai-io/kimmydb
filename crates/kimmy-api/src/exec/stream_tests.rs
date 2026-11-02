//! What an aggregation whose first stage streams does through the executor (ADR-270): the
//! scan ends when a `$limit` is full, the stages that stream hold nothing, a stage that
//! blocks is charged as the documents arrive to it, and the source of such a pipeline has no
//! ceiling of its own.

use bson::{Document, doc};
use serde_json::{Value, json};

use super::tests::{agg_limits, live_state, seed_docs};
use super::*;
use kimmy_query::budget::{BufferBudget, held_docs_bytes};

fn root() -> Auth {
    Auth(kimmy_auth::Principal::superuser("test"))
}

fn capped(cap: usize) -> aggregate::Limits {
    aggregate::Limits { buffer: BufferBudget::new(cap), ..Default::default() }
}

fn with_ceiling(max_documents: usize) -> aggregate::Limits {
    aggregate::Limits { max_documents, ..Default::default() }
}

/// `n` documents `{_id: i, k: i % 5, d: 1}`, with `d: 0` where `zero` says.
fn numbered(n: i64, zero: &[i64]) -> Vec<Document> {
    (0..n).map(|i| doc! {"_id": i, "k": i % 5, "d": i64::from(!zero.contains(&i))}).collect()
}

fn padded(n: i32, pad: usize) -> Vec<Document> {
    (0..n).map(|i| doc! {"_id": i, "pad": "x".repeat(pad)}).collect()
}

fn state_with(dir: &tempfile::TempDir, docs: Vec<Document>) -> SharedState {
    let state = live_state(dir);
    seed_docs(&state, "orders", docs);
    state
}

fn run(state: &SharedState, pipeline: Value, limits: aggregate::Limits) -> Result<Value, ApiError> {
    agg_limits(state, &root(), "app", "orders", &pipeline, limits)
}

/// The answer and how the source was read.
fn run_stats(
    state: &SharedState,
    pipeline: Value,
    limits: aggregate::Limits,
) -> (Value, QueryStats) {
    let (answer, stats) = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(aggregate_run_stats(state, &root(), "app", "orders", &pipeline, limits))
        .unwrap();
    (answer, stats.expect("a filtered source"))
}

fn ids(answer: &Value) -> Vec<i64> {
    answer["documents"].as_array().unwrap().iter().filter_map(|d| d["_id"].as_i64()).collect()
}

#[test]
fn a_full_limit_ends_the_scan_and_the_stats_say_how_far_it_read() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, numbered(50, &[]));
    let read = |pipeline: Value| {
        let (answer, stats) = run_stats(&state, pipeline, aggregate::Limits::default());
        (ids(&answer), stats.examined, stats.matched)
    };
    // The collection is read as far as the limit needs and no further.
    assert_eq!(read(json!([{"$limit": 3}])), (vec![0, 1, 2], 3, 3));
    assert_eq!(read(json!([{"$project": {"k": 1}}, {"$limit": 3}])), (vec![0, 1, 2], 3, 3));
    assert_eq!(read(json!([{"$skip": 2}, {"$limit": 3}])), (vec![2, 3, 4], 5, 5));
    // The documents a `$match` drops on the way are read too.
    assert_eq!(
        read(json!([{"$addFields": {"x": 1}}, {"$match": {"k": 4}}, {"$limit": 2}])),
        (vec![4, 9], 10, 10)
    );
    // A leading `$match` is the source's filter: the limit ends that read.
    assert_eq!(read(json!([{"$match": {"k": 4}}, {"$limit": 2}])), (vec![4, 9], 10, 2));
    // A `$limit` nothing blocks before it; one behind a stage that blocks reads everything.
    assert_eq!(read(json!([{"$sort": {"_id": -1}}, {"$limit": 2}])), (vec![49, 48], 50, 50));
    // A limit that never fills reads the whole collection.
    assert_eq!(read(json!([{"$limit": 500}])).1, 50);
    // `$limit 0` reads no document.
    assert_eq!(read(json!([{"$limit": 0}])), (vec![], 0, 0));
    assert_eq!(read(json!([{"$addFields": {"x": 1}}, {"$limit": 0}, {"$count": "n"}])).1, 0);
}

#[test]
fn an_error_in_a_document_past_a_full_limit_is_not_raised() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, numbered(40, &[20]));
    let divide = json!({"$addFields": {"x": {"$divide": [1, "$d"]}}});
    let ok = run(&state, json!([divide.clone(), {"$limit": 5}]), aggregate::Limits::default());
    assert_eq!(ids(&ok.unwrap()), [0, 1, 2, 3, 4]);
    // After a stage that blocks, the documents come from its output and stop at the limit too.
    let ok = run(
        &state,
        json!([{"$sort": {"_id": 1}}, divide.clone(), {"$limit": 5}]),
        aggregate::Limits::default(),
    );
    assert_eq!(ids(&ok.unwrap()), [0, 1, 2, 3, 4]);
    // The same stage with the failing document inside the window fails, as it always did.
    let err = run(&state, json!([divide.clone(), {"$limit": 25}]), aggregate::Limits::default())
        .unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
    assert!(err.message.contains("$divide"), "{}", err.message);
    // And a stage after the limit sees only what the limit let through, but fails on it.
    let err =
        run(&state, json!([{"$skip": 20}, {"$limit": 3}, divide]), aggregate::Limits::default())
            .unwrap_err();
    assert!(err.message.contains("$divide"), "{}", err.message);
}

#[test]
fn the_source_of_a_pipeline_that_streams_has_no_ceiling_and_one_that_blocks_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, numbered(30, &[]));
    // Thirty documents through stages that hold none, with the pipeline limit at ten.
    let drop_all = json!({"$match": {"k": {"$gt": 100}}});
    let ok = run(&state, json!([{"$addFields": {"x": 1}}, drop_all.clone()]), with_ceiling(10));
    assert_eq!(ok.unwrap()["count"], 0);
    let ok = run(&state, json!([{"$limit": 8}, {"$unwind": "$k"}]), with_ceiling(10));
    assert_eq!(ok.unwrap()["count"], 8);
    // The answer holds its documents, and is held to the limit: refused at the eleventh.
    let err = run(&state, json!([{"$addFields": {"x": 1}}]), with_ceiling(10)).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
    assert!(
        err.message.contains(
            "$addFields produced 11 documents, over the pipeline limit of 10. Narrow the \
             pipeline with an earlier $match"
        ),
        "{}",
        err.message
    );
    // So is a stage that blocks, at the document that passes the limit and not before.
    let err = run(&state, json!([{"$addFields": {"x": 1}}, {"$count": "n"}]), with_ceiling(10))
        .unwrap_err();
    assert!(err.message.contains("$addFields produced 11 documents"), "{}", err.message);
    // A pipeline that begins with a stage that blocks, or with none, reads its source whole
    // and refuses it as it always did.
    for pipeline in [json!([]), json!([{"$sort": {"_id": 1}}, {"$addFields": {"x": 1}}])] {
        let err = run(&state, pipeline.clone(), with_ceiling(10)).unwrap_err();
        assert!(
            err.message.contains(
                "the source collection admits more than 10 documents, the pipeline limit. Narrow \
                 it with a more selective $match"
            ),
            "{pipeline}: {}",
            err.message
        );
    }
    // And a leading `$match` that admits more is named as before.
    let err = run(&state, json!([{"$match": {"k": {"$gte": 0}}}]), with_ceiling(10)).unwrap_err();
    assert!(err.message.contains("the leading $match admits more than 10"), "{}", err.message);
    let err = run(
        &state,
        json!([{"$match": {"k": {"$gte": 0}}}, {"$sort": {"_id": 1}}]),
        with_ceiling(10),
    )
    .unwrap_err();
    assert!(err.message.contains("the leading $match admits more than 10"), "{}", err.message);
}

#[test]
fn a_pipeline_that_streams_holds_nothing_but_its_answer() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, padded(40, 10_000));
    let all = held_docs_bytes(&padded(40, 10_000));
    // The collection is eight times the budget: read whole, it is refused at the source.
    let cap = all / 8;
    let err = run(&state, json!([]), capped(cap)).unwrap_err();
    assert!(err.message.contains("the source read"), "{}", err.message);
    // Streamed through stages that keep a few documents, it is answered.
    for pipeline in [
        json!([{"$addFields": {"c": "$pad"}}, {"$match": {"_id": 3}}]),
        json!([{"$project": {"_id": 1}}]),
        json!([{"$limit": 2}]),
        json!([{"$skip": 38}, {"$replaceRoot": {"newRoot": "$$ROOT"}}]),
        json!([{"$unwind": "$pad"}, {"$match": {"_id": {"$lt": 2}}}]),
    ] {
        let limits = capped(cap);
        let ok = run(&state, pipeline.clone(), limits.clone());
        assert!(ok.is_ok(), "{pipeline}: {ok:?}");
        assert_eq!(limits.buffer.used(), 0, "{pipeline}");
    }
}

#[test]
fn a_stage_that_blocks_is_charged_as_the_documents_stream_to_it() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, padded(40, 10_000));
    let all = held_docs_bytes(&padded(40, 10_000));
    // 40 documents are about 400 KB. The `$sort` is refused a few documents past 50 KB, at the
    // document that takes its input over the limit, not after it has been given them all.
    let limits = capped(50_000);
    let err = run(&state, json!([{"$addFields": {"c": 1}}, {"$sort": {"_id": 1}}]), limits.clone())
        .unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert!(err.message.contains("the input of $sort is "), "{}", err.message);
    let held: usize =
        err.message.split(" is ").nth(1).unwrap().split(' ').next().unwrap().parse().unwrap();
    assert!((50_000..70_000).contains(&held), "stopped at {held}, not after the whole 400 KB");
    assert_eq!(limits.buffer.used(), 0, "gave back what it had");
    // The same for a `$group`, which charges its input as a stage does.
    let err = run(
        &state,
        json!([{"$project": {"pad": 1}}, {"$group": {"_id": null, "n": {"$sum": 1}}}]),
        capped(50_000),
    )
    .unwrap_err();
    assert!(err.message.contains("the input of $group is "), "{}", err.message);
    // What arrived is the stage's input and is not charged again when it runs: a budget of
    // the input and a little over holds a `$sort` and a `$count`, where twice the input would
    // be needed if the stage charged it as its own as well.
    let ok = run(
        &state,
        json!([{"$project": {"pad": 1}}, {"$sort": {"_id": 1}}, {"$count": "n"}]),
        capped(all + 10_000),
    )
    .unwrap();
    assert_eq!(ok["documents"], json!([{"n": 40}]));
    // And one byte of a budget that cannot hold it is refused at the stage.
    let err = run(
        &state,
        json!([{"$project": {"pad": 1}}, {"$sort": {"_id": 1}}, {"$count": "n"}]),
        capped(all / 2),
    )
    .unwrap_err();
    assert!(err.message.contains("the input of $sort"), "{}", err.message);
}

#[test]
fn the_answer_of_a_pipeline_that_streams_is_charged_as_it_arrives() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, padded(40, 10_000));
    let limits = capped(50_000);
    let err = run(&state, json!([{"$addFields": {"c": 1}}]), limits.clone()).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert!(err.message.contains("the output of the pipeline is "), "{}", err.message);
    let held: usize =
        err.message.split(" is ").nth(1).unwrap().split(' ').next().unwrap().parse().unwrap();
    assert!((50_000..70_000).contains(&held), "stopped at {held}, not after the whole 400 KB");
    assert_eq!(limits.buffer.used(), 0);
}

#[test]
fn a_streamed_pipeline_ending_in_a_sink_writes_what_the_stages_left() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, numbered(30, &[]));
    let ok = run(
        &state,
        json!([{"$addFields": {"x": 1}}, {"$match": {"k": 2}}, {"$limit": 4}, {"$out": "copy"}]),
        aggregate::Limits::default(),
    )
    .unwrap();
    assert_eq!(ok["out"]["inserted"], 4);
    let copied = run_on(&state, "copy", json!([{"$project": {"_id": 1}}]));
    assert_eq!(ids(&copied), [2, 7, 12, 17]);
}

fn run_on(state: &SharedState, coll: &str, pipeline: Value) -> Value {
    agg_limits(state, &root(), "app", coll, &pipeline, aggregate::Limits::default()).unwrap()
}
