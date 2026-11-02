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
    // `$limit 0` is refused, as the reference refuses it, so no pipeline reads nothing.
    for pipeline in [json!([{"$limit": 0}]), json!([{"$addFields": {"x": 1}}, {"$limit": 0}])] {
        let err = run(&state, pipeline, aggregate::Limits::default()).unwrap_err();
        assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
        assert!(err.message.contains("$limit must be positive, found 0"), "{}", err.message);
    }
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

#[test]
fn a_pipeline_of_more_than_a_thousand_stages_is_a_400_that_names_the_count_and_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, numbered(3, &[]));
    let skips = |n: usize| Value::Array(vec![json!({"$skip": 0}); n]);
    let ok = run(&state, skips(1_000), aggregate::Limits::default()).unwrap();
    assert_eq!(ok["count"], 3);
    let err = run(&state, skips(1_001), aggregate::Limits::default()).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
    assert!(
        err.message.contains("a pipeline has 1001 stages, over the limit of 1000"),
        "{}",
        err.message
    );
    // A stage the reference expands into two counts as two: a `$count` after 999 is the 1,001st.
    let counted = |n: usize| {
        let mut stages = vec![json!({"$skip": 0}); n];
        stages.push(json!({"$count": "n"}));
        Value::Array(stages)
    };
    let err = run(&state, counted(999), aggregate::Limits::default()).unwrap_err();
    assert!(err.message.contains("a pipeline has 1001 stages"), "{}", err.message);
    let ok = run(&state, counted(998), aggregate::Limits::default()).unwrap();
    assert_eq!(ok["documents"], json!([{"n": 3}]));
}

/// `n` documents `{_id: i, t: i}` under an index on `t`, so a range of `t` is an inexact
/// index range, delivered in `_id` order.
fn indexed(dir: &tempfile::TempDir, n: i32) -> SharedState {
    let state = live_state(dir);
    state.engine.create_collection("app", "orders").unwrap();
    state
        .engine
        .create_index("app", "orders", vec![kimmy_storage::IndexField::ascending("t")], false, None)
        .unwrap();
    let meta = state.engine.get_collection("app", "orders").unwrap();
    let docs: Vec<Document> = (0..n).map(|i| doc! {"_id": i, "t": i}).collect();
    state.engine.insert_many(&meta, docs).unwrap();
    state
}

/// The keys of an index range, as the budget counts one of the `_id`s of `indexed`.
fn key_bytes(keys: usize) -> usize {
    keys * kimmy_query::budget::held_set_key(5)
}

#[test]
fn a_streamed_source_over_an_index_range_is_read_whole_and_never_cut_at_the_ceiling() {
    // The source of a pipeline that streams has no ceiling, and the keys of the range are
    // collected without a count to bound them: all 1,500 are delivered, in order, though the
    // pipeline limit is a thousand. A cut at the limit would answer 1,001.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 1_500);
    let drop_most = json!({"$match": {"_id": {"$gte": 1_490}}});
    let pipeline = json!([{"$match": {"t": {"$gte": 0}}}, {"$project": {"_id": 1}}, drop_most]);
    let (answer, stats) = run_stats(&state, pipeline, with_ceiling(1_000));
    assert_eq!(ids(&answer), (1_490..1_500).collect::<Vec<i64>>());
    assert_eq!((stats.matched, stats.index.as_deref()), (1_500, Some("t_1")));
    assert_eq!(stats.index_entries, Some(1_500));
}

#[test]
fn the_keys_an_unbounded_index_range_collects_are_charged_and_refused_at_the_budget() {
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 1_500);
    let pipeline =
        json!([{"$match": {"t": {"$gte": 0}}}, {"$project": {"_id": 1}}, {"$match": {"_id": 3}}]);
    // Room for 100 keys of the 1,500: refused, naming the lease, near its cap.
    let limits = capped(key_bytes(100));
    let err = run(&state, pipeline.clone(), limits.clone()).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert!(err.message.contains("the index range of $match is "), "{}", err.message);
    let held: usize =
        err.message.split(" is ").nth(1).unwrap().split(' ').next().unwrap().parse().unwrap();
    assert!(held > key_bytes(100) && held <= key_bytes(100) + key_bytes(1), "stopped at {held}");
    assert_eq!(limits.buffer.used(), 0, "gives everything back when refused");
    // Room for all of them, and it is answered, with the keys given back.
    let limits = capped(key_bytes(1_600));
    let ok = run(&state, pipeline, limits.clone()).unwrap();
    assert_eq!(ids(&ok), [3]);
    assert_eq!(limits.buffer.used(), 0);
}

#[test]
fn a_skip_and_a_limit_at_the_head_bound_the_keys_a_range_holds_to_what_they_take() {
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 1_500);
    // The answer is empty, so the budget is the keys' alone. Room for four keys of 1,500:
    // `[$match, $skip 1, $limit 2]` takes three, and the range is read whole to find them,
    // holding the three smallest `_id`s and no more.
    let none = json!({"$match": {"_id": -1}});
    let limits = capped(key_bytes(4));
    let (answer, stats) = run_stats(
        &state,
        json!([{"$match": {"t": {"$gte": 0}}}, {"$skip": 1}, {"$limit": 2}, none.clone()]),
        limits.clone(),
    );
    assert_eq!(ids(&answer), Vec::<i64>::new());
    assert_eq!(stats.matched, 3, "three documents were taken from the range");
    assert_eq!(stats.index_entries, Some(1_500), "a range in `_id` order is read whole");
    assert_eq!(limits.buffer.used(), 0);
    // A `$limit` of one holds one.
    let (_, stats) = run_stats(
        &state,
        json!([{"$match": {"t": {"$gt": 0}}}, {"$limit": 1}, none.clone()]),
        capped(key_bytes(2)),
    );
    assert_eq!(stats.matched, 1);
    // Anything before the limit may need more, and is held to the budget instead.
    let err = run(
        &state,
        json!([{"$match": {"t": {"$gte": 0}}}, {"$project": {"_id": 1}}, {"$limit": 1}, none]),
        capped(key_bytes(4)),
    )
    .unwrap_err();
    assert!(err.message.contains("the index range of $match"), "{}", err.message);
}

/// A document of about a megabyte, with `levels` arrays of two numbers, `a0` to `a<levels-1>`.
fn deep(levels: usize) -> Document {
    let mut doc = doc! {"_id": 1, "pad": "x".repeat(1 << 20)};
    for level in 0..levels {
        doc.insert(format!("a{level}"), vec![1, 2]);
    }
    doc
}

#[test]
fn unwinds_that_do_not_fan_out_hold_nothing_however_many_there_are() {
    // The shape of the reviewer's pipeline: a big document, then a thousand `$unwind`s of a
    // path it does not have, keeping it, then a limit. Each hands the document on; main
    // answered it in the size of two documents, and so does this.
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, vec![deep(0)]);
    let one = held_docs_bytes(&[deep(0)]);
    let mut pipeline: Vec<Value> = vec![json!({"$addFields": {"big": "$pad"}})];
    pipeline.extend(
        (0..998).map(|_| json!({"$unwind": {"path": "$none", "preserveNullAndEmptyArrays": true}})),
    );
    pipeline.push(json!({"$limit": 1}));
    // Room for the document built, and the answer: documents and JSON, a few times over.
    let limits = capped(8 * one);
    let ok = run(&state, Value::Array(pipeline), limits.clone()).unwrap();
    assert_eq!(ok["count"], 1);
    assert_eq!(limits.buffer.used(), 0);
}

#[test]
fn nested_unwinds_over_a_large_document_are_refused_at_the_budget() {
    // Fifty arrays, fifty `$unwind`s: each level of the descent holds the document it
    // unwinds, so the descent is held to the budget at the level that passes it.
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, vec![deep(50)]);
    let one = held_docs_bytes(&[deep(0)]);
    let mut pipeline: Vec<Value> =
        (0..50).map(|level| json!({"$unwind": format!("$a{level}")})).collect();
    pipeline.push(json!({"$limit": 1}));
    let limits = capped(10 * one);
    let err = run(&state, Value::Array(pipeline.clone()), limits.clone()).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert!(err.message.contains("the input of $unwind is "), "{}", err.message);
    let held: usize =
        err.message.split(" is ").nth(1).unwrap().split(' ').next().unwrap().parse().unwrap();
    assert!(held > 10 * one && held <= 12 * one, "stopped at {held} of {}", 10 * one);
    assert_eq!(limits.buffer.used(), 0);
    // Room for the descent, and it answers one document.
    let limits = capped(70 * one);
    let ok = run(&state, Value::Array(pipeline), limits.clone()).unwrap();
    assert_eq!(ok["count"], 1);
    assert_eq!(limits.buffer.used(), 0);
}

/// The smallest budget (in bytes) a pipeline over `orders` is answered under, found by halving
/// between nothing and `hi`, and the refusal one byte under it.
fn smallest_budget(state: &SharedState, pipeline: &Value, mut hi: usize) -> (usize, ApiError) {
    let mut lo = 0usize;
    assert!(run(state, pipeline.clone(), capped(hi)).is_ok(), "must run under {hi}");
    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        if run(state, pipeline.clone(), capped(mid)).is_ok() {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    (hi, run(state, pipeline.clone(), capped(hi - 1)).unwrap_err())
}

#[test]
fn what_a_stage_that_blocks_hands_to_a_stage_that_streams_is_held_while_it_drains() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, padded(20, 10_000));
    let input = held_docs_bytes(&padded(20, 10_000));
    // The `$sort` holds its input; its output is the same documents, which the second
    // `$project` takes as its input and the `$group` collects as it gets them. Each is given
    // back as it is taken, so the documents once is held, and not twice as they drain
    // beside what has arrived.
    let pipeline = json!([
        {"$project": {"pad": 1}},
        {"$sort": {"_id": -1}},
        {"$project": {"pad": 1}},
        {"$group": {"_id": null, "n": {"$sum": 1}}},
    ]);
    let (least, err) = smallest_budget(&state, &pipeline, 4 * input);
    assert!(least > input && least < 2 * input, "answered under {least}, documents {input}");
    // What is full is the `$group`, which holds its input (the documents once) and its
    // buckets: the second `$project` held nothing of what it had taken.
    assert!(
        err.message.contains("the buckets of $group (with its input, still held) is "),
        "{}",
        err.message
    );
}
