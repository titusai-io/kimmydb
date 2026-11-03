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

/// `div` is a `$divide` by the document's `d`, which is 0 in the document that fails.
fn div() -> Value {
    json!({"$divide": [1, "$d"]})
}

#[test]
fn a_skip_and_a_limit_move_ahead_of_the_stages_the_references_optimizer_moves_them_ahead_of() {
    // The reference's rows (`adr270-reorder.js`, `adr270-reorder2.js`): 20 documents, the
    // failing one is number 3, and a `$skip: 5` and a `$limit: 2` that drop it answer `[5, 6]`.
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, numbered(20, &[3]));
    let window = || vec![json!({"$skip": 5}), json!({"$limit": 2})];
    let pipeline = |stages: Vec<Value>| {
        let mut stages = stages;
        stages.extend(window());
        Value::Array(stages)
    };
    let answers = |stages: Vec<Value>| {
        ids(&run(&state, pipeline(stages), aggregate::Limits::default()).unwrap())
    };
    let fails = |stages: Vec<Value>| {
        let err = run(&state, pipeline(stages), aggregate::Limits::default()).unwrap_err();
        assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST, "{}", err.message);
        assert!(err.message.contains("$divide"), "{}", err.message);
    };
    // The stages a window moves ahead of.
    let merged = json!({"$mergeObjects": ["$$ROOT", {"x": div()}]});
    for stage in [
        json!({"$addFields": {"x": div()}}),
        json!({"$set": {"x": div()}}),
        json!({"$project": {"x": div()}}),
        json!({"$replaceRoot": {"newRoot": merged.clone()}}),
        json!({"$replaceWith": merged}),
        json!({"$lookup": {"from": "orders", "let": {"d": "$d"}, "pipeline": [
            {"$match": {"$expr": {"$gt": [{"$divide": [1, "$$d"]}, 0]}}}, {"$limit": 1}],
            "as": "l"}}),
    ] {
        assert_eq!(answers(vec![stage.clone()]), [5, 6], "{stage}");
    }
    // The stages it does not: the window stays behind them, and the failure is raised.
    let redact = json!({"$redact": {"$cond": [{"$gt": [div(), 0]}, "$$KEEP", "$$PRUNE"]}});
    let filter = json!({"$match": {"$expr": {"$gt": [div(), 0]}}});
    let group = json!({"$group": {"_id": "$k", "x": {"$first": div()}}});
    let windows =
        json!({"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"x": {"$sum": div()}}}});
    let graph = json!({"$graphLookup": {"from": "orders", "startWith": div(),
        "connectFromField": "k", "connectToField": "k", "as": "g", "maxDepth": 0}});
    for stage in [redact, filter, group, windows, graph] {
        fails(vec![stage]);
    }
    // A stage that fails, then one a window stays behind, then the window.
    for blocker in [json!({"$sort": {"k": 1}}), json!({"$unwind": "$k"})] {
        fails(vec![json!({"$addFields": {"x": div()}}), blocker]);
    }
    // A window of several `$skip`s and `$limit`s is one, and moves as one.
    let add = json!({"$addFields": {"x": div()}});
    let run_window = |window: Vec<Value>| {
        let mut stages = vec![add.clone()];
        stages.extend(window);
        ids(&run(&state, Value::Array(stages), aggregate::Limits::default()).unwrap())
    };
    assert_eq!(run_window(vec![json!({"$limit": 8}), json!({"$skip": 5})]), [5, 6, 7]);
    assert_eq!(
        run_window(vec![json!({"$limit": 10}), json!({"$limit": 3}), json!({"$skip": 5})]),
        Vec::<i64>::new()
    );
    assert_eq!(
        run_window(vec![json!({"$skip": 2}), json!({"$skip": 3}), json!({"$limit": 4})]),
        [5, 6, 7, 8]
    );
    // Two stages it passes, an empty `$match` the reference drops, and a stage between the
    // skip and the limit.
    assert_eq!(answers(vec![add.clone(), json!({"$project": {"_id": 1}})]), [5, 6]);
    assert_eq!(answers(vec![add.clone(), json!({"$match": {}})]), [5, 6]);
    assert_eq!(
        run_window(vec![json!({"$skip": 5}), json!({"$project": {"k": 1}}), json!({"$limit": 2})]),
        [5, 6]
    );
    // A failure inside the window is raised.
    let inside =
        json!({"$addFields": {"x": {"$divide": [1, {"$cond": [{"$eq": ["$_id", 6]}, 0, 1]}]}}});
    let err =
        run(&state, json!([inside, {"$skip": 5}, {"$limit": 3}]), aggregate::Limits::default())
            .unwrap_err();
    assert!(err.message.contains("$divide"), "{}", err.message);
    // A skip that is first drops the document before the stage sees it, as it always did.
    let ok = run(&state, json!([{"$skip": 5}, add, {"$limit": 2}]), aggregate::Limits::default());
    assert_eq!(ids(&ok.unwrap()), [5, 6]);
}

#[test]
fn a_window_that_has_moved_to_the_head_bounds_the_range_it_reads() {
    // The keys of an inexact range are bounded by what the window takes, though stages that
    // neither filter nor change the count are written before it.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 1_500);
    let none = json!({"$match": {"_id": -1}});
    let limits = capped(key_bytes(4));
    let (answer, stats) = run_stats(
        &state,
        json!([{"$match": {"t": {"$gte": 0}}}, {"$project": {"_id": 1}}, {"$skip": 1},
            {"$limit": 2}, none]),
        limits.clone(),
    );
    assert_eq!(ids(&answer), Vec::<i64>::new());
    assert_eq!(stats.matched, 3, "three documents were taken from the range");
    assert_eq!(limits.buffer.used(), 0);
}

#[test]
fn a_window_that_takes_nothing_reads_nothing_from_the_source() {
    // `$limit 3` then `$skip 5` takes nothing, which no request can write as a `$limit` (it is a
    // `400`): the rewrite makes it `$skip 3, $limit 0`. The chain is full before any document, so
    // the scan reads none, whether it is a collection scan or an index.
    let nothing = |pipeline: Value, state: &SharedState| {
        let (answer, stats) = run_stats(state, pipeline.clone(), aggregate::Limits::default());
        assert_eq!(answer["documents"], json!([]), "{pipeline}");
        assert_eq!((stats.examined, stats.matched), (0, 0), "{pipeline}");
        assert!(
            stats.index_entries.is_none_or(|entries| entries == 0),
            "{pipeline}: {:?}",
            stats.index_entries
        );
    };
    let window = || [json!({"$limit": 3}), json!({"$skip": 5})];
    // A collection scan, with stages the window passes in front of it.
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, numbered(50, &[]));
    nothing(json!(window()), &state);
    let mut behind = vec![json!({"$addFields": {"x": 1}})];
    behind.extend(window());
    nothing(json!(behind), &state);
    // Behind a `$sort`, where the rewrite is a `$limit` of 0 alone and not a heap that keeps the
    // documents of the `$skip` and reads the whole source: it examines none either.
    for sorted in [
        vec![json!({"$sort": {"k": 1}}), json!({"$limit": 30}), json!({"$skip": 40})],
        vec![
            json!({"$sort": {"k": 1}}),
            json!({"$project": {"k": 1}}),
            json!({"$limit": 30}),
            json!({"$skip": 40}),
        ],
        vec![
            json!({"$sort": {"k": 1}}),
            json!({"$limit": 30}),
            json!({"$skip": 40}),
            json!({"$count": "n"}),
        ],
    ] {
        let counted = sorted.last().is_some_and(|stage| stage.get("$count").is_some());
        let (answer, stats) = run_stats(&state, json!(sorted), aggregate::Limits::default());
        assert_eq!(
            answer["documents"],
            if counted { json!([{"n": 0}]) } else { json!([]) },
            "{sorted:?}"
        );
        assert_eq!((stats.examined, stats.matched), (0, 0), "{sorted:?}");
    }
    // An index: an exact probe, an inexact range (read whole in `_id` order when it is wanted),
    // and the sort of the result.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 1_500);
    for filter in [json!({"t": 7}), json!({"t": {"$gte": 0}}), json!({"t": {"$gt": 1_000}})] {
        let mut pipeline = vec![json!({"$match": filter.clone()})];
        pipeline.extend(window());
        nothing(json!(pipeline), &state);
        let sorted =
            json!([{"$match": filter}, {"$sort": {"t": -1}}, {"$limit": 30}, {"$skip": 40}]);
        nothing(sorted, &state);
    }
    // Where the same window takes something, the source is read, so the zeros above are the
    // window's and not the collection's.
    let (answer, stats) = run_stats(
        &state,
        json!([{"$match": {"t": {"$gte": 0}}}, {"$limit": 8}, {"$skip": 5}]),
        aggregate::Limits::default(),
    );
    assert_eq!(ids(&answer), [5, 6, 7]);
    assert!(stats.examined > 0 && stats.index_entries.is_some_and(|entries| entries > 0));
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
    let err =
        run(&state, json!([{"$addFields": {"x": 1}}, {"$sort": {"_id": 1}}]), with_ceiling(10))
            .unwrap_err();
    assert!(err.message.contains("$addFields produced 11 documents"), "{}", err.message);
    // A `$count` holds a counter and not its input, so it has no ceiling to refuse at.
    let ok = run(&state, json!([{"$addFields": {"x": 1}}, {"$count": "n"}]), with_ceiling(10));
    assert_eq!(ok.unwrap()["documents"], json!([{"n": 30}]));
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
    // A `$group` holds its buckets and not its input (ADR-270): one bucket is answered, and
    // a `$push` of the padding is refused as the buckets pass the limit.
    let ok = run(
        &state,
        json!([{"$project": {"pad": 1}}, {"$group": {"_id": null, "n": {"$sum": 1}}}]),
        capped(50_000),
    )
    .unwrap();
    assert_eq!(ok["documents"], json!([{"_id": null, "n": 40}]));
    let err = run(
        &state,
        json!([{"$project": {"pad": 1}}, {"$group": {"_id": null, "p": {"$push": "$pad"}}}]),
        capped(50_000),
    )
    .unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert!(err.message.contains("the buckets of $group is "), "{}", err.message);
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

#[test]
fn a_source_above_the_limit_is_answered_by_a_stage_that_holds_a_counter_groups_a_heap_or_a_sample()
{
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, padded(40, 10_000));
    let all = held_docs_bytes(&padded(40, 10_000));
    // The collection is eight times the budget: read whole, it is refused at the source.
    let cap = all / 8;
    let err = run(&state, json!([{"$sort": {"_id": 1}}]), capped(cap)).unwrap_err();
    assert!(err.message.contains("the source read"), "{}", err.message);
    // And a pipeline that begins with a stage that keeps a counter or its groups is answered, in
    // the memory of its state, whatever it is streamed over.
    let cases = [
        (json!([{"$count": "n"}]), json!([{"n": 40}])),
        (json!([{"$group": {"_id": null, "n": {"$sum": 1}}}]), json!([{"_id": null, "n": 40}])),
        (
            json!([{"$group": {"_id": {"$mod": ["$_id", 2]}, "n": {"$sum": 1}}}]),
            json!([{"_id": 0, "n": 20}, {"_id": 1, "n": 20}]),
        ),
        (
            json!([{"$bucket": {"groupBy": "$_id", "boundaries": [0, 20, 40],
                "output": {"n": {"$sum": 1}}}}]),
            json!([{"_id": 0, "n": 20}, {"_id": 20, "n": 20}]),
        ),
        (
            json!([{"$sortByCount": {"$mod": ["$_id", 3]}}]),
            json!([{"_id": 0, "count": 14}, {"_id": 1, "count": 13}, {"_id": 2, "count": 13}]),
        ),
        (
            json!([{"$sort": {"_id": -1}}, {"$limit": 3}, {"$project": {"_id": 1}}]),
            json!([{"_id": 39}, {"_id": 38}, {"_id": 37}]),
        ),
        (
            json!([{"$sort": {"_id": -1}}, {"$skip": 1}, {"$limit": 2}, {"$project": {"_id": 1}}]),
            json!([{"_id": 38}, {"_id": 37}]),
        ),
        // Behind a stage that streams, and followed by one.
        (
            json!([{"$addFields": {"c": "$pad"}}, {"$count": "n"}, {"$addFields": {"m": 1}}]),
            json!([{"n": 40, "m": 1}]),
        ),
    ];
    // A `$sample` holds its sample and not the collection: its size is what it answers.
    let limits = capped(cap);
    let sampled =
        run(&state, json!([{"$sample": {"size": 4}}, {"$project": {"_id": 1}}]), limits.clone())
            .unwrap();
    assert_eq!(sampled["count"], 4);
    let mut chosen = ids(&sampled);
    chosen.sort_unstable();
    chosen.dedup();
    assert_eq!(chosen.len(), 4, "four different documents of the forty");
    assert_eq!(limits.buffer.used(), 0);
    for (pipeline, answer) in cases {
        let limits = capped(cap);
        let ok = run(&state, pipeline.clone(), limits.clone());
        assert_eq!(
            ok.unwrap_or_else(|e| panic!("{pipeline}: {e:?}"))["documents"],
            answer,
            "{pipeline}"
        );
        assert_eq!(limits.buffer.used(), 0, "{pipeline}");
    }
}

#[test]
fn a_state_that_grows_with_the_input_is_refused_at_its_bytes_or_its_documents() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, padded(40, 10_000));
    // One bucket for each document, each keeping its padding: nothing streaming can help.
    let pipeline = json!([{"$group": {"_id": "$_id", "p": {"$push": "$pad"}}}]);
    let limits = capped(50_000);
    let err = run(&state, pipeline, limits.clone()).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert!(err.message.contains("the buckets of $group is "), "{}", err.message);
    let held: usize =
        err.message.split(" is ").nth(1).unwrap().split(' ').next().unwrap().parse().unwrap();
    assert!((50_000..70_000).contains(&held), "stopped at {held}, not after the whole 400 KB");
    assert_eq!(limits.buffer.used(), 0);
    // And at the pipeline limit when the buckets are small: a source of thirty, a ceiling of ten.
    let state = state_with(&tempfile::tempdir().unwrap(), numbered(30, &[]));
    let err = run(&state, json!([{"$group": {"_id": "$_id"}}]), with_ceiling(10)).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
    assert!(err.message.contains("$group produced 11 documents"), "{}", err.message);
    // A `$sort` with a `$limit` holds that many documents, and the ceiling is on those: a heap
    // of ten is answered over thirty, a heap of eleven is not.
    let sorted = |limit: i64| json!([{"$sort": {"_id": -1}}, {"$limit": limit}]);
    let ok = run(&state, sorted(10), with_ceiling(10)).unwrap();
    assert_eq!(ok["count"], 10);
    let err = run(&state, sorted(11), with_ceiling(10)).unwrap_err();
    assert!(err.message.contains("$sort produced 11 documents"), "{}", err.message);
    // A few groups over the same source and ceiling are answered: the ceiling is on the groups.
    let ok = run(&state, json!([{"$group": {"_id": "$k", "n": {"$sum": 1}}}]), with_ceiling(10));
    assert_eq!(ok.unwrap()["count"], 5);
    let ok = run(&state, json!([{"$count": "n"}]), with_ceiling(10));
    assert_eq!(ok.unwrap()["documents"], json!([{"n": 30}]));
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
    // Anything that may drop a document before the limit (a `$redact` may) may need more, and
    // is held to the budget instead. (A `$project` is not one: the limit is moved ahead of it, and is the head.)
    let err = run(
        &state,
        json!([{"$match": {"t": {"$gte": 0}}}, {"$redact": "$$KEEP"}, {"$limit": 1}, none]),
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
fn what_a_stage_that_blocks_hands_to_a_stage_that_streams_is_released_as_it_drains() {
    // What this pins is that the drain is charged and given back: a drain charged and never
    // released would need the documents twice. That the drain is charged at all is pinned by
    // the unit tests that read the budget at each push (`pipeline::tests`, `budget_tests`).
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, padded(20, 10_000));
    let input = held_docs_bytes(&padded(20, 10_000));
    // The `$sort` holds its input; its output is the same documents, which the second
    // `$project` takes as its input and the `$group` keeps the padding of as it gets them.
    // Each is given back as it is taken, so the documents once is held, and not twice as
    // they drain beside what has arrived.
    let pipeline = json!([
        {"$project": {"pad": 1}},
        {"$sort": {"_id": -1}},
        {"$project": {"pad": 1}},
        {"$group": {"_id": null, "p": {"$push": "$pad"}}},
        {"$project": {"n": {"$size": "$p"}}},
    ]);
    let (least, err) = smallest_budget(&state, &pipeline, 4 * input);
    assert!(
        least >= input && least < input + input / 10,
        "answered under {least}, documents {input}"
    );
    // What is full is the `$group`, which holds the padding it was given and not the input:
    // the second `$project` held nothing of what it had taken.
    assert!(err.message.contains("the buckets of $group is "), "{}", err.message);
}

#[test]
fn a_descent_of_single_element_unwinds_is_answered_within_the_budget_the_loop_needed() {
    // Thirty arrays of one element, thirty `$unwind`s, over a megabyte document: the loop held
    // two documents at a time. A fan that kept a copy for each level was refused at the
    // twenty-third on a 100 MiB budget with four megabytes; here it is the budget of the
    // answer and a few documents.
    let mut doc = deep(0);
    for level in 0..30 {
        doc.insert(format!("a{level}"), vec![1]);
    }
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, vec![doc]);
    let one = held_docs_bytes(&[deep(0)]);
    let pipeline: Vec<Value> =
        (0..30).map(|level| json!({"$unwind": format!("$a{level}")})).collect();
    let limits = capped(8 * one);
    let ok = run(&state, Value::Array(pipeline), limits.clone()).unwrap();
    assert_eq!(ok["count"], 1);
    assert_eq!(limits.buffer.used(), 0);
}

#[test]
fn the_keys_of_an_index_range_are_charged_for_a_pipeline_that_begins_with_a_stage_that_blocks() {
    // The source of a pipeline that begins with a stage that blocks is read whole, with its
    // ceiling and its `400` as they were; the keys the range holds to deliver it in `_id` order
    // are charged to a lease of their own, and refused `413` past the budget.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 1_500);
    let pipeline = json!([{"$match": {"t": {"$gte": 0}}}, {"$sort": {"_id": -1}}, {"$limit": 1}]);
    let limits = capped(key_bytes(100));
    let err = run(&state, pipeline.clone(), limits.clone()).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert!(err.message.contains("the index range of $match is "), "{}", err.message);
    assert_eq!(limits.buffer.used(), 0);
    // Room for the keys and the documents, and it answers; the keys are given back.
    let limits = capped(10_000_000);
    assert_eq!(ids(&run(&state, pipeline, limits.clone()).unwrap()), [1_499]);
    assert_eq!(limits.buffer.used(), 0);
    // The ceiling and its text are as they were.
    let err = run(
        &state,
        json!([{"$match": {"t": {"$gte": 0}}}, {"$sort": {"_id": 1}}]),
        with_ceiling(1_000),
    )
    .unwrap_err();
    assert!(
        err.message.contains("the leading $match admits more than 1000 documents"),
        "{}",
        err.message
    );
}
