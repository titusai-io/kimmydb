//! What the stages that read another collection, the source read, the sink and the answer
//! charge to the request's memory budget (ADR-269), and the settings that size it.

use bson::{Document, doc};
use serde_json::{Value, json};

use super::tests::{agg_limits, live_state, seed_docs};
use super::*;
use crate::limits::AggregateMemory;
use kimmy_query::budget::{BufferBudget, docs_bytes};

fn root() -> Auth {
    Auth(kimmy_auth::Principal::superuser("test"))
}

fn capped(cap: usize) -> aggregate::Limits {
    aggregate::Limits { buffer: BufferBudget::new(cap), ..Default::default() }
}

/// `n` documents of about `pad` bytes each, all joinable on `k: 1`.
fn padded(n: i32, pad: usize) -> Vec<Document> {
    (0..n).map(|i| doc! {"_id": i, "k": 1, "pad": "x".repeat(pad)}).collect()
}

fn state_with(
    dir: &tempfile::TempDir,
    local: Vec<Document>,
    foreign: Vec<Document>,
) -> SharedState {
    let state = live_state(dir);
    seed_docs(&state, "orders", local);
    seed_docs(&state, "users", foreign);
    state
}

fn refusal(result: Result<Value, ApiError>) -> ApiError {
    result.expect_err("must be refused")
}

fn names(err: &ApiError, holder: &str) {
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert!(err.message.contains(holder), "{holder:?} not named in {}", err.message);
    assert!(err.message.contains("aggregate.memory_limit_bytes"), "{}", err.message);
}

fn run(state: &SharedState, pipeline: Value, limits: aggregate::Limits) -> Result<Value, ApiError> {
    agg_limits(state, &root(), "app", "orders", &pipeline, limits)
}

#[test]
fn the_source_read_is_charged_and_ends_at_the_document_that_passes_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, padded(40, 10_000), vec![]);
    // 40 documents are about 400 KB; the read is refused a few documents past 50 KB.
    let err = refusal(run(&state, json!([]), capped(50_000)));
    names(&err, "the source read");
    let held: usize =
        err.message.split(" is ").nth(1).unwrap().split(' ').next().unwrap().parse().unwrap();
    assert!((50_000..70_000).contains(&held), "stopped at {held}, not after the whole 400 KB");
    // The same limit, a leading $match that admits one document: the read is of what the
    // filter admits, not of the collection.
    let one =
        run(&state, json!([{"$match": {"_id": 3}}, {"$count": "n"}]), capped(50_000)).unwrap();
    assert_eq!(one["documents"], json!([{"n": 1}]));
}

#[test]
fn the_answer_is_charged_as_documents_and_their_json() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, padded(10, 10_000), vec![]);
    // Read: ~100 KB. Answer: the documents again and ~100 KB of JSON, built beside them.
    let err = refusal(run(&state, json!([]), capped(150_000)));
    names(&err, "the answer");
    let ok = run(&state, json!([]), capped(250_000)).unwrap();
    assert_eq!(ok["count"], 10);
    // A pipeline that reduces what it answers is not charged for what it read.
    let counted = run(&state, json!([{"$count": "n"}]), capped(150_000)).unwrap();
    assert_eq!(counted["documents"], json!([{"n": 10}]));
}

#[test]
fn every_account_is_empty_when_a_request_ends_by_answer_or_by_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, padded(10, 10_000), padded(5, 10_000));
    let pipelines = [
        json!([]),
        json!([{"$addFields": {"c": "$pad"}}]),
        json!([{"$lookup": {"from": "users", "localField": "k", "foreignField": "k", "as": "u"}}]),
        json!([{"$group": {"_id": "$k", "all": {"$push": "$pad"}}}]),
        json!([{"$unionWith": "users"}]),
        json!([{"$facet": {"a": [{"$count": "n"}], "b": [{"$limit": 2}]}}]),
    ];
    for cap in [20_000usize, 120_000, 300_000, 5_000_000] {
        for pipeline in &pipelines {
            let limits = capped(cap);
            let _ = run(&state, pipeline.clone(), limits.clone());
            assert_eq!(limits.buffer.used(), 0, "cap {cap}: {pipeline}");
        }
    }
}

#[test]
fn a_lookup_charges_the_foreign_documents_it_holds_and_the_arrays_it_attaches() {
    let dir = tempfile::tempdir().unwrap();
    // 4 local documents, each joined to all 5 foreign ones (about 50 KB), so the output
    // holds 4 copies of them.
    let state = state_with(&dir, padded(4, 100), padded(5, 10_000));
    let lookup =
        json!([{"$lookup": {"from": "users", "localField": "k", "foreignField": "k", "as": "u"}}]);
    let input = docs_bytes(&padded(4, 100));
    let foreign = docs_bytes(&padded(5, 10_000));
    // Refused while the foreign side is read.
    let err = refusal(run(&state, lookup.clone(), capped(input + foreign / 2)));
    names(&err, "the foreign documents $lookup holds");
    // Refused while the arrays are attached: the foreign side and the input fit, the copies do not.
    let err = refusal(run(&state, lookup.clone(), capped(input + foreign + foreign)));
    names(&err, "the output of $lookup");
    // Room for the foreign side, the input and the four copies, and the lookup runs.
    let mut counted = lookup.as_array().unwrap().clone();
    counted.push(json!({"$count": "n"}));
    let ok = run(&state, json!(counted), capped(input + foreign + 4 * foreign + 10_000)).unwrap();
    assert_eq!(ok["documents"], json!([{"n": 4}]));
    // And one byte fewer is refused at the last attached array.
    let err = refusal(run(&state, lookup, capped(input + foreign + 4 * foreign - 1)));
    names(&err, "the output of $lookup");
}

#[test]
fn a_lookup_with_a_pipeline_charges_its_foreign_read_its_copies_and_what_it_attaches() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, padded(3, 100), padded(4, 10_000));
    let lookup = json!([{"$lookup": {"from": "users", "pipeline": [{"$match": {}}], "as": "u"}}]);
    let input = docs_bytes(&padded(3, 100));
    let foreign = docs_bytes(&padded(4, 10_000));
    let err = refusal(run(&state, lookup.clone(), capped(input + foreign / 2)));
    names(&err, "the foreign documents $lookup holds");
    // Each input document runs the sub-pipeline over a copy of the foreign side: the input,
    // the foreign documents and the copy are held at once.
    let err = refusal(run(&state, lookup.clone(), capped(input + foreign + foreign / 2)));
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert!(err.message.contains("a copy of the foreign documents"), "{}", err.message);
    let mut counted = lookup.as_array().unwrap().clone();
    counted.push(json!({"$count": "n"}));
    let ok = run(&state, json!(counted), capped(input + 12 * foreign)).unwrap();
    assert_eq!(ok["documents"], json!([{"n": 3}]));
}

#[test]
fn a_union_charges_what_it_reads_and_what_it_appends() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, padded(3, 100), padded(5, 10_000));
    let union = json!([{"$unionWith": "users"}]);
    let input = docs_bytes(&padded(3, 100));
    let foreign = docs_bytes(&padded(5, 10_000));
    let err = refusal(run(&state, union.clone(), capped(input + foreign / 2)));
    names(&err, "the foreign documents $unionWith reads");
    // The input is held beside what is read, and what is read is then what is appended: the
    // stage holds both and no more.
    let err = refusal(run(&state, union.clone(), capped(input + foreign - 1)));
    names(&err, "the foreign documents $unionWith reads");
    let mut counted = union.as_array().unwrap().clone();
    counted.push(json!({"$count": "n"}));
    let ok = run(&state, json!(counted), capped(input + foreign)).unwrap();
    assert_eq!(ok["documents"], json!([{"n": 8}]));
    // The answer holds both and their JSON.
    let ok = run(&state, union, capped(3 * (input + foreign))).unwrap();
    assert_eq!(ok["count"], 8);
}

#[test]
fn a_graph_lookup_charges_the_copies_it_attaches() {
    let dir = tempfile::tempdir().unwrap();
    let nodes: Vec<Document> =
        (0..5).map(|i| doc! {"_id": i, "next": i + 1, "pad": "y".repeat(10_000)}).collect();
    let state = state_with(&dir, vec![doc! {"_id": 1, "start": 0}], nodes);
    let walk = json!([{"$graphLookup": {
        "from": "users", "startWith": "$start", "connectFromField": "next", "connectToField": "_id", "as": "reached", "maxDepth": 10,
    }}]);
    let ok = run(&state, walk.clone(), capped(10_000_000)).unwrap();
    assert_eq!(ok["documents"][0]["reached"].as_array().unwrap().len(), 5);
    let foreign = docs_bytes(
        &(0..5)
            .map(|i| doc! {"_id": i, "next": i + 1, "pad": "y".repeat(10_000)})
            .collect::<Vec<_>>(),
    );
    // The foreign side fits and the five copies beside it do not.
    let err = refusal(run(&state, walk, capped(foreign + foreign / 2)));
    names(&err, "the output of $graphLookup");
}

#[test]
fn a_sink_is_charged_for_the_documents_it_writes() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, padded(10, 10_000), vec![]);
    let docs = docs_bytes(&padded(10, 10_000));
    // The read holds ~100 KB and is handed on; the sink holds the output while writing.
    let ok = run(&state, json!([{"$out": "copy"}]), capped(docs + 1_000)).unwrap();
    assert_eq!(ok["out"]["inserted"], 10);
    let err = refusal(run(
        &state,
        json!([{"$addFields": {"c": "$pad"}}, {"$out": "copy2"}]),
        capped(docs + 1_000),
    ));
    names(&err, "the output of $addFields");
}

#[test]
fn a_request_draws_on_the_nodes_pool_and_gives_it_back() {
    let dir = tempfile::tempdir().unwrap();
    // About 1 MB of documents, a request limit of 3 MB (documents and JSON) and a pool of 3.5
    // MB: an explicit pool under twice the limit, so one other request can fill what is left.
    // (At the derived ratio, a pool of twice the limit, it takes two others: the HTTP test
    // `with_the_derived_ratio_a_third_request_is_the_first_the_pool_refuses`.)
    let state = state_with(&dir, padded(10, 100_000), vec![]);
    state.set_aggregate_memory(AggregateMemory::new(3_000_000, 3_500_000));
    let pool = state.aggregate_memory().pool().clone();
    let ok = agg_limits(&state, &root(), "app", "orders", &json!([]), state.aggregate_limits());
    assert_eq!(ok.unwrap()["count"], 10);
    assert_eq!(pool.used(), 0);

    // Another request holds 3 MB of the pool: this one reads 1 MB and is within its own
    // limit, and is answered 503 with the hint to wait and a Retry-After.
    let other = state.aggregate_limits();
    let mut held = other.buffer.lease("another request");
    held.charge(3_000_000).unwrap();
    assert_eq!(pool.used(), 3_000_000);
    let err = agg_limits(&state, &root(), "app", "orders", &json!([]), state.aggregate_limits())
        .unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::SERVICE_UNAVAILABLE, "{}", err.message);
    assert_eq!(err.code, crate::error::ErrorCode::Timeout);
    assert_eq!(err.code.retry(), crate::error::Retry::Wait);
    assert_eq!(err.retry_after_secs, Some(crate::error::AGGREGATE_POOL_RETRY_AFTER_SECS));
    assert!(err.message.contains("aggregate.memory_pool_bytes"), "{}", err.message);
    assert!(err.message.contains("the source read"), "{}", err.message);
    // The refused request reserved nothing, and gives back what it had when it ends.
    assert_eq!(pool.used(), 3_000_000);
    assert_eq!((pool.pool_refusals(), pool.limit_refusals()), (1, 0));
    // When the other ends, the same request is answered.
    drop(held);
    assert_eq!(pool.used(), 0);
    let ok = agg_limits(&state, &root(), "app", "orders", &json!([]), state.aggregate_limits());
    assert_eq!(ok.unwrap()["count"], 10);
    assert_eq!(pool.used(), 0);
}

#[test]
fn the_defaults_are_the_documented_ones_and_each_request_gets_its_own_budget() {
    let memory = AggregateMemory::default();
    assert_eq!(memory.limit(), 1 << 30);
    assert_eq!(memory.pool().cap(), 2 << 30);
    let (a, b) = (memory.limits(), memory.limits());
    assert_eq!(a.buffer.cap(), 1 << 30);
    a.buffer.lease("x").charge(100).unwrap();
    assert_eq!(a.buffer.used(), 0, "a lease that is dropped gives its bytes back");
    let mut held = a.buffer.lease("x");
    held.charge(100).unwrap();
    assert_eq!((a.buffer.used(), b.buffer.used()), (100, 0), "budgets are per request");
    assert_eq!(memory.pool().used(), 100, "and the pool is shared");
}

#[test]
fn a_facet_does_not_keep_every_lookup_runs_bytes_charged_until_it_ends() {
    // The sub-pipeline of this `$lookup` runs once for each of 8 input documents, over the 4
    // foreign documents of about 10 KB each, and its window stage buffers them each time. Each
    // run gives its bytes back when it ends, in a `$facet` as outside one; if they were kept
    // until the facet was done the 8 runs would hold 8 times what one does.
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, padded(8, 100), padded(4, 10_000));
    let lookup = json!({"$lookup": {"from": "users", "pipeline": [
        {"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"n": {"$documentNumber": {}}}}},
        {"$project": {"_id": 1}},
    ], "as": "u"}});
    let foreign = docs_bytes(&padded(4, 10_000));
    let cap = 5 * foreign;
    let outside = run(&state, json!([lookup.clone(), {"$count": "n"}]), capped(cap)).unwrap();
    assert_eq!(outside["documents"], json!([{"n": 8}]));
    let facet = json!([{"$facet": {"a": [lookup.clone(), {"$count": "n"}]}}]);
    let limits = capped(cap);
    let inside = run(&state, facet.clone(), limits.clone()).unwrap();
    assert_eq!(inside["documents"], json!([{"a": [{"n": 8}]}]));
    assert_eq!(limits.buffer.used(), 0);
}

#[test]
fn a_request_the_deadline_drops_gives_its_bytes_back_to_the_pool() {
    // The request deadline (ADR-266) cuts a request by dropping its future where it waits.
    // Every holder owns a lease, so a dropped future returns what it held to the pool.
    let pool = kimmy_query::budget::MemoryPool::new(1_000_000);
    let limits =
        aggregate::Limits { buffer: BufferBudget::pooled(500_000, &pool), ..Default::default() };
    let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
    let cut = runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_millis(20), async {
            let mut source = limits.buffer.lease("the source read");
            let mut stage = limits.buffer.lease("the output of $addFields (with its input)");
            source.charge(200_000).unwrap();
            stage.charge(100_000).unwrap();
            assert_eq!(pool.used(), 300_000);
            std::future::pending::<()>().await;
        })
        .await
    });
    assert!(cut.is_err(), "the deadline fired");
    assert_eq!(pool.used(), 0);
    assert_eq!(limits.buffer.used(), 0);
}

// ---------------------------------------------------------------------------
// The answer is written once (the review's m2)
// ---------------------------------------------------------------------------

/// Documents of every kind of value the JSON conversion treats apart.
fn mixed(n: i32) -> Vec<Document> {
    (0..n)
        .map(|i| {
            doc! {
                "_id": i,
                "long": i64::MAX - i64::from(i),
                "double": 0.1 + f64::from(i),
                "nan": f64::NAN,
                "date": bson::DateTime::from_millis(1_700_000_000_000 + i64::from(i)),
                "oid": bson::oid::ObjectId::from_bytes([i as u8; 12]),
                "bin": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![i as u8; 5] },
                "dec": bson::Decimal128::from_bytes([i as u8; 16]),
                "ts": bson::Timestamp { time: 7, increment: i as u32 },
                "text": format!("caf\u{e9} \"quoted\" \\ \n {}", "x".repeat(40)),
                "nested": { "a": [1, 2.5, "three", { "deep": [true, bson::Bson::Null] }], "b": {} },
                "empty": [],
                "re": bson::Regex {
                    pattern: bson::raw::CString::try_from("^a.b$".to_string()).unwrap(),
                    options: bson::raw::CString::try_from("i".to_string()).unwrap(),
                },
            }
        })
        .collect()
}

fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(future)
}

#[test]
fn the_body_the_route_sends_is_the_bytes_serde_json_gives_for_the_value() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, mixed(12), vec![]);
    for pipeline in [
        json!([]),
        json!([{"$match": {"_id": {"$lt": 5}}}, {"$project": {"long": 1, "nested": 1}}]),
        json!([{"$match": {"_id": 999}}]),
        json!([{"$count": "n"}]),
        json!([{"$group": {"_id": null, "all": {"$push": "$date"}}}]),
    ] {
        let value = block_on(aggregate(&state, &root(), "app", "orders", &pipeline)).unwrap();
        let (answer, hold) =
            block_on(aggregate_held(&state, &root(), "app", "orders", &pipeline)).unwrap();
        let Answer::Body(body) = answer else { panic!("{pipeline}: documents are a body") };
        assert_eq!(
            String::from_utf8(body).unwrap(),
            serde_json::to_string(&value).unwrap(),
            "{pipeline}"
        );
        // The hold is the JSON of the documents alone, and it is the same as for the value.
        assert!(hold.bytes() > 0 || pipeline == json!([{"$match": {"_id": 999}}]));
    }
}

#[test]
fn the_body_is_charged_the_same_bytes_and_refused_at_the_same_one_as_the_value() {
    // Over a sweep of limits around where the answer stops fitting, the body and the value
    // agree on whether it is answered, and when it is not, on every word of the refusal
    // (which includes the total held when it was refused). A state's limits are set once, so
    // each limit is a state of its own.
    let both = |limit: usize| {
        let dir = tempfile::tempdir().unwrap();
        let state = state_with(&dir, mixed(12), vec![]);
        state.set_aggregate_memory(AggregateMemory::new(limit, 64 << 20));
        let pipeline = json!([]);
        let value = block_on(aggregate(&state, &root(), "app", "orders", &pipeline));
        let body = block_on(aggregate_held(&state, &root(), "app", "orders", &pipeline));
        (value, body)
    };
    // Where the answer stops fitting, found by halving.
    let (mut lo, mut hi) = (1_000usize, 1_000_000usize);
    assert!(both(lo).0.is_err() && both(hi).0.is_ok());
    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        if both(mid).0.is_ok() {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    let (mut refused, mut answered) = (0, 0);
    for limit in (hi - 600..hi + 600).step_by(50) {
        let (value, body) = both(limit);
        match (value, body) {
            (Ok(value), Ok((Answer::Body(body), hold))) => {
                answered += 1;
                assert_eq!(
                    String::from_utf8(body).unwrap(),
                    serde_json::to_string(&value).unwrap()
                );
                drop(hold);
            }
            (Err(a), Err(b)) => {
                refused += 1;
                assert_eq!((a.status, &a.message), (b.status, &b.message), "limit {limit}");
            }
            (a, b) => panic!("limit {limit}: {:?} vs {:?}", a.is_ok(), b.is_ok()),
        }
    }
    assert!(refused > 3 && answered > 3, "the sweep crosses the limit: {refused}/{answered}");
    // And the exact boundary is the same for both: the smallest limit that answers.
    let (value, body) = both(hi);
    assert!(value.is_ok() && body.is_ok());
    let (value, body) = both(hi - 1);
    assert!(value.is_err() && body.is_err());
}

#[test]
fn a_sink_answers_its_counts_as_a_value_and_documents_as_a_body() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, padded(3, 10), vec![]);
    let (answer, _) =
        block_on(aggregate_held(&state, &root(), "app", "orders", &json!([{"$out": "copy"}])))
            .unwrap();
    let Answer::Json(counts) = answer else { panic!("a sink answers a value") };
    assert_eq!(counts["out"]["inserted"], 3);
}
