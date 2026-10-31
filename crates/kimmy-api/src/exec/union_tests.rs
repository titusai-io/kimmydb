//! `$unionWith` (ADR-263), run by the caller: the order of its output, the sub-pipeline,
//! a collection that is not there, and the document and byte limits it is held to.

use bson::{Document, doc};
use serde_json::{Value, json};

use super::tests::{agg, agg_limits, live_state, seed_docs};
use super::*;

fn root() -> Auth {
    Auth(kimmy_auth::Principal::superuser("test"))
}

fn state_with(dir: &tempfile::TempDir) -> SharedState {
    let state = live_state(dir);
    seed_docs(
        &state,
        "orders",
        vec![doc! {"_id": 1, "k": "o1"}, doc! {"_id": 2, "k": "o2"}, doc! {"_id": 3, "k": "o3"}],
    );
    seed_docs(
        &state,
        "archive",
        vec![
            doc! {"_id": 10, "k": "a1", "n": 1},
            doc! {"_id": 11, "k": "a2", "n": 2},
            doc! {"_id": 12, "k": "a3", "n": 3},
        ],
    );
    state
}

fn keys(out: &Value) -> Vec<String> {
    out["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["k"].as_str().unwrap_or("-").to_string())
        .collect()
}

fn run(state: &SharedState, pipeline: Value) -> Result<Value, ApiError> {
    agg(state, &root(), "app", "orders", &pipeline)
}

#[test]
fn the_input_comes_first_then_the_other_collection() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir);
    let out = run(&state, json!([{"$unionWith": "archive"}])).unwrap();
    assert_eq!(keys(&out), ["o1", "o2", "o3", "a1", "a2", "a3"]);
    assert_eq!(out["count"], 6);
    // The document form without a pipeline is the same.
    let out = run(&state, json!([{"$unionWith": {"coll": "archive"}}])).unwrap();
    assert_eq!(keys(&out), ["o1", "o2", "o3", "a1", "a2", "a3"]);
    // Stages before it shape what comes first; stages after it see both.
    let out = run(
        &state,
        json!([{"$match": {"_id": {"$gte": 2}}}, {"$unionWith": "archive"}, {"$sort": {"_id": -1}}, {"$limit": 2}]),
    )
    .unwrap();
    assert_eq!(keys(&out), ["a3", "a2"]);
}

#[test]
fn the_sub_pipeline_shapes_only_the_other_collections_documents() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir);
    let out = run(
        &state,
        json!([{"$unionWith": {"coll": "archive", "pipeline": [
            {"$match": {"n": {"$gte": 2}}},
            {"$project": {"_id": 0, "k": 1}},
        ]}}]),
    )
    .unwrap();
    assert_eq!(
        out["documents"],
        json!([{"_id": 1, "k": "o1"}, {"_id": 2, "k": "o2"}, {"_id": 3, "k": "o3"}, {"k": "a2"}, {"k": "a3"}])
    );
    // A blocking stage in it works on the other collection alone.
    let out = run(
        &state,
        json!([{"$unionWith": {"coll": "archive", "pipeline": [
            {"$sort": {"n": -1}}, {"$limit": 1}, {"$addFields": {"k": "top"}}]}}]),
    )
    .unwrap();
    assert_eq!(keys(&out), ["o1", "o2", "o3", "top"]);
}

#[test]
fn an_empty_input_still_answers_the_other_collection() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir);
    let out = run(&state, json!([{"$match": {"_id": 999}}, {"$unionWith": "archive"}])).unwrap();
    assert_eq!(keys(&out), ["a1", "a2", "a3"]);
}

#[test]
fn a_collection_that_does_not_exist_reads_as_empty() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir);
    let out = run(&state, json!([{"$unionWith": "nowhere"}])).unwrap();
    assert_eq!(keys(&out), ["o1", "o2", "o3"]);
    let out =
        run(&state, json!([{"$unionWith": {"coll": "nowhere", "pipeline": [{"$count": "n"}]}}]))
            .unwrap();
    // A `$count` over nothing is one document holding 0.
    assert_eq!(out["documents"][3], json!({"n": 0}));
    // A name that cannot be a collection is a 400, not an empty read.
    let err = run(&state, json!([{"$unionWith": "a/b"}])).expect_err("a bad name");
    assert_eq!(err.status, 400, "{err:?}");
}

#[test]
fn the_same_collection_can_be_unioned_with_itself_and_unions_nest() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir);
    let out = run(&state, json!([{"$unionWith": "orders"}])).unwrap();
    assert_eq!(keys(&out), ["o1", "o2", "o3", "o1", "o2", "o3"]);
    let out = run(
        &state,
        json!([{"$match": {"_id": 1}}, {"$unionWith": {"coll": "archive", "pipeline": [
            {"$match": {"n": 1}},
            {"$unionWith": {"coll": "orders", "pipeline": [{"$match": {"_id": 3}}]}},
        ]}}]),
    )
    .unwrap();
    assert_eq!(keys(&out), ["o1", "a1", "o3"]);
}

#[test]
fn it_runs_inside_a_facet_and_a_lookup_sub_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir);
    let out = run(
        &state,
        json!([{"$match": {"_id": 1}}, {"$facet": {
            "all": [{"$unionWith": "archive"}, {"$count": "n"}],
            "plain": [{"$project": {"_id": 0, "k": 1}}],
        }}]),
    )
    .unwrap();
    assert_eq!(out["documents"][0], json!({"all": [{"n": 4}], "plain": [{"k": "o1"}]}));
    // In a lookup's pipeline the leading `$match` of the union reads the lookup's `let`.
    let out = run(
        &state,
        json!([{"$match": {"_id": {"$lte": 2}}}, {"$lookup": {
        "from": "orders", "let": {"want": "$_id"}, "as": "j",
        "pipeline": [
            {"$match": {"_id": 3}},
            {"$unionWith": {"coll": "archive", "pipeline": [
                {"$match": {"$expr": {"$eq": ["$n", "$$want"]}}}]}},
            {"$project": {"k": 1}},
        ]}}]),
    )
    .unwrap();
    assert_eq!(out["documents"][0]["j"], json!([{"_id": 3, "k": "o3"}, {"_id": 10, "k": "a1"}]));
    assert_eq!(out["documents"][1]["j"], json!([{"_id": 3, "k": "o3"}, {"_id": 11, "k": "a2"}]));
}

#[test]
fn the_other_collection_is_held_to_the_pipeline_limit_as_it_is_read() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir);
    let limits = |max_documents| aggregate::Limits { max_documents };
    let pipeline = json!([{"$match": {"_id": 1}}, {"$unionWith": "archive"}]);
    // Three foreign documents fit a limit of three; four is one over.
    let ok = agg_limits(&state, &root(), "app", "orders", &pipeline, limits(4)).unwrap();
    assert_eq!(ok["count"], 4);
    let err = agg_limits(&state, &root(), "app", "orders", &pipeline, limits(3))
        .expect_err("the combined output is four");
    assert!(format!("{err:?}").contains("$unionWith produced 4"), "{err:?}");
    let err = agg_limits(
        &state,
        &root(),
        "app",
        "orders",
        &json!([{"$match": {"_id": 1}}, {"$unionWith": "archive"}]),
        limits(2),
    )
    .expect_err("the read itself is over");
    assert!(format!("{err:?}").contains("$unionWith reads `archive` whole"), "{err:?}");
    // A leading $match of the sub-pipeline is applied as the read goes, so what it declines
    // is neither held nor counted: one document of three admitted fits a limit of two.
    let narrowed = json!([{"$match": {"_id": 1}}, {"$unionWith": {"coll": "archive", "pipeline": [
        {"$match": {"n": 3}}]}}]);
    let ok = agg_limits(&state, &root(), "app", "orders", &narrowed, limits(2)).unwrap();
    assert_eq!(keys(&ok), ["o1", "a3"]);
}

#[test]
fn a_sub_pipeline_that_grows_its_input_is_held_to_the_limit_too() {
    let dir = tempfile::tempdir().unwrap();
    let state = live_state(&dir);
    seed_docs(&state, "orders", vec![doc! {"_id": 1}]);
    seed_docs(&state, "wide", vec![doc! {"_id": 1, "xs": [1, 2, 3, 4, 5, 6]}]);
    let pipeline = json!([{"$unionWith": {"coll": "wide", "pipeline": [{"$unwind": "$xs"}]}}]);
    let err = agg_limits(
        &state,
        &root(),
        "app",
        "orders",
        &pipeline,
        aggregate::Limits { max_documents: 5 },
    )
    .expect_err("six unwound documents");
    assert!(format!("{err:?}").contains("$unwind produced 6"), "{err:?}");
}

#[test]
fn the_bytes_read_and_the_bytes_the_sub_pipeline_outputs_are_held_to_the_cap() {
    let dir = tempfile::tempdir().unwrap();
    let state = live_state(&dir);
    seed_docs(&state, "orders", vec![doc! {"_id": 1}]);
    let cap = kimmy_core::limits::MAX_DOCUMENT_BYTES;
    // Five documents of a quarter of the cap each: the fifth passes it.
    let pad = "x".repeat(cap / 4);
    let big: Vec<Document> = (0..5).map(|i| doc! {"_id": i, "pad": pad.clone()}).collect();
    seed_docs(&state, "big", big);
    let err = run(&state, json!([{"$unionWith": "big"}])).expect_err("over the cap");
    assert_eq!(err.status, 413, "{err:?}");
    assert!(err.message.contains("$unionWith collection read"), "{err:?}");
    // Four fit.
    let ok = run(
        &state,
        json!([{"$unionWith": {"coll": "big", "pipeline": [{"$match": {"_id": {"$lt": 3}}}]}}]),
    )
    .unwrap();
    assert_eq!(ok["count"], 4);
    // What the sub-pipeline outputs is held to the cap as well, though what it read was
    // not over it: each document twice over.
    let err = run(
        &state,
        json!([{"$unionWith": {"coll": "big", "pipeline": [
            {"$match": {"_id": {"$lt": 3}}},
            {"$addFields": {"again": "$pad"}}]}}]),
    )
    .expect_err("the output is over");
    assert_eq!(err.status, 413, "{err:?}");
    assert!(err.message.contains("$unionWith output"), "{err:?}");
}

#[test]
fn the_read_of_a_collection_over_the_byte_cap_stops_at_the_document_that_passes_it() {
    let dir = tempfile::tempdir().unwrap();
    let state = live_state(&dir);
    seed_docs(&state, "orders", vec![doc! {"_id": 1}]);
    let cap = kimmy_core::limits::MAX_DOCUMENT_BYTES;
    let pad = "x".repeat(cap / 2);
    seed_docs(&state, "big", (0..4).map(|i| doc! {"_id": i, "pad": pad.clone()}).collect());
    let err = run(&state, json!([{"$unionWith": "big"}])).expect_err("over the cap");
    // Two half-cap documents are over the cap with their keys: refused at the second, at
    // roughly the cap, not after reading all four.
    // "... answer of N bytes ..." is how AnswerTooLarge renders; the first number over a
    // thousand is the bytes held when it was refused.
    let bytes = err
        .message
        .split(|c: char| !c.is_ascii_digit())
        .filter_map(|w| w.parse::<usize>().ok())
        .find(|n| *n > 1000)
        .unwrap_or(0);
    assert!(bytes > cap && bytes < cap + cap / 2, "{bytes} for a cap of {cap}");
}

#[test]
fn a_union_with_is_planned_and_listed_by_explain() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir);
    let out = crate::explain::aggregate_explain(
        &state,
        &root(),
        "app",
        "orders",
        &json!([{"$unionWith": {"coll": "archive", "pipeline": [{"$match": {"n": 1}}]}}]),
    )
    .unwrap();
    let stage = &out["explain"]["stages"][0];
    assert_eq!(stage["stage"], "$unionWith");
    assert_eq!(stage["from"], "archive");
    assert_eq!(stage["runs"], "foreignScan");
    assert_eq!(stage["stages"][0]["stage"], "$match");
}
