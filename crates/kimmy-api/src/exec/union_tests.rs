//! `$unionWith` (ADR-263), run by the caller: the order of its output, the sub-pipeline,
//! a collection that is not there, and the document and byte limits it is held to.

use bson::doc;
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

/// 100 documents of 200 KB: 20 MB, over what a stored document may be.
fn fat(state: &SharedState) {
    let pad = "x".repeat(200 * 1024);
    seed_docs(state, "fat", (0..100).map(|i| doc! {"_id": i, "pad": pad.clone()}).collect());
}

#[test]
fn a_collection_over_16_mib_that_the_sub_pipeline_projects_down_is_answered() {
    // What is scanned is held to the document limit only, as a `$lookup`'s foreign read
    // is; the byte cap is on what the stage appends.
    let dir = tempfile::tempdir().unwrap();
    let state = live_state(&dir);
    seed_docs(&state, "orders", vec![doc! {"_id": -1}]);
    fat(&state);
    for pipeline in [
        json!([{"$project": {"_id": 1}}]),
        json!([{"$match": {"_id": {"$lt": 3}}}, {"$project": {"_id": 1}}]),
        json!([{"$sort": {"_id": -1}}, {"$limit": 5}, {"$project": {"_id": 1}}]),
        json!([{"$group": {"_id": null, "n": {"$sum": 1}}}]),
    ] {
        let out =
            run(&state, json!([{"$unionWith": {"coll": "fat", "pipeline": pipeline.clone()}}]))
                .unwrap_or_else(|e| panic!("{pipeline}: {e:?}"));
        assert!(out["count"].as_u64().unwrap() >= 2, "{pipeline}: {out}");
    }
    // The same collection through `$lookup` is answered, with the same read.
    let out = run(
        &state,
        json!([{"$lookup": {"from": "fat", "pipeline": [{"$project": {"_id": 1}}], "as": "j"}}]),
    )
    .unwrap();
    assert_eq!(out["documents"][0]["j"].as_array().unwrap().len(), 100);
}

#[test]
fn what_the_stage_appends_is_held_to_the_byte_cap_with_a_remedy_that_works() {
    let dir = tempfile::tempdir().unwrap();
    let state = live_state(&dir);
    seed_docs(&state, "orders", vec![doc! {"_id": -1}]);
    fat(&state);
    // All 20 MB appended: refused, at the document that passes the cap.
    let err = run(&state, json!([{"$unionWith": "fat"}])).expect_err("over the cap");
    assert_eq!(err.status, 413, "{err:?}");
    assert!(err.message.contains("$unionWith appended documents"), "{err:?}");
    assert!(err.message.contains("end the sub-pipeline with a $project"), "{err:?}");
    // The remedy the message names works: projected down, or limited.
    for tail in [json!({"$project": {"_id": 1}}), json!({"$limit": 5})] {
        let out =
            run(&state, json!([{"$unionWith": {"coll": "fat", "pipeline": [tail]}}])).unwrap();
        assert!(out["count"].as_u64().unwrap() >= 2);
    }
    // And a sub-pipeline that makes what it appends bigger than what it read is held too.
    let err = run(
        &state,
        json!([{"$unionWith": {"coll": "fat", "pipeline": [
            {"$match": {"_id": {"$lt": 50}}}, {"$addFields": {"again": "$pad"}}]}}]),
    )
    .expect_err("the output is over");
    assert_eq!(err.status, 413, "{err:?}");
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

#[test]
fn on_a_node_with_peers_a_missing_collection_is_the_lookup_404_with_the_retry_hint() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir);
    let members = kimmy_cluster::Members::default();
    state.set_members(members.clone());
    // No peer yet: nowhere else it could be, so empty.
    let out = run(&state, json!([{"$unionWith": "nowhere"}])).unwrap();
    assert_eq!(keys(&out), ["o1", "o2", "o3"]);
    members.insert_for_test("127.0.0.1:7001".parse().unwrap(), kimmy_core::NodeId::generate());
    // A peer may hold it: the same refusal `$lookup` gives, with its hint.
    let union = run(&state, json!([{"$unionWith": "nowhere"}])).expect_err("a peer may have it");
    let lookup = run(
        &state,
        json!([{"$lookup": {"from": "nowhere", "localField": "k", "foreignField": "k", "as": "j"}}]),
    )
    .expect_err("the lookup");
    assert_eq!(union.status, 404, "{union:?}");
    assert_eq!(union.status, lookup.status);
    assert_eq!(union.message, lookup.message);
    assert_eq!(union.retry_override, lookup.retry_override);
    assert!(union.message.contains("not found on this member"), "{union:?}");
    // One that exists is read as before.
    let out = run(&state, json!([{"$unionWith": "archive"}])).unwrap();
    assert_eq!(keys(&out).len(), 6);
    // The same inside a nested sub-pipeline, and a denied caller still gets 403 first (api test).
    let nested = run(
        &state,
        json!([{"$unionWith": {"coll": "archive", "pipeline": [{"$unionWith": "nowhere"}]}}]),
    )
    .expect_err("nested");
    assert_eq!(nested.status, 404);
}

/// The probe's own collections and the answers the reference (6.0.5) gave to
/// `q8-probes/stages.js`, section by section.
#[test]
fn the_references_answers_to_the_union_with_sections() {
    let dir = tempfile::tempdir().unwrap();
    let state = live_state(&dir);
    seed_docs(
        &state,
        "stg",
        vec![
            doc! {"_id": 1, "k": "a", "n": 1, "a": {"x": 1, "y": 2}, "items": [{"sku": 1, "q": 5}, {"sku": 2}, 7]},
            doc! {"_id": 2, "k": "b", "n": 2, "a": {"x": 3, "y": 4}},
            doc! {"_id": 3, "k": "a", "n": 3},
            doc! {"_id": 4, "k": "c", "n": 4},
            doc! {"_id": 5, "k": "a", "n": 5},
            doc! {"_id": 6, "n": 6},
            doc! {"_id": 7, "k": bson::Bson::Null, "n": 7},
        ],
    );
    seed_docs(
        &state,
        "stg_other",
        vec![
            doc! {"_id": 10, "k": "u1", "n": 1},
            doc! {"_id": 11, "k": "u2", "n": 2},
            doc! {"_id": 12, "k": "u3", "n": 3},
        ],
    );
    let go = |pipeline: Value| {
        agg(&state, &root(), "app", "stg", &pipeline).unwrap()["documents"].clone()
    };

    assert_eq!(
        go(
            json!([{"$match": {"_id": {"$lte": 2}}}, {"$unionWith": "stg_other"}, {"$project": {"k": 1}}])
        ),
        json!([{"_id": 1, "k": "a"}, {"_id": 2, "k": "b"}, {"_id": 10, "k": "u1"}, {"_id": 11, "k": "u2"}, {"_id": 12, "k": "u3"}])
    );
    assert_eq!(
        go(
            json!([{"$match": {"_id": 1}}, {"$unionWith": {"coll": "stg_other"}}, {"$project": {"k": 1}}])
        ),
        json!([{"_id": 1, "k": "a"}, {"_id": 10, "k": "u1"}, {"_id": 11, "k": "u2"}, {"_id": 12, "k": "u3"}])
    );
    let out = go(json!([{"$match": {"_id": 1}}, {"$unionWith": {"coll": "stg_other", "pipeline": [
        {"$match": {"n": {"$gte": 2}}}, {"$project": {"_id": 0, "k": 1}}]}}]));
    assert_eq!(out[0]["_id"], 1);
    assert_eq!(out[0]["items"], json!([{"sku": 1, "q": 5}, {"sku": 2}, 7]));
    assert_eq!(out.as_array().unwrap()[1..], [json!({"k": "u2"}), json!({"k": "u3"})]);
    assert_eq!(
        go(json!([{"$match": {"_id": 999}}, {"$unionWith": "stg_other"}, {"$count": "n"}])),
        json!([{"n": 3}])
    );
    assert_eq!(
        go(json!([{"$match": {"_id": 1}}, {"$unionWith": "nowhere"}, {"$count": "n"}])),
        json!([{"n": 1}])
    );
    assert_eq!(
        go(json!([{"$match": {"_id": 1}}, {"$unionWith": "stg"}, {"$count": "n"}])),
        json!([{"n": 8}])
    );
    assert_eq!(
        go(json!([{"$match": {"_id": 1}}, {"$unionWith": {"coll": "stg_other", "pipeline": [
            {"$match": {"n": 1}}, {"$unionWith": {"coll": "stg", "pipeline": [{"$match": {"_id": 2}}]}}]}},
            {"$project": {"k": 1}}])),
        json!([{"_id": 1, "k": "a"}, {"_id": 10, "k": "u1"}, {"_id": 2, "k": "b"}])
    );
    assert_eq!(
        go(
            json!([{"$match": {"_id": 1}}, {"$facet": {"f": [{"$unionWith": "stg_other"}, {"$count": "n"}]}}])
        ),
        json!([{"f": [{"n": 4}]}])
    );
    assert_eq!(
        go(
            json!([{"$match": {"_id": 1}}, {"$lookup": {"from": "stg_other", "as": "j", "pipeline": [
            {"$unionWith": {"coll": "stg", "pipeline": [{"$match": {"_id": 2}}]}}, {"$project": {"k": 1}}]}},
            {"$project": {"j": 1}}])
        ),
        json!([{"_id": 1, "j": [{"_id": 10, "k": "u1"}, {"_id": 11, "k": "u2"}, {"_id": 12, "k": "u3"}, {"_id": 2, "k": "b"}]}])
    );
    assert_eq!(
        go(
            json!([{"$match": {"_id": {"$in": [1, 2]}}}, {"$unionWith": "stg_other"}, {"$sort": {"n": -1, "_id": 1}}, {"$project": {"k": 1}}])
        ),
        json!([{"_id": 12, "k": "u3"}, {"_id": 2, "k": "b"}, {"_id": 11, "k": "u2"}, {"_id": 1, "k": "a"}, {"_id": 10, "k": "u1"}])
    );
    // "facet inside": the seven documents, then the one the facet makes of the other collection.
    let out =
        go(json!([{"$unionWith": {"coll": "stg_other", "pipeline": [{"$facet": {"f": []}}]}}]));
    let out = out.as_array().unwrap();
    assert_eq!(out.len(), 8);
    assert_eq!(
        out[7],
        json!({"f": [{"_id": 10, "k": "u1", "n": 1}, {"_id": 11, "k": "u2", "n": 2}, {"_id": 12, "k": "u3", "n": 3}]})
    );
}

#[test]
fn a_missing_collection_is_one_rule_for_lookup_graph_lookup_and_union_with() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir);
    let lookup_eq = json!([{"$lookup": {"from": "nowhere", "localField": "k", "foreignField": "k", "as": "j"}}]);
    let lookup_pipe =
        json!([{"$lookup": {"from": "nowhere", "pipeline": [{"$match": {}}], "as": "j"}}]);
    let graph = json!([{"$graphLookup": {"from": "nowhere", "startWith": "$k",
        "connectFromField": "k", "connectToField": "k", "as": "j", "maxDepth": 2}}]);
    let union = json!([{"$unionWith": "nowhere"}]);

    // On a node with no peers there is nowhere else it could be: an empty read.
    for pipeline in [&lookup_eq, &lookup_pipe, &graph] {
        let out = run(&state, pipeline.clone()).unwrap_or_else(|e| panic!("{pipeline}: {e:?}"));
        assert_eq!(out["count"], 3, "{pipeline}");
        for doc in out["documents"].as_array().unwrap() {
            assert_eq!(doc["j"], json!([]), "{pipeline}");
        }
    }
    assert_eq!(run(&state, union.clone()).unwrap()["count"], 3);

    // On a node with peers it may be about to arrive: the 404 with the retry hint, for each.
    let members = kimmy_cluster::Members::default();
    state.set_members(members.clone());
    members.insert_for_test("127.0.0.1:7001".parse().unwrap(), kimmy_core::NodeId::generate());
    let mut seen = Vec::new();
    for pipeline in [&lookup_eq, &lookup_pipe, &graph, &union] {
        let err = run(&state, pipeline.clone()).expect_err("a peer may have it");
        assert_eq!(err.status, 404, "{pipeline}: {err:?}");
        assert!(err.message.contains("not found on this member"), "{pipeline}: {err:?}");
        seen.push((err.message.clone(), err.retry_override));
    }
    assert!(seen.windows(2).all(|w| w[0] == w[1]), "one message and hint for all: {seen:?}");
    // A collection that is there is read as ever, and a denied one is the 403 first.
    let out = run(&state, json!([{"$lookup": {"from": "archive", "localField": "k", "foreignField": "k", "as": "j"}}])).unwrap();
    assert_eq!(out["count"], 3);
}
