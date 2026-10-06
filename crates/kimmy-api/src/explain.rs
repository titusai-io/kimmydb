//! A plan-only `explain` for `aggregate` (ADR-221).
//!
//! **It runs nothing.** It reads the parsed pipeline, the definitions of the
//! collections the pipeline names and the leading filter's access path, which
//! [`exec::plan_access`] decides from the filter and the index definitions alone:
//! the same function the read itself asks, so the two cannot diverge. No
//! document, no index entry and no oplog record is read, no embedding provider is
//! called, and nothing is written (`aggregate` is a read, and `$out` and `$merge`
//! do not exist). The answer has no `documents` and no `count`, and none of the
//! measured keys of `find`'s explain (`documentsExamined`, `documentsMatched`,
//! `indexEntriesRead`, ...): nothing was examined, matched or read.
//!
//! **Authorization is the run's, and a little more.** `read` on the collection,
//! `read` on every collection a `$lookup` names, a nested `$lookup`'s included,
//! and `search` for a `$vectorSearch`, in the order a run takes them: the source,
//! then the parse (a `400`), then the foreign collections. A run does not reach a
//! nested `$lookup` when no document reaches it, so explain can be stricter than
//! a run there, and says so in the documentation.

use kimmy_query::Filter;
use kimmy_query::aggregate::{self, Join, Stage};
use serde_json::{Value, json};

use crate::error::ApiError;
use crate::exec::{self, Access, QueryStats};
use crate::state::{Auth, SharedState};

/// Plan `pipeline` over `db.coll` without running it.
pub fn aggregate_explain(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    pipeline: &Value,
) -> Result<Value, ApiError> {
    aggregate_explain_with_limits(state, auth, db, coll, pipeline, aggregate::Limits::default())
}

/// [`aggregate_explain`] under an explicit ceiling, which it reports.
pub fn aggregate_explain_with_limits(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    pipeline: &Value,
    limits: aggregate::Limits,
) -> Result<Value, ApiError> {
    let _span = exec::op_span("aggregate_explain", db, Some(coll)).entered();
    let meta = exec::authorize(state, auth, kimmy_auth::Action::Read, db, coll)?;
    let stages = exec::parse_pipeline(pipeline)?;

    // Every collection the pipeline names, in the order a run would reach them.
    let mut foreign = Vec::new();
    collect_foreign(&stages, &mut foreign);
    if matches!(stages.first(), Some(Stage::VectorSearch(_))) {
        auth.require(kimmy_auth::Action::Search, db, Some(coll))?;
    }
    for from in foreign {
        exec::authorize(state, auth, kimmy_auth::Action::Read, db, from)?;
    }

    let (source, consumed) = match stages.first() {
        Some(Stage::VectorSearch(spec)) => (vector_source(&meta, spec), 1),
        _ => {
            let (filter, consumed) = match aggregate::leading_match(&stages) {
                Some((filter, consumed)) => (filter, consumed),
                None => (kimmy_query::Filter::AlwaysTrue, 0),
            };
            let access = exec::plan_access(&filter, &meta.indexes);
            let mut source = QueryStats::planned(&access).plan_json();
            source["stages"] = json!(consumed);
            source["maxDocuments"] = json!(limits.max_documents);
            // The one way a plan is overtaken: a range plan that intersected both
            // ends is re-read against the index's multikey flag in the snapshot the
            // scan runs in, and falls back to a collection scan if a write has made
            // the index multikey since the plan was chosen.
            if matches!(&access, Access::Index(p) if p.both_bounds) {
                source["note"] = json!(
                    "this range plan is re-checked when the scan runs and falls back to a \
                     collection scan if the index has become multikey since"
                );
            }
            (source, consumed)
        }
    };

    let rest: Vec<Value> = stages[consumed..].iter().map(|stage| describe(stage, false)).collect();
    Ok(json!({
        "explain": {
            "executed": false,
            "planner": "read the parsed pipeline and the definitions of the collections it \
                        names; no document, index entry or provider was touched",
            "source": source,
            "stages": rest,
        }
    }))
}

/// The collections a pipeline's `$lookup`s name, nested ones included, in the order
/// a run reaches them, each once.
fn collect_foreign<'a>(stages: &'a [Stage], out: &mut Vec<&'a str>) {
    for stage in stages {
        if let Stage::Lookup { from, join, .. } = stage {
            if !out.contains(&from.as_str()) {
                out.push(from);
            }
            if let Join::Pipeline { stages: sub, .. } = join {
                collect_foreign(sub, out);
            }
        }
    }
}

/// A `$vectorSearch` source, from the collection's configuration alone. It never
/// reads the shadow collection, so it cannot say whether any vector has been
/// stored (a run refuses an empty one with `409`); it says so.
fn vector_source(meta: &kimmy_storage::CollectionMeta, spec: &aggregate::VectorSearch) -> Value {
    let mut out = json!({
        "strategy": "vectorIndex",
        "stages": 1,
        "k": spec.k,
        "perDocument": spec.per_document,
        "filter": spec.filter.is_some(),
        "vectorsPresent": "not checked: a plan reads no stored vector",
    });
    let Some(config) = &meta.vector else {
        out["configured"] = json!(false);
        out["queryUsable"] = json!(false);
        out["queryNote"] = json!("the collection has no vector configuration");
        return out;
    };
    out["configured"] = json!(true);
    out["dimension"] = json!(config.dim);
    out["metric"] = serde_json::to_value(config.metric).unwrap_or(Value::Null);
    let embeds = config.provider.embeds_server_side();
    out["embedsServerSide"] = json!(embeds);
    let (usable, note) = match (&spec.vector, &spec.query) {
        (Some(vector), _) if vector.len() != config.dim => (
            false,
            format!(
                "the query vector has {} dimensions and the collection stores {}",
                vector.len(),
                config.dim
            ),
        ),
        (Some(_), _) => (true, "a supplied vector is used as it is".to_string()),
        (None, Some(_)) if !embeds => (
            false,
            "the collection uses client-supplied vectors, so the server cannot embed query text"
                .to_string(),
        ),
        (None, Some(_)) => (true, "the query text is embedded when the pipeline runs".to_string()),
        (None, None) => (false, "neither `query` nor `vector` was given".to_string()),
    };
    out["queryUsable"] = json!(usable);
    out["queryNote"] = json!(note);
    out
}

/// One stage after the source. `per_input` marks a stage of a `$lookup`
/// sub-pipeline, which runs once for each input document of that `$lookup`.
fn describe(stage: &Stage, per_input: bool) -> Value {
    let mut out = json!({ "stage": stage.name(), "runs": "inMemory" });
    let detail: String = match stage {
        Stage::Match(_) => "filters the documents that reach it, in memory; never planned, and no \
                            index is used"
            .into(),
        Stage::Project { .. } => "reshapes each document that reaches it".into(),
        Stage::AddFields(_) => "adds computed fields to each document that reaches it".into(),
        Stage::ReplaceRoot(_) => {
            "replaces each document that reaches it with a computed one".into()
        }
        Stage::Sort(keys) => {
            out["keys"] = json!(
                keys.iter()
                    .map(|k| format!("{}{}", if k.descending { "-" } else { "" }, k.path))
                    .collect::<Vec<_>>()
            );
            "sorts everything that reaches it in memory; blocking".into()
        }
        Stage::Limit(n) => {
            out["limit"] = json!(n);
            "keeps the first documents that reach it".into()
        }
        Stage::Skip(n) => {
            out["skip"] = json!(n);
            "drops the first documents that reach it".into()
        }
        Stage::Unwind { path, .. } => {
            out["path"] = json!(path);
            "emits one document per element of the array, which can grow its input".into()
        }
        Stage::Group { .. } => {
            "groups everything that reaches it in memory; blocking, with one output per group"
                .into()
        }
        Stage::Count(field) => {
            out["field"] = json!(field);
            "counts everything that reaches it and emits one document; blocking".into()
        }
        // Only ever first, where it is the source; described here for completeness.
        Stage::VectorSearch(_) => "ranks the vector index; a source, only ever first".into(),
        Stage::Lookup { from, as_field, join } => {
            out["from"] = json!(from);
            out["as"] = json!(as_field);
            out["runs"] = json!("foreignScan");
            match join {
                Join::Equality { local_field, foreign_field } => {
                    out["strategy"] = json!("foreignScanHashed");
                    out["localField"] = json!(local_field);
                    out["foreignField"] = json!(foreign_field);
                    out["readsForeign"] =
                        json!("once per execution of the stage, whatever reaches it");
                    "scans the foreign collection once and files it in memory by the join key; \
                     no index on the foreign field is used"
                        .into()
                }
                Join::Pipeline { vars, stages: sub, lead } => {
                    let joined = lead.as_ref().is_some_and(|l| l.key.is_some());
                    out["strategy"] =
                        json!(if joined { "foreignScanHashJoin" } else { "foreignScanNestedLoop" });
                    out["let"] =
                        json!(vars.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>());
                    out["readsForeign"] = json!(
                        "once per execution of the stage, and not at all when no document reaches it"
                    );
                    if let Some(key) = lead.as_ref().and_then(|l| l.key.as_ref()) {
                        out["joinKey"] = json!({
                            "foreign": key.foreign.describe(),
                            "local": key.local.describe(),
                        });
                        let residual = match lead.as_ref().map(|l| &l.residual) {
                            Some(Filter::And(clauses)) => clauses.len(),
                            Some(Filter::AlwaysTrue) | None => 0,
                            Some(_) => 1,
                        };
                        out["residualClauses"] = json!(residual);
                        out["fallback"] = json!(
                            "nested loop at run time if a key holds a Decimal128, or the build or a \
                             probe raises"
                        );
                    }
                    let mut inner = Vec::new();
                    for (i, stage) in sub.iter().enumerate() {
                        let mut d = describe(stage, true);
                        if i == 0 && matches!(stage, Stage::Match(_)) {
                            match lead {
                                // Reads no variable: applied once, as always.
                                None => {
                                    d["perInputDocument"] = json!(false);
                                    d["detail"] = json!(
                                        "an in-memory filter over the foreign collection, applied \
                                         once before the loop; never planned, and no index is used"
                                    );
                                }
                                // Reads a variable: split in two, the independent part once.
                                Some(lead) => {
                                    if !matches!(lead.independent, Filter::AlwaysTrue) {
                                        let mut once = d.clone();
                                        once["part"] = json!("independent");
                                        once["perInputDocument"] = json!(false);
                                        once["detail"] = json!(
                                            "the clauses that name no `let` variable: an in-memory \
                                             filter applied once to the foreign collection before \
                                             the loop; never planned, and no index is used"
                                        );
                                        inner.push(once);
                                    }
                                    d["part"] = json!("variable");
                                    d["perInputDocument"] = json!(true);
                                    d["detail"] = json!(if joined {
                                        "the clauses that name a `let` variable: one `$eq` is \
                                         answered from a hash table of the foreign collection \
                                         from the second input document on; the rest are checked \
                                         on what it finds"
                                    } else {
                                        "the clauses that name a `let` variable: checked against \
                                         every foreign document for each input document"
                                    });
                                }
                            }
                        }
                        inner.push(d);
                    }
                    out["stages"] = json!(inner);
                    if joined {
                        "joins on one `$eq` from the second input document on, through a hash \
                         table; the first input document is a nested loop"
                    } else {
                        "runs its sub-pipeline over the foreign collection once for each document \
                         that reaches it: a nested loop"
                    }
                    .into()
                }
            }
        }
    };
    out["detail"] = json!(detail);
    if per_input {
        out["perInputDocument"] = json!(true);
    }
    out
}

#[cfg(test)]
mod tests {
    use kimmy_auth::{Action, Grant, Principal};
    use kimmy_storage::IndexField;

    use super::*;

    fn live_state(dir: &tempfile::TempDir) -> SharedState {
        let engine = std::sync::Arc::new(
            kimmy_storage::Engine::open(&dir.path().join("kimmy.redb")).unwrap(),
        );
        let tokens =
            kimmy_auth::TokenIssuer::new("an-adequately-long-test-secret-for-hs256", 3600).unwrap();
        crate::state_with_egress(
            engine,
            tokens,
            false,
            crate::RateLimits::disabled(),
            crate::egress::EgressPolicy::public_only(crate::egress::WEBHOOKS),
        )
        .unwrap()
    }

    fn root() -> Auth {
        Auth(Principal::superuser("test"))
    }

    /// A caller who may read exactly `colls` of `app`.
    fn reader_of(colls: &[&str]) -> Auth {
        Auth(Principal::new(
            "reader",
            colls
                .iter()
                .map(|c| Grant {
                    db: "app".into(),
                    collection: (*c).into(),
                    actions: vec![Action::Read],
                })
                .collect(),
        ))
    }

    /// `app.<name>` of `count` documents `{_id: i, n: i % 5, k: i}`, indexed on `n`.
    fn seed(state: &SharedState, name: &str, count: i64, indexed: bool) {
        state.engine.create_collection("app", name).unwrap();
        if indexed {
            state
                .engine
                .create_index("app", name, vec![IndexField::ascending("n")], false, None)
                .unwrap();
        }
        let meta = state.engine.get_collection("app", name).unwrap();
        for i in 0..count {
            state.engine.insert(&meta, bson::doc! { "_id": i, "n": i % 5, "k": i }).unwrap();
        }
    }

    fn explain(
        state: &SharedState,
        auth: &Auth,
        coll: &str,
        pipeline: Value,
    ) -> Result<Value, ApiError> {
        aggregate_explain(state, auth, "app", coll, &pipeline)
    }

    fn run_stats(
        state: &SharedState,
        coll: &str,
        pipeline: &Value,
    ) -> Result<(Value, Option<QueryStats>), ApiError> {
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(
            exec::aggregate_run_stats(
                state,
                &root(),
                "app",
                coll,
                pipeline,
                aggregate::Limits::default(),
            ),
        )
    }

    fn plan_keys(v: &Value) -> Value {
        json!({
            "strategy": v["strategy"], "index": v["index"],
            "indexFieldsUsed": v["indexFieldsUsed"], "probes": v.get("probes"),
        })
    }

    /// What the read does is what explain says: for each shape of filter, the stats
    /// a real aggregate's source read answers with have the plan explain reports.
    #[test]
    fn the_source_plan_is_the_one_the_read_takes() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 60, true);
        let filters = [
            json!({}),
            json!({ "_id": 7 }),
            // Both a primary key and an index apply: the key wins.
            json!({ "_id": 7, "n": 2 }),
            json!({ "_id": { "$in": [1, 2, 3] } }),
            json!({ "n": 2 }),
            json!({ "n": { "$gt": 1, "$lt": 4 } }),
            json!({ "n": { "$in": [1, 3] } }),
            json!({ "k": 9 }),
            json!({ "k": { "$gt": 3 }, "n": 1 }),
        ];
        let mut strategies = std::collections::BTreeSet::new();
        for filter in filters {
            let pipeline = json!([{ "$match": filter }, { "$limit": 1 }]);
            let (_, stats) = run_stats(&state, "a", &pipeline).unwrap();
            let ran = stats.expect("a filtered source").to_json();
            let planned = explain(&state, &root(), "a", pipeline.clone()).unwrap();
            let source = &planned["explain"]["source"];
            assert_eq!(plan_keys(source), plan_keys(&ran), "{filter}");
            assert_eq!(source["stages"], 1, "{filter}");
            strategies.insert(source["strategy"].as_str().unwrap().to_string());
        }
        // The table reaches every access path, or the comparison proves less.
        for expected in ["idLookup", "index", "indexUnion", "collectionScan"] {
            assert!(strategies.contains(expected), "no filter planned {expected}: {strategies:?}");
        }
    }

    /// The access path has an order, and it is the one thing `explain` and the read
    /// share, so a test must pin it where both would move together: the primary key
    /// beats an index that also applies, and an index beats a scan.
    #[test]
    fn the_primary_key_beats_an_index_and_an_index_beats_a_scan() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 10, true);
        let meta = state.engine.get_collection("app", "a").unwrap();
        let access = |filter: Value| {
            let filter = kimmy_query::filter::parse(&json_to_doc(filter)).unwrap();
            exec::plan_access(&filter, &meta.indexes)
        };
        assert!(matches!(access(json!({ "_id": 7, "n": 2 })), Access::IdLookup(_)));
        assert!(matches!(access(json!({ "n": 2 })), Access::Index(_)));
        assert!(matches!(access(json!({ "k": 2 })), Access::Scan));
    }

    fn json_to_doc(value: Value) -> bson::Document {
        match crate::json::json_to_bson(&value).unwrap() {
            bson::Bson::Document(doc) => doc,
            other => panic!("{other:?}"),
        }
    }

    /// A pipeline with no leading `$match` plans the collection, and says no stage
    /// was taken into the source.
    #[test]
    fn no_leading_match_is_a_collection_scan_that_took_no_stage() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 10, true);
        let out = explain(
            &state,
            &root(),
            "a",
            json!([{ "$sort": { "k": 1 } }, { "$match": { "n": 1 } }]),
        )
        .unwrap();
        let source = &out["explain"]["source"];
        assert_eq!(
            (source["strategy"].clone(), source["stages"].clone()),
            (json!("collectionScan"), json!(0))
        );
        // The `$match` after another stage is an in-memory filter, never planned.
        let stages = out["explain"]["stages"].as_array().unwrap();
        assert_eq!(stages[1]["stage"], "$match");
        assert!(stages[1]["detail"].as_str().unwrap().contains("never planned"));
    }

    /// The answer says it ran nothing, has no documents, and none of the keys that
    /// say what a scan found.
    #[test]
    fn the_answer_has_no_documents_and_no_measured_keys() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 20, true);
        let out = explain(&state, &root(), "a", json!([{ "$match": { "n": 2 } }])).unwrap();
        assert!(out.get("documents").is_none() && out.get("count").is_none(), "{out}");
        assert_eq!(out["explain"]["executed"], false);
        let text = out.to_string();
        for measured in [
            "documentsExamined",
            "documentsMatched",
            "indexEntriesRead",
            "unkeyedCandidates",
            "undecidableCandidates",
        ] {
            assert!(!text.contains(measured), "{measured} in {text}");
        }
        assert_eq!(out["explain"]["source"]["maxDocuments"], aggregate::DEFAULT_MAX_DOCUMENTS);
    }

    /// Nothing runs: pipelines that a run refuses or fails on data are planned.
    #[test]
    fn a_pipeline_that_would_fail_when_run_is_planned() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 20, true);
        for pipeline in [
            // Fails on every document when run.
            json!([{ "$addFields": { "x": { "$divide": ["$k", 0] } } }]),
            json!([{ "$match": { "$expr": { "$divide": ["$k", 0] } } }]),
        ] {
            assert!(run_stats(&state, "a", &pipeline).is_err(), "a run fails: {pipeline}");
            explain(&state, &root(), "a", pipeline.clone())
                .unwrap_or_else(|e| panic!("{pipeline}: {e:?}"));
        }
        // Over the ceiling a run refuses; a plan has nothing to count.
        let tight = aggregate::Limits { max_documents: 3 };
        let pipeline = json!([{ "$match": {} }]);
        let run = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(exec::aggregate_with_limits(&state, &root(), "app", "a", &pipeline, tight));
        assert!(run.is_err());
        let planned =
            aggregate_explain_with_limits(&state, &root(), "app", "a", &pipeline, tight).unwrap();
        assert_eq!(planned["explain"]["source"]["maxDocuments"], 3);
    }

    /// The multikey note is made only for a range plan that intersected both ends,
    /// the one plan the scan can overtake.
    #[test]
    fn the_multikey_note_appears_only_for_a_plan_with_both_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 30, true);
        let note_of = |filter: Value| {
            let out = explain(&state, &root(), "a", json!([{ "$match": filter }])).unwrap();
            (
                out["explain"]["source"]["strategy"].as_str().unwrap().to_string(),
                out["explain"]["source"].get("note").is_some(),
            )
        };
        assert_eq!(note_of(json!({ "n": { "$gt": 1, "$lt": 4 } })), ("index".into(), true));
        assert_eq!(note_of(json!({ "n": 2 })), ("index".into(), false));
        assert_eq!(note_of(json!({ "n": { "$in": [1, 3] } })), ("indexUnion".into(), false));
        assert_eq!(note_of(json!({ "n": { "$gt": 1 } })), ("index".into(), false));
        assert_eq!(note_of(json!({ "k": 3 })), ("collectionScan".into(), false));
    }

    /// A partial index the filter does not provably fall within is not planned.
    #[test]
    fn an_unprovable_partial_index_falls_back_to_a_collection_scan() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        state.engine.create_collection("app", "p").unwrap();
        state
            .engine
            .create_index_with(
                "app",
                "p",
                vec![IndexField::ascending("n")],
                false,
                kimmy_storage::Enforcement::Local,
                None,
                None,
                Some(bson::doc! { "k": { "$gt": 5 } }),
            )
            .unwrap();
        let strategy = |filter: Value| {
            let out = explain(&state, &root(), "p", json!([{ "$match": filter }])).unwrap();
            out["explain"]["source"]["strategy"].as_str().unwrap().to_string()
        };
        assert_eq!(strategy(json!({ "n": 1 })), "collectionScan", "k > 5 is not implied");
        assert_eq!(strategy(json!({ "n": 1, "k": { "$gt": 9 } })), "index");
    }

    /// Every stage kind, in order, with what it says.
    #[test]
    fn every_stage_kind_is_described_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 10, true);
        seed(&state, "b", 10, false);
        let pipeline = json!([
            { "$match": { "n": 1 } },
            { "$addFields": { "x": 1 } },
            { "$project": { "x": 1 } },
            { "$replaceRoot": { "newRoot": { "x": "$x" } } },
            { "$sort": { "x": -1 } },
            { "$skip": 1 },
            { "$limit": 5 },
            { "$unwind": "$x" },
            { "$group": { "_id": "$x", "c": { "$sum": 1 } } },
            { "$count": "total" },
        ]);
        let out = explain(&state, &root(), "a", pipeline).unwrap();
        let stages = out["explain"]["stages"].as_array().unwrap();
        let names: Vec<_> = stages.iter().map(|s| s["stage"].as_str().unwrap()).collect();
        assert_eq!(
            names,
            [
                "$addFields",
                "$project",
                "$replaceRoot",
                "$sort",
                "$skip",
                "$limit",
                "$unwind",
                "$group",
                "$count"
            ]
        );
        assert_eq!(stages[3]["keys"], json!(["-x"]));
        assert_eq!((stages[4]["skip"].clone(), stages[5]["limit"].clone()), (json!(1), json!(5)));
        assert_eq!(stages[8]["field"], "total");
        assert!(stages.iter().all(|s| s["runs"] == "inMemory"));
        assert!(stages[7]["detail"].as_str().unwrap().contains("blocking"));
    }

    /// The two `$lookup` forms say what the executor does, including where it reads
    /// nothing, and what is hoisted.
    #[test]
    fn lookups_say_what_the_executor_does() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 10, true);
        seed(&state, "b", 10, false);
        seed(&state, "c", 10, false);
        let pipeline = json!([
            { "$lookup": { "from": "b", "localField": "n", "foreignField": "n", "as": "eq" } },
            { "$lookup": {
                "from": "c", "let": { "n": "$n" }, "as": "pl",
                "pipeline": [
                    { "$match": { "k": { "$gt": 2 } } },
                    { "$addFields": { "hit": { "$eq": ["$n", "$$n"] } } },
                    { "$lookup": { "from": "b", "localField": "n", "foreignField": "n", "as": "inner" } },
                ],
            } },
        ]);
        let out = explain(&state, &root(), "a", pipeline).unwrap();
        let stages = out["explain"]["stages"].as_array().unwrap();
        assert_eq!(stages[0]["strategy"], "foreignScanHashed");
        assert!(stages[0]["readsForeign"].as_str().unwrap().contains("whatever reaches it"));
        assert!(stages[0]["detail"].as_str().unwrap().contains("no index on the foreign field"));
        assert!(stages[0].get("perInputDocument").is_none());
        assert_eq!(stages[1]["strategy"], "foreignScanNestedLoop");
        assert!(
            stages[1]["readsForeign"]
                .as_str()
                .unwrap()
                .contains("not at all when no document reaches it")
        );
        assert_eq!(stages[1]["let"], json!(["n"]));
        let inner = stages[1]["stages"].as_array().unwrap();
        assert_eq!(inner[0]["perInputDocument"], false, "the leading $match is hoisted");
        assert!(inner[0]["detail"].as_str().unwrap().contains("never planned"));
        assert_eq!(inner[1]["perInputDocument"], true);
        assert_eq!(
            (inner[2]["stage"].clone(), inner[2]["perInputDocument"].clone()),
            (json!("$lookup"), json!(true))
        );
    }

    /// A correlated `$eq` in a sub-pipeline `$match` is planned as a hash join, with the
    /// independent part of the `$match` its own entry (ADR-222); a variable-reading
    /// `$match` with no `$eq` to join on is the nested loop, and says its clauses run
    /// for every input document.
    #[test]
    fn a_correlated_lookup_says_it_is_a_hash_join() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 10, true);
        seed(&state, "b", 10, false);
        let join = json!([{ "$lookup": { "from": "b", "let": { "n": "$n" }, "as": "j",
            "pipeline": [
                { "$match": { "$and": [
                    { "k": { "$gt": 2 } },
                    { "$expr": { "$lte": ["$k", "$$n"] } },
                    { "$expr": { "$eq": ["$n", "$$n"] } } ] } },
                { "$limit": 3 } ] } }]);
        let out = explain(&state, &root(), "a", join).unwrap();
        let lookup = &out["explain"]["stages"].as_array().unwrap()[0];
        assert_eq!(lookup["strategy"], "foreignScanHashJoin");
        assert_eq!(lookup["joinKey"], json!({ "foreign": "$n", "local": "$$n" }));
        assert_eq!(lookup["residualClauses"], 1);
        assert!(lookup["fallback"].as_str().unwrap().contains("Decimal128"));
        let inner = lookup["stages"].as_array().unwrap();
        assert_eq!(inner[0]["part"], "independent");
        assert_eq!(inner[0]["perInputDocument"], false);
        assert_eq!(inner[1]["part"], "variable");
        assert_eq!(inner[1]["perInputDocument"], true);
        assert_eq!(inner[2]["stage"], "$limit");
        assert_eq!(inner[2]["perInputDocument"], true);

        let no_join = json!([{ "$lookup": { "from": "b", "let": { "n": "$n" }, "as": "j",
            "pipeline": [{ "$match": { "$expr": { "$gt": ["$k", "$$n"] } } }] } }]);
        let out = explain(&state, &root(), "a", no_join).unwrap();
        let lookup = &out["explain"]["stages"].as_array().unwrap()[0];
        assert_eq!(lookup["strategy"], "foreignScanNestedLoop");
        assert!(lookup.get("joinKey").is_none());
        let inner = lookup["stages"].as_array().unwrap();
        assert_eq!(inner.len(), 1, "no independent part, so one entry");
        assert_eq!(inner[0]["perInputDocument"], true);
        assert!(inner[0]["detail"].as_str().unwrap().contains("every foreign document"));
    }

    /// Authorization is a run's, in a run's order, and covers every collection named,
    /// a nested `$lookup`'s too.
    #[test]
    fn authorization_covers_every_collection_named_in_a_runs_order() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 3, false);
        seed(&state, "b", 3, false);
        seed(&state, "c", 3, false);
        let lookup = |from: &str| json!({ "$lookup": { "from": from, "localField": "n", "foreignField": "n", "as": "j" } });
        let nested = json!([{ "$lookup": { "from": "b", "let": {}, "as": "p", "pipeline": [lookup("c")] } }]);
        let status = |auth: &Auth, coll: &str, pipeline: Value| match explain(
            &state, auth, coll, pipeline,
        ) {
            Ok(_) => 200,
            Err(e) => e.status.as_u16(),
        };
        assert_eq!(status(&reader_of(&["a", "b", "c"]), "a", nested.clone()), 200);
        assert_eq!(
            status(&reader_of(&["a", "b"]), "a", nested.clone()),
            403,
            "the nested foreign side"
        );
        assert_eq!(status(&reader_of(&["a"]), "a", json!([lookup("b")])), 403);
        assert_eq!(status(&reader_of(&["b"]), "a", json!([])), 403, "the source");
        // A parse failure is a 400 before any foreign collection is looked at.
        assert_eq!(status(&reader_of(&["a"]), "a", json!([lookup("b"), { "$nope": 1 }])), 400);
        // A foreign collection that does not exist is a 404, as in a run.
        assert_eq!(status(&root(), "a", json!([lookup("missing")])), 404);
        assert_eq!(status(&root(), "missing", json!([])), 404);
    }

    /// A `$vectorSearch` source is described from the configuration only.
    #[test]
    fn a_vector_source_reports_configuration_and_whether_the_query_is_usable() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "plain", 1, false);
        seed(&state, "v", 1, false);
        state
            .engine
            .configure_vectors(
                "app",
                "v",
                kimmy_core::VectorConfig {
                    fields: vec!["text".into()],
                    provider: kimmy_core::ProviderConfig::Byo {},
                    dim: 3,
                    metric: Default::default(),
                    chunk: Default::default(),
                    document_prefix: None,
                    query_prefix: None,
                },
            )
            .unwrap();
        let source = |coll: &str, stage: Value| {
            explain(&state, &root(), coll, json!([{ "$vectorSearch": stage }])).unwrap()["explain"]
                ["source"]
                .clone()
        };
        let none = source("plain", json!({ "vector": [1.0, 0.0, 0.0] }));
        assert_eq!(
            (none["configured"].clone(), none["queryUsable"].clone()),
            (json!(false), json!(false))
        );
        let text = source("v", json!({ "query": "hello" }));
        assert_eq!(
            (text["queryUsable"].clone(), text["embedsServerSide"].clone()),
            (json!(false), json!(false))
        );
        let wrong = source("v", json!({ "vector": [1.0, 0.0] }));
        assert_eq!(wrong["queryUsable"], false);
        let right = source("v", json!({ "vector": [1.0, 0.0, 0.0], "k": 4 }));
        assert_eq!(
            (
                right["queryUsable"].clone(),
                right["dimension"].clone(),
                right["strategy"].clone(),
                right["k"].clone()
            ),
            (json!(true), json!(3), json!("vectorIndex"), json!(4))
        );
        assert!(right["vectorsPresent"].as_str().unwrap().contains("not checked"));
    }

    /// A caller granted only `search` cannot explain a `$vectorSearch` any more than
    /// run one: the source needs `read` first, as in a run, and `read` is what a
    /// `search` grant is implied by, not the other way round.
    #[test]
    fn a_vector_search_needs_what_a_run_needs() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "v", 1, false);
        let granted = |actions: Vec<Action>| {
            Auth(Principal::new(
                "u",
                vec![Grant { db: "app".into(), collection: "v".into(), actions }],
            ))
        };
        let pipeline = json!([{ "$vectorSearch": { "vector": [1.0, 0.0, 0.0] } }]);
        let status = |auth: &Auth| match explain(&state, auth, "v", pipeline.clone()) {
            Ok(_) => 200,
            Err(e) => e.status.as_u16(),
        };
        let run_status = |auth: &Auth| {
            let run = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(exec::aggregate(&state, auth, "app", "v", &pipeline));
            match run {
                Ok(_) => 200,
                Err(e) => e.status.as_u16(),
            }
        };
        let search_only = granted(vec![Action::Search]);
        assert_eq!(status(&search_only), 403);
        assert_eq!(run_status(&search_only), 403, "a run refuses it the same way");
        assert_eq!(status(&granted(vec![])), 403);
        assert_eq!(status(&granted(vec![Action::Read])), 200);
    }

    /// Explain walks nothing: with the walks armed to stop at the first row they
    /// check, a plan over a collection with documents, an index and a `$lookup`
    /// foreign side leaves them running, and the same pipeline run stops them.
    #[test]
    fn explain_reads_no_row_and_the_same_pipeline_run_does() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 30, true);
        seed(&state, "b", 30, false);
        let pipeline = json!([
            { "$match": { "n": 2 } },
            { "$lookup": { "from": "b", "localField": "n", "foreignField": "n", "as": "j" } },
            { "$lookup": { "from": "b", "let": { "n": "$n" }, "as": "k", "pipeline": [
                { "$match": { "$expr": { "$eq": ["$n", "$$n"] } } } ] } },
        ]);
        state.engine.stop_walks_after_rows(1);
        let planned = explain(&state, &root(), "a", pipeline.clone());
        assert!(planned.is_ok(), "{planned:?}");
        assert!(!state.engine.walks_stopping(), "explain walked a row");
        // The control: the same pipeline, run, does walk, and the armed stop fires.
        let run = run_stats(&state, "a", &pipeline);
        assert!(state.engine.walks_stopping(), "the run walked no row either: {:?}", run.is_ok());
    }
}
