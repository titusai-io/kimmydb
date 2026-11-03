//! A plan-only `explain` for `aggregate` (ADR-221).
//!
//! **It runs nothing.** It reads the parsed pipeline, the definitions of the
//! collections the pipeline names and the leading filter's access path, which
//! [`exec::plan_access`] decides from the filter and the index definitions alone:
//! the same function the read itself asks, so the two cannot diverge. No
//! document, no index entry and no oplog record is read, no embedding provider is
//! called, and nothing is written (`aggregate` is a read; a trailing `$out` or
//! `$merge` is planned and described, and neither writes nor creates its target). The answer has no `documents` and no `count`, and none of the
//! measured keys of `find`'s explain (`documentsExamined`, `documentsMatched`,
//! `indexEntriesRead`, ...): nothing was examined, matched or read.
//!
//! **Authorization is the run's, and a little more.** `read` on the collection,
//! `read` on every collection a `$lookup` names, a nested `$lookup`'s included,
//! `search` for a `$vectorSearch`, and for a trailing `$merge` or `$out` the run's
//! refusals and grants (a target the pipeline reads, `write` on it, `ddl` when it
//! does not exist), in the order a run takes them: the source,
//! then the parse (a `400`), then the foreign collections. A run does not reach a
//! nested `$lookup` when no document reaches it, so explain can be stricter than
//! a run there, and says so in the documentation.

use kimmy_query::Filter;
use kimmy_query::aggregate::{self, Join, Stage};
use kimmy_query::pipeline::{self, Hold, Kept};
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
    aggregate_explain_with_limits(state, auth, db, coll, pipeline, state.aggregate_limits())
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
    let foreign = aggregate::foreign_collections(&stages);
    if matches!(stages.first(), Some(Stage::VectorSearch(_))) {
        auth.require(kimmy_auth::Action::Search, db, Some(coll))?;
    }
    for from in foreign {
        exec::authorize(state, auth, kimmy_auth::Action::Read, db, from)?;
    }
    let sink = exec::plan_sink(state, auth, db, coll, &stages)?.is_some();
    // What the executor's chain runs: the stages after the source, and not the sink that
    // writes the answer (`$merge`, `$out`), which is not a stage of the chain.
    let body = stages.len() - usize::from(sink);

    let (source, consumed) = match stages.first() {
        Some(Stage::VectorSearch(spec)) => (vector_source(&meta, spec), 1),
        _ => {
            let (filter, consumed) = match aggregate::leading_match(&stages[..body]) {
                Some((filter, consumed)) => (filter, consumed),
                None => (kimmy_query::Filter::AlwaysTrue, 0),
            };
            let access = exec::plan_access(&filter, &meta.indexes);
            let mut source = QueryStats::planned(&access).plan_json();
            source["stages"] = json!(consumed);
            describe_source(&mut source, &access, &stages[consumed..body], limits.max_documents);
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

    let mut rest = describe_in(&stages[consumed..body], 0, false);
    if sink {
        // The documents are all held while the sink writes them, in the engine's chunks.
        let runs = Runs { hold: Hold::Blocks, folded: None };
        rest.push(describe(&stages[body], false, Some(runs)));
    }
    Ok(json!({
        "explain": {
            "executed": false,
            "planner": "read the parsed pipeline and the definitions of the collections it \
                        names; no document, index entry or provider was touched",
            "source": source,
            "stages": rest,
            // What a run may hold, and how much of the node's pool is in use now
            // (ADR-269): a plan reads these, it reserves nothing.
            "memory": {
                "limitBytes": limits.buffer.cap(),
                "poolBytes": state.aggregate_memory().pool().cap(),
                "poolUsedBytes": state.aggregate_memory().pool().used(),
            },
        }
    }))
}

/// What the read of a filtered source holds, from the same predicates the executor asks
/// (`scan_pipeline`): whether the first stage of `chain`, the stages after the leading `$match`,
/// takes the source a document at a time ([`pipeline::streams_source`]), what the pipeline
/// takes of it when that is known ([`pipeline::source_bound`]), and whether the access path is
/// an index range read in `_id` order, which holds its keys.
fn describe_source(source: &mut Value, access: &Access, chain: &[Stage], max_documents: usize) {
    let streams = pipeline::streams_source(chain);
    let nothing = streams && pipeline::takes_nothing(chain);
    source["streams"] = json!(streams);
    // The pipeline's ceiling, on every filtered source: a source read whole is held to it
    // (`the source read`), and what a stage that blocks holds and the answer are held to it
    // whether the source streams or not.
    source["maxDocuments"] = json!(max_documents);
    if nothing {
        source["holds"] = json!(
            "no document: the pipeline takes none of the source (a window that takes nothing), \
             and the run reads none"
        );
    } else if streams {
        source["holds"] = json!(
            "no document of the source: each is passed to the first stage as it is read, and the \
             read has no ceiling of its own; maxDocuments still holds what a stage that blocks \
             keeps and the answer"
        );
    } else {
        source["holds"] = json!(
            "every document the read admits, charged as `the source read` until the first stage \
             takes it; a read past maxDocuments is refused 400, and past the memory limit 413"
        );
    }
    if streams && let Some(bound) = pipeline::source_bound(chain) {
        source["sourceBound"] = json!(bound);
    }
    // An index range that is not one complete key (or a union of them) delivers its documents
    // in key order, not `_id` order, so a read in `_id` order collects the keys of the range.
    // A pipeline that takes nothing reads none of it.
    if !nothing && matches!(access, Access::Index(plan) if !plan.exact) {
        let keys = "charged to the request's memory as `the index range of $match`, and a range \
                    too large is refused 413";
        let read = "read whole, in time proportional to the range, to deliver its documents in \
                    _id order";
        source["indexRange"] = json!(match (streams, source["sourceBound"].as_u64()) {
            (true, Some(bound)) => format!(
                "{read}, in passes of {bound} keys (the pipeline takes at most that many \
                 documents of the source) and more of them while the filter rejects what they \
                 find, up to the whole range; the keys held are {keys}"
            ),
            (true, None) =>
                format!("{read}; every key of the range is held while it is read, {keys}"),
            (false, _) => format!(
                "{read}, in passes of up to maxDocuments + 1 candidates and more of them while \
                 the filter rejects what they find; the keys held are {keys}"
            ),
        });
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
        "streams": false,
        "holds": "the hits, at most k, held whole as the source read and handed to the first \
                  stage, which takes them out of it as it takes each",
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

/// A write stage's target as the plan reports it.
fn target_json(target: &aggregate::Target) -> Value {
    match &target.db {
        Some(db) => json!({ "db": db, "coll": target.coll }),
        None => json!(target.coll),
    }
}

/// How a stage of a chain runs, as the executor lays the chain out ([`pipeline::segments`]).
#[derive(Clone, Copy)]
struct Runs {
    hold: Hold,
    /// The stage it is part of, for a `$skip` or `$limit` that the heap of a `$sort` stands for.
    folded: Option<&'static str>,
}

/// What each stage of `chain` holds, one entry for each, from the executor's own layout of it.
fn runs_of(chain: &[Stage]) -> Vec<Runs> {
    let mut out = Vec::with_capacity(chain.len());
    for segment in pipeline::segments(chain) {
        let first = segment.stages.start;
        for at in segment.stages {
            let folded =
                (at > first && matches!(segment.hold, Hold::State(_))).then(|| chain[first].name());
            out.push(Runs { hold: segment.hold, folded });
        }
    }
    out
}

/// The entries of `stages`, the stages of one chain, the first `lead` of which are not part of
/// it (the leading `$match` stages of a `$lookup` or `$unionWith` sub-pipeline, which the
/// stage applies as it reads the foreign collection) and are described without a `hold`.
fn describe_in(stages: &[Stage], lead: usize, per_input: bool) -> Vec<Value> {
    let (leading, chain) = stages.split_at(lead);
    let runs = runs_of(chain);
    leading
        .iter()
        .map(|stage| describe(stage, per_input, None))
        .chain(chain.iter().zip(runs).map(|(stage, runs)| describe(stage, per_input, Some(runs))))
        .collect()
}

/// The `hold` of a stage and the words for it: whether it passes one document at a time, keeps a
/// state in place of its input, or holds all of its input before it emits any.
fn runs_json(out: &mut Value, runs: Runs, stage: &Stage) -> String {
    match runs.hold {
        Hold::Streams => {
            out["hold"] = json!("streams");
            "streams: passes each document on as it arrives, holding one at a time".into()
        }
        Hold::Blocks => {
            out["hold"] = json!("blocks");
            "blocks: holds its whole input before it emits any output".into()
        }
        Hold::State(kept) => {
            out["hold"] = json!("state");
            let (state, what) = match kept {
                Kept::Counter => ("counter", "a counter".to_string()),
                Kept::Groups if matches!(stage, Stage::Bucket { .. }) => (
                    "groups",
                    "its buckets, one for each range a value falls in (and the default)"
                        .to_string(),
                ),
                Kept::Groups => ("groups", "its groups, one for each distinct key".to_string()),
                Kept::Heap { skip, limit } => {
                    let documents = skip.saturating_add(limit);
                    out["heapDocuments"] = json!(documents);
                    (
                        "heap",
                        format!(
                            "a heap of the first {documents} documents of the sort, the $skip \
                             and $limit after it counted"
                        ),
                    )
                }
                Kept::Reservoir { size } => (
                    "reservoir",
                    format!("a reservoir of {size} documents, the size of the sample"),
                ),
            };
            out["state"] = json!(state);
            match runs.folded {
                Some(owner) => {
                    out["foldedInto"] = json!(owner);
                    format!(
                        "part of the {state} of the {owner} before it, which holds nothing else"
                    )
                }
                None => format!("holds a state, not its input: {what}"),
            }
        }
    }
}

/// One stage after the source. `per_input` marks a stage of a `$lookup`
/// sub-pipeline, which runs once for each input document of that `$lookup`. `runs` is how the
/// executor's chain treats it, and is `None` only for a stage the chain does not run.
fn describe(stage: &Stage, per_input: bool, runs: Option<Runs>) -> Value {
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
            "sorts the documents that reach it by the keys".into()
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
            "groups the documents that reach it by the `_id` expression, with one output per group"
                .into()
        }
        Stage::Count(field) => {
            out["field"] = json!(field);
            "counts the documents that reach it and emits one document".into()
        }
        // Only ever first, where it is the source; described here for completeness.
        Stage::VectorSearch(_) => "ranks the vector index; a source, only ever first".into(),
        Stage::GraphLookup(spec) => {
            out["from"] = json!(spec.from);
            out["as"] = json!(spec.as_field);
            out["runs"] = json!("foreignScan");
            out["maxDepth"] = json!(spec.max_depth);
            out["connectFromField"] = json!(spec.connect_from);
            out["connectToField"] = json!(spec.connect_to);
            out["readsForeign"] = json!(
                "once per execution of the stage, and not at all when no document reaches it"
            );
            "scans the foreign collection once, files it in memory by connectToField and walks \
             it per input document, each document at most once; what is reached counts against \
             the pipeline's document limit"
                .into()
        }
        Stage::UnionWith(spec) => {
            out["from"] = json!(spec.coll);
            out["runs"] = json!("foreignScan");
            // The leading `$match` stages are applied as the foreign collection is read.
            let lead = aggregate::leading_match(&spec.stages).map_or(0, |(_, consumed)| consumed);
            out["stages"] = json!(describe_in(&spec.stages, lead, false));
            out["readsForeign"] = json!(
                "once per execution of the stage, even when no document reaches it; a collection \
                 that does not exist reads as empty on a node with no peers"
            );
            "emits every document that reaches it, then the foreign collection's documents run \
             through the sub-pipeline; the collection is read whole and what is read counts \
             against the pipeline's document limit, as a $lookup's does; what the stage appends \
             (input plus appended, in documents) counts against it too, and the appended \
             documents' bytes against the 16 MiB a stored document may be; the input's bytes \
             are not charged"
                .into()
        }
        Stage::SortByCount(_) => {
            "groups the documents that reach it by the expression, counts each group and sorts the \
             groups by count, largest first"
                .into()
        }
        Stage::Sample(n) => {
            out["size"] = json!(n);
            "keeps this many documents chosen at random, in a random order; the pipeline's document \
             limit applies to what it keeps"
                .into()
        }
        Stage::Redact(_) => {
            "decides each document, and each embedded document below one that descends, by the \
             expression's answer: keep it whole, prune it, or descend into it"
                .into()
        }
        Stage::Bucket { boundaries, default, .. } => {
            out["boundaries"] = json!(boundaries.len());
            out["default"] = json!(default.is_some());
            "counts each document into the range its value falls in; a value in no range is \
             refused unless there is a default"
                .into()
        }
        Stage::BucketAuto { buckets, .. } => {
            out["buckets"] = json!(buckets);
            "sorts by the value and cuts the run into at most that many buckets of about equal \
             size, never splitting a value"
                .into()
        }
        Stage::SetWindowFields(spec) => {
            out["outputs"] = json!(spec.outputs.iter().map(|o| o.name.clone()).collect::<Vec<_>>());
            out["sortBy"] = json!(
                spec.sort
                    .iter()
                    .map(|k| format!("{}{}", if k.descending { "-" } else { "" }, k.path))
                    .collect::<Vec<_>>()
            );
            "files the input by partition, sorts each partition and computes every output from a \
             window of the documents around each one; the buffered documents and outputs are \
             held to a byte limit"
                .into()
        }
        Stage::Densify(spec) => {
            out["field"] = json!(spec.field);
            out["partitionByFields"] = json!(spec.partition_fields);
            "sorts each partition by the field and generates documents for the gaps; the \
             documents it would generate are counted against the pipeline's document limit \
             before any is made"
                .into()
        }
        Stage::Fill(spec) => {
            out["valueFields"] =
                json!(spec.values.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>());
            out["methodFields"] = json!(
                spec.window
                    .as_ref()
                    .map(|w| w.outputs.iter().map(|o| o.name.clone()).collect::<Vec<_>>())
                    .unwrap_or_default()
            );
            if spec.window.is_some() {
                "fills by `locf` or `linear` over sorted partitions, and by `value` where a field is \
                 null or missing"
                    .into()
            } else {
                "fills a null or missing field from an expression".into()
            }
        }
        Stage::Facet(facets) => {
            out["facets"] = json!(
                facets
                    .iter()
                    .map(
                        |(name, sub)| json!({ "name": name, "stages": describe_in(sub, 0, false) })
                    )
                    .collect::<Vec<_>>()
            );
            "runs every facet over the same input, in memory, and answers one document of \
             arrays; each facet's output counts against the pipeline's document limit"
                .into()
        }
        Stage::Merge(spec) => {
            out["into"] = target_json(&spec.into);
            out["on"] = json!("_id");
            out["whenMatched"] = json!(match spec.when_matched {
                aggregate::WhenMatched::Replace => "replace",
                aggregate::WhenMatched::Merge => "merge",
                aggregate::WhenMatched::KeepExisting => "keepExisting",
            });
            out["whenNotMatched"] = json!(if spec.insert_unmatched { "insert" } else { "discard" });
            out["runs"] = json!("write");
            "writes the pipeline's output into the target, matching each document to a stored \
             one by `_id`, in chunks of storage.multi_chunk_docs; not atomic, so a failure \
             leaves the earlier chunks written. A plan writes nothing and creates nothing"
                .into()
        }
        Stage::Out(target) => {
            out["into"] = target_json(target);
            out["runs"] = json!("write");
            "inserts the pipeline's output into the target, which must be new or empty (a run \
             refuses one that holds documents), in chunks of storage.multi_chunk_docs; not \
             atomic. A plan writes nothing and creates nothing"
                .into()
        }
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
                        out["fallback"] =
                            json!("nested loop at run time if the build or a probe raises");
                    }
                    let mut inner = Vec::new();
                    // The stage applies the first `$match` itself, against the foreign
                    // documents it holds; the rest are the chain it runs for each input.
                    let lead_match = usize::from(matches!(sub.first(), Some(Stage::Match(_))));
                    for (i, mut d) in describe_in(sub, lead_match, true).into_iter().enumerate() {
                        if i == 0 && lead_match == 1 {
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
    let detail = match runs {
        Some(runs) => format!("{detail}; {}", runs_json(&mut out, runs, stage)),
        None => detail,
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

    /// A `$limit` that fills ends the source read (ADR-270), so the stats of the read are of a
    /// partial one: how many it examined and matched are what it needed, and the plan, which
    /// is what `explain` is held to, is the one it reports all the same.
    #[test]
    fn a_limit_ends_the_read_and_the_plan_is_still_the_one_explain_reports() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 60, true);
        let cases = [
            // (the pipeline, the documents it needs of the source)
            (json!([{ "$limit": 3 }]), 3, "collectionScan"),
            (json!([{ "$match": { "n": 2 } }, { "$limit": 2 }]), 2, "index"),
            (json!([{ "$match": { "k": { "$gt": 3 } } }, { "$limit": 2 }]), 2, "collectionScan"),
            (
                json!([{ "$match": { "_id": { "$in": [1, 2, 3] } } }, { "$limit": 1 }]),
                1,
                "idLookup",
            ),
            (
                json!([{ "$match": { "n": 2 } }, { "$project": { "k": 1 } }, { "$limit": 2 }]),
                2,
                "index",
            ),
        ];
        for (pipeline, needs, strategy) in cases {
            let (answer, stats) = run_stats(&state, "a", &pipeline).unwrap();
            let ran = stats.expect("a filtered source").to_json();
            assert_eq!(answer["count"], needs, "{pipeline}");
            assert_eq!(ran["documentsMatched"], needs, "{pipeline}: {ran}");
            let examined = ran["documentsExamined"].as_u64().unwrap();
            assert!(examined < 20, "{pipeline}: read {examined} of 60 documents");
            let planned = explain(&state, &root(), "a", pipeline.clone()).unwrap();
            assert_eq!(plan_keys(&planned["explain"]["source"]), plan_keys(&ran), "{pipeline}");
            assert_eq!(planned["explain"]["source"]["strategy"], strategy, "{pipeline}");
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
            // Gone with ADR-238's removal of the undecidable run, and so absent
            // from every answer; named so its return is caught here too.
            "undecidableCandidates",
        ] {
            assert!(!text.contains(measured), "{measured} in {text}");
        }
        assert_eq!(out["explain"]["source"]["maxDocuments"], aggregate::DEFAULT_MAX_DOCUMENTS);
    }

    /// A plan reports what a run may hold and the state of the node's pool (ADR-269), and
    /// reserves nothing.
    #[test]
    fn a_plan_reports_the_memory_limit_and_the_pools_state() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 5, true);
        state.set_aggregate_memory(crate::AggregateMemory::new(5_000_000, 8_000_000));
        let pipeline = json!([{ "$match": { "n": 2 } }]);
        let out = explain(&state, &root(), "a", pipeline.clone()).unwrap();
        assert_eq!(
            out["explain"]["memory"],
            json!({ "limitBytes": 5_000_000, "poolBytes": 8_000_000, "poolUsedBytes": 0 })
        );
        // Another request holds 3 MB of the pool: the plan says so, and reserves none itself.
        let other = state.aggregate_limits();
        let mut lease = other.buffer.lease("another request");
        lease.charge(3_000_000).unwrap();
        let out = explain(&state, &root(), "a", pipeline).unwrap();
        assert_eq!(out["explain"]["memory"]["poolUsedBytes"], 3_000_000);
        assert_eq!(state.aggregate_memory().pool().used(), 3_000_000);
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
        let tight = aggregate::Limits { max_documents: 3, ..Default::default() };
        let pipeline = json!([{ "$match": {} }]);
        let run = tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(
            exec::aggregate_with_limits(&state, &root(), "app", "a", &pipeline, tight.clone()),
        );
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
        // A `$group` keeps its groups and not its input, a `$sort` that a `$limit` follows keeps
        // a heap, and a `$count` a counter (ADR-270): none of them blocks.
        let holds: Vec<_> = stages.iter().map(|s| s["hold"].as_str().unwrap()).collect();
        assert_eq!(
            holds,
            [
                "streams", "streams", "streams", "state", "state", "state", "streams", "state",
                "state"
            ]
        );
        assert_eq!(stages[7]["state"], "groups");
        assert_eq!(stages[8]["state"], "counter");
        assert!(!stages[7]["detail"].as_str().unwrap().contains("blocking"));
    }

    /// The stages a plan lists are the stages that run: a `$skip` and a `$limit` that are moved
    /// ahead of the stages that neither filter nor change the count (ADR-270) are listed there,
    /// coalesced, and a window behind a stage that blocks is listed behind it.
    #[test]
    fn a_window_is_listed_where_it_runs() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 10, true);
        let names = |pipeline: Value| -> Vec<String> {
            let out = explain(&state, &root(), "a", pipeline).unwrap();
            out["explain"]["stages"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| match (s["skip"].as_i64(), s["limit"].as_i64()) {
                    (Some(n), _) => format!("$skip {n}"),
                    (_, Some(n)) => format!("$limit {n}"),
                    _ => s["stage"].as_str().unwrap().to_string(),
                })
                .collect()
        };
        assert_eq!(
            names(json!([
                { "$addFields": { "x": 1 } },
                { "$limit": 8 },
                { "$skip": 5 },
                { "$project": { "x": 1 } },
                { "$skip": 1 },
            ])),
            ["$skip 6", "$limit 2", "$addFields", "$project"]
        );
        assert_eq!(
            names(
                json!([{ "$group": { "_id": "$n" } }, { "$project": { "n": 1 } }, { "$limit": 3 }])
            ),
            ["$group", "$limit 3", "$project"]
        );
        assert_eq!(
            names(json!([{ "$sort": { "n": 1 } }, { "$project": { "n": 1 } }, { "$limit": 3 }])),
            ["$sort", "$limit 3", "$project"]
        );
        assert_eq!(
            names(json!([{ "$match": { "n": 1 } }, { "$limit": 3 }, { "$unwind": "$n" }])),
            ["$limit 3", "$unwind"]
        );
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
        assert!(lookup["fallback"].as_str().unwrap().contains("raises"));
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

    /// The words every stage that streams, and every stage that blocks, ends its `detail` with.
    const STREAMS: &str = "streams: passes each document on as it arrives, holding one at a time";
    const BLOCKS: &str = "blocks: holds its whole input before it emits any output";

    /// What `explain` says of the shapes the executor lays out differently (ADR-270, P3): the
    /// source and every stage, exactly. The collection `a` has an index on `n`.
    #[test]
    fn explain_says_how_each_shape_runs() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 60, true);
        let pin = |pipeline: Value, source: Value, stages: Value| {
            let out = explain(&state, &root(), "a", pipeline.clone()).unwrap();
            assert_eq!(out["explain"]["source"], source, "the source of {pipeline}");
            assert_eq!(out["explain"]["stages"], stages, "the stages of {pipeline}");
        };
        // a streaming pipeline.
        pin(
            json!([{"$match": {"n": {"$gt": 1}}}, {"$project": {"k": 1}}, {"$limit": 5}]),
            json!({"strategy": "index", "index": "n_1", "indexFieldsUsed": 1, "stages": 1, "streams": true, "maxDocuments": 100000, "holds": "no document of the source: each is passed to the first stage as it is read, and the read has no ceiling of its own; maxDocuments still holds what a stage that blocks keeps and the answer", "sourceBound": 5, "indexRange": "read whole, in time proportional to the range, to deliver its documents in _id order, in passes of 5 keys (the pipeline takes at most that many documents of the source) and more of them while the filter rejects what they find, up to the whole range; the keys held are charged to the request's memory as `the index range of $match`, and a range too large is refused 413"}),
            json!([{"stage": "$limit", "runs": "inMemory", "limit": 5, "hold": "streams", "detail": format!("keeps the first documents that reach it; {STREAMS}")}, {"stage": "$project", "runs": "inMemory", "hold": "streams", "detail": format!("reshapes each document that reaches it; {STREAMS}")}]),
        );
        // a pipeline that keeps a state.
        pin(
            json!([{"$match": {"n": {"$gt": 1}}}, {"$group": {"_id": "$n", "c": {"$sum": 1}}}, {"$count": "total"}]),
            json!({"strategy": "index", "index": "n_1", "indexFieldsUsed": 1, "stages": 1, "streams": true, "maxDocuments": 100000, "holds": "no document of the source: each is passed to the first stage as it is read, and the read has no ceiling of its own; maxDocuments still holds what a stage that blocks keeps and the answer", "indexRange": "read whole, in time proportional to the range, to deliver its documents in _id order; every key of the range is held while it is read, charged to the request's memory as `the index range of $match`, and a range too large is refused 413"}),
            json!([{"stage": "$group", "runs": "inMemory", "hold": "state", "state": "groups", "detail": "groups the documents that reach it by the `_id` expression, with one output per group; holds a state, not its input: its groups, one for each distinct key"}, {"stage": "$count", "runs": "inMemory", "field": "total", "hold": "state", "state": "counter", "detail": "counts the documents that reach it and emits one document; holds a state, not its input: a counter"}]),
        );
        // a stage that blocks, and a source read whole.
        pin(
            json!([{"$sort": {"k": 1}}, {"$match": {"n": 1}}]),
            json!({"strategy": "collectionScan", "index": null, "indexFieldsUsed": 0, "stages": 0, "streams": false, "maxDocuments": 100000, "holds": "every document the read admits, charged as `the source read` until the first stage takes it; a read past maxDocuments is refused 400, and past the memory limit 413"}),
            json!([{"stage": "$sort", "runs": "inMemory", "keys": ["k"], "hold": "blocks", "detail": format!("sorts the documents that reach it by the keys; {BLOCKS}")}, {"stage": "$match", "runs": "inMemory", "hold": "streams", "detail": format!("filters the documents that reach it, in memory; never planned, and no index is used; {STREAMS}")}]),
        );
        // a sort and limit is a heap, and what follows it streams.
        pin(
            json!([{"$sort": {"k": -1}}, {"$project": {"k": 1}}, {"$limit": 3}]),
            json!({"strategy": "collectionScan", "index": null, "indexFieldsUsed": 0, "stages": 0, "streams": true, "maxDocuments": 100000, "holds": "no document of the source: each is passed to the first stage as it is read, and the read has no ceiling of its own; maxDocuments still holds what a stage that blocks keeps and the answer"}),
            json!([{"stage": "$sort", "runs": "inMemory", "keys": ["-k"], "hold": "state", "heapDocuments": 3, "state": "heap", "detail": "sorts the documents that reach it by the keys; holds a state, not its input: a heap of the first 3 documents of the sort, the $skip and $limit after it counted"}, {"stage": "$limit", "runs": "inMemory", "limit": 3, "hold": "state", "heapDocuments": 3, "state": "heap", "foldedInto": "$sort", "detail": "keeps the first documents that reach it; part of the heap of the $sort before it, which holds nothing else"}, {"stage": "$project", "runs": "inMemory", "hold": "streams", "detail": format!("reshapes each document that reaches it; {STREAMS}")}]),
        );
        // a sort, skip and limit is one heap.
        pin(
            json!([{"$sort": {"k": -1}}, {"$skip": 2}, {"$limit": 3}]),
            json!({"strategy": "collectionScan", "index": null, "indexFieldsUsed": 0, "stages": 0, "streams": true, "maxDocuments": 100000, "holds": "no document of the source: each is passed to the first stage as it is read, and the read has no ceiling of its own; maxDocuments still holds what a stage that blocks keeps and the answer"}),
            json!([{"stage": "$sort", "runs": "inMemory", "keys": ["-k"], "hold": "state", "heapDocuments": 5, "state": "heap", "detail": "sorts the documents that reach it by the keys; holds a state, not its input: a heap of the first 5 documents of the sort, the $skip and $limit after it counted"}, {"stage": "$skip", "runs": "inMemory", "skip": 2, "hold": "state", "heapDocuments": 5, "state": "heap", "foldedInto": "$sort", "detail": "drops the first documents that reach it; part of the heap of the $sort before it, which holds nothing else"}, {"stage": "$limit", "runs": "inMemory", "limit": 3, "hold": "state", "heapDocuments": 5, "state": "heap", "foldedInto": "$sort", "detail": "keeps the first documents that reach it; part of the heap of the $sort before it, which holds nothing else"}]),
        );
        // a window moved to the head bounds the source.
        pin(
            json!([{"$addFields": {"x": 1}}, {"$skip": 2}, {"$limit": 3}]),
            json!({"strategy": "collectionScan", "index": null, "indexFieldsUsed": 0, "stages": 0, "streams": true, "maxDocuments": 100000, "holds": "no document of the source: each is passed to the first stage as it is read, and the read has no ceiling of its own; maxDocuments still holds what a stage that blocks keeps and the answer", "sourceBound": 5}),
            json!([{"stage": "$skip", "runs": "inMemory", "skip": 2, "hold": "streams", "detail": format!("drops the first documents that reach it; {STREAMS}")}, {"stage": "$limit", "runs": "inMemory", "limit": 3, "hold": "streams", "detail": format!("keeps the first documents that reach it; {STREAMS}")}, {"stage": "$addFields", "runs": "inMemory", "hold": "streams", "detail": format!("adds computed fields to each document that reaches it; {STREAMS}")}]),
        );
        // a barrier-first pipeline keeps the ceiling.
        pin(
            json!([{"$match": {"n": {"$gt": 1}}}, {"$sort": {"k": 1}}]),
            json!({"strategy": "index", "index": "n_1", "indexFieldsUsed": 1, "stages": 1, "streams": false, "maxDocuments": 100000, "holds": "every document the read admits, charged as `the source read` until the first stage takes it; a read past maxDocuments is refused 400, and past the memory limit 413", "indexRange": "read whole, in time proportional to the range, to deliver its documents in _id order, in passes of up to maxDocuments + 1 candidates and more of them while the filter rejects what they find; the keys held are charged to the request's memory as `the index range of $match`, and a range too large is refused 413"}),
            json!([{"stage": "$sort", "runs": "inMemory", "keys": ["k"], "hold": "blocks", "detail": format!("sorts the documents that reach it by the keys; {BLOCKS}")}]),
        );
        // a facet branch is laid out as a pipeline.
        pin(
            json!([{"$facet": {"a": [{"$sort": {"k": 1}}, {"$limit": 2}], "b": [{"$count": "n"}]}}]),
            json!({"strategy": "collectionScan", "index": null, "indexFieldsUsed": 0, "stages": 0, "streams": false, "maxDocuments": 100000, "holds": "every document the read admits, charged as `the source read` until the first stage takes it; a read past maxDocuments is refused 400, and past the memory limit 413"}),
            json!([{"stage": "$facet", "runs": "inMemory", "facets": [{"name": "a", "stages": [{"stage": "$sort", "runs": "inMemory", "keys": ["k"], "hold": "state", "heapDocuments": 2, "state": "heap", "detail": "sorts the documents that reach it by the keys; holds a state, not its input: a heap of the first 2 documents of the sort, the $skip and $limit after it counted"}, {"stage": "$limit", "runs": "inMemory", "limit": 2, "hold": "state", "heapDocuments": 2, "state": "heap", "foldedInto": "$sort", "detail": "keeps the first documents that reach it; part of the heap of the $sort before it, which holds nothing else"}]}, {"name": "b", "stages": [{"stage": "$count", "runs": "inMemory", "field": "n", "hold": "state", "state": "counter", "detail": "counts the documents that reach it and emits one document; holds a state, not its input: a counter"}]}], "hold": "blocks", "detail": format!("runs every facet over the same input, in memory, and answers one document of arrays; each facet's output counts against the pipeline's document limit; {BLOCKS}")}]),
        );
        // a sample is a reservoir, and a sink blocks.
        pin(
            json!([{"$sample": {"size": 4}}, {"$out": "o"}]),
            json!({"strategy": "collectionScan", "index": null, "indexFieldsUsed": 0, "stages": 0, "streams": true, "maxDocuments": 100000, "holds": "no document of the source: each is passed to the first stage as it is read, and the read has no ceiling of its own; maxDocuments still holds what a stage that blocks keeps and the answer"}),
            json!([{"stage": "$sample", "runs": "inMemory", "size": 4, "hold": "state", "state": "reservoir", "detail": "keeps this many documents chosen at random, in a random order; the pipeline's document limit applies to what it keeps; holds a state, not its input: a reservoir of 4 documents, the size of the sample"}, {"stage": "$out", "runs": "write", "into": "o", "hold": "blocks", "detail": format!("inserts the pipeline's output into the target, which must be new or empty (a run refuses one that holds documents), in chunks of storage.multi_chunk_docs; not atomic. A plan writes nothing and creates nothing; {BLOCKS}")}]),
        );
        // a window that takes nothing reads nothing.
        pin(
            json!([{"$match": {"n": {"$gt": 1}}}, {"$limit": 3}, {"$skip": 5}, {"$project": {"k": 1}}]),
            json!({"strategy": "index", "index": "n_1", "indexFieldsUsed": 1, "stages": 1, "streams": true, "maxDocuments": 100000, "holds": "no document: the pipeline takes none of the source (a window that takes nothing), and the run reads none", "sourceBound": 0}),
            json!([{"stage": "$limit", "runs": "inMemory", "limit": 0, "hold": "streams", "detail": format!("keeps the first documents that reach it; {STREAMS}")}, {"stage": "$project", "runs": "inMemory", "hold": "streams", "detail": format!("reshapes each document that reaches it; {STREAMS}")}]),
        );
    }

    /// A `$vectorSearch` source is held whole (at most `k` hits), and the stages after it are laid
    /// out as any pipeline's are.
    #[test]
    fn a_vector_source_is_held_whole_and_the_stages_after_it_are_laid_out() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
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
        let out = explain(
            &state,
            &root(),
            "v",
            json!([
                { "$vectorSearch": { "vector": [1.0, 0.0, 0.0], "k": 4 } },
                { "$group": { "_id": "$n" } },
                { "$sort": { "n": 1 } },
            ]),
        )
        .unwrap();
        let source = &out["explain"]["source"];
        assert_eq!(source["streams"], false);
        assert_eq!(
            source["holds"],
            "the hits, at most k, held whole as the source read and handed to the first stage, \
             which takes them out of it as it takes each"
        );
        assert!(source.get("maxDocuments").is_none(), "{source}");
        let stages = out["explain"]["stages"].as_array().unwrap();
        let holds: Vec<_> = stages.iter().map(|s| (&s["stage"], &s["hold"])).collect();
        assert_eq!(
            holds,
            [(&json!("$group"), &json!("state")), (&json!("$sort"), &json!("blocks"))]
        );
    }

    /// Each stage kind, alone and between two stages that stream: `explain` says what the chain the
    /// executor builds does with it. The `hold` is derived from the executor's own layout
    /// ([`pipeline::segments`]), and this holds it to what a chain built from the stages does: a
    /// stage that blocks is run by the runner, and none other; a stage that streams is the
    /// executor's `streams`; and the source streams exactly when a run is not refused at the
    /// source for being over the ceiling.
    #[test]
    fn every_stage_kind_is_said_to_run_as_the_chain_runs_it() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 60, true);
        seed(&state, "b", 5, false);
        let stage_specs = [
            json!({ "$match": { "k": { "$gte": 0 } } }),
            json!({ "$project": { "k": 1, "x": 1 } }),
            json!({ "$addFields": { "y": 1 } }),
            json!({ "$set": { "y": 1 } }),
            json!({ "$replaceRoot": { "newRoot": "$$ROOT" } }),
            json!({ "$replaceWith": "$$ROOT" }),
            json!({ "$unset": "y" }),
            json!({ "$redact": "$$KEEP" }),
            json!({ "$unwind": "$x" }),
            json!({ "$skip": 1 }),
            json!({ "$limit": 4 }),
            json!({ "$sort": { "k": 1 } }),
            json!({ "$group": { "_id": "$n" } }),
            json!({ "$count": "c" }),
            json!({ "$bucket": { "groupBy": "$n", "boundaries": [0, 2, 4], "default": "o" } }),
            json!({ "$sortByCount": "$n" }),
            json!({ "$sample": { "size": 2 } }),
            json!({ "$bucketAuto": { "groupBy": "$n", "buckets": 2 } }),
            json!({ "$setWindowFields": { "sortBy": { "k": 1 }, "output": {
                "s": { "$sum": "$k", "window": { "documents": ["unbounded", "current"] } } } } }),
            json!({ "$densify": { "field": "k", "range": { "step": 1, "bounds": "full" } } }),
            json!({ "$fill": { "output": { "y": { "value": 0 } } } }),
            json!({ "$fill": { "sortBy": { "k": 1 }, "output": { "y": { "method": "locf" } } } }),
            json!({ "$facet": { "f": [{ "$limit": 1 }] } }),
            json!({ "$lookup": { "from": "b", "localField": "n", "foreignField": "n", "as": "j" } }),
            json!({ "$lookup": { "from": "b", "let": { "n": "$n" }, "as": "j",
                "pipeline": [{ "$match": { "$expr": { "$eq": ["$n", "$$n"] } } }, { "$limit": 1 }] } }),
            json!({ "$graphLookup": { "from": "b", "startWith": "$n", "connectFromField": "n",
                "connectToField": "n", "as": "g", "maxDepth": 2 } }),
            json!({ "$unionWith": "b" }),
        ];
        let sort = json!({ "$sort": { "k": 1 } });
        let mut shapes: Vec<Vec<Value>> = Vec::new();
        for spec in &stage_specs {
            shapes.push(vec![spec.clone()]);
            shapes.push(vec![
                json!({ "$addFields": { "z": 1 } }),
                spec.clone(),
                json!({ "$project": { "k": 1 } }),
            ]);
        }
        shapes.extend([
            vec![sort.clone(), json!({ "$limit": 3 })],
            vec![sort.clone(), json!({ "$skip": 2 }), json!({ "$limit": 3 })],
            vec![sort.clone(), json!({ "$project": { "k": 1 } }), json!({ "$limit": 3 })],
            vec![sort.clone(), json!({ "$match": { "k": 1 } }), json!({ "$limit": 3 })],
            vec![sort.clone(), json!({ "$skip": 2 })],
            vec![json!({ "$match": { "n": { "$gt": 1 } } }), sort.clone(), json!({ "$limit": 3 })],
            vec![json!({ "$project": { "k": 1 } }), json!({ "$skip": 1 }), json!({ "$limit": 3 })],
            vec![json!({ "$group": { "_id": "$n" } }), json!({ "$count": "c" })],
            vec![
                json!({ "$unwind": "$x" }),
                sort.clone(),
                json!({ "$limit": 3 }),
                json!({ "$count": "c" }),
            ],
        ]);
        let tight = aggregate::Limits { max_documents: 3, ..Default::default() };
        let docs = |n: i64| (0..n).map(|i| bson::doc! { "_id": i, "n": i, "k": i, "x": [i] });
        for specs in shapes {
            let pipeline = Value::Array(specs);
            let planned = explain(&state, &root(), "a", pipeline.clone()).unwrap();
            let entries = planned["explain"]["stages"].as_array().unwrap();
            let all = exec::parse_pipeline(&pipeline).unwrap();
            let consumed = aggregate::leading_match(&all).map_or(0, |(_, consumed)| consumed);
            let chain = &all[consumed..];
            assert_eq!(entries.len(), chain.len(), "{pipeline}");

            // What a chain built from the stages does: which stages it runs as a barrier.
            let ran = std::cell::RefCell::new(Vec::new());
            let run = |stage: &Stage,
                       input: Vec<bson::Document>,
                       _: Option<kimmy_query::budget::Lease>| {
                ran.borrow_mut().push(std::ptr::from_ref(stage));
                Ok::<_, kimmy_core::Error>(input)
            };
            let limits = aggregate::Limits::default();
            let mut built = pipeline::build(chain, &run, &limits, &[]);
            let _ = built.push_all(docs(6).collect()).unwrap();
            built.finish().unwrap();

            let mut previous_heap = false;
            for (stage, entry) in chain.iter().zip(entries) {
                let blocked = ran.borrow().contains(&std::ptr::from_ref(stage));
                let folded = entry.get("foldedInto").is_some();
                let hold = entry["hold"].as_str().unwrap();
                assert_eq!(hold == "blocks", blocked, "{pipeline}: {entry}");
                if hold != "blocks" {
                    // Not run as a barrier: a stage that streams, or that a state stands for.
                    assert_eq!(
                        hold == "streams",
                        pipeline::streams(stage) && !folded,
                        "{pipeline}: {entry}"
                    );
                }
                // A `$skip` or `$limit` that the heap of the `$sort` before it stands for is
                // part of it; any other stage of a heap is the `$sort`.
                if folded {
                    assert!(previous_heap && pipeline::streams(stage), "{pipeline}: {entry}");
                } else if entry["state"] == "heap" {
                    assert!(matches!(stage, Stage::Sort(_)), "{pipeline}: {entry}");
                }
                let detail = entry["detail"].as_str().unwrap();
                match hold {
                    "streams" => assert!(detail.ends_with(STREAMS), "{pipeline}: {entry}"),
                    "blocks" => assert!(detail.ends_with(BLOCKS), "{pipeline}: {entry}"),
                    _ => assert!(detail.contains("state") || folded, "{pipeline}: {entry}"),
                }
                previous_heap = entry["state"] == "heap";
            }

            // The source streams exactly when a run does not hold it whole under the ceiling.
            let source = &planned["explain"]["source"];
            assert_eq!(source["streams"], pipeline::streams_source(chain), "{pipeline}");
            // The pipeline's ceiling is reported whether the source streams or not.
            assert_eq!(source["maxDocuments"], aggregate::DEFAULT_MAX_DOCUMENTS, "{pipeline}");
            if consumed == 0 {
                let run = tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(
                    exec::aggregate_with_limits(
                        &state,
                        &root(),
                        "app",
                        "a",
                        &pipeline,
                        tight.clone(),
                    ),
                );
                let over_the_source = run
                    .as_ref()
                    .err()
                    .is_some_and(|e| e.message.contains("source collection admits more than 3"));
                assert_eq!(over_the_source, source["streams"] == false, "{pipeline}: {run:?}");
            }
        }
    }

    /// What each stage kind holds, written out here and not read from the executor's layout: a
    /// regression that laid a stage out as another kind (a `$project` as a barrier, a `$group`
    /// as a stage that keeps its input) moves the chain and `explain` together, so only a table
    /// of the expected answers can fail on it. `(pipeline, [(hold, state)] for each stage listed)`.
    /// A pipeline and the `(hold, state)` of each stage `explain` lists for it.
    type Row = (Vec<Value>, Vec<(&'static str, &'static str)>);

    #[test]
    fn what_each_stage_kind_holds_is_written_out() {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed(&state, "a", 10, true);
        seed(&state, "b", 5, false);
        let sort = || json!({ "$sort": { "k": 1 } });
        let table: Vec<Row> = vec![
            // (A `$match` that begins the pipeline is the source's filter, so one follows a stage.)
            (
                vec![
                    json!({ "$project": { "k": 1 } }),
                    json!({ "$match": { "k": { "$gte": 0 } } }),
                ],
                vec![("streams", ""), ("streams", "")],
            ),
            (vec![json!({ "$project": { "k": 1 } })], vec![("streams", "")]),
            (vec![json!({ "$addFields": { "y": 1 } })], vec![("streams", "")]),
            (vec![json!({ "$set": { "y": 1 } })], vec![("streams", "")]),
            (vec![json!({ "$replaceRoot": { "newRoot": "$$ROOT" } })], vec![("streams", "")]),
            (vec![json!({ "$replaceWith": "$$ROOT" })], vec![("streams", "")]),
            (vec![json!({ "$unset": "y" })], vec![("streams", "")]),
            (vec![json!({ "$redact": "$$KEEP" })], vec![("streams", "")]),
            (vec![json!({ "$unwind": "$x" })], vec![("streams", "")]),
            (vec![json!({ "$skip": 1 })], vec![("streams", "")]),
            (vec![json!({ "$limit": 4 })], vec![("streams", "")]),
            (vec![sort()], vec![("blocks", "")]),
            (vec![json!({ "$group": { "_id": "$n" } })], vec![("state", "groups")]),
            (vec![json!({ "$count": "c" })], vec![("state", "counter")]),
            (
                vec![
                    json!({ "$bucket": { "groupBy": "$n", "boundaries": [0, 2, 4], "default": "o" } }),
                ],
                vec![("state", "groups")],
            ),
            (vec![json!({ "$sortByCount": "$n" })], vec![("state", "groups")]),
            (vec![json!({ "$sample": { "size": 2 } })], vec![("state", "reservoir")]),
            (
                vec![json!({ "$bucketAuto": { "groupBy": "$n", "buckets": 2 } })],
                vec![("blocks", "")],
            ),
            (
                vec![json!({ "$setWindowFields": { "sortBy": { "k": 1 }, "output": {
                    "s": { "$sum": "$k", "window": { "documents": ["unbounded", "current"] } } } } })],
                vec![("blocks", "")],
            ),
            (
                vec![
                    json!({ "$densify": { "field": "k", "range": { "step": 1, "bounds": "full" } } }),
                ],
                vec![("blocks", "")],
            ),
            (vec![json!({ "$fill": { "output": { "y": { "value": 0 } } } })], vec![("blocks", "")]),
            (
                vec![
                    json!({ "$fill": { "sortBy": { "k": 1 }, "output": { "y": { "method": "locf" } } } }),
                ],
                vec![("blocks", "")],
            ),
            (vec![json!({ "$facet": { "f": [{ "$limit": 1 }] } })], vec![("blocks", "")]),
            (
                vec![
                    json!({ "$lookup": { "from": "b", "localField": "n", "foreignField": "n", "as": "j" } }),
                ],
                vec![("blocks", "")],
            ),
            (
                vec![
                    json!({ "$graphLookup": { "from": "b", "startWith": "$n", "connectFromField": "n",
                    "connectToField": "n", "as": "g", "maxDepth": 2 } }),
                ],
                vec![("blocks", "")],
            ),
            (vec![json!({ "$unionWith": "b" })], vec![("blocks", "")]),
            // The fused shapes: the `$skip` and `$limit` a heap stands for hold what it holds.
            (vec![sort(), json!({ "$limit": 3 })], vec![("state", "heap"), ("state", "heap")]),
            (
                vec![sort(), json!({ "$skip": 2 }), json!({ "$limit": 3 })],
                vec![("state", "heap"), ("state", "heap"), ("state", "heap")],
            ),
            // The limit is moved ahead of the `$project`, so the `$sort` is a heap.
            (
                vec![sort(), json!({ "$project": { "k": 1 } }), json!({ "$limit": 3 })],
                vec![("state", "heap"), ("state", "heap"), ("streams", "")],
            ),
            (vec![sort(), json!({ "$skip": 2 })], vec![("blocks", ""), ("streams", "")]),
            (
                vec![sort(), json!({ "$match": { "k": 1 } }), json!({ "$limit": 3 })],
                vec![("blocks", ""), ("streams", ""), ("streams", "")],
            ),
            (
                vec![json!({ "$group": { "_id": "$n" } }), json!({ "$count": "c" })],
                vec![("state", "groups"), ("state", "counter")],
            ),
        ];
        for (specs, expected) in table {
            let pipeline = Value::Array(specs);
            let out = explain(&state, &root(), "a", pipeline.clone()).unwrap();
            let got: Vec<(String, String)> = out["explain"]["stages"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| {
                    (
                        s["hold"].as_str().unwrap().to_string(),
                        s["state"].as_str().unwrap_or("").to_string(),
                    )
                })
                .collect();
            let want: Vec<(String, String)> =
                expected.iter().map(|(h, s)| ((*h).to_string(), (*s).to_string())).collect();
            assert_eq!(got, want, "{pipeline}");
        }
    }
}
