//! The push pipeline against the loop it replaces (ADR-270): a random pipeline over random
//! documents is run four ways and the answers are held to each other.
//!
//! - **The old loop**: `apply` stage after stage, each stage over all of its input.
//! - **The barrier chain** (`pipeline::build_barriers`): every stage a barrier. It must
//!   answer what the old loop answers, error for error, whether the source is pushed whole or
//!   a document at a time.
//! - **The streaming chain** (`pipeline::build`): the stages that stream take one document at
//!   a time through the whole run, the way a scan pushes them, and stop when the chain says
//!   `Done`. It runs each document through every stage before the next one starts, where the
//!   old loop runs each stage over every document before the next stage starts. That changes
//!   what a pipeline that fails can fail with, and makes one that fails only past a full
//!   `$limit` answer, so the relation is not equality:
//!   - if the old loop answers, the streaming chain answers the same documents in the same
//!     order;
//!   - if the streaming chain fails, the old loop fails too (not necessarily with the same
//!     error: the old loop reports the first stage that fails, the chain the first document);
//!   - if the old loop fails and the chain answers, the pipeline has a `$limit` that filled
//!     before the document that fails.
//! - **The lazy model**: a pull-based pipeline built from iterators, one per stage that
//!   streams, over the same per-stage functions. It answers what a reference that works a
//!   document at a time answers, and the streaming chain, pushed whole and pushed one
//!   document at a time, must answer exactly that, the error included. It is the check of the
//!   `$limit` exception, not just a licence for it.
//!
//! `limit_stops_the_source_scan` holds the chain to the other thing a source needs: that it
//! says `Done` when the last document that is wanted has been pushed.
//!
//! The stages that hold a state and not their input (`$count`, `$group`, `$bucket`,
//! `$sortByCount`, a `$sort` with a `$limit` after it) take their documents one at a time, so
//! the first failure in document order is the pipeline's, whether it is the stage's own or an
//! earlier stage's on a later document: the lazy model runs them that way. The heap that
//! stands for a `$sort` and a `$limit` is held to a naive sort-then-take, written out below
//! and sharing nothing with it but the comparison of two documents.

use std::cell::RefCell;

use bson::{Bson, Document, doc};
use kimmy_core::Error;
use kimmy_query::aggregate::{self, Held, Limits, Stage};
use kimmy_query::budget::Lease;
use kimmy_query::pipeline::{self, Flow};
use kimmy_query::shape::{self, SortKey};
use kimmy_query::stages;
use proptest::prelude::*;

/// What a run answers, comparable whatever it is: NaN is not equal to itself, and an error
/// is compared by the message a caller would see.
type Answer = Result<Vec<String>, String>;

fn answer(result: Result<Vec<Document>, Error>) -> Answer {
    result.map(|docs| docs.iter().map(|d| format!("{d:?}")).collect()).map_err(|e| e.to_string())
}

/// The `let` the pipelines are parsed with and run under.
const VAR: &str = "k";

/// A source of random positions that is the same on every run: a `$sample` run twice over the
/// same input answers the same, so that the four ways can be compared.
struct Seeded(u64);

impl Seeded {
    fn below(&mut self, n: usize) -> usize {
        self.0 =
            self.0.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 33) as usize) % n
    }
}

/// What a run holds: its budget and its source of random positions.
struct Run {
    limits: Limits,
    seed: RefCell<Seeded>,
    k: Bson,
}

impl Run {
    fn new() -> Self {
        Run { limits: Limits::default(), seed: RefCell::new(Seeded(7)), k: Bson::Int32(2) }
    }

    fn vars(&self) -> Vec<(&str, &Bson)> {
        vec![(VAR, &self.k)]
    }

    /// One stage on all of its input: `apply`, but for `$sample`, which takes the seeded
    /// positions.
    fn stage(
        &self,
        stage: &Stage,
        input: Vec<Document>,
        arrived: Option<Lease>,
    ) -> Result<Vec<Document>, Error> {
        match stage {
            Stage::Sample(size) => {
                let _held = Held::enter_with(stage, &input, &self.limits, arrived)?;
                let out = stages::sample(input, *size, &self.limits, &mut |n| {
                    self.seed.borrow_mut().below(n)
                })?;
                aggregate::check_limit(stage.name(), out.len(), &self.limits)?;
                Ok(out)
            }
            _ => aggregate::apply_charged(stage, input, &self.limits, &self.vars(), arrived),
        }
    }
}

/// The loop the executor ran before the chain.
fn old_loop(stages: &[Stage], mut docs: Vec<Document>) -> Result<Vec<Document>, Error> {
    let run = Run::new();
    for stage in stages {
        docs = run.stage(stage, docs, None)?;
    }
    Ok(docs)
}

/// The chain `build` makes, driven the way a scan drives it, a document at a time, until it
/// says `Done`. Answers what came out and how many documents were pushed.
fn chain_one_by_one(
    build: Build,
    stages: &[Stage],
    docs: &[Document],
) -> (Result<Vec<Document>, Error>, usize) {
    let run = Run::new();
    let vars = run.vars();
    let runner = |stage: &Stage, input, arrived| run.stage(stage, input, arrived);
    let mut chain = build.chain(stages, &runner, &run, &vars);
    let mut pushed = 0;
    for doc in docs {
        pushed += 1;
        match chain.push(doc.clone()) {
            Ok(Flow::More) => {}
            Ok(Flow::Done) => break,
            Err(e) => return (Err(e), pushed),
        }
    }
    (chain.finish(), pushed)
}

/// The same chain, the source pushed whole.
fn chain_all(build: Build, stages: &[Stage], docs: Vec<Document>) -> Result<Vec<Document>, Error> {
    let run = Run::new();
    let vars = run.vars();
    let runner = |stage: &Stage, input, arrived| run.stage(stage, input, arrived);
    let mut chain = build.chain(stages, &runner, &run, &vars);
    let _more = chain.push_all(docs)?;
    chain.finish()
}

/// Which chain: `pipeline::build` or `pipeline::build_barriers`.
#[derive(Clone, Copy)]
enum Build {
    Streaming,
    Barriers,
}

impl Build {
    /// The chain, with the positions a `$sample` draws from the run's seeded source, as the
    /// barrier draws them.
    fn chain<'a>(
        self,
        stages: &'a [Stage],
        runner: pipeline::Runner<'a, Error>,
        run: &'a Run,
        vars: &'a [kimmy_query::expr::Binding<'a>],
    ) -> pipeline::Chain<'a, Error> {
        match self {
            Build::Streaming => {
                // Leaked: the chain borrows it for as long as it lives, and a test is short.
                let below: &'a dyn Fn() -> Box<dyn FnMut(usize) -> usize + 'a> =
                    Box::leak(Box::new(move || -> Box<dyn FnMut(usize) -> usize + 'a> {
                        Box::new(move |n| run.seed.borrow_mut().below(n))
                    }));
                pipeline::build_with(stages, runner, &run.limits, vars, below)
            }
            Build::Barriers => pipeline::build_barriers(stages, runner, &run.limits, vars),
        }
    }
}

/// The lazy model: a pipeline of iterators, each pulling from the one before only what it
/// needs. A stage that streams is one iterator over `apply` of a single document; a stage
/// that blocks pulls everything, runs, and hands the result on.
fn lazy_model(stages: &[Stage], docs: Vec<Document>) -> Result<Vec<Document>, Error> {
    let run = Run::new();
    let mut input: Box<dyn Iterator<Item = Result<Document, Error>> + '_> =
        Box::new(docs.into_iter().map(Ok));
    for stage in stages {
        input = match stage {
            Stage::Limit(n) => Box::new(input.take(*n)),
            Stage::Skip(n) => {
                let mut left = *n;
                // An error is not a document, and is not skipped.
                Box::new(input.filter(move |item| match item {
                    Ok(_) if left > 0 => {
                        left -= 1;
                        false
                    }
                    _ => true,
                }))
            }
            _ if pipeline::streams(stage) => {
                let run = &run;
                Box::new(input.flat_map(move |item| -> Vec<Result<Document, Error>> {
                    match item {
                        Err(e) => vec![Err(e)],
                        Ok(doc) => match run.stage(stage, vec![doc], None) {
                            Ok(out) => out.into_iter().map(Ok).collect(),
                            Err(e) => vec![Err(e)],
                        },
                    }
                }))
            }
            // A stage that holds a state and not its input takes its documents one at a time,
            // so the first failure in document order is the pipeline's, whether it is the
            // stage's own or an earlier stage's on a later document. (The state's per-document
            // body is the stage's own, which the old loop runs too.)
            Stage::Count(field) => {
                let mut seen = 0;
                for item in input {
                    item?;
                    seen += 1;
                }
                Box::new(std::iter::once(Ok(doc! { field.as_str(): seen })))
            }
            Stage::Group { .. } | Stage::Bucket { .. } | Stage::SortByCount(_) => {
                let vars = run.vars();
                let mut state = aggregate::GroupState::new(stage, &run.limits, &vars)
                    .expect("the stage groups");
                let mut lease = run.limits.buffer.lease("the model");
                for item in input {
                    state.add(&item?, &mut lease)?;
                }
                Box::new(state.finish()?.into_iter().map(Ok))
            }
            // A stage that blocks runs whatever follows it, so its failure, or the failure of
            // what it pulled, is the pipeline's: a `$limit` of 0 after it pulls nothing, and
            // must not make it unread.
            _ => {
                let all = input.collect::<Result<Vec<Document>, Error>>()?;
                Box::new(run.stage(stage, all, None)?.into_iter().map(Ok))
            }
        };
    }
    input.collect()
}

fn number() -> impl Strategy<Value = Bson> {
    prop_oneof![
        (-3i32..6).prop_map(Bson::Int32),
        (-3i64..6).prop_map(Bson::Int64),
        (-3i32..6).prop_map(|n| Bson::Double(f64::from(n) / 2.0)),
    ]
}

/// A value for a scalar field: mostly numbers, sometimes what an expression or a sort
/// has to cope with.
fn scalar() -> impl Strategy<Value = Option<Bson>> {
    prop_oneof![
        6 => number().prop_map(Some),
        1 => Just(Some(Bson::Null)),
        1 => "[xyz]{0,2}".prop_map(|s| Some(Bson::String(s))),
        1 => Just(None),
    ]
}

fn document(id: i32) -> impl Strategy<Value = Document> {
    (
        scalar(),
        scalar(),
        prop_oneof![
            4 => prop::collection::vec(number(), 0..4).prop_map(|v| Some(Bson::Array(v))),
            1 => scalar(),
        ],
        prop_oneof![
            3 => scalar().prop_map(|a| Some(Bson::Document(match a {
                Some(a) => doc! { "k": a },
                None => doc! {},
            }))),
            1 => Just(None),
        ],
    )
        .prop_map(move |(a, b, arr, sub)| {
            let mut d = doc! { "_id": id };
            for (name, value) in [("a", a), ("b", b), ("arr", arr), ("sub", sub)] {
                if let Some(value) = value {
                    d.insert(name, value);
                }
            }
            d
        })
}

fn documents() -> impl Strategy<Value = Vec<Document>> {
    (0usize..14).prop_flat_map(|n| (0..n as i32).map(document).collect::<Vec<_>>())
}

/// One stage of a kind that needs no storage and no sub-pipeline, some of them reading the
/// `let` the pipelines are run under.
fn stage() -> impl Strategy<Value = Document> {
    let n = -2i32..6;
    prop_oneof![
        n.clone().prop_map(|n| doc! { "$match": { "a": { "$gt": n } } }),
        n.clone().prop_map(|n| doc! { "$match": { "b": { "$lte": n } } }),
        Just(doc! { "$match": { "arr": { "$exists": true } } }),
        Just(doc! { "$project": { "a": 1, "x": "$b" } }),
        Just(doc! { "$project": { "b": 0 } }),
        Just(doc! { "$project": { "_id": 0, "a": 1, "arr": 1, "k": "$$k" } }),
        Just(doc! { "$unset": "b" }),
        Just(doc! { "$unset": ["a", "sub.k"] }),
        Just(doc! { "$addFields": { "c": { "$add": ["$a", 1] } } }),
        Just(doc! { "$set": { "d": { "$add": ["$a", "$$k"] } } }),
        Just(doc! { "$addFields": { "q": { "$divide": ["$a", "$b"] } } }),
        Just(doc! { "$replaceRoot": { "newRoot": "$sub" } }),
        Just(doc! { "$replaceRoot": { "newRoot": { "_id": "$_id", "a": "$a" } } }),
        Just(doc! { "$replaceWith": { "_id": "$_id", "v": "$$k", "a": "$a" } }),
        n.clone().prop_map(|n| doc! { "$redact": { "$cond": [
            { "$gt": ["$a", n] }, "$$KEEP", "$$PRUNE"
        ] } }),
        Just(doc! { "$redact": { "$cond": [{ "$eq": ["$$k", 2] }, "$$DESCEND", "$$PRUNE"] } }),
        Just(doc! { "$redact": "$a" }),
        Just(doc! { "$unwind": "$arr" }),
        Just(doc! { "$unwind": {
            "path": "$arr", "preserveNullAndEmptyArrays": true, "includeArrayIndex": "i"
        } }),
        (0i32..6).prop_map(|n| doc! { "$skip": n }),
        (1i32..6).prop_map(|n| doc! { "$limit": n }),
        (1i32..4).prop_map(|n| doc! { "$limit": n }),
        Just(doc! { "$sort": { "a": 1 } }),
        Just(doc! { "$sort": { "b": -1, "_id": 1 } }),
        // Keys that tie: most documents share a value, and an array sorts by an element.
        Just(doc! { "$sort": { "a": -1 } }),
        Just(doc! { "$sort": { "arr": 1 } }),
        Just(doc! { "$sort": { "b": 1, "a": -1 } }),
        Just(doc! { "$group": { "_id": "$a", "n": { "$sum": 1 }, "m": { "$max": "$b" } } }),
        Just(doc! { "$group": { "_id": null, "total": { "$sum": "$b" } } }),
        Just(doc! { "$count": "n" }),
        Just(doc! { "$sortByCount": "$a" }),
        Just(doc! { "$bucket": { "groupBy": "$a", "boundaries": [-3, 0, 3, 6],
        "default": "other", "output": { "n": { "$sum": 1 } } } }),
        // No default: a value outside the boundaries fails the stage.
        Just(doc! { "$bucket": { "groupBy": "$a", "boundaries": [-3, 0, 3, 6] } }),
        (1i32..6).prop_map(|n| doc! { "$sample": { "size": n } }),
        // The stages that block and charge for themselves, which a streamed input to them
        // hands the arrival lease back for, and a `$facet` of pure branches.
        Just(doc! { "$bucketAuto": { "groupBy": "$a", "buckets": 2 } }),
        Just(doc! { "$setWindowFields": { "sortBy": { "_id": 1 },
        "output": { "n": { "$documentNumber": {} } } } }),
        Just(doc! { "$densify": { "field": "a", "range": { "step": 1, "bounds": "full" } } }),
        Just(doc! { "$fill": { "output": { "a": { "value": 0 } } } }),
        Just(doc! { "$facet": { "x": [{ "$count": "n" }], "y": [{ "$limit": 1 }] } }),
    ]
}

fn pipeline_of(specs: &[Document]) -> Vec<Stage> {
    // Every generated shape parses; a case that did not would pass by finding nothing, so
    // a parse change that refuses one fails here rather than hiding.
    aggregate::parse_with_vars(specs, &[VAR.to_string()]).unwrap_or_else(|e| {
        panic!("the generator made a pipeline that does not parse: {e:?}: {specs:?}")
    })
}

fn has_limit(stages: &[Stage]) -> bool {
    stages.iter().any(|s| matches!(s, Stage::Limit(_)))
}

/// The relation between the old loop, the barrier chain, the streaming chain and the lazy
/// model, for one pipeline over one source.
fn held_to_each_other(
    stages: &[Stage],
    docs: &[Document],
    specs: &[Document],
) -> Result<(), TestCaseError> {
    let old = answer(old_loop(stages, docs.to_vec()));

    // Every stage a barrier: the old loop's answer, the error included.
    prop_assert_eq!(
        &answer(chain_all(Build::Barriers, stages, docs.to_vec())),
        &old,
        "barriers, pushed whole: {:?}",
        specs
    );
    let (barriers, _) = chain_one_by_one(Build::Barriers, stages, docs);
    prop_assert_eq!(&answer(barriers), &old, "barriers, one by one: {:?}", specs);

    // The streaming chain: the lazy model's answer, the error included.
    let lazy = answer(lazy_model(stages, docs.to_vec()));
    prop_assert_eq!(
        &answer(chain_all(Build::Streaming, stages, docs.to_vec())),
        &lazy,
        "streaming, pushed whole: {:?}",
        specs
    );
    let (one_by_one, pushed) = chain_one_by_one(Build::Streaming, stages, docs);
    prop_assert_eq!(&answer(one_by_one), &lazy, "streaming, one by one: {:?}", specs);

    // And the loop's answer and the lazy model's are related as they must be.
    match (&old, &lazy) {
        (Ok(old), Ok(lazy)) => prop_assert_eq!(lazy, old, "answered something else: {:?}", specs),
        (Err(_), Ok(_)) => prop_assert!(
            has_limit(stages),
            "the loop failed, the chain answered, and there is no $limit: {:?}",
            specs
        ),
        (Err(_), Err(_)) => {}
        (Ok(_), Err(e)) => {
            prop_assert!(false, "the chain failed with {e} where the loop answered: {:?}", specs)
        }
    }
    // With nothing that blocks, the loop run over just the documents the chain read fails
    // too: it meets every failure the chain did, since those are in the documents it read.
    // (Not with the same error in general: a later element of the failing document's fan
    // could fail an earlier stage first. It is the same here because no generated stage
    // fails on a value that differs between the elements of `arr`.)
    if lazy.is_err() && stages.iter().all(pipeline::streams) {
        let read = &docs[..pushed];
        prop_assert!(
            old_loop(stages, read.to_vec()).is_err(),
            "the loop over the documents read answers: {:?}",
            specs
        );
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 4000, ..ProptestConfig::default() })]

    #[test]
    fn the_chains_answer_what_the_loop_and_the_lazy_model_answer(
        specs in prop::collection::vec(stage(), 0..7),
        docs in documents(),
    ) {
        let stages = pipeline_of(&specs);
        held_to_each_other(&stages, &docs, &specs)?;
    }

    /// A barrier before a `$limit`, and a `$limit` before a barrier, over documents some of
    /// which fail a stage: the limit ends the scan only where nothing before it blocks.
    #[test]
    fn a_limit_after_a_barrier_and_before_one(
        before in prop::collection::vec(stage(), 0..3),
        barrier in prop_oneof![
            Just(doc! { "$sort": { "a": 1 } }),
            Just(doc! { "$count": "n" }),
            Just(doc! { "$group": { "_id": "$a", "n": { "$sum": 1 } } }),
        ],
        middle in prop::collection::vec(stage(), 0..2),
        limit in 1i32..4,
        after in prop::collection::vec(stage(), 0..3),
        docs in documents(),
    ) {
        let mut specs = before;
        specs.push(barrier.clone());
        specs.extend(middle);
        specs.push(doc! { "$limit": limit });
        specs.push(barrier);
        specs.extend(after);
        let stages = pipeline_of(&specs);
        held_to_each_other(&stages, &docs, &specs)?;
    }

    /// An `$unwind` under a `$limit` stops in the middle of an array: the elements past the
    /// limit are not made, so a failure in one of them is not raised.
    #[test]
    fn an_unwind_under_a_limit_stops_in_the_middle_of_an_array(
        arrays in prop::collection::vec(prop::collection::vec(-2i32..3, 0..6), 0..6),
        limit in 1i32..5,
        preserve in any::<bool>(),
    ) {
        let docs: Vec<Document> = arrays
            .iter()
            .enumerate()
            .map(|(id, arr)| doc! { "_id": id as i32, "arr": arr.clone() })
            .collect();
        let specs = [
            doc! { "$unwind": { "path": "$arr", "preserveNullAndEmptyArrays": preserve } },
            // A zero divides. (A `$match` and not an `$addFields`, which the `$limit` would
            // be moved ahead of when the pipeline is parsed.)
            doc! { "$match": { "$expr": { "$gt": [{ "$divide": [1, "$arr"] }, -10] } } },
            doc! { "$limit": limit },
        ];
        let stages = pipeline_of(&specs);
        held_to_each_other(&stages, &docs, &specs)?;
    }
}

/// The reference's reordering of `$skip` and `$limit`, written as rules over the stages as they
/// are written and applied until none applies, which shares nothing with the rewrite at the
/// parse but the rules themselves (ADR-270): a window moves ahead of a stage that neither
/// filters nor changes the count; two `$skip`s are one, two `$limit`s are one, and a `$limit`
/// then a `$skip` is a `$skip` then a `$limit`; a `$skip` of nothing is not a stage.
fn model_reorder(mut specs: Vec<Document>) -> Vec<(String, i64)> {
    fn name(spec: &Document) -> &str {
        spec.keys().next().map(String::as_str).unwrap()
    }
    fn count(spec: &Document) -> i64 {
        spec.values()
            .next()
            .and_then(Bson::as_i64)
            .or_else(|| spec.values().next().and_then(|v| v.as_i32().map(i64::from)))
            .unwrap()
    }
    // `before` is what comes before the stage: a `$match` of nothing is passed unless it is one
    // of the `$match` stages the pipeline begins with, the filter of its source.
    fn passes(spec: &Document, before: &[Document]) -> bool {
        matches!(
            name(spec),
            "$project"
                | "$unset"
                | "$addFields"
                | "$set"
                | "$replaceRoot"
                | "$replaceWith"
                | "$lookup"
        ) || (name(spec) == "$match"
            && spec.get_document("$match").is_ok_and(Document::is_empty)
            && before.iter().any(|spec| name(spec) != "$match"))
    }
    let window = |n: &str, v: i64| doc! { n: v };
    'again: loop {
        for at in 0..specs.len() {
            if name(&specs[at]) == "$skip" && count(&specs[at]) == 0 {
                specs.remove(at);
                continue 'again;
            }
            let Some(next) = specs.get(at + 1) else { continue };
            let (a, b) = (&specs[at], next);
            let rewritten = match (name(a), name(b)) {
                ("$skip", "$skip") => Some(vec![window("$skip", count(a) + count(b))]),
                ("$limit", "$limit") => Some(vec![window("$limit", count(a).min(count(b)))]),
                ("$limit", "$skip") => {
                    let dropped = count(a).min(count(b));
                    Some(vec![window("$skip", dropped), window("$limit", count(a) - dropped)])
                }
                (_, "$skip" | "$limit") if passes(a, &specs[..at]) => {
                    Some(vec![b.clone(), a.clone()])
                }
                _ => None,
            };
            if let Some(rewritten) = rewritten {
                specs.splice(at..at + 2, rewritten);
                continue 'again;
            }
        }
        break;
    }
    // What the parse calls a stage: `$set`, `$unset` and `$replaceWith` are the stages they are
    // another name for.
    specs
        .iter()
        .map(|spec| {
            let name = match name(spec) {
                "$set" => "$addFields",
                "$unset" => "$project",
                "$replaceWith" => "$replaceRoot",
                other => other,
            };
            let n = if matches!(name, "$skip" | "$limit") { count(spec) } else { -1 };
            (name.to_string(), n)
        })
        .collect()
}

/// The stages a pipeline parses to, as the name and, for a window, the number.
fn parsed_shape(specs: &[Document]) -> Vec<(String, i64)> {
    pipeline_of(specs)
        .iter()
        .map(|stage| match stage {
            Stage::Skip(n) | Stage::Limit(n) => (stage.name().to_string(), *n as i64),
            other => (other.name().to_string(), -1),
        })
        .collect()
}

/// Stages a window moves ahead of, and stages it stays behind, as they are written (the
/// stages that need a collection are not run, only parsed).
fn window_stage() -> impl Strategy<Value = Document> {
    prop_oneof![
        6 => (0i32..7).prop_map(|n| doc! { "$skip": n }),
        6 => (1i32..9).prop_map(|n| doc! { "$limit": n }),
        1 => Just(doc! { "$project": { "a": 1 } }),
        1 => Just(doc! { "$unset": "b" }),
        1 => Just(doc! { "$addFields": { "c": 1 } }),
        1 => Just(doc! { "$set": { "c": 1 } }),
        1 => Just(doc! { "$replaceRoot": { "newRoot": "$$ROOT" } }),
        1 => Just(doc! { "$replaceWith": "$$ROOT" }),
        1 => Just(doc! { "$match": {} }),
        1 => Just(doc! { "$lookup": { "from": "c", "localField": "a", "foreignField": "a", "as": "l" } }),
        1 => Just(doc! { "$match": { "a": 1 } }),
        1 => Just(doc! { "$redact": "$$KEEP" }),
        1 => Just(doc! { "$sort": { "a": 1 } }),
        1 => Just(doc! { "$unwind": "$arr" }),
        1 => Just(doc! { "$group": { "_id": "$a" } }),
        1 => Just(doc! { "$sample": { "size": 3 } }),
        1 => Just(doc! { "$count": "n" }),
        1 => Just(doc! { "$sortByCount": "$a" }),
        1 => Just(doc! { "$facet": { "x": [{ "$count": "n" }] } }),
        1 => Just(doc! { "$unionWith": "c" }),
    ]
}

/// The stages of a pipeline as each is written, each parsed alone: no run of them is there to
/// reorder. (A `$skip` of nothing is no stage.)
fn as_written(specs: &[Document]) -> Vec<Stage> {
    specs.iter().flat_map(|spec| pipeline_of(std::slice::from_ref(spec))).collect()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 4000, ..ProptestConfig::default() })]

    /// The stages that come out of the parse are the stages the rules give.
    #[test]
    fn the_parse_reorders_a_window_as_the_rules_do(
        specs in prop::collection::vec(window_stage(), 0..9),
    ) {
        prop_assert_eq!(parsed_shape(&specs), model_reorder(specs.clone()), "{:?}", specs);
    }

    /// Moving a window never changes an answer: where the pipeline as it is written answers,
    /// the reordered one answers the same, and where it fails the reordered one may answer
    /// (the documents the window drops are not evaluated) but never answers something else.
    #[test]
    fn the_reordering_does_not_change_an_answer_that_was_there(
        specs in prop::collection::vec(
            prop_oneof![
                3 => window_stage().prop_filter("runs here", |s| {
                    !matches!(s.keys().next().map(String::as_str), Some("$lookup" | "$unionWith"))
                }),
                1 => stage(),
            ],
            0..8,
        ),
        docs in documents(),
    ) {
        let written = as_written(&specs);
        let reordered = pipeline_of(&specs);
        let before = answer(old_loop(&written, docs.clone()));
        let after = answer(old_loop(&reordered, docs.clone()));
        if let Ok(before) = &before {
            prop_assert_eq!(after, Ok(before.clone()), "{:?}", specs);
        }
    }
}

/// Keys that tie a lot, over documents that share most of their values.
fn tie_keys() -> impl Strategy<Value = Vec<SortKey>> {
    let key = |path: &str, descending: bool| SortKey { path: path.to_string(), descending };
    prop_oneof![
        Just(vec![key("a", false)]),
        Just(vec![key("a", true)]),
        Just(vec![key("b", false), key("a", true)]),
        Just(vec![key("b", true), key("a", false), key("c", false)]),
        Just(vec![key("arr", false)]),
        Just(vec![key("arr", true), key("sub.k", false)]),
        Just(vec![key("missing", false)]),
    ]
}

/// Documents with few distinct values, so that most keys tie: a number in two types that
/// compare equal, a string, a missing field and a null.
fn tie_documents() -> impl Strategy<Value = Vec<Document>> {
    let value = prop_oneof![
        Just(Some(Bson::Int32(0))),
        Just(Some(Bson::Int64(0))),
        Just(Some(Bson::Double(0.0))),
        Just(Some(Bson::Int32(1))),
        Just(Some(Bson::Double(1.0))),
        Just(Some(Bson::String("x".into()))),
        Just(Some(Bson::Null)),
        Just(None),
    ];
    let one = (value.clone(), value.clone(), value, prop::collection::vec(0i32..2, 0..3)).prop_map(
        |(a, b, c, arr)| {
            let mut d = Document::new();
            for (name, v) in [("a", a), ("b", b), ("c", c)] {
                if let Some(v) = v {
                    d.insert(name, v);
                }
            }
            d.insert("arr", arr);
            d.insert("sub", doc! { "k": d.get("a").cloned().unwrap_or(Bson::Null) });
            d
        },
    );
    prop::collection::vec(one, 0..40).prop_map(|docs| {
        docs.into_iter()
            .enumerate()
            .map(|(id, mut d)| {
                // The arrival order is in the document, so a tie shows which came first.
                let mut with_id = doc! { "_id": id as i32 };
                with_id.extend(std::mem::take(&mut d));
                with_id
            })
            .collect()
    })
}

/// The first `skip + limit` documents of a stable sort, `skip` of them dropped, written out:
/// each document goes in after the last one held that does not sort after it, so that equal
/// documents stay in arrival order. It shares with the heap only `shape::compare`, the order of
/// two documents, and not the keys taken once, the comparison of keys, the merge of runs or
/// the heap.
fn naive_sort_then_take(
    keys: &[SortKey],
    docs: &[Document],
    skip: usize,
    limit: usize,
) -> Vec<Document> {
    let mut sorted: Vec<&Document> = Vec::new();
    for doc in docs {
        let mut at = sorted.len();
        while at > 0 && shape::compare(keys, doc, sorted[at - 1]) == std::cmp::Ordering::Less {
            at -= 1;
        }
        sorted.insert(at, doc);
    }
    sorted.into_iter().skip(skip).take(limit).cloned().collect()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 3000, ..ProptestConfig::default() })]

    /// The heap of `keep` documents is the naive sort-then-take, ties in arrival order, and it
    /// never holds more than it keeps.
    #[test]
    fn the_heap_of_a_sort_is_a_naive_sort_then_take(
        keys in tie_keys(),
        docs in tie_documents(),
        skip in 0usize..6,
        limit in 1usize..9,
    ) {
        let limits = Limits::default();
        let mut lease = limits.buffer.lease("the heap");
        let keep = skip + limit;
        let mut top = shape::TopK::new(&keys, keep);
        for doc in &docs {
            top.offer(doc.clone(), &mut lease, &limits).unwrap();
            prop_assert!(top.len() <= keep, "{} held of {keep}", top.len());
        }
        prop_assert!(top.peak() <= keep);
        prop_assert_eq!(top.peak(), docs.len().min(keep), "it fills to the most it keeps");
        let got: Vec<Document> = top.into_sorted().into_iter().skip(skip).take(limit).collect();
        let want = naive_sort_then_take(&keys, &docs, skip, limit);
        prop_assert_eq!(format!("{got:?}"), format!("{want:?}"), "{:?}", keys);
        // And what it held is what the sort of everything gives, as `sort_until_deadline` does.
        let sorted = shape::sort_until_deadline(&keys, docs.clone()).unwrap();
        let sorted: Vec<Document> = sorted.into_iter().skip(skip).take(limit).collect();
        prop_assert_eq!(format!("{sorted:?}"), format!("{want:?}"), "{:?}", keys);
    }

    /// A `$sort` with its `$skip` and `$limit` through the chain is the same naive answer,
    /// with stages before and after, and the chain pushed whole and one at a time.
    #[test]
    fn a_sort_and_its_window_through_the_chain_are_a_naive_sort_then_take(
        keys in tie_keys(),
        docs in tie_documents(),
        skip in prop::option::of(0usize..6),
        limit in 1usize..9,
    ) {
        let mut sort = Document::new();
        for key in &keys {
            sort.insert(key.path.clone(), if key.descending { -1 } else { 1 });
        }
        let mut specs = vec![doc! { "$sort": sort }];
        if let Some(skip) = skip {
            specs.push(doc! { "$skip": skip as i32 });
        }
        specs.push(doc! { "$limit": limit as i32 });
        let stages = pipeline_of(&specs);
        let want = naive_sort_then_take(&keys, &docs, skip.unwrap_or(0), limit);
        let run = Run::new();
        let vars = run.vars();
        let never = |_: &Stage, _: Vec<Document>, _: Option<Lease>| -> Result<Vec<Document>, Error> {
            panic!("the sort and its window are a heap, not a stage run by the runner")
        };
        let mut chain = pipeline::build(&stages, &never, &run.limits, &vars);
        let _more = chain.push_all(docs.clone()).unwrap();
        prop_assert_eq!(format!("{:?}", chain.finish().unwrap()), format!("{want:?}"));
        let mut chain = pipeline::build(&stages, &never, &run.limits, &vars);
        for doc in &docs {
            prop_assert_eq!(chain.push(doc.clone()).unwrap(), Flow::More);
        }
        prop_assert_eq!(format!("{:?}", chain.finish().unwrap()), format!("{want:?}"));
        // The loop agrees too.
        prop_assert_eq!(
            answer(old_loop(&stages, docs.clone())),
            answer(Ok(want)),
            "the loop: {:?}", specs
        );
    }
}

/// A source of positions that is the same for the same seed and keeps what it was asked and
/// what it answered.
fn logged(seed: u64) -> (RefCell<Seeded>, RefCell<Vec<(usize, usize)>>) {
    (RefCell::new(Seeded(seed)), RefCell::new(Vec::new()))
}

/// `stages::sample` as it was before the reservoir was taken a document at a time: the loop
/// that fills the reservoir from the input, replaces slots at random, and shuffles it.
fn loop_sample(
    input: Vec<Document>,
    size: usize,
    below: &mut dyn FnMut(usize) -> usize,
) -> Vec<Document> {
    if size == 0 {
        return Vec::new();
    }
    let mut reservoir: Vec<Document> = Vec::with_capacity(size.min(input.len()));
    for (seen, doc) in input.into_iter().enumerate() {
        if seen < size {
            reservoir.push(doc);
        } else {
            let at = below(seen + 1);
            if at < size {
                reservoir[at] = doc;
            }
        }
    }
    for i in (1..reservoir.len()).rev() {
        reservoir.swap(i, below(i + 1));
    }
    reservoir
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 1500, ..ProptestConfig::default() })]

    /// The reservoir asks `below` for the same positions, in the same order, as the loop did,
    /// whether it is driven by `stages::sample` over the whole input or by a chain a document
    /// at a time, and answers the same sample: every seeded test keeps its numbers.
    #[test]
    fn the_reservoir_draws_the_positions_the_loop_drew(
        n in 0usize..40,
        size in 0usize..14,
        seed in any::<u64>(),
    ) {
        let docs: Vec<Document> = (0..n as i32).map(|id| doc! { "_id": id }).collect();
        let (source, log) = logged(seed);
        let mut below = |n| { let at = source.borrow_mut().below(n); log.borrow_mut().push((n, at)); at };
        let want = loop_sample(docs.clone(), size, &mut below);
        let want_log = log.borrow().clone();
        // A draw for each document past the first `size`, and one for each step of the shuffle.
        let drawn = if size == 0 { 0 } else { n.saturating_sub(size) + want.len().saturating_sub(1) };
        prop_assert_eq!(want_log.len(), drawn);

        let (source, log) = logged(seed);
        let mut below = |n| { let at = source.borrow_mut().below(n); log.borrow_mut().push((n, at)); at };
        let got = stages::sample(docs.clone(), size, &Limits::default(), &mut below).unwrap();
        prop_assert_eq!(&got, &want);
        prop_assert_eq!(&*log.borrow(), &want_log);

        // The chain, a document at a time: the draws for the documents past the first `size`
        // are made as they arrive and the shuffle's when the input ends.
        let (source, log) = logged(seed);
        let counting = || -> Box<dyn FnMut(usize) -> usize + '_> {
            Box::new(|n| { let at = source.borrow_mut().below(n); log.borrow_mut().push((n, at)); at })
        };
        let stages = [Stage::Sample(size)];
        let limits = Limits::default();
        let never = |_: &Stage, _: Vec<Document>, _: Option<Lease>| -> Result<Vec<Document>, Error> {
            panic!("a $sample is a reservoir, not a stage run by the runner")
        };
        let mut chain = pipeline::build_with(&stages, &never, &limits, &[], &counting);
        for doc in &docs {
            prop_assert_eq!(chain.push(doc.clone()).unwrap(), Flow::More);
        }
        let before_the_end = log.borrow().len();
        let expected = if size == 0 { 0 } else { n.saturating_sub(size) };
        prop_assert_eq!(before_the_end, expected, "a draw for each document past the first {}", size);
        let got = chain.finish().unwrap();
        prop_assert_eq!(&got, &want);
        prop_assert_eq!(&*log.borrow(), &want_log);
    }
}

/// The documents pushed to a chain of `[$match, $skip, $limit]` before it says `Done`: those
/// up to and including the one that completes `skip + limit` matches, or all of them.
fn expected_pushes(docs: &[Document], threshold: i32, skip: usize, limit: usize) -> usize {
    let mut kept = 0;
    for (at, doc) in docs.iter().enumerate() {
        if doc.get_i32("a").is_ok_and(|a| a >= threshold) {
            kept += 1;
            if kept == skip + limit {
                return at + 1;
            }
        }
    }
    docs.len()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 500, ..ProptestConfig::default() })]

    /// A scan that stops when the chain says `Done` reads at most `skip + limit` documents
    /// that pass the `$match` before it, plus every document the `$match` drops on the way
    /// (ADR-270 section 6), and the answer is still the loop's.
    #[test]
    fn limit_stops_the_source_scan(
        values in prop::collection::vec(prop::option::of(-3i32..4), 0..20),
        threshold in -3i32..4,
        skip in 0usize..4,
        limit in 1usize..4,
    ) {
        let docs: Vec<Document> = values
            .iter()
            .enumerate()
            .map(|(id, a)| match a {
                Some(a) => doc! { "_id": id as i32, "a": *a },
                None => doc! { "_id": id as i32 },
            })
            .collect();
        let specs = [
            doc! { "$match": { "a": { "$gte": threshold } } },
            doc! { "$skip": skip as i32 },
            doc! { "$limit": limit as i32 },
        ];
        let stages = aggregate::parse(&specs).unwrap();
        let (got, pushed) = chain_one_by_one(Build::Streaming, &stages, &docs);
        prop_assert_eq!(pushed, expected_pushes(&docs, threshold, skip, limit));
        prop_assert_eq!(
            answer(got),
            answer(old_loop(&stages, docs.clone())),
            "the early stop changed the answer"
        );
    }
}

/// `[$match .. $divide, $limit 5]` over documents of which the eighth divides by zero:
/// the loop fails at the eighth document, and the chain stops after the fifth and answers.
/// (A `$match` and not an `$addFields`, which the `$limit` is moved ahead of at the parse: the
/// loop would then answer as well.)
#[test]
fn an_error_past_a_full_limit_is_not_raised() {
    let docs: Vec<Document> =
        (0..12).map(|id| doc! { "_id": id, "d": if id == 7 { 0 } else { 1 } }).collect();
    let divides = doc! { "$match": { "$expr": { "$gt": [{ "$divide": [1, "$d"] }, 0] } } };
    let specs = [divides.clone(), doc! { "$limit": 5 }];
    let stages = aggregate::parse(&specs).unwrap();
    let loop_says = answer(old_loop(&stages, docs.clone()));
    assert!(loop_says.as_ref().is_err_and(|e| e.contains("divide")), "{loop_says:?}");
    let (got, pushed) = chain_one_by_one(Build::Streaming, &stages, &docs);
    assert_eq!(pushed, 5, "the scan stops at the fifth document");
    let got = got.unwrap();
    assert_eq!(got.iter().map(|d| d.get_i32("_id").unwrap()).collect::<Vec<_>>(), [0, 1, 2, 3, 4]);
    assert_eq!(answer(chain_all(Build::Streaming, &stages, docs.clone())), answer(Ok(got)));
    // The failure is raised when it is inside the window, and the same one.
    let docs: Vec<Document> =
        (0..12).map(|id| doc! { "_id": id, "d": if id == 3 { 0 } else { 1 } }).collect();
    let loop_says = answer(old_loop(&stages, docs.clone()));
    let (got, pushed) = chain_one_by_one(Build::Streaming, &stages, &docs);
    assert_eq!(pushed, 4);
    assert_eq!(answer(got), loop_says);
    // A failure past the window that follows a `$limit` is raised: the limit does not hide it.
    let specs = [doc! { "$limit": 5 }, divides];
    let stages = aggregate::parse(&specs).unwrap();
    let (got, _) = chain_one_by_one(Build::Streaming, &stages, &docs);
    assert_eq!(answer(got), answer(old_loop(&stages, docs.clone())));
    assert!(answer(old_loop(&stages, docs)).is_err());
}

/// An array whose later element fails is unwound only as far as the limit takes it.
#[test]
fn an_unwind_stops_before_the_element_that_fails() {
    let docs = vec![doc! { "_id": 1, "arr": [4, 2, 1, 0] }, doc! { "_id": 2, "arr": [0] }];
    // (A `$match` and not an `$addFields`, which the `$limit` is moved ahead of at the parse.)
    let specs = [
        doc! { "$unwind": "$arr" },
        doc! { "$match": { "$expr": { "$gt": [{ "$divide": [8, "$arr"] }, 0] } } },
        doc! { "$limit": 3 },
    ];
    let stages = aggregate::parse(&specs).unwrap();
    assert!(old_loop(&stages, docs.clone()).is_err());
    let (got, pushed) = chain_one_by_one(Build::Streaming, &stages, &docs);
    assert_eq!(pushed, 1);
    let elements: Vec<_> = got.unwrap().iter().map(|d| d.get_i32("arr").unwrap()).collect();
    assert_eq!(elements, [4, 2, 1]);
    // With a fourth the failure is inside, and raised.
    let stages =
        aggregate::parse(&[specs[0].clone(), specs[1].clone(), doc! { "$limit": 4 }]).unwrap();
    let (got, _) = chain_one_by_one(Build::Streaming, &stages, &docs);
    assert_eq!(answer(got), answer(old_loop(&stages, docs)));
}

/// A limit of nothing takes nothing and the chain says so before it is pushed anything. The
/// parser refuses `$limit: 0`, as the reference does, so the stage is made by hand: this is
/// what a chain does for a limit that is full before any document.
#[test]
fn a_limit_that_is_full_before_any_document_wants_none() {
    let docs: Vec<Document> = (0..5).map(|id| doc! { "_id": id }).collect();
    let mut stages = aggregate::parse(&[doc! { "$addFields": { "x": 1 } }]).unwrap();
    stages.push(Stage::Limit(0));
    let run = Run::new();
    let runner = |stage: &Stage, input, arrived| run.stage(stage, input, arrived);
    let vars = run.vars();
    let chain = pipeline::build(&stages, &runner, &run.limits, &vars);
    assert!(chain.is_full());
    assert!(chain.finish().unwrap().is_empty());
    let (got, pushed) = chain_one_by_one(Build::Streaming, &stages, &docs);
    assert_eq!((got.unwrap().len(), pushed), (0, 1), "one pushed, if a source ignores is_full");
}
