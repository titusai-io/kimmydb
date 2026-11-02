//! The push pipeline against the loop it replaces (ADR-270): a random pipeline of the pure
//! stages over random documents answers the same documents, in the same order, or fails
//! with the same error, whether it is run as a chain of sinks (`pipeline::build`) or as
//! `apply` stage after stage.
//!
//! In P0 every stage of the chain is a barrier that runs `apply` itself, so the two sides
//! are the same code and this passes trivially. It is the oracle P1 flips kinds against:
//! each stage kind that becomes a stream is held to the answer of its own barrier here, over
//! the same corpus, before it is believed anywhere else.
//!
//! The second property, `limit_stops_the_source_scan`, is what P1 delivers and cannot pass
//! before it: a chain whose stages are all barriers never says `Done`.

use bson::{Bson, Document, doc};
use kimmy_core::Error;
use kimmy_query::aggregate::{self, Limits, Stage};
use kimmy_query::pipeline::{self, Flow};
use proptest::prelude::*;

/// What a run answers, comparable whatever it is: NaN is not equal to itself, and an error
/// is compared by the message a caller would see.
type Answer = Result<Vec<String>, String>;

fn answer(result: Result<Vec<Document>, Error>) -> Answer {
    result.map(|docs| docs.iter().map(|d| format!("{d:?}")).collect()).map_err(|e| e.to_string())
}

/// The loop the executor ran before the chain.
fn old_loop(stages: &[Stage], mut docs: Vec<Document>) -> Result<Vec<Document>, Error> {
    let limits = Limits::default();
    for stage in stages {
        docs = aggregate::apply(stage, docs, &limits)?;
    }
    Ok(docs)
}

/// The chain, the source pushed whole.
fn chain_all(stages: &[Stage], docs: Vec<Document>) -> Result<Vec<Document>, Error> {
    let limits = Limits::default();
    let run = |stage: &Stage, input| aggregate::apply_with_vars(stage, input, &limits, &[]);
    let mut chain = pipeline::build(stages, &run);
    let _more = chain.push_all(docs)?;
    chain.finish()
}

/// The chain, the source pushed one document at a time, as a scan pushes it. Every document
/// is pushed (the stop signal is not read), because this side is held to the same answer.
fn chain_one_by_one(stages: &[Stage], docs: Vec<Document>) -> Result<Vec<Document>, Error> {
    let limits = Limits::default();
    let run = |stage: &Stage, input| aggregate::apply_with_vars(stage, input, &limits, &[]);
    let mut chain = pipeline::build(stages, &run);
    for doc in docs {
        let _more = chain.push(doc)?;
    }
    chain.finish()
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

/// One stage of a kind that is pure: no storage, no sub-pipeline.
fn stage() -> impl Strategy<Value = Document> {
    let n = -2i32..6;
    prop_oneof![
        n.clone().prop_map(|n| doc! { "$match": { "a": { "$gt": n } } }),
        n.clone().prop_map(|n| doc! { "$match": { "b": { "$lte": n } } }),
        Just(doc! { "$match": { "arr": { "$exists": true } } }),
        Just(doc! { "$project": { "a": 1, "x": "$b" } }),
        Just(doc! { "$project": { "b": 0 } }),
        Just(doc! { "$project": { "_id": 0, "a": 1, "arr": 1 } }),
        Just(doc! { "$addFields": { "c": { "$add": ["$a", 1] } } }),
        Just(doc! { "$addFields": { "q": { "$divide": ["$a", "$b"] } } }),
        Just(doc! { "$replaceRoot": { "newRoot": "$sub" } }),
        Just(doc! { "$replaceRoot": { "newRoot": { "_id": "$_id", "a": "$a" } } }),
        Just(doc! { "$unwind": "$arr" }),
        Just(doc! { "$unwind": {
            "path": "$arr", "preserveNullAndEmptyArrays": true, "includeArrayIndex": "i"
        } }),
        (0i32..6).prop_map(|n| doc! { "$skip": n }),
        (1i32..6).prop_map(|n| doc! { "$limit": n }),
        Just(doc! { "$sort": { "a": 1 } }),
        Just(doc! { "$sort": { "b": -1, "_id": 1 } }),
        Just(doc! { "$group": { "_id": "$a", "n": { "$sum": 1 }, "m": { "$max": "$b" } } }),
        Just(doc! { "$group": { "_id": null, "total": { "$sum": "$b" } } }),
        Just(doc! { "$count": "n" }),
    ]
}

fn pipeline_of(specs: &[Document]) -> Option<Vec<Stage>> {
    aggregate::parse(specs).ok()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2000, ..ProptestConfig::default() })]

    #[test]
    fn the_chain_answers_what_the_loop_answers(
        specs in prop::collection::vec(stage(), 0..7),
        docs in documents(),
    ) {
        let Some(stages) = pipeline_of(&specs) else { return Ok(()); };
        let want = answer(old_loop(&stages, docs.clone()));
        prop_assert_eq!(&answer(chain_all(&stages, docs.clone())), &want, "pushed whole: {:?}", specs);
        prop_assert_eq!(
            &answer(chain_one_by_one(&stages, docs)),
            &want,
            "pushed one by one: {:?}",
            specs
        );
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
    #[ignore = "P1: $match, $skip and $limit are barriers in P0, so the chain never says Done"]
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
        let limits = Limits::default();
        let run = |stage: &Stage, input| aggregate::apply_with_vars(stage, input, &limits, &[]);
        let mut chain = pipeline::build(&stages, &run);
        let mut pushed = 0;
        for doc in docs.iter().cloned() {
            pushed += 1;
            if chain.push(doc).unwrap() == Flow::Done {
                break;
            }
        }
        prop_assert_eq!(pushed, expected_pushes(&docs, threshold, skip, limit));
        prop_assert_eq!(
            answer(chain.finish()),
            answer(old_loop(&stages, docs.clone())),
            "the early stop changed the answer"
        );
    }
}
