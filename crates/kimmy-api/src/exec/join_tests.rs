//! The hash join of a `$lookup` sub-pipeline's correlated `$eq` (ADR-222), held to the
//! nested loop it replaces: every test compares a run with the join against the same
//! run with recognition switched off, and says the join was in play.

use bson::{Bson, Document, doc};
use serde_json::{Value, json};

use super::join_counters::{BUILDS, DISABLED, HOISTS, PROBES, WHOLE};
use super::tests::{agg_limits, live_state, seed_docs};
use super::*;

fn root() -> Auth {
    Auth(kimmy_auth::Principal::superuser("test"))
}

/// The outcome of one run: the answer, or the error as it is shown.
type Outcome = Result<Value, String>;

struct Run {
    outcome: Outcome,
    builds: usize,
    probes: usize,
    hoists: usize,
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    /// Recognition on.
    Join,
    /// Recognition off: the nested loop, with the split `$match` (independent part once).
    Loop,
    /// The shape before the split: the whole conjunction evaluated for every pair.
    Whole,
}

fn run_with(state: &SharedState, pipeline: &Value, join: bool, max_documents: usize) -> Run {
    run_mode(state, pipeline, if join { Mode::Join } else { Mode::Loop }, max_documents)
}

fn run_mode(state: &SharedState, pipeline: &Value, mode: Mode, max_documents: usize) -> Run {
    DISABLED.with(|d| d.set(mode != Mode::Join));
    WHOLE.with(|w| w.set(mode == Mode::Whole));
    BUILDS.with(|n| n.set(0));
    PROBES.with(|n| n.set(0));
    HOISTS.with(|n| n.set(0));
    let limits = aggregate::Limits { max_documents };
    let outcome =
        agg_limits(state, &root(), "app", "orders", pipeline, limits).map_err(|e| format!("{e:?}"));
    let run = Run {
        outcome,
        builds: BUILDS.with(std::cell::Cell::get),
        probes: PROBES.with(std::cell::Cell::get),
        hoists: HOISTS.with(std::cell::Cell::get),
    };
    DISABLED.with(|d| d.set(false));
    WHOLE.with(|w| w.set(false));
    run
}

/// Three ways, asserting the same outcome: the join, the nested loop, and the shape
/// before the split (the whole conjunction for every pair). Returns the join's run.
fn both(state: &SharedState, pipeline: &Value) -> Run {
    both_limited(state, pipeline, aggregate::DEFAULT_MAX_DOCUMENTS)
}

fn both_limited(state: &SharedState, pipeline: &Value, max_documents: usize) -> Run {
    let whole = run_mode(state, pipeline, Mode::Whole, max_documents);
    assert_eq!((whole.builds, whole.probes), (0, 0), "the reference never builds");
    let looped = run_mode(state, pipeline, Mode::Loop, max_documents);
    assert_eq!((looped.builds, looped.probes), (0, 0), "the loop never builds");
    assert_eq!(looped.outcome, whole.outcome, "the split loop against the whole: {pipeline}");
    let joined = run_mode(state, pipeline, Mode::Join, max_documents);
    assert_eq!(joined.outcome, looped.outcome, "pipeline {pipeline}");
    joined
}

fn lookup(let_: Value, pipeline: Value) -> Value {
    json!([{ "$lookup": { "from": "items", "let": let_, "pipeline": pipeline, "as": "got" } }])
}

fn eq_on_k() -> Value {
    lookup(json!({ "v": "$v" }), json!([{ "$match": { "$expr": { "$eq": ["$k", "$$v"] } } }]))
}

fn state_with(
    dir: &tempfile::TempDir,
    inputs: Vec<Document>,
    foreign: Vec<Document>,
) -> SharedState {
    let state = live_state(dir);
    seed_docs(&state, "orders", inputs);
    seed_docs(&state, "items", foreign);
    state
}

fn ids(out: &Value) -> Vec<Vec<Value>> {
    out["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["got"].as_array().unwrap().iter().map(|p| p["_id"].clone()).collect())
        .collect()
}

/// A small deterministic generator, so a corpus is the same on every run.
struct Gen(u64);

impl Gen {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn pool() -> Vec<Option<Bson>> {
    vec![
        Some(Bson::Int32(1)),
        Some(Bson::Int64(1)),
        Some(Bson::Double(1.0)),
        Some(Bson::Double(-0.0)),
        Some(Bson::Int32(0)),
        Some(Bson::Double(f64::NAN)),
        Some(Bson::Double(9_007_199_254_740_992.0)),
        Some(Bson::Int64(9_007_199_254_740_993)),
        Some(Bson::Null),
        None,
        Some(Bson::String("1".into())),
        Some(Bson::String("a".into())),
        Some(Bson::Symbol("a".into())),
        Some(Bson::Boolean(true)),
        Some(Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])),
        Some(Bson::Array(vec![Bson::Int64(1), Bson::Double(2.0)])),
        Some(Bson::Array(vec![])),
        Some(Bson::Document(doc! { "a": 1, "b": 2 })),
        Some(Bson::Document(doc! { "b": 2, "a": 1 })),
        Some(Bson::Document(doc! { "a": 1.0, "b": 2_i64 })),
        Some(Bson::Undefined),
        Some(Bson::Decimal128("1.5".parse().unwrap())),
        Some(Bson::Array(vec![Bson::Decimal128("2".parse().unwrap()), Bson::Int32(1)])),
        Some(Bson::Document(doc! { "a": Bson::Decimal128("1".parse().unwrap()) })),
    ]
}

fn with_field(id: i64, name: &str, value: &Option<Bson>, z: i64) -> Document {
    let mut d =
        doc! { "_id": id, "z": z, "d": id % 3, "kind": if id % 3 == 0 { "x" } else { "y" } };
    if let Some(v) = value {
        d.insert(name, v.clone());
    }
    d
}

const SEEDS: u64 = 100;

/// Every shape the join recognises, and some it does not, over generated corpora of
/// mixed types, null, missing, arrays and reordered documents: the answer and the
/// order are the loop's.
#[test]
fn the_join_equals_the_loop_over_generated_corpora() {
    let pipelines = [
        eq_on_k(),
        // Operands the other way round.
        lookup(json!({ "v": "$v" }), json!([{ "$match": { "$expr": { "$eq": ["$$v", "$k"] } } }])),
        // An independent clause beside it, plain and as `$expr`.
        lookup(
            json!({ "v": "$v" }),
            json!([{ "$match": { "kind": "y", "$expr": { "$eq": ["$k", "$$v"] } } }]),
        ),
        lookup(
            json!({ "v": "$v" }),
            json!([{ "$match": { "$and": [
                { "$expr": { "$gt": ["$z", 1] } },
                { "$expr": { "$eq": ["$k", "$$v"] } } ] } }]),
        ),
        // Two correlated terms: the first is the key, the other a residual.
        lookup(
            json!({ "v": "$v", "w": "$w" }),
            json!([{ "$match": { "$and": [
                { "$expr": { "$eq": ["$k", "$$v"] } },
                { "$expr": { "$lte": ["$z", "$$w"] } } ] } }]),
        ),
        // A residual written before the key.
        lookup(
            json!({ "v": "$v", "w": "$w" }),
            json!([{ "$match": { "$and": [
                { "$expr": { "$lte": ["$z", "$$w"] } },
                { "$expr": { "$eq": ["$k", "$$v"] } } ] } }]),
        ),
        // Not a join: `$or`, an inequality.
        lookup(
            json!({ "v": "$v" }),
            json!([{ "$match": { "$or": [
                { "$expr": { "$eq": ["$k", "$$v"] } }, { "kind": "x" }] } }]),
        ),
        lookup(json!({ "w": "$w" }), json!([{ "$match": { "$expr": { "$gt": ["$z", "$$w"] } } }])),
        // A key expression, and the rest of the pipeline over the matches.
        lookup(
            json!({ "v": "$v" }),
            json!([
                { "$match": { "$expr": { "$eq": [{ "$toString": "$z" }, { "$toString": "$$v" }] } } },
                { "$group": { "_id": null, "n": { "$sum": 1 } } }
            ]),
        ),
        lookup(
            json!({ "v": "$v" }),
            json!([
                { "$match": { "$expr": { "$eq": ["$k", "$$v"] } } },
                { "$count": "n" }
            ]),
        ),
        // Clauses that raise for some documents or some inputs: on the document side of
        // the key, on the variable side, as a residual, as an independent clause, and
        // beside a variable clause that is false for every pair.
        lookup(
            json!({ "v": "$v" }),
            json!([{ "$match": { "$expr": { "$eq": [{ "$divide": [1, "$d"] }, "$$v"] } } }]),
        ),
        lookup(
            json!({ "v": "$v", "w": "$w" }),
            json!([{ "$match": { "$expr": { "$eq": ["$k", { "$divide": [1, "$$w"] }] } } }]),
        ),
        lookup(
            json!({ "v": "$v", "w": "$w" }),
            json!([{ "$match": { "$and": [
                { "$expr": { "$eq": ["$k", "$$v"] } },
                { "$expr": { "$gt": [{ "$divide": [1, "$$w"] }, "$d"] } } ] } }]),
        ),
        lookup(
            json!({ "v": "$v" }),
            json!([{ "$match": { "$and": [
                { "$expr": { "$gt": [{ "$divide": [1, "$d"] }, 0] } },
                { "$expr": { "$eq": ["$k", "$$v"] } } ] } }]),
        ),
        lookup(
            json!({ "v": "$v", "w": "$w" }),
            json!([{ "$match": { "$and": [
                { "$expr": { "$gt": ["$z", 99] } },
                { "$expr": { "$eq": ["$k", { "$divide": [1, "$$w"] }] } },
                { "$expr": { "$eq": ["$k", "$$v"] } } ] } }]),
        ),
    ];
    let pool = pool();
    let mut joined_something = false;
    for seed in 1..=SEEDS {
        let mut rng = Gen(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let foreign: Vec<Document> = (0..14)
            .map(|i| with_field(i, "k", &pool[rng.below(pool.len())], rng.below(4) as i64))
            .collect();
        let inputs: Vec<Document> = (0..9)
            .map(|i| {
                let mut d = with_field(i, "v", &pool[rng.below(pool.len())], 0);
                d.insert("w", rng.below(4) as i64);
                d
            })
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let state = state_with(&dir, inputs, foreign);
        for pipeline in &pipelines {
            let run = both(&state, pipeline);
            joined_something |= run.probes > 0;
        }
    }
    assert!(joined_something, "the corpora exercised the join, not only the loop");
}

/// One row per type pair: a foreign `k` and an input `v` that `$eq` calls equal join,
/// and the others do not, as the loop says.
#[test]
fn each_type_pair_joins_as_eq_says() {
    let big = 9_007_199_254_740_992.0_f64;
    let pairs: Vec<(Bson, Bson, bool)> = vec![
        (Bson::Int32(5), Bson::Int64(5), true),
        (Bson::Int64(5), Bson::Double(5.0), true),
        (Bson::Double(-0.0), Bson::Double(0.0), true),
        (Bson::Double(-0.0), Bson::Int32(0), true),
        (Bson::Double(f64::NAN), Bson::Double(f64::NAN), true),
        (Bson::Int64(9_007_199_254_740_993), Bson::Double(big), false),
        (Bson::Int64(9_007_199_254_740_992), Bson::Double(big), true),
        (Bson::Int64(i64::MAX), Bson::Double(9_223_372_036_854_775_808.0), false),
        (Bson::Symbol("a".into()), Bson::String("a".into()), true),
        (Bson::String("1".into()), Bson::Int32(1), false),
        (Bson::Boolean(true), Bson::Int32(1), false),
        (Bson::Null, Bson::Null, true),
        (Bson::Array(vec![Bson::Int32(1)]), Bson::Int32(1), false),
        (
            Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]),
            Bson::Array(vec![Bson::Int64(1), Bson::Double(2.0)]),
            true,
        ),
        (Bson::Document(doc! { "a": 1, "b": 2 }), Bson::Document(doc! { "b": 2, "a": 1 }), false),
        (
            Bson::Document(doc! { "a": 1, "b": 2 }),
            Bson::Document(doc! { "a": 1.0, "b": 2_i64 }),
            true,
        ),
        (
            Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: vec![1, 2],
            }),
            Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::UserDefined(0x80),
                bytes: vec![1, 2],
            }),
            false,
        ),
    ];
    for (k, v, equal) in pairs {
        let dir = tempfile::tempdir().unwrap();
        // A second input, so the table is built and probed for the pair.
        let state = state_with(
            &dir,
            vec![doc! { "_id": 1, "v": v.clone() }, doc! { "_id": 2, "v": v.clone() }],
            vec![doc! { "_id": 10, "k": k.clone() }],
        );
        let run = both(&state, &eq_on_k());
        assert_eq!(run.probes, 1, "{k:?} against {v:?}: the second input probed");
        let got = ids(run.outcome.as_ref().unwrap());
        let want: Vec<Value> = if equal { vec![json!(10)] } else { vec![] };
        assert_eq!(got, vec![want.clone(), want], "{k:?} against {v:?}");
    }
}

/// A missing field is `null` to `$eq`, so it joins `null` and a missing variable.
#[test]
fn a_missing_key_is_null_to_the_join() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(
        &dir,
        vec![doc! { "_id": 1 }, doc! { "_id": 2, "v": Bson::Null }, doc! { "_id": 3, "v": 4 }],
        vec![doc! { "_id": 10 }, doc! { "_id": 11, "k": Bson::Null }, doc! { "_id": 12, "k": 4 }],
    );
    let run = both(&state, &eq_on_k());
    assert_eq!(
        ids(run.outcome.as_ref().unwrap()),
        vec![vec![json!(10), json!(11)], vec![json!(10), json!(11)], vec![json!(12)]]
    );
}

/// Matches come in the foreign scan's order, whatever order they were filed in, and
/// an empty bucket still runs the remaining stages.
#[test]
fn matches_keep_scan_order_and_an_empty_bucket_still_runs_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    let foreign: Vec<Document> =
        (0..30).map(|i| doc! { "_id": i, "k": i % 3, "z": 30 - i }).collect();
    let state = state_with(
        &dir,
        vec![doc! { "_id": 1, "v": 1 }, doc! { "_id": 2, "v": 2 }, doc! { "_id": 3, "v": 99 }],
        foreign,
    );
    let run = both(&state, &eq_on_k());
    let got = ids(run.outcome.as_ref().unwrap());
    assert_eq!(got[0], (0..30).filter(|i| i % 3 == 1).map(|i| json!(i)).collect::<Vec<_>>());
    assert!(got[2].is_empty());
    // `$count` over an empty bucket answers as the loop does: one document, zero.
    let counted = lookup(
        json!({ "v": "$v" }),
        json!([{ "$match": { "$expr": { "$eq": ["$k", "$$v"] } } }, { "$count": "n" }]),
    );
    let run = both(&state, &counted);
    let docs = run.outcome.unwrap();
    assert_eq!(docs["documents"][2]["got"], json!([{ "n": 0 }]));
    assert_eq!(docs["documents"][0]["got"], json!([{ "n": 10 }]));
}

/// A single-input `$lookup` never builds; two inputs build once and probe once.
#[test]
fn the_table_is_built_from_the_second_input_and_once() {
    let foreign: Vec<Document> = (0..5).map(|i| doc! { "_id": i, "k": i }).collect();
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, vec![doc! { "_id": 1, "v": 1 }], foreign.clone());
    let one = both(&state, &eq_on_k());
    assert_eq!((one.builds, one.probes), (0, 0));
    let dir = tempfile::tempdir().unwrap();
    let inputs = (1..=4).map(|i| doc! { "_id": i, "v": i }).collect();
    let state = state_with(&dir, inputs, foreign);
    let many = both(&state, &eq_on_k());
    assert_eq!((many.builds, many.probes), (1, 3));
}

/// CE-1: `B(i)` raises, but every pair is decided false by another clause, so the
/// loop never evaluates it. The join must not raise either.
#[test]
fn a_local_key_that_raises_is_not_raised_when_the_loop_would_not() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(
        &dir,
        vec![doc! { "_id": 1, "n": 1 }, doc! { "_id": 2, "n": 0 }],
        vec![doc! { "_id": 10, "k": 1, "kind": "y" }],
    );
    let pipeline = lookup(
        json!({ "n": "$n" }),
        json!([{ "$match": { "kind": "x", "$expr": { "$eq": ["$k", { "$divide": [1, "$$n"] }] } } }]),
    );
    let run = both(&state, &pipeline);
    assert!(run.outcome.is_ok(), "{:?}", run.outcome);
    // And where the loop does evaluate it, both raise the same error.
    let raising = lookup(
        json!({ "n": "$n" }),
        json!([{ "$match": { "$expr": { "$eq": ["$k", { "$divide": [1, "$$n"] }] } } }]),
    );
    assert!(both(&state, &raising).outcome.is_err());
}

/// CE-2: `A(d)` raises on a foreign document that no input reaches, because another
/// clause is false for every pair. The loop answers; so must the join.
#[test]
fn a_foreign_key_that_raises_is_not_raised_when_the_loop_would_not() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(
        &dir,
        vec![doc! { "_id": 1, "v": 1, "tag": "p" }, doc! { "_id": 2, "v": 2, "tag": "p" }],
        vec![doc! { "_id": 10, "k": 1, "x": 1 }, doc! { "_id": 11, "k": 2, "x": 0 }],
    );
    let pipeline = lookup(
        json!({ "v": "$v", "tag": "$tag" }),
        json!([{ "$match": { "$and": [
            { "$expr": { "$eq": ["$$tag", "q"] } },
            { "$expr": { "$eq": [{ "$divide": [1, "$x"] }, "$$v"] } } ] } }]),
    );
    let run = both(&state, &pipeline);
    assert!(run.outcome.is_ok(), "{:?}", run.outcome);
    // A later input that does reach it raises, in both.
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(
        &dir,
        vec![doc! { "_id": 1, "v": 1, "tag": "p" }, doc! { "_id": 2, "v": 2, "tag": "q" }],
        vec![doc! { "_id": 10, "k": 1, "x": 1 }, doc! { "_id": 11, "k": 2, "x": 0 }],
    );
    assert!(both(&state, &pipeline).outcome.is_err());
}

/// CE-3: the independent clause raises for a document whose pair another clause has
/// already decided false. Hoisting it once would raise a 400; the pair is false.
#[test]
fn a_mixed_hoist_that_raises_is_abandoned_not_raised() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(
        &dir,
        vec![doc! { "_id": 1, "v": 2 }, doc! { "_id": 2, "v": 3 }],
        vec![
            // The independent clauses raise for this document, whose pair is decided
            // false by the key for both inputs.
            doc! { "_id": 10, "k": 1, "kind": "x", "z": 0 },
            // This one the independent clause rules out, though its key matches input 1:
            // an abandoned hoist must still apply it, per pair.
            doc! { "_id": 11, "k": 2, "kind": "y", "z": 1 },
            doc! { "_id": 12, "k": 2, "kind": "x", "z": 1 },
        ],
    );
    let pipeline = lookup(
        json!({ "v": "$v" }),
        json!([{ "$match": { "$and": [
            { "$expr": { "$eq": ["$k", "$$v"] } },
            { "$expr": { "$eq": ["$kind", "x"] } },
            { "$expr": { "$eq": [{ "$divide": [1, "$z"] }, 1] } } ] } }]),
    );
    let run = both(&state, &pipeline);
    assert_eq!(ids(run.outcome.as_ref().unwrap()), vec![vec![json!(12)], vec![]]);
    // A purely independent leading `$match` keeps raising, as it always did.
    let independent = lookup(
        json!({ "v": "$v" }),
        json!([{ "$match": { "$expr": { "$eq": [{ "$divide": [1, "$z"] }, 1] } } }]),
    );
    assert!(both(&state, &independent).outcome.is_err());
}

/// An error in a residual sends that input to the loop; so does an error in the key's
/// local side. The answers are the loop's, errors included.
#[test]
fn a_residual_that_raises_falls_back_for_that_input_only() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(
        &dir,
        vec![
            doc! { "_id": 1, "v": 1, "d": 1 },
            doc! { "_id": 2, "v": 2, "d": 0 },
            doc! { "_id": 3, "v": 1, "d": 1 },
        ],
        vec![doc! { "_id": 10, "k": 1 }, doc! { "_id": 11, "k": 2 }],
    );
    let pipeline = lookup(
        json!({ "v": "$v", "d": "$d" }),
        json!([{ "$match": { "$and": [
            { "$expr": { "$eq": ["$k", "$$v"] } },
            { "$expr": { "$gt": [{ "$divide": [1, "$$d"] }, 0] } } ] } }]),
    );
    let run = both(&state, &pipeline);
    assert!(run.outcome.is_err(), "the second input raises in the loop too");
}

/// A `Decimal128` on either side cannot be keyed: the loop answers, and `$eq` ranks it
/// equal to every number.
#[test]
fn a_decimal128_key_takes_the_loop() {
    let dec: bson::Decimal128 = "7".parse().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(
        &dir,
        vec![doc! { "_id": 1, "v": 1 }, doc! { "_id": 2, "v": 2 }],
        vec![doc! { "_id": 10, "k": Bson::Decimal128(dec) }, doc! { "_id": 11, "k": 2 }],
    );
    let run = both(&state, &eq_on_k());
    assert_eq!(run.probes, 0, "a Decimal128 on the build side declines the whole execution");
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(
        &dir,
        vec![doc! { "_id": 1, "v": 1 }, doc! { "_id": 2, "v": Bson::Decimal128(dec) }],
        vec![doc! { "_id": 10, "k": 1 }, doc! { "_id": 11, "k": 2 }],
    );
    both(&state, &eq_on_k());
}

/// The attached ceiling reads the same on the first input and on a later one.
#[test]
fn the_ceiling_is_the_loops_on_any_input() {
    let foreign: Vec<Document> = (0..6).map(|i| doc! { "_id": i, "k": i % 2 }).collect();
    for limit in [6, 7, 9, 12, 100] {
        let dir = tempfile::tempdir().unwrap();
        let inputs = (0..4).map(|i| doc! { "_id": i, "v": i % 2 }).collect();
        let state = state_with(&dir, inputs, foreign.clone());
        both_limited(&state, &eq_on_k(), limit);
    }
}

/// An empty foreign side, an empty bucket and an empty input.
#[test]
fn empty_sides_answer_as_the_loop_does() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        state_with(&dir, vec![doc! { "_id": 1, "v": 1 }, doc! { "_id": 2, "v": 2 }], vec![]);
    both(&state, &eq_on_k());
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(&dir, vec![], vec![doc! { "_id": 1, "k": 1 }]);
    both(&state, &eq_on_k());
}

/// Recognition goes through `$and` only: `$or`, `$nor` and an `$expr`'s own `$and` are
/// not joins (no table is built) and still answer as the loop does.
#[test]
fn only_a_top_level_and_of_an_eq_is_recognised() {
    let dir = tempfile::tempdir().unwrap();
    let foreign: Vec<Document> = (0..6).map(|i| doc! { "_id": i, "k": i % 3 }).collect();
    let inputs = (0..4).map(|i| doc! { "_id": i, "v": i % 3 }).collect();
    let state = state_with(&dir, inputs, foreign);
    let shapes = [
        json!({ "$or": [{ "$expr": { "$eq": ["$k", "$$v"] } }, { "k": 99 }] }),
        json!({ "$nor": [{ "$expr": { "$ne": ["$k", "$$v"] } }] }),
        json!({ "$expr": { "$and": [{ "$eq": ["$k", "$$v"] }, { "$gte": ["$k", 0] }] } }),
        json!({ "$expr": { "$eq": ["$k", "$$v", 1] } }),
    ];
    for shape in shapes {
        let pipeline = lookup(json!({ "v": "$v" }), json!([{ "$match": shape }]));
        let run = run_with(&state, &pipeline, true, aggregate::DEFAULT_MAX_DOCUMENTS);
        if shape.to_string().contains("[\"$k\"")
            && !shape.to_string().contains("$eq\":[\"$k\",\"$$v\",1")
        {
            assert_eq!((run.builds, run.probes), (0, 0), "{shape}");
        }
        let looped = run_with(&state, &pipeline, false, aggregate::DEFAULT_MAX_DOCUMENTS);
        assert_eq!(run.outcome, looped.outcome, "{shape}");
    }
}

/// A `$match` that reads a variable but is not first runs per input document with the
/// bindings, and a top-level `$match` has none to read.
#[test]
fn a_later_match_reads_the_variable_and_a_top_level_one_cannot() {
    let dir = tempfile::tempdir().unwrap();
    let foreign: Vec<Document> = (0..6).map(|i| doc! { "_id": i, "k": i % 3 }).collect();
    let state =
        state_with(&dir, vec![doc! { "_id": 1, "v": 1 }, doc! { "_id": 2, "v": 2 }], foreign);
    let pipeline = lookup(
        json!({ "v": "$v" }),
        json!([{ "$limit": 100 }, { "$match": { "$expr": { "$eq": ["$k", "$$v"] } } }]),
    );
    let run = both(&state, &pipeline);
    assert_eq!(
        ids(run.outcome.as_ref().unwrap()),
        vec![vec![json!(1), json!(4)], vec![json!(2), json!(5)]]
    );
    let top = json!([{ "$match": { "$expr": { "$eq": ["$k", "$$v"] } } }]);
    assert!(
        agg_limits(&state, &root(), "app", "orders", &top, aggregate::Limits::default()).is_err()
    );
}

/// An `$elemMatch` body reads the variable, with the element as the document.
#[test]
fn an_elem_match_body_reads_the_variable() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(
        &dir,
        vec![doc! { "_id": 1, "v": 1 }, doc! { "_id": 2, "v": 2 }],
        vec![
            doc! { "_id": 10, "lines": [{ "q": 1 }, { "q": 5 }] },
            doc! { "_id": 11, "lines": [{ "q": 2 }] },
            doc! { "_id": 12, "lines": [] },
        ],
    );
    let pipeline = lookup(
        json!({ "v": "$v" }),
        json!([{ "$match": { "lines": { "$elemMatch": { "$expr": { "$eq": ["$q", "$$v"] } } } } }]),
    );
    let run = both(&state, &pipeline);
    assert_eq!(ids(run.outcome.as_ref().unwrap()), vec![vec![json!(10)], vec![json!(11)]]);
    assert_eq!(run.probes, 0, "a variable inside `$elemMatch` is never a join");
}

/// A nested `$lookup` builds its table for each outer input.
#[test]
fn a_nested_lookup_rebuilds_per_outer_input() {
    let dir = tempfile::tempdir().unwrap();
    let state = live_state(&dir);
    seed_docs(&state, "orders", vec![doc! { "_id": 1, "v": 1 }, doc! { "_id": 2, "v": 2 }]);
    seed_docs(
        &state,
        "mid",
        vec![doc! { "_id": 1, "k": 1, "w": 1 }, doc! { "_id": 2, "k": 2, "w": 2 }],
    );
    seed_docs(&state, "items", (0..4).map(|i| doc! { "_id": i, "k": i % 2 + 1 }).collect());
    let pipeline = json!([{ "$lookup": { "from": "mid", "let": { "v": "$v" }, "as": "got",
        "pipeline": [
            { "$match": { "$expr": { "$eq": ["$k", "$$v"] } } },
            { "$lookup": { "from": "items", "let": { "w": "$w" }, "as": "deep",
                "pipeline": [{ "$match": { "$expr": { "$eq": ["$k", "$$w"] } } }] } }
        ] } }]);
    let run = both(&state, &pipeline);
    assert!(run.outcome.is_ok());
}

/// One cell of the measurement, from the environment: `INPUTS`, `FOREIGN`, `FAN` (matches
/// per input), `PAD` (bytes of padding per foreign document), `JOIN` (`0` switches
/// recognition off), `SHAPE` (`eq`, `independent` or `decimal`). Not run in CI.
/// `cargo test --release -- --ignored join_cell --nocapture`.
#[test]
#[ignore = "a measurement, not a check"]
fn join_cell() {
    let var = |name: &str, default: usize| -> usize {
        std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
    };
    let (inputs, foreign, fan, pad) =
        (var("INPUTS", 1000), var("FOREIGN", 10_000), var("FAN", 1), var("PAD", 24));
    let join = var("JOIN", 1) != 0;
    let shape = std::env::var("SHAPE").unwrap_or_else(|_| "eq".into());
    let dir = tempfile::tempdir().unwrap();
    let state = live_state(&dir);
    state.engine.create_collection("app", "orders").unwrap();
    state.engine.create_collection("app", "items").unwrap();
    let orders = state.engine.get_collection("app", "orders").unwrap();
    let items = state.engine.get_collection("app", "items").unwrap();
    let keys = (foreign / fan) as i64;
    let docs: Vec<Document> =
        (0..inputs as i64).map(|i| doc! { "_id": i, "v": (i * 7919) % keys }).collect();
    for chunk in docs.chunks(1000) {
        state.engine.insert_many(&orders, chunk.to_vec()).unwrap();
    }
    let padding = "x".repeat(pad);
    let docs: Vec<Document> = (0..foreign as i64)
        .map(|i| {
            let mut d = doc! { "_id": i, "kind": "line", "pad": padding.clone() };
            // The `decimal` shape files one `Decimal128` key among the others.
            if shape == "decimal" && i == 0 {
                d.insert("k", bson::Bson::Decimal128("7".parse().unwrap()));
            } else {
                d.insert("k", i % keys);
            }
            d
        })
        .collect();
    for chunk in docs.chunks(1000) {
        state.engine.insert_many(&items, chunk.to_vec()).unwrap();
    }
    let pipeline = if shape == "independent" {
        // A leading `$match` that names no variable, then the correlation computed
        // in a stage: what had to be written before, and still runs as before.
        lookup(
            json!({ "v": "$v" }),
            json!([
                { "$match": { "kind": "line" } },
                { "$addFields": { "mine": { "$eq": ["$k", "$$v"] } } },
                { "$match": { "mine": true } },
            ]),
        )
    } else {
        eq_on_k()
    };
    let started = std::time::Instant::now();
    let run = run_with(&state, &pipeline, join, 10_000_000);
    let ms = started.elapsed().as_millis();
    let attached: usize =
        run.outcome.as_ref().map(|v| ids(v).iter().map(Vec::len).sum()).unwrap_or(0);
    println!(
        "CELL shape={shape} inputs={inputs} foreign={foreign} fan={fan} pad={pad} join={join} ms={ms} attached={attached} builds={} probes={} ok={}",
        run.builds,
        run.probes,
        run.outcome.is_ok()
    );
}

/// CE-1 with a **variable** clause as the false one: `$gt` of the foreign `z` against a
/// variable is false for every pair and is written before the key, so the loop never
/// evaluates the key's local side, which raises for the second input. The probe does
/// evaluate it, raises, and hands that input to the loop: no error.
#[test]
fn a_local_key_that_raises_beside_a_false_variable_clause_is_not_raised() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(
        &dir,
        vec![
            doc! { "_id": 1, "m": 5, "n": 1 },
            doc! { "_id": 2, "m": 5, "n": 0 },
            doc! { "_id": 3, "m": 5, "n": 1 },
        ],
        vec![doc! { "_id": 10, "k": 1, "z": 0 }, doc! { "_id": 11, "k": 2, "z": 1 }],
    );
    let pipeline = lookup(
        json!({ "m": "$m", "n": "$n" }),
        json!([{ "$match": { "$and": [
            { "$expr": { "$gt": ["$z", "$$m"] } },
            { "$expr": { "$eq": ["$k", { "$divide": [1, "$$n"] }] } } ] } }]),
    );
    let run = both(&state, &pipeline);
    assert_eq!(ids(run.outcome.as_ref().unwrap()), vec![vec![], vec![], vec![]] as Vec<Vec<Value>>);
    assert_eq!(
        run.probes, 2,
        "the probe ran for the second and third input, and declined for the second"
    );
}

/// A residual that raises gives the loop's own error, word for word.
#[test]
fn a_residual_error_is_the_loops_message() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_with(
        &dir,
        vec![doc! { "_id": 1, "v": 1, "d": 1 }, doc! { "_id": 2, "v": 1, "d": 0 }],
        vec![doc! { "_id": 10, "k": 1 }],
    );
    let pipeline = lookup(
        json!({ "v": "$v", "d": "$d" }),
        json!([{ "$match": { "$and": [
            { "$expr": { "$eq": ["$k", "$$v"] } },
            { "$expr": { "$gt": [{ "$divide": [1, "$$d"] }, 0] } } ] } }]),
    );
    let run = both(&state, &pipeline);
    let message = run.outcome.expect_err("the second input raises");
    assert!(message.contains("divide") || message.contains("zero"), "{message}");
}

/// The split of a conjunction hides an error only where ADR-211 lets a false clause
/// hold it back, and that is all of them only while every error an expression raises
/// while evaluating is deferrable (`Error::is_deferrable`). The day one is not, a
/// non-deferrable error written before a false independent clause would be returned by
/// the whole conjunction and hidden by the split: this test fails then, and the split
/// (`hoist_independent`, `matched_for_input`) is to be rethought, not this list edited.
/// Whether a document can make evaluating an expression or a filter raise this error.
/// **No catch-all arm**: a new `kimmy_core::Error` variant does not compile here until
/// someone has said which side it is on, and a variant that data can raise must be
/// deferrable (checked below), or the split of a conjunction is no longer exact.
fn data_can_raise(e: &kimmy_core::Error) -> bool {
    use kimmy_core::Error as E;
    match e {
        E::InvalidQuery(_) => true,
        E::Internal(_)
        | E::Limit(_)
        | E::DatabaseNotFound(_)
        | E::CollectionNotFound { .. }
        | E::CollectionExists { .. }
        | E::IndexExists { .. }
        | E::DocumentNotFound(_)
        | E::DuplicateKey(_)
        | E::UniqueViolation { .. }
        | E::Unsupported(_)
        | E::InvalidDocumentId { .. }
        | E::InvalidName { .. }
        | E::InvalidUpdate(_)
        | E::UnsupportedOperator { .. }
        | E::ResumeTokenExpired
        | E::MalformedResumeToken
        | E::MalformedCursor
        | E::MalformedStamp
        | E::Bson(_)
        | E::Serialization(_) => false,
    }
}

#[test]
fn an_evaluation_error_is_always_deferrable() {
    let doc = doc! { "a": "text", "n": 0, "arr": [1, 2], "o": { "x": 1 } };
    let failing = [
        json!({ "$divide": [1, "$n"] }),
        json!({ "$add": ["$a", 1] }),
        json!({ "$mod": [5, "$n"] }),
        json!({ "$toInt": "$a" }),
        json!({ "$sqrt": -1 }),
        json!({ "$arrayElemAt": ["$a", 0] }),
        json!({ "$size": "$a" }),
        json!({ "$substr": ["$arr", 0, 1] }),
        json!({ "$multiply": ["$a", 2] }),
        json!({ "$concat": ["$a", 1] }),
        json!({ "$let": { "vars": { "x": { "$divide": [1, "$n"] } }, "in": "$$x" } }),
    ];
    let mut seen = 0;
    for expr in failing {
        let bson = bson::serialize_to_bson(&expr).unwrap();
        let Ok(parsed) = kimmy_query::Expr::parse(&bson) else { continue };
        if let Err(e) = parsed.eval(&doc) {
            seen += 1;
            assert!(data_can_raise(&e), "{expr}: {e:?} is not classed as raised by data");
            assert!(e.is_deferrable(), "{expr}: {e:?} is not deferrable");
        }
    }
    assert!(seen >= 5, "the list exercised {seen} failures");
    // Whatever data can raise is deferrable.
    let sample = kimmy_core::Error::InvalidQuery("x".into());
    assert!(!data_can_raise(&sample) || sample.is_deferrable());
}

/// An empty input reads nothing of the foreign collection, but the stage is still
/// authorized first: a missing foreign collection is a `404` and a forbidden one a
/// `403` whether or not any document reaches the stage (ADR-206's note, ADR-222).
#[test]
fn an_empty_input_still_authorizes_the_foreign_collection() {
    let dir = tempfile::tempdir().unwrap();
    let state = live_state(&dir);
    seed_docs(&state, "orders", vec![]);
    let status = |auth: &Auth, pipeline: &Value| match agg_limits(
        &state,
        auth,
        "app",
        "orders",
        pipeline,
        aggregate::Limits::default(),
    ) {
        Ok(_) => 200,
        Err(e) => e.status.as_u16(),
    };
    // No such foreign collection: 404, with no input at all.
    assert_eq!(status(&root(), &eq_on_k()), 404);
    // It exists but the caller may not read it: 403.
    seed_docs(&state, "items", vec![doc! { "_id": 1, "k": 1 }]);
    assert_eq!(status(&root(), &eq_on_k()), 200);
    let reader = Auth(kimmy_auth::Principal::new(
        "reader",
        vec![kimmy_auth::Grant {
            db: "app".into(),
            collection: "orders".into(),
            actions: vec![kimmy_auth::Action::Read],
        }],
    ));
    assert_eq!(status(&reader, &eq_on_k()), 403);
}
