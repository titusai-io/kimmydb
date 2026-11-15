//! Random operator updates against `merge_state::Writer`: the effects `apply_with_effects`
//! plans, written to a writer that holds the same document, render the post-image the engine
//! produced, step after step (the document of each step is what the writer rendered), and the
//! only refusals are the writer's documented ones (`field-merge-refusals-q7`). The plan errs
//! exactly when the engine errs, except for the `$rename` shapes it refuses itself. The steps
//! include positional paths (`$`, `$[]`, `$[id]` with `arrayFilters`), nested arrays, pipeline
//! updates and NaN.

use bson::{Bson, Document, doc};
use kimmy_core::effects::{Outcome, UpdateRefused};
use proptest::prelude::*;
use proptest::test_runner::{TestCaseError, TestRunner};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::path;
use crate::update::{OpKind, Operation, Update};
use crate::update_effects::{Plan, apply_with_effects};
use crate::update_effects_tests::{NOW, bytes, canonical, render, writer_with};

fn small_int() -> impl Strategy<Value = i32> {
    prop_oneof![Just(0), Just(1), Just(-1), Just(7), -50..50, Just(i32::MAX), Just(i32::MIN)]
}

fn an_int() -> impl Strategy<Value = Bson> {
    prop_oneof![small_int().prop_map(Bson::Int32), (-1_000_i64..1_000).prop_map(Bson::Int64)]
}

fn a_double() -> impl Strategy<Value = f64> {
    prop_oneof![
        Just(0.0),
        Just(-0.0),
        Just(1.5),
        Just(-2.25),
        Just(1e300),
        Just(3.0),
        Just(f64::NAN)
    ]
}

pub(crate) fn arb_doc() -> impl Strategy<Value = Document> {
    (
        an_int(),
        a_double(),
        "[a-c]{0,3}",
        prop::collection::vec(small_int(), 0..4),
        prop::option::of((small_int(), prop::option::of(small_int()))),
        prop::option::of(an_int()),
        prop::collection::vec((small_int(), small_int()), 0..4),
        prop::collection::vec(prop::collection::vec(small_int(), 0..3), 0..3),
    )
        .prop_map(|(n, d, s, arr, sub, x, items, grid)| {
            let mut doc = doc! { "_id": 1, "n": n, "d": d, "s": s, "arr": arr };
            let items: Vec<Bson> =
                items.into_iter().map(|(k, q)| Bson::Document(doc! { "k": k, "q": q })).collect();
            doc.insert("items", items);
            doc.insert("grid", grid);
            // the fields the `$rename` slots work on: an array of documents, a scalar, a string,
            // an integer (a counter)
            doc.insert(
                "ra",
                vec![Bson::Document(doc! { "t": 1 }), Bson::Document(doc! { "t": 2 })],
            );
            doc.insert("rs", 5);
            doc.insert("rt", "txt");
            doc.insert("rn", 7);
            if let Some((k, j)) = sub {
                let mut sub = doc! { "k": k };
                if let Some(j) = j {
                    sub.insert("j", j);
                }
                doc.insert("sub", sub);
            }
            if let Some(x) = x {
                doc.insert("x", x);
            }
            doc
        })
}

type Slot = Option<(&'static str, &'static str, Bson)>;

fn slot_n() -> impl Strategy<Value = Slot> {
    prop::option::of(prop_oneof![
        an_int().prop_map(|b| ("$set", "n", b)),
        "[a-z]{0,2}".prop_map(|s| ("$set", "n", Bson::String(s))),
        Just(("$unset", "n", Bson::String(String::new()))),
        an_int().prop_map(|b| ("$inc", "n", b)),
        small_int().prop_map(|b| ("$mul", "n", Bson::Int32(b))),
        small_int().prop_map(|b| ("$min", "n", Bson::Int32(b))),
        small_int().prop_map(|b| ("$max", "n", Bson::Int32(b))),
        (prop_oneof![Just("and"), Just("or"), Just("xor")], small_int()).prop_map(|(op, x)| {
            let mut step = Document::new();
            step.insert(op, Bson::Int32(x));
            ("$bit", "n", Bson::Document(step))
        }),
    ])
}

fn slot_d() -> impl Strategy<Value = Slot> {
    prop::option::of(prop_oneof![
        a_double().prop_map(|d| ("$set", "d", Bson::Double(d))),
        small_int().prop_map(|b| ("$inc", "d", Bson::Int32(b))),
        a_double().prop_map(|d| ("$inc", "d", Bson::Double(d))),
        small_int().prop_map(|b| ("$mul", "d", Bson::Int32(b))),
        a_double().prop_map(|d| ("$min", "d", Bson::Double(d))),
        a_double().prop_map(|d| ("$max", "d", Bson::Double(d))),
    ])
}

fn slot_s() -> impl Strategy<Value = Slot> {
    prop::option::of(prop_oneof![
        "[a-c]{0,3}".prop_map(|s| ("$set", "s", Bson::String(s))),
        Just(("$unset", "s", Bson::String(String::new()))),
        Just(("$rename", "s", Bson::String("t".into()))),
    ])
}

fn slot_arr() -> impl Strategy<Value = Slot> {
    prop::option::of(prop_oneof![
        small_int().prop_map(|b| ("$push", "arr", Bson::Int32(b))),
        small_int().prop_map(|b| ("$addToSet", "arr", Bson::Int32(b))),
        small_int().prop_map(|b| ("$pull", "arr", Bson::Int32(b))),
        prop_oneof![Just(1), Just(-1)].prop_map(|b| ("$pop", "arr", Bson::Int32(b))),
        prop::collection::vec(small_int(), 0..3).prop_map(|v| (
            "$set",
            "arr",
            Bson::Array(v.into_iter().map(Bson::Int32).collect())
        )),
        Just(("$unset", "arr", Bson::String(String::new()))),
        small_int().prop_map(|b| ("$set", "arr.0", Bson::Int32(b))),
        small_int().prop_map(|b| ("$inc", "arr.1", Bson::Int32(b))),
        Just(("$unset", "arr.1", Bson::String(String::new()))),
    ])
}

fn slot_k() -> impl Strategy<Value = Slot> {
    prop::option::of(prop_oneof![
        small_int().prop_map(|b| ("$set", "sub.k", Bson::Int32(b))),
        small_int().prop_map(|b| ("$inc", "sub.k", Bson::Int32(b))),
        Just(("$unset", "sub.k", Bson::String(String::new()))),
    ])
}

fn slot_j() -> impl Strategy<Value = Slot> {
    prop::option::of(prop_oneof![
        small_int().prop_map(|b| ("$set", "sub.j", Bson::Int32(b))),
        Just(("$unset", "sub.j", Bson::String(String::new()))),
    ])
}

fn slot_x() -> impl Strategy<Value = Slot> {
    prop::option::of(prop_oneof![
        an_int().prop_map(|b| ("$set", "x", b)),
        an_int().prop_map(|b| ("$inc", "x", b)),
        a_double().prop_map(|d| ("$inc", "x", Bson::Double(d))),
        small_int().prop_map(|b| ("$mul", "x", Bson::Int32(b))),
        small_int().prop_map(|b| ("$push", "x", Bson::Int32(b))),
        Just(("$unset", "x", Bson::String(String::new()))),
    ])
}

fn slot_date() -> impl Strategy<Value = Slot> {
    prop::option::of(Just(("$currentDate", "dt", Bson::Boolean(true))))
}

fn slot_items() -> impl Strategy<Value = Slot> {
    prop::option::of(prop_oneof![
        small_int().prop_map(|b| ("$set", "items.$[].q", Bson::Int32(b))),
        small_int().prop_map(|b| ("$inc", "items.$[].q", Bson::Int32(b))),
        small_int().prop_map(|b| ("$set", "items.$[e].q", Bson::Int32(b))),
        small_int().prop_map(|b| ("$inc", "items.$[e].q", Bson::Int32(b))),
        small_int().prop_map(|b| ("$set", "items.$.q", Bson::Int32(b))),
        Just(("$unset", "items.$.q", Bson::String(String::new()))),
        small_int().prop_map(|b| ("$set", "items.0.q", Bson::Int32(b))),
        Just(("$unset", "items.1.k", Bson::String(String::new()))),
        small_int().prop_map(|b| ("$push", "items", Bson::Document(doc! { "k": b, "q": b }))),
        small_int().prop_map(|b| ("$pull", "items", Bson::Document(doc! { "k": b }))),
    ])
}

fn slot_grid() -> impl Strategy<Value = Slot> {
    prop::option::of(prop_oneof![
        small_int().prop_map(|b| ("$set", "grid.0.1", Bson::Int32(b))),
        small_int().prop_map(|b| ("$inc", "grid.1.0", Bson::Int32(b))),
        small_int().prop_map(|b| ("$push", "grid.0", Bson::Int32(b))),
        Just(("$unset", "grid.0.0", Bson::String(String::new()))),
        small_int().prop_map(|b| ("$set", "grid.2.5", Bson::Int32(b))),
        small_int().prop_map(|b| ("$set", "grid.0", Bson::Array(vec![Bson::Int32(b)]))),
    ])
}

/// A `$rename`: the shapes the reference refuses (a source or a destination inside an array
/// element by an index or not, below a scalar, a counter into an array), the missing sources it
/// leaves alone, and the moves it allows.
fn slot_rename() -> impl Strategy<Value = Slot> {
    let pairs: Vec<(&'static str, &'static str)> = vec![
        // refused
        ("ra.0.t", "r1"),
        ("ra.t", "r2"),
        ("rt", "ra.0.u"),
        ("rt", "ra.u"),
        ("rs.y", "r3"),
        ("rt", "rs.y"),
        ("rn", "ra.1"),
        ("ra.1.t", "ra.0.t"),
        // a missing source: nothing is done, nothing is checked
        ("ra.0.nope", "r4"),
        ("ra.5.t", "r5"),
        ("nomissing", "ra.0.u"),
        ("nomissing", "rs.y"),
        ("ra.0.nope", "ra.1.x"),
        // allowed (the counter rename is the writer's own refusal)
        ("rt", "r6"),
        ("ra", "ra2"),
        ("rn", "rn2"),
    ];
    prop::option::of(
        proptest::sample::select(pairs)
            .prop_map(|(from, to)| ("$rename", from, Bson::String(to.into()))),
    )
}

/// One step of a run: an operator update (with the `arrayFilters` and the matching filter its
/// positional paths need) or a pipeline update.
#[derive(Clone, Debug)]
enum Step {
    Ops { update: Document, array_filters: Vec<Document>, matched: Document },
    Pipeline(Vec<Document>),
}

fn arb_ops() -> impl Strategy<Value = Step> {
    (
        (slot_n(), slot_d(), slot_s(), slot_arr(), slot_k(), slot_j(), slot_x(), slot_date()),
        (slot_items(), slot_grid(), slot_rename()),
    )
        .prop_map(|((a, b, c, d, e, f, g, h), (i, j, k))| {
            let mut update = Document::new();
            let mut each = false;
            let mut dollar = false;
            for (op, path, arg) in [a, b, c, d, e, f, g, h, i, j, k].into_iter().flatten() {
                each |= path.contains("$[e]");
                dollar |= path.contains(".$.");
                if !update.contains_key(op) {
                    update.insert(op, Document::new());
                }
                update.get_document_mut(op).unwrap().insert(path, arg);
            }
            let array_filters = if each { vec![doc! { "e.k": { "$gte": 1 } }] } else { Vec::new() };
            let matched = if dollar {
                doc! { "items.k": { "$gte": 1 } }
            } else {
                doc! {}
            };
            Step::Ops { update, array_filters, matched }
        })
}

fn arb_stage() -> impl Strategy<Value = Document> {
    prop_oneof![
        small_int().prop_map(|b| doc! { "$set": { "p1": { "$add": ["$n", b] } } }),
        Just(doc! { "$set": { "s": "str" } }),
        Just(doc! { "$unset": "s" }),
        Just(doc! { "$unset": ["x", "arr"] }),
        Just(doc! { "$set": { "sub": { "j": 2, "k": 1 } } }),
        Just(doc! { "$set": { "n": "$$REMOVE" } }),
        Just(doc! { "$set": { "_id": "$_id" } }),
        Just(doc! { "$set": { "items": { "$map": { "input": "$items", "as": "i",
        "in": { "k": "$$i.k", "q": 0 } } } } }),
        Just(doc! { "$project": { "n": 1, "s": 1, "sub": 1, "items": 1 } }),
        Just(doc! { "$replaceWith": { "$mergeObjects": ["$$ROOT", { "zz": 1 }] } }),
        Just(doc! { "$replaceWith": { "_id": "$_id", "n": "$n", "z": 9 } }),
        Just(doc! { "$set": { "d": { "$multiply": ["$d", -1] } } }),
    ]
}

fn arb_step() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => arb_ops(),
        1 => prop::collection::vec(arb_stage(), 1..4).prop_map(Step::Pipeline),
    ]
}

/// The writer's documented refusals (a double on an integer counter, a rename of a counter, an
/// integer past the writer's bound): not a difference between the effects and the engine.
fn documented(r: &UpdateRefused) -> bool {
    matches!(
        r,
        UpdateRefused::IncDoubleOnCounter(_)
            | UpdateRefused::RenameOfCounter(_)
            | UpdateRefused::IncOutOfRange(_)
    )
}

/// How many steps hit the extra refusal of a `$rename`, and how many a `$rename` whose source is
/// not there (a no-op): both must happen, or the generators prove nothing.
static REFUSED_RENAMES: AtomicUsize = AtomicUsize::new(0);
static NOOP_RENAMES: AtomicUsize = AtomicUsize::new(0);
/// Steps whose update holds a `$rename` and is refused for another reason (printed, not floored).
static OTHER_REFUSALS: AtomicUsize = AtomicUsize::new(0);

/// Whether a refusal text names a `$rename` of `operations`: it says `$rename` (the source and
/// array-element cases, q7s2f.out), or it is the recorded "cannot create field '<x>' in element
/// {...}" (q7s2d.out 4e) and `<x>` is a segment of the DESTINATION of one of the renames.
fn names_a_rename(text: &str, operations: &[Operation]) -> bool {
    if text.contains("$rename") {
        return true;
    }
    let Some(rest) = text.split("cannot create field '").nth(1) else { return false };
    let Some(x) = rest.split("' in element").next() else { return false };
    operations.iter().any(|op| match &op.kind {
        OpKind::Rename(target) => target.split('.').any(|seg| seg == x),
        _ => false,
    })
}

/// One run: the steps applied in turn to an engine document and to a writer holding the same one.
fn run(start: Document, steps: Vec<Step>) -> Result<(), TestCaseError> {
    let mut doc = start;
    let mut w = writer_with(&doc);
    for step in steps {
        let (update, matched, pipeline) = match &step {
            Step::Ops { update, array_filters, matched } => {
                if update.is_empty() {
                    continue;
                }
                match crate::update::parse_with_filters(update, array_filters) {
                    Ok(u) => (u, Some(matched.clone()), false),
                    Err(_) => continue,
                }
            }
            Step::Pipeline(stages) => {
                let stages: Vec<Bson> = stages.iter().cloned().map(Bson::Document).collect();
                match crate::update::parse_pipeline(&stages, &[]) {
                    Ok(u) => (u, None, true),
                    Err(_) => continue,
                }
            }
        };
        let filter = crate::update_effects_tests::filt(matched.unwrap_or_default());
        // the engine alone, on a copy: the plan must err exactly when it errs
        let mut engine_doc = doc.clone();
        let engine = crate::update::apply_matched(&update, &filter, &mut engine_doc, NOW);
        let planned = apply_with_effects(&update, Some(&filter), &doc, NOW);
        let (effects, post_image) = match (engine, planned) {
            (Ok(()), Ok(Plan::Effects { effects, post_image })) => {
                prop_assert_eq!(
                    bytes(&post_image),
                    bytes(&engine_doc),
                    "the plan's post-image is not the engine's: {:?} over {}",
                    step,
                    doc
                );
                (effects, post_image)
            }
            (Ok(()), Ok(Plan::Replace { .. })) => {
                return Err(TestCaseError::fail("a replacement from a generated update"));
            }
            // both refuse: the engine's `$rename` rules (a source or a destination in an array
            // element, below a scalar) are counted so that the run proves it reaches them
            // (counted only when BOTH refusal texts name the rename: any other refusal of an
            // update that holds a `$rename` goes to `OTHER_REFUSALS`)
            (Err(from_engine), Err(from_plan)) => {
                if let Update::Operators(operations) = &update
                    && operations.iter().any(|op| matches!(op.kind, OpKind::Rename(_)))
                {
                    let named = names_a_rename(&from_engine.to_string(), operations)
                        && names_a_rename(&from_plan.to_string(), operations);
                    let counter = if named { &REFUSED_RENAMES } else { &OTHER_REFUSALS };
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                continue;
            }
            (engine, planned) => {
                return Err(TestCaseError::fail(format!(
                    "the engine says {engine:?} and the plan says {:?} for {step:?} over {doc}",
                    planned.map(|_| ())
                )));
            }
        };
        // a `$rename` of a source that is not there did nothing and was not refused
        if let Update::Operators(operations) = &update {
            let missing = |op: &Operation| {
                matches!(op.kind, OpKind::Rename(_)) && path::resolve(&doc, &op.path).is_empty()
            };
            if operations.iter().any(missing) {
                NOOP_RENAMES.fetch_add(1, Ordering::Relaxed);
            }
        }
        let outcome = effects.apply(&mut w, 0);
        match &outcome {
            Outcome::Refused(r) => {
                prop_assert!(documented(r), "{step:?}: refused {r:?} over {doc}");
                // a refusal changes nothing
                prop_assert_eq!(bytes(&render(&w)), bytes(&doc));
            }
            _ => {
                let rendered = render(&w);
                if pipeline {
                    prop_assert_eq!(
                        canonical(&rendered),
                        canonical(&post_image),
                        "{:?} over {}",
                        step,
                        doc
                    );
                    prop_assert_eq!(
                        outcome.modified(),
                        canonical(&post_image) != canonical(&doc),
                        "modified: {:?} over {}: {:?}",
                        step,
                        doc,
                        effects
                    );
                } else {
                    prop_assert_eq!(
                        bytes(&rendered),
                        bytes(&post_image),
                        "{:?} over {}",
                        step,
                        doc
                    );
                    prop_assert_eq!(
                        outcome.modified(),
                        bytes(&post_image) != bytes(&doc),
                        "modified: {:?} over {}: {:?}",
                        step,
                        doc,
                        effects
                    );
                }
                doc = rendered;
            }
        }
    }
    Ok(())
}

/// The strategies are large, and a debug build gives each value tree a big frame: the run goes on
/// a thread with a stack of its own, not the test thread's 2 MiB.
#[test]
fn effects_through_the_writer_render_the_engines_post_image() {
    REFUSED_RENAMES.store(0, Ordering::Relaxed);
    NOOP_RENAMES.store(0, Ordering::Relaxed);
    OTHER_REFUSALS.store(0, Ordering::Relaxed);
    std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(|| {
            let mut runner = TestRunner::new(ProptestConfig::with_cases(400));
            let strategy = (arb_doc().boxed(), prop::collection::vec(arb_step().boxed(), 1..4));
            runner
                .run(&strategy, |(start, steps)| run(start, steps))
                .unwrap_or_else(|e| panic!("{e}"));
        })
        .unwrap()
        .join()
        .unwrap();
    let (refused, noop) =
        (REFUSED_RENAMES.load(Ordering::Relaxed), NOOP_RENAMES.load(Ordering::Relaxed));
    eprintln!(
        "renames: {refused} refused with a text that names the rename, {} other refusals of an \
         update holding a rename, {noop} of a missing source",
        OTHER_REFUSALS.load(Ordering::Relaxed)
    );
    // the floor was `> 0` (r1 and r2); the hold prints the count, to be pinned from the run
    assert!(refused > 0, "no generated step hit a refused rename");
    assert!(noop > 0, "no generated step renamed a missing source");
}
