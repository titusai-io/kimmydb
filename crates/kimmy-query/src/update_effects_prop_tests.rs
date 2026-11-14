//! Random operator updates against `merge_state::Writer`: the effects `apply_with_effects`
//! plans, written to a writer that holds the same document, render the post-image the engine
//! produced, step after step (the document of each step is what the writer rendered), and the
//! only refusals are the writer's documented ones (`field-merge-refusals-q7`).

use bson::{Bson, Document, doc};
use kimmy_core::effects::{Outcome, UpdateRefused};
use proptest::prelude::*;
use proptest::test_runner::{TestCaseError, TestRunner};

use crate::update_effects::{Plan, apply_with_effects};
use crate::update_effects_tests::{NOW, bytes, render, writer_with};

fn small_int() -> impl Strategy<Value = i32> {
    prop_oneof![Just(0), Just(1), Just(-1), Just(7), -50..50, Just(i32::MAX), Just(i32::MIN)]
}

fn an_int() -> impl Strategy<Value = Bson> {
    prop_oneof![small_int().prop_map(Bson::Int32), (-1_000_i64..1_000).prop_map(Bson::Int64)]
}

fn a_double() -> impl Strategy<Value = f64> {
    prop_oneof![Just(0.0), Just(-0.0), Just(1.5), Just(-2.25), Just(1e300), Just(3.0)]
}

pub(crate) fn arb_doc() -> impl Strategy<Value = Document> {
    (
        an_int(),
        a_double(),
        "[a-c]{0,3}",
        prop::collection::vec(small_int(), 0..4),
        prop::option::of((small_int(), prop::option::of(small_int()))),
        prop::option::of(an_int()),
    )
        .prop_map(|(n, d, s, arr, sub, x)| {
            let mut doc = doc! { "_id": 1, "n": n, "d": d, "s": s, "arr": arr };
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

fn arb_update() -> impl Strategy<Value = Document> {
    (slot_n(), slot_d(), slot_s(), slot_arr(), slot_k(), slot_j(), slot_x(), slot_date()).prop_map(
        |(a, b, c, d, e, f, g, h)| {
            let mut out = Document::new();
            for (op, path, arg) in [a, b, c, d, e, f, g, h].into_iter().flatten() {
                if !out.contains_key(op) {
                    out.insert(op, Document::new());
                }
                out.get_document_mut(op).unwrap().insert(path, arg);
            }
            out
        },
    )
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

/// One run: the steps applied in turn to an engine document and to a writer holding the same one.
fn run(start: Document, steps: Vec<Document>) -> Result<(), TestCaseError> {
    let mut doc = start;
    let mut w = writer_with(&doc);
    for step in steps {
        if step.is_empty() {
            continue;
        }
        let Ok(update) = crate::update::parse(&step) else { continue };
        let Ok(Plan::Effects { effects, post_image }) =
            apply_with_effects(&update, None, &doc, NOW)
        else {
            continue;
        };
        let outcome = effects.apply(&mut w, 0);
        match &outcome {
            Outcome::Refused(r) => {
                prop_assert!(documented(r), "{step}: refused {r:?} over {doc}");
                // a refusal changes nothing
                prop_assert_eq!(render(&w), doc.clone());
            }
            _ => {
                let rendered = render(&w);
                prop_assert_eq!(&rendered, &post_image, "{} over {}", step, doc);
                prop_assert_eq!(
                    outcome.modified(),
                    bytes(&post_image) != bytes(&doc),
                    "modified: {} over {}: {:?}",
                    step,
                    doc,
                    effects
                );
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
    std::thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(|| {
            let mut runner = TestRunner::new(ProptestConfig::with_cases(400));
            let strategy = (arb_doc().boxed(), prop::collection::vec(arb_update().boxed(), 1..4));
            runner
                .run(&strategy, |(start, steps)| run(start, steps))
                .unwrap_or_else(|e| panic!("{e}"));
        })
        .unwrap()
        .join()
        .unwrap();
}
