//! The effects of an update (`update_effects.rs`): the harness, and the checks of the core.
//!
//! The harness plans an update, applies the effects to a `merge_state::Writer` holding the same
//! document, and compares what the writer renders with the post-image the engine produced, byte
//! for byte, so field order is part of every comparison.

use bson::{Bson, Document, doc};
use kimmy_core::effects::{Effects, Outcome};
use kimmy_core::ids::NodeId;
use kimmy_core::merge_state::bridge::{document_to_fields, ordered_to_document};
use kimmy_core::merge_state::{Epoch, JoinOptions, Path, Value, Writer, WriterId};

use crate::filter::Filter;
use crate::update::{self, Update};
use crate::update_effects::{Plan, apply_with_effects};

pub(crate) const NOW: i64 = 1_700_000_000_000;

pub(crate) fn p(name: &str) -> Path {
    Path::flat(name).unwrap()
}

pub(crate) fn np(segs: &[&str]) -> Path {
    Path::new(segs.iter().copied()).unwrap()
}

pub(crate) fn v(b: impl Into<Bson>) -> Value {
    kimmy_core::merge_state::bridge::bson_to_value(&b.into()).unwrap()
}

pub(crate) fn wid(b: u8) -> WriterId {
    WriterId { node: NodeId::from_bytes([b; 16]), epoch: Epoch::from_u64(0) }
}

/// A writer holding `doc` as one insert.
pub(crate) fn writer_with(doc: &Document) -> Writer {
    let mut w = Writer::new(wid(1), JoinOptions::default());
    w.insert(&document_to_fields(doc).unwrap(), 0).expect("an insert writes");
    w
}

/// What the writer renders, fields in the order of the read.
pub(crate) fn render(w: &Writer) -> Document {
    ordered_to_document(&w.state.ordered_document(&JoinOptions::default()).expect("alive"))
}

/// A document as bytes: field order and every bit of every value count.
pub(crate) fn bytes(d: &Document) -> Vec<u8> {
    let mut out = Vec::new();
    d.to_writer(&mut out).unwrap();
    out
}

pub(crate) fn parse(u: Document) -> Update {
    update::parse(&u).unwrap_or_else(|e| panic!("{u}: {e}"))
}

/// The effects and the post-image of an operator update.
pub(crate) fn plan_of(u: &Update, f: Option<&Filter>, d: &Document) -> (Effects, Document) {
    match apply_with_effects(u, f, d, NOW).unwrap_or_else(|e| panic!("refused: {e}")) {
        Plan::Effects { effects, post_image } => (effects, post_image),
        Plan::Replace { .. } => panic!("a replacement has no effects"),
    }
}

/// The expected effects, built with the same builder.
pub(crate) fn fx(build: impl FnOnce(&mut Effects)) -> Effects {
    let mut e = Effects::new();
    build(&mut e);
    e
}

/// `Effects` has no equality: compare what it debug-prints (the ordered ops and the renames).
pub(crate) fn same_effects(got: &Effects, want: &Effects, what: &str) {
    assert_eq!(format!("{got:?}"), format!("{want:?}"), "{what}");
}

/// Plan `u` over `d`, apply the effects to a writer holding `d`, and require the writer to render
/// the engine's post-image, byte for byte. Returns the effects and the outcome.
pub(crate) fn parity(u: &Update, f: Option<&Filter>, d: &Document) -> (Effects, Outcome) {
    let (effects, post) = plan_of(u, f, d);
    let mut w = writer_with(d);
    let outcome = effects.apply(&mut w, 0);
    assert!(outcome.refusal().is_none(), "refused: {outcome:?} for {effects:?}");
    assert_eq!(render(&w), post, "the writer renders another document");
    assert_eq!(bytes(&render(&w)), bytes(&post), "same fields, other order or bits");
    assert_eq!(outcome.modified(), post != *d || bytes(&post) != bytes(d), "modified: {effects:?}");
    (effects, outcome)
}

pub(crate) fn parity_doc(u: Document, d: &Document) -> (Effects, Outcome) {
    parity(&parse(u), None, d)
}

#[test]
fn an_update_that_changes_nothing_has_no_effects() {
    let d = doc! { "_id": 1, "n": 5, "s": "x", "arr": [1, 2], "empty": [], "sub": { "k": 1 } };
    for u in [
        doc! { "$set": { "n": 5, "sub.k": 1, "s": "x" } },
        doc! { "$max": { "n": 3 } },
        doc! { "$max": { "n": 5.0 } },
        doc! { "$min": { "n": 9 } },
        doc! { "$addToSet": { "arr": 2 } },
        doc! { "$pull": { "arr": 99 } },
        doc! { "$pullAll": { "arr": [98, 99] } },
        doc! { "$pull": { "missing": 1 } },
        doc! { "$pop": { "empty": 1 } },
        doc! { "$unset": { "missing": "", "sub.nope": "" } },
        doc! { "$rename": { "missing": "other" } },
        doc! { "$inc": { "n": 0 } },
        doc! { "$setOnInsert": { "z": 1 } },
        doc! { "$bit": { "n": { "or": 0 } } },
        doc! { "$mul": { "n": 1 } },
    ] {
        let (effects, outcome) = parity_doc(u.clone(), &d);
        assert!(effects.is_empty(), "{u}: {effects:?}");
        assert_eq!(outcome, Outcome::Unchanged, "{u}");
    }
}

#[test]
fn an_inc_is_the_operand_not_the_sum() {
    let d = doc! { "_id": 1, "n": 5 };
    let (effects, _) = parity_doc(doc! { "$inc": { "n": 3 } }, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.inc(p("n"), Value::Int32(3));
        }),
        "inc",
    );
}

#[test]
fn arrays_are_atomic_and_one_array_is_one_set() {
    let d = doc! { "_id": 1, "a": [0, 0, 0], "x": 1 };
    let (effects, _) = parity_doc(doc! { "$set": { "a.0": 1, "a.1": 2 } }, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.set(p("a"), v(vec![1, 2, 0]));
        }),
        "one Set of a",
    );
    // no leaf below an array is ever an effect
    let (effects, _) = parity_doc(doc! { "$set": { "a.1": 9, "x": 2 } }, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.set(p("a"), v(vec![0, 9, 0]));
            e.set(p("x"), v(2));
        }),
        "array and leaf",
    );
}

#[test]
fn the_same_update_on_the_same_document_gives_the_same_list() {
    let d = doc! { "_id": 1, "a": [1, 2], "n": 1 };
    let u = parse(doc! { "$inc": { "n": 2 }, "$push": { "a": 3 }, "$set": { "z": 1, "b.c": 2 } });
    let first = plan_of(&u, None, &d).0;
    for _ in 0..5 {
        same_effects(&plan_of(&u, None, &d).0, &first, "determinism");
    }
}

#[test]
fn new_fields_take_the_order_of_the_engines_post_image() {
    let d = doc! { "_id": 1, "x": 1 };
    let (effects, _) = parity_doc(doc! { "$set": { "z": 1, "a": 2, "m.q": 3, "m.b": 4 } }, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.set(p("z"), v(1));
            e.set(p("a"), v(2));
            e.set(np(&["m", "q"]), v(3));
            e.set(np(&["m", "b"]), v(4));
        }),
        "post-image order",
    );
}

#[test]
fn a_replacement_is_not_an_effect_list() {
    let d = doc! { "_id": 1, "a": 1 };
    let u = parse(doc! { "b": 2 });
    assert!(matches!(
        apply_with_effects(&u, None, &d, NOW).unwrap(),
        Plan::Replace { post_image } if post_image == doc! { "_id": 1, "b": 2 }
    ));
}

#[test]
fn a_refusal_of_the_engine_is_the_refusal_of_the_plan() {
    let d = doc! { "_id": 1, "s": "text" };
    let u = parse(doc! { "$inc": { "s": 1 } });
    assert!(apply_with_effects(&u, None, &d, NOW).is_err());
}

