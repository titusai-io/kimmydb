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

pub(crate) fn plan(u: Document, d: &Document) -> Effects {
    plan_of(&parse(u), None, d).0
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

// ---------------------------------------------------------------------------------------------
// P2: the operator-by-effect table
// ---------------------------------------------------------------------------------------------

fn dec(s: &str) -> Bson {
    Bson::Decimal128(s.parse().unwrap())
}

fn base() -> Document {
    doc! {
        "_id": 1, "n": 5, "l": 7i64, "f": 1.5, "dec": dec("2.5"), "s": "x", "nul": Bson::Null,
        "arr": [1, 2, 3], "sub": { "k": 1, "j": 2 }, "tags": ["a", "b"],
    }
}

type Row = (&'static str, Document, Effects);

/// One row per operator and state: the update, and the effects it must produce over `base()`.
fn table() -> Vec<Row> {
    let d = |t: &'static str, u: Document, e: Effects| (t, u, e);
    vec![
        // $set
        d(
            "set absent",
            doc! {"$set": {"new": 1}},
            fx(|e| {
                e.set(p("new"), v(1));
            }),
        ),
        d(
            "set present",
            doc! {"$set": {"n": 6}},
            fx(|e| {
                e.set(p("n"), v(6));
            }),
        ),
        d(
            "set type change",
            doc! {"$set": {"n": 5i64}},
            fx(|e| {
                e.set(p("n"), v(5i64));
            }),
        ),
        d(
            "set null over value",
            doc! {"$set": {"s": Bson::Null}},
            fx(|e| {
                e.set(p("s"), Value::Null);
            }),
        ),
        d(
            "set nested existing",
            doc! {"$set": {"sub.k": 9}},
            fx(|e| {
                e.set(np(&["sub", "k"]), v(9));
            }),
        ),
        d(
            "set nested new",
            doc! {"$set": {"deep.x.y": 1}},
            fx(|e| {
                e.set(np(&["deep", "x", "y"]), v(1));
            }),
        ),
        d(
            "set document",
            doc! {"$set": {"sub": {"k": 1}}},
            fx(|e| {
                e.set(p("sub"), v(doc! {"k": 1}));
            }),
        ),
        d(
            "set array",
            doc! {"$set": {"arr": [9]}},
            fx(|e| {
                e.set(p("arr"), v(vec![9]));
            }),
        ),
        // $unset
        d(
            "unset present",
            doc! {"$unset": {"s": ""}},
            fx(|e| {
                e.unset(p("s"));
            }),
        ),
        d(
            "unset nested",
            doc! {"$unset": {"sub.k": ""}},
            fx(|e| {
                e.unset(np(&["sub", "k"]));
            }),
        ),
        d("unset absent", doc! {"$unset": {"nope": ""}}, fx(|_| {})),
        d("unset below absent", doc! {"$unset": {"nope.x": ""}}, fx(|_| {})),
        // $inc: the operand as written, typed
        d(
            "inc int32 on int32",
            doc! {"$inc": {"n": 2}},
            fx(|e| {
                e.inc(p("n"), v(2));
            }),
        ),
        d(
            "inc int64 on int32",
            doc! {"$inc": {"n": 2i64}},
            fx(|e| {
                e.inc(p("n"), v(2i64));
            }),
        ),
        d(
            "inc int32 on int64",
            doc! {"$inc": {"l": 1}},
            fx(|e| {
                e.inc(p("l"), v(1));
            }),
        ),
        d(
            "inc negative",
            doc! {"$inc": {"n": -3}},
            fx(|e| {
                e.inc(p("n"), v(-3));
            }),
        ),
        d(
            "inc double on double",
            doc! {"$inc": {"f": 0.5}},
            fx(|e| {
                e.inc(p("f"), v(0.5));
            }),
        ),
        d(
            "inc int on double",
            doc! {"$inc": {"f": 1}},
            fx(|e| {
                e.inc(p("f"), v(1));
            }),
        ),
        d(
            "inc decimal on decimal",
            doc! {"$inc": {"dec": dec("1.5")}},
            fx(|e| {
                e.inc(p("dec"), v(dec("1.5")));
            }),
        ),
        d(
            "inc absent int32",
            doc! {"$inc": {"c": 1}},
            fx(|e| {
                e.inc(p("c"), v(1));
            }),
        ),
        d(
            "inc absent int64",
            doc! {"$inc": {"c": 1i64}},
            fx(|e| {
                e.inc(p("c"), v(1i64));
            }),
        ),
        d(
            "inc absent double",
            doc! {"$inc": {"c": 1.5}},
            fx(|e| {
                e.inc(p("c"), v(1.5));
            }),
        ),
        d(
            "inc absent decimal",
            doc! {"$inc": {"c": dec("3.25")}},
            fx(|e| {
                e.inc(p("c"), v(dec("3.25")));
            }),
        ),
        d(
            "inc nested absent",
            doc! {"$inc": {"q.r.c": 4}},
            fx(|e| {
                e.inc(np(&["q", "r", "c"]), v(4));
            }),
        ),
        d(
            "inc int32 overflow widens",
            doc! {"$inc": {"n": i32::MAX}},
            fx(|e| {
                e.inc(p("n"), v(i32::MAX));
            }),
        ),
        d("inc zero on a number is nothing", doc! {"$inc": {"n": 0, "l": 0i64}}, fx(|_| {})),
        d(
            "inc zero absent creates a typed zero",
            doc! {"$inc": {"c": 0}},
            fx(|e| {
                e.inc(p("c"), v(0));
            }),
        ),
        d(
            "inc zero int64 absent",
            doc! {"$inc": {"c": 0i64}},
            fx(|e| {
                e.inc(p("c"), v(0i64));
            }),
        ),
        // $mul, $bit: register writes of the computed value
        d(
            "mul",
            doc! {"$mul": {"n": 3}},
            fx(|e| {
                e.set(p("n"), v(15));
            }),
        ),
        d(
            "mul double",
            doc! {"$mul": {"f": 2}},
            fx(|e| {
                e.set(p("f"), v(3.0));
            }),
        ),
        d(
            "mul absent is a typed zero",
            doc! {"$mul": {"m": 3}},
            fx(|e| {
                e.set(p("m"), v(0));
            }),
        ),
        d(
            "mul absent int64",
            doc! {"$mul": {"m": 3i64}},
            fx(|e| {
                e.set(p("m"), v(0i64));
            }),
        ),
        d(
            "bit and",
            doc! {"$bit": {"n": {"and": 4}}},
            fx(|e| {
                e.set(p("n"), v(4));
            }),
        ),
        d(
            "bit or absent",
            doc! {"$bit": {"m": {"or": 6}}},
            fx(|e| {
                e.set(p("m"), v(6));
            }),
        ),
        d(
            "bit xor int64",
            doc! {"$bit": {"l": {"xor": 2}}},
            fx(|e| {
                e.set(p("l"), v(5i64));
            }),
        ),
        // $min, $max: only when the reference's comparison says it changes
        d(
            "min lower",
            doc! {"$min": {"n": 3}},
            fx(|e| {
                e.set(p("n"), v(3));
            }),
        ),
        d("min not lower", doc! {"$min": {"n": 9}}, fx(|_| {})),
        d("min equal other type", doc! {"$min": {"n": 5.0}}, fx(|_| {})),
        d(
            "min absent",
            doc! {"$min": {"m": 2}},
            fx(|e| {
                e.set(p("m"), v(2));
            }),
        ),
        d(
            "max higher",
            doc! {"$max": {"n": 9}},
            fx(|e| {
                e.set(p("n"), v(9));
            }),
        ),
        d("max not higher", doc! {"$max": {"n": 3}}, fx(|_| {})),
        d("max equal other type", doc! {"$max": {"n": 5.0}}, fx(|_| {})),
        d("max across types", doc! {"$max": {"s": 1}}, fx(|_| {})),
        d(
            "max absent",
            doc! {"$max": {"m": 2}},
            fx(|e| {
                e.set(p("m"), v(2));
            }),
        ),
        // $currentDate: one now
        d(
            "currentDate once for two fields",
            doc! {"$currentDate": {"d1": true, "d2": {"$type": "date"}}},
            fx(|e| {
                e.set(p("d1"), Value::Date(NOW));
                e.set(p("d2"), Value::Date(NOW));
            }),
        ),
        // the array operators: the whole array
        d(
            "push",
            doc! {"$push": {"arr": 4}},
            fx(|e| {
                e.set(p("arr"), v(vec![1, 2, 3, 4]));
            }),
        ),
        d(
            "push absent",
            doc! {"$push": {"na": 4}},
            fx(|e| {
                e.set(p("na"), v(vec![4]));
            }),
        ),
        d(
            "push each position",
            doc! {"$push": {"arr": {"$each": [8, 9], "$position": 1}}},
            fx(|e| {
                e.set(p("arr"), v(vec![1, 8, 9, 2, 3]));
            }),
        ),
        d(
            "push nested array",
            doc! {"$push": {"sub.list": 1}},
            fx(|e| {
                e.set(np(&["sub", "list"]), v(vec![1]));
            }),
        ),
        d(
            "addToSet new",
            doc! {"$addToSet": {"tags": "c"}},
            fx(|e| {
                e.set(p("tags"), v(vec!["a", "b", "c"]));
            }),
        ),
        d("addToSet present", doc! {"$addToSet": {"tags": "a"}}, fx(|_| {})),
        d("addToSet equal number", doc! {"$addToSet": {"arr": 2.0}}, fx(|_| {})),
        d(
            "pull",
            doc! {"$pull": {"arr": 2}},
            fx(|e| {
                e.set(p("arr"), v(vec![1, 3]));
            }),
        ),
        d(
            "pull condition",
            doc! {"$pull": {"arr": {"$gte": 2}}},
            fx(|e| {
                e.set(p("arr"), v(vec![1]));
            }),
        ),
        d("pull absent value", doc! {"$pull": {"arr": 9}}, fx(|_| {})),
        d(
            "pullAll",
            doc! {"$pullAll": {"arr": [1, 3]}},
            fx(|e| {
                e.set(p("arr"), v(vec![2]));
            }),
        ),
        d(
            "pop last",
            doc! {"$pop": {"arr": 1}},
            fx(|e| {
                e.set(p("arr"), v(vec![1, 2]));
            }),
        ),
        d(
            "pop first",
            doc! {"$pop": {"arr": -1}},
            fx(|e| {
                e.set(p("arr"), v(vec![2, 3]));
            }),
        ),
        // $rename
        d(
            "rename",
            doc! {"$rename": {"s": "t"}},
            fx(|e| {
                e.rename(p("s"), p("t"), v("x"));
            }),
        ),
        d(
            "rename onto existing",
            doc! {"$rename": {"s": "tags"}},
            fx(|e| {
                e.rename(p("s"), p("tags"), v("x"));
            }),
        ),
        d("rename a missing sub-document", doc! {"$rename": {"sub2": "t"}}, fx(|_| {})),
        d(
            "rename an array",
            doc! {"$rename": {"tags": "labels"}},
            fx(|e| {
                e.rename(p("tags"), p("labels"), v(vec!["a", "b"]));
            }),
        ),
        d(
            "rename nested",
            doc! {"$rename": {"s": "deep.t"}},
            fx(|e| {
                e.rename(p("s"), np(&["deep", "t"]), v("x"));
            }),
        ),
        d("rename missing source", doc! {"$rename": {"nope": "t"}}, fx(|_| {})),
        // $setOnInsert is nothing on an existing document
        d("setOnInsert", doc! {"$setOnInsert": {"z": 1}}, fx(|_| {})),
        // several operators, one list in post-image order
        d(
            "several operators",
            doc! {"$set": {"z": 1}, "$inc": {"n": 1}, "$unset": {"s": ""}, "$push": {"arr": 4}},
            fx(|e| {
                e.unset(p("s"));
                e.inc(p("n"), v(1));
                e.set(p("arr"), v(vec![1, 2, 3, 4]));
                e.set(p("z"), v(1));
            }),
        ),
    ]
}

#[test]
fn the_operator_by_effect_table() {
    for (label, u, want) in table() {
        let d = base();
        let got = plan(u.clone(), &d);
        same_effects(&got, &want, label);
    }
}

#[test]
fn every_row_of_the_table_writes_what_the_engine_produced() {
    // the rows with a double operand over an integer counter are refused by the writer, not here
    for (label, u, _) in table() {
        let d = base();
        let (effects, outcome) = parity(&parse(u), None, &d);
        if effects.is_empty() {
            assert_eq!(outcome, Outcome::Unchanged, "{label}");
        }
    }
}

#[test]
fn a_double_on_an_integer_counter_is_the_writers_refusal() {
    // the engine makes the field a double (the reference's answer); the writer refuses it
    // (`field-merge-refusals-q7` in docs/deviations.md)
    let d = base();
    let (effects, post) = plan_of(&parse(doc! {"$inc": {"n": 0.5}}), None, &d);
    assert_eq!(post.get("n"), Some(&Bson::Double(5.5)));
    same_effects(
        &effects,
        &fx(|e| {
            e.inc(p("n"), v(0.5));
        }),
        "inc double",
    );
    let mut w = writer_with(&d);
    assert!(matches!(
        effects.apply(&mut w, 0),
        Outcome::Refused(kimmy_core::effects::UpdateRefused::IncDoubleOnCounter(_))
    ));
}

#[test]
fn renaming_a_counter_is_the_writers_refusal_and_a_string_rename_is_not() {
    let d = base();
    for (u, counter) in [
        (doc! {"$rename": {"n": "m"}}, true),
        (doc! {"$rename": {"sub": "sub2"}}, true),
        (doc! {"$rename": {"f": "g"}}, false),
        (doc! {"$rename": {"s": "t"}}, false),
        (doc! {"$rename": {"arr": "arr2"}}, false),
    ] {
        let (effects, _) = plan_of(&parse(u.clone()), None, &d);
        let mut w = writer_with(&d);
        let outcome = effects.apply(&mut w, 0);
        assert_eq!(
            matches!(
                outcome,
                Outcome::Refused(kimmy_core::effects::UpdateRefused::RenameOfCounter(_))
            ),
            counter,
            "{u}: {outcome:?}"
        );
    }
}

#[test]
fn inc_zero_on_an_absent_path_is_a_counter_cell_of_the_operand_type() {
    let d = doc! { "_id": 1 };
    for (operand, want) in [(Bson::Int32(0), Bson::Int32(0)), (Bson::Int64(0), Bson::Int64(0))] {
        let (effects, outcome) = parity_doc(doc! {"$inc": {"c": operand}}, &d);
        assert!(outcome.modified());
        assert_eq!(effects.len(), 1);
        let mut w = writer_with(&d);
        effects.apply(&mut w, 0);
        assert_eq!(render(&w).get("c"), Some(&want));
        assert!(w.sees_counter(&p("c")));
        // and a second `$inc 0` on the counter it made is no effect
        let d2 = render(&w);
        let (again, outcome) = parity_doc(doc! {"$inc": {"c": 0}}, &d2);
        assert!(again.is_empty(), "{again:?}");
        assert_eq!(outcome, Outcome::Unchanged);
    }
}

#[test]
fn an_int32_overflow_and_decimal_totals_land_as_the_engine_computes_them() {
    let d = doc! { "_id": 1, "n": i32::MAX, "dec": dec("2.5") };
    parity_doc(doc! {"$inc": {"n": 1}}, &d);
}

#[test]
fn decimal_operands_over_decimal_and_absent_fields() {
    let d = doc! { "_id": 1, "dec": dec("2.5") };
    parity_doc(doc! {"$inc": {"dec": dec("1.5")}}, &d);
    parity_doc(doc! {"$inc": {"fresh": dec("3.25")}}, &d);
}

