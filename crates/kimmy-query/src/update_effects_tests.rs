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

use crate::filter::{self, Filter};
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
        .expect("the writer renders documents")
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

pub(crate) fn filt(f: Document) -> Filter {
    filter::parse(&f).unwrap_or_else(|e| panic!("{f}: {e}"))
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

/// The document as bytes with its top-level fields sorted by name and `_id` left out: the
/// comparison for a PIPELINE step, where the writer keeps the order of the fields that stay
/// (`pipeline-reorder-only-q7`) and the stored `_id` (`pipeline-id-type-q7`). Every value, nested
/// order and bit pattern still counts.
pub(crate) fn canonical(d: &Document) -> Vec<u8> {
    let mut fields: Vec<(&String, &Bson)> = d.iter().filter(|(k, _)| *k != "_id").collect();
    fields.sort_by(|a, b| a.0.cmp(b.0));
    let sorted: Document = fields.into_iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    bytes(&sorted)
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

/// New fields are listed in the order the reference stores them: sorted by name, nested fields
/// too (q7s2.out o3 `$set z,a,m`, o4 `$set b.y,b.x,a`), whatever the order of the update.
#[test]
fn new_fields_are_sorted_like_the_reference() {
    let d = doc! { "_id": 1, "x": 1 };
    let (effects, _) = parity_doc(doc! { "$set": { "z": 1, "a": 2, "m.q": 3, "m.b": 4 } }, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.set(p("a"), v(2));
            e.set(np(&["m", "b"]), v(4));
            e.set(np(&["m", "q"]), v(3));
            e.set(p("z"), v(1));
        }),
        "sorted",
    );
}

/// q7s2.out o7: a `$set` over existing fields keeps their place, the new field goes after them.
#[test]
fn existing_fields_keep_their_place_and_new_ones_follow() {
    let d = doc! { "_id": 1, "a": 1, "b": 2 };
    let (effects, _) = parity_doc(doc! { "$set": { "c": 3, "b": 9, "a": 8 } }, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.set(p("a"), v(8));
            e.set(p("b"), v(9));
            e.set(p("c"), v(3));
        }),
        "o7",
    );
}

/// q7s2.out o8: an `$unset` is listed first, then the new field.
#[test]
fn an_unset_comes_before_a_new_field() {
    let d = doc! { "_id": 1, "a": 1, "b": 2 };
    let (effects, _) = parity_doc(doc! { "$unset": { "a": "" }, "$set": { "z": 1 } }, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.unset(p("a"));
            e.set(p("z"), v(1));
        }),
        "o8",
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
        // `$inc 0` of another width on a number that is there: nothing, the stored type is not
        // widened (q7s2.out z4 Int64 + Int32 0, z5 Double + Int32 0, z6 Decimal + Int32 0)
        d("inc int32 zero on an int64 is nothing (q7s2 z4)", doc! {"$inc": {"l": 0}}, fx(|_| {})),
        d("inc int32 zero on a double is nothing (q7s2 z5)", doc! {"$inc": {"f": 0}}, fx(|_| {})),
        d(
            "inc int32 zero on a decimal is nothing (q7s2 z6)",
            doc! {"$inc": {"dec": 0}},
            fx(|_| {}),
        ),
        // a wider operand does change the type, so it is an effect (q7s2.out z2 Int32 + Int64 0)
        d(
            "inc int64 zero on an int32 widens (q7s2 z2)",
            doc! {"$inc": {"n": 0i64}},
            fx(|e| {
                e.inc(p("n"), v(0i64));
            }),
        ),
        // the typed zero of the operand on an absent path (q7s2.out z9, z10)
        d(
            "inc double zero absent creates a double zero (q7s2 z9)",
            doc! {"$inc": {"c": 0.0}},
            fx(|e| {
                e.inc(p("c"), v(0.0));
            }),
        ),
        d(
            "inc decimal zero absent creates a decimal zero (q7s2 z10)",
            doc! {"$inc": {"c": dec("0")}},
            fx(|e| {
                e.inc(p("c"), v(dec("0")));
            }),
        ),
        // a zero `$inc` beside a `$set` that changes: only the `$set` is an effect (q7s2.out z11)
        d(
            "inc zero beside a set (q7s2 z11)",
            doc! {"$inc": {"n": 0}, "$set": {"m": 1}},
            fx(|e| {
                e.set(p("m"), v(1));
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
        // several operators, one list in post-image order: the new fields sorted by name
        // (q7s2.out o1, o9)
        d(
            "several operators",
            doc! {"$set": {"z": 1, "c": 2}, "$inc": {"n": 1}, "$unset": {"s": ""}, "$push": {"arr": 4}},
            fx(|e| {
                e.unset(p("s"));
                e.inc(p("n"), v(1));
                e.set(p("arr"), v(vec![1, 2, 3, 4]));
                e.set(p("c"), v(2));
                e.set(p("z"), v(1));
            }),
        ),
        // `$set` b,a then `$inc` c, and the operators the other way round: the same list, sorted
        // (q7s2.out o1 and o2)
        d(
            "new fields sorted, $set before $inc",
            doc! {"$set": {"nb": 1, "na": 2}, "$inc": {"nc": 1}},
            fx(|e| {
                e.set(p("na"), v(2));
                e.set(p("nb"), v(1));
                e.inc(p("nc"), v(1));
            }),
        ),
        d(
            "new fields sorted, $inc before $set",
            doc! {"$inc": {"nc": 1}, "$set": {"nb": 1, "na": 2}},
            fx(|e| {
                e.set(p("na"), v(2));
                e.set(p("nb"), v(1));
                e.inc(p("nc"), v(1));
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

/// Amendment 8 (the design page, in force with rename-narrow): a rename is refused only for a
/// number that has been incremented, at the source or below it. The fixture increments `n` and
/// `sub.k` first, through the writer, so that each holds a slot.
#[test]
fn renaming_a_counter_is_the_writers_refusal_and_a_string_rename_is_not() {
    let d = base();
    for (u, incremented) in [
        (doc! {"$rename": {"n": "m"}}, Some(p("n"))),
        (doc! {"$rename": {"sub": "sub2"}}, Some(np(&["sub", "k"]))),
        (doc! {"$rename": {"f": "g"}}, None),
        (doc! {"$rename": {"s": "t"}}, None),
        (doc! {"$rename": {"arr": "arr2"}}, None),
    ] {
        let (effects, _) = plan_of(&parse(u.clone()), None, &d);
        let mut w = writer_with(&d);
        if let Some(at) = &incremented {
            w.write_inc(at, 1, 0).expect("the increment writes");
        }
        let outcome = effects.apply(&mut w, 0);
        assert_eq!(
            matches!(
                outcome,
                Outcome::Refused(kimmy_core::effects::UpdateRefused::RenameOfCounter(_))
            ),
            incremented.is_some(),
            "{u}: {outcome:?}"
        );
        // the narrowed side: over the plain inserted numbers the same rename is accepted
        if incremented.is_some() {
            let mut plain = writer_with(&d);
            let outcome = effects.apply(&mut plain, 0);
            assert!(outcome.refusal().is_none(), "{u}: {outcome:?}");
        }
    }
    // and its effects are the Unset of the source with the Set of the destination
    let (effects, _) = plan_of(&parse(doc! {"$rename": {"n": "m"}}), None, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.rename(p("n"), p("m"), v(5));
        }),
        "n to m",
    );
    let (effects, _) = plan_of(&parse(doc! {"$rename": {"sub": "sub2"}}), None, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.rename(p("sub"), p("sub2"), v(d.get("sub").unwrap().clone()));
        }),
        "sub to sub2",
    );
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

/// q7s2.out z9, z10: `$inc` of a double or a decimal zero on an absent path creates a zero of the
/// operand's type, and a zero `$inc` of an `Int32` on that value is nothing (z5, z6).
#[test]
fn inc_zero_on_an_absent_path_is_a_typed_zero_of_a_double_and_a_decimal() {
    let d = doc! { "_id": 1 };
    for (operand, want) in [(Bson::Double(0.0), Bson::Double(0.0)), (dec("0"), dec("0"))] {
        let (effects, outcome) = parity_doc(doc! {"$inc": {"c": operand.clone()}}, &d);
        assert!(outcome.modified());
        same_effects(
            &effects,
            &fx(|e| {
                e.inc(p("c"), v(operand.clone()));
            }),
            "z9 and z10",
        );
        let mut w = writer_with(&d);
        effects.apply(&mut w, 0);
        assert_eq!(render(&w).get("c"), Some(&want));
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

// ---------------------------------------------------------------------------------------------
// P3: arrays, positional forms and arrayFilters
// ---------------------------------------------------------------------------------------------

fn lines() -> Document {
    doc! {
        "_id": 1, "x": 1,
        "items": [ {"sku": "a", "qty": 1}, {"sku": "b", "qty": 2}, {"sku": "c", "qty": 3} ],
        "grid": [[1, 2], [3, 4]],
        "a": [10, 20, 30],
        "sub": { "arr": [1, 2], "k": 1 },
        "dz": { "0": 5 },
    }
}

fn plan_filtered(
    u: Document,
    filters: Vec<Document>,
    f: Document,
    d: &Document,
) -> (Effects, Document) {
    let parsed = update::parse_with_filters(&u, &filters).unwrap_or_else(|e| panic!("{u}: {e}"));
    let filter = filt(f);
    let r = plan_of(&parsed, Some(&filter), d);
    // the writer renders what the engine produced
    let mut w = writer_with(d);
    let outcome = r.0.apply(&mut w, 0);
    assert!(outcome.refusal().is_none(), "{u}: {outcome:?}");
    assert_eq!(bytes(&render(&w)), bytes(&r.1), "{u}");
    r
}

#[test]
fn the_matched_position_is_one_set_of_the_array() {
    let d = lines();
    let (effects, post) =
        plan_filtered(doc! {"$set": {"items.$.qty": 9}}, vec![], doc! {"items.sku": "b"}, &d);
    let want = Bson::Array(vec![
        Bson::Document(doc! {"sku": "a", "qty": 1}),
        Bson::Document(doc! {"sku": "b", "qty": 9}),
        Bson::Document(doc! {"sku": "c", "qty": 3}),
    ]);
    assert_eq!(post.get("items"), Some(&want));
    same_effects(
        &effects,
        &fx(|e| {
            e.set(p("items"), v(want.clone()));
        }),
        "$",
    );
}

#[test]
fn all_positional_and_inc_on_an_element_is_a_register_write() {
    let d = lines();
    let (effects, post) = plan_filtered(doc! {"$inc": {"items.$[].qty": 10}}, vec![], doc! {}, &d);
    let items = post.get_array("items").unwrap().clone();
    assert_eq!(items[0].as_document().unwrap().get("qty"), Some(&Bson::Int32(11)));
    same_effects(
        &effects,
        &fx(|e| {
            e.set(p("items"), v(Bson::Array(items)));
        }),
        "$[] inc",
    );
}

#[test]
fn a_filtered_positional_changes_only_the_selected_elements_in_one_set() {
    let d = lines();
    let (effects, post) = plan_filtered(
        doc! {"$set": {"items.$[big].flag": true}},
        vec![doc! {"big.qty": {"$gte": 2}}],
        doc! {},
        &d,
    );
    let items = post.get_array("items").unwrap();
    assert!(items[0].as_document().unwrap().get("flag").is_none());
    assert_eq!(items[2].as_document().unwrap().get("flag"), Some(&Bson::Boolean(true)));
    same_effects(
        &effects,
        &fx(|e| {
            e.set(p("items"), v(Bson::Array(items.clone())));
        }),
        "$[id]",
    );
}

#[test]
fn a_positional_that_selects_nothing_has_no_effects() {
    let d = lines();
    let (effects, _) = plan_filtered(
        doc! {"$set": {"items.$[big].flag": true}},
        vec![doc! {"big.qty": {"$gte": 99}}],
        doc! {},
        &d,
    );
    assert!(effects.is_empty());
}

#[test]
fn indexes_and_nested_arrays_are_one_set_of_the_outermost_array() {
    let d = lines();
    for (u, array) in [
        (doc! {"$set": {"a.1": 99}}, "a"),
        (doc! {"$set": {"grid.1.0": 99}}, "grid"),
        // below a nested array (q7s2.out a1)
        (doc! {"$set": {"grid.0.1": 99}}, "grid"),
        (doc! {"$set": {"items.0.sku": "z"}}, "items"),
        (doc! {"$set": {"items.0.deep.x": 1}}, "items"),
        (doc! {"$set": {"sub.arr.0": 5}}, "sub.arr"),
        (doc! {"$inc": {"a.0": 5}}, "a"),
        (doc! {"$mul": {"a.2": 2}}, "a"),
        (doc! {"$set": {"a.5": 1}}, "a"),
        (doc! {"$push": {"grid.0": 9}}, "grid"),
    ] {
        let (effects, _) = parity_doc(u.clone(), &d);
        let post = plan_of(&parse(u.clone()), None, &d).1;
        let segs: Vec<&str> = array.split('.').collect();
        let whole = post
            .get_array(segs[0])
            .map(|a| Bson::Array(a.clone()))
            .or_else(|_| post.get_document(segs[0]).unwrap().get(segs[1]).cloned().ok_or(()));
        let want = fx(|e| {
            e.set(np(&segs), v(whole.unwrap()));
        });
        same_effects(&effects, &want, &u.to_string());
    }
}

/// A numeric segment below an array that is itself an element sets the element of the INNER
/// array, as the reference does (q7s2.out a1-a4 for two levels, a8-a10 for three, q7s2t t0003 for a
/// document element); the effect is still one `Set` of the outermost array.
#[test]
fn a_write_below_a_nested_array_sets_the_inner_element() {
    use bson::bson;
    let nested = |u: Document, d: &Document, array: &str, want: Bson, label: &str| {
        let (effects, _) = parity_doc(u, d);
        same_effects(
            &effects,
            &fx(|e| {
                e.set(p(array), v(want));
            }),
            label,
        );
    };
    let grid = lines();
    nested(doc! {"$set": {"grid.0.1": 9}}, &grid, "grid", bson!([[1, 9], [3, 4]]), "a1 grid.0.1");
    nested(doc! {"$set": {"grid.1.0": 9}}, &grid, "grid", bson!([[1, 2], [9, 4]]), "a2 grid.1.0");
    nested(
        doc! {"$set": {"grid.0.5": 9}},
        &grid,
        "grid",
        bson!([[1, 2, null, null, null, 9], [3, 4]]),
        "a3 grid.0.5 pads the inner array",
    );
    nested(
        doc! {"$set": {"grid.2.0": 9}},
        &grid,
        "grid",
        bson!([[1, 2], [3, 4], { "0": 9 }]),
        "a4 grid.2.0 pads the outer array with a document",
    );
    let deep = doc! { "_id": 1, "d": [[[1, 2], [3]], [[4]]] };
    nested(doc! {"$set": {"d.0.0.1": 9}}, &deep, "d", bson!([[[1, 9], [3]], [[4]]]), "a8 d.0.0.1");
    nested(doc! {"$set": {"d.1.0.0": 9}}, &deep, "d", bson!([[[1, 2], [3]], [[9]]]), "a9 d.1.0.0");
    nested(doc! {"$set": {"d.0.1.0": 9}}, &deep, "d", bson!([[[1, 2], [9]], [[4]]]), "a10 d.0.1.0");
    // a number below a document element is a field named by it, APPENDED after the existing
    // ones (q7s2t.out t0003 line 14 and t0122 line 728: `[{k: 1, "1": 9}]`). q7s2.out a17
    // (`{"1": 9, "k": 1}`) is a recording artifact of the first harness, which read `after`
    // through JS objects that move integer-like names to the front.
    let a17 = doc! { "_id": 1, "a": [{ "k": 1 }] };
    nested(
        doc! {"$set": {"a.0.1": 9}},
        &a17,
        "a",
        bson!([{ "k": 1, "1": 9 }]),
        "a17 a.0.1 over a document element (q7s2t t0003, t0122)",
    );
}

#[test]
fn unset_of_an_element_writes_null_into_the_array() {
    let d = lines();
    let (effects, _) = parity_doc(doc! {"$unset": {"a.1": ""}}, &d);
    let post = plan_of(&parse(doc! {"$unset": {"a.1": ""}}), None, &d).1;
    assert_eq!(post.get_array("a").unwrap()[1], Bson::Null);
    same_effects(
        &effects,
        &fx(|e| {
            e.set(p("a"), v(vec![Bson::Int32(10), Bson::Null, Bson::Int32(30)]));
        }),
        "unset element",
    );
}

#[test]
fn operators_on_one_array_combine_and_two_arrays_are_two_sets() {
    let d = lines();
    let (effects, _) = parity_doc(doc! {"$set": {"a.0": 1, "a.2": 3, "grid.0.0": 0}}, &d);
    assert_eq!(effects.len(), 2);
    let (effects, _) = parity_doc(doc! {"$set": {"a.0": 1}, "$inc": {"x": 1}}, &d);
    assert_eq!(effects.len(), 2);
}

#[test]
fn a_numeric_segment_is_an_index_only_when_the_stored_value_is_an_array() {
    let d = lines();
    // an absent parent: a field named 0, a leaf effect
    let (effects, _) = parity_doc(doc! {"$set": {"fresh.0": 1}}, &d);
    let post = plan_of(&parse(doc! {"$set": {"fresh.0": 1}}), None, &d).1;
    assert_eq!(post.get_document("fresh").unwrap().get("0"), Some(&Bson::Int32(1)));
    same_effects(
        &effects,
        &fx(|e| {
            e.set(np(&["fresh", "0"]), v(1));
        }),
        "absent parent",
    );
    // a document with a field named 0
    let (effects, _) = parity_doc(doc! {"$set": {"dz.0": 6}}, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.set(np(&["dz", "0"]), v(6));
        }),
        "document key 0",
    );
}

#[test]
fn a_noop_through_an_array_has_no_effects() {
    let d = lines();
    for u in [
        doc! {"$set": {"a.0": 10}},
        doc! {"$max": {"a.0": 1}},
        doc! {"$unset": {"a.9": ""}},
        doc! {"$set": {"grid.1": [3, 4]}},
    ] {
        let (effects, outcome) = parity_doc(u.clone(), &d);
        assert!(effects.is_empty(), "{u}: {effects:?}");
        assert_eq!(outcome, Outcome::Unchanged);
    }
}

/// The reference creates an absent field from an `$inc` as the operand EXACTLY, sign and exponent
/// kept (q7s5a.out I1-I7, with q7s2k.out k0010): the writer must render the engine's post-image
/// byte for byte, and the effect is the increment of the operand as written.
fn absent_inc_stores_the_operand(label: &str, operand: Bson) {
    let d = doc! { "_id": 1 };
    let (effects, outcome) = parity_doc(doc! {"$inc": {"x": operand.clone()}}, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.inc(p("x"), v(operand.clone()));
        }),
        label,
    );
    assert!(outcome.modified(), "{label}: {outcome:?}");
}

fn decimal(text: &str) -> Bson {
    Bson::Decimal128(text.parse().unwrap())
}

/// I1, q7s5a0004: `$inc x Double -0.0` over `{_id: 1}` stores `-0.0`.
#[test]
fn an_inc_of_a_negative_double_zero_on_an_absent_field_stores_it_as_it_is() {
    absent_inc_stores_the_operand("q7s5a0004", Bson::Double(-0.0));
}

/// I2, q7s5a0005: Decimal128 `-0`.
#[test]
fn an_inc_of_a_negative_decimal_zero_on_an_absent_field_keeps_its_sign() {
    absent_inc_stores_the_operand("q7s5a0005", decimal("-0"));
}

/// I3, q7s5a0006: Decimal128 `0E+3`.
#[test]
fn an_inc_of_a_decimal_zero_with_a_positive_exponent_on_an_absent_field_keeps_the_exponent() {
    absent_inc_stores_the_operand("q7s5a0006", decimal("0E+3"));
}

/// I4, q7s5a0007: Decimal128 `-0E-2` (the shell prints `-0.00`).
#[test]
fn an_inc_of_a_negative_decimal_zero_with_a_negative_exponent_on_an_absent_field_keeps_both() {
    absent_inc_stores_the_operand("q7s5a0007", decimal("-0E-2"));
}

/// I5, q7s5a0008: Decimal128 `1.50`.
#[test]
fn an_inc_of_a_decimal_with_a_trailing_zero_on_an_absent_field_keeps_it() {
    absent_inc_stores_the_operand("q7s5a0008", decimal("1.50"));
}

/// I6 and I7, q7s5a0009 and q7s5a0010: an Int32 zero and an Int64 zero keep their types.
#[test]
fn an_inc_of_a_zero_integer_on_an_absent_field_keeps_its_type() {
    absent_inc_stores_the_operand("q7s5a0009", Bson::Int32(0));
    absent_inc_stores_the_operand("q7s5a0010", Bson::Int64(0));
}

/// D1 and D2, q7s5b0001 and q7s5b0002: a Decimal128 with a positive exponent and a non-zero
/// coefficient on an absent field is stored as written, `5E+3` and `-5E+3` (not `5000`).
#[test]
fn an_inc_of_a_decimal_with_a_positive_exponent_on_an_absent_field_keeps_the_exponent() {
    absent_inc_stores_the_operand("q7s5b0001", decimal("5E+3"));
    absent_inc_stores_the_operand("q7s5b0002", decimal("-5E+3"));
}

/// The stored `x` after `$inc first` and then `$inc second` on `{_id: 1}`: the effects of each
/// step through the writer render the engine's post-image (`parity_doc`), and the second step
/// finds the first one's result as the document.
fn two_decimal_incs(label: &str, first: &str, second: &str, want: &str) {
    let d = doc! { "_id": 1 };
    let u1 = doc! {"$inc": {"x": decimal(first)}};
    let after1 = plan_of(&parse(u1.clone()), None, &d).1;
    parity_doc(u1, &d);
    let u2 = doc! {"$inc": {"x": decimal(second)}};
    let after2 = plan_of(&parse(u2.clone()), None, &after1).1;
    parity_doc(u2, &after1);
    assert_eq!(after2.get("x"), Some(&decimal(want)), "{label}: {after2}");
}

/// D3 and D4, q7s5b0003 and q7s5b0004: `5E+3` and `2E+3` added one after the other, in either
/// order, store `7E+3` (the exponent of both).
#[test]
fn two_decimal_incs_with_a_positive_exponent_add_up_in_either_order() {
    two_decimal_incs("q7s5b0003", "5E+3", "2E+3", "7E+3");
    two_decimal_incs("q7s5b0004", "2E+3", "5E+3", "7E+3");
}

/// D5, q7s5b0005: `$inc -0` creates `-0`, and `$inc 0E+3` after it is no write (nModified 0, the
/// stored `-0` stays): the engine finds the result equal to what is stored.
#[test]
fn an_inc_that_leaves_a_stored_decimal_zero_equal_is_no_write() {
    let d = doc! { "_id": 1, "x": decimal("-0") };
    let (effects, outcome) = parity_doc(doc! {"$inc": {"x": decimal("0E+3")}}, &d);
    assert!(effects.is_empty(), "{effects:?}");
    assert_eq!(outcome, Outcome::Unchanged);
}

/// D6, q7s5b0006: over a PRESENT Decimal128 `0`, `$inc 5E+3` stores `5000` (the sum with a base
/// takes the smaller exponent, as it does for any base): the base is the only part that is
/// added to.
#[test]
fn an_inc_over_a_present_decimal_zero_is_a_sum_with_a_base() {
    let d = doc! { "_id": 1, "x": decimal("0") };
    let (_, post) = plan_of(&parse(doc! {"$inc": {"x": decimal("5E+3")}}), None, &d);
    assert_eq!(post.get("x"), Some(&decimal("5000")));
    parity_doc(doc! {"$inc": {"x": decimal("5E+3")}}, &d);
}
