//! `$rename` on a `field` collection: the shapes the reference refuses are refused by the engine
//! (its source walk `rename_source` and the destination check, main #315 and #316), the ones it
//! allows are written as a plain move. The refusals follow `q7s2.out` lines 101 to 121 and 586,
//! `q7s2d.out` lines 76 to 116 and `q7s2f.out`. A rename through an array element or a scalar
//! must not reach the writer: it holds no counter check there (`RenameOfCounter` is decided on
//! the leaf the writer sees), so an integer renamed into an array would otherwise pass it.

use bson::{Document, doc};
use kimmy_core::effects::Outcome;

use crate::update_effects::{UpsertPlan, apply_with_effects, plan_upsert};
use crate::update_effects_tests::{
    NOW, filt, fx, p, parity_doc, parse, same_effects, v, writer_with,
};

fn items() -> Document {
    doc! { "_id": 1, "items": [{ "sku": "a", "qty": 1 }, { "sku": "b" }], "s": "x",
    "arr": [1, 2], "o": { "p": "v" } }
}
fn pairs() -> Document {
    doc! { "_id": 1, "items": [{ "s": 1 }, { "s": 2 }], "x": 1 }
}
fn nested() -> Document {
    doc! { "_id": 1, "items": [[{ "s": 1 }], [{ "s": 2 }]], "x": 1 }
}
fn scalar() -> Document {
    doc! { "_id": 1, "a": 5, "x": 1 }
}
fn counter() -> Document {
    doc! { "_id": 1, "n": 5, "arr": [1, 2] }
}

const SOURCE: &str = "the source field cannot be an array element";
const DEST: &str = "the destination field cannot be an array element";
const TRAVERSE: &str = "cannot use the part";
const BELOW: &str = "cannot create field";

fn refusal(d: &Document, u: Document) -> String {
    let update = parse(u.clone());
    match apply_with_effects(&update, None, d, NOW) {
        Err(e) => e.to_string(),
        Ok(plan) => panic!("{u} over {d} was not refused: {plan:?}"),
    }
}

/// Every recorded refusal, with the part of the message that names it.
#[test]
fn the_recorded_refusals_of_a_rename() {
    let cases: Vec<(&str, Document, Document, &str)> = vec![
        // q7s2.out
        (
            "c1 source inside an element by an index",
            items(),
            doc! {"$rename": {"items.0.sku": "code"}},
            SOURCE,
        ),
        ("c2 destination arr.1", items(), doc! {"$rename": {"s": "arr.1"}}, DEST),
        ("c3 destination items.0.s", items(), doc! {"$rename": {"s": "items.0.s"}}, DEST),
        (
            "c4 source items.sku, no index",
            items(),
            doc! {"$rename": {"items.sku": "code"}},
            TRAVERSE,
        ),
        ("c5 source arr.0", items(), doc! {"$rename": {"arr.0": "z"}}, SOURCE),
        ("s7 source below a scalar", scalar(), doc! {"$rename": {"a.y": "b"}}, TRAVERSE),
        // q7s2d.out
        ("4a destination items.s", pairs(), doc! {"$rename": {"x": "items.s"}}, DEST),
        (
            "4b destination items.s over arrays of arrays",
            nested(),
            doc! {"$rename": {"x": "items.s"}},
            DEST,
        ),
        ("4c source items.s", pairs(), doc! {"$rename": {"items.s": "y"}}, TRAVERSE),
        (
            "4d source items.s over arrays of arrays",
            nested(),
            doc! {"$rename": {"items.s": "y"}},
            TRAVERSE,
        ),
        ("4e destination below a scalar", scalar(), doc! {"$rename": {"x": "a.y"}}, BELOW),
        ("4f source below a scalar", scalar(), doc! {"$rename": {"a.y": "b"}}, TRAVERSE),
        ("4g destination items.s.t", pairs(), doc! {"$rename": {"x": "items.s.t"}}, DEST),
        (
            "4i source and destination inside one element",
            pairs(),
            doc! {"$rename": {"items.0.s": "items.0.t"}},
            SOURCE,
        ),
        // a counter into an array element: the writer's counter check never sees it
        ("a counter into an array element", counter(), doc! {"$rename": {"n": "arr.1"}}, DEST),
        ("a counter out of an array element", counter(), doc! {"$rename": {"arr.0": "n2"}}, SOURCE),
    ];
    for (label, d, u, want) in cases {
        let err = refusal(&d, u.clone());
        assert!(err.contains(want), "{label}: {u}: {err}");
        // the reference words a destination below a scalar without the operator's name
        if want != BELOW {
            assert!(err.starts_with("invalid update: $rename"), "{label}: {err}");
        }
    }
}

/// The failing input of the review: an integer counter renamed into an array element.
#[test]
fn a_counter_cannot_get_into_an_array_by_a_rename() {
    let d = counter();
    let update = parse(doc! {"$rename": {"n": "arr.1"}});
    assert!(apply_with_effects(&update, None, &d, NOW).is_err());
    // and the writer still refuses a plain rename of the counter, the check it owns
    let (effects, _) =
        match apply_with_effects(&parse(doc! {"$rename": {"n": "m"}}), None, &d, NOW).unwrap() {
            crate::update_effects::Plan::Effects { effects, post_image } => (effects, post_image),
            _ => panic!("effects"),
        };
    let mut w = writer_with(&d);
    assert!(matches!(
        effects.apply(&mut w, 0),
        Outcome::Refused(kimmy_core::effects::UpdateRefused::RenameOfCounter(_))
    ));
}

/// Allowed, as in the reference: a whole array, a field of a sub-document, a rename beside an
/// array write, a document key spelled `0`.
#[test]
fn the_allowed_renames_are_plain_moves() {
    let (effects, _) = parity_doc(doc! {"$rename": {"items": "lines"}}, &items());
    same_effects(
        &effects,
        &fx(|e| {
            e.rename(p("items"), p("lines"), v(items().get("items").unwrap().clone()));
        }),
        "whole array",
    );
    let (effects, _) = parity_doc(doc! {"$rename": {"o.p": "o.q"}}, &items());
    assert_eq!(effects.renames().len(), 1);
    let (effects, _) = parity_doc(doc! {"$rename": {"s": "s2"}, "$set": {"arr.0": 9}}, &items());
    assert_eq!(effects.renames().len(), 1);
    assert_eq!(effects.len(), 3);
    let d = doc! { "_id": 1, "o": { "0": "k" }, "x": "v" };
    let (effects, _) = parity_doc(doc! {"$rename": {"x": "o.1"}}, &d);
    assert_eq!(effects.renames().len(), 1);
    let (effects, _) = parity_doc(doc! {"$rename": {"x": "o.0"}}, &d);
    assert_eq!(effects.renames().len(), 1);
}

/// The nine recorded cases of a missing source (`q7s2e.out` lines 5 to 45): the reference does
/// nothing (matched 1, modified 0, the document unchanged) and does not look at the destination,
/// however it goes through an array or a scalar.
#[test]
fn a_missing_source_is_a_no_op_whatever_the_destination() {
    let two = || doc! { "_id": 1, "items": [{ "sku": "a" }, { "sku": "b" }], "arr": [1, 2] };
    let cases: Vec<(&str, Document, Document)> = vec![
        ("1a items.0.nope -> code (line 5)", two(), doc! {"$rename": {"items.0.nope": "code"}}),
        ("1b items.5.sku -> code (line 10)", two(), doc! {"$rename": {"items.5.sku": "code"}}),
        (
            "1c items.0.nope -> items.1.x (line 15)",
            two(),
            doc! {"$rename": {"items.0.nope": "items.1.x"}},
        ),
        ("1d arr.5 -> y (line 20)", two(), doc! {"$rename": {"arr.5": "y"}}),
        ("1e nope -> items.0.t (line 25)", two(), doc! {"$rename": {"nope": "items.0.t"}}),
        (
            "1f nope -> a.b over {a:5} (line 30)",
            doc! { "_id": 1, "a": 5 },
            doc! {"$rename": {"nope": "a.b"}},
        ),
        ("1g nope -> items.s (line 35)", two(), doc! {"$rename": {"nope": "items.s"}}),
        (
            "1h items.0.nope -> code, items absent (line 40)",
            doc! { "_id": 1 },
            doc! {"$rename": {"items.0.nope": "code"}},
        ),
        ("1i nope -> missing.deep (line 45)", two(), doc! {"$rename": {"nope": "missing.deep"}}),
    ];
    for (label, d, u) in cases {
        let (effects, outcome) = parity_doc(u, &d);
        assert!(effects.is_empty(), "{label}: {effects:?}");
        assert_eq!(outcome, Outcome::Unchanged, "{label}");
    }
}

/// A missing source that goes through an array by a non-numeric segment, or below a scalar, is
/// still refused, as the reference refuses it (`q7s2.out` lines 116 and 586, `q7s2d.out` 86 to 101).
#[test]
fn a_source_through_an_array_by_name_or_below_a_scalar_is_refused_even_when_missing() {
    let err =
        refusal(&doc! { "_id": 1, "items": [{ "s": 1 }] }, doc! {"$rename": {"items.nope": "y"}});
    assert!(err.contains(TRAVERSE), "{err}");
    let err = refusal(&doc! { "_id": 1, "a": 5 }, doc! {"$rename": {"a.y": "b"}});
    assert!(err.contains(TRAVERSE), "{err}");
}

/// An existing source with a destination into an array stays refused (the counter of the review).
#[test]
fn an_existing_source_into_an_array_stays_refused() {
    let err = refusal(&counter(), doc! {"$rename": {"n": "arr.1"}});
    assert!(err.contains(DEST), "{err}");
}

#[test]
fn a_rename_of_a_missing_source_is_nothing() {
    let (effects, outcome) = parity_doc(doc! {"$rename": {"nope": "t"}}, &items());
    assert!(effects.is_empty());
    assert_eq!(outcome, Outcome::Unchanged);
}

/// An upsert is checked on the seed the filter builds.
#[test]
fn an_upsert_refuses_the_same_ways() {
    let update = parse(doc! {"$rename": {"x": "items.s"}});
    let filter = filt(doc! {"items": [{ "s": 1 }], "x": 1});
    let err = match plan_upsert(&update, &filter, NOW) {
        Err(e) => e.to_string(),
        Ok(UpsertPlan::Effects { effects, .. }) => panic!("{effects:?}"),
        Ok(UpsertPlan::Replace { .. }) => panic!("replace"),
    };
    assert!(err.contains(DEST), "{err}");
}

/// The `$rename` source rule, case by case (`q7s2f.out`): a source that goes through a scalar, or
/// through an array by a name, is refused with the part that names the value, whatever the
/// destination and whether or not anything lies beyond it; a missing source is nothing; an
/// existing source inside an array element is refused as such.
#[test]
fn the_source_rule_of_q7s2f_case_by_case() {
    let arr = || doc! { "_id": 1, "arr": [1, 2] };
    let skus = || doc! { "_id": 1, "items": [{ "sku": "a" }, { "sku": "b" }] };
    let grid = || doc! { "_id": 1, "grid": [[1, 2], [3, 4]] };
    let seven = || doc! { "_id": 1, "items": [7] };
    let seven_eight = || doc! { "_id": 1, "items": [7, 8] };
    let ys = || doc! { "_id": 1, "items": [{ "y": 5 }] };
    let ys2 = || doc! { "_id": 1, "items": [{ "y": 5 }, { "y": 6 }] };
    // (label, document, update, the traversal message the reference gives)
    let traverse: Vec<(&str, Document, Document, &str)> = vec![
        (
            "q7s2f f1 arr.0.x -> y over arr:[1,2]",
            arr(),
            doc! {"$rename": {"arr.0.x": "y"}},
            "(0 of arr.0.x) to traverse the element ({0: 1})",
        ),
        (
            "q7s2f f2 items.0.sku.x -> y",
            skus(),
            doc! {"$rename": {"items.0.sku.x": "y"}},
            "(sku of items.0.sku.x) to traverse the element ({sku: \"a\"})",
        ),
        (
            "q7s2f f3 grid.0.s -> y over an array of arrays",
            grid(),
            doc! {"$rename": {"grid.0.s": "y"}},
            "(0 of grid.0.s) to traverse the element",
        ),
        (
            "q7s2f f4 arr.0.x -> arr.1: the destination is not looked at",
            arr(),
            doc! {"$rename": {"arr.0.x": "arr.1"}},
            "(0 of arr.0.x) to traverse the element ({0: 1})",
        ),
        (
            "q7s2f f5 items.0.sku.x -> items.1.z",
            skus(),
            doc! {"$rename": {"items.0.sku.x": "items.1.z"}},
            "(sku of items.0.sku.x) to traverse the element ({sku: \"a\"})",
        ),
        (
            "q7s2f f6 grid.0.s -> grid.1.0",
            grid(),
            doc! {"$rename": {"grid.0.s": "grid.1.0"}},
            "(0 of grid.0.s) to traverse the element",
        ),
        (
            "q7s2f f9 items.0.x -> y over items:[7]",
            seven(),
            doc! {"$rename": {"items.0.x": "y"}},
            "(0 of items.0.x) to traverse the element ({0: 7})",
        ),
        (
            "q7s2f f10 items.0.x -> items.1.t over items:[7,8]",
            seven_eight(),
            doc! {"$rename": {"items.0.x": "items.1.t"}},
            "(0 of items.0.x) to traverse the element ({0: 7})",
        ),
        (
            "q7s2f f11 items.0.y.z -> w over items:[{y:5}]",
            ys(),
            doc! {"$rename": {"items.0.y.z": "w"}},
            "(y of items.0.y.z) to traverse the element ({y: 5})",
        ),
        (
            "q7s2f f12 items.0.y.z -> items.1.t",
            ys2(),
            doc! {"$rename": {"items.0.y.z": "items.1.t"}},
            "(y of items.0.y.z) to traverse the element ({y: 5})",
        ),
    ];
    for (label, d, u, want) in traverse {
        let err = refusal(&d, u.clone());
        assert!(err.contains(want), "{label}: {err}");
        assert!(err.starts_with("invalid update: $rename cannot use the part"), "{label}: {err}");
    }
    // an existing source inside an array element (the reference: code 2)
    for (label, d, u) in [
        (
            "q7s2f f14 arr.0 -> y (an existing scalar element)",
            arr(),
            doc! {"$rename": {"arr.0": "y"}},
        ),
        ("q7s2f f15 items.0.sku -> y", skus(), doc! {"$rename": {"items.0.sku": "y"}}),
    ] {
        let err = refusal(&d, u.clone());
        assert!(err.contains(SOURCE), "{label}: {err}");
    }
    // a source that is not there: nothing, and the destination is not looked at
    for (label, d, u) in [
        ("q7s2f f7 nope.x -> y (control)", skus(), doc! {"$rename": {"nope.x": "y"}}),
        ("q7s2f f8 nope.x -> items.1.t", skus(), doc! {"$rename": {"nope.x": "items.1.t"}}),
        (
            "q7s2f f13 items.5.x.y -> y: an index past the end ends the walk",
            seven(),
            doc! {"$rename": {"items.5.x.y": "y"}},
        ),
    ] {
        let (effects, outcome) = parity_doc(u, &d);
        assert!(effects.is_empty(), "{label}: {effects:?}");
        assert_eq!(outcome, Outcome::Unchanged, "{label}");
    }
}
