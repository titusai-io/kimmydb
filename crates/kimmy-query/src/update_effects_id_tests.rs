//! `_id`, the parse-time overlap checks, and the engine behaviours the effects follow as they are
//! today (pinned, each marked with what the reference does).

use bson::{Bson, doc};

use kimmy_core::effects::Outcome;

use crate::update;
use crate::update_effects_tests::{fx, p, parity_doc, parse, plan_of, same_effects, v};

/// The `_id` of a document that matched is judged by the RESULT of the update, as the reference
/// judges it (q7s2r.out, document `{_id: 1, a: 1}`, filter `{_id: 1}`): an update whose result
/// leaves the stored `_id` as it was is no write at all, and the effects layer never emits an
/// effect at `_id`. The write that changes it is the engine's refusal (below).
const MATCHED: fn() -> bson::Document = || doc! {"_id": 1, "a": 1};

/// Nothing is written, so there are no effects: `$set _id` with the same value (r0137), a
/// `$setOnInsert` of `_id` on a document that matched, whatever its value (r0159, r0161), a
/// `$rename` of a source that is not there onto `_id` (r0155).
#[test]
fn an_id_the_result_keeps_is_no_write_and_no_effect() {
    let d = MATCHED();
    for (label, u) in [
        ("r0137", doc! {"$set": {"_id": 1}}),
        ("r0155", doc! {"$rename": {"y": "_id"}}),
        ("r0159", doc! {"$setOnInsert": {"_id": 1}}),
        ("r0161", doc! {"$setOnInsert": {"_id": 2}}),
    ] {
        let (effects, outcome) = parity_doc(u.clone(), &d);
        assert!(effects.is_empty(), "{label} {u}: {effects:?}");
        assert_eq!(outcome, Outcome::Unchanged, "{label} {u}");
    }
}

/// An operator whose arithmetic leaves the `_id` as it was is no write either: `$inc` of 0
/// (r0169), `$mul` of 1 (r0173), `$min` and `$max` of the same value (r0163, r0165), `$bit` or 0
/// (r0175).
#[test]
fn an_arithmetic_that_leaves_the_id_alone_is_no_write() {
    let d = MATCHED();
    for (label, u) in [
        ("r0169", doc! {"$inc": {"_id": 0}}),
        ("r0173", doc! {"$mul": {"_id": 1}}),
        ("r0163", doc! {"$min": {"_id": 1}}),
        ("r0165", doc! {"$max": {"_id": 1}}),
        ("r0175", doc! {"$bit": {"_id": {"or": 0}}}),
    ] {
        let (effects, outcome) = parity_doc(u.clone(), &d);
        assert!(effects.is_empty(), "{label} {u}: {effects:?}");
        assert_eq!(outcome, Outcome::Unchanged, "{label} {u}");
    }
}

/// With another operator beside it, only that operator produces an effect: `$set _id 1` and
/// `$set a 2` write `a` and nothing at `_id` (r0179).
#[test]
fn an_id_beside_another_write_gives_the_effect_of_the_other_only() {
    let (effects, outcome) = parity_doc(doc! {"$set": {"_id": 1, "a": 2}}, &MATCHED());
    same_effects(
        &effects,
        &fx(|e| {
            e.set(p("a"), v(2));
        }),
        "r0179",
    );
    assert!(outcome.modified(), "{outcome:?}");
}

/// A result that would change the `_id`, in value or in type, or drop it, is the engine's refusal
/// (the reference's code 66, "would modify the immutable field", as a 400), and nothing is
/// written: the `_id` as another type (r0139, `$set _id 1` as an Int64), another value (r0145),
/// `$unset` (r0149), `$rename` from `_id` (r0151) and to `_id` (r0153), `$inc` of 1 (r0171), and
/// `$set a 2` beside `$set _id 2` (r0181); a path below `_id` is code 28 (r0177).
#[test]
fn a_result_that_would_change_the_id_is_refused_and_writes_nothing() {
    let d = MATCHED();
    for (label, u) in [
        ("r0139", doc! {"$set": {"_id": 1_i64}}),
        ("r0145", doc! {"$set": {"_id": 2}}),
        ("r0149", doc! {"$unset": {"_id": ""}}),
        ("r0151", doc! {"$rename": {"_id": "y"}}),
        ("r0153", doc! {"$rename": {"a": "_id"}}),
        ("r0167", doc! {"$max": {"_id": 5}}),
        ("r0171", doc! {"$inc": {"_id": 1}}),
        ("r0177", doc! {"$set": {"_id.x": 1}}),
        ("r0181", doc! {"$set": {"a": 2, "_id": 2}}),
    ] {
        // the refusal is the parser's or the engine's; either way the update does not apply
        let err = match update::parse(&u) {
            Err(e) => e.to_string(),
            Ok(parsed) => crate::update_effects::apply_with_effects(
                &parsed,
                None,
                &d,
                crate::update_effects_tests::NOW,
            )
            .expect_err(&format!("{label} {u}"))
            .to_string(),
        };
        assert!(err.contains("_id"), "{label} {u}: {err}");
    }
}

/// A `$rename` TO `_id` of a source that is there is refused when the update is applied
/// (r0153), and so is one to a path below `_id`.
#[test]
fn a_rename_to_id_is_refused_when_the_update_is_applied() {
    for u in [doc! {"$rename": {"x": "_id"}}, doc! {"$rename": {"x": "_id.y"}}] {
        let parsed = update::parse(&u).unwrap_or_else(|e| panic!("{u}: {e}"));
        let err = crate::update_effects::apply_with_effects(
            &parsed,
            None,
            &doc! {"_id": 1, "x": 1},
            crate::update_effects_tests::NOW,
        )
        .expect_err(&u.to_string())
        .to_string();
        assert!(err.contains("_id"), "{u}: {err}");
    }
}

/// The reference refuses two operators on paths that overlap at parse, matched or not
/// (`UpsertEffects::check` mirrors it). The parser does it for every operator pair.
#[test]
fn overlapping_operators_are_refused_at_parse_matched_or_not() {
    for u in [
        doc! {"$setOnInsert": {"a": 1}, "$inc": {"a": 1}},
        doc! {"$set": {"a": 1}, "$inc": {"a": 1}},
        doc! {"$set": {"a": {"x": 1}}, "$inc": {"a.x": 1}},
        doc! {"$setOnInsert": {"a": {"x": 1}}, "$inc": {"a.x": 1}},
        doc! {"$set": {"a.b": 1}, "$setOnInsert": {"a": 1}},
        doc! {"$set": {"a": 1}, "$unset": {"a.b": ""}},
        doc! {"$set": {"a.b": 1, "a": 2}},
    ] {
        assert!(update::parse(&u).is_err(), "{u}");
    }
    // several $inc on distinct paths, and a leaf beside a leaf, are fine
    for u in
        [doc! {"$inc": {"a": 1, "b": 1, "c.d": 1}}, doc! {"$set": {"a.x": 1}, "$inc": {"a.n": 1}}]
    {
        assert!(update::parse(&u).is_ok(), "{u}");
    }
}

fn lines() -> bson::Document {
    doc! {
        "_id": 1, "s": "x", "arr": [1, 2],
        "items": [ {"sku": "a", "qty": 1}, {"sku": "b", "qty": 2} ],
        "grid": [[1, 2], [3, 4]],
    }
}

/// A numeric path component below an element that is itself an array addresses the inner
/// element, as the reference does (q7s2.out a1-a4, a8-a10): `grid.0.1` sets `grid[0][1]`, and
/// the effect is one `Set` of the outermost array.
#[test]
fn nested_array_index_sets_the_inner_element() {
    let d = lines();
    let (effects, _) = parity_doc(doc! {"$set": {"grid.0.1": 9}}, &d);
    let want = Bson::Array(vec![
        Bson::Array(vec![1.into(), 9.into()]),
        Bson::Array(vec![3.into(), 4.into()]),
    ]);
    crate::update_effects_tests::same_effects(
        &effects,
        &fx(|e| {
            e.set(p("grid"), v(want.clone()));
        }),
        "grid.0.1",
    );
}

/// UNCHANGED ENGINE BEHAVIOUR, both policies, pinned: the engine renames a field inside an array
/// element (`items.0.sku` to `code`); the reference refuses a `$rename` whose source or target is
/// inside an array element. The effects follow the engine: the source's array is rewritten whole
/// (`Set(items)`), the target is a `Set`, and a counter check does not apply (an array holds no
/// counters).
#[test]
fn two_writes_inside_one_array_are_one_set_of_it() {
    let d = lines();
    let u = parse(doc! {"$set": {"items.0.qty": 5, "items.1.qty": 6}});
    let (effects, _) = plan_of(&u, None, &d);
    assert_eq!(effects.len(), 1);
}
