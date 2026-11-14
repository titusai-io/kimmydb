//! `_id`, the parse-time overlap checks, and the engine behaviours the effects follow as they are
//! today (pinned, each marked with what the reference does).

use bson::{Bson, doc};

use crate::update;
use crate::update_effects_tests::{fx, p, parity_doc, parse, plan_of, v};

/// `_id` is the document's identity: any write to it is refused, for both policies (the same
/// value included), so the effects layer never sees one. A write to `_id` as the path of an
/// operator is refused at parse; a `$rename` TO `_id` is refused when the update is applied.
#[test]
fn a_write_to_id_is_refused() {
    for u in [
        doc! {"$set": {"_id": 1}},
        doc! {"$set": {"_id": 2}},
        doc! {"$set": {"_id.x": 2}},
        doc! {"$setOnInsert": {"_id": 1}},
        doc! {"$unset": {"_id": ""}},
        doc! {"$inc": {"_id": 1}},
        doc! {"$rename": {"_id": "x"}},
    ] {
        let err = update::parse(&u).expect_err(&u.to_string()).to_string();
        assert!(err.contains("_id"), "{u}: {err}");
    }
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
    for u in [
        doc! {"$inc": {"a": 1, "b": 1, "c.d": 1}},
        doc! {"$set": {"a.x": 1}, "$inc": {"a.n": 1}},
    ] {
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

/// UNCHANGED ENGINE BEHAVIOUR, both policies, pinned as it is today: a numeric path component
/// below an element that is itself an array does not address the inner element: the engine
/// replaces the inner array by a document (`{"1": 2}`). The reference sets `grid[0][1]`. The
/// effects follow the engine's post-image; a fix of the engine is a separate change on main.
#[test]
fn nested_array_index_follows_the_engines_post_image_as_it_is() {
    let d = lines();
    let (effects, _) = parity_doc(doc! {"$set": {"grid.0.1": 9}}, &d);
    let want = Bson::Array(vec![Bson::Document(doc! {"1": 9}), Bson::Array(vec![3.into(), 4.into()])]);
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
fn a_rename_from_inside_an_array_element_is_a_set_of_the_array_and_a_set() {
    let d = lines();
    let (effects, _) = parity_doc(doc! {"$rename": {"items.0.sku": "code"}}, &d);
    let items = Bson::Array(vec![
        Bson::Document(doc! {"qty": 1}),
        Bson::Document(doc! {"sku": "b", "qty": 2}),
    ]);
    crate::update_effects_tests::same_effects(
        &effects,
        &fx(|e| {
            e.set(p("items"), v(items.clone()));
            e.set(p("code"), v("a"));
        }),
        "rename out of an element",
    );
}

#[test]
fn a_rename_into_an_array_element_is_an_unset_and_a_set_of_the_array() {
    let d = lines();
    let (effects, outcome) = parity_doc(doc! {"$rename": {"s": "arr.1"}}, &d);
    assert!(outcome.modified());
    crate::update_effects_tests::same_effects(
        &effects,
        &fx(|e| {
            e.unset(p("s"));
            e.set(p("arr"), v(vec![Bson::Int32(1), Bson::String("x".into())]));
        }),
        "rename into an element",
    );
}

#[test]
fn two_writes_inside_one_array_are_one_set_of_it() {
    let d = lines();
    let u = parse(doc! {"$set": {"items.0.qty": 5, "items.1.qty": 6}});
    let (effects, _) = plan_of(&u, None, &d);
    assert_eq!(effects.len(), 1);
}
