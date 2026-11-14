//! The effects of a pipeline update (ADR-265): per changed top-level field, one-segment paths,
//! field order, `_id`, and the recorded reference answers.

use bson::{Bson, Document, doc};
use kimmy_core::effects::{Effects, Outcome};

use crate::update::{self, Update};
use crate::update_effects::{Plan, apply_with_effects};
use crate::update_effects_tests::{
    NOW, bytes, fx, np, p, parity, plan_of, render, same_effects, v, writer_with,
};

fn pipe(stages: Vec<Document>) -> Update {
    let stages: Vec<Bson> = stages.into_iter().map(Bson::Document).collect();
    update::parse_pipeline(&stages, &[]).unwrap_or_else(|e| panic!("{stages:?}: {e}"))
}

/// The effects checked through the writer (as `parity`) and the engine's post-image.
fn parity_post(u: &Update, d: &Document) -> (Effects, Document) {
    let (effects, _) = parity(u, None, d);
    (effects, plan_of(u, None, d).1)
}

fn keys(d: &Document) -> Vec<&str> {
    d.keys().map(String::as_str).collect()
}

#[test]
fn a_set_and_an_unset_stage_are_one_set_and_one_unset() {
    let d = doc! { "_id": 1, "a": 2, "b": 3, "scratch": true };
    let u =
        pipe(vec![doc! {"$set": {"total": {"$add": ["$a", "$b"]}}}, doc! {"$unset": "scratch"}]);
    let (effects, post) = parity_post(&u, &d);
    let total = post.get("total").unwrap().clone();
    same_effects(
        &effects,
        &fx(|e| {
            e.unset(p("scratch"));
            e.set(p("total"), v(total));
        }),
        "set and unset",
    );
    assert!(!effects.is_empty() && effects.len() == 2);
}

#[test]
fn a_pipeline_that_changes_nothing_has_no_effects() {
    let d = doc! { "_id": 1, "a": 2, "sub": { "k": [1, 2] } };
    for stages in [
        vec![],
        vec![doc! {"$set": {"a": 2}}],
        vec![doc! {"$set": {"sub": {"k": [1, 2]}}}],
        vec![doc! {"$unset": "missing"}],
        vec![doc! {"$project": {"a": 1, "sub": 1}}],
    ] {
        let (effects, outcome) = parity(&pipe(stages.clone()), None, &d);
        assert!(effects.is_empty(), "{stages:?}: {effects:?}");
        assert_eq!(outcome, Outcome::Unchanged);
    }
}

#[test]
fn a_set_of_a_sub_document_is_one_set_of_the_whole_field() {
    let d = doc! { "_id": 1, "sub": { "k": 1, "j": 2 } };
    let u = pipe(vec![doc! {"$set": {"sub": {"k": 1, "n": 3}}}]);
    let (effects, _) = parity(&u, None, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.set(p("sub"), v(doc! {"k": 1, "n": 3}));
        }),
        "whole field",
    );
}

#[test]
fn id_is_never_an_effect() {
    let d = doc! { "_id": 1, "a": 1 };
    // removed: the stored one is back; restated as another type of the same id: the stored one stays
    for stages in [vec![doc! {"$unset": "_id"}], vec![doc! {"$replaceWith": {"a": "$a"}}]] {
        let (effects, outcome) = parity(&pipe(stages.clone()), None, &d);
        assert!(effects.is_empty(), "{stages:?}: {effects:?}");
        assert_eq!(outcome, Outcome::Unchanged);
    }
    // the same id of another type: the engine stores it as written, the field policy keeps the
    // stored one (the id is the document's identity, never an effect)
    let (effects, post) = plan_of(&pipe(vec![doc! {"$set": {"_id": 1i64}}]), None, &d);
    assert_eq!(post.get("_id"), Some(&Bson::Int64(1)));
    assert!(effects.is_empty());
    // another document's id is the engine's refusal
    let u = pipe(vec![doc! {"$set": {"_id": 2}}]);
    assert!(apply_with_effects(&u, None, &d, NOW).is_err());
}

#[test]
fn a_dotted_set_field_name_is_one_segment_and_is_never_split() {
    let d = doc! { "_id": 1, "x": 5 };
    let u = pipe(vec![doc! {"$replaceWith": {"$setField": {
    "field": "a.b", "input": "$$ROOT", "value": 1 }}}]);
    let (effects, _) = parity(&u, None, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.set(p("a.b"), v(1));
        }),
        "one segment",
    );
    // the writer holds the field `a.b`, not the field `b` of a map `a`
    let mut w = writer_with(&d);
    effects.apply(&mut w, 0);
    let seen = w.visible().unwrap();
    assert!(seen.contains_key(&p("a.b")), "{seen:?}");
    assert!(!seen.contains_key(&np(&["a", "b"])));
    assert!(!seen.contains_key(&p("a")));
    assert_eq!(render(&w).get("a.b"), Some(&Bson::Int32(1)));
    assert!(render(&w).get("a").is_none());
}

#[test]
fn a_dotted_set_field_name_inside_a_sub_document_stays_one_field() {
    let d = doc! { "_id": 1, "sub": { "k": 1 } };
    let u = pipe(vec![doc! {"$set": {"sub": {"$setField": {
    "field": "x.y", "input": "$sub", "value": 2 }}}}]);
    let (effects, _) = parity(&u, None, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.set(p("sub"), v(doc! {"k": 1, "x.y": 2}));
        }),
        "document with a dotted field",
    );
    let mut w = writer_with(&d);
    effects.apply(&mut w, 0);
    assert!(w.visible().unwrap().contains_key(&np(&["sub", "x.y"])));
    assert!(!w.visible().unwrap().contains_key(&np(&["sub", "x", "y"])));
}

#[test]
fn a_dollar_field_name_and_unset_field_are_one_segment_too() {
    let d = doc! { "_id": 1, "a.b": 1, "c": 2 };
    let u = pipe(vec![doc! {"$replaceWith": {"$unsetField": {
    "field": "a.b", "input": "$$ROOT" }}}]);
    let (effects, _) = parity(&u, None, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.unset(p("a.b"));
        }),
        "unset of a dotted name",
    );
    let u = pipe(vec![doc! {"$replaceWith": {"$setField": {
    "field": {"$literal": "$x"}, "input": "$$ROOT", "value": 1 }}}]);
    let (effects, _) = parity(&u, None, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.set(p("$x"), v(1));
        }),
        "dollar name",
    );
}

#[test]
fn new_fields_keep_the_order_the_stages_gave_them() {
    let d = doc! { "_id": 1, "x": 1 };
    let (effects, post) = parity_post(&pipe(vec![doc! {"$set": {"zz": 1, "aa": 2}}]), &d);
    assert_eq!(keys(&post), ["_id", "x", "zz", "aa"]);
    same_effects(
        &effects,
        &fx(|e| {
            e.set(p("zz"), v(1));
            e.set(p("aa"), v(2));
        }),
        "pipeline order",
    );
    // after $replaceWith: the new fields in the order of the replacement
    let u = pipe(vec![doc! {"$replaceWith": {"_id": "$_id", "q": 1, "p": 2}}]);
    let (effects, post) = parity_post(&u, &doc! { "_id": 1, "x": 1, "y": 2 });
    assert_eq!(keys(&post), ["_id", "q", "p"]);
    same_effects(
        &effects,
        &fx(|e| {
            e.unset(p("x"));
            e.unset(p("y"));
            e.set(p("q"), v(1));
            e.set(p("p"), v(2));
        }),
        "after replaceWith",
    );
    // after $project: only unsets
    let u = pipe(vec![doc! {"$project": {"a": 1}}]);
    let (effects, post) = parity_post(&u, &doc! { "_id": 1, "a": 1, "b": 2, "c": 3 });
    assert_eq!(keys(&post), ["_id", "a"]);
    same_effects(
        &effects,
        &fx(|e| {
            e.unset(p("b"));
            e.unset(p("c"));
        }),
        "after project",
    );
}

#[test]
fn swapped_values_keep_the_fields_where_they_are() {
    let d = doc! { "_id": 1, "a": 1, "b": 2 };
    let u = pipe(vec![doc! {"$replaceWith": {"_id": "$_id", "a": "$b", "b": "$a"}}]);
    let (effects, post) = parity_post(&u, &d);
    assert_eq!(post, doc! { "_id": 1, "a": 2, "b": 1 });
    assert_eq!(effects.len(), 2);
}

/// A deviation of the `field` policy (a pipeline update is diffed, not a replace): the order of
/// the fields that stay is the writer's, so a result that only reorders them is no write, and a
/// new field goes after the fields that were already there. `replaceOne` sets the order.
#[test]
fn a_reorder_is_not_an_effect_and_new_fields_go_after_the_kept_ones() {
    let d = doc! { "_id": 1, "a": 1, "b": 2 };
    let u = pipe(vec![doc! {"$replaceWith": {"_id": "$_id", "b": "$b", "a": "$a"}}]);
    let (effects, post) = plan_of(&u, None, &d);
    assert_eq!(keys(&post), ["_id", "b", "a"]);
    assert!(effects.is_empty());
    let mut w = writer_with(&d);
    assert_eq!(effects.apply(&mut w, 0), Outcome::Unchanged);
    assert_eq!(keys(&render(&w)), ["_id", "a", "b"]);

    let u = pipe(vec![doc! {"$replaceWith": {"n": 9, "b": "$b", "_id": "$_id"}}]);
    let (effects, post) = plan_of(&u, None, &d);
    assert_eq!(keys(&post), ["_id", "n", "b"]);
    let mut w = writer_with(&d);
    assert!(effects.apply(&mut w, 0).modified());
    assert_eq!(keys(&render(&w)), ["_id", "b", "n"]);
    assert_eq!(render(&w), post);
    assert_ne!(bytes(&render(&w)), bytes(&post));
}

#[test]
fn a_pipeline_is_never_an_ancestor_beside_a_descendant() {
    // one top-level field changed deep inside: one Set of the field, never a leaf beside it
    let d = doc! { "_id": 1, "sub": { "deep": { "x": 1 } } };
    let u = pipe(vec![doc! {"$set": {"sub.deep.x": 2}}]);
    let (effects, _) = parity(&u, None, &d);
    same_effects(
        &effects,
        &fx(|e| {
            e.set(p("sub"), v(doc! {"deep": {"x": 2}}));
        }),
        "one set",
    );
}

#[test]
fn a_replacement_is_still_not_an_effect_list() {
    let u = update::parse(&doc! { "b": 2 }).unwrap();
    assert!(matches!(
        apply_with_effects(&u, None, &doc! { "_id": 1 }, NOW).unwrap(),
        Plan::Replace { .. }
    ));
}

/// The reference's recorded answers (`reference-6.0.5/q8a1/q8a1.rec.out`, the pipeline updates
/// with an empty filter and no error). Where the engine's result is the recorded one, the writer
/// fed the effects renders it too, and where the recorded answer differs from the engine's (a
/// known deviation of the engine's, not of the effects) the case is counted and skipped.
#[test]
fn the_recorded_reference_answers_are_what_the_writer_renders() {
    let data: serde_json::Value =
        serde_json::from_str(include_str!("testdata/q8a1_pipeline_updates.json")).unwrap();
    let docs = |j: &serde_json::Value| -> Vec<Document> {
        j.as_array()
            .unwrap()
            .iter()
            .map(|d| match Bson::try_from(d.clone()).unwrap() {
                Bson::Document(d) => d,
                other => panic!("{other:?}"),
            })
            .collect()
    };
    let (mut same_as_reference, mut engine_differs) = (0, 0);
    for case in data.as_array().unwrap() {
        let label = case["label"].as_str().unwrap();
        let stages: Vec<Bson> = docs(&case["pipeline"]).into_iter().map(Bson::Document).collect();
        let Ok(update) = update::parse_pipeline(&stages, &[]) else {
            engine_differs += 1;
            continue;
        };
        let after = docs(&case["after"]);
        for before in docs(&case["before"]) {
            let Ok(Plan::Effects { effects, post_image }) =
                apply_with_effects(&update, None, &before, NOW)
            else {
                engine_differs += 1;
                continue;
            };
            let mut w = writer_with(&before);
            let outcome = effects.apply(&mut w, 0);
            assert!(outcome.refusal().is_none(), "{label}: {outcome:?}");
            assert_eq!(render(&w), post_image, "{label}: the writer renders another document");
            let want = after.iter().find(|a| a.get("_id") == before.get("_id")).unwrap();
            if post_image == *want {
                same_as_reference += 1;
                assert_eq!(render(&w), *want, "{label}");
            } else {
                engine_differs += 1;
            }
        }
    }
    assert!(same_as_reference >= 8, "{same_as_reference} same, {engine_differs} different");
}
