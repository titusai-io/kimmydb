//! The effects of an upsert that matched nothing (`plan_upsert`): the layers of the creation, as
//! written, and the creation through the writer's own entries.

use bson::{Bson, Document, doc};
use kimmy_core::effects::{CellTag, UpsertEffects};
use kimmy_core::merge_state::{JoinOptions, Path, Value, Writer};

use crate::update::{self, Update};
use crate::update_effects::{UpsertPlan, plan_upsert};
use crate::update_effects_tests::{NOW, filt, np, p, parse, render, v, wid};

fn plan(filter: Document, u: Document) -> (UpsertEffects, Document) {
    match plan_upsert(&parse(u.clone()), &filt(filter.clone()), NOW)
        .unwrap_or_else(|e| panic!("{filter} {u}: refused: {e}"))
    {
        UpsertPlan::Effects { effects, post_image } => (effects, post_image),
        UpsertPlan::Replace { .. } => panic!("a replacement"),
    }
}

fn refused(filter: Document, u: Document) -> String {
    match plan_upsert(&parse(u), &filt(filter), NOW) {
        Err(e) => e.to_string(),
        Ok(p) => panic!("not refused: {p:?}"),
    }
}

/// Create the document through the writer's creation entries, as S2a will: the `Creation` cells
/// are `fields`, the `Plain` ones `sets`, then every `$inc` (an Int32 or Int64 operand: the entry
/// takes an `i64` until S2a widens it).
fn create(fx: &UpsertEffects) -> Writer {
    let cells = fx.creation_cells().expect("the creation composes");
    let part = |t: CellTag| -> Vec<(Path, Value)> {
        cells.iter().filter(|c| c.tag == t).map(|c| (c.path.clone(), c.value.clone())).collect()
    };
    let mut w = Writer::new(wid(5), JoinOptions::default());
    let none = p("__no_inc__");
    let inc_path = fx.inc.first().map(|(q, _)| q).unwrap_or(&none);
    let made = w
        .upsert_soi_create(&part(CellTag::Creation), inc_path, &part(CellTag::Plain), 0)
        .expect("the creation writes");
    for (q, by) in &fx.inc {
        let n = match by {
            Value::Int32(n) => i64::from(*n),
            Value::Int64(n) => *n,
            other => panic!("tests use Int32 and Int64 operands, not {other:?}"),
        };
        w.upsert_inc_on(made.stamp, q, n, 0).expect("the increment writes");
    }
    w
}

/// The writer renders the inserted document (the order of the inc fields is the creation's, then
/// the increments': compare without order when there is an `$inc`).
fn creates(filter: Document, u: Document) -> (UpsertEffects, Document) {
    let (fx, post) = plan(filter, u);
    let got = render(&create(&fx));
    assert_eq!(got, post, "the writer creates another document");
    if fx.inc.is_empty() {
        let (mut a, mut b) = (Vec::new(), Vec::new());
        got.to_writer(&mut a).unwrap();
        post.to_writer(&mut b).unwrap();
        assert_eq!(a, b, "same fields, other order: {got} / {post}");
    }
    (fx, post)
}

#[test]
fn set_only_set_on_insert_only_and_the_filter_layer() {
    let (fx, post) = creates(doc! {"k": 5}, doc! {"$set": {"a": 1}});
    assert_eq!(fx.from_filter, vec![(p("k"), v(5))]);
    assert_eq!(fx.set, vec![(p("a"), v(1))]);
    assert!(fx.set_on_insert.is_empty() && fx.inc.is_empty());
    assert_eq!(post, doc! {"k": 5, "a": 1});

    let (fx, _) = creates(doc! {}, doc! {"$setOnInsert": {"s": 1}});
    assert_eq!(fx.set_on_insert, vec![(p("s"), v(1))]);
    assert!(fx.set.is_empty() && fx.from_filter.is_empty());

    let (fx, post) = creates(doc! {"k": 1}, doc! {"$set": {}});
    assert!(fx.set.is_empty());
    assert_eq!(post, doc! {"k": 1});
}

#[test]
fn a_set_overrides_the_filter_and_dotted_leaves_merge() {
    let (_, post) = creates(doc! {"a": 1}, doc! {"$set": {"a": 2}});
    assert_eq!(post, doc! {"a": 2});
    let (fx, post) = creates(doc! {"a.b": 1}, doc! {"$set": {"a.c": 2}});
    assert_eq!(fx.from_filter, vec![(np(&["a", "b"]), v(1))]);
    assert_eq!(fx.set, vec![(np(&["a", "c"]), v(2))]);
    assert_eq!(post, doc! {"a": {"b": 1, "c": 2}});
    // a whole-document filter value and a field set below it
    let (_, post) = creates(doc! {"a": {"x": 1}}, doc! {"$set": {"a.y": 2}});
    assert_eq!(post, doc! {"a": {"x": 1, "y": 2}});
}

/// PINNED TO THE ENGINE until the main PR (A)-(D) lands: the order of the listed new fields is the engine's, the reference sorts them. Flip this test with that PR.
#[test]
fn the_creation_values_of_the_register_operators_go_in_set() {
    let (fx, post) = creates(
        doc! {},
        doc! {
            "$mul": {"m": 3}, "$min": {"lo": 2}, "$max": {"hi": 7}, "$bit": {"b": {"or": 6}},
            "$push": {"arr": 1}, "$addToSet": {"set": "x"}, "$currentDate": {"d": true},
        },
    );
    assert!(fx.inc.is_empty() && fx.set_on_insert.is_empty());
    let want = vec![
        (p("m"), v(0)),
        (p("lo"), v(2)),
        (p("hi"), v(7)),
        (p("b"), v(6)),
        (p("arr"), v(vec![1])),
        (p("set"), v(vec!["x"])),
        (p("d"), Value::Date(NOW)),
    ];
    let mut got = fx.set.clone();
    got.sort_by(|a, b| a.0.cmp(&b.0));
    let mut want_sorted = want.clone();
    want_sorted.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(got, want_sorted);
    // listed in the order of the inserted document
    let order: Vec<&str> = post.keys().map(String::as_str).collect();
    let listed: Vec<String> = fx.set.iter().map(|(q, _)| q.name().to_string()).collect();
    assert_eq!(listed, order);
}

#[test]
fn inc_is_the_operand_and_several_incs_are_allowed() {
    let (fx, post) =
        creates(doc! {"k": 1}, doc! {"$inc": {"n": 2i64, "q.m": 3i64}, "$set": {"a": 1}});
    assert_eq!(fx.inc, vec![(p("n"), v(2i64)), (np(&["q", "m"]), v(3i64))]);
    assert_eq!(post.get("n"), Some(&Bson::Int64(2)));
    // typed operands as written
    let (fx, _) = plan(doc! {}, doc! {"$inc": {"a": 1, "b": 1.5, "c": 2i64}});
    assert_eq!(fx.inc, vec![(p("a"), v(1)), (p("b"), v(1.5)), (p("c"), v(2i64))]);
}

#[test]
fn an_unset_removes_the_filter_field_and_a_rename_moves_it() {
    let (fx, post) = creates(doc! {"a": 1, "b": 2}, doc! {"$unset": {"a": ""}});
    assert_eq!(fx.set, vec![(p("a"), Value::Unset)]);
    assert_eq!(post, doc! {"b": 2});
    let (fx, post) = creates(doc! {"a": 1}, doc! {"$rename": {"a": "b"}});
    assert_eq!(fx.set, vec![(p("a"), Value::Unset), (p("b"), v(1))]);
    assert_eq!(post, doc! {"b": 1});
}

/// PINNED TO THE ENGINE until the main PR (A)-(D) lands: the engine inserts new fields in the order of the update, the reference sorts them (the filter fields too). Flip this test with that PR.
#[test]
fn new_fields_are_in_the_order_of_the_inserted_document() {
    // $setOnInsert first in the document, then $set: the engine inserts the $setOnInsert fields
    // first whatever the order written
    let (fx, post) = creates(
        doc! {"k": 1},
        doc! {"$set": {"zz": 1, "aa": 2}, "$setOnInsert": {"s2": 3, "s1": 4}},
    );
    let keys: Vec<&str> = post.keys().map(String::as_str).collect();
    assert_eq!(keys, ["k", "s2", "s1", "zz", "aa"]);
    assert_eq!(fx.set_on_insert.len() + fx.set.len(), 4);
}

#[test]
fn the_filters_id_paths_are_left_out() {
    let (fx, _) = plan(doc! {"_id": 7, "k": 1}, doc! {"$set": {"a": 1}});
    assert_eq!(fx.from_filter, vec![(p("k"), v(1))]);
    let (fx, _) = plan(doc! {"_id.x": 7, "k": 1}, doc! {"$set": {"a": 1}});
    assert_eq!(fx.from_filter, vec![(p("k"), v(1))]);
}

#[test]
fn a_pipeline_upsert_sets_what_the_stages_changed() {
    let stages = vec![Bson::Document(doc! {"$set": {"t": 2, "k": 9}})];
    let update = update::parse_pipeline(&stages, &[]).unwrap();
    let UpsertPlan::Effects { effects, post_image } =
        plan_upsert(&update, &filt(doc! {"k": 1}), NOW).unwrap()
    else {
        panic!("effects")
    };
    assert_eq!(effects.from_filter, vec![(p("k"), v(1))]);
    assert_eq!(effects.set, vec![(p("k"), v(9)), (p("t"), v(2))]);
    assert_eq!(post_image, doc! {"k": 9, "t": 2});
    assert_eq!(render(&create(&effects)), post_image);
}

#[test]
fn a_replacement_upsert_is_not_an_effect_list() {
    let u: Update = update::parse(&doc! {"b": 2}).unwrap();
    assert!(matches!(
        plan_upsert(&u, &filt(doc! {"k": 1}), NOW).unwrap(),
        UpsertPlan::Replace { .. }
    ));
}

#[test]
fn the_refusals_of_the_creation_are_400s_at_plan_time() {
    // an $inc on a path the filter writes (a deviation, docs/deviations.md)
    assert!(refused(doc! {"a": 5}, doc! {"$inc": {"a": 1}}).contains("a"));
    assert!(refused(doc! {"a": {"b": 1}}, doc! {"$inc": {"a.c": 1}}).contains("a"));
    // below a scalar filter value
    assert!(!refused(doc! {"a": 5}, doc! {"$set": {"a.x": 1}}).is_empty());
    // a `$` on an insert is the engine's refusal
    assert!(refused(doc! {"k": 1}, doc! {"$set": {"items.$.x": 1}}).contains("$"));
    // two filter equalities on one path
    assert!(!refused(doc! {"$and": [{"a": 1}, {"a": 2}]}, doc! {"$set": {"z": 1}}).is_empty());
}

#[test]
fn a_filter_leaf_beside_an_inc_is_accepted() {
    let (fx, post) = creates(doc! {"a.b": 1}, doc! {"$inc": {"a.c": 1i64}});
    assert_eq!(fx.inc, vec![(np(&["a", "c"]), v(1i64))]);
    assert_eq!(post, doc! {"a": {"b": 1, "c": 1i64}});
}
