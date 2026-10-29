//! The positional `$` of an update path and of a find projection (ADR-256):
//! which array, which element, and what is refused.

use bson::{Bson, Document, doc};

use crate::filter::{self, ArrayPosition, Filter};
use crate::shape;
use crate::update;

const NOW: i64 = 0;

fn filt(f: Document) -> Filter {
    filter::parse(&f).unwrap_or_else(|e| panic!("{f}: {e}"))
}

/// Where `f` matched in `d`, as the array path and index.
fn at(f: Document, d: &Document) -> Option<(String, usize)> {
    let (hit, found) = filter::matches_at(&filt(f.clone()), d).unwrap();
    assert!(hit, "{f} must match {d}");
    found.map(|ArrayPosition { path, index }| (path, index))
}

fn updated(f: Document, u: Document, filters: Vec<Document>, d: &Document) -> Document {
    try_update(f, u, filters, d).unwrap_or_else(|e| panic!("refused: {e}"))
}

fn try_update(
    f: Document,
    u: Document,
    filters: Vec<Document>,
    d: &Document,
) -> Result<Document, String> {
    let parsed = update::parse_with_filters(&u, &filters).map_err(|e| e.to_string())?;
    let mut out = d.clone();
    update::apply_matched(&parsed, &filt(f), &mut out, NOW).map_err(|e| e.to_string())?;
    Ok(out)
}

fn refused(f: Document, u: Document, d: &Document) -> String {
    try_update(f, u, vec![], d).expect_err("must be refused")
}

fn order() -> Document {
    doc! {
        "_id": 7,
        "items": [
            { "sku": "bolt", "qty": 1, "shipped": false },
            { "sku": "gasket", "qty": 2, "shipped": false },
            { "sku": "gasket", "qty": 3, "shipped": false },
        ],
        "scores": [3, 8, 9, 2],
    }
}

fn shipped(d: &Document) -> Vec<Option<bool>> {
    d.get_array("items")
        .unwrap()
        .iter()
        .map(|i| i.as_document().unwrap().get_bool("shipped").ok())
        .collect()
}

// ---- the element ----------------------------------------------------------

#[test]
fn a_dotted_equality_sets_the_first_matching_element_only() {
    let out = updated(
        doc! { "_id": 7, "items.sku": "gasket" },
        doc! { "$set": { "items.$.shipped": true } },
        vec![],
        &order(),
    );
    assert_eq!(shipped(&out), [Some(false), Some(true), Some(false)]);
}

#[test]
fn an_array_filter_reaches_every_matching_element_where_dollar_reaches_one() {
    let out = updated(
        doc! { "items.sku": "gasket" },
        doc! { "$set": { "items.$[line].shipped": true } },
        vec![doc! { "line.sku": "gasket" }],
        &order(),
    );
    assert_eq!(shipped(&out), [Some(false), Some(true), Some(true)]);
}

#[test]
fn elem_match_names_the_element_satisfying_the_sub_filter() {
    let f = doc! { "items": { "$elemMatch": { "sku": "gasket", "qty": { "$gt": 2 } } } };
    assert_eq!(at(f.clone(), &order()), Some(("items".into(), 2)));
    let out = updated(f, doc! { "$inc": { "items.$.qty": 10 } }, vec![], &order());
    assert_eq!(
        out.get_array("items").unwrap()[2].as_document().unwrap().get_i32("qty").ok(),
        Some(13)
    );
}

#[test]
fn a_scalar_array_names_the_first_element_satisfying_the_comparison() {
    assert_eq!(at(doc! { "scores": { "$gte": 6 } }, &order()), Some(("scores".into(), 1)));
    let out = updated(
        doc! { "scores": { "$gte": 6 } },
        doc! { "$set": { "scores.$": 0 } },
        vec![],
        &order(),
    );
    assert_eq!(
        out.get_array("scores").unwrap(),
        &vec![Bson::Int32(3), 0.into(), 9.into(), 2.into()]
    );
}

#[test]
fn an_equality_on_a_scalar_array_and_in_name_an_element() {
    assert_eq!(at(doc! { "scores": 9 }, &order()), Some(("scores".into(), 2)));
    assert_eq!(at(doc! { "scores": { "$in": [2, 8] } }, &order()), Some(("scores".into(), 1)));
}

#[test]
fn the_last_array_the_filter_matched_on_wins() {
    let d = order();
    let scores_then_items = doc! { "scores": { "$gte": 6 }, "items.sku": "gasket" };
    assert_eq!(at(scores_then_items, &d), Some(("items".into(), 1)));
    let items_then_scores = doc! { "items.sku": "gasket", "scores": { "$gte": 6 } };
    assert_eq!(at(items_then_scores, &d), Some(("scores".into(), 1)));
    let and = doc! { "$and": [ { "items.sku": "bolt" }, { "scores": 9 }, { "items.qty": 3 } ] };
    assert_eq!(at(and, &d), Some(("items".into(), 2)));
}

#[test]
fn several_conditions_on_one_array_leave_the_first_element_of_the_last_one() {
    // The condition written last decides: the first element it holds for.
    let d = doc! { "_id": 1, "s": [1, 5, 9, 3] };
    assert_eq!(at(doc! { "s": { "$lt": 9, "$gt": 2 } }, &d), Some(("s".into(), 1)));
    assert_eq!(at(doc! { "s": { "$gt": 2, "$lt": 9 } }, &d), Some(("s".into(), 0)));
    assert_eq!(at(doc! { "s": { "$gt": 4, "$lt": 4 } }, &d), Some(("s".into(), 0)));
    assert_eq!(
        at(doc! { "$and": [ { "s": { "$gt": 4 } }, { "s": { "$lt": 4 } } ] }, &d),
        Some(("s".into(), 0))
    );
}

#[test]
fn a_nested_array_is_addressed_by_its_outermost_array() {
    let d = doc! {
        "_id": 1,
        "orders": [
            { "id": 1, "lines": [ { "sku": "a", "done": false } ] },
            { "id": 2, "lines": [ { "sku": "x", "done": false }, { "sku": "y", "done": false } ] },
        ],
    };
    let f = doc! { "orders.lines.sku": "y" };
    assert_eq!(at(f.clone(), &d), Some(("orders".into(), 1)));
    let out = updated(
        f.clone(),
        doc! { "$set": { "orders.$.seen": true }, "$unset": { "orders.$.id": "" } },
        vec![],
        &d,
    );
    let second = out.get_array("orders").unwrap()[1].as_document().unwrap().clone();
    assert_eq!(second.get_bool("seen").ok(), Some(true));
    assert!(!second.contains_key("id"));
    // `$[]` below the matched order reaches each of its lines.
    let out = updated(f, doc! { "$set": { "orders.$.lines.$[].done": true } }, vec![], &d);
    let lines = out.get_array("orders").unwrap()[1]
        .as_document()
        .unwrap()
        .get_array("lines")
        .unwrap()
        .clone();
    assert!(lines.iter().all(|l| l.as_document().unwrap().get_bool("done").ok() == Some(true)));
    let first =
        out.get_array("orders").unwrap()[0].as_document().unwrap().get_array("lines").unwrap()[0]
            .clone();
    assert_eq!(first.as_document().unwrap().get_bool("done").ok(), Some(false));
}

#[test]
fn dollar_combines_with_array_filters_in_one_update() {
    let out = updated(
        doc! { "items.sku": "gasket" },
        doc! { "$set": { "items.$.shipped": true, "items.$[big].big": true } },
        vec![doc! { "big.qty": { "$gte": 3 } }],
        &order(),
    );
    assert_eq!(shipped(&out), [Some(false), Some(true), Some(false)]);
    assert_eq!(
        out.get_array("items").unwrap()[2].as_document().unwrap().get_bool("big").ok(),
        Some(true)
    );
}

#[test]
fn an_or_records_from_the_first_branch_that_holds() {
    let d = order();
    let f = doc! { "$or": [ { "items.sku": "nut" }, { "scores": 9 }, { "items.sku": "bolt" } ] };
    assert_eq!(at(f, &d), Some(("scores".into(), 2)));
    let f = doc! { "$or": [ { "items.sku": "gasket" }, { "scores": 9 } ] };
    assert_eq!(at(f, &d), Some(("items".into(), 1)));
}

#[test]
fn nor_not_ne_and_nin_record_nothing() {
    let d = order();
    assert_eq!(at(doc! { "$nor": [ { "items.sku": "nut" } ] }, &d), None);
    assert_eq!(at(doc! { "scores": { "$not": { "$gt": 100 } } }, &d), None);
    assert_eq!(at(doc! { "items.sku": { "$ne": "nut" } }, &d), None);
    assert_eq!(at(doc! { "scores": { "$nin": [100] } }, &d), None);
    // A positive clause beside them still records.
    assert_eq!(
        at(doc! { "scores": { "$nin": [100] }, "items.sku": "bolt" }, &d),
        Some(("items".into(), 0))
    );
}

// ---- refusals -------------------------------------------------------------

#[test]
fn a_filter_that_matched_no_array_leaves_dollar_nothing_to_name() {
    let err = refused(doc! { "_id": 7 }, doc! { "$set": { "items.$.shipped": true } }, &order());
    assert!(err.contains("matched an element of an array"), "{err}");
    let err =
        refused(doc! { "scores": { "$size": 4 } }, doc! { "$set": { "scores.$": 1 } }, &order());
    assert!(err.contains("matched an element of an array"), "{err}");
}

#[test]
fn dollar_must_follow_the_array_the_filter_matched_on() {
    let err = refused(
        doc! { "scores": { "$gte": 6 } },
        doc! { "$set": { "items.$.shipped": true } },
        &order(),
    );
    assert!(
        err.contains("the update path must go through the array the filter matched on"),
        "{err}"
    );
}

#[test]
fn an_upsert_that_inserts_has_no_element_for_dollar() {
    let parsed = update::parse(&doc! { "$set": { "items.$.shipped": true } }).unwrap();
    let mut seed = doc! {};
    let err = update::apply_on_insert(&parsed, &mut seed, NOW).unwrap_err().to_string();
    assert!(err.contains("upsert"), "{err}");
}

#[test]
fn one_dollar_to_a_path_and_never_first() {
    let err =
        refused(doc! { "items.sku": "bolt" }, doc! { "$set": { "items.$.a.$.b": 1 } }, &order());
    assert!(err.contains("more than one"), "{err}");
    let err = refused(doc! { "items.sku": "bolt" }, doc! { "$set": { "$.a": 1 } }, &order());
    assert!(err.contains("begins with a positional"), "{err}");
}

#[test]
fn rename_refuses_dollar() {
    let err = refused(
        doc! { "items.sku": "bolt" },
        doc! { "$rename": { "items.$.sku": "code" } },
        &order(),
    );
    assert!(err.contains("$rename"), "{err}");
}

#[test]
fn an_explicit_index_in_the_filter_records_no_element() {
    assert_eq!(at(doc! { "items.0.sku": "bolt" }, &order()), None);
}

#[test]
fn every_update_operator_resolves_dollar() {
    let d = doc! { "_id": 1, "a": [ { "k": "x", "n": 2, "t": [1], "m": 5 }, { "k": "x", "n": 2, "t": [1], "m": 5 } ] };
    let f = || doc! { "a.k": "x" };
    let first = |out: &Document| out.get_array("a").unwrap()[0].as_document().unwrap().clone();
    let second = |out: &Document| out.get_array("a").unwrap()[1].as_document().unwrap().clone();
    let out = updated(
        f(),
        doc! {
            "$mul": { "a.$.n": 3 }, "$min": { "a.$.m": 1 }, "$max": { "a.$.q": 4 },
            "$push": { "a.$.t": 2 }, "$addToSet": { "a.$.u": 1 }, "$bit": { "a.$.b": { "or": 3 } },
            "$currentDate": { "a.$.d": true }, "$unset": { "a.$.k": "" },
        },
        vec![],
        &d,
    );
    let one = first(&out);
    assert_eq!(one.get_i32("n").ok(), Some(6));
    assert_eq!(one.get_i32("m").ok(), Some(1));
    assert_eq!(one.get_i32("q").ok(), Some(4));
    assert_eq!(one.get_array("t").unwrap().len(), 2);
    assert_eq!(one.get_array("u").unwrap().len(), 1);
    assert_eq!(one.get_i32("b").ok(), Some(3));
    assert!(one.contains_key("d") && !one.contains_key("k"));
    assert_eq!(second(&out), d.get_array("a").unwrap()[1].as_document().unwrap().clone());
    let out = updated(f(), doc! { "$pull": { "a.$.t": 1 } }, vec![], &d);
    assert!(first(&out).get_array("t").unwrap().is_empty());
}

#[test]
fn each_document_has_its_own_position() {
    let a = doc! { "_id": 1, "items": [ { "sku": "x", "f": 0 }, { "sku": "g", "f": 0 } ] };
    let b = doc! { "_id": 2, "items": [ { "sku": "g", "f": 0 }, { "sku": "x", "f": 0 } ] };
    let u = update::parse(&doc! { "$set": { "items.$.f": 1 } }).unwrap();
    let f = filt(doc! { "items.sku": "g" });
    let (mut a, mut b) = (a, b);
    update::apply_matched(&u, &f, &mut a, NOW).unwrap();
    update::apply_matched(&u, &f, &mut b, NOW).unwrap();
    assert_eq!(a.get_array("items").unwrap()[1].as_document().unwrap().get_i32("f").ok(), Some(1));
    assert_eq!(b.get_array("items").unwrap()[0].as_document().unwrap().get_i32("f").ok(), Some(1));
}

// ---- the matcher is untouched ----------------------------------------------

#[test]
fn the_recorder_never_changes_the_yes_or_no() {
    let filters: Vec<Document> = vec![
        doc! {},
        doc! { "a": 1 },
        doc! { "s": { "$gte": 4 } },
        doc! { "s": { "$gt": 1, "$lt": 4 } },
        doc! { "items.sku": "g" },
        doc! { "items.sku": { "$ne": "g" } },
        doc! { "items": { "$elemMatch": { "sku": "g", "qty": { "$gt": 1 } } } },
        doc! { "items": { "$elemMatch": { "sku": "z" } } },
        doc! { "s": { "$elemMatch": { "$gt": 3 } } },
        doc! { "$or": [ { "a": 9 }, { "s": 5 } ] },
        doc! { "$nor": [ { "a": 1 } ] },
        doc! { "$and": [ { "s": 1 }, { "items.qty": 2 } ] },
        doc! { "s": { "$not": { "$gt": 4 } } },
        doc! { "s": { "$size": 3 } },
        doc! { "s": { "$all": [1, 5] } },
        doc! { "x": { "$exists": false } },
        doc! { "t": { "$type": "string" } },
        doc! { "t": { "$regex": "^ab" } },
        doc! { "s": { "$mod": [2, 1] } },
        doc! { "s": { "$bitsAllSet": 1 } },
        doc! { "s": [1, 5, 3] },
        doc! { "$expr": { "$gt": ["$a", 0] } },
        doc! { "$expr": { "$gt": ["$t", 0] }, "a": 7 },
        doc! { "items.0.sku": "g" },
        doc! { "s": { "$in": [5, 100] } },
        doc! { "s": null },
    ];
    let docs: Vec<Document> = vec![
        doc! {},
        doc! { "a": 1 },
        doc! { "a": 1, "s": [1, 5, 3], "t": "abc", "items": [ { "sku": "g", "qty": 2 }, { "sku": "h", "qty": 1 } ] },
        doc! { "a": 7, "s": [4], "t": 5, "items": [ { "sku": "z", "qty": 9 } ] },
        doc! { "s": [], "items": [] },
        doc! { "s": 5, "items": { "sku": "g", "qty": 2 } },
        doc! { "s": [[1, 2], [5]], "items": [ 3, { "sku": "g" } ] },
        doc! { "a": 0, "s": [2, 4, 6], "t": "xabc", "items": [ { "qty": 2 }, { "sku": "g", "qty": 2 } ] },
    ];
    let mut with_position = 0;
    for f in &filters {
        let parsed = filt(f.clone());
        for d in &docs {
            let plain = filter::matches(&parsed, d);
            let recorded = filter::matches_at(&parsed, d);
            match (plain, recorded) {
                (Ok(yes), Ok((also, found))) => {
                    assert_eq!(yes, also, "{f} over {d}");
                    assert!(also || found.is_none(), "{f} over {d}: a position without a match");
                    with_position += usize::from(found.is_some());
                }
                (Err(a), Err(b)) => assert_eq!(a.to_string(), b.to_string(), "{f} over {d}"),
                (a, b) => panic!("{f} over {d}: {a:?} against {b:?}"),
            }
        }
    }
    assert!(with_position > 10, "the table must exercise recorded positions: {with_position}");
}

// ---- find projection --------------------------------------------------------

fn projection(spec: Document) -> shape::FindProjection {
    shape::parse_find_projection(&spec).unwrap().expect("a projection")
}

fn projection_refusal(spec: Document) -> String {
    shape::parse_find_projection(&spec).expect_err("must be refused").to_string()
}

#[test]
fn a_positional_projection_keeps_only_the_matched_element() {
    let p = projection(doc! { "items.$": 1 });
    let out = p.project(&filt(doc! { "items.sku": "gasket" }), &order()).unwrap();
    assert_eq!(
        out,
        doc! { "_id": 7, "items": [ { "sku": "gasket", "qty": 2, "shipped": false } ] }
    );
}

#[test]
fn other_inclusions_and_id_behave_as_in_an_inclusion_projection() {
    let p = projection(doc! { "items.$": 1, "scores": 1, "_id": 0 });
    let out = p.project(&filt(doc! { "items.sku": "bolt" }), &order()).unwrap();
    assert_eq!(
        out,
        doc! { "items": [ { "sku": "bolt", "qty": 1, "shipped": false } ], "scores": [3, 8, 9, 2] }
    );
}

#[test]
fn a_positional_projection_follows_the_last_array_rule() {
    let p = projection(doc! { "scores.$": 1 });
    let out =
        p.project(&filt(doc! { "items.sku": "bolt", "scores": { "$gte": 8 } }), &order()).unwrap();
    assert_eq!(out, doc! { "_id": 7, "scores": [8] });
    let err = p
        .project(&filt(doc! { "scores": { "$gte": 8 }, "items.sku": "bolt" }), &order())
        .unwrap_err()
        .to_string();
    assert!(err.contains("did not match on"), "{err}");
}

#[test]
fn a_positional_projection_needs_a_matched_array() {
    let p = projection(doc! { "items.$": 1 });
    let err = p.project(&filt(doc! { "_id": 7 }), &order()).unwrap_err().to_string();
    assert!(err.contains("matched an element"), "{err}");
}

#[test]
fn a_projection_without_dollar_is_the_ordinary_one() {
    let p = projection(doc! { "scores": 1 });
    assert!(!p.is_positional());
    assert_eq!(p.plain(), shape::parse_projection(&doc! { "scores": 1 }).unwrap().as_ref());
    assert!(shape::parse_find_projection(&doc! {}).unwrap().is_none());
}

#[test]
fn the_positional_projection_refusals() {
    for (spec, words) in [
        (doc! { "items.$": 0 }, "must be 1"),
        (doc! { "items.$": false }, "must be 1"),
        (doc! { "items.$.sku": 1 }, "last segment"),
        (doc! { "$": 1 }, "follow the path of an array"),
        (doc! { "items.$": 1, "scores.$": 1 }, "one `$` entry"),
        (doc! { "items.$": 1, "x": 0 }, "mix inclusion and exclusion"),
        (doc! { "items.$": 1, "items": 1 }, "named by another"),
        (doc! { "items.$": 1, "items.sku": 1 }, "named by another"),
        (doc! { "items.$": 1, "items": { "$slice": 1 } }, "named by another"),
        (doc! { "items.$": 1, "items": { "$elemMatch": { "a": 1 } } }, "named by another"),
        (doc! { "a.$.b.$": 1 }, "last segment"),
    ] {
        let err = projection_refusal(spec.clone());
        assert!(err.contains(words), "{spec}: {err}");
    }
}

#[test]
fn project_stages_still_read_dollar_as_a_field_name() {
    let p = shape::parse_projection(&doc! { "items.$": 1 }).unwrap();
    let d = doc! { "_id": 1, "items": [ { "$": 5, "z": 1 }, 3 ] };
    assert_eq!(shape::project(p.as_ref(), &d), doc! { "_id": 1, "items": [ { "$": 5 } ] });
    let _ = Bson::Null;
}
