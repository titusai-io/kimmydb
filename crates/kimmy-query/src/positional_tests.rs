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
    // A `$nor` branch is false as a whole, though one of its clauses held.
    let f = doc! { "$nor": [ { "scores": { "$gte": 6 }, "items.sku": "nut" } ] };
    assert_eq!(at(f, &d), None);
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

// ---- the element is the one the matcher tested ------------------------------

/// The matcher goes one level into an array and no further: `{a: 1}` does not
/// match `{a: [[1, 2]]}`. The recorded element is the one it tested.
#[test]
fn the_matcher_does_not_look_inside_an_array_that_is_an_element() {
    assert!(!filter::matches(&filt(doc! { "a": 1 }), &doc! { "a": [[1, 2]] }).unwrap());
}

#[test]
fn an_element_that_is_an_array_is_not_the_one_a_scalar_condition_matched() {
    let d = doc! { "_id": 1, "tags": [["red", "blue"], "red"] };
    assert_eq!(at(doc! { "tags": "red" }, &d), Some(("tags".into(), 1)));
    let out = updated(doc! { "tags": "red" }, doc! { "$set": { "tags.$": "green" } }, vec![], &d);
    assert_eq!(out, doc! { "_id": 1, "tags": [["red", "blue"], "green"] });
}

/// Each element condition, over an array whose first element is an array that
/// holds a value the condition would accept and whose second is the match.
#[test]
fn each_element_condition_skips_an_array_element_holding_its_value() {
    let cases: Vec<(Document, Document)> = vec![
        (doc! { "a": [[1, 2], 1] }, doc! { "a": 1 }),
        (doc! { "a": [[1, 2], 1] }, doc! { "a": { "$eq": 1 } }),
        (doc! { "a": [[1, 2], 1] }, doc! { "a": { "$in": [1, 9] } }),
        (doc! { "a": [[7], 8] }, doc! { "a": { "$gt": 5 } }),
        (doc! { "a": [[7], 5] }, doc! { "a": { "$gte": 5 } }),
        (doc! { "a": [[3], 1] }, doc! { "a": { "$lt": 2 } }),
        (doc! { "a": [[1], 2] }, doc! { "a": { "$lte": 2 } }),
        (doc! { "a": [[7], 8] }, doc! { "a": { "$type": "int" } }),
        (doc! { "a": [["xa"], "xb"] }, doc! { "a": { "$regex": "^x" } }),
        (doc! { "a": [[4], 6] }, doc! { "a": { "$mod": [2, 0] } }),
        (doc! { "a": [[3], 3] }, doc! { "a": { "$bitsAllSet": 3 } }),
        (doc! { "a": [[3], 3] }, doc! { "a": { "$bitsAnySet": 1 } }),
        (doc! { "a": [[3], 4] }, doc! { "a": { "$bitsAllClear": 3 } }),
        (doc! { "a": [[7], 3] }, doc! { "a": { "$bitsAnyClear": 4 } }),
        (doc! { "a": [[null], null] }, doc! { "a": null }),
        (doc! { "a": [[1, 2], { "b": 1 }] }, doc! { "a": { "$elemMatch": { "b": 1 } } }),
        (doc! { "a": [[{ "b": 1 }], { "b": 1 }] }, doc! { "a.b": 1 }),
    ];
    for (mut d, f) in cases {
        d.insert("_id", 1);
        let parsed = filt(f.clone());
        assert!(filter::matches(&parsed, &d).unwrap(), "{f} must match {d}");
        let (_, found) = filter::matches_at(&parsed, &d).unwrap();
        assert_eq!(found.map(|p| (p.path, p.index)), Some(("a".into(), 1)), "{f} over {d}");
    }
}

/// Conditions that are not about one element record nothing, whatever the
/// array holds.
#[test]
fn conditions_that_are_not_about_an_element_record_nothing() {
    let d = doc! { "_id": 1, "a": [[1, 2], 1, 5] };
    for f in [
        doc! { "a": { "$nin": [9] } },
        doc! { "a": { "$ne": 9 } },
        doc! { "a": { "$size": 3 } },
        doc! { "a": { "$exists": true } },
        doc! { "a": { "$all": [1, 5] } },
        doc! { "a": { "$not": { "$gt": 99 } } },
    ] {
        assert_eq!(at(f.clone(), &d), None, "{f}");
    }
}

#[test]
fn a_find_projection_returns_the_element_the_matcher_tested() {
    let d = doc! { "_id": 1, "tags": [["red", "blue"], "red"], "n": 1 };
    let p = projection(doc! { "tags.$": 1 });
    assert_eq!(
        p.project(&filt(doc! { "tags": "red" }), &d).unwrap(),
        doc! { "_id": 1, "tags": ["red"] }
    );
}

/// The oracle: the first index `i` for which the filter matches the document
/// with the array replaced by `[array[i]]` alone.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn generated_element(r: &mut Rng, depth: u32, nested: bool) -> Bson {
    let mut pick = r.below(if depth == 0 { 6 } else { 3 });
    if !nested && depth == 0 && pick == 3 {
        pick = 4;
    }
    match pick {
        0 | 2 => Bson::Int32(r.below(4) as i32),
        1 => Bson::String(["x", "y"][r.below(2) as usize].into()),
        3 => {
            Bson::Array((0..r.below(3)).map(|_| generated_element(r, depth + 1, nested)).collect())
        }
        _ => {
            let mut d = Document::new();
            if r.below(3) > 0 {
                d.insert("b", generated_element(r, depth + 1, nested));
            }
            if r.below(2) > 0 {
                let c = (0..r.below(3)).map(|_| Bson::Int32(r.below(4) as i32));
                d.insert("c", Bson::Array(c.collect()));
            }
            Bson::Document(d)
        }
    }
}

fn generated_filter(r: &mut Rng) -> Document {
    let v = Bson::Int32(r.below(4) as i32);
    match r.below(16) {
        0 => doc! { "a": v },
        1 => doc! { "a": { "$gt": v } },
        2 => doc! { "a": { "$in": [v, "y"] } },
        3 => {
            let kind = ["int", "string", "object"][r.below(3) as usize];
            doc! { "a": { "$type": kind } }
        }
        4 => doc! { "a.b": v },
        5 => doc! { "a.b": { "$gte": v } },
        6 => doc! { "a.c": v },
        7 => doc! { "a": { "$elemMatch": { "b": v } } },
        8 => doc! { "a": { "$elemMatch": { "$gt": v } } },
        9 => doc! { "a": { "$regex": "^x" } },
        10 => doc! { "a": { "$lt": v } },
        11 => doc! { "a": { "$lte": v } },
        12 => doc! { "a": { "$mod": [2, r.below(2) as i32] } },
        13 => doc! { "a": { "$bitsAllSet": r.below(4) as i32 } },
        14 => doc! { "a": { "$bitsAnyClear": r.below(4) as i32 } },
        _ => doc! { "a": { "$eq": v } },
    }
}

fn differential(nested: bool) -> (usize, Vec<String>) {
    let mut r = Rng(0x2710_5eed);
    let (mut checked, mut wrong) = (0, Vec::new());
    for _ in 0..20000 {
        let array: Vec<Bson> =
            (0..1 + r.below(4)).map(|_| generated_element(&mut r, 0, nested)).collect();
        let d = doc! { "_id": 1, "a": array.clone() };
        let f = generated_filter(&mut r);
        let parsed = filt(f.clone());
        if !filter::matches(&parsed, &d).unwrap() {
            continue;
        }
        checked += 1;
        let oracle = (0..array.len()).find(|&i| {
            let single = doc! { "_id": 1, "a": [array[i].clone()] };
            filter::matches(&parsed, &single).unwrap()
        });
        let (_, got) = filter::matches_at(&parsed, &d).unwrap();
        let got = got.map(|p| (p.path, p.index));
        let want = oracle.map(|i| ("a".to_string(), i));
        if got != want {
            wrong.push(format!("{f} over {d}: recorded {got:?}, oracle {want:?}"));
        }
    }
    (checked, wrong)
}

/// The recorded element is the first one for which the matcher, given the
/// array with that element alone, answers yes. Over arrays that hold arrays
/// and arrays that do not, for every element condition.
#[test]
fn the_recorded_element_is_the_first_the_matcher_accepts_alone() {
    for nested in [true, false] {
        let (checked, wrong) = differential(nested);
        assert!(checked > 3000, "nested {nested}: only {checked} cases matched");
        assert!(
            wrong.is_empty(),
            "nested {nested}: {} of {checked} disagree: {:?}",
            wrong.len(),
            &wrong[..wrong.len().min(5)]
        );
    }
}

// ---- written order -----------------------------------------------------------

#[test]
fn the_last_array_is_the_last_written_even_when_a_clause_holds_an_expr() {
    let d = doc! { "_id": 1, "items": [{ "q": 5, "m": 1 }], "tags": ["x", "y"] };
    let items_first = doc! {
        "items": { "$elemMatch": { "$expr": { "$gt": ["$q", "$m"] } } },
        "tags": "y",
    };
    assert_eq!(at(items_first, &d), Some(("tags".into(), 1)));
    let tags_first = doc! {
        "tags": "y",
        "items": { "$elemMatch": { "$expr": { "$gt": ["$q", "$m"] } } },
    };
    assert_eq!(at(tags_first, &d), Some(("items".into(), 0)));
    let in_and = doc! { "$and": [
        { "items": { "$elemMatch": { "$expr": { "$gt": ["$q", "$m"] } } } },
        { "tags": "y" },
    ] };
    assert_eq!(at(in_and, &d), Some(("tags".into(), 1)));
}

#[test]
fn an_or_records_from_the_first_branch_written_that_holds_even_with_an_expr() {
    let d = doc! { "_id": 1, "items": [{ "sku": "a" }] };
    let expr_first = doc! { "$or": [ { "$expr": { "$eq": [1, 1] } }, { "items.sku": "a" } ] };
    assert_eq!(at(expr_first, &d), None);
    let expr_last = doc! { "$or": [ { "items.sku": "a" }, { "$expr": { "$eq": [1, 1] } } ] };
    assert_eq!(at(expr_last, &d), Some(("items".into(), 0)));
}

// ---- `$` after an array-positional segment -----------------------------------

#[test]
fn dollar_after_an_array_positional_segment_is_refused_at_parse() {
    for path in ["items.$[].tags.$", "items.$[line].tags.$", "a.$[].b.$[x].c.$"] {
        let err = update::parse_with_filters(
            &doc! { "$set": { path: 1 } },
            &[doc! { "line.k": 1 }, doc! { "x.k": 1 }],
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("must come before"), "{path}: {err}");
    }
    // `$` before `$[]` is accepted.
    assert!(update::parse(&doc! { "$set": { "orders.$.lines.$[].done": true } }).is_ok());
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

// ---- the answer of a findAndModify --------------------------------------------

#[test]
fn the_image_after_an_in_place_change_is_cut_at_the_matched_index() {
    let p = projection(doc! { "items.$": 1 });
    let f = filt(doc! { "items.sku": "gasket" });
    let before = order();
    let after = updated(
        doc! { "items.sku": "gasket" },
        doc! { "$set": { "items.$.sku": "washer" } },
        vec![],
        &before,
    );
    // The element no longer satisfies the filter, and is still the one named.
    let out = p.project_returned(&f, &before, &after).unwrap();
    assert_eq!(out.get_array("items").unwrap().len(), 1);
    assert_eq!(
        out.get_array("items").unwrap()[0].as_document().unwrap().get_str("sku").ok(),
        Some("washer")
    );
    let same = p.project_returned(&f, &before, &before).unwrap();
    assert_eq!(
        same.get_array("items").unwrap()[0].as_document().unwrap().get_i32("qty").ok(),
        Some(2)
    );
}

#[test]
fn the_image_after_is_refused_for_an_update_that_writes_outside_the_matched_element() {
    let p = projection(doc! { "items.$": 1 });
    for (update, written) in [
        (doc! { "$set": { "items": [ { "sku": "a" } ] } }, "items"),
        (doc! { "$unset": { "items": "" } }, "items"),
        (doc! { "$rename": { "items": "lines" } }, "items"),
        (doc! { "$rename": { "other": "items" } }, "items"),
        (doc! { "$push": { "items": { "sku": "nut" } } }, "items"),
        (doc! { "$push": { "items": { "$each": [], "$sort": { "sku": -1 } } } }, "items"),
        (doc! { "$pull": { "items": { "sku": "bolt" } } }, "items"),
        (doc! { "$pop": { "items": 1 } }, "items"),
        (doc! { "$addToSet": { "items": { "sku": "nut" } } }, "items"),
        (doc! { "$set": { "items.0.qty": 9 } }, "items.0.qty"),
        (doc! { "$set": { "items.$[].qty": 9 } }, "items.$[].qty"),
        (doc! { "$set": { "items.$[f].qty": 9 } }, "items.$[f].qty"),
        (doc! { "$set": { "items.$.qty": 1, "items.1.qty": 9 } }, "items.1.qty"),
    ] {
        let filters =
            if written.contains("$[f]") { vec![doc! { "f.qty": { "$gte": 0 } }] } else { vec![] };
        let parsed = update::parse_with_filters(&update, &filters).unwrap();
        let err = p.check_update(&parsed).expect_err(&format!("{update}")).to_string();
        assert!(err.contains(&format!("writes {written:?}")), "{update}: {err}");
    }
    // A replacement document writes everything.
    let replace = update::parse(&doc! { "items": [] }).unwrap();
    assert!(p.check_update(&replace).is_err());
}

#[test]
fn the_image_after_is_allowed_for_an_update_confined_to_the_matched_element() {
    let p = projection(doc! { "items.$": 1 });
    for update in [
        doc! { "$inc": { "items.$.qty": 1 } },
        doc! { "$set": { "items.$": { "sku": "x" } } },
        doc! { "$set": { "items.$.sku": "washer" }, "$unset": { "note": "" } },
        doc! { "$set": { "itemsx": 1, "item": 2, "scores": [1] } },
    ] {
        let parsed = update::parse(&update).unwrap();
        p.check_update(&parsed).unwrap_or_else(|e| panic!("{update}: {e}"));
    }
}

#[test]
fn the_image_after_is_refused_for_a_parent_of_a_nested_array() {
    let p = projection(doc! { "a.items.$": 1 });
    for (update, ok) in [
        (doc! { "$set": { "a": { "items": [] } } }, false),
        (doc! { "$unset": { "a": "" } }, false),
        (doc! { "$set": { "a.items": [] } }, false),
        (doc! { "$set": { "a.items.$.qty": 1 } }, true),
        (doc! { "$set": { "a.other": 1 } }, true),
        (doc! { "$set": { "b": 1 } }, true),
    ] {
        let parsed = update::parse(&update).unwrap();
        assert_eq!(p.check_update(&parsed).is_ok(), ok, "{update}");
    }
}

#[test]
fn an_index_past_the_end_is_refused_and_never_the_whole_array() {
    let p = projection(doc! { "items.$": 1 });
    let f = filt(doc! { "items.sku": "gasket" });
    let before = order();
    for after in [
        doc! { "_id": 7, "items": [ { "sku": "a" } ] },
        doc! { "_id": 7, "items": 5 },
        doc! { "_id": 7 },
    ] {
        let err = p.project_returned(&f, &before, &after).expect_err(&format!("{after}"));
        assert!(err.to_string().contains("found no element"), "{after}: {err}");
    }
}

#[test]
fn a_whole_array_operand_equals_a_nested_array_element_as_a_whole() {
    let d = doc! { "a": [ [1, 2], 3 ] };
    assert_eq!(at(doc! { "a": [1, 2] }, &d), Some(("a".to_string(), 0)));
}
