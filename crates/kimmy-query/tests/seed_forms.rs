//! Which filter forms seed an upsert, as recorded in `q7s2g.out` (cases g4.1 to
//! g4.12, reference 6.0.5, a filter that matches nothing, update `{$set: {z: 1}}`).
//!
//! Not recorded, so not seeded here: a `$in` or an `$all` of one regular
//! expression, and an `$all` with an `$elemMatch` (UNRECORDED). Both are refused
//! when the filter is parsed, so no seed is asked of them. Also UNRECORDED: an
//! array or a document as the single `$all` member, and `null` (`{a: null}`,
//! `$eq: null`); the build seeds the value as given.

use bson::{Document, doc};
use kimmy_query::filter;

use kimmy_query::seed::implied_equalities;

fn seeded(filter_doc: Document) -> Result<String, String> {
    let parsed = filter::parse(&filter_doc).expect("a filter");
    let mut out = Document::new();
    implied_equalities(&parsed, &mut out, &mut Vec::new())?;
    Ok(out.to_string())
}

#[test]
fn g4_1_an_eq_seeds() {
    assert_eq!(seeded(doc! { "a": { "$eq": 5 } }).unwrap(), doc! { "a": 5 }.to_string());
}

#[test]
fn g4_2_an_in_of_one_value_seeds() {
    assert_eq!(seeded(doc! { "a": { "$in": [5] } }).unwrap(), doc! { "a": 5 }.to_string());
}

#[test]
fn g4_3_an_in_of_two_values_seeds_nothing() {
    assert_eq!(seeded(doc! { "a": { "$in": [5, 6] } }).unwrap(), "{}");
}

#[test]
fn g4_4_an_all_of_one_value_seeds() {
    assert_eq!(seeded(doc! { "a": { "$all": [5] } }).unwrap(), doc! { "a": 5 }.to_string());
}

#[test]
fn g4_5_an_or_of_one_branch_seeds() {
    assert_eq!(seeded(doc! { "$or": [{ "a": 5 }] }).unwrap(), doc! { "a": 5 }.to_string());
}

#[test]
fn g4_6_an_or_of_two_branches_seeds_nothing() {
    assert_eq!(seeded(doc! { "$or": [{ "a": 5 }, { "b": 6 }] }).unwrap(), "{}");
}

#[test]
fn g4_7_and_g4_8_an_and_seeds_each_of_its_forms() {
    let want = doc! { "a": 5, "b": 6 }.to_string();
    assert_eq!(seeded(doc! { "$and": [{ "a": 5 }, { "b": 6 }] }).unwrap(), want);
    assert_eq!(seeded(doc! { "$and": [{ "a": 5 }, { "$or": [{ "b": 6 }] }] }).unwrap(), want);
    // The same through `$in` and `$all` of one value, and an `$or` in an `$or`.
    assert_eq!(
        seeded(doc! { "$and": [{ "a": { "$in": [5] } }, { "b": { "$all": [6] } }] }).unwrap(),
        want
    );
    assert_eq!(seeded(doc! { "$or": [{ "$or": [{ "a": 5 }] }], "b": 6 }).unwrap(), want);
}

#[test]
fn g4_9_a_range_that_pins_one_value_seeds_nothing() {
    assert_eq!(seeded(doc! { "a": { "$gte": 5, "$lte": 5 } }).unwrap(), "{}");
}

#[test]
fn g4_10_a_dotted_eq_seeds_a_nested_document() {
    assert_eq!(seeded(doc! { "a.b": { "$eq": 1 } }).unwrap(), doc! { "a": { "b": 1 } }.to_string());
}

#[test]
fn g4_11_and_g4_12_an_overlap_is_refused_through_every_form() {
    let both = "both paths 'a.b' and 'a' are matched";
    for filter_doc in [
        doc! { "a": { "$eq": 5 }, "a.b": 1 },
        doc! { "a": { "$in": [5] }, "a.b": 1 },
        doc! { "a": { "$all": [5] }, "a.b": 1 },
        doc! { "$or": [{ "a": 5 }], "a.b": 1 },
        doc! { "a": { "$in": [5] }, "a.b": { "$all": [1] } },
    ] {
        let err = seeded(filter_doc.clone()).unwrap_err();
        assert!(err.contains(both), "{filter_doc}: {err}");
    }
    // The same path twice, through two forms.
    let err = seeded(doc! { "a": { "$in": [5] }, "$and": [{ "a": 6 }] }).unwrap_err();
    assert!(err.contains("path 'a' is matched twice"), "{err}");
    // A form that seeds nothing cannot overlap.
    seeded(doc! { "a": { "$in": [5, 6] }, "a.b": 1 }).unwrap();
}
