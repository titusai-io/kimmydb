//! The path resolver rule (q7s3-resolver-rule.md) as the filter, a partial index and the planner
//! read it: null through arrays, positions, and what an index may answer.
//!
//! `resolver_cases.rs` is generated from the recordings q7s3f, q7s3g and q7s3j: each case
//! names its label (the set and the case number, which is the line of the set's `.jsonl`) and
//! the line of the case in the set's `.out`.

use super::common::{assert_partial_membership, assert_recorded};
use super::resolver_cases::cases_filters;
use bson::{Bson, Document, doc};
use kimmy_core::{IndexField, IndexMeta};
use kimmy_query::{filter, plan};

/// Every recorded `find` with a filter over arrays of documents, positions, and the clash and
/// past-the-end fixtures (f:1-26, 41-131; g:101-104, 128-130, 152, 153, 331-378; j:153-174),
/// answered as the reference answers it.
#[test]
fn every_recorded_filter_over_a_path_through_arrays_answers_as_recorded() {
    let cases = cases_filters();
    assert!(cases.len() > 100, "premise: {}", cases.len());
    assert_recorded("the resolver rule", cases);
}

/// What a partial index holds is what the filter selects, for each recorded filter a partial
/// index can carry, over the same documents: membership is the filter's own answer.
#[test]
fn a_partial_index_holds_what_the_resolver_rule_selects() {
    let checked = assert_partial_membership("the resolver rule", &cases_filters());
    assert!(checked > 30, "premise: the cases a partial index can carry: {checked}");
}

fn ids(docs: &[Document], query: Document) -> Vec<i32> {
    let f = filter::parse(&query).unwrap();
    docs.iter()
        .filter(|d| filter::matches(&f, d).unwrap())
        .map(|d| d.get_i32("_id").unwrap())
        .collect()
}

/// `{"a.c": {$gte: 4, $lte: null}}` over `{a: [{c: 9}, {d: 1}]}` (derived from f:8, f:9 and
/// f:15; NOT BACKED as one recording): 9 meets `$gte`, and the element lacking `c` meets
/// `$lte: null`, so the scan matches it, and a document with neither does not.
#[test]
fn two_bounds_are_met_by_different_elements_one_of_them_a_lacking_field() {
    let docs = [
        doc! { "_id": 1, "a": [{ "c": 9 }, { "d": 1 }] },
        doc! { "_id": 2, "a": [{ "c": 9 }] },
        doc! { "_id": 3, "a": [{ "d": 1 }] },
        doc! { "_id": 4, "a": [{ "c": 2 }, { "d": 1 }] },
    ];
    assert_eq!(ids(&docs, doc! { "a.c": { "$gte": 4, "$lte": Bson::Null } }), vec![1]);
}

fn index(path: &str, multikey: bool) -> IndexMeta {
    IndexMeta {
        id: 1,
        name: "i".into(),
        fields: vec![IndexField::ascending(path)],
        unique: false,
        enforcement: Default::default(),
        multikey,
        expire_after_secs: None,
        partial_filter: None,
        created: None,
    }
}

/// On a path with a digit position the index does not hold the null a filter reads from the
/// field-name reading of a position inside the array, so the planner scans for every predicate
/// a null satisfies (`{"a.0": null}` matches `{a: [{b: 1}]}`: the element at 0 lacks the field
/// "0"; its key under `a.0` is the element). Deviation D3, g:133.
#[test]
fn a_predicate_a_null_satisfies_is_scanned_on_a_path_with_a_position() {
    for path in ["a.0", "a.1", "a.0.c", "a.b.1", "a.99999999999"] {
        for multikey in [false, true] {
            let indexes = [index(path, multikey)];
            for query in [
                doc! { path: Bson::Null },
                doc! { path: { "$eq": Bson::Null } },
                doc! { path: { "$in": [Bson::Null] } },
                doc! { path: { "$in": [Bson::Null, 1] } },
                doc! { path: { "$lte": Bson::Null } },
                doc! { path: { "$gte": Bson::Null } },
            ] {
                let f = filter::parse(&query).unwrap();
                assert!(plan::choose(&f, &indexes).is_none(), "{query} on {path} must scan");
            }
            for query in [
                doc! { path: 5 },
                doc! { path: { "$in": [1, 5] } },
                doc! { path: { "$gte": 1 } },
                doc! { path: { "$lte": 5 } },
                doc! { path: { "$gte": 1, "$lte": 5 } },
                // The lower bound is used and the null upper bound is not.
                doc! { path: { "$gte": 4, "$lte": Bson::Null } },
            ] {
                let f = filter::parse(&query).unwrap();
                assert!(
                    plan::choose(&f, &indexes).is_some(),
                    "{query} on {path} may use the index"
                );
            }
        }
    }
}

/// A path with no position keeps every one: the lacking field is keyed as null there.
#[test]
fn a_path_with_no_position_answers_the_null_predicates_from_the_index() {
    for path in ["a.c", "a.b", "a.00", "a.01", "a.+0"] {
        let indexes = [index(path, true)];
        for query in [
            doc! { path: Bson::Null },
            doc! { path: { "$in": [Bson::Null, 1] } },
            doc! { path: { "$lte": Bson::Null } },
            doc! { path: { "$gte": Bson::Null } },
        ] {
            let f = filter::parse(&query).unwrap();
            assert!(plan::choose(&f, &indexes).is_some(), "{query} on {path} may use the index");
        }
    }
}

/// A compound index whose first field has a position and a null equality: the equality prefix
/// ends before it, so the second field's equality is not used either.
#[test]
fn a_null_equality_on_a_position_field_ends_the_equality_prefix() {
    let compound = IndexMeta {
        fields: vec![IndexField::ascending("a.0"), IndexField::ascending("b")],
        ..index("a.0", false)
    };
    let f = filter::parse(&doc! { "a.0": Bson::Null, "b": 1 }).unwrap();
    assert!(plan::choose(&f, std::slice::from_ref(&compound)).is_none());
    let f = filter::parse(&doc! { "a.0": 5, "b": 1 }).unwrap();
    let chosen = plan::choose(&f, std::slice::from_ref(&compound)).expect("an equality on both");
    assert_eq!(chosen.fields_used, 2);
}
