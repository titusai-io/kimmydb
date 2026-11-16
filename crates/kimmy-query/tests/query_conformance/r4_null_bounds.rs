use super::b_null_bounds::cases_r4_b;
use super::common::{assert_partial_membership, assert_recorded, ranges_over};
use super::ranges_a::cases_ranges_a;
use bson::{Bson, doc};
use kimmy_core::{IndexField, IndexMeta, PartialFilter, PartialOp};
use kimmy_query::{filter, plan};

/// `$gte: null` and `$lte: null` return the `null`, the `[null]` and the document with no
/// field (q7s3a0168 and a0238, q7s3a.out:7232 and 11082; q7s3b0075 and b0077, q7s3b.out:2149
/// and 2207: ids 2, 3, 10 and 13 over the 24 documents of set b); `$gt: null` and `$lt: null`
/// return nothing (a0133, a0203, b0074 and b0076).
#[test]
fn a_null_bound_holds_for_a_missing_field_in_gte_and_lte_only() {
    let mut cases = ranges_over(cases_ranges_a(), |bound| matches!(bound, Bson::Null));
    assert_eq!(cases.len(), 4, "premise: the four cases of the sweep over 50 values");
    cases.extend(cases_r4_b());
    assert_recorded("a null bound", cases);
}

/// Every one of the 245 cases of the sweep over the 50 values of q7s3a that takes a filter on
/// `v`, q7s3a0047 to a0291 (q7s3a.out:577 to 14052): equality over every type, `$ne`, the four
/// ranges over every type of bound and the implicit equality, answered as recorded. With
/// the NaN, `MinKey` and `MaxKey` and regex rules this is all of them.
#[test]
fn the_whole_sweep_over_50_values_as_recorded() {
    let cases = cases_ranges_a();
    assert_eq!(cases.len(), 245);
    assert_recorded("q7s3a0047 to a0291", cases);
}

/// A partial index holds what the filter selects, the document with no field included when the
/// bound is `null` under `$gte` or `$lte`.
#[test]
fn a_partial_index_holds_what_a_null_bound_selects() {
    let mut cases = ranges_over(cases_ranges_a(), |bound| matches!(bound, Bson::Null));
    cases.extend(cases_r4_b());
    let checked = assert_partial_membership("a null bound", &cases);
    assert_eq!(checked, 8, "premise: the eight cases, each a filter a partial index can carry");
    // Every one of the sweep's ranges and equalities that a partial index can carry.
    let checked = assert_partial_membership("the sweep", &cases_ranges_a());
    assert!(checked > 100, "premise: {checked}");
    let without = doc! { "_id": 1 };
    for (op, held) in [("$gte", true), ("$lte", true), ("$gt", false), ("$lt", false)] {
        let partial = PartialFilter::parse(&doc! { "v": { op: Bson::Null } }).unwrap();
        assert_eq!(partial.selects(&without), held, "{op}: null");
    }
}

/// A `$gte: null` or `$lte: null` is satisfied by a missing field, so it does not prove the
/// field is there, as an equality on `null` never did; and a partial index on it holds what it
/// selects, so a query proves only what its own bound implies.
#[test]
fn a_null_bound_proves_no_presence() {
    let query = |op| vec![("v".to_string(), op)];
    let exists = PartialFilter::parse(&doc! { "v": { "$exists": true } }).unwrap();
    assert!(!exists.covered_by(&query(PartialOp::Gte(Bson::Null))));
    assert!(!exists.covered_by(&query(PartialOp::Lte(Bson::Null))));
    // `$gt` and `$lt` of null hold for no document, and a value is what it is.
    assert!(exists.covered_by(&query(PartialOp::Eq(Bson::Int32(5)))));
    let at_least_null = PartialFilter::parse(&doc! { "v": { "$gte": Bson::Null } }).unwrap();
    assert!(at_least_null.covered_by(&query(PartialOp::Gte(Bson::Null))));
    assert!(!at_least_null.covered_by(&query(PartialOp::Eq(Bson::Int32(5)))));
}

/// The wrong implication, `{v: {$gte: null}}` implies `{v: {$exists: true}}`, would answer the
/// query from a partial index on `{v: {$exists: true}}`, which holds no document without `v`,
/// and return one too few: the documents the index does not hold are the ones `$gte: null`
/// and `$lte: null` newly match. The planner does not choose it for either, and still does for
/// a bound that implies the field is there.
#[test]
fn a_partial_index_on_exists_is_not_chosen_for_gte_or_lte_of_null() {
    let index = IndexMeta {
        id: 1,
        name: "p".into(),
        fields: vec![IndexField::ascending("v")],
        unique: false,
        enforcement: Default::default(),
        multikey: true,
        expire_after_secs: None,
        partial_filter: Some(doc! { "v": { "$exists": true } }),
        created: None,
    };
    let held = PartialFilter::parse(&doc! { "v": { "$exists": true } }).unwrap();
    let without = doc! { "_id": 1 };
    assert!(!held.selects(&without));
    for query in [doc! { "v": { "$gte": Bson::Null } }, doc! { "v": { "$lte": Bson::Null } }] {
        let f = filter::parse(&query).unwrap();
        assert!(filter::matches(&f, &without).unwrap(), "premise: {query} holds without a v");
        assert!(plan::choose(&f, std::slice::from_ref(&index)).is_none(), "{query}: answered");
    }
    for query in [doc! { "v": { "$gt": 5 } }, doc! { "v": { "$gte": "a" } }] {
        let f = filter::parse(&query).unwrap();
        assert!(plan::choose(&f, std::slice::from_ref(&index)).is_some(), "{query}");
    }
}
