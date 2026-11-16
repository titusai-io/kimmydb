use super::common::{Case, assert_partial_membership, assert_recorded, ranges_over};
use super::f_end_keys::cases_r2_f;
use super::ranges_a::cases_ranges_a;
use bson::{Bson, doc};
use kimmy_core::{IndexField, IndexMeta, PartialFilter, PartialOp};
use kimmy_query::{filter, plan};

fn end_key_ranges() -> Vec<Case> {
    ranges_over(cases_ranges_a(), |bound| matches!(bound, Bson::MinKey | Bson::MaxKey))
}

/// `$gt`, `$gte`, `$lt` and `$lte` of `MinKey` and of `MaxKey` over the 50 values of q7s3a
/// (q7s3a0147, a0148, a0182, a0183, a0217, a0218, a0252 and a0253, q7s3a.out:6077 to 11907):
/// the bound is ordered against every type. `$gt: MinKey` returns the 49 documents but the
/// `MinKey` one, the regular expression, `null` and the missing field among them, `$lt: MinKey`
/// and `$gt: MaxKey` return nothing, `$lte: MinKey` only the `MinKey` and `$gte: MaxKey` only
/// the `MaxKey`.
#[test]
fn an_end_key_bound_is_ordered_against_every_type() {
    let cases = end_key_ranges();
    assert_eq!(cases.len(), 8, "premise: the eight cases of the sweep");
    assert_recorded("MinKey and MaxKey as a bound", cases);
}

/// The thirteen values of q7s3f0132 to f0162 (q7s3f.out:2298 to 2838): the same bounds over
/// arrays that hold a `MinKey` or a `MaxKey`, beside a second bound, with `$exists`, `$ne` and
/// `$type`, under `$not` and `$or`, and as the bounds of an `$elemMatch`.
#[test]
fn an_end_key_bound_beside_other_conditions_as_recorded() {
    assert_recorded("MinKey and MaxKey, set f", cases_r2_f());
}

/// A partial index holds what the filter selects, a document without the field included when
/// the bound is an end key: `{v: {$gt: MinKey}}` holds the document with no `v`, and
/// `{v: {$lt: MinKey}}` holds nothing.
#[test]
fn a_partial_index_holds_what_an_end_key_range_selects() {
    let mut checked = assert_partial_membership("MinKey and MaxKey", &end_key_ranges());
    checked += assert_partial_membership("MinKey and MaxKey, set f", &cases_r2_f());
    assert_eq!(checked, 18, "premise: the 18 cases whose filter a partial index can carry");
    let without = doc! { "_id": 1 };
    let with_null = doc! { "_id": 2, "v": Bson::Null };
    for (op, bound, held) in [
        ("$gt", Bson::MinKey, true),
        ("$gte", Bson::MinKey, true),
        ("$lt", Bson::MaxKey, true),
        ("$lte", Bson::MaxKey, true),
        ("$lt", Bson::MinKey, false),
        ("$lte", Bson::MinKey, false),
        ("$gt", Bson::MaxKey, false),
        ("$gte", Bson::MaxKey, false),
    ] {
        let partial = PartialFilter::parse(&doc! { "v": { op: bound.clone() } }).unwrap();
        for d in [&without, &with_null] {
            assert_eq!(partial.selects(d), held, "{op} {bound}: {d}");
        }
    }
}

/// What a query proves: a bound an end key makes holds for a missing field, so it does not imply
/// the field is there, and it implies no other bound; the values it does hold on imply it.
#[test]
fn an_end_key_bound_proves_no_presence() {
    let query = |op| vec![("v".to_string(), op)];
    let exists = PartialFilter::parse(&doc! { "v": { "$exists": true } }).unwrap();
    for op in [
        PartialOp::Gt(Bson::MinKey),
        PartialOp::Gte(Bson::MinKey),
        PartialOp::Lt(Bson::MaxKey),
        PartialOp::Lte(Bson::MaxKey),
    ] {
        assert!(!exists.covered_by(&query(op.clone())), "{op:?}");
    }
    // A bound over a number, and a value, hold for a document that has the field only.
    assert!(exists.covered_by(&query(PartialOp::Gt(Bson::Int32(5)))));
    assert!(exists.covered_by(&query(PartialOp::Eq(Bson::Int32(5)))));
    let above_min = PartialFilter::parse(&doc! { "v": { "$gt": Bson::MinKey } }).unwrap();
    assert!(above_min.covered_by(&query(PartialOp::Eq(Bson::Int32(5)))));
    assert!(!above_min.covered_by(&query(PartialOp::Eq(Bson::MinKey))));
    let five = PartialFilter::parse(&doc! { "v": { "$gt": 5 } }).unwrap();
    assert!(!five.covered_by(&query(PartialOp::Gt(Bson::MinKey))));
    assert!(!five.covered_by(&query(PartialOp::Lt(Bson::MaxKey))));
}

/// A partial index on `{v: {$exists: true}}`, over a field that is not the filtered path, as
/// the planner reads it.
fn exists_index() -> IndexMeta {
    IndexMeta {
        id: 1,
        name: "p".into(),
        fields: vec![IndexField::ascending("v")],
        unique: false,
        enforcement: Default::default(),
        multikey: true,
        expire_after_secs: None,
        partial_filter: Some(doc! { "v": { "$exists": true } }),
        created: None,
    }
}

/// The wrong implication, `{v: {$gt: MinKey}}` implies `{v: {$exists: true}}`, would answer the
/// query from this index, which holds no document without `v`, and return one too few. The
/// planner does not choose it for a bound that a missing field satisfies, and does for a bound
/// that implies the field is there; and what the index holds of the documents is the part of
/// `find`'s answer it was chosen for.
#[test]
fn a_partial_index_on_exists_is_not_chosen_for_a_bound_a_missing_field_satisfies() {
    let docs = [
        doc! { "_id": 1 },
        doc! { "_id": 2, "v": 5 },
        doc! { "_id": 3, "v": Bson::Null },
        doc! { "_id": 4, "v": "x" },
    ];
    let held = PartialFilter::parse(&doc! { "v": { "$exists": true } }).unwrap();
    for query in [
        doc! { "v": { "$gt": Bson::MinKey } },
        doc! { "v": { "$gte": Bson::MinKey } },
        doc! { "v": { "$lt": Bson::MaxKey } },
        doc! { "v": { "$lte": Bson::MaxKey } },
    ] {
        let f = filter::parse(&query).unwrap();
        assert!(
            filter::matches(&f, &docs[0]).unwrap() && !held.selects(&docs[0]),
            "premise: {query} holds for a document the index does not"
        );
        assert!(plan::choose(&f, &[exists_index()]).is_none(), "{query} is answered from it");
    }
    for query in [
        doc! { "v": { "$gt": 5 } },
        doc! { "v": { "$lte": 5 } },
        doc! { "v": 5 },
        doc! { "v": { "$gte": "a" } },
    ] {
        let f = filter::parse(&query).unwrap();
        assert!(plan::choose(&f, &[exists_index()]).is_some(), "{query} is not");
        for d in &docs {
            assert!(!filter::matches(&f, d).unwrap() || held.selects(d), "{query} on {d}");
        }
    }
}
