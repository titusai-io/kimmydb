use super::common::{
    Case, assert_partial_membership, assert_recorded, is_number, pick, ranges_over,
};
use super::ranges_a::cases_ranges_a;
use bson::{Bson, doc};
use kimmy_core::PartialFilter;
use kimmy_core::partial::PartialOp;

fn numeric_ranges() -> Vec<Case> {
    ranges_over(cases_ranges_a(), is_number)
}

/// `{v: {$lt | $lte | $gt | $gte: <a number>}}` over the 50 values of q7s3a, each of the
/// numeric types as the bound, NaN and the infinities included (q7s3a0120 to a0127, a0150 to a0162,
/// a0185 to a0197, a0222 to a0235 and the rest of the sweep, q7s3a.out:4592 to 11907): NaN is
/// ordered against no number, so `$lt: 5` and `$lte: 5` leave out the NaNs (ids 10 and 15),
/// `$gt: NaN` and `$lt: NaN` return nothing, and `$gte: NaN` and `$lte: NaN` return the NaNs.
#[test]
fn nan_is_ordered_against_no_number_as_recorded() {
    let cases = numeric_ranges();
    assert!(cases.len() > 40, "premise: {} cases", cases.len());
    assert_recorded("a number as a range bound", cases);
}

/// The cases the triage names for R1: `$lte: 5` (q7s3a0222, a.out:10202), `$lt: 5` (a0187,
/// 8277), `$gt: NaN` (a0124, 4812) and `$lte: -Infinity` (a0232, 10752).
#[test]
fn the_named_nan_cases() {
    let cases = pick(cases_ranges_a(), &["q7s3a0222", "q7s3a0187", "q7s3a0124", "q7s3a0232"]);
    assert_recorded("the named NaN cases", cases);
}

/// A partial index holds what the filter selects: for every recorded range over a number, the
/// membership `PartialFilter::selects` decides is the recorded answer, document by document,
/// so a NaN is held by `{v: {$gte: NaN}}` and by no other range.
#[test]
fn a_partial_index_holds_what_a_numeric_range_selects() {
    let cases = numeric_ranges();
    let checked = assert_partial_membership("numeric ranges", &cases);
    assert_eq!(checked, cases.len(), "every recorded numeric range is a partial filter");
}

/// What a query proves of a NaN: it is in the index of `{v: {$lt: 5}}` no more than in the
/// answer of the query. `Eq(NaN)` and `Lt(NaN)` do not imply the filter's bound, a NaN bound
/// is implied by nothing but itself, and the numbers still imply each other.
#[test]
fn a_partial_filter_proves_nothing_of_a_nan() {
    let nan = || Bson::Double(f64::NAN);
    let query = |op| vec![("v".to_string(), op)];
    let lt5 = PartialFilter::parse(&doc! { "v": { "$lt": 5 } }).unwrap();
    assert!(!lt5.covered_by(&query(PartialOp::Eq(nan()))));
    assert!(!lt5.covered_by(&query(PartialOp::Lt(nan()))));
    assert!(!lt5.covered_by(&query(PartialOp::Lte(nan()))));
    assert!(lt5.covered_by(&query(PartialOp::Lt(Bson::Int32(3)))));
    assert!(lt5.covered_by(&query(PartialOp::Eq(Bson::Int32(4)))));
    let gt_nan = PartialFilter::parse(&doc! { "v": { "$gt": f64::NAN } }).unwrap();
    assert!(!gt_nan.covered_by(&query(PartialOp::Gt(Bson::Int32(5)))));
    assert!(!gt_nan.covered_by(&query(PartialOp::Eq(Bson::Int32(5)))));
    let gte_nan = PartialFilter::parse(&doc! { "v": { "$gte": f64::NAN } }).unwrap();
    assert!(gte_nan.covered_by(&query(PartialOp::Eq(nan()))));
    assert!(!gte_nan.covered_by(&query(PartialOp::Gte(Bson::Int32(5)))));
}
