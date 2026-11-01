//! The width of an integer result (ADR-268). Every expected type is the
//! reference's: `Int32` with `Int32` is an `Int32`, widening to `Int64` on
//! overflow and to a double past `Int64`; an `Int64` operand gives an `Int64`;
//! a double gives a double; the counting operators answer `Int32`; `$sum` is
//! typed by its exact total.
//!
//! A case the probes in `q8-probes/` did not settle is marked `UNSURE`.

use bson::{Bson, Document, doc};

use crate::aggregate::{self, Limits};

use super::{Expr, Total, count_bson};

const MAX: i32 = i32::MAX;
const MIN: i32 = i32::MIN;

fn eval(expr: Document, d: &Document) -> Bson {
    Expr::parse(&Bson::Document(expr)).unwrap().eval(d).unwrap()
}

fn ok(expr: Document) -> Bson {
    eval(expr, &Document::new())
}

fn group(accumulator: Bson, values: Vec<Bson>) -> Bson {
    let stages =
        aggregate::parse(&[doc! { "$group": { "_id": null, "r": { "$sum": accumulator } } }])
            .unwrap();
    let input: Vec<Document> = values.into_iter().map(|v| doc! { "v": v }).collect();
    let out = aggregate::apply(&stages[0], input, &Limits::default()).unwrap();
    out[0].get("r").unwrap().clone()
}

fn sum(values: Vec<Bson>) -> Bson {
    group(Bson::String("$v".into()), values)
}

// -- $add, $subtract, $multiply ---------------------------------------------

#[test]
fn int32_arithmetic_stays_int32_up_to_the_last_value_that_fits() {
    assert_eq!(ok(doc! {"$add": [MAX - 1, 1]}), Bson::Int32(MAX));
    assert_eq!(ok(doc! {"$add": [MIN + 1, -1]}), Bson::Int32(MIN));
    assert_eq!(ok(doc! {"$add": [MAX, 0]}), Bson::Int32(MAX));
    assert_eq!(ok(doc! {"$subtract": [MIN + 1, 1]}), Bson::Int32(MIN));
    assert_eq!(ok(doc! {"$subtract": [-1, MAX]}), Bson::Int32(MIN));
    assert_eq!(ok(doc! {"$subtract": [MIN, 0]}), Bson::Int32(MIN));
    assert_eq!(ok(doc! {"$multiply": [46_340, 46_340]}), Bson::Int32(2_147_395_600));
    assert_eq!(ok(doc! {"$multiply": [MIN, 1]}), Bson::Int32(MIN));
    assert_eq!(ok(doc! {"$multiply": [MAX, -1]}), Bson::Int32(-MAX));
}

#[test]
fn int32_arithmetic_widens_to_int64_on_overflow() {
    assert_eq!(ok(doc! {"$add": [MAX, 1]}), Bson::Int64(2_147_483_648));
    assert_eq!(ok(doc! {"$add": [MIN, -1]}), Bson::Int64(-2_147_483_649));
    assert_eq!(ok(doc! {"$add": [MAX, MAX]}), Bson::Int64(4_294_967_294));
    assert_eq!(ok(doc! {"$subtract": [MIN, 1]}), Bson::Int64(-2_147_483_649));
    assert_eq!(ok(doc! {"$subtract": [MAX, -1]}), Bson::Int64(2_147_483_648));
    assert_eq!(ok(doc! {"$subtract": [0, MIN]}), Bson::Int64(2_147_483_648));
    assert_eq!(ok(doc! {"$multiply": [46_341, 46_341]}), Bson::Int64(2_147_488_281));
    assert_eq!(ok(doc! {"$multiply": [65_536, 32_768]}), Bson::Int64(2_147_483_648));
    assert_eq!(ok(doc! {"$multiply": [MIN, -1]}), Bson::Int64(2_147_483_648));
    assert_eq!(ok(doc! {"$multiply": [MIN, MIN]}), Bson::Int64(4_611_686_018_427_387_904));
}

#[test]
fn a_chain_widens_at_the_step_that_overflows_and_stays_wide() {
    // Left to right: `MAX + 1` is an Int64, and the `-1` after it does not narrow it.
    assert_eq!(ok(doc! {"$add": [MAX, 1, -1]}), Bson::Int64(i64::from(MAX)));
    assert_eq!(ok(doc! {"$add": [1, 2, 3, MAX]}), Bson::Int64(i64::from(MAX) + 6));
    assert_eq!(ok(doc! {"$multiply": [2, 3, 4, 5]}), Bson::Int32(120));
    assert_eq!(ok(doc! {"$multiply": [65_536, 65_536, 1]}), Bson::Int64(4_294_967_296));
}

#[test]
fn an_int64_operand_makes_an_int64_whatever_the_value() {
    assert_eq!(ok(doc! {"$add": [1, 1i64]}), Bson::Int64(2));
    assert_eq!(ok(doc! {"$add": [1i64, 1]}), Bson::Int64(2));
    assert_eq!(ok(doc! {"$subtract": [5, 7i64]}), Bson::Int64(-2));
    assert_eq!(ok(doc! {"$multiply": [2, 3i64]}), Bson::Int64(6));
    assert_eq!(ok(doc! {"$multiply": [3i64, 2, 2]}), Bson::Int64(12));
    assert_eq!(ok(doc! {"$add": [1i64, 2i64]}), Bson::Int64(3));
}

#[test]
fn int64_overflow_and_a_double_operand_give_a_double() {
    assert_eq!(ok(doc! {"$add": [i64::MAX, 1]}), Bson::Double(9_223_372_036_854_775_808.0));
    assert_eq!(ok(doc! {"$subtract": [i64::MIN, 1]}), Bson::Double(-9_223_372_036_854_775_808.0));
    assert_eq!(ok(doc! {"$multiply": [i64::MAX, 2]}), Bson::Double(18_446_744_073_709_551_616.0));
    assert_eq!(ok(doc! {"$add": [1, 1.5]}), Bson::Double(2.5));
    assert_eq!(ok(doc! {"$add": [MAX, 1.0]}), Bson::Double(2_147_483_648.0));
    assert_eq!(ok(doc! {"$add": [1i64, 0.5]}), Bson::Double(1.5));
    assert_eq!(ok(doc! {"$multiply": [3, 0.5]}), Bson::Double(1.5));
    assert_eq!(ok(doc! {"$subtract": [3, 1.0]}), Bson::Double(2.0));
}

#[test]
fn a_date_difference_is_an_int64_of_milliseconds() {
    let (a, b) = (bson::DateTime::from_millis(5_000), bson::DateTime::from_millis(1_500));
    assert_eq!(ok(doc! {"$subtract": [a, b]}), Bson::Int64(3_500));
}

// -- $mod -------------------------------------------------------------------

#[test]
fn mod_follows_the_widest_operand() {
    assert_eq!(ok(doc! {"$mod": [7, 3]}), Bson::Int32(1));
    assert_eq!(ok(doc! {"$mod": [-7, 3]}), Bson::Int32(-1));
    assert_eq!(ok(doc! {"$mod": [7, 3i64]}), Bson::Int64(1));
    assert_eq!(ok(doc! {"$mod": [7i64, 3]}), Bson::Int64(1));
    assert_eq!(ok(doc! {"$mod": [7, 2.5]}), Bson::Double(2.0));
    assert_eq!(ok(doc! {"$mod": [7.5, 2]}), Bson::Double(1.5));
}

#[test]
fn mod_by_minus_one_is_zero_for_the_smallest_integers_too() {
    assert_eq!(ok(doc! {"$mod": [MIN, -1]}), Bson::Int32(0));
    assert_eq!(ok(doc! {"$mod": [i64::MIN, -1i64]}), Bson::Int64(0));
    assert_eq!(ok(doc! {"$mod": [MIN, -1i64]}), Bson::Int64(0));
}

// -- $abs, $ceil, $floor, $round, $trunc, $pow --------------------------------

#[test]
fn abs_keeps_the_width_and_widens_the_smallest_int32() {
    assert_eq!(ok(doc! {"$abs": -5}), Bson::Int32(5));
    assert_eq!(ok(doc! {"$abs": MAX}), Bson::Int32(MAX));
    assert_eq!(ok(doc! {"$abs": MIN + 1}), Bson::Int32(MAX));
    // The one value an Int32 cannot negate.
    assert_eq!(ok(doc! {"$abs": MIN}), Bson::Int64(2_147_483_648));
    assert_eq!(ok(doc! {"$abs": -5i64}), Bson::Int64(5));
    // An Int64 that would fit an Int32 stays an Int64.
    assert_eq!(ok(doc! {"$abs": i64::from(MIN)}), Bson::Int64(2_147_483_648));
    assert_eq!(ok(doc! {"$abs": i64::MIN}), Bson::Double(9_223_372_036_854_775_808.0));
    assert_eq!(ok(doc! {"$abs": -2.5}), Bson::Double(2.5));
}

#[test]
fn ceil_and_floor_keep_the_width() {
    assert_eq!(ok(doc! {"$ceil": 7}), Bson::Int32(7));
    assert_eq!(ok(doc! {"$floor": -7}), Bson::Int32(-7));
    assert_eq!(ok(doc! {"$ceil": 7i64}), Bson::Int64(7));
    assert_eq!(ok(doc! {"$floor": -7i64}), Bson::Int64(-7));
    assert_eq!(ok(doc! {"$ceil": 7.2}), Bson::Double(8.0));
    assert_eq!(ok(doc! {"$floor": 7.8}), Bson::Double(7.0));
}

#[test]
fn round_and_trunc_keep_the_width() {
    assert_eq!(ok(doc! {"$round": [1250, -2]}), Bson::Int32(1200));
    assert_eq!(ok(doc! {"$round": [1350, -2]}), Bson::Int32(1400));
    assert_eq!(ok(doc! {"$trunc": [1999, -2]}), Bson::Int32(1900));
    assert_eq!(ok(doc! {"$round": [7, 2]}), Bson::Int32(7));
    assert_eq!(ok(doc! {"$round": [7]}), Bson::Int32(7));
    assert_eq!(ok(doc! {"$round": [1250i64, -2]}), Bson::Int64(1200));
    assert_eq!(ok(doc! {"$trunc": [1999i64, -2]}), Bson::Int64(1900));
    assert_eq!(ok(doc! {"$round": [1250i64, 3]}), Bson::Int64(1250));
    assert_eq!(ok(doc! {"$round": [2.5]}), Bson::Double(2.0));
    // UNSURE: an Int32 rounded up past Int32 (the probes did not reach it)
    // widens to an Int64, as an overflowing `$add` does.
    assert_eq!(ok(doc! {"$round": [MAX, -1]}), Bson::Int64(2_147_483_650));
    assert_eq!(ok(doc! {"$trunc": [MAX, -1]}), Bson::Int32(2_147_483_640));
}

#[test]
fn pow_of_two_int32s_is_an_int32_while_it_fits() {
    // UNSURE: the probes did not type `$pow`; this is the reference's documented rule.
    assert_eq!(ok(doc! {"$pow": [2, 30]}), Bson::Int32(1 << 30));
    assert_eq!(ok(doc! {"$pow": [2, 31]}), Bson::Int64(1 << 31));
    assert_eq!(ok(doc! {"$pow": [-2, 31]}), Bson::Int32(MIN));
    assert_eq!(ok(doc! {"$pow": [-2, 32]}), Bson::Int64(1 << 32));
    assert_eq!(ok(doc! {"$pow": [10, 9]}), Bson::Int32(1_000_000_000));
    assert_eq!(ok(doc! {"$pow": [10, 10]}), Bson::Int64(10_000_000_000));
    assert_eq!(ok(doc! {"$pow": [2, 62]}), Bson::Int64(1 << 62));
    assert_eq!(ok(doc! {"$pow": [2, 63]}), Bson::Double(9_223_372_036_854_775_808.0));
}

#[test]
fn pow_with_an_int64_operand_is_an_int64() {
    assert_eq!(ok(doc! {"$pow": [2, 3i64]}), Bson::Int64(8));
    assert_eq!(ok(doc! {"$pow": [2i64, 3]}), Bson::Int64(8));
    assert_eq!(ok(doc! {"$pow": [2.0, 3]}), Bson::Double(8.0));
    assert_eq!(ok(doc! {"$pow": [2, -1]}), Bson::Double(0.5));
}

// -- The counting operators ---------------------------------------------------

#[test]
fn counts_and_indexes_are_int32() {
    let d = doc! {"xs": [1, 2, 3], "s": "héllo"};
    assert_eq!(eval(doc! {"$size": "$xs"}, &d), Bson::Int32(3));
    assert_eq!(eval(doc! {"$strLenCP": "$s"}, &d), Bson::Int32(5));
    assert_eq!(eval(doc! {"$strLenBytes": "$s"}, &d), Bson::Int32(6));
    assert_eq!(eval(doc! {"$indexOfArray": ["$xs", 3]}, &d), Bson::Int32(2));
    assert_eq!(eval(doc! {"$indexOfArray": ["$xs", 9]}, &d), Bson::Int32(-1));
    assert_eq!(eval(doc! {"$indexOfCP": ["$s", "l"]}, &d), Bson::Int32(2));
    assert_eq!(eval(doc! {"$indexOfBytes": ["$s", "l"]}, &d), Bson::Int32(3));
    assert_eq!(eval(doc! {"$indexOfBytes": ["$s", "z"]}, &d), Bson::Int32(-1));
    assert_eq!(eval(doc! {"$cmp": ["$s", "a"]}, &d), Bson::Int32(1));
}

#[test]
fn range_elements_are_int32_and_an_element_past_int32_is_an_int64() {
    assert_eq!(ok(doc! {"$range": [0, 3]}), bson::bson!([0, 1, 2]));
    assert_eq!(ok(doc! {"$range": [5, 0, -2]}), bson::bson!([5, 3, 1]));
    assert_eq!(ok(doc! {"$range": [-1, 1]}), bson::bson!([-1, 0]));
    // UNSURE: the reference refuses an argument past `Int32`; here it is accepted and
    // its elements keep the width they need.
    assert_eq!(
        ok(doc! {"$range": [i64::from(MAX) - 1, i64::from(MAX) + 2]}),
        Bson::Array(vec![Bson::Int32(MAX - 1), Bson::Int32(MAX), Bson::Int64(i64::from(MAX) + 1)])
    );
}

#[test]
fn regex_find_idx_is_int32() {
    let found = ok(doc! {"$regexFind": {"input": "abc", "regex": "c"}});
    assert_eq!(found.as_document().unwrap().get("idx"), Some(&Bson::Int32(2)));
}

#[test]
fn a_count_is_int32_while_it_fits_and_int64_after() {
    assert_eq!(count_bson(0), Bson::Int32(0));
    assert_eq!(count_bson(MAX as usize), Bson::Int32(MAX));
    assert_eq!(count_bson(MAX as usize + 1), Bson::Int64(2_147_483_648));
}

// -- $sum ---------------------------------------------------------------------

#[test]
fn a_sum_of_int32s_is_an_int32_while_the_total_fits() {
    assert_eq!(sum(vec![Bson::Int32(1), Bson::Int32(2)]), Bson::Int32(3));
    assert_eq!(sum(vec![Bson::Int32(MAX - 1), Bson::Int32(1)]), Bson::Int32(MAX));
    assert_eq!(sum(vec![Bson::Int32(MIN + 1), Bson::Int32(-1)]), Bson::Int32(MIN));
    // Nothing numeric to add is the Int32 zero.
    assert_eq!(Total::default().to_bson(), Bson::Int32(0));
    assert_eq!(sum(vec![Bson::String("x".into()), Bson::Null]), Bson::Int32(0));
}

#[test]
fn a_sum_widens_to_int64_then_to_a_double() {
    assert_eq!(sum(vec![Bson::Int32(MAX), Bson::Int32(1)]), Bson::Int64(2_147_483_648));
    assert_eq!(sum(vec![Bson::Int32(MIN), Bson::Int32(-1)]), Bson::Int64(-2_147_483_649));
    assert_eq!(sum(vec![Bson::Int32(MAX), Bson::Int32(MAX), Bson::Int32(MAX)]), {
        Bson::Int64(3 * i64::from(MAX))
    });
    assert_eq!(
        sum(vec![Bson::Int64(i64::MAX), Bson::Int32(1)]),
        Bson::Double(9_223_372_036_854_775_808.0)
    );
    assert_eq!(sum(vec![Bson::Int32(1), Bson::Double(0.5)]), Bson::Double(1.5));
}

#[test]
fn a_sum_is_typed_by_its_total_and_not_by_the_order_or_the_width_of_the_values() {
    // The exact total fits an Int32, so it is one, however it was reached.
    assert_eq!(sum(vec![Bson::Int32(MAX), Bson::Int32(1), Bson::Int32(-1)]), Bson::Int32(MAX));
    assert_eq!(sum(vec![Bson::Int32(-1), Bson::Int32(MAX), Bson::Int32(1)]), Bson::Int32(MAX));
}

#[test]
fn a_sum_of_int64s_is_an_int32_when_the_total_fits_one() {
    // The reference types a window sum of `1` and `2` stored as longs as the int 3.
    assert_eq!(sum(vec![Bson::Int64(1), Bson::Int64(2)]), Bson::Int32(3));
    assert_eq!(sum(vec![Bson::Int32(1), Bson::Int64(2)]), Bson::Int32(3));
    assert_eq!(
        sum(vec![Bson::Int64(i64::from(MAX)), Bson::Int64(i64::from(MAX)), Bson::Int64(1)]),
        Bson::Int64(2 * i64::from(MAX) + 1)
    );
}

#[test]
fn sum_one_counts_as_an_int32() {
    let stages =
        aggregate::parse(&[doc! { "$group": { "_id": null, "n": { "$sum": 1 } } }]).unwrap();
    let input: Vec<Document> = (0..5).map(|i| doc! { "i": i }).collect();
    let out = aggregate::apply(&stages[0], input, &Limits::default()).unwrap();
    assert_eq!(out[0].get("n"), Some(&Bson::Int32(5)));
    // And a literal past Int32 widens the same way a field does.
    assert_eq!(group(Bson::Int32(MAX), vec![Bson::Null, Bson::Null]), Bson::Int64(4_294_967_294));
}

#[test]
fn the_total_widens_past_two_to_the_31_minus_1() {
    let mut total = Total::default();
    total.add(&Bson::Int32(MAX));
    assert_eq!(total.to_bson(), Bson::Int32(MAX));
    total.add(&Bson::Int32(1));
    assert_eq!(total.to_bson(), Bson::Int64(2_147_483_648));
}

#[test]
fn avg_of_integers_is_a_double() {
    let stages =
        aggregate::parse(&[doc! { "$group": { "_id": null, "a": { "$avg": "$v" } } }]).unwrap();
    let input = vec![doc! { "v": 1 }, doc! { "v": 2 }];
    let out = aggregate::apply(&stages[0], input, &Limits::default()).unwrap();
    assert_eq!(out[0].get("a"), Some(&Bson::Double(1.5)));
}

// -- The stages ---------------------------------------------------------------

#[test]
fn the_count_stage_and_the_bucket_counts_are_int32() {
    let stages = aggregate::parse(&[doc! { "$count": "n" }]).unwrap();
    let input: Vec<Document> = (0..3).map(|i| doc! { "i": i }).collect();
    let out = aggregate::apply(&stages[0], input, &Limits::default()).unwrap();
    assert_eq!(out, vec![doc! { "n": 3 }]);

    let stages = aggregate::parse(&[doc! { "$bucket": {
    "groupBy": "$i", "boundaries": [0, 10], "default": "other" } }])
    .unwrap();
    let input: Vec<Document> = (0..3).map(|i| doc! { "i": i }).collect();
    let out = aggregate::apply(&stages[0], input, &Limits::default()).unwrap();
    assert_eq!(out, vec![doc! { "_id": 0, "count": 3 }]);
}

#[test]
fn an_unwind_index_stays_an_int64() {
    // The reference types `includeArrayIndex` as a long.
    let stages =
        aggregate::parse(&[doc! { "$unwind": { "path": "$xs", "includeArrayIndex": "i" } }])
            .unwrap();
    let out =
        aggregate::apply(&stages[0], vec![doc! { "xs": [7, 8] }], &Limits::default()).unwrap();
    assert_eq!(out[1].get("i"), Some(&Bson::Int64(1)));
}
