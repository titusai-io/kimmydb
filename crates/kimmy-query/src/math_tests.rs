//! The math operators, `$$NOW` and `$$REMOVE` (ADR-253), one row at a time:
//! which operands give which type, what is refused, and in whose words.
//!
//! The decimal functions are checked against references in `kimmy-core`; these
//! tests pin what the operators do with them.

use bson::{Bson, Document, doc};
use kimmy_core::decimal::parse;

use crate::aggregate::{self, Limits, Stage};
use crate::expr::Expr;
use crate::filter;

fn dec(text: &str) -> Bson {
    Bson::Decimal128(parse(text).unwrap_or_else(|e| panic!("{text}: {e}")))
}

fn eval(expr: Bson, doc: Document) -> kimmy_core::Result<Bson> {
    Expr::parse(&expr).unwrap_or_else(|e| panic!("{expr:?}: {e}")).eval(&doc)
}

fn val(expr: Bson, doc: Document) -> Bson {
    eval(expr.clone(), doc).unwrap_or_else(|e| panic!("{expr:?}: {e}"))
}

fn err(expr: Bson, doc: Document) -> String {
    eval(expr, doc).expect_err("must be refused").to_string()
}

fn op(name: &str, args: Vec<Bson>) -> Bson {
    Bson::Document(doc! { name: args })
}

fn one(name: &str, arg: Bson) -> Bson {
    op(name, vec![arg])
}

fn text(value: &Bson) -> String {
    match value {
        Bson::Decimal128(d) => d.to_string(),
        other => panic!("not a decimal: {other:?}"),
    }
}

fn same(value: Bson, want: &str) {
    assert_eq!(text(&value), text(&dec(want)), "want {want}");
}

fn none() -> Document {
    doc! {}
}

fn double(value: &Bson) -> f64 {
    match value {
        Bson::Double(d) => *d,
        other => panic!("not a double: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// $abs, $ceil, $floor
// ---------------------------------------------------------------------------

#[test]
fn abs_keeps_the_type_and_falls_to_a_double_only_where_an_int64_cannot_hold_it() {
    assert_eq!(val(one("$abs", Bson::Int32(-5)), none()), Bson::Int64(5));
    assert_eq!(val(one("$abs", Bson::Int64(7)), none()), Bson::Int64(7));
    assert_eq!(val(one("$abs", Bson::Double(-2.5)), none()), Bson::Double(2.5));
    assert_eq!(val(one("$abs", Bson::Double(-0.0)), none()), Bson::Double(0.0));
    assert_eq!(val(one("$abs", Bson::Int64(i64::MIN)), none()), Bson::Double(2f64.powi(63)));
    assert_eq!(val(one("$abs", Bson::String("$n".into())), doc! {"n": -3}), Bson::Int64(3));
    same(val(one("$abs", dec("-1.50")), none()), "1.50");
    same(val(one("$abs", dec("-0E+3")), none()), "0E+3");
}

#[test]
fn ceil_and_floor_leave_an_integer_alone_and_a_double_a_double() {
    assert_eq!(val(one("$ceil", Bson::Int32(7)), none()), Bson::Int64(7));
    assert_eq!(val(one("$ceil", Bson::Double(1.2)), none()), Bson::Double(2.0));
    assert_eq!(val(one("$floor", Bson::Double(-1.2)), none()), Bson::Double(-2.0));
    assert_eq!(val(one("$floor", Bson::Double(2.0)), none()), Bson::Double(2.0));
    same(val(one("$ceil", dec("1.20")), none()), "2");
    same(val(one("$floor", dec("-1.20")), none()), "-2");
    same(val(one("$floor", dec("5E+2")), none()), "5E+2");
    assert!(double(&val(one("$ceil", Bson::Double(f64::NAN)), none())).is_nan());
    assert_eq!(val(one("$ceil", Bson::Double(f64::INFINITY)), none()), Bson::Double(f64::INFINITY));
}

#[test]
fn null_and_missing_give_null_and_a_non_number_is_refused() {
    for name in ["$abs", "$ceil", "$floor", "$sqrt", "$exp", "$ln", "$log10"] {
        assert_eq!(val(one(name, Bson::Null), none()), Bson::Null, "{name}");
        assert_eq!(val(one(name, Bson::String("$missing".into())), none()), Bson::Null, "{name}");
        let message = err(one(name, Bson::String("text".into())), none());
        assert!(message.contains(name) && message.contains("needs numbers"), "{message}");
        let message = err(one(name, Bson::Boolean(true)), none());
        assert!(message.contains("needs numbers"), "{message}");
    }
    for name in ["$round", "$trunc"] {
        assert_eq!(val(op(name, vec![Bson::Null]), none()), Bson::Null);
        assert_eq!(val(op(name, vec![Bson::Int32(5), Bson::Null]), none()), Bson::Null);
        assert_eq!(val(op(name, vec![Bson::Null, Bson::Int32(2)]), none()), Bson::Null);
        assert!(err(op(name, vec![Bson::String("x".into())]), none()).contains("needs numbers"));
        // A value that is not a number is refused whatever the place is, a null one included.
        for place in [Bson::Null, Bson::Int32(2)] {
            assert!(
                err(op(name, vec![Bson::String("text".into()), place]), none())
                    .contains("needs numbers")
            );
        }
        // An unusable place is refused even when the number is null.
        assert!(
            err(op(name, vec![Bson::Null, Bson::Int32(101)]), none())
                .contains("between -20 and 100")
        );
    }
    assert_eq!(val(op("$pow", vec![Bson::Null, Bson::Int32(2)]), none()), Bson::Null);
    assert_eq!(val(op("$pow", vec![Bson::Int32(2), Bson::Null]), none()), Bson::Null);
    assert!(
        err(op("$pow", vec![Bson::Int32(2), Bson::String("x".into())]), none())
            .contains("needs numbers")
    );
}

#[test]
fn the_wrong_number_of_arguments_is_refused_at_parse() {
    for expr in [
        op("$abs", vec![]),
        op("$abs", vec![Bson::Int32(1), Bson::Int32(2)]),
        op("$sqrt", vec![Bson::Int32(1), Bson::Int32(2)]),
        op("$round", vec![]),
        op("$round", vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]),
        op("$trunc", vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]),
        op("$pow", vec![Bson::Int32(1)]),
        op("$pow", vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]),
    ] {
        assert!(Expr::parse(&expr).is_err(), "{expr:?} should be refused");
    }
}

// ---------------------------------------------------------------------------
// $round, $trunc
// ---------------------------------------------------------------------------

fn round(x: Bson, place: i32) -> Bson {
    val(op("$round", vec![x, Bson::Int32(place)]), none())
}

fn trunc(x: Bson, place: i32) -> Bson {
    val(op("$trunc", vec![x, Bson::Int32(place)]), none())
}

#[test]
fn round_goes_half_to_even_and_a_double_stays_a_double() {
    let r = |x: f64| val(one("$round", Bson::Double(x)), none());
    assert_eq!(r(2.5), Bson::Double(2.0));
    assert_eq!(r(3.5), Bson::Double(4.0));
    assert_eq!(r(-2.5), Bson::Double(-2.0));
    assert_eq!(r(-3.5), Bson::Double(-4.0));
    assert_eq!(r(0.5), Bson::Double(0.0));
    assert_eq!(r(1.5), Bson::Double(2.0));
    assert_eq!(r(2.4999999999999996), Bson::Double(2.0));
    assert_eq!(r(2.5000000000000004), Bson::Double(3.0));
    assert_eq!(r(1e300), Bson::Double(1e300));
    assert_eq!(r(f64::INFINITY), Bson::Double(f64::INFINITY));
    assert!(double(&r(f64::NAN)).is_nan());
}

#[test]
fn round_of_a_double_acts_on_its_exact_binary_value() {
    // 0.125 is exact, so this is a tie; to even.
    assert_eq!(round(Bson::Double(0.125), 2), Bson::Double(0.12));
    assert_eq!(round(Bson::Double(0.375), 2), Bson::Double(0.38));
    // The double nearest 2.675 is 2.67499999999999982236431605997495353221893310546875.
    assert_eq!(round(Bson::Double(2.675), 2), Bson::Double(2.67));
    // And the one nearest 1.005 is below it too.
    assert_eq!(round(Bson::Double(1.005), 2), Bson::Double(1.0));
    assert_eq!(round(Bson::Double(1.015), 2), Bson::Double(1.01));
    assert_eq!(round(Bson::Double(1.025), 2), Bson::Double(1.02));
    assert_eq!(round(Bson::Double(-0.125), 2), Bson::Double(-0.12));
    assert_eq!(round(Bson::Double(123.456), 1), Bson::Double(123.5));
    assert_eq!(round(Bson::Double(123.456), 10), Bson::Double(123.456));
    assert_eq!(round(Bson::Double(0.04), 1), Bson::Double(0.0));
    assert_eq!(round(Bson::Double(0.05), 1), Bson::Double(0.1));
    assert_eq!(round(Bson::Double(1234.5678), -2), Bson::Double(1200.0));
    assert_eq!(round(Bson::Double(1250.0), -2), Bson::Double(1200.0));
    assert_eq!(round(Bson::Double(1350.0), -2), Bson::Double(1400.0));
    assert_eq!(round(Bson::Double(49.0), -2), Bson::Double(0.0));
    assert_eq!(round(Bson::Double(51.0), -2), Bson::Double(100.0));
    assert_eq!(round(Bson::Double(9.9), 0), Bson::Double(10.0));
    assert_eq!(round(Bson::Double(99.999), 2), Bson::Double(100.0));
    assert_eq!(round(Bson::Double(5e-324), 100), Bson::Double(0.0));
    assert_eq!(round(Bson::Double(1e-5), 3), Bson::Double(0.0));
    assert!(double(&round(Bson::Double(-0.0001), 2)).is_sign_negative());
}

#[test]
fn trunc_cuts_toward_zero() {
    assert_eq!(trunc(Bson::Double(2.99), 0), Bson::Double(2.0));
    assert_eq!(trunc(Bson::Double(-2.99), 0), Bson::Double(-2.0));
    assert_eq!(trunc(Bson::Double(-2.99), 1), Bson::Double(-2.9));
    assert_eq!(trunc(Bson::Double(0.129), 2), Bson::Double(0.12));
    assert_eq!(trunc(Bson::Double(1999.0), -2), Bson::Double(1900.0));
    assert_eq!(trunc(Bson::Double(2.675), 2), Bson::Double(2.67));
    assert_eq!(trunc(Bson::Int64(1999), -2), Bson::Int64(1900));
    assert_eq!(trunc(Bson::Int64(-1999), -2), Bson::Int64(-1900));
    assert_eq!(trunc(Bson::Int64(5), 3), Bson::Int64(5));
}

#[test]
fn round_of_an_integer_with_a_negative_place_rounds_half_to_even_and_stays_an_integer() {
    assert_eq!(round(Bson::Int64(1250), -2), Bson::Int64(1200));
    assert_eq!(round(Bson::Int64(1350), -2), Bson::Int64(1400));
    assert_eq!(round(Bson::Int64(1351), -2), Bson::Int64(1400));
    assert_eq!(round(Bson::Int64(1249), -2), Bson::Int64(1200));
    assert_eq!(round(Bson::Int64(5), -1), Bson::Int64(0));
    assert_eq!(round(Bson::Int64(15), -1), Bson::Int64(20));
    assert_eq!(round(Bson::Int64(-15), -1), Bson::Int64(-20));
    assert_eq!(round(Bson::Int64(-25), -1), Bson::Int64(-20));
    assert_eq!(round(Bson::Int32(7), 0), Bson::Int64(7));
    assert_eq!(round(Bson::Int64(7), 5), Bson::Int64(7));
    assert_eq!(round(Bson::Int64(i64::MAX), -20), Bson::Int64(0));
    // Rounding up to a multiple past Int64 is a double.
    assert_eq!(round(Bson::Int64(i64::MAX), -1), Bson::Double(9223372036854775810.0));
}

#[test]
fn round_and_trunc_of_a_decimal_quantize_to_the_place() {
    same(round(dec("2.5"), 0), "2");
    same(round(dec("3.5"), 0), "4");
    same(round(dec("-2.5"), 0), "-2");
    same(round(dec("2.5"), 2), "2.50");
    same(round(dec("1.005"), 2), "1.00");
    same(round(dec("1.015"), 2), "1.02");
    same(round(dec("1234.5"), -2), "1.2E+3");
    same(round(dec("9.995"), 2), "10.00");
    same(trunc(dec("9.999"), 2), "9.99");
    same(trunc(dec("-9.999"), 2), "-9.99");
    same(trunc(dec("1.5"), 3), "1.500");
    same(round(dec("0.4"), 0), "0");
    // Rounding is exact: no double is involved.
    same(
        round(dec("0.1234567890123456789012345678901234"), 30),
        "0.123456789012345678901234567890",
    );
    assert_eq!(text(&val(op("$round", vec![dec("NaN")]), none())), "NaN");
}

#[test]
fn a_place_outside_minus_20_to_100_or_not_whole_is_refused() {
    for name in ["$round", "$trunc"] {
        for place in [Bson::Int32(101), Bson::Int32(-21), Bson::Int64(1 << 40)] {
            let message = err(op(name, vec![Bson::Double(1.5), place.clone()]), none());
            assert!(
                message.contains(name) && message.contains("between -20 and 100"),
                "{name} {place:?}: {message}"
            );
        }
        for place in [
            Bson::Double(1.5),
            Bson::String("2".into()),
            Bson::Boolean(true),
            Bson::Double(f64::NAN),
        ] {
            let message = err(op(name, vec![Bson::Double(1.5), place.clone()]), none());
            assert!(message.contains("must be a whole number"), "{name} {place:?}: {message}");
        }
        // The edges are accepted, and a whole double or decimal is a whole number.
        for place in [Bson::Int32(-20), Bson::Int32(100), Bson::Double(2.0), dec("2")] {
            assert!(
                eval(op(name, vec![Bson::Double(1.5), place.clone()]), none()).is_ok(),
                "{place:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// $pow
// ---------------------------------------------------------------------------

fn pow(base: Bson, exponent: Bson) -> Bson {
    val(op("$pow", vec![base, exponent]), none())
}

#[test]
fn pow_of_integers_is_an_integer_while_it_fits() {
    assert_eq!(pow(Bson::Int32(2), Bson::Int32(10)), Bson::Int64(1024));
    assert_eq!(pow(Bson::Int32(2), Bson::Int32(62)), Bson::Int64(1 << 62));
    assert_eq!(pow(Bson::Int32(-2), Bson::Int32(3)), Bson::Int64(-8));
    assert_eq!(pow(Bson::Int32(-2), Bson::Int32(63)), Bson::Int64(i64::MIN));
    assert_eq!(pow(Bson::Int32(7), Bson::Int32(0)), Bson::Int64(1));
    assert_eq!(pow(Bson::Int32(0), Bson::Int32(0)), Bson::Int64(1));
    assert_eq!(pow(Bson::Int32(0), Bson::Int32(5)), Bson::Int64(0));
    assert_eq!(pow(Bson::Int32(1), Bson::Int64(i64::MAX)), Bson::Int64(1));
    assert_eq!(pow(Bson::Int32(-1), Bson::Int64(i64::MAX)), Bson::Int64(-1));
    assert_eq!(pow(Bson::Int32(0), Bson::Int64(i64::MAX)), Bson::Int64(0));
    // Past Int64 it is a double, as an overflowing `$multiply` is.
    assert_eq!(pow(Bson::Int32(2), Bson::Int32(63)), Bson::Double(2f64.powi(63)));
    assert_eq!(pow(Bson::Int32(10), Bson::Int32(30)), Bson::Double(1e30));
}

#[test]
fn pow_with_a_negative_or_fractional_power_is_a_double() {
    assert_eq!(pow(Bson::Int32(2), Bson::Int32(-2)), Bson::Double(0.25));
    assert_eq!(pow(Bson::Int32(4), Bson::Double(0.5)), Bson::Double(2.0));
    assert_eq!(pow(Bson::Double(2.0), Bson::Int32(3)), Bson::Double(8.0));
    assert_eq!(pow(Bson::Double(-2.0), Bson::Int32(3)), Bson::Double(-8.0));
    assert_eq!(pow(Bson::Double(-2.0), Bson::Double(2.0)), Bson::Double(4.0));
    assert_eq!(pow(Bson::Int32(0), Bson::Int32(0)), Bson::Int64(1));
    assert_eq!(pow(Bson::Double(0.0), Bson::Double(2.0)), Bson::Double(0.0));
    assert_eq!(pow(Bson::Double(f64::INFINITY), Bson::Int32(2)), Bson::Double(f64::INFINITY));
    assert_eq!(pow(Bson::Double(10.0), Bson::Int32(400)), Bson::Double(f64::INFINITY));
}

#[test]
fn pow_refuses_a_zero_to_a_negative_power_and_a_negative_base_to_a_fraction() {
    for (base, exponent) in [
        (Bson::Int32(0), Bson::Int32(-1)),
        (Bson::Double(0.0), Bson::Double(-0.5)),
        (Bson::Double(-0.0), Bson::Int32(-3)),
    ] {
        let message = err(op("$pow", vec![base, exponent]), none());
        assert!(message.contains("$pow cannot raise 0 to a negative power"), "{message}");
    }
    for (base, exponent) in [
        (Bson::Int32(-8), Bson::Double(0.5)),
        (Bson::Double(-2.0), Bson::Double(1.0 / 3.0)),
        (dec("-8"), dec("0.5")),
    ] {
        let message = err(op("$pow", vec![base, exponent]), none());
        assert!(message.contains("negative number to a power"), "{message}");
    }
    let message = err(op("$pow", vec![dec("0"), dec("-1")]), none());
    assert!(message.contains("$pow cannot raise"), "{message}");
}

#[test]
fn pow_of_a_decimal_is_exact_and_rounded_once() {
    same(pow(dec("1.1"), Bson::Int32(2)), "1.21");
    same(pow(dec("2"), Bson::Int32(10)), "1024");
    same(pow(Bson::Int32(2), dec("-2")), "0.25");
    same(pow(dec("10"), Bson::Int32(3)), "1E+3");
    same(pow(dec("-2"), Bson::Int32(3)), "-8");
    same(pow(dec("0.5"), Bson::Int32(3)), "0.125");
    same(pow(dec("2"), dec("0.5")), "1.414213562373095048801688724209698");
    same(pow(dec("5"), dec("0")), "1");
    assert_eq!(text(&pow(dec("10"), Bson::Int32(7000))), "Infinity");
    // A double operand enters by its exact value.
    same(pow(Bson::Double(0.5), dec("2")), "0.25");
}

// ---------------------------------------------------------------------------
// $sqrt, $exp, $ln, $log10
// ---------------------------------------------------------------------------

#[test]
fn sqrt_exp_ln_and_log10_of_a_double() {
    assert_eq!(val(one("$sqrt", Bson::Int32(9)), none()), Bson::Double(3.0));
    assert_eq!(val(one("$sqrt", Bson::Double(2.0)), none()), Bson::Double(2f64.sqrt()));
    assert_eq!(val(one("$sqrt", Bson::Int32(0)), none()), Bson::Double(0.0));
    assert_eq!(val(one("$exp", Bson::Int32(0)), none()), Bson::Double(1.0));
    assert!(
        (double(&val(one("$exp", Bson::Int32(1)), none())) - std::f64::consts::E).abs() < 1e-15
    );
    assert_eq!(val(one("$exp", Bson::Double(1000.0)), none()), Bson::Double(f64::INFINITY));
    assert_eq!(val(one("$exp", Bson::Double(f64::NEG_INFINITY)), none()), Bson::Double(0.0));
    assert_eq!(val(one("$ln", Bson::Int32(1)), none()), Bson::Double(0.0));
    assert!(
        (double(&val(one("$ln", Bson::Int32(10)), none())) - std::f64::consts::LN_10).abs() < 1e-15
    );
    assert!((double(&val(one("$log10", Bson::Int32(1000)), none())) - 3.0).abs() < 1e-12);
    assert_eq!(
        val(one("$log10", Bson::Double(f64::INFINITY)), none()),
        Bson::Double(f64::INFINITY)
    );
    for name in ["$sqrt", "$exp", "$ln", "$log10"] {
        assert!(double(&val(one(name, Bson::Double(f64::NAN)), none())).is_nan(), "{name}");
    }
}

#[test]
fn a_domain_error_is_refused_not_a_nan() {
    let message = err(one("$sqrt", Bson::Int32(-4)), none());
    assert_eq!(message, "invalid query: $sqrt needs a number greater than or equal to 0");
    for name in ["$ln", "$log10"] {
        for arg in [
            Bson::Int32(0),
            Bson::Int32(-1),
            Bson::Double(-0.0),
            Bson::Double(f64::NEG_INFINITY),
            dec("0"),
            dec("-3"),
        ] {
            let message = err(one(name, arg.clone()), none());
            assert!(
                message.contains(name) && message.contains("needs a positive number"),
                "{name} {arg:?}: {message}"
            );
        }
    }
    for arg in [Bson::Double(-1e-300), Bson::Double(f64::NEG_INFINITY), dec("-1E-6000")] {
        assert!(err(one("$sqrt", arg.clone()), none()).contains("$sqrt needs"), "{arg:?}");
    }
}

#[test]
fn the_decimal_functions_give_a_decimal_rounded_once() {
    same(val(one("$sqrt", dec("4.00")), none()), "2");
    same(val(one("$sqrt", dec("2")), none()), "1.414213562373095048801688724209698");
    same(val(one("$sqrt", dec("0.0625")), none()), "0.25");
    same(val(one("$sqrt", dec("1E+4000")), none()), "1E+2000");
    same(val(one("$sqrt", dec("0")), none()), "0");
    same(val(one("$exp", dec("0")), none()), "1");
    same(val(one("$exp", dec("1")), none()), "2.718281828459045235360287471352662");
    same(val(one("$exp", dec("-1")), none()), "0.3678794411714423215955237701614609");
    assert_eq!(text(&val(one("$exp", dec("100000")), none())), "Infinity");
    same(val(one("$ln", dec("1")), none()), "0");
    same(val(one("$ln", dec("2")), none()), "0.6931471805599453094172321214581766");
    same(val(one("$ln", dec("10")), none()), "2.302585092994045684017991454684364");
    same(val(one("$log10", dec("1000")), none()), "3");
    same(val(one("$log10", dec("0.001")), none()), "-3");
    same(val(one("$log10", dec("2")), none()), "0.301029995663981195213738894724493");
    // An integer or a double argument to a decimal-valued function is a double.
    assert!(matches!(val(one("$ln", Bson::Int32(2)), none()), Bson::Double(_)));
}

// ---------------------------------------------------------------------------
// $$NOW
// ---------------------------------------------------------------------------

fn add_fields(pipeline: &[Document]) -> Vec<Stage> {
    aggregate::parse(pipeline).unwrap_or_else(|e| panic!("{pipeline:?}: {e}"))
}

fn run(stages: &[Stage], input: Document) -> Vec<Document> {
    let mut docs = vec![input];
    for stage in stages {
        docs = aggregate::apply(stage, docs, &Limits::default()).unwrap();
    }
    docs
}

fn literal(stage: &Stage) -> Bson {
    let Stage::AddFields(computed) = stage else { panic!("not $addFields: {stage:?}") };
    let Expr::Literal(value) = &computed[0].1 else { panic!("not a literal: {:?}", computed[0]) };
    value.clone()
}

#[test]
fn now_is_one_date_for_every_stage_of_the_request() {
    let before = bson::DateTime::now();
    let stages = add_fields(&[
        doc! {"$addFields": {"a": "$$NOW"}},
        doc! {"$addFields": {"b": {"$add": ["$$NOW", 0]}}},
        doc! {"$addFields": {"c": {"$subtract": ["$$NOW", "$a"]}}},
    ]);
    std::thread::sleep(std::time::Duration::from_millis(5));
    let after = bson::DateTime::now();
    let out = run(&stages, doc! {"_id": 1});
    let a = out[0].get_datetime("a").unwrap();
    assert!(*a >= before && *a <= after, "{a:?} not in [{before:?}, {after:?}]");
    assert_eq!(out[0].get_datetime("b").unwrap(), a);
    assert_eq!(out[0].get_i64("c").unwrap(), 0);
    // And it is the same in the next document: it was read when the pipeline was parsed.
    let again = run(&stages, doc! {"_id": 2});
    assert_eq!(again[0].get_datetime("a").unwrap(), a);
}

#[test]
fn a_lookup_sub_pipeline_sees_the_same_now_as_the_pipeline_around_it() {
    let stages = add_fields(&[
        doc! {"$addFields": {"outer": "$$NOW"}},
        doc! {"$lookup": {
            "from": "other",
            "let": {"t": "$$NOW"},
            "pipeline": [
                {"$addFields": {"inner": "$$NOW"}},
                {"$lookup": {"from": "third", "pipeline": [{"$addFields": {"deep": "$$NOW"}}], "as": "x"}},
            ],
            "as": "joined",
        }},
    ]);
    let outer = literal(&stages[0]);
    let Stage::Lookup { join: aggregate::Join::Pipeline { vars, stages: sub, .. }, .. } =
        &stages[1]
    else {
        panic!("not a pipeline $lookup: {:?}", stages[1])
    };
    assert_eq!(vars[0].1, Expr::Literal(outer.clone()));
    assert_eq!(literal(&sub[0]), outer);
    let Stage::Lookup { join: aggregate::Join::Pipeline { stages: deep, .. }, .. } = &sub[1] else {
        panic!("not a nested $lookup")
    };
    assert_eq!(literal(&deep[0]), outer);
}

#[test]
fn each_request_reads_the_clock_again() {
    let first = add_fields(&[doc! {"$addFields": {"a": "$$NOW"}}]);
    std::thread::sleep(std::time::Duration::from_millis(5));
    let second = add_fields(&[doc! {"$addFields": {"a": "$$NOW"}}]);
    assert!(literal(&second[0]).as_datetime() > literal(&first[0]).as_datetime());
}

#[test]
fn now_in_an_expr_filter_and_in_a_bare_expression() {
    let f = filter::parse(&doc! {"$expr": {"$lt": ["$expires", "$$NOW"]}}).unwrap();
    assert!(filter::matches(&f, &doc! {"expires": bson::DateTime::from_millis(0)}).unwrap());
    assert!(
        !filter::matches(&f, &doc! {"expires": bson::DateTime::from_millis(i64::MAX / 2)}).unwrap()
    );
    assert!(matches!(val(Bson::String("$$NOW".into()), none()), Bson::DateTime(_)));
    assert!(matches!(
        val(op("$add", vec![Bson::String("$$NOW".into()), Bson::Int32(1000)]), none()),
        Bson::DateTime(_)
    ));
}

#[test]
fn now_and_remove_have_no_fields_to_read_into() {
    for expr in ["$$NOW.x", "$$REMOVE.x", "$$NOW.", "$$REMOVE."] {
        let message = Expr::parse(&Bson::String(expr.into())).expect_err(expr).to_string();
        assert!(
            message.contains("has no fields") || message.contains("needs a field path"),
            "{expr}: {message}"
        );
    }
}

#[test]
fn other_system_variables_are_still_refused_by_name() {
    for name in ["$$DESCEND", "$$PRUNE", "$$KEEP", "$$SEARCH_META"] {
        let message = Expr::parse(&Bson::String(name.into())).expect_err(name).to_string();
        assert!(
            message.contains(name) && message.contains("$$NOW") && message.contains("$$REMOVE"),
            "{message}"
        );
    }
    // A user variable cannot take either name: names start with a lowercase letter.
    let bound = Expr::parse(&Bson::Document(doc! {"$let": {"vars": {"NOW": 1}, "in": "$$NOW"}}));
    assert!(bound.is_err());
}

// ---------------------------------------------------------------------------
// $$REMOVE
// ---------------------------------------------------------------------------

fn apply_one(stage: Document, input: Document) -> Document {
    run(&add_fields(&[stage]), input).remove(0)
}

#[test]
fn remove_drops_the_field_it_is_the_value_of() {
    let out = apply_one(doc! {"$addFields": {"a": "$$REMOVE", "c": 3}}, doc! {"a": 1, "b": 2});
    assert_eq!(out, doc! {"b": 2, "c": 3});
    let out = apply_one(doc! {"$addFields": {"x": "$$REMOVE"}}, doc! {"a": 1});
    assert_eq!(out, doc! {"a": 1});
    let out = apply_one(doc! {"$addFields": {"o.a": "$$REMOVE"}}, doc! {"o": {"a": 1, "b": 2}});
    assert_eq!(out, doc! {"o": {"b": 2}});
    let out = apply_one(
        doc! {"$project": {"a": 1, "b": "$$REMOVE", "c": "$$REMOVE"}},
        doc! {"_id": 1, "a": 1, "b": 2},
    );
    assert_eq!(out, doc! {"_id": 1, "a": 1});
}

#[test]
fn remove_reaches_the_field_through_cond_switch_and_let() {
    let stage = doc! {"$addFields": {"n": {"$cond": [{"$gt": ["$n", 0]}, "$n", "$$REMOVE"]}}};
    assert_eq!(apply_one(stage.clone(), doc! {"n": 5}), doc! {"n": 5});
    assert_eq!(apply_one(stage, doc! {"n": -5}), doc! {});
    let stage = doc! {"$addFields": {"s": {"$switch": {
        "branches": [{"case": {"$eq": ["$k", 1]}, "then": "one"}, {"case": {"$eq": ["$k", 2]}, "then": "$$REMOVE"}],
        "default": "$$REMOVE",
    }}}};
    assert_eq!(apply_one(stage.clone(), doc! {"k": 1, "s": 0}), doc! {"k": 1, "s": "one"});
    assert_eq!(apply_one(stage.clone(), doc! {"k": 2, "s": 0}), doc! {"k": 2});
    assert_eq!(apply_one(stage, doc! {"k": 3, "s": 0}), doc! {"k": 3});
    let stage = doc! {"$addFields": {"v": {"$let": {"vars": {"z": 0}, "in": {"$cond": [{"$eq": ["$$z", 0], }, "$$REMOVE", 1]}}}}};
    assert_eq!(apply_one(stage, doc! {"v": 9}), doc! {});
}

#[test]
fn remove_inside_a_built_document_drops_that_key_and_inside_an_array_is_null() {
    let out = apply_one(
        doc! {"$addFields": {"o": {"keep": 1, "drop": "$$REMOVE", "n": {"deep": "$$REMOVE", "k": 2}}}},
        doc! {},
    );
    assert_eq!(out, doc! {"o": {"keep": 1, "n": {"k": 2}}});
    let out = apply_one(doc! {"$addFields": {"arr": ["$$REMOVE", 1, {"x": "$$REMOVE"}]}}, doc! {});
    assert_eq!(out, doc! {"arr": [Bson::Null, 1, {}]});
}

#[test]
fn remove_read_as_a_value_is_null() {
    assert_eq!(val(Bson::String("$$REMOVE".into()), none()), Bson::Null);
    assert_eq!(
        val(op("$ifNull", vec![Bson::String("$$REMOVE".into()), Bson::Int32(1)]), none()),
        Bson::Int32(1)
    );
    assert_eq!(
        val(op("$add", vec![Bson::String("$$REMOVE".into()), Bson::Int32(1)]), none()),
        Bson::Null
    );
    assert_eq!(
        val(op("$eq", vec![Bson::String("$$REMOVE".into()), Bson::Null]), none()),
        Bson::Boolean(true)
    );
    // `$replaceRoot` needs a document, and a removed one is not.
    let stages = add_fields(&[doc! {"$replaceRoot": {"newRoot": "$$REMOVE"}}]);
    assert!(aggregate::apply(&stages[0], vec![doc! {"a": 1}], &Limits::default()).is_err());
}

// ---------------------------------------------------------------------------
// One clock read per request
// ---------------------------------------------------------------------------

/// The reads of the wall clock the closure's parses make.
fn reads(parse: impl FnOnce()) -> u64 {
    let before = crate::expr::clock_reads();
    parse();
    crate::expr::clock_reads() - before
}

#[test]
fn a_pipeline_reads_the_clock_once_however_many_places_name_now() {
    let pipeline = [
        doc! {"$match": {"$expr": {"$lt": ["$at", "$$NOW"]}}},
        doc! {"$addFields": {"a": "$$NOW"}},
        doc! {"$lookup": {
            "from": "x", "let": {"t": "$$NOW"},
            "pipeline": [
                {"$match": {"$expr": {"$lt": ["$at", "$$NOW"]}}},
                {"$lookup": {"from": "y", "pipeline": [{"$addFields": {"deep": "$$NOW"}}], "as": "z"}},
            ],
            "as": "j",
        }},
    ];
    assert_eq!(reads(|| drop(aggregate::parse(&pipeline).unwrap())), 1);
    // A pipeline that never names it does not read the clock at all.
    assert_eq!(reads(|| drop(aggregate::parse(&[doc! {"$limit": 1}]).unwrap())), 0);
}

#[test]
fn an_update_its_array_filters_and_its_filter_read_the_clock_once() {
    let update = doc! {"$set": {"items.$[a].x": 1, "items.$[b].y": 1},
    "$pull": {"tags": {"$elemMatch": {"$expr": {"$lt": ["$$ROOT", "$$NOW"]}}}}};
    let filters = vec![
        doc! {"a.at": {"$lt": 5}, "$expr": {"$lt": ["$$a", "$$NOW"]}},
        doc! {"$expr": {"$gt": ["$$b", "$$NOW"]}},
    ];
    assert_eq!(
        reads(|| drop(crate::update::parse_with_filters(&update, &filters).unwrap())),
        1,
        "the update, its arrayFilters and a $pull condition"
    );
    // And a handler that parses the filter beside it joins them with its own clock.
    let one = reads(|| {
        let _clock = crate::expr::RequestClock::open();
        filter::parse(&doc! {"$expr": {"$lt": ["$at", "$$NOW"]}}).unwrap();
        crate::update::parse_with_filters(&update, &filters).unwrap();
    });
    assert_eq!(one, 1);
}

#[test]
fn every_now_of_a_request_is_the_same_date_across_parses_that_share_a_clock() {
    let _clock = crate::expr::RequestClock::open();
    let first = Expr::parse(&Bson::String("$$NOW".into())).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    let second = crate::filter::parse(&doc! {"$expr": {"$lt": ["$at", "$$NOW"]}}).unwrap();
    let third = Expr::parse(&Bson::String("$$NOW".into())).unwrap();
    assert_eq!(first, third);
    drop(second);
}

/// A pipeline of one stage run over one document, which may be refused.
fn try_run(pipeline: &[Document], input: Document) -> kimmy_core::Result<Vec<Document>> {
    let mut docs = vec![input];
    for stage in aggregate::parse(pipeline)? {
        docs = aggregate::apply(&stage, docs, &Limits::default())?;
    }
    Ok(docs)
}

#[test]
fn remove_at_a_path_a_value_could_not_be_written_to_is_refused_with_the_same_error() {
    // Through an array by a name: `$addFields` of a value there is refused, so is leaving it out.
    let value = try_run(&[doc! {"$addFields": {"a.b": 1}}], doc! {"a": [{"b": 1}]})
        .expect_err("a value cannot be written through an array by a name")
        .to_string();
    let removed = try_run(&[doc! {"$addFields": {"a.b": "$$REMOVE"}}], doc! {"a": [{"b": 1}]})
        .expect_err("nor can it be left out")
        .to_string();
    assert_eq!(removed, value);
    assert!(removed.contains("cannot set \"a.b\""), "{removed}");
    // A position in the array leaves a null hole, as `$unset` does; a position that is not
    // there, a field that is not, and a path through a scalar are nothing to remove.
    assert_eq!(
        try_run(&[doc! {"$addFields": {"a.0": "$$REMOVE"}}], doc! {"a": [1, 2]}).unwrap(),
        vec![doc! {"a": [Bson::Null, 2]}]
    );
    assert_eq!(
        try_run(&[doc! {"$addFields": {"a.0.b": "$$REMOVE"}}], doc! {"a": [{"b": 1, "c": 2}]})
            .unwrap(),
        vec![doc! {"a": [{"c": 2}]}]
    );
    for input in [doc! {"a": [1, 2]}, doc! {"a": 5}, doc! {}, doc! {"a": {"c": 1}}] {
        let path =
            if input.get("a").is_some_and(|a| a.as_array().is_some()) { "a.7" } else { "a.b.c" };
        assert_eq!(
            try_run(&[doc! {"$addFields": {path: "$$REMOVE"}}], input.clone()).unwrap(),
            vec![input]
        );
    }
}
