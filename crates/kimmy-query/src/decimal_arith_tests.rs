//! Decimal128 arithmetic through the query layer (ADR-252): the result-type,
//! conversion, special-value, update-guard and cohort tables, one row at a time.
//!
//! The arithmetic itself is checked against an exact-rational reference in
//! `kimmy-core`; these tests pin what the operators do with it: which operands
//! make a decimal, which are refused and in whose words.

use bson::{Bson, Document, doc};
use kimmy_core::decimal::parse;

use crate::aggregate::{self, Limits};
use crate::expr::Expr;
use crate::update;

fn dec(text: &str) -> Bson {
    Bson::Decimal128(parse(text).unwrap_or_else(|e| panic!("{text}: {e}")))
}

fn eval(expr: Bson, doc: Document) -> kimmy_core::Result<Bson> {
    Expr::parse(&expr).unwrap_or_else(|e| panic!("{expr:?}: {e}")).eval(&doc)
}

/// The value of an expression, which must evaluate.
fn val(expr: Bson, doc: Document) -> Bson {
    eval(expr.clone(), doc).unwrap_or_else(|e| panic!("{expr:?}: {e}"))
}

fn err(expr: Bson, doc: Document) -> String {
    eval(expr, doc).expect_err("must be refused").to_string()
}

/// A `Decimal128`'s text, which keeps its cohort member: `2.30` and `2.3` differ.
fn text(value: &Bson) -> String {
    match value {
        Bson::Decimal128(d) => d.to_string(),
        other => panic!("not a decimal: {other:?}"),
    }
}

fn same(value: Bson, want: &str) {
    assert_eq!(text(&value), text(&dec(want)), "want {want}");
}

fn op(name: &str, args: Vec<Bson>) -> Bson {
    Bson::Document(doc! { name: args })
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

#[test]
fn a_decimal_operand_makes_the_result_a_decimal_at_the_ieee_quantum() {
    let none = || doc! {};
    same(val(op("$add", vec![dec("1.10"), dec("1.20")]), none()), "2.30");
    same(val(op("$add", vec![dec("1.5"), dec("0.00")]), none()), "1.50");
    same(val(op("$add", vec![dec("1.5"), Bson::Int32(0)]), none()), "1.5");
    same(val(op("$subtract", vec![dec("1.00"), dec("1.00")]), none()), "0.00");
    same(val(op("$subtract", vec![dec("10.00"), dec("0.50")]), none()), "9.50");
    same(val(op("$multiply", vec![dec("1.5"), dec("1.0")]), none()), "1.50");
    same(val(op("$multiply", vec![dec("0"), dec("1.50")]), none()), "0.00");
    same(val(op("$divide", vec![dec("2.30"), Bson::Int32(2)]), none()), "1.15");
    same(val(op("$divide", vec![Bson::Int32(3), dec("2")]), none()), "1.5");
    same(val(op("$divide", vec![dec("6"), Bson::Int64(3)]), none()), "2");
    same(val(op("$divide", vec![dec("1.00"), Bson::Int32(4)]), none()), "0.25");
    same(
        val(op("$divide", vec![dec("1"), Bson::Int32(3)]), none()),
        "0.3333333333333333333333333333333333",
    );
    same(
        val(op("$add", vec![dec("1"), dec("1E-40")]), none()),
        "1.000000000000000000000000000000000",
    );
    // An integer counts at exponent 0, and `[1E+2, 3]` is `103`.
    same(val(op("$add", vec![dec("1E+2"), Bson::Int32(3)]), none()), "103");
}

#[test]
fn a_sum_and_a_product_of_any_length_are_one_exact_value() {
    let none = || doc! {};
    // The type is decided over all the operands before anything is computed.
    same(
        val(op("$add", vec![Bson::Int64(i64::MAX), Bson::Int32(1), dec("0")]), none()),
        "9223372036854775808",
    );
    same(val(op("$add", vec![dec("1.10"), dec("1.20"), Bson::Int32(3)]), none()), "5.30");
    // The tie to even that a left fold meets twice.
    let sum = |terms: Vec<Bson>| val(op("$add", terms), none());
    let terms = [dec("1E+34"), dec("0.5"), dec("0.5"), dec("-1E+34")];
    same(sum(terms.to_vec()), "1.0");
    same(sum(vec![terms[0].clone(), terms[3].clone(), terms[1].clone(), terms[2].clone()]), "1.0");
    same(sum(terms.iter().rev().cloned().collect()), "1.0");
    // A product is a chain, left to right, each step rounded (as MongoDB
    // multiplies): it is linear in the operands, and the order matters where a
    // step overflows or rounds.
    let product = |ops: Vec<Bson>| val(op("$multiply", ops), none());
    same(product(vec![dec("1E+5000"), dec("1E+5000"), dec("1E-5000")]), "Infinity");
    same(product(vec![dec("1E+5000"), dec("1E-5000"), dec("1E+5000")]), "1E+5000");
    same(product(vec![dec("1E-5000"), dec("1E+5000"), dec("1E+5000")]), "1E+5000");
    same(product(vec![dec("1.5"), dec("1.0"), Bson::Int32(2)]), "3.00");
    // A step that rounds, against the exact product rounded once.
    same(
        product(vec![
            dec("1000000000000000000000000957877941E-34"),
            dec("9999999999999999999999999787852355E-35"),
            dec("1000000000000000000000000407746305E-32"),
        ]),
        "1000000000000000000000001344409481E-34",
    );
    // An integer is exact and a double enters by R1 at its position.
    same(product(vec![Bson::Int32(3), dec("1.5"), Bson::Double(0.5)]), "2.25");
    same(product(vec![Bson::Double(0.1), dec("10")]), "1.000000000000000055511151231257827");
}

#[test]
fn a_double_enters_by_its_exact_binary_value_rounded_once() {
    let at = |expr: Bson| val(expr, doc! { "bal": dec("10.00"), "d": 0.1 });
    // The design's worked example: the double's noise is in the digits.
    same(
        at(op("$add", vec![Bson::from("$bal"), Bson::Double(0.01)])),
        "10.01000000000000000020816681711722",
    );
    same(
        at(op("$add", vec![Bson::Double(0.1), dec("0.2")])),
        "0.3000000000000000055511151231257827",
    );
    // An exact double adds as itself.
    same(at(op("$add", vec![Bson::from("$bal"), Bson::Double(0.25)])), "10.25");
    same(at(op("$multiply", vec![dec("100.00"), Bson::Double(2.0)])), "200.00");
    // A decimal literal and a double are two numbers.
    assert_eq!(
        at(op("$eq", vec![op("$add", vec![Bson::from("$bal"), dec("0.1")]), Bson::Double(10.1)])),
        Bson::Boolean(false)
    );
    // Narrowing would lose the range: this is a decimal, not an infinity.
    same(
        at(op("$add", vec![dec("1E+400"), Bson::Double(1.0)])),
        "1.000000000000000000000000000000000E+400",
    );
}

#[test]
fn special_values_follow_ieee_754() {
    let none = || doc! {};
    let add = |a: Bson, b: Bson| val(op("$add", vec![a, b]), none());
    same(add(dec("Infinity"), dec("-Infinity")), "NaN");
    same(add(dec("Infinity"), dec("5")), "Infinity");
    same(add(dec("5"), dec("-Infinity")), "-Infinity");
    same(add(dec("NaN"), dec("1")), "NaN");
    // Every NaN result is the one quiet NaN, whatever came in.
    let signalling = parse("-sNaN123").unwrap();
    let got = add(Bson::Decimal128(signalling), dec("1"));
    assert_eq!(got, dec("NaN"));
    // A NaN double is a NaN.
    same(add(Bson::Double(f64::NAN), dec("1")), "NaN");
    same(add(Bson::Double(f64::INFINITY), dec("1")), "Infinity");
    let mul = |a: Bson, b: Bson| val(op("$multiply", vec![a, b]), none());
    same(mul(dec("Infinity"), dec("0")), "NaN");
    same(mul(dec("Infinity"), dec("-2")), "-Infinity");
    // Overflow is the infinity in an expression, underflow is gradual.
    same(mul(dec("9.999999999999999999999999999999999E+6144"), Bson::Int32(10)), "Infinity");
    same(mul(dec("1E-6143"), dec("0.1")), "1E-6144");
    same(mul(dec("1E-6000"), dec("1E-6000")), "0E-6176");
    let div = |a: Bson, b: Bson| val(op("$divide", vec![a, b]), none());
    same(div(dec("3E-6176"), Bson::Int32(2)), "2E-6176");
    same(div(dec("1E-6176"), Bson::Int32(2)), "0E-6176");
    same(div(dec("5"), dec("-Infinity")), "-0E-6176");
    same(div(dec("Infinity"), dec("Infinity")), "NaN");
    // The sign of a zero.
    same(add(dec("-0"), dec("-0")), "-0");
    same(add(dec("-0"), dec("0")), "0");
    same(add(dec("1.00"), dec("-1.00")), "0.00");
}

#[test]
fn a_zero_divisor_is_refused_in_every_spelling() {
    for zero in ["0", "-0", "0E-100", "0.0", "-0.0", "0E+6111"] {
        let msg = err(op("$divide", vec![dec("1"), dec(zero)]), doc! {});
        assert_eq!(msg, "invalid query: $divide by zero", "{zero}");
        // Whatever the dividend, a NaN included.
        let msg = err(op("$divide", vec![dec("NaN"), dec(zero)]), doc! {});
        assert_eq!(msg, "invalid query: $divide by zero", "{zero}");
    }
    // A plain zero divisor under a decimal dividend.
    for zero in [Bson::Int32(0), Bson::Int64(0), Bson::Double(0.0), Bson::Double(-0.0)] {
        assert_eq!(
            err(op("$divide", vec![dec("5"), zero.clone()]), doc! {}),
            "invalid query: $divide by zero",
            "{zero:?}"
        );
    }
    // The non-canonical forms of zero: a coefficient above 10^34 - 1, and the
    // large-coefficient form.
    for bits in [(((1u128 << 113) - 1) | (6176u128 << 113)), (0b11u128 << 125) | (6176u128 << 111)]
    {
        let zero = Bson::Decimal128(bson::Decimal128::from_bytes(bits.to_le_bytes()));
        assert_eq!(
            err(op("$divide", vec![dec("1"), zero]), doc! {}),
            "invalid query: $divide by zero"
        );
    }
}

#[test]
fn null_propagates_and_other_types_keep_the_old_words() {
    let none = || doc! {};
    assert_eq!(val(op("$add", vec![dec("1"), Bson::Null]), none()), Bson::Null);
    assert_eq!(val(op("$multiply", vec![dec("1"), Bson::from("$gone")]), none()), Bson::Null);
    assert_eq!(val(op("$divide", vec![Bson::Null, dec("1")]), none()), Bson::Null);
    assert_eq!(val(op("$subtract", vec![dec("1"), Bson::Null]), none()), Bson::Null);
    assert_eq!(
        err(op("$add", vec![dec("1"), Bson::from("x")]), none()),
        "invalid query: $add needs numbers, found a string"
    );
    // A date with a decimal is refused as it was.
    let date = Bson::DateTime(bson::DateTime::from_millis(0));
    assert_eq!(
        err(op("$add", vec![date.clone(), dec("1")]), none()),
        "invalid query: $add needs numbers or one date, found a Decimal128"
    );
    assert_eq!(
        err(op("$subtract", vec![date, dec("1")]), none()),
        "invalid query: $subtract needs a date or a number, found a Decimal128"
    );
    // `$mod` refuses a decimal by name.
    assert_eq!(
        err(op("$mod", vec![dec("5"), Bson::Int32(2)]), none()),
        "invalid query: $mod needs numbers, found a Decimal128"
    );
    // Arithmetic with no decimal in it is what it always was.
    assert_eq!(val(op("$add", vec![Bson::Int32(1), Bson::Int32(2)]), none()), Bson::Int64(3));
    assert_eq!(val(op("$divide", vec![Bson::Int32(1), Bson::Int32(2)]), none()), Bson::Double(0.5));
    assert_eq!(val(op("$add", vec![Bson::Int32(1), Bson::Double(0.5)]), none()), Bson::Double(1.5));
}

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

fn convert(to: &str, input: Bson) -> Bson {
    val(Bson::Document(doc! { "$convert": { "input": input, "to": to } }), doc! {})
}

#[test]
fn to_decimal_converts_by_the_design_table() {
    let to = |input: Bson| val(Bson::Document(doc! { "$toDecimal": input }), doc! {});
    same(to(Bson::Int32(7)), "7");
    same(to(Bson::Int64(i64::MIN)), "-9223372036854775808");
    same(to(Bson::Double(0.1)), "0.1000000000000000055511151231257827");
    same(to(Bson::Double(2.5)), "2.5");
    same(to(Bson::Double(-0.0)), "-0");
    same(to(Bson::Double(f64::NAN)), "NaN");
    same(to(Bson::Double(f64::NEG_INFINITY)), "-Infinity");
    same(to(Bson::Boolean(true)), "1");
    same(to(Bson::Boolean(false)), "0");
    same(to(Bson::DateTime(bson::DateTime::from_millis(1500))), "1500");
    // A decimal is itself, the same bytes.
    assert_eq!(to(dec("2.30")), dec("2.30"));
    // A string is read by the strict parser, keeping the written quantum.
    same(to(Bson::from("1.50")), "1.50");
    same(to(Bson::from("1E+3")), "1E+3");
    same(to(Bson::from("-Infinity")), "-Infinity");
    // The shorthand and `$convert` by name and by code are the one operator.
    assert_eq!(convert("decimal", Bson::Int32(2)), dec("2"));
    assert_eq!(
        val(Bson::Document(doc! { "$convert": { "input": 2, "to": 19 } }), doc! {}),
        dec("2")
    );
    // Null and missing answer `onNull`, as for every target.
    assert_eq!(val(Bson::Document(doc! { "$toDecimal": Bson::Null }), doc! {}), Bson::Null);
    assert_eq!(val(Bson::Document(doc! { "$toDecimal": "$gone" }), doc! {}), Bson::Null);
}

#[test]
fn a_string_that_is_not_exactly_a_decimal_is_a_conversion_error() {
    for bad in [
        "",
        ".",
        "++1",
        "e100",
        " 1",
        "1 ",
        "0x10",
        // A nonzero digit past the 34th would need rounding.
        "1.0000000000000000000000000000000001",
        // Above the range, and below it.
        "1E+6145",
        "1E-6177",
    ] {
        let msg = match eval(Bson::Document(doc! { "$toDecimal": bad }), doc! {}) {
            Ok(accepted) => panic!("accepted {bad:?} as {accepted:?}"),
            Err(e) => e.to_string(),
        };
        assert!(msg.contains("cannot read") && msg.contains("as a decimal"), "{bad:?}: {msg}");
        // `onError` is the escape.
        let rescued = val(
            Bson::Document(
                doc! { "$convert": { "input": bad, "to": "decimal", "onError": "bad" } },
            ),
            doc! {},
        );
        assert_eq!(rescued, Bson::from("bad"), "{bad:?}");
    }
    // Trailing zeros that only need dropping are accepted.
    same(
        val(
            Bson::Document(doc! { "$toDecimal": "1.0000000000000000000000000000000000000" }),
            doc! {},
        ),
        "1.000000000000000000000000000000000",
    );
    // Another type is a conversion error.
    let msg = err(Bson::Document(doc! { "$toDecimal": { "$literal": [1] } }), doc! {});
    assert!(msg.contains("cannot convert an array to decimal"), "{msg}");
}

#[test]
fn a_decimal_converts_to_the_other_types() {
    let to = |name: &str, d: &str| eval(Bson::Document(doc! { name: dec(d) }), doc! {});
    // Double: the nearest, half to even.
    assert_eq!(to("$toDouble", "0.1").unwrap(), Bson::Double(0.1));
    assert_eq!(to("$toDouble", "1E+400").unwrap(), Bson::Double(f64::INFINITY));
    assert_eq!(to("$toDouble", "1E-400").unwrap(), Bson::Double(0.0));
    assert_eq!(to("$toDouble", "1E-320").unwrap(), Bson::Double(1e-320));
    assert_eq!(to("$toDouble", "-0").unwrap().as_f64().unwrap().to_bits(), (-0.0f64).to_bits());
    assert!(to("$toDouble", "NaN").unwrap().as_f64().unwrap().is_nan());
    // Long and int truncate toward zero.
    assert_eq!(to("$toLong", "2.9").unwrap(), Bson::Int64(2));
    assert_eq!(to("$toLong", "-2.9").unwrap(), Bson::Int64(-2));
    assert_eq!(to("$toInt", "2.9").unwrap(), Bson::Int32(2));
    assert_eq!(to("$toLong", "9223372036854775807.9").unwrap(), Bson::Int64(i64::MAX));
    for bad in ["NaN", "Infinity", "-Infinity", "9223372036854775808", "1E+30"] {
        assert!(to("$toLong", bad).is_err(), "$toLong {bad}");
    }
    assert!(to("$toInt", "2147483648").is_err());
    assert_eq!(to("$toInt", "2147483647").unwrap(), Bson::Int32(i32::MAX));
    // String keeps the cohort.
    assert_eq!(to("$toString", "2.30").unwrap(), Bson::from("2.30"));
    assert_eq!(to("$toString", "1E+3").unwrap(), Bson::from("1E+3"));
    assert_eq!(to("$toString", "-0").unwrap(), Bson::from("-0"));
    assert_eq!(to("$toString", "NaN").unwrap(), Bson::from("NaN"));
    // Bool is truthiness: a zero of any sign or exponent is false.
    assert_eq!(to("$toBool", "0.00").unwrap(), Bson::Boolean(false));
    assert_eq!(to("$toBool", "-0E-100").unwrap(), Bson::Boolean(false));
    assert_eq!(to("$toBool", "0.01").unwrap(), Bson::Boolean(true));
    assert_eq!(to("$toBool", "NaN").unwrap(), Bson::Boolean(true));
    // Date: truncated to whole milliseconds from the exact value.
    assert_eq!(to("$toDate", "1500.9").unwrap(), Bson::DateTime(bson::DateTime::from_millis(1500)));
    assert_eq!(
        to("$toDate", "-1500.9").unwrap(),
        Bson::DateTime(bson::DateTime::from_millis(-1500))
    );
    assert!(to("$toDate", "Infinity").is_err());
    assert!(to("$toDate", "1E+30").is_err());
    // Not to an ObjectId.
    assert!(to("$toObjectId", "1").is_err());
    // Round trip: a double's R1 converts back to the double.
    for d in [0.1, 2.5, -1e300, 5e-324, 1.7976931348623157e308, 123456.789] {
        let back = val(Bson::Document(doc! { "$toDouble": { "$toDecimal": d } }), doc! {});
        assert_eq!(back, Bson::Double(d), "{d:e}");
    }
}

// ---------------------------------------------------------------------------
// Accumulators
// ---------------------------------------------------------------------------

fn group(accumulator: &str, values: Vec<Bson>) -> Bson {
    let stages =
        aggregate::parse(&[doc! { "$group": { "_id": null, "r": { accumulator: "$v" } } }])
            .unwrap();
    let input: Vec<Document> = values.into_iter().map(|v| doc! { "v": v }).collect();
    let out = aggregate::apply(&stages[0], input, &Limits::default()).unwrap();
    out[0].get("r").unwrap().clone()
}

#[test]
fn sum_is_exact_over_integers_doubles_and_decimals() {
    same(group("$sum", vec![Bson::Int32(1), Bson::Double(0.5), dec("0.25")]), "1.75");
    same(group("$sum", vec![dec("1.10"), dec("1.20"), Bson::Int32(3)]), "5.30");
    same(group("$sum", vec![dec("1E+2"), Bson::Int32(3)]), "103");
    // A double enters by R1.
    same(group("$sum", vec![Bson::Double(0.1), dec("0")]), "0.1000000000000000055511151231257827");
    // A NaN double or a decimal infinity decide the sum.
    same(group("$sum", vec![Bson::Double(f64::NAN), dec("1")]), "NaN");
    same(group("$sum", vec![Bson::Double(f64::INFINITY), dec("1")]), "Infinity");
    same(group("$sum", vec![dec("Infinity"), dec("-Infinity")]), "NaN");
    // Non-numeric values are skipped, as ever.
    same(group("$sum", vec![dec("1"), Bson::from("x"), Bson::Null]), "1");
    // Large integers add exactly into the decimal.
    same(
        group("$sum", vec![Bson::Int64(i64::MAX), Bson::Int64(i64::MAX), dec("0")]),
        "18446744073709551614",
    );
    // A group with no decimal is what it was.
    assert_eq!(group("$sum", vec![Bson::Int32(1), Bson::Int64(2)]), Bson::Int64(3));
    assert_eq!(group("$sum", vec![Bson::Int32(1), Bson::Double(0.5)]), Bson::Double(1.5));
    assert_eq!(group("$sum", vec![Bson::from("x")]), Bson::Int64(0));
}

#[test]
fn avg_divides_the_exact_sum_and_rounds_once() {
    same(
        group("$avg", vec![dec("1.00"), dec("2.00"), dec("2.00")]),
        "1.666666666666666666666666666666667",
    );
    same(group("$avg", vec![dec("1.10"), dec("1.20")]), "1.15");
    same(
        group("$avg", vec![dec("1E+6111"), dec("1E-6176")]),
        "5.000000000000000000000000000000000E+6110",
    );
    // Not the rounded sum divided: that is one unit higher.
    same(
        group(
            "$avg",
            vec![
                dec("2.693319332926108795000640676319904E+35"),
                dec("7.447367703126639551991307315256628E+34"),
            ],
        ),
        "1.719028051619386375099885703922783E+35",
    );
    // Integers and doubles join the exact mean once a decimal is among them.
    same(group("$avg", vec![Bson::Int32(1), Bson::Int32(2), dec("0")]), "1");
    // A group with no decimal averages as a double, and an empty one is null.
    assert_eq!(group("$avg", vec![Bson::Int32(1), Bson::Int32(2)]), Bson::Double(1.5));
    assert_eq!(group("$avg", vec![Bson::from("x")]), Bson::Null);
}

#[test]
fn a_sum_does_not_depend_on_the_order_of_the_documents() {
    let values = vec![
        dec("1E+34"),
        dec("0.5"),
        Bson::Double(0.1),
        dec("0.5"),
        Bson::Int64(7),
        dec("-1E+34"),
        Bson::Double(2.5),
    ];
    let want = (group("$sum", values.clone()), group("$avg", values.clone()));
    for rotation in 0..values.len() {
        let mut order = values.clone();
        order.rotate_left(rotation);
        assert_eq!((group("$sum", order.clone()), group("$avg", order.clone())), want);
        order.reverse();
        assert_eq!((group("$sum", order.clone()), group("$avg", order)), want);
    }
}

// ---------------------------------------------------------------------------
// Updates
// ---------------------------------------------------------------------------

fn upd(update: Document, mut doc: Document) -> kimmy_core::Result<Document> {
    let parsed = update::parse(&update).unwrap_or_else(|e| panic!("refused {update}: {e}"));
    update::apply(&parsed, &mut doc, 0)?;
    Ok(doc)
}

fn after(update: Document, doc: Document) -> Document {
    upd(update.clone(), doc).unwrap_or_else(|e| panic!("{update}: {e}"))
}

fn field(doc: &Document, key: &str) -> Bson {
    doc.get(key).unwrap().clone()
}

#[test]
fn inc_and_mul_take_a_decimal_operand() {
    // An absent field starts as the integer 0.
    same(field(&after(doc! { "$inc": { "n": dec("1.50") } }, doc! {}), "n"), "1.50");
    same(field(&after(doc! { "$inc": { "n": dec("1E+3") } }, doc! {}), "n"), "1000");
    same(field(&after(doc! { "$mul": { "n": dec("1.50") } }, doc! {}), "n"), "0.00");
    // A double operand with no decimal in sight stays a double.
    assert_eq!(field(&after(doc! { "$inc": { "n": 0.5 } }, doc! {}), "n"), Bson::Double(0.5));
    // Integers become decimals, exactly.
    same(field(&after(doc! { "$inc": { "n": dec("0.5") } }, doc! { "n": 5 }), "n"), "5.5");
    same(field(&after(doc! { "$inc": { "n": dec("0") } }, doc! { "n": 5 }), "n"), "5");
    same(
        field(&after(doc! { "$mul": { "n": dec("1.10") } }, doc! { "n": Bson::Int64(10) }), "n"),
        "11.00",
    );
    // A stored decimal takes an integer or a decimal.
    same(
        field(&after(doc! { "$inc": { "bal": 1 } }, doc! { "bal": dec("10.00") }), "bal"),
        "11.00",
    );
    same(
        field(&after(doc! { "$inc": { "bal": dec("0.01") } }, doc! { "bal": dec("10.00") }), "bal"),
        "10.01",
    );
    same(
        field(
            &after(doc! { "$mul": { "price": dec("1.1") } }, doc! { "price": dec("100.00") }),
            "price",
        ),
        "110.000",
    );
    // A stored double meets a decimal operand by R1, exact or not.
    same(
        field(&after(doc! { "$inc": { "v": dec("1") } }, doc! { "v": 0.1 }), "v"),
        "1.100000000000000005551115123125783",
    );
}

#[test]
fn a_double_operand_that_is_not_an_exact_decimal_is_refused_on_a_decimal_field() {
    let msg = upd(doc! { "$inc": { "bal": 0.01 } }, doc! { "bal": dec("10.00") })
        .unwrap_err()
        .to_string();
    assert!(
        msg.contains("$inc on field \"bal\": the double 0.01 is not exactly a decimal")
            && msg.contains("0.01000000000000000020816681711721685")
            && msg.contains("{\"$numberDecimal\": \"0.01\"}"),
        "{msg}"
    );
    let msg = upd(doc! { "$mul": { "price": 1.1 } }, doc! { "price": dec("100.00") })
        .unwrap_err()
        .to_string();
    assert!(
        msg.contains("$mul on field \"price\": the double 1.1 is not exactly a decimal")
            && msg.contains("1.100000000000000088817841970012523")
            && msg.contains("{\"$numberDecimal\": \"1.1\"}"),
        "{msg}"
    );
    // Exact doubles are applied: 0.5, 0.25, 2.0, 1e20, the infinities and a NaN.
    same(
        field(&after(doc! { "$inc": { "bal": 0.25 } }, doc! { "bal": dec("10.00") }), "bal"),
        "10.25",
    );
    same(
        field(&after(doc! { "$inc": { "bal": 2.0 } }, doc! { "bal": dec("10.00") }), "bal"),
        "12.00",
    );
    same(
        field(&after(doc! { "$mul": { "bal": 0.5 } }, doc! { "bal": dec("10.00") }), "bal"),
        "5.000",
    );
    same(
        field(&after(doc! { "$inc": { "bal": 1e20 } }, doc! { "bal": dec("1") }), "bal"),
        "100000000000000000001",
    );
    same(
        field(&after(doc! { "$inc": { "bal": f64::INFINITY } }, doc! { "bal": dec("1") }), "bal"),
        "Infinity",
    );
    same(
        field(&after(doc! { "$inc": { "bal": f64::NAN } }, doc! { "bal": dec("1") }), "bal"),
        "NaN",
    );
    // Every integer and every decimal.
    same(
        field(
            &after(doc! { "$inc": { "bal": Bson::Int64(i64::MIN) } }, doc! { "bal": dec("0") }),
            "bal",
        ),
        "-9223372036854775808",
    );
    // A stored double is not guarded: its R1 is the value the database holds.
    assert!(upd(doc! { "$inc": { "v": dec("0.01") } }, doc! { "v": 0.1 }).is_ok());
    // And with no decimal on either side the guard has nothing to say.
    assert_eq!(
        field(&after(doc! { "$inc": { "v": 0.01 } }, doc! { "v": 0.1 }), "v"),
        Bson::Double(0.1 + 0.01)
    );
    // Under an array path each element is checked, and one refusal refuses the update.
    let doc = doc! { "items": [{ "q": dec("1") }, { "q": dec("2") }] };
    assert!(upd(doc! { "$inc": { "items.$[].q": 0.01 } }, doc.clone()).is_err());
    let doc = after(doc! { "$inc": { "items.$[].q": 0.5 } }, doc);
    assert_eq!(doc.get_array("items").unwrap()[1], Bson::Document(doc! { "q": dec("2.5") }));
}

#[test]
fn overflow_is_refused_and_underflow_is_accepted_in_updates() {
    let top = "9.999999999999999999999999999999999E+6144";
    let msg = upd(doc! { "$mul": { "v": 10 } }, doc! { "v": dec(top) }).unwrap_err().to_string();
    assert!(msg.contains("overflowed a Decimal128"), "{msg}");
    let msg =
        upd(doc! { "$inc": { "v": dec(top) } }, doc! { "v": dec(top) }).unwrap_err().to_string();
    assert!(msg.contains("overflowed a Decimal128"), "{msg}");
    // An infinite input is not an overflow: its IEEE result is stored.
    same(
        field(&after(doc! { "$mul": { "v": 10 } }, doc! { "v": dec("Infinity") }), "v"),
        "Infinity",
    );
    same(
        field(
            &after(doc! { "$inc": { "v": dec("-Infinity") } }, doc! { "v": dec("Infinity") }),
            "v",
        ),
        "NaN",
    );
    // Underflow is gradual and accepted.
    same(
        field(&after(doc! { "$mul": { "v": dec("1E-6000") } }, doc! { "v": dec("1E-6000") }), "v"),
        "0E-6176",
    );
    same(
        field(&after(doc! { "$mul": { "v": dec("0.1") } }, doc! { "v": dec("1E-6143") }), "v"),
        "1E-6144",
    );
    // An inexact result from exact operands is rounded and stored.
    same(
        field(
            &after(
                doc! { "$inc": { "v": dec("0.001") } },
                doc! { "v": dec("1234567890123456789012345678901234") },
            ),
            "v",
        ),
        "1234567890123456789012345678901234",
    );
    // A stored value that is not a number is refused as it was.
    let msg = upd(doc! { "$inc": { "v": dec("1") } }, doc! { "v": "x" }).unwrap_err().to_string();
    assert!(msg.contains("non-numeric field"), "{msg}");
}

#[test]
fn a_value_preserving_update_changes_the_bytes_only_when_the_cohort_member_changes() {
    // ADR-208 reads "the same" as the same stored bytes: these are the rows.
    let same_bytes = |update: Document, stored: Bson| {
        let before = doc! { "n": stored };
        after(update, before.clone()) == before
    };
    assert!(same_bytes(doc! { "$inc": { "n": 0 } }, dec("1.5")));
    assert!(same_bytes(doc! { "$inc": { "n": 0.0 } }, dec("1.5")));
    assert!(same_bytes(doc! { "$inc": { "n": -0.0 } }, dec("1.5")));
    assert!(same_bytes(doc! { "$mul": { "n": 1 } }, dec("1.5")));
    assert!(same_bytes(doc! { "$inc": { "n": 0 } }, dec("0.00")));
    assert!(same_bytes(doc! { "$inc": { "n": 0 } }, dec("NaN")));
    // Another member of the cohort is a different value to a write.
    assert!(!same_bytes(doc! { "$inc": { "n": dec("0.00") } }, dec("1.5")));
    assert!(!same_bytes(doc! { "$mul": { "n": dec("1.0") } }, dec("1.5")));
    assert!(!same_bytes(doc! { "$inc": { "n": 0 } }, dec("-0")));
    assert!(!same_bytes(doc! { "$inc": { "n": 0 } }, Bson::Decimal128(parse("-sNaN7").unwrap())));
    assert!(!same_bytes(doc! { "$inc": { "n": dec("0") } }, Bson::Int32(5)));
    let non_canonical = Bson::Decimal128(bson::Decimal128::from_bytes(
        (((1u128 << 113) - 1) | (6176u128 << 113)).to_le_bytes(),
    ));
    assert!(!same_bytes(doc! { "$inc": { "n": 0 } }, non_canonical));
}

#[test]
fn add_over_a_list_is_sum_over_the_same_values() {
    // `$add` of the operands and `$sum` of a group holding them are one exact
    // sum, rounded once: the same bytes, whatever the values.
    let lists: [Vec<Bson>; 5] = [
        vec![dec("1.10"), dec("1.20"), Bson::Int32(3)],
        vec![dec("1E+34"), dec("0.5"), dec("0.5"), dec("-1E+34")],
        vec![Bson::Double(0.1), Bson::Double(0.2), dec("0.3")],
        vec![dec("-0"), dec("-0")],
        vec![Bson::Int64(i64::MAX), Bson::Int64(i64::MAX), dec("1E-5")],
    ];
    for values in lists {
        let added = val(op("$add", values.clone()), doc! {});
        assert_eq!(added, group("$sum", values.clone()), "{values:?}");
    }
}
