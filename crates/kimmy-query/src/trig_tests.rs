//! The trigonometric operators, `$atan2` and the two-argument `$log` (the reference's answers
//! for every operand type: `tests/data/q8d_reference.tsv`, recorded from MongoDB 6.0.5 by
//! `q8-probes/q8d.js` and `q8d-log.js`), which operand gives which type, and in whose words
//! each is refused.
//!
//! The doubles are compared bit for bit. A `Decimal128` answer is held to the reference's
//! within three units of its last place, and the exact points (zeros, infinities, NaN) to the
//! same digits and cohort: the decimal functions are correctly rounded, and the reference's
//! library is not (see `kimmy-core`'s `decimal_trig_reference` test).

use bson::{Bson, doc};
use kimmy_core::decimal::{Raw, decode_raw, parse};

use crate::expr::Expr;

fn dec(text: &str) -> Bson {
    Bson::Decimal128(parse(text).unwrap_or_else(|e| panic!("{text}: {e}")))
}

fn eval(expr: Bson) -> kimmy_core::Result<Bson> {
    Expr::parse(&expr).unwrap_or_else(|e| panic!("{expr:?}: {e}")).eval(&doc! {})
}

fn val(expr: Bson) -> Bson {
    eval(expr.clone()).unwrap_or_else(|e| panic!("{expr:?}: {e}"))
}

fn op(name: &str, args: Vec<Bson>) -> Bson {
    Bson::Document(doc! { name: args })
}

/// The words of a refusal, without the class the error prints in front of them.
fn err(expr: Bson) -> String {
    let text = eval(expr).expect_err("must be refused").to_string();
    text.strip_prefix("invalid query: ").unwrap_or(&text).to_string()
}

fn double(value: &Bson) -> f64 {
    match value {
        Bson::Double(d) => *d,
        other => panic!("not a double: {other:?}"),
    }
}

/// An operand as the fixture spells it: `null`, `missing`, `i5`, `l5`, `d1.5`, `D1.5`, `sabc`,
/// `arr`, `bool`, `date0`, `obj`.
fn operand(code: &str) -> Bson {
    match code {
        "null" => Bson::Null,
        "missing" => Bson::String("$nope".into()),
        "arr" => Bson::Array(vec![Bson::Int32(1)]),
        "bool" => Bson::Boolean(true),
        "date0" => Bson::DateTime(bson::DateTime::from_millis(0)),
        "obj" => Bson::Document(doc! { "a": 1 }),
        _ => {
            let (kind, rest) = code.split_at(1);
            match kind {
                "i" => Bson::Int32(rest.parse().unwrap()),
                "l" => Bson::Int64(rest.parse().unwrap()),
                "d" => Bson::Double(rest.parse().unwrap()),
                "D" => dec(rest),
                "s" => Bson::String(rest.into()),
                other => panic!("operand {code}: {other}"),
            }
        }
    }
}

/// `raw` with its coefficient widened to 34 digits.
fn widened(raw: &Raw) -> Raw {
    match *raw {
        Raw::Finite { negative, coefficient, exponent } if coefficient != 0 => {
            let pad = 34 - coefficient.ilog10() - 1;
            Raw::Finite {
                negative,
                coefficient: coefficient * 10u128.pow(pad),
                exponent: exponent - pad as i16,
            }
        }
        other => other,
    }
}

/// Whether two decimals are within three units of the last place of 34 digits.
fn close(a: &Raw, b: &Raw) -> bool {
    let (
        Raw::Finite { negative: na, coefficient: ca, exponent: ea },
        Raw::Finite { negative: nb, coefficient: cb, exponent: eb },
    ) = (widened(a), widened(b))
    else {
        return a == b;
    };
    if ca == 0 || cb == 0 {
        return ca == cb;
    }
    let (high, low, shift) = if ea >= eb { (ca, cb, ea - eb) } else { (cb, ca, eb - ea) };
    na == nb && shift <= 3 && (high * 10u128.pow(shift as u32)).abs_diff(low) <= 3
}

/// Whether `got` is the reference's `want` (a fixture code): bit for bit for a double, exact
/// for an integer, and for a decimal the same value to within the last digits.
fn agrees(got: &Bson, want: &str) -> bool {
    match (got, &want[..1]) {
        (Bson::Null, _) => want == "null",
        (Bson::Double(d), "d") => {
            let w: f64 = want[1..].parse().unwrap();
            d.to_bits() == w.to_bits() || (d.is_nan() && w.is_nan())
        }
        (Bson::Int32(n), "i") => want[1..].parse::<i32>().unwrap() == *n,
        (Bson::Int64(n), "l") => want[1..].parse::<i64>().unwrap() == *n,
        (Bson::Decimal128(d), "D") => {
            let Bson::Decimal128(w) = dec(&want[1..]) else { unreachable!() };
            d == &w || close(&decode_raw(d), &decode_raw(&w))
        }
        _ => false,
    }
}

#[test]
fn every_recorded_answer_is_the_references() {
    let table = include_str!("../tests/data/q8d_reference.tsv");
    let mut checked = 0;
    let mut exact = 0;
    let mut wrong = Vec::new();
    for line in table.lines() {
        let mut cols = line.split('\t');
        let (name, args, want) = (cols.next().unwrap(), cols.next().unwrap(), cols.next().unwrap());
        let call = op(name, args.split('|').map(operand).collect());
        let got = eval(call);
        checked += 1;
        match (&got, want.strip_prefix('E')) {
            (Err(e), Some(message)) => {
                if !e.to_string().contains(message) {
                    wrong.push(format!("{name} {args}: wanted `{message}`, got `{e}`"));
                }
            }
            (Ok(value), None) => {
                if !agrees(value, want) {
                    wrong.push(format!("{name} {args}: want {want}, got {value:?}"));
                } else if matches!(value, Bson::Decimal128(_)) {
                    exact += usize::from(agrees_exactly(value, want));
                }
            }
            (got, _) => wrong.push(format!("{name} {args}: want {want}, got {got:?}")),
        }
    }
    assert!(wrong.is_empty(), "{} of {checked}:\n{}", wrong.len(), wrong.join("\n"));
    assert!(checked > 1500, "{checked}");
    assert!(exact > 50, "{exact}");
}

fn agrees_exactly(got: &Bson, want: &str) -> bool {
    matches!(got, Bson::Decimal128(d) if Bson::Decimal128(*d) == dec(&want[1..]))
}

// ---------------------------------------------------------------------------
// Forms and refusals
// ---------------------------------------------------------------------------

#[test]
fn an_operator_takes_its_operand_bare_or_in_a_list() {
    let bare = val(Bson::Document(doc! { "$sin": 1 }));
    let listed = val(op("$sin", vec![Bson::Int32(1)]));
    assert_eq!(double(&bare).to_bits(), double(&listed).to_bits());
    assert_eq!(double(&bare), 1f64.sin());
    // A field read gives the field's value, and a missing one is null.
    let d = doc! { "x": 0.5 };
    let read = |name: &str, field: &str| {
        Expr::parse(&Bson::Document(doc! { name: field })).unwrap().eval(&d).unwrap()
    };
    assert_eq!(double(&read("$atan", "$x")), 0.5f64.atan());
    assert_eq!(read("$atan", "$missing"), Bson::Null);
}

#[test]
fn the_number_of_operands_is_checked_when_the_expression_is_parsed() {
    for (call, found) in [
        (op("$sin", vec![]), 0),
        (op("$sin", vec![Bson::Int32(1), Bson::Int32(2)]), 2),
        (op("$atan2", vec![Bson::Int32(1)]), 1),
        (op("$atan2", vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]), 3),
        (op("$log", vec![Bson::Int32(10)]), 1),
        (op("$log", vec![]), 0),
        (op("$log", vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]), 3),
    ] {
        let message = Expr::parse(&call).expect_err("must be refused").to_string();
        assert!(
            message.contains("argument(s)") && message.contains(&format!("found {found}")),
            "{call:?}: {message}"
        );
    }
    assert!(Expr::parse(&Bson::Document(doc! { "$atan2": 1 })).is_err());
    assert!(Expr::parse(&Bson::Document(doc! { "$log": 10 })).is_err());
}

#[test]
fn the_answer_is_a_double_for_every_integer_width() {
    for operand in [Bson::Int32(1), Bson::Int64(1), Bson::Double(1.0)] {
        assert!(matches!(val(op("$cos", vec![operand.clone()])), Bson::Double(_)));
        assert!(matches!(val(op("$log", vec![operand.clone(), Bson::Int32(2)])), Bson::Double(_)));
    }
    assert!(matches!(val(op("$sin", vec![dec("1")])), Bson::Decimal128(_)));
}

#[test]
fn a_refusal_names_the_number_as_the_reference_prints_it() {
    let asin = |x: Bson| err(op("$asin", vec![x]));
    assert_eq!(asin(Bson::Int32(2)), "cannot apply $asin to 2, value must be in [-1,1]");
    assert_eq!(asin(Bson::Double(1.5)), "cannot apply $asin to 1.5, value must be in [-1,1]");
    assert_eq!(asin(Bson::Double(1e30)), "cannot apply $asin to 1e+30, value must be in [-1,1]");
    assert_eq!(
        asin(Bson::Double(123456789.12345679)),
        "cannot apply $asin to 1.23457e+08, value must be in [-1,1]"
    );
    assert_eq!(
        asin(Bson::Double(123456.7)),
        "cannot apply $asin to 123457, value must be in [-1,1]"
    );
    assert_eq!(
        asin(Bson::Int64(9007199254740993)),
        "cannot apply $asin to 9.0072e+15, value must be in [-1,1]"
    );
    assert_eq!(
        asin(Bson::Int64(i64::MIN)),
        "cannot apply $asin to -9.22337e+18, value must be in [-1,1]"
    );
    assert_eq!(
        asin(Bson::Double(f64::INFINITY)),
        "cannot apply $asin to inf, value must be in [-1,1]"
    );
    assert_eq!(
        err(op("$acosh", vec![Bson::Double(5e-324)])),
        "cannot apply $acosh to 4.94066e-324, value must be in [1,inf]"
    );
    assert_eq!(
        err(op("$acosh", vec![Bson::Double(1e-7)])),
        "cannot apply $acosh to 1e-07, value must be in [1,inf]"
    );
    assert_eq!(
        err(op("$acosh", vec![Bson::Double(1.0 / 3.0)])),
        "cannot apply $acosh to 0.333333, value must be in [1,inf]"
    );
    // A decimal is named as a decimal prints.
    assert_eq!(asin(dec("1E+30")), "cannot apply $asin to 1E+30, value must be in [-1,1]");
    assert_eq!(
        asin(dec("123456789012345678901234567890.5")),
        "cannot apply $asin to 123456789012345678901234567890.5, value must be in [-1,1]"
    );
    assert_eq!(asin(dec("-1E+400")), "cannot apply $asin to -1E+400, value must be in [-1,1]");
}

#[test]
fn log_names_a_decimal_as_a_double() {
    let log = |x: Bson, b: Bson| err(op("$log", vec![x, b]));
    assert_eq!(
        log(dec("-1E+400"), Bson::Int32(2)),
        "$log's argument must be a positive number, but is -inf"
    );
    assert_eq!(
        log(Bson::Int32(2), dec("-Infinity")),
        "$log's base must be a positive number not equal to 1, but is -inf"
    );
    assert_eq!(
        log(dec("-0"), Bson::Int32(2)),
        "$log's argument must be a positive number, but is -0"
    );
    assert_eq!(
        log(Bson::Double(-123456.7), Bson::Int32(2)),
        "$log's argument must be a positive number, but is -123457"
    );
    assert_eq!(
        log(Bson::Double(-1e-7), Bson::Int32(2)),
        "$log's argument must be a positive number, but is -1e-07"
    );
}

#[test]
fn log_checks_in_the_references_order() {
    let s = || Bson::String("a".into());
    let log = |x: Bson, b: Bson| eval(op("$log", vec![x, b]));
    // A null either side is null before anything is looked at.
    assert_eq!(log(s(), Bson::Null).unwrap(), Bson::Null);
    assert_eq!(log(Bson::Null, s()).unwrap(), Bson::Null);
    // Both types before either value.
    assert!(log(Bson::Int32(-1), s()).unwrap_err().to_string().contains("base must be numeric"));
    // The number's value before the base's.
    assert!(
        log(Bson::Int32(0), Bson::Int32(1))
            .unwrap_err()
            .to_string()
            .contains("argument must be a positive")
    );
    // A NaN is a NaN, of the width of the operands, and a decimal NaN a double NaN.
    assert!(double(&log(Bson::Int32(2), Bson::Double(f64::NAN)).unwrap()).is_nan());
    assert!(double(&log(Bson::Int32(2), dec("NaN")).unwrap()).is_nan());
}

#[test]
fn a_decimal_base_other_than_two_and_ten_divides_by_the_base_as_the_reference_does() {
    // `ln 2 / 3`, not `ln 2 / ln 3`: the reference's answer, matched.
    assert_eq!(
        val(op("$log", vec![dec("2"), dec("3")])),
        dec("0.2310490601866484364724107071527255")
    );
    assert_eq!(val(op("$log", vec![dec("8"), dec("2")])), dec("3"));
    assert_eq!(val(op("$log", vec![dec("1000"), dec("10")])), dec("3"));
    // A double beside a decimal is read to 15 digits: `0.1`, not its binary value.
    assert_eq!(
        val(op("$log", vec![dec("2"), Bson::Double(0.1)])),
        dec("6.931471805599453094172321214581766")
    );
    // Doubles are the quotient of the natural logarithms, which is not always exact.
    assert_eq!(
        double(&val(op("$log", vec![Bson::Int32(1000), Bson::Int32(10)]))),
        2.9999999999999996
    );
    assert_eq!(double(&val(op("$log", vec![Bson::Int32(8), Bson::Int32(2)]))), 3.0);
}

#[test]
fn degrees_and_radians_of_a_decimal_multiply_by_the_constant_to_34_digits() {
    assert_eq!(
        val(op("$degreesToRadians", vec![dec("1")])),
        dec("0.01745329251994329576923690768488613")
    );
    assert_eq!(
        val(op("$radiansToDegrees", vec![dec("3.141592653589793238462643383279503")])),
        dec("180.0000000000000000000000000000000")
    );
    assert_eq!(val(op("$degreesToRadians", vec![dec("0E-5")])), dec("0E-40"));
    assert_eq!(val(op("$radiansToDegrees", vec![dec("-0")])), dec("-0E-32"));
}

#[test]
fn atan2_reads_y_then_x() {
    let atan2 = |y: Bson, x: Bson| double(&val(op("$atan2", vec![y, x])));
    assert_eq!(atan2(Bson::Int32(1), Bson::Int32(1)), std::f64::consts::FRAC_PI_4);
    assert_eq!(atan2(Bson::Double(0.0), Bson::Double(-0.0)), std::f64::consts::PI);
    assert_eq!(atan2(Bson::Double(-0.0), Bson::Double(-1.0)), -std::f64::consts::PI);
    // A decimal beside a double makes both decimals.
    assert_eq!(
        val(op("$atan2", vec![dec("1"), Bson::Double(2.0)])),
        dec("0.4636476090008061162142562314612144")
    );
    assert_eq!(
        val(op("$atan2", vec![Bson::Double(2.0), dec("1")])),
        dec("1.107148717794090503017065460178537")
    );
    let msg = err(op("$atan2", vec![Bson::Int32(1), Bson::String("a".into())]));
    assert_eq!(msg, "$atan2 only supports numeric types, not string");
}

#[test]
fn the_decimal_functions_reach_a_large_operand() {
    // `sin` and `cos` of 1E+6000 are reduced exactly (`pi` to 6000 digits): the reference's.
    assert!(close_dec(
        &val(op("$sin", vec![dec("1E+6000")])),
        "-0.7249266534376325863453460198136025"
    ));
    assert!(close_dec(
        &val(op("$cos", vec![dec("1E+100")])),
        "-0.9280819050746553434561946437769560"
    ));
}

fn close_dec(got: &Bson, want: &str) -> bool {
    let (Bson::Decimal128(g), Bson::Decimal128(w)) = (got, dec(want)) else { return false };
    close(&decode_raw(g), &decode_raw(&w))
}
