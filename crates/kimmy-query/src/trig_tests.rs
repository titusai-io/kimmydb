//! The trigonometric operators, `$atan2` and the two-argument `$log` (the reference's answers
//! for every operand type: `tests/data/q8d_reference.tsv`, recorded from the reference (6.0.5) by
//! `q8-probes/q8d.js` and `q8d-log.js`), which operand gives which type, and in whose words
//! each is refused.
//!
//! The doubles are compared bit for bit, except the answers of the operators that call the
//! platform's math library (ADR-271, decision 3): those may differ from the reference's by
//! [`LIBM_ULP_BOUND`] unit in the last place, because the library differs between platforms and
//! between versions of one platform. A `Decimal128` answer is held to the reference's
//! within three units of its last place, and the exact points (zeros, infinities, NaN) to the
//! same digits and cohort: the decimal functions are correctly rounded, and the reference's
//! library is not (see `kimmy-core`'s `decimal_trig_reference` test).
//!
//! A row of the fixture with a fourth column is a call whose answer here is **not** the
//! reference's: a decimal `$log` with a base other than 2 and 10, where the reference divides
//! `ln(x)` by the base. The third column is the logarithm (computed independently, rounded to
//! 34 digits, and held to the same value here) and the fourth is what the reference recorded.

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

/// The operators whose double answers are the platform's math library's (ADR-271, decision 3:
/// "the doubles are the platform's math library's, as `$exp` and `$ln` are"; `$log` with a base
/// divides two of its logarithms). `$degreesToRadians` and `$radiansToDegrees` multiply by a
/// constant and do not call it, so their answers stay bit for bit.
const LIBM_OPERATORS: [&str; 14] = [
    "$sin", "$cos", "$tan", "$asin", "$acos", "$atan", "$atan2", "$sinh", "$cosh", "$tanh",
    "$asinh", "$acosh", "$atanh", "$log",
];

/// How far a platform-library answer may be from the recorded one, in units in the last place.
/// The recorded answers were taken on one platform. Another platform's library gave 4 of the
/// 1572 answers one unit away (on the CI runner image Ubuntu 26.04: `$asinh` of 1.5 and `$acosh`
/// of 2 as an `Int32`, an `Int64` and a double) and every other answer bit for bit; the bound is
/// that difference.
const LIBM_ULP_BOUND: u64 = 1;

/// Whether two doubles are the same, or at most `bound` units in the last place apart (the
/// same sign, so a result is never allowed to cross zero), or both NaN.
fn within_ulps(a: f64, b: f64, bound: u64) -> bool {
    if a.is_nan() || b.is_nan() {
        return a.is_nan() && b.is_nan();
    }
    if a == b {
        return true;
    }
    a.is_sign_negative() == b.is_sign_negative() && a.to_bits().abs_diff(b.to_bits()) <= bound
}

/// Whether `got` is the reference's `want` (a fixture code) for the operator `name`: bit for bit
/// for a double (within [`LIBM_ULP_BOUND`] for an operator of [`LIBM_OPERATORS`]), exact for an
/// integer, and for a decimal the same value to within the last digits.
fn agrees(name: &str, got: &Bson, want: &str) -> bool {
    match (got, &want[..1]) {
        (Bson::Null, _) => want == "null",
        (Bson::Double(d), "d") => {
            let w: f64 = want[1..].parse().unwrap();
            let bound = if LIBM_OPERATORS.contains(&name) { LIBM_ULP_BOUND } else { 0 };
            within_ulps(*d, w, bound)
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
        let reference_differs = cols.next().is_some();
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
                if reference_differs {
                    if !same_value(value, want) {
                        wrong.push(format!("{name} {args}: want {want}, got {value:?}"));
                    }
                } else if !agrees(name, value, want) {
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

/// Whether a decimal answer is the decimal `want` to the last digit, whatever its cohort.
fn same_value(got: &Bson, want: &str) -> bool {
    match (got, dec(&want[1..])) {
        (Bson::Decimal128(g), Bson::Decimal128(w)) => {
            let (g, w) = (widened(&decode_raw(g)), widened(&decode_raw(&w)));
            g == w
                || matches!(
                    (g, w),
                    (Raw::Finite { coefficient: 0, .. }, Raw::Finite { coefficient: 0, .. })
                )
        }
        _ => false,
    }
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
fn a_decimal_log_is_the_logarithm_where_the_reference_divides_by_the_base() {
    // The reference answers `ln 2 / 3`, `0.2310490601866484364724107071527255`; the logarithm
    // is `ln 2 / ln 3`, rounded once.
    assert_eq!(
        val(op("$log", vec![dec("2"), dec("3")])),
        dec("0.6309297535714574370995271143427609")
    );
    assert_eq!(
        val(op("$log", vec![dec("100"), dec("7")])),
        dec("2.366589324909876653635857123293718")
    );
    // A base below one, and a double beside a decimal read to 15 digits (`0.1`, not its binary
    // value): the reference answers `6.93…` and `-1.386…`.
    assert!(same_value(
        &val(op("$log", vec![dec("2"), Bson::Double(0.1)])),
        "D-0.3010299956639811952137388947244930"
    ));
    assert_eq!(val(op("$log", vec![dec("8"), dec("0.5")])), dec("-3"));
    // The base 2 and the base 10 are the reference's, and exact for a power.
    assert_eq!(val(op("$log", vec![dec("8"), dec("2")])), dec("3"));
    assert_eq!(val(op("$log", vec![dec("1000"), dec("10")])), dec("3"));
    assert_eq!(val(op("$log", vec![dec("27"), dec("3")])), dec("3"));
    assert_eq!(val(op("$log", vec![dec("5"), dec("5")])), dec("1"));
    assert_eq!(val(op("$log", vec![dec("1"), dec("3")])), dec("0"));
    // An infinite number is an infinity of the sign of `ln(base)`, an infinite base a zero.
    assert_eq!(val(op("$log", vec![dec("Infinity"), dec("0.5")])), dec("-Infinity"));
    assert_eq!(val(op("$log", vec![dec("Infinity"), dec("3")])), dec("Infinity"));
    // A decimal agrees with the double of the same call, which the reference gets right.
    for (x, b) in [(2.0, 3.0), (100.0, 7.0), (0.25, 0.5), (5.0, 1.5), (1e6, 100.0), (3.0, 0.1)] {
        let double_form = double(&val(op("$log", vec![Bson::Double(x), Bson::Double(b)])));
        let Bson::Decimal128(d) = val(op("$log", vec![dec(&x.to_string()), dec(&b.to_string())]))
        else {
            panic!("not a decimal")
        };
        let decimal_form: f64 = d.to_string().parse().unwrap();
        assert!(
            (decimal_form - double_form).abs() <= 1e-14 * double_form.abs().max(1.0),
            "{x} {b}: {decimal_form} vs {double_form}"
        );
    }
    // Doubles are the quotient of the natural logarithms, which is not always exact.
    assert_eq!(
        double(&val(op("$log", vec![Bson::Int32(1000), Bson::Int32(10)]))),
        2.9999999999999996
    );
    assert_eq!(double(&val(op("$log", vec![Bson::Int32(8), Bson::Int32(2)]))), 3.0);
}

/// The domain of a decimal `$log` is decided on the decimals, not on the doubles they convert
/// to (review q8d, m2): under `ln(x) / ln(base)` all of these are defined.
#[test]
fn the_domain_of_a_decimal_log_is_decided_on_the_decimals() {
    // `1E-400` is the double 0, and a decimal logarithm of it is -400.
    assert!(same_value(&val(op("$log", vec![dec("1E-400"), Bson::Int32(10)])), "D-400"));
    assert!(same_value(&val(op("$log", vec![dec("1E-400"), dec("10")])), "D-400"));
    // A base that converts to the double 1 and is not 1: finite answers, to 34 digits (the
    // logarithms of 2 and of the base are taken in one fixed point and divided).
    assert!(same_value(
        &val(op("$log", vec![Bson::Int32(2), dec("1.000000000000000000000000000000001")])),
        "D693147180559945309417232121458176.9"
    ));
    assert!(same_value(
        &val(op("$log", vec![dec("2"), dec("0.9999999999999999999999999999999999")])),
        "D-6931471805599453094172321214581765"
    ));
    // The refusals are still the reference's: a decimal 1 in any cohort, zero, a negative.
    for base in ["1", "1.00", "1.000000000000000000000000000000000"] {
        assert_eq!(
            err(op("$log", vec![dec("2"), dec(base)])),
            "$log's base must be a positive number not equal to 1, but is 1"
        );
    }
    assert!(err(op("$log", vec![dec("-1E-400"), Bson::Int32(10)])).contains("positive number"));
    assert!(err(op("$log", vec![dec("0E-6000"), Bson::Int32(10)])).contains("positive number"));
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
    // `sin` of 1E+6000 is reduced exactly (`pi` to 6000 digits), and is the reference's.
    assert!(same_value(
        &val(op("$sin", vec![dec("1E+6000")])),
        "D-0.7249266534376325863453460198136025"
    ));
    // `cos` of 1E+100 is the correctly rounded value; the reference ends 9560 (one unit off).
    assert!(same_value(
        &val(op("$cos", vec![dec("1E+100")])),
        "D-0.9280819050746553434561946437769559"
    ));
    // `tan` of 1E+6000 ends 600; the reference's is 601.
    assert!(same_value(
        &val(op("$tan", vec![dec("1E+6000")])),
        "D1.052408860229401513505574301283600"
    ));
}

#[test]
fn a_double_is_within_a_bound_of_units_in_the_last_place() {
    let x = 1.1947632172871094_f64;
    let next = f64::from_bits(x.to_bits() + 1);
    assert!(within_ulps(x, x, 0));
    assert!(!within_ulps(x, next, 0), "a bound of 0 is bit for bit");
    assert!(within_ulps(x, next, 1) && within_ulps(next, x, 1));
    let two = f64::from_bits(x.to_bits() + 2);
    assert!(!within_ulps(x, two, 1));
    // Zeros of either sign are equal; a result never crosses zero; NaN matches only NaN.
    assert!(within_ulps(0.0, -0.0, 0));
    assert!(!within_ulps(f64::from_bits(1), -f64::from_bits(1), 1));
    assert!(within_ulps(f64::NAN, f64::NAN, 0) && !within_ulps(f64::NAN, 1.0, 1));
    assert!(within_ulps(f64::INFINITY, f64::INFINITY, 0));
}

#[test]
fn only_the_platform_library_operators_have_a_bound() {
    // `agrees` takes the bound from the operator: one unit away is accepted for `$asinh` and
    // refused for an operator that does not call the library.
    let x = 1.1947632172871094_f64;
    let next = f64::from_bits(x.to_bits() + 1);
    let want = format!("d{x}");
    assert!(agrees("$asinh", &Bson::Double(next), &want));
    assert!(!agrees("$degreesToRadians", &Bson::Double(next), &want));
    assert!(agrees("$degreesToRadians", &Bson::Double(x), &want));
}
