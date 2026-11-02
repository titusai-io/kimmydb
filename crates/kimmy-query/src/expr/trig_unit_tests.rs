//! The helpers of the trigonometric operators that a refusal or a deadline charge rests on:
//! `print_g` (C's `%g`, in the reference's refusals), `reduction_ticks` (the charge of a decimal
//! `$sin`, `$cos` or `$tan`) and `decimal_log` on the infinities. Each expected value is worked
//! out from C's rule or from the function's doc, by hand, and not by the code under test.

use kimmy_core::decimal::Raw;

use super::trig::{decimal_log, print_g, reduction_ticks};

fn dec(negative: bool, coefficient: u128, exponent: i16) -> Raw {
    Raw::Finite { negative, coefficient, exponent }
}

// ---------------------------------------------------------------------------
// print_g: six significant digits, `e` notation when the exponent is below -4 or at least 6
// ---------------------------------------------------------------------------

#[test]
fn print_g_switches_to_an_exponent_below_1e_4_and_from_1e6() {
    for (value, want) in [
        // The small side: 0.0001 is 1e-4 (fixed), 0.00001 is 1e-5 (exponent).
        (0.0001, "0.0001"),
        (0.00001, "1e-05"),
        (0.0000999999, "9.99999e-05"),
        (0.0001000001, "0.0001"),
        (0.000123456789, "0.000123457"),
        // Rounding up across the boundary: 9.999995e-5 (the double is a hair above the tie)
        // rounds to 1.00000e-4, which is fixed again; 9.999994e-5 stays below.
        (0.00009999995, "0.0001"),
        (0.000099999951, "0.0001"),
        (0.00009999994, "9.99999e-05"),
        // The large side: 999999 is exponent 5 (fixed), 1e6 is exponent 6 (exponent).
        (999999.0, "999999"),
        (1e6, "1e+06"),
        (999999.4, "999999"),
        // 999999.5 is an exact tie, half to even: 1000000, so 1e+06; 999999.6 likewise.
        (999999.5, "1e+06"),
        (999999.6, "1e+06"),
        (9999995.0, "1e+07"),
        (100000.0, "100000"),
        (1234567.0, "1.23457e+06"),
        // Six significant digits, trailing zeros off, whole numbers without a point.
        (12345.67, "12345.7"),
        (123456.7, "123457"),
        (99999.95, "99999.9"),
        (12345.65, "12345.6"),
        (1.5, "1.5"),
        (2.0, "2"),
        (100.0, "100"),
        (0.5, "0.5"),
        (0.30000000000000004, "0.3"),
        // The extremes of a double.
        (1e100, "1e+100"),
        (5e-324, "4.94066e-324"),
        (1.7976931348623157e308, "1.79769e+308"),
        // The same on the negative side.
        (-0.0001, "-0.0001"),
        (-0.00001, "-1e-05"),
        (-999999.5, "-1e+06"),
        (-999999.0, "-999999"),
        (-1e6, "-1e+06"),
        (-123456.7, "-123457"),
        (-1e-100, "-1e-100"),
    ] {
        assert_eq!(print_g(value), want, "{value:e}");
    }
}

#[test]
fn print_g_names_the_special_values_as_c_does() {
    assert_eq!(print_g(f64::NAN), "nan");
    assert_eq!(print_g(f64::INFINITY), "inf");
    assert_eq!(print_g(f64::NEG_INFINITY), "-inf");
    assert_eq!(print_g(0.0), "0");
    assert_eq!(print_g(-0.0), "-0");
}

// ---------------------------------------------------------------------------
// reduction_ticks: the adjusted exponent, `digits - 1 + exponent`, and none below zero
// ---------------------------------------------------------------------------

#[test]
fn a_reduction_is_charged_the_adjusted_exponent_of_its_operand() {
    for (operand, ticks) in [
        // One digit: the adjusted exponent is the exponent.
        (dec(false, 1, 0), 0),
        (dec(false, 1, 1), 1),
        (dec(false, 5, 18), 18),
        (dec(false, 1, 6000), 6000),
        (dec(false, 1, 6144), 6144),
        // More digits add theirs, less one: 12 is 1.2E+1, 123E+2 is 1.23E+4.
        (dec(false, 12, 0), 1),
        (dec(false, 123, 2), 4),
        (dec(false, 100, 0), 2),
        // The widest coefficient and the largest exponent: 34 digits, 33 + 6111.
        (dec(false, 9_999_999_999_999_999_999_999_999_999_999_999, 6111), 6144),
        // Fractions: 12.3 is 1.23E+1, 1.23 is 1.23E+0, and below one nothing is charged.
        (dec(false, 123, -1), 1),
        (dec(false, 123, -2), 0),
        (dec(false, 123, -3), 0),
        (dec(false, 5, -1), 0),
        (dec(false, 1, -6176), 0),
        // The sign is not charged for, and zero, an infinity and a NaN are nothing to reduce.
        (dec(true, 1, 100), 100),
        (dec(false, 0, 6000), 0),
        (Raw::Inf { negative: false }, 0),
        (Raw::Nan, 0),
    ] {
        assert_eq!(reduction_ticks(&operand), ticks, "{operand:?}");
    }
}

// ---------------------------------------------------------------------------
// decimal_log on the infinities
// ---------------------------------------------------------------------------

#[test]
fn a_decimal_log_with_an_infinity_has_the_limits_answer() {
    let inf = Raw::Inf { negative: false };
    let smallest = |negative| dec(negative, 0, -6176);
    // Infinity over infinity is a NaN.
    assert_eq!(decimal_log(&inf, &inf), Raw::Nan);
    // An infinite number is an infinity of the sign of ln(base): positive above 1, negative
    // below it.
    assert_eq!(decimal_log(&inf, &dec(false, 3, 0)), Raw::Inf { negative: false });
    assert_eq!(decimal_log(&inf, &dec(false, 15, 0)), Raw::Inf { negative: false });
    assert_eq!(decimal_log(&inf, &dec(false, 5, -1)), Raw::Inf { negative: true });
    assert_eq!(decimal_log(&inf, &dec(false, 1, -3)), Raw::Inf { negative: true });
    // A finite number over an infinite base is a zero of the smallest exponent, signed as the
    // logarithm of the number is: positive above 1, negative below.
    assert_eq!(decimal_log(&dec(false, 5, 0), &inf), smallest(false));
    assert_eq!(decimal_log(&dec(false, 5, -1), &inf), smallest(true));
    // The base 2 and the base 10 are their own logarithms, beside an infinity too.
    assert_eq!(decimal_log(&inf, &dec(false, 2, 0)), Raw::Inf { negative: false });
    assert_eq!(decimal_log(&inf, &dec(false, 10, 0)), Raw::Inf { negative: false });
    // And finite with finite is a number (the base 3 of 9 is 2).
    assert_eq!(decimal_log(&dec(false, 9, 0), &dec(false, 3, 0)), dec(false, 2, 0));
}
