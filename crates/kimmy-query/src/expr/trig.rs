//! `$sin`, `$cos`, `$tan`, `$asin`, `$acos`, `$atan`, `$atan2`, `$sinh`, `$cosh`, `$tanh`,
//! `$asinh`, `$acosh`, `$atanh`, `$degreesToRadians`, `$radiansToDegrees`, and `$log` with a
//! base.
//!
//! Each answers what the reference answers. A null or a missing operand is null, and an
//! operand that is not a number is refused in the reference's words. The answer is a double
//! whatever the width of an integer operand; a `Decimal128` operand gives a `Decimal128`
//! (computed to 34 digits by `kimmy_core::decimal::arith::func`, see its `trig` module for how
//! close that is to the reference's library). The sine, cosine and tangent refuse an infinity,
//! the arcsine and arccosine a number outside `[-1, 1]`, `$acosh` a number below one and
//! `$atanh` a number outside `[-1, 1]`; a NaN is a NaN. The doubles come from the platform's
//! math library, so the last digit of one can differ between architectures (as `$exp` and
//! `$ln` can, see `math.rs`).
//!
//! `$log` is `ln(number) / ln(base)` for doubles. A `Decimal128` in either operand makes both
//! decimals and the answer one. The reference does not change base there: it divides `ln(number)` by the base itself for any base but
//! 2 and 10 (`{$log: [NumberDecimal("2"), NumberDecimal("3")]}` is `0.2310490601866484364724107071527255`,
//! `ln 2 / 3`), a defect. A decimal here is the logarithm, `ln(number) / ln(base)` rounded once
//! (`0.6309297535714574370995271143427…` for that call), as the reference's own double form answers;
//! `docs/deviations.md` records the difference.

use bson::Bson;
use kimmy_core::decimal::arith::{self, func};
use kimmy_core::decimal::{Raw, decode_raw, pack_raw};
use kimmy_core::{Error, Result};

use super::docops::bson_type_name;
use super::{Num, Op};

/// `x * (pi / 180)` and `x * (180 / pi)` for a decimal, with the constants to 34 digits.
const DEGREES_TO_RADIANS: Raw = Raw::Finite {
    negative: false,
    coefficient: 1_745_329_251_994_329_576_923_690_768_488_613,
    exponent: -35,
};
const RADIANS_TO_DEGREES: Raw = Raw::Finite {
    negative: false,
    coefficient: 5_729_577_951_308_232_087_679_815_481_410_517,
    exponent: -32,
};

/// A number as C's `%g` prints it (six significant digits), which is how the reference words
/// the number in a refusal: `2`, `1.5`, `1e+30`, `1.23457e+08`, `-inf`.
pub(super) fn print_g(value: f64) -> String {
    if value.is_nan() {
        return "nan".into();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() { "-inf" } else { "inf" }.into();
    }
    if value == 0.0 {
        return if value.is_sign_negative() { "-0" } else { "0" }.into();
    }
    let scientific = format!("{value:.5e}");
    let (mantissa, exponent) = scientific.split_once('e').expect("an exponent");
    let exponent: i32 = exponent.parse().expect("an integer exponent");
    if !(-4..6).contains(&exponent) {
        let mantissa = if mantissa.contains('.') {
            mantissa.trim_end_matches('0').trim_end_matches('.')
        } else {
            mantissa
        };
        let sign = if exponent.is_negative() { '-' } else { '+' };
        return format!("{mantissa}e{sign}{:02}", exponent.abs());
    }
    let fixed = format!("{value:.*}", (5 - exponent) as usize);
    if fixed.contains('.') {
        fixed.trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        fixed
    }
}

/// A number operand: the plain number or a decimal. `None` for anything else.
enum Number {
    Plain(Num),
    Decimal(Raw),
}

impl Number {
    fn of(value: &Bson) -> Option<Number> {
        match value {
            Bson::Decimal128(d) => Some(Number::Decimal(decode_raw(d))),
            other => Num::from_bson(other).map(Number::Plain),
        }
    }

    /// The number as a decimal operand: an integer exactly, a double rounded to 15 significant
    /// digits, which is how the reference converts a double that meets a `Decimal128` here
    /// (`0.1` is `0.1`, not its binary value; `$sum` converts by the exact value instead).
    fn widen(&self) -> Raw {
        match self {
            Number::Plain(Num::Int32(n)) => arith::from_i64(i64::from(*n)),
            Number::Plain(Num::Int64(n)) => arith::from_i64(*n),
            Number::Plain(Num::Dbl(d)) => round_to_15_digits(*d),
            Number::Decimal(raw) => *raw,
        }
    }

    /// The number as a double, as the reference words a refusal that names a decimal.
    fn double(&self) -> f64 {
        match self {
            Number::Plain(n) => n.as_f64(),
            Number::Decimal(raw) => f64::from_bits(arith::to_binary64(raw)),
        }
    }
}

/// A double as a decimal of at most 15 significant digits, trailing zeros dropped.
fn round_to_15_digits(d: f64) -> Raw {
    if !d.is_finite() || d == 0.0 {
        return arith::from_binary64(d.to_bits()).0;
    }
    let text = format!("{d:.14e}");
    let (mantissa, exponent) = text.split_once('e').expect("an exponent");
    let mantissa = if mantissa.contains('.') {
        mantissa.trim_end_matches('0').trim_end_matches('.')
    } else {
        mantissa
    };
    let parsed = kimmy_core::decimal::parse(&format!("{mantissa}E{exponent}"))
        .expect("a finite double prints as a decimal");
    decode_raw(&parsed)
}

fn refusal(op: Op, shown: &str, range: &str) -> Error {
    Error::InvalidQuery(format!("cannot apply {} to {shown}, value must be in {range}", op.name()))
}

/// The unary operators: `$sin` .. `$radiansToDegrees` but `$atan2`.
pub(super) fn unary(op: Op, arg: &Bson) -> Result<Bson> {
    let n = match arg {
        Bson::Null | Bson::Undefined => return Ok(Bson::Null),
        other => Number::of(other).ok_or_else(|| {
            Error::InvalidQuery(format!(
                "{} only supports numeric types, not {}",
                op.name(),
                bson_type_name(Some(other))
            ))
        })?,
    };
    match n {
        Number::Plain(n) => plain(op, n),
        Number::Decimal(raw) => decimal(op, &raw, arg),
    }
}

fn plain(op: Op, n: Num) -> Result<Bson> {
    let x = n.as_f64();
    let shown = || print_g(x);
    let out = match op {
        Op::Sin | Op::Cos | Op::Tan => {
            if x.is_infinite() {
                return Err(refusal(op, &shown(), "(-inf,inf)"));
            }
            match op {
                Op::Sin => x.sin(),
                Op::Cos => x.cos(),
                _ => x.tan(),
            }
        }
        Op::Asin | Op::Acos | Op::Atanh => {
            if x.abs() > 1.0 {
                return Err(refusal(op, &shown(), "[-1,1]"));
            }
            match op {
                Op::Asin => x.asin(),
                Op::Acos => x.acos(),
                _ => x.atanh(),
            }
        }
        Op::Acosh => {
            if x < 1.0 {
                return Err(refusal(op, &shown(), "[1,inf]"));
            }
            x.acosh()
        }
        Op::Atan => x.atan(),
        Op::Sinh => x.sinh(),
        Op::Cosh => x.cosh(),
        Op::Tanh => x.tanh(),
        Op::Asinh => x.asinh(),
        Op::DegreesToRadians => x * (std::f64::consts::PI / 180.0),
        Op::RadiansToDegrees => x * (180.0 / std::f64::consts::PI),
        _ => unreachable!("not a trigonometric operator"),
    };
    Ok(Bson::Double(out))
}

/// The ticks of the request deadline a decimal `$sin`, `$cos` or `$tan` is charged before it
/// runs: its adjusted exponent, since an operand of `1E+6000` is reduced modulo `pi/2` with a
/// division of numbers some thousands of digits long. A charge past the stride reads the clock
/// at once (ADR-266), so a late switch to such an operand ends at the deadline.
pub(super) fn reduction_ticks(x: &Raw) -> usize {
    match x {
        Raw::Finite { coefficient, exponent, .. } if *coefficient != 0 => {
            let digits = coefficient.ilog10() as i64 + 1;
            usize::try_from(digits - 1 + i64::from(*exponent)).unwrap_or(0)
        }
        _ => 0,
    }
}

fn decimal(op: Op, x: &Raw, arg: &Bson) -> Result<Bson> {
    if matches!(op, Op::Sin | Op::Cos | Op::Tan) {
        kimmy_core::deadline::tick_n(reduction_ticks(x))?;
    }
    let answer = match op {
        Op::Sin => func::sin(x),
        Op::Cos => func::cos(x),
        Op::Tan => func::tan(x),
        Op::Asin => func::asin(x),
        Op::Acos => func::acos(x),
        Op::Atan => func::atan(x),
        Op::Sinh => func::sinh(x),
        Op::Cosh => func::cosh(x),
        Op::Tanh => func::tanh(x),
        Op::Asinh => func::asinh(x),
        Op::Acosh => func::acosh(x),
        Op::Atanh => func::atanh(x),
        Op::DegreesToRadians => Ok(arith::mul(x, &DEGREES_TO_RADIANS)),
        Op::RadiansToDegrees => Ok(arith::mul(x, &RADIANS_TO_DEGREES)),
        _ => unreachable!("not a trigonometric operator"),
    };
    match answer {
        Ok(raw) => Ok(Bson::Decimal128(pack_raw(raw))),
        Err(func::Domain) => {
            let shown = match arg {
                Bson::Decimal128(d) => d.to_string(),
                _ => unreachable!("a decimal operand"),
            };
            Err(refusal(
                op,
                &shown,
                match op {
                    Op::Sin | Op::Cos | Op::Tan => "(-inf,inf)",
                    Op::Acosh => "[1,inf]",
                    _ => "[-1,1]",
                },
            ))
        }
    }
}

/// `$atan2: [y, x]`. A null `y` is null before `x` is looked at, and a `y` that is not a
/// number is refused before a null `x` is.
pub(super) fn atan2(y: &Bson, x: &Bson) -> Result<Bson> {
    if matches!(y, Bson::Null | Bson::Undefined) {
        return Ok(Bson::Null);
    }
    let not_numeric = |value: &Bson| {
        Error::InvalidQuery(format!(
            "$atan2 only supports numeric types, not {}",
            bson_type_name(Some(value))
        ))
    };
    let y_number = Number::of(y).ok_or_else(|| not_numeric(y))?;
    if matches!(x, Bson::Null | Bson::Undefined) {
        return Ok(Bson::Null);
    }
    let x_number = Number::of(x).ok_or_else(|| not_numeric(x))?;
    Ok(match (&y_number, &x_number) {
        (Number::Plain(y), Number::Plain(x)) => Bson::Double(y.as_f64().atan2(x.as_f64())),
        _ => Bson::Decimal128(pack_raw(func::atan2(&y_number.widen(), &x_number.widen()))),
    })
}

/// `$log: [number, base]`. A null or missing operand is null before either is checked; then
/// the types, then the number (`> 0`), then the base (`> 0` and not 1). A NaN is a double NaN.
pub(super) fn log(number: &Bson, base: &Bson) -> Result<Bson> {
    if matches!(number, Bson::Null | Bson::Undefined)
        || matches!(base, Bson::Null | Bson::Undefined)
    {
        return Ok(Bson::Null);
    }
    let value = Number::of(number).ok_or_else(|| {
        Error::InvalidQuery(format!(
            "$log's argument must be numeric, not {}",
            bson_type_name(Some(number))
        ))
    })?;
    let base = Number::of(base).ok_or_else(|| {
        Error::InvalidQuery(format!(
            "$log's base must be numeric, not {}",
            bson_type_name(Some(base))
        ))
    })?;
    // `x <= 0` and a base that is not positive or is 1 are refused; a NaN is neither. With a
    // decimal among the operands the decision is made on the decimals the logarithm is taken
    // of (`1E-400` is positive and `1.000000000000000000000000000000001` is not 1, whatever
    // doubles they convert to); the double is kept for the words of the refusal.
    let refuse_number = |shown: f64| {
        Error::InvalidQuery(format!(
            "$log's argument must be a positive number, but is {}",
            print_g(shown)
        ))
    };
    let refuse_base = |shown: f64| {
        Error::InvalidQuery(format!(
            "$log's base must be a positive number not equal to 1, but is {}",
            print_g(shown)
        ))
    };
    let (Number::Plain(plain_x), Number::Plain(plain_b)) = (&value, &base) else {
        let (x, b) = (value.widen(), base.widen());
        if non_positive(&x) {
            return Err(refuse_number(value.double()));
        }
        if non_positive(&b) || is_one(&b) {
            return Err(refuse_base(base.double()));
        }
        if matches!(x, Raw::Nan) || matches!(b, Raw::Nan) {
            return Ok(Bson::Double(f64::NAN));
        }
        return Ok(Bson::Decimal128(pack_raw(decimal_log(&x, &b))));
    };
    let (x, b) = (plain_x.as_f64(), plain_b.as_f64());
    if x <= 0.0 {
        return Err(refuse_number(x));
    }
    if b <= 0.0 || b == 1.0 {
        return Err(refuse_base(b));
    }
    Ok(Bson::Double(x.ln() / b.ln()))
}

/// Whether a decimal is zero or below it (a NaN is neither).
fn non_positive(x: &Raw) -> bool {
    match x {
        Raw::Nan => false,
        Raw::Inf { negative } => *negative,
        Raw::Finite { negative, coefficient, .. } => *negative || *coefficient == 0,
    }
}

/// Whether a decimal is one, in any cohort (`1.00` is).
fn is_one(x: &Raw) -> bool {
    matches!(x, Raw::Finite { .. }) && arith::sub(x, &arith::from_i64(1)).is_zero()
}

/// The decimal logarithm: `ln(x) / ln(base)`, rounded once, with the base 2 and the base 10 as
/// `log2` and `log10`. The reference computes any other base as `ln(x) / base` (a defect, see
/// the register); this is the logarithm, which is what its double form and its base-2 and
/// base-10 forms answer. An infinite `x` is an infinity of the sign of `ln(base)`, an infinite
/// base makes the answer a zero (or a `NaN` beside an infinite `x`).
pub(super) fn decimal_log(x: &Raw, base: &Raw) -> Raw {
    let is = |n: i64| arith::sub(base, &arith::from_i64(n)).is_zero();
    if is(2) {
        return func::log2(x).unwrap_or(Raw::Nan);
    }
    if is(10) {
        return func::log10(x).unwrap_or(Raw::Nan);
    }
    match (x, base) {
        // Beside an infinite base, `ln(base)` is an infinity and not a finite number, which is
        // the `NaN` of an infinity over an infinity.
        (Raw::Inf { .. }, _) => match func::ln(base) {
            Ok(Raw::Finite { negative, .. }) => Raw::Inf { negative },
            _ => Raw::Nan,
        },
        (_, Raw::Inf { .. }) => match func::ln(x) {
            Ok(ln) => arith::div(&ln, base).unwrap_or(Raw::Nan),
            Err(func::Domain) => Raw::Nan,
        },
        _ => func::log(x, base).unwrap_or(Raw::Nan),
    }
}
