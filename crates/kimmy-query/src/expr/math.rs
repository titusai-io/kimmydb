//! `$abs`, `$ceil`, `$floor`, `$round`, `$trunc`, `$pow`, `$sqrt`, `$exp`, `$ln`
//! and `$log10` (ADR-253).
//!
//! An integer stays an integer where the result is one, and falls to a double
//! where it cannot be (`$abs` of the smallest `Int64`, `$pow` past `Int64`), as
//! `$add` and `$multiply` do. A `Decimal128` argument gives a `Decimal128`
//! result, computed in integers by `kimmy_core::decimal::arith::func` and
//! rounded once to 34 digits (ADR-252's context). Null and a missing field give
//! null; a value that is not a number, and a number outside the function's
//! domain, are refused, never turned into a `NaN`.
//!
//! **Doubles** use the platform's math library for `$sqrt`, `$exp`, `$ln`,
//! `$log10` and `$pow` of a non-integer power, which is correctly rounded for
//! `$sqrt` and within a unit in the last place for the others on the platforms
//! the server runs on. The answer for a double can therefore differ in the last
//! digit between architectures; a `Decimal128` argument is exact to the rule
//! above on all of them. **`$round` and `$trunc` of a double** act on its exact
//! binary value, so `$round: [2.675, 2]` is `2.67` (the double nearest 2.675
//! is below it) and `$round: [0.125, 2]` is `0.12` (an exact tie, to even).

use bson::Bson;
use kimmy_core::decimal::arith::func;
use kimmy_core::{Error, Result};

use super::{Num, Op, Operand, any_decimal, as_integer, decimal_result, operands};

/// The `place` of `$round` and `$trunc` runs this far either side of the point.
const MIN_PLACE: i64 = -20;
const MAX_PLACE: i64 = 100;

/// `$abs`, `$ceil`, `$floor`, `$sqrt`, `$exp`, `$ln`, `$log10`.
pub(super) fn unary(op: Op, arg: &Bson) -> Result<Bson> {
    let Some(mut operands) = operands(op, std::slice::from_ref(arg))? else {
        return Ok(Bson::Null);
    };
    match operands.remove(0) {
        Operand::Decimal(raw) => {
            let out = match op {
                Op::Abs => func::abs(&raw),
                Op::Ceil => func::ceil(&raw),
                Op::Floor => func::floor(&raw),
                Op::Exp => func::exp(&raw),
                Op::Sqrt => func::sqrt(&raw).map_err(|_| domain(op))?,
                Op::Ln => func::ln(&raw).map_err(|_| domain(op))?,
                Op::Log10 => func::log10(&raw).map_err(|_| domain(op))?,
                _ => unreachable!("not a unary math operator"),
            };
            Ok(decimal_result(out))
        }
        Operand::Plain(n) => plain_unary(op, n),
    }
}

fn plain_unary(op: Op, n: Num) -> Result<Bson> {
    let x = n.as_f64();
    Ok(match op {
        Op::Abs => match n {
            Num::Int(i) => i.checked_abs().map_or(Num::Dbl(-(i as f64)), Num::Int).to_bson(),
            Num::Dbl(d) => Bson::Double(d.abs()),
        },
        Op::Ceil => match n {
            Num::Int(_) => n.to_bson(),
            Num::Dbl(d) => Bson::Double(d.ceil()),
        },
        Op::Floor => match n {
            Num::Int(_) => n.to_bson(),
            Num::Dbl(d) => Bson::Double(d.floor()),
        },
        Op::Sqrt => {
            if x < 0.0 {
                return Err(domain(op));
            }
            Bson::Double(x.sqrt())
        }
        Op::Exp => Bson::Double(x.exp()),
        Op::Ln => {
            if x <= 0.0 {
                return Err(domain(op));
            }
            Bson::Double(x.ln())
        }
        Op::Log10 => {
            if x <= 0.0 {
                return Err(domain(op));
            }
            Bson::Double(x.log10())
        }
        _ => unreachable!("not a unary math operator"),
    })
}

fn domain(op: Op) -> Error {
    let needs = match op {
        Op::Sqrt => "a number greater than or equal to 0",
        _ => "a positive number",
    };
    Error::InvalidQuery(format!("{} needs {needs}", op.name()))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    HalfEven,
    Trunc,
}

/// `$round` and `$trunc`: `[number]` or `[number, place]`. A positive place keeps
/// that many digits after the point, a negative one rounds to the tens, hundreds
/// and so on; a null number or a null place gives null.
pub(super) fn round(op: Op, number: &Bson, place: Option<&Bson>) -> Result<Bson> {
    // The number is read first: a value that is not a number is `400` whatever the
    // place is, a null place included.
    let operands = operands(op, std::slice::from_ref(number))?;
    let place = match place {
        None => 0,
        Some(Bson::Null | Bson::Undefined) => return Ok(Bson::Null),
        Some(p) => {
            let n = as_integer(p, &format!("the place of {}", op.name()))?;
            if !(MIN_PLACE..=MAX_PLACE).contains(&n) {
                return Err(Error::InvalidQuery(format!(
                    "the place of {} must be between {MIN_PLACE} and {MAX_PLACE}, found {n}",
                    op.name()
                )));
            }
            n as i32
        }
    };
    let Some(mut operands) = operands else {
        return Ok(Bson::Null);
    };
    let mode = if op == Op::Round { Mode::HalfEven } else { Mode::Trunc };
    Ok(match operands.remove(0) {
        Operand::Decimal(raw) => decimal_result(match mode {
            Mode::HalfEven => func::round(&raw, place),
            Mode::Trunc => func::trunc(&raw, place),
        }),
        Operand::Plain(Num::Int(n)) => round_int(n, place, mode).to_bson(),
        Operand::Plain(Num::Dbl(d)) => Bson::Double(round_double(d, place, mode)),
    })
}

/// An integer rounded to a multiple of `10^-place` (a no-op for a place of zero
/// or more); a result past `Int64` is a double, as every integer result is.
fn round_int(n: i64, place: i32, mode: Mode) -> Num {
    if place >= 0 {
        return Num::Int(n);
    }
    let unit = 10i128.pow(place.unsigned_abs());
    let wide = i128::from(n);
    let (mut quotient, remainder) = (wide / unit, wide % unit);
    let away = match mode {
        Mode::Trunc => false,
        Mode::HalfEven => match (remainder.abs() * 2).cmp(&unit) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Equal => quotient % 2 != 0,
            std::cmp::Ordering::Less => false,
        },
    };
    if away {
        quotient += wide.signum();
    }
    match i64::try_from(quotient * unit) {
        Ok(rounded) => Num::Int(rounded),
        Err(_) => Num::Dbl((quotient * unit) as f64),
    }
}

/// A double rounded to `place` digits after the point, on its exact value.
///
/// The exact decimal expansion of a double is finite (as many fractional digits
/// as it has fractional bits), and `{:.N}` formatting prints it exactly, so the
/// digits past the cut and the tie are read from the text rather than guessed
/// from a scaled multiplication, which rounds twice. The rounded decimal is
/// turned back into the nearest double by the standard library's correctly
/// rounded parse.
fn round_double(d: f64, place: i32, mode: Mode) -> f64 {
    if !d.is_finite() || d == 0.0 {
        return d;
    }
    let bits = d.abs().to_bits();
    let biased = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1u64 << 52) - 1);
    let (mantissa, exponent) =
        if biased == 0 { (fraction, -1074) } else { (fraction | (1 << 52), biased - 1075) };
    let low_zeros = mantissa.trailing_zeros() as i32;
    let fractional_digits = (-(exponent + low_zeros)).max(0) as usize;
    let text = format!("{:.*}", fractional_digits, d.abs());
    let (whole, fractional) = text.split_once('.').unwrap_or((text.as_str(), ""));
    let digits: Vec<u8> = whole.bytes().chain(fractional.bytes()).map(|b| b - b'0').collect();
    let cut = whole.len() as i64 + i64::from(place);
    if cut >= digits.len() as i64 {
        return d;
    }
    let negative = d.is_sign_negative();
    if cut < 0 {
        // Everything is below a tenth of the unit: zero, whichever way it rounds.
        return if negative { -0.0 } else { 0.0 };
    }
    let cut = cut as usize;
    let (kept, rest) = digits.split_at(cut);
    let up = match mode {
        Mode::Trunc => false,
        Mode::HalfEven => match rest[0].cmp(&5) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => {
                rest[1..].iter().any(|&digit| digit != 0) || kept.last().is_some_and(|d| d % 2 == 1)
            }
        },
    };
    let mut kept = kept.to_vec();
    if up {
        let mut at = kept.len();
        loop {
            if at == 0 {
                kept.insert(0, 1);
                break;
            }
            at -= 1;
            if kept[at] == 9 {
                kept[at] = 0;
            } else {
                kept[at] += 1;
                break;
            }
        }
    }
    let mut text: String = kept.iter().map(|&digit| char::from(b'0' + digit)).collect();
    if text.is_empty() {
        text.push('0');
    }
    let rounded: f64 = format!("{text}e{}", -i64::from(place)).parse().unwrap_or(f64::NAN);
    if negative { -rounded } else { rounded }
}

/// `$pow`: `[base, exponent]`. Integers raised to a non-negative integer power
/// stay integers while they fit; a `Decimal128` operand gives a `Decimal128`.
/// A zero base with a negative exponent, and a negative base with a power that is
/// not a whole number, are refused.
pub(super) fn pow(base: &Bson, exponent: &Bson) -> Result<Bson> {
    let Some(operands) = operands(Op::Pow, &[base.clone(), exponent.clone()])? else {
        return Ok(Bson::Null);
    };
    if any_decimal(&operands) {
        return func::pow(&operands[0].widen(), &operands[1].widen()).map(decimal_result).map_err(
            |_| {
                Error::InvalidQuery(
                    "$pow cannot raise 0 to a negative power or a negative number to a power \
                     that is not a whole number"
                        .into(),
                )
            },
        );
    }
    let [Operand::Plain(b), Operand::Plain(e)] = operands.as_slice() else {
        unreachable!("two plain operands");
    };
    if let (Num::Int(b), Num::Int(e)) = (b, e)
        && *e >= 0
        && let Some(power) = int_pow(*b, *e)
    {
        return Ok(Bson::Int64(power));
    }
    let (b, e) = (b.as_f64(), e.as_f64());
    if b == 0.0 && e < 0.0 {
        return Err(Error::InvalidQuery("$pow cannot raise 0 to a negative power".into()));
    }
    if b < 0.0 && e.is_finite() && e.fract() != 0.0 {
        return Err(Error::InvalidQuery(
            "$pow cannot raise a negative number to a power that is not a whole number".into(),
        ));
    }
    Ok(Bson::Double(b.powf(e)))
}

/// `base^exponent` by squaring, or `None` when an intermediate leaves `Int64`.
fn int_pow(base: i64, exponent: i64) -> Option<i64> {
    let (mut result, mut base, mut exponent) = (1i64, base, exponent as u64);
    loop {
        if exponent & 1 == 1 {
            result = result.checked_mul(base)?;
        }
        exponent >>= 1;
        if exponent == 0 {
            return Some(result);
        }
        base = base.checked_mul(base)?;
    }
}
