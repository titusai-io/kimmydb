//! A single writer's `$inc` of a Decimal128 counter that is accepted reads, afterwards, exactly what it read
//! before plus the operand, or it is refused (Amendment 10). Through the public API only.
//!
//! The write rule checks that the visible sum and the writer's own slot total are exact, which is not enough: the
//! read is `base + (total - absorbed)` per slot, and that gain can round although both totals are exact.
//! `{n: Int32 0}`, `$inc` -1E+34, `$set` the decimal -1E+34 (the base write absorbs the slot: pair -1E+34), `$inc`
//! +1E+34 (visible 0, slot total 0), `$inc` +1E-34: the visible sum 0 + 1E-34 and the slot total 0 + 1E-34 are
//! exact, the `$inc` was accepted and the counter read -1E+34 + (1E-34 - (-1E+34)) = -1E+34 + 1E+34 = 0, having
//! lost the operand. It is refused now: the writer evaluates the counter as it will read it with the planned slot
//! and accepts only a value equal to the visible sum.
//!
//! The comparison here is an independent exact sum (decimal digits in an array, no code of the product's arithmetic).

use kimmy_core::ids::NodeId;
use kimmy_core::merge_state::{
    Epoch, JoinOptions, Path, UpdateOp, UpdateRefused, Value, Writer, WriterId,
};

fn p(s: &str) -> Path {
    Path::flat(s).expect("a valid name")
}

fn dec(t: &str) -> Value {
    Value::Decimal128(t.parse::<bson::Decimal128>().unwrap().bytes())
}

fn writer() -> Writer {
    let id = WriterId { node: NodeId::from_bytes([b'A'; 16]), epoch: Epoch::from_u64(0) };
    Writer::new(id, JoinOptions::default())
}

fn read(w: &Writer) -> Value {
    w.state.value(&p("n"), &JoinOptions::default()).expect("the counter is present")
}

/// A number as signed decimal digits: (sign, digits least significant first, exponent). `None` for a NaN or an
/// infinity.
fn digits(v: &Value) -> Option<(i64, Vec<i64>, i64)> {
    let int = |i: i64| {
        let ds: Vec<i64> =
            i.unsigned_abs().to_string().bytes().rev().map(|b| i64::from(b - b'0')).collect();
        Some((if i < 0 { -1 } else { 1 }, ds, 0))
    };
    match v {
        Value::Int32(i) => int(i64::from(*i)),
        Value::Int64(i) => int(*i),
        Value::Decimal128(b) => {
            let text = bson::Decimal128::from_bytes(*b).to_string();
            let (sign, text) = match text.strip_prefix('-') {
                Some(rest) => (-1, rest.to_string()),
                None => (1, text),
            };
            if text.starts_with(|c: char| !c.is_ascii_digit()) {
                return None;
            }
            let (mantissa, exp) = match text.split_once('E') {
                Some((m, e)) => (m.to_string(), e.parse::<i64>().unwrap()),
                None => (text, 0),
            };
            let (whole, frac) = mantissa.split_once('.').unwrap_or((&mantissa, ""));
            let ds: Vec<i64> =
                format!("{whole}{frac}").bytes().rev().map(|b| i64::from(b - b'0')).collect();
            Some((sign, ds, exp - frac.len() as i64))
        }
        other => panic!("not a number: {other:?}"),
    }
}

/// Does the signed sum of the terms equal zero? Every digit goes to its place, the places are carried.
fn sums_to_zero(terms: &[(i64, Vec<i64>, i64)]) -> bool {
    let lo = terms.iter().map(|t| t.2).min().unwrap();
    let hi = terms.iter().map(|t| t.2 + t.1.len() as i64).max().unwrap();
    let mut acc = vec![0i64; (hi - lo + 2) as usize];
    for (sign, ds, exp) in terms {
        for (j, d) in ds.iter().enumerate() {
            acc[(exp - lo) as usize + j] += sign * d;
        }
    }
    let mut carry = 0i64;
    for a in &mut acc {
        let v = *a + carry;
        carry = v.div_euclid(10);
        *a = v.rem_euclid(10);
    }
    carry == 0 && acc.iter().all(|a| *a == 0)
}

/// `after - before - operand == 0`, exactly.
fn read_moved_by(before: &Value, after: &Value, operand: &Value) -> bool {
    let (b, a, o) = (digits(before), digits(after), digits(operand));
    let (Some(b), Some(a), Some(o)) = (b, a, o) else { return true };
    let neg = |t: &(i64, Vec<i64>, i64)| (-t.0, t.1.clone(), t.2);
    sums_to_zero(&[a, neg(&b), neg(&o)])
}

/// One `$inc`: accepted (and then the read moved by exactly the operand) or refused (and then nothing moved).
/// Returns whether it was accepted.
fn inc_checked(w: &mut Writer, operand: &Value, what: &str) -> bool {
    let before = read(w);
    let state = w.state.clone();
    match w.write_update(&[UpdateOp::Inc(p("n"), operand.clone())], 0) {
        Ok(Some(_)) => {
            let after = read(w);
            assert!(
                read_moved_by(&before, &after, operand),
                "{what}: {before:?} + {operand:?} read {after:?}"
            );
            true
        }
        Err(UpdateRefused::IncOutOfRange(_)) => {
            assert_eq!(w.state, state, "{what}: a refusal moves nothing");
            false
        }
        other => panic!("{what}: {other:?}"),
    }
}

/// The history of the defect: the last `$inc` is refused, nothing moved, and the counter keeps reading 0.
#[test]
fn an_increment_whose_read_would_lose_it_is_refused() {
    let mut w = writer();
    w.insert(&[(p("n"), Value::Int32(0))], 0).unwrap();
    assert!(inc_checked(&mut w, &dec("-1E+34"), "-1E+34"));
    w.write_set(&p("n"), dec("-1E+34"), 0).unwrap();
    assert!(inc_checked(&mut w, &dec("1E+34"), "+1E+34"));
    assert!(!inc_checked(&mut w, &dec("1E-34"), "+1E-34"), "the read would be 0, not 1E-34");
    // the way out is a `$set`
    w.write_set(&p("n"), dec("0"), 0).unwrap();
    assert!(inc_checked(&mut w, &dec("1E-34"), "after the $set"));
}

fn lcg(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

/// Random single-writer histories of decimal and integer `$inc`s and numeric `$set`s over operands of very
/// different magnitudes: after every accepted `$inc` the counter reads the read before plus the operand, exactly.
#[test]
fn every_accepted_increment_moves_the_read_by_exactly_the_operand() {
    let incs = [
        dec("1E+34"),
        dec("-1E+34"),
        dec("1E-34"),
        dec("-1E-34"),
        dec("1"),
        dec("-0.5"),
        dec("12.34"),
        Value::Int32(7),
        Value::Int32(-3),
        Value::Int64(1 << 40),
        Value::Int64(-5),
    ];
    let sets = [
        Value::Int32(0),
        Value::Int32(5),
        dec("0"),
        dec("-1E+34"),
        dec("1E+34"),
        dec("0.5"),
        dec("7.25"),
    ];
    let (mut accepted, mut refused) = (0usize, 0usize);
    let mut seed = 17u64;
    for history in 0..4000 {
        let mut w = writer();
        w.insert(&[(p("n"), Value::Int32(0))], 0).unwrap();
        for step in 0..9 {
            let what = format!("history {history}, step {step}");
            if lcg(&mut seed) % 10 < 7 {
                let op = &incs[(lcg(&mut seed) % incs.len() as u64) as usize];
                if inc_checked(&mut w, op, &what) {
                    accepted += 1;
                } else {
                    refused += 1;
                }
            } else {
                let v = &sets[(lcg(&mut seed) % sets.len() as u64) as usize];
                let _ = w.write_set(&p("n"), v.clone(), 0);
            }
        }
    }
    assert!(accepted > 10_000 && refused > 100, "{accepted} accepted, {refused} refused");
}

/// What the check decides on a planned slot must be what the write stores: the first decimal operand after an
/// absorb. `{n: 0}`, `$inc` an Int32 (or an Int64) 1, `$set` 5, `$inc` the decimal 0.5 is accepted and reads 5.5;
/// `{n: 1.5}`, `$inc` Int32 1, `$set` 2, `$inc` 0.25 reads 2.25; a slot whose decimal increments net to zero after an
/// absorb leaves the counter where the base is.
#[test]
fn the_first_decimal_operand_after_an_absorb_is_accepted_and_read() {
    for first in [Value::Int32(1), Value::Int64(1)] {
        let mut w = writer();
        w.insert(&[(p("n"), Value::Int32(0))], 0).unwrap();
        assert!(inc_checked(&mut w, &first, "the first operand"));
        w.write_set(&p("n"), Value::Int32(5), 0).unwrap();
        assert!(inc_checked(&mut w, &dec("0.5"), "the decimal after the absorb"));
        assert_eq!(read(&w), dec("5.5"), "after {first:?}");
    }
    let mut w = writer();
    w.insert(&[(p("n"), dec("1.5"))], 0).unwrap();
    assert!(inc_checked(&mut w, &Value::Int32(1), "Int32 1"));
    w.write_set(&p("n"), Value::Int32(2), 0).unwrap();
    assert!(inc_checked(&mut w, &dec("0.25"), "0.25"));
    assert_eq!(read(&w), dec("2.25"));
    // increments that net to zero after an absorb
    let mut w = writer();
    w.insert(&[(p("n"), Value::Int32(0))], 0).unwrap();
    assert!(inc_checked(&mut w, &dec("5"), "+5"));
    assert!(inc_checked(&mut w, &dec("-5"), "-5"));
    w.write_set(&p("n"), Value::Int32(3), 0).unwrap();
    assert!(inc_checked(&mut w, &dec("5"), "+5 after the absorb"));
    assert!(inc_checked(&mut w, &dec("-5"), "-5 after the absorb"));
    assert!(read_moved_by(&Value::Int32(3), &read(&w), &Value::Int32(0)));
}

/// A counter that is already an infinity or a NaN keeps its behaviour: `$inc` 1 is accepted and the counter reads
/// the same infinity or NaN (the check compares two non-finite numbers by kind and sign).
#[test]
fn an_increment_of_a_stored_infinity_or_nan_is_unchanged() {
    for stored in ["Infinity", "-Infinity", "NaN"] {
        let mut w = writer();
        w.insert(&[(p("n"), dec(stored))], 0).unwrap();
        let r = w.write_update(&[UpdateOp::Inc(p("n"), Value::Int32(1))], 0);
        assert!(matches!(r, Ok(Some(_))), "{stored}: {r:?}");
        let Value::Decimal128(b) = read(&w) else { panic!("{stored}") };
        assert_eq!(bson::Decimal128::from_bytes(b).to_string(), stored);
    }
}

/// The mirror of the property above: no accepted-and-wrong read, and no WRONG refusal. Single-writer histories
/// whose magnitudes stay within 30 significant digits of each other (every operand below 10^15 with at most 6
/// fraction digits, a dozen steps), of `$inc` by an Int32, an Int64 or a Decimal128, numeric and non-numeric
/// `$set`s and `$unset`: no `$inc` is refused (the integer bound is out of reach by construction), whatever the
/// counter was set to and absorbed before.
#[test]
fn no_increment_of_a_counter_within_30_digits_is_refused() {
    let mut seed = 29u64;
    let mut incs = 0usize;
    for history in 0..4000 {
        let mut w = writer();
        w.insert(&[(p("n"), Value::Int32(0))], 0).unwrap();
        let mut numeric = true;
        for step in 0..12 {
            let what = format!("history {history}, step {step}");
            let r = lcg(&mut seed) % 10;
            if numeric && r < 6 {
                let op = match lcg(&mut seed) % 3 {
                    0 => Value::Int32((lcg(&mut seed) % 2001) as i32 - 1000),
                    1 => Value::Int64((lcg(&mut seed) % (1 << 41)) as i64 - (1 << 40)),
                    _ => random_decimal(&mut seed),
                };
                let res = w.write_update(&[UpdateOp::Inc(p("n"), op.clone())], 0);
                assert!(matches!(res, Ok(Some(_))), "{what}: {op:?} refused: {res:?}");
                incs += 1;
            } else if r < 8 {
                let v = match lcg(&mut seed) % 3 {
                    0 => Value::Int32((lcg(&mut seed) % 2001) as i32 - 1000),
                    1 => Value::Int64((lcg(&mut seed) % (1 << 41)) as i64 - (1 << 40)),
                    _ => random_decimal(&mut seed),
                };
                numeric = true;
                let _ = w.write_set(&p("n"), v, 0);
            } else if r == 8 {
                numeric = false;
                let _ = w.write_set(&p("n"), Value::String("text".into()), 0);
            } else {
                // an unset: the next increment starts a counter again
                numeric = true;
                let _ = w.write_unset(&p("n"), 0);
            }
        }
    }
    assert!(incs > 15_000, "{incs} increments");
}

/// A decimal below 10^15 with up to 6 fraction digits, of either sign.
fn random_decimal(seed: &mut u64) -> Value {
    let whole = lcg(seed) % 1_000_000_000_000_000;
    let places = (lcg(seed) % 7) as usize;
    let frac: String = (0..places).map(|_| char::from(b'0' + (lcg(seed) % 10) as u8)).collect();
    let sign = if lcg(seed).is_multiple_of(2) { "-" } else { "" };
    dec(&if places == 0 { format!("{sign}{whole}") } else { format!("{sign}{whole}.{frac}") })
}
