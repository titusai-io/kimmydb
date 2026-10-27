//! `kimmy_core::decimal::arith::func` against decNumber (ADR-252), through the
//! `dec` crate, at 120 digits: the reference is each function computed with a
//! context of 120 digits and an exponent range of a million, then rounded here to
//! 34 digits half to even in the Decimal128 range (overflow is an infinity,
//! underflow is gradual), by integer arithmetic of our own.
//!
//! A 120-digit decNumber result is within a unit of its last digit of the true
//! value, so the rounding to 34 differs from the correctly rounded value only
//! where the true value is within `10^-85` of a midpoint, which none of these
//! inputs is. A disagreement is therefore a bug in one side, and none is excused.
//!
//! The cohorts are normalised on both sides (trailing zeros off while the
//! exponent is at most 6111), so values and cohorts are compared together, byte
//! for byte of the decoded form. A whole `y` of at most 1024 in magnitude is also
//! checked against the exact power from big integers, because decNumber's own
//! power at 120 digits is not exact for a 34-digit base.
//!
//! The inputs are pseudo-random from a fixed generator: magnitudes from `1E-6176`
//! to `1E+6111`, values near 1 (where `ln` and `pow` lose the most), the
//! thresholds of overflow and underflow, whole and fractional exponents, and both
//! signs of the base.

use std::cmp::Ordering;

use dec::{Context, Decimal};
use kimmy_core::decimal::arith::func;
use kimmy_core::decimal::{MAX_COEFFICIENT, MAX_EXPONENT, MIN_EXPONENT, Raw};
use num_bigint::BigUint;

type Wide = Decimal<40>;

const ETINY: i64 = MIN_EXPONENT as i64;
const ETOP: i64 = MAX_EXPONENT as i64;

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn fin(negative: bool, coefficient: u128, exponent: i64) -> Raw {
    Raw::Finite { negative, coefficient, exponent: exponent as i16 }
}

fn text(raw: &Raw) -> String {
    match raw {
        Raw::Nan => "NaN".into(),
        Raw::Inf { negative } => format!("{}Infinity", if *negative { "-" } else { "" }),
        Raw::Finite { negative, coefficient, exponent } => {
            format!("{}{}E{}", if *negative { "-" } else { "" }, coefficient, exponent)
        }
    }
}

fn context() -> Context<Wide> {
    let mut ctx = Context::<Wide>::default();
    ctx.set_max_exponent(900_000).expect("an exponent");
    ctx.set_min_exponent(-900_000).expect("an exponent");
    ctx
}

fn wide(ctx: &mut Context<Wide>, raw: &Raw) -> Wide {
    ctx.parse(text(raw)).expect("decNumber reads it")
}

/// A decNumber result as `(negative, digits, exponent)`, or the special.
enum Answer {
    Nan,
    Inf(bool),
    Finite(bool, BigUint, i64),
}

fn read(d: &Wide) -> Answer {
    if d.is_nan() {
        return Answer::Nan;
    }
    let s = d.to_string();
    let (negative, s) = match s.strip_prefix('-') {
        Some(rest) => (true, rest.to_string()),
        None => (false, s),
    };
    if d.is_infinite() {
        return Answer::Inf(negative);
    }
    let (mantissa, exponent) = match s.split_once(['E', 'e']) {
        Some((m, e)) => (m.to_string(), e.parse::<i64>().expect("an exponent")),
        None => (s.clone(), 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((&mantissa, ""));
    let digits = format!("{whole}{fraction}");
    let n = digits.parse::<BigUint>().expect("digits");
    Answer::Finite(negative, n, exponent - fraction.len() as i64)
}

/// `n · 10^e` (and a sub-unit remainder if `sticky`) rounded half to even to
/// 34 digits in the Decimal128 range, and normalised (trailing zeros off while
/// the exponent is at most 6111; a zero is `+0` at exponent 0 but for `negative`).
fn round34(negative: bool, n: &BigUint, e: i64, sticky: bool) -> Raw {
    let zero = BigUint::from(0u8);
    if *n == zero {
        return fin(negative, 0, 0);
    }
    let digits = n.to_string().len() as i64;
    let cut = (digits - 34).max(ETINY - e).max(0);
    let p = BigUint::from(10u8).pow(cut as u32);
    let (q, r) = (n / &p, n % &p);
    let up = match (&r * 2u8).cmp(&p) {
        Ordering::Greater => true,
        Ordering::Equal => sticky || &q % 2u8 == BigUint::from(1u8),
        Ordering::Less => false,
    };
    let mut q = q + BigUint::from(u8::from(up));
    let mut e = e + cut;
    if q.to_string().len() > 34 {
        q /= 10u8;
        e += 1;
    }
    if q == zero {
        return fin(negative, 0, 0);
    }
    let mut c: u128 = q.to_string().parse().expect("34 digits");
    if e + i64::from(c.ilog10()) > 6144 {
        return Raw::Inf { negative };
    }
    if e > ETOP {
        c *= 10u128.pow((e - ETOP) as u32);
        e = ETOP;
    }
    while c.is_multiple_of(10) && e < ETOP {
        c /= 10;
        e += 1;
    }
    fin(negative, c, e)
}

fn expected(answer: Answer) -> Result<Raw, ()> {
    match answer {
        Answer::Nan => Err(()),
        Answer::Inf(negative) => Ok(Raw::Inf { negative }),
        Answer::Finite(negative, n, e) => Ok(round34(negative, &n, e, false)),
    }
}

/// The `x` of a monotone, random shape: any width, any magnitude.
fn random_positive(state: &mut u64) -> Raw {
    let digits = 1 + splitmix(state) % 34;
    let wide = u128::from(splitmix(state)) << 64 | u128::from(splitmix(state));
    let c = (wide % 10u128.pow(digits as u32)).max(1);
    let e = match splitmix(state) % 5 {
        0 => (splitmix(state) % 80) as i64 - 40,
        _ => (splitmix(state) % 12_288) as i64 - 6176,
    };
    fin(false, c, e.clamp(ETINY, ETOP))
}

/// `1 + d` or `1 - d` for a `d` from `10^-1` to `10^-33`, at the widest the
/// format holds (34 digits).
fn near_one(state: &mut u64) -> Raw {
    let k = (1 + splitmix(state) % 33) as u32; // d is about 10^-k
    let unit = 10u128.pow(34 - k);
    let d = (1 + u128::from(splitmix(state) % 9)) * unit + u128::from(splitmix(state)) % unit;
    if splitmix(state).is_multiple_of(2) {
        fin(false, 10u128.pow(34) - d, -34)
    } else {
        fin(false, 10u128.pow(33) + d / 10, -33)
    }
}

fn compare(
    name: &str,
    inputs: &[String],
    got: Result<Raw, func::Domain>,
    want: Result<Raw, ()>,
    failures: &mut Vec<String>,
) {
    let ok = match (&got, &want) {
        (Ok(a), Ok(b)) => a == b,
        (Err(_), Err(())) => true,
        _ => false,
    };
    if !ok {
        failures.push(format!(
            "{name}({}): got {}, want {}",
            inputs.join(", "),
            got.map_or("Domain".to_string(), |r| text(&r)),
            want.map_or("NaN".to_string(), |r| text(&r))
        ));
    }
}

fn report(name: &str, count: usize, failures: &[String]) {
    eprintln!("{name}: {count} inputs, {} disagreements", failures.len());
    assert!(
        failures.is_empty(),
        "{name}: {} of {count} disagree with decNumber:\n{}",
        failures.len(),
        failures.iter().take(20).cloned().collect::<Vec<_>>().join("\n")
    );
}

#[test]
fn sqrt_agrees_with_decnumber() {
    let mut ctx = context();
    let mut state = 1u64;
    let mut failures = Vec::new();
    let mut count = 0;
    for i in 0..4000 {
        let x = match i % 8 {
            // Exact squares and their neighbours.
            0 => {
                let root = u128::from(splitmix(&mut state)) % 10u128.pow(17) + 1;
                let sq = root * root;
                fin(
                    false,
                    sq + u128::from(splitmix(&mut state) % 3) - 1,
                    (splitmix(&mut state) % 400) as i64 - 200,
                )
            }
            1 => fin(false, MAX_COEFFICIENT - splitmix(&mut state) as u128 % 1000, ETOP),
            2 => fin(false, 1 + u128::from(splitmix(&mut state) % 1000), ETINY),
            _ => random_positive(&mut state),
        };
        let x = if matches!(x, Raw::Finite { coefficient, .. } if coefficient > MAX_COEFFICIENT) {
            fin(false, 1, 0)
        } else {
            x
        };
        let mut w = wide(&mut ctx, &x);
        ctx.sqrt(&mut w);
        compare("sqrt", &[text(&x)], func::sqrt(&x), expected(read(&w)), &mut failures);
        count += 1;
    }
    report("sqrt", count, &failures);
}

fn exp_input(state: &mut u64, i: usize) -> Raw {
    let digits = 1 + splitmix(state) % 34;
    let c = ((u128::from(splitmix(state)) << 64 | u128::from(splitmix(state)))
        % 10u128.pow(digits as u32))
    .max(1);
    let negative = splitmix(state).is_multiple_of(2);
    match i % 8 {
        // Over the range where the result is finite: |x| below 10^4.
        0..=2 => {
            let adjusted = (splitmix(state) % 4) as i64;
            fin(negative, c, adjusted + 1 - digits as i64)
        }
        // Small: |x| from 10^-1 down to 10^-40 and below.
        3 => fin(negative, c, -(digits as i64) - (splitmix(state) % 40) as i64),
        // Around the overflow threshold: ln(10^6145) = 14149.38539644841072829055748903541...
        4 => {
            let delta = u128::from(splitmix(state) % 2_000_000);
            fin(false, 14_149_385_396_448_410_728_290_557_489_035u128 + delta - 1_000_000, -27)
        }
        // Around the underflow threshold: ln(5E-6177) = -14221.4586815117860898045324562520...
        5 => {
            let delta = u128::from(splitmix(state) % 2_000_000);
            fin(true, 14_221_458_681_511_786_089_804_532_456_252u128 + delta - 1_000_000, -27)
        }
        // The subnormal range: results from 10^-6000 down to the smallest.
        6 => {
            let whole = 13_800 + u128::from(splitmix(state) % 422);
            fin(true, whole * 10u128.pow(20) + u128::from(splitmix(state)) % 10u128.pow(20), -20)
        }
        // A multiple of ln 10, and a hair off it.
        _ => {
            let k = 1 + u128::from(splitmix(state) % 6000);
            let mut product = k * 2_302_585_092_994_045_684_017_991_454_684_364u128
                + u128::from(splitmix(state) % 5)
                - 2;
            let mut e = -33;
            while product > MAX_COEFFICIENT {
                product /= 10;
                e += 1;
            }
            fin(negative, product, e)
        }
    }
}

#[test]
fn exp_agrees_with_decnumber() {
    let mut ctx = context();
    let mut state = 2u64;
    let mut failures = Vec::new();
    let mut count = 0;
    let mut finite = 0;
    for i in 0..4200 {
        let x = exp_input(&mut state, i);
        let Raw::Finite { coefficient, .. } = x else { continue };
        if coefficient > MAX_COEFFICIENT {
            continue;
        }
        let mut w = wide(&mut ctx, &x);
        ctx.exp(&mut w);
        let want = expected(read(&w));
        if matches!(want, Ok(Raw::Finite { coefficient, .. }) if coefficient != 0) {
            finite += 1;
        }
        compare("exp", &[text(&x)], Ok(func::exp(&x)), want, &mut failures);
        count += 1;
    }
    eprintln!("exp: {finite} of {count} results finite and nonzero");
    assert!(finite > 2500);
    report("exp", count, &failures);
}

fn log_input(state: &mut u64, i: usize) -> Raw {
    match i % 7 {
        0 | 1 => near_one(state),
        2 => {
            // A power of ten, and one unit either side.
            let k = (splitmix(state) % 12_288) as i64 - 6176;
            let (c, e) = match splitmix(state) % 3 {
                0 => (10u128.pow(33), k - 33),
                1 => (10u128.pow(33) + 1, k - 33),
                _ => (MAX_COEFFICIENT, k - 34),
            };
            fin(false, c, e.clamp(ETINY, ETOP))
        }
        3 => {
            // A power of two.
            let k = splitmix(state) % 20_000;
            let mut c = BigUint::from(1u8) << (k as usize % 110);
            let e = (k as i64 / 110) * 3 - 120;
            while c.to_string().len() > 34 {
                c /= 10u8;
            }
            fin(false, c.to_string().parse().expect("a coefficient"), e)
        }
        _ => random_positive(state),
    }
}

#[test]
fn ln_agrees_with_decnumber() {
    let mut ctx = context();
    let mut state = 3u64;
    let mut failures = Vec::new();
    let mut count = 0;
    for i in 0..4200 {
        let x = log_input(&mut state, i);
        let mut w = wide(&mut ctx, &x);
        ctx.ln(&mut w);
        compare("ln", &[text(&x)], func::ln(&x), expected(read(&w)), &mut failures);
        count += 1;
    }
    report("ln", count, &failures);
}

#[test]
fn log10_agrees_with_decnumber() {
    let mut ctx = context();
    let mut state = 4u64;
    let mut failures = Vec::new();
    let mut count = 0;
    for i in 0..4200 {
        let x = log_input(&mut state, i);
        let mut w = wide(&mut ctx, &x);
        ctx.log10(&mut w);
        compare("log10", &[text(&x)], func::log10(&x), expected(read(&w)), &mut failures);
        count += 1;
    }
    report("log10", count, &failures);
}

/// A random decimal `y`, `1 <= |y| < limit`, with up to 6 fractional digits.
fn random_y(state: &mut u64, limit: u64) -> Raw {
    let k = (splitmix(state) % 7) as u32;
    let whole = u128::from(1 + splitmix(state) % limit);
    let fraction = u128::from(splitmix(state)) % 10u128.pow(k);
    fin(splitmix(state).is_multiple_of(2), whole * 10u128.pow(k) + fraction, -i64::from(k))
}

/// A whole `y` of magnitude `low..low + span`.
fn whole(state: &mut u64, low: u64, span: u64) -> Raw {
    let n = low + splitmix(state) % span;
    fin(splitmix(state).is_multiple_of(2), u128::from(n), 0)
}

/// Whether the exact power `x^n` is small enough to form here cheaply.
fn exact_feasible(x: &Raw, n: u64) -> bool {
    let Raw::Finite { coefficient, .. } = x else { return false };
    (coefficient.to_string().len() as u64 - 1) * n <= 1200
}

/// The exact `x^y` for a whole `y` from big integers, rounded once.
fn exact_power(x: &Raw, y: &Raw) -> Raw {
    let (
        Raw::Finite { negative: xn, coefficient: xc, exponent: xe },
        Raw::Finite { negative: yn, coefficient: yc, .. },
    ) = (x, y)
    else {
        panic!("finite")
    };
    let n = *yc as u32;
    let negative = *xn && n % 2 == 1;
    let power = BigUint::from(*xc).pow(n);
    let e = i64::from(*xe) * i64::from(n);
    if !yn {
        return round34(negative, &power, e, false);
    }
    let s = power.to_string().len() as u32 + 45;
    let numerator = BigUint::from(10u8).pow(s);
    let (q, r) = (&numerator / &power, &numerator % &power);
    round34(negative, &q, -e - i64::from(s), r != BigUint::from(0u8))
}

#[test]
fn pow_agrees_with_decnumber() {
    let mut ctx = context();
    let mut state = 5u64;
    let mut failures = Vec::new();
    let (mut count, mut finite, mut exact) = (0, 0, 0);
    let cases = if cfg!(debug_assertions) { 2700 } else { 9000 };
    for i in 0..cases {
        let (x, y) = match i % 9 {
            // A fractional exponent on a positive base of ordinary size.
            0 | 1 => (
                fin(
                    false,
                    u128::from(splitmix(&mut state)) % 10u128.pow(20) + 1,
                    (splitmix(&mut state) % 30) as i64 - 20,
                ),
                random_y(&mut state, 60),
            ),
            // A base of any magnitude, a whole exponent past the exact path.
            2 => (random_positive(&mut state), whole(&mut state, 1025, 4000)),
            // Near 1 to a large exponent.
            3 | 4 => {
                let y = fin(
                    splitmix(&mut state).is_multiple_of(2),
                    u128::from(splitmix(&mut state) % 10_000_000_000) + 1,
                    (splitmix(&mut state) % 25) as i64,
                );
                (near_one(&mut state), y)
            }
            // The overflow and underflow boundary of x^y: y about 6140 / log10 x.
            5 => {
                let x = random_positive(&mut state);
                let Raw::Finite { coefficient, exponent, .. } = x else { unreachable!() };
                let log10_x = (coefficient as f64).log10() + f64::from(exponent);
                let target = 6140.0 + (splitmix(&mut state) % 10) as f64 * 0.7;
                let scaled = (target / log10_x * 1e6).abs().round();
                if log10_x.abs() < 1e-9 || scaled < 1.0 || scaled > 1e15 {
                    (x, fin(false, 1, 0))
                } else {
                    (x, fin(splitmix(&mut state).is_multiple_of(2), scaled as u128, -6))
                }
            }
            // A negative base and a whole exponent.
            6 => {
                let x = random_positive(&mut state).negated();
                (x, whole(&mut state, 1, 3000))
            }
            // A power of ten and a power of two to a fractional exponent.
            7 => (
                fin(
                    false,
                    [1u128, 2, 4, 5, 10][(splitmix(&mut state) % 5) as usize],
                    (splitmix(&mut state) % 400) as i64 - 200,
                ),
                random_y(&mut state, 40),
            ),
            // Huge or tiny exponents on bases near 1.
            _ => {
                let y = fin(
                    splitmix(&mut state).is_multiple_of(2),
                    u128::from(splitmix(&mut state) % 1_000_000_000_000) + 1,
                    (splitmix(&mut state) % 28) as i64 - 6,
                );
                (near_one(&mut state), y)
            }
        };
        let (
            Raw::Finite { coefficient: xc, .. },
            Raw::Finite { coefficient: yc, exponent: ye, .. },
        ) = (&x, &y)
        else {
            continue;
        };
        if *xc > MAX_COEFFICIENT || *yc > MAX_COEFFICIENT || *xc == 0 || *yc == 0 {
            continue;
        }
        // decNumber refuses a non-integer power of a negative; a huge integer y it computes.
        let integral = *ye >= 0 || yc % 10u128.pow((-ye) as u32) == 0;
        let want = if let Raw::Finite { negative: true, .. } = x {
            if !integral {
                Err(())
            } else {
                let mut w = wide(&mut ctx, &x);
                let wy = wide(&mut ctx, &y);
                ctx.pow(&mut w, &wy);
                expected(read(&w))
            }
        } else {
            let mut w = wide(&mut ctx, &x);
            let wy = wide(&mut ctx, &y);
            ctx.pow(&mut w, &wy);
            expected(read(&w))
        };
        let got = func::pow(&x, &y);
        // A small whole y: the exact power from big integers too.
        if integral
            && matches!(y, Raw::Finite { exponent: 0, coefficient, .. } if coefficient <= 1024 && exact_feasible(&x, coefficient as u64))
        {
            let exact_want = exact_power(&x, &y);
            compare("pow (exact)", &[text(&x), text(&y)], got, Ok(exact_want), &mut failures);
            exact += 1;
        } else {
            if matches!(want, Ok(Raw::Finite { coefficient, .. }) if coefficient != 0) {
                finite += 1;
            }
            compare("pow", &[text(&x), text(&y)], got, want, &mut failures);
        }
        count += 1;
    }
    eprintln!(
        "pow: {finite} of {count} results finite and nonzero by decNumber, {exact} against big integers"
    );
    assert!(finite > if cfg!(debug_assertions) { 900 } else { 3000 });
    report("pow", count, &failures);
}

#[test]
fn pow_of_small_whole_exponents_is_the_exact_power() {
    let mut state = 6u64;
    let mut failures = Vec::new();
    let mut count = 0;
    let cases = if cfg!(debug_assertions) { 600 } else { 4000 };
    for i in 0..cases {
        let x = match i % 4 {
            0 => random_positive(&mut state),
            1 => random_positive(&mut state).negated(),
            2 => fin(
                splitmix(&mut state).is_multiple_of(2),
                u128::from(splitmix(&mut state) % 100_000) + 1,
                (splitmix(&mut state) % 10) as i64 - 6,
            ),
            // A coefficient ending in 5 whose square has 35 digits ending in 5: ties.
            _ => fin(
                false,
                5 + 10 * (u128::from(splitmix(&mut state)) % 10u128.pow(16)),
                (splitmix(&mut state) % 20) as i64 - 10,
            ),
        };
        let y = whole(&mut state, 1, 1024);
        let Raw::Finite { coefficient, .. } = x else { continue };
        if coefficient > MAX_COEFFICIENT {
            continue;
        }
        let got = func::pow(&x, &y);
        let power_digits = (coefficient.to_string().len() as u64 - 1)
            * (match y {
                Raw::Finite { coefficient, .. } => coefficient as u64,
                _ => 1,
            });
        if power_digits > if cfg!(debug_assertions) { 500 } else { 3000 } {
            // Past the exact path's cap the result is the logarithm route's: not
            // checked here (the decNumber test above covers it).
            continue;
        }
        compare("pow (exact)", &[text(&x), text(&y)], got, Ok(exact_power(&x, &y)), &mut failures);
        count += 1;
    }
    report("pow exact", count, &failures);
}
