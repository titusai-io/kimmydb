//! The decimal functions (`func.rs`) against hand-written tables of known values
//! (checked against Python's `decimal`, a correctly rounded implementation), the
//! rules for every special value, an exact-integer reference for `round`,
//! `trunc`, `ceil` and `floor`, `num-bigint` for the `Mag` helpers, and
//! properties. The oracle against decNumber at 120 digits is in
//! `tests/decimal_func_oracle.rs`.

use std::cmp::Ordering;

use num_bigint::{BigInt, BigUint};
use num_rational::BigRational;

use super::*;
use crate::decimal::{MAX_COEFFICIENT, MIN_EXPONENT, arith, decode_raw, parse};

const ETINY_: i64 = MIN_EXPONENT as i64;
const MAX_EXP: i64 = MAX_EXPONENT as i64;

fn fin(negative: bool, coefficient: u128, exponent: i16) -> Raw {
    Raw::Finite { negative, coefficient, exponent }
}

fn d(text: &str) -> Raw {
    decode_raw(&parse(text).unwrap_or_else(|e| panic!("{text}: {e}")))
}

fn show(raw: &Raw) -> String {
    match raw {
        Raw::Nan => "NaN".into(),
        Raw::Inf { negative } => format!("{}Infinity", if *negative { "-" } else { "" }),
        Raw::Finite { negative, coefficient, exponent } => {
            format!("{}{}E{}", if *negative { "-" } else { "" }, coefficient, exponent)
        }
    }
}

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Whether a `Raw` is one the format can hold.
fn valid(raw: &Raw) -> bool {
    match raw {
        Raw::Finite { coefficient, exponent, .. } => {
            *coefficient <= MAX_COEFFICIENT && (MIN_EXPONENT..=MAX_EXPONENT).contains(exponent)
        }
        _ => true,
    }
}

/// The exact value of a finite `Raw`.
fn value(raw: &Raw) -> BigRational {
    let Raw::Finite { negative, coefficient, exponent } = *raw else { panic!("finite") };
    let c = BigInt::from(coefficient) * if negative { -1 } else { 1 };
    let ten = BigInt::from(10);
    let e = i32::from(exponent);
    if e >= 0 {
        BigRational::from_integer(c * ten.pow(e as u32))
    } else {
        BigRational::new(c, ten.pow((-e) as u32))
    }
}

/// `a` against `b` as numbers (an infinity is beyond every finite value): by
/// sign, adjusted exponent and the leading 34 digits, with no big numbers.
fn order(a: &Raw, b: &Raw) -> Ordering {
    fn key(raw: &Raw) -> (i8, i64, u128) {
        match *raw {
            Raw::Nan => panic!("a number"),
            Raw::Inf { negative } => (if negative { -2 } else { 2 }, 0, 0),
            Raw::Finite { coefficient: 0, .. } => (0, 0, 0),
            Raw::Finite { negative, coefficient, exponent } => {
                let digits = i64::from(digits_u128(coefficient));
                let lead = coefficient * 10u128.pow((34 - digits) as u32);
                (if negative { -1 } else { 1 }, digits + i64::from(exponent) - 1, lead)
            }
        }
    }
    let (ka, kb) = (key(a), key(b));
    match ka.0.cmp(&kb.0) {
        Ordering::Equal if ka.0 > 0 => (ka.1, ka.2).cmp(&(kb.1, kb.2)),
        Ordering::Equal if ka.0 < 0 => (kb.1, kb.2).cmp(&(ka.1, ka.2)),
        other => other,
    }
}

fn big(m: &Mag) -> BigUint {
    m.0.iter()
        .rev()
        .fold(BigUint::from(0u8), |acc, &l| acc * BigUint::from(LIMB) + BigUint::from(l))
}

fn mag(b: &BigUint) -> Mag {
    let digits = b.to_string();
    let mut limbs = Vec::new();
    let mut end = digits.len();
    while end > 0 {
        let start = end.saturating_sub(19);
        limbs.push(digits[start..end].parse::<u64>().expect("a limb"));
        end = start;
    }
    let mut m = Mag(limbs);
    m.trim();
    m
}

/// A random magnitude of `limbs` limbs, with edge limbs often.
fn random_mag(state: &mut u64, limbs: usize) -> Mag {
    let edge = [0, 1, 2, LIMB / 2 - 1, LIMB / 2, LIMB / 2 + 1, LIMB - 2, LIMB - 1];
    let mut m = Mag((0..limbs)
        .map(|_| {
            let r = splitmix(state);
            if r.is_multiple_of(3) {
                edge[(r >> 8) as usize % edge.len()]
            } else {
                splitmix(state) % LIMB
            }
        })
        .collect());
    m.trim();
    m
}

fn random_up_to(state: &mut u64, max: u64) -> Mag {
    let limbs = (splitmix(state) % max) as usize;
    random_mag(state, limbs)
}

fn normalised(text: &str) -> Raw {
    normalise(d(text))
}

fn call(function: &str, x: &Raw, y: &Raw) -> Result<Raw, Domain> {
    match function {
        "sqrt" => sqrt(x),
        "exp" => Ok(exp(x)),
        "ln" => ln(x),
        "log10" => log10(x),
        "pow" => pow(x, y),
        other => panic!("{other}"),
    }
}

// ---------------------------------------------------------------------------
// The helpers
// ---------------------------------------------------------------------------

#[test]
fn the_column_product_is_the_product() {
    let mut state = 1u64;
    for _ in 0..3000 {
        let (la, lb) = ((splitmix(&mut state) % 14) as usize, (splitmix(&mut state) % 14) as usize);
        let (a, b) = (random_mag(&mut state, la), random_mag(&mut state, lb));
        assert_eq!(big(&m_mul(&a, &b)), big(&a) * big(&b));
        assert_eq!(m_mul(&a, &b), a.mul(&b), "the same product as Mag::mul");
    }
    // The largest limbs everywhere: every partial product is as large as it gets.
    for n in 1..=12 {
        let a = Mag(vec![LIMB - 1; n]);
        assert_eq!(big(&m_mul(&a, &a)), big(&a) * big(&a));
    }
}

#[test]
fn sums_and_differences_are_exact() {
    let mut state = 2u64;
    for _ in 0..3000 {
        let (la, lb) = ((splitmix(&mut state) % 9) as usize, (splitmix(&mut state) % 9) as usize);
        let (a, b) = (random_mag(&mut state, la), random_mag(&mut state, lb));
        assert_eq!(big(&m_add(&a, &b)), big(&a) + big(&b));
        let (hi, lo) = if m_cmp(&a, &b) == Ordering::Less { (&b, &a) } else { (&a, &b) };
        assert_eq!(big(&m_sub(hi, lo)), big(hi) - big(lo));
        assert_eq!(m_cmp(&a, &b), big(&a).cmp(&big(&b)));
    }
}

#[test]
fn division_by_a_small_number_is_exact() {
    let mut state = 3u64;
    for _ in 0..3000 {
        let a = random_up_to(&mut state, 9);
        let d = splitmix(&mut state) % (1 << (1 + splitmix(&mut state) % 63)) + 1;
        let (q, r) = m_div_small(&a, d);
        assert_eq!(big(&q), big(&a) / BigUint::from(d));
        assert_eq!(BigUint::from(r), big(&a) % BigUint::from(d));
    }
}

#[test]
fn long_division_is_exact() {
    let mut state = 4u64;
    for round in 0..60_000 {
        let lb = 2 + (splitmix(&mut state) % 6) as usize;
        let la = lb + (splitmix(&mut state) % 7) as usize;
        let (a, b) = (random_mag(&mut state, la), random_mag(&mut state, lb));
        if b.is_zero() {
            continue;
        }
        let (q, r) = m_div_rem(&a, &b);
        let (bq, br) = (big(&a) / big(&b), big(&a) % big(&b));
        assert_eq!((big(&q), big(&r)), (bq, br), "round {round}: {:?} / {:?}", a.0, b.0);
    }
}

/// The estimate of a quotient limb is two too large in the cases this builds
/// (a divisor with a near-maximal second limb under a dividend that is a
/// multiple of a power of the base), which is what the correction steps and the
/// add-back are for.
#[test]
fn long_division_survives_a_bad_estimate() {
    let edge = [0, 1, LIMB / 2, LIMB - 2, LIMB - 1];
    let mut cases = 0;
    for &v2 in &edge {
        for &v1 in &edge {
            for &v0 in &edge {
                for &u3 in &edge {
                    for &u2 in &edge {
                        for &u1 in &edge {
                            for &u0 in &edge {
                                let mut b = Mag(vec![v0, v1, v2]);
                                b.trim();
                                let mut a = Mag(vec![u0, u1, u2, u3, 0]);
                                a.trim();
                                if b.is_zero() || b.0.len() < 2 {
                                    continue;
                                }
                                let (q, r) = m_div_rem(&a, &b);
                                assert_eq!(big(&q), big(&a) / big(&b));
                                assert_eq!(big(&r), big(&a) % big(&b));
                                cases += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    assert!(cases > 50_000);
}

#[test]
fn the_square_root_is_the_floor_root() {
    let mut state = 5u64;
    for _ in 0..3000 {
        let n = random_up_to(&mut state, 8);
        assert_eq!(big(&m_isqrt(&n)), big(&n).sqrt());
    }
    for k in 0..40 {
        // Perfect squares and their neighbours.
        let root = BigUint::from(10u8).pow(k) + BigUint::from(k);
        let square = &root * &root;
        for n in [square.clone() - BigUint::from(1u8), square.clone(), square + BigUint::from(1u8)]
        {
            assert_eq!(big(&m_isqrt(&mag(&n))), n.sqrt());
        }
    }
}

#[test]
fn fixed_point_division_and_product_agree_with_exact() {
    let mut state = 6u64;
    let scale = BigUint::from(10u8).pow(S);
    for _ in 0..2000 {
        let a = random_up_to(&mut state, 8);
        let b = random_up_to(&mut state, 8);
        if b.is_zero() {
            continue;
        }
        assert_eq!(big(&fx_mul(&a, &b)), big(&a) * big(&b) / &scale);
        assert_eq!(big(&fx_div(&a, &b)), big(&a) * &scale / big(&b));
    }
}

#[test]
fn the_constants_are_ln_2_and_ln_10() {
    // To 114 places, from Python's `decimal` at 160 digits.
    let ln2 = "693147180559945309417232121458176568075500134360255254120680009493393621969694715605863326996418687542001481020571";
    let ln10 = "2302585092994045684017991454684364207601101488628772976033327900967572609677352480235997205089598298341967784042286";
    let c = consts();
    // The text is the first 114 places of the true value, rounded: the
    // constant is built 19 places wider and rounded to 114, and agrees.
    assert_eq!(big(&c.ln10).to_string(), &ln10[..115]);
    assert_eq!(big(&c.ln2).to_string(), &ln2[..114]);
}

// ---------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------

/// `(function, x, y, want)`: `want` is a value (compared as the normalised
/// cohort), `Domain`, `NaN`, `Infinity` or `-Infinity`.
const TABLE: &[(&str, &str, &str, &str)] = &[
    ("sqrt", "2", "", "1.414213562373095048801688724209698"),
    ("sqrt", "4", "", "2"),
    ("sqrt", "0.25", "", "0.5"),
    ("sqrt", "1E-6176", "", "1E-3088"),
    (
        "sqrt",
        "9.999999999999999999999999999999999E+6144",
        "",
        "3.162277660168379331998893544432718E+3072",
    ),
    ("sqrt", "1E+6111", "", "3.162277660168379331998893544432719E+3055"),
    ("sqrt", "2E-6176", "", "1.414213562373095048801688724209698E-3088"),
    ("sqrt", "0.5", "", "0.7071067811865475244008443621048490"),
    ("sqrt", "3", "", "1.732050807568877293527446341505872"),
    ("sqrt", "1.21", "", "1.1"),
    ("sqrt", "144", "", "12"),
    ("sqrt", "123456789.123456789", "", "11111.11106611111096943055498174930"),
    ("sqrt", "5", "", "2.236067977499789696409173668731276"),
    ("sqrt", "1E+10", "", "1E+5"),
    ("sqrt", "17", "", "4.123105625617660549821409855974077"),
    ("sqrt", "0.000001", "", "0.001"),
    ("sqrt", "99999999999999999999999999999.99", "", "316227766016837.9331998893544432560"),
    ("sqrt", "7E-6176", "", "2.645751311064590590501615753639260E-3088"),
    ("sqrt", "1E+6144", "", "1E+3072"),
    ("exp", "1", "", "2.718281828459045235360287471352662"),
    ("exp", "-1", "", "0.3678794411714423215955237701614609"),
    ("exp", "0.5", "", "1.648721270700128146848650787814164"),
    ("exp", "10", "", "22026.46579480671651695790064528424"),
    ("exp", "-10", "", "0.00004539992976248485153559151556055061"),
    ("exp", "100", "", "2.688117141816135448412625551580014E+43"),
    ("exp", "-100", "", "3.720075976020835962959695803863118E-44"),
    ("exp", "1000", "", "1.970071114017046993888879352243323E+434"),
    ("exp", "14149", "", "6.801809260978894125530050851897730E+6144"),
    ("exp", "14150", "", "Infinity"),
    ("exp", "14149.4", "", "Infinity"),
    ("exp", "-14222", "", "0E-6176"),
    ("exp", "-14223", "", "0E-6176"),
    ("exp", "-14300", "", "0E-6176"),
    ("exp", "0.0000000001", "", "1.000000000100000000005000000000167"),
    ("exp", "-0.0000000001", "", "0.9999999999000000000049999999998333"),
    ("exp", "1E-40", "", "1.000000000000000000000000000000000"),
    ("exp", "34.5", "", "961965785544776.4104871247859639303"),
    ("exp", "-0.5", "", "0.6065306597126334236037995349911805"),
    ("exp", "0.6931471805599453094172321214581766", "", "2.000000000000000000000000000000000"),
    ("exp", "2.5", "", "12.18249396070347343807017595116797"),
    ("exp", "-745.13", "", "2.478293280882716614475767059690385E-324"),
    ("exp", "7000", "", "1.151790050806878291441536725002971E+3040"),
    ("exp", "-7000", "", "8.682137854025194472103888116862580E-3041"),
    ("exp", "-14000", "", "7.537951771629720907593006284141136E-6081"),
    ("ln", "2", "", "0.6931471805599453094172321214581766"),
    ("ln", "10", "", "2.302585092994045684017991454684364"),
    ("ln", "0.5", "", "-0.6931471805599453094172321214581766"),
    ("ln", "1.5", "", "0.4054651081081643819780131154643491"),
    ("ln", "1E-6176", "", "-14220.76553433122614449511522413063"),
    ("ln", "9.999999999999999999999999999999999E+6144", "", "14149.38539644841072829055748903542"),
    ("ln", "1.000000000000000000000000000000001", "", "9.999999999999999999999999999999995E-34"),
    ("ln", "0.9999999999999999999999999999999999", "", "-1.000000000000000000000000000000000E-34"),
    ("ln", "3", "", "1.098612288668109691395245236922526"),
    ("ln", "100", "", "4.605170185988091368035982909368728"),
    ("ln", "1E+100", "", "230.2585092994045684017991454684364"),
    ("ln", "123456.789", "", "11.72364648718588098113995898391011"),
    ("ln", "0.1", "", "-2.302585092994045684017991454684364"),
    ("ln", "7", "", "1.945910149055313305105352743443180"),
    ("ln", "1E-3", "", "-6.907755278982137052053974364053093"),
    ("ln", "1E+6000", "", "13815.51055796427410410794872810619"),
    ("ln", "0.3", "", "-1.203972804325935992622746217761839"),
    ("log10", "2", "", "0.3010299956639811952137388947244930"),
    ("log10", "3", "", "0.4771212547196624372950279032551153"),
    ("log10", "0.5", "", "-0.3010299956639811952137388947244930"),
    ("log10", "5", "", "0.6989700043360188047862611052755070"),
    ("log10", "1000", "", "3"),
    ("log10", "0.001", "", "-3"),
    ("log10", "1E-6176", "", "-6176"),
    ("log10", "1E+6144", "", "6144"),
    ("log10", "1.000000000000000000000000000000001", "", "4.342944819032518276511289189166049E-34"),
    ("log10", "12345", "", "4.091491094267951081848996765130174"),
    (
        "log10",
        "0.9999999999999999999999999999999999",
        "",
        "-4.342944819032518276511289189166051E-35",
    ),
    ("log10", "7", "", "0.8450980400142568307122162585926362"),
    ("log10", "100000", "", "5"),
    ("log10", "0.0001", "", "-4"),
    ("log10", "1E+6111", "", "6111"),
    ("pow", "2", "0.5", "1.414213562373095048801688724209698"),
    ("pow", "2", "-2", "0.25"),
    ("pow", "2", "10", "1024"),
    ("pow", "1.1", "2", "1.21"),
    ("pow", "1.1", "-2", "0.8264462809917355371900826446280992"),
    ("pow", "10", "6111", "1.000000000000000000000000000000000E+6111"),
    ("pow", "10", "6144", "1.000000000000000000000000000000000E+6144"),
    ("pow", "10", "6145", "Infinity"),
    ("pow", "10", "-6176", "1E-6176"),
    ("pow", "10", "-6177", "0E-6176"),
    ("pow", "0.5", "1024", "5.562684646268003457725581793331010E-309"),
    ("pow", "-2", "3", "-8"),
    ("pow", "-2", "-3", "-0.125"),
    ("pow", "-2", "2", "4"),
    ("pow", "2", "1.5", "2.828427124746190097603377448419396"),
    ("pow", "3", "-1", "0.3333333333333333333333333333333333"),
    ("pow", "7", "0.3333333333333333333333333333333333", "1.912931182772389101199116839548760"),
    ("pow", "1.000000000000000000000000000000001", "1E+20", "1.000000000000100000000000005000000"),
    ("pow", "9.99", "1000", "3.676954247709640446268061392204613E+999"),
    ("pow", "0.9", "1000", "1.747871251722651609659974619164661E-46"),
    ("pow", "123456789", "3", "1881676371789154860897069"),
    ("pow", "2", "100", "1267650600228229401496703205376"),
    ("pow", "2", "-100", "7.888609052210118054117285652827862E-31"),
    ("pow", "2", "0.1", "1.071773462536293164213006325023342"),
    ("pow", "1.5", "2.5", "2.755675960631075360471944584044128"),
    ("pow", "5", "3000", "8.128548625557735440471878057468511E+2096"),
    ("pow", "2", "5000", "1.412467032139426036835209667016147E+1505"),
    ("pow", "2", "20000", "3.980276840337966592354307206191202E+6020"),
    ("pow", "2", "-20000", "2.512388057698744585180135042133610E-6021"),
    ("pow", "0.5", "-1074", "2.024022533073106183524953467189173E+323"),
    ("pow", "1.0000000000000000000000000000001", "1E+30", "1.105170918075647624811707826490241"),
    ("pow", "10", "0.5", "3.162277660168379331998893544432719"),
    ("pow", "10", "1.5", "31.62277660168379331998893544432719"),
    ("pow", "100", "0.5", "10.00000000000000000000000000000000"),
    ("pow", "0.01", "0.5", "0.1000000000000000000000000000000000"),
    ("pow", "-8", "3", "-512"),
    ("pow", "-1.5", "2", "2.25"),
    ("pow", "-1.5", "3", "-3.375"),
    ("pow", "-1.5", "-3", "-0.2962962962962962962962962962962963"),
    ("pow", "12345.678", "-2.5", "5.904901210504673707417116012086268E-11"),
    ("pow", "1E+3000", "2", "1E+6000"),
    ("pow", "1E+3000", "3", "Infinity"),
    ("pow", "1E-3000", "2", "1E-6000"),
    ("pow", "1E-3000", "3", "0E-6176"),
    (
        "pow",
        "9.999999999999999999999999999999999",
        "1000",
        "9.999999999999999999999999999999000E+999",
    ),
    ("pow", "1.1", "1024", "2.432817896953482871137299809352404E+42"),
    ("pow", "1.1", "-1024", "4.110459731705601960648799657737901E-43"),
    ("pow", "3.14159265358979323846264338327950", "25", "2683779414317.764549009928124395325"),
    ("pow", "0.001", "-3", "1E+9"),
    ("pow", "4", "0.5", "2.000000000000000000000000000000000"),
    ("pow", "8", "0.3333333333333333333333333333333333", "2.000000000000000000000000000000000"),
    ("pow", "2", "0.5E+0", "1.414213562373095048801688724209698"),
    ("pow", "1E+10", "1E+3", "Infinity"),
    ("pow", "1.01", "1E+4", "1.635828711188895969668098383848620E+43"),
    ("pow", "1.01", "1E+5", "1.372076304635232474602674744278458E+432"),
    ("pow", "1.00000001", "1E+9", "22026.46469348346165185870218274457"),
    ("pow", "0.99999999", "-1E+9", "22026.46689613004113253211599579822"),
];

#[test]
fn the_table_of_known_values() {
    let mut failures = Vec::new();
    for &(function, x, y, want) in TABLE {
        let (x, y) = (d(x), if y.is_empty() { ONE } else { d(y) });
        let want = if want == "Domain" { Err(Domain) } else { Ok(normalised(want)) };
        let got = call(function, &x, &y);
        if got != want {
            failures.push(format!(
                "{function}({}, {}): got {:?}, want {:?}",
                show(&x),
                show(&y),
                got.map(|r| show(&r)),
                want.map(|r| show(&r))
            ));
        }
    }
    assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
}

#[test]
fn the_cohort_of_a_result_is_normalised() {
    assert_eq!(sqrt(&d("4")), Ok(fin(false, 2, 0)));
    assert_eq!(sqrt(&d("4.00")), Ok(fin(false, 2, 0)));
    assert_eq!(sqrt(&d("100")), Ok(fin(false, 1, 1)));
    assert_eq!(pow(&d("1.1"), &d("2")), Ok(fin(false, 121, -2)));
    assert_eq!(pow(&d("1.10"), &d("2.0")), Ok(fin(false, 121, -2)));
    assert_eq!(log10(&d("1000")), Ok(fin(false, 3, 0)));
    assert_eq!(log10(&d("1.000E+3")), Ok(fin(false, 3, 0)));
    assert_eq!(log10(&d("0.001")), Ok(fin(true, 3, 0)));
    assert_eq!(exp(&d("0")), fin(false, 1, 0));
    assert_eq!(exp(&d("-0.00")), fin(false, 1, 0));
    assert_eq!(ln(&d("1")), Ok(fin(false, 0, 0)));
    assert_eq!(ln(&d("1.0000")), Ok(fin(false, 0, 0)));
    // A power of ten keeps a coefficient of 1 up to exponent 6111, then pads.
    assert_eq!(pow(&d("10"), &d("6111")), Ok(fin(false, 1, 6111)));
    assert_eq!(pow(&d("10"), &d("6144")), Ok(fin(false, 10u128.pow(33), 6111)));
    assert_eq!(pow(&d("10"), &d("6145")), Ok(Raw::Inf { negative: false }));
    assert_eq!(pow(&d("-10"), &d("6145")), Ok(Raw::Inf { negative: true }));
    assert_eq!(pow(&d("10"), &d("-6176")), Ok(fin(false, 1, -6176)));
    assert_eq!(pow(&d("10"), &d("-6177")), Ok(fin(false, 0, 0)));
    assert_eq!(pow(&d("-10"), &d("-6177")), Ok(fin(true, 0, 0)));
    assert_eq!(pow(&d("-10"), &d("-6178")), Ok(fin(false, 0, 0)));
    // The 34-digit results are what the standard digits are.
    assert_eq!(sqrt(&d("2")), Ok(fin(false, 1_414_213_562_373_095_048_801_688_724_209_698, -33)));
    assert_eq!(exp(&d("1")), fin(false, 2_718_281_828_459_045_235_360_287_471_352_662, -33));
    assert_eq!(ln(&d("2")), Ok(fin(false, 6_931_471_805_599_453_094_172_321_214_581_766, -34)));
    assert_eq!(ln(&d("10")), Ok(fin(false, 2_302_585_092_994_045_684_017_991_454_684_364, -33)));
    // `0.3010299956639811952137388947244930` without its trailing zero.
    assert_eq!(log10(&d("2")), Ok(fin(false, 301_029_995_663_981_195_213_738_894_724_493, -33)));
}

#[test]
fn the_square_root_is_exact_where_a_float_would_not_be() {
    // The squares of 17-digit roots, and a number one unit above each.
    let mut state = 77u64;
    for _ in 0..2000 {
        let root = u128::from(splitmix(&mut state)) % 10u128.pow(17) + 1;
        let e = (splitmix(&mut state) % 40) as i16 - 20;
        let x = fin(false, root, e);
        let square = arith::mul(&x, &x);
        let got = sqrt(&square).unwrap();
        assert_eq!(order(&got, &x), Ordering::Equal, "sqrt of {}", show(&square));
        let Raw::Finite { coefficient, exponent, .. } = square else { panic!() };
        if coefficient < MAX_COEFFICIENT {
            let above = fin(false, coefficient + 1, exponent);
            let got = sqrt(&above).unwrap();
            assert_eq!(
                got,
                ref_sqrt(coefficient + 1, i64::from(exponent)),
                "sqrt of {}",
                show(&above)
            );
        }
    }
}

/// Round half to even of the exact square root, from `num-bigint`.
fn ref_sqrt(c: u128, e: i64) -> Raw {
    let mut n = BigUint::from(c);
    let mut e = e;
    if e.rem_euclid(2) == 1 {
        n *= 10u8;
        e -= 1;
    }
    // 60 digits more than needed, in pairs.
    n *= BigUint::from(10u8).pow(120);
    e -= 120;
    let root = n.sqrt();
    let exact = &root * &root == n;
    round_big(false, &root, e / 2, !exact)
}

/// A magnitude `n · 10^e` (plus a sub-unit amount if `sticky`) rounded half to
/// even to 34 digits and the format's range, normalised.
fn round_big(negative: bool, n: &BigUint, e: i64, sticky: bool) -> Raw {
    let digits = n.to_string().len() as i64;
    let cut = (digits - 34).max(ETINY_ - e).max(0);
    let p = BigUint::from(10u8).pow(cut as u32);
    let (q, r) = (n / &p, n % &p);
    let twice = &r * 2u8;
    let up = match twice.cmp(&p) {
        Ordering::Greater => true,
        Ordering::Equal => sticky || (&q % 2u8) == BigUint::from(1u8),
        Ordering::Less => false,
    };
    let mut q = q + BigUint::from(u8::from(up));
    let mut e = e + cut;
    if q.to_string().len() > 34 {
        q /= 10u8;
        e += 1;
    }
    let c: u128 = q.to_string().parse().expect("34 digits");
    if c == 0 {
        return fin(negative, 0, 0);
    }
    if e + (c.ilog10() as i64) > 6144 {
        return Raw::Inf { negative };
    }
    let (c, e) = if e > MAX_EXP { (c * 10u128.pow((e - MAX_EXP) as u32), MAX_EXP) } else { (c, e) };
    normalise(fin(negative, c, e as i16))
}

// ---------------------------------------------------------------------------
// Special values
// ---------------------------------------------------------------------------

const NEG_INF: Raw = Raw::Inf { negative: true };

#[test]
fn abs_clears_the_sign_and_keeps_the_cohort() {
    assert_eq!(abs(&d("-1.50")), d("1.50"));
    assert_eq!(abs(&d("-0")), fin(false, 0, 0));
    assert_eq!(abs(&d("-0.00")), fin(false, 0, -2));
    assert_eq!(abs(&d("0E+5")), fin(false, 0, 5));
    assert_eq!(abs(&NEG_INF), INFINITY);
    assert_eq!(abs(&INFINITY), INFINITY);
    assert_eq!(abs(&NAN), NAN);
    assert_eq!(abs(&fin(true, MAX_COEFFICIENT, 6111)), fin(false, MAX_COEFFICIENT, 6111));
}

#[test]
fn ceil_and_floor_by_the_rules() {
    for (x, c, f) in [
        ("1.2", "2", "1"),
        ("-1.2", "-1", "-2"),
        ("2.00", "2", "2"),
        ("-2.00", "-2", "-2"),
        ("0.5", "1", "0"),
        ("-0.5", "-0", "-1"),
        ("0.00", "0", "0"),
        ("-0.00", "-0", "-0"),
        ("5", "5", "5"),
        ("5E+3", "5E+3", "5E+3"),
        ("0E+3", "0E+3", "0E+3"),
    ] {
        assert_eq!(show(&ceil(&d(x))), show(&d(c)), "ceil {x}");
        assert_eq!(show(&floor(&d(x))), show(&d(f)), "floor {x}");
    }
    // 999...9.9 (33 nines): ceil to 10^33, floor to the nines.
    let x = d("9999999999999999999999999999999999E-1");
    assert_eq!(ceil(&x), fin(false, 10u128.pow(33), 0));
    assert_eq!(floor(&x), fin(false, 999_999_999_999_999_999_999_999_999_999_999, 0));
    for r in [NAN, INFINITY, NEG_INF] {
        assert_eq!(ceil(&r), r);
        assert_eq!(floor(&r), r);
    }
}

#[test]
fn round_and_trunc_by_the_rules() {
    for (x, place, r, t) in [
        ("1.5", 3, "1.500", "1.500"),
        ("1234.5", -2, "1.2E+3", "1.2E+3"),
        ("2.5", 0, "2", "2"),
        ("3.5", 0, "4", "3"),
        ("-2.5", 0, "-2", "-2"),
        ("-3.5", 0, "-4", "-3"),
        ("0.5", 0, "0", "0"),
        ("-0.5", 0, "-0", "-0"),
        ("1.005", 2, "1.00", "1.00"),
        ("1.015", 2, "1.02", "1.01"),
        ("1.025", 2, "1.02", "1.02"),
        ("1.0251", 2, "1.03", "1.02"),
        ("99.99", 1, "100.0", "99.9"),
        ("99.95", 1, "100.0", "99.9"),
        ("-99.95", 1, "-100.0", "-99.9"),
        ("12345", -3, "12E+3", "12E+3"),
        ("15000", -4, "2E+4", "1E+4"),
        ("25000", -4, "2E+4", "2E+4"),
        ("0E+3", 1, "0.0", "0.0"),
        ("-0E-5", 2, "-0.00", "-0.00"),
        ("7", 2, "7.00", "7.00"),
        ("1.23", 2, "1.23", "1.23"),
    ] {
        assert_eq!(show(&round(&d(x), place)), show(&d(r)), "round({x}, {place})");
        assert_eq!(show(&trunc(&d(x), place)), show(&d(t)), "trunc({x}, {place})");
    }
    // Padding that needs more than 34 digits leaves the value alone.
    let wide = fin(false, MAX_COEFFICIENT, 0);
    assert_eq!(round(&wide, 3), wide);
    let wide = fin(false, 10u128.pow(33), 0);
    assert_eq!(round(&wide, 1), wide);
    assert_eq!(round(&fin(false, 10u128.pow(32), 0), 1), fin(false, 10u128.pow(33), -1));
    for r in [NAN, INFINITY, NEG_INF] {
        assert_eq!(round(&r, 2), r);
        assert_eq!(trunc(&r, 2), r);
    }
    // A place outside the caller's range is clamped, never a panic.
    for place in [i32::MIN, -1_000_000, -100_000, 100_000, 1_000_000, i32::MAX] {
        for x in [d("1.5"), d("0"), d("-7E+100"), fin(false, MAX_COEFFICIENT, -6176)] {
            assert!(valid(&round(&x, place)) && valid(&trunc(&x, place)));
        }
    }
}

#[test]
fn sqrt_special_values() {
    assert_eq!(sqrt(&NAN), Ok(NAN));
    assert_eq!(sqrt(&INFINITY), Ok(INFINITY));
    assert_eq!(sqrt(&NEG_INF), Err(Domain));
    assert_eq!(sqrt(&d("-1")), Err(Domain));
    assert_eq!(sqrt(&d("-1E-6176")), Err(Domain));
    assert_eq!(sqrt(&d("0")), Ok(fin(false, 0, 0)));
    assert_eq!(sqrt(&d("-0")), Ok(fin(true, 0, 0)));
    assert_eq!(sqrt(&d("0E+100")), Ok(fin(false, 0, 0)));
    assert_eq!(sqrt(&d("-0.000")), Ok(fin(true, 0, 0)));
}

#[test]
fn exp_special_values() {
    assert_eq!(exp(&NAN), NAN);
    assert_eq!(exp(&INFINITY), INFINITY);
    assert_eq!(exp(&NEG_INF), fin(false, 0, 0));
    assert_eq!(exp(&d("0")), ONE);
    assert_eq!(exp(&d("-0")), ONE);
    assert_eq!(exp(&d("1E+6000")), INFINITY);
    assert_eq!(exp(&d("-1E+6000")), fin(false, 0, 0));
    assert_eq!(exp(&d("1E-6176")), ONE);
    assert_eq!(exp(&d("-1E-6176")), ONE);
    assert_eq!(exp(&d("100000")), INFINITY);
    assert_eq!(exp(&d("-100000")), fin(false, 0, 0));
    assert_eq!(exp(&d("14150")), INFINITY);
    // Gradual underflow: exp(-14221) is 10^-6176.1, one unit of the smallest
    // subnormal; exp(-14221.5) is below half of it.
    assert_eq!(exp(&d("-14221")), fin(false, 1, -6176));
    assert_eq!(exp(&d("-14221.45")), fin(false, 1, -6176));
    assert_eq!(exp(&d("-14221.5")), fin(false, 0, 0));
}

#[test]
fn ln_and_log10_special_values() {
    for f in [ln, log10] {
        assert_eq!(f(&NAN), Ok(NAN));
        assert_eq!(f(&INFINITY), Ok(INFINITY));
        assert_eq!(f(&NEG_INF), Err(Domain));
        assert_eq!(f(&d("0")), Err(Domain));
        assert_eq!(f(&d("-0")), Err(Domain));
        assert_eq!(f(&d("0E+50")), Err(Domain));
        assert_eq!(f(&d("-1")), Err(Domain));
        assert_eq!(f(&d("-1E-6176")), Err(Domain));
        assert_eq!(f(&d("1")), Ok(fin(false, 0, 0)));
        assert_eq!(f(&d("1.000")), Ok(fin(false, 0, 0)));
    }
    for k in [-6176i64, -100, -1, 0, 1, 2, 33, 100, 6111] {
        let x = fin(false, 1, k as i16);
        assert_eq!(log10(&x), Ok(normalise(integer(k))), "log10(1E{k})");
        let padded = fin(false, 10, (k - 1).max(-6176) as i16);
        if k > -6176 {
            assert_eq!(log10(&padded), Ok(normalise(integer(k))), "log10(10E{})", k - 1);
        }
    }
    for k in [0i16, 1, 33, 6100] {
        assert_eq!(
            log10(&fin(false, 10u128.pow(33), k - 33)),
            Ok(normalise(integer(i64::from(k))))
        );
    }
}

#[test]
fn pow_special_values() {
    let zero = fin(false, 0, 0);
    let neg_zero = fin(true, 0, 0);
    let (two, three, half, minus_two) = (d("2"), d("3"), d("0.5"), d("-2"));
    // y zero is 1 whatever x is; x one is 1 whatever y is.
    for x in [NAN, INFINITY, NEG_INF, zero, neg_zero, d("-5"), d("7.5")] {
        assert_eq!(pow(&x, &zero), Ok(ONE));
        assert_eq!(pow(&x, &neg_zero), Ok(ONE));
        assert_eq!(pow(&x, &d("0E-100")), Ok(ONE));
    }
    for y in [NAN, INFINITY, NEG_INF, d("-5"), d("7.5"), zero] {
        assert_eq!(pow(&d("1"), &y), Ok(ONE));
        assert_eq!(pow(&d("1.000"), &y), Ok(ONE));
    }
    // Other NaNs.
    assert_eq!(pow(&NAN, &two), Ok(NAN));
    assert_eq!(pow(&two, &NAN), Ok(NAN));
    assert_eq!(pow(&NAN, &NAN), Ok(NAN));
    assert_eq!(pow(&d("-1"), &NAN), Ok(NAN));
    assert_eq!(pow(&zero, &NAN), Ok(NAN));
    // x zero.
    assert_eq!(pow(&zero, &two), Ok(zero));
    assert_eq!(pow(&neg_zero, &two), Ok(zero));
    assert_eq!(pow(&neg_zero, &three), Ok(neg_zero));
    assert_eq!(pow(&neg_zero, &half), Ok(zero));
    assert_eq!(pow(&neg_zero, &d("3.0")), Ok(neg_zero));
    assert_eq!(pow(&neg_zero, &d("30E-1")), Ok(neg_zero));
    assert_eq!(pow(&neg_zero, &d("1E+5")), Ok(zero));
    assert_eq!(pow(&neg_zero, &INFINITY), Ok(zero));
    assert_eq!(pow(&zero, &INFINITY), Ok(zero));
    assert_eq!(pow(&zero, &minus_two), Err(Domain));
    assert_eq!(pow(&neg_zero, &d("-3")), Err(Domain));
    assert_eq!(pow(&zero, &NEG_INF), Err(Domain));
    assert_eq!(pow(&d("0E+50"), &d("-0.5")), Err(Domain));
    // Negative x.
    assert_eq!(pow(&d("-8"), &half), Err(Domain));
    assert_eq!(pow(&d("-8"), &d("0.3333333333333333333333333333333333")), Err(Domain));
    assert_eq!(pow(&d("-8"), &d("1E-6176")), Err(Domain));
    assert_eq!(pow(&d("-2"), &three), Ok(d("-8")));
    assert_eq!(pow(&d("-2"), &d("4")), Ok(d("16")));
    assert_eq!(pow(&d("-2"), &d("-2")), Ok(d("0.25")));
    assert_eq!(pow(&d("-2"), &d("-3")), Ok(d("-0.125")));
    assert_eq!(pow(&d("-2"), &d("3.00")), Ok(d("-8")));
    assert_eq!(pow(&d("-2"), &d("1E+2")), Ok(normalised("1267650600228229401496703205376")));
    assert_eq!(pow(&d("-1"), &d("7")), Ok(d("-1")));
    assert_eq!(pow(&d("-1"), &d("1E+30")), Ok(d("1")));
    assert_eq!(pow(&d("-1"), &d("1E+30")).unwrap(), ONE);
    // Infinities.
    assert_eq!(pow(&INFINITY, &two), Ok(INFINITY));
    assert_eq!(pow(&INFINITY, &d("-2")), Ok(zero));
    assert_eq!(pow(&INFINITY, &half), Ok(INFINITY));
    assert_eq!(pow(&NEG_INF, &three), Ok(NEG_INF));
    assert_eq!(pow(&NEG_INF, &d("-3")), Ok(neg_zero));
    assert_eq!(pow(&NEG_INF, &two), Ok(INFINITY));
    assert_eq!(pow(&NEG_INF, &d("-2")), Ok(zero));
    // A negative infinity to a power that is not a whole number has no real value.
    assert_eq!(pow(&NEG_INF, &half), Err(Domain));
    assert_eq!(pow(&NEG_INF, &d("2.5")), Err(Domain));
    assert_eq!(pow(&NEG_INF, &d("-0.5")), Err(Domain));
    assert_eq!(pow(&INFINITY, &INFINITY), Ok(INFINITY));
    assert_eq!(pow(&INFINITY, &NEG_INF), Ok(zero));
    assert_eq!(pow(&NEG_INF, &INFINITY), Ok(INFINITY));
    assert_eq!(pow(&NEG_INF, &NEG_INF), Ok(zero));
    // Infinite y: by |x| against 1.
    for (x, plus, minus) in [
        ("2", INFINITY, zero),
        ("-2", INFINITY, zero),
        ("1.000000000000000000000000000000001", INFINITY, zero),
        ("0.5", zero, INFINITY),
        ("-0.5", zero, INFINITY),
        ("0.9999999999999999999999999999999999", zero, INFINITY),
        ("1E-6176", zero, INFINITY),
        ("1E+6111", INFINITY, zero),
    ] {
        let x = d(x);
        assert_eq!(pow(&x, &INFINITY), Ok(plus), "pow({}, Inf)", show(&x));
        assert_eq!(pow(&x, &NEG_INF), Ok(minus), "pow({}, -Inf)", show(&x));
    }
    assert_eq!(pow(&d("-1"), &INFINITY), Ok(ONE));
    assert_eq!(pow(&d("-1"), &NEG_INF), Ok(ONE));
    // The overflow and underflow of an exact power.
    assert_eq!(pow(&d("10"), &d("6145")), Ok(INFINITY));
    assert_eq!(pow(&d("1E+3000"), &d("3")), Ok(INFINITY));
    assert_eq!(pow(&d("1E-3000"), &d("3")), Ok(zero));
    assert_eq!(
        pow(&d("0.5"), &d("1024")),
        Ok(normalised("5.562684646268003457725581793331010E-309"))
    );
    assert_eq!(pow(&d("2"), &d("-2")), Ok(d("0.25")));
}

#[test]
fn pow_of_a_huge_exponent_is_decided_without_work() {
    for y in ["1E+6000", "1E+39", "-1E+6000", "-1E+39", "1E+100", "-1E+100", "9.99E+300"] {
        for x in [
            "2",
            "0.5",
            "1.000000000000000000000000000000001",
            "0.9999999999999999999999999999999999",
            "-2",
            "1E+6111",
            "1E-6176",
        ] {
            let r = pow(&d(x), &d(y)).unwrap();
            assert!(
                matches!(r, Raw::Inf { .. } | Raw::Finite { coefficient: 0, .. }),
                "pow({x}, {y}) = {}",
                show(&r)
            );
        }
    }
    // The sign of an overflow follows the parity of a whole y.
    assert_eq!(pow(&d("-2"), &d("1E+6000")), Ok(INFINITY));
    assert_eq!(pow(&d("-2"), &d("22777")), Ok(NEG_INF));
    assert_eq!(pow(&d("-2"), &d("22776")), Ok(INFINITY));
    assert_eq!(pow(&d("-2"), &d("-22777")), Ok(fin(true, 0, 0)));
    assert_eq!(pow(&d("-2"), &d("-22776")), Ok(fin(false, 0, 0)));
}

#[test]
fn a_decision_at_the_edge_of_the_range() {
    // exp near the overflow: ln(max) is 14149.385...
    assert!(matches!(exp(&d("14149.385")), Raw::Finite { .. }));
    assert_eq!(exp(&d("14149.386")), INFINITY);
    // pow's own: 2^20400 is about 10^6141, 2^20700 is past 10^6145.
    assert!(matches!(pow(&d("2"), &d("20400")).unwrap(), Raw::Finite { .. }));
    assert!(matches!(pow(&d("2"), &d("20700")).unwrap(), Raw::Inf { .. }));
}

// ---------------------------------------------------------------------------
// round, trunc, ceil, floor against an exact reference
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum RefMode {
    HalfEven,
    Trunc,
    Ceil,
    Floor,
}

/// The result of quantizing `±c · 10^e` to the exponent `target`, from big
/// integers: padding when the exponent is above (if it fits), a floor division
/// and a comparison with half when below.
fn quantize_reference(negative: bool, c: u128, e: i64, target: i64, mode: RefMode) -> Raw {
    let x = fin(negative, c, e as i16);
    if e >= target {
        let pad = (e - target) as u32;
        let digits = c.checked_ilog10().map_or(0, |l| l as i64 + 1);
        return if c == 0 {
            fin(negative, 0, target as i16)
        } else if digits + i64::from(pad) <= 34 {
            fin(negative, c * 10u128.pow(pad), target as i16)
        } else {
            x
        };
    }
    // Cutting more than 34 digits leaves nothing above, and less than half of 10^k.
    let k = (target - e).min(40) as u32;
    let unit = BigInt::from(10).pow(k);
    let (q, r) = (BigInt::from(c) / &unit, BigInt::from(c) % &unit);
    let twice: BigInt = &r * 2;
    let nonzero = r != BigInt::from(0);
    let up = match mode {
        RefMode::HalfEven => match twice.cmp(&unit) {
            Ordering::Greater => true,
            Ordering::Equal => &q % 2 == BigInt::from(1),
            Ordering::Less => false,
        },
        RefMode::Trunc => false,
        RefMode::Ceil => !negative && nonzero,
        RefMode::Floor => negative && nonzero,
    };
    let mut q = q + BigInt::from(i32::from(up));
    let mut target = target;
    if q.to_string().len() > 34 {
        q /= 10;
        target += 1;
    }
    let c: u128 = q.to_string().parse().expect("34 digits");
    fin(negative, c, target as i16)
}

/// Coefficients around every rounding midpoint, carry and width.
fn coefficients() -> Vec<u128> {
    let mut v: Vec<u128> = vec![
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        9,
        10,
        11,
        14,
        15,
        16,
        24,
        25,
        26,
        49,
        50,
        51,
        95,
        99,
        100,
        101,
        105,
        149,
        150,
        151,
        249,
        250,
        251,
        995,
        999,
        1000,
        1001,
        1005,
        1015,
        1025,
        1250,
        1500,
        9995,
        9999,
        12345,
        123_456_789,
        5_000_000_000,
        999_999_999_999_999,
    ];
    for k in [10u32, 17, 18, 19, 20, 32, 33] {
        let p = 10u128.pow(k);
        v.extend([p - 1, p, p + 1, 5 * p / 10, 5 * p / 10 - 1, 5 * p / 10 + 1, 15 * p / 10]);
    }
    v.extend([
        MAX_COEFFICIENT,
        MAX_COEFFICIENT - 1,
        MAX_COEFFICIENT - 4,
        MAX_COEFFICIENT - 5,
        MAX_COEFFICIENT - 6,
        1_234_567_890_123_456_789_012_345_678_901_234,
        5_000_000_000_000_000_000_000_000_000_000_000,
        4_999_999_999_999_999_999_999_999_999_999_999,
        5_000_000_000_000_000_000_000_000_000_000_001,
        9_999_999_999_999_999_999_999_999_999_999_995,
        1_000_000_000_000_000_000_000_000_000_000_005,
        1_000_000_000_000_000_000_000_000_000_000_015,
    ]);
    v.sort_unstable();
    v.dedup();
    v
}

#[test]
fn round_and_trunc_agree_with_the_reference_everywhere() {
    let exponents =
        [-6176i64, -100, -40, -35, -34, -33, -21, -20, -10, -3, -2, -1, 0, 1, 2, 5, 20, 100, 6111];
    let mut cases = 0u64;
    for &c in &coefficients() {
        for &e in &exponents {
            for negative in [false, true] {
                let x = fin(negative, c, e as i16);
                for place in -20..=100i32 {
                    let target = -i64::from(place);
                    for (got, mode, name) in [
                        (round(&x, place), RefMode::HalfEven, "round"),
                        (trunc(&x, place), RefMode::Trunc, "trunc"),
                    ] {
                        let want = quantize_reference(negative, c, e, target, mode);
                        assert_eq!(got, want, "{name}({}, {place})", show(&x));
                        cases += 1;
                    }
                }
            }
        }
    }
    assert!(cases > 100_000, "{cases}");
}

#[test]
fn ceil_and_floor_agree_with_the_reference_everywhere() {
    let exponents = [-6176i64, -100, -40, -34, -33, -20, -3, -2, -1, 0, 1, 5, 6111];
    for &c in &coefficients() {
        for &e in &exponents {
            for negative in [false, true] {
                let x = fin(negative, c, e as i16);
                for (got, mode, name) in
                    [(ceil(&x), RefMode::Ceil, "ceil"), (floor(&x), RefMode::Floor, "floor")]
                {
                    let want = if e >= 0 { x } else { quantize_reference(negative, c, e, 0, mode) };
                    assert_eq!(got, want, "{name}({})", show(&x));
                    // The integer ceil/floor of the exact rational, independently.
                    if e >= -100 {
                        let exact = value(&x);
                        let integer =
                            if mode == RefMode::Ceil { exact.ceil() } else { exact.floor() };
                        assert_eq!(value(&got), integer, "{name}({}) value", show(&x));
                    }
                }
            }
        }
    }
}

#[test]
fn the_rounding_of_a_tie_goes_to_the_even_neighbour() {
    for k in 0..30u32 {
        let half = 5 * 10u128.pow(k);
        for lead in 0..40u128 {
            // `lead` followed by a 5 and k zeros, cut at the 5.
            let c = lead * 10u128.pow(k + 1) + half;
            let x = fin(false, c, 0);
            let got = round(&x, -(k as i32 + 1));
            let Raw::Finite { coefficient, exponent, .. } = got else { panic!() };
            assert_eq!(i32::from(exponent), k as i32 + 1);
            assert_eq!(coefficient, lead + lead % 2, "round({c}, -{})", k + 1);
        }
    }
}

// ---------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------

/// A pseudo-random finite positive value of every width and magnitude.
fn random_positive(state: &mut u64) -> Raw {
    let digits = 1 + splitmix(state) % 34;
    let mut c = (u128::from(splitmix(state)) << 64 | u128::from(splitmix(state)))
        % 10u128.pow(digits as u32);
    if c == 0 {
        c = 1;
    }
    let e = (splitmix(state) % 12_000) as i64 - 6176;
    fin(false, c, e.clamp(ETINY_, MAX_EXP) as i16)
}

#[test]
fn exp_and_ln_are_monotone() {
    let mut state = 91u64;
    let mut xs: Vec<Raw> = (0..600).map(|_| random_positive(&mut state)).collect();
    // `ln` over positive values.
    xs.sort_by(order);
    xs.push(fin(false, 1, 0));
    xs.sort_by(order);
    let mut previous = ln(&xs[0]).unwrap();
    for x in &xs[1..] {
        let now = ln(x).unwrap();
        assert_ne!(order(&previous, &now), Ordering::Greater, "ln not monotone at {}", show(x));
        previous = now;
    }
    // `exp` over a spread of arguments, both signs, from tiny to overflow.
    let mut args: Vec<Raw> = Vec::new();
    for _ in 0..800 {
        let digits = 1 + splitmix(&mut state) % 30;
        let c = u128::from(splitmix(&mut state)) % 10u128.pow(digits as u32) + 1;
        let e = -(digits as i64) - (splitmix(&mut state) % 8) as i64 + 4;
        args.push(fin(splitmix(&mut state).is_multiple_of(2), c, e as i16));
    }
    args.sort_by(order);
    let mut previous = exp(&args[0]);
    for x in &args[1..] {
        let now = exp(x);
        assert_ne!(order(&previous, &now), Ordering::Greater, "exp not monotone at {}", show(x));
        previous = now;
    }
}

#[test]
fn ln_undoes_exp_to_two_units() {
    let mut state = 92u64;
    for _ in 0..1500 {
        // |x| from 1 to 14000, the exponent range of the result.
        let digits = 1 + splitmix(&mut state) % 34;
        let c = (u128::from(splitmix(&mut state)) << 64 | u128::from(splitmix(&mut state)))
            % 10u128.pow(digits as u32);
        let magnitude = (splitmix(&mut state) % 4) as i64; // 10^0 .. 10^3 before the digits
        let e = magnitude + 1 - digits as i64;
        let x = fin(
            splitmix(&mut state).is_multiple_of(2),
            c.max(10u128.pow((digits - 1) as u32)),
            e as i16,
        );
        let y = exp(&x);
        if !matches!(y, Raw::Finite { coefficient, .. } if coefficient != 0) {
            continue;
        }
        let Raw::Finite { coefficient: xc, exponent: xe, .. } = x else { unreachable!() };
        let adjusted = i64::from(digits_u128(xc)) - 1 + i64::from(xe);
        if adjusted < 0 {
            continue;
        }
        let back = ln(&y).unwrap();
        // 2 units in the last place of x.
        let ulp = fin(false, 2, (adjusted - 33) as i16);
        let difference = arith::sub(&back, &x);
        assert_ne!(
            order(&abs(&difference), &ulp),
            Ordering::Greater,
            "ln(exp({})) = {}",
            show(&x),
            show(&back)
        );
    }
}

#[test]
fn sqrt_of_an_exact_square_is_the_magnitude() {
    let mut state = 93u64;
    for _ in 0..2000 {
        let c = u128::from(splitmix(&mut state)) % 10u128.pow(17);
        let e = (splitmix(&mut state) % 400) as i16 - 200;
        let x = fin(splitmix(&mut state).is_multiple_of(2), c, e);
        let square = arith::mul(&x, &x);
        assert_eq!(sqrt(&square), Ok(normalise(abs(&x))), "x = {}", show(&x));
    }
}

#[test]
fn pow_two_is_the_product() {
    let mut state = 94u64;
    for round in 0..3000 {
        let mut x = random_positive(&mut state);
        if round % 3 == 0 {
            // Near the overflow and underflow of a square.
            x = fin(
                false,
                u128::from(splitmix(&mut state)) % 10u128.pow(20) + 1,
                [-3088i16, -3089, 3055, 3056, 3050][round % 5],
            );
        }
        if splitmix(&mut state).is_multiple_of(2) {
            x = x.negated();
        }
        let square = arith::mul(&x, &x);
        let got = pow(&x, &d("2")).unwrap();
        assert_eq!(
            order(&got, &square),
            Ordering::Equal,
            "pow({}, 2) = {} against {}",
            show(&x),
            show(&got),
            show(&square)
        );
    }
}

#[test]
fn pow_a_half_is_the_square_root_to_a_unit() {
    let mut state = 95u64;
    let mut equal = 0;
    for _ in 0..800 {
        let x = random_positive(&mut state);
        let (a, b) = (pow(&x, &d("0.5")).unwrap(), sqrt(&x).unwrap());
        let Raw::Finite { coefficient, exponent, .. } = b else { panic!() };
        let adjusted = i64::from(digits_u128(coefficient)) + i64::from(exponent) - 1;
        let ulp = fin(false, 1, (adjusted - 33).max(ETINY_) as i16);
        let difference = arith::sub(&a, &b);
        assert_ne!(order(&abs(&difference), &ulp), Ordering::Greater, "pow({}, 0.5)", show(&x));
        if a == b {
            equal += 1;
        }
    }
    assert!(equal > 790, "{equal} of 800 identical");
}

#[test]
fn pow_of_a_whole_power_is_the_exact_power_rounded_once() {
    let mut state = 96u64;
    for i in 0..1500 {
        let x = random_positive(&mut state);
        let Raw::Finite { coefficient, exponent, .. } = x else { unreachable!() };
        let n = 1 + splitmix(&mut state) % 12;
        let negative = i % 2 == 0;
        let y = fin(negative, u128::from(n), 0);
        let got = pow(&x, &y).unwrap();
        let power = BigUint::from(coefficient).pow(n as u32);
        let e = i64::from(exponent) * n as i64;
        let want = if !negative {
            round_big(false, &power, e, false)
        } else {
            // 10^s / power, with s large enough for 40 digits.
            let s = power.to_string().len() as u32 + 45;
            let (q, r) = (BigUint::from(10u8).pow(s) / &power, BigUint::from(10u8).pow(s) % &power);
            round_big(false, &q, -e - i64::from(s), r != BigUint::from(0u8))
        };
        assert_eq!(got, want, "pow({}, {})", show(&x), show(&y));
    }
}

#[test]
fn no_input_panics() {
    let specials = [
        NAN,
        INFINITY,
        NEG_INF,
        fin(false, 0, 0),
        fin(true, 0, 0),
        fin(false, 0, -6176),
        fin(true, 0, 6111),
        fin(false, 1, 0),
        fin(true, 1, 0),
        fin(false, 1, -1),
        fin(true, 1, -1),
        fin(false, 5, -1),
        fin(false, 2, 0),
        fin(true, 2, 0),
        fin(false, 3, 0),
        fin(false, 1, -6176),
        fin(true, 1, -6176),
        fin(false, MAX_COEFFICIENT, 6111),
        fin(true, MAX_COEFFICIENT, 6111),
        fin(false, MAX_COEFFICIENT, -6176),
        fin(false, MAX_COEFFICIENT, -33),
        fin(false, 10u128.pow(33), -33),
        fin(false, 10u128.pow(33) + 1, -33),
        fin(false, MAX_COEFFICIENT, -34),
        fin(false, 1, 6111),
        fin(false, 1, 3055),
        fin(false, 14150, 0),
        fin(true, 14150, 0),
        fin(false, 1, 5),
        fin(true, 1, 5),
        fin(false, 1024, 0),
        fin(false, 1025, 0),
        fin(true, 1025, 0),
        fin(false, 1, 40),
    ];
    for x in &specials {
        let results = [
            abs(x),
            ceil(x),
            floor(x),
            round(x, 0),
            round(x, -20),
            round(x, 100),
            trunc(x, 5),
            exp(x),
            sqrt(x).unwrap_or(NAN),
            ln(x).unwrap_or(NAN),
            log10(x).unwrap_or(NAN),
        ];
        for r in results {
            assert!(valid(&r), "{r:?} from {x:?}");
        }
        for y in &specials {
            let r = pow(x, y).unwrap_or(NAN);
            assert!(valid(&r), "pow({x:?}, {y:?}) = {r:?}");
        }
    }
    // Random raw values of every shape.
    let mut state = 97u64;
    for _ in 0..3000 {
        let make = |state: &mut u64| match splitmix(state) % 8 {
            0 => NAN,
            1 => Raw::Inf { negative: splitmix(state).is_multiple_of(2) },
            _ => fin(
                splitmix(state).is_multiple_of(2),
                (u128::from(splitmix(state)) << 64 | u128::from(splitmix(state)))
                    % (MAX_COEFFICIENT + 1),
                ((splitmix(state) % 12_288) as i64 - 6176).clamp(ETINY_, MAX_EXP) as i16,
            ),
        };
        let (x, y) = (make(&mut state), make(&mut state));
        for r in [
            exp(&x),
            sqrt(&x).unwrap_or(NAN),
            ln(&x).unwrap_or(NAN),
            log10(&x).unwrap_or(NAN),
            pow(&x, &y).unwrap_or(NAN),
        ] {
            assert!(valid(&r), "{r:?} from {x:?}, {y:?}");
        }
    }
}

/// Every result of the five is in normal form: trailing zeros only where the
/// exponent is already 6111, a zero as `+0` at exponent 0 (but for `pow`'s sign).
#[test]
fn the_five_return_the_normal_cohort() {
    let mut state = 98u64;
    for _ in 0..1500 {
        let x = random_positive(&mut state);
        let y = fin(
            splitmix(&mut state).is_multiple_of(2),
            u128::from(splitmix(&mut state) % 3000),
            -((splitmix(&mut state) % 3) as i16),
        );
        for r in
            [exp(&x), sqrt(&x).unwrap(), ln(&x).unwrap(), log10(&x).unwrap(), pow(&x, &y).unwrap()]
        {
            if let Raw::Finite { coefficient, exponent, .. } = r {
                if coefficient == 0 {
                    assert_eq!(exponent, 0);
                } else {
                    assert!(!coefficient.is_multiple_of(10) || exponent == MAX_EXPONENT, "{r:?}");
                }
            }
        }
    }
}
