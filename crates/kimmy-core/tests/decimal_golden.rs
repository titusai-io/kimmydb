//! The golden corpus for the Decimal128 arithmetic (ADR-252): about 3,000 rows
//! written by `scripts/decimal_golden.py` from libmpdec, an implementation that
//! shares no code with this one.
//!
//! The rows pin the bytes of R1 (a double to a decimal, ties included), the four
//! binary operations at the edges of the format, exact sums, means and products
//! of multisets, the conversions out of a decimal, and the strict parser. CI
//! runs this file in release on both architectures, x86_64 and aarch64, because
//! the shipped profile wraps on integer overflow and the test profile panics,
//! and because it is the file that pins the two release targets to one answer.
//!
//! A change in any result is a diff of the corpus, and so is reviewed.

use kimmy_core::decimal::arith::{self, ExactSum};
use kimmy_core::decimal::{Raw, decode_raw, pack_raw, parse};

/// A value in the corpus's spelling: `[-]<coefficient>E<exponent>`, or a special.
fn value(token: &str) -> Raw {
    match token {
        "NaN" => Raw::Nan,
        "Infinity" => Raw::Inf { negative: false },
        "-Infinity" => Raw::Inf { negative: true },
        _ => {
            let (negative, rest) = match token.strip_prefix('-') {
                Some(rest) => (true, rest),
                None => (false, token),
            };
            let (coefficient, exponent) = rest.split_once('E').unwrap_or_else(|| panic!("{token}"));
            Raw::Finite {
                negative,
                coefficient: coefficient.parse().unwrap_or_else(|_| panic!("{token}")),
                exponent: exponent.parse().unwrap_or_else(|_| panic!("{token}")),
            }
        }
    }
}

fn values(field: &str) -> Vec<Raw> {
    field.split(' ').map(value).collect()
}

fn word(raw: Raw) -> u128 {
    u128::from_le_bytes(pack_raw(raw).bytes())
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

#[test]
fn every_row_of_the_corpus_is_reproduced_to_the_byte() {
    let corpus = include_str!("data/decimal_golden.tsv");
    let mut counts = std::collections::BTreeMap::<String, u64>::new();
    for (number, line) in corpus.lines().enumerate() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split('\t').collect();
        let at = format!("line {}: {line}", number + 1);
        match f[0] {
            "r1" => {
                let bits = u64::from_str_radix(f[1], 16).unwrap();
                let (got, inexact) = arith::from_binary64(bits);
                assert_eq!(word(got), word(value(f[2])), "{at}: got {}", text(&got));
                assert_eq!(u8::from(inexact), f[3].parse::<u8>().unwrap(), "{at}");
                // A double converts to a decimal and back to itself.
                if bits & 0x7FF0_0000_0000_0000 != 0x7FF0_0000_0000_0000 {
                    assert_eq!(arith::to_binary64(&got), bits, "{at}: the round trip");
                }
            }
            "add" | "sub" | "mul" | "div" => {
                let (a, b, want) = (value(f[1]), value(f[2]), value(f[3]));
                let got = match f[0] {
                    "add" => arith::add(&a, &b),
                    "sub" => arith::sub(&a, &b),
                    "mul" => arith::product(&[a, b]),
                    _ => arith::div(&a, &b).expect("the corpus has no zero divisor"),
                };
                assert_eq!(word(got), word(want), "{at}: got {}", text(&got));
            }
            "sum" | "avg" => {
                let terms = values(f[1]);
                let want = value(f[2]);
                let mut sum = ExactSum::new();
                for t in &terms {
                    sum.add(t);
                }
                let got = if f[0] == "sum" { sum.finish_sum() } else { sum.finish_avg() };
                assert_eq!(word(got), word(want), "{at}: got {}", text(&got));
                // The same bytes in the reverse order.
                let mut backwards = ExactSum::new();
                for t in terms.iter().rev() {
                    backwards.add(t);
                }
                let again =
                    if f[0] == "sum" { backwards.finish_sum() } else { backwards.finish_avg() };
                assert_eq!(word(again), word(want), "{at}: reversed");
            }
            "prod" => {
                let got = arith::product(&values(f[1]));
                assert_eq!(word(got), word(value(f[2])), "{at}: got {}", text(&got));
            }
            "todouble" => {
                let got = arith::to_binary64(&value(f[1]));
                assert_eq!(format!("{got:016x}"), f[2], "{at}");
            }
            "tolong" => {
                let d = pack_raw(value(f[1]));
                let part = kimmy_core::decimal::integer_part(&d);
                let got = match part {
                    Some(p) if p.in_range => p.value.to_string(),
                    _ => "ERR".to_string(),
                };
                assert_eq!(got, f[2], "{at}");
            }
            "tostr" => {
                assert_eq!(pack_raw(value(f[1])).to_string(), f[2], "{at}");
            }
            "parse" => {
                let got = match parse(f[1]) {
                    Ok(d) => text(&decode_raw(&d)),
                    Err(_) => "ERR".to_string(),
                };
                // Infinity and NaN are read by the parser as themselves.
                assert_eq!(got, f[2], "{at}");
            }
            other => panic!("{at}: a row kind this test does not know: {other}"),
        }
        *counts.entry(f[0].to_string()).or_default() += 1;
    }
    // A corpus that shrank is a test that stopped testing.
    let total: u64 = counts.values().sum();
    assert!(total > 2_500, "the corpus has {total} rows: {counts:?}");
    for kind in [
        "r1", "add", "sub", "mul", "div", "sum", "avg", "prod", "todouble", "tolong", "tostr",
        "parse",
    ] {
        assert!(counts.get(kind).copied().unwrap_or(0) > 100, "{kind}: {counts:?}");
    }
}
