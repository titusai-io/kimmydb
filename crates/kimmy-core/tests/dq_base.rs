//! `kimmy_core::decimal::parse` against every case of the General Decimal
//! Arithmetic suite's `dqBase.decTest` (version 2.62), the decQuad string
//! conversions: each syntax error is refused, each case the suite converts
//! without rounding (no `Inexact`, `Overflow` or `Underflow`) gives exactly the
//! suite's result, and each case that needs rounding is refused, never rounded.
//!
//! The files are not in this repository (they are © Mike Cowlishaw and IBM,
//! under their own licence, and are never vendored). `scripts/fetch-dectest.sh`
//! fetches them into a directory, checked against the sha256 pinned in
//! `tests/common/dectest.rs`, and CI keeps that directory in its own cache. Point
//! the test at the directory:
//!
//! ```text
//! KIMMY_DECTEST_DIR=/path/to/dectest \
//!     cargo test -p kimmy-core --test dq_base -- --ignored
//! ```
//!
//! Two sets are accepted, each by the sha256 of its files: version 2.62 from
//! speleotrove's `dectest.zip`, and version 2.59 as CPython ships it
//! (`Lib/test/decimaltestdata`). `dqBase.decTest` has the same cases in both
//! (the files differ in their headers, so their sha256s do).

#[path = "common/dectest.rs"]
mod dectest;

use std::str::FromStr;

use bson::Decimal128;
use kimmy_core::decimal::{ParseError, parse};
use num_bigint::BigInt;

use dectest::{result_bits, tokens};

/// `±c × 10^e` of a finite text, exactly: `(negative, c, e)`.
fn exact(text: &str) -> Option<(bool, BigInt, i64)> {
    let (negative, rest) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (mantissa, exponent) = match rest.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i64>().ok()?),
        None => (rest, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let c = BigInt::from_str(&format!("0{whole}{fraction}")).ok()?;
    Some((negative, c, exponent - fraction.len() as i64))
}

/// Whether two finite values are equal: `c1 × 10^e1 == c2 × 10^e2`.
fn same_value((n1, c1, e1): &(bool, BigInt, i64), (n2, c2, e2): &(bool, BigInt, i64)) -> bool {
    let zero = BigInt::from(0);
    if *c1 == zero || *c2 == zero {
        return *c1 == zero && *c2 == zero;
    }
    let shift = |c: &BigInt, by: i64| c * BigInt::from(10).pow(by as u32);
    let (a, b) =
        if e1 >= e2 { (shift(c1, e1 - e2), c2.clone()) } else { (c1.clone(), shift(c2, e2 - e1)) };
    n1 == n2 && a == b
}

#[test]
#[ignore = "needs the dectest files, which are not in the repository: see the module documentation"]
fn every_dq_base_case_is_refused_or_read_exactly() {
    let Some((version, files)) = dectest::load() else { return };
    let text = &files.iter().find(|(name, _)| *name == "dqBase.decTest").expect("dqBase").1;
    assert!(text.lines().any(|l| l.trim() == format!("version: {version}")), "the file's version");

    let (mut syntax, mut exact_cases, mut rounding, mut bson_lax) = (0, 0, 0, Vec::new());
    for line in text.lines() {
        let t = tokens(line);
        let Some(arrow) = t.iter().position(|x| x == "->") else { continue };
        if t[0].ends_with(':') {
            continue;
        }
        let (id, op, input) = (&t[0], t[1].to_ascii_lowercase(), &t[2]);
        assert!(op == "tosci" || op == "toeng", "{id}: an operation this test does not know: {op}");
        assert_eq!(arrow, 3, "{id}: one operand");
        let result = &t[4];
        let conditions: Vec<&str> = t[5..].iter().map(String::as_str).collect();
        let got = parse(input);

        if conditions.contains(&"Conversion_syntax") {
            syntax += 1;
            assert_eq!(got, Err(ParseError::Syntax), "{id}: {input:?} is a syntax error");
            if Decimal128::from_str(input).is_ok() {
                bson_lax.push(input.clone());
            }
        } else if conditions.iter().any(|c| matches!(*c, "Inexact" | "Overflow" | "Underflow")) {
            rounding += 1;
            match got {
                Err(ParseError::Inexact | ParseError::Overflow | ParseError::Underflow) => {}
                other => panic!("{id}: {input:?} needs rounding ({conditions:?}), got {other:?}"),
            }
        } else {
            exact_cases += 1;
            let d =
                got.unwrap_or_else(|e| panic!("{id}: {input:?} is exact ({conditions:?}): {e}"));
            let word = u128::from_le_bytes(d.bytes());
            if op == "tosci" {
                // A scientific string reads back to the decQuad it came from,
                // coefficient and exponent both.
                assert_eq!(word, result_bits(result), "{id}: {input:?} -> {result} {conditions:?}");
            } else if let (Some(written), Some(shown)) = (exact(input), exact(result)) {
                // An engineering string shows the value, not the quantum: the
                // value must agree, and the exponent is the written one where
                // the written pair fits.
                assert!(same_value(&written, &shown), "{id}: {input:?} is not {result}");
                let (_, c, e) = &written;
                let exponent = ((word >> 113) & 0x3FFF) as i64 - 6176;
                if *c == BigInt::from(0) {
                    assert_eq!(exponent, (*e).clamp(-6176, 6111), "{id}: {input:?}");
                } else if c.to_string().len() <= 34 && (-6176..=6111).contains(e) {
                    assert_eq!(exponent, *e, "{id}: {input:?} keeps its exponent");
                }
            } else {
                assert_eq!(word, result_bits(result), "{id}: {input:?} -> {result}");
            }
            // And the value is the input's, by a rational check of its own.
            if let (Some(written), Some(read)) = (exact(input), exact(result)) {
                assert!(same_value(&written, &read), "{id}: {input:?} is not {result}");
            }
            // Where `bson` reads the text too, it gives the same bytes.
            if let Ok(theirs) = Decimal128::from_str(input) {
                assert_eq!(theirs.bytes(), d.bytes(), "{id}: {input:?} differs from bson");
            }
        }
    }
    eprintln!(
        "dqBase: {syntax} syntax errors refused ({} of them read by bson 3.1.0: {bson_lax:?}), \
         {exact_cases} exact cases read exactly, {rounding} that need rounding refused",
        bson_lax.len()
    );
    assert_eq!(syntax, 99, "premise: the suite's 99 syntax errors");
    assert_eq!(bson_lax.len(), 13, "premise: the 13 the lax parser read as numbers");
    // The cases are the same in both pinned sets.
    assert_eq!(
        (exact_cases, rounding),
        (606, 223),
        "premise: the exact cases read exactly, and those that need rounding refused"
    );
}

#[test]
fn the_tokenizer_reads_the_suites_quoting() {
    assert_eq!(
        tokens("dqbas504 toSci '--1'  -> NaN Conversion_syntax"),
        ["dqbas504", "toSci", "--1", "->", "NaN", "Conversion_syntax"]
    );
    assert_eq!(tokens(r#"dqbas559 toSci "1E""1"   -> NaN Conversion_syntax"#)[2], "1E\"1");
    assert_eq!(tokens("dqbas558 toSci '1E''1'   -> NaN")[2], "1E'1");
    assert_eq!(tokens("dqbas519 toSci ''  -> NaN Conversion_syntax")[2], "");
    assert_eq!(
        tokens(r#"dqbast800 toEng 0e+1 -> "0.00E+3"  -- doc example"#),
        ["dqbast800", "toEng", "0e+1", "->", "0.00E+3"]
    );
    assert_eq!(result_bits("0.00E+3"), (6177u128) << 113);
    assert_eq!(result_bits("-sNaN7"), (1 << 127) | (0b11111 << 122) | (1 << 121) | 7);
}
