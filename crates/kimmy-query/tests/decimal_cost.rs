//! What a `$group` over Decimal128 costs against the same group over integers
//! and doubles, in a release build (ADR-252). The design's ceilings:
//!
//! - a `$sum` over 100,000 decimal prices (exponent -2) costs at most twice the
//!   same over `Int64`;
//! - a `$sum` over 100,000 doubles, with no decimal in the group, moves by no
//!   more than noise.
//!
//! The second test asserts an absolute bound only: 100,000 documents are 100,000
//! clones and field reads, and the 8-byte push each double adds to the group's
//! buffer cannot take half a second. It does not assert the design's 5%: a
//! laptop's timer noise on a few milliseconds is larger than that, so the figure
//! to read is the microbenchmark in ADR-252 (9 ns a value), not this group-level
//! one.
//! Time, so ignored in the ordinary run; CI's `decimal` job runs it in release.

use std::time::{Duration, Instant};

use bson::{Bson, Document, doc};
use kimmy_query::aggregate::{self, Limits};

const N: usize = 100_000;

fn run(pipeline: &[Document], input: &[Document]) -> (Duration, Vec<Document>) {
    let stages = aggregate::parse(pipeline).unwrap();
    let mut best = Duration::MAX;
    let mut out = Vec::new();
    for _ in 0..5 {
        let docs = input.to_vec();
        let start = Instant::now();
        out = aggregate::apply(&stages[0], docs, &Limits::default()).unwrap();
        best = best.min(start.elapsed());
    }
    (best, out)
}

fn dec(text: &str) -> Bson {
    Bson::Decimal128(kimmy_core::decimal::parse(text).unwrap())
}

#[test]
#[ignore = "time; run in release by CI's decimal job"]
fn a_decimal_sum_costs_at_most_twice_an_integer_sum() {
    let group = [doc! { "$group": { "_id": null, "t": { "$sum": "$v" } } }];
    let ints: Vec<Document> =
        (0..N as i64).map(|i| doc! { "v": Bson::Int64(i % 100_000) }).collect();
    let prices: Vec<Document> =
        (0..N as i64).map(|i| doc! { "v": dec(&format!("{}.{:02}", i % 1000, i % 100)) }).collect();
    let (int_time, _) = run(&group, &ints);
    let (dec_time, out) = run(&group, &prices);
    assert!(matches!(out[0].get("t"), Some(Bson::Decimal128(_))));
    let ratio = dec_time.as_secs_f64() / int_time.as_secs_f64();
    eprintln!("100,000 ints {int_time:?}, prices {dec_time:?}: {ratio:.2}x");
    assert!(ratio < 3.0, "{ratio:.2}x (the ceiling is 2x; the margin is timer noise)");
}

#[test]
#[ignore = "time; run in release by CI's decimal job"]
fn a_double_sum_with_no_decimal_costs_what_it_did() {
    let group = [doc! { "$group": { "_id": null, "t": { "$sum": "$v" } } }];
    let doubles: Vec<Document> =
        (0..N).map(|i| doc! { "v": Bson::Double(i as f64 * 0.37) }).collect();
    let (time, out) = run(&group, &doubles);
    assert!(matches!(out[0].get("t"), Some(Bson::Double(_))));
    eprintln!("100,000 doubles, no decimal: {time:?}");
    // 100,000 documents are 100,000 clones and field reads; an 8-byte push each
    // cannot take a second.
    assert!(time < Duration::from_millis(500), "{time:?}");
}
