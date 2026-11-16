//! `$substr`, `$substrBytes` and `$substrCP` over ranges that split a character, whole-character
//! controls, a negative length, a negative start and a length of 1.5 (q7s4r, 27 rows,
//! `data/substr_splits.tsv`, built by `data/substr_splits_gen.py`).
//!
//! The legacy `$substr` refuses a range that splits a character exactly as `$substrBytes` does
//! (28656 for a start inside one, 28657 for an end inside one); `$substrCP` never refuses a split
//! but refuses a negative length, a negative start and a fractional length. `$substr` and
//! `$substrBytes` take a negative length as "to the end", refuse a negative start, and accept a
//! length of 1.5. Every failing row is reported before the test asserts, with both answers.
//!
//! q7s4u adds a Decimal128 or a Double as the start or the length of `$substr` and `$substrBytes`: a
//! Decimal128 is rounded to the nearest whole number, halves to the even one (0.5 to 0, 1.5 to 2,
//! 2.5 to 2, 3.5 to 4, -0.5 to 0), a Double is truncated toward zero; after that a negative length is
//! the rest of the string and a negative start is refused (50752). `$substrCP` is not covered.
//!
//! q7s4s adds the start and the length as a Double 1.5, 1.0 or 4294967296, a Decimal128 1.5 or 2,
//! an Int64 2 or 4294967296, a string, null and a missing field, and a start beyond or at the end:
//! `$substrCP` takes only a number that is a 32-bit integer in value (34451 start, 34453 length)
//! and refuses a non-number (34450, 34452); `$substr` and `$substrBytes` take every number
//! (a Decimal128 length of 1.5 answers "bc"; other decimals are UNRECORDED) and refuse a
//! non-number (16034 start, 16035 length).

use super::common::run;
use bson::{Bson, Document, doc};
use serde_json::Value;

fn bson_of(value: &Value) -> Bson {
    Bson::try_from(value.clone()).expect("extended JSON")
}

#[test]
fn every_recorded_substring_answers_or_is_refused_as_the_reference_does() {
    let mut failures = Vec::new();
    let mut rows = 0;
    for row in include_str!("../data/substr_splits.tsv").lines() {
        let [label, function, args, outcome] = row.split('\t').collect::<Vec<_>>()[..] else {
            panic!("a row of four columns: {row}")
        };
        rows += 1;
        let args = bson_of(&serde_json::from_str(args).unwrap());
        let outcome: Value = serde_json::from_str(outcome).unwrap();
        let stage = doc! { "$project": { "_id": 1, "r": { function: args } } };
        let got = run(&[stage], vec![doc! { "_id": 1 }]);
        let want_error = outcome.get("error").and_then(Value::as_i64);
        match (want_error, &got) {
            (Some(_), Err(_)) => {}
            (Some(code), Ok(docs)) => {
                failures.push(format!(
                    "{label}: recorded a refusal (code {code}), ours answered {docs:?}"
                ));
            }
            (None, Ok(docs)) => {
                let want = bson_of(&outcome["r"]);
                let answered: Option<&Bson> = docs.first().and_then(|d: &Document| d.get("r"));
                if answered != Some(&want) {
                    failures
                        .push(format!("{label}: recorded {want:?}, ours answered {answered:?}"));
                }
            }
            (None, Err(e)) => {
                failures.push(format!("{label}: recorded {:?}, ours refused: {e}", outcome["r"]));
            }
        }
    }
    assert_eq!(rows, 147, "the generator wrote 147 cases (q7s4r 27, q7s4s 60, q7s4u 60)");
    println!("substr_splits: {rows} cases (q7s4r 27, q7s4s 60, q7s4u 60)");
    assert!(
        failures.is_empty(),
        "{} of {rows} rows differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
