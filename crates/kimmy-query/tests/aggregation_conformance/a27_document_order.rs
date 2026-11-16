//! Documents as ordering values (q7s4t, 18 rows) and the order of the output of `$densify` (q7s4v, 12 rows):
//! `data/doc_order.tsv`, built by `data/doc_order_gen.py`; q7s4t2 and q7s4v2 are the same scripts in a second
//! container.
//!
//! `k` is {a:1}, {a:"x"}, {b:0}, {a:1,b:2}, {b:2,a:1}, {}, {a:null}, {a:[1]}. A document orders by the
//! type of its first field's value, then the field's name, then the value, a shorter prefix first:
//! ascending {} {a:null} {a:1} {a:1,b:2} {b:0} {b:2,a:1} {a:"x"} {a:[1]} (ids 6 7 1 4 3 5 2 8), and
//! descending the reverse, in a find sort and in `$sort`, in the ranks of `$setWindowFields`
//! (`$rank`, `$documentNumber`, `$denseRank`), in its partitions, in the sort of `$group` keys and
//! in the partitions of `$densify`. No two documents tie, and {a:1,b:2} and {b:2,a:1} are two
//! values. The rows whose order is not a promise of the reference (`$densify` with full bounds or explicit
//! bounds: q7s4t0015 and q7s4t0017, in `unstable.tsv`, and the full and explicit rows of q7s4v, which agreed
//! between two recordings by chance) are compared as a set of rows.
//!
//! `$densify` with bounds "partition" (q7s4t0016, q7s4t0018, q7s4v0001, q7s4v0007): the partitions come in the
//! order of their keys (a sort on the key: document order for document keys), and inside one the documents with
//! no numeric value for the field (absent or null) come first in stored order, then the values ascending with the
//! generated rows between them. Bounds "partition" without `partitionByFields` is refused by both (the
//! reference's code 5733408 and text are not ours: q7s4v0002, 0008); a null value is passed through like a missing one, never refused.
//!
//! Every failing row is reported before the test asserts, with both answers.

use super::common::run;
use bson::{Bson, Document};
use kimmy_query::shape;
use serde_json::Value;

fn bson_of(value: &Value) -> Bson {
    Bson::try_from(value.clone()).expect("extended JSON")
}

fn documents(value: &Value) -> Vec<Document> {
    value.as_array().unwrap().iter().map(|d| bson_of(d).as_document().unwrap().clone()).collect()
}

#[test]
fn documents_order_by_type_then_name_then_value_everywhere_the_reference_orders_them() {
    let mut failures = Vec::new();
    let mut rows = 0;
    for row in include_str!("../data/doc_order.tsv").lines() {
        let [label, kind, docs, spec, answer, set] = row.split('\t').collect::<Vec<_>>()[..] else {
            panic!("a row of six columns: {row}")
        };
        rows += 1;
        let docs = documents(&serde_json::from_str(docs).unwrap());
        let spec: Value = serde_json::from_str(spec).unwrap();
        let answer: Value = serde_json::from_str(answer).unwrap();
        if answer.get("error").is_some() {
            // Both refuse; the code and the text are the reference's own, not ours.
            if let Ok(out) = run(&documents(&spec), docs) {
                failures
                    .push(format!("{label}: recorded a refusal {answer}, ours answered {out:?}"));
            }
            continue;
        }
        let (got, want): (Result<Vec<Bson>, String>, Vec<Bson>) = if kind == "find" {
            let sort = bson_of(&spec).as_document().unwrap().clone();
            let keys = shape::parse_sort(&sort).unwrap();
            let mut sorted = docs;
            shape::sort(&keys, &mut sorted);
            (
                Ok(sorted.iter().map(|d| d.get("_id").unwrap().clone()).collect()),
                answer.as_array().unwrap().iter().map(bson_of).collect(),
            )
        } else {
            (
                run(&documents(&spec), docs)
                    .map(|out| out.into_iter().map(Bson::Document).collect()),
                documents(&answer).into_iter().map(Bson::Document).collect(),
            )
        };
        let mut got = match got {
            Ok(got) => got,
            Err(e) => {
                failures.push(format!("{label}: ours refused: {e}"));
                continue;
            }
        };
        let mut want = want;
        if set == "1" {
            got.sort_by_key(|b| format!("{b:?}"));
            want.sort_by_key(|b| format!("{b:?}"));
        }
        if got != want {
            failures.push(format!(
                "{label} ({kind}{}):\n   ours     {got:?}\n   recorded {want:?}",
                if set == "1" { ", as a set" } else { "" }
            ));
        }
    }
    assert_eq!(rows, 30, "the generator wrote 30 cases (q7s4t 18, q7s4v 12)");
    println!("doc_order: {rows} cases (q7s4t 18, q7s4v 12)");
    assert!(
        failures.is_empty(),
        "{} of {rows} rows differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
