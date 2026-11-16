//! The readers of a path in a stage that are not an expression, over the digit paths of q7s4i
//! 313 to 426 (`a.01`, `a.00`, `a.0`, `a.1`, `a.01.x`, `a.0.0` over eight documents):
//!
//! - `$unwind` and `$densify` read **field names only**, and an array met before the last
//!   segment makes the path missing: the document is dropped (kept unchanged with
//!   `preserveNullAndEmptyArrays`, passed by `$densify`), never refused (316, 317, 328 and the
//!   same stage of each path);
//! - a `$lookup` **foreign** field follows the filter rule and does not expand an array it reached
//!   by a position (360: `a.0` over `[[1]]` does not join a local `1`; 417: `a.0.0` does);
//! - a `$lookup` **local** field selects a position only when the element is a document, and
//!   every segment is a field name in document elements (321, 340, 359).
//!
//! The rows of the local side leave out the foreign document with no `k`, which the reference
//! joins to nothing found and ours never makes a candidate: the documented deviation.

use super::common::{json, run};
use bson::Document;
use kimmy_core::cmp::canonical_cmp;
use kimmy_query::aggregate::{foreign_join_values, lookup_keys};
use std::cmp::Ordering;

fn documents(text: &str) -> Vec<Document> {
    json(text).as_array().unwrap().iter().map(|d| d.as_document().unwrap().clone()).collect()
}

/// The foreign collection of q7s4i, read from its script: ids 1 to 7.
fn foreign() -> Vec<Document> {
    documents(
        r#"[{"_id": 1, "k": 10, "a": [10, 20]}, {"_id": 2, "k": 5, "a": [{"01": 5}, 9]},
            {"_id": 3, "k": 7, "a": {"01": 7}}, {"_id": 4, "k": 1}, {"_id": 5, "k": [5, 20]},
            {"_id": 6, "a": [9]}, {"_id": 7, "k": 20, "a": [[1]]}]"#,
    )
}

#[test]
fn unwind_and_densify_read_field_names_and_a_path_through_an_array_is_missing() {
    let mut failures = Vec::new();
    for (line, stages, want) in stage_rows() {
        match run(&stages, fixture()) {
            Ok(out) if out == want => {}
            Ok(out) => {
                failures.push(format!("q7s4i:{line}\n   ours     {out:?}\n   recorded {want:?}"))
            }
            Err(e) => failures.push(format!("q7s4i:{line}: {e}")),
        }
    }
    assert!(failures.is_empty(), "{} rows differ:\n{}", failures.len(), failures.join("\n"));
}

#[test]
fn a_lookup_reads_its_two_sides_by_their_own_rules() {
    let locals = fixture();
    let foreign = foreign();
    let mut failures = Vec::new();
    for (line, local_path, foreign_path, want) in lookup_rows() {
        for (local, want) in locals.iter().zip(want) {
            let wanted = lookup_keys(std::slice::from_ref(local), local_path);
            let joined: Vec<i32> = foreign
                .iter()
                .filter(|doc| {
                    foreign_join_values(doc, foreign_path)
                        .iter()
                        .any(|key| wanted.iter().any(|w| canonical_cmp(w, key) == Ordering::Equal))
                })
                .map(|doc| doc.get_i32("_id").unwrap())
                .collect();
            if joined != want {
                failures.push(format!(
                    "q7s4i:{line}, _id {}\n   ours     {joined:?}\n   recorded {want:?}",
                    local.get_i32("_id").unwrap()
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{} rows differ:\n{}", failures.len(), failures.join("\n"));
}

#[test]
fn a_path_through_an_array_before_the_last_segment_is_not_refused() {
    // q7s4i:316 and 411: the path is missing; with the option the document stays, as it was.
    let rows = vec![bson::doc! { "_id": 1, "items": [{ "sku": "a" }, { "sku": "b" }] }];
    let out = run(&[bson::doc! { "$unwind": "$items.sku" }], rows.clone()).unwrap();
    assert!(out.is_empty());
    let kept = run(
        &[bson::doc! { "$unwind": { "path": "$items.sku", "preserveNullAndEmptyArrays": true } }],
        rows.clone(),
    )
    .unwrap();
    assert_eq!(kept, rows);
    // A dotted path through documents still unwinds.
    let nested = vec![bson::doc! { "y": { "b": [1, 2] } }];
    let out = run(&[bson::doc! { "$unwind": "$y.b" }], nested).unwrap();
    assert_eq!(out, vec![bson::doc! { "y": { "b": 1 } }, bson::doc! { "y": { "b": 2 } }]);
}

// The recorded data, generated from q7s4i.jsonl.

fn fixture() -> Vec<Document> {
    documents(
        r#"[{"_id": 1, "a": [10, 20]}, {"_id": 2, "a": [[1], [2]]}, {"_id": 3, "a": [{"01": 5}, 9]}, {"_id": 4, "a": {"0": 6, "1": 8, "01": 7}}, {"_id": 5, "a": [{"x": 1}, {"x": 2}]}, {"_id": 6, "a": {"00": 1}}, {"_id": 7, "a": [[1, 2], [3]]}, {"_id": 8}]"#,
    )
}

/// A recorded stage over the fixture: its line in `q7s4i.jsonl`, its stages and the documents the
/// reference answered, in `_id` order.
fn stage_rows() -> Vec<(u32, Vec<Document>, Vec<Document>)> {
    vec![
        (
            316,
            documents(r#"[{"$unwind": "$a.01"}, {"$sort": {"_id": 1}}]"#),
            documents(r#"[{"_id": 4, "a": {"0": 6, "1": 8, "01": 7}}]"#),
        ),
        (
            317,
            documents(
                r#"[{"$unwind": {"path": "$a.01", "preserveNullAndEmptyArrays": true}}, {"$sort": {"_id": 1}}]"#,
            ),
            documents(
                r#"[{"_id": 1, "a": [10, 20]}, {"_id": 2, "a": [[1], [2]]}, {"_id": 3, "a": [{"01": 5}, 9]}, {"_id": 4, "a": {"0": 6, "1": 8, "01": 7}}, {"_id": 5, "a": [{"x": 1}, {"x": 2}]}, {"_id": 6, "a": {"00": 1}}, {"_id": 7, "a": [[1, 2], [3]]}, {"_id": 8}]"#,
            ),
        ),
        (
            328,
            documents(
                r#"[{"$densify": {"field": "a.01", "range": {"step": 1, "bounds": "full"}}}, {"$sort": {"_id": 1}}]"#,
            ),
            documents(
                r#"[{"_id": 1, "a": [10, 20]}, {"_id": 2, "a": [[1], [2]]}, {"_id": 3, "a": [{"01": 5}, 9]}, {"_id": 4, "a": {"0": 6, "1": 8, "01": 7}}, {"_id": 5, "a": [{"x": 1}, {"x": 2}]}, {"_id": 6, "a": {"00": 1}}, {"_id": 7, "a": [[1, 2], [3]]}, {"_id": 8}]"#,
            ),
        ),
        (
            335,
            documents(r#"[{"$unwind": "$a.00"}, {"$sort": {"_id": 1}}]"#),
            documents(r#"[{"_id": 6, "a": {"00": 1}}]"#),
        ),
        (
            336,
            documents(
                r#"[{"$unwind": {"path": "$a.00", "preserveNullAndEmptyArrays": true}}, {"$sort": {"_id": 1}}]"#,
            ),
            documents(
                r#"[{"_id": 1, "a": [10, 20]}, {"_id": 2, "a": [[1], [2]]}, {"_id": 3, "a": [{"01": 5}, 9]}, {"_id": 4, "a": {"0": 6, "1": 8, "01": 7}}, {"_id": 5, "a": [{"x": 1}, {"x": 2}]}, {"_id": 6, "a": {"00": 1}}, {"_id": 7, "a": [[1, 2], [3]]}, {"_id": 8}]"#,
            ),
        ),
        (
            347,
            documents(
                r#"[{"$densify": {"field": "a.00", "range": {"step": 1, "bounds": "full"}}}, {"$sort": {"_id": 1}}]"#,
            ),
            documents(
                r#"[{"_id": 1, "a": [10, 20]}, {"_id": 2, "a": [[1], [2]]}, {"_id": 3, "a": [{"01": 5}, 9]}, {"_id": 4, "a": {"0": 6, "1": 8, "01": 7}}, {"_id": 5, "a": [{"x": 1}, {"x": 2}]}, {"_id": 6, "a": {"00": 1}}, {"_id": 7, "a": [[1, 2], [3]]}, {"_id": 8}]"#,
            ),
        ),
        (
            354,
            documents(r#"[{"$unwind": "$a.0"}, {"$sort": {"_id": 1}}]"#),
            documents(r#"[{"_id": 4, "a": {"0": 6, "1": 8, "01": 7}}]"#),
        ),
        (
            355,
            documents(
                r#"[{"$unwind": {"path": "$a.0", "preserveNullAndEmptyArrays": true}}, {"$sort": {"_id": 1}}]"#,
            ),
            documents(
                r#"[{"_id": 1, "a": [10, 20]}, {"_id": 2, "a": [[1], [2]]}, {"_id": 3, "a": [{"01": 5}, 9]}, {"_id": 4, "a": {"0": 6, "1": 8, "01": 7}}, {"_id": 5, "a": [{"x": 1}, {"x": 2}]}, {"_id": 6, "a": {"00": 1}}, {"_id": 7, "a": [[1, 2], [3]]}, {"_id": 8}]"#,
            ),
        ),
        (
            366,
            documents(
                r#"[{"$densify": {"field": "a.0", "range": {"step": 1, "bounds": "full"}}}, {"$sort": {"_id": 1}}]"#,
            ),
            documents(
                r#"[{"_id": 1, "a": [10, 20]}, {"_id": 2, "a": [[1], [2]]}, {"_id": 3, "a": [{"01": 5}, 9]}, {"_id": 4, "a": {"0": 6, "1": 8, "01": 7}}, {"_id": 5, "a": [{"x": 1}, {"x": 2}]}, {"_id": 6, "a": {"00": 1}}, {"_id": 7, "a": [[1, 2], [3]]}, {"_id": 8}]"#,
            ),
        ),
        (
            373,
            documents(r#"[{"$unwind": "$a.1"}, {"$sort": {"_id": 1}}]"#),
            documents(r#"[{"_id": 4, "a": {"0": 6, "1": 8, "01": 7}}]"#),
        ),
        (
            374,
            documents(
                r#"[{"$unwind": {"path": "$a.1", "preserveNullAndEmptyArrays": true}}, {"$sort": {"_id": 1}}]"#,
            ),
            documents(
                r#"[{"_id": 1, "a": [10, 20]}, {"_id": 2, "a": [[1], [2]]}, {"_id": 3, "a": [{"01": 5}, 9]}, {"_id": 4, "a": {"0": 6, "1": 8, "01": 7}}, {"_id": 5, "a": [{"x": 1}, {"x": 2}]}, {"_id": 6, "a": {"00": 1}}, {"_id": 7, "a": [[1, 2], [3]]}, {"_id": 8}]"#,
            ),
        ),
        (
            385,
            documents(
                r#"[{"$densify": {"field": "a.1", "range": {"step": 1, "bounds": "full"}}}, {"$sort": {"_id": 1}}]"#,
            ),
            documents(
                r#"[{"_id": 1, "a": [10, 20]}, {"_id": 2, "a": [[1], [2]]}, {"_id": 3, "a": [{"01": 5}, 9]}, {"_id": 4, "a": {"0": 6, "1": 8, "01": 7}}, {"_id": 5, "a": [{"x": 1}, {"x": 2}]}, {"_id": 6, "a": {"00": 1}}, {"_id": 7, "a": [[1, 2], [3]]}, {"_id": 8}]"#,
            ),
        ),
        (392, documents(r#"[{"$unwind": "$a.01.x"}, {"$sort": {"_id": 1}}]"#), documents(r#"[]"#)),
        (
            393,
            documents(
                r#"[{"$unwind": {"path": "$a.01.x", "preserveNullAndEmptyArrays": true}}, {"$sort": {"_id": 1}}]"#,
            ),
            documents(
                r#"[{"_id": 1, "a": [10, 20]}, {"_id": 2, "a": [[1], [2]]}, {"_id": 3, "a": [{"01": 5}, 9]}, {"_id": 4, "a": {"0": 6, "1": 8, "01": 7}}, {"_id": 5, "a": [{"x": 1}, {"x": 2}]}, {"_id": 6, "a": {"00": 1}}, {"_id": 7, "a": [[1, 2], [3]]}, {"_id": 8}]"#,
            ),
        ),
        (
            404,
            documents(
                r#"[{"$densify": {"field": "a.01.x", "range": {"step": 1, "bounds": "full"}}}, {"$sort": {"_id": 1}}]"#,
            ),
            documents(
                r#"[{"_id": 1, "a": [10, 20]}, {"_id": 2, "a": [[1], [2]]}, {"_id": 3, "a": [{"01": 5}, 9]}, {"_id": 4, "a": {"0": 6, "1": 8, "01": 7}}, {"_id": 5, "a": [{"x": 1}, {"x": 2}]}, {"_id": 6, "a": {"00": 1}}, {"_id": 7, "a": [[1, 2], [3]]}, {"_id": 8}]"#,
            ),
        ),
        (411, documents(r#"[{"$unwind": "$a.0.0"}, {"$sort": {"_id": 1}}]"#), documents(r#"[]"#)),
        (
            412,
            documents(
                r#"[{"$unwind": {"path": "$a.0.0", "preserveNullAndEmptyArrays": true}}, {"$sort": {"_id": 1}}]"#,
            ),
            documents(
                r#"[{"_id": 1, "a": [10, 20]}, {"_id": 2, "a": [[1], [2]]}, {"_id": 3, "a": [{"01": 5}, 9]}, {"_id": 4, "a": {"0": 6, "1": 8, "01": 7}}, {"_id": 5, "a": [{"x": 1}, {"x": 2}]}, {"_id": 6, "a": {"00": 1}}, {"_id": 7, "a": [[1, 2], [3]]}, {"_id": 8}]"#,
            ),
        ),
        (
            423,
            documents(
                r#"[{"$densify": {"field": "a.0.0", "range": {"step": 1, "bounds": "full"}}}, {"$sort": {"_id": 1}}]"#,
            ),
            documents(
                r#"[{"_id": 1, "a": [10, 20]}, {"_id": 2, "a": [[1], [2]]}, {"_id": 3, "a": [{"01": 5}, 9]}, {"_id": 4, "a": {"0": 6, "1": 8, "01": 7}}, {"_id": 5, "a": [{"x": 1}, {"x": 2}]}, {"_id": 6, "a": {"00": 1}}, {"_id": 7, "a": [[1, 2], [3]]}, {"_id": 8}]"#,
            ),
        ),
    ]
}

/// `$lookup` rows: (line, local path, foreign path, for each fixture `_id` in order the foreign ids joined).
fn lookup_rows() -> Vec<(u32, &'static str, &'static str, Vec<Vec<i32>>)> {
    vec![
        (
            321,
            "a.01",
            "k",
            vec![vec![], vec![], vec![2, 5], vec![3], vec![], vec![], vec![], vec![]],
        ),
        (
            322,
            "_id",
            "a.01",
            vec![vec![], vec![], vec![], vec![], vec![2], vec![], vec![3], vec![]],
        ),
        (340, "a.00", "k", vec![vec![], vec![], vec![], vec![], vec![], vec![4], vec![], vec![]]),
        (341, "_id", "a.00", vec![vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]]),
        (359, "a.0", "k", vec![vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]]),
        (360, "_id", "a.0", vec![vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]]),
        (378, "a.1", "k", vec![vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]]),
        (379, "_id", "a.1", vec![vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]]),
        (397, "a.01.x", "k", vec![vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]]),
        (
            398,
            "_id",
            "a.01.x",
            vec![vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]],
        ),
        (416, "a.0.0", "k", vec![vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]]),
        (
            417,
            "_id",
            "a.0.0",
            vec![vec![7], vec![], vec![], vec![], vec![], vec![], vec![], vec![]],
        ),
    ]
}
