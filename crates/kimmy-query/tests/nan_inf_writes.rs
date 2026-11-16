//! NaN, the infinities and numbers in the FILTER of an update, a delete and a findAndModify
//! (q7s3k, lines 9 to 66, `data/nan_inf_writes.tsv`): each write touches exactly the documents the
//! same filter finds, and the collection afterwards is the recorded one. The operators `$eq`,
//! `$ne`, `$gt`, `$gte`, `$lt` and `$lte` of a Double NaN, of 5 and of `+Infinity`, the implicit
//! equality `{v: NaN}`, `$in: [NaN]`, and the two upserts over an empty collection: `{v: NaN}`
//! creates a document with `v` NaN, `{v: {$gte: NaN}}` creates one without `v`.
//!
//! The documents are compared with their types and key order (a NaN equal to a NaN); the `_id` of a
//! document an upsert creates is generated and left out. **Every failing row is reported, all of
//! them, before the test asserts.**

mod support;

use bson::{Bson, Document};
use kimmy_core::cmp::canonical_cmp;
use kimmy_query::{filter, seed, update};
use support::{documents, json};

const NOW: i64 = 1_700_000_000_000;

/// The documents in `_id` order, each as text (a document's own order of fields kept; an
/// ObjectId `_id` that nobody gave is left out).
fn shown(docs: &[Document], known: &[Document]) -> Vec<String> {
    let given: Vec<&Bson> = known.iter().filter_map(|d| d.get("_id")).collect();
    let mut docs: Vec<Document> = docs
        .iter()
        .map(|d| match d.get("_id") {
            Some(id @ Bson::ObjectId(_)) if !given.contains(&id) => {
                d.iter().filter(|(k, _)| *k != "_id").map(|(k, v)| (k.clone(), v.clone())).collect()
            }
            _ => d.clone(),
        })
        .collect();
    docs.sort_by(|a, b| match (a.get("_id"), b.get("_id")) {
        (Some(x), Some(y)) => canonical_cmp(x, y),
        (x, y) => x.is_some().cmp(&y.is_some()),
    });
    docs.iter().map(ToString::to_string).collect()
}

/// What the write answers: the number of documents it touched, and the collection afterwards.
fn replay(
    kind: &str,
    before: &[Document],
    command: &Document,
) -> Result<(i64, Vec<Document>), String> {
    let multi = command.get_bool("multi").unwrap_or(false);
    let mut n = 0;
    let mut after = Vec::new();
    match kind {
        "update" => {
            let query =
                filter::parse(command.get_document("q").unwrap()).map_err(|e| e.to_string())?;
            let spec =
                update::parse(command.get_document("u").unwrap()).map_err(|e| e.to_string())?;
            for doc in before {
                let mut doc = doc.clone();
                if (multi || n == 0) && filter::matches(&query, &doc).map_err(|e| e.to_string())? {
                    update::apply_matched(&spec, &query, &mut doc, NOW)
                        .map_err(|e| e.to_string())?;
                    n += 1;
                }
                after.push(doc);
            }
            if n == 0 && command.get_bool("upsert").unwrap_or(false) {
                let mut doc = Document::new();
                seed::implied_equalities(&query, &mut doc, &mut Vec::new())?;
                update::apply_on_insert(&spec, &mut doc, NOW).map_err(|e| e.to_string())?;
                after.push(doc);
                n = 1;
            }
        }
        "delete" => {
            let query = filter::parse(command.get_document("filter").unwrap())
                .map_err(|e| e.to_string())?;
            for doc in before {
                let hit = filter::matches(&query, doc).map_err(|e| e.to_string())?;
                if hit && (multi || n == 0) {
                    n += 1;
                } else {
                    after.push(doc.clone());
                }
            }
        }
        "findAndModify" => {
            let query =
                filter::parse(command.get_document("query").unwrap()).map_err(|e| e.to_string())?;
            let spec = update::parse(command.get_document("update").unwrap())
                .map_err(|e| e.to_string())?;
            // The sort is `{_id: 1}`: the first match in `_id` order.
            let mut docs = before.to_vec();
            docs.sort_by(|a, b| canonical_cmp(a.get("_id").unwrap(), b.get("_id").unwrap()));
            for mut doc in docs {
                if n == 0 && filter::matches(&query, &doc).map_err(|e| e.to_string())? {
                    update::apply_matched(&spec, &query, &mut doc, NOW)
                        .map_err(|e| e.to_string())?;
                    n += 1;
                }
                after.push(doc);
            }
        }
        other => panic!("a kind of write: {other}"),
    }
    Ok((n, after))
}

#[test]
fn every_recorded_write_with_a_nan_or_infinity_filter_touches_what_the_reference_touches() {
    let mut failures = Vec::new();
    let mut rows = 0;
    for row in include_str!("data/nan_inf_writes.tsv").lines() {
        let [label, kind, before, command, outcome] = row.split('\t').collect::<Vec<_>>()[..]
        else {
            panic!("a row of five columns: {row}")
        };
        rows += 1;
        let before = documents(&json(before));
        let Bson::Document(command) = json(command) else { panic!("{command}") };
        let Bson::Document(outcome) = json(outcome) else { panic!("{outcome}") };
        let want_n =
            outcome.get("n").and_then(|v| v.as_i32().map(i64::from).or(v.as_i64())).unwrap();
        let want_after = documents(outcome.get("after").unwrap());
        match replay(kind, &before, &command) {
            Err(message) => failures.push(format!("{label} ({kind}): refused: {message}")),
            Ok((n, after)) => {
                let (got, want) = (shown(&after, &before), shown(&want_after, &before));
                if n != want_n || got != want {
                    failures.push(format!(
                        "{label} ({kind}): recorded n {want_n}, after {want:?}; ours n {n}, after {got:?}"
                    ));
                }
            }
        }
    }
    assert_eq!(rows, 58, "premise: the 58 write records of q7s3k (the generator wrote 58)");
    println!("nan_inf_writes: {rows} cases");
    assert!(failures.is_empty(), "{} of {rows} differ:\n{}", failures.len(), failures.join("\n"));
}
