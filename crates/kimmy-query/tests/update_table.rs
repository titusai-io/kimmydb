//! Every update and find case of q7s2q and q7s2l, and the listed cases of q7s2r, q7s2t, q7s3b,
//! q7s3g, q7s3j, q7s2aa (1-10: the `$pull` of an `$elemMatch` with an `$all`, and of an `$all`),
//! q7s2u, q7s2v, q7s2y and q7s2z (1-12: the `$pull` of a document, a number and an array as a
//! condition), replayed from the recordings and compared with what the reference answered
//! (the stack's coverage table, `stack-coverage-ead7a07f.md`): the stored documents with their key
//! order, `n` and `nModified`, or a refusal.
//!
//! `data/update_table.tsv` is built from the recordings by `data/update_table_gen.py`, and `data/q7s2ab.tsv` (the positional `$` under `$or`, 102 rows) by `data/q7s2ab_gen.py`. Numbers are
//! compared by value, not by width (`norm`: the recordings are typed, but the aim of this table is
//! the behaviour). A refusal is compared as a refusal, not by code. A row of
//! `data/update_table_exceptions.txt` (label, then the heading text of the entry that covers it, which `kimmy-api/tests/exception_headings.rs` finds in `docs/deviations.md`) is a case a `docs/deviations.md` entry covers: it is run, and
//! its failure is not reported. **Every other failing row is reported, all of them, before the test
//! asserts.**
//!
//! q7s2y 1-68 and the 46 rows of q7s2u and q7s2v are the digit-segment writes (`o.01` beside
//! `o.1`): two fields where the container is a document or missing, applied; a refusal where it is
//! an array and both name the same element (the reference's server aborts, `serverAborted`, which
//! is read here as a refusal that writes nothing).

mod support;

use bson::{Bson, Document};
use kimmy_core::cmp::canonical_cmp;
use kimmy_query::{filter, seed, update};
use std::collections::HashMap;
use support::{documents, json};

const NOW: i64 = 1_700_000_000_000;

fn norm(value: &Bson) -> Bson {
    match value {
        Bson::Int32(n) => Bson::Double(f64::from(*n)),
        Bson::Int64(n) => Bson::Double(*n as f64),
        Bson::Array(items) => Bson::Array(items.iter().map(norm).collect()),
        Bson::Document(d) => Bson::Document(d.iter().map(|(k, v)| (k.clone(), norm(v))).collect()),
        other => other.clone(),
    }
}

/// The documents in `_id` order, each as the text of its normalised self (which keeps the order
/// of its fields).
fn shown(docs: &[Document]) -> Vec<String> {
    let mut sorted: Vec<&Document> = docs.iter().collect();
    sorted.sort_by(|a, b| match (a.get("_id"), b.get("_id")) {
        (Some(x), Some(y)) => canonical_cmp(x, y),
        (x, y) => x.is_some().cmp(&y.is_some()),
    });
    sorted.iter().map(|d| norm(&Bson::Document((*d).clone())).to_string()).collect()
}

struct Replied {
    n: i64,
    modified: i64,
    after: Vec<Document>,
}

fn replay_update(before: &[Document], command: &Document) -> Result<Replied, String> {
    let query = command.get_document("q").map_err(|e| e.to_string())?;
    let filter = filter::parse(query).map_err(|e| e.to_string())?;
    // An entry that is not a document is refused (q7s2l0283: `arrayFilters: [2]`, code 14); the
    // update language takes documents only, so it is refused here before the update is read.
    let array_filters: Vec<Document> = match command.get_array("arrayFilters") {
        Ok(entries) => entries
            .iter()
            .map(|entry| {
                entry
                    .as_document()
                    .cloned()
                    .ok_or_else(|| format!("an arrayFilters entry that is not a document: {entry}"))
            })
            .collect::<Result<_, _>>()?,
        Err(_) => Vec::new(),
    };
    let update = match command.get("u") {
        Some(Bson::Document(spec)) => update::parse_with_filters(spec, &array_filters),
        Some(Bson::Array(stages)) => update::parse_pipeline(stages, &array_filters),
        other => panic!("an update document: {other:?}"),
    }
    .map_err(|e| e.to_string())?;
    let multi = command.get_bool("multi").unwrap_or(false);
    let upsert = command.get_bool("upsert").unwrap_or(false);
    let (mut n, mut modified) = (0, 0);
    let mut after = Vec::new();
    for doc in before {
        let mut doc = doc.clone();
        if (multi || n == 0) && filter::matches(&filter, &doc).map_err(|e| e.to_string())? {
            let was = doc.to_string();
            update::apply_matched(&update, &filter, &mut doc, NOW).map_err(|e| e.to_string())?;
            n += 1;
            modified += i64::from(doc.to_string() != was);
        }
        after.push(doc);
    }
    if n == 0 && upsert {
        // As the executor builds the document an upsert inserts.
        let mut doc = Document::new();
        seed::implied_equalities(&filter, &mut doc, &mut Vec::new())?;
        let mut fields: Vec<(String, Bson)> = std::mem::take(&mut doc).into_iter().collect();
        fields.sort_by(|a, b| (a.0 != "_id", &a.0).cmp(&(b.0 != "_id", &b.0)));
        doc.extend(fields);
        update::apply_on_insert(&update, &mut doc, NOW).map_err(|e| e.to_string())?;
        after.push(doc);
        n = 1;
    }
    Ok(Replied { n, modified, after })
}

fn replay_find(before: &[Document], command: &Document) -> Result<Vec<Bson>, String> {
    let filter = filter::parse(command.get_document("filter").map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let mut ids = Vec::new();
    for doc in before {
        if filter::matches(&filter, doc).map_err(|e| e.to_string())? {
            ids.push(doc.get("_id").cloned().unwrap_or(Bson::Null));
        }
    }
    Ok(ids)
}

/// A recorded `after` and ours, with the `_id` of a document an upsert generated left out of both:
/// the reference makes an ObjectId and the executor, not the update, makes ours.
fn comparable(
    expected: &[Document],
    got: &[Document],
    before: &[Document],
) -> (Vec<String>, Vec<String>) {
    let known: Vec<&Bson> = before.iter().filter_map(|d| d.get("_id")).collect();
    let strip = |docs: &[Document]| -> Vec<Document> {
        docs.iter()
            .map(|d| match d.get("_id") {
                Some(Bson::ObjectId(_)) if !known.contains(&d.get("_id").unwrap()) => d
                    .iter()
                    .filter(|(k, _)| *k != "_id")
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                _ => d.clone(),
            })
            .collect()
    };
    (shown(&strip(expected)), shown(&strip(got)))
}

#[test]
fn every_recorded_update_and_find_answers_as_the_reference_does() {
    let exceptions: HashMap<&str, &str> = include_str!("data/update_table_exceptions.txt")
        .lines()
        .filter_map(|l| l.split_once('\t'))
        .collect();
    // Every exception cites the entry of docs/deviations.md that covers it by its heading text;
    // `kimmy-api/tests/exception_headings.rs` checks that the heading is there (only kimmy-api
    // tests read a documentation file).
    let mut failures = Vec::new();
    let mut total = 0;
    for (name, table, want_rows) in [
        ("update_table", include_str!("data/update_table.tsv"), 919),
        ("q7s2ab", include_str!("data/q7s2ab.tsv"), 102),
    ] {
        let mut rows = 0;
        for row in table.lines() {
            let [label, kind, before, command, outcome] = row.split('\t').collect::<Vec<_>>()[..]
            else {
                panic!("a row of five columns: {row}")
            };
            rows += 1;
            let before = documents(&json(before));
            let Bson::Document(command) = json(command) else { panic!("{command}") };
            let Bson::Document(outcome) = json(outcome) else { panic!("{outcome}") };
            let failure: Option<String> = if kind == "find" {
                match (outcome.get_array("ids"), replay_find(&before, &command)) {
                    (Ok(want), Ok(got))
                        if norm(&Bson::Array(got.clone())) == norm(&Bson::Array(want.clone())) =>
                    {
                        None
                    }
                    (Ok(want), got) => Some(format!("recorded ids {want:?}, ours {got:?}")),
                    _ => Some("a find with no recorded ids".into()),
                }
            } else if outcome.contains_key("error") {
                match replay_update(&before, &command) {
                    Err(_) => None,
                    Ok(done) => Some(format!(
                        "recorded a refusal (code {}), ours applied: n {}, nModified {}, after {:?}",
                        outcome.get("error").unwrap(),
                        done.n,
                        done.modified,
                        shown(&done.after)
                    )),
                }
            } else {
                let want_n = outcome
                    .get("n")
                    .and_then(|v| v.as_i64().or(v.as_i32().map(i64::from)))
                    .unwrap();
                let want_modified = outcome
                    .get("nModified")
                    .and_then(|v| v.as_i64().or(v.as_i32().map(i64::from)))
                    .unwrap();
                let want_after = documents(outcome.get("after").unwrap());
                match replay_update(&before, &command) {
                    Err(message) => Some(format!(
                        "recorded n {want_n}, nModified {want_modified}, after {:?}; ours refused: {message}",
                        shown(&want_after)
                    )),
                    Ok(done) => {
                        let (want, got) = comparable(&want_after, &done.after, &before);
                        if (done.n, done.modified) == (want_n, want_modified) && want == got {
                            None
                        } else {
                            Some(format!(
                                "recorded n {want_n}, nModified {want_modified}, after {want:?}; ours n {}, nModified {}, after {got:?}",
                                done.n, done.modified
                            ))
                        }
                    }
                }
            };
            if let Some(failure) = failure
                && !exceptions.contains_key(label)
            {
                failures.push(format!("{label} ({kind}): {failure}"));
            }
        }
        assert_eq!(rows, want_rows, "the generator wrote {want_rows} cases of {name}");
        println!("{name}: {rows} cases");
        total += rows;
    }
    assert!(
        failures.is_empty(),
        "{} of {total} rows differ and have no entry in docs/deviations.md:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
