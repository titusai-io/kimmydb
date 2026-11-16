//! `$elemMatch`, `arrayFilters` and the positional `$` (q7s3-resolver-rule.md 2(d)).
//!
//! `resolver_element_cases.rs` is generated from the recordings q7s3b, q7s3g and q7s3i, and the
//! `$elemMatch` filters of `resolver_cases.rs` from q7s3f and q7s3g: each case names its label
//! (the set and the case number, the line of the set's `.jsonl`) and its line in the set's `.out`.

use super::common::assert_recorded;
use super::resolver_cases::{CASES_ELEM_MATCH_COUNT, cases_elem_match};
use super::resolver_element_cases::{CASES_ELEMENTS_COUNT, cases_elements};
use bson::{Bson, Document};
use kimmy_query::{filter, shape, update};
use std::collections::HashMap;

pub enum Run {
    /// A `find` with a filter and, when there is one, a projection.
    Find { filter: Document, projection: Document },
    /// An update of the documents `query` selects, once or for each.
    Update { query: Document, update: Document, array_filters: Vec<Document>, multi: bool },
}

pub enum Outcome {
    /// The ids a `find` answers when it has no projection (`resolver_element_cases_gen.py` emits
    /// it for a selected `find` with a filter alone that the reference answers). None of the
    /// recorded cases selected for these sets is one, so nothing builds it yet.
    #[allow(dead_code)]
    Ids(Vec<i32>),
    /// The documents a projection answers, or the collection after an update.
    Docs(Vec<Document>),
    /// Refused with this code (51246: the positional projection found no element).
    Refused(i32),
}

pub struct Rec {
    pub label: &'static str,
    pub line: u32,
    pub docs: fn() -> Vec<Document>,
    pub run: Run,
    pub outcome: Outcome,
}

fn norm(value: &Bson) -> Bson {
    match value {
        Bson::Int32(n) => Bson::Double(f64::from(*n)),
        Bson::Int64(n) => Bson::Double(*n as f64),
        Bson::Array(items) => Bson::Array(items.iter().map(norm).collect()),
        Bson::Document(d) => Bson::Document(d.iter().map(|(k, v)| (k.clone(), norm(v))).collect()),
        other => other.clone(),
    }
}

fn norm_all(docs: &[Document]) -> Vec<Bson> {
    docs.iter().map(|d| norm(&Bson::Document(d.clone()))).collect()
}

fn run(rec: &Rec) -> Result<Vec<Document>, String> {
    let docs = (rec.docs)();
    match &rec.run {
        Run::Find { filter: query, projection } => {
            let parsed = filter::parse(query).map_err(|e| e.to_string())?;
            let shaped = if projection.is_empty() {
                None
            } else {
                shape::parse_find_projection(projection).map_err(|e| e.to_string())?
            };
            let mut out = Vec::new();
            for d in docs {
                if !filter::matches(&parsed, &d).map_err(|e| e.to_string())? {
                    continue;
                }
                out.push(match &shaped {
                    Some(projection) => {
                        projection.project(&parsed, &d).map_err(|e| e.to_string())?
                    }
                    None => d,
                });
            }
            Ok(out)
        }
        Run::Update { query, update: spec, array_filters, multi } => {
            let parsed = filter::parse(query).map_err(|e| e.to_string())?;
            let update =
                update::parse_with_filters(spec, array_filters).map_err(|e| e.to_string())?;
            let mut out = Vec::new();
            let mut done = false;
            for mut d in docs {
                if !done && filter::matches(&parsed, &d).map_err(|e| e.to_string())? {
                    update::apply_matched(&update, &parsed, &mut d, 0)
                        .map_err(|e| e.to_string())?;
                    done = !multi;
                }
                out.push(d);
            }
            Ok(out)
        }
    }
}

/// The rows that differ from the recording and are kept: the label, then the text of the heading
/// of the `docs/deviations.md` entry that covers it, which must be there (checked by
/// `kimmy-api/tests/exception_headings.rs`). A row here is run and
/// its difference is not reported.
fn exceptions() -> HashMap<&'static str, &'static str> {
    include_str!("resolver_elements_exceptions.txt")
        .lines()
        .filter_map(|line| line.split_once('\t'))
        .collect()
}

/// Every recorded `$elemMatch` over arrays, `arrayFilters` over arrays of arrays, and positional
/// `$` of a projection and of an update, as the reference answers it.
#[test]
fn every_recorded_element_condition_answers_as_recorded() {
    let cases = cases_elements();
    assert_eq!(
        cases.len(),
        CASES_ELEMENTS_COUNT,
        "element cases: the generator wrote CASES_ELEMENTS_COUNT cases"
    );
    println!("element cases: {} cases", cases.len());
    let exceptions = exceptions();
    // The heading each exception cites is checked against docs/deviations.md by
    // `kimmy-api/tests/exception_headings.rs`: only kimmy-api tests read a documentation file.
    for label in exceptions.keys() {
        assert!(cases.iter().any(|c| c.label == *label), "{label}: not a row of the table");
    }
    let mut wrong = Vec::new();
    for rec in &cases {
        let got = run(rec);
        let ok = match (&rec.outcome, &got) {
            (Outcome::Refused(code), Err(message)) => {
                assert_eq!(*code, 51246, "{}", rec.label);
                message.contains("positional projection")
            }
            (Outcome::Ids(want), Ok(docs)) => {
                &docs.iter().map(|d| d.get_i32("_id").unwrap()).collect::<Vec<_>>() == want
            }
            (Outcome::Docs(want), Ok(docs)) => norm_all(docs) == norm_all(want),
            _ => false,
        };
        if !ok && !exceptions.contains_key(rec.label) {
            wrong.push(format!("{} (line {}): ours {got:?}", rec.label, rec.line));
        }
    }
    let refused = cases.iter().filter(|c| matches!(c.outcome, Outcome::Refused(_))).count();
    assert!(refused >= 4, "premise: the recorded 51246 cases: {refused}");
    assert!(
        wrong.is_empty(),
        "{} of {} cases differ:\n{}",
        wrong.len(),
        cases.len(),
        wrong.join("\n")
    );
}

/// The `$elemMatch` filters over arrays of arrays and positions (q7s3f 18, 19, 301-341, q7s3g
/// 139-155): a value form takes each element whole, a document included; a document form reads an
/// array element as `{"0": ..}`.
#[test]
fn every_recorded_elem_match_filter_answers_as_recorded() {
    let cases = cases_elem_match();
    assert_eq!(
        cases.len(),
        CASES_ELEM_MATCH_COUNT,
        "elem-match cases: the generator wrote CASES_ELEM_MATCH_COUNT cases"
    );
    println!("elem-match cases: {} cases", cases.len());
    assert_recorded("$elemMatch", cases);
}
