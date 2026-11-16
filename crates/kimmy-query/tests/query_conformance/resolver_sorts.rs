//! Sort keys through arrays, and the reference's 16746 (q7s3-resolver-rule.md 2(b)), as the
//! `$sort` stage, the heap of a bounded sort, `$setWindowFields`, `$sortArray` and `$push`'s
//! `$sort` read them.
//!
//! `resolver_sort_cases.rs` is generated from the recordings q7s3f, q7s3g, q7s3i and q7s3j: each
//! case names its label (the set and the case number, the line of the set's `.jsonl`) and its
//! line in the set's `.out`.

use super::resolver_bound_cases::{
    CASES_BOUNDS_COUNT, CASES_WHOLE_COUNT, cases_bounds, cases_whole,
};
use super::resolver_order_cases::{CASES_DOCUMENT_ORDER_COUNT, cases_document_order};
use super::resolver_sort_cases::{CASES_SORTS_COUNT, cases_sorts};
use bson::{Bson, Document, doc};
use kimmy_query::aggregate::{self, Limits};
use kimmy_query::{filter, shape, update};
use std::collections::HashMap;

pub enum Run {
    /// A `find` with a filter and a sort.
    Find { filter: Document, sort: Document },
    /// An aggregation, run a stage at a time.
    Pipeline(Vec<Document>),
    /// An update of the documents `query` selects.
    Update { query: Document, update: Document },
}

pub enum Outcome {
    /// The ids a `find` answers, in order.
    Ids(Vec<i32>),
    /// The documents an aggregation answers, or the collection after an update.
    Docs(Vec<Document>),
    /// The documents an aggregation answers, whose ORDER the reference does not keep between two
    /// runs (a tie): compared as a set. The generator lists each case with its reason.
    DocsAsSet(Vec<Document>),
    /// Refused with this code (only 16746 is recorded here).
    Refused(i32),
}

/// A recorded `find` of one collection with a sort bounded by `skip` and `limit` (0 is none).
pub struct Bound {
    pub label: &'static str,
    pub line: u32,
    pub docs: fn() -> Vec<Document>,
    pub sort: Document,
    pub skip: usize,
    pub limit: usize,
    pub ids: Vec<i32>,
}

pub struct Rec {
    pub label: &'static str,
    pub line: u32,
    pub docs: fn() -> Vec<Document>,
    pub run: Run,
    pub outcome: Outcome,
}

const AMBIGUOUS: &str = "Ambiguous field name found in array";

/// A number as a double throughout, so that a recorded `Int32` meets an `Int64` of the same value.
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

/// Two normalised values that are equal, a NaN being equal to a NaN (it is not equal to itself
/// under `==`, and the documents of these cases hold one).
fn same_value(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Double(x), Bson::Double(y)) => x == y || (x.is_nan() && y.is_nan()),
        (Bson::Array(x), Bson::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same_value(p, q))
        }
        (Bson::Document(x), Bson::Document(y)) => {
            x.len() == y.len()
                && x.iter().zip(y.iter()).all(|((kp, p), (kq, q))| kp == kq && same_value(p, q))
        }
        (p, q) => p == q,
    }
}

/// The documents `got` and `want`, in order, compared by [`same_value`] after [`norm`].
fn same_docs(got: &[Document], want: &[Document]) -> bool {
    let (got, want) = (norm_all(got), norm_all(want));
    got.len() == want.len() && got.iter().zip(&want).all(|(a, b)| same_value(a, b))
}

/// Two values of the same type and value: an `Int32` is not an `Int64` or a double, a `NaN` is
/// the `NaN` it was recorded as, and a document's fields are compared by name.
fn same_typed(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Double(x), Bson::Double(y)) => x == y || (x.is_nan() && y.is_nan()),
        (Bson::Array(x), Bson::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same_typed(p, q))
        }
        (Bson::Document(x), Bson::Document(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| same_typed(v, w)))
        }
        _ => std::mem::discriminant(a) == std::mem::discriminant(b) && a == b,
    }
}

fn same_typed_docs(ours: &[Document], want: &[Document]) -> bool {
    ours.len() == want.len()
        && ours
            .iter()
            .zip(want)
            .all(|(p, q)| same_typed(&Bson::Document(p.clone()), &Bson::Document(q.clone())))
}

fn ids(docs: &[Document]) -> Vec<i32> {
    docs.iter().map(|d| d.get_i32("_id").unwrap()).collect()
}

fn pipeline(docs: Vec<Document>, specs: &[Document]) -> Result<Vec<Document>, String> {
    let stages = aggregate::parse(specs).map_err(|e| e.to_string())?;
    let mut out = docs;
    for stage in &stages {
        out = aggregate::apply(stage, out, &Limits::default()).map_err(|e| e.to_string())?;
    }
    Ok(out)
}

fn run(rec: &Rec) -> Result<Vec<Document>, String> {
    let docs = (rec.docs)();
    match &rec.run {
        Run::Find { filter: query, sort } => {
            let parsed = filter::parse(query).map_err(|e| e.to_string())?;
            let matched: Vec<Document> =
                docs.into_iter().filter(|d| filter::matches(&parsed, d).unwrap()).collect();
            pipeline(matched, &[doc! { "$sort": sort.clone() }])
        }
        Run::Pipeline(specs) => pipeline(docs, specs),
        Run::Update { query, update: spec } => {
            let parsed = filter::parse(query).map_err(|e| e.to_string())?;
            let update = update::parse(spec).map_err(|e| e.to_string())?;
            let mut out = Vec::new();
            for mut d in docs {
                if filter::matches(&parsed, &d).unwrap() {
                    update::apply(&update, &mut d, 0).map_err(|e| e.to_string())?;
                }
                out.push(d);
            }
            Ok(out)
        }
    }
}

/// Documents compared with documents (q7s3g 98, 99; q7s3h 1-112; q7s3j 210-369): by the type of
/// each field's value, then its name, then the value, in a sort, a `$cmp`, `$max` and `$min` as
/// expressions and as `$group` accumulators, and the update operators `$max` and `$min`.
#[test]
fn every_recorded_comparison_of_documents_answers_as_recorded() {
    let cases = cases_document_order();
    assert_eq!(
        cases.len(),
        CASES_DOCUMENT_ORDER_COUNT,
        "order cases: the generator wrote CASES_DOCUMENT_ORDER_COUNT cases"
    );
    println!("order cases: {} cases", cases.len());
    // Every one of these recordings is typed (`$numberInt`, `$numberLong`, `$numberDouble`,
    // `$numberDecimal`), so the documents are compared by type and value: an `Int32` the
    // reference answers is not met by a double. No case of this table keeps the width-blind
    // `norm`; the other tables of this file still do.
    let mut wrong = Vec::new();
    for rec in &cases {
        let got = run(rec);
        let ok = match (&rec.outcome, &got) {
            (Outcome::Refused(_), Err(message)) => message.contains(AMBIGUOUS),
            (Outcome::Ids(want), Ok(docs)) => &ids(docs) == want,
            (Outcome::Docs(want), Ok(docs)) => same_typed_docs(docs, want),
            _ => false,
        };
        if !ok {
            let recorded = match &rec.outcome {
                Outcome::Docs(want) | Outcome::DocsAsSet(want) => format!("{want:?}"),
                Outcome::Ids(want) => format!("{want:?}"),
                Outcome::Refused(code) => format!("refused {code}"),
            };
            wrong.push(format!(
                "{} (line {}): recorded {recorded}; ours {got:?}",
                rec.label, rec.line
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "the document order: {} of {} differ:\n{}",
        wrong.len(),
        cases.len(),
        wrong.join("\n")
    );
}

/// Every recorded sort over arrays, in an aggregation's stages and a `find`'s sort, an update's
/// `$push` with `$sort` and a `$sortArray`: the ids or documents the reference answers, or its
/// 16746.
#[test]
fn every_recorded_sort_over_a_path_through_arrays_answers_as_recorded() {
    let cases = cases_sorts();
    assert_eq!(
        cases.len(),
        CASES_SORTS_COUNT,
        "sort cases: the generator wrote CASES_SORTS_COUNT cases"
    );
    println!("sort cases: {} cases", cases.len());
    let exceptions = exceptions();
    for (label, reason) in &exceptions {
        assert!(
            cases.iter().any(|c| c.label == *label)
                || cases_bounds().iter().any(|c| c.label == *label),
            "{label}: not a row of the tables"
        );
        assert!(reason.contains("built in the commit"), "{label}: the reason names no commit");
    }
    let mut wrong = Vec::new();
    let mut refused = 0;
    for rec in &cases {
        let got = run(rec);
        let ok = match (&rec.outcome, &got) {
            (Outcome::Refused(code), Err(message)) => {
                refused += 1;
                assert_eq!(*code, 16746, "{}", rec.label);
                message.contains(AMBIGUOUS)
            }
            (Outcome::Ids(want), Ok(docs)) => &ids(docs) == want,
            (Outcome::Docs(want), Ok(docs)) => same_docs(docs, want),
            (Outcome::DocsAsSet(want), Ok(docs)) => same_set(docs, want),
            _ => false,
        };
        if !ok && !exceptions.contains_key(rec.label) {
            wrong.push(format!(
                "{} (line {}): ours {:?}",
                rec.label,
                rec.line,
                got.map(|d| ids_or_docs(&d))
            ));
        }
    }
    assert!(refused >= 20, "premise: the recorded 16746 cases: {refused}");
    assert!(
        wrong.is_empty(),
        "{} of {} cases differ:\n{}",
        wrong.len(),
        cases.len(),
        wrong.join("\n")
    );
}

/// The rows that differ from the recording for a reason a later commit of the stack removes: the
/// label, then the reason, which names that commit. A row here is run and its difference is not
/// reported; the commit that removes the reason removes the row (the file is then empty of it).
fn exceptions() -> HashMap<&'static str, &'static str> {
    include_str!("resolver_sorts_exceptions.txt")
        .lines()
        .filter_map(|line| line.split_once('\t'))
        .collect()
}

/// The two-key sort of q7s3f0035 with every bound the reference was asked (q7s3m 1 to 22), by the
/// full sort and by the heap of a bounded one. The rows of the table are the rows of the tuple
/// rule, and are in the exceptions file until the commit that builds it.
#[test]
fn every_recorded_bound_of_the_two_key_sort_answers_as_recorded() {
    let cases = cases_bounds();
    assert_eq!(
        cases.len(),
        CASES_BOUNDS_COUNT,
        "bound cases: the generator wrote CASES_BOUNDS_COUNT cases"
    );
    println!("bound cases: {} cases", cases.len());
    let exceptions = exceptions();
    let mut wrong = Vec::new();
    for case in &cases {
        let keys = shape::parse_sort(&case.sort).unwrap();
        let docs = (case.docs)();
        let take = if case.limit == 0 { usize::MAX } else { case.limit };
        let mut full = docs.clone();
        shape::sort(&keys, &mut full);
        let by_sort: Vec<i32> = ids(&full).into_iter().skip(case.skip).take(take).collect();
        let by_heap: Vec<i32> = if case.limit == 0 {
            by_sort.clone()
        } else {
            let limits = Limits::default();
            let mut lease = limits.buffer.lease("the heap");
            let mut top = shape::TopK::new(&keys, case.skip + case.limit);
            for d in &docs {
                top.offer(d.clone(), &mut lease, &limits).unwrap();
            }
            ids(&top.into_sorted()).into_iter().skip(case.skip).take(take).collect()
        };
        if (by_sort != case.ids || by_heap != case.ids) && !exceptions.contains_key(case.label) {
            wrong.push(format!(
                "{} (line {}): skip {} limit {}: recorded {:?}, sort {:?}, heap {:?}",
                case.label, case.line, case.skip, case.limit, case.ids, by_sort, by_heap
            ));
        }
    }
    assert!(wrong.is_empty(), "{} of {} differ:\n{}", wrong.len(), cases.len(), wrong.join("\n"));
}

/// An element sort (`$sortArray`, `$push` with `$sort`) compares the key as a whole value and
/// descending is the reverse of ascending (q7s3m 23 to 50: `[{s: 3}, {s: [0, 5]}, {s: [1, 2]}]`
/// ascending, the reverse descending; q7s4o 1 to 10: the eight values of `k`, ascending
/// `[4, 8, 5, 2, 6, 7, 1, 3]` and descending `[3, 1, 7, 6, 2, 5, 4, 8]`); a `find` or a `$sort`
/// stage takes the smallest element of an array ascending and the largest descending (q7s4o 11
/// to 20: `[4, 8, 1, 3, 5, 2, 6, 7]` and `[7, 6, 1, 3, 2, 5, 4, 8]`), and one-key sorts of the
/// 16 documents of q7s3f0035 (q7s3m 51 to 54). An empty array as such a key is an array compared
/// whole, after every number and before the other arrays ascending, reversed descending, in both
/// element sorts (q7s3m0041, q7s3m0042 for `$push`; q7s4p 1 to 16 for `$sortArray` and `$push`).
#[test]
fn the_element_sorts_compare_whole_values_where_the_sort_takes_an_element() {
    let cases = cases_whole();
    assert_eq!(
        cases.len(),
        CASES_WHOLE_COUNT,
        "whole-value cases: the generator wrote CASES_WHOLE_COUNT cases"
    );
    println!("whole-value cases: {} cases", cases.len());
    let mut wrong = Vec::new();
    for rec in &cases {
        let got = run(rec);
        let ok = match (&rec.outcome, &got) {
            (Outcome::Ids(want), Ok(docs)) => &ids(docs) == want,
            (Outcome::Docs(want), Ok(docs)) => same_docs(docs, want),
            _ => false,
        };
        if !ok {
            wrong.push(format!("{} (line {}): ours {:?}", rec.label, rec.line, got));
        }
    }
    assert!(wrong.is_empty(), "{} of {} differ:\n{}", wrong.len(), cases.len(), wrong.join("\n"));
}

/// The same documents in any order, each compared as `norm` reads it.
fn same_set(got: &[Document], want: &[Document]) -> bool {
    let sorted = |docs: &[Document]| {
        let mut texts: Vec<String> = norm_all(docs).iter().map(ToString::to_string).collect();
        texts.sort();
        texts
    };
    sorted(got) == sorted(want)
}

fn ids_or_docs(docs: &[Document]) -> String {
    format!("{docs:?}")
}

/// The refusal is made on the documents the sort reads, every key of each taken before any is
/// compared, whatever the window: a bounded sort (`limit: 3`, `skip: 2`) refuses as an unbounded
/// one does, and a document the `limit` would have dropped still refuses (q7s3g 105, 106, 126,
/// 127; q7s3i 68). A filter that removes the clash documents first does not refuse (g:131, i:44).
#[test]
fn a_bounded_sort_refuses_as_the_whole_sort_does() {
    let exceptions = exceptions();
    for rec in cases_sorts() {
        if exceptions.contains_key(rec.label) {
            continue;
        }
        let Run::Find { filter: query, sort } = &rec.run else { continue };
        let parsed = filter::parse(query).unwrap();
        let keys = shape::parse_sort(sort).unwrap();
        let matched: Vec<Document> =
            (rec.docs)().into_iter().filter(|d| filter::matches(&parsed, d).unwrap()).collect();
        let limits = Limits::default();
        for (skip, limit) in [(0usize, 1usize), (0, 3), (2, 3), (0, 100)] {
            let mut lease = limits.buffer.lease("the heap");
            let mut top = shape::TopK::new(&keys, skip + limit);
            let offered: Result<Vec<()>, _> =
                matched.iter().map(|d| top.offer(d.clone(), &mut lease, &limits)).collect();
            match (&rec.outcome, offered) {
                (Outcome::Refused(_), Err(e)) => {
                    assert!(e.to_string().contains(AMBIGUOUS), "{}: {e}", rec.label)
                }
                (Outcome::Ids(want), Ok(_)) => {
                    let got: Vec<i32> = ids(&top
                        .into_sorted()
                        .into_iter()
                        .skip(skip)
                        .take(limit)
                        .collect::<Vec<_>>());
                    let want: Vec<i32> = want.iter().copied().skip(skip).take(limit).collect();
                    assert_eq!(got, want, "{} skip {skip} limit {limit}", rec.label);
                }
                (outcome, offered) => panic!(
                    "{}: skip {skip} limit {limit}: {} against {:?}",
                    rec.label,
                    matches!(outcome, Outcome::Refused(_)),
                    offered.map(|_| ())
                ),
            }
        }
    }
}

/// `$sortArray` and `$push`'s `$sort` never refuse, and compare what a path ends at whole
/// (q7s3j 187-209; Dissent 1 of the brief): `$push … $sort: {x: 1}` orders `[3]`, `[{"0":1},5]`
/// and `[{"0":2},1]` as arrays.
#[test]
fn an_element_path_sort_never_refuses_and_compares_the_value_whole() {
    let clash = doc! { "x": [{ "0": 2 }, 1] };
    let plain = doc! { "x": [3] };
    let keys = shape::parse_sort(&doc! { "x": 1 }).unwrap();
    let a = shape::element_sort_keys(&keys, &clash);
    let b = shape::element_sort_keys(&keys, &plain);
    assert_eq!(a, vec![Bson::Array(vec![Bson::Document(doc! { "0": 2 }), Bson::Int32(1)])]);
    // `[3]` is below `[{"0":2},1]`: the arrays compare by their first elements, a number
    // against a document, whole (j:202-205), where a sort key would take the least element.
    assert_eq!(shape::compare_keys(&keys, &b, &a), std::cmp::Ordering::Less);
    // A digit is a position and a name is not looked up through an array (j:189, 194).
    let keys = shape::parse_sort(&doc! { "x.0": 1 }).unwrap();
    assert_eq!(
        shape::element_sort_keys(&keys, &clash),
        vec![Bson::Document(doc! { "0": 2 })],
        "the element at 0, not the field named 0 of an element"
    );
    let keys = shape::parse_sort(&doc! { "x.b": 1 }).unwrap();
    assert_eq!(shape::element_sort_keys(&keys, &doc! { "x": [{ "b": 1 }] }), vec![Bson::Null]);
    // The sort key of the `$sort` stage on the same path refuses.
    let keys = shape::parse_sort(&doc! { "x.0": 1 }).unwrap();
    let err = shape::sort_keys(&keys, &doc! { "x": [{ "0": 2 }, 1] }).unwrap_err();
    assert!(err.to_string().contains(AMBIGUOUS), "{err}");
}
