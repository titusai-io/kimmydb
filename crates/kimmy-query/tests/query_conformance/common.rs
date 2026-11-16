//! What the recorded cases are checked with: the case table, and the two checks over it.

#![allow(dead_code)]

use bson::{Bson, Document};
use kimmy_core::PartialFilter;
use kimmy_query::filter::{self, matches};

/// One recorded find: the documents it ran over, the filter, and the ids the reference
/// answered in order (`None` when it refused). `line` is the line of the case in the set's
/// `.out` recording.
pub struct Case {
    pub label: &'static str,
    pub line: u32,
    pub docs: fn() -> Vec<Document>,
    pub filter: Document,
    pub ids: Option<Vec<i32>>,
}

pub fn dec(text: &str) -> Bson {
    Bson::Decimal128(text.parse().expect("a decimal"))
}

pub fn re(pattern: &str, options: &str) -> Bson {
    Bson::RegularExpression(bson::Regex {
        pattern: pattern.to_string().try_into().expect("a pattern"),
        options: options.to_string().try_into().expect("options"),
    })
}

pub fn bin(bytes: &[u8], subtype: u8) -> Bson {
    Bson::Binary(bson::Binary {
        subtype: bson::spec::BinarySubtype::from(subtype),
        bytes: bytes.to_vec(),
    })
}

/// What this engine answers for a case: the ids in document order, or `None` when the filter is
/// refused (at parse, or by a document it cannot be evaluated on).
pub fn ours(docs: &[Document], query: &Document) -> Option<Vec<i32>> {
    let parsed = filter::parse(query).ok()?;
    let mut ids = Vec::new();
    for d in docs {
        if matches(&parsed, d).ok()? {
            ids.push(d.get_i32("_id").expect("an Int32 _id"));
        }
    }
    Some(ids)
}

/// Every case answered as recorded: a panic that lists each case that is not, with its label
/// and the `.out` line it is recorded at.
pub fn assert_recorded(set: &str, cases: Vec<Case>) {
    assert!(!cases.is_empty(), "{set}: no cases");
    let mut wrong = Vec::new();
    for case in &cases {
        let got = ours(&(case.docs)(), &case.filter);
        if got != case.ids {
            wrong.push(format!(
                "{} (line {}): {}\n    recorded {:?}\n    ours     {:?}",
                case.label, case.line, case.filter, case.ids, got
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{set}: {} of {} cases differ:\n{}",
        wrong.len(),
        cases.len(),
        wrong.join("\n")
    );
}

/// A partial index holds exactly what the filter selects: for every case whose filter a partial
/// index can carry, `PartialFilter::selects` answers each document as the recording does. That
/// is the membership `kimmy-storage` files a document under. Returns how many cases it checked.
pub fn assert_partial_membership(set: &str, cases: &[Case]) -> usize {
    let mut checked = 0;
    for case in cases {
        let Some(ids) = &case.ids else { continue };
        let Ok(partial) = PartialFilter::parse(&case.filter) else { continue };
        checked += 1;
        for d in (case.docs)() {
            let id = d.get_i32("_id").expect("an Int32 _id");
            assert_eq!(
                partial.selects(&d),
                ids.contains(&id),
                "{set}: {} (line {}): a partial index on {} and _id {id}",
                case.label,
                case.line,
                case.filter
            );
        }
    }
    checked
}

/// The cases with these labels, in the order the table lists them.
pub fn pick(cases: Vec<Case>, labels: &[&str]) -> Vec<Case> {
    let picked: Vec<Case> = cases.into_iter().filter(|c| labels.contains(&c.label)).collect();
    assert_eq!(picked.len(), labels.len(), "a label is not in the table: {labels:?}");
    picked
}

/// The operator and the bound when the case is `{v: {$gt | $gte | $lt | $lte: bound}}` alone.
pub fn range_of(case: &Case) -> Option<(&str, &Bson)> {
    let (field, Bson::Document(spec)) = case.filter.iter().next()? else { return None };
    if case.filter.len() != 1 || field != "v" || spec.len() != 1 {
        return None;
    }
    let (op, bound) = spec.iter().next()?;
    matches!(op.as_str(), "$gt" | "$gte" | "$lt" | "$lte").then_some((op.as_str(), bound))
}

/// The cases of a table that are a range over a bound of the kind `wanted` says.
pub fn ranges_over(cases: Vec<Case>, wanted: impl Fn(&Bson) -> bool) -> Vec<Case> {
    let picked: Vec<Case> =
        cases.into_iter().filter(|c| range_of(c).is_some_and(|(_, bound)| wanted(bound))).collect();
    assert!(!picked.is_empty(), "no case is a range over such a bound");
    picked
}

pub fn is_number(bound: &Bson) -> bool {
    matches!(bound, Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_))
}
