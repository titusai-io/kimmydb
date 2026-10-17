//! What `find` answers, value for value, over a corpus meant to reach every
//! corner of the comparison operators: every value shape against every
//! operator with every value shape as its operand.
//!
//! Not an ordinary test. It proves that a change to the matcher's
//! implementation preserves behaviour, against the implementation it
//! replaced: run it on the commit before the change and on the change, each
//! writing its table to `FIND_DIFFERENTIAL_OUT`, and compare the two files.
//! ADR-181 moved the evaluation of `$exists`, equality and the four
//! comparisons into `kimmy-core`; this is how that move was shown to change
//! no answer. A test that compares the new code against its own caller cannot
//! show that, because after the move the two are the same code.
//!
//! 0.51.0's Decimal128 change was measured the same way, over this corpus with
//! its decimals (288,040 rows), against two parents:
//!
//! - against 0.50.0, 79,096 rows change. 72,960 went from `refused` (a decimal
//!   operand) to an answer, and 6,136 changed answer, 3,969 from a match to no
//!   match and 2,167 the other way. Every one of those 6,136 is a document
//!   holding a decimal, which the old order ranked equal to every number: the
//!   comparator's fix.
//! - against the exact comparator alone (before the operand refusals were
//!   lifted), the lifts change only the 72,960 `refused` rows.
//!
//! The answers they now give are held, without a second commit, by
//! `a_decimal128_answers_as_the_number_it_equals` and
//! `every_numeric_comparison_answers_by_exact_value` below.
//!
//! ```text
//! FIND_DIFFERENTIAL_OUT=/tmp/before.tsv cargo test -p kimmy-query \
//!     --test find_differential -- --ignored      # on the parent commit
//! FIND_DIFFERENTIAL_OUT=/tmp/after.tsv  cargo test ...  # on the change
//! cmp /tmp/before.tsv /tmp/after.tsv
//! ```

use std::fmt::Write as _;

use bson::spec::BinarySubtype;
use bson::{Binary, Bson, Document, doc};
use kimmy_query::filter;

fn dec(text: &str) -> Bson {
    Bson::Decimal128(text.parse().unwrap())
}

fn values() -> Vec<Bson> {
    let oid = bson::oid::ObjectId::parse_str("65a1b2c3d4e5f60718293a4b").unwrap();
    vec![
        Bson::Null,
        Bson::Undefined,
        Bson::MinKey,
        Bson::MaxKey,
        Bson::Boolean(false),
        Bson::Boolean(true),
        Bson::Int32(0),
        Bson::Int32(-1),
        Bson::Int32(5),
        Bson::Int32(6),
        Bson::Int32(i32::MAX),
        Bson::Int64(5),
        Bson::Int64(i64::MAX),
        Bson::Int64(i64::MIN),
        Bson::Double(5.0),
        Bson::Double(0.0),
        Bson::Double(-0.0),
        Bson::Double(7.5),
        Bson::Double(f64::NAN),
        Bson::Double(f64::INFINITY),
        Bson::Double(f64::NEG_INFINITY),
        Bson::Decimal128(bson::Decimal128::from_bytes([0; 16])),
        // Decimals equal to a number above (5, 7.5, zero, the specials and
        // i64::MAX), one just past 5, one tenth beside the double one tenth,
        // and one past every double; as stored values and as operands.
        dec("5"),
        dec("5.00"),
        dec("5.000000000000000000000000000000001"),
        dec("7.5"),
        dec("-0"),
        dec("-1"),
        dec("0.1"),
        Bson::Double(0.1),
        dec("9223372036854775807"),
        dec("1E+400"),
        dec("NaN"),
        dec("Infinity"),
        dec("-Infinity"),
        Bson::String(String::new()),
        Bson::String("5".into()),
        Bson::String("a".into()),
        Bson::String("x".into()),
        Bson::String("é\u{0}z".into()),
        Bson::Symbol("x".into()),
        Bson::Document(doc! {}),
        Bson::Document(doc! {"a": 1}),
        Bson::Document(doc! {"a": null}),
        Bson::Array(vec![]),
        Bson::Array(vec![Bson::Array(vec![])]),
        Bson::Array(vec![1.into(), 2.into()]),
        Bson::Array(vec![Bson::Array(vec![1.into(), 2.into()])]),
        Bson::Array(vec![5.into()]),
        Bson::Array(vec![1.into(), "x".into()]),
        Bson::Array(vec![Bson::Null]),
        Bson::Array(vec![Bson::Document(doc! {"a": 1})]),
        Bson::Array(vec![Bson::Document(doc! {"a": 5}), Bson::Document(doc! {"b": 1})]),
        Bson::Array(vec![dec("5.0")]),
        Bson::Array(vec![dec("1"), "x".into()]),
        Bson::Document(doc! {"a": dec("1.0")}),
        Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2] }),
        Bson::Binary(Binary { subtype: BinarySubtype::Uuid, bytes: vec![0; 16] }),
        Bson::ObjectId(oid),
        Bson::DateTime(bson::DateTime::from_millis(1_000)),
        Bson::DateTime(bson::DateTime::from_millis(-1_000)),
        Bson::Timestamp(bson::Timestamp { time: 5, increment: 1 }),
        Bson::RegularExpression(bson::Regex {
            pattern: "^x".try_into().unwrap(),
            options: "".try_into().unwrap(),
        }),
    ]
}

/// Every document: the field absent, and the field holding each value, at
/// `k` and one level down through `k.a`.
fn docs() -> Vec<Document> {
    let mut out = vec![doc! {"_id": 0}];
    for v in values() {
        out.push(doc! {"_id": 0, "k": v.clone()});
        out.push(doc! {"_id": 0, "k": {"a": v.clone()}});
        out.push(doc! {"_id": 0, "k": [{"a": v}]});
    }
    out
}

/// Every filter: each operator with each value as its operand, on `k` and on
/// `k.a`, plus the operand-free and compound forms.
fn filters() -> Vec<Document> {
    let mut out = Vec::new();
    for path in ["k", "k.a"] {
        out.push(doc! {path: {"$exists": true}});
        out.push(doc! {path: {"$exists": false}});
        for v in values() {
            out.push(doc! {path: v.clone()});
            for op in ["$eq", "$ne", "$gt", "$gte", "$lt", "$lte"] {
                out.push(doc! {path: {op: v.clone()}});
            }
            out.push(doc! {path: {"$in": [v.clone()]}});
            out.push(doc! {path: {"$nin": [v.clone()]}});
            out.push(doc! {path: {"$all": [v.clone()]}});
            out.push(doc! {path: {"$not": {"$gt": v.clone()}}});
            out.push(doc! {path: {"$gt": v.clone(), "$lt": Bson::MaxKey}});
        }
    }
    out
}

#[test]
#[ignore = "a differential across two commits: see the module documentation"]
fn write_the_table() {
    let Ok(path) = std::env::var("FIND_DIFFERENTIAL_OUT") else {
        panic!("set FIND_DIFFERENTIAL_OUT to the file to write; see the module documentation");
    };
    let docs = docs();
    let mut out = String::new();
    let mut rows = 0usize;
    for (i, f) in filters().iter().enumerate() {
        let parsed = filter::parse(f);
        for (j, d) in docs.iter().enumerate() {
            let answer = match &parsed {
                Ok(parsed) => match filter::matches(parsed, d) {
                    Ok(true) => "1",
                    Ok(false) => "0",
                    Err(_) => "unevaluable",
                },
                Err(_) => "refused",
            };
            writeln!(out, "{i}\t{j}\t{answer}\t{f:?}\t{d:?}").unwrap();
            rows += 1;
        }
    }
    std::fs::write(&path, out).unwrap();
    eprintln!("wrote {rows} rows to {path}");
}

/// Every filter the partial-filter language can express over `k` and `k.a`:
/// `$exists: true`, equality, and the four comparisons, each with every value
/// as its operand.
fn partial_filters() -> Vec<Document> {
    let mut out = Vec::new();
    for path in ["k", "k.a"] {
        out.push(doc! {path: {"$exists": true}});
        for v in values() {
            if kimmy_core::holds_decimal128(&v) {
                continue;
            }
            out.push(doc! {path: v.clone()});
            for op in ["$gt", "$gte", "$lt", "$lte"] {
                out.push(doc! {path: {op: v.clone()}});
            }
        }
    }
    out
}

#[test]
fn a_partial_filter_selects_exactly_what_find_returns() {
    // `PartialFilter::selects` is index membership (ADR-183) and what TTL
    // expiry asks before deleting (ADR-181), and it must be `find`'s answer
    // for every expression the partial language can carry. It shares
    // `kimmy_core::matching` with `find`; this holds the path resolution and
    // the conjunction around it to the same answer too.
    let docs = docs();
    for f in partial_filters() {
        let Ok(partial) = kimmy_core::PartialFilter::parse(&f) else { continue };
        let query = filter::parse(&f).unwrap();
        for d in &docs {
            assert_eq!(
                partial.selects(d),
                filter::matches(&query, d).unwrap(),
                "selects and find disagree on {f:?} for {d:?}"
            );
        }
    }
}

/// The filters of [`filters`] whose operand is `v`, in the same order.
fn filters_with(v: &Bson) -> Vec<Document> {
    let mut out = Vec::new();
    for path in ["k", "k.a"] {
        out.push(doc! {path: v.clone()});
        for op in ["$eq", "$ne", "$gt", "$gte", "$lt", "$lte"] {
            out.push(doc! {path: {op: v.clone()}});
        }
        out.push(doc! {path: {"$in": [v.clone()]}});
        out.push(doc! {path: {"$nin": [v.clone()]}});
        out.push(doc! {path: {"$all": [v.clone()]}});
        out.push(doc! {path: {"$not": {"$gt": v.clone()}}});
        out.push(doc! {path: {"$gt": v.clone(), "$lt": Bson::MaxKey}});
        out.push(doc! {path: {"$elemMatch": {"$gte": v.clone()}}});
    }
    out
}

/// The documents of [`docs`] holding `v`, in the same order.
fn docs_with(v: &Bson) -> Vec<Document> {
    vec![
        doc! {"_id": 0, "k": v.clone()},
        doc! {"_id": 0, "k": {"a": v.clone()}},
        doc! {"_id": 0, "k": [{"a": v.clone()}]},
        doc! {"_id": 0, "k": [v.clone(), "x"]},
    ]
}

fn answer(filter: &Document, d: &Document) -> &'static str {
    match filter::parse(filter) {
        Ok(parsed) => match filter::matches(&parsed, d) {
            Ok(true) => "1",
            Ok(false) => "0",
            Err(_) => "unevaluable",
        },
        Err(_) => "refused",
    }
}

/// A `Decimal128` behaves, as an operand and as a stored value, exactly as
/// the number of another type it equals: every filter answers every document
/// the same with one in place of the other. The numbers' own answers are the
/// ones this file pinned before a decimal could be an operand, so this holds
/// the decimal to them.
#[test]
fn a_decimal128_answers_as_the_number_it_equals() {
    let pairs = [
        (dec("5"), Bson::Int32(5)),
        (dec("5.00"), Bson::Double(5.0)),
        (dec("5E0"), Bson::Int64(5)),
        (dec("7.5"), Bson::Double(7.5)),
        (dec("-0"), Bson::Double(-0.0)),
        (Bson::Decimal128(bson::Decimal128::from_bytes([0; 16])), Bson::Int32(0)),
        (dec("-1"), Bson::Int32(-1)),
        (dec("NaN"), Bson::Double(f64::NAN)),
        (dec("Infinity"), Bson::Double(f64::INFINITY)),
        (dec("-Infinity"), Bson::Double(f64::NEG_INFINITY)),
        (dec("9223372036854775807"), Bson::Int64(i64::MAX)),
        (dec("2147483647.0"), Bson::Int32(i32::MAX)),
        (Bson::Array(vec![dec("5.0")]), Bson::Array(vec![5.into()])),
        (Bson::Document(doc! {"a": dec("1.0")}), Bson::Document(doc! {"a": 1})),
    ];
    let (all_docs, all_filters) = (docs(), filters());
    let mut compared = 0usize;
    for (decimal, number) in &pairs {
        // As the operand, against every document.
        for (fd, fnum) in filters_with(decimal).iter().zip(filters_with(number).iter()) {
            for d in &all_docs {
                assert_eq!(answer(fd, d), answer(fnum, d), "{fd} and {fnum} on {d}");
                compared += 1;
            }
        }
        // As the stored value, under every filter.
        for (dd, dnum) in docs_with(decimal).iter().zip(docs_with(number).iter()) {
            for f in &all_filters {
                assert_eq!(answer(f, dd), answer(f, dnum), "{f} on {dd} and {dnum}");
                compared += 1;
            }
        }
    }
    assert!(compared > 100_000, "premise: {compared} answers compared");
}

/// The numbers of the corpus by exact value, least first: each inner list is
/// one value in every type the corpus holds it in. Written by hand, as the
/// oracle the comparisons are held to.
fn numeric_classes() -> Vec<Vec<Bson>> {
    vec![
        vec![Bson::Double(f64::NAN), dec("NaN")],
        vec![Bson::Double(f64::NEG_INFINITY), dec("-Infinity")],
        vec![Bson::Int64(i64::MIN)],
        vec![Bson::Int32(-1), dec("-1")],
        vec![
            Bson::Int32(0),
            Bson::Double(0.0),
            Bson::Double(-0.0),
            dec("-0"),
            Bson::Decimal128(bson::Decimal128::from_bytes([0; 16])),
        ],
        // A tenth, then the double written 0.1, which is slightly above one.
        vec![dec("0.1")],
        vec![Bson::Double(0.1)],
        vec![Bson::Int32(5), Bson::Int64(5), Bson::Double(5.0), dec("5"), dec("5.00")],
        vec![dec("5.000000000000000000000000000000001")],
        vec![Bson::Int32(6)],
        vec![Bson::Double(7.5), dec("7.5")],
        vec![Bson::Int32(i32::MAX)],
        vec![Bson::Int64(i64::MAX), dec("9223372036854775807")],
        // Past every double but the infinity.
        vec![dec("1E+400")],
        vec![Bson::Double(f64::INFINITY), dec("Infinity")],
    ]
}

/// Every comparison operator, with every number of the corpus as the operand,
/// against every number of the corpus stored, answers by the exact values
/// alone: the hand-written order of [`numeric_classes`] says what each must be.
#[test]
fn every_numeric_comparison_answers_by_exact_value() {
    let classes = numeric_classes();
    // Premise: the oracle covers every number in the corpus.
    let listed: Vec<String> = classes.iter().flatten().map(|v| format!("{v:?}")).collect();
    for v in values() {
        if matches!(v, Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_)) {
            assert!(listed.contains(&format!("{v:?}")), "the oracle does not place {v:?}");
        }
    }
    let mut checked = 0usize;
    for (rs, stored) in classes.iter().enumerate() {
        for s in stored {
            let d = doc! {"k": s.clone()};
            for (ro, operands) in classes.iter().enumerate() {
                for o in operands {
                    let want = [
                        (doc! {"k": o.clone()}, rs == ro),
                        (doc! {"k": {"$eq": o.clone()}}, rs == ro),
                        (doc! {"k": {"$ne": o.clone()}}, rs != ro),
                        (doc! {"k": {"$gt": o.clone()}}, rs > ro),
                        (doc! {"k": {"$gte": o.clone()}}, rs >= ro),
                        (doc! {"k": {"$lt": o.clone()}}, rs < ro),
                        (doc! {"k": {"$lte": o.clone()}}, rs <= ro),
                        (doc! {"k": {"$in": [o.clone()]}}, rs == ro),
                        (doc! {"k": {"$nin": [o.clone()]}}, rs != ro),
                        (doc! {"k": {"$all": [o.clone()]}}, rs == ro),
                        (doc! {"k": {"$not": {"$lte": o.clone()}}}, rs > ro),
                    ];
                    for (f, expected) in want {
                        let got = answer(&f, &d);
                        assert_eq!(got, if expected { "1" } else { "0" }, "{f:?} on {d:?}");
                        checked += 1;
                    }
                }
            }
        }
    }
    assert!(checked > 9_000, "premise: {checked}");
}
