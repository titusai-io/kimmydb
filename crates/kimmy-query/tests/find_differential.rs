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
                Ok(parsed) => {
                    if filter::matches(parsed, d) {
                        "1"
                    } else {
                        "0"
                    }
                }
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
    // `PartialFilter::selects` is what TTL expiry asks before deleting
    // (ADR-181), and it must be `find`'s answer for every expression the
    // partial language can carry. It shares `kimmy_core::matching` with
    // `find`; this holds the path resolution and the conjunction around it to
    // the same answer too.
    let docs = docs();
    let mut membership_differs = 0usize;
    for f in partial_filters() {
        let Ok(partial) = kimmy_core::PartialFilter::parse(&f) else { continue };
        let query = filter::parse(&f).unwrap();
        for d in &docs {
            assert_eq!(
                partial.selects(d),
                filter::matches(&query, d),
                "selects and find disagree on {f:?} for {d:?}"
            );
            membership_differs += usize::from(partial.matches(d) != partial.selects(d));
        }
    }
    // The corpus has to be able to tell `find`'s answer from the index's
    // membership rule, which is the one the guard must not borrow. If it
    // could not, an implementation of `selects` that called `matches` would
    // pass.
    assert!(membership_differs > 0, "premise: the corpus separates the two rules");
    eprintln!("membership rule and find differ on {membership_differs} cases");
}
