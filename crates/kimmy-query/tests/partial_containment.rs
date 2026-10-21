//! A partial index the planner chooses holds every document `find` returns —
//! **including documents holding a `Decimal128`, decimal query operands and
//! decimal partial-filter bounds**.
//!
//! Deliberately end-to-end rather than a property of `implies` alone: the
//! planner's own containment predicates, its choice of index, and `find`'s
//! matching, against the membership the storage layer files.
//!
//! **History.** [ADR-185](../../../docs/decisions.md) was written when the
//! canonical order ranked a `Decimal128` equal to every number, so
//! `PartialFilter::implies` was not sound for a decimal document value: `Eq(5)`
//! and `Eq(6)` both held on one while neither implied the other. An independent
//! review of #363 built this search and found **825 (filter, query) pairs that
//! lose documents**, every one involving a `Decimal128` document value, out of
//! 14,698 index uses. ADR-185 widened membership instead (an undecidable run
//! the scan re-checked), and making `implies` sound was measured and rejected
//! then: the rule that was sound under that order declined 67% of index uses.
//! ADR-238 gives the comparator a decimal's exact value, which makes `implies`
//! sound with no rule changed, and removes the widening: **membership is
//! `selects(d)` and nothing else**. Restoring the widening is not a loss this
//! search can see (it only adds documents), so its mutation rows live where
//! membership is filed and enforced; restoring the old comparator brings the
//! lost pairs back, which is this test's.
//!
//! `selects(d)` is the call `kimmy_storage::index::document_keys` makes.
//! `a_partial_index_holds_exactly_what_the_filter_selects` in `kimmy-storage`
//! pins the two together, so this file cannot drift into testing a membership
//! rule the storage layer does not implement.

use bson::spec::BinarySubtype;
use bson::{Binary, Bson, Document, doc};
use kimmy_core::{IndexField, IndexMeta, PartialFilter};
use kimmy_query::{filter, plan};

/// Whether a partial index holds `d`: the filter selects it (ADR-183, ADR-238).
fn holds(p: &PartialFilter, d: &Document) -> bool {
    p.selects(d)
}

/// Every BSON shape the key encoding and the comparator distinguish, including
/// the `Decimal128`s that are the whole point of this corpus and what the
/// pre-ADR-185 version of it left out: cohorts of integers, values between and
/// beside doubles, an integer past 2^53, zeros, values past every double, the
/// specials, and decimals inside arrays and documents.
fn values() -> Vec<Bson> {
    let oid = bson::oid::ObjectId::parse_str("65a1b2c3d4e5f60718293a4b").unwrap();
    let re = |p: &str| {
        Bson::RegularExpression(bson::Regex {
            pattern: p.try_into().unwrap(),
            options: "".try_into().unwrap(),
        })
    };
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
        Bson::Int64(5),
        Bson::Int64(9_007_199_254_740_993),
        Bson::Double(9_007_199_254_740_992.0),
        Bson::Double(5.0),
        Bson::Double(-0.0),
        Bson::Double(7.5),
        Bson::Double(f64::NAN),
        Bson::Double(f64::INFINITY),
        Bson::Double(f64::NEG_INFINITY),
        Bson::Decimal128("1".parse().unwrap()),
        Bson::Decimal128("9".parse().unwrap()),
        dec("5.0"),
        dec("5.5"),
        dec("0.1"),
        Bson::Double(0.1),
        dec("0.1000000000000000055511151231257828"),
        dec("9007199254740993"),
        dec("-0E-100"),
        dec("1E+400"),
        dec("NaN"),
        dec("-Infinity"),
        Bson::String(String::new()),
        Bson::String("5".into()),
        Bson::String("x".into()),
        Bson::Symbol("x".into()),
        Bson::Document(doc! {}),
        Bson::Document(doc! {"a": 1}),
        Bson::Document(doc! {"a": null}),
        Bson::Document(doc! {"a": [1, 2]}),
        Bson::Document(doc! {"a": dec("1.0")}),
        Bson::Array(vec![]),
        Bson::Array(vec![Bson::Array(vec![])]),
        Bson::Array(vec![1.into(), 2.into()]),
        Bson::Array(vec![Bson::Array(vec![1.into(), 2.into()])]),
        Bson::Array(vec![5.into()]),
        Bson::Array(vec![1.into(), "x".into()]),
        Bson::Array(vec![Bson::Null]),
        Bson::Array(vec![Bson::Document(doc! {"a": 1})]),
        Bson::Array(vec![Bson::Decimal128("1".parse().unwrap()), 2.into()]),
        Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2] }),
        Bson::Binary(Binary { subtype: BinarySubtype::Uuid, bytes: vec![0; 16] }),
        Bson::ObjectId(oid),
        Bson::DateTime(bson::DateTime::from_millis(1_000)),
        Bson::DateTime(bson::DateTime::from_millis(-1_000)),
        Bson::Timestamp(bson::Timestamp { time: 5, increment: 1 }),
        re("^x"),
        re("5"),
        Bson::JavaScriptCode("f()".into()),
    ]
}

fn dec(text: &str) -> Bson {
    Bson::Decimal128(text.parse().unwrap())
}

/// The values a partial filter may hold as a bound: every value, decimals
/// included, since `PartialFilter::parse` accepts a `Decimal128` bound and
/// compares by its exact value (ADR-238). A query's operands are every value
/// too ([`values`]).
fn operands() -> Vec<Bson> {
    values()
}

fn docs() -> Vec<Document> {
    let mut out = vec![
        doc! {},
        doc! {"k": []},
        doc! {"k": {}},
        doc! {"k": [[]]},
        doc! {"k": [{}]},
        doc! {"k": {"a": []}},
        doc! {"k": [{"a": []}]},
        doc! {"k": [{"b": 1}]},
        doc! {"k": [[{"a": 1}]]},
    ];
    for v in values() {
        out.push(doc! {"k": v.clone()});
        out.push(doc! {"k": [v.clone()]});
        out.push(doc! {"k": [v.clone(), 1]});
        out.push(doc! {"k": [[v.clone()]]});
        out.push(doc! {"k": {"a": v.clone()}});
        out.push(doc! {"k": {"a": [v.clone()]}});
        out.push(doc! {"k": [{"a": v.clone()}]});
        out.push(doc! {"k": [{"a": v.clone()}, {"a": 1}]});
        out.push(doc! {"k": [{"b": 1}, {"a": v.clone()}]});
    }
    // A second field, so an index can sit somewhere other than the filtered
    // path — which is the shape the finding was reported in: an index on
    // `name`, a partial filter on `k`.
    for d in &mut out {
        d.insert("z", 1);
    }
    out
}

fn partials() -> Vec<Document> {
    let mut out = Vec::new();
    for path in ["k", "k.a"] {
        out.push(doc! {path: {"$exists": true}});
        for v in operands() {
            out.push(doc! {path: v.clone()});
            for op in ["$gt", "$gte", "$lt", "$lte"] {
                out.push(doc! {path: {op: v.clone()}});
            }
        }
    }
    out
}

fn queries() -> Vec<Document> {
    let mut out = Vec::new();
    for path in ["k", "k.a"] {
        out.push(doc! {path: {"$exists": true}});
        out.push(doc! {path: {"$exists": false}});
        for v in values() {
            out.push(doc! {path: v.clone()});
            out.push(doc! {path: {"$eq": v.clone()}});
            for op in ["$gt", "$gte", "$lt", "$lte", "$ne"] {
                out.push(doc! {path: {op: v.clone()}});
            }
            out.push(doc! {path: {"$in": [v.clone()]}});
            out.push(doc! {path: {"$in": [v.clone(), Bson::Null]}});
            out.push(doc! {path: {"$nin": [v.clone()]}});
            out.push(doc! {path: {"$not": {"$gt": v.clone()}}});
            out.push(doc! {path: {"$elemMatch": {"$gte": v.clone()}}});
            out.push(doc! {path: {"$all": [v.clone()]}});
            out.push(doc! {"$or": [{path: v.clone()}, {path: {"$gt": v.clone()}}]});
            out.push(doc! {"$and": [{path: {"$gte": v.clone()}}, {path: {"$exists": true}}]});
            out.push(doc! {path: {"$gt": v.clone(), "$exists": true}});
        }
    }
    out
}

fn index(on: &str, p: &Document) -> IndexMeta {
    IndexMeta {
        id: 1,
        name: "p".into(),
        fields: vec![IndexField::ascending(on)],
        unique: false,
        enforcement: Default::default(),
        multikey: true,
        expire_after_secs: None,
        partial_filter: Some(p.clone()),
        created: None,
    }
}

#[test]
fn a_chosen_partial_index_holds_every_document_find_returns() {
    let docs = docs();
    let partials: Vec<(Document, PartialFilter)> = partials()
        .into_iter()
        .filter_map(|p| PartialFilter::parse(&p).ok().map(|f| (p, f)))
        .collect();
    let queries: Vec<(Document, filter::Filter, filter::Filter)> = queries()
        .into_iter()
        .filter_map(|q| {
            let bare = filter::parse(&q).ok()?;
            let mut with_z = q.clone();
            with_z.insert("z", 1);
            let z = filter::parse(&with_z).ok()?;
            Some((q, bare, z))
        })
        .collect();

    // Premises. An empty search passes every assertion below, so each input is
    // asserted to be the size it is meant to be, and the corpus is asserted to
    // hold the values the whole record is about. 58 values: 9 + 58 x 9
    // documents; 2 paths x (1 + 58 x 5) filters, every one of which parses now
    // that a `Decimal128` bound does (ADR-238); 2 x (2 + 58 x 16) queries less
    // 20 refused (two regex operands among the values of `$in`, `$nin` or
    // `$all`, or as the operand of `$ne`: five queries each, on two paths).
    let has_decimal = |d: &Document| kimmy_core::holds_decimal128(&Bson::Document(d.clone()));
    assert_eq!(docs.len(), 531, "the document corpus");
    assert_eq!(partials.len(), 582, "the partial filters that parse");
    assert_eq!(queries.len(), 1_840, "the queries that parse");
    // Thirteen values hold a `Decimal128`.
    assert_eq!(
        docs.iter().filter(|d| has_decimal(d)).count(),
        117,
        "the documents holding a Decimal128 — the ones this record is about; a corpus without \
         them is the corpus that let the defect through"
    );
    assert_eq!(
        partials.iter().filter(|(p, _)| has_decimal(p)).count(),
        130,
        "the partial filters with a Decimal128 bound"
    );
    assert_eq!(
        queries.iter().filter(|(q, _, _)| has_decimal(q)).count(),
        416,
        "the queries with a Decimal128 operand"
    );

    let (mut used, mut lost) = (0usize, Vec::<String>::new());
    // Uses whose filter or query holds a decimal, and decimal documents a used
    // index was asked to hold: the parts of the search ADR-238 added.
    let (mut decimal_uses, mut decimal_returned) = (0usize, 0usize);
    let mut seen = std::collections::BTreeSet::new();
    for (pdoc, partial) in &partials {
        let path = pdoc.keys().next().unwrap().clone();
        for (qdoc, bare, with_z) in &queries {
            // Two index shapes: on a field the filter does not constrain, and
            // on the filtered path itself. The first is how the finding was
            // reported and the one a filtered path cannot accidentally cover.
            for (on, q) in [("z", with_z), (path.as_str(), bare)] {
                let Some(chosen) = plan::choose(q, &[index(on, pdoc)]) else { continue };
                assert_eq!(chosen.index_name, "p");
                used += 1;
                decimal_uses += usize::from(has_decimal(pdoc) || has_decimal(qdoc));
                for d in &docs {
                    let returned = filter::matches(q, d).unwrap();
                    decimal_returned += usize::from(returned && has_decimal(d));
                    if returned
                        && !holds(partial, d)
                        && seen.insert((pdoc.to_string(), qdoc.to_string()))
                    {
                        // `{:?}` for the document, not `{}`: a `Decimal128`
                        // renders as a bare `1` in Display, which is exactly the
                        // value the reader needs to see is a decimal.
                        lost.push(format!("index {pdoc} used for {qdoc} (on {on}) misses {d:?}"));
                    }
                }
            }
        }
    }

    assert!(used > 30_000, "premise: the planner chose the index, {used} times (35,106)");
    assert!(
        decimal_uses > 15_000,
        "premise: a decimal bound or operand was in the use, {decimal_uses} times (19,276)"
    );
    assert!(
        decimal_returned > 300_000,
        "premise: find returned a decimal document from a used index, {decimal_returned} times \
         (386,865)"
    );
    assert!(
        lost.is_empty(),
        "a partial index was used for a query whose matches it does not hold, which returns a \
         document short and says nothing (ADR-183, ADR-238). {} (filter, query) pairs:\n  {}",
        lost.len(),
        lost.iter().take(10).cloned().collect::<Vec<_>>().join("\n  ")
    );
}
