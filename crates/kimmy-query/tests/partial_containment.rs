//! A partial index the planner chooses holds every document `find` returns —
//! **including documents holding a `Decimal128`**.
//!
//! This is the property [ADR-185](../../../docs/decisions.md) exists for, and
//! it is deliberately end-to-end rather than a property of `implies`.
//! `PartialFilter::implies` is **still** not transitive for a `Decimal128`, and
//! ADR-185 does not change that: the canonical order ranks one equal to every
//! number, so `Eq(5)` and `Eq(6)` both hold on one while neither implies the
//! other. What changed is what the index *holds*. A document whose membership
//! the filter cannot decide is held anyway, for the scan to re-check, so the
//! planner's answer is a superset of `find`'s and never a subset.
//!
//! **Where the numbers come from.** An independent review of #363 built this
//! search and found **825 (filter, query) pairs that lose documents**, every one
//! of them involving a `Decimal128` document value, out of 14,698 index uses.
//! Reverting the filing rule brings those 825 back, which is this test's
//! mutation row. The alternative — making `implies` itself sound — was measured
//! and rejected: it does not converge on witnesses, and the rule that is sound
//! declines 67% of index uses.
//!
//! Membership is modelled here as `selects(d) || undecidable_path(d).is_some()`,
//! which is the pair of calls `kimmy_storage::index::document_keys` makes, in
//! that order. `a_partial_index_holds_exactly_what_the_filter_decides_or_cannot`
//! in `kimmy-storage` pins the two together, so this file cannot drift into
//! testing a membership rule the storage layer does not implement.

use bson::spec::BinarySubtype;
use bson::{Binary, Bson, Document, doc};
use kimmy_core::{IndexField, IndexMeta, PartialFilter};
use kimmy_query::{filter, plan};

/// Whether a partial index holds `d` (ADR-185): the filter selects it, or the
/// filter cannot decide it and the index holds it for the scan to re-check.
fn holds(p: &PartialFilter, d: &Document) -> bool {
    p.selects(d) || p.undecidable_path(d).is_some()
}

/// Every BSON shape the key encoding and the comparator distinguish, including
/// three that carry a `Decimal128` — which is the whole point of this corpus
/// and what the pre-ADR-185 version of it left out.
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
        Bson::String(String::new()),
        Bson::String("5".into()),
        Bson::String("x".into()),
        Bson::Symbol("x".into()),
        Bson::Document(doc! {}),
        Bson::Document(doc! {"a": 1}),
        Bson::Document(doc! {"a": null}),
        Bson::Document(doc! {"a": [1, 2]}),
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

/// The values a *caller* may write, which excludes every `Decimal128`: the
/// product refuses one as a filter operand, an index bound, a sort key and an
/// expression literal, precisely because the order cannot rank it. So a
/// `Decimal128` reaches the question only as a stored document value, which is
/// the asymmetry this whole record is about.
fn operands() -> Vec<Bson> {
    values().into_iter().filter(|v| !kimmy_core::holds_decimal128(v)).collect()
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
        for v in operands() {
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
    // hold the values the whole record is about.
    assert_eq!(docs.len(), 432, "the document corpus");
    assert_eq!(partials.len(), 442, "the partial filters that parse");
    assert_eq!(queries.len(), 1_412, "the queries that parse");
    assert_eq!(
        docs.iter().filter(|d| kimmy_core::holds_decimal128(&Bson::Document((*d).clone()))).count(),
        27,
        "the documents holding a Decimal128 — the 27 this record is about; a corpus without them \
         is the corpus that let the defect through"
    );

    let (mut used, mut lost) = (0usize, Vec::<String>::new());
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
                for d in &docs {
                    if filter::matches(q, d)
                        && !holds(partial, d)
                        && seen.insert((pdoc.to_string(), qdoc.to_string()))
                    {
                        lost.push(format!("index {pdoc} used for {qdoc} (on {on}) misses {d}"));
                    }
                }
            }
        }
    }

    assert!(used > 10_000, "premise: the planner chose the index, {used} times");
    assert!(
        lost.is_empty(),
        "a partial index was used for a query whose matches it does not hold, which returns a \
         document short and says nothing (ADR-185). {} (filter, query) pairs:\n  {}",
        lost.len(),
        lost.iter().take(10).cloned().collect::<Vec<_>>().join("\n  ")
    );
}
