//! What the executor actually yields for a query the planner answered from a
//! partial index — **once each, and none missing**.
//!
//! `partial_containment.rs` in `kimmy-query` models membership: it asks whether
//! the set of documents an index *should* hold covers what `find` returns. It
//! never runs the executor, and that gap hid a defect for the whole of
//! [ADR-185](../../../docs/decisions.md)'s first review.
//!
//! **The defect it hid.** A range with an unbounded lower end encodes that end
//! as the empty key, which is the unkeyed run itself, so such a range walked the
//! run a second time after it had already been walked as the prepended range.
//! The multikey de-duplication skips empty keys, so nothing caught the repeat:
//! `count` over-counted, a sorted `find` returned the same `_id` twice, and
//! `explain` over-reported entries read. It predates ADR-185 — a document no
//! index can key was already doubled — and ADR-185 made the class common,
//! because a `Decimal128` on a money field puts most of a collection in that run.
//!
//! So this file drives the real `visit_index_candidates`, in each of the orders
//! a query can ask for, and compares what comes back with what a collection scan
//! says. Two properties, and the first is the one a membership model cannot see:
//!
//! 1. **No document is yielded twice.**
//! 2. **No document `find` returns is missing.**

use bson::{Bson, Document, doc};
use kimmy_storage::{CandidateOrder, CollectionMeta, Engine, IndexField, IndexScan};

/// Values that put a document in the unkeyed run, and values that do not.
///
/// The `Decimal128`s are ADR-185's case — the order cannot rank one against a
/// number, so the filter cannot decide the document. `NaN` and the infinities
/// are ordinary `Double`s that the order *can* rank, and they are here because a
/// reviewer asked for them and because they are the values most likely to be
/// confused with the first group.
fn values() -> Vec<(&'static str, Bson)> {
    let dec = |s: &str| Bson::Decimal128(s.parse().unwrap());
    vec![
        ("a plain number below the bound", Bson::Int32(1)),
        ("a plain number above it", Bson::Int32(9)),
        ("a decimal", dec("1")),
        ("a decimal above the bound", dec("9")),
        ("a decimal in an array", Bson::Array(vec![dec("1"), Bson::Int32(2)])),
        ("a decimal alone in an array", Bson::Array(vec![dec("1")])),
        ("a decimal nested twice", Bson::Array(vec![Bson::Array(vec![dec("1")])])),
        ("NaN", Bson::Double(f64::NAN)),
        ("positive infinity", Bson::Double(f64::INFINITY)),
        ("negative infinity", Bson::Double(f64::NEG_INFINITY)),
        ("a string", Bson::String("x".into())),
        ("absent", Bson::Null),
    ]
}

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    e.create_collection("app", "docs").unwrap();
    (e, dir)
}

/// Every document the executor yields for `query` through `index`, in order.
fn candidates(
    engine: &Engine,
    coll: &CollectionMeta,
    query: &Document,
    order: CandidateOrder<'_>,
) -> Option<Vec<i64>> {
    let filter = kimmy_query::filter::parse(query).unwrap();
    let plan = kimmy_query::plan::choose(&filter, &coll.indexes)?;
    let scan = IndexScan {
        index_id: plan.index_id,
        ranges: &plan.ranges,
        both_bounds: plan.both_bounds,
        exact: plan.exact,
    };
    let mut ids = Vec::new();
    engine
        .visit_index_candidates(coll, &scan, order, |_, _, doc| {
            ids.push(doc.get_i64("_id").unwrap());
            Ok(true)
        })
        .unwrap()
        .expect("the scan was not refused");
    Some(ids)
}

/// What a collection scan says the query returns — the oracle.
fn by_scan(engine: &Engine, coll: &CollectionMeta, query: &Document, docs: usize) -> Vec<i64> {
    let filter = kimmy_query::filter::parse(query).unwrap();
    let mut out = Vec::new();
    for id in 0..docs as i64 {
        if let Some(doc) = engine.get(coll, &kimmy_core::DocId::Int64(id)).unwrap()
            && kimmy_query::filter::matches(&filter, &doc)
        {
            out.push(id);
        }
    }
    out
}

#[test]
fn the_executor_yields_each_candidate_once_and_loses_none() {
    // Four index shapes, because the walk differs by shape: a plain ascending
    // index takes one path, a descending one inverts its bounds, and a compound
    // one can be multikey — which is where the de-duplication that skips empty
    // keys lives.
    let shapes: Vec<(&str, Vec<IndexField>)> = vec![
        ("ascending", vec![IndexField::ascending("name")]),
        ("descending", vec![IndexField::descending("name")]),
        ("compound", vec![IndexField::ascending("name"), IndexField::ascending("tag")]),
        ("on the filtered path", vec![IndexField::ascending("k")]),
    ];
    // Filters on `k` and on a nested `k.a`, so a decimal at either depth is
    // covered.
    let filters = [doc! { "k": { "$gt": 5 } }, doc! { "k.a": { "$gt": 5 } }];
    // Queries whose ranges have an open low end, an open high end, both bounds,
    // and an exact probe — the open low end is the shape that was broken, and
    // the others are here so a fix that breaks them is caught.
    let queries = [
        doc! { "name": { "$lt": "z" }, "k": 7 },
        doc! { "name": { "$gt": "a" }, "k": 7 },
        doc! { "name": { "$gt": "a", "$lt": "z" }, "k": 7 },
        doc! { "name": "n", "k": 7 },
        doc! { "name": { "$lt": "z" }, "k": { "$gte": 6 } },
        doc! { "k": { "$gte": 6 } },
    ];

    let mut checked = 0;
    let mut used = 0;
    for (shape, fields) in &shapes {
        for filter in &filters {
            let (engine, _dir) = engine();
            let coll = engine.get_collection("app", "docs").unwrap();
            // One document per value, at both `k` and `k.a`, so the filter's
            // path and the other path are both exercised.
            let mut docs = 0;
            for (_, value) in values() {
                for nested in [false, true] {
                    // **The indexed value varies too.** With every document
                    // holding `name: "n"`, no document's *key* could ever be an
                    // unusual encoding, so this file could not see a change to
                    // the key encoding at all — the empty string at an indexed
                    // path is precisely what `no_value_encodes_to_an_empty_key`
                    // in `kimmy-core` forbids, and it has to reach a key here for
                    // the two tests to guard each other.
                    let name: Bson = match docs % 3 {
                        0 => "n".into(),
                        1 => "".into(),
                        _ => Bson::MinKey,
                    };
                    let mut d = doc! { "_id": docs as i64, "name": name, "tag": 1 };
                    if value != Bson::Null {
                        if nested {
                            d.insert("k", doc! { "a": value.clone() });
                        } else {
                            d.insert("k", value.clone());
                        }
                    }
                    engine.insert(&coll, d).unwrap();
                    docs += 1;
                }
            }
            // **Documents that populate the other sentinel run.** Arrays at both
            // compound paths are unkeyable — but only for a document the filter
            // *selects*, because an unselected one never reaches the keying step
            // at all. So these carry a `k` above the bound as well. Without them
            // the `UNKEYED` run stayed empty and this file exercised one of the
            // two prepended ranges.
            for extra in 0..3 {
                engine
                    .insert(
                        &coll,
                        doc! {
                            "_id": (docs + extra) as i64,
                            "name": [format!("arr{extra}"), "m".to_string()],
                            "tag": [1, 2],
                            // The value has to sit at the path *this* filter
                            // constrains, or the document is not selected, never
                            // reaches the keying step, and the run stays empty —
                            // which is how the first version of this premise
                            // failed.
                            "k": if filter.contains_key("k") {
                                Bson::Int32(9)
                            } else {
                                Bson::Document(doc! { "a": 9 })
                            },
                        },
                    )
                    .unwrap();
            }
            docs += 3;

            engine
                .create_index_with(
                    "app",
                    "docs",
                    fields.clone(),
                    false,
                    Default::default(),
                    Some("idx".into()),
                    None,
                    Some(filter.clone()),
                )
                .unwrap();
            let coll = engine.get_collection("app", "docs").unwrap();

            // Premise: both sentinel runs hold something for at least the
            // compound shape, or this file is testing one of the two paths.
            let idx = coll.index("idx").unwrap();
            let (cannot_key, undecided) = (
                engine.unkeyed_count(&coll, idx.id).unwrap(),
                engine.undecidable_count(&coll, idx.id).unwrap(),
            );
            if *shape == "compound" {
                assert!(
                    cannot_key > 0 && undecided > 0,
                    "premise: the {shape} index with filter {filter} must populate both sentinel \
                     runs, and holds {cannot_key} unkeyable and {undecided} undecidable"
                );
            }

            for query in &queries {
                // Both deliveries: `Any` is what a `count` uses, and `ById`
                // is what a sorted `find` and a cursor page use — the two
                // surfaces the repeat showed up on.
                for (order_name, order) in [
                    ("any", CandidateOrder::Any),
                    ("by id", CandidateOrder::ById { after: None, want: None }),
                ] {
                    let Some(ids) = candidates(&engine, &coll, query, order) else {
                        continue; // the planner scanned, which is always sound
                    };
                    used += 1;
                    let where_ = format!(
                        "{shape} index, filter {filter}, query {query}, order {order_name}"
                    );

                    // 1. Once each.
                    let mut seen = std::collections::BTreeSet::new();
                    let repeats: Vec<i64> =
                        ids.iter().filter(|id| !seen.insert(**id)).copied().collect();
                    assert!(
                        repeats.is_empty(),
                        "the executor yielded {repeats:?} more than once, so count over-counts \
                         and a sorted find repeats a document: {where_}"
                    );

                    // 2. Nothing missing.
                    let truth = by_scan(&engine, &coll, query, docs);
                    let missing: Vec<i64> =
                        truth.iter().filter(|id| !seen.contains(id)).copied().collect();
                    assert!(
                        missing.is_empty(),
                        "a collection scan returns {missing:?} and the indexed answer does not, \
                         which is a document short and says nothing: {where_}"
                    );
                    checked += 1;
                }
            }
        }
    }

    // Premises: an empty search satisfies both assertions above.
    assert!(used > 30, "premise: the planner chose the index, {used} times");
    assert!(checked > 30, "premise: comparisons were made ({checked})");
}
