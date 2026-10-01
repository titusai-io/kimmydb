//! Several bounds on one indexed path: the planner reads their intersection,
//! whatever order the clauses arrive in, and the answer is the one a
//! collection scan gives.
//!
//! The planner used to keep the **first** lower and the first upper bound it
//! met, so `{$and: [{a: {$gt: 0}}, {a: {$gt: 5}}]}` read from 0 and the same
//! clauses swapped read from 5. Parsing reorders clauses (those holding an
//! `$expr` go last), so the range a query read could change between two
//! commits with no change to the query. It now keeps the tightest of each.
//!
//! Two properties, over a real engine:
//!
//! 1. **The range read.** Each combination reads exactly the index entries
//!    its tightest bounds alone read, in both clause orders.
//! 2. **The answer.** Over a corpus of documents — scalars of every comparable
//!    type, and arrays whose elements satisfy different bounds — and every pair
//!    of bounds drawn from a set of values of mixed types, the indexed answer is
//!    the collection scan's, in both clause orders, on ascending, descending and
//!    compound indexes, with and without the multikey flag set.

use bson::{Bson, Document, doc};
use kimmy_storage::{CandidateOrder, CollectionMeta, Engine, IndexField, IndexScan};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    e.create_collection("app", "docs").unwrap();
    (e, dir)
}

/// What an indexed read of `query` returns, and how many index entries it
/// read; `None` when the planner chose to scan.
fn indexed(engine: &Engine, coll: &CollectionMeta, query: &Document) -> Option<(Vec<i64>, usize)> {
    let filter = kimmy_query::filter::parse(query).unwrap();
    let plan = kimmy_query::plan::choose(&filter, &coll.indexes)?;
    let scan = IndexScan {
        index_id: plan.index_id,
        ranges: &plan.ranges,
        both_bounds: plan.both_bounds,
        exact: plan.exact,
    };
    let mut ids = Vec::new();
    let outcome = engine
        .visit_index_candidates(
            coll,
            &scan,
            CandidateOrder::Any,
            kimmy_storage::WalkScope::Request,
            |_, _, doc| {
                if kimmy_query::filter::matches(&filter, &doc).unwrap() {
                    ids.push(doc.get_i64("_id").unwrap());
                }
                Ok(true)
            },
        )
        .unwrap()
        .expect("the scan was not refused");
    ids.sort_unstable();
    Some((ids, outcome.entries))
}

/// What the filter selects from `docs` — the oracle.
fn by_scan(docs: &[Document], query: &Document) -> Vec<i64> {
    let filter = kimmy_query::filter::parse(query).unwrap();
    docs.iter()
        .filter(|d| kimmy_query::filter::matches(&filter, d).unwrap())
        .map(|d| d.get_i64("_id").unwrap())
        .collect()
}

fn load(engine: &Engine, docs: &[Document], fields: Vec<IndexField>) -> CollectionMeta {
    let coll = engine.get_collection("app", "docs").unwrap();
    for d in docs {
        engine.insert(&coll, d.clone()).unwrap();
    }
    engine.create_index("app", "docs", fields, false, Some("idx".into())).unwrap();
    engine.get_collection("app", "docs").unwrap()
}

/// `{$and: [{a: first}, {a: second}]}` and the same clauses swapped.
fn both_orders(first: &Bson, second: &Bson) -> [Document; 2] {
    [
        doc! { "$and": [ { "a": first.clone() }, { "a": second.clone() } ] },
        doc! { "$and": [ { "a": second.clone() }, { "a": first.clone() } ] },
    ]
}

#[test]
fn each_combination_reads_the_range_of_its_tightest_bounds() {
    // `a` is 0..20, each once, so on the scalar index a range's entry count
    // is the number of integers in it. The second collection adds one
    // document holding `[3, 50]`, which makes the index multikey, where only
    // one end of a range is usable, and adds three keys: 3, 50 and the whole
    // array, which sorts above every number and string.
    let scalars: Vec<Document> = (0..20i64).map(|i| doc! { "_id": i, "a": i }).collect();
    let mut arrays = scalars.clone();
    arrays.push(doc! { "_id": 100_i64, "a": [3, 50] });

    // (first clause, second clause, the clause alone that reads the same
    // range, entries it reads without and with the multikey flag).
    let d = |d: Document| Bson::Document(d);
    let cases: Vec<(Bson, Bson, Document, usize, usize)> = vec![
        // The higher lower bound: 5..=19, with 5 read and rechecked away;
        // and 50 and the array.
        (d(doc! {"$gt": 5}), d(doc! {"$gt": 0}), doc! {"a": {"$gt": 5}}, 15, 17),
        // The lower upper bound: 0..=5, and the array's 3.
        (d(doc! {"$lt": 5}), d(doc! {"$lte": 12}), doc! {"a": {"$lt": 5}}, 6, 7),
        // Both ends across clauses: [3, 9] where the multikey flag allows it,
        // the lower end alone where it does not (3..=19 and all three keys).
        (
            d(doc! {"$gt": 0, "$lt": 9}),
            d(doc! {"$gte": 3}),
            doc! {"a": {"$gte": 3, "$lt": 9}},
            7,
            20,
        ),
        // `$gt` against `$gte` on one value: the same inclusive range.
        (d(doc! {"$gt": 5}), d(doc! {"$gte": 5}), doc! {"a": {"$gt": 5}}, 15, 17),
        // An equality beats any range: one probe.
        (Bson::Int64(7), d(doc! {"$gt": 0}), doc! {"a": 7}, 1, 1),
        // A `$in` beats any range: two probes.
        (d(doc! {"$in": [2, 7]}), d(doc! {"$gt": 0}), doc! {"a": {"$in": [2, 7]}}, 2, 2),
        // A lower bound of a later type wins: strings sort above numbers,
        // and there are none; only the whole array lies above.
        (d(doc! {"$gt": 5}), d(doc! {"$gt": "a"}), doc! {"a": {"$gt": "a"}}, 0, 1),
        // Nothing can match: on the scalar index the range is empty; on the
        // multikey one the higher lower bound reads 10..=19, 50 and the array.
        (d(doc! {"$gt": 10}), d(doc! {"$lt": 5}), doc! {"a": {"$gt": 10, "$lt": 5}}, 0, 12),
    ];

    for (corpus, multikey) in [(&scalars, false), (&arrays, true)] {
        let (engine, _dir) = engine();
        let coll = load(&engine, corpus, vec![IndexField::ascending("a")]);
        assert_eq!(coll.index("idx").unwrap().multikey, multikey, "premise");
        for (first, second, alone, plain, multi) in &cases {
            let expected = if multikey { *multi } else { *plain };
            let (alone_ids, alone_entries) = indexed(&engine, &coll, alone).unwrap();
            for query in both_orders(first, second) {
                let (ids, entries) = indexed(&engine, &coll, &query).unwrap();
                assert_eq!(
                    entries, expected,
                    "{query} read {entries} index entries, expected {expected} (multikey: \
                     {multikey})"
                );
                assert_eq!(entries, alone_entries, "{query} should read what {alone} reads");
                assert_eq!(ids, by_scan(corpus, &query), "{query} answered differently");
                // Every conjunct holds for each match, so the clause alone
                // answers a superset.
                assert!(ids.iter().all(|id| alone_ids.contains(id)), "{query}");
            }
        }
    }
}

/// Values of every comparable type, for bounds and for documents.
///
/// No `Decimal128`: a filter refuses one as a comparison operand.
fn values() -> Vec<Bson> {
    vec![
        Bson::MinKey,
        Bson::Null,
        Bson::Double(f64::NAN),
        Bson::Double(f64::NEG_INFINITY),
        Bson::Int32(0),
        Bson::Double(4.5),
        Bson::Int32(5),
        Bson::Int64(5),
        Bson::Double(5.0),
        Bson::Int32(10),
        Bson::Double(f64::INFINITY),
        Bson::String(String::new()),
        Bson::String("a".into()),
        Bson::String("m".into()),
        Bson::Symbol("m".into()),
        Bson::Document(doc! {}),
        Bson::Document(doc! {"x": 1}),
        Bson::Array(vec![]),
        Bson::Array(vec![5.into()]),
        Bson::Boolean(true),
        Bson::DateTime(bson::DateTime::from_millis(1_000)),
        Bson::MaxKey,
    ]
}

/// Arrays whose elements satisfy different bounds — the documents an
/// intersection that is too narrow would lose.
fn arrays() -> Vec<Bson> {
    vec![
        Bson::Array(vec![11.into(), 0.into()]),
        Bson::Array(vec![6.into(), "b".into()]),
        Bson::Array(vec!["a".into(), 4.5.into()]),
        Bson::Array(vec![5.into()]),
        Bson::Array(vec![]),
        Bson::Array(vec![1.into(), 2.into()]),
        Bson::Array(vec![Bson::Array(vec![1.into(), 2.into()])]),
        Bson::Array(vec![doc! {"x": 1}.into(), 7.into()]),
        Bson::Array(vec![Bson::Null, true.into()]),
        Bson::Array(vec![Bson::MinKey, Bson::MaxKey]),
    ]
}

/// Every clause a bound pair is drawn from: the four comparisons with every
/// value, plus equalities and `$in`s for the clauses a range competes with.
fn clauses() -> Vec<Bson> {
    let mut out = Vec::new();
    for v in values() {
        for op in ["$gt", "$gte", "$lt", "$lte"] {
            out.push(Bson::Document(doc! { op: v.clone() }));
        }
    }
    out.push(Bson::Int32(5));
    out.push(Bson::String("a".into()));
    out.push(Bson::Document(doc! { "$in": [0, "m"] }));
    out.push(Bson::Document(doc! { "$in": [] }));
    out
}

#[test]
fn every_bound_pair_answers_what_a_scan_answers() {
    // Index shapes: the walk differs for an inverted (descending) field, and
    // for a range behind an equality prefix, which the compound shape puts
    // on `b`.
    let shapes: Vec<(&str, Vec<IndexField>, bool)> = vec![
        ("ascending", vec![IndexField::ascending("a")], false),
        ("descending", vec![IndexField::descending("a")], false),
        ("compound", vec![IndexField::ascending("b"), IndexField::descending("a")], true),
    ];

    let mut used = 0usize;
    let mut matched = 0usize;
    for with_arrays in [false, true] {
        let mut docs = vec![doc! { "_id": 0_i64, "b": 1 }];
        // Arrays only where the index is meant to be multikey; as bounds they
        // are in every pass.
        let mut contents: Vec<Bson> =
            values().into_iter().filter(|v| !matches!(v, Bson::Array(_))).collect();
        if with_arrays {
            contents.extend(arrays());
        }
        for (i, v) in contents.into_iter().enumerate() {
            // Two `b` values, so the compound prefix excludes something.
            docs.push(doc! { "_id": 2 * i as i64 + 1, "b": 1, "a": v.clone() });
            docs.push(doc! { "_id": 2 * i as i64 + 2, "b": 2, "a": v });
        }

        for (shape, fields, prefixed) in &shapes {
            let (engine, _dir) = engine();
            let coll = load(&engine, &docs, fields.clone());
            assert_eq!(
                coll.index("idx").unwrap().multikey,
                with_arrays,
                "premise: the {shape} index is multikey exactly when arrays are present"
            );

            let clauses = clauses();
            for (i, first) in clauses.iter().enumerate() {
                for second in &clauses[i..] {
                    for mut query in both_orders(first, second) {
                        if *prefixed {
                            query.insert("b", 1);
                        }
                        let Some((ids, _)) = indexed(&engine, &coll, &query) else {
                            continue; // the planner scanned, which is always sound
                        };
                        used += 1;
                        let truth = by_scan(&docs, &query);
                        matched += usize::from(!truth.is_empty());
                        assert_eq!(
                            ids, truth,
                            "the indexed answer differs from a scan's for {query} on the {shape} \
                             index (multikey: {with_arrays})"
                        );
                    }
                }
            }
        }
    }
    // Premises: an empty search, or one where nothing matches, satisfies the
    // assertion above.
    assert!(used > 20_000, "premise: the planner chose the index {used} times");
    assert!(matched > 5_000, "premise: {matched} queries matched something");
}
