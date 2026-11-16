//! The superset invariant of the path resolver rule (q7s3 recordings), over a real engine: an
//! index never offers fewer candidates than the scan's answer.
//!
//! For every document of the recorded fixtures AD, POS, SD, EP and PE, the clash documents CL
//! (which a local write refuses, so they arrive replicated and are filed unkeyed), and every
//! predicate over an index on `a.c`, `a.b`, `a.0`, `a.1`, `a.0.c`, `a.1.c`, `a.0.0` and
//! `a.99999999999`, the answer from the index (when the planner chooses it) is the answer of a
//! collection scan, on an index built over the documents and on one the documents were written
//! to. The predicates that a null satisfies are among them: on a path with a digit position
//! the planner must scan.

use bson::{Bson, Document, bson, doc};
use kimmy_core::{DocId, Epoch, Hlc, NodeId, OpKind, OplogEntry, Stamp};
use kimmy_storage::{CandidateOrder, CollectionMeta, Engine, IndexField, IndexScan};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    e.create_collection("app", "docs").unwrap();
    (e, dir)
}

/// One value of `a` per fixture document of the recordings; `None` is a missing `a`.
fn values() -> Vec<Option<Bson>> {
    let v = Some::<Bson>;
    vec![
        // AD (f:1-40)
        v(bson!([{ "b": null }, { "c": 1 }])),
        v(bson!([{ "c": 5 }, { "d": 1 }])),
        v(bson!([1, 2])),
        v(bson!([])),
        v(bson!([{ "c": null }])),
        v(bson!([{ "c": 1 }, { "c": null }])),
        v(bson!([{ "c": [] }])),
        v(bson!({ "c": 1 })),
        v(bson!([{ "c": 3 }, { "c": 1 }])),
        None,
        v(bson!([{ "c": 2 }, { "d": 0 }])),
        v(bson!([{ "d": 7 }])),
        v(bson!([{ "c": [5, 1] }, { "c": 4 }])),
        v(bson!([null, { "c": 9 }])),
        v(bson!([[{ "c": 1 }]])),
        v(bson!({ "c": null })),
        // POS (f:41-131)
        v(bson!([5, 6, 7])),
        v(bson!([[1, 2], [3, 4]])),
        v(bson!([{ "x": 1 }, { "x": 2 }])),
        v(bson!({ "0": "x", "1": "y", "01": "z", "00": "w" })),
        v(bson!({ "b": [5, 6] })),
        v(bson!([[5]])),
        v(bson!({ "0": [1, 2] })),
        v(bson!("str")),
        // SD (g:93-104)
        v(bson!([5, { "c": 9 }])),
        v(bson!([[1], { "c": 9 }])),
        v(bson!(["x", { "c": 9 }])),
        v(bson!([{ "c": 1 }, [2]])),
        v(bson!([{ "d": 1 }, 7])),
        v(bson!([null, { "c": 5 }])),
        // EP (g:139-155)
        v(bson!([[1, 2]])),
        v(bson!([[[3]]])),
        v(bson!([[{ "b": 2 }]])),
        v(bson!([{ "b": [1, 2] }])),
        v(bson!([[1], [2, 5]])),
        v(bson!([5])),
        v(bson!([[[1, 2]], [3]])),
        // PE (j:151-174), the ones that key
        v(bson!([{ "1": 5 }])),
        v(bson!([5, 6])),
        v(bson!([{ "99999999999": 1 }])),
        v(bson!([{ "b": 1 }])),
    ]
}

/// CL 1-3 and PE 2: a position inside the array beside a field of that name. A local write is
/// refused, so each arrives replicated.
fn clashes() -> Vec<Bson> {
    vec![
        bson!([{ "0": 9 }]),
        bson!([{ "b": 1 }, { "0": 2 }]),
        bson!([[5], { "0": 9 }]),
        bson!([{ "1": 5 }, { "b": 1 }]),
    ]
}

fn corpus() -> Vec<Document> {
    values()
        .into_iter()
        .enumerate()
        .map(|(i, a)| match a {
            Some(a) => doc! { "_id": i as i64, "a": a },
            None => doc! { "_id": i as i64 },
        })
        .collect()
}

fn clash_docs(first_id: i64) -> Vec<Document> {
    clashes()
        .into_iter()
        .enumerate()
        .map(|(i, a)| doc! { "_id": first_id + i as i64, "a": a })
        .collect()
}

fn remote(coll: &CollectionMeta, doc: &Document, wall: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall, 0), NodeId::generate(), Epoch::ZERO),
        kind: OpKind::Insert,
        collection: coll.id,
        doc_id: Some(DocId::try_from_bson(doc.get("_id").unwrap()).unwrap()),
        body: Some(bson::serialize_to_vec(doc).unwrap()),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}

/// The ids a plan answers from the index, `None` when the planner chose a scan.
fn indexed(engine: &Engine, coll: &CollectionMeta, query: &Document) -> Option<Vec<i64>> {
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
    ids.dedup();
    Some(ids)
}

fn by_scan(docs: &[Document], query: &Document) -> Vec<i64> {
    let filter = kimmy_query::filter::parse(query).unwrap();
    docs.iter()
        .filter(|d| kimmy_query::filter::matches(&filter, d).unwrap())
        .map(|d| d.get_i64("_id").unwrap())
        .collect()
}

/// The predicates over `path`: equalities, a `$in`, and every bound, with the null forms.
fn queries(path: &str) -> Vec<Document> {
    let q = |v: Bson| doc! { path: v };
    let op = |op: &str, v: Bson| doc! { path: { op: v } };
    vec![
        q(Bson::Int32(5)),
        q(Bson::Int32(1)),
        q(Bson::String("x".into())),
        q(Bson::Null),
        q(Bson::Array(vec![])),
        q(bson!([1, 2])),
        op("$eq", Bson::Null),
        op("$in", bson!([null])),
        op("$in", bson!([null, 1])),
        op("$in", bson!([1, 5])),
        op("$gte", Bson::Null),
        op("$lte", Bson::Null),
        op("$gt", Bson::Null),
        op("$lt", Bson::Null),
        op("$gte", Bson::Int32(1)),
        op("$gt", Bson::Int32(1)),
        op("$lt", Bson::Int32(6)),
        op("$lte", Bson::Int32(5)),
        op("$gte", Bson::MinKey),
        op("$lte", Bson::MaxKey),
        doc! { path: { "$gte": 1, "$lte": 5 } },
        doc! { path: { "$gte": 4, "$lte": Bson::Null } },
        doc! { path: { "$exists": true } },
        doc! { path: { "$exists": false } },
        doc! { path: { "$type": "null" } },
        doc! { path: { "$ne": Bson::Null } },
        doc! { path: { "$size": 0 } },
    ]
}

const PATHS: [&str; 9] =
    ["a.c", "a.b", "a.0", "a.1", "a.0.c", "a.1.c", "a.0.0", "a.99999999999", "a.00"];

fn check(label: &str, engine: &Engine, coll: &CollectionMeta, docs: &[Document]) {
    check_paths(label, &PATHS, engine, coll, docs);
}

fn check_paths(
    label: &str,
    paths: &[&str],
    engine: &Engine,
    coll: &CollectionMeta,
    docs: &[Document],
) {
    let mut planned = 0;
    for path in paths.iter().copied() {
        for query in queries(path) {
            let want = by_scan(docs, &query);
            if let Some(got) = indexed(engine, coll, &query) {
                planned += 1;
                assert_eq!(got, want, "{label}: the index answered {query} otherwise than a scan");
            }
        }
    }
    assert!(planned > 8, "{label}: premise: the planner used the index: {planned}");
}

fn one_index(path: &str) -> Vec<IndexField> {
    vec![IndexField::ascending(path)]
}

/// An index built over the documents, then clash documents arriving replicated.
#[test]
fn an_index_built_over_the_documents_answers_as_a_scan_does() {
    for path in PATHS {
        let (engine, _dir) = engine();
        let coll = engine.get_collection("app", "docs").unwrap();
        let mut docs = corpus();
        for d in &docs {
            engine.insert(&coll, d.clone()).unwrap();
        }
        engine.create_index("app", "docs", one_index(path), false, Some("idx".into())).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();
        for (i, d) in clash_docs(1000).iter().enumerate() {
            assert!(engine.apply_remote(&coll, &remote(&coll, d, now_ms() + i as u64)).unwrap());
            docs.push(d.clone());
        }
        // Only the one index exists, so every query on another path is a scan: `check` asks
        // the planner and compares what it chose.
        let label = format!("an index on {path}");
        check(&label, &engine, &coll, &docs);
    }
}

/// The same documents written to an index that already exists.
#[test]
fn an_index_the_documents_were_written_to_answers_as_a_scan_does() {
    for path in PATHS {
        let (engine, _dir) = engine();
        engine.create_index("app", "docs", one_index(path), false, Some("idx".into())).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();
        let mut docs = corpus();
        for d in &docs {
            engine.insert(&coll, d.clone()).unwrap();
        }
        let coll = engine.get_collection("app", "docs").unwrap();
        for (i, d) in clash_docs(1000).iter().enumerate() {
            assert!(engine.apply_remote(&coll, &remote(&coll, d, now_ms() + i as u64)).unwrap());
            docs.push(d.clone());
        }
        let label = format!("documents written to an index on {path}");
        check(&label, &engine, &coll, &docs);
    }
}

fn load_with_clashes(engine: &Engine, docs: &[Document]) {
    let coll = engine.get_collection("app", "docs").unwrap();
    for d in docs.iter() {
        engine.insert(&coll, d.clone()).unwrap();
    }
}

fn add_clashes(engine: &Engine, docs: &mut Vec<Document>, extra: Option<(&str, i64)>) {
    let coll = engine.get_collection("app", "docs").unwrap();
    for (i, mut d) in clash_docs(1000).into_iter().enumerate() {
        if let Some((name, value)) = extra {
            d.insert(name, value);
        }
        assert!(engine.apply_remote(&coll, &remote(&coll, &d, now_ms() + i as u64)).unwrap());
        docs.push(d);
    }
}

/// A compound index whose first field has a position: an equality with null on it ends the
/// equality prefix, so the second field's equality is not used either, and no answer is short.
#[test]
fn a_compound_index_with_a_positioned_field_answers_as_a_scan_does() {
    let (engine, _dir) = engine();
    let mut docs: Vec<Document> = corpus()
        .into_iter()
        .enumerate()
        .map(|(i, mut d)| {
            d.insert("b", (i % 3) as i64);
            d
        })
        .collect();
    load_with_clashes(&engine, &docs);
    let fields = vec![IndexField::ascending("a.0"), IndexField::ascending("b")];
    engine.create_index("app", "docs", fields, false, Some("idx".into())).unwrap();
    add_clashes(&engine, &mut docs, Some(("b", 1)));
    let coll = engine.get_collection("app", "docs").unwrap();
    let mut planned = 0;
    for first in [
        Bson::Null,
        Bson::Int32(5),
        Bson::Int32(1),
        bson!({ "$in": [null, 1] }),
        bson!({ "$in": [1, 5] }),
        bson!({ "$gte": null }),
        bson!({ "$lte": null }),
        bson!({ "$gte": 1, "$lte": 5 }),
    ] {
        for b in 0..3i64 {
            let query = doc! { "a.0": first.clone(), "b": b };
            let want = by_scan(&docs, &query);
            if let Some(got) = indexed(&engine, &coll, &query) {
                planned += 1;
                assert_eq!(got, want, "the compound index answered {query} otherwise than a scan");
            } else {
                assert!(
                    matches!(first, Bson::Null)
                        || first == bson!({ "$in": [null, 1] })
                        || first == bson!({ "$gte": null })
                        || first == bson!({ "$lte": null }),
                    "{query} was not planned"
                );
            }
        }
    }
    assert!(planned > 8, "premise: the index was used: {planned}");
}

/// A descending field, where a null bound is dropped and the other end is used: the range is
/// the value-space bound on the other side of the key space.
#[test]
fn a_descending_positioned_field_answers_as_a_scan_does() {
    for path in ["a.0", "a.1", "a.0.c", "a.c"] {
        let (engine, _dir) = engine();
        let mut docs = corpus();
        load_with_clashes(&engine, &docs);
        let fields = vec![IndexField::descending(path)];
        engine.create_index("app", "docs", fields, false, Some("idx".into())).unwrap();
        add_clashes(&engine, &mut docs, None);
        let coll = engine.get_collection("app", "docs").unwrap();
        check_paths(&format!("a descending index on {path}"), &[path], &engine, &coll, &docs);
    }
}

/// A partial index, built over the documents and written to: the planner chooses it only for
/// a query it contains, and the answer is the scan's, on a path with a position and on one
/// without.
#[test]
fn a_partial_index_answers_as_a_scan_does() {
    for path in ["a.c", "a.0", "a.1"] {
        for built_first in [true, false] {
            let (engine, _dir) = engine();
            let mut docs = corpus();
            let make = |engine: &Engine| {
                engine
                    .create_index_with(
                        "app",
                        "docs",
                        one_index(path),
                        false,
                        Default::default(),
                        Some("idx".into()),
                        None,
                        Some(doc! { path: { "$exists": true } }),
                    )
                    .unwrap();
            };
            if built_first {
                load_with_clashes(&engine, &docs);
                make(&engine);
            } else {
                make(&engine);
                load_with_clashes(&engine, &docs);
            }
            add_clashes(&engine, &mut docs, None);
            let coll = engine.get_collection("app", "docs").unwrap();
            let label = format!("a partial index on {path}, built first: {built_first}");
            let mut planned = 0;
            for query in queries(path) {
                let want = by_scan(&docs, &query);
                if let Some(got) = indexed(&engine, &coll, &query) {
                    planned += 1;
                    assert_eq!(got, want, "{label}: {query}");
                }
            }
            assert!(planned > 3, "{label}: premise: the partial index was chosen: {planned}");
        }
    }
}

/// A digit with a leading zero is a field name for a read (q7s3f 48-82): over `a: [5]` the path
/// `a.00` reaches nothing, over `a: [{"00": 5}]` and `a: {"00": 5}` it reaches the field. The scan
/// and an index on that path answer alike, and by the read rule; the parent of the read-position
/// switch answered `a: [5]` (the write rule) and not `a: [{"00": 5}]`.
#[test]
fn a_zero_prefixed_digit_is_a_field_name_in_a_scan_and_in_an_index() {
    let docs = vec![
        doc! { "_id": 1_i64, "a": [5] },
        doc! { "_id": 2_i64, "a": [{ "00": 5 }] },
        doc! { "_id": 3_i64, "a": { "00": 5 } },
        doc! { "_id": 4_i64, "a": [{ "x": 5 }] },
    ];
    let query = doc! { "a.00": 5 };
    assert_eq!(by_scan(&docs, &query), vec![2, 3], "the scan reads `00` as a field name");
    let (engine, _dir) = engine();
    engine.create_index("app", "docs", one_index("a.00"), false, Some("idx".into())).unwrap();
    let coll = engine.get_collection("app", "docs").unwrap();
    for d in &docs {
        engine.insert(&coll, d.clone()).unwrap();
    }
    let coll = engine.get_collection("app", "docs").unwrap();
    if let Some(got) = indexed(&engine, &coll, &query) {
        assert_eq!(got, vec![2, 3], "the index answers as the scan does");
    }
}
