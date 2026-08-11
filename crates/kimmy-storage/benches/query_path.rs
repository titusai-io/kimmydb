//! Index-backed lookup versus a collection scan.
//!
//! The planner's entire premise is that using an index beats scanning, and
//! nothing had measured it. That premise got more interesting once the write
//! path was measured: maintaining a secondary index turns out to cost nothing
//! on a write ([Benchmarks](../../../docs/benchmarks.md)), so if the read side
//! wins too, an index is close to free and the only question left is disk.
//!
//! The two paths are measured the way `kimmy_api::exec` runs them, which is
//! what makes the comparison meaningful rather than synthetic:
//!
//! - **indexed** — `index_candidates` for the key range, then
//!   `get_by_encoded_key` per candidate, then the filter re-checked on each
//!   document (the index narrows, it does not decide).
//! - **scan** — `for_each_doc` over the collection, filtering every document.
//!
//! Selectivity is the variable that matters. An index that narrows 10,000
//! documents to one should win enormously; an index that narrows them to 9,000
//! should lose, because it pays a random read per candidate on top of the scan
//! it effectively still does. Both ends are measured, because "use an index" is
//! only good advice on one of them.

use std::hint::black_box;

use bson::doc;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kimmy_core::index_meta::IndexField;
use kimmy_query::{Filter, filter, plan};
use kimmy_storage::{CollectionMeta, Engine};

/// Documents in the collection under test.
const DOCS: i64 = 10_000;

fn open() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    (engine, dir)
}

/// A collection of `DOCS` documents with a `bucket` field of known selectivity.
///
/// `bucket` takes `buckets` distinct values spread evenly, so a query for one
/// value matches `DOCS / buckets` documents. That makes selectivity a dial
/// rather than something to be inferred from the data.
fn fixture(buckets: i64, indexed: bool) -> (Engine, CollectionMeta, tempfile::TempDir) {
    let (engine, dir) = open();
    engine.create_collection("bench", "docs").unwrap();
    if indexed {
        engine
            .create_index(
                "bench",
                "docs",
                vec![IndexField { path: "bucket".into(), descending: false }],
                false,
                None,
            )
            .unwrap();
    }
    let coll = engine.get_collection("bench", "docs").unwrap();

    for i in 0..DOCS {
        engine
            .insert(
                &coll,
                doc! {
                    "_id": i,
                    "bucket": i % buckets,
                    "name": "a product with a name of unremarkable length",
                    "qty": i % 97,
                },
            )
            .unwrap();
    }
    let coll = engine.get_collection("bench", "docs").unwrap();
    (engine, coll, dir)
}

/// Run a filter the way `exec` does, through the index.
fn indexed_find(engine: &Engine, coll: &CollectionMeta, parsed: &Filter) -> usize {
    let plan = plan::choose(parsed, &coll.indexes).expect("the planner must pick the index");
    let mut candidates = std::collections::BTreeSet::new();
    for (lower, upper) in &plan.ranges {
        candidates.extend(engine.index_candidates(coll, plan.index_id, lower, upper).unwrap());
    }
    let mut matched = 0;
    for key in candidates {
        if let Some(doc) = engine.get_by_encoded_key(coll, &key).unwrap()
            && filter::matches(parsed, &doc)
        {
            matched += 1;
        }
    }
    matched
}

/// Run the same filter as a collection scan.
fn scanned_find(engine: &Engine, coll: &CollectionMeta, parsed: &Filter) -> usize {
    let mut matched = 0;
    engine
        .for_each_doc(coll, |_id, doc| {
            if filter::matches(parsed, &doc) {
                matched += 1;
            }
            Ok(true)
        })
        .unwrap();
    matched
}

fn find_paths(c: &mut Criterion) {
    let mut group = c.benchmark_group("find_10k");
    group.sample_size(20);

    // buckets -> how many of the 10,000 documents one query matches.
    for &buckets in &[10_000i64, 100, 10, 2] {
        let matching = DOCS / buckets;

        let (engine, coll, _dir) = fixture(buckets, true);
        // Parsed once, outside the measured loop: parsing is a per-request cost
        // both paths pay identically, so including it would only add noise to
        // the difference this benchmark exists to show.
        let parsed = filter::parse(&doc! { "bucket": 0i64 }).unwrap();

        // Both paths must agree, or the comparison is between two different
        // questions. Checked once here rather than asserted per iteration.
        let via_index = indexed_find(&engine, &coll, &parsed);
        let via_scan = scanned_find(&engine, &coll, &parsed);
        assert_eq!(via_index, via_scan, "the two paths must return the same count");
        assert_eq!(via_index as i64, matching, "selectivity dial is not doing what it claims");

        group.bench_with_input(BenchmarkId::new("indexed", matching), &parsed, |b, parsed| {
            b.iter(|| black_box(indexed_find(&engine, &coll, parsed)));
        });
        group.bench_with_input(BenchmarkId::new("scan", matching), &parsed, |b, parsed| {
            b.iter(|| black_box(scanned_find(&engine, &coll, parsed)));
        });
    }
    group.finish();
}

criterion_group!(benches, find_paths);
criterion_main!(benches);
