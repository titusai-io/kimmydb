//! The write path, and what a secondary index costs it.
//!
//! Written after a wrong number was published. The vector benchmark's fixture
//! setup looked like ~50-65 ms per stored vector, and that got recorded as an
//! observation implying vector ingest around 15-20 documents per second. It was
//! wrong by roughly six times: the figure came from a **debug** test binary,
//! while the benchmarks that produced the search numbers run in release.
//!
//! The lesson is worth more than the number. A timing taken as a by-product of
//! measuring something else inherits whatever the other thing was compiled and
//! configured as — so it is not a measurement, it is an anecdote. Anything
//! quoted as a rate belongs in a harness that states its own conditions.
//!
//! What is measured here:
//!
//! - `insert` — the baseline. Every mutation also appends an oplog entry in the
//!   same transaction ([ADR-008](../../../docs/decisions.md)), so this is the
//!   cost of a document *and* its log record, which is the honest unit.
//! - `insert` with one and two secondary indexes — index maintenance is on the
//!   write path, and its cost is the argument against indexing everything.
//! - `put_vectors` — replace-all per document, so it does more than an insert.
//! - `replace` and `delete` — the other two mutations.

use std::hint::black_box;

use bson::{Document, doc};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kimmy_core::index_meta::IndexField;
use kimmy_core::vector_meta::{ChunkConfig, Metric, ProviderConfig, VectorConfig};
use kimmy_core::{DocId, Hlc, VectorRecord};
use kimmy_storage::{CollectionMeta, Engine};

/// A document of a size a real application might store.
fn document(id: i64) -> Document {
    doc! {
        "_id": id,
        "sku": format!("SKU-{id:08}"),
        "name": "a product with a name of unremarkable length",
        "qty": id % 500,
        "tags": ["alpha", "beta", "gamma"],
        "address": { "city": "Springfield", "zip": "12345" },
    }
}

fn open() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    (engine, dir)
}

fn field(path: &str) -> IndexField {
    IndexField { path: path.to_string(), descending: false }
}

/// Insert cost, and what each additional secondary index adds to it.
fn inserts(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");
    // Each iteration commits durably, so samples are milliseconds and the
    // default 100 would spend most of the run on one benchmark.
    group.sample_size(30);

    for indexes in [0usize, 1, 2] {
        group.bench_with_input(
            BenchmarkId::new("secondary_indexes", indexes),
            &indexes,
            |b, &indexes| {
                // The collection is rebuilt per batch rather than per iteration:
                // `_id` must stay unique, and a counter that outlives the engine
                // is simpler than resetting one.
                let (engine, _dir) = open();
                engine.create_collection("bench", "docs").unwrap();
                if indexes >= 1 {
                    engine.create_index("bench", "docs", vec![field("sku")], false, None).unwrap();
                }
                if indexes >= 2 {
                    engine.create_index("bench", "docs", vec![field("qty")], false, None).unwrap();
                }
                let coll = engine.get_collection("bench", "docs").unwrap();

                let mut next = 0i64;
                b.iter(|| {
                    next += 1;
                    let id = engine.insert(&coll, document(next)).unwrap();
                    black_box(id)
                });
            },
        );
    }
    group.finish();
}

/// Replace and delete, against a collection that already holds documents.
fn other_mutations(c: &mut Criterion) {
    let mut group = c.benchmark_group("mutation");
    group.sample_size(30);

    group.bench_function("replace", |b| {
        let (engine, _dir) = open();
        let coll = engine.create_collection("bench", "docs").unwrap();
        engine.insert(&coll, document(1)).unwrap();
        let mut n = 0i64;
        b.iter(|| {
            n += 1;
            // Same id every time: this measures overwriting, not growth.
            let mut d = document(1);
            d.insert("revision", n);
            black_box(engine.replace(&coll, &DocId::Int64(1), d, false).unwrap())
        });
    });

    group.bench_function("delete_then_insert", |b| {
        // Delete needs something to delete, so the pair is measured together
        // and the insert cost above is what to subtract.
        let (engine, _dir) = open();
        let coll = engine.create_collection("bench", "docs").unwrap();
        let mut n = 0i64;
        b.iter(|| {
            n += 1;
            engine.insert(&coll, document(n)).unwrap();
            black_box(engine.delete(&coll, &DocId::Int64(n)).unwrap())
        });
    });

    group.finish();
}

/// Storing vectors for one document.
///
/// This is the number that was previously quoted wrong. It is replace-all per
/// document, so it removes any existing chunks before writing — more work than
/// an insert, and the comparison against `insert/secondary_indexes/0` is the
/// point of measuring it here rather than alone.
fn vector_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_vectors");
    group.sample_size(30);

    let vector: Vec<f32> = (0..384).map(|i| i as f32 / 384.0).collect();

    for chunks in [1usize, 4] {
        group.bench_with_input(BenchmarkId::new("chunks", chunks), &chunks, |b, &chunks| {
            let (engine, _dir) = open();
            engine.create_collection("bench", "docs").unwrap();
            engine
                .configure_vectors(
                    "bench",
                    "docs",
                    VectorConfig {
                        fields: vec!["name".into()],
                        provider: ProviderConfig::Byo,
                        dim: 384,
                        metric: Metric::Cosine,
                        chunk: ChunkConfig::default(),
                    },
                )
                .unwrap();
            let shadow: CollectionMeta =
                engine.vector_collection("bench", "docs").unwrap().unwrap();

            let mut next = 0i64;
            b.iter(|| {
                next += 1;
                let source = DocId::Int64(next);
                let records: Vec<VectorRecord> = (0..chunks)
                    .map(|chunk| VectorRecord {
                        source: source.clone(),
                        chunk: chunk as u32,
                        source_hlc: Hlc::new(1, 0),
                        vector: vector.clone(),
                        text: "a chunk of document text".into(),
                    })
                    .collect();
                // `put_vectors` returns nothing, so the records are what gets
                // hidden from the optimiser rather than a result value.
                engine.put_vectors(&shadow, &source, black_box(&records)).unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(benches, inserts, other_mutations, vector_writes);
criterion_main!(benches);
