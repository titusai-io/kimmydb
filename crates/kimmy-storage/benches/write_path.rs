//! The write path, and what a secondary index costs it.
//!
//! Written after a figure was published that had never been measured: a
//! `put_vectors` cost inferred from how long a *test* took divided by the writes
//! inside it. That test was a debug binary while benchmarks run in release, and
//! its timing also contained a graph build and ten searches. The estimate was
//! about six times too slow. See the retraction in
//! [Benchmarks](../../../docs/benchmarks.md).
//!
//! The lesson outlives the number. A timing taken as a by-product of measuring
//! something else inherits whatever the other thing was compiled and configured
//! as — it is an anecdote, not a measurement. Anything quoted as a rate belongs
//! in a harness that states its own conditions, which is what this is.
//!
//! What is measured here:
//!
//! - `insert` — the baseline. Every mutation also appends an oplog entry in the
//!   same transaction ([ADR-008](../../../docs/decisions.md)), so this is the
//!   cost of a document *and* its log record, which is the honest unit.
//! - `insert` with one and two secondary indexes — index maintenance is on the
//!   write path, and its cost is the argument against indexing everything.
//! - `insert_with_consumer` — the same insert with a background oplog consumer
//!   recording its position behind it, which is what a daemon runs and a bare
//!   engine does not. This is the daemon-versus-engine write gap reproduced
//!   with no HTTP in it; see [Benchmarks](../../../docs/benchmarks.md).
//! - `bulk` — the same insert, batched into one commit. Reported per document,
//!   so the distance from `insert/secondary_indexes/0` is the per-commit
//!   overhead a batch removes, which is the entire case for the bulk API.
//! - `put_vectors` — replace-all per document, so it does more than an insert.
//! - `replace` and `delete` — the other two mutations.

use std::hint::black_box;

use bson::{Document, doc};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kimmy_core::index_meta::IndexField;
use kimmy_core::vector_meta::{ChunkConfig, Metric, ProviderConfig, VectorConfig};
use kimmy_core::{DocId, Hlc, NodeId, OpKind, OplogEntry, ResumeToken, Stamp, VectorRecord};
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

/// How large [`grown`] grows its store: larger than [`CACHE_BYTES`], so the
/// tree is deeper than the cache is wide.
///
/// Held down by shrinking the cache rather than by growing a gigabyte. What
/// the measurement needs is a store the cache cannot hold, and the cheap way
/// to get one is a small cache — the same trick ADR-174's own cluster test
/// used, for the same reason. A gigabyte fixture took minutes to build, which
/// forced every workload to share one store and made the store grow under the
/// measurement; the drift that produced was larger than the effect being
/// measured.
const GROWN_BYTES: u64 = 64 << 20;

/// redb's page cache for the benchmark store: the smallest a node accepts.
const CACHE_BYTES: usize = 8 << 20;

/// Documents per commit while growing. The bulk path, because growing a
/// gigabyte one commit per document would take the fixture into hours.
const GROW_CHUNK: usize = 1_000;

/// A store grown past the page cache, built once per process and shared.
struct Grown {
    engine: Engine,
    _dir: tempfile::TempDir,
}

/// The shared pre-grown store.
///
/// Every benchmark measuring what the live counts cost runs against this,
/// because the work in question is table opens and B-tree seeks: on a store
/// small enough to sit in redb's page cache every page they touch is already
/// resident, so they cost nothing and the measurement says nothing. That is
/// exactly how the cost stayed invisible in the bench ADR-174 quoted, where a
/// synchronous flush dominated a store of a few thousand documents.
///
/// Built once for the whole process, not once per benchmark and certainly not
/// per sample: the fixture takes minutes, and rebuilding it would measure the
/// rebuilding. Each benchmark writes into a collection of its own inside it, so
/// they share the tree's depth without sharing their documents.
fn grown() -> &'static Grown {
    static GROWN: std::sync::OnceLock<Grown> = std::sync::OnceLock::new();
    GROWN.get_or_init(|| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let engine = Engine::open_with_cache(&path, Some(CACHE_BYTES)).unwrap();
        let coll = engine.create_collection("bench", "grown").unwrap();
        let mut next = 0i64;
        while std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0) < GROWN_BYTES {
            let docs: Vec<Document> = (0..GROW_CHUNK)
                .map(|_| {
                    next += 1;
                    document(next)
                })
                .collect();
            engine.insert_many(&coll, docs).unwrap();
        }
        Grown { engine, _dir: dir }
    })
}

/// The collection named `name` in the shared store.
///
/// Created on first ask and looked up after. Criterion calls a benchmark's
/// setup more than once, so creating unconditionally fails the second time.
fn collection(engine: &Engine, name: &str) -> CollectionMeta {
    engine
        .get_collection("bench", name)
        .unwrap_or_else(|_| engine.create_collection("bench", name).unwrap())
}

/// Entries in a replicated batch: the transport's batch size (ADR-119).
const BATCH: u64 = 1_024;

/// How many times each workload is measured.
///
/// Reported as an ordered series rather than one figure, because a mean hides
/// the shape that matters most here: a first trial slower than the rest is a
/// warm-up or page-cache effect rather than a cost of the code, and that is a
/// live competing explanation for the finding this work came from.
const TRIALS: usize = 5;

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

/// What a background oplog consumer costs the writes it watches.
///
/// This is the daemon reproduced at the engine, and it is the whole of M11
/// task 1. A daemon runs consumers of the oplog that a bare `Engine` does not
/// — the embedding worker, the webhook dispatcher — and each records its
/// position in its own write transaction. redb has a single writer, so the
/// next insert does not merely share a disk with that commit, it queues behind
/// it. No HTTP is involved here: if these two differ by about the cost of one
/// commit, the gap [Benchmarks](../../../docs/benchmarks.md) recorded over a
/// socket is explained without protocol overhead entering into it.
///
/// The consumer records a position for *every* entry, which is what the
/// embedding worker does — including for collections it has nothing to do
/// with.
///
/// An iteration ends when the consumer has caught up, not when the insert
/// returns. That is deliberate and it is the only honest unit: a consumer that
/// falls behind has not made the write cheaper, it has moved the commit out of
/// the window being timed. Sustained throughput has to absorb both commits, and
/// timing only the insert reports a 0.2 ms difference for work that halves what
/// a node can do — which is how this cost stayed invisible through M10.
fn background_consumer(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_with_consumer");
    group.sample_size(30);

    for consumers in [0usize, 1] {
        group.bench_with_input(
            BenchmarkId::new("consumers", consumers),
            &consumers,
            |b, &consumers| {
                let (engine, _dir) = open();
                let engine = std::sync::Arc::new(engine);
                engine.create_collection("bench", "docs").unwrap();
                let coll = engine.get_collection("bench", "docs").unwrap();

                let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                let handled = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
                let consumer = (consumers == 1).then(|| {
                    let engine = std::sync::Arc::clone(&engine);
                    let stop = std::sync::Arc::clone(&stop);
                    let handled = std::sync::Arc::clone(&handled);
                    let mut events = engine.subscribe();
                    std::thread::spawn(move || {
                        while let Ok(entry) = events.blocking_recv() {
                            if stop.load(std::sync::atomic::Ordering::Relaxed) {
                                break;
                            }
                            let token = ResumeToken::new(entry.stamp.hlc, entry.stamp.node);
                            engine.put_consumer_position("bench", token).unwrap();
                            handled.fetch_add(1, std::sync::atomic::Ordering::Release);
                        }
                    })
                });

                let mut next = 0i64;
                b.iter(|| {
                    next += 1;
                    let id = engine.insert(&coll, document(next)).unwrap();
                    if consumers == 1 {
                        while handled.load(std::sync::atomic::Ordering::Acquire) < next as u64 {
                            std::thread::yield_now();
                        }
                    }
                    black_box(id)
                });

                if let Some(consumer) = consumer {
                    stop.store(true, std::sync::atomic::Ordering::Relaxed);
                    // One more entry to wake the thread out of its blocking
                    // receive so the batch does not leak a thread per sample.
                    next += 1;
                    engine.insert(&coll, document(next)).unwrap();
                    consumer.join().unwrap();
                }
            },
        );
    }
    group.finish();
}

/// What batching into one commit is worth.
///
/// The whole justification for a bulk API is per-commit overhead: concurrent
/// writers are flat because redb has a single writer, so the only cost left to
/// remove is the commit itself. `Throughput::Elements` reports per document, so
/// these read directly against `insert/secondary_indexes/0` — the same work,
/// one commit each — and the gap between them *is* the feature.
fn bulk_inserts(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk");
    group.sample_size(30);

    for batch in [1usize, 10, 100, 1000] {
        group.throughput(criterion::Throughput::Elements(batch as u64));
        group.bench_with_input(BenchmarkId::new("batch_size", batch), &batch, |b, &batch| {
            let (engine, _dir) = open();
            let coll = engine.create_collection("bench", "docs").unwrap();

            let mut next = 0i64;
            b.iter(|| {
                // Ids must stay unique across iterations, so the batch walks a
                // counter rather than reusing one range.
                let docs: Vec<Document> = (0..batch)
                    .map(|_| {
                        next += 1;
                        document(next)
                    })
                    .collect();
                black_box(engine.insert_many(&coll, docs).unwrap())
            });
        });
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
///
/// The chunk counts climb because the whole replacement is one commit
/// ([ADR-149](../../../docs/decisions.md)) and the way to see that is a
/// document whose chunks would each have been a commit before: one chunk
/// cannot show a per-chunk cost, four barely can, 32 cannot hide one.
fn vector_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_vectors");
    group.sample_size(30);

    let vector: Vec<f32> = (0..384).map(|i| i as f32 / 384.0).collect();

    for chunks in [1usize, 4, 32] {
        group.bench_with_input(BenchmarkId::new("chunks", chunks), &chunks, |b, &chunks| {
            let (engine, _dir) = open();
            engine.create_collection("bench", "docs").unwrap();
            engine
                .configure_vectors(
                    "bench",
                    "docs",
                    VectorConfig {
                        fields: vec!["name".into()],
                        provider: ProviderConfig::Byo {},
                        dim: 384,
                        metric: Metric::Cosine,
                        document_prefix: None,
                        query_prefix: None,
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

/// What a batch from a peer costs, on a store deep enough for its seeks to be
/// real.
///
/// A replicated batch is one run and one transaction of up to 1,024 entries
/// (ADR-119), and it is the unit a replica's landing time is made of. Measured
/// here rather than inferred from `bulk`, because the two differ in what they
/// do per entry: a bulk insert mints its own stamps, while a batch applies
/// entries that already carry them and decides last-writer-wins per document
/// against what the store already holds.
///
/// On [`Fixture::Grown`] deliberately. The per-entry work this exists to
/// measure is table opens and B-tree seeks, and on a small store every page
/// they touch is already in redb's cache — which is why a bench over a few
/// thousand documents, where a synchronous flush dominates, reported the cost
/// of ADR-174's per-entry shape as noise.
///
/// `Throughput::Elements` reports per entry, so this reads against
/// `bulk/batch_size/1000`.
fn batch_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_new");
    // The fixture alone is minutes, and every sample applies 1,024 documents
    // into it; the default 100 samples would take this benchmark into hours.
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(BATCH));

    let engine = &grown().engine;
    let coll = collection(engine, "batch_new");
    // One origin for the whole run: a fresh node id per entry would add a
    // version-vector row per document and measure that instead.
    let origin = NodeId::generate();
    // Above anything the fixture wrote, so every entry wins its conflict and
    // the batch does the work a real one does rather than being superseded and
    // writing nothing.
    let base = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64 + 1_000_000;
    let mut next = 0u64;

    // Set up once and measured `TRIALS` times in order: the series is the
    // result, not its mean.
    for trial in 1..=TRIALS {
        group.bench_function(format!("trial_{trial}"), |b| {
            b.iter(|| {
                // Ids stay unique across trials, as the bulk benchmark's do.
                let entries: Vec<OplogEntry> = (0..BATCH)
                    .map(|_| {
                        next += 1;
                        entry(origin, base + next, coll.id, next as i64)
                    })
                    .collect();
                black_box(engine.apply_batch(&entries).unwrap())
            });
        });
    }
    group.finish();
}

/// A replicated insert of document `id`, stamped at `wall` from `origin`.
fn entry(origin: NodeId, wall: u64, collection: kimmy_core::CollectionId, id: i64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall, 0), origin),
        kind: OpKind::Insert,
        collection,
        doc_id: Some(DocId::Int64(id)),
        body: Some(bson::serialize_to_vec(&document(id)).unwrap()),
    }
}

/// A replicated batch of **in-place updates**: every entry replaces a document
/// this node already holds, at a higher stamp.
///
/// No live count moves, because nothing changes liveness — so this batch pays
/// only the mark, while `replicated_batch/batch_size/1024` writes all-new
/// documents and pays the count as well. The difference between the two is
/// what the count half costs, and the reason to measure both is that the two
/// halves have different reach: the count moves only on a write that changes
/// liveness, the mark is written by every oplog append whatever the write did.
/// An all-new batch is the worst case and says nothing about the update-heavy
/// workload, which is the common one.
fn batch_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_update");
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(BATCH));

    let engine = &grown().engine;
    let coll = collection(engine, "batch_update");
    let origin = NodeId::generate();
    let base = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64 + 1_000_000;

    // The documents the batch updates, written once at the lowest stamps so
    // that every round after this one out-stamps them and wins.
    let seed: Vec<OplogEntry> =
        (0..BATCH).map(|i| entry(origin, base + i, coll.id, i as i64)).collect();
    engine.apply_batch(&seed).unwrap();

    let mut round = 0u64;
    for trial in 1..=TRIALS {
        group.bench_function(format!("trial_{trial}"), |b| {
            b.iter(|| {
                round += 1;
                let entries: Vec<OplogEntry> = (0..BATCH)
                    .map(|i| entry(origin, base + round * BATCH * 2 + i, coll.id, i as i64))
                    .collect();
                black_box(engine.apply_batch(&entries).unwrap())
            });
        });
    }
    group.finish();
}

/// What the live counts cost the two paths a client drives directly, on the
/// shared pre-grown store.
///
/// A single-document write is the sharpest probe there is of per-transaction
/// work: one entry in one transaction, with no loop to amortise anything over,
/// so everything the transaction does lands on it whole.
fn grown_writes(c: &mut Criterion) {
    {
        let mut group = c.benchmark_group("insert_one");
        group.sample_size(10);
        let engine = &grown().engine;
        let coll = collection(engine, "insert_one");
        let mut n = 0i64;
        for trial in 1..=TRIALS {
            group.bench_function(format!("trial_{trial}"), |b| {
                b.iter(|| {
                    n += 1;
                    black_box(engine.insert(&coll, document(n)).unwrap())
                });
            });
        }
        group.finish();
    }

    // The local bulk path: 1,000 new documents in one transaction, every one
    // of them changing liveness and so paying both halves. Not one of the
    // three workloads the comparison turns on, but the round reported a bulk
    // regression too, so it is measured rather than reasoned about.
    let mut group = c.benchmark_group("bulk_new");
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(1_000));
    let engine = &grown().engine;
    let coll = collection(engine, "bulk_new");
    let mut next = 0i64;
    for trial in 1..=TRIALS {
        group.bench_function(format!("trial_{trial}"), |b| {
            b.iter(|| {
                let docs: Vec<Document> = (0..1_000)
                    .map(|_| {
                        next += 1;
                        document(next)
                    })
                    .collect();
                black_box(engine.insert_many(&coll, docs).unwrap())
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    inserts,
    background_consumer,
    bulk_inserts,
    batch_apply,
    batch_update,
    grown_writes,
    other_mutations,
    vector_writes
);
criterion_main!(benches);
