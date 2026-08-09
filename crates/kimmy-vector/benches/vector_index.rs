//! Measuring the vector-index constants that were guessed.
//!
//! Three numbers in `kimmy_vector::cache` and `kimmy_api::exec` were chosen by
//! reasoning rather than measurement, and the code says so. This benchmark
//! exists to replace two of them with evidence:
//!
//! - `MIN_VECTORS_FOR_INDEX = 2_000` — below this an exact scan is assumed to
//!   beat building and walking a graph. **Where is the real crossover?**
//! - `MAX_STALENESS = 30s` — a bound on how long a stale graph may serve,
//!   chosen against an unmeasured rebuild cost. **What does a rebuild cost?**
//!
//! The comparison that matters is *per query*, between the two paths a caller
//! cannot choose between: the exhaustive scan in [`vector_search`] and the
//! graph walk in [`HnswIndex::search`]. Both are measured against the same
//! stored data, so the difference is the path and nothing else.
//!
//! Build cost is measured separately because it is amortised differently: a
//! rebuild is paid once per staleness window, not once per query, so the two
//! numbers answer different questions and averaging them would answer neither.
//!
//! Fixtures use 384 dimensions — the width of `all-MiniLM-L6-v2`, the most
//! common small embedding model — because the crossover depends on the cost of
//! one distance computation, and that scales with width. A toy width would
//! move the answer in the direction that flatters the graph.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kimmy_core::vector_meta::{ChunkConfig, Metric, ProviderConfig, VectorConfig};
use kimmy_core::{DocId, Hlc, VectorRecord};
use kimmy_storage::{CollectionMeta, Engine};
use kimmy_vector::{HnswIndex, SearchOptions, vector_search};

/// The width of `all-MiniLM-L6-v2`.
const DIM: usize = 384;

/// Collection sizes to sweep, bracketing the guessed threshold of 2,000.
///
/// Stops at 4,000 deliberately. Building a fixture is one storage write per
/// vector, so setup — not measurement — dominates the run, and 8,000 pushed a
/// full sweep past twenty minutes. The exact path is linear in collection size
/// (measured: ~31 us per vector, flat across the range), so a larger point is
/// extrapolation rather than information, and a benchmark too slow to rerun is
/// a benchmark nobody reruns.
const SIZES: &[usize] = &[250, 500, 1_000, 2_000, 4_000];

/// Deterministic pseudo-random vectors.
///
/// A fixed sequence rather than `rand`, so a re-run compares against the same
/// data — a benchmark whose input changes between runs measures the input.
fn pseudo_random(seed: u64, dim: usize) -> Vec<f32> {
    let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    (0..dim)
        .map(|_| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            ((state >> 33) as f32 / (u32::MAX as f32)) - 0.5
        })
        .collect()
}

fn config(dim: usize) -> VectorConfig {
    VectorConfig {
        fields: vec!["body".into()],
        provider: ProviderConfig::Byo,
        dim,
        metric: Metric::Cosine,
        chunk: ChunkConfig::default(),
    }
}

/// A collection holding `count` single-chunk vectors.
///
/// Built once per size and reused across samples: this is setup, not the thing
/// being measured, and it dominates everything else.
fn fixture(count: usize) -> (Engine, CollectionMeta, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    engine.create_collection("bench", "docs").unwrap();
    engine.configure_vectors("bench", "docs", config(DIM)).unwrap();
    let shadow = engine.vector_collection("bench", "docs").unwrap().unwrap();

    for i in 0..count {
        let source = DocId::Int64(i as i64);
        let record = VectorRecord {
            source: source.clone(),
            chunk: 0,
            source_hlc: Hlc::new(1, 0),
            vector: pseudo_random(i as u64 + 1, DIM),
            text: format!("document {i}"),
        };
        engine.put_vectors(&shadow, &source, &[record]).unwrap();
    }
    (engine, shadow, dir)
}

/// Per-query cost of each path, across collection sizes.
///
/// This is the measurement `MIN_VECTORS_FOR_INDEX` should be derived from: the
/// threshold is only correct at the size where the two lines cross.
fn search_paths(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_search");
    // A search is milliseconds at the top sizes; the default 100 samples would
    // make the sweep take minutes without changing the conclusion.
    group.sample_size(30);

    let options = SearchOptions { k: 10, metric: Metric::Cosine, per_document: 1 };
    let query = pseudo_random(999_999, DIM);

    for &size in SIZES {
        let (engine, shadow, _dir) = fixture(size);
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, DIM).unwrap();
        assert_eq!(index.len(), size, "the graph must hold every vector it will be judged on");

        group.bench_with_input(BenchmarkId::new("exact", size), &size, |b, _| {
            b.iter(|| {
                let hits =
                    vector_search(&engine, &shadow, black_box(&query), &options, None).unwrap();
                black_box(hits.len())
            });
        });

        group.bench_with_input(BenchmarkId::new("approximate", size), &size, |b, _| {
            b.iter(|| {
                let hits =
                    index.search(&engine, &shadow, black_box(&query), &options, None).unwrap();
                black_box(hits.len())
            });
        });
    }
    group.finish();
}

/// What one graph construction costs.
///
/// `MAX_STALENESS` trades result freshness against this number, and it was set
/// without it. Measured across the same sizes so the trade can be read at the
/// size a deployment actually has.
fn index_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_build");
    // A build is far slower than a query; ten samples is enough to establish an
    // order of magnitude, which is all this decision needs.
    group.sample_size(10);

    for &size in SIZES {
        let (engine, shadow, _dir) = fixture(size);
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, DIM).unwrap();
                black_box(index.len())
            });
        });
    }
    group.finish();
}

criterion_group!(benches, search_paths, index_build);
criterion_main!(benches);
