//! Deciding when to use the approximate index.
//!
//! The HNSW graph is built from a snapshot and does not track later writes, so
//! something has to own the question "is this index still good enough". This
//! module is that decision, kept in one place rather than scattered through
//! the search path.
//!
//! # Why bounded staleness is safe here
//!
//! A stale *secondary* index returns wrong documents. A stale *vector* index
//! does not, because of two properties of the search path:
//!
//! - **Scores are recomputed** from the current stored vector, never from the
//!   graph's distances. An updated document scores by its new vector.
//! - **Missing records are skipped.** A deleted document cannot surface, even
//!   though its node is still in the graph.
//!
//! So the only effect of staleness is that a *recently added* document may not
//! be found yet. That is bounded recall loss on new data, not incorrect data —
//! which is what makes a rebuild interval an acceptable trade rather than a
//! silent correctness hole.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kimmy_core::{CollectionId, Metric};
use kimmy_storage::{CollectionMeta, Engine};
use parking_lot::Mutex;
use tracing::debug;

use crate::error::Result;
use crate::index::HnswIndex;

/// Below this many vectors, an exact scan is not worth displacing.
///
/// **Measured, as of 2026-08-08** — see [Benchmarks](../../../docs/benchmarks.md).
/// The previous value, 2,000, was a guess, and the guess was wrong in an
/// interesting way: there is no crossover. At 384 dimensions the graph answers
/// faster at *every* size measured, from 250 vectors (1.4 ms vs 7.6 ms) to
/// 4,000 (3.1 ms vs 126 ms).
///
/// The reason is that neither path is dominated by arithmetic. An exact scan
/// costs ~31 µs **per vector**, which is far too slow for 384 floats — it is
/// the storage read and record decode. The graph's ~1.4 ms floor is the same
/// cost paid for the ~40 candidates it actually loads. So the exact path is
/// linear in collection size and the graph path is very nearly flat.
///
/// What remains is the build, which is not free and grows faster than linearly
/// (50 ms at 250, 5.4 s at 4,000). Dividing it by the per-query saving gives
/// the number of queries a build repays itself in: **8 at 250 vectors, 12 at
/// 500, 44 at 4,000.**
///
/// 500 is chosen from that: a collection that serves a dozen searches between
/// rebuilds comes out ahead, and one that serves fewer is scanning ≤ 15 ms,
/// which is not a latency worth spending 161 ms of build to improve. Lower
/// would start paying build costs for collections that are barely queried.
const MIN_VECTORS_FOR_INDEX: usize = 500;

/// How long a stale index may keep serving before it is rebuilt.
///
/// **Kept at 30s after measurement**, which is a different statement from
/// having guessed it. A rebuild costs 1.7 s at 2,000 vectors and 5.4 s at
/// 4,000 ([Benchmarks](../../../docs/benchmarks.md)), so on a continuously
/// written collection this window is what stands between the node and spending
/// most of a core on rebuilds: at 4,000 vectors a 30 s window caps that at
/// roughly 18% of one core, while a 5 s window would exceed 100% and never
/// finish.
///
/// The freshness cost is unchanged and bounded — a document written in the last
/// 30 s may not be found yet, and never returns a *wrong* answer, because the
/// graph supplies candidates only and scores are recomputed from stored vectors
/// ([ADR-022](../../../docs/decisions.md)).
///
/// Raising it is the lever for a large write-heavy collection; the measurements
/// say what it buys.
const MAX_STALENESS: Duration = Duration::from_secs(30);

/// What the last decision for a collection was.
enum Decision {
    /// A built graph, ready to serve.
    Index(Arc<HnswIndex>),
    /// Too few vectors to be worth indexing. Cached so the O(n) count that
    /// produced this verdict is not repeated on every query.
    TooSmall,
}

struct Entry {
    decision: Decision,
    /// The generation the decision was made at. A mismatch means writes have
    /// landed since.
    generation: u64,
    decided: Instant,
}

/// Per-collection index cache.
pub struct IndexCache {
    entries: Mutex<HashMap<CollectionId, Entry>>,
    /// Overridable so tests can exercise the threshold on small fixtures.
    min_vectors: usize,
}

impl Default for IndexCache {
    fn default() -> Self {
        Self { entries: Mutex::new(HashMap::new()), min_vectors: MIN_VECTORS_FOR_INDEX }
    }
}

/// Which access path a search should take.
pub enum Access {
    /// Use this graph.
    Approximate(Arc<HnswIndex>),
    /// Scan every vector. Correct, and faster below the size threshold.
    Exact,
}

impl IndexCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// A cache with a non-default size threshold. For tests.
    #[cfg(test)]
    fn with_min_vectors(min_vectors: usize) -> Self {
        Self { min_vectors, ..Self::default() }
    }

    /// Choose an access path, building or rebuilding the index if warranted.
    ///
    /// Never fails the query: if building an index errors — an unsupported
    /// metric, say — it falls back to the exact scan, which is always correct.
    ///
    /// The size count this needs is O(n), so its verdict is cached under the
    /// same generation-and-staleness rule as the graph itself. Otherwise the
    /// check meant to *avoid* a full scan would perform one on every query.
    pub fn access(
        &self,
        engine: &Engine,
        shadow: &CollectionMeta,
        metric: Metric,
        dim: usize,
    ) -> Access {
        // No graph exists for this metric at all, so there is nothing to cache
        // and nothing to count.
        if !HnswIndex::supports(metric) {
            return Access::Exact;
        }

        let generation = engine.vector_generation(shadow.id);
        let mut entries = self.entries.lock();

        if let Some(entry) = entries.get(&shadow.id) {
            let fresh = entry.generation == generation;
            // Serving a stale graph is bounded recall loss on new documents,
            // never wrong data — see the module comment.
            if fresh || entry.decided.elapsed() < MAX_STALENESS {
                return match &entry.decision {
                    Decision::Index(index) => Access::Approximate(Arc::clone(index)),
                    Decision::TooSmall => Access::Exact,
                };
            }
        }

        // Falling back on error keeps the query correct; the alternative is
        // failing a search because an optimisation could not be built.
        let decision = match self.decide(engine, shadow, metric, dim) {
            Ok(decision) => decision,
            Err(e) => {
                debug!(error = %e, "falling back to an exact scan");
                return Access::Exact;
            }
        };

        let access = match &decision {
            Decision::Index(index) => Access::Approximate(Arc::clone(index)),
            Decision::TooSmall => Access::Exact,
        };
        entries.insert(shadow.id, Entry { decision, generation, decided: Instant::now() });
        access
    }

    fn decide(
        &self,
        engine: &Engine,
        shadow: &CollectionMeta,
        metric: Metric,
        dim: usize,
    ) -> Result<Decision> {
        if count_vectors(engine, shadow)? < self.min_vectors {
            return Ok(Decision::TooSmall);
        }
        let index = HnswIndex::build(engine, shadow, metric, dim)?;
        debug!(collection = %shadow.name, vectors = index.len(), "rebuilt vector index");
        Ok(Decision::Index(Arc::new(index)))
    }

    /// Forget a collection's index. Used when its vectors are dropped.
    pub fn invalidate(&self, collection: CollectionId) {
        self.entries.lock().remove(&collection);
    }

    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Count a collection's vectors.
///
/// O(n) — see `access`, which caches its verdict for exactly that reason.
pub fn count_vectors(engine: &Engine, shadow: &CollectionMeta) -> Result<usize> {
    let mut n = 0;
    engine.for_each_vector(shadow, |_| {
        n += 1;
        Ok(true)
    })?;
    Ok(n)
}

#[cfg(test)]
mod tests {
    use kimmy_core::{ChunkConfig, DocId, Hlc, ProviderConfig, VectorConfig, VectorRecord};

    use super::*;

    fn setup(count: usize) -> (Engine, CollectionMeta, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        engine.create_collection("app", "docs").unwrap();
        engine
            .configure_vectors(
                "app",
                "docs",
                VectorConfig {
                    fields: vec!["body".into()],
                    provider: ProviderConfig::Byo,
                    dim: 4,
                    metric: Metric::Cosine,
                    chunk: ChunkConfig::default(),
                },
            )
            .unwrap();
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        for i in 0..count {
            let source = DocId::Int64(i as i64);
            engine
                .put_vectors(
                    &shadow,
                    &source,
                    &[VectorRecord {
                        source: source.clone(),
                        chunk: 0,
                        source_hlc: Hlc::new(1, 0),
                        vector: vec![i as f32, 1.0, 0.0, 0.0],
                        text: "t".into(),
                    }],
                )
                .unwrap();
        }
        (engine, shadow, dir)
    }

    #[test]
    fn a_small_collection_uses_an_exact_scan() {
        // Building a graph over a handful of vectors costs more than scanning.
        let (engine, shadow, _dir) = setup(5);
        let cache = IndexCache::new();
        assert!(matches!(cache.access(&engine, &shadow, Metric::Cosine, 4), Access::Exact));
    }

    #[test]
    fn the_too_small_verdict_is_cached() {
        // The count behind that verdict is O(n). Repeating it per query would
        // make the check that exists to avoid a full scan perform one.
        let (engine, shadow, _dir) = setup(5);
        let cache = IndexCache::new();
        cache.access(&engine, &shadow, Metric::Cosine, 4);
        assert_eq!(cache.len(), 1, "the verdict should be remembered, not recomputed");
    }

    #[test]
    fn an_unsupported_metric_uses_an_exact_scan() {
        // Dot has no approximate index; the query must still work.
        let (engine, shadow, _dir) = setup(5);
        let cache = IndexCache::with_min_vectors(1);
        assert!(matches!(cache.access(&engine, &shadow, Metric::Dot, 4), Access::Exact));
        assert!(cache.is_empty(), "an unsupported metric needs no cache entry");
    }

    #[test]
    fn a_large_collection_builds_and_reuses_one_index() {
        let (engine, shadow, _dir) = setup(60);
        // A lowered threshold keeps the fixture small while still crossing it.
        let cache = IndexCache::with_min_vectors(10);

        let first = cache.access(&engine, &shadow, Metric::Cosine, 4);
        assert!(matches!(first, Access::Approximate(_)));
        assert_eq!(cache.len(), 1);

        let Access::Approximate(a) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("expected the cached index");
        };
        let Access::Approximate(b) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("expected the cached index");
        };
        // Reused rather than rebuilt on every query.
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn writes_bump_the_generation_so_staleness_is_detectable() {
        // Counting vectors would be O(n) per query, and a count cannot see a
        // delete-then-add that leaves the total unchanged.
        let (engine, shadow, _dir) = setup(3);
        let before = engine.vector_generation(shadow.id);

        let source = DocId::Int64(99);
        engine
            .put_vectors(
                &shadow,
                &source,
                &[VectorRecord {
                    source: source.clone(),
                    chunk: 0,
                    source_hlc: Hlc::new(2, 0),
                    vector: vec![1.0, 0.0, 0.0, 0.0],
                    text: "new".into(),
                }],
            )
            .unwrap();
        assert!(engine.vector_generation(shadow.id) > before);

        let after_write = engine.vector_generation(shadow.id);
        engine.delete_vectors(&shadow, &source).unwrap();
        assert!(engine.vector_generation(shadow.id) > after_write, "deletes must count too");
    }

    #[test]
    fn a_stale_index_is_served_until_the_interval_elapses() {
        // Rebuilding on every write would rebuild continuously under load, and
        // each rebuild is O(n log n).
        let (engine, shadow, _dir) = setup(60);
        let cache = IndexCache::with_min_vectors(10);

        let Access::Approximate(first) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("expected an index");
        };

        // A write makes the cached index stale.
        let source = DocId::Int64(999);
        engine
            .put_vectors(
                &shadow,
                &source,
                &[VectorRecord {
                    source: source.clone(),
                    chunk: 0,
                    source_hlc: Hlc::new(2, 0),
                    vector: vec![1.0, 0.0, 0.0, 0.0],
                    text: "new".into(),
                }],
            )
            .unwrap();

        let Access::Approximate(second) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("expected an index");
        };
        assert!(
            Arc::ptr_eq(&first, &second),
            "a just-built index should keep serving rather than rebuilding per write"
        );
    }

    #[test]
    fn invalidating_forgets_the_index() {
        let (engine, shadow, _dir) = setup(60);
        let cache = IndexCache::with_min_vectors(10);
        cache.access(&engine, &shadow, Metric::Cosine, 4);
        assert_eq!(cache.len(), 1);

        cache.invalidate(shadow.id);
        assert!(cache.is_empty());
    }

    #[test]
    fn the_approximate_path_agrees_with_the_exact_one() {
        // The point of the dispatch is that a caller cannot tell which path ran
        // except by speed. If the two disagree on the nearest neighbour, the
        // cache has silently changed what a search means.
        use crate::search::{self, SearchOptions};

        let (engine, shadow, _dir) = setup(60);
        let cache = IndexCache::with_min_vectors(10);
        let query = vec![37.0, 1.0, 0.0, 0.0];
        let options = SearchOptions { k: 5, metric: Metric::Cosine, per_document: 1 };

        let Access::Approximate(index) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("60 vectors over a threshold of 10 should build an index");
        };
        let approximate = index.search(&engine, &shadow, &query, &options, None).unwrap();
        let exact = search::vector_search(&engine, &shadow, &query, &options, None).unwrap();

        assert_eq!(approximate.len(), exact.len());
        assert_eq!(approximate[0].id, exact[0].id, "the paths disagree on the nearest neighbour");
        // Scores come from the stored vector on both paths, never from the
        // graph's own distances — so they must match exactly, not approximately.
        assert_eq!(approximate[0].score, exact[0].score);
    }

    #[test]
    fn counting_vectors_matches_what_was_written() {
        let (engine, shadow, _dir) = setup(7);
        assert_eq!(count_vectors(&engine, &shadow).unwrap(), 7);
    }
}
