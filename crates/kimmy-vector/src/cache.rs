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
    /// Where graphs are persisted across restarts, when anywhere.
    ///
    /// `None` — the default — is the pre-M8 behaviour: in-memory only, a
    /// restart rebuilds lazily. With a directory, every successful build is
    /// saved and a process's first look at a collection tries the snapshot
    /// before paying the O(n log n) build.
    snapshot_dir: Option<std::path::PathBuf>,
}

impl Default for IndexCache {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            min_vectors: MIN_VECTORS_FOR_INDEX,
            snapshot_dir: None,
        }
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

    /// A cache that persists graphs under `dir` and reloads them on the first
    /// access after a restart.
    pub fn with_snapshot_dir(dir: std::path::PathBuf) -> Self {
        Self { snapshot_dir: Some(dir), ..Self::default() }
    }

    /// A cache with a non-default size threshold. For tests.
    #[cfg(test)]
    fn with_min_vectors(min_vectors: usize) -> Self {
        Self { min_vectors, ..Self::default() }
    }

    /// Where one collection's snapshot lives, when snapshots are on at all.
    fn snapshot_path(&self, collection: CollectionId) -> Option<std::path::PathBuf> {
        // Hex of the derived collection id: stable across restarts and nodes
        // because ids are derived from names (ADR-031), and free of anything
        // that needs escaping in a filename.
        self.snapshot_dir.as_ref().map(|dir| dir.join(format!("{:016x}", collection.0)))
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

        // A process's first look at this collection: try the snapshot before
        // paying the build. Only on a true miss — a cache entry that has gone
        // stale means this process has newer knowledge than any snapshot.
        if !entries.contains_key(&shadow.id)
            && let Some(entry) = self.try_snapshot(engine, shadow, metric, dim, generation)
        {
            let access = match &entry.decision {
                Decision::Index(index) => Access::Approximate(Arc::clone(index)),
                Decision::TooSmall => Access::Exact,
            };
            entries.insert(shadow.id, entry);
            return access;
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

    /// Adopt a persisted graph, deciding how much to trust it.
    ///
    /// The generation counter cannot vouch for a snapshot — it is in-memory
    /// and resets with the process — so the check is the vector *count* the
    /// snapshot covered against the count stored now. Equal counts adopt the
    /// snapshot as fresh. Unequal counts still adopt it — serving a stale
    /// graph is bounded recall loss, never wrong data, and it answers this
    /// query instantly — but marked already-stale, so the very next access
    /// rebuilds. The corner this accepts, on purpose: a delete-and-add while
    /// the node was down leaves the count equal, and that snapshot serves as
    /// fresh until the next vector write bumps the generation. Same class of
    /// bound as the 30-second staleness window, with a longer clock.
    ///
    /// Anything unreadable is deleted and `None` returned: a corrupt snapshot
    /// is discarded, not trusted, and the ordinary build path takes over.
    fn try_snapshot(
        &self,
        engine: &Engine,
        shadow: &CollectionMeta,
        metric: Metric,
        dim: usize,
        generation: u64,
    ) -> Option<Entry> {
        let path = self.snapshot_path(shadow.id)?;
        if !path.is_dir() {
            return None;
        }
        let index = match HnswIndex::load(&path, metric, dim) {
            Ok(index) => index,
            Err(e) => {
                tracing::warn!(error = %e, ?path, "discarding an unusable HNSW snapshot");
                let _ = std::fs::remove_dir_all(&path);
                return None;
            }
        };

        let current = count_vectors(engine, shadow).ok()?;
        let (generation, decided) = if current == index.len() {
            (generation, Instant::now())
        } else {
            debug!(
                snapshot = index.len(),
                current, "snapshot is behind the store; serving it once and rebuilding"
            );
            // A generation no live counter returns, plus an already-expired
            // clock: the next access falls through to a rebuild.
            (u64::MAX, Instant::now() - MAX_STALENESS)
        };
        Some(Entry { decision: Decision::Index(Arc::new(index)), generation, decided })
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

        // Every successful build is persisted, so whatever graph a restart
        // finds is the newest one that existed. Failure to save costs the
        // next process a rebuild, not this query an answer.
        if let Some(path) = self.snapshot_path(shadow.id)
            && let Err(e) = index.save(&path)
        {
            tracing::warn!(error = %e, ?path, "could not save the HNSW snapshot");
        }
        Ok(Decision::Index(Arc::new(index)))
    }

    /// Forget a collection's index. Used when its vectors are dropped.
    ///
    /// The snapshot goes with it: the caller is telling us the vectors this
    /// graph described no longer exist, and a snapshot that outlived them
    /// would be adopted by the next restart.
    pub fn invalidate(&self, collection: CollectionId) {
        self.entries.lock().remove(&collection);
        if let Some(path) = self.snapshot_path(collection) {
            let _ = std::fs::remove_dir_all(&path);
        }
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

    // -----------------------------------------------------------------------
    // Snapshots: a restart should not pay the build again
    // -----------------------------------------------------------------------

    /// A cache with snapshots on and a low threshold, plus its snapshot dir.
    fn snapshot_cache(dir: &tempfile::TempDir) -> IndexCache {
        IndexCache { min_vectors: 10, ..IndexCache::with_snapshot_dir(dir.path().join("hnsw")) }
    }

    /// Simulate a restart: reopen the engine from the same directory, which
    /// also resets the in-memory generation counters — the condition that
    /// makes snapshot validation interesting.
    fn reopen(dir: &tempfile::TempDir) -> (Engine, CollectionMeta) {
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        (engine, shadow)
    }

    #[test]
    fn a_snapshot_survives_a_restart_and_is_served_not_rebuilt() {
        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        assert!(matches!(
            cache.access(&engine, &shadow, Metric::Cosine, 4),
            Access::Approximate(_)
        ));
        drop(cache);
        drop(engine);

        // A vector written while the process was "down": the snapshot covers
        // 60, the store holds 61. The first access after restart must serve
        // the 60-vector snapshot — that is the observable proof it was loaded
        // rather than rebuilt, since a rebuild would already hold 61.
        let (engine, shadow) = reopen(&dir);
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
                    text: "written while down".into(),
                }],
            )
            .unwrap();

        let cache = snapshot_cache(&dir);
        let Access::Approximate(first) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("the snapshot should serve");
        };
        assert_eq!(first.len(), 60, "must be the loaded snapshot, not a rebuild");

        // ...and because it was behind the store, the very next access must
        // rebuild to the current 61 rather than serving stale for a window.
        let Access::Approximate(second) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("the rebuild should serve");
        };
        assert_eq!(second.len(), 61, "a behind snapshot is served once, then rebuilt");
    }

    #[test]
    fn a_snapshot_matching_the_store_is_adopted_as_fresh() {
        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        cache.access(&engine, &shadow, Metric::Cosine, 4);
        drop(cache);
        drop(engine);

        let (engine, shadow) = reopen(&dir);
        let cache = snapshot_cache(&dir);
        let Access::Approximate(first) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("the snapshot should serve");
        };
        let Access::Approximate(second) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("the snapshot should keep serving");
        };
        assert!(Arc::ptr_eq(&first, &second), "an up-to-date snapshot must not trigger a rebuild");
    }

    #[test]
    fn a_corrupt_snapshot_is_discarded_not_trusted() {
        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        cache.access(&engine, &shadow, Metric::Cosine, 4);
        drop(cache);

        // Torn write, disk fault, editor accident: the graph bytes are junk.
        let snapshot = dir.path().join("hnsw").join(format!("{:016x}", shadow.id.0));
        std::fs::write(snapshot.join("index.hnsw.graph"), b"not a graph").unwrap();

        let (engine, shadow) = {
            drop(engine);
            reopen(&dir)
        };
        let cache = snapshot_cache(&dir);
        // The query still gets an index — rebuilt — and the junk is gone.
        let Access::Approximate(index) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("a corrupt snapshot must fall back to a rebuild");
        };
        assert_eq!(index.len(), 60);
    }

    #[test]
    fn a_reconfigured_dimension_refuses_the_old_snapshot() {
        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        cache.access(&engine, &shadow, Metric::Cosine, 4);
        drop(cache);
        drop(engine);

        // Asking for a different dimension than the snapshot holds: the load
        // must refuse — distances computed across widths are garbage — and
        // the ordinary paths take over.
        let (engine, shadow) = reopen(&dir);
        let cache = snapshot_cache(&dir);
        let path = cache.snapshot_path(shadow.id).unwrap();
        assert!(crate::index::HnswIndex::load(&path, Metric::Cosine, 8).is_err());
        // Through the cache, the mismatch falls back cleanly too.
        assert!(matches!(
            cache.access(&engine, &shadow, Metric::Cosine, 4),
            Access::Approximate(_)
        ));
    }

    #[test]
    fn invalidating_removes_the_snapshot_too() {
        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        cache.access(&engine, &shadow, Metric::Cosine, 4);
        let path = cache.snapshot_path(shadow.id).unwrap();
        assert!(path.is_dir(), "the build should have been persisted");

        cache.invalidate(shadow.id);
        assert!(!path.exists(), "dropped vectors must take their snapshot with them");
    }

    #[test]
    fn the_loaded_snapshot_agrees_with_the_exact_path() {
        // The reload must preserve what the graph *means*, not merely parse:
        // same nearest neighbour, byte-identical score, exactly as a built
        // index is held to.
        use crate::search::{self, SearchOptions};

        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        cache.access(&engine, &shadow, Metric::Cosine, 4);
        drop(cache);
        drop(engine);

        let (engine, shadow) = reopen(&dir);
        let cache = snapshot_cache(&dir);
        let Access::Approximate(index) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("the snapshot should serve");
        };

        let query = vec![37.0, 1.0, 0.0, 0.0];
        let options = SearchOptions { k: 5, metric: Metric::Cosine, per_document: 1 };
        let approximate = index.search(&engine, &shadow, &query, &options, None).unwrap();
        let exact = search::vector_search(&engine, &shadow, &query, &options, None).unwrap();
        assert_eq!(approximate[0].id, exact[0].id);
        assert_eq!(approximate[0].score, exact[0].score);
    }
}
