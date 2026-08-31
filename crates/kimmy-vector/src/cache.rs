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
//!
//! # Two more things this module owns
//!
//! **The build runs off the lock.** A graph takes seconds to build (5.4 s at
//! 4,000 vectors of 384 dimensions — [Benchmarks](../../../docs/benchmarks.md))
//! and used to be built while holding the map every collection's entry lives
//! in, so one collection's rebuild stalled vector search on every other
//! collection for that long. Now the map is locked only to look and to
//! install; the build itself runs under a per-collection lock, on a thread
//! the async runtime has been told about ([`kimmy_storage::blocking`]), and a
//! second caller arriving mid-build either serves the graph that already
//! exists — bounded staleness, as above — or, when there is none, waits for
//! that one build rather than starting another.
//!
//! **Resident graphs live under a budget.** A graph costs roughly
//! `dim × 4 + 5 KB` per chunk and was kept forever, so a node's memory was
//! the sum of every collection ever searched. [`IndexCache::set_max_bytes`]
//! bounds that: when installing a graph would exceed it, the least recently
//! used graphs go first. A single graph larger than the whole budget still
//! loads — a search is never refused over a memory policy — and is warned
//! about once. An evicted collection costs its next search a snapshot reload
//! or, without snapshots, a rebuild; the budget should be sized so that is
//! rare (`docs/operations.md`).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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

/// How many bytes of graphs a cache keeps resident unless told otherwise.
///
/// 512 MiB: twice `storage.cache_bytes`' default. A ceiling rather than an
/// allocation — a node whose searched collections fit in less uses less, as
/// before — so the question is how much a node that *does* fill it should
/// spend before rebuilding graphs on demand. A rebuild is seconds to minutes,
/// against microseconds for a page-cache miss, which is why this is larger
/// than the page cache and not equal to it: an eviction here costs a great
/// deal more than one there. At ~6.5 KB per 384-dimensional chunk it holds
/// about 82,000 chunks; at ~11 KB per 1,536-dimensional chunk, about 48,000.
/// Zero means unbounded, which is the behaviour before the budget existed.
pub const DEFAULT_MAX_BYTES: u64 = 512 * 1024 * 1024;

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
    /// When a search last took this entry. Eviction order under the budget.
    last_used: Instant,
}

impl Entry {
    fn new(decision: Decision, generation: u64, decided: Instant) -> Self {
        Self { decision, generation, decided, last_used: Instant::now() }
    }

    fn access(&self) -> Access {
        match &self.decision {
            Decision::Index(index) => Access::Approximate(Arc::clone(index)),
            Decision::TooSmall => Access::Exact,
        }
    }

    /// What this entry costs against the budget. A verdict costs nothing.
    fn bytes(&self) -> usize {
        match &self.decision {
            Decision::Index(index) => index.approx_bytes(),
            Decision::TooSmall => 0,
        }
    }
}

/// Everything under the one short lock.
#[derive(Default)]
struct Entries {
    map: HashMap<CollectionId, Entry>,
    /// Sum of [`Entry::bytes`] over `map`, kept alongside so `/metrics` and
    /// the budget check read a number rather than walking the map.
    resident: usize,
    /// Collections already warned about outgrowing the whole budget alone, so
    /// a collection that is rebuilt every staleness window warns once.
    warned: HashSet<CollectionId>,
}

/// Per-collection index cache.
pub struct IndexCache {
    entries: Mutex<Entries>,
    /// One lock per collection, held for the length of a build.
    ///
    /// This is what keeps a build off `entries`: the builder holds its
    /// collection's lock and nothing else, so a search on any other
    /// collection takes `entries` for a lookup and carries on. A second
    /// caller for the *same* collection finds the lock taken and either
    /// serves what is already cached or waits for this one build. Entries
    /// here are created on first use and removed on `invalidate`, so the map
    /// is bounded by the number of vector collections.
    builds: Mutex<HashMap<CollectionId, Arc<Mutex<()>>>>,
    /// Overridable so tests can exercise the threshold on small fixtures.
    min_vectors: usize,
    /// Where graphs are persisted across restarts, when anywhere.
    ///
    /// `None` — the default — is the pre-M8 behaviour: in-memory only, a
    /// restart rebuilds lazily. With a directory, every successful build is
    /// saved and a process's first look at a collection tries the snapshot
    /// before paying the O(n log n) build.
    snapshot_dir: Option<std::path::PathBuf>,
    /// Resident-graph budget in bytes; zero is unbounded. Atomic so the
    /// server can set it after the state that owns this cache is built,
    /// without a lock on the read side of every search.
    max_bytes: AtomicU64,
    /// Called with the collection about to be built, before the build. Lets
    /// a test hold a build open to observe what happens around it.
    #[cfg(test)]
    build_hook: Mutex<Option<BuildHook>>,
}

#[cfg(test)]
type BuildHook = Arc<dyn Fn(CollectionId) + Send + Sync>;

impl Default for IndexCache {
    fn default() -> Self {
        Self {
            entries: Mutex::new(Entries::default()),
            builds: Mutex::new(HashMap::new()),
            min_vectors: MIN_VECTORS_FOR_INDEX,
            snapshot_dir: None,
            max_bytes: AtomicU64::new(DEFAULT_MAX_BYTES),
            #[cfg(test)]
            build_hook: Mutex::new(None),
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

    /// Bound the bytes of graphs kept resident; zero lifts the bound.
    ///
    /// Takes effect at the next install: a cache already over a lowered
    /// budget is trimmed when the next graph arrives, not immediately.
    pub fn set_max_bytes(&self, bytes: u64) {
        self.max_bytes.store(bytes, Ordering::Relaxed);
    }

    /// The budget, in bytes; zero is unbounded.
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes.load(Ordering::Relaxed)
    }

    /// Estimated bytes of graphs resident now — the `/metrics` gauge.
    pub fn resident_bytes(&self) -> u64 {
        self.entries.lock().resident as u64
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
    ///
    /// The cache-wide lock is held only to look and to install. The build —
    /// seconds, and O(n log n) — runs between the two under this collection's
    /// own lock, so it delays nothing but a search on this same collection
    /// that has nothing older to serve.
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
        if let Some(access) = self.serve(shadow.id, Serve::Usable(generation)) {
            return access;
        }

        // A build, or a wait for one. Off the async runtime's worker either
        // way: the same reasoning as a storage commit's fsync — seconds spent
        // here on a worker thread are seconds every other task on it waits.
        kimmy_storage::blocking(|| self.build_or_wait(engine, shadow, metric, dim, generation))
    }

    /// The slow path of [`Self::access`]: everything after the lookup missed.
    fn build_or_wait(
        &self,
        engine: &Engine,
        shadow: &CollectionMeta,
        metric: Metric,
        dim: usize,
        generation: u64,
    ) -> Access {
        let build_lock = self.build_lock(shadow.id);
        let _building = match build_lock.try_lock() {
            Some(guard) => guard,
            None => {
                // Someone is building this collection right now. A graph from
                // before the write that made it stale is still a correct
                // answer, so serve that rather than queue behind the build.
                if let Some(access) = self.serve(shadow.id, Serve::Anything) {
                    return access;
                }
                // Nothing to serve: wait for that build, then take its result.
                // If it failed — there is no entry — this caller tries once
                // itself, which is what it would have done unopposed.
                let guard = build_lock.lock();
                if let Some(access) = self.serve(shadow.id, Serve::Usable(generation)) {
                    return access;
                }
                guard
            }
        };

        // Holding the build lock. A build that finished between the lookup
        // and here has installed its result, and this one would be a
        // duplicate.
        if let Some(access) = self.serve(shadow.id, Serve::Usable(generation)) {
            return access;
        }

        // Cloned out in its own statement, so the guard is gone before the
        // hook runs: a hook that parks must not park the lock too.
        #[cfg(test)]
        {
            let hook = self.build_hook.lock().clone();
            if let Some(hook) = hook {
                hook(shadow.id);
            }
        }

        // A process's first look at this collection: try the snapshot before
        // paying the build. Only on a true miss — a cache entry that has gone
        // stale means this process has newer knowledge than any snapshot.
        // Eviction removes the entry, so an evicted collection comes back
        // this way too, which is the cheaper of its two ways back.
        let seen = self.entries.lock().map.contains_key(&shadow.id);
        if !seen && let Some(entry) = self.try_snapshot(engine, shadow, metric, dim, generation) {
            return self.install(shadow, entry);
        }

        // Falling back on error keeps the query correct; the alternative is
        // failing a search because an optimisation could not be built.
        match self.decide(engine, shadow, metric, dim) {
            Ok(decision) => self.install(shadow, Entry::new(decision, generation, Instant::now())),
            Err(e) => {
                debug!(error = %e, "falling back to an exact scan");
                Access::Exact
            }
        }
    }

    /// This collection's build lock, created on first use.
    fn build_lock(&self, collection: CollectionId) -> Arc<Mutex<()>> {
        Arc::clone(self.builds.lock().entry(collection).or_default())
    }

    /// Serve from the cache if an entry qualifies, touching it for eviction
    /// order. Holds the lock for a lookup and nothing longer.
    fn serve(&self, collection: CollectionId, rule: Serve) -> Option<Access> {
        let mut entries = self.entries.lock();
        let entry = entries.map.get_mut(&collection)?;
        let usable = match rule {
            Serve::Anything => true,
            // Serving a stale graph is bounded recall loss on new documents,
            // never wrong data — see the module comment.
            Serve::Usable(generation) => {
                entry.generation == generation || entry.decided.elapsed() < MAX_STALENESS
            }
        };
        if !usable {
            return None;
        }
        entry.last_used = Instant::now();
        Some(entry.access())
    }

    /// Put a decision in the cache, making room under the budget first.
    ///
    /// Least recently used goes first, judged by the last search that took
    /// each graph. The entry being replaced is dropped before the budget is
    /// judged, so a rebuild of a collection is charged its new size rather
    /// than both. A verdict of "too small" costs nothing and evicts nothing.
    ///
    /// A graph larger than the whole budget is installed anyway, once every
    /// other graph has gone: refusing it would turn every search on that
    /// collection into an exact scan for want of memory the budget was only
    /// ever an estimate of, and the operator is warned so the budget can be
    /// raised or the collection reconsidered.
    fn install(&self, shadow: &CollectionMeta, entry: Entry) -> Access {
        let access = entry.access();
        let bytes = entry.bytes();
        let budget = self.max_bytes() as usize;

        let mut entries = self.entries.lock();
        if let Some(old) = entries.map.remove(&shadow.id) {
            entries.resident -= old.bytes();
        }
        if budget > 0 && bytes > 0 {
            while entries.resident + bytes > budget {
                let victim = entries
                    .map
                    .iter()
                    .filter(|(_, e)| e.bytes() > 0)
                    .min_by_key(|(_, e)| e.last_used)
                    .map(|(id, _)| *id);
                let Some(victim) = victim else { break };
                let evicted = entries.map.remove(&victim).expect("chosen from the map");
                entries.resident -= evicted.bytes();
                debug!(
                    collection = victim.0,
                    bytes = evicted.bytes(),
                    for_collection = %shadow.name,
                    "evicted an HNSW graph to stay under vector.index_cache.max_bytes"
                );
            }
            if bytes > budget && entries.warned.insert(shadow.id) {
                tracing::warn!(
                    collection = %shadow.name,
                    bytes,
                    budget,
                    "one collection's HNSW graph is larger than the whole \
                     vector.index_cache.max_bytes budget; it is resident anyway and every \
                     other graph has been evicted for it. Raise the budget, or expect \
                     searches on other vector collections to rebuild their graphs"
                );
            }
        }
        entries.resident += bytes;
        entries.map.insert(shadow.id, entry);
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
        Some(Entry::new(Decision::Index(Arc::new(index)), generation, decided))
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
        debug!(
            collection = %shadow.name,
            vectors = index.len(),
            bytes = index.approx_bytes(),
            "rebuilt vector index"
        );

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
        {
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.map.remove(&collection) {
                entries.resident -= entry.bytes();
            }
            entries.warned.remove(&collection);
        }
        self.builds.lock().remove(&collection);
        if let Some(path) = self.snapshot_path(collection) {
            let _ = std::fs::remove_dir_all(&path);
        }
    }

    pub fn len(&self) -> usize {
        self.entries.lock().map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether a collection has an entry, graph or verdict. For tests.
    #[cfg(test)]
    fn contains(&self, collection: CollectionId) -> bool {
        self.entries.lock().map.contains_key(&collection)
    }

    /// Make a collection's entry look older than the staleness window, so the
    /// next access rebuilds without a test having to wait 30 seconds.
    #[cfg(test)]
    fn age(&self, collection: CollectionId) {
        if let Some(entry) = self.entries.lock().map.get_mut(&collection) {
            entry.decided = Instant::now() - MAX_STALENESS;
        }
    }
}

/// Which cached entries [`IndexCache::serve`] may hand out.
#[derive(Clone, Copy)]
enum Serve {
    /// Fresh at this generation, or stale within the window: the ordinary
    /// rule.
    Usable(u64),
    /// Any entry at all. For a caller that would otherwise wait on a build
    /// already under way — whatever exists is no staler than what it would
    /// have been served a moment before the build began.
    Anything,
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
        let shadow = add_collection(&engine, "docs", count);
        (engine, shadow, dir)
    }

    /// One more vector collection in the same engine, holding `count`
    /// vectors of the same shape `setup` writes.
    fn add_collection(engine: &Engine, name: &str, count: usize) -> CollectionMeta {
        engine.create_collection("app", name).unwrap();
        engine
            .configure_vectors(
                "app",
                name,
                VectorConfig {
                    fields: vec!["body".into()],
                    provider: ProviderConfig::Byo,
                    dim: 4,
                    metric: Metric::Cosine,
                    document_prefix: None,
                    query_prefix: None,
                    chunk: ChunkConfig::default(),
                },
            )
            .unwrap();
        let shadow = engine.vector_collection("app", name).unwrap().unwrap();
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
        shadow
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

    // -----------------------------------------------------------------------
    // Builds run off the lock
    // -----------------------------------------------------------------------

    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;

    /// Long enough that a wait which should not happen fails the test rather
    /// than hanging it; a real wait here is microseconds.
    const PATIENCE: Duration = Duration::from_secs(10);

    /// Parks the first build of `target` until [`Gate::release`], counts every
    /// build of it, and says when one has started.
    struct Gate {
        entered: mpsc::Receiver<()>,
        barrier: Arc<std::sync::Barrier>,
        builds: Arc<AtomicUsize>,
    }

    impl Gate {
        fn install(cache: &IndexCache, target: CollectionId) -> Self {
            let (tx, entered) = mpsc::channel();
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let builds = Arc::new(AtomicUsize::new(0));
            let hook = {
                let barrier = Arc::clone(&barrier);
                let builds = Arc::clone(&builds);
                move |id: CollectionId| {
                    if id != target {
                        return;
                    }
                    // Only the first build parks; a second — the duplicate
                    // the tests exist to rule out — runs through and is
                    // counted, so the assertion fails rather than deadlocks.
                    if builds.fetch_add(1, Ordering::SeqCst) == 0 {
                        tx.send(()).unwrap();
                        barrier.wait();
                    }
                }
            };
            *cache.build_hook.lock() = Some(Arc::new(hook));
            Self { entered, barrier, builds }
        }

        fn wait_for_build_to_start(&self) {
            self.entered.recv_timeout(PATIENCE).expect("the gated build never started");
        }

        fn release(&self) {
            self.barrier.wait();
        }
    }

    /// Run `access` on its own thread and say how long it took to come back,
    /// or that it did not within `PATIENCE`.
    fn timed_access<'s, 'e: 's>(
        scope: &'s std::thread::Scope<'s, 'e>,
        cache: &'e IndexCache,
        engine: &'e Engine,
        shadow: &'e CollectionMeta,
    ) -> mpsc::Receiver<Access> {
        let (tx, rx) = mpsc::channel();
        scope.spawn(move || {
            let _ = tx.send(cache.access(engine, shadow, Metric::Cosine, 4));
        });
        rx
    }

    #[test]
    fn a_slow_build_on_one_collection_does_not_delay_a_search_on_another() {
        // The defect: the build ran under the cache-wide lock, so a
        // multi-second rebuild of one collection stalled vector search on
        // every collection the node serves. With the build off the lock, B
        // is served — including its own build — while A's is parked.
        let (engine, a, _dir) = setup(60);
        let b = add_collection(&engine, "other", 60);
        let cache = IndexCache::with_min_vectors(10);
        let gate = Gate::install(&cache, a.id);

        std::thread::scope(|s| {
            let a_result = timed_access(s, &cache, &engine, &a);
            gate.wait_for_build_to_start();

            let b_result = timed_access(s, &cache, &engine, &b);
            let served = b_result
                .recv_timeout(PATIENCE)
                .expect("a search on B waited behind A's build: the build holds the lock");
            assert!(matches!(served, Access::Approximate(_)), "B should have built its own graph");

            gate.release();
            let served = a_result.recv_timeout(PATIENCE).expect("A's build never finished");
            assert!(matches!(served, Access::Approximate(_)));
        });
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn concurrent_accesses_to_one_collection_share_a_single_build() {
        // Two searches arriving at an unbuilt collection must not each build
        // it: the second waits for the first and takes the same graph. The
        // count is the proof; pointer equality is the corollary.
        let (engine, a, _dir) = setup(60);
        let cache = IndexCache::with_min_vectors(10);
        let gate = Gate::install(&cache, a.id);

        std::thread::scope(|s| {
            let first = timed_access(s, &cache, &engine, &a);
            gate.wait_for_build_to_start();
            let second = timed_access(s, &cache, &engine, &a);
            // Nothing to serve yet, so the second caller must be waiting on
            // the build rather than have returned — or started its own.
            assert!(
                second.recv_timeout(Duration::from_millis(200)).is_err(),
                "a second caller with nothing to serve should wait for the build in progress"
            );
            gate.release();

            let Access::Approximate(x) = first.recv_timeout(PATIENCE).unwrap() else {
                panic!("the builder should get a graph");
            };
            let Access::Approximate(y) = second.recv_timeout(PATIENCE).unwrap() else {
                panic!("the waiter should get the builder's graph");
            };
            assert!(Arc::ptr_eq(&x, &y), "the waiter got a different graph: it built its own");
        });
        assert_eq!(gate.builds.load(Ordering::SeqCst), 1, "exactly one build");
    }

    #[test]
    fn a_caller_arriving_mid_rebuild_is_served_the_previous_graph() {
        // The staleness policy, under concurrency: a graph from before the
        // write is a correct answer with bounded recall loss, so a search
        // that finds a rebuild in progress takes it rather than queueing.
        let (engine, a, _dir) = setup(60);
        let cache = IndexCache::with_min_vectors(10);
        let Access::Approximate(old) = cache.access(&engine, &a, Metric::Cosine, 4) else {
            panic!("expected an index");
        };

        // A write makes it stale, and the clock says the window has passed.
        let source = DocId::Int64(999);
        engine
            .put_vectors(
                &a,
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
        cache.age(a.id);
        let gate = Gate::install(&cache, a.id);

        std::thread::scope(|s| {
            let rebuild = timed_access(s, &cache, &engine, &a);
            gate.wait_for_build_to_start();

            let meanwhile = timed_access(s, &cache, &engine, &a);
            let Access::Approximate(served) = meanwhile
                .recv_timeout(PATIENCE)
                .expect("a caller with a stale graph to serve waited on the rebuild")
            else {
                panic!("expected the stale graph");
            };
            assert!(Arc::ptr_eq(&served, &old), "should be served the graph that exists");
            assert_eq!(served.len(), 60);

            gate.release();
            let Access::Approximate(rebuilt) = rebuild.recv_timeout(PATIENCE).unwrap() else {
                panic!("expected the rebuilt graph");
            };
            assert_eq!(rebuilt.len(), 61, "the rebuild should see the write");
        });
        assert_eq!(gate.builds.load(Ordering::SeqCst), 1);
    }

    // -----------------------------------------------------------------------
    // Resident graphs live under a budget
    // -----------------------------------------------------------------------

    /// The graph for one of these fixtures, and what it says it costs.
    fn graph(cache: &IndexCache, engine: &Engine, shadow: &CollectionMeta) -> Arc<HnswIndex> {
        // Instants are what eviction orders by; a pause keeps two accesses
        // from sharing one on a coarse clock.
        std::thread::sleep(Duration::from_millis(2));
        let Access::Approximate(index) = cache.access(engine, shadow, Metric::Cosine, 4) else {
            panic!("{} should be served a graph", shadow.name);
        };
        index
    }

    #[test]
    fn graphs_are_evicted_least_recently_used_first() {
        let (engine, a, _dir) = setup(60);
        let b = add_collection(&engine, "b", 60);
        let c = add_collection(&engine, "c", 60);
        let cache = IndexCache::with_min_vectors(10);

        let ga = graph(&cache, &engine, &a);
        let bytes = ga.approx_bytes() as u64;
        assert!(bytes > 0, "a graph must report a size to be budgeted");
        // Room for two of these graphs, not three.
        cache.set_max_bytes(bytes * 2 + bytes / 2);

        let gb = graph(&cache, &engine, &b);
        assert_eq!(gb.approx_bytes() as u64, bytes, "same shape, same estimate");
        assert_eq!(cache.resident_bytes(), bytes * 2);

        // Touch A, so B is the one nobody has used for longest.
        assert!(Arc::ptr_eq(&graph(&cache, &engine, &a), &ga));

        let _gc = graph(&cache, &engine, &c);
        assert!(!cache.contains(b.id), "B was least recently used and should have gone");
        assert!(cache.contains(a.id), "A was touched and should stay");
        assert!(cache.contains(c.id));
        assert_eq!(
            cache.resident_bytes(),
            bytes * 2,
            "the total is what is resident, not what was"
        );

        // An evicted collection is not refused: it is rebuilt on its next
        // search, displacing the next-oldest.
        let gb2 = graph(&cache, &engine, &b);
        assert!(!Arc::ptr_eq(&gb, &gb2), "B's graph was dropped, so this is a new one");
        assert!(!cache.contains(a.id), "A was older than C by then");
    }

    #[test]
    fn a_graph_larger_than_the_whole_budget_still_loads() {
        // A search is never refused over a memory policy. The oversized
        // graph is held, and it is the only one held.
        let (engine, a, _dir) = setup(60);
        let b = add_collection(&engine, "b", 60);
        let cache = IndexCache::with_min_vectors(10);
        cache.set_max_bytes(1);

        let ga = graph(&cache, &engine, &a);
        assert_eq!(cache.resident_bytes(), ga.approx_bytes() as u64);

        let gb = graph(&cache, &engine, &b);
        assert!(!cache.contains(a.id), "the next oversized graph evicts the last");
        assert_eq!(cache.resident_bytes(), gb.approx_bytes() as u64);
    }

    #[test]
    fn a_zero_budget_is_unbounded() {
        let (engine, a, _dir) = setup(60);
        let b = add_collection(&engine, "b", 60);
        let c = add_collection(&engine, "c", 60);
        let cache = IndexCache::with_min_vectors(10);
        cache.set_max_bytes(0);

        let bytes = graph(&cache, &engine, &a).approx_bytes() as u64;
        graph(&cache, &engine, &b);
        graph(&cache, &engine, &c);
        assert_eq!(cache.len(), 3, "nothing is evicted without a bound");
        assert_eq!(cache.resident_bytes(), bytes * 3);
    }

    #[test]
    fn a_rebuild_is_charged_its_new_size_not_both() {
        // Replacing a collection's graph frees the old one before the budget
        // is judged, or a collection sized at half the budget could never
        // rebuild without evicting a neighbour.
        let (engine, a, _dir) = setup(60);
        let b = add_collection(&engine, "b", 60);
        let cache = IndexCache::with_min_vectors(10);

        let bytes = graph(&cache, &engine, &a).approx_bytes() as u64;
        cache.set_max_bytes(bytes * 2 + bytes / 2);
        graph(&cache, &engine, &b);

        // Force A to rebuild: a write, and an aged entry.
        let source = DocId::Int64(999);
        engine
            .put_vectors(
                &a,
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
        cache.age(a.id);

        let rebuilt = graph(&cache, &engine, &a);
        assert_eq!(rebuilt.len(), 61);
        assert!(cache.contains(b.id), "B should not have been evicted to rebuild A");
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn the_budget_is_released_by_invalidation_and_untouched_by_verdicts() {
        let (engine, a, _dir) = setup(60);
        let small = add_collection(&engine, "small", 5);
        let cache = IndexCache::with_min_vectors(10);

        let bytes = graph(&cache, &engine, &a).approx_bytes() as u64;
        assert!(matches!(cache.access(&engine, &small, Metric::Cosine, 4), Access::Exact));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.resident_bytes(), bytes, "a too-small verdict holds no graph");

        cache.invalidate(a.id);
        assert_eq!(cache.resident_bytes(), 0);
    }

    #[test]
    fn an_evicted_collection_comes_back_from_its_snapshot() {
        // Eviction removes the entry, so the collection's next search is a
        // true miss — and a true miss tries the snapshot first. The graph
        // that comes back is the 60 the snapshot holds, not a rebuild's 61.
        let (engine, a, dir) = setup(60);
        let b = add_collection(&engine, "b", 60);
        let cache = snapshot_cache(&dir);
        cache.set_max_bytes(1);

        graph(&cache, &engine, &a);
        graph(&cache, &engine, &b);
        assert!(!cache.contains(a.id));

        let source = DocId::Int64(999);
        engine
            .put_vectors(
                &a,
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
        assert_eq!(graph(&cache, &engine, &a).len(), 60, "loaded from the snapshot, not rebuilt");
    }
}
