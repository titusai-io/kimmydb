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

use kimmy_core::{CollectionId, Hlc, Metric};
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
    /// The shadow collection incarnation this decision was made for.
    ///
    /// The same discriminator the snapshot carries on disk, and needed here
    /// for the same reason one lock further in: an id is derived from a name,
    /// the generation counter is keyed by that id and only ever counts up, so
    /// a collection dropped and recreated arrives at an entry that is older
    /// than it and yet matches on every other test. A recreated collection
    /// with no vectors written yet has not bumped the generation at all, so
    /// without this the previous incarnation's graph serves its searches with
    /// no expiry — the staleness window never opens, because nothing looks
    /// stale.
    created: Hlc,
    /// The metric and width this decision was made for.
    ///
    /// A discriminator of the same kind as `created`, and for the same
    /// reason: neither is a matter of staleness. A stale graph is a graph of
    /// this collection at this shape from before a write — a bounded loss the
    /// window exists to permit. A graph built at another metric scores its
    /// walk with that metric and hands back the wrong ordering; a graph
    /// built at another width cannot take the query at all. Nothing about
    /// either expires, and neither the generation nor the incarnation can
    /// see it: a reconfiguration that keeps the shadow writes no vector and
    /// mints no drop, so the entry matches on every other test. The
    /// reconfiguring route forgets the entry on the member that ran it, and a
    /// change-feed consumer does so on the members it reaches by replication,
    /// but a consumer is a task and can lag or miss entries; recording the
    /// shape here is what makes a search correct however late that is. A
    /// "too small" verdict carries the shape too — a width change alters
    /// which stored vectors count, so a verdict for one shape says nothing
    /// about another.
    metric: Metric,
    dim: usize,
    decided: Instant,
    /// When a search last took this entry. Eviction order under the budget.
    last_used: Instant,
}

impl Entry {
    fn new(
        decision: Decision,
        generation: u64,
        created: Hlc,
        metric: Metric,
        dim: usize,
        decided: Instant,
    ) -> Self {
        Self { decision, generation, created, metric, dim, decided, last_used: Instant::now() }
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
    /// How many times each collection has been forgotten: the fence between
    /// a forget and a build that started before it.
    ///
    /// A build runs for seconds under its own lock and installs when it is
    /// done. Forgetting the collection meanwhile removes the entry, the
    /// build-lock slot and the snapshot — and stops nothing, because the
    /// builder holds its guard and reads a transaction from before the drop.
    /// It would install a graph, charge it to the budget and write a
    /// snapshot for a collection that no longer exists, until eviction or
    /// the next startup sweep. So a build captures this counter before it
    /// looks at the snapshot or the store, every forget increments it, and
    /// [`IndexCache::install`] refuses to install under a counter that has
    /// moved. Both sides act under the one lock this struct is behind, so
    /// there is no instant between the compare and the insert for a forget
    /// to fall into.
    ///
    /// Never removed: a forget must be visible to a build that captured the
    /// counter before it, and clearing the slot would hand the next build a
    /// zero that looks like the one it started at. Bounded by the number of
    /// ids ever passed to a forget — the consumer asks about a dropped
    /// collection's own id as well as its shadow's, so about two per drop —
    /// at sixteen bytes each.
    epochs: HashMap<CollectionId, u64>,
}

impl Entries {
    /// Where a collection's fence stands now. A collection never forgotten
    /// is at zero.
    fn epoch(&self, collection: CollectionId) -> u64 {
        self.epochs.get(&collection).copied().unwrap_or(0)
    }

    /// Move the fence: every build of `collection` that captured the epoch
    /// before this call will decline to install.
    fn fence(&mut self, collection: CollectionId) {
        *self.epochs.entry(collection).or_default() += 1;
    }
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
    /// is bounded by the number of vector collections. Removing a slot does
    /// not stop the build holding it — the guard is the builder's own — which
    /// is what [`Entries::epochs`] is for.
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
    /// The generation each collection's snapshot was written at, for the
    /// snapshots **this process** wrote.
    ///
    /// A snapshot is written by a build and read back by a later miss, and
    /// between the two the collection can be written to. The generation
    /// counter is in-memory, so it cannot vouch for a snapshot a *previous*
    /// process left — that case keeps the count check in
    /// [`Self::try_snapshot`] — but within one process it can, and it is the
    /// only thing that can: an eviction under the budget keeps the snapshot,
    /// a vector write that replaces a document's chunks with the same number
    /// of chunks leaves the count equal, and the count check would then
    /// adopt a graph from before that write as fresh at the generation after
    /// it. Removed with the snapshot in [`Self::invalidate`].
    saved_at: Mutex<HashMap<CollectionId, u64>>,
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
            saved_at: Mutex::new(HashMap::new()),
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

/// What [`IndexCache::sweep_snapshots`] does with a `<id>.build` directory.
///
/// `save` stages a rebuild there and renames it over the snapshot once every
/// file is on disk, so one exists for exactly as long as a build is writing —
/// or forever, if the process died in between. Which of those it is depends
/// on who is asking, and the sweep cannot tell from the directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Staging {
    /// Remove every staging directory. For a caller that knows no build can
    /// be under way — the startup sweep, which runs before the listener binds
    /// — so that anything staged was left by an interrupted build, and only a
    /// future save of that same collection would ever clear it otherwise.
    Remove,
    /// Leave staging directories alone. For a caller on a running node, where
    /// a directory being written this instant is indistinguishable from one
    /// abandoned a month ago; the next startup sweep takes the latter.
    Keep,
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
        if let Some(access) = self.serve(shadow, metric, dim, Serve::Usable(generation)) {
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
                if let Some(access) = self.serve(shadow, metric, dim, Serve::Anything) {
                    return access;
                }
                // Nothing to serve: wait for that build, then take its result.
                // If it failed — there is no entry — this caller tries once
                // itself, which is what it would have done unopposed.
                let guard = build_lock.lock();
                if let Some(access) = self.serve(shadow, metric, dim, Serve::Usable(generation)) {
                    return access;
                }
                guard
            }
        };

        // Holding the build lock. A build that finished between the lookup
        // and here has installed its result, and this one would be a
        // duplicate.
        if let Some(access) = self.serve(shadow, metric, dim, Serve::Usable(generation)) {
            return access;
        }

        // Two things read under one lock before anything slow starts.
        //
        // The epoch is the fence `install` compares against: captured here,
        // ahead of the snapshot load and the build alike, so a forget that
        // lands during either — a drop committing while the graph is being
        // built from a read transaction that still sees every vector — is
        // seen by the install that follows. See [`Entries::epochs`].
        //
        // `seen` decides whether the snapshot is worth a look: a process's
        // first look at this collection tries it before paying the build,
        // but only on a true miss — an entry that has gone stale means this
        // process has newer knowledge than any snapshot. Eviction removes
        // the entry, so an evicted collection comes back this way too, which
        // is the cheaper of its two ways back.
        let (epoch, seen) = {
            let entries = self.entries.lock();
            (entries.epoch(shadow.id), entries.map.contains_key(&shadow.id))
        };

        // Cloned out in its own statement, so the guard is gone before the
        // hook runs: a hook that parks must not park the lock too.
        #[cfg(test)]
        {
            let hook = self.build_hook.lock().clone();
            if let Some(hook) = hook {
                hook(shadow.id);
            }
        }

        if !seen && let Some(entry) = self.try_snapshot(engine, shadow, metric, dim, generation) {
            return self.install(shadow, entry, epoch);
        }

        // Falling back on error keeps the query correct; the alternative is
        // failing a search because an optimisation could not be built.
        match self.decide(engine, shadow, metric, dim, generation) {
            Ok(decision) => self.install(
                shadow,
                Entry::new(decision, generation, shadow.created, metric, dim, Instant::now()),
                epoch,
            ),
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
    ///
    /// The incarnation is checked ahead of the rule and outside it, `Anything`
    /// included: a stale graph is a graph of *this* collection from before a
    /// write, which is a bounded loss and the whole point of the window. A
    /// graph of a **previous collection of the same name** is not that. It
    /// describes chunks that no longer exist, so it is no answer at all, and
    /// nothing about it expires. Declining here is enough to be rid of it —
    /// the caller falls through to a build, and installing replaces the entry.
    ///
    /// The shape — `metric` and `dim`, what the caller's configuration says
    /// now — is checked in the same place and for the same reason. A
    /// reconfigured collection keeps its shadow, so `created` still matches,
    /// and writes nothing, so the generation does too; the entry's own record
    /// of what it was built for is the only thing that can tell. See
    /// [`Entry::metric`].
    fn serve(
        &self,
        shadow: &CollectionMeta,
        metric: Metric,
        dim: usize,
        rule: Serve,
    ) -> Option<Access> {
        let mut entries = self.entries.lock();
        let entry = entries.map.get_mut(&shadow.id)?;
        if entry.created != shadow.created {
            return None;
        }
        if entry.metric != metric || entry.dim != dim {
            return None;
        }
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
    ///
    /// Nothing is installed when the collection has been forgotten since
    /// `epoch` was captured — see [`Entries::epochs`]. The graph is dropped
    /// here, and the snapshot the build may have just written goes with it,
    /// along with the generation recorded for it: `decide` saves and records
    /// after the forget removed both, so a fenced build is the one thing that
    /// can leave either behind. The query is answered with the exact scan,
    /// which is what a search on a collection that has been dropped or
    /// replaced resolves to anyway — the graph and the verdict alike were
    /// decided over vectors that are no longer the collection's. A build of
    /// the collection now standing under the id, if one has started since,
    /// has its own lock and its own epoch, and is fenced by nothing here; the
    /// directory removal can cross its save, and costs a snapshot reload its
    /// rebuild, as `invalidate`'s own removal already could.
    fn install(&self, shadow: &CollectionMeta, entry: Entry, epoch: u64) -> Access {
        let access = entry.access();
        let bytes = entry.bytes();
        let budget = self.max_bytes() as usize;

        let mut entries = self.entries.lock();
        if entries.epoch(shadow.id) != epoch {
            drop(entries);
            drop(entry);
            debug!(
                collection = %shadow.name,
                "the collection was forgotten while its index was being built; discarding \
                 the build"
            );
            self.saved_at.lock().remove(&shadow.id);
            if let Some(path) = self.snapshot_path(shadow.id) {
                // Filesystem work, and this is reached from a request handler
                // on a runtime worker: off the worker, as `invalidate` is.
                kimmy_storage::blocking(|| {
                    let _ = std::fs::remove_dir_all(&path);
                });
            }
            return Access::Exact;
        }
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
    /// A snapshot this process wrote is vouched for by the generation it was
    /// written at (`saved_at`): equal to the generation now, nothing has been
    /// written since and the graph is fresh; otherwise it is behind, however
    /// the count compares. This is the case an eviction under the budget
    /// produces — the entry goes, the snapshot stays, a document's chunks are
    /// replaced with the same number of chunks, and the next miss reloads the
    /// snapshot — and a count check alone would adopt that graph as fresh at
    /// the generation *after* the write it predates.
    ///
    /// A snapshot a previous process wrote has no generation to compare — the
    /// counter is in-memory and resets with the process — so for that one the
    /// check is the vector *count* the snapshot covered against the count
    /// stored now. Equal counts adopt the snapshot as fresh. The corner this
    /// accepts, on purpose: a delete-and-add while the node was down leaves
    /// the count equal, and that snapshot serves as fresh until the next
    /// vector write bumps the generation. Same class of bound as the
    /// 30-second staleness window, with a longer clock.
    ///
    /// A snapshot judged behind is still adopted — serving a stale graph is
    /// bounded recall loss, never wrong data, and it answers this query
    /// instantly — but marked already-stale, so the very next access rebuilds.
    ///
    /// Anything unreadable is deleted and `None` returned: a corrupt snapshot
    /// is discarded, not trusted, and the ordinary build path takes over. That
    /// now includes a snapshot left by a *previous* collection of this name —
    /// [`HnswIndex::load`] refuses one whose `created` is not this shadow's —
    /// which is the case the count check above cannot see, because an orphan
    /// whose count happens to match would otherwise be adopted as **fresh**
    /// and serve until a vector write bumped the generation.
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
        let index = match HnswIndex::load(&path, metric, dim, shadow.created) {
            Ok(index) => index,
            Err(e) => {
                tracing::warn!(error = %e, ?path, "discarding an unusable HNSW snapshot");
                let _ = std::fs::remove_dir_all(&path);
                return None;
            }
        };

        let fresh = match self.saved_at.lock().get(&shadow.id).copied() {
            Some(saved_at) => {
                let fresh = saved_at == generation;
                if !fresh {
                    debug!(
                        saved_at,
                        generation,
                        "snapshot predates a write this process made; serving it once and \
                         rebuilding"
                    );
                }
                fresh
            }
            None => {
                let current = count_vectors(engine, shadow).ok()?;
                let fresh = current == index.len();
                if !fresh {
                    debug!(
                        snapshot = index.len(),
                        current, "snapshot is behind the store; serving it once and rebuilding"
                    );
                }
                fresh
            }
        };
        let (generation, decided) = if fresh {
            (generation, Instant::now())
        } else {
            // A generation no live counter returns, plus an already-expired
            // clock: the next access falls through to a rebuild.
            (u64::MAX, Instant::now() - MAX_STALENESS)
        };
        Some(Entry::new(
            Decision::Index(Arc::new(index)),
            generation,
            shadow.created,
            metric,
            dim,
            decided,
        ))
    }

    /// Build the graph or the "too small" verdict, persisting a graph and
    /// remembering `generation` — the generation the build was decided at —
    /// as the one its snapshot is good for.
    fn decide(
        &self,
        engine: &Engine,
        shadow: &CollectionMeta,
        metric: Metric,
        dim: usize,
        generation: u64,
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
        if let Some(path) = self.snapshot_path(shadow.id) {
            match index.save(&path) {
                // `generation` was read before the build, so a write that
                // landed during it makes this snapshot look behind — the
                // conservative side, and the same one the entry itself is on.
                Ok(()) => {
                    self.saved_at.lock().insert(shadow.id, generation);
                }
                Err(e) => {
                    self.saved_at.lock().remove(&shadow.id);
                    tracing::warn!(error = %e, ?path, "could not save the HNSW snapshot");
                }
            }
        }
        Ok(Decision::Index(Arc::new(index)))
    }

    /// Forget a collection's index. Used when its vectors are dropped.
    ///
    /// The snapshot goes with it: the caller is telling us the vectors this
    /// graph described no longer exist, and a snapshot that outlived them
    /// would be adopted by the next restart.
    ///
    /// So does a build in progress, at the moment it would install: removing
    /// the entry and the build-lock slot reaches nothing that is already
    /// running, and a build that began before the drop would otherwise
    /// install its graph and write its snapshot afterwards. The epoch is
    /// moved under the same lock the entry is removed under, so a build
    /// cannot install between the two — see [`Entries::epochs`].
    pub fn invalidate(&self, collection: CollectionId) {
        {
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.map.remove(&collection) {
                entries.resident -= entry.bytes();
            }
            entries.warned.remove(&collection);
            entries.fence(collection);
        }
        self.builds.lock().remove(&collection);
        self.saved_at.lock().remove(&collection);
        if let Some(path) = self.snapshot_path(collection) {
            // Removing a directory is filesystem work, and this is reached from
            // a request handler and from a change consumer, both on runtime
            // workers. `blocking` is what the build above already goes through,
            // and for the same reason: an async worker held on I/O is what
            // stalled `/metrics` and flapped membership on a live cluster.
            kimmy_storage::blocking(|| {
                let _ = std::fs::remove_dir_all(&path);
            });
        }
    }

    /// Forget a collection's index unless the collection standing under its
    /// id is the one the index was built for.
    ///
    /// `live` is what the caller found under the id: `None`, and this is
    /// [`Self::invalidate`]. `Some(created)`, and the id is in use — but an
    /// id is derived from a name, so that says only that *a* collection of
    /// this name exists, not that the entry or the snapshot describes it. Each
    /// half is compared on its own stamp: the resident entry goes when its
    /// `created` is not the live one's, and the snapshot directory goes when
    /// its `meta.json` says another incarnation wrote it. A `meta.json` that
    /// cannot be read or parsed is left where it is, for [`Self::try_snapshot`]
    /// to discard on the first search that opens it — this method deletes on
    /// evidence, and an unreadable file is not evidence of anything.
    ///
    /// The directory is touched only under the collection's build lock, taken
    /// with `try_lock` and skipped when held: a build in progress renames its
    /// own result over the directory when it finishes, so whatever is there
    /// now is about to be replaced, and removing it from under the rename
    /// would win nothing. What remains is the window the caller opens by
    /// reading `created` from the store first — a drop and a recreate landing
    /// between that read and this call would have this compare against a
    /// stamp that is already history. That is the class [`Self::forget_absent`]
    /// already accepts, and it costs a rebuild, never a wrong answer: `serve`
    /// and `HnswIndex::load` refuse the other incarnation regardless.
    ///
    /// A build in progress is fenced on this path as on the other, whether
    /// or not anything was found to forget: the epoch moves on every call.
    /// The build that matters here holds nothing resident yet — it is the
    /// one a search on the *previous* collection of this name started, still
    /// running on a read transaction from before the drop — so whether the
    /// map held an entry says nothing about it, and comparing on what was
    /// found would let exactly that build install a graph stamped with the
    /// old incarnation under the live id, and write its snapshot. `serve`
    /// would decline the graph at the next search and that search's build
    /// would replace both; but a collection searched only on other members
    /// has no next search here, and the resident bytes and the directory
    /// would stay until eviction or the next startup sweep. One rule is
    /// easier to hold than two: a forget fences every build that started
    /// before it. The cost is a build of the *live* incarnation that happens
    /// to be under way when a late drop entry arrives, which is discarded and
    /// paid again at the next search — a rebuild, never a wrong answer.
    ///
    /// Returns whether anything was forgotten, resident or on disk.
    pub fn forget_unless_created(&self, collection: CollectionId, live: Option<Hlc>) -> bool {
        let Some(created) = live else {
            let held = self.entries.lock().map.contains_key(&collection)
                || self.snapshot_path(collection).is_some_and(|p| p.exists());
            self.invalidate(collection);
            return held;
        };

        let mut forgot = false;
        {
            let mut entries = self.entries.lock();
            if entries.map.get(&collection).is_some_and(|entry| entry.created != created)
                && let Some(entry) = entries.map.remove(&collection)
            {
                entries.resident -= entry.bytes();
                entries.warned.remove(&collection);
                forgot = true;
            }
            entries.fence(collection);
        }
        if forgot {
            // The generation a snapshot was written at vouches for the
            // snapshot of *this* incarnation only; with the entry gone the
            // count check in `try_snapshot` is the right judge again. The
            // build-lock slot stays, unlike in `invalidate`: the id is live,
            // and a build of the new incarnation may be holding that lock
            // now — removing the slot would hand the next caller a fresh
            // lock and let two builds of one collection run at once.
            self.saved_at.lock().remove(&collection);
        }

        if let Some(path) = self.snapshot_path(collection)
            && path.is_dir()
        {
            let build_lock = self.build_lock(collection);
            let Some(_guard) = build_lock.try_lock() else {
                debug!(
                    collection = collection.0,
                    "a build is writing this collection's snapshot; leaving the directory to it"
                );
                return forgot;
            };
            // Filesystem work from a request handler or the change consumer,
            // both on runtime workers: off the worker, as `invalidate` is.
            let removed = kimmy_storage::blocking(|| remove_if_other_incarnation(&path, created));
            if removed {
                self.saved_at.lock().remove(&collection);
                forgot = true;
            }
        }
        forgot
    }

    /// Delete every snapshot on disk that `live` does not vouch for.
    ///
    /// The complement to [`Self::invalidate`] and [`Self::forget_absent`],
    /// which between them cover a collection this process saw go. A snapshot
    /// orphaned by a drop this node was not running for, or by one whose entry
    /// was lost to a lagging consumer, is reached by neither: nothing looks at
    /// a directory until something asks for that collection, and nothing asks
    /// for a collection that no longer exists. So it sits there, costing disk,
    /// indefinitely.
    ///
    /// `live` maps each collection id this node holds to that collection's
    /// `created`. A directory whose id is absent goes, as before. A directory
    /// whose id is present is not thereby vouched for — ids are derived from
    /// names, so a name dropped and created again is "live" at the same path
    /// its predecessor's snapshot occupies — and its `meta.json` is read for
    /// the stamp the graph was built under: another incarnation's, and it
    /// goes; the live one's, and it stays. A `meta.json` that cannot be read
    /// or parsed is left alone, on the same rule that leaves alone a name
    /// that is not a collection id: this deletes directories, and it acts on
    /// nothing it does not understand. `try_snapshot` discards such a
    /// snapshot on the first search that opens it.
    ///
    /// A live id's own directory is removed only under its build lock, taken
    /// with `try_lock` and skipped when held, for the reason
    /// [`Self::forget_unless_created`] gives: a build in progress is about to
    /// rename over it. Taken on every call, the startup sweep included, where
    /// no build can be running and the lock is uncontended — one rule is
    /// easier to reason about than one per caller. Staging directories are
    /// the caller's call, through `staging`: see [`Staging`].
    ///
    /// Returns how many it removed, for the line that says so.
    pub fn sweep_snapshots(&self, live: &HashMap<CollectionId, Hlc>, staging: Staging) -> usize {
        let Some(dir) = &self.snapshot_dir else { return 0 };
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            // No directory yet is the ordinary state of a node that has never
            // built a graph, and not something to complain about.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return 0,
            Err(e) => {
                tracing::warn!(error = %e, ?dir, "could not read the HNSW snapshot directory");
                return 0;
            }
        };

        let mut removed = 0;
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            // `save` stages a rebuild at `<id>.build` beside the snapshot and
            // renames over it, so a build cut short by a crash leaves one
            // here. Recognised, so it is not warned about at every start as
            // a file this crate does not know, and handled as `staging` says.
            let (stem, is_staging) = match name.strip_suffix(".build") {
                Some(stem) => (stem, true),
                None => (&*name, false),
            };
            let parsed = (stem.len() == 16 && stem.bytes().all(|b| b.is_ascii_hexdigit()))
                .then(|| u64::from_str_radix(stem, 16).ok())
                .flatten();
            let Some(id) = parsed else {
                tracing::warn!(
                    snapshot = %name,
                    "left a file in the HNSW snapshot directory that is not a collection id"
                );
                continue;
            };
            let id = CollectionId(id);

            if is_staging {
                if staging == Staging::Keep {
                    continue;
                }
                match std::fs::remove_dir_all(entry.path()) {
                    Ok(()) => {
                        removed += 1;
                        debug!(snapshot = %name, "removed the staging directory of an interrupted HNSW build");
                    }
                    Err(e) => tracing::warn!(error = %e, snapshot = %name, "could not remove it"),
                }
                continue;
            }

            match live.get(&id) {
                None => match std::fs::remove_dir_all(entry.path()) {
                    Ok(()) => {
                        removed += 1;
                        debug!(
                            snapshot = %name,
                            "removed an HNSW snapshot for a collection this node no longer holds"
                        );
                    }
                    Err(e) => tracing::warn!(error = %e, snapshot = %name, "could not remove it"),
                },
                Some(created) => {
                    let build_lock = self.build_lock(id);
                    let Some(_guard) = build_lock.try_lock() else {
                        debug!(snapshot = %name, "a build is writing it; left to the build");
                        continue;
                    };
                    if remove_if_other_incarnation(&entry.path(), *created) {
                        self.saved_at.lock().remove(&id);
                        removed += 1;
                    }
                }
            }
        }
        removed
    }

    /// Forget every cached collection that `live` does not vouch for.
    ///
    /// [`Self::invalidate`] is told which collection went away. This is for a
    /// caller that has lost track — a change consumer told it missed entries —
    /// and reconciles instead: a graph held for a collection this node no
    /// longer has is precisely what a drop should already have removed, so it
    /// goes, snapshot and all. So does a graph held under an id this node
    /// *does* have, when the collection there was created at another stamp
    /// than the graph was built for: the drop that was missed was followed by
    /// a create of the same name, and the entry describes the one that went.
    /// Returns how many were forgotten, for the log line that says so.
    ///
    /// It can also forget a collection created and first searched in the
    /// instant between `live` being read and this being called. That costs a
    /// rebuild, which is what an eviction under the budget costs anyway.
    ///
    /// A build in flight for a collection `live` does not name is fenced as
    /// well, though it holds no entry to be found by the walk above: the
    /// drop it straddles is one this node never read, so no other forget
    /// will ever reach it, and it would install its graph under the dead id
    /// once the reconciliation had passed. Every build-lock slot whose id is
    /// absent has its epoch moved and its saved generation cleared — a slot
    /// exists for every collection built here, so a build in flight has one
    /// — and the install that follows discards the graph and the snapshot
    /// it wrote. A snapshot already on disk for such an id is the sweep's,
    /// which the caller runs next. Fenced builds are not counted in the
    /// return value: nothing was held to forget.
    pub fn forget_absent(&self, live: &HashMap<CollectionId, Hlc>) -> usize {
        let gone: Vec<(CollectionId, Option<Hlc>)> = {
            let entries = self.entries.lock();
            entries
                .map
                .iter()
                .filter_map(|(id, entry)| match live.get(id) {
                    None => Some((*id, None)),
                    Some(created) if *created != entry.created => Some((*id, Some(*created))),
                    Some(_) => None,
                })
                .collect()
        };
        for (id, live) in &gone {
            self.forget_unless_created(*id, *live);
        }

        // Read after the loop, so a slot `invalidate` just removed is not
        // fenced twice, and released before `entries` is taken: the two
        // locks are never nested anywhere, and are not here either. The
        // slots go as `invalidate` removes them — a build holding one keeps
        // its own guard and is fenced regardless, and a dead id's slot would
        // otherwise sit in the map until a restart.
        let building: Vec<CollectionId> = {
            let mut builds = self.builds.lock();
            let absent: Vec<CollectionId> =
                builds.keys().filter(|id| !live.contains_key(id)).copied().collect();
            for id in &absent {
                builds.remove(id);
            }
            absent
        };
        if !building.is_empty() {
            let mut entries = self.entries.lock();
            for id in &building {
                entries.fence(*id);
            }
            drop(entries);
            let mut saved_at = self.saved_at.lock();
            for id in &building {
                saved_at.remove(id);
            }
        }
        gone.len()
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

/// Remove the snapshot at `path` if its `meta.json` names an incarnation
/// other than `created`. Whether it did.
///
/// Unreadable or unparseable is *not* "other": it is left for `try_snapshot`,
/// which discards what it cannot load. A file that parses but predates the
/// `created` field reads as the default stamp, which no live collection
/// carries, and goes — `load` would refuse it on its format anyway.
fn remove_if_other_incarnation(path: &std::path::Path, created: Hlc) -> bool {
    let Some(built_for) = crate::index::snapshot_created(path) else { return false };
    if built_for == created {
        return false;
    }
    match std::fs::remove_dir_all(path) {
        Ok(()) => {
            debug!(
                ?path,
                "removed an HNSW snapshot a previous collection of the same name left behind"
            );
            true
        }
        Err(e) => {
            tracing::warn!(error = %e, ?path, "could not remove it");
            false
        }
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
        let shadow = add_collection(&engine, "docs", count);
        (engine, shadow, dir)
    }

    /// One more vector collection in the same engine, holding `count`
    /// vectors of the same shape `setup` writes.
    fn add_collection(engine: &Engine, name: &str, count: usize) -> CollectionMeta {
        let shadow = empty_collection(engine, name);
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

    /// A vector collection with nothing written to it yet, so its generation
    /// counter has not moved.
    fn empty_collection(engine: &Engine, name: &str) -> CollectionMeta {
        engine.create_collection("app", name).unwrap();
        engine
            .configure_vectors(
                "app",
                name,
                VectorConfig {
                    fields: vec!["body".into()],
                    provider: ProviderConfig::Byo {},
                    dim: 4,
                    metric: Metric::Cosine,
                    document_prefix: None,
                    query_prefix: None,
                    chunk: ChunkConfig::default(),
                },
            )
            .unwrap();
        engine.vector_collection("app", name).unwrap().unwrap()
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
    fn a_graph_built_for_one_metric_is_not_served_for_another() {
        // A reconfiguration that changes the metric at the same width keeps
        // the shadow and writes no vector, so the entry matches on
        // incarnation and generation alike. Only the entry's own record of
        // what it was built for can decline it — and it must, because the
        // graph scores with its own metric and would hand back the old
        // ordering under the new configuration.
        let (engine, shadow, _dir) = setup(60);
        let cache = IndexCache::with_min_vectors(10);

        let Access::Approximate(cosine) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("expected an index");
        };
        assert_eq!(cosine.metric(), Metric::Cosine);

        let Access::Approximate(euclidean) = cache.access(&engine, &shadow, Metric::Euclidean, 4)
        else {
            panic!("a supported metric over enough vectors should build an index");
        };
        assert!(
            !Arc::ptr_eq(&cosine, &euclidean),
            "the graph built for cosine was served for a euclidean configuration"
        );
        assert_eq!(
            euclidean.metric(),
            Metric::Euclidean,
            "the graph must be built for the metric asked for"
        );
        assert_eq!(
            cache.len(),
            1,
            "the new graph replaces the old one rather than sitting beside it"
        );
    }

    #[test]
    fn a_graph_built_for_one_width_is_not_served_for_another() {
        // A width change is the loud case: the old graph refuses a query of
        // the new width, so every search would be an error until something
        // evicted it. The vectors of the old width are skipped by a build at
        // the new one, so what replaces it may be a graph over nothing; what
        // matters is that the old graph is not what comes back.
        let (engine, shadow, _dir) = setup(60);
        let cache = IndexCache::with_min_vectors(10);

        let Access::Approximate(old) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("expected an index");
        };
        assert_eq!(old.dim(), 4);

        if let Access::Approximate(served) = cache.access(&engine, &shadow, Metric::Cosine, 8) {
            assert!(
                !Arc::ptr_eq(&old, &served),
                "the graph built at width 4 was served for a configuration of width 8"
            );
            assert_eq!(served.dim(), 8);
        }
        assert_eq!(cache.len(), 1, "one entry per collection, whatever shape it holds");
        assert_eq!(cache.entries.lock().map[&shadow.id].dim, 8, "the entry records the new width");
    }

    #[test]
    fn a_too_small_verdict_for_one_shape_is_not_reused_for_another() {
        // A width change alters which stored vectors count, so a verdict for
        // one shape says nothing about another. The hook fires once per
        // decision, so it counts how many times the cache looked rather than
        // reused.
        let (engine, shadow, _dir) = setup(5);
        let cache = IndexCache::with_min_vectors(10);
        let decisions = Arc::new(AtomicUsize::new(0));
        *cache.build_hook.lock() = Some(Arc::new({
            let decisions = Arc::clone(&decisions);
            move |_| {
                decisions.fetch_add(1, Ordering::SeqCst);
            }
        }));

        assert!(matches!(cache.access(&engine, &shadow, Metric::Cosine, 4), Access::Exact));
        assert!(matches!(cache.access(&engine, &shadow, Metric::Cosine, 4), Access::Exact));
        assert_eq!(decisions.load(Ordering::SeqCst), 1, "the same shape reuses the verdict");

        assert!(matches!(cache.access(&engine, &shadow, Metric::Cosine, 8), Access::Exact));
        assert_eq!(
            decisions.load(Ordering::SeqCst),
            2,
            "a verdict decided for width 4 was reused for a configuration of width 8"
        );
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.entries.lock().map[&shadow.id].dim, 8);
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
    fn a_replicated_chunk_write_makes_the_graph_stale() {
        // A member that does not own a collection's embedding receives every
        // chunk by replication, and the apply path is a document write into
        // the shadow collection rather than `put_vectors`. If that write left
        // the generation alone, the entry built before it would match the
        // counter forever and `serve` would call it fresh at every access —
        // the staleness window never opening because, by the counter, nothing
        // had happened.
        use kimmy_core::{NodeId, OpKind, OplogEntry, Stamp};

        let (engine, shadow, _dir) = setup(60);
        let cache = IndexCache::with_min_vectors(10);
        let Access::Approximate(first) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("expected an index");
        };
        let built_at = engine.vector_generation(shadow.id);
        assert_eq!(cache.entries.lock().map[&shadow.id].generation, built_at);

        // The chunk as a peer would send it: the vector record as
        // `put_vectors` stores it, `_id` and all, stamped by a node this
        // engine has never heard from, into the shadow collection by id.
        let source = DocId::Int64(999);
        let chunk_id = VectorRecord::id(&source, 0);
        let record = VectorRecord {
            source: source.clone(),
            chunk: 0,
            source_hlc: Hlc::new(2, 0),
            vector: vec![1.0, 0.0, 0.0, 0.0],
            text: "replicated".into(),
        };
        let mut body = bson::serialize_to_document(&record).unwrap();
        body.insert("_id", chunk_id.to_bson());
        let entry = OplogEntry {
            stamp: Stamp::new(Hlc::new(9_000_000_000_000, 0), NodeId::from_bytes([9; 16])),
            kind: OpKind::Replace,
            collection: shadow.id,
            doc_id: Some(chunk_id),
            body: Some(bson::serialize_to_vec(&body).unwrap()),
        };
        assert!(engine.apply_remote(&shadow, &entry).unwrap(), "the chunk must have applied");
        assert_eq!(count_vectors(&engine, &shadow).unwrap(), 61);

        // The counter moved, so the entry is no longer fresh: it is served
        // only inside the window now, as after a local write.
        let replicated = engine.vector_generation(shadow.id);
        assert!(replicated > built_at, "a replicated chunk write must bump the generation");

        // Close the window. Ageing alone does not force a rebuild — an aged
        // entry whose generation still matches the counter is fresh under
        // `Serve::Usable`, and that is precisely how the old graph was served
        // forever. Only the generation mismatch makes this access rebuild.
        cache.age(shadow.id);
        let Access::Approximate(second) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("expected an index");
        };
        assert!(
            !Arc::ptr_eq(&first, &second),
            "the graph built before the replicated write was served as fresh after it"
        );
        assert_eq!(cache.entries.lock().map[&shadow.id].generation, replicated);
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
        assert!(crate::index::HnswIndex::load(&path, Metric::Cosine, 8, shadow.created).is_err());
        // Through the cache, the mismatch falls back cleanly too.
        assert!(matches!(
            cache.access(&engine, &shadow, Metric::Cosine, 4),
            Access::Approximate(_)
        ));
    }

    #[test]
    fn a_snapshot_this_process_wrote_is_not_fresh_past_a_later_write() {
        // An eviction under the budget removes the entry and keeps the
        // snapshot. If the collection is then written to in a way that leaves
        // its vector count unchanged — one document's chunks replaced by the
        // same number of chunks, which is what every re-embed does — the next
        // miss reloads that snapshot, and a count check alone would install a
        // graph from before the write as fresh at the generation after it.
        let (engine, docs, dir) = setup(60);
        let other = add_collection(&engine, "other", 60);
        let cache = snapshot_cache(&dir);
        cache.access(&engine, &docs, Metric::Cosine, 4);
        let snapshot = cache.snapshot_path(docs.id).unwrap();
        assert!(snapshot.is_dir(), "the build should have been persisted");

        // A budget too small for two graphs: installing the other evicts docs.
        cache.set_max_bytes(1);
        cache.access(&engine, &other, Metric::Cosine, 4);
        assert!(!cache.contains(docs.id), "docs should have been evicted for other");
        assert!(snapshot.is_dir(), "eviction keeps the snapshot");

        // Control: nothing written since the snapshot, so reloading it is
        // adopting it as fresh — and that install evicts `other` in turn.
        cache.access(&engine, &docs, Metric::Cosine, 4);
        let generation = engine.vector_generation(docs.id);
        assert_eq!(
            cache.entries.lock().map[&docs.id].generation,
            generation,
            "with no write since it was saved, the snapshot is fresh"
        );
        assert!(!cache.contains(other.id));

        // The write: document 0's one chunk replaced by one chunk. The count
        // is unchanged; the generation is not.
        let source = DocId::Int64(0);
        engine
            .put_vectors(
                &docs,
                &source,
                &[VectorRecord {
                    source: source.clone(),
                    chunk: 0,
                    source_hlc: Hlc::new(2, 0),
                    vector: vec![0.0, 0.0, 0.0, 1.0],
                    text: "moved".into(),
                }],
            )
            .unwrap();
        assert_eq!(count_vectors(&engine, &docs).unwrap(), 60, "the count must not give it away");
        let written = engine.vector_generation(docs.id);
        assert!(written > generation);

        // Evicted again, then reloaded: the snapshot predates the write and
        // must come back already stale, whatever its count says.
        cache.access(&engine, &other, Metric::Cosine, 4);
        assert!(!cache.contains(docs.id));
        assert!(snapshot.is_dir());
        cache.access(&engine, &docs, Metric::Cosine, 4);
        assert_eq!(
            cache.entries.lock().map[&docs.id].generation,
            u64::MAX,
            "a snapshot written before a later write was adopted as fresh"
        );
        // And the access after it rebuilds, landing at the live generation.
        cache.access(&engine, &docs, Metric::Cosine, 4);
        assert_eq!(cache.entries.lock().map[&docs.id].generation, written);
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
    fn a_snapshot_left_by_a_previous_collection_of_the_same_name_is_refused() {
        // A collection id is derived from its name, so a name dropped and used
        // again lands on the same snapshot path. Nothing about the old graph's
        // shape says it is the wrong one: same metric, same width, and if the
        // vector counts happen to agree it is adopted as *fresh* and serves
        // until a write bumps the generation — on a collection that is only
        // read, indefinitely, and again after every restart. So the graph
        // records which incarnation it was built for.
        let (engine, first, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        cache.access(&engine, &first, Metric::Cosine, 4);
        let path = cache.snapshot_path(first.id).unwrap();
        assert!(path.is_dir(), "the build should have been persisted");

        engine.drop_collection("app", "docs").unwrap();
        let second = add_collection(&engine, "docs", 70);
        assert_eq!(second.id, first.id, "the id is derived from the name, so it repeats");
        assert_ne!(second.created, first.created, "the incarnation does not");
        assert!(path.is_dir(), "the orphan is sitting where the new graph's would go");

        assert!(
            crate::index::HnswIndex::load(&path, Metric::Cosine, 4, second.created).is_err(),
            "a graph built for a previous collection of this name must not be trusted"
        );

        // And through the cache: 70 is the new collection's own count, 60 the
        // orphan's, so the size says which graph answered.
        let fresh = snapshot_cache(&dir);
        let Access::Approximate(index) = fresh.access(&engine, &second, Metric::Cosine, 4) else {
            panic!("expected an index");
        };
        assert_eq!(index.len(), 70, "the orphan was adopted instead of a rebuild");
    }

    #[test]
    fn a_resident_graph_does_not_outlive_the_collection_it_was_built_for() {
        // The in-memory half of the same problem, and the sharper one, because
        // nothing on this path expires. The graph is resident when the drop is
        // applied; the name is used again before the consumer catches up, so
        // the entry is still there under an id derived from that name. The
        // generation counter is keyed by the id and only ever counts up, and
        // the new collection has had nothing written to it — so the entry
        // matches on generation and would serve the previous collection's
        // graph for as long as that stays true, which is indefinitely.
        let (engine, first, _dir) = setup(60);
        let cache = IndexCache::with_min_vectors(10);
        let Access::Approximate(built) = cache.access(&engine, &first, Metric::Cosine, 4) else {
            panic!("expected an index");
        };
        assert_eq!(built.len(), 60);
        let generation = engine.vector_generation(first.id);

        engine.drop_collection("app", "docs").unwrap();
        let second = empty_collection(&engine, "docs");
        assert_eq!(second.id, first.id, "the id is derived from the name, so it repeats");
        assert_ne!(second.created, first.created, "the incarnation does not");
        assert_eq!(
            engine.vector_generation(second.id),
            generation,
            "nothing has been written, so nothing has bumped the generation"
        );
        assert!(cache.contains(second.id), "the consumer has not caught up yet");

        // The honest answer for a collection holding no vectors is the exact
        // scan. Anything else is the dead graph still answering.
        assert!(
            matches!(cache.access(&engine, &second, Metric::Cosine, 4), Access::Exact),
            "a graph built for the previous collection of this name served a search on the new one"
        );
    }

    #[test]
    fn the_sweep_removes_snapshots_this_node_has_no_collection_for() {
        // The complement to invalidation: a snapshot orphaned while this node
        // was not running is reached by nothing, because nothing opens a
        // snapshot directory until something asks for that collection, and
        // nothing asks for one that no longer exists.
        let (engine, kept, dir) = setup(60);
        let gone = add_collection(&engine, "gone", 60);
        let cache = snapshot_cache(&dir);
        cache.access(&engine, &kept, Metric::Cosine, 4);
        cache.access(&engine, &gone, Metric::Cosine, 4);
        let kept_path = cache.snapshot_path(kept.id).unwrap();
        let gone_path = cache.snapshot_path(gone.id).unwrap();

        // A name the sweep cannot read as a collection id. It removes
        // directories, so the one thing it must never do is act on a name it
        // does not understand.
        let stranger = dir.path().join("hnsw").join("notes.txt");
        std::fs::write(&stranger, b"not a collection id").unwrap();

        // What a build interrupted by a crash leaves: `save` stages under
        // `<id>.build` and renames over the snapshot. A collection that is
        // never searched again never rebuilds, so nothing else would ever
        // clear one — and warning about it at every start would say a file is
        // unrecognised when it is this crate's own. At startup no build can be
        // writing one, so every one there is abandoned, live collection or
        // not.
        let kept_staging = kept_path.with_extension("build");
        let gone_staging = gone_path.with_extension("build");
        std::fs::create_dir_all(&kept_staging).unwrap();
        std::fs::create_dir_all(&gone_staging).unwrap();

        engine.drop_collection("app", "gone").unwrap();
        let live = live_collections(&engine);

        // A fresh cache, as a restart has: nothing resident, only what is on
        // disk.
        let restarted = snapshot_cache(&dir);
        assert_eq!(
            restarted.sweep_snapshots(&live, Staging::Remove),
            3,
            "the snapshot and both staging directories"
        );
        assert!(!gone_path.exists(), "a dropped collection's snapshot must go");
        assert!(!gone_staging.exists(), "and the staging directory beside it");
        assert!(kept_path.is_dir(), "a live collection's must not");
        assert!(!kept_staging.exists(), "but its abandoned staging directory must");
        assert!(stranger.exists(), "an unrecognised name must be left alone, not deleted");
    }

    /// What the daemon hands the sweep: every collection this engine holds,
    /// shadows included, by id and incarnation.
    fn live_collections(engine: &Engine) -> HashMap<CollectionId, Hlc> {
        engine.live_collections().unwrap()
    }

    #[test]
    fn the_sweep_removes_a_snapshot_a_previous_collection_of_the_same_name_left() {
        // A recreated name derives the id it had before, so its predecessor's
        // snapshot sits under an id the sweep finds live. Keeping every
        // directory whose id is live keeps that one on every restart, and
        // only a search of the new collection on this node — with nothing
        // resident to answer it — would ever refuse and delete it. A
        // collection never searched here keeps its predecessor's graph on
        // disk indefinitely. So a live id is not enough: the directory's own
        // `meta.json` says which incarnation built it, and that is compared.
        let (engine, first, dir) = setup(60);
        let other = add_collection(&engine, "other", 60);
        let cache = snapshot_cache(&dir);
        cache.access(&engine, &first, Metric::Cosine, 4);
        cache.access(&engine, &other, Metric::Cosine, 4);
        let path = cache.snapshot_path(first.id).unwrap();
        let other_path = cache.snapshot_path(other.id).unwrap();
        assert!(path.is_dir() && other_path.is_dir(), "both builds should have been persisted");
        // An interrupted build's residue beside the live one.
        let staging = other_path.with_extension("build");
        std::fs::create_dir_all(&staging).unwrap();

        engine.drop_collection("app", "docs").unwrap();
        let second = add_collection(&engine, "docs", 60);
        assert_eq!(second.id, first.id, "the id is derived from the name, so it repeats");
        assert_ne!(second.created, first.created, "the incarnation does not");

        // The previous process's view: the sweep sees a live id and a
        // directory, and the map still names the incarnation that built it.
        // Nothing is wrong, so nothing goes.
        let mut stale = live_collections(&engine);
        stale.insert(first.id, first.created);
        let restarted = snapshot_cache(&dir);
        assert_eq!(restarted.sweep_snapshots(&stale, Staging::Keep), 0);
        assert!(path.is_dir(), "a snapshot the live map vouches for must stay");
        assert!(staging.is_dir(), "and a staging directory is kept when told to");

        // What the daemon reads at startup: the recreated collection's stamp.
        let live = live_collections(&engine);
        assert_eq!(
            restarted.sweep_snapshots(&live, Staging::Remove),
            2,
            "the predecessor's snapshot and the staging directory"
        );
        assert!(!path.exists(), "a previous incarnation's snapshot survived the sweep");
        assert!(other_path.is_dir(), "a live collection's matching snapshot must stay");
        assert!(!staging.exists(), "an abandoned staging directory must go at startup");
    }

    #[test]
    fn reconciling_forgets_a_graph_built_for_a_previous_collection_of_the_same_name() {
        // The map-only half of the same problem. The consumer missed the
        // drop; the name is back, so the id is live; the entry and snapshot
        // were built for the collection that went. A reconciliation keyed on
        // the id alone finds nothing absent and leaves both, and `serve`
        // then declines the entry on every search without ever releasing it.
        let (engine, first, dir) = setup(60);
        let kept = add_collection(&engine, "kept", 60);
        let cache = snapshot_cache(&dir);
        cache.access(&engine, &first, Metric::Cosine, 4);
        cache.access(&engine, &kept, Metric::Cosine, 4);
        let path = cache.snapshot_path(first.id).unwrap();
        assert!(path.is_dir(), "the build should have been persisted");

        engine.drop_collection("app", "docs").unwrap();
        let second = empty_collection(&engine, "docs");
        assert_eq!(second.id, first.id, "the id is derived from the name, so it repeats");
        assert_ne!(second.created, first.created, "the incarnation does not");
        assert!(cache.contains(second.id), "the consumer has not caught up yet");

        // A matching map first: nothing to forget.
        let matching: HashMap<CollectionId, Hlc> =
            [(first.id, first.created), (kept.id, kept.created)].into_iter().collect();
        assert_eq!(cache.forget_absent(&matching), 0);
        assert!(cache.contains(first.id) && path.is_dir(), "a matching entry must stay");

        let live = live_collections(&engine);
        assert_eq!(live[&second.id], second.created);
        assert_eq!(cache.forget_absent(&live), 1);
        assert!(!cache.contains(second.id), "a graph of the previous incarnation stayed resident");
        assert!(!path.exists(), "and its snapshot stayed on disk");
        assert!(cache.contains(kept.id), "a live collection must keep its graph");
        assert!(cache.snapshot_path(kept.id).unwrap().is_dir(), "and its snapshot");
    }

    #[test]
    fn reconciling_forgets_the_graphs_of_collections_this_node_no_longer_holds() {
        // What a consumer told it missed entries has left: it cannot say which
        // collections went away, only which are still here. Anything held for
        // a collection that is not must go, snapshot included, and everything
        // else must survive — a reconciliation that cleared the cache would
        // charge every live collection a rebuild to punish one dead one.
        let (engine, gone, dir) = setup(60);
        let kept = add_collection(&engine, "kept", 60);
        let cache = snapshot_cache(&dir);
        cache.access(&engine, &gone, Metric::Cosine, 4);
        cache.access(&engine, &kept, Metric::Cosine, 4);
        let stranded = cache.snapshot_path(gone.id).unwrap();
        assert!(stranded.is_dir(), "the build should have been persisted");

        let live: HashMap<CollectionId, Hlc> = [(kept.id, kept.created)].into_iter().collect();
        assert_eq!(cache.forget_absent(&live), 1);

        assert!(!cache.contains(gone.id), "a collection this node lacks must not stay resident");
        assert!(!stranded.exists(), "and must not leave its snapshot behind");
        assert!(cache.contains(kept.id), "a live collection must keep its graph");
        assert!(cache.snapshot_path(kept.id).unwrap().is_dir(), "and its snapshot");
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
    // A forget fences every build that started before it
    // -----------------------------------------------------------------------

    /// Everything a fenced build must not leave behind: no entry, no
    /// generation recorded for a snapshot, no snapshot, nothing resident.
    fn assert_nothing_installed(cache: &IndexCache, collection: CollectionId) {
        assert!(!cache.contains(collection), "a build fenced by a forget installed its entry");
        assert!(
            !cache.saved_at.lock().contains_key(&collection),
            "a fenced build left the generation it saved its snapshot at"
        );
        if let Some(path) = cache.snapshot_path(collection) {
            assert!(!path.exists(), "a fenced build left its snapshot on disk");
        }
        assert_eq!(cache.resident_bytes(), 0, "a fenced build was charged to the budget");
    }

    #[test]
    fn a_build_that_outlives_a_drop_installs_nothing() {
        // The drop route forgets the collection after the drop commits, and
        // forgetting removes the entry, the build-lock slot and the snapshot.
        // None of that reaches a build already running: it holds its own
        // guard and reads a transaction from before the drop, so it would
        // build the graph, save it, and install both under the dead id. The
        // vectors are left in the store here for the same reason — the
        // build's read view still sees every one of them.
        let (engine, a, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        let gate = Gate::install(&cache, a.id);

        std::thread::scope(|s| {
            let result = timed_access(s, &cache, &engine, &a);
            gate.wait_for_build_to_start();
            cache.invalidate(a.id);
            gate.release();

            let served = result.recv_timeout(PATIENCE).expect("the fenced build never returned");
            assert!(
                matches!(served, Access::Exact),
                "a build fenced by a drop must answer with the exact scan, not its graph"
            );
        });
        assert_nothing_installed(&cache, a.id);
        assert_eq!(gate.builds.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_verdict_that_outlives_a_drop_installs_nothing() {
        // The same race starting just after the drop decides "too small" over
        // whatever the read view holds. A verdict costs no bytes and so is
        // never evicted: installed under a dead id it would sit there for
        // the life of the process.
        let (engine, a, _dir) = setup(5);
        let cache = IndexCache::with_min_vectors(10);
        let gate = Gate::install(&cache, a.id);

        std::thread::scope(|s| {
            let result = timed_access(s, &cache, &engine, &a);
            gate.wait_for_build_to_start();
            cache.invalidate(a.id);
            gate.release();
            assert!(matches!(
                result.recv_timeout(PATIENCE).expect("the fenced verdict never returned"),
                Access::Exact
            ));
        });
        assert!(cache.is_empty(), "a verdict decided before the drop was installed after it");
        assert_nothing_installed(&cache, a.id);
    }

    #[test]
    fn a_snapshot_load_that_outlives_a_drop_installs_nothing() {
        // The epoch must be captured before the snapshot is looked at, not
        // only before the build: a process's first access loads the snapshot
        // instead of building, and a drop landing during the load would
        // install the loaded graph just the same. The forget removes the
        // directory, so the snapshot is kept aside and put back before the
        // access resumes — what runs after the park is the load, and it must
        // be fenced by the epoch captured ahead of it.
        let (engine, a, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        cache.access(&engine, &a, Metric::Cosine, 4);
        let path = cache.snapshot_path(a.id).unwrap();
        assert!(path.is_dir(), "the build should have been persisted");
        drop(cache);

        let aside = dir.path().join("aside");
        std::fs::rename(&path, &aside).unwrap();
        let cache = snapshot_cache(&dir);
        let gate = Gate::install(&cache, a.id);

        std::thread::scope(|s| {
            let result = timed_access(s, &cache, &engine, &a);
            gate.wait_for_build_to_start();
            cache.invalidate(a.id);
            std::fs::rename(&aside, &path).unwrap();
            gate.release();
            assert!(matches!(
                result.recv_timeout(PATIENCE).expect("the fenced load never returned"),
                Access::Exact
            ));
        });
        assert_nothing_installed(&cache, a.id);
        assert!(!path.exists(), "the snapshot a fenced load adopted must go");
    }

    #[test]
    fn a_build_of_a_previous_incarnation_that_outlives_the_recreate_installs_nothing() {
        // The consumer's late-drop path: by the time the drop entry is read,
        // the name has been created again, so the id is live and the forget
        // compares incarnations. A build that a search on the *old*
        // collection started holds nothing resident to compare, so the
        // forget finds nothing — and must fence it regardless, or the graph
        // it installs is stamped with the old incarnation under the live id,
        // declined by every search and released by none.
        let (engine, first, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        let gate = Gate::install(&cache, first.id);

        std::thread::scope(|s| {
            let result = timed_access(s, &cache, &engine, &first);
            gate.wait_for_build_to_start();

            engine.drop_collection("app", "docs").unwrap();
            let second = add_collection(&engine, "docs", 60);
            assert_eq!(second.id, first.id, "the id is derived from the name, so it repeats");
            assert_ne!(second.created, first.created, "the incarnation does not");
            assert!(
                !cache.forget_unless_created(first.id, Some(second.created)),
                "nothing was resident to forget; the fence is the whole effect"
            );

            gate.release();
            assert!(matches!(
                result.recv_timeout(PATIENCE).expect("the fenced build never returned"),
                Access::Exact
            ));
        });
        assert_nothing_installed(&cache, first.id);
    }

    #[test]
    fn a_build_of_a_collection_lost_to_a_lagging_consumer_installs_nothing() {
        // The consumer fell behind and never read the drop; all it can do is
        // reconcile against what the node holds now. A build in flight for
        // the dropped collection has no entry for that walk to find and no
        // drop entry will ever name it, so the reconciliation is the only
        // forget that can reach it — and it must, or the build installs its
        // graph under the dead id the moment the reconciliation has passed.
        let (engine, a, dir) = setup(60);
        let kept = add_collection(&engine, "kept", 60);
        let cache = snapshot_cache(&dir);
        let gate = Gate::install(&cache, a.id);

        std::thread::scope(|s| {
            let result = timed_access(s, &cache, &engine, &a);
            gate.wait_for_build_to_start();

            let live: HashMap<CollectionId, Hlc> = [(kept.id, kept.created)].into_iter().collect();
            assert_eq!(
                cache.forget_absent(&live),
                0,
                "nothing was resident to forget; the fence is the whole effect"
            );

            gate.release();
            assert!(matches!(
                result.recv_timeout(PATIENCE).expect("the fenced build never returned"),
                Access::Exact
            ));
        });
        assert_nothing_installed(&cache, a.id);
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
