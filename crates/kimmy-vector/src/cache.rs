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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use kimmy_core::{CollectionId, Hlc, Metric};
use kimmy_storage::{CollectionMeta, Engine};
use parking_lot::{Condvar, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::debug;

use crate::error::{Result, VectorError};
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

/// The generation of an entry adopted from a snapshot that is behind the
/// store: no live counter returns it, so the entry is never usable as fresh
/// and its rebuild is queued at once; it serves until the rebuild installs.
const BEHIND: u64 = u64::MAX;

/// The generation of an entry adopted from a snapshot a previous process
/// saved, until a builder has counted the collection to judge it: fresh, and
/// re-stamped at the generation the count began at, or behind, and rebuilt.
const UNVERIFIED: u64 = u64::MAX - 1;

/// The generation every collection's counter starts at in a process: what a
/// snapshot a previous process saved is judged at, since it predates every
/// write this process has made.
const PROCESS_START: u64 = 0;

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

/// How many graph builds run at once unless told otherwise
/// (`vector.index_cache.build_concurrency`).
///
/// One: a build is CPU and memory bound (a 100,000 × 384 graph peaks at
/// 1.3 GiB with its records), and a roll that starts every collection's
/// rebuild at once would otherwise run them all side by side. A collection
/// waiting its turn is searched by the graph it had, or by exact scan.
pub const DEFAULT_BUILD_CONCURRENCY: usize = 1;

/// How many exact scans of one collection run at once while it has no graph
/// to serve. A scan of 100,000 vectors of 384 dimensions takes about 1.4 s
/// and 10,000 about 135 ms (release, measured), all of it CPU and storage
/// reads; more than this many at once would take the cores the build needs
/// and the rest of the member's.
/// A search that cannot get a turn before its deadline answers as a request
/// that ran out of time does.
pub const EXACT_SCANS_PER_COLLECTION: usize = 2;

/// Per-collection index cache.
///
/// A handle: the state is shared with the builder threads that run every
/// graph build and save off the request path (ADR-241).
#[derive(Default)]
pub struct IndexCache {
    shared: Arc<Shared>,
}

impl std::ops::Deref for IndexCache {
    type Target = Shared;

    fn deref(&self) -> &Shared {
        &self.shared
    }
}

/// What an [`IndexCache`] and its builder threads share.
#[doc(hidden)]
pub struct Shared {
    entries: Mutex<Entries>,
    /// One lock per collection, held for the length of a build.
    ///
    /// This is what keeps a build off `entries`: the builder holds its
    /// collection's lock and nothing else, so a search on any other
    /// collection takes `entries` for a lookup and carries on. A search of
    /// the same collection that finds the lock taken serves what is cached,
    /// or scans exactly; it never waits for the build. Entries here are
    /// created on first use and removed on `invalidate`, so the map is
    /// bounded by the number of vector collections. Removing a slot does not
    /// stop the build holding it — the guard is the builder's own — which is
    /// what [`Entries::epochs`] is for.
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
    /// Builds waiting for a builder thread, and running on one.
    queue: Mutex<Queue>,
    /// Signalled when a build finishes or a builder thread exits.
    idle: Condvar,
    /// How many builds run at once; [`DEFAULT_BUILD_CONCURRENCY`] unless set.
    max_builds: AtomicUsize,
    /// Each collection's turns at an exact scan
    /// ([`EXACT_SCANS_PER_COLLECTION`]), awaited rather than blocked on, so a
    /// search waiting for one holds no thread (ADR-245).
    scans: Mutex<HashMap<CollectionId, Arc<Semaphore>>>,
    /// Called with the collection about to be built, on its builder, before
    /// the build. Lets a test hold a build open to observe what happens
    /// around it.
    #[cfg(test)]
    build_hook: Mutex<Option<BuildHook>>,
    /// Called with the collection a search is about to look for a snapshot
    /// of, before the load: the same, for a first look.
    #[cfg(test)]
    load_hook: Mutex<Option<BuildHook>>,
    /// Called with the collection whose graph a builder is about to save,
    /// once the builder has let go of the store.
    #[cfg(test)]
    save_hook: Mutex<Option<BuildHook>>,
    /// Called inside an exact scan's turn with how many of the collection's
    /// turns are taken.
    #[cfg(test)]
    scan_hook: Mutex<Option<ScanHook>>,
}

#[cfg(test)]
type BuildHook = Arc<dyn Fn(CollectionId) + Send + Sync>;

#[cfg(test)]
type ScanHook = Arc<dyn Fn(usize) + Send + Sync>;

impl Default for Shared {
    fn default() -> Self {
        Self {
            entries: Mutex::new(Entries::default()),
            builds: Mutex::new(HashMap::new()),
            min_vectors: MIN_VECTORS_FOR_INDEX,
            snapshot_dir: None,
            max_bytes: AtomicU64::new(DEFAULT_MAX_BYTES),
            saved_at: Mutex::new(HashMap::new()),
            queue: Mutex::new(Queue::default()),
            idle: Condvar::new(),
            max_builds: AtomicUsize::new(DEFAULT_BUILD_CONCURRENCY),
            scans: Mutex::new(HashMap::new()),
            #[cfg(test)]
            build_hook: Mutex::new(None),
            #[cfg(test)]
            load_hook: Mutex::new(None),
            #[cfg(test)]
            save_hook: Mutex::new(None),
            #[cfg(test)]
            scan_hook: Mutex::new(None),
        }
    }
}

/// A graph build a search asked for, waiting for a builder or running on one.
struct Job {
    /// The store to build from. Weak while the job waits, so a queued build
    /// keeps nothing from the node's stop; the builder holds it strongly for
    /// the count and the build only, and lets go before the save.
    engine: std::sync::Weak<Engine>,
    shadow: CollectionMeta,
    metric: Metric,
    dim: usize,
    /// [`Entries::epoch`] when the search that asked for the build missed: a
    /// forget since then fences the build, as it fences one in progress.
    epoch: u64,
}

#[derive(Default)]
struct Queue {
    waiting: VecDeque<Job>,
    /// Collections with a build waiting or running, and the shape it is a
    /// build of: one at a time each, and a search that finds its collection
    /// here asks for nothing more.
    pending: HashMap<CollectionId, (Metric, usize)>,
    /// Builder threads alive.
    builders: usize,
    /// Builds running now.
    running: usize,
}

/// Which access path a search should take.
pub enum Access {
    /// Use this graph.
    Approximate(Arc<HnswIndex>),
    /// Scan every vector. Correct, and faster below the size threshold.
    Exact,
    /// The collection has no graph to serve and one is being built: scan
    /// every vector meanwhile, in a turn and under the request's deadline
    /// ([`Shared::scan_while_building`]).
    Building,
}

/// A turn at an exact scan of one collection ([`Shared::exact_scan_turn`]),
/// given back when dropped.
pub struct ScanTurn {
    _turn: OwnedSemaphorePermit,
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
        Self { shared: Arc::new(Shared { snapshot_dir: Some(dir), ..Shared::default() }) }
    }

    /// A cache with a non-default size threshold. For tests.
    #[cfg(test)]
    fn with_min_vectors(min_vectors: usize) -> Self {
        Self::new().min_vectors(min_vectors)
    }

    /// This cache with a non-default size threshold. For tests, before the
    /// cache is shared.
    #[cfg(test)]
    fn min_vectors(mut self, min_vectors: usize) -> Self {
        Arc::get_mut(&mut self.shared).expect("not yet shared").min_vectors = min_vectors;
        self
    }

    /// Choose an access path, asking for a build or a rebuild if warranted.
    ///
    /// Never fails the query, and **never waits for a build** (ADR-241). A
    /// usable graph is served. A graph from before a write, or one a
    /// snapshot said was behind, is served while its rebuild runs, which is
    /// bounded recall loss on new data, never a wrong answer. With nothing to
    /// serve, the first look tries the snapshot; failing that, the build is
    /// queued and the answer is [`Access::Building`], an exact scan. Every
    /// build, the O(n) count that decides whether a collection is worth
    /// one, and every save run on the builder threads, at most
    /// [`Shared::set_build_concurrency`] at once, one per collection.
    ///
    /// The size count is O(n), so its verdict is cached under the same
    /// generation-and-staleness rule as the graph itself. Otherwise the
    /// check meant to *avoid* a full scan would perform one on every query.
    pub fn access(
        &self,
        engine: &Arc<Engine>,
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
        // A graph of this collection and shape from before a write: served
        // while it is rebuilt.
        if let Some(access) = self.serve(shadow, metric, dim, Serve::Anything) {
            self.enqueue(engine, shadow, metric, dim);
            return access;
        }
        // The snapshot is a read of a file, under a second at 100,000
        // vectors; off the async worker, as a storage read is.
        kimmy_storage::blocking(|| self.first_look(engine, shadow, metric, dim, generation))
    }

    /// The slow path of [`Self::access`]: nothing resident to serve.
    fn first_look(
        &self,
        engine: &Arc<Engine>,
        shadow: &CollectionMeta,
        metric: Metric,
        dim: usize,
        generation: u64,
    ) -> Access {
        let build_lock = self.build_lock(shadow.id);
        let Some(building) = build_lock.try_lock() else {
            // A builder has this collection now and there is nothing older to
            // serve: scan meanwhile. Its result is installed when it is done.
            self.enqueue(engine, shadow, metric, dim);
            return Access::Building;
        };
        // A build that finished between the lookup and here has installed.
        if let Some(access) = self.serve(shadow, metric, dim, Serve::Usable(generation)) {
            return access;
        }
        // The epoch is the fence `install` compares against, read before the
        // snapshot load; `seen` says whether this process has newer knowledge
        // than any snapshot (an entry of another incarnation or shape).
        let (epoch, seen) = {
            let entries = self.entries.lock();
            (entries.epoch(shadow.id), entries.map.contains_key(&shadow.id))
        };
        #[cfg(test)]
        if !seen {
            self.run_hook(&self.load_hook, shadow.id);
        }
        if !seen && let Some(entry) = self.try_snapshot(shadow, metric, dim, generation) {
            let behind = entry.generation != generation;
            let access = self.install(shadow, entry, epoch);
            drop(building);
            if behind && matches!(access, Access::Approximate(_)) {
                self.enqueue(engine, shadow, metric, dim);
            }
            return access;
        }
        drop(building);
        self.enqueue(engine, shadow, metric, dim);
        Access::Building
    }

    /// Queue a build of `shadow`, unless one is waiting or running, and start
    /// a builder thread if fewer run than the concurrency allows.
    fn enqueue(&self, engine: &Arc<Engine>, shadow: &CollectionMeta, metric: Metric, dim: usize) {
        let epoch = self.entries.lock().epoch(shadow.id);
        let mut queue = self.queue.lock();
        if queue.pending.contains_key(&shadow.id) {
            return;
        }
        queue.pending.insert(shadow.id, (metric, dim));
        queue.waiting.push_back(Job {
            engine: Arc::downgrade(engine),
            shadow: shadow.clone(),
            metric,
            dim,
            epoch,
        });
        if queue.builders < self.max_builds.load(Ordering::Relaxed).max(1) {
            let shared = Arc::clone(&self.shared);
            let started = std::thread::Builder::new()
                .name("kimmy-hnsw-build".into())
                // UNSUPERVISED: a builder catches each build's panic and lives on, exits when the queue is empty, and the next search that queues a build starts another.
                .spawn(move || shared.builder());
            match started {
                Ok(_) => queue.builders += 1,
                // The build stays queued, for the next builder to start.
                Err(e) => tracing::warn!(error = %e, "could not start an HNSW builder thread"),
            }
        }
    }

    /// Block until no build is waiting or running. For tests, which would
    /// otherwise see [`Access::Building`] where a graph is about to be.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn settle(&self) {
        let mut queue = self.queue.lock();
        while !(queue.waiting.is_empty() && queue.running == 0) {
            self.idle.wait(&mut queue);
        }
    }
}

impl Shared {
    /// Run builds from the queue until it is empty. A builder thread's body.
    fn builder(&self) {
        loop {
            let job = {
                let mut queue = self.queue.lock();
                match queue.waiting.pop_front() {
                    Some(job) => {
                        queue.running += 1;
                        job
                    }
                    None => {
                        queue.builders -= 1;
                        drop(queue);
                        self.idle.notify_all();
                        return;
                    }
                }
            };
            let collection = job.shadow.id;
            let name = job.shadow.name.clone();
            // The job, and the engine it holds, are gone before the build is
            // counted as done. A build that panics (the library asserts on
            // what it cannot take) costs that build and not the builder: the
            // counts below stay true, so no later build waits on a builder
            // that is gone, and the collection is scanned until a later
            // search asks again. `AssertUnwindSafe`: the build's own state
            // goes with it, and what it shares is behind locks that do not
            // poison and are not held across the library's calls.
            let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.build(job)));
            if built.is_err() {
                tracing::error!(
                    collection = %name,
                    "an HNSW build panicked; the collection is scanned until a later search asks again"
                );
            }
            let mut queue = self.queue.lock();
            queue.running -= 1;
            queue.pending.remove(&collection);
            drop(queue);
            self.idle.notify_all();
        }
    }

    /// One queued build: decide, build, save and install, under the
    /// collection's build lock and the forget fence the search read.
    ///
    /// The store is held for the count and the build, whose walks end at the
    /// node's stop, and let go before the save, which needs none: a save of
    /// 100,000 vectors takes up to two minutes, and a stop that lands in it
    /// must find the store free to close (ADR-245). A save the stop's exit
    /// cuts short leaves its staging directory, never a half snapshot, and the
    /// next start sweeps it.
    fn build(&self, job: Job) {
        let Job { engine, shadow, metric, dim, epoch } = job;
        // Gone: the store is closing, and nothing is built for it.
        let Some(engine) = engine.upgrade() else { return };
        let build_lock = self.build_lock(shadow.id);
        let _building = build_lock.lock();
        // Read before the build, so a write that lands during it makes the
        // result look behind, never fresh.
        let generation = engine.vector_generation(shadow.id);
        if self.serve(&shadow, metric, dim, Serve::Usable(generation)).is_some() {
            return;
        }
        #[cfg(test)]
        self.run_hook(&self.build_hook, shadow.id);
        if self.verify_snapshot(&engine, &shadow, metric, dim, epoch) {
            return;
        }
        // Whether this build replaces a graph adopted behind or unjudged:
        // the swap an upgrade's rebuilds make, said at `INFO`. An ordinary
        // rebuild after a write is said at `DEBUG`.
        let after_adoption = self.entries.lock().map.get(&shadow.id).is_some_and(|e| {
            e.created == shadow.created
                && e.metric == metric
                && e.dim == dim
                && matches!(e.generation, BEHIND | UNVERIFIED)
        });
        // Falling back on error keeps the query correct: the collection is
        // scanned until a later search asks again.
        let decision = match self.decide(&engine, &shadow, metric, dim) {
            Ok(decision) => decision,
            Err(e) => {
                debug!(collection = %shadow.name, error = %e, "the HNSW build failed; scanning");
                return;
            }
        };
        // Not saved once the node's stop has begun: a save writes the whole
        // graph, and the next start builds or loads one either way.
        let stopping = engine.walk_stop(kimmy_storage::WalkScope::Background).is_some();
        drop(engine);
        if let Decision::Index(index) = &decision
            && !stopping
        {
            #[cfg(test)]
            self.run_hook(&self.save_hook, shadow.id);
            self.save(&shadow, index, generation);
        }
        let graph = matches!(decision, Decision::Index(_));
        let entry = Entry::new(decision, generation, shadow.created, metric, dim, Instant::now());
        if matches!(self.install(&shadow, entry, epoch), Access::Approximate(_)) && graph {
            if after_adoption {
                tracing::info!(collection = %shadow.name, "a rebuilt HNSW graph replaced the one served meanwhile");
            } else {
                debug!(collection = %shadow.name, "a rebuilt HNSW graph replaced the one served meanwhile");
            }
        }
    }

    /// Judge a snapshot a previous process saved, which the first look
    /// adopted [`UNVERIFIED`]: when the collection still holds what it was
    /// built over, by count and by moves of chunk ids, re-stamp it at the
    /// generation it was adopted at, and say so; otherwise say not, and the
    /// caller rebuilds. The adoption's generation, not the one now: a write
    /// this process made since the adoption that keeps the count and the
    /// keys, an edit of a document into as many chunks, is one the count
    /// cannot see and the generation can, and it leaves the graph behind, to
    /// be rebuilt once the window passes, rather than fresh for good.
    fn verify_snapshot(
        &self,
        engine: &Engine,
        shadow: &CollectionMeta,
        metric: Metric,
        dim: usize,
        epoch: u64,
    ) -> bool {
        let index = {
            let entries = self.entries.lock();
            match entries.map.get(&shadow.id) {
                Some(Entry {
                    decision: Decision::Index(index), generation: UNVERIFIED, ..
                }) if entries.epoch(shadow.id) == epoch => Arc::clone(index),
                _ => return false,
            }
        };
        let (Ok(current), Ok(renames)) =
            (count_vectors(engine, shadow), engine.vector_renames(shadow.id))
        else {
            return false;
        };
        if current != index.len() || renames != index.renames() {
            debug!(
                snapshot = index.len(),
                current,
                snapshot_renames = index.renames(),
                renames,
                "snapshot is behind the store; rebuilding while it serves"
            );
            return false;
        }
        let mut entries = self.entries.lock();
        if entries.epoch(shadow.id) != epoch {
            return true;
        }
        if let Some(entry) = entries.map.get_mut(&shadow.id)
            && entry.generation == UNVERIFIED
            && entry.created == shadow.created
            && entry.metric == metric
            && entry.dim == dim
        {
            entry.generation = PROCESS_START;
            entry.decided = Instant::now();
        }
        true
    }

    /// A test hook, cloned out in its own statement so the guard is gone
    /// before the hook runs: a hook that parks must not park the lock too.
    #[cfg(test)]
    fn run_hook(&self, hook: &Mutex<Option<BuildHook>>, collection: CollectionId) {
        let hook = hook.lock().clone();
        if let Some(hook) = hook {
            hook(collection);
        }
    }

    /// How many builds run at once; zero is taken as one.
    pub fn set_build_concurrency(&self, builds: usize) {
        self.max_builds.store(builds.max(1), Ordering::Relaxed);
    }

    /// A turn at an exact scan of `collection` while it has no graph
    /// ([`Access::Building`]), awaited until `deadline`, or `None` at it. At
    /// most [`EXACT_SCANS_PER_COLLECTION`] run at once. Awaited, not blocked
    /// on: a search waiting for a turn holds no thread.
    pub async fn exact_scan_turn(
        &self,
        collection: CollectionId,
        deadline: Option<Instant>,
    ) -> Option<ScanTurn> {
        let turns = Arc::clone(
            self.scans
                .lock()
                .entry(collection)
                .or_insert_with(|| Arc::new(Semaphore::new(EXACT_SCANS_PER_COLLECTION))),
        );
        let turn = match deadline {
            Some(deadline) => {
                tokio::time::timeout_at(deadline.into(), turns.acquire_owned()).await.ok()?.ok()?
            }
            None => turns.acquire_owned().await.ok()?,
        };
        Some(ScanTurn { _turn: turn })
    }

    /// The search of a collection whose graph is being built
    /// ([`Access::Building`]): every stored vector scored, in one of the
    /// collection's turns ([`Self::exact_scan_turn`]) and under `deadline`,
    /// which the scan reads itself because a request's timeout cannot end a
    /// thread that does not yield. The turn is held for the whole scan, and
    /// the scan, a walk, runs off the async worker. Past the deadline, waiting
    /// or scanning, [`VectorError::DeadlineExceeded`].
    pub async fn scan_while_building(
        &self,
        engine: &Engine,
        shadow: &CollectionMeta,
        query: &[f32],
        options: &crate::search::SearchOptions,
        allowed: Option<&HashSet<kimmy_core::DocId>>,
        deadline: Option<Instant>,
    ) -> Result<Vec<crate::search::Hit>> {
        let Some(turn) = self.exact_scan_turn(shadow.id, deadline).await else {
            return Err(VectorError::DeadlineExceeded);
        };
        let hits = kimmy_storage::blocking(|| {
            #[cfg(test)]
            {
                let hook = self.scan_hook.lock().clone();
                if let Some(hook) = hook {
                    let taken = self
                        .scans
                        .lock()
                        .get(&shadow.id)
                        .map_or(0, |t| EXACT_SCANS_PER_COLLECTION - t.available_permits());
                    hook(taken);
                }
            }
            crate::search::vector_search_until(engine, shadow, query, options, allowed, deadline)
        });
        drop(turn);
        hits
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

    /// How many resident graphs came from a snapshot that is behind the
    /// store, or not yet judged, and are served while a builder rebuilds or
    /// judges them — the `kimmy_vector_graphs_stale` gauge (ADR-241). A
    /// roll to 0.52.0 raises it to every vector collection a member serves
    /// and it falls as the rebuilds land.
    pub fn stale_graphs(&self) -> u64 {
        let entries = self.entries.lock();
        entries
            .map
            .values()
            .filter(|e| {
                matches!(e.decision, Decision::Index(_))
                    && matches!(e.generation, BEHIND | UNVERIFIED)
            })
            .count() as u64
    }

    /// Builds queued or running: one per collection, whether or not it has a
    /// graph resident meanwhile — the `kimmy_vector_graph_builds_pending`
    /// gauge. It counts what [`Self::stale_graphs`] cannot: a collection with
    /// no graph at all, scanned until its build lands, and one whose adopted
    /// graph the budget evicted while its rebuild waited (ADR-245).
    pub fn builds_pending(&self) -> u64 {
        self.queue.lock().pending.len() as u64
    }

    /// The highest sampled score among the resident graphs this process
    /// built: of [`crate::index`]'s 128 probes, how many could not find
    /// themselves at the build check's effort — the
    /// `kimmy_vector_graph_unreached_sample` gauge. A graph loaded from a
    /// snapshot was not sampled here and counts 0.
    pub fn unreached_sample(&self) -> u64 {
        let entries = self.entries.lock();
        entries
            .map
            .values()
            .filter_map(|e| match &e.decision {
                Decision::Index(index) => Some(index.unreached() as u64),
                Decision::TooSmall => None,
            })
            .max()
            .unwrap_or(0)
    }

    /// Where one collection's snapshot lives, when snapshots are on at all.
    fn snapshot_path(&self, collection: CollectionId) -> Option<std::path::PathBuf> {
        // Hex of the derived collection id: stable across restarts and nodes
        // because ids are derived from names (ADR-031), and free of anything
        // that needs escaping in a filename.
        self.snapshot_dir.as_ref().map(|dir| dir.join(format!("{:016x}", collection.0)))
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
    /// stored now, and the shadow's moves of chunk ids since it was built
    /// ([`Engine::vector_renames`], ADR-239), which keep the count and change
    /// the keys. Both equal adopt the snapshot as fresh. The count is a walk
    /// of the collection, so it is a builder's (ADR-241): the snapshot is
    /// served meanwhile, marked [`UNVERIFIED`], and the builder either
    /// re-stamps it fresh or rebuilds it. The corner this
    /// accepts, on purpose: a delete-and-add while the node was down leaves
    /// the count equal, and that snapshot serves as fresh until the next
    /// vector write bumps the generation. Same class of bound as the
    /// 30-second staleness window, with a longer clock.
    ///
    /// A snapshot judged behind is still adopted — serving a stale graph is
    /// bounded recall loss, never wrong data, and it answers this query
    /// instantly — but marked [`BEHIND`], so that its rebuild is queued and it
    /// serves until the rebuild replaces it.
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

        // A graph `hnsw_rs` 0.3.4 built, saved by 0.51 or earlier, is behind
        // whatever the counts say: it left part of the collection where no
        // search reaches it (ADR-241). It is served while its rebuild runs,
        // as any behind snapshot is.
        if !index.built_by_this_build() {
            tracing::info!(
                ?path,
                vectors = index.len(),
                "adopting an HNSW snapshot a build before 0.52.0 made as behind; it is \
                 served until its rebuild replaces it"
            );
            return Some(Entry::new(
                Decision::Index(Arc::new(index)),
                BEHIND,
                shadow.created,
                metric,
                dim,
                Instant::now() - MAX_STALENESS,
            ));
        }
        let (generation, decided) = match self.saved_at.lock().get(&shadow.id).copied() {
            Some(saved_at) if saved_at == generation => (generation, Instant::now()),
            Some(saved_at) => {
                debug!(
                    saved_at,
                    generation,
                    "snapshot predates a write this process made; serving it until its \
                     rebuild replaces it"
                );
                (BEHIND, Instant::now() - MAX_STALENESS)
            }
            // A previous process's snapshot: judged by the vector count and the
            // moves of chunk ids since its build, which is a walk of the
            // collection, so not here. It is served while a builder counts
            // (`Shared::build`), and either adopted as fresh or rebuilt.
            None => (UNVERIFIED, Instant::now() - MAX_STALENESS),
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

    /// Build the graph or the "too small" verdict.
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

        Ok(Decision::Index(Arc::new(index)))
    }

    /// Persist a built graph, remembering `generation` — the generation the
    /// build was decided at — as the one its snapshot is good for. Every
    /// successful build is saved, so whatever graph a restart finds is the
    /// newest one that existed; a failure to save costs the next process a
    /// rebuild, not a search an answer.
    fn save(&self, shadow: &CollectionMeta, index: &HnswIndex, generation: u64) {
        let Some(path) = self.snapshot_path(shadow.id) else { return };
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

    /// Forget a collection's index unless it was built for the shape the
    /// collection is configured with now.
    ///
    /// The reconfiguration counterpart of [`Self::forget_unless_created`].
    /// A `ConfigureVectors` entry keeps the shadow and writes no vector, so
    /// a graph built for the previous configuration matches the cache on
    /// every test but its shape; and, as with a drop, the entry is history
    /// by the time the change-feed consumer reads it — the same entry
    /// arrives again in every anti-entropy round that re-scans it, long
    /// after the graph has been rebuilt for the configuration in force. So
    /// the caller reads that configuration from the store and asks here
    /// whether what is held matches it, rather than forgetting on the
    /// entry's say-so.
    ///
    /// A resident entry of the given shape means **nothing happens at all**:
    /// no epoch bump, no look at the disk, `false` back. That is the
    /// re-delivered case, and doing anything with it would be the cost this
    /// method exists to remove — one rebuild of a multi-second graph per
    /// round, per member, for as long as replication lag spans the
    /// reconfiguration. The disk is not looked at because the entry answers
    /// for it: a graph of the shape in force was installed by the build that
    /// renamed its snapshot over the directory, or by the load that checked
    /// the directory's shape on the way in; a save that failed can leave an
    /// old-shape file behind a new-shape entry, and that is `load`'s to
    /// refuse, as it does for every other snapshot it cannot vouch for.
    ///
    /// A resident entry of another shape goes, with its `saved_at`, and the
    /// snapshot directory is then checked on its own `meta.json`, as it is
    /// when nothing was resident at all. That second case is not idle: a
    /// graph evicted under the budget leaves its snapshot on disk with no
    /// entry to be forgotten through, and a snapshot of the old shape would
    /// otherwise sit there until a search on this member opened it — which a
    /// collection searched only on other members never does. A `meta.json`
    /// that cannot be read is left alone, as everywhere.
    ///
    /// The fence rule: a forget fences, a no-op does not. Removing the
    /// resident entry moves the epoch under the same lock, so a build that
    /// captured the epoch before it will decline to install — without that,
    /// a build of the old shape already under way would install a graph
    /// `serve` declines at every search, and the resident bytes would stay
    /// until eviction. Nothing resident with a build queued or running moves
    /// it too: a build asked for before the reconfiguration is of the old
    /// shape, and would run, save and install a graph `serve` declines while
    /// the build of the shape in force waited behind it (ADR-245). A match,
    /// or nothing resident and nothing queued, moves nothing.
    ///
    /// The directory is touched only under the collection's build lock,
    /// taken with `try_lock` and skipped when held, for the reason
    /// [`Self::forget_unless_created`] gives: a build in progress renames
    /// its own result over it when it finishes.
    ///
    /// Returns whether anything was forgotten, resident or on disk.
    pub fn forget_unless_shaped(
        &self,
        collection: CollectionId,
        metric: Metric,
        dim: usize,
    ) -> bool {
        let mut forgot = false;
        // Read before the entries, so the two locks are never held together.
        let queued = self.queue.lock().pending.get(&collection).copied();
        {
            let mut entries = self.entries.lock();
            match entries.map.get(&collection) {
                Some(entry) if entry.metric == metric && entry.dim == dim => return false,
                Some(_) => {
                    if let Some(entry) = entries.map.remove(&collection) {
                        entries.resident -= entry.bytes();
                    }
                    entries.warned.remove(&collection);
                    entries.fence(collection);
                    forgot = true;
                }
                // Nothing resident, and a build of another shape queued or
                // running: one asked for before the reconfiguration, which
                // would run, save and install a graph `serve` declines while
                // the build of the shape in force waited behind it. A build of
                // the shape in force, which a re-delivered configuration finds
                // queued, is the one wanted, and is left alone.
                None if queued.is_some_and(|shape| shape != (metric, dim)) => {
                    entries.fence(collection)
                }
                None => {}
            }
        }
        if forgot {
            // As in `forget_unless_created`: the entry gone, the count check
            // in `try_snapshot` judges the next snapshot, and the build-lock
            // slot stays because the id is live.
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
            // Filesystem work from the change consumer, on a runtime worker:
            // off the worker, as `invalidate` is.
            let removed = kimmy_storage::blocking(|| remove_if_other_shape(&path, metric, dim));
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
    let Some(built_for) = crate::index::snapshot_identity(path) else { return false };
    if built_for.created == created {
        return false;
    }
    remove_snapshot(
        path,
        "removed an HNSW snapshot a previous collection of the same name left behind",
    )
}

/// Remove the snapshot at `path` if its `meta.json` says it was built at a
/// shape other than `(metric, dim)`. Whether it did.
///
/// The shape counterpart of [`remove_if_other_incarnation`], on the same
/// rule for a file it cannot read: unreadable is not "other", and is left
/// for `try_snapshot`, which discards what it cannot load.
fn remove_if_other_shape(path: &std::path::Path, metric: Metric, dim: usize) -> bool {
    let Some(built_for) = crate::index::snapshot_identity(path) else { return false };
    if built_for.metric == metric && built_for.dim == dim {
        return false;
    }
    remove_snapshot(
        path,
        "removed an HNSW snapshot built for a configuration the collection no longer has",
    )
}

/// The removal the two `remove_if_*` checks share, with `why` as the line
/// that says so.
fn remove_snapshot(path: &std::path::Path, why: &'static str) -> bool {
    match std::fs::remove_dir_all(path) {
        Ok(()) => {
            debug!(?path, "{why}");
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
    // A builder's walk (ADR-241), which the node's stop ends.
    engine.for_each_vector(shadow, kimmy_storage::WalkScope::Background, |_, _| {
        n += 1;
        Ok(true)
    })?;
    Ok(n)
}

#[cfg(test)]
mod tests {
    use kimmy_core::{ChunkConfig, DocId, Hlc, ProviderConfig, VectorConfig, VectorRecord};

    use super::*;

    /// [`IndexCache::access`], waiting for the build when there was nothing to
    /// serve: what the searches that follow a first one see once its graph is
    /// built. A test of what a search sees *while* a build runs calls
    /// `cache.access` itself.
    fn access(
        cache: &IndexCache,
        engine: &Arc<Engine>,
        shadow: &CollectionMeta,
        metric: Metric,
        dim: usize,
    ) -> Access {
        match cache.access(engine, shadow, metric, dim) {
            Access::Building => {
                cache.settle();
                cache.access(engine, shadow, metric, dim)
            }
            other => other,
        }
    }

    fn setup(count: usize) -> (Arc<Engine>, CollectionMeta, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
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
                        config: None,
                    }],
                )
                .unwrap();
        }
        shadow
    }

    /// A vector collection with nothing written to it yet, so its generation
    /// counter has not moved.
    fn empty_collection(engine: &Engine, name: &str) -> CollectionMeta {
        // A previous collection of this name may have been dropped: its
        // purge finished, as the drop purger would, before the name is
        // created again (ADR-189).
        engine.finish_purges_now().unwrap();
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
        assert!(matches!(access(&cache, &engine, &shadow, Metric::Cosine, 4), Access::Exact));
    }

    #[test]
    fn the_too_small_verdict_is_cached() {
        // The count behind that verdict is O(n). Repeating it per query would
        // make the check that exists to avoid a full scan perform one.
        let (engine, shadow, _dir) = setup(5);
        let cache = IndexCache::new();
        access(&cache, &engine, &shadow, Metric::Cosine, 4);
        assert_eq!(cache.len(), 1, "the verdict should be remembered, not recomputed");
    }

    #[test]
    fn an_unsupported_metric_uses_an_exact_scan() {
        // Dot has no approximate index; the query must still work.
        let (engine, shadow, _dir) = setup(5);
        let cache = IndexCache::with_min_vectors(1);
        assert!(matches!(access(&cache, &engine, &shadow, Metric::Dot, 4), Access::Exact));
        assert!(cache.is_empty(), "an unsupported metric needs no cache entry");
    }

    #[test]
    fn a_large_collection_builds_and_reuses_one_index() {
        let (engine, shadow, _dir) = setup(60);
        // A lowered threshold keeps the fixture small while still crossing it.
        let cache = IndexCache::with_min_vectors(10);

        let first = access(&cache, &engine, &shadow, Metric::Cosine, 4);
        assert!(matches!(first, Access::Approximate(_)));
        assert_eq!(cache.len(), 1);

        let Access::Approximate(a) = access(&cache, &engine, &shadow, Metric::Cosine, 4) else {
            panic!("expected the cached index");
        };
        let Access::Approximate(b) = access(&cache, &engine, &shadow, Metric::Cosine, 4) else {
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

        let Access::Approximate(cosine) = access(&cache, &engine, &shadow, Metric::Cosine, 4)
        else {
            panic!("expected an index");
        };
        assert_eq!(cosine.metric(), Metric::Cosine);

        let Access::Approximate(euclidean) = access(&cache, &engine, &shadow, Metric::Euclidean, 4)
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

        let Access::Approximate(old) = access(&cache, &engine, &shadow, Metric::Cosine, 4) else {
            panic!("expected an index");
        };
        assert_eq!(old.dim(), 4);

        if let Access::Approximate(served) = access(&cache, &engine, &shadow, Metric::Cosine, 8) {
            assert!(
                !Arc::ptr_eq(&old, &served),
                "the graph built at width 4 was served for a configuration of width 8"
            );
            assert_eq!(served.dim(), 8);
        }
        assert_eq!(cache.len(), 1, "one entry per collection, whatever shape it holds");
        assert_eq!(cache.entries.lock().map[&shadow.id].dim, 8, "the entry records the new width");
    }

    /// At most [`EXACT_SCANS_PER_COLLECTION`] exact scans of one collection
    /// run at once (ADR-245): a third waits for a turn until its deadline and
    /// is refused at it, takes the turn a finished scan gives back, and
    /// another collection's scans are not counted against it.
    #[tokio::test]
    async fn a_collections_exact_scans_take_turns_until_their_deadline() {
        let cache = IndexCache::new();
        let (a, b) = (CollectionId(1), CollectionId(2));
        let soon = || Some(Instant::now() + Duration::from_millis(50));
        let first = cache.exact_scan_turn(a, soon()).await.expect("a turn");
        let second = cache.exact_scan_turn(a, soon()).await.expect("a second turn");
        let started = Instant::now();
        let third = tokio::time::timeout(PATIENCE, cache.exact_scan_turn(a, soon()))
            .await
            .expect("a third is answered at its deadline, not when a turn comes back");
        assert!(third.is_none(), "a third at once");
        assert!(started.elapsed() >= Duration::from_millis(50), "refused at its deadline");
        assert!(cache.exact_scan_turn(b, soon()).await.is_some(), "another collection's own turns");
        let giving_back = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            drop(first);
        };
        let (_, third) =
            tokio::join!(giving_back, cache.exact_scan_turn(a, Some(Instant::now() + PATIENCE)));
        assert!(third.is_some(), "the third took the turn the first gave back");
        drop((second, third));
        let turns = Arc::clone(&cache.scans.lock()[&a]);
        assert_eq!(turns.available_permits(), EXACT_SCANS_PER_COLLECTION, "every turn back");
    }

    /// A scan of a collection whose graph is being built holds its turn
    /// while it reads, and gives it back when it is done (ADR-245).
    #[tokio::test]
    async fn a_scan_holds_its_turn_while_it_reads() {
        let (engine, shadow, _dir) = setup(60);
        let cache = IndexCache::new();
        let seen = Arc::new(AtomicUsize::new(usize::MAX));
        *cache.scan_hook.lock() = Some(Arc::new({
            let seen = Arc::clone(&seen);
            move |taken| seen.store(taken, Ordering::SeqCst)
        }));
        let options =
            crate::search::SearchOptions { k: 3, metric: Metric::Cosine, per_document: 1 };
        let later = Some(Instant::now() + PATIENCE);
        let hits = cache
            .scan_while_building(&engine, &shadow, &[1.0, 1.0, 0.0, 0.0], &options, None, later)
            .await
            .unwrap();
        assert_eq!(hits.len(), 3);
        assert_eq!(seen.load(Ordering::SeqCst), 1, "one turn taken while the scan read");
        let turns = Arc::clone(&cache.scans.lock()[&shadow.id]);
        assert_eq!(turns.available_permits(), EXACT_SCANS_PER_COLLECTION, "and given back");
    }

    /// The stale gauge counts the graphs served while they are rebuilt: one
    /// adopted from a snapshot a previous process saved, until a builder has
    /// judged it, and none once the judgement or the rebuild lands (ADR-245).
    #[test]
    fn the_stale_gauge_counts_graphs_served_while_they_are_rebuilt() {
        let dir = tempfile::tempdir().unwrap();
        let snapshots = dir.path().join("snapshots");
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let shadow = add_collection(&engine, "docs", 60);
        let first = IndexCache::with_snapshot_dir(snapshots.clone()).min_vectors(10);
        assert!(matches!(
            access(&first, &engine, &shadow, Metric::Cosine, 4),
            Access::Approximate(_)
        ));
        assert_eq!(first.stale_graphs(), 0, "a graph this process built and judged");
        drop(first);

        let second = IndexCache::with_snapshot_dir(snapshots).min_vectors(10);
        let gate = Gate::install(&second, shadow.id);
        assert!(matches!(
            second.access(&engine, &shadow, Metric::Cosine, 4),
            Access::Approximate(_)
        ));
        gate.wait_for_build_to_start();
        assert_eq!(second.stale_graphs(), 1, "adopted, and not yet judged");
        gate.release();
        second.settle();
        assert_eq!(second.stale_graphs(), 0, "judged, and fresh");
    }

    /// A build that panics costs that build and not the builder (ADR-245):
    /// the queue's counts stay true, so settling returns, and the next search
    /// queues the build again and a builder runs it.
    #[test]
    fn a_build_that_panics_costs_that_build_and_not_the_builder() {
        let (engine, shadow, _dir) = setup(10);
        let cache = IndexCache::with_min_vectors(5);
        let panics = Arc::new(AtomicUsize::new(1));
        *cache.build_hook.lock() = Some(Arc::new({
            let panics = Arc::clone(&panics);
            move |_| {
                if panics.fetch_sub(1, Ordering::SeqCst) == 1 {
                    panic!("a build that panics");
                }
            }
        }));
        assert!(matches!(cache.access(&engine, &shadow, Metric::Cosine, 4), Access::Building));
        // Polled rather than settled, so a builder lost to the panic, with
        // its build still counted as running, fails the test rather than
        // hanging it.
        let deadline = Instant::now() + PATIENCE;
        loop {
            {
                let queue = cache.queue.lock();
                if queue.waiting.is_empty() && queue.running == 0 {
                    break;
                }
            }
            assert!(Instant::now() < deadline, "the queue never settled");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(cache.len(), 0, "the build that panicked installed nothing");
        assert!(matches!(cache.access(&engine, &shadow, Metric::Cosine, 4), Access::Building));
        cache.settle();
        assert!(matches!(
            cache.access(&engine, &shadow, Metric::Cosine, 4),
            Access::Approximate(_)
        ));
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

        assert!(matches!(access(&cache, &engine, &shadow, Metric::Cosine, 4), Access::Exact));
        assert!(matches!(access(&cache, &engine, &shadow, Metric::Cosine, 4), Access::Exact));
        assert_eq!(decisions.load(Ordering::SeqCst), 1, "the same shape reuses the verdict");

        assert!(matches!(access(&cache, &engine, &shadow, Metric::Cosine, 8), Access::Exact));
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
                    config: None,
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
        let Access::Approximate(first) = access(&cache, &engine, &shadow, Metric::Cosine, 4) else {
            panic!("expected an index");
        };
        let built_at = engine.vector_generation(shadow.id);
        assert_eq!(cache.entries.lock().map[&shadow.id].generation, built_at);

        // The chunk as a peer would send it: the vector record as
        // `put_vectors` stores it, `_id` and all, stamped by a node this
        // engine has never heard from, into the shadow collection by id.
        let source = DocId::Int64(999);
        let chunk_id = VectorRecord::id(&kimmy_core::ChunkSource::from(&source), 0);
        let record = VectorRecord {
            source: source.clone(),
            chunk: 0,
            source_hlc: Hlc::new(2, 0),
            vector: vec![1.0, 0.0, 0.0, 0.0],
            text: "replicated".into(),
            config: None,
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
        // Served stale while a builder rebuilds it (ADR-245).
        assert!(matches!(
            cache.access(&engine, &shadow, Metric::Cosine, 4),
            Access::Approximate(_)
        ));
        cache.settle();
        let Access::Approximate(second) = access(&cache, &engine, &shadow, Metric::Cosine, 4)
        else {
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

        let Access::Approximate(first) = access(&cache, &engine, &shadow, Metric::Cosine, 4) else {
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
                    config: None,
                }],
            )
            .unwrap();

        let Access::Approximate(second) = access(&cache, &engine, &shadow, Metric::Cosine, 4)
        else {
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
        access(&cache, &engine, &shadow, Metric::Cosine, 4);
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

        let Access::Approximate(index) = access(&cache, &engine, &shadow, Metric::Cosine, 4) else {
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
        IndexCache::with_snapshot_dir(dir.path().join("hnsw")).min_vectors(10)
    }

    /// Simulate a restart: reopen the engine from the same directory, which
    /// also resets the in-memory generation counters — the condition that
    /// makes snapshot validation interesting.
    fn reopen(dir: &tempfile::TempDir) -> (Arc<Engine>, CollectionMeta) {
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        (engine, shadow)
    }

    /// A stop that lands while a builder saves finds the store free (ADR-245):
    /// the builder let go of it before the save, and a build queued behind
    /// holds none while it waits. The save, which needs no store, finishes
    /// whole. The save is held open as one of 100,000 vectors runs for up to
    /// two minutes.
    #[test]
    fn a_stop_during_a_save_finds_the_store_free() {
        let (engine, shadow, dir) = setup(60);
        let other = add_collection(&engine, "other", 60);
        let cache = snapshot_cache(&dir);
        let gate = Gate::install_at(&cache.save_hook, shadow.id);
        assert!(matches!(cache.access(&engine, &shadow, Metric::Cosine, 4), Access::Building));
        gate.wait_for_build_to_start();
        assert!(matches!(cache.access(&engine, &other, Metric::Cosine, 4), Access::Building));
        assert_eq!(cache.queue.lock().waiting.len(), 1, "the other build waits behind the save");

        engine.stop_walks();
        let stop = Instant::now();
        while Arc::strong_count(&engine) > 1 {
            assert!(
                stop.elapsed() < Duration::from_millis(500),
                "the store was held {:?} after the stop began, through a save",
                stop.elapsed()
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        gate.release();
        cache.settle();
        let path = cache.snapshot_path(shadow.id).unwrap();
        assert!(path.join("meta.json").is_file(), "the save finished whole");
        assert!(!path.with_extension("build").exists(), "and left no staging behind");
        assert!(!cache.snapshot_path(other.id).unwrap().exists(), "nothing built once stopped");
    }

    /// A write this process makes between adopting a previous process's
    /// snapshot and a builder judging it is not lost (ADR-245): the edit of
    /// one document keeps the count and the keys, so the count cannot see it,
    /// and the graph is re-stamped at the generation it was adopted at, which
    /// leaves it behind and rebuilt once the window passes.
    #[test]
    fn a_write_between_adopting_a_snapshot_and_judging_it_leaves_it_behind() {
        let (engine, shadow, dir) = setup(60);
        add_collection(&engine, "other", 60);
        let first = snapshot_cache(&dir);
        assert!(matches!(
            access(&first, &engine, &shadow, Metric::Cosine, 4),
            Access::Approximate(_)
        ));
        first.settle();
        drop(first);
        drop(engine);

        let (engine, shadow) = reopen(&dir);
        let other = engine.vector_collection("app", "other").unwrap().unwrap();
        let cache = snapshot_cache(&dir);
        let gate = Gate::install(&cache, other.id);
        let _ = cache.access(&engine, &other, Metric::Cosine, 4);
        gate.wait_for_build_to_start();
        let adopted_at = engine.vector_generation(shadow.id);
        let Access::Approximate(adopted) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("the snapshot should have been adopted");
        };
        let source = DocId::Int64(7);
        engine
            .put_vectors(
                &shadow,
                &source,
                &[VectorRecord {
                    source: source.clone(),
                    chunk: 0,
                    source_hlc: Hlc::new(5, 0),
                    vector: vec![-1000.0, -1000.0, 7.0, 7.0],
                    text: "edited".into(),
                    config: None,
                }],
            )
            .unwrap();
        let live = engine.vector_generation(shadow.id);
        assert_ne!(live, adopted_at, "the premise: the edit moved the generation");
        gate.release();
        cache.settle();
        assert_eq!(
            cache.entries.lock().map[&shadow.id].generation,
            adopted_at,
            "judged at the process's start, before the edit"
        );
        cache.age(shadow.id);
        let Access::Approximate(served) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("a graph");
        };
        assert!(Arc::ptr_eq(&served, &adopted), "served while it is rebuilt");
        cache.settle();
        let Access::Approximate(rebuilt) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("a graph");
        };
        assert!(!Arc::ptr_eq(&rebuilt, &adopted), "rebuilt past the window, edit and all");
    }

    /// An edit this process makes after a restart and before the first
    /// search adopts the previous process's snapshot is not lost either
    /// (ADR-245): the snapshot predates every write this process has made, so
    /// it is judged at the process's start, and the edit, which keeps the
    /// count and the keys, leaves it behind and rebuilt once the window
    /// passes.
    #[test]
    fn an_edit_before_the_first_search_after_a_restart_leaves_the_snapshot_behind() {
        let (engine, shadow, dir) = setup(60);
        let first = snapshot_cache(&dir);
        assert!(matches!(
            access(&first, &engine, &shadow, Metric::Cosine, 4),
            Access::Approximate(_)
        ));
        first.settle();
        drop(first);
        drop(engine);
        let (engine, shadow) = reopen(&dir);
        let source = DocId::Int64(7);
        engine
            .put_vectors(
                &shadow,
                &source,
                &[VectorRecord {
                    source: source.clone(),
                    chunk: 0,
                    source_hlc: Hlc::new(5, 0),
                    vector: vec![-1000.0, -1000.0, 7.0, 7.0],
                    text: "edited".into(),
                    config: None,
                }],
            )
            .unwrap();
        let cache = snapshot_cache(&dir);
        let Access::Approximate(adopted) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("the snapshot should have been adopted");
        };
        cache.settle();
        let live = engine.vector_generation(shadow.id);
        assert_ne!(cache.entries.lock().map[&shadow.id].generation, live, "judged behind the edit");
        cache.age(shadow.id);
        let Access::Approximate(served) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("a graph");
        };
        assert!(Arc::ptr_eq(&served, &adopted), "served while it is rebuilt");
        cache.settle();
        let Access::Approximate(rebuilt) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("a graph");
        };
        assert!(!Arc::ptr_eq(&rebuilt, &adopted), "rebuilt past the window, edit and all");
    }

    /// A reconfiguration re-delivered with the shape already in force finds
    /// the build of that shape queued, and leaves it alone (ADR-245): only a
    /// queued build of another shape is fenced.
    #[test]
    fn a_re_delivered_configuration_keeps_the_queued_build_of_its_shape() {
        let (engine, a, dir) = setup(60);
        let b = add_collection(&engine, "other", 60);
        let cache = snapshot_cache(&dir);
        let gate = Gate::install(&cache, b.id);
        assert!(matches!(cache.access(&engine, &b, Metric::Cosine, 4), Access::Building));
        gate.wait_for_build_to_start();
        assert!(matches!(cache.access(&engine, &a, Metric::Cosine, 4), Access::Building));
        assert!(!cache.forget_unless_shaped(a.id, Metric::Cosine, 4), "nothing to forget");
        gate.release();
        cache.settle();
        assert!(cache.contains(a.id), "the build of the shape in force installed");
    }

    /// A drop that lands while the build is still queued, not yet running,
    /// fences it: the epoch it carries was read when it was queued
    /// (ADR-245), so it installs nothing, saves nothing and records nothing.
    #[test]
    fn a_drop_while_the_build_is_queued_installs_nothing() {
        let (engine, a, dir) = setup(60);
        let b = add_collection(&engine, "other", 60);
        let cache = snapshot_cache(&dir);
        let gate = Gate::install(&cache, b.id);
        assert!(matches!(cache.access(&engine, &b, Metric::Cosine, 4), Access::Building));
        gate.wait_for_build_to_start();
        assert!(matches!(cache.access(&engine, &a, Metric::Cosine, 4), Access::Building));
        assert_eq!(cache.queue.lock().waiting.len(), 1, "a's build is queued behind b's");
        cache.invalidate(a.id);
        gate.release();
        cache.settle();
        assert!(!cache.contains(a.id), "a build queued before the drop installed its entry");
        assert!(!cache.saved_at.lock().contains_key(&a.id), "or its saved generation");
        assert!(!cache.snapshot_path(a.id).unwrap().exists(), "or its snapshot");
    }

    /// A reconfiguration fences a build queued before it even with nothing
    /// resident (ADR-245): the build of the old shape installs nothing and
    /// saves nothing, and the next search asks for the shape in force.
    #[test]
    fn a_reconfiguration_fences_a_queued_build_with_nothing_resident() {
        let (engine, a, dir) = setup(60);
        let b = add_collection(&engine, "other", 60);
        let cache = snapshot_cache(&dir);
        let gate = Gate::install(&cache, b.id);
        assert!(matches!(cache.access(&engine, &b, Metric::Cosine, 4), Access::Building));
        gate.wait_for_build_to_start();
        assert!(matches!(cache.access(&engine, &a, Metric::Cosine, 4), Access::Building));
        assert!(!cache.contains(a.id), "the premise: nothing resident");
        assert!(!cache.forget_unless_shaped(a.id, Metric::Euclidean, 4), "nothing to forget");
        gate.release();
        cache.settle();
        assert!(!cache.contains(a.id), "the build of the old shape installed nothing");
        assert!(!cache.snapshot_path(a.id).unwrap().exists(), "and saved nothing");
        assert!(matches!(
            access(&cache, &engine, &a, Metric::Euclidean, 4),
            Access::Approximate(_)
        ));
    }

    /// The pending gauge counts the builds the stale gauge cannot (ADR-245):
    /// a collection with no graph at all, and an adopted graph the budget
    /// evicted while its rebuild waited. The stale gauge falls with the
    /// eviction; the pending gauge holds every build queued or running.
    #[test]
    fn the_pending_gauge_counts_builds_the_stale_gauge_cannot() {
        let (engine, a, dir) = setup(60);
        let b = add_collection(&engine, "b", 60);
        let c = add_collection(&engine, "c", 60);
        let first = snapshot_cache(&dir);
        for s in [&a, &b, &c] {
            assert!(matches!(
                access(&first, &engine, s, Metric::Cosine, 4),
                Access::Approximate(_)
            ));
        }
        first.settle();
        let one_graph = first.resident_bytes() / 3;
        drop(first);
        drop(engine);
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let a = engine.vector_collection("app", "docs").unwrap().unwrap();
        let b = engine.vector_collection("app", "b").unwrap().unwrap();
        let c = engine.vector_collection("app", "c").unwrap().unwrap();
        let none = add_collection(&engine, "none", 60);
        let cache = snapshot_cache(&dir);
        cache.set_max_bytes(one_graph + one_graph / 2);
        let gate = Gate::install(&cache, c.id);
        assert!(matches!(cache.access(&engine, &c, Metric::Cosine, 4), Access::Approximate(_)));
        gate.wait_for_build_to_start();
        assert!(matches!(cache.access(&engine, &a, Metric::Cosine, 4), Access::Approximate(_)));
        assert!(matches!(cache.access(&engine, &b, Metric::Cosine, 4), Access::Approximate(_)));
        assert!(matches!(cache.access(&engine, &none, Metric::Cosine, 4), Access::Building));
        assert_eq!(cache.builds_pending(), 4, "c running; a, b and the one with no graph queued");
        assert!(cache.stale_graphs() < 4, "the stale gauge sees only what is resident");
        let next = Gate::install(&cache, a.id);
        gate.release();
        next.wait_for_build_to_start();
        assert_eq!(cache.builds_pending(), 3, "a running; b and the one with no graph queued");
        assert!(!cache.contains(b.id), "the premise: b's adopted graph was evicted");
        next.release();
        cache.settle();
        assert_eq!(cache.builds_pending(), 0);
        assert_eq!(cache.stale_graphs(), 0);
    }

    /// Once the node's stop has begun no graph is built or saved: a builder's
    /// walk is background work, which the stop ends (ADR-245). The search is
    /// answered by the exact scan, and no snapshot is written.
    #[test]
    fn no_graph_is_built_or_saved_once_the_stop_has_begun() {
        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        engine.stop_walks();
        assert!(matches!(cache.access(&engine, &shadow, Metric::Cosine, 4), Access::Building));
        cache.settle();
        assert!(
            matches!(cache.access(&engine, &shadow, Metric::Cosine, 4), Access::Building),
            "a graph was built at the stop"
        );
        cache.settle();
        let path = cache.snapshot_path(shadow.id).unwrap();
        assert!(!path.exists(), "a graph was saved at the stop: {path:?}");
    }

    #[test]
    fn a_snapshot_survives_a_restart_and_is_served_not_rebuilt() {
        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        assert!(matches!(
            access(&cache, &engine, &shadow, Metric::Cosine, 4),
            Access::Approximate(_)
        ));
        cache.settle();
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
                    config: None,
                }],
            )
            .unwrap();

        let cache = snapshot_cache(&dir);
        let Access::Approximate(first) = access(&cache, &engine, &shadow, Metric::Cosine, 4) else {
            panic!("the snapshot should serve");
        };
        assert_eq!(first.len(), 60, "must be the loaded snapshot, not a rebuild");

        // ...and because it was behind the store, a builder rebuilds it to the
        // current 61 rather than letting it serve stale for a window; the
        // searches meanwhile are served the snapshot (ADR-245).
        cache.settle();
        let Access::Approximate(second) = access(&cache, &engine, &shadow, Metric::Cosine, 4)
        else {
            panic!("the rebuild should serve");
        };
        assert_eq!(second.len(), 61, "a behind snapshot is served once, then rebuilt");
    }

    #[test]
    fn a_snapshot_matching_the_store_is_adopted_as_fresh() {
        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        access(&cache, &engine, &shadow, Metric::Cosine, 4);
        cache.settle();
        drop(cache);
        drop(engine);

        let (engine, shadow) = reopen(&dir);
        let cache = snapshot_cache(&dir);
        let Access::Approximate(first) = access(&cache, &engine, &shadow, Metric::Cosine, 4) else {
            panic!("the snapshot should serve");
        };
        let Access::Approximate(second) = access(&cache, &engine, &shadow, Metric::Cosine, 4)
        else {
            panic!("the snapshot should keep serving");
        };
        assert!(Arc::ptr_eq(&first, &second), "an up-to-date snapshot must not trigger a rebuild");
    }

    #[test]
    fn a_corrupt_snapshot_is_discarded_not_trusted() {
        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        access(&cache, &engine, &shadow, Metric::Cosine, 4);
        cache.settle();
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
        let Access::Approximate(index) = access(&cache, &engine, &shadow, Metric::Cosine, 4) else {
            panic!("a corrupt snapshot must fall back to a rebuild");
        };
        assert_eq!(index.len(), 60);
    }

    /// A snapshot a 0.51 build saved, its graph built by `hnsw_rs` 0.3.4, is
    /// served once and rebuilt, never adopted as fresh (ADR-241). Its
    /// `meta.json` has no `graph` field, and its dump's layout is the same as
    /// this build's, so nothing else tells the graph with 0.3.4's unreachable
    /// vectors from a sound one. It covers exactly what the store holds, so
    /// on any other test it would be adopted as fresh; the control shows that.
    #[test]
    fn a_snapshot_0_51_saved_serves_once_and_is_rebuilt() {
        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        access(&cache, &engine, &shadow, Metric::Cosine, 4);
        cache.settle();
        drop(cache);
        drop(engine);
        let snapshot = dir.path().join("hnsw").join(format!("{:016x}", shadow.id.0));
        let meta = |path: &std::path::Path| -> serde_json::Value {
            serde_json::from_slice(&std::fs::read(path.join("meta.json")).unwrap()).unwrap()
        };
        assert_eq!(meta(&snapshot)["format"], 3, "the format is 0.51's");
        assert_eq!(meta(&snapshot)["graph"], 2, "this build says what built the graph");
        // A file a load leaves where it is and a save, which replaces the
        // directory, does not: which of the two an access did.
        let marked = |path: &std::path::Path| path.join("adopted-marker").exists();

        // The control: as saved, the snapshot is adopted as fresh, and the
        // next access serves the same graph.
        std::fs::write(snapshot.join("adopted-marker"), b"").unwrap();
        let (engine, shadow) = reopen(&dir);
        let cache = snapshot_cache(&dir);
        let Access::Approximate(first) = access(&cache, &engine, &shadow, Metric::Cosine, 4) else {
            panic!("the snapshot should serve");
        };
        let Access::Approximate(second) = access(&cache, &engine, &shadow, Metric::Cosine, 4)
        else {
            panic!("and keep serving");
        };
        assert!(Arc::ptr_eq(&first, &second), "the control: a marked snapshot is fresh");
        assert!(marked(&snapshot));
        cache.settle();
        drop((cache, first, second));
        drop(engine);

        // The same snapshot as a 0.51 build writes it: no `graph` field.
        let mut old = meta(&snapshot);
        old.as_object_mut().unwrap().remove("graph");
        std::fs::write(snapshot.join("meta.json"), serde_json::to_vec(&old).unwrap()).unwrap();
        let (engine, shadow) = reopen(&dir);
        let cache = snapshot_cache(&dir);
        let gate = Gate::install(&cache, shadow.id);
        let Access::Approximate(first) = cache.access(&engine, &shadow, Metric::Cosine, 4) else {
            panic!("0.51's snapshot should serve the search that loads it");
        };
        assert_eq!(first.len(), 60);
        gate.wait_for_build_to_start();
        assert!(marked(&snapshot), "served as it was, not rebuilt yet");
        assert!(
            matches!(cache.access(&engine, &shadow, Metric::Cosine, 4), Access::Approximate(g) if Arc::ptr_eq(&g, &first)),
            "served while its rebuild runs"
        );
        gate.release();
        cache.settle();
        let Access::Approximate(second) = access(&cache, &engine, &shadow, Metric::Cosine, 4)
        else {
            panic!("then a rebuild serves");
        };
        assert!(!Arc::ptr_eq(&first, &second), "0.51's snapshot was adopted as fresh");
        assert!(!marked(&snapshot), "the rebuild saved its own graph");
        assert_eq!(meta(&snapshot)["graph"], 2);
    }

    // -----------------------------------------------------------------------
    // A dump the library would end the process on (ADR-241)
    // -----------------------------------------------------------------------

    /// The child's instructions: `<mode>:<case>:<directory>`.
    const DUMP_CHILD: &str = "KIMMY_VECTOR_DUMP_CHILD";

    /// How `hnsw_rs` 0.3.5's loader ends the process on a dump.
    #[derive(Debug, Clone, Copy)]
    enum Ends {
        /// `std::process::exit` with this code.
        Exit(i32),
        /// An allocation of a size the file declares fails, which aborts.
        Abort,
    }

    /// A change to a saved snapshot's files.
    type SpoilDump = fn(&std::path::Path);

    /// The length of a graph file's description, from its two name lengths.
    fn description_len(graph: &[u8]) -> usize {
        let len = |at: usize| usize::from_ne_bytes(graph[at..at + 8].try_into().unwrap());
        let distance = len(39);
        47 + distance + 8 + len(47 + distance)
    }

    /// Each spoiled dump the loader ends the process on, as a change to a
    /// saved snapshot's files, and how it ends.
    fn process_ending_dumps() -> [(&'static str, SpoilDump, Ends); 3] {
        [
            // Format 2's description, which has no level scale: the loader
            // reads it, and ends the process at the first vector.
            (
                "format-2",
                |dir| {
                    let path = dir.join("index.hnsw.graph");
                    let mut bytes = std::fs::read(&path).unwrap();
                    bytes[0..4].copy_from_slice(&0x002a_677fu32.to_ne_bytes());
                    bytes.drain(6..14);
                    std::fs::write(&path, bytes).unwrap();
                },
                Ends::Exit(1),
            ),
            // A data file cut short, as a crash after a save's rename can
            // leave one: any vector the loader cannot read ends the process.
            (
                "torn-vector",
                |dir| {
                    let path = dir.join("index.hnsw.data");
                    let mut bytes = std::fs::read(&path).unwrap();
                    bytes.truncate(bytes.len() - 3);
                    std::fs::write(&path, bytes).unwrap();
                },
                Ends::Exit(1),
            ),
            // A layer's point count, allocated before a point is read.
            (
                "declared-count",
                |dir| {
                    let path = dir.join("index.hnsw.graph");
                    let mut bytes = std::fs::read(&path).unwrap();
                    let at = description_len(&bytes) + 1 + 4;
                    bytes[at..at + 8].copy_from_slice(&(1usize << 59).to_ne_bytes());
                    std::fs::write(&path, bytes).unwrap();
                },
                Ends::Abort,
            ),
        ]
    }

    /// A dump that `hnsw_rs` 0.3.5 answers by ending the process is refused
    /// before the library reads it, and rebuilt, and the process survives.
    ///
    /// Each case runs in a child process, twice: once handing the dump to the
    /// library directly, which must end that child the way the case says (the
    /// proof the fixture is the dangerous one, and a tripwire for a library
    /// upgrade that changes it), and once through the cache, which must refuse
    /// the snapshot, rebuild the graph and save one that loads. A child that
    /// the library ends fails this test with its status rather than taking
    /// the test binary with it.
    #[test]
    fn a_dump_that_would_end_the_process_is_refused_and_rebuilt_and_the_process_survives() {
        if let Ok(task) = std::env::var(DUMP_CHILD) {
            return dump_child(&task);
        }
        let name = "cache::tests::a_dump_that_would_end_the_process_is_refused_and_rebuilt_and_the_process_survives";
        for (case, spoil, ends) in process_ending_dumps() {
            let (engine, shadow, dir) = setup(60);
            let cache = snapshot_cache(&dir);
            access(&cache, &engine, &shadow, Metric::Cosine, 4);
            cache.settle();
            drop(cache);
            drop(engine);
            let snapshot = dir.path().join("hnsw").join(format!("{:016x}", shadow.id.0));
            spoil(&snapshot);
            let pristine: Vec<(String, Vec<u8>)> = ["index.hnsw.graph", "index.hnsw.data"]
                .iter()
                .map(|f| (f.to_string(), std::fs::read(snapshot.join(f)).unwrap()))
                .collect();

            for mode in ["library", "cache"] {
                // The library child does not remove what it read; the cache
                // child does. Each starts from the spoiled files.
                for (file, bytes) in &pristine {
                    std::fs::write(snapshot.join(file), bytes).unwrap();
                }
                let out = std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", name, "--nocapture", "--test-threads=1"])
                    .env(DUMP_CHILD, format!("{mode}:{case}:{}", dir.path().display()))
                    .output()
                    .unwrap();
                let said = format!(
                    "{case}, {mode}: {:?}\nstdout: {}\nstderr: {}",
                    out.status,
                    String::from_utf8_lossy(&out.stdout),
                    String::from_utf8_lossy(&out.stderr)
                );
                let stdout = String::from_utf8_lossy(&out.stdout);
                if mode == "library" {
                    match ends {
                        Ends::Exit(code) => assert_eq!(out.status.code(), Some(code), "{said}"),
                        Ends::Abort => {
                            #[cfg(unix)]
                            {
                                use std::os::unix::process::ExitStatusExt;
                                assert_eq!(out.status.signal(), Some(6), "{said}");
                            }
                            #[cfg(not(unix))]
                            assert!(!out.status.success(), "{said}");
                        }
                    }
                    assert!(!stdout.contains("CHILD-RETURNED"), "{said}");
                } else {
                    assert!(out.status.success(), "the process did not survive: {said}");
                    assert!(stdout.contains("CHILD-SURVIVED refused and rebuilt"), "{said}");
                }
            }
        }
    }

    /// The child half of the test above.
    fn dump_child(task: &str) {
        let mut parts = task.splitn(3, ':');
        let (mode, case, dir) =
            (parts.next().unwrap(), parts.next().unwrap(), parts.next().unwrap());
        let dir = std::path::Path::new(dir);
        let engine = Arc::new(Engine::open(&dir.join("kimmy.redb")).unwrap());
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let snapshot = dir.join("hnsw").join(format!("{:016x}", shadow.id.0));
        match mode {
            "library" => {
                let loaded = std::panic::catch_unwind(|| {
                    let mut io = hnsw_rs::hnswio::HnswIo::new(&snapshot, "index");
                    io.load_hnsw::<f32, hnsw_rs::prelude::DistCosine>().is_ok()
                });
                println!("CHILD-RETURNED {case}: {loaded:?}");
            }
            "cache" => {
                let cache = IndexCache::with_snapshot_dir(dir.join("hnsw")).min_vectors(10);
                let Access::Approximate(index) =
                    access(&cache, &engine, &shadow, Metric::Cosine, 4)
                else {
                    panic!("{case}: no graph");
                };
                assert_eq!(index.len(), 60, "{case}");
                let reloaded =
                    crate::index::HnswIndex::load(&snapshot, Metric::Cosine, 4, shadow.created);
                assert!(reloaded.is_ok(), "{case}: the rebuild's snapshot: {:?}", reloaded.err());
                println!("CHILD-SURVIVED refused and rebuilt {case}");
            }
            other => panic!("mode {other}"),
        }
    }

    #[test]
    fn a_reconfigured_dimension_refuses_the_old_snapshot() {
        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        access(&cache, &engine, &shadow, Metric::Cosine, 4);
        cache.settle();
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
            access(&cache, &engine, &shadow, Metric::Cosine, 4),
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
        access(&cache, &engine, &docs, Metric::Cosine, 4);
        let snapshot = cache.snapshot_path(docs.id).unwrap();
        assert!(snapshot.is_dir(), "the build should have been persisted");

        // A budget too small for two graphs: installing the other evicts docs.
        cache.set_max_bytes(1);
        access(&cache, &engine, &other, Metric::Cosine, 4);
        assert!(!cache.contains(docs.id), "docs should have been evicted for other");
        assert!(snapshot.is_dir(), "eviction keeps the snapshot");

        // Control: nothing written since the snapshot, so reloading it is
        // adopting it as fresh — and that install evicts `other` in turn.
        access(&cache, &engine, &docs, Metric::Cosine, 4);
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
                    config: None,
                }],
            )
            .unwrap();
        assert_eq!(count_vectors(&engine, &docs).unwrap(), 60, "the count must not give it away");
        let written = engine.vector_generation(docs.id);
        assert!(written > generation);

        // Evicted again, then reloaded: the snapshot predates the write and
        // must come back already stale, whatever its count says.
        access(&cache, &engine, &other, Metric::Cosine, 4);
        assert!(!cache.contains(docs.id));
        assert!(snapshot.is_dir());
        // The rebuild it asks for is held, so that what was adopted can be
        // read before the rebuild replaces it.
        let gate = Gate::install(&cache, docs.id);
        assert!(matches!(cache.access(&engine, &docs, Metric::Cosine, 4), Access::Approximate(_)));
        gate.wait_for_build_to_start();
        assert_eq!(
            cache.entries.lock().map[&docs.id].generation,
            BEHIND,
            "a snapshot written before a later write was adopted as fresh"
        );
        // And the rebuild lands at the live generation.
        gate.release();
        cache.settle();
        assert_eq!(cache.entries.lock().map[&docs.id].generation, written);
    }

    #[test]
    fn invalidating_removes_the_snapshot_too() {
        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        access(&cache, &engine, &shadow, Metric::Cosine, 4);
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
        access(&cache, &engine, &first, Metric::Cosine, 4);
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
        let Access::Approximate(index) = access(&fresh, &engine, &second, Metric::Cosine, 4) else {
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
        let Access::Approximate(built) = access(&cache, &engine, &first, Metric::Cosine, 4) else {
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
            matches!(access(&cache, &engine, &second, Metric::Cosine, 4), Access::Exact),
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
        access(&cache, &engine, &kept, Metric::Cosine, 4);
        access(&cache, &engine, &gone, Metric::Cosine, 4);
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
        access(&cache, &engine, &first, Metric::Cosine, 4);
        access(&cache, &engine, &other, Metric::Cosine, 4);
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
        access(&cache, &engine, &first, Metric::Cosine, 4);
        access(&cache, &engine, &kept, Metric::Cosine, 4);
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
        access(&cache, &engine, &gone, Metric::Cosine, 4);
        access(&cache, &engine, &kept, Metric::Cosine, 4);
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
        access(&cache, &engine, &shadow, Metric::Cosine, 4);
        cache.settle();
        drop(cache);
        drop(engine);

        let (engine, shadow) = reopen(&dir);
        let cache = snapshot_cache(&dir);
        let Access::Approximate(index) = access(&cache, &engine, &shadow, Metric::Cosine, 4) else {
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
            Self::install_at(&cache.build_hook, target)
        }

        /// The gate at a first look's snapshot load, not at a build.
        fn install_at_load(cache: &IndexCache, target: CollectionId) -> Self {
            Self::install_at(&cache.load_hook, target)
        }

        fn install_at(slot: &Mutex<Option<BuildHook>>, target: CollectionId) -> Self {
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
            *slot.lock() = Some(Arc::new(hook));
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
        engine: &'e Arc<Engine>,
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
        // The defect this guards: a build ran under the cache-wide lock, so a
        // multi-second rebuild of one collection stalled vector search on
        // every collection the node serves. Builds run on builder threads
        // now (ADR-245): while A's is parked, a search of B is answered at
        // once, by an exact scan while B waits its turn at the one builder,
        // and by B's own graph when two builders run.
        let (engine, a, _dir) = setup(60);
        let b = add_collection(&engine, "other", 60);
        let cache = IndexCache::with_min_vectors(10);
        let gate = Gate::install(&cache, a.id);

        assert!(matches!(cache.access(&engine, &a, Metric::Cosine, 4), Access::Building));
        gate.wait_for_build_to_start();
        assert!(
            matches!(cache.access(&engine, &b, Metric::Cosine, 4), Access::Building),
            "B waits its turn behind A's build, and is scanned meanwhile"
        );
        std::thread::sleep(Duration::from_millis(300));
        assert!(!cache.contains(b.id), "one builder: B's build waits for A's");
        cache.set_build_concurrency(2);
        // A second builder starts for the next build asked for; B's is queued
        // already, so the next search of B asks again and is deduplicated.
        let other = add_collection(&engine, "third", 60);
        assert!(matches!(cache.access(&engine, &other, Metric::Cosine, 4), Access::Building));
        let deadline = Instant::now() + PATIENCE;
        while !cache.contains(b.id) || !cache.contains(other.id) {
            assert!(Instant::now() < deadline, "B and the third collection were never built");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!cache.contains(a.id), "A's build is still parked");
        assert!(matches!(cache.access(&engine, &b, Metric::Cosine, 4), Access::Approximate(_)));

        gate.release();
        cache.settle();
        assert!(matches!(cache.access(&engine, &a, Metric::Cosine, 4), Access::Approximate(_)));
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn concurrent_accesses_to_one_collection_share_a_single_build() {
        // Searches arriving at an unbuilt collection must not each build it,
        // and none waits for the build (ADR-245): each is answered by an
        // exact scan, one build runs, and every search after it is served its
        // graph. The count is the proof; pointer equality is the corollary.
        let (engine, a, _dir) = setup(60);
        let cache = IndexCache::with_min_vectors(10);
        let gate = Gate::install(&cache, a.id);

        assert!(matches!(cache.access(&engine, &a, Metric::Cosine, 4), Access::Building));
        gate.wait_for_build_to_start();
        for _ in 0..3 {
            assert!(
                matches!(cache.access(&engine, &a, Metric::Cosine, 4), Access::Building),
                "a search with nothing to serve is scanned, not held for the build"
            );
        }
        assert!(
            cache.queue.lock().waiting.is_empty(),
            "the running build is the only one asked for"
        );
        gate.release();
        cache.settle();

        let Access::Approximate(x) = cache.access(&engine, &a, Metric::Cosine, 4) else {
            panic!("the build should have installed a graph");
        };
        let Access::Approximate(y) = cache.access(&engine, &a, Metric::Cosine, 4) else {
            panic!("and keep serving it");
        };
        assert!(Arc::ptr_eq(&x, &y));
        assert_eq!(gate.builds.load(Ordering::SeqCst), 1, "exactly one build");
    }

    #[test]
    fn a_caller_arriving_mid_rebuild_is_served_the_previous_graph() {
        // The staleness policy, under concurrency: a graph from before the
        // write is a correct answer with bounded recall loss, so every search
        // while the rebuild runs takes it, the one that asked for the rebuild
        // included (ADR-245), and none waits.
        let (engine, a, _dir) = setup(60);
        let cache = IndexCache::with_min_vectors(10);
        let Access::Approximate(old) = access(&cache, &engine, &a, Metric::Cosine, 4) else {
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
                    config: None,
                }],
            )
            .unwrap();
        cache.age(a.id);
        let gate = Gate::install(&cache, a.id);

        let Access::Approximate(asked) = cache.access(&engine, &a, Metric::Cosine, 4) else {
            panic!("the search that asks for the rebuild should be served the stale graph");
        };
        assert!(Arc::ptr_eq(&asked, &old));
        gate.wait_for_build_to_start();
        let Access::Approximate(served) = cache.access(&engine, &a, Metric::Cosine, 4) else {
            panic!("expected the stale graph");
        };
        assert!(Arc::ptr_eq(&served, &old), "should be served the graph that exists");
        assert_eq!(served.len(), 60);

        gate.release();
        cache.settle();
        let Access::Approximate(rebuilt) = cache.access(&engine, &a, Metric::Cosine, 4) else {
            panic!("expected the rebuilt graph");
        };
        assert_eq!(rebuilt.len(), 61, "the rebuild should see the write");
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

        assert!(matches!(cache.access(&engine, &a, Metric::Cosine, 4), Access::Building));
        gate.wait_for_build_to_start();
        cache.invalidate(a.id);
        gate.release();
        cache.settle();
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

        assert!(matches!(cache.access(&engine, &a, Metric::Cosine, 4), Access::Building));
        gate.wait_for_build_to_start();
        cache.invalidate(a.id);
        gate.release();
        cache.settle();
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
        access(&cache, &engine, &a, Metric::Cosine, 4);
        let path = cache.snapshot_path(a.id).unwrap();
        assert!(path.is_dir(), "the build should have been persisted");
        cache.settle();
        drop(cache);

        let aside = dir.path().join("aside");
        std::fs::rename(&path, &aside).unwrap();
        let cache = snapshot_cache(&dir);
        let gate = Gate::install_at_load(&cache, a.id);

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

        assert!(matches!(cache.access(&engine, &first, Metric::Cosine, 4), Access::Building));
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
        cache.settle();
        assert_nothing_installed(&cache, first.id);
    }

    #[test]
    fn a_forget_by_shape_leaves_a_graph_of_that_shape_entirely_alone() {
        // The re-delivered case: a `ConfigureVectors` entry read again, with
        // the graph already rebuilt for the configuration in force. Nothing
        // may move — not the entry, not the snapshot, not the generation it
        // was saved at, and not the epoch, or a build under way for the
        // right shape would be discarded for nothing.
        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        let Access::Approximate(before) = access(&cache, &engine, &shadow, Metric::Cosine, 4)
        else {
            panic!("60 vectors over a threshold of 10 should build a graph");
        };
        let path = cache.snapshot_path(shadow.id).unwrap();
        assert!(path.is_dir(), "the build should have been persisted");
        assert_eq!(cache.entries.lock().epoch(shadow.id), 0);

        assert!(
            !cache.forget_unless_shaped(shadow.id, Metric::Cosine, 4),
            "a graph of the configured shape is nothing to forget"
        );
        assert!(cache.contains(shadow.id), "the entry must stay");
        assert!(path.is_dir(), "the snapshot must stay");
        assert!(
            cache.saved_at.lock().contains_key(&shadow.id),
            "the generation the snapshot was saved at must stay"
        );
        assert_eq!(cache.entries.lock().epoch(shadow.id), 0, "a no-op must not fence");
        let Access::Approximate(after) = access(&cache, &engine, &shadow, Metric::Cosine, 4) else {
            panic!("the graph should still be served");
        };
        assert!(Arc::ptr_eq(&before, &after), "the same graph, not a rebuild");
    }

    #[test]
    fn a_forget_by_shape_removes_a_graph_of_another_metric_and_its_snapshot() {
        // The reconfiguration proper: the collection now scores by another
        // metric, and the graph resident here was built for the old one. It
        // goes, with its snapshot and the generation it was saved at, and the
        // epoch moves so a build of the old shape still in flight installs
        // nothing.
        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        assert!(matches!(
            access(&cache, &engine, &shadow, Metric::Cosine, 4),
            Access::Approximate(_)
        ));
        let path = cache.snapshot_path(shadow.id).unwrap();
        assert!(path.is_dir());

        assert!(
            cache.forget_unless_shaped(shadow.id, Metric::Euclidean, 4),
            "a graph of another metric must be forgotten"
        );
        assert!(!cache.contains(shadow.id), "the entry must go");
        assert!(!path.exists(), "and the snapshot with it");
        assert!(!cache.saved_at.lock().contains_key(&shadow.id));
        assert_eq!(cache.resident_bytes(), 0, "the budget must be released");
        assert_eq!(cache.entries.lock().epoch(shadow.id), 1, "a forget fences");

        // A width change is the other half of a shape, and is judged the
        // same way.
        assert!(matches!(
            access(&cache, &engine, &shadow, Metric::Cosine, 4),
            Access::Approximate(_)
        ));
        assert!(cache.forget_unless_shaped(shadow.id, Metric::Cosine, 8));
        assert!(!cache.contains(shadow.id));
        assert!(!path.exists());
    }

    #[test]
    fn a_forget_by_shape_with_nothing_resident_still_removes_a_snapshot_of_another_shape() {
        // An evicted graph, or one a previous process wrote, has no entry to
        // be forgotten through; its snapshot of the old shape would sit on
        // disk until a search on this member opened it, which a collection
        // searched only elsewhere never does. So the directory is checked on
        // its own `meta.json` even when the map has nothing.
        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        assert!(matches!(
            access(&cache, &engine, &shadow, Metric::Cosine, 4),
            Access::Approximate(_)
        ));
        let path = cache.snapshot_path(shadow.id).unwrap();
        assert!(path.is_dir());
        cache.settle();
        drop(cache);

        // A fresh cache over the same directory: the snapshot, and no entry.
        let cache = snapshot_cache(&dir);
        assert!(cache.is_empty());
        assert!(
            cache.forget_unless_shaped(shadow.id, Metric::Euclidean, 4),
            "a snapshot of another shape is something to forget"
        );
        assert!(!path.exists(), "the old shape's snapshot must go");
    }

    #[test]
    fn a_forget_by_shape_with_nothing_resident_leaves_a_snapshot_of_that_shape() {
        // The evicted counterpart of the re-delivered case: the snapshot is
        // of the configured shape, so the next search may adopt it rather
        // than rebuild, and the forget leaves it where it is.
        let (engine, shadow, dir) = setup(60);
        let cache = snapshot_cache(&dir);
        assert!(matches!(
            access(&cache, &engine, &shadow, Metric::Cosine, 4),
            Access::Approximate(_)
        ));
        let path = cache.snapshot_path(shadow.id).unwrap();
        cache.settle();
        drop(cache);

        let cache = snapshot_cache(&dir);
        assert!(cache.is_empty());
        assert!(
            !cache.forget_unless_shaped(shadow.id, Metric::Cosine, 4),
            "a snapshot of the configured shape is nothing to forget"
        );
        assert!(path.is_dir(), "the snapshot must stay for the next search to adopt");
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

        assert!(matches!(cache.access(&engine, &a, Metric::Cosine, 4), Access::Building));
        gate.wait_for_build_to_start();

        let live: HashMap<CollectionId, Hlc> = [(kept.id, kept.created)].into_iter().collect();
        assert_eq!(
            cache.forget_absent(&live),
            0,
            "nothing was resident to forget; the fence is the whole effect"
        );

        gate.release();
        cache.settle();
        assert_nothing_installed(&cache, a.id);
    }

    // -----------------------------------------------------------------------
    // Resident graphs live under a budget
    // -----------------------------------------------------------------------

    /// The graph for one of these fixtures, and what it says it costs.
    fn graph(cache: &IndexCache, engine: &Arc<Engine>, shadow: &CollectionMeta) -> Arc<HnswIndex> {
        // Instants are what eviction orders by; a pause keeps two accesses
        // from sharing one on a coarse clock.
        std::thread::sleep(Duration::from_millis(2));
        let Access::Approximate(index) = access(cache, engine, shadow, Metric::Cosine, 4) else {
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
                    config: None,
                }],
            )
            .unwrap();
        cache.age(a.id);
        // Served stale while a builder rebuilds it.
        assert!(matches!(cache.access(&engine, &a, Metric::Cosine, 4), Access::Approximate(_)));
        cache.settle();

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
        assert!(matches!(access(&cache, &engine, &small, Metric::Cosine, 4), Access::Exact));
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
                    config: None,
                }],
            )
            .unwrap();
        assert_eq!(graph(&cache, &engine, &a).len(), 60, "loaded from the snapshot, not rebuilt");
    }
}
