//! Approximate nearest-neighbour indexing.
//!
//! Exact search ([`crate::search`]) scores every vector: O(n) per query, no
//! recall loss. HNSW trades a little recall for a large speedup by walking a
//! navigable graph instead.
//!
//! Because the exact path already exists, recall is **measurable** rather than
//! assumed — the tests below compare HNSW's results against the exact answer
//! and assert a floor. That is the one genuine benefit of having built exact
//! search first.

use std::collections::HashSet;

use hnsw_rs::prelude::{DistCosine, DistL2, Hnsw};
use kimmy_core::{DocId, Hlc, Metric, VectorRecord, similarity};
use kimmy_storage::{CollectionMeta, Engine};
use tracing::debug;

use crate::error::{Result, VectorError};
use crate::search::{Hit, SearchOptions};

/// Graph tuning. These are the values the HNSW paper and most
/// implementations use as defaults; they trade build time and memory for
/// recall, and are only worth changing with recall measurements to hand.
const MAX_CONNECTIONS: usize = 16;
const EF_CONSTRUCTION: usize = 200;
const MAX_LAYERS: usize = 16;

/// How much wider than `k` to explore at query time. Higher means better
/// recall and slower queries.
const EF_SEARCH_FACTOR: usize = 4;
const MIN_EF_SEARCH: usize = 50;

/// How many stored vectors a finished build asks to find themselves.
///
/// Sized from the measurement rather than picked. A catastrophic build orphans
/// a tenth of the collection or more, which even 32 probes catch essentially
/// always. What slipped through at 32 was *mild* orphaning of a few percent,
/// where detection is `1 - 0.97^n`: 62% at 32, and 98% at 128. The cost is a
/// fixed number of searches against an O(n log n) build, so the wider net is
/// close to free; exhaustive checking would cost as much as the build itself.
const REACHABILITY_SAMPLE: usize = 128;

/// What a sampled probe found, split by what the answer means.
///
/// The split is the point. A missed probe is two different events added
/// together — a search that ran out of budget, and a point the graph cannot
/// reach at all — and only the second is data loss. An ordinary search explores
/// a fixed amount, so the first grows as a collection does while the second
/// does not, and adding them gave a threshold that could not hold at more than
/// one collection size.
#[derive(Default, Debug, Clone, Copy)]
struct Reachability {
    /// Points probed. Zero for a collection too small to sample.
    sampled: usize,
    /// Probes that did not return their own point at ordinary search effort.
    missed: usize,
    /// Of those, the ones still missing when effort stops being the limit.
    unreachable: usize,
    /// Edges the build added at layer 0 to link points it had cut off
    /// ([`link_cut_off`]).
    linked: usize,
}

/// How hard a second look is, before a miss is called data loss.
///
/// Sized from the plateau, which is also what proved the original diagnosis:
/// re-probing the missed points at 200, 400, 800 and 1600 changes the answer
/// between 200 and 800 and then stops. Past the plateau more exploration buys
/// nothing, because no amount of exploring crosses into a part of the graph
/// that has no edges leading to it.
const REACHABILITY_CONFIRM_EF: usize = 800;

/// How many neighbours the second look asks for.
///
/// Not one. Identical vectors are all at distance zero, so a `k` of one can
/// return a different point that is exactly as close and report a miss that is
/// not one.
const REACHABILITY_CONFIRM_K: usize = 32;

/// How many unreachable points in a sample mean the graph lost data, rather
/// than being the graph HNSW ordinarily produces.
///
/// **Not zero, and not for the reason this comment used to give.** An earlier
/// revision said a healthy miss was a vector among near-duplicates getting
/// edged out by a neighbour — approximation working as designed. That was
/// assumed, and it is wrong: re-probed at [`REACHABILITY_CONFIRM_EF`] those
/// points are still not found, so they are not edged out, they are genuinely
/// unreachable. **Every graph this builds orphans a little.** Over 599 healthy
/// builds at 400 vectors, checking every point rather than a sample, the true
/// orphaned fraction ran **0.8% to 3.0%** — never zero.
///
/// So the threshold separates *routine* orphaning from *catastrophic*
/// orphaning, and both sides are measured:
///
/// | | true orphaned | sampled score |
/// |---|---|---|
/// | 599 healthy builds | 0.8%–3.0% | 0–5, median 1 |
/// | the 1 catastrophic build | 14.8% | **22** |
///
/// Across a further 126 builds spanning 16 and 384 dimensions and 500 to 8,000
/// vectors, no healthy build scored above 7. **Eight sits above every healthy
/// observation and far below the only bad one**, and a catastrophic build at
/// the mildest fraction on record (10%) still expects ~13 of 128 — with
/// [`MAX_BUILD_ATTEMPTS`] fresh draws behind it.
///
/// **This was 3, sized against a 400-vector, 16-dimensional fixture, and the
/// shape depends on both.** At 384 dimensions — the realistic embedding width —
/// misses grow steeply with collection size, so `3` fired on 7 builds in 10 at
/// 4,000 vectors: every such build was discarded, rebuilt twice, and then
/// reported as losing data. A 3× cost and a false alarm, on a healthy graph.
/// [`REACHABILITY_CONFIRM_EF`] removes most of that growth and this threshold
/// covers the rest.
const REACHABILITY_MAX_UNREACHABLE: usize = 8;

/// How many times a build that loses data is discarded and retried.
///
/// The layer draw is independent each time, so at a measured ~0.4% failure
/// rate two retries take the odds to roughly one in ten million. It is
/// bounded because an index that genuinely cannot be built well — a
/// pathological collection rather than an unlucky draw — must still return
/// something rather than loop.
const MAX_BUILD_ATTEMPTS: usize = 2;

/// How many of a cut-off point's nearest linked points get an edge into it
/// ([`link_cut_off`]). Two, so that one later removal of a list entry cannot
/// cut it off again; one more edge each costs nothing measurable.
const LINK_EDGES: usize = 2;

/// How wide the search for a cut-off point's nearest linked points looks.
/// The candidates only have to be near and linked, not the true nearest: an
/// edge from any linked point makes the point reachable.
const LINK_CANDIDATES: usize = 16;
const LINK_EF: usize = 100;

/// How many rounds of linking a build makes before it stops. Every measured
/// build needed one: an edge is only ever added, so a point linked stays
/// linked, and a round can only leave a point cut off whose candidates were
/// all cut off too.
const LINK_ROUNDS: usize = 4;

/// What a snapshot must say about itself to be trusted.
///
/// The keys ride along because graph ids are dense integers whose meaning
/// lives in this map — a graph without it can find neighbours but cannot name
/// them.
#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotMeta {
    format: u32,
    metric: Metric,
    dim: usize,
    /// The `created` stamp of the shadow collection this graph was built from
    /// — *which* collection it describes, not merely which name.
    ///
    /// A collection id is derived from its name, so a name that is dropped and
    /// created again gets the same id, the same snapshot path, and would adopt
    /// its predecessor's graph. `CollectionMeta` already carries `created` for
    /// exactly this reason and `sync.rs` already discriminates a stale drop by
    /// it; this is the same discriminator at the snapshot's trust boundary.
    /// `created` rather than `incarnation_floor`, which is `None` on a first
    /// creation and so cannot tell two incarnations apart when it matters
    /// most.
    ///
    /// Defaulted on read so that a format-1 file, which has no such field,
    /// parses far enough for the format check below to name what is wrong,
    /// rather than failing on a missing field first. Either way it is refused.
    #[serde(default)]
    created: Hlc,
    keys: Vec<String>,
    /// [`Engine::vector_renames`] of the shadow when the graph was built: a
    /// move of chunks to other ids since (an adoption, or a re-embed that took
    /// a document off its 0.50 ids) keeps the count and changes the keys, so
    /// a previous process's graph is judged by this as well as by the count
    /// (ADR-239). Defaulted, for a snapshot without it, which format 3 refuses
    /// anyway when it is older.
    #[serde(default)]
    renames: u64,
}

/// Bumped when the snapshot layout changes; a mismatch is a rebuild, not a
/// migration.
///
/// 2 added `created`. Every snapshot written by an earlier build is refused
/// and rebuilt on its first access, which is the ordinary cost of a format
/// change and cheaper than trusting a graph that cannot say which incarnation
/// of a name it was built for.
///
/// 3 keys the graph by the id each chunk is stored under (ADR-239). A format-2
/// graph's keys were recomputed from the records under 0.50's chunk ids, which
/// named every binary `_id` of one length alike and miss every chunk this build
/// writes for a binary or for a String whose id changed; adopted on an equal
/// count, it would serve them for as long as the count held. The cost is one
/// rebuild per vector collection per member, on its first search after the
/// upgrade. A 0.50 build refuses a format-3 snapshot in turn, and rebuilds.
///
/// 4 is a graph `hnsw_rs` 0.3.5 built (ADR-241). The dump's layout did not
/// change, so 0.3.5 would read a format-3 snapshot; what changed is the graph
/// in it. 0.3.4 stored each reverse edge at the new point's top layer instead
/// of the layer it was found on, which left 0.5% to 3.8% of every collection
/// of 500 vectors or more that no search reaches, and a format-3 graph adopted
/// on an equal count would keep them unreachable until the collection's next
/// write. Refused, it is rebuilt once per vector collection per member, on the
/// collection's first search. A 0.51 build refuses a format-4 snapshot in
/// turn, before its library reads the dump, and rebuilds.
const SNAPSHOT_FORMAT: u32 = 4;

/// What the snapshot at `dir` says it was built for, read from its
/// `meta.json` and nothing else: which incarnation, and at which shape.
///
/// For the cache's reconciliation passes, which need to know whether a
/// directory belongs to the collection standing under its id — and, since a
/// reconfiguration keeps the collection, whether it describes the shape that
/// collection now has — *without* loading the graph: a load is the cost the
/// snapshot exists to avoid, and a sweep at startup would pay it once per
/// directory. `None` when the file is missing or does not parse; what the
/// caller does with that is its own decision, and `load` refuses such a
/// snapshot in any case.
pub(crate) fn snapshot_identity(dir: &std::path::Path) -> Option<SnapshotIdentity> {
    let raw = std::fs::read(dir.join("meta.json")).ok()?;
    let meta: SnapshotMeta = serde_json::from_slice(&raw).ok()?;
    Some(SnapshotIdentity { created: meta.created, metric: meta.metric, dim: meta.dim })
}

/// The part of a snapshot's `meta.json` that says *what* it is a snapshot
/// of, for [`snapshot_identity`]. The rest of the file (format, keys) is
/// `load`'s concern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SnapshotIdentity {
    /// The incarnation of the shadow collection the graph was built from.
    pub(crate) created: Hlc,
    /// The metric the graph scores with.
    pub(crate) metric: Metric,
    /// The width of every vector in it.
    pub(crate) dim: usize,
}

/// Reload one graph from a leaked io handle, once [`crate::dump::check`] has
/// passed its files, containing `hnsw_rs`'s panics.
///
/// The library `unwrap`s and asserts on a corrupt dump, and a torn file on
/// disk must cost a rebuild, never the node. Found by the corrupt-snapshot
/// test, the same way `DistDot`'s assert was found: by testing the failure
/// path rather than trusting the library's. **`catch_unwind` is not enough on
/// its own** (ADR-241): the loader also ends the process (`exit(1)`) and
/// aborts it (an allocation of a declared size), which no unwind sees, and so
/// the files are checked first, by `check`. The catch stays for whatever that
/// check does not foresee. `AssertUnwindSafe` is sound here: the closure
/// touches only the leaked io and the files it reads — no engine state a
/// mid-load panic could tear.
fn load_graph<D>(dir: &std::path::Path) -> std::result::Result<Hnsw<'static, f32, D>, String>
where
    D: hnsw_rs::anndists::dist::Distance<f32> + Default + Send + Sync,
{
    let dir = dir.to_path_buf();
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        // The io handle is leaked *inside* the closure — a captured `&mut`
        // reborrows at the closure's lifetime, which the invariant
        // `Hnsw<'static>` refuses; a local born `'static` does not have that
        // problem. One small allocation per collection per process, bounded
        // and deliberate, in exchange for not pulling in a
        // self-referential-struct dependency to hold the pair.
        let io = Box::leak(Box::new(hnsw_rs::hnswio::HnswIo::new(&dir, crate::dump::BASENAME)));
        io.load_hnsw::<f32, D>().map_err(|e| e.to_string())
    }))
    .unwrap_or_else(|_| Err("reload panicked; the file is corrupt".to_string()))
}

/// The graph, one variant per distance.
///
/// `hnsw_rs` takes the distance as a type parameter, so the choice has to be
/// made at construction rather than carried as data.
enum Graph {
    Cosine(Hnsw<'static, f32, DistCosine>),
    Euclidean(Hnsw<'static, f32, DistL2>),
}

/// What one graph node costs beyond its vector, approximately.
///
/// **Measured, not derived** — see [Benchmarks](../../../docs/benchmarks.md).
/// Building 20,000 pseudo-random vectors under a counting allocator, the
/// graph held 6,512 bytes per node at 384 dimensions and 5,315 at 64. Take
/// the vector out of each (1,536 and 256 bytes) and what is left is the same
/// in both — 4,976 and 5,059 — which is `hnsw_rs`'s bookkeeping: the node (a
/// `Vec` of sixteen per-layer neighbour lists, allocated for every point
/// whatever its level), the neighbour lists themselves (up to
/// `2 × MAX_CONNECTIONS` at layer 0, each an `Arc` to a small heap cell, in
/// `Vec`s that grow by doubling), and the layer tables. It does not depend on
/// the width, which is why it is a constant added to `dim × 4` rather than a
/// multiplier; at 4,000 vectors it measured within 1% of the 20,000 figure.
///
/// The figure is for the graph the build produces; a snapshot reloaded from
/// disk has the same shape. It is an estimate for a budget, not an
/// accounting — the allocator's own overhead and fragmentation sit on top.
const NODE_OVERHEAD_BYTES: usize = 5_000;

/// Estimated resident bytes of a graph over `keys`, at `dim` dimensions.
///
/// `n × (dim × 4 + NODE_OVERHEAD_BYTES) + Σ (key length + 24)`: the vectors
/// the graph copies in, the per-node bookkeeping above, and the key table
/// that names each node (a `String` is 24 bytes of header plus its bytes).
/// The same formula is documented in `docs/vectors.md`, so an operator can
/// size `vector.index_cache.max_bytes` from a chunk count.
fn estimate_bytes(keys: &[String], dim: usize) -> usize {
    let per_node = dim * 4 + NODE_OVERHEAD_BYTES;
    let key_bytes: usize = keys.iter().map(|k| k.len() + 24).sum();
    keys.len() * per_node + key_bytes
}

/// Layer 0 of a graph, read from its structure: for each graph id, whether
/// no edge leads into it, and whether the entry point's component at layer 0
/// does not hold it (no path of edges leads to it from the entry point).
///
/// Every search ends its descent at some point of layer 0 and walks the edges
/// from there, so a point with no edge into it is returned only when a
/// descent ends on it, and a point outside the entry point's component only
/// from a descent that ends inside its own. Either is a point the graph has
/// lost. The entry point is the first point inserted at the top level: the
/// library replaces it only on a strictly higher one, and graph ids are
/// insertion order.
struct Layer0 {
    no_edge_in: Vec<bool>,
    outside: Vec<bool>,
}

impl Layer0 {
    fn read<D>(graph: &Hnsw<'static, f32, D>) -> Self
    where
        D: hnsw_rs::anndists::dist::Distance<f32> + Send + Sync,
    {
        let n = graph.get_nb_point();
        let mut level = vec![0u8; n];
        let mut edges: Vec<Vec<usize>> = vec![Vec::new(); n];
        for point in graph.get_point_indexation() {
            let at = point.get_origin_id();
            if at >= n {
                continue;
            }
            level[at] = point.get_point_id().0;
            edges[at] = point.get_neighborhood_id()[0].iter().map(|e| e.d_id).collect();
        }
        let mut into = vec![false; n];
        for (at, out) in edges.iter().enumerate() {
            for &to in out {
                if to != at && to < n {
                    into[to] = true;
                }
            }
        }
        let top = level.iter().copied().max().unwrap_or(0);
        let mut reached = vec![false; n];
        if let Some(entry) = (0..n).find(|&i| level[i] == top) {
            // The entry point needs no edge into it: every search starts there.
            into[entry] = true;
            reached[entry] = true;
            let mut stack = vec![entry];
            while let Some(at) = stack.pop() {
                for &to in &edges[at] {
                    if to < n && !std::mem::replace(&mut reached[to], true) {
                        stack.push(to);
                    }
                }
            }
        }
        Self {
            no_edge_in: into.iter().map(|linked| !linked).collect(),
            outside: reached.iter().map(|r| !r).collect(),
        }
    }

    /// The graph ids the graph has lost, in id order.
    fn cut_off(&self) -> Vec<usize> {
        (0..self.outside.len()).filter(|&i| self.no_edge_in[i] || self.outside[i]).collect()
    }
}

/// Give every point a build left cut off at layer 0 ([`Layer0`]) an edge into
/// it from [`LINK_EDGES`] of its nearest linked points, until none is cut off
/// (ADR-241). Returns how many edges were added.
///
/// **Why a build leaves any.** `hnsw_rs` adds a new point to each of its
/// neighbours' lists and, when a list is over its cap, drops the farthest
/// entry, which can be the new point; a point dropped by every neighbour has
/// no edge into it. 0.3.5 left 0.1% of a collection of low intrinsic
/// dimension like that at 10,000 to 100,000 vectors, and 1.5% to 8% of
/// uniformly random 384-dimensional ones; no search returns them. The edge is
/// added past the cap, never by displacing another (`add_layer0_edges`, the
/// one function kimmydb's copy of the library adds), so no point loses an
/// edge into it, and every measured build was linked in one round, in under a
/// second at 100,000 vectors of realistic data.
fn link_cut_off<D>(graph: &Hnsw<'static, f32, D>, engine: &Engine) -> Result<usize>
where
    D: hnsw_rs::anndists::dist::Distance<f32> + Send + Sync,
{
    let mut added = 0;
    for _ in 0..LINK_ROUNDS {
        // A search's build, which the drain's deadline ends, as the inserts.
        engine.check_walk(kimmy_storage::WalkScope::Request)?;
        let layer0 = Layer0::read(graph);
        let cut = layer0.cut_off();
        if cut.is_empty() {
            break;
        }
        let lost: HashSet<usize> = cut.iter().copied().collect();
        let mut vectors: Vec<Option<Vec<f32>>> = vec![None; layer0.outside.len()];
        for point in graph.get_point_indexation() {
            let at = point.get_origin_id();
            if lost.contains(&at) {
                vectors[at] = Some(point.get_v().to_vec());
            }
        }
        let mut edges = Vec::with_capacity(cut.len() * LINK_EDGES);
        for &point in &cut {
            let Some(vector) = vectors[point].as_deref() else { continue };
            edges.extend(
                graph
                    .search(vector, LINK_CANDIDATES, LINK_EF)
                    .iter()
                    .map(|n| n.d_id)
                    .filter(|&from| from != point && !lost.contains(&from))
                    .take(LINK_EDGES)
                    .map(|from| (from, point)),
            );
        }
        let linked = graph.add_layer0_edges(&edges);
        if linked == 0 {
            // Nothing linked could be found near what is left: a graph with
            // no linked point at all, or one of nothing but duplicates of a
            // cut-off point. Another round would find the same.
            break;
        }
        added += linked;
    }
    Ok(added)
}

/// fsync a file or a directory: what [`HnswIndex::save`] writes, and the
/// directory entries that name it.
fn sync(path: &std::path::Path) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

/// Put `items` in the order build `attempt` inserts them (ADR-241).
///
/// **The order is the draw.** `hnsw_rs` 0.3.5 gives each point the next level
/// of one generator that every graph seeds with the constant 397, and has no
/// way to seed it otherwise: the point inserted `i`-th always gets the `i`-th
/// level of the same sequence. So the same records inserted in the same order
/// build the same graph, level for level and edge for edge, and a retry that
/// kept the order would rebuild exactly the graph it was retrying (0.3.4 drew
/// levels from an unseeded generator, which is what made its retries
/// independent). Each attempt therefore inserts in another order.
///
/// The scheme: a Fisher–Yates shuffle of the walk's order (the chunk keys in
/// key order), driven by SplitMix64 seeded with the attempt number, every
/// attempt the first included. It is a function of the records and the
/// attempt alone, so a collection's graph is reproducible on every member and
/// across restarts, and two attempts differ. Shuffling the first attempt as
/// well keeps the key order out of the build: a key order that tracks the
/// vectors (ids that grow with time while what is written drifts) is an
/// insertion order HNSW builds poorer graphs from.
fn insertion_order<T>(items: &mut [T], attempt: usize) {
    let mut state = attempt as u64;
    let mut next = move || {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    for i in (1..items.len()).rev() {
        // Lemire's multiply-shift: an index in 0..=i without a modulo's bias.
        let j = ((u128::from(next()) * (i as u128 + 1)) >> 64) as usize;
        items.swap(i, j);
    }
}

/// An approximate index over one collection's vectors.
pub struct HnswIndex {
    graph: Graph,
    /// Graph ids are dense integers, so this maps them back to chunk keys.
    keys: Vec<String>,
    metric: Metric,
    dim: usize,
    /// The shadow collection incarnation this was built from. Persisted, and
    /// checked on load — see [`SnapshotMeta::created`].
    created: Hlc,
    /// [`estimate_bytes`] at construction, so the cache can budget without
    /// recomputing it on every install.
    bytes: usize,
    /// [`SnapshotMeta::renames`]: the shadow's moves of chunk ids when this was
    /// built, read before its walk.
    renames: u64,
}

impl HnswIndex {
    /// Whether this metric can be indexed approximately.
    ///
    /// `Dot` cannot. The underlying `anndists::DistDot` computes `1 - dot` and
    /// **asserts the result is non-negative**, which only holds for unit-length
    /// vectors. An embedding with magnitude greater than one would panic the
    /// process — an unacceptable crash path from user data — so dot-product
    /// collections use the exact scan instead.
    ///
    /// Normalizing vectors on the way in would make `Dot` usable, but it would
    /// also silently change what a dot-product search *means*, which is the
    /// one thing a caller choosing that metric did not ask for.
    pub fn supports(metric: Metric) -> bool {
        !matches!(metric, Metric::Dot)
    }

    /// Build an index, and **verify it can find its own data**.
    ///
    /// O(n log n). Callers cache the result; see [`Self::needs_rebuild`].
    ///
    /// # Why a build is checked rather than trusted
    ///
    /// HNSW's graph is built from a randomised layer assignment, and about
    /// **one build in 250 orphans a large part of the collection**: measured at
    /// 400 vectors, a bad build left 40–96 of them — 10% to 24% — unable to
    /// retrieve themselves even when queried with their own exact vector.
    /// Those documents cannot be returned by any search, at any `k`, until
    /// something rebuilds the index.
    ///
    /// It is not a tuning problem, and both obvious knobs were measured and
    /// rejected. `keep_pruned` improves the bulk of the distribution (median
    /// recall 0.990 → 1.000) and leaves the failures untouched. Widening the
    /// search helps partially and then **plateaus** — failures 5/800 at
    /// `ef=50`, 2/800 at `ef=100`, and 2/800 again at `ef=200` — which is what
    /// a genuinely disconnected component looks like: exploring further cannot
    /// reach it.
    ///
    /// So the graph is checked against the property that matters, by asking a
    /// sample of stored vectors to find themselves. A failed build is
    /// discarded and rebuilt from a fresh random draw. Draws are independent,
    /// so one retry takes ~0.4% to ~1 in 60,000.
    pub fn build(
        engine: &Engine,
        shadow: &CollectionMeta,
        metric: Metric,
        dim: usize,
    ) -> Result<Self> {
        let mut built = Self::build_once(engine, shadow, metric, dim, 0)?;
        for attempt in 1..=MAX_BUILD_ATTEMPTS {
            if built.1.unreachable < REACHABILITY_MAX_UNREACHABLE {
                return Ok(built.0);
            }
            debug!(
                attempt,
                unreachable = built.1.unreachable,
                missed = built.1.missed,
                vectors = built.0.keys.len(),
                "HNSW build left data unreachable; rebuilding in another insertion order"
            );
            built = Self::build_once(engine, shadow, metric, dim, attempt)?;
        }
        if built.1.unreachable >= REACHABILITY_MAX_UNREACHABLE {
            // Never fail: a poor index still answers, and refusing to build one
            // would take vector search down entirely over a quality problem.
            // The caller gets a working index and the operator gets a warning.
            tracing::warn!(
                vectors = built.0.keys.len(),
                unreachable = built.1.unreachable,
                sampled = built.1.sampled,
                "HNSW index still loses data after {MAX_BUILD_ATTEMPTS} rebuilds; \
                 searches on this collection may be incomplete"
            );
        }
        Ok(built.0)
    }

    /// One build, with the count of sampled vectors that could not find
    /// themselves.
    ///
    /// Self-retrieval is the strongest query a stored vector has: its own
    /// value, exactly. A point that does not come back for it is unreachable
    /// from the entry point, which is precisely the failure this guards. The
    /// count is zero for an index too small to sample — a graph nothing can
    /// check is not one to rebuild forever.
    ///
    /// Sampled rather than exhaustive because the failure is not subtle: a bad
    /// build orphans a *tenth* of the collection or more, so a spread sample of
    /// [`REACHABILITY_SAMPLE`] finds one with overwhelming probability while
    /// costing a few dozen searches against an O(n log n) build.
    fn build_once(
        engine: &Engine,
        shadow: &CollectionMeta,
        metric: Metric,
        dim: usize,
        attempt: usize,
    ) -> Result<(Self, Reachability)> {
        if !Self::supports(metric) {
            return Err(VectorError::MalformedResponse {
                provider: "hnsw",
                detail: format!("{metric:?} has no approximate index; searches use an exact scan"),
            });
        }
        // Collected first, for two reasons: the expected element count is
        // known before the graph allocates its tables, and the read
        // transaction closes before the O(n log n) build starts rather than
        // pinning the store's pages for the seconds it takes.
        //
        // Only the key and the vector are kept. A `VectorRecord` also carries
        // the chunk's text, which the graph never looks at, and holding every
        // record whole meant the build's peak was the graph *plus* a copy of
        // the entire shadow collection — text included — until the
        // reachability probe below had finished with it. That was most of the
        // headroom a rebuild needed, paid every `MAX_STALENESS` under writes.
        //
        // The key is the id the chunk is stored under, never one recomputed
        // from the record (ADR-239): the record's source carries no binary
        // subtype, and a chunk under a 0.50 id is not at the id this build
        // would compute, so a recomputed key misses both. A record stored
        // under an id that is not a String (a client's insert into the
        // shadow) has no key a load can name, and is left out.
        // Read before the walk, so a move that lands during it makes the
        // graph read as behind, never as fresh.
        let renames = engine.vector_renames(shadow.id)?;
        let mut records: Vec<(String, Vec<f32>)> = Vec::new();
        engine.for_each_vector(shadow, kimmy_storage::WalkScope::Request, |id, record| {
            // A wrong width belongs to a different model and would corrupt
            // every distance the graph computes.
            if record.vector.len() == dim
                && let DocId::String(key) = id
            {
                records.push((key.clone(), record.vector));
            }
            Ok(true)
        })?;
        // The library draws every level from one fixed seed, so the order is
        // the only thing that tells two attempts apart ([`insertion_order`]).
        insertion_order(&mut records, attempt);

        let expected = records.len().max(1);
        let graph = match metric {
            Metric::Cosine => Graph::Cosine(Hnsw::new(
                MAX_CONNECTIONS,
                expected,
                MAX_LAYERS,
                EF_CONSTRUCTION,
                DistCosine {},
            )),
            Metric::Euclidean => Graph::Euclidean(Hnsw::new(
                MAX_CONNECTIONS,
                expected,
                MAX_LAYERS,
                EF_CONSTRUCTION,
                DistL2 {},
            )),
            // Unreachable: `supports` gates this, and build() returns early.
            Metric::Dot => unreachable!("dot is not supported; see HnswIndex::supports"),
        };

        // The probe below needs a few vectors to query with. They are copied
        // out now — a spread sample of at most `REACHABILITY_SAMPLE` — so
        // that the rest can be released as they go in, rather than the whole
        // collection staying resident until the probe has run.
        let n = records.len();
        let probes: Vec<(usize, Vec<f32>)> = if n >= REACHABILITY_SAMPLE {
            let stride = (n / REACHABILITY_SAMPLE).max(1);
            (0..n)
                .step_by(stride)
                .take(REACHABILITY_SAMPLE)
                .map(|i| (i, records[i].1.clone()))
                .collect()
        } else {
            Vec::new()
        };

        // Sequential, deliberately. `hnsw_rs` offers `parallel_insert` over
        // rayon's global pool, and it was measured rather than assumed away
        // ([Benchmarks](../../../docs/benchmarks.md)): on a ten-core host at
        // 4,000 × 384 it built in 1.1 s on every core and 1.4 s confined to
        // four threads against 4.3 s sequential, and at 20,000 × 384 in 13 s
        // on four threads against 45 s — with recall and reachability
        // indistinguishable from the sequential graph. It is not used, for
        // now, because the win is bounded by contention inside the graph
        // (four threads get most of what ten do) while the cost is not: a
        // four-thread pool is every core of the hosts this project runs on,
        // which puts the multi-second stall back on the request path in a
        // different suit, and a pool bounded to half the cores is one thread
        // on those hosts anyway. It would also need rayon as a direct
        // dependency, and the reachability constants above were sized over
        // hundreds of sequential builds, not three parallel ones. The build
        // is off the cache lock and paid at most once per staleness window;
        // revisit with a measured rebuild backlog on a host with cores to
        // spare.
        //
        // `into_iter`, not `iter`: the graph copies each vector in, so the
        // collected one is freed the moment it has been inserted and the
        // build's peak is one copy plus the graph's bookkeeping, not two.
        let mut keys = Vec::with_capacity(n);
        for (key, vector) in records {
            // A build is for a search, which the drain's deadline ends.
            engine.check_walk(kimmy_storage::WalkScope::Request)?;
            let id = keys.len();
            match &graph {
                Graph::Cosine(g) => g.insert((vector.as_slice(), id)),
                Graph::Euclidean(g) => g.insert((vector.as_slice(), id)),
            }
            keys.push(key);
        }

        // Link what the insertion left where no search reaches (ADR-241).
        let linked = match &graph {
            Graph::Cosine(g) => link_cut_off(g, engine)?,
            Graph::Euclidean(g) => link_cut_off(g, engine)?,
        };

        debug!(vectors = keys.len(), linked, "built HNSW index");
        let bytes = estimate_bytes(&keys, dim);
        let index = Self { graph, keys, metric, dim, created: shadow.created, bytes, renames };

        // Ask the sampled vectors to find themselves. A few dozen searches
        // against an O(n log n) build, and no extra reads.
        let mut reach = Reachability { linked, ..Reachability::default() };
        for (i, vector) in &probes {
            reach.sampled += 1;
            let found = index.search_keys(vector, 1)?;
            if found.first() == index.keys.get(*i) {
                continue;
            }
            reach.missed += 1;
            // Asked again with a much larger budget. An ordinary search is a
            // fixed amount of exploration, so as a collection grows the same
            // budget covers less of it and a reachable point starts being
            // missed for want of looking — which is a fact about the query,
            // not about the graph. Only a point that stays missing when the
            // budget stops mattering is one the graph has lost.
            if !index.finds_itself(vector, *i) {
                reach.unreachable += 1;
            }
        }
        Ok((index, reach))
    }

    /// Whether `id` comes back for its own vector when effort stops being the
    /// limit.
    ///
    /// `k` above one matters as much as the wide `ef`: two identical vectors
    /// are both at distance zero, so a `k` of one can return the other and
    /// report a miss that has nothing to do with reachability.
    fn finds_itself(&self, query: &[f32], id: usize) -> bool {
        self.finds_itself_at(query, id, REACHABILITY_CONFIRM_EF)
    }

    /// [`Self::finds_itself`] at a chosen effort.
    fn finds_itself_at(&self, query: &[f32], id: usize, ef: usize) -> bool {
        let neighbours = match &self.graph {
            Graph::Cosine(g) => g.search(query, REACHABILITY_CONFIRM_K, ef),
            Graph::Euclidean(g) => g.search(query, REACHABILITY_CONFIRM_K, ef),
        };
        neighbours.iter().any(|n| n.d_id == id)
    }

    /// Whether the collection has drifted far enough to justify a rebuild.
    ///
    /// The graph is built from a snapshot and does not track later writes, so
    /// this is how a caller knows its cached index has gone stale.
    pub fn needs_rebuild(&self, current_vectors: usize) -> bool {
        let built = self.keys.len();
        // A quarter drift, or any change at all on a tiny index.
        current_vectors.abs_diff(built) > (built / 4).max(1)
    }

    /// The metric the graph was built for. Its walk chooses candidates by
    /// this, and [`Self::search`] scores them by this too — from the stored
    /// vectors, but with the graph's own metric — so a graph of one metric
    /// answering a query configured for another orders the results wrongly.
    pub fn metric(&self) -> Metric {
        self.metric
    }

    /// The width every vector in the graph has. A query of another width is
    /// refused by [`Self::search_keys`].
    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// The shadow's moves of chunk ids when this graph was built
    /// ([`Engine::vector_renames`]).
    pub(crate) fn renames(&self) -> u64 {
        self.renames
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Approximately how many bytes this graph keeps resident.
    ///
    /// An estimate from the node count, the width and the key table — the
    /// formula is on [`NODE_OVERHEAD_BYTES`] — not a measurement of the heap.
    /// It is what the index cache budgets against.
    pub fn approx_bytes(&self) -> usize {
        self.bytes
    }

    /// Persist this index into `dir`, atomically.
    ///
    /// Written beside the data rather than in it: redb stores the vectors —
    /// the *source* — and the graph is derived state, exactly like the index
    /// entries a secondary index derives from documents. The difference is
    /// that this derivation costs O(n log n) and seconds, which is what makes
    /// persisting it worth a file where a secondary index rebuild would not
    /// be.
    ///
    /// Atomic by staging: the dump lands in a `.build` sibling that replaces
    /// the real directory only once every file is on disk. A crash mid-save
    /// leaves the previous snapshot (or none), never a torn one — and the
    /// loader refuses anything unreadable anyway ([`crate::dump::check`]).
    ///
    /// **On disk means synced** (ADR-241). The files, then the staging
    /// directory, are fsynced before the rename, and the parent after it.
    /// Without that a rename can reach the disk before the data it names: a
    /// crash then leaves the new name over files cut short, which `hnsw_rs`
    /// answered by ending the process at the collection's first search, on
    /// every restart, until the dump check refused them.
    pub fn save(&self, dir: &std::path::Path) -> Result<()> {
        use hnsw_rs::api::AnnT;

        let io = |e: std::io::Error| VectorError::Snapshot(e.to_string());
        let staging = dir.with_extension("build");
        let _ = std::fs::remove_dir_all(&staging);
        std::fs::create_dir_all(&staging).map_err(io)?;

        match &self.graph {
            Graph::Cosine(g) => g.file_dump(&staging, crate::dump::BASENAME),
            Graph::Euclidean(g) => g.file_dump(&staging, crate::dump::BASENAME),
        }
        .map_err(|e| VectorError::Snapshot(e.to_string()))?;

        let meta = SnapshotMeta {
            format: SNAPSHOT_FORMAT,
            metric: self.metric,
            dim: self.dim,
            created: self.created,
            keys: self.keys.clone(),
            renames: self.renames,
        };
        std::fs::write(
            staging.join("meta.json"),
            serde_json::to_vec(&meta).map_err(|e| VectorError::Snapshot(e.to_string()))?,
        )
        .map_err(io)?;
        for file in [
            format!("{}.hnsw.graph", crate::dump::BASENAME),
            format!("{}.hnsw.data", crate::dump::BASENAME),
            "meta.json".to_string(),
        ] {
            sync(&staging.join(file)).map_err(io)?;
        }
        sync(&staging).map_err(io)?;

        let _ = std::fs::remove_dir_all(dir);
        std::fs::rename(&staging, dir).map_err(io)?;
        if let Some(parent) = dir.parent() {
            sync(parent).map_err(io)?;
        }
        debug!(vectors = self.keys.len(), ?dir, "saved HNSW snapshot");
        Ok(())
    }

    /// Load a snapshot from `dir`, refusing one that describes a different
    /// index than the caller wants.
    ///
    /// The metric, dimension and incarnation checks are the trust boundary. A
    /// snapshot written under an old vector configuration would compute every
    /// distance wrongly, which is worse than the rebuild it saves; a snapshot
    /// left behind by a *previous collection of the same name* describes
    /// chunks that no longer exist, and because ids are derived from names it
    /// is sitting at exactly the path the new collection's graph would use.
    /// Any other failure — missing files, torn writes, a format this build
    /// does not speak — is a plain `Err`, and the caller discards the snapshot
    /// and rebuilds. Correctness never depends on a load succeeding.
    pub fn load(dir: &std::path::Path, metric: Metric, dim: usize, created: Hlc) -> Result<Self> {
        let corrupt = |detail: String| VectorError::Snapshot(detail);

        let raw = std::fs::read(dir.join("meta.json")).map_err(|e| corrupt(e.to_string()))?;
        let meta: SnapshotMeta =
            serde_json::from_slice(&raw).map_err(|e| corrupt(e.to_string()))?;
        if meta.format != SNAPSHOT_FORMAT {
            return Err(corrupt(format!(
                "snapshot format {} is not {SNAPSHOT_FORMAT}",
                meta.format
            )));
        }
        if meta.metric != metric || meta.dim != dim {
            return Err(corrupt(format!(
                "snapshot is {:?}/{}d, wanted {:?}/{}d — a reconfigured collection's snapshot \
                 must not be trusted",
                meta.metric, meta.dim, metric, dim
            )));
        }
        if meta.created != created {
            return Err(corrupt(format!(
                "snapshot was built for the collection created at {:?}, not the one created \
                 at {created:?} — a previous collection of this name left it behind",
                meta.created
            )));
        }

        // Before the library sees a byte of the dump: it ends the process on
        // some of what a corrupt or foreign file holds (ADR-241).
        let distance = match metric {
            Metric::Cosine => std::any::type_name::<DistCosine>(),
            Metric::Euclidean => std::any::type_name::<DistL2>(),
            Metric::Dot => return Err(corrupt("dot has no approximate index".to_string())),
        };
        let expect = crate::dump::Expect {
            points: meta.keys.len(),
            dim,
            connections: MAX_CONNECTIONS as u8,
            distance,
        };
        crate::dump::check(dir, &expect).map_err(|e| corrupt(format!("graph dump: {e}")))?;

        let graph = match metric {
            Metric::Cosine => load_graph::<DistCosine>(dir).map(Graph::Cosine),
            Metric::Euclidean => load_graph::<DistL2>(dir).map(Graph::Euclidean),
            Metric::Dot => Err("dot has no approximate index".to_string()),
        }
        .map_err(|e| corrupt(format!("graph reload: {e}")))?;

        debug!(vectors = meta.keys.len(), ?dir, "loaded HNSW snapshot");
        let bytes = estimate_bytes(&meta.keys, dim);
        Ok(Self {
            graph,
            keys: meta.keys,
            metric,
            dim,
            created: meta.created,
            bytes,
            renames: meta.renames,
        })
    }

    /// Approximate k-nearest neighbours, as chunk keys: the `k` nearest the
    /// graph finds, whatever document each belongs to.
    pub fn search_keys(&self, query: &[f32], k: usize) -> Result<Vec<String>> {
        if query.len() != self.dim {
            return Err(VectorError::DimensionMismatch { expected: self.dim, found: query.len() });
        }
        if self.keys.is_empty() {
            return Ok(Vec::new());
        }

        let ef = (k * EF_SEARCH_FACTOR).max(MIN_EF_SEARCH);
        let neighbours = match &self.graph {
            Graph::Cosine(g) => g.search(query, k, ef),
            Graph::Euclidean(g) => g.search(query, k, ef),
        };
        Ok(neighbours.into_iter().filter_map(|n| self.keys.get(n.d_id).cloned()).collect())
    }

    /// Approximate search returning full hits.
    ///
    /// Over-fetches from the graph, because the per-document cap can collapse
    /// several chunks of one document into a single result and would otherwise
    /// return fewer than `k`.
    ///
    /// `allowed` holds *document* ids. A candidate is admitted only once its
    /// record is loaded, by the document the record names (ADR-239): a key
    /// names a slot, not who holds it, and a chunk a 0.50 build left at
    /// another document's id, or debris, would be admitted under one document
    /// and returned as another. **What is loaded is chosen by the key first**
    /// ([`may_hold`]): only a key that can hold an admitted document's chunk,
    /// its run's or its 0.50 id, so a filtered search reads what it reads
    /// unfiltered scaled by the filter, not every candidate of the widened
    /// walk. Filtering happens after the graph walk, so the search is widened
    /// to compensate.
    pub fn search(
        &self,
        engine: &Engine,
        shadow: &CollectionMeta,
        query: &[f32],
        options: &SearchOptions,
        allowed: Option<&HashSet<DocId>>,
    ) -> Result<Vec<Hit>> {
        let keys = self.search_keys(query, walk_width(options.k, allowed.is_some()))?;
        let old_forms = allowed.map(old_forms).unwrap_or_default();

        let mut hits = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(allowed) = allowed
                && !may_hold(allowed, &old_forms, &key)
            {
                continue;
            }
            let Some(record) = load_record(engine, shadow, &key)? else {
                // The index can lag storage; a missing record just means this
                // chunk was deleted since the build.
                continue;
            };
            if !crate::search::admitted(allowed, &record) {
                continue;
            }
            hits.push(Hit {
                score: similarity(query, &record.vector, self.metric),
                id: record.source,
                chunk: record.chunk,
                text: record.text,
            });
        }
        Ok(crate::search::rank_hits(hits, options))
    }
}

/// How many candidates [`HnswIndex::search`] asks the graph walk for, for `k`
/// hits.
fn walk_width(k: usize, filtered: bool) -> usize {
    // Over-fetch: the per-document cap can collapse several chunks of one
    // document into a single result, which would otherwise return under k.
    let over_fetch = (k * 4).max(k + 10);
    // A post-filter discards results after the walk, so a selective filter
    // needs a much wider search to still return k.
    if filtered { over_fetch * 8 } else { over_fetch }
}

/// The 0.50 forms of the admitted documents whose chunk ids changed in 0.51.0
/// ([`VectorRecord::legacy_prefix`], less its `#`): the text before a key's
/// last `#` when it holds such a document's chunk under the id a 0.50 build
/// wrote. Empty unless the filter admitted a binary or a reserved String.
fn old_forms(allowed: &HashSet<DocId>) -> HashSet<String> {
    allowed
        .iter()
        .filter_map(VectorRecord::legacy_prefix)
        .map(|mut prefix| {
            prefix.pop();
            prefix
        })
        .collect()
}

/// Whether the chunk at `key` can be an admitted document's, judged before it
/// is loaded: its id parses to a source of one (a binary's under any subtype,
/// as the document key folds it), or it is one's 0.50 id (`old_forms`). Those
/// are exactly the places a document's own read covers, its run and its 0.50
/// ids, so no chunk of an admitted document is passed over; whether a loaded
/// chunk is that document's is still the record's to say.
fn may_hold(allowed: &HashSet<DocId>, old_forms: &HashSet<String>, key: &str) -> bool {
    let by_run = VectorRecord::parse_key(key)
        .and_then(|(source, _)| source.document())
        .is_some_and(|document| allowed.contains(&document));
    let by_legacy = !old_forms.is_empty()
        && key.rsplit_once('#').is_some_and(|(text, _)| old_forms.contains(text));
    by_run || by_legacy
}

/// Fetch one chunk record by its stored key.
fn load_record(
    engine: &Engine,
    shadow: &CollectionMeta,
    key: &str,
) -> Result<Option<VectorRecord>> {
    let id = DocId::String(key.to_string());
    let Some(doc) = engine.get(shadow, &id)? else {
        return Ok(None);
    };
    Ok(Some(
        bson::deserialize_from_document(doc).map_err(|e| VectorError::MalformedResponse {
            provider: "hnsw",
            detail: e.to_string(),
        })?,
    ))
}

#[cfg(test)]
mod tests {
    use kimmy_core::{ChunkConfig, ChunkSource, Hlc, ProviderConfig, VectorConfig};

    use super::*;

    fn config(dim: usize) -> VectorConfig {
        VectorConfig {
            fields: vec!["body".into()],
            provider: ProviderConfig::Byo {},
            dim,
            metric: Metric::Cosine,
            document_prefix: None,
            query_prefix: None,
            chunk: ChunkConfig::default(),
        }
    }

    /// Deterministic pseudo-random vectors, so recall numbers are reproducible.
    fn pseudo_random(seed: u64, dim: usize) -> Vec<f32> {
        let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        (0..dim)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                ((state >> 33) as f32 / (u32::MAX as f32)) - 0.5
            })
            .collect()
    }

    fn setup(count: usize, dim: usize) -> (Engine, CollectionMeta, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        engine.create_collection("app", "docs").unwrap();
        engine.configure_vectors("app", "docs", config(dim)).unwrap();
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();

        for i in 0..count {
            let source = DocId::Int64(i as i64);
            let record = VectorRecord {
                source: source.clone(),
                chunk: 0,
                source_hlc: Hlc::new(1, 0),
                vector: pseudo_random(i as u64 + 1, dim),
                text: format!("document {i}"),
                config: None,
            };
            engine.put_vectors(&shadow, &source, &[record]).unwrap();
        }
        (engine, shadow, dir)
    }

    /// `setup`'s shape at sizes where a commit per document is minutes: the
    /// same records, written a few thousand to a commit, with `fixture`
    /// choosing another set of vectors (`0` is `setup`'s).
    fn setup_bulk(
        count: usize,
        dim: usize,
        fixture: u64,
    ) -> (Engine, CollectionMeta, tempfile::TempDir) {
        const PER_COMMIT: usize = 5_000;
        // `KIMMY_REACH_LATENT=d` draws each vector from `d` dimensions mapped
        // into `dim` by one fixed projection, plus a little noise: data of
        // intrinsic dimension `d`, nearer what an embedding model produces
        // than `dim` independent coordinates are.
        let latent: Option<usize> =
            std::env::var("KIMMY_REACH_LATENT").ok().and_then(|d| d.parse().ok());
        let projection: Vec<Vec<f32>> = latent
            .map(|d| (0..dim).map(|row| pseudo_random(0xfeed_0000 + row as u64, d)).collect())
            .unwrap_or_default();
        let vector = |i: usize| {
            let seed = (fixture << 32) | (i as u64 + 1);
            match latent {
                None => pseudo_random(seed, dim),
                Some(d) => {
                    let z = pseudo_random(seed, d);
                    let noise = pseudo_random(seed ^ 0x5eed_5eed_5eed_5eed, dim);
                    projection
                        .iter()
                        .zip(noise)
                        .map(|(row, e)| {
                            row.iter().zip(&z).map(|(p, x)| p * x).sum::<f32>() + 0.05 * e
                        })
                        .collect()
                }
            }
        };
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        engine.create_collection("app", "docs").unwrap();
        engine.configure_vectors("app", "docs", config(dim)).unwrap();
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        for start in (0..count).step_by(PER_COMMIT) {
            engine
                .write_batch(kimmy_storage::WriterHolder::Embedding, |scope| {
                    for i in start..(start + PER_COMMIT).min(count) {
                        let source = DocId::Int64(i as i64);
                        let record = VectorRecord {
                            source: source.clone(),
                            chunk: 0,
                            source_hlc: Hlc::new(1, 0),
                            vector: vector(i),
                            text: format!("document {i}"),
                            config: None,
                        };
                        let write = kimmy_storage::VectorWrite::encode(
                            &source,
                            &ChunkSource::from(&source),
                            &[record],
                        )?;
                        scope.put_vectors(&shadow, write)?;
                    }
                    Ok(())
                })
                .unwrap();
        }
        (engine, shadow, dir)
    }

    /// Every stored vector by the key the graph names it by.
    fn vectors_by_key(
        engine: &Engine,
        shadow: &CollectionMeta,
    ) -> std::collections::HashMap<String, Vec<f32>> {
        let mut vectors = std::collections::HashMap::new();
        engine
            .for_each_vector(shadow, kimmy_storage::WalkScope::Request, |id, record| {
                if let DocId::String(key) = id {
                    vectors.insert(key.clone(), record.vector);
                }
                Ok(true)
            })
            .unwrap();
        vectors
    }

    /// The graph's structure, read rather than searched: how many points have
    /// no edge into them at layer 0, and how many layer 0 does not link to
    /// the entry point at all, directly or through other points. Every
    /// search's walk at layer 0 is a walk along those edges from the point
    /// its descent ends at, so a point with no edge into it is returned only
    /// when the descent ends on it, and a point outside the entry point's
    /// component only from a descent that ends inside its own.
    fn cut_off(index: &HnswIndex) -> (usize, usize) {
        let (no_edge_in, outside) = cut_off_points(index);
        (no_edge_in.iter().filter(|c| **c).count(), outside.iter().filter(|c| **c).count())
    }

    /// [`cut_off`] point by point: whether each graph id has no layer-0 edge
    /// into it, and whether it is outside the entry point's component.
    fn cut_off_points(index: &HnswIndex) -> (Vec<bool>, Vec<bool>) {
        let layer0 = match &index.graph {
            Graph::Cosine(g) => Layer0::read(g),
            Graph::Euclidean(g) => Layer0::read(g),
        };
        (layer0.no_edge_in, layer0.outside)
    }

    /// How many of the graph's points do not come back for their own vector
    /// when effort stops being the limit: every point checked, not a sample.
    /// The ground truth a sampled score is judged against.
    fn truly_orphaned(
        index: &HnswIndex,
        vectors: &std::collections::HashMap<String, Vec<f32>>,
    ) -> usize {
        (0..index.keys.len()).filter(|&i| !index.finds_itself(&vectors[&index.keys[i]], i)).count()
    }

    #[test]
    fn an_index_builds_over_stored_vectors() {
        let (engine, shadow, _dir) = setup(50, 8);
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, 8).unwrap();
        assert_eq!(index.len(), 50);
        assert!(!index.is_empty());
    }

    #[test]
    fn an_empty_collection_yields_an_empty_index() {
        let (engine, shadow, _dir) = setup(0, 8);
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, 8).unwrap();
        assert!(index.is_empty());
        assert!(index.search_keys(&pseudo_random(1, 8), 5).unwrap().is_empty());
    }

    #[test]
    fn a_query_of_the_wrong_width_is_rejected() {
        let (engine, shadow, _dir) = setup(10, 8);
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, 8).unwrap();
        assert!(matches!(
            index.search_keys(&[0.0; 4], 5).err(),
            Some(VectorError::DimensionMismatch { .. })
        ));
    }

    /// A healthy graph at a realistic embedding width is not treated as broken,
    /// and is healthy: no more than 0.5% of it is out of reach.
    ///
    /// The threshold this guards was once sized against a 400-vector,
    /// 16-dimensional fixture, and at 384 dimensions a fixed search budget
    /// covers proportionally less of a growing collection, so ordinary misses
    /// climbed with size until *every* build at 4,000 vectors was discarded,
    /// rebuilt twice, and then reported as losing data. Hence 384 dimensions.
    ///
    /// **And it is the realistic-width half of the regression test for
    /// `hnsw_rs` 0.3.4** (ADR-241). Measured with every point checked, at this
    /// size and width:
    ///
    /// | library | draws | truly orphaned | sampled score |
    /// |---|---|---|---|
    /// | 0.3.4 | 12 | 0.8%–2.1%, and 34.6% in one | 0–2, and more |
    /// | 0.3.5 | 52 | 0–0.2% | 0–1 |
    ///
    /// A build is a function of its records and its attempt, so the three
    /// draws here are the same three graphs on every run (0.3.5: 2, 0 and 0
    /// points out of reach), and the bound is an assertion about the library,
    /// not a bet on a draw: 0.3.4's best of twelve left 8 out of reach.
    ///
    /// **Ignored, and run by its own CI job in release.** A 384-dimensional
    /// graph costs 146 seconds to build in a debug profile against 21 in
    /// release, and doubling the default suite to hold one property is a good
    /// way to have the property removed later. Same arrangement, and the same
    /// reasoning, as the cluster harness. Checking every point adds about eight
    /// seconds a draw there.
    #[test]
    #[ignore = "384-dimensional builds are minutes in debug; CI runs this in release"]
    fn a_healthy_graph_at_a_realistic_width_is_not_rebuilt() {
        const COUNT: usize = 1_000;
        const DIM: usize = 384;
        const DRAWS: usize = 3;
        /// Out of reach in one draw, at most: 0.5%.
        const MOST_OUT_OF_REACH: usize = COUNT / 200;

        let (engine, shadow, _dir) = setup(COUNT, DIM);
        let vectors = vectors_by_key(&engine, &shadow);

        for draw in 0..DRAWS {
            let (index, reach) =
                HnswIndex::build_once(&engine, &shadow, Metric::Cosine, DIM, draw).unwrap();
            assert_eq!(reach.sampled, REACHABILITY_SAMPLE, "the sample must be full at this size");
            assert!(
                reach.unreachable <= reach.missed,
                "a confirmed loss is a subset of the misses it was found among"
            );
            // Every point, not the sample: the ground truth the score is
            // judged against.
            let orphaned = truly_orphaned(&index, &vectors);
            assert!(
                orphaned <= MOST_OUT_OF_REACH,
                "draw {draw}: {orphaned} of {COUNT} {DIM}-dimensional vectors cannot find \
                 themselves; hnsw_rs 0.3.5 leaves at most 2 here and 0.3.4 at least 8"
            );
            assert!(
                reach.unreachable < REACHABILITY_MAX_UNREACHABLE,
                "draw {draw}: a healthy {DIM}-dimensional graph of {COUNT} vectors ({orphaned} \
                 truly orphaned) scored {} unreachable of {} sampled ({} missed at ordinary \
                 effort) and would be discarded and rebuilt",
                reach.unreachable,
                reach.sampled,
                reach.missed,
            );
        }
    }

    /// Every vector can be found, in every build: the regression test for
    /// `hnsw_rs` 0.3.4 in the default suite (ADR-241).
    ///
    /// 0.3.4 wrote a reverse edge at the new point's top layer rather than
    /// the layer the edge was found on, so a point drawn above layer 0 could
    /// be left with no edge into it at layer 0, and no search could return it.
    /// At this size it left 0.25% to 3% of every graph out of reach (one of
    /// thirty draws reached all), so eight draws all clean is a red on 0.3.4
    /// on essentially every run; 0.3.5 left none out of reach in 315 draws.
    /// The draws are attempts, so they are the same eight graphs on every
    /// run. Every point is checked twice: by the search a probe makes, and by
    /// the graph's own edges.
    #[test]
    fn every_vector_is_reachable_in_every_build() {
        const COUNT: usize = 400;
        const DIM: usize = 16;
        const DRAWS: usize = 8;
        let (engine, shadow, _dir) = setup(COUNT, DIM);
        let vectors = vectors_by_key(&engine, &shadow);
        for draw in 0..DRAWS {
            let (index, _) =
                HnswIndex::build_once(&engine, &shadow, Metric::Cosine, DIM, draw).unwrap();
            assert_eq!(index.len(), COUNT);
            let (no_edge_in, outside) = cut_off(&index);
            assert_eq!(
                (no_edge_in, outside),
                (0, 0),
                "draw {draw}: points with no layer-0 edge into them, and points the entry \
                 point's layer 0 does not reach"
            );
            assert_eq!(
                truly_orphaned(&index, &vectors),
                0,
                "draw {draw}: vectors that cannot find themselves"
            );
        }
    }

    /// The distribution the reachability constants are sized from.
    ///
    /// Ignored because it builds hundreds of graphs and takes minutes. It
    /// exists so the numbers in those constants can be re-derived rather than
    /// trusted — this project has now set them wrongly twice, both times by
    /// picking a value without knowing the shape of what it bounds, and the
    /// second time the shape turned out to depend on collection size.
    ///
    /// ```bash
    /// cargo test -p kimmy-vector --release -- --ignored --nocapture reachability
    /// ```
    #[test]
    #[ignore = "builds hundreds of graphs; run it when changing a reachability constant"]
    fn reachability_distribution_by_collection_size() {
        // Both axes, because the first cut of this measurement swept only
        // collection size at 16 dimensions and concluded the miss rate was
        // flat. It is not. Width drives it far harder than count does, and 384
        // is the realistic embedding width — the regime that matters is the one
        // a 16-dimensional fixture does not show.
        //
        // A build is a function of its records and its attempt (see
        // `insertion_order`), so the draws are the attempts over several
        // fixtures, and every point of every build is checked: the true
        // orphaned count beside the sampled score the threshold judges.
        // `KIMMY_REACH_SIZES` (`dim:count:fixtures:attempts,...`) runs a
        // chosen grid instead; `row,` lines are the raw data.
        let grid: Vec<(usize, usize, u64, usize)> = match std::env::var("KIMMY_REACH_SIZES") {
            Ok(spec) => spec
                .split(',')
                .map(|cell| {
                    let v: Vec<usize> = cell.split(':').map(|x| x.parse().unwrap()).collect();
                    (v[0], v[1], v[2] as u64, v[3])
                })
                .collect(),
            Err(_) => vec![
                (8, 50, 10, 20),
                (8, 100, 10, 20),
                (16, 200, 10, 20),
                (16, 400, 10, 30),
                (16, 500, 10, 20),
                (16, 4_000, 4, 5),
                (384, 500, 6, 10),
                (384, 1_000, 5, 8),
                (384, 2_000, 4, 5),
                (384, 4_000, 3, 4),
                (384, 10_000, 2, 3),
            ],
        };
        for (dim, count, fixtures, attempts) in grid {
            let mut orphaned = Vec::new();
            let mut cut_offs = Vec::new();
            let mut no_edges_in = Vec::new();
            let mut lost_wider = Vec::new();
            let mut missed = Vec::new();
            let mut unreachable = Vec::new();
            for fixture in 0..fixtures {
                let (engine, shadow, _dir) = setup_bulk(count, dim, fixture);
                let vectors = vectors_by_key(&engine, &shadow);
                for attempt in 0..attempts {
                    let started = std::time::Instant::now();
                    let (index, reach) =
                        HnswIndex::build_once(&engine, &shadow, Metric::Cosine, dim, attempt)
                            .unwrap();
                    let built = started.elapsed();
                    let lost = truly_orphaned(&index, &vectors);
                    // Of those, the ones the graph's structure cuts off, and
                    // the ones still missing at four times the effort: a
                    // point found there, or linked, is hard to find rather
                    // than lost.
                    let (no_edge_in, cut) = cut_off(&index);
                    // Lost although layer 0 links it in: what a repair of the
                    // structure, or an exact look at the cut-off points,
                    // would still leave.
                    let (none_in, outside) = cut_off_points(&index);
                    let linked_lost = (0..index.keys.len())
                        .filter(|&i| !none_in[i] && !outside[i])
                        .filter(|&i| !index.finds_itself(&vectors[&index.keys[i]], i))
                        .count();
                    let wider = (0..index.keys.len())
                        .filter(|&i| {
                            !index.finds_itself(&vectors[&index.keys[i]], i)
                                && !index.finds_itself_at(&vectors[&index.keys[i]], i, 3_200)
                        })
                        .count();
                    println!(
                        "row,{dim},{count},{fixture},{attempt},{lost},{},{},{},{},{cut},{wider},{no_edge_in},{linked_lost}",
                        reach.sampled,
                        reach.missed,
                        reach.unreachable,
                        built.as_millis()
                    );
                    cut_offs.push(cut);
                    no_edges_in.push(no_edge_in);
                    lost_wider.push(wider);
                    orphaned.push(lost);
                    missed.push(reach.missed);
                    unreachable.push(reach.unreachable);
                }
            }
            for v in [
                &mut orphaned,
                &mut cut_offs,
                &mut no_edges_in,
                &mut lost_wider,
                &mut missed,
                &mut unreachable,
            ] {
                v.sort_unstable();
            }
            let builds = orphaned.len();
            let median = |v: &Vec<usize>| v[v.len() / 2];
            let over = |v: &Vec<usize>, t: usize| v.iter().filter(|&&x| x >= t).count();
            println!(
                "dim={dim:<4} n={count:<6} builds={builds:<4} \
                 truly orphaned: median={} max={} ({:.2}%) builds with any={} | \
                 outside the entry's component: max={} | no edge in: max={} | \
                 lost at ef 3200: max={} | \
                 missed: median={:<3} max={:<3} | unreachable: median={:<3} max={:<3} \
                 >=1:{:<3} >=2:{:<3} >=3:{:<3} >=8:{}",
                median(&orphaned),
                orphaned.last().unwrap(),
                100.0 * *orphaned.last().unwrap() as f64 / count as f64,
                over(&orphaned, 1),
                cut_offs.last().unwrap(),
                no_edges_in.last().unwrap(),
                lost_wider.last().unwrap(),
                median(&missed),
                missed.last().unwrap(),
                median(&unreachable),
                unreachable.last().unwrap(),
                over(&unreachable, 1),
                over(&unreachable, 2),
                over(&unreachable, 3),
                over(&unreachable, 8),
            );
        }
    }

    /// What the threshold actually has to separate.
    ///
    /// The distribution above says what a healthy build scores. It says
    /// nothing about what a *bad* one scores, and a threshold is a statement
    /// about the gap between the two — so assuming the bad side from a
    /// binomial argument would be picking a number without knowing the shape
    /// of what it bounds, one more time.
    ///
    /// So this builds many graphs at the size where the catastrophic failure
    /// is known to occur (~1 in 250 at 400 vectors), checks **every** point
    /// rather than a sample to get the true orphaned fraction, and prints the
    /// sampled score beside it. The two columns are what the threshold sits
    /// between.
    ///
    /// ```bash
    /// cargo test -p kimmy-vector --release -- --ignored --nocapture separates
    /// ```
    #[test]
    #[ignore = "builds ~600 graphs; run it when changing a reachability constant"]
    fn the_threshold_separates_catastrophic_builds_from_healthy_ones() {
        const COUNT: usize = 400;
        const DIM: usize = 16;
        const BUILDS: usize = 600;

        let (engine, shadow, _dir) = setup(COUNT, DIM);
        let vectors = vectors_by_key(&engine, &shadow);

        let mut healthy = Vec::new();
        let mut bad = Vec::new();
        for attempt in 0..BUILDS {
            let (index, reach) =
                HnswIndex::build_once(&engine, &shadow, Metric::Cosine, DIM, attempt).unwrap();
            // Every point, not a sample: this is the ground truth the sampled
            // score is being judged against.
            let truly = truly_orphaned(&index, &vectors);
            let fraction = truly as f64 / index.keys.len() as f64;
            if fraction >= 0.05 { &mut bad } else { &mut healthy }
                .push((fraction, reach.unreachable));
        }

        let summarize = |label: &str, v: &mut Vec<(f64, usize)>| {
            if v.is_empty() {
                println!("{label:<12} none observed in {BUILDS} builds");
                return;
            }
            v.sort_by_key(|a| a.1);
            let scores: Vec<usize> = v.iter().map(|(_, s)| *s).collect();
            println!(
                "{label:<12} builds={:<4} true orphaned {:.1}%-{:.1}%  \
                 sampled score: min={} median={} max={}",
                v.len(),
                v.first().unwrap().0 * 100.0,
                v.iter().map(|(f, _)| *f).fold(0.0, f64::max) * 100.0,
                scores.first().unwrap(),
                scores[scores.len() / 2],
                scores.last().unwrap(),
            );
        };
        summarize("healthy", &mut healthy);
        summarize("catastrophic", &mut bad);
    }

    /// The measurement that justifies using an approximate index at all.
    ///
    /// Recall is the fraction of the true top-k that HNSW also returns. A
    /// well-built graph should be near-perfect at this scale; a low number
    /// would mean the index is silently losing results.
    ///
    /// # This test was right, and it caught a real defect
    ///
    /// It failed CI once at **0.865**, and the tempting reading was that a
    /// randomised structure makes a hard threshold flaky. It is worth recording
    /// that the tempting reading was wrong. Measured over 1,500 builds the
    /// distribution was **bimodal** — median 0.995, and about one build in 250
    /// collapsing to 0.55 — and in those builds a tenth to a quarter of the
    /// collection could not retrieve *itself*. The test was reporting data loss,
    /// not noise.
    ///
    /// Averaging several graphs to quiet it down would have been exactly the
    /// wrong move: it hides a data-loss bug behind a statistic, and — because a
    /// mean of three draws three chances at a bad one — it fails *more* often,
    /// not less.
    ///
    /// [`HnswIndex::build`] now verifies a finished graph can find its own data
    /// and rebuilds one that cannot, so a single graph is a stable measurement
    /// again: over 2,500 builds with that check in place, the minimum was
    /// **0.96** and nothing fell below 0.95. The bar stays where it was.
    #[test]
    fn recall_against_exact_search_is_high() {
        const COUNT: usize = 400;
        const DIM: usize = 16;
        const K: usize = 10;
        const QUERIES: usize = 20;

        let (engine, shadow, _dir) = setup(COUNT, DIM);
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, DIM).unwrap();
        let options = SearchOptions { k: K, metric: Metric::Cosine, per_document: 1 };

        let mut total_recall = 0.0;
        for q in 0..QUERIES {
            let query = pseudo_random(10_000 + q as u64, DIM);

            let exact = crate::search::vector_search(&engine, &shadow, &query, &options, None)
                .unwrap()
                .into_iter()
                .map(|h| h.id.to_string())
                .collect::<HashSet<_>>();
            let approx = index
                .search(&engine, &shadow, &query, &options, None)
                .unwrap()
                .into_iter()
                .map(|h| h.id.to_string())
                .collect::<HashSet<_>>();

            total_recall += exact.intersection(&approx).count() as f64 / exact.len() as f64;
        }

        let recall = total_recall / QUERIES as f64;
        assert!(
            recall >= 0.90,
            "recall {recall:.3} is too low; the index is losing results a scan would find"
        );
    }

    #[test]
    fn recall_holds_at_a_realistic_embedding_width() {
        // The test above measures recall at 16 dimensions. Approximate search
        // gets *harder* as width grows — distances concentrate, and a greedy
        // graph walk has less signal to follow — so 16 is the flattering case
        // and no evidence about the widths anyone actually deploys.
        //
        // This matters more since `MIN_VECTORS_FOR_INDEX` dropped to 500: the
        // graph now serves collections that previously took the exact path, so
        // the recall claim covers more traffic than it used to.
        //
        // 384 is `all-MiniLM-L6-v2`, the narrowest width in common use.
        // Exactly at `MIN_VECTORS_FOR_INDEX`, so this is the boundary case:
        // the smallest collection the graph is now trusted to serve.
        const COUNT: usize = 500;
        const DIM: usize = 384;
        const K: usize = 10;

        let (engine, shadow, _dir) = setup(COUNT, DIM);
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, DIM).unwrap();
        let options = SearchOptions { k: K, metric: Metric::Cosine, per_document: 1 };

        let mut total_recall = 0.0;
        const QUERIES: usize = 10;
        for q in 0..QUERIES {
            let query = pseudo_random(20_000 + q as u64, DIM);

            let exact = crate::search::vector_search(&engine, &shadow, &query, &options, None)
                .unwrap()
                .into_iter()
                .map(|h| h.id.to_string())
                .collect::<HashSet<_>>();
            let approx = index
                .search(&engine, &shadow, &query, &options, None)
                .unwrap()
                .into_iter()
                .map(|h| h.id.to_string())
                .collect::<HashSet<_>>();

            total_recall += exact.intersection(&approx).count() as f64 / exact.len() as f64;
        }

        let recall = total_recall / QUERIES as f64;
        assert!(
            recall >= 0.90,
            "recall {recall:.3} at {DIM} dimensions is too low; the threshold that routes \
             collections to this index assumes the graph finds what a scan would"
        );
    }

    #[test]
    fn the_top_result_matches_exact_search() {
        // Recall can be high while the *ranking* is wrong; this checks the
        // nearest neighbour specifically.
        //
        // Nine of ten, not ten of ten — and the tolerance is principled, not a
        // shrug. Scores are recomputed from the stored vectors and sorted
        // (`scores_match_the_exact_path_exactly` pins that), so a disagreement
        // here can only mean the true nearest neighbour was not among the
        // graph's candidates — a recall miss, the bounded loss this index is
        // documented to trade. Graph construction is not deterministic: levels
        // are drawn from an RNG and insertion parallelism varies with core
        // count, which is how this failed on a two-core CI runner while
        // passing forty consecutive local runs. A *ranking* bug cannot hide in
        // the tolerance: with exact scores, misordering would break most
        // queries, not one. The 9/10 bar matches the ≥ 0.90 recall test above.
        let (engine, shadow, _dir) = setup(200, 16);
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, 16).unwrap();
        let options = SearchOptions { k: 5, metric: Metric::Cosine, per_document: 1 };

        let mut agreed = 0;
        for q in 0..10u64 {
            let query = pseudo_random(50_000 + q, 16);
            let exact =
                crate::search::vector_search(&engine, &shadow, &query, &options, None).unwrap();
            let approx = index.search(&engine, &shadow, &query, &options, None).unwrap();
            if approx[0].id == exact[0].id {
                agreed += 1;
            }
        }
        assert!(
            agreed >= 9,
            "only {agreed}/10 nearest neighbours agreed with the exact scan; one miss is a \
             recall roll, this is a ranking bug"
        );
    }

    #[test]
    fn scores_match_the_exact_path_exactly() {
        // Scores are recomputed rather than derived from graph distances, so
        // they must be byte-identical to what a scan reports.
        //
        // Compared **per document**, not per rank. This test used to assert
        // `approx[0].score == exact[0].score`, which holds only while the
        // approximate search happens to return the same top-1 as the scan —
        // and it is an approximate index over a graph built from a randomised
        // layer assignment (see `build`), so it does not always. When it did
        // not, this test failed with two unequal floats and the appearance of
        // a scoring bug, where the actual event was a recall roll that
        // `recall_against_exact_search_is_high` explicitly tolerates one of in
        // ten. It blocked a release that way.
        //
        // Per-document is also the stronger claim: it checks every result the
        // index returned rather than only the first, and it is what the
        // sentence above actually says.
        let (engine, shadow, _dir) = setup(50, 8);
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, 8).unwrap();
        let options = SearchOptions { k: 3, metric: Metric::Cosine, per_document: 1 };
        let query = pseudo_random(999, 8);

        let exact = crate::search::vector_search(&engine, &shadow, &query, &options, None).unwrap();
        let approx = index.search(&engine, &shadow, &query, &options, None).unwrap();

        assert!(!approx.is_empty(), "the index returned nothing to compare");
        let mut compared = 0;
        for hit in &approx {
            // A document the scan did not return at this `k` is a recall miss,
            // which is not what this test is about. Skipping it is not a hole:
            // the assertion below requires at least one comparison, and recall
            // itself is covered by its own tests.
            let Some(scanned) = exact.iter().find(|e| e.id == hit.id) else { continue };
            assert_eq!(
                hit.score, scanned.score,
                "document {} scored {} through the index and {} through the scan; scores are \
                 recomputed from the stored vector on both paths, so they must agree bit for bit",
                hit.id, hit.score, scanned.score
            );
            compared += 1;
        }
        assert!(
            compared > 0,
            "no document was returned by both paths, so nothing was actually compared — that is a \
             total recall failure rather than the scoring question this test asks"
        );
    }

    #[test]
    fn a_filter_restricts_the_results() {
        // Half of the documents are allowed, not two of them. The graph's
        // construction is randomised, and a point it leaves hard to reach is
        // a recall roll — the one-in-ten `recall_against_exact_search_is_high`
        // explicitly tolerates, and the roll `a_score_through_the_index…`
        // once blocked a release on. Allowing two ids out of a hundred made
        // this test a bet that neither was that point, and it lost on CI.
        // With fifty allowed, an empty result would need every one of them
        // unreachable, which is a broken graph rather than a roll. What the
        // test asserts is unchanged: nothing outside the filter comes back,
        // and the filter does not come back empty.
        let (engine, shadow, _dir) = setup(100, 8);
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, 8).unwrap();
        let options = SearchOptions { k: 5, metric: Metric::Cosine, per_document: 1 };

        let allowed: HashSet<DocId> = (0..100).filter(|i| i % 2 == 0).map(DocId::Int64).collect();
        let hits =
            index.search(&engine, &shadow, &pseudo_random(1, 8), &options, Some(&allowed)).unwrap();

        assert!(!hits.is_empty(), "the filter should not exclude everything");
        assert!(hits.len() <= options.k, "no more than k: {}", hits.len());
        for hit in &hits {
            assert!(allowed.contains(&hit.id), "{:?} was filtered out", hit.id);
        }
    }

    #[test]
    fn growth_past_capacity_is_reported() {
        // Capacity is fixed at construction, so exceeding it must be visible
        // rather than silently dropping inserts.
        let (engine, shadow, _dir) = setup(20, 8);
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, 8).unwrap();
        assert!(!index.needs_rebuild(21), "a small change should not force a rebuild");
        assert!(index.needs_rebuild(10_000));
    }

    #[test]
    fn supported_metrics_build_and_search() {
        let (engine, shadow, _dir) = setup(40, 8);
        for metric in [Metric::Cosine, Metric::Euclidean] {
            assert!(HnswIndex::supports(metric));
            let index = HnswIndex::build(&engine, &shadow, metric, 8).unwrap();
            let options = SearchOptions { k: 3, metric, per_document: 1 };
            let hits =
                index.search(&engine, &shadow, &pseudo_random(5, 8), &options, None).unwrap();
            assert!(!hits.is_empty(), "{metric:?} returned nothing");
        }
    }

    #[test]
    fn dot_is_refused_rather_than_panicking() {
        // anndists::DistDot asserts `1 - dot >= 0`, which only holds for
        // unit-length vectors. Real embeddings routinely exceed that, and the
        // assert aborts the process rather than returning an error.
        assert!(!HnswIndex::supports(Metric::Dot));

        let (engine, shadow, _dir) = setup(20, 8);
        assert!(
            HnswIndex::build(&engine, &shadow, Metric::Dot, 8).is_err(),
            "building a dot index must fail cleanly, not panic later during a query"
        );
    }

    // -----------------------------------------------------------------------
    // Build attempts (ADR-241)
    // -----------------------------------------------------------------------

    /// What a graph is, named by keys rather than by the ids an insertion
    /// order assigns: each point's level and its neighbours at every layer.
    type Shape = std::collections::BTreeMap<String, (u8, Vec<Vec<String>>)>;

    fn shape(index: &HnswIndex) -> Shape {
        let Graph::Cosine(g) = &index.graph else { unimplemented!("cosine fixtures") };
        g.get_point_indexation()
            .into_iter()
            .map(|point| {
                let layers = point
                    .get_neighborhood_id()
                    .iter()
                    .map(|layer| {
                        let mut keys: Vec<String> =
                            layer.iter().map(|n| index.keys[n.d_id].clone()).collect();
                        keys.sort();
                        keys
                    })
                    .collect();
                (index.keys[point.get_origin_id()].clone(), (point.get_point_id().0, layers))
            })
            .collect()
    }

    /// An attempt builds one graph, and each retry another.
    ///
    /// hnsw_rs 0.3.5 seeds every graph's level draw with one constant, so the
    /// same records inserted in the same order build the same graph: the
    /// first assertion is that property, and the reason the second can fail.
    /// A retry that kept the order would rebuild the graph it was retrying.
    #[test]
    fn an_attempt_builds_one_graph_and_each_retry_another() {
        let (engine, shadow, _dir) = setup(200, 16);
        let built = |attempt| {
            shape(&HnswIndex::build_once(&engine, &shadow, Metric::Cosine, 16, attempt).unwrap().0)
        };
        let attempts: Vec<Shape> = (0..=MAX_BUILD_ATTEMPTS).map(built).collect();
        assert_eq!(built(0), attempts[0], "one attempt, two graphs: the build is not reproducible");
        for (a, first) in attempts.iter().enumerate() {
            for (b, second) in attempts.iter().enumerate().skip(a + 1) {
                assert_ne!(first, second, "attempts {a} and {b} built the same graph");
            }
        }
    }

    /// A build that loses data is retried, each time in that attempt's order,
    /// and what it returns is the last attempt's graph. A collection of one
    /// vector repeated loses data in every draw: a probe among hundreds of
    /// points at distance zero cannot tell itself from the rest, so every
    /// attempt is judged lost and `build` runs them all.
    #[test]
    fn a_build_that_loses_data_retries_in_each_attempts_order() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        engine.create_collection("app", "docs").unwrap();
        engine.configure_vectors("app", "docs", config(4)).unwrap();
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        for i in 0..300 {
            let source = DocId::Int64(i);
            let record = VectorRecord {
                source: source.clone(),
                chunk: 0,
                source_hlc: Hlc::new(1, 0),
                vector: vec![1.0, 0.0, 0.0, 0.0],
                text: "the same".into(),
                config: None,
            };
            engine.put_vectors(&shadow, &source, &[record]).unwrap();
        }
        let (first, reach) = HnswIndex::build_once(&engine, &shadow, Metric::Cosine, 4, 0).unwrap();
        assert!(
            reach.unreachable >= REACHABILITY_MAX_UNREACHABLE,
            "the fixture must lose data in a draw to be retried: {reach:?}"
        );
        let last = HnswIndex::build_once(&engine, &shadow, Metric::Cosine, 4, MAX_BUILD_ATTEMPTS)
            .unwrap()
            .0;
        let returned = HnswIndex::build(&engine, &shadow, Metric::Cosine, 4).unwrap();
        assert_ne!(shape(&returned), shape(&first), "the retries rebuilt the first draw");
        assert_eq!(shape(&returned), shape(&last), "not the last attempt's graph");
    }

    /// A build links every point its insertion cut off at layer 0, so none is
    /// left that no search reaches (ADR-241).
    ///
    /// Two fixtures that the library's insertion cuts points off in, measured
    /// here first by inserting the same vectors in the same order with the
    /// same parameters and reading layer 0: 300 copies of one vector (every
    /// list fills with copies at distance zero and drops the newcomer: about
    /// two thirds are cut off), and 1,500 uniformly random 96-dimensional
    /// vectors (a few, in some orders). The build must leave none.
    #[test]
    fn a_build_links_every_point_its_insertion_cut_off() {
        for (name, count, dim, same) in
            [("one vector repeated", 300, 4, true), ("uniform 96-d", 1_500, 96, false)]
        {
            let dir = tempfile::tempdir().unwrap();
            let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
            engine.create_collection("app", "docs").unwrap();
            engine.configure_vectors("app", "docs", config(dim)).unwrap();
            let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
            let vector = |i: usize| {
                if same {
                    let mut v = vec![0.0; dim];
                    v[0] = 1.0;
                    v
                } else {
                    pseudo_random(i as u64 + 1, dim)
                }
            };
            engine
                .write_batch(kimmy_storage::WriterHolder::Embedding, |scope| {
                    for i in 0..count {
                        let source = DocId::Int64(i as i64);
                        let record = VectorRecord {
                            source: source.clone(),
                            chunk: 0,
                            source_hlc: Hlc::new(1, 0),
                            vector: vector(i),
                            text: String::new(),
                            config: None,
                        };
                        let write = kimmy_storage::VectorWrite::encode(
                            &source,
                            &ChunkSource::from(&source),
                            &[record],
                        )?;
                        scope.put_vectors(&shadow, write)?;
                    }
                    Ok(())
                })
                .unwrap();

            // The precondition: the library's insertion alone cuts points off,
            // in the first attempt's order that does so (the same one on every
            // run: a build is a function of its records and its attempt).
            let mut walked: Vec<Vec<f32>> = Vec::new();
            engine
                .for_each_vector(&shadow, kimmy_storage::WalkScope::Request, |_, record| {
                    walked.push(record.vector);
                    Ok(true)
                })
                .unwrap();
            let cut_by_insertion = |attempt: usize| {
                let mut ordered = walked.clone();
                insertion_order(&mut ordered, attempt);
                let raw = Hnsw::<f32, DistCosine>::new(
                    MAX_CONNECTIONS,
                    count,
                    MAX_LAYERS,
                    EF_CONSTRUCTION,
                    DistCosine {},
                );
                for (i, v) in ordered.iter().enumerate() {
                    raw.insert((v.as_slice(), i));
                }
                Layer0::read(&raw).cut_off().len()
            };
            let Some((attempt, before)) =
                (0..32).map(|a| (a, cut_by_insertion(a))).find(|(_, cut)| *cut > 0)
            else {
                panic!("{name}: no order of 32 cuts a point off, so nothing tests the linking");
            };

            let (index, reach) =
                HnswIndex::build_once(&engine, &shadow, Metric::Cosine, dim, attempt).unwrap();
            assert_eq!(
                cut_off(&index),
                (0, 0),
                "{name}: {before} points cut off before linking; points with no layer-0 edge \
                 into them, and points outside the entry point's component, after it"
            );
            assert!(reach.linked >= before, "{name}: {before} cut off, {} edges", reach.linked);
        }
    }

    // -----------------------------------------------------------------------
    // Chunk ids (ADR-239)
    // -----------------------------------------------------------------------

    /// `n` documents with UUID `_id`s (binary subtype 4), each chunk stored
    /// under its own id as the embedding worker stores it, the first at the
    /// query `[1, 0]` and the rest spread away from it.
    fn uuid_documents(n: u8) -> (Engine, CollectionMeta, Vec<DocId>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        engine.create_collection("app", "docs").unwrap();
        engine.configure_vectors("app", "docs", config(2)).unwrap();
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let mut ids = Vec::new();
        for i in 0..n {
            let id = DocId::Binary(vec![i; 16]);
            let angle = f32::from(i) * 0.05;
            put_as_uuid(&engine, &shadow, &id, vec![angle.cos(), angle.sin()], &format!("doc {i}"));
            ids.push(id);
        }
        (engine, shadow, ids, dir)
    }

    /// The graph's walk of `width` candidates for `query` returns the chunk
    /// key of each of `documents` (UUIDs, as `uuid_documents` stores them).
    /// A precondition: a test that asserts on hits holds the walk to this
    /// first, so that its own red names what the hits add, not graph recall.
    fn assert_walk_returns(index: &HnswIndex, query: &[f32], width: usize, documents: &[DocId]) {
        let walked = index.search_keys(query, width).unwrap();
        for document in documents {
            let DocId::Binary(bytes) = document else { panic!("a binary") };
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            let key = format!("~b:{hex}.04#0");
            assert!(
                walked.contains(&key),
                "graph recall: the walk of {width} returned {} keys and not {key}",
                walked.len()
            );
        }
    }

    fn put_as_uuid(
        engine: &Engine,
        shadow: &CollectionMeta,
        id: &DocId,
        vector: Vec<f32>,
        text: &str,
    ) {
        let DocId::Binary(bytes) = id else { panic!("a binary") };
        let source = ChunkSource::Binary { subtype: 4, bytes: bytes.clone() };
        let record = VectorRecord {
            source: id.clone(),
            chunk: 0,
            source_hlc: Hlc::new(1, 0),
            vector,
            text: text.into(),
            config: None,
        };
        let write = kimmy_storage::VectorWrite::encode(id, &source, &[record]).unwrap();
        engine
            .write_batch(kimmy_storage::WriterHolder::Embedding, |scope| {
                scope.put_vectors(shadow, write)
            })
            .unwrap();
    }

    /// Test 18: the graph finds a subtype-4 document's chunks. Keyed by an id
    /// recomputed from the record, which carries no subtype, it named ids no
    /// chunk is stored under and loaded nothing.
    ///
    /// Deterministic: the graph is a function of the records and the attempt
    /// (`insertion_order`), so this builds one graph on every run. And the
    /// walk is checked first, so a red names graph recall, not the keys.
    #[test]
    fn the_graph_finds_the_chunks_of_a_uuid_document() {
        let (engine, shadow, ids, _dir) = uuid_documents(40);
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, 2).unwrap();
        assert_eq!(index.len(), 40);
        let hex: String = std::iter::repeat_n("00", 16).collect();
        assert!(index.keys.contains(&format!("~b:{hex}.04#0")), "keyed by the stored id");
        let options = SearchOptions { k: 3, metric: Metric::Cosine, per_document: 1 };
        assert_walk_returns(&index, &[1.0, 0.0], walk_width(options.k, false), &ids[..3]);
        let hits = index.search(&engine, &shadow, &[1.0, 0.0], &options, None).unwrap();
        assert_eq!(hits.len(), 3);
        assert_eq!(hits[0].id, ids[0]);
        assert_eq!(hits[0].text, "doc 0");
    }

    /// Test 16, the graph's direction: a filtered search admits a candidate by
    /// the document its record names, after the load. The nearest chunk sits
    /// at an admitted document's id and names another one (debris); it is not
    /// returned, though its key names the admitted document.
    #[test]
    fn the_graph_admits_a_candidate_by_its_record_not_its_key() {
        let (engine, shadow, ids, _dir) = uuid_documents(40);
        let hex: String = std::iter::repeat_n("01", 16).collect();
        let debris = VectorRecord {
            source: ids[39].clone(),
            chunk: 1,
            source_hlc: Hlc::new(1, 0),
            vector: vec![1.0, 0.0],
            text: "debris".into(),
            config: None,
        };
        let doc = bson::serialize_to_document(&debris).unwrap();
        engine.replace(&shadow, &DocId::String(format!("~b:{hex}.04#1")), doc, true).unwrap();
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, 2).unwrap();
        let allowed: HashSet<DocId> = [ids[1].clone(), ids[2].clone()].into_iter().collect();
        let options = SearchOptions { k: 5, metric: Metric::Cosine, per_document: 3 };

        let graph = index.search(&engine, &shadow, &[1.0, 0.0], &options, Some(&allowed)).unwrap();
        let exact =
            crate::search::vector_search(&engine, &shadow, &[1.0, 0.0], &options, Some(&allowed))
                .unwrap();
        let keyed =
            crate::search::vector_search_among(&engine, &shadow, &[1.0, 0.0], &options, &ids[1..3])
                .unwrap();
        for (path, hits) in [("graph", graph), ("exact", exact), ("keyed", keyed)] {
            let got: Vec<(DocId, String)> = hits.into_iter().map(|h| (h.id, h.text)).collect();
            assert_eq!(
                got,
                [(ids[1].clone(), "doc 1".to_string()), (ids[2].clone(), "doc 2".to_string())],
                "{path}"
            );
        }
    }

    /// Test 18: a snapshot in the format a 0.50 build wrote, whose keys were
    /// recomputed from the records, is refused, and so rebuilt.
    #[test]
    fn a_format_2_snapshot_is_refused() {
        let (engine, shadow, _ids, _dir) = uuid_documents(10);
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, 2).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snapshot");
        index.save(&path).unwrap();
        assert!(HnswIndex::load(&path, Metric::Cosine, 2, shadow.created).is_ok(), "the control");

        let mut meta: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path.join("meta.json")).unwrap()).unwrap();
        meta["format"] = serde_json::json!(2);
        std::fs::write(path.join("meta.json"), serde_json::to_vec(&meta).unwrap()).unwrap();
        let refused = HnswIndex::load(&path, Metric::Cosine, 2, shadow.created);
        assert!(
            matches!(&refused, Err(VectorError::Snapshot(detail)) if detail.contains("format 2")),
            "a format-2 graph keyed by 0.50's ids must not be adopted: {:?}",
            refused.as_ref().err()
        );
    }

    /// The filtered graph path loads a candidate only when its key can hold an
    /// admitted document's chunk, and that includes the ids a 0.50 build
    /// wrote: an admitted binary whose only chunk is at `bin(16 bytes)#0`, and
    /// an admitted reserved String whose only chunk is at its raw text, are
    /// found; another document's chunk at those ids is still refused by its
    /// record.
    #[test]
    fn the_filtered_graph_finds_a_document_whose_only_chunk_is_under_its_050_id() {
        let (engine, shadow, ids, _dir) = uuid_documents(40);
        let old_binary = DocId::Binary(vec![200; 16]);
        let tilde = DocId::String("~x".into());
        for (key, source, vector, text) in [
            ("bin(16 bytes)#0", &old_binary, vec![1.0, 0.0], "old binary"),
            ("~x#0", &tilde, vec![0.999, 0.04], "old tilde"),
        ] {
            let record = VectorRecord {
                source: source.clone(),
                chunk: 0,
                source_hlc: Hlc::new(1, 0),
                vector,
                text: text.into(),
                config: None,
            };
            let doc = bson::serialize_to_document(&record).unwrap();
            engine.replace(&shadow, &DocId::String(key.into()), doc, true).unwrap();
        }
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, 2).unwrap();
        let allowed: HashSet<DocId> =
            [old_binary.clone(), tilde.clone(), ids[1].clone()].into_iter().collect();
        let options = SearchOptions { k: 5, metric: Metric::Cosine, per_document: 1 };
        // The walk returns every admitted chunk first, so a red below names
        // the key pre-filter or the admission, and a red here graph recall.
        // This test failed about one run in sixteen on hnsw_rs 0.3.4, whose
        // graph left `~x#0` unreachable: that red was here, not below.
        let walked = index.search_keys(&[1.0, 0.0], walk_width(options.k, true)).unwrap();
        for key in ["bin(16 bytes)#0", "~x#0"] {
            assert!(
                walked.iter().any(|k| k == key),
                "graph recall: the walk of {} returned {} keys and not {key}",
                walk_width(options.k, true),
                walked.len()
            );
        }
        assert_walk_returns(&index, &[1.0, 0.0], walk_width(options.k, true), &ids[1..3]);
        let hits = index.search(&engine, &shadow, &[1.0, 0.0], &options, Some(&allowed)).unwrap();
        let got: Vec<(DocId, String)> = hits.into_iter().map(|h| (h.id, h.text)).collect();
        assert_eq!(
            got,
            [
                (old_binary, "old binary".to_string()),
                (tilde, "old tilde".to_string()),
                (ids[1].clone(), "doc 1".to_string())
            ]
        );
        // A 16-byte binary not admitted keeps its own chunks out.
        let only: HashSet<DocId> = [ids[2].clone()].into_iter().collect();
        let hits = index.search(&engine, &shadow, &[1.0, 0.0], &options, Some(&only)).unwrap();
        assert_eq!(hits.iter().map(|h| h.id.clone()).collect::<Vec<_>>(), [ids[2].clone()]);
    }

    /// The key pre-filter loads what the filter can admit and nothing else:
    /// a candidate whose key names no admitted document, in its run or under a
    /// 0.50 id, is never read.
    #[test]
    fn the_key_pre_filter_admits_by_run_and_by_050_id_only() {
        let allowed: HashSet<DocId> =
            [DocId::Binary(vec![1; 16]), DocId::String("7".into()), DocId::Int64(9)]
                .into_iter()
                .collect();
        let old = old_forms(&allowed);
        let hex: String = std::iter::repeat_n("01", 16).collect();
        for (key, expected) in [
            (format!("~b:{hex}.04#0"), true),
            (format!("~b:{hex}.00#3"), true),
            ("bin(16 bytes)#0".to_string(), true),
            ("~s:7#0".to_string(), true),
            ("7#0".to_string(), true),
            ("9#2".to_string(), true),
            ("8#0".to_string(), false),
            ("bin(8 bytes)#0".to_string(), false),
            ("~s:8#0".to_string(), false),
            ("junk".to_string(), false),
        ] {
            assert_eq!(may_hold(&allowed, &old, &key), expected, "{key}");
        }
    }
}
