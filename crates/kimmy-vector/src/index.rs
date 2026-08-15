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
use kimmy_core::{DocId, Metric, VectorRecord, similarity};
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

/// How many sampled misses mean the graph orphaned data, rather than merely
/// approximated.
///
/// **Not zero, and the difference matters.** A healthy graph still fails to
/// self-retrieve a couple of points in four hundred — a vector sitting among
/// near-duplicates gets edged out by a neighbour, which is approximation
/// working as designed. Measured over 400 builds the full-collection miss count
/// runs median 2, p99 10. In a [`REACHABILITY_SAMPLE`] of 128 that is around
/// 0.6 expected misses, so retrying on any miss at all would rebuild most
/// collections two extra times for nothing.
///
/// An orphaned build loses a tenth of the collection or more, which is ~13
/// expected misses in the same sample. Three separates them.
///
/// **The measured cost is one rebuild for 11.2% of builds** — about 1.11×
/// the build work overall, against eliminating a defect that silently dropped
/// a tenth of a collection out of every search result. Worth stating because
/// the first estimate here was 3%, taken from an assumed miss rate rather than
/// the observed one; the number above is from 1,500 builds.
const REACHABILITY_MAX_MISSES: usize = 3;

/// How many times a build that loses data is discarded and retried.
///
/// The layer draw is independent each time, so at a measured ~0.4% failure
/// rate two retries take the odds to roughly one in ten million. It is
/// bounded because an index that genuinely cannot be built well — a
/// pathological collection rather than an unlucky draw — must still return
/// something rather than loop.
const MAX_BUILD_ATTEMPTS: usize = 2;

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
    keys: Vec<String>,
}

/// Bumped when the snapshot layout changes; a mismatch is a rebuild, not a
/// migration.
const SNAPSHOT_FORMAT: u32 = 1;

/// Reload one graph from a leaked io handle, containing `hnsw_rs`'s panics.
///
/// The library `unwrap`s on a corrupt graph file — bad magic aborts the
/// process — and a torn file on disk must cost a rebuild, never the node.
/// Found by the corrupt-snapshot test, the same way `DistDot`'s assert was
/// found: by testing the failure path rather than trusting the library's.
/// `AssertUnwindSafe` is sound here: the closure touches only the leaked io
/// and the files it reads — no engine state a mid-load panic could tear.
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
        let io = Box::leak(Box::new(hnsw_rs::hnswio::HnswIo::new(&dir, "index")));
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

/// An approximate index over one collection's vectors.
pub struct HnswIndex {
    graph: Graph,
    /// Graph ids are dense integers, so this maps them back to chunk keys.
    keys: Vec<String>,
    metric: Metric,
    dim: usize,
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
        let mut built = Self::build_once(engine, shadow, metric, dim)?;
        for attempt in 1..=MAX_BUILD_ATTEMPTS {
            if built.1 < REACHABILITY_MAX_MISSES {
                return Ok(built.0);
            }
            debug!(
                attempt,
                misses = built.1,
                vectors = built.0.keys.len(),
                "HNSW build left data unreachable; rebuilding"
            );
            built = Self::build_once(engine, shadow, metric, dim)?;
        }
        if built.1 >= REACHABILITY_MAX_MISSES {
            // Never fail: a poor index still answers, and refusing to build one
            // would take vector search down entirely over a quality problem.
            // The caller gets a working index and the operator gets a warning.
            tracing::warn!(
                vectors = built.0.keys.len(),
                misses = built.1,
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
    ) -> Result<(Self, usize)> {
        if !Self::supports(metric) {
            return Err(VectorError::MalformedResponse {
                provider: "hnsw",
                detail: format!("{metric:?} has no approximate index; searches use an exact scan"),
            });
        }
        // Collected first so the expected element count is known before the
        // graph allocates its tables.
        let mut records = Vec::new();
        engine.for_each_vector(shadow, |record| {
            // A wrong width belongs to a different model and would corrupt
            // every distance the graph computes.
            if record.vector.len() == dim {
                records.push(record);
            }
            Ok(true)
        })?;

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

        let mut keys = Vec::with_capacity(records.len());
        for record in &records {
            let id = keys.len();
            let vector = record.vector.as_slice();
            match &graph {
                Graph::Cosine(g) => g.insert((vector, id)),
                Graph::Euclidean(g) => g.insert((vector, id)),
            }
            keys.push(VectorRecord::id(&record.source, record.chunk).to_string());
        }

        debug!(vectors = keys.len(), "built HNSW index");
        let index = Self { graph, keys, metric, dim };

        // Ask a spread sample of the stored vectors to find themselves. The
        // records are still in hand here, so this costs a few dozen searches
        // and no extra reads.
        let n = index.keys.len();
        let mut misses = 0;
        if n >= REACHABILITY_SAMPLE {
            let stride = (n / REACHABILITY_SAMPLE).max(1);
            for i in (0..n).step_by(stride).take(REACHABILITY_SAMPLE) {
                let found = index.search_keys(&records[i].vector, 1, None)?;
                if found.first() != index.keys.get(i) {
                    misses += 1;
                }
            }
        }
        Ok((index, misses))
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

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
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
    /// loader treats anything unreadable as absent anyway.
    pub fn save(&self, dir: &std::path::Path) -> Result<()> {
        use hnsw_rs::api::AnnT;

        let io = |e: std::io::Error| VectorError::Snapshot(e.to_string());
        let staging = dir.with_extension("build");
        let _ = std::fs::remove_dir_all(&staging);
        std::fs::create_dir_all(&staging).map_err(io)?;

        match &self.graph {
            Graph::Cosine(g) => g.file_dump(&staging, "index"),
            Graph::Euclidean(g) => g.file_dump(&staging, "index"),
        }
        .map_err(|e| VectorError::Snapshot(e.to_string()))?;

        let meta = SnapshotMeta {
            format: SNAPSHOT_FORMAT,
            metric: self.metric,
            dim: self.dim,
            keys: self.keys.clone(),
        };
        std::fs::write(
            staging.join("meta.json"),
            serde_json::to_vec(&meta).map_err(|e| VectorError::Snapshot(e.to_string()))?,
        )
        .map_err(io)?;

        let _ = std::fs::remove_dir_all(dir);
        std::fs::rename(&staging, dir).map_err(io)?;
        debug!(vectors = self.keys.len(), ?dir, "saved HNSW snapshot");
        Ok(())
    }

    /// Load a snapshot from `dir`, refusing one that describes a different
    /// index than the caller wants.
    ///
    /// The metric and dimension checks are the trust boundary: a snapshot
    /// written under an old vector configuration would compute every distance
    /// wrongly, which is worse than the rebuild it saves. Any other failure —
    /// missing files, torn writes, a format this build does not speak — is a
    /// plain `Err`, and the caller discards the snapshot and rebuilds.
    /// Correctness never depends on a load succeeding.
    pub fn load(dir: &std::path::Path, metric: Metric, dim: usize) -> Result<Self> {
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

        let graph = match metric {
            Metric::Cosine => load_graph::<DistCosine>(dir).map(Graph::Cosine),
            Metric::Euclidean => load_graph::<DistL2>(dir).map(Graph::Euclidean),
            Metric::Dot => Err("dot has no approximate index".to_string()),
        }
        .map_err(|e| corrupt(format!("graph reload: {e}")))?;

        debug!(vectors = meta.keys.len(), ?dir, "loaded HNSW snapshot");
        Ok(Self { graph, keys: meta.keys, metric, dim })
    }

    /// Approximate k-nearest neighbours, as chunk keys.
    ///
    /// `allowed` holds *document* ids; chunk keys are mapped back before
    /// comparing. Filtering happens after the graph walk, so the search is
    /// widened to compensate.
    pub fn search_keys(
        &self,
        query: &[f32],
        k: usize,
        allowed: Option<&HashSet<String>>,
    ) -> Result<Vec<String>> {
        if query.len() != self.dim {
            return Err(VectorError::DimensionMismatch { expected: self.dim, found: query.len() });
        }
        if self.keys.is_empty() {
            return Ok(Vec::new());
        }

        // A post-filter discards results after the walk, so a selective filter
        // needs a much wider search to still return k.
        let wanted = if allowed.is_some() { k * 8 } else { k };
        let ef = (wanted * EF_SEARCH_FACTOR).max(MIN_EF_SEARCH);

        let neighbours = match &self.graph {
            Graph::Cosine(g) => g.search(query, wanted, ef),
            Graph::Euclidean(g) => g.search(query, wanted, ef),
        };

        let mut out = Vec::with_capacity(neighbours.len());
        for neighbour in neighbours {
            let Some(key) = self.keys.get(neighbour.d_id) else {
                continue;
            };
            if let Some(allowed) = allowed {
                let matches = VectorRecord::parse_id(&DocId::String(key.clone()))
                    .is_some_and(|(source, _)| allowed.contains(&source));
                if !matches {
                    continue;
                }
            }
            out.push(key.clone());
        }
        Ok(out)
    }

    /// Approximate search returning full hits.
    ///
    /// Over-fetches from the graph, because the per-document cap can collapse
    /// several chunks of one document into a single result and would otherwise
    /// return fewer than `k`.
    pub fn search(
        &self,
        engine: &Engine,
        shadow: &CollectionMeta,
        query: &[f32],
        options: &SearchOptions,
        allowed: Option<&HashSet<String>>,
    ) -> Result<Vec<Hit>> {
        // Over-fetch: the per-document cap can collapse several chunks of one
        // document into a single result, which would otherwise return under k.
        let over_fetch = (options.k * 4).max(options.k + 10);
        let keys = self.search_keys(query, over_fetch, allowed)?;

        let mut hits = Vec::with_capacity(keys.len());
        for key in keys {
            let Some(record) = load_record(engine, shadow, &key)? else {
                // The index can lag storage; a missing record just means this
                // chunk was deleted since the build.
                continue;
            };
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
    use kimmy_core::{ChunkConfig, Hlc, ProviderConfig, VectorConfig};

    use super::*;

    fn config(dim: usize) -> VectorConfig {
        VectorConfig {
            fields: vec!["body".into()],
            provider: ProviderConfig::Byo,
            dim,
            metric: Metric::Cosine,
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
            };
            engine.put_vectors(&shadow, &source, &[record]).unwrap();
        }
        (engine, shadow, dir)
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
        assert!(index.search_keys(&pseudo_random(1, 8), 5, None).unwrap().is_empty());
    }

    #[test]
    fn a_query_of_the_wrong_width_is_rejected() {
        let (engine, shadow, _dir) = setup(10, 8);
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, 8).unwrap();
        assert!(matches!(
            index.search_keys(&[0.0; 4], 5, None).err(),
            Some(VectorError::DimensionMismatch { .. })
        ));
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
        let (engine, shadow, _dir) = setup(50, 8);
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, 8).unwrap();
        let options = SearchOptions { k: 3, metric: Metric::Cosine, per_document: 1 };
        let query = pseudo_random(999, 8);

        let exact = crate::search::vector_search(&engine, &shadow, &query, &options, None).unwrap();
        let approx = index.search(&engine, &shadow, &query, &options, None).unwrap();
        assert_eq!(approx[0].score, exact[0].score);
    }

    #[test]
    fn a_filter_restricts_the_results() {
        let (engine, shadow, _dir) = setup(100, 8);
        let index = HnswIndex::build(&engine, &shadow, Metric::Cosine, 8).unwrap();
        let options = SearchOptions { k: 5, metric: Metric::Cosine, per_document: 1 };

        let allowed: HashSet<String> = ["7".to_string(), "9".to_string()].into_iter().collect();
        let hits =
            index.search(&engine, &shadow, &pseudo_random(1, 8), &options, Some(&allowed)).unwrap();

        assert!(!hits.is_empty(), "the filter should not exclude everything");
        for hit in &hits {
            assert!(allowed.contains(&hit.id.to_string()), "{:?} was filtered out", hit.id);
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
}
