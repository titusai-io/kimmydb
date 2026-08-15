//! The embedding worker.
//!
//! This is where the oplog design pays off. "Keep vectors in sync with
//! documents" reduces to "consume the change stream", which already works —
//! including resume-after-restart, gap-free delivery, and recovery from
//! falling behind. The worker holds no special privileges and uses no
//! machinery a WebSocket subscriber could not.
//!
//! ```text
//!   document write ──► oplog ──► change stream ──► worker ──► {coll}.__vectors
//!                                                    │
//!                                          extract → chunk → embed
//! ```
//!
//! **Off the write path.** A write returns as soon as its oplog entry is
//! durable; embedding happens behind it. That is the only reason a remote
//! provider — which can be slow, rate-limited, or briefly down — is tolerable
//! at all.

use std::collections::HashMap;
use std::sync::Arc;

use kimmy_core::{Hlc, OpKind, VectorConfig, VectorRecord, path};
use kimmy_storage::{ChangeEvent, CollectionMeta, Engine, WatchOptions, WatchScope};
use tracing::{debug, info, warn};

use crate::error::{Result, VectorError};
use crate::provider::{self, EmbeddingProvider};

/// Name under which the worker records its oplog position.
pub const CONSUMER: &str = "embedding-worker";

/// How long to wait before retrying after a provider failure.
///
/// A remote provider being briefly unavailable must not cost the position:
/// the worker retries the same entry rather than skipping it, so a rate limit
/// delays embedding but never silently loses it.
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

/// Keeps a collection's vectors in step with its documents.
pub struct EmbeddingWorker {
    engine: Arc<Engine>,
    /// Providers are built once per configuration and reused — constructing a
    /// local one loads a model, which is far too expensive per document.
    ///
    /// Keyed with the configuration that built each one, because a
    /// reconfigured collection must not keep embedding through the *old*
    /// provider — which it silently did until the reindex work made
    /// reconfiguration a live event. `None` marks a test-injected provider
    /// that no configuration should evict.
    providers: HashMap<u64, (Option<VectorConfig>, Arc<dyn EmbeddingProvider>)>,
}

/// What one processed entry did, so tests and metrics can tell the cases apart.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Outcome {
    /// Vectors were written.
    Embedded { chunks: usize },
    /// The document's vectors were removed.
    Removed,
    /// A `ConfigureVectors` entry triggered a scan of the whole collection.
    Backfilled { embedded: usize },
    /// Nothing to do: not a vector-enabled collection, no embeddable text, or
    /// the vectors were already current.
    Skipped,
}

impl EmbeddingWorker {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine, providers: HashMap::new() }
    }

    /// Run until the change stream ends.
    ///
    /// Starts from the recorded position, or from the beginning of the oplog
    /// on first run — which is what backfills a collection that already had
    /// documents when embedding was enabled.
    pub async fn run(&mut self) -> Result<()> {
        let resume = self.engine.consumer_position(CONSUMER)?;
        let options = WatchOptions {
            resume_after: resume,
            // No recorded position means everything so far is unembedded.
            start_at: resume.is_none().then_some(Hlc::ZERO),
        };

        let mut stream = self.engine.watch(WatchScope::Cluster, options)?;
        info!(resumed = resume.is_some(), "embedding worker started");

        while let Some(event) = stream.next(&self.engine).await {
            let ChangeEvent::Change { entry, token } = event else {
                // An invalidated stream cannot be trusted to be gap-free, and
                // silently continuing would leave documents unembedded.
                warn!("change stream invalidated; embedding worker stopping");
                break;
            };

            // Retry rather than advance: losing an entry means a document stays
            // unembedded with nothing to notice it.
            loop {
                match self.process(&entry).await {
                    Ok(_) => break,
                    Err(e) if e.is_retryable() => {
                        warn!(error = %e, "embedding failed; retrying");
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                    Err(e) => {
                        // A permanent failure (bad config, wrong dimension)
                        // would retry forever. Record it and move on, so one
                        // poisoned document cannot stall every other one.
                        warn!(error = %e, "embedding permanently failed; skipping this entry");
                        break;
                    }
                }
            }

            // Only after the work is done, so a crash re-processes rather than
            // skips. Re-processing is safe because embedding is idempotent.
            self.engine.put_consumer_position(CONSUMER, token)?;
        }
        Ok(())
    }

    /// Handle one oplog entry.
    pub async fn process(&mut self, entry: &kimmy_core::OplogEntry) -> Result<Outcome> {
        // Not every oplog entry describes a mutation. A unique-violation entry
        // reports something that happened *to* the data and has nothing to
        // embed. It would be filtered by the `doc_id` check below anyway, but
        // relying on that would make the safety incidental.
        if entry.kind == OpKind::UniqueViolation {
            return Ok(Outcome::Skipped);
        }

        // A configuration change is the reindex trigger. The entry arrives
        // through the same stream as every document write — the oplog is the
        // wake-up here exactly as it is everywhere else — and the position is
        // recorded only after the scan completes, so a crash mid-backfill
        // replays the entry and the staleness check skips what already
        // landed. Before this, enabling embedding on a collection that
        // already held documents embedded *nothing*: the M2 "backfill" was
        // the worker's first-ever run starting from zero, and a worker whose
        // position had ever advanced was past those entries forever.
        if entry.kind == OpKind::ConfigureVectors {
            return self.backfill_from_entry(entry).await;
        }

        let Some(source) = entry.doc_id.clone() else {
            return Ok(Outcome::Skipped);
        };
        let Some(collection) = self.engine.collection_by_id(entry.collection)? else {
            return Ok(Outcome::Skipped);
        };
        // A shadow collection is the worker's own output; embedding it would
        // recurse.
        let Some(config) = collection.vector.clone() else {
            return Ok(Outcome::Skipped);
        };
        if kimmy_core::vector_meta::is_shadow(&collection.name) {
            return Ok(Outcome::Skipped);
        }
        // `byo` means the client supplies vectors, so there is nothing to do.
        if !config.provider.embeds_server_side() {
            return Ok(Outcome::Skipped);
        }

        let shadow = self.engine.get_collection(
            &collection.db,
            &kimmy_core::vector_meta::shadow_name(&collection.name),
        )?;

        if entry.kind == OpKind::Delete {
            let removed = self.engine.delete_vectors(&shadow, &source)?;
            debug!(chunks = removed, "removed vectors for a deleted document");
            return Ok(Outcome::Removed);
        }

        let Some(document) = entry.document()? else {
            return Ok(Outcome::Skipped);
        };

        // The entry carries the version this work is for. Anything newer has
        // its own entry coming, so redoing older work would be wasted.
        if !self.engine.vectors_are_stale(&shadow, &source, entry.stamp.hlc)? {
            return Ok(Outcome::Skipped);
        }

        let text = extract_text(&document, &config);
        let chunks = config.chunk.split(&text);
        if chunks.is_empty() {
            // No embeddable text: drop any vectors from a previous version
            // that did have some, or they would outlive their source text.
            self.engine.delete_vectors(&shadow, &source)?;
            return Ok(Outcome::Skipped);
        }

        let provider = self.provider_for(collection.id.0, &config)?;
        let vectors = provider.embed(&chunks).await?;

        let records: Vec<VectorRecord> = chunks
            .into_iter()
            .zip(vectors)
            .enumerate()
            .map(|(i, (text, vector))| VectorRecord {
                source: source.clone(),
                chunk: i as u32,
                source_hlc: entry.stamp.hlc,
                vector,
                text,
            })
            .collect();

        let count = records.len();
        self.engine.put_vectors(&shadow, &source, &records)?;
        debug!(chunks = count, "embedded a document");
        Ok(Outcome::Embedded { chunks: count })
    }

    /// React to a `ConfigureVectors` entry: scan the collection and bring
    /// every document's vectors up to date.
    ///
    /// The scan walks the **collection**, not the oplog — the oplog may have
    /// collected the entries that created these documents, and the documents
    /// themselves are the durable source. Per document the staleness check
    /// decides: already-current vectors are skipped, so replaying this entry
    /// after a crash re-does only what had not landed, and a configuration
    /// change that alters nothing a document produced (a metric change, say)
    /// costs a scan and no embedding.
    async fn backfill_from_entry(&mut self, entry: &kimmy_core::OplogEntry) -> Result<Outcome> {
        let Some(body) = &entry.body else {
            return Ok(Outcome::Skipped);
        };
        let set: kimmy_core::VectorSet = match bson::deserialize_from_slice(body) {
            Ok(set) => set,
            Err(e) => {
                warn!(error = %e, "undecodable ConfigureVectors entry; skipping");
                return Ok(Outcome::Skipped);
            }
        };
        // Disabling embeds nothing, and byo has nothing the server could
        // compute — the client supplies vectors, so a backfill would only be
        // able to delete theirs.
        let Some(config) = set.config else {
            return Ok(Outcome::Skipped);
        };
        if !config.provider.embeds_server_side() {
            return Ok(Outcome::Skipped);
        }
        let Ok(collection) = self.engine.get_collection(&set.db, &set.collection) else {
            // Dropped since the entry was written; nothing to scan.
            return Ok(Outcome::Skipped);
        };
        let shadow = self
            .engine
            .get_collection(&set.db, &kimmy_core::vector_meta::shadow_name(&set.collection))?;

        // Whether this scan must re-embed regardless of per-document
        // staleness. The HLC check cannot see a configuration change —
        // configurations do not touch documents — so the decision comes from
        // a fingerprint of the configuration the last *completed* scan ran
        // under. Written only after the scan, so a crash mid-backfill leaves
        // it stale and the replayed entry redoes the whole scan: some
        // documents embed twice, which idempotent output makes harmless,
        // where the alternative — recording first — would leave the rest
        // embedded under the old model with nothing to notice.
        let fingerprint = config_fingerprint(&config);
        let force = self.engine.vector_fingerprint(collection.id)? != Some(fingerprint);

        // Ids first, documents re-read one at a time: the scan must not hold
        // a read transaction across provider calls, and holding every
        // document in memory would make backfill cost O(collection).
        let mut ids = Vec::new();
        self.engine.for_each_doc(&collection, |id, _| {
            ids.push(id);
            Ok(true)
        })?;

        let total = ids.len();
        let mut embedded = 0usize;
        for source in ids {
            // Retry transient provider failures per document, exactly as the
            // streaming path does; a permanent failure skips the document
            // rather than stalling the rest of the scan.
            loop {
                match self.embed_one(&collection, &shadow, &config, &source, force).await {
                    Ok(true) => {
                        embedded += 1;
                        break;
                    }
                    Ok(false) => break,
                    Err(e) if e.is_retryable() => {
                        warn!(error = %e, "backfill embedding failed; retrying");
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                    Err(e) => {
                        warn!(error = %e, ?source, "backfill permanently failed for a document");
                        break;
                    }
                }
            }
        }

        // The completed scan is what the fingerprint attests. Failing to
        // write it costs a redundant re-scan next time, never a gap.
        self.engine.put_vector_fingerprint(collection.id, fingerprint)?;
        info!(
            collection = %collection.name,
            embedded,
            total,
            "backfilled vectors after a configuration change"
        );
        Ok(Outcome::Backfilled { embedded })
    }

    /// Bring one document's vectors up to date. `Ok(true)` if work was done.
    ///
    /// `force` re-embeds even current-looking vectors — the configuration
    /// changed, so "current" was measured against the wrong ruler.
    async fn embed_one(
        &mut self,
        collection: &CollectionMeta,
        shadow: &CollectionMeta,
        config: &VectorConfig,
        source: &kimmy_core::DocId,
        force: bool,
    ) -> Result<bool> {
        // The stamp is the document's *current* version, read fresh — a
        // document replaced mid-scan is embedded at whichever version the
        // read sees, and the newer version's own oplog entry follows behind
        // this backfill in the stream.
        let Some(stamp) = self.engine.document_stamp(collection, source)? else {
            return Ok(false);
        };
        if !force && !self.engine.vectors_are_stale(shadow, source, stamp.hlc)? {
            return Ok(false);
        }
        let Some(document) = self.engine.get(collection, source)? else {
            return Ok(false);
        };

        let text = extract_text(&document, config);
        let chunks = config.chunk.split(&text);
        if chunks.is_empty() {
            self.engine.delete_vectors(shadow, source)?;
            return Ok(false);
        }

        let provider = self.provider_for(collection.id.0, config)?;
        let vectors = provider.embed(&chunks).await?;
        let records: Vec<VectorRecord> = chunks
            .into_iter()
            .zip(vectors)
            .enumerate()
            .map(|(i, (text, vector))| VectorRecord {
                source: source.clone(),
                chunk: i as u32,
                source_hlc: stamp.hlc,
                vector,
                text,
            })
            .collect();
        self.engine.put_vectors(shadow, source, &records)?;
        Ok(true)
    }

    /// A provider for one collection, built once per configuration.
    ///
    /// Rebuilt when the configuration it was built from no longer matches —
    /// a collection reconfigured to a new model must not keep embedding
    /// through the old one. A test-injected provider (`None` config) is
    /// never evicted.
    fn provider_for(
        &mut self,
        collection: u64,
        config: &VectorConfig,
    ) -> Result<Arc<dyn EmbeddingProvider>> {
        if let Some((built_from, existing)) = self.providers.get(&collection)
            && built_from.as_ref().is_none_or(|c| c == config)
        {
            return Ok(Arc::clone(existing));
        }
        let built: Arc<dyn EmbeddingProvider> =
            Arc::from(provider::build(&config.provider, config.dim)?);
        self.providers.insert(collection, (Some(config.clone()), Arc::clone(&built)));
        Ok(built)
    }

    /// Replace the provider for a collection. Used by tests to inject a fake.
    pub fn set_provider(&mut self, collection: u64, provider: Arc<dyn EmbeddingProvider>) {
        self.providers.insert(collection, (None, provider));
    }
}

/// A stable fingerprint of a vector configuration.
///
/// FNV-1a over the JSON serialization. Stable across restarts, which is what
/// the backfill decision needs; a build that changes the config's *shape*
/// changes the fingerprint and costs one spurious full re-embed after
/// upgrade, which is the safe direction to be wrong in.
fn config_fingerprint(config: &VectorConfig) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x100_0000_01b3;

    let bytes = serde_json::to_vec(config).unwrap_or_default();
    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Gather the configured fields into one block of text.
///
/// Fields are joined with a blank line so that a chunk boundary falling
/// between two fields does not glue unrelated sentences together.
fn extract_text(document: &bson::Document, config: &VectorConfig) -> String {
    let mut parts = Vec::new();
    for field in &config.fields {
        for value in path::resolve(document, field) {
            match value {
                bson::Bson::String(s) if !s.trim().is_empty() => parts.push(s.clone()),
                // Arrays of strings are common (tags, paragraphs) and worth
                // including; other types have no meaningful text.
                bson::Bson::Array(items) => {
                    for item in items {
                        if let bson::Bson::String(s) = item
                            && !s.trim().is_empty()
                        {
                            parts.push(s.clone());
                        }
                    }
                }
                _ => {}
            }
        }
    }
    parts.join("\n\n")
}

impl VectorError {
    /// Whether retrying could plausibly succeed.
    ///
    /// Transport failures and rate limits are temporary; a wrong dimension or
    /// a missing API key will fail identically forever, and retrying those
    /// would stall every document behind them.
    pub fn is_retryable(&self) -> bool {
        match self {
            VectorError::Transport { .. } => true,
            // 429 and 5xx are worth another attempt; 4xx is a bad request.
            VectorError::ProviderRejected { status, .. } => {
                *status == 429 || (500..600).contains(status)
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use bson::doc;
    use kimmy_core::{ChunkConfig, Metric, ProviderConfig};
    use kimmy_storage::CollectionMeta;

    use super::*;

    /// A provider that returns deterministic vectors without any I/O.
    struct FakeProvider {
        dim: usize,
        /// Fails this many times before succeeding, to exercise retry.
        fail_times: std::sync::atomic::AtomicUsize,
        /// Fails forever with an error retrying cannot fix, to exercise the
        /// other half of the classification.
        permanent: std::sync::atomic::AtomicBool,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl FakeProvider {
        fn new(dim: usize) -> Arc<Self> {
            Arc::new(Self {
                dim,
                fail_times: Default::default(),
                permanent: Default::default(),
                calls: Default::default(),
            })
        }
    }

    #[async_trait]
    impl EmbeddingProvider for FakeProvider {
        async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
            use std::sync::atomic::Ordering;
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.permanent.load(Ordering::SeqCst) {
                // A 400 is the canonical "retrying will not help".
                return Err(VectorError::ProviderRejected {
                    provider: "fake",
                    status: 400,
                    detail: "injected permanent failure".into(),
                });
            }
            if self.fail_times.load(Ordering::SeqCst) > 0 {
                self.fail_times.fetch_sub(1, Ordering::SeqCst);
                return Err(VectorError::Transport { provider: "fake", detail: "injected".into() });
            }
            // Encode the text length so different text yields different vectors.
            Ok(texts
                .iter()
                .map(|t| {
                    let mut v = vec![0.0; self.dim];
                    v[0] = t.chars().count() as f32;
                    v
                })
                .collect())
        }

        fn dim(&self) -> usize {
            self.dim
        }

        fn name(&self) -> &'static str {
            "fake"
        }
    }

    fn config(fields: &[&str]) -> VectorConfig {
        VectorConfig {
            fields: fields.iter().map(|s| s.to_string()).collect(),
            // Ollama rather than Byo: the worker skips Byo entirely, and these
            // tests inject a fake provider anyway.
            provider: ProviderConfig::Ollama {
                model: "m".into(),
                endpoint: "http://localhost:1".into(),
            },
            dim: 4,
            metric: Metric::Cosine,
            chunk: ChunkConfig { max_chars: 20, overlap: 5 },
        }
    }

    /// An engine with an embedding-enabled collection and a fake provider.
    async fn setup() -> (Arc<Engine>, CollectionMeta, EmbeddingWorker, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        engine.create_collection("app", "docs").unwrap();
        engine.configure_vectors("app", "docs", config(&["title", "body"])).unwrap();
        // Re-read so the returned metadata carries the vector config.
        let coll = engine.get_collection("app", "docs").unwrap();

        let mut worker = EmbeddingWorker::new(Arc::clone(&engine));
        worker.set_provider(coll.id.0, FakeProvider::new(4));
        (engine, coll, worker, dir)
    }

    /// The oplog entry a write produced.
    fn last_entry(engine: &Engine) -> kimmy_core::OplogEntry {
        engine.read_oplog_from(Hlc::ZERO, 10_000).unwrap().pop().expect("an entry")
    }

    #[tokio::test]
    async fn a_written_document_gets_embedded() {
        let (engine, coll, mut worker, _dir) = setup().await;
        let id = engine.insert(&coll, doc! { "_id": 1i64, "title": "hello world" }).unwrap();

        let outcome = worker.process(&last_entry(&engine)).await.unwrap();
        assert!(matches!(outcome, Outcome::Embedded { chunks: 1 }));

        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let vectors = engine.get_vectors(&shadow, &id).unwrap();
        assert_eq!(vectors.len(), 1);
        assert_eq!(vectors[0].text, "hello world");
        assert_eq!(vectors[0].vector.len(), 4);
    }

    #[tokio::test]
    async fn only_the_configured_fields_are_embedded() {
        let (engine, coll, mut worker, _dir) = setup().await;
        let id =
            engine.insert(&coll, doc! { "_id": 1i64, "title": "keep", "secret": "drop" }).unwrap();
        worker.process(&last_entry(&engine)).await.unwrap();

        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let text = &engine.get_vectors(&shadow, &id).unwrap()[0].text;
        assert!(text.contains("keep"));
        assert!(!text.contains("drop"), "an unconfigured field must not be embedded");
    }

    #[tokio::test]
    async fn long_text_produces_several_chunks() {
        let (engine, coll, mut worker, _dir) = setup().await;
        let long = "abcdefghij".repeat(8); // 80 chars, max_chars = 20
        let id = engine.insert(&coll, doc! { "_id": 1i64, "body": long }).unwrap();

        let outcome = worker.process(&last_entry(&engine)).await.unwrap();
        let Outcome::Embedded { chunks } = outcome else {
            panic!("expected chunks, got {outcome:?}");
        };
        assert!(chunks > 1, "long text should split");

        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let vectors = engine.get_vectors(&shadow, &id).unwrap();
        assert_eq!(vectors.len(), chunks);
        // Chunk numbers must be dense and ordered, or lookups by index break.
        for (i, v) in vectors.iter().enumerate() {
            assert_eq!(v.chunk, i as u32);
        }
    }

    #[tokio::test]
    async fn re_processing_the_same_version_does_no_work() {
        // Positions are recorded after the work, so a crash re-delivers. That
        // is only safe because this is idempotent.
        let (engine, coll, mut worker, _dir) = setup().await;
        engine.insert(&coll, doc! { "_id": 1i64, "title": "text" }).unwrap();
        let entry = last_entry(&engine);

        assert!(matches!(worker.process(&entry).await.unwrap(), Outcome::Embedded { .. }));
        assert_eq!(
            worker.process(&entry).await.unwrap(),
            Outcome::Skipped,
            "already-current vectors should not be recomputed"
        );
    }

    #[tokio::test]
    async fn updating_a_document_re_embeds_it() {
        let (engine, coll, mut worker, _dir) = setup().await;
        let id = engine.insert(&coll, doc! { "_id": 1i64, "title": "before" }).unwrap();
        worker.process(&last_entry(&engine)).await.unwrap();

        engine.replace(&coll, &id, doc! { "title": "after" }, false).unwrap();
        worker.process(&last_entry(&engine)).await.unwrap();

        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let vectors = engine.get_vectors(&shadow, &id).unwrap();
        assert_eq!(vectors.len(), 1);
        assert_eq!(vectors[0].text, "after");
    }

    #[tokio::test]
    async fn deleting_a_document_removes_its_vectors() {
        // Otherwise deleted content stays searchable.
        let (engine, coll, mut worker, _dir) = setup().await;
        let id = engine.insert(&coll, doc! { "_id": 1i64, "title": "text" }).unwrap();
        worker.process(&last_entry(&engine)).await.unwrap();

        engine.delete(&coll, &id).unwrap();
        assert_eq!(worker.process(&last_entry(&engine)).await.unwrap(), Outcome::Removed);

        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        assert!(engine.get_vectors(&shadow, &id).unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_document_with_no_embeddable_text_is_skipped() {
        let (engine, coll, mut worker, _dir) = setup().await;
        engine.insert(&coll, doc! { "_id": 1i64, "other": 42 }).unwrap();
        assert_eq!(worker.process(&last_entry(&engine)).await.unwrap(), Outcome::Skipped);
    }

    #[tokio::test]
    async fn losing_its_text_drops_a_documents_vectors() {
        // The vectors would otherwise outlive the text that produced them.
        let (engine, coll, mut worker, _dir) = setup().await;
        let id = engine.insert(&coll, doc! { "_id": 1i64, "title": "text" }).unwrap();
        worker.process(&last_entry(&engine)).await.unwrap();

        engine.replace(&coll, &id, doc! { "other": 1 }, false).unwrap();
        worker.process(&last_entry(&engine)).await.unwrap();

        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        assert!(engine.get_vectors(&shadow, &id).unwrap().is_empty());
    }

    #[tokio::test]
    async fn collections_without_embedding_are_ignored() {
        let (engine, _coll, mut worker, _dir) = setup().await;
        let plain = engine.create_collection("app", "plain").unwrap();
        engine.insert(&plain, doc! { "_id": 1i64, "title": "text" }).unwrap();
        assert_eq!(worker.process(&last_entry(&engine)).await.unwrap(), Outcome::Skipped);
    }

    #[tokio::test]
    async fn the_workers_own_output_is_not_re_embedded() {
        // Writing to the shadow collection produces oplog entries too; treating
        // them as work would recurse.
        let (engine, coll, mut worker, _dir) = setup().await;
        engine.insert(&coll, doc! { "_id": 1i64, "title": "text" }).unwrap();
        worker.process(&last_entry(&engine)).await.unwrap();

        // The most recent entry is now the shadow-collection write.
        let shadow_entry = last_entry(&engine);
        assert_eq!(worker.process(&shadow_entry).await.unwrap(), Outcome::Skipped);
    }

    #[tokio::test]
    async fn array_fields_contribute_their_strings() {
        let (engine, coll, mut worker, _dir) = setup().await;
        let id = engine.insert(&coll, doc! { "_id": 1i64, "body": ["one", "two"] }).unwrap();
        worker.process(&last_entry(&engine)).await.unwrap();

        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let text = &engine.get_vectors(&shadow, &id).unwrap()[0].text;
        assert!(text.contains("one") && text.contains("two"));
    }

    #[tokio::test]
    async fn a_transient_provider_failure_is_retried_not_skipped() {
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        fake.fail_times.store(1, std::sync::atomic::Ordering::SeqCst);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);

        engine.insert(&coll, doc! { "_id": 1i64, "title": "text" }).unwrap();
        let entry = last_entry(&engine);

        // The first attempt fails with a retryable error rather than silently
        // recording success.
        let err = worker.process(&entry).await.unwrap_err();
        assert!(err.is_retryable(), "{err} should be retryable");

        // A retry then succeeds.
        assert!(matches!(worker.process(&entry).await.unwrap(), Outcome::Embedded { .. }));
    }

    #[test]
    fn only_temporary_failures_are_retryable() {
        // Retrying a permanent failure would stall every document behind it.
        assert!(VectorError::Transport { provider: "x", detail: String::new() }.is_retryable());
        assert!(
            VectorError::ProviderRejected { provider: "x", status: 503, detail: String::new() }
                .is_retryable()
        );
        assert!(
            VectorError::ProviderRejected { provider: "x", status: 429, detail: String::new() }
                .is_retryable()
        );
        assert!(
            !VectorError::ProviderRejected { provider: "x", status: 400, detail: String::new() }
                .is_retryable()
        );
        assert!(!VectorError::DimensionMismatch { expected: 4, found: 8 }.is_retryable());
        assert!(!VectorError::MissingApiKey { var: "K".into() }.is_retryable());
    }

    #[tokio::test]
    async fn the_position_is_recorded_and_resumed() {
        let (engine, _coll, _worker, _dir) = setup().await;
        assert!(engine.consumer_position(CONSUMER).unwrap().is_none());

        let token = kimmy_core::ResumeToken::new(Hlc::new(42, 1), engine.node_id());
        engine.put_consumer_position(CONSUMER, token).unwrap();
        assert_eq!(engine.consumer_position(CONSUMER).unwrap(), Some(token));
    }

    /// Wait for the worker to have started and then gone quiet.
    ///
    /// Quiet is defined on the commit counter rather than on a number of
    /// entries, so that a change to how many oplog entries the setup produces
    /// makes this test *slower* rather than flaky.
    async fn worker_is_idle(engine: &Engine) {
        let mut last = engine.commits();
        let mut stable = 0;
        for _ in 0..1_000 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            let now = engine.commits();
            // Started, and then unchanged for 50ms.
            if now == last && engine.consumer_position(CONSUMER).unwrap().is_some() {
                stable += 1;
                if stable == 10 {
                    return;
                }
            } else {
                stable = 0;
                last = now;
            }
        }
        panic!("the worker never started, or never stopped writing");
    }

    /// Wait for the worker's recorded position to move past `from`, or give up.
    ///
    /// Polled rather than signalled because the thing under test is precisely
    /// that the worker writes without being asked to.
    async fn position_advances_past(engine: &Engine, from: Option<kimmy_core::ResumeToken>) {
        for _ in 0..1_000 {
            if let Some(now) = engine.consumer_position(CONSUMER).unwrap()
                && Some(now) != from
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("the worker never recorded a position past {from:?}");
    }

    /// The daemon-versus-engine write gap, as a test.
    ///
    /// A bare `Engine` spends one commit on an insert
    /// (`kimmy_storage::docs::tests::one_insert_is_one_commit`). A daemon runs
    /// this worker, which records its oplog position after **every** entry —
    /// including the ones it has nothing to do with — and each of those is its
    /// own write transaction and its own fsync. So an insert into a collection
    /// with no vector configuration costs two commits on a daemon and one at
    /// the engine, which is the write gap M10 task 7 measured and could not
    /// explain.
    ///
    /// Nothing caught it because every other test in this file drives
    /// `process` directly and never `run`, and `process` is not where the
    /// position is recorded.
    ///
    /// **This test passing is not an endorsement.** It pins the current cost so
    /// that a fix has to change it deliberately; if you are reading this
    /// because you just made it fail, you are probably doing the right thing.
    #[tokio::test]
    async fn a_write_the_worker_skips_still_costs_a_second_commit() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        // No vector configuration, so every entry here is an `Outcome::Skipped`.
        let coll = engine.create_collection("app", "plain").unwrap();

        let mut worker = EmbeddingWorker::new(Arc::clone(&engine));
        tokio::spawn(async move { worker.run().await });

        // Let it finish with whatever creating the collection produced, so the
        // measurement below covers one insert and nothing else.
        worker_is_idle(&engine).await;
        let settled = engine.consumer_position(CONSUMER).unwrap();

        let before = engine.commits();
        engine.insert(&coll, doc! { "n": 1i64 }).unwrap();
        position_advances_past(&engine, settled).await;

        assert_eq!(
            engine.commits() - before,
            2,
            "an insert the worker skips still costs the insert's commit plus the worker's \
             position write — one write, two fsyncs"
        );
    }

    // -----------------------------------------------------------------------
    // Backfill: a ConfigureVectors entry is the reindex trigger
    // -----------------------------------------------------------------------

    /// A collection with documents written *before* embedding was configured —
    /// the situation the streaming path structurally cannot backfill, because
    /// a worker whose position has ever advanced is past those entries.
    async fn setup_with_history(
        count: usize,
    ) -> (Arc<Engine>, CollectionMeta, EmbeddingWorker, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let coll = engine.create_collection("app", "docs").unwrap();
        for i in 0..count {
            engine.insert(&coll, doc! { "_id": i as i64, "title": format!("doc {i}") }).unwrap();
        }
        engine.configure_vectors("app", "docs", config(&["title", "body"])).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();

        let mut worker = EmbeddingWorker::new(Arc::clone(&engine));
        worker.set_provider(coll.id.0, FakeProvider::new(4));
        (engine, coll, worker, dir)
    }

    #[tokio::test]
    async fn enabling_embedding_backfills_documents_that_predate_it() {
        // The gap this closes: before the ConfigureVectors entry became a
        // trigger, these three documents were never embedded at all — the
        // "backfill" M2 recorded was the worker's first-ever run from zero,
        // which a long-lived worker never repeats.
        let (engine, coll, mut worker, _dir) = setup_with_history(3).await;

        let outcome = worker.process(&last_entry(&engine)).await.unwrap();
        assert_eq!(outcome, Outcome::Backfilled { embedded: 3 });

        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        for i in 0..3i64 {
            let vectors = engine.get_vectors(&shadow, &kimmy_core::DocId::Int64(i)).unwrap();
            assert_eq!(vectors.len(), 1, "document {i} must be embedded");
        }
        let _ = coll;
    }

    #[test]
    fn a_changed_configuration_changes_its_fingerprint() {
        // The whole backfill decision is "does the stored fingerprint match
        // the live configuration". A hash that collapsed — mixing bytes with
        // `|` rather than `^` saturates towards all-ones — would answer "yes"
        // for configurations that differ, and a reconfigured collection would
        // never be re-embedded.
        let base = VectorConfig {
            fields: vec!["title".into()],
            provider: ProviderConfig::Byo,
            dim: 4,
            metric: Metric::Cosine,
            chunk: ChunkConfig::default(),
        };

        let mut wider = base.clone();
        wider.dim = 8;
        let mut other_field = base.clone();
        other_field.fields = vec!["body".into()];
        let mut both_fields = base.clone();
        both_fields.fields = vec!["title".into(), "body".into()];
        let mut other_metric = base.clone();
        other_metric.metric = Metric::Dot;

        let all = [&base, &wider, &other_field, &both_fields, &other_metric];
        let prints: std::collections::BTreeSet<u64> =
            all.iter().map(|c| config_fingerprint(c)).collect();
        assert_eq!(prints.len(), all.len(), "each configuration must fingerprint differently");

        // ...and the same configuration must fingerprint the same, or every
        // replay would look like a change and re-embed the collection.
        assert_eq!(config_fingerprint(&base), config_fingerprint(&base.clone()));
    }

    #[tokio::test]
    async fn a_permanent_failure_skips_a_document_instead_of_retrying_it_forever() {
        // The backfill retries per document, exactly as the streaming path
        // does — which means it inherits the classification, and gets it wrong
        // in the worst way if the classification says everything is worth
        // another go: the loop never exits and the whole scan stalls on one
        // document. Nothing exercised this path, because the fake provider
        // could only fail *retryably*.
        let (engine, _coll, mut worker, _dir) = setup_with_history(3).await;
        let fake = FakeProvider::new(4);
        fake.permanent.store(true, std::sync::atomic::Ordering::SeqCst);
        let coll = engine.get_collection("app", "docs").unwrap();
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);

        // A deadline rather than a bare await: the failure this guards against
        // is a hang, and a hung test that never finishes reports nothing.
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            worker.process(&last_entry(&engine)),
        )
        .await
        .expect("a permanent failure must not retry forever")
        .unwrap();

        assert_eq!(outcome, Outcome::Backfilled { embedded: 0 }, "nothing could be embedded");
        assert_eq!(
            fake.calls.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "each document is attempted exactly once, then skipped"
        );
    }

    // Paused clock: the retry delay is five seconds of real time, and a test
    // that waits it out is a test nobody runs. Virtual time still advances
    // past the deadline if the loop never exits, so this keeps its teeth.
    #[tokio::test(start_paused = true)]
    async fn a_transient_failure_during_a_backfill_is_retried_rather_than_skipped() {
        // The other direction: a classification that called everything
        // permanent would drop a document on one blip, and the scan would
        // report success having silently embedded less than it walked.
        let (engine, _coll, mut worker, _dir) = setup_with_history(3).await;
        let fake = FakeProvider::new(4);
        fake.fail_times.store(2, std::sync::atomic::Ordering::SeqCst);
        let coll = engine.get_collection("app", "docs").unwrap();
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);

        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(300),
            worker.process(&last_entry(&engine)),
        )
        .await
        .expect("retries are bounded by the documents, not unbounded")
        .unwrap();

        assert_eq!(
            outcome,
            Outcome::Backfilled { embedded: 3 },
            "every document must land despite the transient failures"
        );
    }

    #[tokio::test]
    async fn replaying_a_backfill_entry_redoes_nothing() {
        // The position is recorded after the scan, so a crash replays the
        // entry. Idempotency is the staleness check, per document.
        let (engine, _coll, mut worker, _dir) = setup_with_history(3).await;
        let entry = last_entry(&engine);

        assert_eq!(worker.process(&entry).await.unwrap(), Outcome::Backfilled { embedded: 3 });
        assert_eq!(
            worker.process(&entry).await.unwrap(),
            Outcome::Backfilled { embedded: 0 },
            "already-current vectors must not be re-embedded on replay"
        );
    }

    #[tokio::test]
    async fn reconfiguring_re_embeds_every_document() {
        // The reindex operation itself: same collection, new configuration —
        // here a changed field set, standing in for a changed model. Every
        // document's vectors must be rebuilt from the documents, not from
        // whatever the oplog still retains.
        let (engine, coll, mut worker, _dir) = setup_with_history(2).await;
        worker.process(&last_entry(&engine)).await.unwrap();

        // Reconfigure to embed a different field; doc texts change meaning.
        for i in 0..2i64 {
            engine
                .replace(
                    &coll,
                    &kimmy_core::DocId::Int64(i),
                    doc! { "title": format!("doc {i}"), "body": format!("body {i}") },
                    false,
                )
                .unwrap();
        }
        // Process the replaces so vectors are current for the old config.
        let entries = engine.read_oplog_from(Hlc::ZERO, 10_000).unwrap();
        for entry in &entries {
            worker.process(entry).await.unwrap();
        }

        engine.configure_vectors("app", "docs", config(&["body"])).unwrap();
        let outcome = worker.process(&last_entry(&engine)).await.unwrap();
        assert_eq!(outcome, Outcome::Backfilled { embedded: 2 });

        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let text = &engine.get_vectors(&shadow, &kimmy_core::DocId::Int64(0)).unwrap()[0].text;
        assert!(text.contains("body 0"), "must be embedded under the new fields: {text}");
        assert!(!text.contains("doc 0"), "the old field must be gone: {text}");
    }

    #[tokio::test]
    async fn disabling_and_byo_trigger_no_backfill() {
        let (engine, _coll, mut worker, _dir) = setup_with_history(1).await;

        engine.disable_vectors("app", "docs", false).unwrap();
        assert_eq!(
            worker.process(&last_entry(&engine)).await.unwrap(),
            Outcome::Skipped,
            "disabling has nothing to embed"
        );

        let byo = VectorConfig { provider: ProviderConfig::Byo, ..config(&["title"]) };
        engine.configure_vectors("app", "docs", byo).unwrap();
        assert_eq!(
            worker.process(&last_entry(&engine)).await.unwrap(),
            Outcome::Skipped,
            "byo vectors are the client's to supply; a backfill could only delete them"
        );
    }

    #[tokio::test]
    async fn a_reconfigured_collection_does_not_keep_its_old_provider() {
        // Found while building the backfill: the provider cache had no
        // eviction, so a collection reconfigured to a new model kept
        // embedding through the old provider forever. The cache now stores
        // the configuration each provider was built from and rebuilds on
        // mismatch — asserted here through the dimension, which is the one
        // externally visible property a provider owns.
        let (_engine, coll, mut worker, _dir) = setup_with_history(0).await;

        let first = worker.provider_for(coll.id.0, &config(&["title"])).unwrap();
        assert_eq!(first.dim(), 4, "the injected fake is trusted while config is unqueried");

        // A genuinely different configuration must evict even a cached entry
        // built from a real config. Build one from config A, then ask with
        // config B: the provider must be rebuilt, not reused.
        let mut real = EmbeddingWorker::new(Arc::clone(&_engine));
        let a = config(&["title"]);
        let built_a = real.provider_for(coll.id.0, &a).unwrap();
        let mut b = config(&["title"]);
        b.dim = 8;
        let built_b = real.provider_for(coll.id.0, &b).unwrap();
        assert_eq!(built_a.dim(), 4);
        assert_eq!(built_b.dim(), 8, "a changed configuration must rebuild the provider");
    }
}
