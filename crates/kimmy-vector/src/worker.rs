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
use kimmy_storage::{ChangeEvent, Engine, WatchOptions, WatchScope};
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
    providers: HashMap<u64, Arc<dyn EmbeddingProvider>>,
}

/// What one processed entry did, so tests and metrics can tell the cases apart.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Outcome {
    /// Vectors were written.
    Embedded { chunks: usize },
    /// The document's vectors were removed.
    Removed,
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

    /// A provider for one collection, built once and reused.
    fn provider_for(
        &mut self,
        collection: u64,
        config: &VectorConfig,
    ) -> Result<Arc<dyn EmbeddingProvider>> {
        if let Some(existing) = self.providers.get(&collection) {
            return Ok(Arc::clone(existing));
        }
        let built: Arc<dyn EmbeddingProvider> =
            Arc::from(provider::build(&config.provider, config.dim)?);
        self.providers.insert(collection, Arc::clone(&built));
        Ok(built)
    }

    /// Replace the provider for a collection. Used by tests to inject a fake.
    pub fn set_provider(&mut self, collection: u64, provider: Arc<dyn EmbeddingProvider>) {
        self.providers.insert(collection, provider);
    }
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
        calls: std::sync::atomic::AtomicUsize,
    }

    impl FakeProvider {
        fn new(dim: usize) -> Arc<Self> {
            Arc::new(Self { dim, fail_times: Default::default(), calls: Default::default() })
        }
    }

    #[async_trait]
    impl EmbeddingProvider for FakeProvider {
        async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
            use std::sync::atomic::Ordering;
            self.calls.fetch_add(1, Ordering::SeqCst);
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
}
