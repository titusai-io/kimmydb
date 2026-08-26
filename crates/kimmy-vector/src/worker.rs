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

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

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

/// How long a node waits before embedding a document written somewhere else.
///
/// Every node runs a worker and every node sees every write, so before this
/// existed all of them embedded the same document at once. The stored result
/// was still correct — `vectors_are_stale` makes a losing write a no-op, which
/// is why the shadow collection holds one chunk per document and not one per
/// node — but the *provider calls* were not deduplicated. Measured against a
/// live three-node cluster: 23 embedding requests for 10 documents. Against a
/// metered API that is the bill; locally it is CPU and latency; either way it
/// scales with the number of nodes.
///
/// The node that originated the write embeds immediately, so the common path
/// costs nothing. Everyone else waits this long and then checks again: by then
/// the originator's vectors have normally replicated, `vectors_are_stale` says
/// no, and no provider call happens at all.
///
/// It has to exceed one anti-entropy round comfortably — the default sync
/// interval is 5s — or the deferral expires before the originator's work could
/// have arrived and the duplicate call happens anyway.
const FOREIGN_GRACE: Duration = Duration::from_secs(30);

/// How often the worker wakes to re-check deferred documents when no writes
/// are arriving to wake it anyway.
const DEFERRAL_TICK: Duration = Duration::from_secs(5);

/// The most documents held for a later re-check.
///
/// Bounded because this is memory a burst of remote writes can grow. On
/// overflow the *oldest* deferral is embedded immediately rather than dropped:
/// spending a duplicate provider call is the right way to lose this race, and
/// silently forgetting a document would leave it unembedded with nothing to
/// notice.
const MAX_DEFERRED: usize = 4096;

/// The injected ownership test. See [`EmbeddingWorker::set_owner_check`].
pub type OwnerCheck = Box<dyn Fn(&str) -> bool + Send>;

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
    /// Documents written by another node, waiting to see whether that node
    /// embeds them. Ordered by deadline, which insertion order already gives.
    deferred: VecDeque<Deferred>,
    /// Whether *this* node owns embedding work for a collection, injected by
    /// the caller because [`crate`] sits below the cluster crates that know
    /// the member set. The closure receives a stable `"{db}/{collection}"`
    /// key and answers with the same rendezvous hash the webhook dispatcher
    /// and the expiry sweeper use, so all three subsystems land their work on
    /// the same nodes for the same membership.
    ///
    /// `None` — the default, and what every single-node deployment runs —
    /// owns everything, which is exactly right: with no peers there is
    /// nobody to defer to.
    am_owner: Option<OwnerCheck>,
    /// Counters behind `/metrics`. Plain atomics for the same reason
    /// `kimmy_api::metrics` is plain atomics: a fixed small set of series
    /// does not want a registry dependency. Shared by handle so the
    /// renderer can read them after the worker has been spawned.
    counters: Arc<WorkerCounters>,
}

/// Worker-side counts for `/metrics`, shared between the worker task and the
/// renderer by handle. See `kimmy_api::metrics`, which renders these as the
/// `kimmy_embed_*` series.
///
/// Deliberately counts *documents* and *chunks* separately: a chunk count
/// without a document count cannot distinguish "one huge document" from
/// "many small ones", and the provider bill scales with chunks while the
/// operational question ("is it keeping up?") scales with documents.
#[derive(Debug, Default)]
pub struct WorkerCounters {
    /// Documents whose vectors were written by this node, streaming or
    /// backfill.
    pub documents_embedded: AtomicU64,
    /// Chunks written — the number of provider inputs, and so the closest
    /// proxy there is for provider spend.
    pub chunks_embedded: AtomicU64,
    /// Documents held for a later re-check because another node wrote them.
    pub deferred: AtomicU64,
    /// Documents dropped un-embedded because this node does not own the
    /// collection. The signature of ownership working: without the gate this
    /// counter's work would have been duplicate provider calls.
    pub skipped_not_owned: AtomicU64,
    /// Provider calls that failed, retryable and permanent together. A
    /// number climbing while `documents_embedded` does not move is the
    /// "provider is down" signature.
    pub failures: AtomicU64,
}

impl WorkerCounters {
    fn embedded(&self, chunks: usize) {
        self.documents_embedded.fetch_add(1, Ordering::Relaxed);
        self.chunks_embedded.fetch_add(chunks as u64, Ordering::Relaxed);
    }
}

/// A document another node wrote, to be re-checked once its owner has had
/// [`FOREIGN_GRACE`] to embed it.
///
/// Holds ids rather than the entry: by the time this is examined the document
/// may have been replaced or deleted, and the re-check has to see whatever is
/// current, not what this entry described.
struct Deferred {
    collection: kimmy_core::CollectionId,
    source: kimmy_core::DocId,
    due: Instant,
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
    /// Written by another node, so its owner is given first refusal. Re-checked
    /// after [`FOREIGN_GRACE`] and embedded then if nobody else did.
    Deferred,
}

impl EmbeddingWorker {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            providers: HashMap::new(),
            deferred: VecDeque::new(),
            am_owner: None,
            counters: Arc::new(WorkerCounters::default()),
        }
    }

    /// Install the cluster-ownership check. See the `am_owner` field for why
    /// it is injected rather than computed here.
    ///
    /// The key format is `{db}/{collection}` — names, not ids, because names
    /// are what every node agrees on across a restore, and because the key is
    /// hashed, not parsed.
    pub fn set_owner_check(&mut self, f: OwnerCheck) {
        self.am_owner = Some(f);
    }

    /// The counters to hand the `/metrics` renderer. Call once, before
    /// [`Self::run`]: the renderer keeps the handle, so replacing it later
    /// would fork the counts.
    pub fn counters(&self) -> Arc<WorkerCounters> {
        Arc::clone(&self.counters)
    }

    /// Replace the built-in counters with a caller-supplied set — how
    /// `kimmyd` shares one handle between the worker task and `/metrics`.
    pub fn set_counters(&mut self, counters: Arc<WorkerCounters>) {
        self.counters = counters;
    }

    /// Whether this node owns embedding for a collection. With no check
    /// installed — single node, or a caller that has not wired clustering —
    /// everything is owned, which is the correct default.
    fn is_owner_of(&self, db: &str, collection: &str) -> bool {
        match &self.am_owner {
            None => true,
            Some(f) => f(&format!("{db}/{collection}")),
        }
    }

    /// Run until the change stream ends.
    ///
    /// Starts from the recorded position, or from the beginning of the oplog
    /// on first run — which is what backfills a collection that already had
    /// documents when embedding was enabled.
    ///
    /// **A lost position is recovered, not fatal.** The recorded position is
    /// an oplog arrival, and retention collects the oplog: a node that was
    /// down, or partitioned, for longer than `oplog_retention_secs` comes
    /// back to a position the oplog no longer holds. Before this, `watch`
    /// refused it and the worker returned the error — `kimmyd` logged
    /// "embedding worker stopped" once and ran on with **no embedding worker
    /// at all**, silently, until the next restart hit the same position and
    /// stopped again. Seen on a three-member cluster after a member spent ten
    /// hours unable to sync: it came back, owned a collection, and embedded
    /// nothing from then on. The same applies to a stream invalidated
    /// mid-run, which used to end the worker just as quietly.
    ///
    /// Recovery is two steps in a deliberate order: open a fresh stream from
    /// the oldest retained entry — `watch` subscribes before it reads, so
    /// anything written from here on is in the stream — and *then* rescan
    /// every owned, server-embedded collection for documents whose vectors
    /// are stale or missing, which covers the history retention already
    /// collected. Embedding is idempotent (`vectors_are_stale`), so the
    /// overlap between the two costs storage reads, not provider calls.
    pub async fn run(&mut self) -> Result<()> {
        let mut resume = self.engine.consumer_position(CONSUMER)?;
        let mut recovering = false;
        loop {
            let options = WatchOptions {
                resume_after: resume,
                // No recorded position means everything so far is unembedded.
                start_at: resume.is_none().then_some(Hlc::ZERO),
            };
            let mut stream = match self.engine.watch(WatchScope::Cluster, options) {
                Ok(stream) => stream,
                Err(e) if is_lost_position(&e) && resume.is_some() => {
                    warn!(
                        error = %e,
                        "embedding worker's recorded position has been collected; restarting \
                         from the oldest retained entry and rescanning owned collections"
                    );
                    resume = None;
                    recovering = true;
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            info!(resumed = resume.is_some(), recovering, "embedding worker started");

            if std::mem::take(&mut recovering) {
                let embedded = self.rescan_owned().await?;
                info!(embedded, "rescanned owned collections after a lost position");
            }

            match self.drive(&mut stream).await? {
                StreamEnd::Ended => return Ok(()),
                StreamEnd::Invalidated(reason) => {
                    // An invalidated stream cannot be trusted to be gap-free,
                    // and silently continuing would leave documents
                    // unembedded. Neither would stopping.
                    warn!(?reason, "change stream invalidated; recovering with a rescan");
                    resume = None;
                    recovering = true;
                }
            }
        }
    }

    /// Process one stream until it ends or is invalidated.
    async fn drive(&mut self, stream: &mut kimmy_storage::ChangeStream) -> Result<StreamEnd> {
        loop {
            // Timed rather than a plain await, so deferred documents are still
            // re-checked on a cluster that has gone quiet. `next` is safe to
            // cancel here: its only await is the wake-up channel, and it
            // records where to resume *before* waiting, so a dropped future
            // costs a notification and not a position. The re-read from the
            // arrival index on the next call is what recovers it.
            let event = match tokio::time::timeout(DEFERRAL_TICK, stream.next(&self.engine)).await {
                Ok(Some(event)) => event,
                Ok(None) => return Ok(StreamEnd::Ended),
                Err(_) => {
                    self.drain_deferred(Instant::now()).await;
                    continue;
                }
            };

            let (entry, token) = match event {
                ChangeEvent::Change { entry, token } => (entry, token),
                ChangeEvent::Invalidate { reason } => return Ok(StreamEnd::Invalidated(reason)),
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

            // Also here, not only on the idle tick: a busy cluster may never
            // reach the timeout, and a deferral that is due should not have to
            // wait for a lull.
            self.drain_deferred(Instant::now()).await;
        }
    }

    /// Bring every owned, server-embedded collection up to date by scanning
    /// it — the recovery for a stream position the oplog no longer holds.
    ///
    /// Returns how many documents were embedded. Collections owned elsewhere
    /// are skipped and counted, exactly as a replicated backfill is: their
    /// owner runs the same recovery when its own position is lost, and
    /// replication carries the vectors here.
    async fn rescan_owned(&mut self) -> Result<usize> {
        let mut embedded = 0;
        for db in self.engine.list_databases()? {
            for collection in self.engine.list_collections(&db.name)? {
                if kimmy_core::vector_meta::is_shadow(&collection.name) {
                    continue;
                }
                let Some(config) = collection.vector.clone() else {
                    continue;
                };
                if !config.provider.embeds_server_side() {
                    continue;
                }
                if !self.is_owner_of(&db.name, &collection.name) {
                    self.counters.skipped_not_owned.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let Some(shadow) = self.engine.vector_collection(&db.name, &collection.name)?
                else {
                    continue;
                };
                embedded += self
                    .scan_collection(&collection, &shadow, &config, "a lost stream position")
                    .await?;
            }
        }
        Ok(embedded)
    }

    /// Hold a remotely-written document for a later re-check.
    fn defer(&mut self, collection: kimmy_core::CollectionId, source: kimmy_core::DocId) {
        // Replaced rather than duplicated: a document written twice in quick
        // succession only needs one re-check, and it needs the later deadline.
        self.deferred.retain(|d| !(d.collection == collection && d.source == source));
        self.deferred.push_back(Deferred {
            collection,
            source,
            due: Instant::now() + FOREIGN_GRACE,
        });
        self.counters.deferred.fetch_add(1, Ordering::Relaxed);

        while self.deferred.len() > MAX_DEFERRED {
            // Made due immediately rather than dropped -- see MAX_DEFERRED.
            if let Some(oldest) = self.deferred.front_mut() {
                oldest.due = Instant::now();
                break;
            }
        }
    }

    /// Embed any deferred document whose owner has had its chance and did not
    /// take it.
    ///
    /// Nearly always a no-op beyond a storage read: by the time a deferral is
    /// due the originator's vectors have replicated, `embed_one` finds them
    /// current, and no provider is called.
    /// `now` is a parameter so a test can reach the deadline without sleeping
    /// through [`FOREIGN_GRACE`].
    pub async fn drain_deferred(&mut self, now: Instant) -> usize {
        let mut embedded = 0;

        while self.deferred.front().is_some_and(|d| d.due <= now) {
            let Some(item) = self.deferred.pop_front() else { break };
            match self.embed_deferred(&item).await {
                Ok(true) => {
                    embedded += 1;
                    debug!(
                        collection = %item.collection,
                        "embedded a document its own node did not"
                    );
                }
                Ok(false) => {}
                // Put back to try again rather than lost: a provider that is
                // briefly down must not cost the document.
                Err(e) if e.is_retryable() => {
                    warn!(error = %e, "deferred embedding failed; will retry");
                    self.deferred.push_back(Deferred { due: now + RETRY_DELAY, ..item });
                    break;
                }
                Err(e) => {
                    warn!(error = %e, "deferred embedding permanently failed; skipping");
                }
            }
        }
        embedded
    }

    async fn embed_deferred(&mut self, item: &Deferred) -> Result<bool> {
        let Some(collection) = self.engine.collection_by_id(item.collection)? else {
            // The collection was dropped while this waited. Nothing to embed,
            // and the drop took the vectors with it.
            return Ok(false);
        };
        // Not the owner: drop rather than re-check. Every node defers the
        // same foreign writes, so the owner holds this document in its own
        // queue — embedding it here too would be exactly the duplicate
        // provider call the deferral mechanism exists to prevent. The one
        // scenario dropping could lose coverage is the owner dying *and*
        // this node being its only survivor, and there the rendezvous owner
        // among the survivors has the same deferral and takes the work.
        //
        // Measured cost of leaving this gate out, from the 2026-08-24 load
        // test on a three-node cluster whose replication had fallen hours
        // behind: every node's [`FOREIGN_GRACE`] expiry found vectors that
        // were still stale, so all three embedded the same backlog against
        // one provider — the "3× amplification" that motivated this gate.
        if !self.is_owner_of(&collection.db, &collection.name) {
            self.counters.skipped_not_owned.fetch_add(1, Ordering::Relaxed);
            return Ok(false);
        };
        let Some(config) = collection.vector.clone() else {
            return Ok(false);
        };
        if !config.provider.embeds_server_side() {
            return Ok(false);
        }
        let shadow = self.engine.get_collection(
            &collection.db,
            &kimmy_core::vector_meta::shadow_name(&collection.name),
        )?;
        // `force: false` is the whole point: this re-reads the document's
        // current stamp and does nothing if the owner's vectors arrived.
        self.embed_one(&collection, &shadow, &config, &item.source, false).await
    }

    /// Handle one oplog entry.
    #[tracing::instrument(
        name = "vector.process",
        skip_all,
        fields(kind = ?entry.kind, collection_id = entry.collection.0 as i64)
    )]
    pub async fn process(&mut self, entry: &kimmy_core::OplogEntry) -> Result<Outcome> {
        // The collection *id* rather than its name, and no document id: the
        // span answers "which entry, and how long did embedding it take", and
        // an id answers that without publishing what a deployment calls its
        // data (ADR-068). Embedding is the slowest thing this node does off
        // the write path — a remote provider is a network round trip per chunk
        // — so this is the span that explains why a document's vectors are
        // minutes behind its write.
        //
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

        // Written elsewhere: let the node that wrote it embed it, and look
        // again later. Deciding by the entry's own stamp needs no membership
        // view and no agreement — every node reaches the same conclusion from
        // the entry alone, and the one that reaches "mine" is by definition
        // the one that has the document already.
        if entry.stamp.node != self.engine.node_id() {
            self.defer(entry.collection, source);
            return Ok(Outcome::Deferred);
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

        // One node scans. A `ConfigureVectors` entry replicates to every
        // member, and before this gate each of them ran the same full-
        // collection scan — N× the provider calls for exactly one corpus,
        // which on a fresh backfill is the single most expensive thing the
        // worker ever does. The rendezvous owner (same function as webhooks
        // and expiry) scans; the others skip and let replication bring them
        // the vectors, which costs storage I/O instead of model inference.
        //
        // The fingerprint is deliberately *not* written here: it attests a
        // completed scan by whoever ran it, and only the owner completes
        // scans. If ownership moves later, vectors already replicate; a new
        // owner re-scans only when someone changes the configuration again.
        if !self.is_owner_of(&set.db, &set.collection) {
            debug!(
                db = %set.db,
                collection = %set.collection,
                "backfill owned elsewhere; relying on replication"
            );
            self.counters.skipped_not_owned.fetch_add(1, Ordering::Relaxed);
            return Ok(Outcome::Skipped);
        }

        let embedded =
            self.scan_collection(&collection, &shadow, &config, "a configuration change").await?;
        Ok(Outcome::Backfilled { embedded })
    }

    /// Scan one collection, embedding every document whose vectors are stale
    /// or missing. Returns how many were embedded. `reason` names the trigger
    /// in the completion line.
    async fn scan_collection(
        &mut self,
        collection: &CollectionMeta,
        shadow: &CollectionMeta,
        config: &VectorConfig,
        reason: &'static str,
    ) -> Result<usize> {
        // Whether this scan must re-embed regardless of per-document
        // staleness. The HLC check cannot see a configuration change —
        // configurations do not touch documents — so the decision comes from
        // a fingerprint of the configuration the last *completed* scan ran
        // under. Written only after the scan, so a crash mid-backfill leaves
        // it stale and the replayed entry redoes the whole scan: some
        // documents embed twice, which idempotent output makes harmless,
        // where the alternative — recording first — would leave the rest
        // embedded under the old model with nothing to notice.
        let fingerprint = config_fingerprint(config);
        let force = self.engine.vector_fingerprint(collection.id)? != Some(fingerprint);

        // Ids first, documents re-read one at a time: the scan must not hold
        // a read transaction across provider calls, and holding every
        // document in memory would make backfill cost O(collection).
        let mut ids = Vec::new();
        self.engine.for_each_doc(collection, |id, _| {
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
                match self.embed_one(collection, shadow, config, &source, force).await {
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
            reason,
            "scanned a collection's vectors"
        );
        Ok(embedded)
    }

    /// Bring one document's vectors up to date. `Ok(true)` if work was done.
    ///
    /// `force` re-embeds even current-looking vectors — the configuration
    /// changed, so "current" was measured against the wrong ruler.
    #[tracing::instrument(
        name = "vector.embed",
        skip_all,
        fields(force, provider = config.provider.name(), chunks = tracing::field::Empty)
    )]
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
        // Recorded rather than declared up front, because the split is what
        // decides it — and the chunk count is what makes a slow embed legible:
        // a remote provider is charged a round trip per chunk, so thirty
        // chunks and one are two different costs behind the same span name.
        tracing::Span::current().record("chunks", chunks.len() as i64);
        if chunks.is_empty() {
            self.engine.delete_vectors(shadow, source)?;
            return Ok(false);
        }

        let provider = self.provider_for(collection.id.0, config)?;
        // Counted at the only line a provider outage can produce — including
        // the retries, so a sustained outage reads as a climbing counter
        // rather than one flat increment.
        let vectors = provider.embed(&chunks).await.inspect_err(|_| {
            self.counters.failures.fetch_add(1, Ordering::Relaxed);
        })?;
        let chunk_count = chunks.len();
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
        // The single choke point every embedding path funnels through —
        // streaming, deferred re-check and backfill — so one increment here
        // counts each exactly once.
        self.counters.embedded(chunk_count);
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
/// How a stream stopped yielding.
enum StreamEnd {
    /// The stream closed; there is nothing further to watch.
    Ended,
    /// The stream can no longer be trusted to be gap-free.
    Invalidated(kimmy_storage::InvalidateReason),
}

/// Whether opening a stream failed because its resume position has been
/// collected from the oplog — the one open-time failure the worker recovers
/// from rather than reports.
fn is_lost_position(e: &kimmy_storage::StorageError) -> bool {
    matches!(e, kimmy_storage::StorageError::Core(kimmy_core::Error::ResumeTokenExpired))
}

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
    async fn a_collected_position_is_recovered_by_a_rescan_and_a_fresh_stream() {
        // A member that was down for longer than the oplog retains comes back
        // to a recorded position the oplog no longer holds. This used to end
        // the worker with one warning, leaving the node with no embedding
        // worker at all — seen live on a cluster member that had been unable
        // to sync for ten hours.
        let (engine, coll, mut worker, _dir) = setup().await;
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();

        // The worker last recorded the oldest entry there is; then a document
        // was written while it was "down", and retention collected everything
        // but the newest entry — which the GC never removes, so the position
        // must point below it to be collected at all.
        let first = engine.read_oplog_from(Hlc::ZERO, 1).unwrap().remove(0);
        let stale = kimmy_core::ResumeToken::new(first.stamp.hlc, first.stamp.node);
        engine.put_consumer_position(CONSUMER, stale).unwrap();
        let before = engine.insert(&coll, doc! { "_id": "before", "title": "before" }).unwrap();
        engine
            .collect_garbage_at(
                kimmy_storage::physical_now_ms() + 1_000_000_000,
                kimmy_storage::RetentionPolicy::new(0, u64::MAX),
            )
            .unwrap();
        let refused = engine
            .watch(WatchScope::Cluster, WatchOptions { resume_after: Some(stale), start_at: None });
        assert!(
            matches!(&refused, Err(e) if is_lost_position(e)),
            "the fixture must reproduce the refused position: {:?}",
            refused.as_ref().err()
        );
        drop(refused);

        let handle = tokio::spawn(async move { worker.run().await });

        // The rescan covers what retention collected …
        for _ in 0..1_000 {
            if !engine.get_vectors(&shadow, &before).unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            engine.get_vectors(&shadow, &before).unwrap().len(),
            1,
            "the document written before the outage must be embedded by the rescan"
        );

        // … and the fresh stream covers what comes next.
        let after = engine.insert(&coll, doc! { "_id": "after", "title": "after" }).unwrap();
        for _ in 0..1_000 {
            if !engine.get_vectors(&shadow, &after).unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            engine.get_vectors(&shadow, &after).unwrap().len(),
            1,
            "a document written after recovery must be embedded by the live stream"
        );
        assert!(!handle.is_finished(), "the worker must keep running");
        assert_ne!(
            engine.consumer_position(CONSUMER).unwrap(),
            Some(stale),
            "a fresh position must be recorded, so the next start resumes normally"
        );
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

    /// The same entry as if a different node had written it.
    ///
    /// Only the stamp's node changes: that is the whole input to the decision,
    /// which is what makes it need no membership view.
    fn as_if_written_elsewhere(mut entry: kimmy_core::OplogEntry) -> kimmy_core::OplogEntry {
        entry.stamp.node = kimmy_core::NodeId::from_bytes([0xAB; 16]);
        entry
    }

    #[tokio::test]
    async fn a_document_written_elsewhere_is_not_embedded_immediately() {
        // Every node runs a worker and every node sees every write, so all of
        // them used to embed the same document at once. The stored result was
        // right -- the staleness check makes a losing write a no-op -- but the
        // provider calls were not deduplicated. Measured on a live three-node
        // cluster: 23 embedding requests for 10 documents.
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);

        engine.insert(&coll, bson::doc! { "_id": "a", "title": "hello", "body": "world" }).unwrap();
        let entry = as_if_written_elsewhere(last_entry(&engine));

        assert_eq!(worker.process(&entry).await.unwrap(), Outcome::Deferred);
        assert_eq!(
            fake.calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the node that wrote the document embeds it; nobody else pays for it too"
        );
    }

    #[tokio::test]
    async fn a_document_this_node_wrote_is_embedded_without_waiting() {
        // The other half. Deferring everything would trade duplicated work for
        // embedding nothing until a timer fired, so the common path has to stay
        // immediate.
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);

        engine.insert(&coll, bson::doc! { "_id": "a", "title": "hello", "body": "world" }).unwrap();
        let entry = last_entry(&engine);

        assert!(matches!(worker.process(&entry).await.unwrap(), Outcome::Embedded { .. }));
        assert_eq!(fake.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_deferred_document_is_embedded_if_its_own_node_never_does() {
        // What stops this being a way to lose embeddings. The originator may
        // have crashed between the write and the embed, so the grace period
        // ends in doing the work rather than in forgetting it.
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);

        engine.insert(&coll, bson::doc! { "_id": "a", "title": "hello", "body": "world" }).unwrap();
        let entry = as_if_written_elsewhere(last_entry(&engine));
        worker.process(&entry).await.unwrap();

        // Nothing replicated in the meantime: the owner never embedded it.
        let embedded = worker.drain_deferred(Instant::now() + FOREIGN_GRACE).await;

        assert_eq!(embedded, 1, "a document nobody embedded must not stay unembedded");
        assert_eq!(fake.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_deferred_document_is_dropped_when_another_node_owns_the_collection() {
        // The gate that closes the residual amplification. Every node defers
        // the same foreign writes, so when this node is not the rendezvous
        // owner its re-check would be a duplicate of the owner's — and under
        // replication lag measured on 2026-08-24 it *was*, three times over.
        // Dropping here is safe because the owner holds the same deferral.
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);
        worker.set_owner_check(Box::new(|_| false));

        engine.insert(&coll, bson::doc! { "_id": "a", "title": "hello", "body": "world" }).unwrap();
        let entry = as_if_written_elsewhere(last_entry(&engine));
        worker.process(&entry).await.unwrap();

        let embedded = worker.drain_deferred(Instant::now() + FOREIGN_GRACE).await;

        assert_eq!(embedded, 0, "a non-owner must not embed");
        assert_eq!(
            fake.calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "no provider call may happen off-owner"
        );
        let counters = worker.counters();
        assert!(counters.skipped_not_owned.load(std::sync::atomic::Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn a_backfill_is_skipped_when_another_node_owns_the_collection() {
        // A ConfigureVectors entry replicates to every member. Without the
        // ownership gate each of them ran the same full-collection scan —
        // three complete corpora through the provider for one collection.
        let (engine, _coll, mut worker, _dir) = setup_with_history(3).await;
        worker.set_owner_check(Box::new(|_| false));
        let entry = last_entry(&engine);

        assert_eq!(worker.process(&entry).await.unwrap(), Outcome::Skipped);

        let counters = worker.counters();
        assert!(counters.skipped_not_owned.load(std::sync::atomic::Ordering::SeqCst) >= 1);
        assert_eq!(
            counters.documents_embedded.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "an owned-elsewhere backfill must not embed anything"
        );
        // And the fingerprint stays unwritten, so if ownership moves here
        // before any other node's vectors arrive, a later entry can still
        // trigger this node's own scan.
    }

    #[tokio::test]
    async fn without_an_owner_check_everything_is_owned() {
        // Single-node deployments never install a check; they must keep the
        // pre-ownership behaviour exactly — including picking up documents
        // whose writer vanished, which is what makes deferral lossless.
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);

        engine.insert(&coll, bson::doc! { "_id": "a", "title": "hello", "body": "world" }).unwrap();
        worker.process(&as_if_written_elsewhere(last_entry(&engine))).await.unwrap();
        let embedded = worker.drain_deferred(Instant::now() + FOREIGN_GRACE).await;

        assert_eq!(embedded, 1);
        let counters = worker.counters();
        assert_eq!(
            counters.documents_embedded.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the default must be full ownership"
        );
        assert_eq!(
            counters.chunks_embedded.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "\"hello world\" is eleven characters; one chunk under max_chars = 20"
        );
        assert_eq!(counters.skipped_not_owned.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_deferred_document_costs_nothing_once_its_vectors_arrive() {
        // The case the deferral exists for: by the time the grace period is up
        // the originator's vectors have replicated, so the re-check finds them
        // current and no provider is called at all.
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);

        engine.insert(&coll, bson::doc! { "_id": "a", "title": "hello", "body": "world" }).unwrap();
        let entry = as_if_written_elsewhere(last_entry(&engine));
        worker.process(&entry).await.unwrap();

        // Stand in for replication: the owner's vectors land before the deadline.
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let stamp =
            engine.document_stamp(&coll, &kimmy_core::DocId::String("a".into())).unwrap().unwrap();
        engine
            .put_vectors(
                &shadow,
                &kimmy_core::DocId::String("a".into()),
                &[VectorRecord {
                    source: kimmy_core::DocId::String("a".into()),
                    chunk: 0,
                    source_hlc: stamp.hlc,
                    vector: vec![0.1; 4],
                    text: "hello".into(),
                }],
            )
            .unwrap();

        let embedded = worker.drain_deferred(Instant::now() + FOREIGN_GRACE).await;

        assert_eq!(embedded, 0);
        assert_eq!(
            fake.calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the whole point: the duplicate provider call never happens"
        );
    }

    #[tokio::test]
    async fn a_deferral_is_not_due_before_its_grace_has_passed() {
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);

        engine.insert(&coll, bson::doc! { "_id": "a", "title": "hello", "body": "world" }).unwrap();
        let entry = as_if_written_elsewhere(last_entry(&engine));
        worker.process(&entry).await.unwrap();

        assert_eq!(worker.drain_deferred(Instant::now()).await, 0, "the owner still has time");
        assert_eq!(fake.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}
