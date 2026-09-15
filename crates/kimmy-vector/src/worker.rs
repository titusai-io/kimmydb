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
//!
//! **One provider call carries many documents.** The provider takes a batch,
//! and the worker fills it from consecutive documents of the same collection
//! rather than from one document's chunks (ADR-095). Staleness stays per
//! document — one document's HLC, checked again after the call — but the
//! storage write is per batch: every document the call answered for is
//! written in one commit, with the worker's oplog position in the same
//! commit when the flush that sent the batch had one to record (ADR-149).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use kimmy_core::{ChunkConfig, Hlc, OpKind, VectorConfig, VectorRecord, path};
use kimmy_storage::{
    ChangeEvent, CollectionMeta, Engine, VectorWrite, WatchOptions, WatchScope, WriterHolder,
};
use tracing::{debug, error, info, warn};

use crate::error::{Result, TransportKind, VectorError};
use crate::policy::ProviderPolicy;
use crate::provider::{self, EmbeddingProvider};

/// Name under which the worker records its oplog position.
pub const CONSUMER: &str = "embedding-worker";

/// How long to wait before retrying after a provider failure.
///
/// A remote provider being briefly unavailable must not cost the position:
/// the worker retries the same entry rather than skipping it, so a rate limit
/// delays embedding but never silently loses it.
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

/// How long a non-owner waits before re-checking a document it did not embed.
///
/// Every node runs a worker and every node sees every write, so before
/// ownership existed all of them embedded the same document at once. The stored result
/// was still correct — `vectors_are_stale` makes a losing write a no-op, which
/// is why the shadow collection holds one chunk per document and not one per
/// node — but the *provider calls* were not deduplicated. Measured against a
/// live three-node cluster: 23 embedding requests for 10 documents. Against a
/// metered API that is the bill; locally it is CPU and latency; either way it
/// scales with the number of nodes.
///
/// The collection's rendezvous owner embeds every write it sees immediately,
/// whether it wrote the document or not (ADR-077, as amended 2026-08-28).
/// Everyone else holds the document and, this long later, asks one question:
/// *am I the owner now?* Ownership is a pure function of the live member set,
/// so the answer changes exactly when the owner has left — and then the
/// survivor that inherited the collection embeds what the owner never got to.
/// While the owner is alive the re-check embeds nothing, however far behind
/// the owner is, which is what keeps a bulk load at one provider call per
/// document: a 2026-08-28 ingest of 361 documents cost 570 under the old
/// rule, where the originator embedded immediately *and* the owner took over
/// everything the originator had not reached within this grace.
///
/// It has to exceed one anti-entropy round comfortably — the default sync
/// interval is 5s — so that a re-check on a new owner finds the vectors the
/// old owner did write before it left.
const FOREIGN_GRACE: Duration = Duration::from_secs(30);

/// How long a non-owner keeps re-checking a deferred document before letting
/// it go. Long enough to outlast a member's failure detection and the first
/// rounds of replication onto whoever inherits the collection; bounded so a
/// sustained bulk load on a healthy cluster does not pin its whole history in
/// every non-owner's queue. Past this, the document is the owner's alone —
/// and the owner's own oplog stream, or a lost-position rescan, still has it.
const DEFERRAL_MAX_AGE: Duration = Duration::from_secs(10 * 60);

/// How often the worker wakes to re-check deferred documents when no writes
/// are arriving to wake it anyway.
const DEFERRAL_TICK: Duration = Duration::from_secs(5);

/// How long the worker may hold its oplog position before writing it.
///
/// The position is pending work with a deadline, exactly as a partial batch
/// is (ADR-125). Every entry the worker sees moves the position, including
/// the ones it has nothing to do with — a collection with no vector
/// configuration, a document another node owns, a delete — and
/// `put_consumer_position` is a write transaction of its own, so under
/// `durable` it is an fsync of its own. Written per entry, that was one
/// commit per oplog entry on every member, for the writer's own entries and
/// the replicated ones alike: on a three-member cluster running 0.20.0 a
/// 1,000-document bulk insert converged everywhere in 3–5 s and then every
/// member, the writer included, committed at a steady ~18/s for about 75 s
/// until it had added roughly 1.3 commits per document — the single-writer
/// fsync rate, with nothing throttling the stream. Held instead, a burst of
/// N entries is one position write however large N is; the burst is
/// coalesced by any wait at all, so it is the *trickle* — one entry every
/// so often — that sets this bound, at one commit per second at most.
///
/// What holding costs is the window a crash re-processes: at most this much
/// of the stream, plus whatever a batch was holding. That is safe because
/// embedding is idempotent — `vectors_are_stale` makes a replayed embed a
/// no-op — and every other outcome an entry can have (skip, delete, defer,
/// backfill) is re-derived from what is stored, not from having seen the
/// entry. One second of replay on a restart is a handful of storage reads;
/// one fsync per entry was a second copy of every write the cluster makes.
///
/// Not a setting. An operator knows nothing that would move it: it is the
/// trade between restart replay and commit amplification, both of which
/// are the worker's own, and a bound of a second decides that trade the
/// same way on every deployment.
const POSITION_WAIT: Duration = Duration::from_secs(1);

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

/// How the worker gathers documents into one provider call (ADR-095).
///
/// [`EmbeddingProvider::embed`] has always taken a batch; until this the
/// worker handed it one document's chunks at a time, so a document short
/// enough to be one chunk was a batch of one and paid a whole round trip —
/// HTTP, tokenisation, the provider's own scheduling — by itself. Measured
/// against a llama.cpp CPU server with ~43-character inputs: 32 calls of one
/// input took 394 ms; one call of 32 inputs took 18 ms. Under a steady
/// arrival rate a little above the per-document service rate the backlog
/// grows without bound (Little's law), which is what a live ingest showed.
///
/// These bound the call, not the collection: they are about the provider
/// round trip this node makes, which is why they are process settings
/// (`[vector.batch]` in `kimmyd`) and not part of a collection's vector
/// configuration. A batch only ever holds documents of one collection, so
/// the per-collection provider, model and prefix are respected without
/// being repeated here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchSettings {
    /// The most chunks one provider call carries.
    ///
    /// 32 is the size the measurement above was taken at; it is below every
    /// hosted provider's per-request input cap (Cohere accepts 96 texts,
    /// Gemini 100) so the worker's batch is never re-split by the provider.
    pub max_chunks: usize,
    /// The most *estimated* tokens one provider call carries, by the same
    /// estimate `chunk.max_tokens` cuts on ([`ChunkConfig::estimate_tokens`]:
    /// one token per two bytes, an overestimate for every common script).
    ///
    /// 32 768 is `max_chunks` chunks at the default chunk ceiling — 2 000
    /// characters is at most 1 000 estimated tokens — so with default
    /// chunking the count bound is the one that binds, and a collection
    /// with `chunk.max_tokens = 1024` fills a call exactly. That is about
    /// 64 KiB of text per request, well inside every hosted provider's
    /// per-request token limit.
    ///
    /// One document's chunks always travel together, because the storage
    /// write behind them replaces the document's chunks as one. A single
    /// document over this bound goes alone rather than being split across
    /// calls, exactly as it did before batching.
    pub max_tokens: usize,
    /// How long the streaming path holds a partial batch for company before
    /// sending it.
    ///
    /// The wait only happens when the stream is idle: on a backlog the next
    /// entry is already there and the batch fills without waiting. So this
    /// costs a quiet collection at most this much added latency per
    /// document, and 100 ms is less than the remote round trip it saves.
    /// Zero sends whatever has queued the moment the stream is idle.
    pub max_wait: Duration,
}

impl Default for BatchSettings {
    fn default() -> Self {
        Self { max_chunks: 32, max_tokens: 32_768, max_wait: Duration::from_millis(100) }
    }
}

/// One document's share of a provider call: what was prepared from its text,
/// and the version that text belongs to.
struct Job {
    source: kimmy_core::DocId,
    /// The document version this work is for. Checked again after the
    /// provider call: a document that moved meanwhile has its own entry
    /// behind this one, and a deleted one has nothing to store vectors for.
    hlc: Hlc,
    /// Bare chunk text, as stored.
    chunks: Vec<String>,
    /// What the provider is sent: the chunks behind the document prefix.
    inputs: Vec<String>,
    /// Estimated tokens across `inputs`, for the batch's token bound.
    tokens: usize,
}

/// Documents of one collection gathered for one provider call.
///
/// One collection per batch, always: the collection names the provider, the
/// model and the prefix, and a call can only go to one of those.
struct Batch {
    collection: CollectionMeta,
    shadow: CollectionMeta,
    config: VectorConfig,
    jobs: Vec<Job>,
    chunks: usize,
    tokens: usize,
}

impl Batch {
    fn new(collection: CollectionMeta, shadow: CollectionMeta, config: VectorConfig) -> Self {
        Self { collection, shadow, config, jobs: Vec::new(), chunks: 0, tokens: 0 }
    }

    /// Whether a job fits under both bounds. An empty batch takes anything:
    /// a document too large for the bounds still has to be embedded, and it
    /// goes alone.
    fn accepts(&self, job: &Job, limits: &BatchSettings) -> bool {
        self.jobs.is_empty()
            || (self.chunks + job.chunks.len() <= limits.max_chunks
                && self.tokens + job.tokens <= limits.max_tokens)
    }

    fn full(&self, limits: &BatchSettings) -> bool {
        self.chunks >= limits.max_chunks || self.tokens >= limits.max_tokens
    }

    fn push(&mut self, job: Job) {
        // A document written twice before the batch went out needs only its
        // latest version embedded; the older version's vectors would fail
        // the stamp check after the call and be discarded unread.
        if let Some(i) = self.jobs.iter().position(|j| j.source == job.source) {
            let stale = self.jobs.remove(i);
            self.chunks -= stale.chunks.len();
            self.tokens -= stale.tokens;
        }
        self.chunks += job.chunks.len();
        self.tokens += job.tokens;
        self.jobs.push(job);
    }
}

/// A prepared document together with where its vectors go.
struct Item {
    collection: CollectionMeta,
    shadow: CollectionMeta,
    config: VectorConfig,
    job: Job,
}

/// What looking at one oplog entry decided.
enum Prepared {
    /// Handled in full: nothing to embed, or the work was not a provider call.
    Done(Outcome),
    /// A document to embed, ready to join a batch.
    Embed(Box<Item>),
}

/// What the streaming path holds between flushes: the batches, the newest
/// position seen, and when each began waiting.
///
/// Two things wait here, each with its own deadline. A partial batch waits
/// `max_wait` for company; a held position waits [`POSITION_WAIT`] for more
/// entries to cover. The position is recorded only by a flush — in the
/// commit of the last batch the flush stores, or in a commit of its own
/// when there is no batch — so the recorded position never runs ahead of an
/// entry whose vectors are still in a batch. The crash-replay guarantee is
/// unchanged; it just covers a window of entries at once rather than one.
#[derive(Default)]
struct Pending {
    /// At most one per collection, in the order the collections first
    /// appeared.
    batches: Vec<Batch>,
    /// The position of the newest entry seen since the last flush — the one
    /// to record once everything before it has landed.
    token: Option<kimmy_core::ResumeToken>,
    /// When `token` went from none to some; the position wait is measured
    /// from here, not from the newest entry, so a steady trickle still
    /// checkpoints once per [`POSITION_WAIT`] rather than never.
    held_since: Option<Instant>,
    /// When the first job now waiting arrived; the batch wait is measured
    /// from here.
    opened: Option<Instant>,
}

impl Pending {
    /// Note an entry's position as the newest to record.
    fn hold(&mut self, token: kimmy_core::ResumeToken, now: Instant) {
        self.token = Some(token);
        self.held_since.get_or_insert(now);
    }

    /// When whatever is waiting must go out: the earlier of the batch wait
    /// and the position wait, or `None` when nothing is waiting at all — no
    /// batch and no held position.
    ///
    /// The earlier, never the later: a batch's wait is the freshness a
    /// document's vectors are promised, and a position that happens to be
    /// waiting alongside it must not stretch that. It does not, because the
    /// flush a batch deadline triggers writes the position too.
    fn deadline(&self, wait: Duration) -> Option<Instant> {
        let batch = self.opened.map(|opened| opened + wait);
        let position = self.held_since.map(|since| since + POSITION_WAIT);
        match (batch, position) {
            (Some(batch), Some(position)) => Some(batch.min(position)),
            (batch, position) => batch.or(position),
        }
    }

    /// Whether the item fits its collection's batch without a flush first.
    fn accepts(&self, item: &Item, limits: &BatchSettings) -> bool {
        self.batches
            .iter()
            .find(|b| b.collection.id == item.collection.id)
            .is_none_or(|b| b.accepts(&item.job, limits))
    }

    /// Add a job to its collection's batch, opening one if needed. Returns
    /// whether that batch is now full.
    fn push(&mut self, item: Item, now: Instant, limits: &BatchSettings) -> bool {
        self.opened.get_or_insert(now);
        let Item { collection, shadow, config, job } = item;
        let batch = match self.batches.iter().position(|b| b.collection.id == collection.id) {
            Some(i) => &mut self.batches[i],
            None => {
                self.batches.push(Batch::new(collection, shadow, config));
                self.batches.last_mut().expect("just pushed")
            }
        };
        batch.push(job);
        batch.full(limits)
    }
}

/// A position on its way into a batch's commit.
///
/// `flush` hands this down to the last batch it stores, so the position is
/// written by the same commit as that batch's vectors rather than by a
/// commit of its own: the ADR-125 rule that the position is recorded only
/// after the work before it has landed is then kept by one transaction
/// rather than by the order of two. `token` is the position still to
/// write, `None` once a store has committed it. `failed` records that a
/// store failed somewhere in the flush — a storage error, or a provider
/// answer with the wrong number of vectors, which `store` refuses before it
/// opens a scope; the flush then writes no position
/// at all — a failed flush commits nothing — and the token goes back to
/// [`Pending`] with its deadline untouched, for the next flush to write.
#[derive(Default)]
struct Checkpoint {
    token: Option<kimmy_core::ResumeToken>,
    failed: bool,
}

/// Keeps a collection's vectors in step with its documents.
pub struct EmbeddingWorker {
    engine: Arc<Engine>,
    /// How documents are gathered into provider calls.
    batching: BatchSettings,
    /// Providers are built once per configuration and reused — constructing a
    /// local one loads a model, which is far too expensive per document.
    ///
    /// Keyed with the configuration that built each one, because a
    /// reconfigured collection must not keep embedding through the *old*
    /// provider — which it silently did until the reindex work made
    /// reconfiguration a live event. `None` marks a test-injected provider
    /// that no configuration should evict.
    providers: HashMap<u64, (Option<VectorConfig>, Arc<dyn EmbeddingProvider>)>,
    /// What a provider may be handed and where it may be sent (ADR-115).
    /// Consulted when a provider is built — here, at use time, and not only
    /// when the configuration was accepted, because a configuration also
    /// arrives by replication from a member whose API this node never saw.
    policy: ProviderPolicy,
    /// Configurations the policy refused, by collection, with the refusal as
    /// it was logged. A refusal is permanent for that configuration and is
    /// said once; every document behind it is skipped quietly until the
    /// collection is reconfigured, which evicts the entry.
    refused: HashMap<u64, (VectorConfig, String)>,
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
    /// The transport failures among `failures`, by what failed — connect,
    /// timeout, reset, other — in [`TransportKind::ALL`] order. Splitting
    /// them is what lets an operator tell a DNS or firewall problem from a
    /// slow provider from a load balancer closing idle connections without
    /// reading logs.
    pub transport: [AtomicU64; 4],
}

impl WorkerCounters {
    fn embedded(&self, chunks: usize) {
        self.documents_embedded.fetch_add(1, Ordering::Relaxed);
        self.chunks_embedded.fetch_add(chunks as u64, Ordering::Relaxed);
    }

    /// Count a failed provider call, and its transport kind when it has one.
    fn failed(&self, error: &VectorError) {
        self.failures.fetch_add(1, Ordering::Relaxed);
        if let VectorError::Transport { kind, .. } = error {
            let i = TransportKind::ALL.iter().position(|k| k == kind).unwrap_or(3);
            self.transport[i].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Transport failures of one kind.
    pub fn transport_failures(&self, kind: TransportKind) -> u64 {
        let i = TransportKind::ALL.iter().position(|k| *k == kind).unwrap_or(3);
        self.transport[i].load(Ordering::Relaxed)
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
    /// When the document was first deferred; the age [`DEFERRAL_MAX_AGE`] is
    /// measured from.
    since: Instant,
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
    /// This node does not own the collection, so the write is held rather
    /// than embedded. Re-checked after [`FOREIGN_GRACE`] and embedded then
    /// only if ownership has moved here in the meantime.
    Deferred,
}

/// What re-checking one deferred document found.
enum Recheck {
    /// This node owns the collection now and the vectors were missing.
    Embedded,
    /// This node owns the collection and the vectors were already current.
    Current,
    /// Someone else still owns the collection.
    NotOwner,
    /// Nothing to embed: the collection is gone or no longer server-embedded.
    Gone,
}

impl EmbeddingWorker {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            batching: BatchSettings::default(),
            providers: HashMap::new(),
            policy: ProviderPolicy::default(),
            refused: HashMap::new(),
            deferred: VecDeque::new(),
            am_owner: None,
            counters: Arc::new(WorkerCounters::default()),
        }
    }

    /// Set how documents are gathered into provider calls. See
    /// [`BatchSettings`] for the bounds and their defaults.
    pub fn set_batching(&mut self, batching: BatchSettings) {
        self.batching = batching;
    }

    /// Install the operator's provider policy. Call before [`Self::run`]: a
    /// provider built under the default policy is not rebuilt when this
    /// changes, and the default admits nothing the operator's would not.
    pub fn set_policy(&mut self, policy: ProviderPolicy) {
        self.policy = policy;
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
                resume_after: resume.clone(),
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
    ///
    /// Entries that need a provider call are gathered into batches — one per
    /// collection, bounded by [`BatchSettings`] — and sent when a batch is
    /// full, when the oldest waiting job has waited `max_wait`, or when the
    /// stream ends. Everything else an entry can mean (a delete, a skip, a
    /// deferral, a configuration change) is handled the moment it arrives.
    ///
    /// The position is never written per entry. It is held with the batches
    /// and written by the same flush: when a batch goes out, when a held
    /// position has waited [`POSITION_WAIT`] — checked after every entry as
    /// well as when the stream is idle, since a draining stream never idles —
    /// when the stream ends or is invalidated, and before a configuration
    /// change's backfill. A run of
    /// entries the worker has nothing to do with — a replicated batch
    /// published in one burst, a bulk insert into a collection with no
    /// vector configuration — is one position write, not one per entry.
    async fn drive(&mut self, stream: &mut kimmy_storage::ChangeStream) -> Result<StreamEnd> {
        let mut pending = Pending::default();
        loop {
            // Timed rather than a plain await, so a partial batch and a held
            // position go out on schedule and deferred documents are still
            // re-checked on a cluster that has gone quiet. `next` is safe to
            // cancel here: its only await is the wake-up channel, and it
            // records where to resume *before* waiting, so a dropped future
            // costs a notification and not a position. The re-read from the
            // arrival index on the next call is what recovers it.
            let wait = pending.deadline(self.batching.max_wait).map_or(DEFERRAL_TICK, |due| {
                due.saturating_duration_since(Instant::now()).min(DEFERRAL_TICK)
            });
            let event = match tokio::time::timeout(wait, stream.next(&self.engine)).await {
                Ok(Some(event)) => event,
                Ok(None) => {
                    self.flush(&mut pending).await?;
                    return Ok(StreamEnd::Ended);
                }
                Err(_) => {
                    // The combined deadline: whichever of the batch wait and
                    // the position wait is due, one flush serves both.
                    let now = Instant::now();
                    if pending.deadline(self.batching.max_wait).is_some_and(|due| due <= now) {
                        self.flush(&mut pending).await?;
                    }
                    self.drain_deferred(now).await;
                    continue;
                }
            };

            let (entry, token) = match event {
                ChangeEvent::Change { entry, token } => (entry, token),
                ChangeEvent::Invalidate { reason } => {
                    // What was gathered is real work from a stream that was
                    // good when it delivered it; cheaper to finish than to
                    // leave for the rescan.
                    self.flush(&mut pending).await?;
                    return Ok(StreamEnd::Invalidated(reason));
                }
            };

            // A configuration change rescans the collection under the new
            // configuration. Anything gathered under the old one goes first:
            // its stamp check would still pass after the scan, and it would
            // overwrite the new model's vectors with the old model's.
            if entry.kind == OpKind::ConfigureVectors {
                self.flush(&mut pending).await?;
            }

            // Retry rather than advance: losing an entry means a document stays
            // unembedded with nothing to notice it.
            let prepared = loop {
                match self.prepare_entry(&entry).await {
                    Ok(prepared) => break prepared,
                    Err(e) if e.is_retryable() => {
                        warn!(error = %e, "embedding failed; retrying");
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                    Err(e) => {
                        // A permanent failure would retry forever. Record it
                        // and move on, so one poisoned entry cannot stall
                        // every other one. A policy refusal was reported when
                        // the provider failed to build, once; the documents
                        // behind it are noted at debug.
                        if e.is_refused_by_policy() {
                            debug!(
                                collection = ?entry.collection,
                                doc = ?entry.doc_id,
                                "skipping an entry of a collection whose provider is refused"
                            );
                        } else {
                            warn!(
                                error = %e,
                                collection = ?entry.collection,
                                doc = ?entry.doc_id,
                                "embedding permanently failed; skipping this entry"
                            );
                        }
                        break Prepared::Done(Outcome::Skipped);
                    }
                }
            };

            match prepared {
                // Held, not written: the position is recorded by a flush,
                // only after the work is done, so a crash re-processes rather
                // than skips. Re-processing is safe because embedding is
                // idempotent and every other outcome is re-derived. Writing
                // it here instead — which this arm did until ADR-125 — cost
                // one commit and one fsync per oplog entry on every member,
                // for entries the worker had nothing to do with.
                Prepared::Done(_) => pending.hold(token, Instant::now()),
                Prepared::Embed(item) => {
                    if !pending.accepts(&item, &self.batching) {
                        self.flush(&mut pending).await?;
                    }
                    let now = Instant::now();
                    let full = pending.push(*item, now, &self.batching);
                    pending.hold(token, now);
                    if full {
                        self.flush(&mut pending).await?;
                    }
                }
            }

            // The deadline is checked here as well as in the timeout branch,
            // because the timeout never fires during a drain: `next` returns
            // without waiting while the arrival index has entries queued, so
            // on a backlog the timed wait above is never reached. Without
            // this, a held position waited for the whole drain — a member
            // coming back to an hour of entries would checkpoint nothing
            // until it had caught up — and a partial batch whose entries
            // were slow to prepare waited past `max_wait`. A batch that is
            // full still goes the moment it fills, above.
            let now = Instant::now();
            if pending.deadline(self.batching.max_wait).is_some_and(|due| due <= now) {
                self.flush(&mut pending).await?;
            }

            // Also here, not only on the idle tick: a busy cluster may never
            // reach the timeout, and a deferral that is due should not have to
            // wait for a lull.
            self.drain_deferred(now).await;
        }
    }

    /// Send everything gathered, oldest collection first, and record the
    /// position that covers it in the last batch's own commit.
    ///
    /// Any batch here may span several collections' worth of entries in the
    /// stream, and the token recorded is the newest one seen: by the time it
    /// is written every entry before it has either been handled on arrival or
    /// embedded just now. It is written by the store of the last batch — one
    /// commit for the batch's vectors and the position together, where it
    /// was one for the batch and a second for the position — and by a commit
    /// of its own only when no store carried it: there may be no batch at
    /// all — a position that has waited [`POSITION_WAIT`] with nothing to
    /// embed flushes through here too, and that is the one position write
    /// for however many entries were held, as before — or the last batch may
    /// have stored nothing, its documents skipped or refused. The provider
    /// calls are made before any of that: a scope holds the engine's one
    /// writer, and nothing network-bound runs inside one.
    ///
    /// A store that fails is logged where it fails and leaves its documents
    /// stale for a rescan to find, as before; what changes is the position.
    /// A flush in which a store failed writes no position: the token goes
    /// back to `pending` with its deadline untouched, so the next flush —
    /// due when the position wait elapses, or sooner with the next batch —
    /// writes it. The deadline rule is unchanged; the failed flush simply
    /// commits nothing.
    async fn flush(&mut self, pending: &mut Pending) -> Result<()> {
        let batches = std::mem::take(&mut pending.batches);
        pending.opened = None;
        let count = batches.len();
        let mut checkpoint = Checkpoint::default();
        for (i, batch) in batches.into_iter().enumerate() {
            // Only the last batch carries the position, and only while no
            // store has failed: the position must not land in a commit
            // that follows a batch whose vectors did not.
            if i + 1 == count && !checkpoint.failed {
                checkpoint.token = pending.token.take();
            }
            self.embed_batch(batch, &mut checkpoint).await;
        }
        if checkpoint.failed {
            pending.token = pending.token.take().or(checkpoint.token.take());
            return Ok(());
        }
        if let Some(token) = pending.token.take().or(checkpoint.token) {
            self.engine.put_consumer_position(CONSUMER, token)?;
        }
        pending.held_since = None;
        Ok(())
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

    /// Hold a document this node does not own for a later re-check.
    fn defer(&mut self, collection: kimmy_core::CollectionId, source: kimmy_core::DocId) {
        // Replaced rather than duplicated: a document written twice in quick
        // succession only needs one re-check, and it needs the later deadline.
        self.deferred.retain(|d| !(d.collection == collection && d.source == source));
        let now = Instant::now();
        self.deferred.push_back(Deferred {
            collection,
            source,
            due: now + FOREIGN_GRACE,
            since: now,
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

    /// Re-check every due deferral: embed it if this node has become the
    /// collection's owner and the vectors are still missing, hold it a while
    /// longer if someone else still owns it, let it go once it is older than
    /// [`DEFERRAL_MAX_AGE`].
    ///
    /// On a healthy cluster this embeds nothing and costs a storage read per
    /// item: the owner embedded the document from its own stream long before
    /// the grace ran out. `now` is a parameter so a test can reach the
    /// deadline without sleeping through [`FOREIGN_GRACE`].
    pub async fn drain_deferred(&mut self, now: Instant) -> usize {
        let mut embedded = 0;

        while self.deferred.front().is_some_and(|d| d.due <= now) {
            let Some(item) = self.deferred.pop_front() else { break };
            match self.embed_deferred(&item).await {
                Ok(Recheck::Embedded) => {
                    embedded += 1;
                    debug!(
                        collection = %item.collection,
                        "embedded a document whose previous owner did not"
                    );
                }
                Ok(Recheck::Current | Recheck::Gone) => {}
                Ok(Recheck::NotOwner) => {
                    if now.duration_since(item.since) < DEFERRAL_MAX_AGE {
                        // Still someone else's: look again after another
                        // grace, in case that someone leaves.
                        self.deferred.push_back(Deferred { due: now + FOREIGN_GRACE, ..item });
                    } else {
                        // The owner has had ten minutes of being alive to do
                        // this; from here the document is its stream's alone.
                        self.counters.skipped_not_owned.fetch_add(1, Ordering::Relaxed);
                    }
                }
                // Put back to try again rather than lost: a provider that is
                // briefly down must not cost the document.
                Err(e) if e.is_retryable() => {
                    warn!(error = %e, "deferred embedding failed; will retry");
                    self.deferred.push_back(Deferred { due: now + RETRY_DELAY, ..item });
                    break;
                }
                Err(e) if e.is_refused_by_policy() => {
                    debug!(
                        collection = %item.collection,
                        doc = %item.source,
                        "skipping a deferred document of a collection whose provider is refused"
                    );
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        collection = %item.collection,
                        doc = %item.source,
                        "deferred embedding permanently failed; skipping"
                    );
                }
            }
        }
        embedded
    }

    async fn embed_deferred(&mut self, item: &Deferred) -> Result<Recheck> {
        let Some(collection) = self.engine.collection_by_id(item.collection)? else {
            // The collection was dropped while this waited. Nothing to embed,
            // and the drop took the vectors with it.
            return Ok(Recheck::Gone);
        };
        // The only question a re-check asks. Ownership is computed over the
        // live member set, so "not mine" means the owner is alive and the
        // document is in its stream — embedding it here would be the
        // duplicate provider call ownership exists to prevent. "Mine" means
        // the owner left and this node inherited the collection, and the
        // staleness check below says whether it left this document undone.
        //
        // Measured cost of getting this wrong, from the 2026-08-24 load test
        // on a three-node cluster whose replication had fallen hours behind:
        // every node's grace expiry found vectors that were still stale, so
        // all three embedded the same backlog against one provider.
        if !self.is_owner_of(&collection.db, &collection.name) {
            return Ok(Recheck::NotOwner);
        };
        let Some(config) = collection.vector.clone() else {
            return Ok(Recheck::Gone);
        };
        if !config.provider.embeds_server_side() {
            return Ok(Recheck::Gone);
        }
        let shadow = self.engine.get_collection(
            &collection.db,
            &kimmy_core::vector_meta::shadow_name(&collection.name),
        )?;
        // `force: false` is the whole point: this re-reads the document's
        // current stamp and does nothing if the previous owner's vectors
        // arrived before it left.
        Ok(if self.embed_one(&collection, &shadow, &config, &item.source, false).await? {
            Recheck::Embedded
        } else {
            Recheck::Current
        })
    }

    /// Handle one oplog entry on its own, embedding at once.
    ///
    /// The streaming path in [`Self::run`] gathers entries into batches
    /// instead; this is the one-entry form, which embeds a document as a
    /// batch of one and reports the provider's answer — including its
    /// failure — to the caller.
    pub async fn process(&mut self, entry: &kimmy_core::OplogEntry) -> Result<Outcome> {
        match self.prepare_entry(entry).await? {
            Prepared::Done(outcome) => Ok(outcome),
            Prepared::Embed(item) => {
                let Item { collection, shadow, config, job } = *item;
                let chunks = job.chunks.len();
                let vectors =
                    self.call_provider(&collection, &config, std::slice::from_ref(&job)).await?;
                let stored = self.store(
                    &collection,
                    &shadow,
                    &config,
                    vec![job],
                    vectors,
                    &mut Checkpoint::default(),
                )?;
                Ok(if stored == 1 { Outcome::Embedded { chunks } } else { Outcome::Skipped })
            }
        }
    }

    /// Decide what one oplog entry means: handle everything that is not a
    /// provider call here and now, and hand back what is.
    #[tracing::instrument(
        name = "vector.process",
        skip_all,
        fields(kind = ?entry.kind, collection_id = entry.collection.0 as i64)
    )]
    async fn prepare_entry(&mut self, entry: &kimmy_core::OplogEntry) -> Result<Prepared> {
        // The collection *id* rather than its name, and no document id: the
        // span answers "which entry, and what did it take to look at", and
        // an id answers that without publishing what a deployment calls its
        // data (ADR-068). The provider call itself — the slowest thing this
        // node does off the write path — is the `vector.embed` span, one per
        // batch, which is the span that explains why a document's vectors
        // are minutes behind its write.
        //
        // Not every oplog entry describes a mutation. A unique-violation entry
        // reports something that happened *to* the data and has nothing to
        // embed. It would be filtered by the `doc_id` check below anyway, but
        // relying on that would make the safety incidental.
        if entry.kind == OpKind::UniqueViolation {
            return Ok(Prepared::Done(Outcome::Skipped));
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
            return Ok(Prepared::Done(self.backfill_from_entry(entry).await?));
        }

        let Some(source) = entry.doc_id.clone() else {
            return Ok(Prepared::Done(Outcome::Skipped));
        };
        let Some(collection) = self.engine.collection_by_id(entry.collection)? else {
            return Ok(Prepared::Done(Outcome::Skipped));
        };
        // A shadow collection is the worker's own output; embedding it would
        // recurse.
        let Some(config) = collection.vector.clone() else {
            return Ok(Prepared::Done(Outcome::Skipped));
        };
        if kimmy_core::vector_meta::is_shadow(&collection.name) {
            return Ok(Prepared::Done(Outcome::Skipped));
        }

        let shadow = self.engine.get_collection(
            &collection.db,
            &kimmy_core::vector_meta::shadow_name(&collection.name),
        )?;

        // Before the provider check, deliberately: a `byo` collection's
        // vectors are the client's to supply, but not the client's to clean
        // up after — the client sees a document, not the chunks behind it.
        // Until 0.14.0 this branch sat below the bail and a deleted `byo`
        // document's vectors stayed searchable until someone called the
        // explicit vectors DELETE route by hand.
        if entry.kind == OpKind::Delete {
            let removed = self.engine.delete_vectors(&shadow, &source)?;
            debug!(chunks = removed, "removed vectors for a deleted document");
            return Ok(Prepared::Done(Outcome::Removed));
        }

        // `byo` means the client supplies vectors, so there is nothing to embed.
        if !config.provider.embeds_server_side() {
            return Ok(Prepared::Done(Outcome::Skipped));
        }

        let Some(document) = entry.document()? else {
            return Ok(Prepared::Done(Outcome::Skipped));
        };

        // The entry carries the version this work is for. Anything newer has
        // its own entry coming, so redoing older work would be wasted.
        if !self.engine.vectors_are_stale(&shadow, &source, entry.stamp.hlc)? {
            return Ok(Prepared::Done(Outcome::Skipped));
        }

        // Ownership decides, not origin. Until 2026-08-28 the node that wrote
        // the document embedded it immediately and everyone else deferred; on
        // a bulk load the owner's re-checks then found everything the writer
        // had not reached yet and embedded it too — 570 provider calls for
        // 361 documents on a three-member cluster. Now the owner embeds every write
        // it sees, from its own oplog image, the moment it sees it (the write
        // reaches it by replication within a sync round), and a non-owner
        // holds the document only against the owner leaving.
        if !self.is_owner_of(&collection.db, &collection.name) {
            self.defer(entry.collection, source);
            return Ok(Prepared::Done(Outcome::Deferred));
        }

        // Embedding from the entry's own image is what makes this path cheap:
        // no re-read of the document. The version is the entry's, and the
        // stamp check after the provider call is what keeps that honest.
        let Some(job) = self.prepare(&shadow, &config, source, entry.stamp.hlc, &document)? else {
            return Ok(Prepared::Done(Outcome::Skipped));
        };
        Ok(Prepared::Embed(Box::new(Item { collection, shadow, config, job })))
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
        // Documents are gathered into provider calls up to the batch bounds
        // and sent as each fills; the last, partial one goes when the scan
        // ends. No timer here — the set is known, so there is nothing to
        // wait for. Transient failures retry and a permanent one skips its
        // document, inside `embed_batch`, exactly as the streaming path.
        let mut batch = Batch::new(collection.clone(), shadow.clone(), config.clone());
        for source in ids {
            let job = match self.prepare_one(collection, shadow, config, &source, force) {
                Ok(Some(job)) => job,
                Ok(None) => continue,
                Err(e) => {
                    warn!(
                        error = %e,
                        db = %collection.db,
                        collection = %collection.name,
                        doc = %source,
                        "backfill could not read a document; skipping it"
                    );
                    continue;
                }
            };
            if !batch.accepts(&job, &self.batching) {
                let ready = std::mem::replace(
                    &mut batch,
                    Batch::new(collection.clone(), shadow.clone(), config.clone()),
                );
                embedded += self.embed_batch(ready, &mut Checkpoint::default()).await;
            }
            batch.push(job);
            if batch.full(&self.batching) {
                let ready = std::mem::replace(
                    &mut batch,
                    Batch::new(collection.clone(), shadow.clone(), config.clone()),
                );
                embedded += self.embed_batch(ready, &mut Checkpoint::default()).await;
            }
        }
        embedded += self.embed_batch(batch, &mut Checkpoint::default()).await;

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

    /// Bring one document's vectors up to date, alone. `Ok(true)` if work was
    /// done. The deferred re-check's path: one document, one call, and the
    /// provider's failure reported to the caller, which has its own retry.
    async fn embed_one(
        &mut self,
        collection: &CollectionMeta,
        shadow: &CollectionMeta,
        config: &VectorConfig,
        source: &kimmy_core::DocId,
        force: bool,
    ) -> Result<bool> {
        let Some(job) = self.prepare_one(collection, shadow, config, source, force)? else {
            return Ok(false);
        };
        let vectors = self.call_provider(collection, config, std::slice::from_ref(&job)).await?;
        let stored =
            self.store(collection, shadow, config, vec![job], vectors, &mut Checkpoint::default())?;
        Ok(stored == 1)
    }

    /// Read one document afresh and prepare it, or nothing if its vectors are
    /// already current.
    ///
    /// `force` re-embeds even current-looking vectors — the configuration
    /// changed, so "current" was measured against the wrong ruler.
    fn prepare_one(
        &self,
        collection: &CollectionMeta,
        shadow: &CollectionMeta,
        config: &VectorConfig,
        source: &kimmy_core::DocId,
        force: bool,
    ) -> Result<Option<Job>> {
        // The stamp is the document's *current* version, read fresh — a
        // document replaced mid-scan is embedded at whichever version the
        // read sees, and the newer version's own oplog entry follows behind
        // this backfill in the stream.
        let Some(stamp) = self.engine.document_stamp(collection, source)? else {
            return Ok(None);
        };
        if !force && !self.engine.vectors_are_stale(shadow, source, stamp.hlc)? {
            return Ok(None);
        }
        let Some(document) = self.engine.get(collection, source)? else {
            return Ok(None);
        };
        self.prepare(shadow, config, source.clone(), stamp.hlc, &document)
    }

    /// Text to chunks to provider inputs, for one version of one document.
    ///
    /// `None` means there is nothing to embed — and any vectors a previous
    /// version had are removed here, or they would outlive their source text.
    fn prepare(
        &self,
        shadow: &CollectionMeta,
        config: &VectorConfig,
        source: kimmy_core::DocId,
        hlc: Hlc,
        document: &bson::Document,
    ) -> Result<Option<Job>> {
        let text = extract_text(document, config);
        let chunks = config.chunk.split(&text);
        if chunks.is_empty() {
            self.engine.delete_vectors(shadow, &source)?;
            return Ok(None);
        }
        let inputs = prefixed(config, &chunks);
        let tokens = inputs.iter().map(|input| ChunkConfig::estimate_tokens(input)).sum();
        Ok(Some(Job { source, hlc, chunks, inputs, tokens }))
    }

    /// One provider call for every chunk of every job, in job order.
    ///
    /// The document and chunk counts are on the span because they are what
    /// make a slow embed legible: thirty chunks and one are two different
    /// costs behind the same span name.
    #[tracing::instrument(
        name = "vector.embed",
        skip_all,
        fields(
            provider = config.provider.name(),
            documents = jobs.len(),
            chunks = jobs.iter().map(|j| j.chunks.len()).sum::<usize>(),
        )
    )]
    async fn call_provider(
        &mut self,
        collection: &CollectionMeta,
        config: &VectorConfig,
        jobs: &[Job],
    ) -> Result<Vec<Vec<f32>>> {
        let provider = self.provider_for(collection.id.0, config)?;
        let gathered: Vec<String>;
        let inputs: &[String] = match jobs {
            [only] => &only.inputs,
            _ => {
                gathered = jobs.iter().flat_map(|j| j.inputs.iter().cloned()).collect();
                &gathered
            }
        };
        // Counted at the only line a provider outage can produce — including
        // the retries, so a sustained outage reads as a climbing counter
        // rather than one flat increment.
        provider.embed(inputs).await.inspect_err(|e| self.counters.failed(e))
    }

    /// Write each job's vectors, and the position the flush handed down, in
    /// one commit. Each document written is counted as one document and
    /// its chunks. Returns how many were written.
    ///
    /// One scope for the batch (ADR-149): every document's replace-all
    /// write goes into one transaction, with the checkpoint's position when
    /// it holds one, and the scope commits once — about thirty documents
    /// and their position were thirty-one commits and as many fsyncs. The
    /// staleness check stays per document, inside the scope, because a
    /// document can move while the provider call runs; it is a read, and
    /// opens no write. The records are encoded before the scope opens, so
    /// nothing runs under the writer but the writes. Any failure aborts the
    /// whole batch's writes: a
    /// document's chunk set is replaced whole or not at all, and a batch is
    /// stored whole or not at all, so a retry is a retry of the batch. The
    /// counters move after the commit, and count what was written, never
    /// how many calls it took. The shadow's generation is bumped by the
    /// scope, after the commit, never inside it.
    fn store(
        &self,
        collection: &CollectionMeta,
        shadow: &CollectionMeta,
        config: &VectorConfig,
        jobs: Vec<Job>,
        vectors: Vec<Vec<f32>>,
        checkpoint: &mut Checkpoint,
    ) -> Result<usize> {
        let expected: usize = jobs.iter().map(|j| j.chunks.len()).sum();
        if vectors.len() != expected {
            // Storing a shifted answer would give every later document in
            // the batch its neighbour's vectors; refusing costs one retry.
            return Err(VectorError::MalformedResponse {
                provider: config.provider.name(),
                detail: format!("expected {expected} vectors, got {}", vectors.len()),
            });
        }
        // Encoded before the scope opens: the scope holds the engine's one
        // writer, and every other writer on the node waits behind it.
        let mut vectors = vectors.into_iter();
        let mut writes = Vec::with_capacity(jobs.len());
        for job in jobs {
            let own: Vec<Vec<f32>> = vectors.by_ref().take(job.chunks.len()).collect();
            let count = job.chunks.len();
            let records: Vec<VectorRecord> = job
                .chunks
                .into_iter()
                .zip(own)
                .enumerate()
                .map(|(i, (text, vector))| VectorRecord {
                    source: job.source.clone(),
                    chunk: i as u32,
                    source_hlc: job.hlc,
                    vector,
                    text,
                })
                .collect();
            let write = VectorWrite::encode(&job.source, &records)?;
            writes.push((job.source, job.hlc, count, write));
        }

        let position = checkpoint.token.clone();
        let written: Vec<usize> = self.engine.write_batch(WriterHolder::Embedding, |scope| {
            let mut written = Vec::new();
            for (source, hlc, count, write) in writes {
                // The provider call is the long part, and the document can
                // move while it runs. Writing vectors for a version that has
                // since been deleted would leave chunks with no source — and
                // the `Delete` entry that would have removed them has
                // already gone by. A newer version is the same case with a
                // different ending: its own entry is behind this one and
                // will do the work, so this write would only be overwritten.
                // Either way, nothing to store.
                match self.engine.document_stamp(collection, &source)? {
                    Some(current) if current.hlc == hlc => {}
                    Some(_) => {
                        debug!("document moved while it was being embedded; its own entry follows");
                        continue;
                    }
                    None => {
                        debug!("document was deleted while it was being embedded");
                        continue;
                    }
                }
                scope.put_vectors(shadow, write)?;
                written.push(count);
            }
            // Last, so that the position covers every write before it in
            // the one commit that carries them all.
            if let Some(token) = position {
                scope.put_consumer_position(CONSUMER, token)?;
            }
            Ok(written)
        })?;
        checkpoint.token = None;
        // Every path that writes vectors ends here, so this is the one place
        // the document and chunk counters move. They count what was written,
        // never how many calls it took: a batch of thirty documents is
        // thirty here — after the commit, so a counter never claims a
        // document a failed scope did not store.
        for count in &written {
            self.counters.embedded(*count);
            debug!(chunks = count, "embedded a document");
        }
        Ok(written.len())
    }

    /// Embed one batch and return how many documents were written.
    ///
    /// A retryable failure retries the whole batch, forever, exactly as one
    /// document retried before: a provider that is briefly down must not
    /// cost a document. A permanent failure on a batch of several documents
    /// is almost never the batch's fault but one document's — a `400` for an
    /// input the model cannot take — and the provider does not say which. So
    /// the batch is taken apart and each document sent alone: the one at
    /// fault is skipped and named, the rest land. That costs one extra call
    /// per document, once, on a path that had already failed.
    ///
    /// A storage error is not a provider error: it is logged, marked on the
    /// checkpoint, and ends the batch, whose documents stay stale for a
    /// rescan to find — none of them landed, since the batch is one commit.
    ///
    /// `checkpoint` is the position the flush wants in this batch's commit.
    /// It rides with the one store that answers for the whole batch, or
    /// with the store of a lone document; a batch taken apart after a
    /// permanent failure hands it to none of its documents, and the flush
    /// writes it on its own afterwards — one extra commit on a path that
    /// had already failed.
    async fn embed_batch(&mut self, batch: Batch, checkpoint: &mut Checkpoint) -> usize {
        let Batch { collection, shadow, config, jobs, .. } = batch;
        if jobs.is_empty() {
            return 0;
        }
        if jobs.len() == 1 {
            let job = jobs.into_iter().next().expect("one job");
            return self.embed_alone(&collection, &shadow, &config, job, checkpoint).await;
        }
        let vectors = loop {
            match self.call_provider(&collection, &config, &jobs).await {
                Ok(vectors) => break vectors,
                Err(e) if e.is_retryable() => {
                    warn!(error = %e, documents = jobs.len(), "embedding failed; retrying");
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(e) => {
                    // Splitting a refused batch would ask the policy once per
                    // document and get the same answer; the batch is simply
                    // skipped.
                    if e.is_refused_by_policy() {
                        debug!(
                            db = %collection.db,
                            collection = %collection.name,
                            documents = jobs.len(),
                            "skipping a batch of a collection whose provider is refused"
                        );
                        return 0;
                    }
                    warn!(
                        error = %e,
                        db = %collection.db,
                        collection = %collection.name,
                        documents = jobs.len(),
                        "a batch permanently failed; embedding its documents one at a time"
                    );
                    let mut written = 0;
                    let mut alone = Checkpoint::default();
                    for job in jobs {
                        written +=
                            self.embed_alone(&collection, &shadow, &config, job, &mut alone).await;
                    }
                    checkpoint.failed |= alone.failed;
                    return written;
                }
            }
        };
        match self.store(&collection, &shadow, &config, jobs, vectors, checkpoint) {
            Ok(written) => written,
            Err(e) => {
                warn!(
                    error = %e,
                    db = %collection.db,
                    collection = %collection.name,
                    "storing a batch's vectors failed; none of the batch landed and it stays stale"
                );
                checkpoint.failed = true;
                0
            }
        }
    }

    /// Embed one document by itself, retrying what is worth retrying and
    /// skipping — by name — what is not.
    async fn embed_alone(
        &mut self,
        collection: &CollectionMeta,
        shadow: &CollectionMeta,
        config: &VectorConfig,
        job: Job,
        checkpoint: &mut Checkpoint,
    ) -> usize {
        let vectors = loop {
            match self.call_provider(collection, config, std::slice::from_ref(&job)).await {
                Ok(vectors) => break vectors,
                Err(e) if e.is_retryable() => {
                    warn!(error = %e, "embedding failed; retrying");
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(e) => {
                    // A permanent failure (bad config, wrong dimension, an
                    // input the model refuses) would retry forever. Name it
                    // and move on, so one poisoned document cannot stall
                    // every other one. A policy refusal was named once when
                    // the provider failed to build.
                    if e.is_refused_by_policy() {
                        debug!(
                            db = %collection.db,
                            collection = %collection.name,
                            doc = %job.source,
                            "skipping a document of a collection whose provider is refused"
                        );
                    } else {
                        warn!(
                            error = %e,
                            db = %collection.db,
                            collection = %collection.name,
                            doc = %job.source,
                            "embedding permanently failed; skipping this document"
                        );
                    }
                    return 0;
                }
            }
        };
        match self.store(collection, shadow, config, vec![job], vectors, checkpoint) {
            Ok(written) => written,
            Err(e) => {
                warn!(
                    error = %e,
                    db = %collection.db,
                    collection = %collection.name,
                    "storing a document's vectors failed; it stays stale"
                );
                checkpoint.failed = true;
                0
            }
        }
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
        // A configuration the policy already refused is refused again without
        // being said again — the message named the variable or the host once,
        // and a line per document behind it would say nothing new.
        if let Some((refused_config, message)) = self.refused.get(&collection)
            && refused_config == config
        {
            return Err(VectorError::PolicyRefused(message.clone()));
        }
        let built: Arc<dyn EmbeddingProvider> =
            match provider::build(&config.provider, config.dim, &self.policy) {
                Ok(built) => Arc::from(built),
                Err(e) if e.is_refused_by_policy() => {
                    // The one line an operator gets, so it carries what they
                    // need: which collection, and the name or host refused.
                    // Never a value — the policy refused the *name* before
                    // anything read it.
                    let message = e.to_string();
                    error!(
                        collection = collection,
                        provider = config.provider.name(),
                        error = %message,
                        "this node's provider policy refuses the collection's embedding \
                         configuration; its documents will not be embedded here until it is \
                         reconfigured"
                    );
                    self.refused.insert(collection, (config.clone(), message));
                    return Err(e);
                }
                Err(e) => return Err(e),
            };
        self.refused.remove(&collection);
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
/// What the provider is sent for each chunk: the chunk, behind the
/// collection's document prefix when it has one. The stored chunk text stays
/// bare — the prefix is the model's business, not the caller's.
fn prefixed(config: &VectorConfig, chunks: &[String]) -> Vec<String> {
    match &config.document_prefix {
        Some(prefix) => chunks.iter().map(|c| format!("{prefix}{c}")).collect(),
        None => chunks.to_vec(),
    }
}

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

    /// Whether this node's provider policy refused the configuration, or
    /// the configuration names a profile this node does not define. Both are
    /// permanent for the configuration and reported once, when the provider
    /// fails to build, rather than per document.
    pub fn is_refused_by_policy(&self) -> bool {
        matches!(self, VectorError::PolicyRefused(_) | VectorError::UnknownProfile { .. })
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
        /// Every input text the provider was asked to embed, in order.
        inputs: std::sync::Mutex<Vec<String>>,
        /// How many inputs each call carried, in order — the batching
        /// evidence.
        sizes: std::sync::Mutex<Vec<usize>>,
        /// An input the model "cannot take": any call containing it fails
        /// permanently, as a provider refusing one oversized input does.
        poison: std::sync::Mutex<Option<String>>,
    }

    impl FakeProvider {
        fn new(dim: usize) -> Arc<Self> {
            Arc::new(Self {
                dim,
                fail_times: Default::default(),
                permanent: Default::default(),
                calls: Default::default(),
                inputs: Default::default(),
                sizes: Default::default(),
                poison: Default::default(),
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn sizes(&self) -> Vec<usize> {
            self.sizes.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl EmbeddingProvider for FakeProvider {
        async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
            use std::sync::atomic::Ordering;
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inputs.lock().unwrap().extend(texts.iter().cloned());
            self.sizes.lock().unwrap().push(texts.len());
            let poisoned = self
                .poison
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|p| texts.iter().any(|t| t.contains(p.as_str())));
            if self.permanent.load(Ordering::SeqCst) || poisoned {
                // A 400 is the canonical "retrying will not help".
                return Err(VectorError::ProviderRejected {
                    provider: "fake",
                    status: 400,
                    detail: "injected permanent failure".into(),
                });
            }
            if self.fail_times.load(Ordering::SeqCst) > 0 {
                self.fail_times.fetch_sub(1, Ordering::SeqCst);
                return Err(VectorError::Transport {
                    provider: "fake",
                    kind: TransportKind::Reset,
                    detail: "injected".into(),
                });
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
            document_prefix: None,
            query_prefix: None,
            chunk: ChunkConfig { max_chars: 20, overlap: 5, max_tokens: None },
        }
    }

    /// A policy admitting the loopback endpoint the test configuration names.
    fn loopback_policy() -> ProviderPolicy {
        ProviderPolicy::new(
            crate::policy::default_allowed_key_env(),
            vec!["localhost".into()],
            false,
            Default::default(),
        )
        .unwrap()
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
    async fn a_locally_written_document_is_counted_when_embedded() {
        // The streaming path — a document this node wrote, embedded from its
        // own entry — is the common case on an owner, and it shipped in 0.5.0
        // with no counter on it: on a production cluster the owner's vectors
        // landed within ten seconds of every insert while
        // `kimmy_embed_documents_total` read 1 from a rescan and never moved.
        // The tests that asserted the counters moved all went through
        // `embed_one` (deferred re-checks and scans), never through here.
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);
        engine.insert(&coll, doc! { "_id": 1i64, "title": "hello", "body": "world" }).unwrap();

        let outcome = worker.process(&last_entry(&engine)).await.unwrap();
        assert!(matches!(outcome, Outcome::Embedded { chunks: 1 }), "{outcome:?}");

        let counters = worker.counters();
        assert_eq!(counters.documents_embedded.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(counters.chunks_embedded.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(counters.failures.load(std::sync::atomic::Ordering::SeqCst), 0);

        // And a provider outage on this path is counted per attempt — the
        // "provider is down" signature is failures climbing while
        // documents_embedded does not, and it needs both sides to be live.
        fake.fail_times.store(1, std::sync::atomic::Ordering::SeqCst);
        engine.insert(&coll, doc! { "_id": 2i64, "title": "again" }).unwrap();
        let entry = last_entry(&engine);
        let err = worker.process(&entry).await.unwrap_err();
        assert!(err.is_retryable(), "{err}");
        assert_eq!(counters.failures.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(counters.documents_embedded.load(std::sync::atomic::Ordering::SeqCst), 1);
        // The retry succeeds and is counted once, like any other embedding.
        assert!(matches!(worker.process(&entry).await.unwrap(), Outcome::Embedded { .. }));
        assert_eq!(counters.documents_embedded.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(counters.failures.load(std::sync::atomic::Ordering::SeqCst), 1);
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
    async fn deleting_a_byo_document_removes_its_vectors_too() {
        // The client supplies a `byo` collection's vectors, but it deletes
        // documents, not chunks — so the cleanup is still the worker's. Until
        // 0.14.0 the byo bail sat above the delete branch and these chunks
        // stayed searchable for ever.
        let (engine, _coll, mut worker, _dir) = setup().await;
        let byo = VectorConfig { provider: ProviderConfig::Byo {}, ..config(&["title"]) };
        engine.configure_vectors("app", "docs", byo).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();

        let id = engine.insert(&coll, doc! { "_id": 1i64, "title": "text" }).unwrap();
        let stamp = engine.document_stamp(&coll, &id).unwrap().unwrap();
        engine
            .put_vectors(
                &shadow,
                &id,
                &[VectorRecord {
                    source: id.clone(),
                    chunk: 0,
                    source_hlc: stamp.hlc,
                    vector: vec![1.0, 0.0, 0.0, 0.0],
                    text: "text".into(),
                }],
            )
            .unwrap();
        assert_eq!(engine.get_vectors(&shadow, &id).unwrap().len(), 1);

        engine.delete(&coll, &id).unwrap();
        assert_eq!(worker.process(&last_entry(&engine)).await.unwrap(), Outcome::Removed);
        assert!(engine.get_vectors(&shadow, &id).unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_embed_that_finishes_after_the_delete_writes_nothing() {
        // The streaming path embeds from the entry's image while the document
        // is free to move. If the provider call outlasts a delete, the chunks
        // would land after the `Delete` entry that should have removed them,
        // with nothing left to remove them ever.
        let (engine, coll, mut worker, _dir) = setup().await;
        let id = engine.insert(&coll, doc! { "_id": 1i64, "title": "text" }).unwrap();
        let insert_entry = last_entry(&engine);

        // The delete happens "during" the embed: before the worker gets to
        // the insert entry at all, which is the same thing from the write's
        // point of view.
        engine.delete(&coll, &id).unwrap();
        assert_eq!(worker.process(&insert_entry).await.unwrap(), Outcome::Skipped);

        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        assert!(engine.get_vectors(&shadow, &id).unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_embed_of_a_superseded_version_writes_nothing() {
        // Same shape as a delete, different ending: the newer version has its
        // own entry behind this one, and that entry does the work.
        let (engine, coll, mut worker, _dir) = setup().await;
        let id = engine.insert(&coll, doc! { "_id": 1i64, "title": "before" }).unwrap();
        let insert_entry = last_entry(&engine);
        engine.replace(&coll, &id, doc! { "title": "after" }, false).unwrap();

        assert_eq!(worker.process(&insert_entry).await.unwrap(), Outcome::Skipped);
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        assert!(engine.get_vectors(&shadow, &id).unwrap().is_empty(), "nothing stale landed");

        worker.process(&last_entry(&engine)).await.unwrap();
        assert_eq!(engine.get_vectors(&shadow, &id).unwrap()[0].text, "after");
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
        assert!(
            VectorError::Transport {
                provider: "x",
                kind: TransportKind::Connect,
                detail: String::new()
            }
            .is_retryable()
        );
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
        engine.put_consumer_position(CONSUMER, stale.clone()).unwrap();
        let before = engine.insert(&coll, doc! { "_id": "before", "title": "before" }).unwrap();
        engine
            .collect_garbage_at(
                kimmy_storage::physical_now_ms() + 1_000_000_000,
                kimmy_storage::RetentionPolicy::new(0, u64::MAX),
            )
            .unwrap();
        let refused = engine.watch(
            WatchScope::Cluster,
            WatchOptions { resume_after: Some(stale.clone()), start_at: None },
        );
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
        engine.put_consumer_position(CONSUMER, token.clone()).unwrap();
        assert_eq!(engine.consumer_position(CONSUMER).unwrap(), Some(token));
    }

    /// Wait for the worker to have started and then gone quiet.
    ///
    /// Quiet is defined on the commit counter rather than on a number of
    /// entries, so that a change to how many oplog entries the setup produces
    /// makes this test *slower* rather than flaky. "Started" is a recorded
    /// position, which lands up to [`POSITION_WAIT`] after the setup's last
    /// entry now that the position is held rather than written per entry;
    /// the poll allows several times that.
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

    /// Wait for the worker's recorded position to reach exactly `stamp`, or
    /// give up. The way to know the worker has nothing left to write for a
    /// burst: its position covers the burst's last entry.
    async fn position_reaches(engine: &Engine, stamp: kimmy_core::Stamp) {
        for _ in 0..1_000 {
            if engine.consumer_position(CONSUMER).unwrap().map(|t| t.to_stamp()) == Some(stamp) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("the worker never recorded a position at {stamp:?}");
    }

    /// An engine with a collection that has no vector configuration and a
    /// worker running against it — so every entry the worker sees is an
    /// `Outcome::Skipped`, and the only commits it can add are position
    /// writes. Returned settled: the worker has recorded a position for
    /// whatever creating the collection produced, so a measurement taken
    /// after this covers the caller's writes and nothing else.
    async fn setup_skipping_worker()
    -> (Arc<Engine>, CollectionMeta, Option<kimmy_core::ResumeToken>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let coll = engine.create_collection("app", "plain").unwrap();

        let mut worker = EmbeddingWorker::new(Arc::clone(&engine));
        tokio::spawn(async move { worker.run().await });

        worker_is_idle(&engine).await;
        let settled = engine.consumer_position(CONSUMER).unwrap();
        (engine, coll, settled, dir)
    }

    /// The daemon-versus-engine write gap, closed.
    ///
    /// A bare `Engine` spends one commit on an insert
    /// (`kimmy_storage::docs::tests::one_insert_is_one_commit`). A daemon runs
    /// this worker, which used to record its oplog position after **every**
    /// entry — including the ones it had nothing to do with — and each of
    /// those was its own write transaction and its own fsync. So an insert
    /// into a collection with no vector configuration cost two commits on a
    /// daemon and one at the engine, which was the write gap M10 task 7
    /// measured and could not explain; and on a cluster, where every member
    /// runs the worker over its own arrival index, it was one commit per
    /// replicated document on every member, on top of the one transaction
    /// ADR-119 had just reduced the sync batch to.
    ///
    /// The position is held now and written by deadline (ADR-125), so a
    /// burst of writes the worker skips costs the writes' own commits plus a
    /// small constant of position writes — one, unless the burst straddles
    /// a [`POSITION_WAIT`] boundary — and never one per write.
    ///
    /// Nothing caught the original because every other test in this file
    /// drives `process` directly and never `run`, and `process` is not where
    /// the position is recorded.
    #[tokio::test]
    async fn a_burst_of_writes_the_worker_skips_costs_one_position_write_not_one_each() {
        let (engine, coll, _settled, _dir) = setup_skipping_worker().await;

        let count = 50u64;
        let before = engine.commits();
        for n in 0..count {
            engine.insert(&coll, doc! { "n": n as i64 }).unwrap();
        }
        // Until the position covers the whole burst — not merely until it has
        // moved, and not a quiet window on the commit counter either: a
        // second held token can land after any fixed window on a loaded
        // machine, and the count below has to be the whole cost.
        position_reaches(&engine, last_entry(&engine).stamp).await;

        let added = engine.commits() - before;
        assert!(
            added <= count + 2,
            "{count} inserts the worker skips cost {added} commits; the worker may add a \
             position write or two, not one per insert"
        );
        assert!(
            added < 2 * count,
            "{count} inserts cost {added} commits — one per insert for the position, which \
             is the write gap this test exists to keep closed"
        );
        assert_eq!(
            engine.consumer_position(CONSUMER).unwrap().map(|t| t.to_stamp()),
            Some(last_entry(&engine).stamp),
            "the one position write must cover the whole burst"
        );
    }

    /// The other half of the trade: holding the position must not mean
    /// never writing it. A lone entry nothing follows still gets its
    /// position recorded, within about [`POSITION_WAIT`], so that a restart
    /// resumes from near the end of the log rather than replaying all of it
    /// — and so that retention, which collects what the position is behind,
    /// never finds the position stranded.
    #[tokio::test]
    async fn a_lone_skipped_entrys_position_is_recorded_within_the_position_wait() {
        let (engine, coll, settled, _dir) = setup_skipping_worker().await;

        let started = Instant::now();
        engine.insert(&coll, doc! { "n": 1i64 }).unwrap();
        position_advances_past(&engine, settled).await;
        let took = started.elapsed();

        // Generous — the test is that it lands on the position's own
        // schedule, not the five-second deferral tick or the next write.
        assert!(
            took < POSITION_WAIT * 3,
            "a lone skipped entry's position took {took:?} to land; it should have been \
             written within about {POSITION_WAIT:?}"
        );
        assert_eq!(
            engine.consumer_position(CONSUMER).unwrap().map(|t| t.to_stamp()),
            Some(last_entry(&engine).stamp),
            "the recorded position must be the entry itself"
        );
    }

    /// A held position must not delay embedding. The deadline the worker
    /// waits on is the *earlier* of the batch wait and the position wait; if
    /// it were the later, an embeddable document arriving behind a burst of
    /// skipped entries would wait out the position's second instead of the
    /// batch's `max_wait`.
    #[tokio::test]
    async fn a_burst_of_skipped_entries_does_not_delay_the_embeddable_document_behind_it() {
        let (engine, coll, mut worker, _dir) = setup().await;
        let plain = engine.create_collection("app", "plain").unwrap();
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);
        worker.set_batching(BatchSettings {
            max_wait: std::time::Duration::from_millis(50),
            ..Default::default()
        });
        position_at_latest(&engine);
        tokio::spawn(async move { worker.run().await });

        // The burst: entries the worker holds a position for and nothing else.
        for n in 0..20 {
            engine.insert(&plain, doc! { "n": n as i64 }).unwrap();
        }
        // Then one document that needs a provider call.
        let started = Instant::now();
        engine.insert(&coll, doc! { "_id": 0i64, "title": "behind the burst" }).unwrap();
        vectors_land(&engine, 1).await;
        let took = started.elapsed();

        assert!(
            took < POSITION_WAIT,
            "the document behind the burst waited {took:?} for its vectors; the batch wait \
             should have sent it long before the held position's {POSITION_WAIT:?}"
        );
        assert_eq!(fake.sizes(), vec![1]);
    }

    /// The bound holds *during* a backlog, not only once the stream goes
    /// quiet. `ChangeStream::next` returns without waiting while the arrival
    /// index has entries queued, so the timed wait in `drive` never elapses
    /// during a drain; the deadline has to be checked per entry as well, or
    /// a held position waits for the whole drain — and a member coming back
    /// to a long backlog would checkpoint nothing until it had caught up,
    /// which is exactly the restart-replay window `POSITION_WAIT` bounds.
    ///
    /// The drain is slowed deterministically rather than by volume: the
    /// owner check, which the worker asks once per embeddable entry, sleeps
    /// on the calling thread and answers "not mine", so every entry is a
    /// deferral the worker holds a position for and the backlog takes
    /// several `POSITION_WAIT`s to cross. A second runtime thread keeps the
    /// poll below running while the worker's thread sleeps.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_held_position_is_checkpointed_during_a_long_drain_not_only_after_it() {
        let (engine, coll, mut worker, _dir) = setup().await;
        let count = 600usize;
        let docs =
            (0..count).map(|i| doc! { "_id": i as i64, "title": format!("doc {i}") }).collect();
        engine.insert_many(&coll, docs).unwrap();
        let last = last_entry(&engine).stamp;

        let handled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counting = Arc::clone(&handled);
        worker.set_owner_check(Box::new(move |_| {
            std::thread::sleep(Duration::from_millis(5));
            counting.fetch_add(1, Ordering::SeqCst);
            false
        }));
        tokio::spawn(async move { worker.run().await });

        // Every distinct position recorded while the drain was still in
        // progress. The position is read *before* the count, so a position
        // paired with an incomplete count was recorded before the last entry
        // was handled.
        let mut during: Vec<kimmy_core::ResumeToken> = Vec::new();
        let mut drained = 0;
        for _ in 0..4_000 {
            let position = engine.consumer_position(CONSUMER).unwrap();
            drained = handled.load(Ordering::SeqCst);
            if drained >= count {
                break;
            }
            if let Some(position) = position
                && during.last() != Some(&position)
            {
                during.push(position);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(drained >= count, "the drain never completed: {drained} of {count} handled");

        assert!(
            during.len() >= 2,
            "a backlog that took about {:?} to drain saw {} position checkpoints while it was \
             draining; one is due every {POSITION_WAIT:?} whether or not the stream goes quiet",
            Duration::from_millis(5 * count as u64),
            during.len()
        );
        for position in &during {
            assert!(
                position.to_stamp() < last,
                "a position recorded mid-drain must trail the backlog, not lead it"
            );
        }

        // Leave with the worker parked on its wake-up channel, not mid-poll:
        // a runtime torn down under a task that is still polling — this one
        // sleeps inside the owner check — trips tokio's own shutdown
        // assertion on the next timer it touches, which would be noise on
        // top of whatever this test had to say.
        position_reaches(&engine, last).await;
    }

    /// The pure form of the rule above, with no clock to wait on.
    #[test]
    fn the_deadline_is_the_earlier_of_the_batch_wait_and_the_position_wait() {
        let wait = Duration::from_millis(100);
        let now = Instant::now();
        let node = kimmy_core::NodeId::generate();
        let token = kimmy_core::ResumeToken::new(Hlc::new(1, 1), node);

        let mut pending = Pending::default();
        assert_eq!(pending.deadline(wait), None, "nothing waiting, nothing due");

        pending.hold(token, now);
        assert_eq!(pending.deadline(wait), Some(now + POSITION_WAIT), "a held position alone");

        // A batch opened later than the position was first held is still
        // due sooner: `max_wait` is shorter than `POSITION_WAIT`.
        pending.opened = Some(now + Duration::from_millis(500));
        assert_eq!(pending.deadline(wait), Some(now + Duration::from_millis(600)));

        // A position held almost a second ago is due before a batch just
        // opened; it goes out first and the flush takes the batch with it.
        pending.opened = Some(now + POSITION_WAIT - Duration::from_millis(10));
        assert_eq!(pending.deadline(wait), Some(now + POSITION_WAIT));

        // Holding a newer token does not restart the clock.
        pending.hold(kimmy_core::ResumeToken::new(Hlc::new(2, 1), node), now + POSITION_WAIT);
        assert_eq!(pending.held_since, Some(now));
    }

    /// `count` documents written and prepared into one batch, with the
    /// position that covers them held beside it — what `drive` hands to
    /// `flush` when a batch fills or its wait elapses, built by hand so the
    /// commit count is measured across exactly one flush.
    fn a_batch_of(
        engine: &Engine,
        worker: &EmbeddingWorker,
        coll: &CollectionMeta,
        titles: &[&str],
    ) -> (Pending, kimmy_core::ResumeToken) {
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let config = coll.vector.clone().unwrap();
        let mut pending = Pending::default();
        let now = Instant::now();
        for (i, title) in titles.iter().enumerate() {
            let id = engine.insert(coll, doc! { "_id": i as i64, "title": *title }).unwrap();
            let job = worker
                .prepare_one(coll, &shadow, &config, &id, false)
                .unwrap()
                .expect("a fresh document has a job");
            let item = Item {
                collection: coll.clone(),
                shadow: shadow.clone(),
                config: config.clone(),
                job,
            };
            assert!(!pending.push(item, now, &worker.batching), "the batch is not full");
        }
        let latest = last_entry(engine);
        let token = kimmy_core::ResumeToken::new(latest.stamp.hlc, latest.stamp.node);
        pending.hold(token.clone(), now);
        (pending, token)
    }

    /// The ADR-119 test rule applied to the worker's store (ADR-149): a
    /// provider batch of K documents plus the position that covers them is
    /// one commit, where it was K commits for the documents — each its own
    /// `put_vectors` — and one more for the position. The counters still
    /// read K documents: they count what was written, not the commits.
    #[tokio::test]
    async fn a_provider_batch_is_one_commit_with_its_position() {
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let titles = ["alpha", "beta", "gamma", "delta", "epsilon"];
        let (mut pending, token) = a_batch_of(&engine, &worker, &coll, &titles);
        let generation = engine.vector_generation(shadow.id);

        let commits = engine.commits();
        worker.flush(&mut pending).await.unwrap();
        assert_eq!(
            engine.commits() - commits,
            1,
            "{} documents and their position must be one commit, not {} plus one",
            titles.len(),
            titles.len()
        );
        assert_eq!(fake.calls(), 1, "one provider call for the batch");
        assert_eq!(
            engine.consumer_position(CONSUMER).unwrap(),
            Some(token),
            "the position landed in the batch's commit"
        );
        for i in 0..titles.len() {
            let source = kimmy_core::DocId::Int64(i as i64);
            assert_eq!(engine.get_vectors(&shadow, &source).unwrap().len(), 1, "{source} landed");
        }
        assert_eq!(worker.counters.documents_embedded.load(Ordering::Relaxed), titles.len() as u64);
        assert_eq!(worker.counters.chunks_embedded.load(Ordering::Relaxed), titles.len() as u64);
        assert_eq!(engine.vector_generation(shadow.id), generation + 1, "bumped once, after");
        assert!(pending.token.is_none() && pending.held_since.is_none(), "nothing left held");
    }

    /// A batch is stored whole or not at all. A failure on the second
    /// document — a unique index on the shadow that its chunk collides
    /// with — leaves the first document's chunks unstored too, the position
    /// where it was, and the commit counter where it was: the position
    /// must not advance past work that did not land, and a failed flush
    /// commits nothing. The token is still held, with its deadline, for the
    /// next flush.
    #[tokio::test]
    async fn a_storage_failure_mid_batch_advances_neither_the_vectors_nor_the_position() {
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let text = kimmy_storage::IndexField { path: "text".into(), descending: false };
        engine.create_index("app", &shadow.name, vec![text], true, None).unwrap();
        // A committed chunk whose text the second document's chunk repeats.
        engine.insert(&coll, doc! { "_id": "taken", "title": "same" }).unwrap();
        assert!(matches!(
            worker.process(&last_entry(&engine)).await.unwrap(),
            Outcome::Embedded { .. }
        ));
        position_at_latest(&engine);
        let before = engine.consumer_position(CONSUMER).unwrap();

        let (mut pending, token) = a_batch_of(&engine, &worker, &coll, &["alpha", "same", "beta"]);
        assert_ne!(Some(token.clone()), before);
        let held_since = pending.held_since;
        let documents = worker.counters.documents_embedded.load(Ordering::Relaxed);
        let calls = fake.calls();

        let commits = engine.commits();
        worker.flush(&mut pending).await.unwrap();
        assert_eq!(fake.calls() - calls, 1, "the provider answered; storage is what failed");
        assert_eq!(engine.commits(), commits, "a failed flush commits nothing");
        assert_eq!(engine.consumer_position(CONSUMER).unwrap(), before, "the position stayed");
        for i in 0..3i64 {
            let source = kimmy_core::DocId::Int64(i);
            assert!(
                engine.get_vectors(&shadow, &source).unwrap().is_empty(),
                "no chunk of the batch may land when one document of it cannot: {source}"
            );
        }
        assert_eq!(worker.counters.documents_embedded.load(Ordering::Relaxed), documents);
        assert_eq!(pending.token, Some(token), "the position is held for the next flush");
        assert_eq!(pending.held_since, held_since, "with its deadline untouched");
        assert!(pending.batches.is_empty(), "the batch itself is spent");
    }

    /// ADR-125's other shape, unchanged: a held position with nothing to
    /// embed is one commit of its own.
    #[tokio::test]
    async fn a_held_position_with_no_batch_is_still_one_commit() {
        let (engine, _coll, mut worker, _dir) = setup().await;
        let token = kimmy_core::ResumeToken::new(Hlc::new(42, 1), engine.node_id());
        let mut pending = Pending::default();
        pending.hold(token.clone(), Instant::now());

        let commits = engine.commits();
        worker.flush(&mut pending).await.unwrap();
        assert_eq!(engine.commits() - commits, 1, "one position write, and nothing else");
        assert_eq!(engine.consumer_position(CONSUMER).unwrap(), Some(token));
        assert!(pending.token.is_none() && pending.held_since.is_none());
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
            provider: ProviderConfig::Byo {},
            dim: 4,
            metric: Metric::Cosine,
            document_prefix: None,
            query_prefix: None,
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
        // The three documents go out as one batch; when that fails
        // permanently each is tried alone, once, to find the one at fault —
        // here all of them — and then skipped.
        assert_eq!(fake.sizes(), vec![3, 1, 1, 1], "one batch, then each document once");
    }

    // -----------------------------------------------------------------------
    // Batching: one provider call carries many documents (ADR-095)
    // -----------------------------------------------------------------------

    /// A collection whose documents are one short chunk each — the case
    /// batching exists for — with a fake provider that records call sizes.
    async fn setup_with_short_documents(
        count: usize,
    ) -> (Arc<Engine>, CollectionMeta, EmbeddingWorker, Arc<FakeProvider>, tempfile::TempDir) {
        let (engine, coll, mut worker, dir) = setup_with_history(count).await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);
        (engine, coll, worker, fake, dir)
    }

    fn total_chunks(engine: &Engine, count: usize) -> usize {
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        (0..count)
            .map(|i| {
                engine.get_vectors(&shadow, &kimmy_core::DocId::Int64(i as i64)).unwrap().len()
            })
            .sum()
    }

    #[tokio::test]
    async fn a_backfill_of_short_documents_makes_far_fewer_calls_than_documents() {
        // Before this, 100 one-chunk documents were 100 provider calls, each
        // paying a whole round trip for one input. Measured against a CPU
        // llama.cpp server: 32 calls of one input took 394 ms, one call of
        // 32 took 18 ms.
        let count = 100;
        let (engine, _coll, mut worker, fake, _dir) = setup_with_short_documents(count).await;

        let outcome = worker.process(&last_entry(&engine)).await.unwrap();
        assert_eq!(outcome, Outcome::Backfilled { embedded: count });

        let max = BatchSettings::default().max_chunks;
        assert!(
            fake.calls() <= count.div_ceil(max) + 1,
            "{} documents took {} provider calls",
            count,
            fake.calls()
        );
        assert!(fake.sizes().iter().all(|&n| n <= max), "{:?}", fake.sizes());
        assert_eq!(fake.sizes().iter().sum::<usize>(), count, "every document went exactly once");
        assert_eq!(total_chunks(&engine, count), count);

        // The counters count documents and chunks, never calls.
        let counters = worker.counters();
        assert_eq!(counters.documents_embedded.load(Ordering::SeqCst), count as u64);
        assert_eq!(counters.chunks_embedded.load(Ordering::SeqCst), count as u64);
    }

    #[tokio::test]
    async fn multi_chunk_documents_keep_their_chunks_in_one_call_and_count_as_chunks() {
        // Each document is several chunks; the counters must say so, and no
        // document may be split across two calls, because the storage write
        // behind it replaces the document's chunks as one.
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let coll = engine.create_collection("app", "docs").unwrap();
        let count = 10;
        // 50 characters at max_chars 20 / overlap 5 is four chunks.
        let chunks_each = config(&["body"]).chunk.split(&"x".repeat(50)).len();
        assert!(chunks_each > 1);
        for i in 0..count {
            engine.insert(&coll, doc! { "_id": i as i64, "body": "x".repeat(50) }).unwrap();
        }
        engine.configure_vectors("app", "docs", config(&["body"])).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();
        let mut worker = EmbeddingWorker::new(Arc::clone(&engine));
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);
        worker.set_batching(BatchSettings { max_chunks: 10, ..Default::default() });

        let outcome = worker.process(&last_entry(&engine)).await.unwrap();
        assert_eq!(outcome, Outcome::Backfilled { embedded: count });

        // Ten chunks per call and several per document: as many whole
        // documents as fit, never a partial one, so every call is a multiple
        // of the per-document chunk count.
        assert!(fake.sizes().iter().all(|&n| n % chunks_each == 0), "{:?}", fake.sizes());
        assert!(fake.sizes().iter().all(|&n| n <= 10), "{:?}", fake.sizes());
        assert_eq!(fake.calls(), count.div_ceil(10 / chunks_each));

        let counters = worker.counters();
        assert_eq!(counters.documents_embedded.load(Ordering::SeqCst), count as u64);
        assert_eq!(counters.chunks_embedded.load(Ordering::SeqCst), (count * chunks_each) as u64);
    }

    #[tokio::test]
    async fn the_token_bound_splits_a_batch_of_large_chunks_at_a_small_count() {
        // Every document here is one 20-character chunk, ten estimated tokens
        // at two bytes per token. With room for 25 tokens a call takes two;
        // with room for 5 a single document already exceeds the bound and
        // goes alone — never split, because its storage write cannot be.
        let count = 6;
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let coll = engine.create_collection("app", "docs").unwrap();
        for i in 0..count {
            engine
                .insert(&coll, doc! { "_id": i as i64, "title": "abcdefghijklmnopqrst" })
                .unwrap();
        }
        engine.configure_vectors("app", "docs", config(&["title"])).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();
        let entry = last_entry(&engine);
        let mut worker = EmbeddingWorker::new(Arc::clone(&engine));
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);

        worker.set_batching(BatchSettings { max_chunks: 32, max_tokens: 25, ..Default::default() });
        assert_eq!(worker.process(&entry).await.unwrap(), Outcome::Backfilled { embedded: count });
        assert_eq!(fake.sizes(), vec![2, 2, 2], "25 tokens is room for two ten-token chunks");

        // Reconfigure so the scan is forced, then bound below one document.
        let mut tighter = config(&["title"]);
        tighter.chunk.overlap = 4;
        engine.configure_vectors("app", "docs", tighter).unwrap();
        let entry = last_entry(&engine);
        fake.sizes.lock().unwrap().clear();
        worker.set_batching(BatchSettings { max_chunks: 32, max_tokens: 5, ..Default::default() });
        assert_eq!(worker.process(&entry).await.unwrap(), Outcome::Backfilled { embedded: count });
        assert_eq!(fake.sizes(), vec![1; count], "a document over the bound goes alone");
    }

    #[tokio::test]
    async fn a_permanently_failing_document_does_not_poison_its_batch() {
        // A provider refuses one input in a batch of five — an oversized
        // chunk, say — with a 400 that names nothing. The batch fails once;
        // each document is then sent alone, the four good ones land, and the
        // bad one is skipped after exactly one attempt of its own.
        let count = 5;
        let (engine, _coll, mut worker, fake, _dir) = setup_with_short_documents(count).await;
        *fake.poison.lock().unwrap() = Some("doc 2".into());

        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            worker.process(&last_entry(&engine)),
        )
        .await
        .expect("a permanent failure must not retry forever")
        .unwrap();
        assert_eq!(outcome, Outcome::Backfilled { embedded: count - 1 });

        assert_eq!(fake.sizes(), vec![5, 1, 1, 1, 1, 1], "the batch once, then each alone once");
        let attempts = fake.inputs.lock().unwrap().iter().filter(|t| t.contains("doc 2")).count();
        assert_eq!(attempts, 2, "the bad document: once in the batch, once alone, never again");

        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        for i in 0..count as i64 {
            let vectors = engine.get_vectors(&shadow, &kimmy_core::DocId::Int64(i)).unwrap();
            assert_eq!(vectors.len(), usize::from(i != 2), "document {i}");
        }
        let counters = worker.counters();
        assert_eq!(counters.documents_embedded.load(Ordering::SeqCst), count as u64 - 1);
        assert_eq!(counters.failures.load(Ordering::SeqCst), 2, "the batch and the lone retry");
    }

    /// Park the worker's recorded position on the newest entry, so a worker
    /// started afterwards streams only what is written from here on — and
    /// finds it all already on disk, back to back, as a backlog is.
    fn position_at_latest(engine: &Engine) {
        let latest = last_entry(engine);
        let token = kimmy_core::ResumeToken::new(latest.stamp.hlc, latest.stamp.node);
        engine.put_consumer_position(CONSUMER, token.clone()).unwrap();
    }

    /// Wait until `count` documents have vectors, or give up.
    async fn vectors_land(engine: &Engine, count: usize) {
        for _ in 0..2_000 {
            if total_chunks(engine, count) >= count {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("only {} of {count} documents were embedded", total_chunks(engine, count));
    }

    #[tokio::test]
    async fn a_streaming_backlog_is_embedded_in_batches_and_the_position_trails_it() {
        // The live-ingest shape: writes already on disk when the worker
        // reaches them. Consecutive entries fill a batch without waiting,
        // the remainder goes when the timer runs out, and the position is
        // recorded only once everything before it has landed.
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);
        position_at_latest(&engine);

        let count = 50;
        for i in 0..count {
            engine.insert(&coll, doc! { "_id": i as i64, "title": format!("doc {i}") }).unwrap();
        }
        let entries: Vec<kimmy_core::OplogEntry> = engine
            .read_oplog_from(Hlc::ZERO, 10_000)
            .unwrap()
            .into_iter()
            .filter(|e| e.collection == coll.id && e.doc_id.is_some())
            .collect();
        assert_eq!(entries.len(), count);
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        tokio::spawn(async move { worker.run().await });

        // At every observation, an entry the recorded position has passed is
        // an entry whose vectors are on disk: the position is written after
        // the batch, never before it, so a crash replays rather than skips.
        let mut landed = 0;
        for _ in 0..2_000 {
            let position = engine.consumer_position(CONSUMER).unwrap().map(|t| t.to_stamp());
            for entry in &entries {
                if position.is_some_and(|p| entry.stamp <= p) {
                    let source = entry.doc_id.clone().unwrap();
                    assert!(
                        !engine.get_vectors(&shadow, &source).unwrap().is_empty(),
                        "the position passed {source} before its vectors were written"
                    );
                }
            }
            landed = total_chunks(&engine, count);
            if landed == count {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(landed, count, "every streamed document must be embedded");

        let max = BatchSettings::default().max_chunks;
        assert!(
            fake.calls() <= count.div_ceil(max) + 1,
            "{count} streamed documents took {} provider calls: {:?}",
            fake.calls(),
            fake.sizes()
        );
        assert_eq!(fake.sizes().iter().sum::<usize>(), count);
    }

    #[tokio::test]
    async fn a_lone_document_on_a_quiet_stream_is_embedded_within_the_batch_wait() {
        // The timer, not the size bound: one document arrives and nothing
        // follows it. It must go out after `max_wait`, not sit until the
        // five-second deferral tick or the next write.
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);
        worker.set_batching(BatchSettings {
            max_wait: std::time::Duration::from_millis(50),
            ..Default::default()
        });
        position_at_latest(&engine);
        tokio::spawn(async move { worker.run().await });

        let started = Instant::now();
        engine.insert(&coll, doc! { "_id": 0i64, "title": "alone" }).unwrap();
        vectors_land(&engine, 1).await;
        let took = started.elapsed();
        assert!(
            took < DEFERRAL_TICK / 2,
            "a lone document waited {took:?}; the batch timer should have sent it long before \
             the {DEFERRAL_TICK:?} tick"
        );
        assert_eq!(fake.sizes(), vec![1]);
    }

    #[tokio::test]
    async fn a_batch_holds_one_collection_only() {
        // Two vector-enabled collections written turn about. Each has its
        // own provider — model, prefix, endpoint — so a call can only carry
        // one collection's chunks; but the *other* collection's run must not
        // break the first one's batch either.
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        engine.create_collection("app", "a").unwrap();
        engine.create_collection("app", "b").unwrap();
        engine.configure_vectors("app", "a", config(&["title"])).unwrap();
        engine.configure_vectors("app", "b", config(&["title"])).unwrap();
        let a = engine.get_collection("app", "a").unwrap();
        let b = engine.get_collection("app", "b").unwrap();
        let mut worker = EmbeddingWorker::new(Arc::clone(&engine));
        let fake_a = FakeProvider::new(4);
        let fake_b = FakeProvider::new(4);
        worker.set_provider(a.id.0, Arc::clone(&fake_a) as Arc<dyn EmbeddingProvider>);
        worker.set_provider(b.id.0, Arc::clone(&fake_b) as Arc<dyn EmbeddingProvider>);
        position_at_latest(&engine);

        let count = 20;
        for i in 0..count {
            engine.insert(&a, doc! { "_id": i as i64, "title": format!("a {i}") }).unwrap();
            engine.insert(&b, doc! { "_id": i as i64, "title": format!("b {i}") }).unwrap();
        }
        tokio::spawn(async move { worker.run().await });

        for coll in ["a", "b"] {
            let shadow = engine.vector_collection("app", coll).unwrap().unwrap();
            for _ in 0..2_000 {
                let n: usize = (0..count)
                    .map(|i| engine.get_vectors(&shadow, &kimmy_core::DocId::Int64(i as i64)))
                    .map(|v| v.unwrap().len())
                    .sum();
                if n == count {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }

        for (fake, tag) in [(&fake_a, "a "), (&fake_b, "b ")] {
            assert!(fake.calls() <= 2, "{tag}took {} calls: {:?}", fake.calls(), fake.sizes());
            assert_eq!(fake.sizes().iter().sum::<usize>(), count);
            assert!(fake.inputs.lock().unwrap().iter().all(|t| t.starts_with(tag)));
        }
    }

    #[tokio::test]
    async fn a_batch_carries_only_the_latest_version_of_a_document() {
        // Two writes to one document before the batch goes out: embedding
        // the first would be a wasted input whose vectors the stamp check
        // discards. Only the second version is sent.
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);
        position_at_latest(&engine);

        let id = engine.insert(&coll, doc! { "_id": 0i64, "title": "first" }).unwrap();
        engine.replace(&coll, &id, doc! { "title": "second" }, false).unwrap();
        tokio::spawn(async move { worker.run().await });
        vectors_land(&engine, 1).await;

        assert_eq!(fake.sizes(), vec![1]);
        assert_eq!(*fake.inputs.lock().unwrap(), vec!["second".to_string()]);
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

        let byo = VectorConfig { provider: ProviderConfig::Byo {}, ..config(&["title"]) };
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
        // The test configuration points at localhost, which the default
        // policy refuses; this worker is an operator who listed it.
        real.set_policy(loopback_policy());
        let a = config(&["title"]);
        let built_a = real.provider_for(coll.id.0, &a).unwrap();
        let mut b = config(&["title"]);
        b.dim = 8;
        let built_b = real.provider_for(coll.id.0, &b).unwrap();
        assert_eq!(built_a.dim(), 4);
        assert_eq!(built_b.dim(), 8, "a changed configuration must rebuild the provider");
    }

    #[tokio::test]
    async fn a_stored_configuration_naming_a_node_secret_is_refused_by_the_worker() {
        // The layer replication reaches. A configuration written straight
        // into the engine — as a replicated `ConfigureVectors` entry is —
        // never passed this node's API, so the worker has to refuse it on
        // its own: no provider is built, the variable is never read, the
        // document is skipped rather than retried, and the refusal is a
        // permanent one for that configuration.
        let (engine, coll, mut worker, _dir) = setup().await;
        let mut stolen = config(&["title"]);
        stolen.provider = ProviderConfig::OpenAi {
            model: "m".into(),
            endpoint: Some("https://93.184.216.34".into()),
            api_key_env: "KIMMY_JWT_SECRET".into(),
            dimensions: None,
        };
        engine.configure_vectors("app", "docs", stolen.clone()).unwrap();
        // The injected fake is evicted by the changed configuration, so the
        // real builder — and the policy — decide.
        worker.providers.clear();

        let err = worker.provider_for(coll.id.0, &stolen).err().expect("refused");
        assert!(err.is_refused_by_policy(), "{err:?}");
        assert!(!err.is_retryable(), "a policy refusal must not stall the stream");
        assert!(err.to_string().contains("KIMMY_JWT_SECRET"), "{err}");
        assert!(worker.refused.contains_key(&coll.id.0), "remembered, so it is said once");

        // The same answer again, from memory rather than a second report.
        let again = worker.provider_for(coll.id.0, &stolen).err().expect("still refused");
        assert!(matches!(again, VectorError::PolicyRefused(_)), "{again:?}");

        // Through the stream: the entry fails with the refusal — which `run`
        // classifies as permanent and skips — and nothing was written to the
        // shadow collection.
        let coll = engine.get_collection("app", "docs").unwrap();
        let id = engine.insert(&coll, doc! { "_id": 1i64, "title": "hello" }).unwrap();
        let err = worker.process(&last_entry(&engine)).await.unwrap_err();
        assert!(err.is_refused_by_policy() && !err.is_retryable(), "{err:?}");
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        assert!(engine.get_vectors(&shadow, &id).unwrap().is_empty());

        // Reconfiguring to something the policy admits forgets the refusal.
        let mut fixed = stolen.clone();
        fixed.provider =
            ProviderConfig::Ollama { model: "m".into(), endpoint: "http://localhost:1".into() };
        worker.set_policy(loopback_policy());
        worker.provider_for(coll.id.0, &fixed).expect("an admitted configuration builds");
        assert!(!worker.refused.contains_key(&coll.id.0));
    }

    #[tokio::test]
    async fn a_profile_resolves_in_the_worker_and_a_missing_one_fails_permanently() {
        // A collection naming a profile embeds through the operator's
        // provider, with the dialect and endpoint the profile defines. One
        // naming a profile this node lacks is a permanent, remembered
        // failure — a retry would ask the same configuration the same
        // question.
        let (_engine, coll, mut worker, _dir) = setup().await;
        let mut profiles = std::collections::BTreeMap::new();
        profiles.insert(
            "lan".to_string(),
            ProviderConfig::Ollama { model: "m".into(), endpoint: "http://localhost:1".into() },
        );
        worker.set_policy(
            ProviderPolicy::new(
                crate::policy::default_allowed_key_env(),
                vec!["localhost".into()],
                false,
                profiles,
            )
            .unwrap(),
        );
        worker.providers.clear();

        let mut named = config(&["title"]);
        named.provider = ProviderConfig::Profile { name: "lan".into() };
        let built = worker.provider_for(coll.id.0, &named).expect("the profile resolves");
        assert_eq!(built.name(), "ollama", "the profile's dialect");

        let mut missing = config(&["title"]);
        missing.provider = ProviderConfig::Profile { name: "nope".into() };
        let err = worker.provider_for(coll.id.0, &missing).err().expect("no such profile");
        assert!(
            matches!(err, VectorError::UnknownProfile { ref name } if name == "nope"),
            "{err:?}"
        );
        assert!(!err.is_retryable());
        assert!(worker.refused.contains_key(&coll.id.0));
    }

    /// The same entry as if a different node had written it. Since ownership
    /// replaced the origin rule this changes nothing about what the worker
    /// does with it — which is exactly what the tests below check.
    fn as_if_written_elsewhere(mut entry: kimmy_core::OplogEntry) -> kimmy_core::OplogEntry {
        entry.stamp.node = kimmy_core::NodeId::from_bytes([0xAB; 16]);
        entry
    }

    /// An ownership check a test can flip mid-flight, standing in for the
    /// member set changing under a running worker.
    fn switchable_owner(
        worker: &mut EmbeddingWorker,
        initially: bool,
    ) -> Arc<std::sync::atomic::AtomicBool> {
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(initially));
        let seen = Arc::clone(&flag);
        worker.set_owner_check(Box::new(move |_| seen.load(std::sync::atomic::Ordering::SeqCst)));
        flag
    }

    #[tokio::test]
    async fn a_non_owner_does_not_embed_a_document_written_elsewhere() {
        // Every node runs a worker and every node sees every write, so all of
        // them used to embed the same document at once. The stored result was
        // right -- the staleness check makes a losing write a no-op -- but the
        // provider calls were not deduplicated. Measured on a live three-node
        // cluster: 23 embedding requests for 10 documents.
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);
        switchable_owner(&mut worker, false);

        engine.insert(&coll, bson::doc! { "_id": "a", "title": "hello", "body": "world" }).unwrap();
        let entry = as_if_written_elsewhere(last_entry(&engine));

        assert_eq!(worker.process(&entry).await.unwrap(), Outcome::Deferred);
        assert_eq!(
            fake.calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the owner pays; nobody else"
        );
    }

    #[tokio::test]
    async fn a_non_owner_does_not_embed_a_document_it_wrote_itself() {
        // The half of the origin rule that cost 570 provider calls for 361
        // documents on 2026-08-28: the node the client happened to write to
        // embedded everything itself while the owner, thirty seconds behind,
        // embedded it all again. The writer is just another non-owner now.
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);
        switchable_owner(&mut worker, false);

        engine.insert(&coll, bson::doc! { "_id": "a", "title": "hello", "body": "world" }).unwrap();
        let entry = last_entry(&engine);

        assert_eq!(worker.process(&entry).await.unwrap(), Outcome::Deferred);
        assert_eq!(fake.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn the_owner_embeds_a_document_written_elsewhere_without_waiting() {
        // The other half of the fix. The owner used to hold a foreign write
        // for the full grace before looking at it; now a replicated write is
        // embedded the moment the owner's stream delivers it, from the
        // entry's own image, exactly as a local write always was.
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);
        switchable_owner(&mut worker, true);

        engine.insert(&coll, bson::doc! { "_id": "a", "title": "hello", "body": "world" }).unwrap();
        let entry = as_if_written_elsewhere(last_entry(&engine));

        assert!(matches!(worker.process(&entry).await.unwrap(), Outcome::Embedded { .. }));
        assert_eq!(fake.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(worker.deferred.is_empty(), "the owner has nothing to re-check");
    }

    #[tokio::test]
    async fn the_document_prefix_reaches_the_provider_but_not_the_stored_chunk() {
        // Models trained on `passage: …` rank badly without it (measured:
        // e5-large's separation 0.04, Nemotron-1B's recall 0.40 on the
        // 2026-08-29 evaluation). The prefix is for the model; a hit's text
        // stays what the document said.
        let (engine, coll, mut worker, _dir) = setup().await;
        let fake = FakeProvider::new(4);
        worker.set_provider(coll.id.0, Arc::clone(&fake) as Arc<dyn EmbeddingProvider>);
        let mut prefixed_config = config(&["title"]);
        prefixed_config.document_prefix = Some("passage: ".into());
        engine.configure_vectors("app", "docs", prefixed_config).unwrap();

        engine.insert(&coll, bson::doc! { "_id": "a", "title": "hello world" }).unwrap();
        let entry = last_entry(&engine);
        assert!(matches!(worker.process(&entry).await.unwrap(), Outcome::Embedded { .. }));

        assert_eq!(fake.inputs.lock().unwrap().as_slice(), ["passage: hello world"]);
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let stored = engine.get_vectors(&shadow, &kimmy_core::DocId::String("a".into())).unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].text, "hello world", "the prefix is never stored");
    }

    #[tokio::test]
    async fn a_document_this_node_wrote_is_embedded_without_waiting() {
        // Deferring everything would trade duplicated work for embedding
        // nothing until a timer fired, so the owner's path has to stay
        // immediate. With no check installed this node owns everything.
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
        let owner = switchable_owner(&mut worker, false);

        engine.insert(&coll, bson::doc! { "_id": "a", "title": "hello", "body": "world" }).unwrap();
        let entry = as_if_written_elsewhere(last_entry(&engine));
        assert_eq!(worker.process(&entry).await.unwrap(), Outcome::Deferred);

        // Nothing replicated in the meantime: the owner never embedded it.
        // The owner leaves before embedding it; the rendezvous hash over the
        // survivors now lands the collection here.
        owner.store(true, std::sync::atomic::Ordering::SeqCst);
        let embedded = worker.drain_deferred(Instant::now() + FOREIGN_GRACE).await;

        assert_eq!(embedded, 1, "a document nobody embedded must not stay unembedded");
        assert_eq!(fake.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_deferred_document_is_held_while_another_node_owns_the_collection() {
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

        let deferred_at = Instant::now();
        // Grace after grace, the owner is still alive: nothing is embedded and
        // the document stays in the queue against the owner leaving later.
        for i in 1..=3 {
            let embedded = worker.drain_deferred(deferred_at + FOREIGN_GRACE * i).await;
            assert_eq!(embedded, 0, "a non-owner must not embed");
            assert_eq!(worker.deferred.len(), 1, "held, not dropped, while the owner is alive");
        }
        assert_eq!(
            fake.calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "no provider call may happen off-owner"
        );
        let counters = worker.counters();
        assert_eq!(counters.skipped_not_owned.load(std::sync::atomic::Ordering::SeqCst), 0);

        // Past the age cap the document is the owner's alone, and the queue
        // does not keep a healthy cluster's whole history.
        let embedded = worker.drain_deferred(deferred_at + DEFERRAL_MAX_AGE + FOREIGN_GRACE).await;
        assert_eq!(embedded, 0);
        assert!(worker.deferred.is_empty(), "let go once older than DEFERRAL_MAX_AGE");
        assert_eq!(counters.skipped_not_owned.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(fake.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
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
        let outcome = worker.process(&as_if_written_elsewhere(last_entry(&engine))).await.unwrap();
        assert!(matches!(outcome, Outcome::Embedded { .. }), "owned, so embedded at once");
        assert!(worker.deferred.is_empty());
        assert_eq!(worker.drain_deferred(Instant::now() + FOREIGN_GRACE).await, 0);
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
        let owner = switchable_owner(&mut worker, false);

        engine.insert(&coll, bson::doc! { "_id": "a", "title": "hello", "body": "world" }).unwrap();
        let entry = as_if_written_elsewhere(last_entry(&engine));
        assert_eq!(worker.process(&entry).await.unwrap(), Outcome::Deferred);

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

        // Ownership moves here after the old owner's vectors landed: the
        // re-check finds them current and calls nobody.
        owner.store(true, std::sync::atomic::Ordering::SeqCst);
        let embedded = worker.drain_deferred(Instant::now() + FOREIGN_GRACE).await;

        assert_eq!(embedded, 0);
        assert!(worker.deferred.is_empty());
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
        switchable_owner(&mut worker, false);

        engine.insert(&coll, bson::doc! { "_id": "a", "title": "hello", "body": "world" }).unwrap();
        let entry = as_if_written_elsewhere(last_entry(&engine));
        assert_eq!(worker.process(&entry).await.unwrap(), Outcome::Deferred);

        assert_eq!(worker.drain_deferred(Instant::now()).await, 0, "the owner still has time");
        assert_eq!(fake.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}
