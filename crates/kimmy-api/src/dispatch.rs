//! The webhook dispatcher: an ordinary oplog consumer that dials out.
//!
//! # Progress is replicated, which is what makes failover work
//!
//! Each node records, per subscription, a [`VersionVector`] of what it has
//! delivered — "for events originating at node X, delivered through HLC H".
//! Records live in `__kimmy.__webhook_progress` keyed `{subscription}:{node}`,
//! so **a node only ever writes its own** and there is nothing for
//! last-writer-wins to discard. Any node reads the union to learn what the
//! cluster as a whole has already sent.
//!
//! That is the piece that makes a death survivable. When the owner dies, the
//! next owner reads the union and resumes from it rather than from the
//! beginning — see [`crate::ownership`] for who that is and why deriving it is
//! not leader election.
//!
//! # At-least-once, and why that is the honest guarantee
//!
//! Progress advances **after** a delivery succeeds, so a crash mid-flight
//! redelivers rather than skipping. Exactly-once is not achievable over a
//! network by any design, so every delivery carries the originating `Stamp` as
//! a globally unique `X-Kimmy-Event-Id`, and deduplicating is a set-membership
//! test on the receiver.
//!
//! # The egress policy is re-checked before every delivery
//!
//! Not only at registration. A hostname is not a destination: a name that
//! resolves publicly when a webhook is registered can resolve to
//! `169.254.169.254` an hour later. Checking once would validate a promise DNS
//! can withdraw.
//!
//! # A pass plans serially, delivers concurrently, and applies serially
//!
//! Only the network call runs concurrently, under a semaphore of
//! [`Limits::max_concurrent_deliveries`]. Everything that touches the engine —
//! reading progress, choosing a batch, recording what landed — stays on one
//! thread, so there is no interleaving to reason about and no way for two
//! subscriptions' progress writes to race.
//!
//! Concurrency is not an optimisation here. A serial pass lets one endpoint
//! that has stopped answering hold the whole node for the delivery timeout,
//! delaying every subscription behind it — which is the cross-subscription
//! interference the per-subscription [`Backoff`] exists to prevent, one layer
//! up. The bound is what keeps a webhook on a hot collection from consuming
//! every outbound connection the node has.
//!
//! # An event is never dropped for being large
//!
//! A batch is trimmed to fit [`Limits::max_payload_bytes`]; the remainder goes
//! next pass. A single event whose document alone exceeds the cap is delivered
//! with `fullDocument` omitted and `fullDocumentOmitted` set, so the receiver
//! still learns the change happened and can fetch the document itself. Skipping
//! it would leave a gap the receiver could never detect — the thing
//! [`invalidate`] exists to avoid.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bson::{Document, doc};
use kimmy_core::{Hlc, OpKind, OplogEntry, Stamp, VersionVector};
use serde_json::{Value, json};
use tracing::{debug, info, warn};

use crate::egress::EgressPolicy;
use crate::state::SharedState;
use crate::webhooks::{WEBHOOKS_COLLECTION, WEBHOOKS_DB};

/// Where per-node delivery progress lives.
pub const PROGRESS_COLLECTION: &str = "__webhook_progress";

/// Events delivered in one request.
///
/// A bulk load writes far faster than any endpoint can answer, so events are
/// batched rather than sent one per request. Small enough that a failed batch
/// is a cheap retry.
const BATCH: usize = 64;

/// How long a delivery may take before it counts as failed.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);

/// How often a node looks for work.
const TICK: Duration = Duration::from_secs(2);

/// Backoff bounds for an endpoint that is failing.
const BACKOFF_MIN: Duration = Duration::from_secs(2);
const BACKOFF_MAX: Duration = Duration::from_secs(300);

/// What one node will spend on delivery.
///
/// Held together rather than passed as loose numbers so the dispatcher's
/// signature does not grow a parameter every time a bound is added.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Deliveries in flight at once, across every subscription this node owns.
    pub max_concurrent_deliveries: usize,
    /// The largest request body a delivery may carry.
    pub max_payload_bytes: usize,
    /// How old a subscription's resume point may get before it is written
    /// forward even though nothing was delivered.
    ///
    /// Not a knob an operator sets — a cadence. See
    /// [`Limits::DEFAULT_PROGRESS_HEARTBEAT`] for why it exists at all.
    pub progress_heartbeat: Duration,
}

impl Limits {
    /// Why the resume point has to move even when nothing is delivered.
    ///
    /// Retention collects by age. A subscription's resume point only advanced
    /// when something was delivered, so a webhook on a quiet collection — or on
    /// a busy one that goes quiet for a night — sat still while the horizon
    /// walked toward it, and was invalidated for "falling behind" events it had
    /// never been going to be sent. Every healthy webhook died one retention
    /// window after its last delivery.
    ///
    /// A minute, against a retention window measured in hours: frequent enough
    /// that the horizon never catches up, rare enough that an idle node writes
    /// once a minute per subscription rather than once per two-second tick.
    pub const DEFAULT_PROGRESS_HEARTBEAT: Duration = Duration::from_secs(60);
}

impl Default for Limits {
    /// The same numbers as `webhooks` in the config file, so a test and a
    /// default-configured node behave alike.
    fn default() -> Self {
        Self {
            max_concurrent_deliveries: 8,
            max_payload_bytes: 1024 * 1024,
            progress_heartbeat: Self::DEFAULT_PROGRESS_HEARTBEAT,
        }
    }
}

/// What one dispatch pass did, for tests and for metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DispatchOutcome {
    pub delivered: usize,
    pub failed: usize,
    pub skipped_not_owner: usize,
    /// Subscriptions passed over because they are still backing off.
    pub skipped_backoff: usize,
    /// Subscriptions stopped because the history they needed was collected.
    pub invalidated: usize,
}

/// Per-subscription failure state, held across passes.
///
/// **Per subscription, not per pass.** One endpoint that has stopped answering
/// must not slow deliveries to every other endpoint on the node — which is
/// exactly what a single shared backoff does, and what this replaces.
#[derive(Default)]
pub struct Backoff {
    failures: std::collections::HashMap<String, (u32, std::time::Instant)>,
}

impl Backoff {
    /// Whether this subscription may be attempted now.
    fn ready(&self, subscription: &str) -> bool {
        match self.failures.get(subscription) {
            Some((_, next)) => std::time::Instant::now() >= *next,
            None => true,
        }
    }

    fn failed(&mut self, subscription: &str) {
        let entry =
            self.failures.entry(subscription.to_string()).or_insert((0, std::time::Instant::now()));
        entry.0 = entry.0.saturating_add(1);
        // Doubling, capped. Capped because an endpoint that comes back after an
        // hour should be noticed in minutes, not left waiting for a delay that
        // grew while it was down.
        let delay = BACKOFF_MIN.saturating_mul(1u32 << entry.0.min(8));
        entry.1 = std::time::Instant::now() + delay.min(BACKOFF_MAX);
    }

    fn succeeded(&mut self, subscription: &str) {
        self.failures.remove(subscription);
    }

    /// Forget subscriptions that no longer exist.
    ///
    /// Failure state is otherwise only cleared by a successful delivery, which
    /// a removed subscription can never have — so a subscription deleted while
    /// its endpoint was failing would leave its entry behind forever.
    fn prune(&mut self, live: &std::collections::HashSet<&str>) {
        self.failures.retain(|id, _| live.contains(id.as_str()));
    }

    /// Consecutive failures recorded for a subscription.
    pub fn failure_count(&self, subscription: &str) -> u32 {
        self.failures.get(subscription).map(|(n, _)| *n).unwrap_or(0)
    }
}

/// Read the union of every node's progress for a subscription.
///
/// The union rather than this node's own record: after a failover the new owner
/// must not resend what the previous owner already delivered.
pub fn union_progress(state: &SharedState, subscription: &str) -> VersionVector {
    let mut union = VersionVector::new();
    let Ok(meta) = state.engine.get_collection(WEBHOOKS_DB, PROGRESS_COLLECTION) else {
        return union;
    };
    let prefix = format!("{subscription}:");
    let _ = state.engine.for_each_doc(&meta, |_id, document| {
        if document.get_str("_id").is_ok_and(|id| id.starts_with(&prefix))
            && let Ok(vector) = document.get_document("delivered")
        {
            for (node, hlc) in vector {
                // The HLC is stored as its own byte encoding rather than a
                // string: it has no `FromStr`, and its wall clock is a `u64`
                // that BSON cannot hold as an integer above `i64::MAX` — the
                // bug that once stopped half of all collections replicating.
                let bson::Bson::Binary(binary) = hlc else {
                    continue;
                };
                let (Ok(node), bytes) = (node.parse(), binary.bytes.as_slice()) else {
                    continue;
                };
                let Ok(bytes) = <[u8; kimmy_core::HLC_ENCODED_LEN]>::try_from(bytes) else {
                    continue;
                };
                let mut one = VersionVector::new();
                one.insert(node, Hlc::from_bytes(bytes));
                union.merge(&one);
            }
        }
        Ok(true)
    });
    union
}

/// Drop every node's progress record for a subscription that is gone.
///
/// Called when a subscription is removed. Without it `__webhook_progress`
/// accumulates one orphaned record per node that ever delivered the
/// subscription, and nothing ever reads or collects them again.
///
/// Best-effort: the subscription is already deleted by the time this runs, and
/// failing the removal over leftover bookkeeping would be the worse outcome.
pub fn forget_progress(state: &SharedState, subscription: &str) {
    let Ok(meta) = state.engine.get_collection(WEBHOOKS_DB, PROGRESS_COLLECTION) else {
        return;
    };
    let prefix = format!("{subscription}:");
    let mut stale = Vec::new();
    let _ = state.engine.for_each_doc(&meta, |_id, document| {
        if let Ok(id) = document.get_str("_id")
            && id.starts_with(&prefix)
        {
            stale.push(id.to_string());
        }
        Ok(true)
    });
    for id in stale {
        if let Err(e) = state.engine.delete(&meta, &kimmy_core::DocId::String(id.clone())) {
            warn!(subscription, record = %id, error = %e, "could not remove webhook progress");
        }
    }
}

/// Mark a subscription as already caught up to `now`.
///
/// Used at registration so a new webhook hears about what happens next rather
/// than being answered with the whole retained oplog. Failure is logged rather
/// than propagated: the subscription is already stored, and refusing to
/// register because a progress record could not be written would be a worse
/// outcome than the replay it prevents.
pub fn seed_progress(state: &SharedState, subscription: &str, now: &VersionVector) {
    if let Err(e) = record_progress(state, subscription, now) {
        warn!(subscription, error = %e, "could not seed webhook progress; it will replay history");
    }
}

/// Record what this node has delivered.
fn record_progress(
    state: &SharedState,
    subscription: &str,
    progress: &VersionVector,
) -> Result<(), String> {
    let meta = match state.engine.get_collection(WEBHOOKS_DB, PROGRESS_COLLECTION) {
        Ok(meta) => meta,
        // `__` is reserved for internal objects, so this takes the sanctioned
        // way in, as the registry itself does.
        Err(_) => state
            .engine
            .create_system_collection(WEBHOOKS_DB, PROGRESS_COLLECTION)
            .map_err(|e| e.to_string())?,
    };

    let mut delivered = Document::new();
    for (node, hlc) in progress.iter() {
        delivered.insert(
            node.to_string(),
            bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: hlc.to_bytes().to_vec(),
            },
        );
    }

    let id = format!("{subscription}:{}", state.engine.node_id());
    let document = doc! { "_id": id.clone(), "delivered": delivered };
    state
        .engine
        .replace(&meta, &kimmy_core::DocId::String(id), document, true)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Stop a subscription whose history has been collected.
///
/// Mirrors what a lagging change-stream consumer gets: the oplog no longer
/// describes the range it needs, so there is no honest way to continue. The
/// alternative — resuming from whatever is left — would silently skip every
/// event that was collected, which is a gap the receiver could never detect.
///
/// Recorded on the subscription so it is visible in a listing rather than only
/// in a log nobody is reading.
fn invalidate(state: &SharedState, subscription: &str, reason: &str) {
    let Ok(meta) = state.engine.get_collection(WEBHOOKS_DB, WEBHOOKS_COLLECTION) else {
        return;
    };
    let key = kimmy_core::DocId::String(subscription.to_string());
    let Ok(Some(mut document)) = state.engine.get(&meta, &key) else {
        return;
    };
    document.insert("state", "invalidated");
    document.insert("invalidReason", reason.to_string());
    let _ = state.engine.replace(&meta, &key, document, false);

    warn!(
        target: "kimmy::audit",
        subscription,
        reason,
        decision = "invalidated",
        "webhook stopped: the oplog no longer holds the events it needed"
    );
}

/// Whether an entry belongs to a subscription.
fn wanted(entry: &OplogEntry, collection_id: u64, operations: &[String]) -> bool {
    if entry.collection.0 != collection_id {
        return false;
    }
    // Schema changes and violation records are not document changes; a webhook
    // asked for changes to documents.
    let name = match entry.kind {
        OpKind::Insert => "insert",
        OpKind::Update => "update",
        OpKind::Replace => "replace",
        OpKind::Delete => "delete",
        _ => return false,
    };
    operations.is_empty() || operations.iter().any(|o| o == name)
}

/// The event body a receiver gets, in the change-stream shape.
///
/// The same field names a WebSocket subscriber sees, so a consumer can move
/// between the two without relearning anything — plus the database and
/// collection names, which a WebSocket subscriber does not need (it named them
/// to connect) but a webhook receiver fed by several subscriptions does.
pub fn render(entry: &OplogEntry, database: &str, collection: &str) -> Value {
    let mut payload = render_without_document(entry, database, collection);
    if let Ok(Some(document)) = entry.document() {
        payload["fullDocument"] = crate::json::document_to_json(&document);
    }
    payload
}

/// The same event with the document left out.
///
/// What a receiver gets when one document alone exceeds the payload cap. It
/// still carries the event id, the operation and the document key, so the
/// change is not lost — only the copy of the document is, and the receiver can
/// read that itself.
fn render_without_document(entry: &OplogEntry, database: &str, collection: &str) -> Value {
    let operation = match entry.kind {
        OpKind::Insert => "insert",
        OpKind::Update => "update",
        OpKind::Replace => "replace",
        OpKind::Delete => "delete",
        _ => "unknown",
    };
    let mut payload = json!({
        "eventId": event_id(entry),
        "operationType": operation,
        "clusterTime": entry.stamp.hlc.to_string(),
        "database": database,
        "collection": collection,
    });
    if let Some(id) = &entry.doc_id {
        payload["documentKey"] = json!({ "_id": crate::json::bson_to_json(&id.to_bson()) });
    }
    payload
}

/// The body envelope, so the cap covers what actually goes on the wire rather
/// than the events alone.
fn envelope(subscription: &str, events: &[Value]) -> String {
    json!({ "subscription": subscription, "events": events }).to_string()
}

/// How many bytes the envelope costs before any event is in it.
///
/// Measured rather than assumed: the subscription id is inside it, and a
/// hand-counted constant would drift the moment the envelope gains a field.
fn envelope_overhead(subscription: &str) -> usize {
    envelope(subscription, &[]).len()
}

/// Choose the events that fit within `max_payload_bytes`, and render them.
///
/// Returns the body and the stamps it covers — the stamps rather than the
/// entries because that is all advancing progress needs.
///
/// Three rules, in order:
///
/// 1. Events are taken in oplog order and stop at the first one that would not
///    fit. The rest go next pass; ordering per subscription is preserved.
/// 2. If the *first* event does not fit on its own, it is re-rendered without
///    `fullDocument` and sent alone. An event is never dropped for being large.
/// 3. That lone event goes out even if it still does not fit, which can only
///    happen with an enormous `_id`. A subscription that refused to send it
///    would never advance past it.
fn assemble(job: &Job, entries: &[OplogEntry], max_payload_bytes: usize) -> Delivery {
    let subscription = job.id.as_str();
    let mut events: Vec<Value> = Vec::new();
    let mut stamps: Vec<Stamp> = Vec::new();
    // Accumulated rather than re-serialising the whole batch per candidate,
    // which would be quadratic in the batch size. Compact JSON adds exactly the
    // event's own text plus a comma once there is something to separate it
    // from.
    let mut used = envelope_overhead(subscription);

    for entry in entries.iter().take(BATCH) {
        let candidate = render(entry, &job.database, &job.collection);
        let cost = candidate.to_string().len() + usize::from(!events.is_empty());
        if used + cost <= max_payload_bytes {
            used += cost;
            events.push(candidate);
            stamps.push(entry.stamp);
            continue;
        }

        if events.is_empty() {
            // Rule 2: too large on its own, so the document comes off rather
            // than the event being skipped.
            let mut stripped = render_without_document(entry, &job.database, &job.collection);
            stripped["fullDocumentOmitted"] = json!(true);
            stripped["omittedReason"] =
                json!("document exceeds webhooks.max_payload_bytes; fetch it from the collection");
            events.push(stripped);
            stamps.push(entry.stamp);
            warn!(
                subscription,
                event = %event_id(entry),
                "webhook event exceeds the payload cap; delivering it without fullDocument"
            );
        }
        // Rule 1: whether this event was stripped or left for later, the batch
        // ends here.
        break;
    }

    Delivery { body: envelope(subscription, &events), stamps }
}

/// One batch, rendered and ready to send.
struct Delivery {
    body: String,
    stamps: Vec<Stamp>,
}

/// A globally unique id for one change.
///
/// The originating stamp: `(hlc, node)`. Unique without coordination, identical
/// on every node that holds the entry, and stable across redeliveries — which
/// is exactly what a receiver needs to deduplicate.
pub fn event_id(entry: &OplogEntry) -> String {
    format!("{}-{}", entry.stamp.hlc, entry.stamp.node)
}

/// `HMAC-SHA256(secret, timestamp || "." || body)`, hex.
///
/// The timestamp is inside the signature so a captured delivery cannot be
/// replayed later with a fresh one — signing the body alone would leave the
/// timestamp free to change.
pub fn sign(secret: &str, timestamp_ms: u64, body: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts a key of any length");
    mac.update(timestamp_ms.to_string().as_bytes());
    mac.update(b".");
    mac.update(body.as_bytes());
    mac.finalize().into_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

/// One subscription, as the dispatcher needs it.
struct Job {
    id: String,
    url: String,
    secret: String,
    /// The names, kept alongside the derived id: every delivered event carries
    /// them, so a receiver fed by several subscriptions can route without
    /// parsing anything out of the URL it registered.
    database: String,
    collection: String,
    collection_id: u64,
    operations: Vec<String>,
    invalidated: bool,
}

fn load_jobs(state: &SharedState) -> Vec<Job> {
    let Ok(meta) = state.engine.get_collection(WEBHOOKS_DB, WEBHOOKS_COLLECTION) else {
        return Vec::new();
    };
    let mut jobs = Vec::new();
    let _ = state.engine.for_each_doc(&meta, |_id, document| {
        let (Ok(id), Ok(url), Ok(secret), Ok(db), Ok(coll)) = (
            document.get_str("_id"),
            document.get_str("url"),
            document.get_str("secret"),
            document.get_str("database"),
            document.get_str("collection"),
        ) else {
            return Ok(true);
        };
        // Derived rather than stored, so a collection dropped and recreated
        // under the same name keeps working — ids come from the name (ADR-031).
        let collection_id = kimmy_core::ids::CollectionId::derive(db, coll).0;
        let operations = document
            .get_array("operations")
            .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
            .unwrap_or_default();
        jobs.push(Job {
            id: id.to_string(),
            url: url.to_string(),
            secret: secret.to_string(),
            database: db.to_string(),
            collection: coll.to_string(),
            collection_id,
            operations,
            invalidated: document.get_str("state").is_ok_and(|s| s == "invalidated"),
        });
        Ok(true)
    });
    jobs
}

/// A subscription with a batch ready to go out.
struct Planned {
    job: Job,
    /// The union progress as it stood, to be advanced once the batch lands.
    progress: VersionVector,
    delivery: Delivery,
    /// Events in the batch — the count `DispatchOutcome` and the metrics report.
    events: usize,
}

/// Run one dispatch pass over every subscription this node owns.
///
/// Exposed rather than buried in the loop so a test can drive a single pass
/// deterministically instead of sleeping and hoping.
///
/// Three phases, and the split is deliberate: **plan** serially, **deliver**
/// concurrently under a bound, **apply** serially. Every engine read and write
/// is in a serial phase, so two subscriptions can never race each other's
/// progress records, and only the network call — the slow part, and the part a
/// dead endpoint stalls — overlaps.
pub async fn dispatch_once(
    state: &SharedState,
    client: &reqwest::Client,
    policy: &EgressPolicy,
    me: SocketAddr,
    members: &BTreeSet<SocketAddr>,
    backoff: &mut Backoff,
    limits: Limits,
) -> DispatchOutcome {
    let mut outcome = DispatchOutcome::default();
    let mut planned: Vec<Planned> = Vec::new();

    // Gauges, gathered as the pass already walks the registry rather than
    // recomputed on every `/metrics` scrape.
    let (mut active, mut invalidated_count) = (0u64, 0u64);
    let mut backlog_ms = 0u64;
    let now_ms = kimmy_storage::physical_now_ms();

    // --- Phase 1: plan, serially -------------------------------------------
    let jobs = load_jobs(state);
    backoff.prune(&jobs.iter().map(|j| j.id.as_str()).collect());
    for job in jobs {
        if job.invalidated {
            invalidated_count += 1;
        } else {
            active += 1;
        }
        if !crate::ownership::owns(&job.id, me, members) {
            outcome.skipped_not_owner += 1;
            continue;
        }
        if job.invalidated {
            continue;
        }

        let mut progress = union_progress(state, &job.id);
        // `behind` answers "from where must I read", which is the same question
        // anti-entropy asks of a peer. `None` means the subscription's progress
        // already covers everything this node holds — it is caught up, and
        // there is nothing to read, invalidate or deliver.
        let Some(from) =
            state.engine.version_vector().ok().and_then(|current| progress.behind(&current))
        else {
            continue;
        };

        // Backoff is checked after the progress read but before the oplog scan
        // and the delivery. Reading a small system collection is not what one
        // dead endpoint must not cost the others; dialling it is.
        if !backoff.ready(&job.id) {
            // Counted as backlog even though the scan is skipped: a
            // subscription is only in backoff because a delivery carrying
            // events just failed, so it is precisely the one falling behind.
            // The resume point's age is the honest approximation available
            // without reading the oplog.
            backlog_ms = backlog_ms.max(now_ms.saturating_sub(from.wall_ms));
            outcome.skipped_backoff += 1;
            continue;
        }

        // The events this subscription still needs may have been collected.
        // `from` is where it must resume; if that is behind the retention
        // horizon, the range no longer exists anywhere.
        if let Ok(horizon) = state.engine.oplog_collected_through()
            && horizon > Hlc::ZERO
            && from < horizon
        {
            invalidate(
                state,
                &job.id,
                "delivery fell behind storage.oplog_retention_secs; the events it had not \
                 delivered have been collected",
            );
            outcome.invalidated += 1;
            continue;
        }

        let Ok(scanned) = state.engine.entries_for_peer(from, BATCH * 4) else {
            continue;
        };
        let batch: Vec<OplogEntry> = scanned
            .iter()
            .filter(|e| wanted(e, job.collection_id, &job.operations))
            .filter(|e| progress.get(e.stamp.node) < e.stamp.hlc)
            .take(BATCH)
            .cloned()
            .collect();

        if batch.is_empty() {
            // Nothing here was this subscription's — every entry belonged to
            // another collection or another operation. Progress still advances
            // over them, because deciding an entry is not yours *is* the work,
            // and a position that never moves is one the retention horizon
            // eventually overtakes. A webhook on a quiet collection in a busy
            // database would otherwise be invalidated for falling behind events
            // it was never going to be sent.
            //
            // Safe by construction: `scanned` is contiguous from `from`, and
            // nothing in it matched `wanted`, so nothing deliverable is being
            // stepped over.
            //
            // Written forward on a heartbeat rather than every pass. Recording
            // progress is itself a write, so it appends the very entry the next
            // pass reads; doing it every tick would have an idle node writing
            // to the oplog — and replicating it — every two seconds forever.
            // Once a minute is far more often than retention needs and rare
            // enough to cost nothing. See `Limits::DEFAULT_PROGRESS_HEARTBEAT`.
            let stale =
                now_ms.saturating_sub(from.wall_ms) >= limits.progress_heartbeat.as_millis() as u64;
            if stale {
                for entry in &scanned {
                    progress.observe(entry.stamp);
                }
                if let Err(e) = record_progress(state, &job.id, &progress) {
                    warn!(subscription = %job.id, error = %e, "could not record webhook progress");
                }
            }
            continue;
        }

        // Backlog is the age of the oldest event this subscription has **not
        // delivered**, and it is only measured here — where there demonstrably
        // is one. Deriving it from the resume point instead would have an idle,
        // fully caught-up subscription report a backlog that grows with the
        // clock, which is an alert firing for a webhook that is working.
        if let Some(oldest) = batch.first() {
            backlog_ms = backlog_ms.max(now_ms.saturating_sub(oldest.stamp.hlc.wall_ms));
        }

        let delivery = assemble(&job, &batch, limits.max_payload_bytes);
        let events = delivery.stamps.len();
        planned.push(Planned { job, progress, delivery, events });
    }

    state.metrics.set_webhook_gauges(active, invalidated_count, backlog_ms / 1_000);

    // --- Phase 2: deliver, concurrently under a bound ----------------------
    //
    // `max(1)` because a semaphore of zero permits never wakes. The config
    // refuses zero at startup; this makes a hand-built `Limits` in a test
    // deliver slowly rather than hang forever.
    let permits = Arc::new(tokio::sync::Semaphore::new(limits.max_concurrent_deliveries.max(1)));
    let results = futures::future::join_all(planned.iter().map(|plan| {
        let permits = Arc::clone(&permits);
        async move {
            // Held for the request only. A subscription waiting for a permit is
            // not holding one.
            let _permit = permits.acquire().await;
            deliver(client, policy, &plan.job, &plan.delivery).await
        }
    }))
    .await;

    // --- Phase 3: apply, serially ------------------------------------------
    for (mut plan, result) in planned.into_iter().zip(results) {
        match result {
            Ok(()) => {
                for stamp in &plan.delivery.stamps {
                    plan.progress.observe(*stamp);
                }
                // Recorded only after the endpoint accepted it. Recording first
                // would turn a failed delivery into a silently skipped event.
                if let Err(e) = record_progress(state, &plan.job.id, &plan.progress) {
                    warn!(subscription = %plan.job.id, error = %e, "could not record webhook progress");
                }
                backoff.succeeded(&plan.job.id);
                state.metrics.record_webhook_delivery(true, plan.events);
                outcome.delivered += plan.events;
                debug!(subscription = %plan.job.id, events = plan.events, "delivered");
            }
            Err(e) => {
                backoff.failed(&plan.job.id);
                state.metrics.record_webhook_delivery(false, plan.events);
                outcome.failed += 1;
                warn!(
                    subscription = %plan.job.id,
                    url = %plan.job.url,
                    error = %e,
                    attempts = backoff.failure_count(&plan.job.id),
                    "webhook delivery failed"
                );
            }
        }
    }
    outcome
}

async fn deliver(
    client: &reqwest::Client,
    policy: &EgressPolicy,
    job: &Job,
    delivery: &Delivery,
) -> Result<(), String> {
    // Re-checked here, not just at registration: a name that resolved publicly
    // then can resolve inward now.
    policy.check(&job.url).map_err(|e| e.to_string())?;

    // Signed at the moment of sending rather than when the batch was planned,
    // so the timestamp a receiver checks against replay is the send time.
    let timestamp = kimmy_storage::physical_now_ms();
    let signature = sign(&job.secret, timestamp, &delivery.body);
    let first =
        delivery.stamps.first().map(|s| format!("{}-{}", s.hlc, s.node)).unwrap_or_default();

    let response = client
        .post(&job.url)
        .header("content-type", "application/json")
        .header("x-kimmy-event-id", first)
        .header("x-kimmy-timestamp", timestamp.to_string())
        .header("x-kimmy-signature", signature)
        .timeout(DELIVERY_TIMEOUT)
        .body(delivery.body.clone())
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if response.status().is_success() {
        Ok(())
    } else {
        Err(format!("endpoint returned {}", response.status()))
    }
}

/// Run the dispatcher until the process ends.
pub async fn run(
    state: SharedState,
    policy: EgressPolicy,
    me: SocketAddr,
    members: Option<kimmy_cluster::Members>,
    limits: Limits,
) {
    // Redirects are refused: a permitted host answering `302` to
    // `169.254.169.254` would otherwise walk the request through the policy.
    // And the client resolves through the policy itself, so the addresses the
    // egress check approves are the addresses the connection uses — two
    // separate resolutions would give a zero-TTL name a window between them.
    let client = match reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(Arc::new(crate::egress::CheckedResolver::new(policy.clone())))
        .build()
    {
        Ok(client) => client,
        // Not `unwrap_or_default()`: a default client follows redirects and
        // resolves unchecked, so falling back to it would silently shed both
        // egress protections. No client, no deliveries — loudly.
        Err(e) => {
            tracing::error!(
                error = %e,
                "cannot build the webhook delivery client; webhooks will not be delivered"
            );
            return;
        }
    };

    info!("webhook dispatcher started");
    let mut backoff = Backoff::default();
    loop {
        // Re-read every tick rather than once: the whole point is that
        // ownership follows the live set as it changes.
        let live = members.as_ref().map(|m| m.snapshot()).unwrap_or_default();
        // The pass itself always runs on the tick. Backoff is held per
        // subscription inside it, so a failing endpoint delays only its own
        // deliveries and every other subscription keeps its cadence.
        dispatch_once(&state, &client, &policy, me, &live, &mut backoff, limits).await;
        tokio::time::sleep(TICK).await;
    }
}

#[cfg(test)]
mod tests {
    use kimmy_core::{DocId, NodeId, Stamp};

    use super::*;

    fn entry(kind: OpKind, collection: u64, ms: u64) -> OplogEntry {
        OplogEntry {
            stamp: Stamp::new(Hlc::new(ms, 0), NodeId::generate()),
            kind,
            collection: kimmy_core::ids::CollectionId(collection),
            doc_id: Some(DocId::Int64(1)),
            body: None,
        }
    }

    #[test]
    fn a_signature_covers_the_timestamp_as_well_as_the_body() {
        // Signing the body alone would leave the timestamp free to change, so a
        // captured delivery could be replayed later with a fresh one and still
        // verify.
        let a = sign("secret", 1_000, "{}");
        let b = sign("secret", 2_000, "{}");
        assert_ne!(a, b, "the timestamp must be inside the signature");

        assert_ne!(sign("secret", 1_000, "{}"), sign("other", 1_000, "{}"));
        assert_eq!(sign("secret", 1_000, "{}"), sign("secret", 1_000, "{}"));
        assert_eq!(a.len(), 64, "hex-encoded SHA-256");
    }

    #[test]
    fn the_secret_never_appears_in_a_signature() {
        let secret = "a-very-recognizable-webhook-secret";
        let signature = sign(secret, 1, "{}");
        assert!(!signature.contains(secret));
    }

    #[test]
    fn an_event_id_is_stable_and_unique() {
        // Stable across redeliveries, so a receiver deduplicating on it works;
        // unique per change, so two changes never collapse into one.
        let e = entry(OpKind::Insert, 1, 100);
        assert_eq!(event_id(&e), event_id(&e), "must be stable");
        assert_ne!(event_id(&e), event_id(&entry(OpKind::Insert, 1, 101)));
    }

    #[test]
    fn only_document_changes_are_delivered() {
        // A webhook asked for changes to documents. Schema changes carry no
        // document, and a violation record is not a change at all.
        for kind in [OpKind::CreateCollection, OpKind::DropIndex, OpKind::UniqueViolation] {
            assert!(!wanted(&entry(kind, 1, 1), 1, &[]), "{kind:?} must not be delivered");
        }
        for kind in [OpKind::Insert, OpKind::Update, OpKind::Replace, OpKind::Delete] {
            assert!(wanted(&entry(kind, 1, 1), 1, &[]), "{kind:?} should be delivered");
        }
    }

    #[test]
    fn a_subscription_only_sees_its_own_collection() {
        assert!(wanted(&entry(OpKind::Insert, 7, 1), 7, &[]));
        assert!(!wanted(&entry(OpKind::Insert, 8, 1), 7, &[]));
    }

    #[test]
    fn an_operation_filter_is_honoured_and_empty_means_all() {
        let filter = vec!["insert".to_string(), "delete".to_string()];
        assert!(wanted(&entry(OpKind::Insert, 1, 1), 1, &filter));
        assert!(wanted(&entry(OpKind::Delete, 1, 1), 1, &filter));
        assert!(!wanted(&entry(OpKind::Update, 1, 1), 1, &filter));
        // Empty is "everything", which is what someone who did not think about
        // it wants.
        assert!(wanted(&entry(OpKind::Update, 1, 1), 1, &[]));
    }

    #[test]
    fn a_rendered_event_carries_what_a_receiver_needs() {
        let rendered = render(&entry(OpKind::Insert, 1, 100), "shop", "orders").to_string();
        assert!(rendered.contains("eventId"), "{rendered}");
        assert!(rendered.contains("\"operationType\":\"insert\""), "{rendered}");
        assert!(rendered.contains("documentKey"), "{rendered}");
    }

    #[test]
    fn a_rendered_event_names_its_database_and_collection() {
        // A receiver fed by several subscriptions routes on these. They
        // shipped as empty strings for all of M6 — this is the test that was
        // missing.
        let rendered = render(&entry(OpKind::Insert, 1, 100), "shop", "orders");
        assert_eq!(rendered["database"], "shop");
        assert_eq!(rendered["collection"], "orders");
    }

    #[test]
    fn backoff_state_does_not_outlive_its_subscription() {
        // Failure state is otherwise only cleared by a successful delivery,
        // which a removed subscription can never have.
        let mut backoff = Backoff::default();
        backoff.failed("wh_kept");
        backoff.failed("wh_removed");

        backoff.prune(&std::collections::HashSet::from(["wh_kept"]));

        assert_eq!(backoff.failure_count("wh_kept"), 1, "a live subscription keeps its state");
        assert_eq!(backoff.failure_count("wh_removed"), 0, "a removed one is forgotten");
    }
}
