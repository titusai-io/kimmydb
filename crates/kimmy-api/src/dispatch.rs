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

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Duration;

use bson::{Document, doc};
use kimmy_core::{Hlc, OpKind, OplogEntry, VersionVector};
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
/// between the two without relearning anything.
pub fn render(entry: &OplogEntry) -> Value {
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
        "database": "",
        "collection": "",
    });
    if let Some(id) = &entry.doc_id {
        payload["documentKey"] = json!({ "_id": crate::json::bson_to_json(&id.to_bson()) });
    }
    if let Ok(Some(document)) = entry.document() {
        payload["fullDocument"] = crate::json::document_to_json(&document);
    }
    payload
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
            collection_id,
            operations,
            invalidated: document.get_str("state").is_ok_and(|s| s == "invalidated"),
        });
        Ok(true)
    });
    jobs
}

/// Run one dispatch pass over every subscription this node owns.
///
/// Exposed rather than buried in the loop so a test can drive a single pass
/// deterministically instead of sleeping and hoping.
pub async fn dispatch_once(
    state: &SharedState,
    client: &reqwest::Client,
    policy: &EgressPolicy,
    me: SocketAddr,
    members: &BTreeSet<SocketAddr>,
    backoff: &mut Backoff,
) -> DispatchOutcome {
    let mut outcome = DispatchOutcome::default();

    for job in load_jobs(state) {
        if !crate::ownership::owns(&job.id, me, members) {
            outcome.skipped_not_owner += 1;
            continue;
        }
        if job.invalidated {
            continue;
        }
        // Checked before any work: a subscription in backoff costs nothing,
        // and that is what keeps one dead endpoint from slowing the others.
        if !backoff.ready(&job.id) {
            outcome.skipped_backoff += 1;
            continue;
        }

        let mut progress = union_progress(state, &job.id);
        // `behind` answers "from where must I read", which is the same question
        // anti-entropy asks of a peer.
        let from = state
            .engine
            .version_vector()
            .ok()
            .and_then(|current| progress.behind(&current))
            .unwrap_or(Hlc::ZERO);

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

        let Ok(entries) = state.engine.entries_for_peer(from, BATCH * 4) else {
            continue;
        };
        let batch: Vec<OplogEntry> = entries
            .into_iter()
            .filter(|e| wanted(e, job.collection_id, &job.operations))
            .filter(|e| progress.get(e.stamp.node) < e.stamp.hlc)
            .take(BATCH)
            .collect();
        if batch.is_empty() {
            continue;
        }

        match deliver(client, policy, &job, &batch).await {
            Ok(()) => {
                for entry in &batch {
                    progress.observe(entry.stamp);
                }
                // Recorded only after the endpoint accepted it. Recording first
                // would turn a failed delivery into a silently skipped event.
                if let Err(e) = record_progress(state, &job.id, &progress) {
                    warn!(subscription = %job.id, error = %e, "could not record webhook progress");
                }
                backoff.succeeded(&job.id);
                state.metrics.record_webhook_delivery(true, batch.len());
                outcome.delivered += batch.len();
                debug!(subscription = %job.id, events = batch.len(), "delivered");
            }
            Err(e) => {
                backoff.failed(&job.id);
                state.metrics.record_webhook_delivery(false, batch.len());
                outcome.failed += 1;
                warn!(
                    subscription = %job.id,
                    url = %job.url,
                    error = %e,
                    attempts = backoff.failure_count(&job.id),
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
    batch: &[OplogEntry],
) -> Result<(), String> {
    // Re-checked here, not just at registration: a name that resolved publicly
    // then can resolve inward now.
    policy.check(&job.url).map_err(|e| e.to_string())?;

    let events: Vec<Value> = batch.iter().map(render).collect();
    let body = json!({ "subscription": job.id, "events": events }).to_string();
    let timestamp = kimmy_storage::physical_now_ms();
    let signature = sign(&job.secret, timestamp, &body);
    let first = batch.first().map(event_id).unwrap_or_default();

    let response = client
        .post(&job.url)
        .header("content-type", "application/json")
        .header("x-kimmy-event-id", first)
        .header("x-kimmy-timestamp", timestamp.to_string())
        .header("x-kimmy-signature", signature)
        .timeout(DELIVERY_TIMEOUT)
        .body(body)
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
) {
    // Redirects are refused: a permitted host answering `302` to
    // `169.254.169.254` would otherwise walk the request through the policy.
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap_or_default();

    info!("webhook dispatcher started");
    let mut backoff = Backoff::default();
    loop {
        // Re-read every tick rather than once: the whole point is that
        // ownership follows the live set as it changes.
        let live = members.as_ref().map(|m| m.snapshot()).unwrap_or_default();
        // The pass itself always runs on the tick. Backoff is held per
        // subscription inside it, so a failing endpoint delays only its own
        // deliveries and every other subscription keeps its cadence.
        dispatch_once(&state, &client, &policy, me, &live, &mut backoff).await;
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
        let rendered = render(&entry(OpKind::Insert, 1, 100)).to_string();
        assert!(rendered.contains("eventId"), "{rendered}");
        assert!(rendered.contains("\"operationType\":\"insert\""), "{rendered}");
        assert!(rendered.contains("documentKey"), "{rendered}");
    }
}
