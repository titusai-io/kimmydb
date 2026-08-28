//! Process counters behind `/metrics`.
//!
//! Plain atomics rather than a metrics framework. The endpoint renders
//! Prometheus text directly, the set of series is small and fixed, and a
//! registry would add a dependency and an abstraction to hold nine numbers.
//!
//! # What is deliberately not here
//!
//! **Per-collection series.** `/metrics` is unauthenticated, and a series per
//! collection would put the schema on it. The endpoint has always reported
//! counts rather than names for that reason.
//!
//! The two absences ADR-043 recorded are now filled, each on the terms that
//! kept it out. The latency histogram's buckets were **measured**, not
//! guessed — end-to-end against a release build, conditions in ADR-046 — and
//! replication lag is **pushed here by the replication loop**, which is the
//! only place a peer's version vector exists.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Histogram bucket upper bounds, in microseconds.
///
/// Chosen from measurement, not preference (ADR-046): end-to-end against a
/// release build, point reads (`GET /docs/{id}`) run p50 ≈ 250 µs / p99 under
/// 1 ms, filtered finds ≈ 1.4–2.6 ms, single-document inserts p50 ≈ 6 ms —
/// one durable commit each — and a 10k-document aggregation 10–43 ms. The
/// buckets bracket those clusters with headroom on both ends; the wide top
/// bucket exists so a stall shows as a shape change rather than vanishing
/// into `+Inf`.
const LATENCY_BUCKETS_US: [u64; 12] =
    [100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 1_000_000, 10_000_000];

/// Every counter as a plain value, for a reader that is not the renderer.
///
/// A value struct rather than an accessor per counter: the OTLP bridge wants
/// all of them, once, and twenty-five getters would be twenty-five things to
/// forget when a counter is added. See [`Metrics::snapshot`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub uptime_secs: u64,
    pub requests: u64,
    pub responses_2xx: u64,
    pub responses_4xx: u64,
    pub responses_5xx: u64,
    pub authz_denied: u64,
    pub auth_failures: u64,
    pub rate_limited: u64,
    pub backups: u64,
    pub ttl_expired: u64,
    pub ttl_skipped: u64,
    pub webhook_delivered: u64,
    pub webhook_failed: u64,
    pub webhook_events: u64,
    pub webhook_active: u64,
    pub webhook_invalidated: u64,
    pub webhook_backlog_secs: u64,
    pub cluster_members: u64,
    pub replication_lag_secs: u64,
    /// Worst runtime scheduling delay since the last scrape, microseconds.
    pub runtime_stall_us: u64,
    pub tls_reloads_ok: u64,
    pub tls_reloads_failed: u64,
    pub jwks_refresh_ok: u64,
    pub jwks_refresh_failed: u64,
    /// Embedding-worker counters, all zero when the worker is disabled on
    /// this node — which is exactly the distinction an operator needs.
    pub embed_documents_embedded: u64,
    pub embed_chunks_embedded: u64,
    pub embed_deferred: u64,
    pub embed_skipped_not_owned: u64,
    pub embed_failures: u64,
    /// Transport failures among `embed_failures`, in the order connect,
    /// timeout, reset, other.
    pub embed_transport: [u64; 4],
    /// Requests observed by the latency histogram — health and metrics routes
    /// excluded, so this is smaller than `requests` on any real node.
    pub latency_count: u64,
    pub latency_sum_us: u64,
}

/// Counters for one running server.
pub struct Metrics {
    started: Instant,
    latency_buckets: [AtomicU64; LATENCY_BUCKETS_US.len()],
    latency_sum_us: AtomicU64,
    latency_count: AtomicU64,
    replication_lag_secs: AtomicU64,
    /// The worst scheduling delay the runtime probe saw since the last
    /// scrape, in microseconds. A worker that blocks on a storage commit
    /// shows up here before it shows up as a peer's handshake timeout.
    runtime_stall_us: AtomicU64,
    requests: AtomicU64,
    responses_2xx: AtomicU64,
    responses_4xx: AtomicU64,
    responses_5xx: AtomicU64,
    authz_denied: AtomicU64,
    auth_failures: AtomicU64,
    rate_limited: AtomicU64,
    backups: AtomicU64,
    webhook_delivered: AtomicU64,
    webhook_failed: AtomicU64,
    webhook_events: AtomicU64,
    webhook_active: AtomicU64,
    webhook_invalidated: AtomicU64,
    webhook_backlog_secs: AtomicU64,
    cluster_members: AtomicU64,
    tls_reloads_ok: AtomicU64,
    tls_reloads_failed: AtomicU64,
    jwks_refresh_ok: AtomicU64,
    jwks_refresh_failed: AtomicU64,
    ttl_expired: AtomicU64,
    ttl_skipped: AtomicU64,
    /// Set once at startup when the embedding worker runs. `None` — the
    /// renderer then reports zeros — means this node has
    /// `[vector] worker_enabled = false`, which an operator must be able to
    /// distinguish from "worker enabled but idle".
    vector_counters: OnceLock<std::sync::Arc<kimmy_vector::WorkerCounters>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            latency_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_sum_us: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            replication_lag_secs: AtomicU64::new(0),
            runtime_stall_us: AtomicU64::new(0),
            requests: AtomicU64::new(0),
            responses_2xx: AtomicU64::new(0),
            responses_4xx: AtomicU64::new(0),
            responses_5xx: AtomicU64::new(0),
            authz_denied: AtomicU64::new(0),
            auth_failures: AtomicU64::new(0),
            rate_limited: AtomicU64::new(0),
            backups: AtomicU64::new(0),
            webhook_delivered: AtomicU64::new(0),
            webhook_failed: AtomicU64::new(0),
            webhook_events: AtomicU64::new(0),
            ttl_expired: AtomicU64::new(0),
            ttl_skipped: AtomicU64::new(0),
            webhook_active: AtomicU64::new(0),
            webhook_invalidated: AtomicU64::new(0),
            webhook_backlog_secs: AtomicU64::new(0),
            cluster_members: AtomicU64::new(0),
            tls_reloads_ok: AtomicU64::new(0),
            tls_reloads_failed: AtomicU64::new(0),
            jwks_refresh_ok: AtomicU64::new(0),
            jwks_refresh_failed: AtomicU64::new(0),
            vector_counters: OnceLock::new(),
        }
    }
}

impl Metrics {
    /// Share the worker's counters with this renderer. Called once at node
    /// startup, before the worker task is spawned; later calls are ignored,
    /// because a renderer that switched handles mid-flight would report two
    /// partial series where one total was meant.
    pub fn set_vector_counters(&self, counters: std::sync::Arc<kimmy_vector::WorkerCounters>) {
        let _ = self.vector_counters.set(counters);
    }
    /// Count one finished request.
    ///
    /// The three specific counters are derived from the status rather than
    /// incremented where the refusal happens, because each of those statuses
    /// has exactly one source: 401 from token or credential rejection, 403 from
    /// `ApiError::forbidden` (RBAC and nothing else), 429 from the rate
    /// limiter. Deriving them here keeps the counting in one place instead of
    /// threading a metrics handle into the authorization path — and a counter
    /// that lives beside the check is a counter someone forgets to bump when
    /// they add a route.
    pub fn record_request(&self, status: u16) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        match status {
            200..=299 => &self.responses_2xx,
            400..=499 => &self.responses_4xx,
            500..=599 => &self.responses_5xx,
            // 1xx and 3xx are counted in the total and nowhere else; neither is
            // a success or a failure worth its own series here.
            _ => return,
        }
        .fetch_add(1, Ordering::Relaxed);

        match status {
            401 => self.auth_failures.fetch_add(1, Ordering::Relaxed),
            403 => self.authz_denied.fetch_add(1, Ordering::Relaxed),
            429 => self.rate_limited.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
    }

    /// One delivery attempt, and how many events it carried.
    ///
    /// Batches, not events, are counted as the outcome: a retried batch is one
    /// failure, and counting per event would make one dead endpoint look like
    /// thousands of separate problems.
    pub fn record_webhook_delivery(&self, succeeded: bool, events: usize) {
        if succeeded {
            self.webhook_delivered.fetch_add(1, Ordering::Relaxed);
            self.webhook_events.fetch_add(events as u64, Ordering::Relaxed);
        } else {
            self.webhook_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The webhook gauges, set by the dispatcher at the end of each pass.
    ///
    /// Set rather than accumulated: all three describe a state at an instant,
    /// and the dispatcher already computes them while walking the registry. The
    /// alternative — recomputing on every `/metrics` scrape — would re-read the
    /// progress collection once per subscription for a number the dispatcher
    /// had in hand two seconds earlier.
    ///
    /// `backlog_secs` covers only subscriptions **this node owns**. A node that
    /// has stood down must not report a backlog it is not the one working
    /// through, or every node in a cluster would alert for the same lag.
    pub fn set_webhook_gauges(&self, active: u64, invalidated: u64, backlog_secs: u64) {
        self.webhook_active.store(active, Ordering::Relaxed);
        self.webhook_invalidated.store(invalidated, Ordering::Relaxed);
        self.webhook_backlog_secs.store(backlog_secs, Ordering::Relaxed);
    }

    /// Record one request's end-to-end latency.
    ///
    /// Non-cumulative per bucket; the render accumulates, because Prometheus
    /// buckets are cumulative on the wire but a store-time increment of every
    /// bucket ≥ the observation would be N writes for one sample.
    pub fn record_latency(&self, elapsed: std::time::Duration) {
        let micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        let slot = LATENCY_BUCKETS_US.iter().position(|&upper| micros <= upper);
        if let Some(slot) = slot {
            self.latency_buckets[slot].fetch_add(1, Ordering::Relaxed);
        }
        // Above every bound: only `+Inf` (derived from the count) sees it.
        self.latency_sum_us.fetch_add(micros, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Seconds of peer oplog history this node has not yet applied.
    ///
    /// Pushed by the replication loop after each round, because that is the
    /// only place a peer's version vector exists — the reason ADR-043 left
    /// this out rather than guessing. Zero when caught up; measured from the
    /// entries' own timestamps, so it is the age of undelivered work, not the
    /// age of a cursor.
    pub fn set_replication_lag_secs(&self, secs: u64) {
        self.replication_lag_secs.store(secs, Ordering::Relaxed);
    }

    /// How many peers this node's SWIM instance currently considers alive.
    ///
    /// The observable the cluster harness asserts gossip *formed* with —
    /// replication converging is not proof, because discovery alone can
    /// deliver convergence while gossip silently never forms, which is
    /// exactly what the shipped compose file once did.
    pub fn set_cluster_members(&self, n: u64) {
        self.cluster_members.store(n, Ordering::Relaxed);
    }

    pub fn record_backup(&self) {
        self.backups.fetch_add(1, Ordering::Relaxed);
    }

    /// One collection's expiry pass.
    ///
    /// `expired` is what makes the ownership choice checkable: expiry is owned
    /// by one node per collection so a document produces **one** delete
    /// cluster-wide, and summing this counter across a cluster is how that
    /// stops being an assertion nobody measures. `skipped` counts candidates
    /// refused because the document was refreshed between the scan and the
    /// write — a steady rise there means TTLs are being reset as fast as the
    /// pass finds them.
    pub fn record_expiry(&self, expired: u64, skipped: u64) {
        self.ttl_expired.fetch_add(expired, Ordering::Relaxed);
        self.ttl_skipped.fetch_add(skipped, Ordering::Relaxed);
    }

    /// Count one certificate reload attempt.
    ///
    /// A failed reload is the quiet failure this metric exists for: the node
    /// keeps serving the certificate it already had, perfectly, until that one
    /// expires and every client drops at once. Nothing about a working request
    /// reveals that the renewal never took (ADR-049).
    pub fn record_tls_reload(&self, succeeded: bool) {
        let counter = if succeeded { &self.tls_reloads_ok } else { &self.tls_reloads_failed };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Count one attempt to refresh the identity provider's signing keys.
    ///
    /// The same quiet failure as a certificate reload, one step worse: a node
    /// that cannot reach its provider keeps verifying tokens perfectly against
    /// the keys it already has, until the provider rotates and every federated
    /// caller is refused at once. Nothing about a working request reveals that
    /// the refresh has been failing for a day (ADR-064).
    pub fn record_jwks_refresh(&self, succeeded: bool) {
        let counter = if succeeded { &self.jwks_refresh_ok } else { &self.jwks_refresh_failed };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn uptime_secs(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    /// Record how late the runtime probe woke up. Keeps the maximum until the
    /// next scrape reads it, so a one-off stall between scrapes is not lost.
    pub fn record_runtime_stall(&self, late: std::time::Duration) {
        let us = u64::try_from(late.as_micros()).unwrap_or(u64::MAX);
        self.runtime_stall_us.fetch_max(us, Ordering::Relaxed);
    }

    /// The worst runtime stall since the last call, in seconds.
    fn take_runtime_stall_secs(&self) -> f64 {
        self.runtime_stall_us.swap(0, Ordering::Relaxed) as f64 / 1_000_000.0
    }

    fn get(&self, counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    /// Every counter, read once, as plain numbers.
    ///
    /// The read surface the OTLP bridge uses (ADR-070). Counters are
    /// **bridged, not duplicated**: an observable instrument's callback reads
    /// this and reports it, so `/metrics` and a collector are two renderings of
    /// one set of atomics rather than two sets that can drift. A second set is
    /// the failure this avoids — a Prometheus dashboard and a trace backend
    /// disagreeing about how many requests a node served, with nothing to say
    /// which is right.
    ///
    /// Not a consistent snapshot, and it does not need to be: each field is a
    /// relaxed load, so a value may be one increment behind a sibling. That is
    /// already true of `render`, and of any counter read without a lock.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            uptime_secs: self.uptime_secs(),
            requests: self.get(&self.requests),
            responses_2xx: self.get(&self.responses_2xx),
            responses_4xx: self.get(&self.responses_4xx),
            responses_5xx: self.get(&self.responses_5xx),
            authz_denied: self.get(&self.authz_denied),
            auth_failures: self.get(&self.auth_failures),
            rate_limited: self.get(&self.rate_limited),
            backups: self.get(&self.backups),
            ttl_expired: self.get(&self.ttl_expired),
            ttl_skipped: self.get(&self.ttl_skipped),
            webhook_delivered: self.get(&self.webhook_delivered),
            webhook_failed: self.get(&self.webhook_failed),
            webhook_events: self.get(&self.webhook_events),
            webhook_active: self.get(&self.webhook_active),
            webhook_invalidated: self.get(&self.webhook_invalidated),
            webhook_backlog_secs: self.get(&self.webhook_backlog_secs),
            cluster_members: self.get(&self.cluster_members),
            replication_lag_secs: self.get(&self.replication_lag_secs),
            runtime_stall_us: self.get(&self.runtime_stall_us),
            tls_reloads_ok: self.get(&self.tls_reloads_ok),
            tls_reloads_failed: self.get(&self.tls_reloads_failed),
            jwks_refresh_ok: self.get(&self.jwks_refresh_ok),
            jwks_refresh_failed: self.get(&self.jwks_refresh_failed),
            latency_count: self.get(&self.latency_count),
            latency_sum_us: self.get(&self.latency_sum_us),
            embed_documents_embedded: self
                .vector_counters
                .get()
                .map_or(0, |c| c.documents_embedded.load(Ordering::Relaxed)),
            embed_chunks_embedded: self
                .vector_counters
                .get()
                .map_or(0, |c| c.chunks_embedded.load(Ordering::Relaxed)),
            embed_deferred: self
                .vector_counters
                .get()
                .map_or(0, |c| c.deferred.load(Ordering::Relaxed)),
            embed_skipped_not_owned: self
                .vector_counters
                .get()
                .map_or(0, |c| c.skipped_not_owned.load(Ordering::Relaxed)),
            embed_failures: self
                .vector_counters
                .get()
                .map_or(0, |c| c.failures.load(Ordering::Relaxed)),
            embed_transport: self.vector_counters.get().map_or([0; 4], |c| {
                kimmy_vector::TransportKind::ALL.map(|k| c.transport_failures(k))
            }),
        }
    }

    /// Render the process counters in Prometheus text format.
    ///
    /// The storage gauges are rendered by the caller, which has the engine;
    /// keeping them apart avoids giving this type a database handle purely to
    /// print two numbers.
    pub fn render(&self) -> String {
        // Read once: the worker's atomics move as it runs, and a render that
        // straddled an increment would show mismatched document/chunk pairs.
        let vc = self.vector_counters.get();
        let (embed_docs, embed_chunks, embed_deferred, embed_not_owned, embed_failures, transport) =
            match vc {
                Some(c) => (
                    c.documents_embedded.load(Ordering::Relaxed),
                    c.chunks_embedded.load(Ordering::Relaxed),
                    c.deferred.load(Ordering::Relaxed),
                    c.skipped_not_owned.load(Ordering::Relaxed),
                    c.failures.load(Ordering::Relaxed),
                    kimmy_vector::TransportKind::ALL.map(|k| c.transport_failures(k)),
                ),
                None => (0, 0, 0, 0, 0, [0; 4]),
            };
        let [t_connect, t_timeout, t_reset, t_other] = transport;
        let mut out = format!(
            "# HELP kimmy_uptime_seconds Seconds since this process started serving.\n\
             # TYPE kimmy_uptime_seconds gauge\n\
             kimmy_uptime_seconds {uptime}\n\
             # HELP kimmy_runtime_stall_seconds Worst delay a 250 ms timer on the async runtime saw since the last scrape. Above a few tens of milliseconds, something blocked a worker thread - the storage lock or an fsync - and peers may have marked this node down.\n\
             # TYPE kimmy_runtime_stall_seconds gauge\n\
             kimmy_runtime_stall_seconds {stall}\n\
             # HELP kimmy_requests_total HTTP requests handled.\n\
             # TYPE kimmy_requests_total counter\n\
             kimmy_requests_total {requests}\n\
             # HELP kimmy_responses_total HTTP responses by status class.\n\
             # TYPE kimmy_responses_total counter\n\
             kimmy_responses_total{{class=\"2xx\"}} {ok}\n\
             kimmy_responses_total{{class=\"4xx\"}} {client}\n\
             kimmy_responses_total{{class=\"5xx\"}} {server}\n\
             # HELP kimmy_authz_denied_total Operations refused by RBAC.\n\
             # TYPE kimmy_authz_denied_total counter\n\
             kimmy_authz_denied_total {denied}\n\
             # HELP kimmy_auth_failures_total Rejected credentials and tokens.\n\
             # TYPE kimmy_auth_failures_total counter\n\
             kimmy_auth_failures_total {auth}\n\
             # HELP kimmy_rate_limited_total Requests refused by a rate limit.\n\
             # TYPE kimmy_rate_limited_total counter\n\
             kimmy_rate_limited_total {limited}\n\
             # HELP kimmy_backups_total Backups served.\n\
             # TYPE kimmy_backups_total counter\n\
             kimmy_backups_total {backups}\n\
             # HELP kimmy_ttl_expired_total Documents deleted by a TTL index.\n\
             # TYPE kimmy_ttl_expired_total counter\n\
             kimmy_ttl_expired_total {ttl_expired}\n\
             # HELP kimmy_ttl_skipped_total Expiry candidates refused because the document was refreshed before the delete.\n\
             # TYPE kimmy_ttl_skipped_total counter\n\
             kimmy_ttl_skipped_total {ttl_skipped}\n\
             # HELP kimmy_webhook_deliveries_total Webhook delivery attempts by outcome.\n\
             # TYPE kimmy_webhook_deliveries_total counter\n\
             kimmy_webhook_deliveries_total{{outcome=\"delivered\"}} {wh_ok}\n\
             kimmy_webhook_deliveries_total{{outcome=\"failed\"}} {wh_fail}\n\
             # HELP kimmy_webhook_events_total Change events pushed to endpoints.\n\
             # TYPE kimmy_webhook_events_total counter\n\
             kimmy_webhook_events_total {wh_events}\n\
             # HELP kimmy_webhook_subscriptions Registered subscriptions, as this node sees the registry.\n\
             # TYPE kimmy_webhook_subscriptions gauge\n\
             kimmy_webhook_subscriptions{{state=\"active\"}} {wh_active}\n\
             kimmy_webhook_subscriptions{{state=\"invalidated\"}} {wh_invalid}\n\
             # HELP kimmy_webhook_backlog_seconds Age of the oldest undelivered event, across subscriptions this node owns.\n\
             # TYPE kimmy_webhook_backlog_seconds gauge\n\
             kimmy_webhook_backlog_seconds {wh_backlog}\n\
             # HELP kimmy_cluster_members Peers this node's SWIM membership currently considers alive. 0 with clustering off.\n\
             # TYPE kimmy_cluster_members gauge\n\
             kimmy_cluster_members {cluster}\n\
             # HELP kimmy_replication_lag_seconds Seconds of peer oplog history not yet applied locally, max over peers in the last sync round. 0 when caught up or clustering is off.\n\
             # TYPE kimmy_replication_lag_seconds gauge\n\
             kimmy_replication_lag_seconds {lag}\n\
             # HELP kimmy_tls_reloads_total Certificate reload attempts by outcome. A failed reload leaves the certificate already in use serving.\n\
             # TYPE kimmy_tls_reloads_total counter\n\
             kimmy_tls_reloads_total{{outcome=\"ok\"}} {tls_ok}\n\
             kimmy_tls_reloads_total{{outcome=\"failed\"}} {tls_fail}\n\
             # HELP kimmy_jwks_refresh_total Attempts to refresh the OIDC provider's signing keys, by outcome. A failed refresh leaves the key set already in use verifying.\n\
             # TYPE kimmy_jwks_refresh_total counter\n\
             kimmy_jwks_refresh_total{{outcome=\"ok\"}} {jwks_ok}\n\
             kimmy_jwks_refresh_total{{outcome=\"failed\"}} {jwks_fail}\n\
             # HELP kimmy_embed_documents_total Documents whose vectors this node wrote.\n\
             # TYPE kimmy_embed_documents_total counter\n\
             kimmy_embed_documents_total {embed_docs}\n\
             # HELP kimmy_embed_chunks_total Provider inputs embedded - the closest proxy for provider spend.\n\
             # TYPE kimmy_embed_chunks_total counter\n\
             kimmy_embed_chunks_total {embed_chunks}\n\
             # HELP kimmy_embed_deferred_total Foreign-written documents held for a later re-check.\n\
             # TYPE kimmy_embed_deferred_total counter\n\
             kimmy_embed_deferred_total {embed_deferred}\n\
             # HELP kimmy_embed_skipped_not_owned_total Documents dropped un-embedded because another node owns embedding - the duplicate provider calls this counts replacing is the 3x amplification measured in the August 2026 load test.\n\
             # TYPE kimmy_embed_skipped_not_owned_total counter\n\
             kimmy_embed_skipped_not_owned_total {embed_not_owned}\n\
             # HELP kimmy_embed_failures_total Failed provider calls, including each retry. Climbing while embed_documents stays flat is a provider outage.\n\
             # TYPE kimmy_embed_failures_total counter\n\
             kimmy_embed_failures_total {embed_failures}\n\
             # HELP kimmy_embed_provider_errors_total Provider calls that failed before a response, by what failed: connect (DNS, TCP, TLS), timeout, reset (the far side closed an open connection), other.\n\
             # TYPE kimmy_embed_provider_errors_total counter\n\
             kimmy_embed_provider_errors_total{{kind=\"connect\"}} {t_connect}\n\
             kimmy_embed_provider_errors_total{{kind=\"timeout\"}} {t_timeout}\n\
             kimmy_embed_provider_errors_total{{kind=\"reset\"}} {t_reset}\n\
             kimmy_embed_provider_errors_total{{kind=\"other\"}} {t_other}\n",
            uptime = self.uptime_secs(),
            stall = self.take_runtime_stall_secs(),
            requests = self.get(&self.requests),
            ok = self.get(&self.responses_2xx),
            client = self.get(&self.responses_4xx),
            server = self.get(&self.responses_5xx),
            denied = self.get(&self.authz_denied),
            auth = self.get(&self.auth_failures),
            limited = self.get(&self.rate_limited),
            backups = self.get(&self.backups),
            wh_ok = self.get(&self.webhook_delivered),
            wh_fail = self.get(&self.webhook_failed),
            wh_events = self.get(&self.webhook_events),
            ttl_expired = self.get(&self.ttl_expired),
            ttl_skipped = self.get(&self.ttl_skipped),
            wh_active = self.get(&self.webhook_active),
            wh_invalid = self.get(&self.webhook_invalidated),
            wh_backlog = self.get(&self.webhook_backlog_secs),
            cluster = self.get(&self.cluster_members),
            lag = self.get(&self.replication_lag_secs),
            tls_ok = self.get(&self.tls_reloads_ok),
            tls_fail = self.get(&self.tls_reloads_failed),
            embed_docs = embed_docs,
            embed_chunks = embed_chunks,
            embed_deferred = embed_deferred,
            embed_not_owned = embed_not_owned,
            embed_failures = embed_failures,
            jwks_ok = self.get(&self.jwks_refresh_ok),
            jwks_fail = self.get(&self.jwks_refresh_failed),
        );
        self.render_latency(&mut out);
        out
    }

    /// The latency histogram, in Prometheus's cumulative-bucket form.
    ///
    /// Buckets are stored non-cumulative and summed here, and the `le` labels
    /// are the microsecond bounds converted to seconds — `f64` prints `0.0001`
    /// and `10` exactly for every bound in the table, so the labels stay
    /// stable strings rather than formatting artifacts.
    fn render_latency(&self, out: &mut String) {
        use std::fmt::Write;

        out.push_str(
            "# HELP kimmy_request_duration_seconds End-to-end request latency. Health and \
             metrics routes are excluded, so scrapes do not crowd the buckets the real \
             traffic lands in.\n\
             # TYPE kimmy_request_duration_seconds histogram\n",
        );
        let mut cumulative = 0u64;
        for (slot, upper) in LATENCY_BUCKETS_US.iter().enumerate() {
            cumulative += self.get(&self.latency_buckets[slot]);
            let le = *upper as f64 / 1e6;
            let _ =
                writeln!(out, "kimmy_request_duration_seconds_bucket{{le=\"{le}\"}} {cumulative}");
        }
        let count = self.get(&self.latency_count);
        let sum = self.get(&self.latency_sum_us) as f64 / 1e6;
        let _ = writeln!(out, "kimmy_request_duration_seconds_bucket{{le=\"+Inf\"}} {count}");
        let _ = writeln!(out, "kimmy_request_duration_seconds_sum {sum}");
        let _ = writeln!(out, "kimmy_request_duration_seconds_count {count}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One instance with every counter at a value nothing else has.
    ///
    /// Distinct on purpose: with several counters sharing a value, a render
    /// that printed the wrong one would still match.
    fn every_counter_distinct() -> Metrics {
        use std::time::Duration;

        let m = Metrics::default();
        // 2 × 2xx, 3 × 4xx (one each of 401/403/429), 4 × 5xx, and one 304
        // that lands in the total alone: ten requests, no two classes equal.
        for _ in 0..2 {
            m.record_request(200);
        }
        m.record_request(401);
        m.record_request(403);
        m.record_request(429);
        for _ in 0..4 {
            m.record_request(500);
        }
        m.record_request(304);

        m.record_backup();
        m.record_expiry(11, 12);
        m.record_webhook_delivery(true, 13);
        m.record_webhook_delivery(true, 14);
        m.record_webhook_delivery(false, 0);
        m.set_webhook_gauges(15, 16, 17);
        m.set_cluster_members(18);
        m.set_replication_lag_secs(19);
        for _ in 0..20 {
            m.record_tls_reload(true);
        }
        m.record_tls_reload(false);
        for _ in 0..22 {
            m.record_jwks_refresh(true);
        }
        m.record_jwks_refresh(false);

        // One observation in three different buckets, so the cumulative sum is
        // visible in the golden text rather than being three copies of 1.
        m.record_latency(Duration::from_micros(90));
        m.record_latency(Duration::from_micros(400));
        m.record_latency(Duration::from_millis(30));
        m
    }

    /// **Production clusters scrape this endpoint. Any diff is a
    /// regression** — a renamed series is a dashboard that goes blank and an
    /// alert that stops firing, and neither announces itself.
    ///
    /// A whole-string comparison rather than a set of `contains` assertions,
    /// because the failure this guards against is the one `contains` cannot
    /// see: a series *added*, a HELP line reworded, a blank line appearing
    /// between two samples. `render` is fully deterministic in a test —
    /// `uptime_secs` is 0 on a fresh instance and nothing else reads a clock —
    /// so there is no reason to check it loosely.
    ///
    /// If this fails because you meant to change the output, read the diff as
    /// the release note it is: every line here is something a scrape config or
    /// a dashboard may name.
    #[test]
    fn the_render_is_byte_for_byte_what_a_scrape_receives() {
        let expected = "\
# HELP kimmy_uptime_seconds Seconds since this process started serving.
# TYPE kimmy_uptime_seconds gauge
kimmy_uptime_seconds 0
# HELP kimmy_runtime_stall_seconds Worst delay a 250 ms timer on the async runtime saw since the last scrape. Above a few tens of milliseconds, something blocked a worker thread - the storage lock or an fsync - and peers may have marked this node down.
# TYPE kimmy_runtime_stall_seconds gauge
kimmy_runtime_stall_seconds 0
# HELP kimmy_requests_total HTTP requests handled.
# TYPE kimmy_requests_total counter
kimmy_requests_total 10
# HELP kimmy_responses_total HTTP responses by status class.
# TYPE kimmy_responses_total counter
kimmy_responses_total{class=\"2xx\"} 2
kimmy_responses_total{class=\"4xx\"} 3
kimmy_responses_total{class=\"5xx\"} 4
# HELP kimmy_authz_denied_total Operations refused by RBAC.
# TYPE kimmy_authz_denied_total counter
kimmy_authz_denied_total 1
# HELP kimmy_auth_failures_total Rejected credentials and tokens.
# TYPE kimmy_auth_failures_total counter
kimmy_auth_failures_total 1
# HELP kimmy_rate_limited_total Requests refused by a rate limit.
# TYPE kimmy_rate_limited_total counter
kimmy_rate_limited_total 1
# HELP kimmy_backups_total Backups served.
# TYPE kimmy_backups_total counter
kimmy_backups_total 1
# HELP kimmy_ttl_expired_total Documents deleted by a TTL index.
# TYPE kimmy_ttl_expired_total counter
kimmy_ttl_expired_total 11
# HELP kimmy_ttl_skipped_total Expiry candidates refused because the document was refreshed before the delete.
# TYPE kimmy_ttl_skipped_total counter
kimmy_ttl_skipped_total 12
# HELP kimmy_webhook_deliveries_total Webhook delivery attempts by outcome.
# TYPE kimmy_webhook_deliveries_total counter
kimmy_webhook_deliveries_total{outcome=\"delivered\"} 2
kimmy_webhook_deliveries_total{outcome=\"failed\"} 1
# HELP kimmy_webhook_events_total Change events pushed to endpoints.
# TYPE kimmy_webhook_events_total counter
kimmy_webhook_events_total 27
# HELP kimmy_webhook_subscriptions Registered subscriptions, as this node sees the registry.
# TYPE kimmy_webhook_subscriptions gauge
kimmy_webhook_subscriptions{state=\"active\"} 15
kimmy_webhook_subscriptions{state=\"invalidated\"} 16
# HELP kimmy_webhook_backlog_seconds Age of the oldest undelivered event, across subscriptions this node owns.
# TYPE kimmy_webhook_backlog_seconds gauge
kimmy_webhook_backlog_seconds 17
# HELP kimmy_cluster_members Peers this node's SWIM membership currently considers alive. 0 with clustering off.
# TYPE kimmy_cluster_members gauge
kimmy_cluster_members 18
# HELP kimmy_replication_lag_seconds Seconds of peer oplog history not yet applied locally, max over peers in the last sync round. 0 when caught up or clustering is off.
# TYPE kimmy_replication_lag_seconds gauge
kimmy_replication_lag_seconds 19
# HELP kimmy_tls_reloads_total Certificate reload attempts by outcome. A failed reload leaves the certificate already in use serving.
# TYPE kimmy_tls_reloads_total counter
kimmy_tls_reloads_total{outcome=\"ok\"} 20
kimmy_tls_reloads_total{outcome=\"failed\"} 1
# HELP kimmy_jwks_refresh_total Attempts to refresh the OIDC provider's signing keys, by outcome. A failed refresh leaves the key set already in use verifying.
# TYPE kimmy_jwks_refresh_total counter
kimmy_jwks_refresh_total{outcome=\"ok\"} 22
kimmy_jwks_refresh_total{outcome=\"failed\"} 1
# HELP kimmy_embed_documents_total Documents whose vectors this node wrote.
# TYPE kimmy_embed_documents_total counter
kimmy_embed_documents_total 0
# HELP kimmy_embed_chunks_total Provider inputs embedded - the closest proxy for provider spend.
# TYPE kimmy_embed_chunks_total counter
kimmy_embed_chunks_total 0
# HELP kimmy_embed_deferred_total Foreign-written documents held for a later re-check.
# TYPE kimmy_embed_deferred_total counter
kimmy_embed_deferred_total 0
# HELP kimmy_embed_skipped_not_owned_total Documents dropped un-embedded because another node owns embedding - the duplicate provider calls this counts replacing is the 3x amplification measured in the August 2026 load test.
# TYPE kimmy_embed_skipped_not_owned_total counter
kimmy_embed_skipped_not_owned_total 0
# HELP kimmy_embed_failures_total Failed provider calls, including each retry. Climbing while embed_documents stays flat is a provider outage.
# TYPE kimmy_embed_failures_total counter
kimmy_embed_failures_total 0
# HELP kimmy_embed_provider_errors_total Provider calls that failed before a response, by what failed: connect (DNS, TCP, TLS), timeout, reset (the far side closed an open connection), other.
# TYPE kimmy_embed_provider_errors_total counter
kimmy_embed_provider_errors_total{kind=\"connect\"} 0
kimmy_embed_provider_errors_total{kind=\"timeout\"} 0
kimmy_embed_provider_errors_total{kind=\"reset\"} 0
kimmy_embed_provider_errors_total{kind=\"other\"} 0
# HELP kimmy_request_duration_seconds End-to-end request latency. Health and metrics routes are excluded, so scrapes do not crowd the buckets the real traffic lands in.
# TYPE kimmy_request_duration_seconds histogram
kimmy_request_duration_seconds_bucket{le=\"0.0001\"} 1
kimmy_request_duration_seconds_bucket{le=\"0.00025\"} 1
kimmy_request_duration_seconds_bucket{le=\"0.0005\"} 2
kimmy_request_duration_seconds_bucket{le=\"0.001\"} 2
kimmy_request_duration_seconds_bucket{le=\"0.0025\"} 2
kimmy_request_duration_seconds_bucket{le=\"0.005\"} 2
kimmy_request_duration_seconds_bucket{le=\"0.01\"} 2
kimmy_request_duration_seconds_bucket{le=\"0.025\"} 2
kimmy_request_duration_seconds_bucket{le=\"0.05\"} 3
kimmy_request_duration_seconds_bucket{le=\"0.1\"} 3
kimmy_request_duration_seconds_bucket{le=\"1\"} 3
kimmy_request_duration_seconds_bucket{le=\"10\"} 3
kimmy_request_duration_seconds_bucket{le=\"+Inf\"} 3
kimmy_request_duration_seconds_sum 0.03049
kimmy_request_duration_seconds_count 3
";

        assert_eq!(every_counter_distinct().render(), expected);
    }

    #[test]
    fn the_snapshot_reads_the_same_atomics_the_render_does() {
        // The bridge's whole claim (ADR-070) is that there is one source of
        // truth per counter. A snapshot that drifted from the render would be
        // the duplication this was written to avoid, arrived at by accident —
        // so every field is checked against the text a scrape would see.
        let m = every_counter_distinct();
        let s = m.snapshot();
        let out = m.render();

        let expect = |line: &str| {
            assert!(out.contains(line), "the render disagrees with the snapshot: {line}\n{out}")
        };
        expect(&format!("kimmy_uptime_seconds {}\n", s.uptime_secs));
        expect(&format!("kimmy_requests_total {}\n", s.requests));
        expect(&format!("kimmy_responses_total{{class=\"2xx\"}} {}\n", s.responses_2xx));
        expect(&format!("kimmy_responses_total{{class=\"4xx\"}} {}\n", s.responses_4xx));
        expect(&format!("kimmy_responses_total{{class=\"5xx\"}} {}\n", s.responses_5xx));
        expect(&format!("kimmy_authz_denied_total {}\n", s.authz_denied));
        expect(&format!("kimmy_auth_failures_total {}\n", s.auth_failures));
        expect(&format!("kimmy_rate_limited_total {}\n", s.rate_limited));
        expect(&format!("kimmy_backups_total {}\n", s.backups));
        expect(&format!("kimmy_ttl_expired_total {}\n", s.ttl_expired));
        expect(&format!("kimmy_ttl_skipped_total {}\n", s.ttl_skipped));
        expect(&format!(
            "kimmy_webhook_deliveries_total{{outcome=\"delivered\"}} {}\n",
            s.webhook_delivered
        ));
        expect(&format!(
            "kimmy_webhook_deliveries_total{{outcome=\"failed\"}} {}\n",
            s.webhook_failed
        ));
        expect(&format!("kimmy_webhook_events_total {}\n", s.webhook_events));
        expect(&format!("kimmy_webhook_subscriptions{{state=\"active\"}} {}\n", s.webhook_active));
        expect(&format!(
            "kimmy_webhook_subscriptions{{state=\"invalidated\"}} {}\n",
            s.webhook_invalidated
        ));
        expect(&format!("kimmy_webhook_backlog_seconds {}\n", s.webhook_backlog_secs));
        expect(&format!("kimmy_cluster_members {}\n", s.cluster_members));
        expect(&format!("kimmy_replication_lag_seconds {}\n", s.replication_lag_secs));
        expect(&format!("kimmy_tls_reloads_total{{outcome=\"ok\"}} {}\n", s.tls_reloads_ok));
        expect(&format!(
            "kimmy_tls_reloads_total{{outcome=\"failed\"}} {}\n",
            s.tls_reloads_failed
        ));
        expect(&format!("kimmy_jwks_refresh_total{{outcome=\"ok\"}} {}\n", s.jwks_refresh_ok));
        expect(&format!(
            "kimmy_jwks_refresh_total{{outcome=\"failed\"}} {}\n",
            s.jwks_refresh_failed
        ));
        expect(&format!("kimmy_embed_documents_total {}\n", s.embed_documents_embedded));
        expect(&format!("kimmy_embed_chunks_total {}\n", s.embed_chunks_embedded));
        expect(&format!("kimmy_embed_deferred_total {}\n", s.embed_deferred));
        expect(&format!("kimmy_embed_skipped_not_owned_total {}\n", s.embed_skipped_not_owned));
        expect(&format!("kimmy_embed_failures_total {}\n", s.embed_failures));
        for (kind, n) in ["connect", "timeout", "reset", "other"].iter().zip(s.embed_transport) {
            expect(&format!("kimmy_embed_provider_errors_total{{kind=\"{kind}\"}} {n}\n"));
        }
        expect(&format!("kimmy_request_duration_seconds_count {}\n", s.latency_count));

        // Not a rendered series of its own — the histogram prints it in seconds
        // — but the bridge reports microseconds, so the conversion is the thing
        // that can silently be wrong.
        assert_eq!(s.latency_sum_us, 90 + 400 + 30_000);
    }

    #[test]
    fn statuses_land_in_the_right_class() {
        let m = Metrics::default();
        for status in [200, 201, 204] {
            m.record_request(status);
        }
        for status in [400, 403, 429] {
            m.record_request(status);
        }
        m.record_request(500);
        m.record_request(304);

        let out = m.render();
        assert!(out.contains("kimmy_requests_total 8"), "{out}");
        assert!(out.contains("class=\"2xx\"} 3"), "{out}");
        assert!(out.contains("class=\"4xx\"} 3"), "{out}");
        assert!(out.contains("class=\"5xx\"} 1"), "{out}");
    }

    #[test]
    fn the_render_is_parseable_prometheus_text() {
        // Every series needs its HELP and TYPE, and every sample line must be
        // `name value`. A scrape failing on a malformed line loses the whole
        // endpoint, not just the bad series.
        let m = Metrics::default();
        m.record_request(403);
        let out = m.render();

        let mut samples = 0;
        for line in out.lines() {
            if line.starts_with('#') {
                assert!(
                    line.starts_with("# HELP ") || line.starts_with("# TYPE "),
                    "unexpected comment: {line}"
                );
                continue;
            }
            let value = line.rsplit(' ').next().expect("a value");
            // f64 rather than u64: the histogram's `_sum` is in seconds.
            assert!(value.parse::<f64>().is_ok(), "not a numeric sample: {line}");
            samples += 1;
        }
        // 28 scalar series plus the histogram: 12 buckets, +Inf, sum, count.
        assert_eq!(samples, 48, "expected one sample per series: {out}");
    }

    #[test]
    fn latency_buckets_are_cumulative_and_the_sum_is_in_seconds() {
        use std::time::Duration;
        let m = Metrics::default();
        m.record_latency(Duration::from_micros(200)); // ≤ 250µs
        m.record_latency(Duration::from_micros(200));
        m.record_latency(Duration::from_millis(3)); // ≤ 5ms
        let out = m.render();

        // Cumulative: the 250µs bucket holds 2, everything from 5ms up holds
        // all 3 — a non-cumulative render would break every Prometheus
        // quantile function silently.
        assert!(out.contains("kimmy_request_duration_seconds_bucket{le=\"0.00025\"} 2"), "{out}");
        assert!(out.contains("kimmy_request_duration_seconds_bucket{le=\"0.001\"} 2"), "{out}");
        assert!(out.contains("kimmy_request_duration_seconds_bucket{le=\"0.005\"} 3"), "{out}");
        assert!(out.contains("kimmy_request_duration_seconds_bucket{le=\"+Inf\"} 3"), "{out}");
        assert!(out.contains("kimmy_request_duration_seconds_count 3"), "{out}");
        assert!(out.contains("kimmy_request_duration_seconds_sum 0.0034"), "{out}");
    }

    #[test]
    fn an_observation_above_every_bound_reaches_only_inf() {
        use std::time::Duration;
        let m = Metrics::default();
        m.record_latency(Duration::from_secs(60));
        let out = m.render();
        assert!(out.contains("kimmy_request_duration_seconds_bucket{le=\"10\"} 0"), "{out}");
        assert!(out.contains("kimmy_request_duration_seconds_bucket{le=\"+Inf\"} 1"), "{out}");
    }

    #[test]
    fn the_specific_counters_track_their_statuses() {
        let m = Metrics::default();
        m.record_request(401);
        m.record_request(403);
        m.record_request(403);
        m.record_request(429);

        let out = m.render();
        assert!(out.contains("kimmy_auth_failures_total 1"), "{out}");
        assert!(out.contains("kimmy_authz_denied_total 2"), "{out}");
        assert!(out.contains("kimmy_rate_limited_total 1"), "{out}");
        assert!(out.contains("class=\"4xx\"} 4"), "all four are client errors too: {out}");
    }

    #[test]
    fn counters_start_at_zero_rather_than_being_absent() {
        // A counter that only appears after its first event makes a dashboard
        // show "no data" instead of "nothing has gone wrong yet".
        let out = Metrics::default().render();
        assert!(out.contains("kimmy_authz_denied_total 0"), "{out}");
        assert!(out.contains("kimmy_rate_limited_total 0"), "{out}");
    }

    #[test]
    fn the_pushed_gauges_render_what_was_pushed() {
        // Each of these is *set* from somewhere else — the replication loop,
        // the dispatcher, the certificate reloader — and every existing
        // assertion about them checks a value a broken setter would also
        // produce: the cluster harness waits for replication lag to reach
        // **zero**, which is exactly what a setter that does nothing reports.
        // A non-zero value is the only one that distinguishes the two.
        let m = Metrics::default();
        m.set_replication_lag_secs(7);
        m.set_cluster_members(2);
        m.record_tls_reload(true);
        m.record_tls_reload(false);
        m.record_tls_reload(false);
        m.record_jwks_refresh(true);
        m.record_jwks_refresh(true);
        m.record_jwks_refresh(false);

        let out = m.render();
        assert!(out.contains("kimmy_replication_lag_seconds 7"), "{out}");
        assert!(out.contains("kimmy_cluster_members 2"), "{out}");
        assert!(out.contains("kimmy_tls_reloads_total{outcome=\"ok\"} 1"), "{out}");
        assert!(out.contains("kimmy_tls_reloads_total{outcome=\"failed\"} 2"), "{out}");
        // A node that stops being able to reach its identity provider keeps
        // serving until the provider rotates, so the failed count is the only
        // warning there is (ADR-064).
        assert!(out.contains("kimmy_jwks_refresh_total{outcome=\"ok\"} 2"), "{out}");
        assert!(out.contains("kimmy_jwks_refresh_total{outcome=\"failed\"} 1"), "{out}");
    }
}
