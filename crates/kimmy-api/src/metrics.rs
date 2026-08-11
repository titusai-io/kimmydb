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

/// Counters for one running server.
pub struct Metrics {
    started: Instant,
    latency_buckets: [AtomicU64; LATENCY_BUCKETS_US.len()],
    latency_sum_us: AtomicU64,
    latency_count: AtomicU64,
    replication_lag_secs: AtomicU64,
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
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            latency_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_sum_us: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            replication_lag_secs: AtomicU64::new(0),
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
            webhook_active: AtomicU64::new(0),
            webhook_invalidated: AtomicU64::new(0),
            webhook_backlog_secs: AtomicU64::new(0),
            cluster_members: AtomicU64::new(0),
        }
    }
}

impl Metrics {
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

    pub fn uptime_secs(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    fn get(&self, counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    /// Render the process counters in Prometheus text format.
    ///
    /// The storage gauges are rendered by the caller, which has the engine;
    /// keeping them apart avoids giving this type a database handle purely to
    /// print two numbers.
    pub fn render(&self) -> String {
        let mut out = format!(
            "# HELP kimmy_uptime_seconds Seconds since this process started serving.\n\
             # TYPE kimmy_uptime_seconds gauge\n\
             kimmy_uptime_seconds {uptime}\n\
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
             kimmy_replication_lag_seconds {lag}\n",
            uptime = self.uptime_secs(),
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
            wh_active = self.get(&self.webhook_active),
            wh_invalid = self.get(&self.webhook_invalidated),
            wh_backlog = self.get(&self.webhook_backlog_secs),
            cluster = self.get(&self.cluster_members),
            lag = self.get(&self.replication_lag_secs),
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
        // 17 scalar series plus the histogram: 12 buckets, +Inf, sum, count.
        assert_eq!(samples, 32, "expected one sample per series: {out}");
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
}
