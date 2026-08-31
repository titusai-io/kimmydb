//! Tracing subscriber setup, and the OpenTelemetry exporters behind it.
//!
//! # One registry, two consumers
//!
//! The `tracing` events this process emits go to a formatting layer as they
//! always have, and — when a collector is configured — to a second layer that
//! converts spans into OTLP. That is what lets `kimmy-storage`,
//! `kimmy-cluster` and `kimmy-vector` be instrumented with the plain `tracing`
//! they already had and no new dependency at all.
//!
//! # `kimmy::audit` never reaches the collector
//!
//! The audit target is filtered out of the OTLP layer and only that layer, so
//! an operator's `RUST_LOG` routing of it keeps working untouched. Audit
//! records carry principal names *and* collection names — exactly what
//! `telemetry.include_names` exists to gate — and a collector is a different
//! destination from a log file even when the same person runs both. See
//! ADR-068, and `the_otel_layer_refuses_audit_records_and_accepts_everything_else`
//! below, which is what stops this from being a promise nobody checks.
//!
//! # Why the exporter is blocking
//!
//! [`init`] runs **before** the tokio runtime is built — see `main.rs`, where
//! configuration is resolved and logging installed before a runtime exists so
//! that a misconfiguration is a one-line error rather than a panic in a worker
//! thread. An async exporter constructed there would have no reactor, so the
//! OTLP builder is fed the blocking `reqwest` client, which owns its own
//! threads (ADR-069).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use opentelemetry_semantic_conventions::attribute as semconv;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::prelude::*;

use crate::config::{LogConfig, LogFormat, TelemetryConfig};

/// Keeps the exporters alive, and flushes them on the way out.
///
/// Held by `main` across `node::run` and dropped after it returns. A batch
/// processor buffers, so a process that exits without this drops whatever it
/// had not yet sent — which is precisely the last few seconds before a crash,
/// the part anybody debugging actually wants.
///
/// Empty when no collector is configured, which is the ordinary case: the guard
/// still exists so the call site has one shape.
#[must_use = "dropping the guard immediately shuts the exporters down again"]
pub struct TelemetryGuard {
    tracer: Option<SdkTracerProvider>,
    meter: Option<SdkMeterProvider>,
}

impl TelemetryGuard {
    /// A guard for a node that exports nothing.
    fn inert() -> Self {
        Self { tracer: None, meter: None }
    }

    /// Report every process counter to the collector, from the atomics
    /// `/metrics` already renders.
    ///
    /// **Bridged, not duplicated** (ADR-070). Each instrument is *observable*:
    /// its callback reads [`kimmy_api::metrics::Metrics::snapshot`] at export
    /// time, so there is exactly one source of truth per counter and no way
    /// for a Prometheus dashboard and a trace backend to disagree about how
    /// many requests a node served.
    ///
    /// Called from `node::run` rather than from [`init`], because the counters
    /// live in the API state and the API state needs a database — which does
    /// not exist yet when the subscriber is installed. With no meter provider
    /// installed the global meter is a no-op and this registers nothing.
    pub fn bridge_metrics(state: &kimmy_api::SharedState) {
        let meter = opentelemetry::global::meter("kimmydb");

        // Weak, so the callbacks the global meter provider holds forever do
        // not keep the engine alive past shutdown. A dead weak reference simply
        // observes nothing, which is the honest reading of "the node is gone".
        let weak = Arc::downgrade(state);
        let snapshot = move || weak.upgrade().map(|state| state.metrics.snapshot());

        macro_rules! observe {
            ($build:ident, $name:literal, $unit:literal, $description:literal, $field:ident) => {{
                let snapshot = snapshot.clone();
                let _ = meter
                    .$build($name)
                    .with_unit($unit)
                    .with_description($description)
                    .with_callback(move |observer| {
                        if let Some(s) = snapshot() {
                            observer.observe(s.$field, &[]);
                        }
                    })
                    .build();
            }};
        }

        // The same names `/metrics` uses, minus the `_total` suffix Prometheus
        // adds to a counter: an OTLP counter called `kimmy_requests_total`
        // becomes `kimmy_requests_total_total` the moment a collector exports
        // it back to Prometheus.
        observe!(u64_observable_gauge, "kimmy.uptime", "s", "Seconds serving.", uptime_secs);
        observe!(
            u64_observable_counter,
            "kimmy.requests",
            "{request}",
            "HTTP requests handled.",
            requests
        );
        observe!(
            u64_observable_counter,
            "kimmy.responses.2xx",
            "{response}",
            "HTTP responses with a 2xx status.",
            responses_2xx
        );
        observe!(
            u64_observable_counter,
            "kimmy.responses.4xx",
            "{response}",
            "HTTP responses with a 4xx status.",
            responses_4xx
        );
        observe!(
            u64_observable_counter,
            "kimmy.responses.5xx",
            "{response}",
            "HTTP responses with a 5xx status.",
            responses_5xx
        );
        observe!(
            u64_observable_counter,
            "kimmy.authz.denied",
            "{decision}",
            "Operations refused by RBAC.",
            authz_denied
        );
        observe!(
            u64_observable_counter,
            "kimmy.auth.failures",
            "{attempt}",
            "Rejected credentials and tokens.",
            auth_failures
        );
        observe!(
            u64_observable_counter,
            "kimmy.rate_limited",
            "{request}",
            "Requests refused by a rate limit.",
            rate_limited
        );
        observe!(
            u64_observable_counter,
            "kimmy.rate_limited.principal",
            "{request}",
            "Authenticated requests refused by the per-principal rate limit.",
            rate_limited_principal
        );
        observe!(u64_observable_counter, "kimmy.backups", "{backup}", "Backups served.", backups);
        observe!(
            u64_observable_counter,
            "kimmy.ttl.expired",
            "{document}",
            "Documents deleted by a TTL index.",
            ttl_expired
        );
        observe!(
            u64_observable_counter,
            "kimmy.ttl.skipped",
            "{document}",
            "Expiry candidates refused because the document was refreshed first.",
            ttl_skipped
        );
        observe!(
            u64_observable_counter,
            "kimmy.webhook.delivered",
            "{batch}",
            "Webhook batches an endpoint accepted.",
            webhook_delivered
        );
        observe!(
            u64_observable_counter,
            "kimmy.webhook.failed",
            "{batch}",
            "Webhook batches an endpoint refused or did not answer.",
            webhook_failed
        );
        observe!(
            u64_observable_counter,
            "kimmy.webhook.events",
            "{event}",
            "Change events pushed to endpoints.",
            webhook_events
        );
        observe!(
            u64_observable_gauge,
            "kimmy.webhook.subscriptions.active",
            "{subscription}",
            "Active subscriptions, as this node sees the registry.",
            webhook_active
        );
        observe!(
            u64_observable_gauge,
            "kimmy.webhook.subscriptions.invalidated",
            "{subscription}",
            "Invalidated subscriptions, as this node sees the registry.",
            webhook_invalidated
        );
        observe!(
            u64_observable_gauge,
            "kimmy.webhook.backlog",
            "s",
            "Age of the oldest undelivered event, across subscriptions this node owns.",
            webhook_backlog_secs
        );
        observe!(
            u64_observable_gauge,
            "kimmy.cluster.members",
            "{node}",
            "Peers this node's SWIM membership considers alive.",
            cluster_members
        );
        observe!(
            u64_observable_gauge,
            "kimmy.replication.lag",
            "s",
            "Peer oplog history not yet applied locally, worst peer in the last round.",
            replication_lag_secs
        );
        observe!(
            u64_observable_counter,
            "kimmy.tls.reloads.ok",
            "{reload}",
            "Certificate reloads that succeeded.",
            tls_reloads_ok
        );
        observe!(
            u64_observable_counter,
            "kimmy.tls.reloads.failed",
            "{reload}",
            "Certificate reloads that failed, leaving the one in use serving.",
            tls_reloads_failed
        );
        observe!(
            u64_observable_counter,
            "kimmy.jwks.refresh.ok",
            "{refresh}",
            "OIDC signing-key refreshes that succeeded.",
            jwks_refresh_ok
        );
        observe!(
            u64_observable_counter,
            "kimmy.jwks.refresh.failed",
            "{refresh}",
            "OIDC signing-key refreshes that failed, leaving the key set in use verifying.",
            jwks_refresh_failed
        );
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        // Errors are logged rather than propagated: this runs on the way out,
        // there is nothing left to abort, and a failed flush must not turn a
        // clean shutdown into a non-zero exit.
        if let Some(tracer) = self.tracer.take()
            && let Err(e) = tracer.shutdown()
        {
            tracing::warn!(error = %e, "could not flush buffered spans to the collector");
        }
        if let Some(meter) = self.meter.take()
            && let Err(e) = meter.shutdown()
        {
            tracing::warn!(error = %e, "could not flush buffered metrics to the collector");
        }
    }
}

/// Install the global tracing subscriber, and the exporters if any.
///
/// `RUST_LOG` takes precedence over the configured level when set, since that
/// is what an operator will reach for when debugging a running container.
///
/// `telemetry` is `Some` only when the process is going to serve. `check-config`
/// and `restore` pass `None`: neither serves, both exit in under a second, and
/// starting an exporter for them would mean a config check opens a connection
/// to production's collector.
pub fn init(cfg: &LogConfig, telemetry: Option<&TelemetryConfig>) -> Result<TelemetryGuard> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&cfg.level))
        .with_context(|| format!("invalid log filter {:?}", cfg.level))?;

    let registry = tracing_subscriber::registry().with(filter);

    // Boxed, because the two formats are different types and the OTel layer
    // below has to be built against *one* subscriber type. Building it inside
    // each arm instead would mean writing the exporter setup twice, and the
    // half that would drift is the audit filter.
    let fmt = match cfg.format {
        LogFormat::Pretty => tracing_subscriber::fmt::layer().with_target(true).boxed(),
        LogFormat::Json => tracing_subscriber::fmt::layer().json().with_target(true).boxed(),
    };

    let Some(telemetry) = telemetry.filter(|t| t.is_configured()) else {
        registry.with(fmt).init();
        return Ok(TelemetryGuard::inert());
    };

    let resource = resource(telemetry);
    let tracer_provider = tracer_provider(telemetry, resource.clone())?;
    let meter_provider = meter_provider(telemetry, resource)?;

    // Without this, `kimmy-api`'s extract and inject calls are silent no-ops:
    // the global propagator defaults to one that reads and writes nothing, so
    // every request would start its own trace and no webhook would carry one.
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    let otel = tracing_opentelemetry::layer()
        .with_tracer(tracer_provider.tracer("kimmydb"))
        // ADR-068, and attached to this layer alone: the log keeps receiving
        // everything, and an operator's `RUST_LOG` routing is untouched.
        .with_filter(filter_fn(exported));

    registry.with(fmt).with(otel).init();

    Ok(TelemetryGuard { tracer: Some(tracer_provider), meter: Some(meter_provider) })
}

/// Whether a callsite may reach the collector: **spans only, never the audit
/// target**.
///
/// # Spans, not events, and this is the load-bearing half
///
/// `tracing-opentelemetry` turns every log event that happens inside a span
/// into a **span event**, carrying that event's own fields with it. Nothing in
/// this codebase writes log lines with telemetry in mind, and they are full of
/// exactly what ADR-068 gates: `kimmy_storage` logs `db` and `collection` on
/// every DDL line, the dispatcher logs a webhook `url`, `kimmy-auth` logs
/// `user`. Found by reading what a live collector actually received — with the
/// audit filter alone in place, a `create_collection` span arrived with
/// `collection: "orders"` hanging off it as an event field, while
/// `include_names` was off and every span attribute was correctly empty.
///
/// Gating on names one field at a time is not a fix: it is an audit of every
/// `info!` in the workspace, redone whenever anyone adds one. Spans are the
/// surface this feature designed for privacy — bounded, reviewed, and named
/// from route templates — and events are the surface nobody designed for it, so
/// events do not go. Logs stay logs.
///
/// # The audit target, separately and unconditionally
///
/// Audit records are events, so the span rule already excludes them. Naming
/// them anyway is deliberate: the exclusion is a promise about *identities*
/// rather than a consequence of how one layer happens to be filtered, and if
/// auditing ever grew a span this must still refuse it.
fn exported(metadata: &tracing::Metadata<'_>) -> bool {
    metadata.is_span() && metadata.target() != kimmy_api::telemetry::AUDIT_TARGET
}

/// What every span and metric from this node is attributed to.
///
/// Version *and* commit, because the startup line at `node.rs` pairs them for
/// exactly this reason: during a rolling upgrade or an incident the question is
/// "which build is this exactly", and a version number alone does not answer it
/// between releases. A trace is read at precisely that moment.
fn resource(cfg: &TelemetryConfig) -> Resource {
    Resource::builder()
        .with_service_name(cfg.service_name.clone())
        .with_attributes([
            KeyValue::new(semconv::SERVICE_VERSION, kimmy_core::build::VERSION),
            KeyValue::new("service.commit", kimmy_core::build::COMMIT),
        ])
        .build()
}

/// `http/protobuf` or `http/json`, already validated.
fn protocol(cfg: &TelemetryConfig) -> Protocol {
    match cfg.protocol.as_str() {
        "http/json" => Protocol::HttpJson,
        // `Config::validate` refuses anything else, so this is the protobuf
        // arm rather than a fallback that could quietly change an operator's
        // encoding.
        _ => Protocol::HttpBinary,
    }
}

/// The signal's own URL.
///
/// The configured endpoint is a **base**, matching what
/// `OTEL_EXPORTER_OTLP_ENDPOINT` means everywhere else, so an operator can
/// paste the value their other services use. The exporter does *not* append the
/// path when the endpoint is set programmatically — it takes the URI verbatim —
/// so a base handed straight through would POST to the collector's root and be
/// answered with 404 forever.
fn signal_url(endpoint: &str, path: &str) -> String {
    format!("{}/{}", endpoint.trim_end_matches('/'), path)
}

fn tracer_provider(cfg: &TelemetryConfig, resource: Resource) -> Result<SdkTracerProvider> {
    let endpoint = cfg.endpoint.as_deref().context("no telemetry endpoint configured")?;

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(signal_url(endpoint, "v1/traces"))
        .with_protocol(protocol(cfg))
        .with_timeout(Duration::from_secs(cfg.export_timeout_secs))
        .build()
        .context("building the OTLP span exporter")?;

    Ok(SdkTracerProvider::builder()
        // Batched, never simple. A simple processor exports inside the span's
        // own `end`, which would put a network round trip to the collector on
        // the request path — and make an unreachable collector into request
        // latency, which is the one thing tracing must never cost.
        .with_batch_exporter(exporter)
        // Parent-based, so a request arriving with a sampled `traceparent` is
        // recorded whatever the local ratio says. A ratio applied
        // independently per node produces traces with holes in them, which is
        // worse than fewer whole ones.
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(cfg.sample_ratio))))
        .with_resource(resource)
        .build())
}

fn meter_provider(cfg: &TelemetryConfig, resource: Resource) -> Result<SdkMeterProvider> {
    let endpoint = cfg.endpoint.as_deref().context("no telemetry endpoint configured")?;

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(signal_url(endpoint, "v1/metrics"))
        .with_protocol(protocol(cfg))
        .with_timeout(Duration::from_secs(cfg.export_timeout_secs))
        .build()
        .context("building the OTLP metric exporter")?;

    Ok(SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter).build())
        .with_resource(resource)
        .build())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `Metadata` for a callsite that does not exist, so the filter can be
    /// asked about targets and kinds no test would otherwise produce.
    ///
    /// `tracing::Metadata::new` is public and this constructs nothing that is
    /// ever registered or emitted — it is an argument to a pure predicate.
    fn meta(target: &'static str, kind: tracing::metadata::Kind) -> tracing::Metadata<'static> {
        tracing::Metadata::new(
            "callsite",
            target,
            tracing::Level::INFO,
            None,
            None,
            None,
            tracing::field::FieldSet::new(&[], tracing::callsite::Identifier(&TEST_CALLSITE)),
            kind,
        )
    }

    struct TestCallsite;
    impl tracing::callsite::Callsite for TestCallsite {
        fn set_interest(&self, _: tracing::subscriber::Interest) {}
        fn metadata(&self) -> &tracing::Metadata<'_> {
            unreachable!("never registered; only its identifier is used")
        }
    }
    static TEST_CALLSITE: TestCallsite = TestCallsite;

    #[test]
    fn only_spans_are_exported_and_never_the_audit_target() {
        // ADR-068's promise, as a check rather than a sentence, in both
        // directions — a filter that excluded too much would silently stop
        // exporting the spans the feature exists for.
        let span = |target| exported(&meta(target, tracing::metadata::Kind::SPAN));
        let event = |target| exported(&meta(target, tracing::metadata::Kind::EVENT));

        // Spans go, from every crate that has them.
        for target in [
            "kimmy_api::exec",
            "kimmy_api::routes",
            "kimmy_api::dispatch",
            "kimmy_storage::engine",
            "kimmy_cluster::peers",
            "kimmy_vector::worker",
            "kimmyd::node",
        ] {
            assert!(span(target), "{target} spans must be exported");
        }

        // **Events never do.** `tracing-opentelemetry` would otherwise attach
        // each one to its enclosing span *with its own fields*, and this
        // codebase's log lines carry `db`, `collection`, `user` and webhook
        // `url`s — the exact things `include_names` gates. Caught against a
        // live collector: a `create_collection` span arrived carrying
        // `collection: "orders"` as an event field while every span attribute
        // was correctly empty.
        for target in ["kimmy_storage::engine", "kimmy_api::dispatch", "kimmy_auth::users"] {
            assert!(!event(target), "{target} events must not be exported");
        }

        // And the audit target is refused as both, unconditionally. The span
        // rule already covers today's records — they are events — but the
        // promise is about identities, not about how one layer is filtered.
        assert!(!event(kimmy_api::telemetry::AUDIT_TARGET));
        assert!(!span(kimmy_api::telemetry::AUDIT_TARGET));

        // Near misses, because a `starts_with` or a `contains` would catch
        // these too and quietly stop exporting real spans.
        for target in ["kimmy::auditor", "kimmy_audit", "audit"] {
            assert!(span(target), "{target} must still be exported");
        }
    }

    #[test]
    fn the_signal_path_is_appended_to_the_base_exactly_once() {
        // The exporter takes a programmatic endpoint verbatim, so getting this
        // wrong is a node that POSTs to the collector's root and is answered
        // 404 forever — while serving perfectly, which is why it would not be
        // noticed.
        assert_eq!(
            signal_url("http://otel-collector:4318", "v1/traces"),
            "http://otel-collector:4318/v1/traces"
        );
        // A trailing slash must not double one.
        assert_eq!(
            signal_url("http://otel-collector:4318/", "v1/metrics"),
            "http://otel-collector:4318/v1/metrics"
        );
        // A collector behind a path prefix keeps it, for the same reason
        // OIDC discovery is appended rather than URL-joined.
        assert_eq!(signal_url("http://gateway/otlp", "v1/traces"), "http://gateway/otlp/v1/traces");
    }

    #[test]
    fn the_default_configuration_installs_no_exporter() {
        // Telemetry off is the default, and "off" has to mean no exporter and
        // no connection rather than an exporter pointed at nothing. Checked
        // through the guard rather than through `init`, which installs a
        // process-global subscriber and so can only run once per test binary.
        let cfg = TelemetryConfig::default();
        assert!(!cfg.is_configured());

        let guard = TelemetryGuard::inert();
        assert!(guard.tracer.is_none(), "no span exporter");
        assert!(guard.meter.is_none(), "no metric exporter");
    }

    #[test]
    fn the_resource_carries_the_version_and_the_commit_this_binary_was_built_from() {
        // Paired for the same reason the startup log pairs them: during a
        // rolling upgrade the question is which build this exactly is, and a
        // version number alone does not answer it between releases.
        let cfg = TelemetryConfig { service_name: "kimmydb-test".into(), ..Default::default() };
        let resource = resource(&cfg);

        assert_eq!(
            resource.get(&opentelemetry::Key::from_static_str(semconv::SERVICE_NAME)),
            Some(opentelemetry::Value::from("kimmydb-test"))
        );
        assert_eq!(
            resource.get(&opentelemetry::Key::from_static_str(semconv::SERVICE_VERSION)),
            Some(opentelemetry::Value::from(kimmy_core::build::VERSION))
        );
        assert_eq!(
            resource.get(&opentelemetry::Key::from_static_str("service.commit")),
            Some(opentelemetry::Value::from(kimmy_core::build::COMMIT))
        );
    }

    #[test]
    fn the_configured_protocol_decides_the_encoding() {
        let mut cfg = TelemetryConfig::default();
        assert!(matches!(protocol(&cfg), Protocol::HttpBinary));
        cfg.protocol = "http/json".into();
        assert!(matches!(protocol(&cfg), Protocol::HttpJson));
    }
}
