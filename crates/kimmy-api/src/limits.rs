//! Per-request resource limits: the deadline and the body ceiling (ADR-099).
//!
//! The third limit in that decision, the per-principal rate limit, is not here
//! — it needs the principal, which only the `Auth` extractor has, so it lives
//! in [`crate::state`]. What this module holds is the two limits that need
//! nothing but the request.
//!
//! # What the deadline does and does not bound
//!
//! The timeout wraps the whole service call for a route: extractors, the body
//! read, and the handler. It fires when that future is still *pending* at the
//! deadline, and the two places a request in this server is pending are the
//! request body arriving and an upstream embedding provider answering. Both
//! are bounded honestly: the future is dropped, the connection is answered
//! with `503 timeout`, and whatever was being waited for is no longer waited
//! for.
//!
//! What it does **not** interrupt is synchronous work. A storage operation — a
//! scan, a bulk commit, an index backfill, a database drop — runs to completion
//! on the worker thread without yielding, and a future that never yields is
//! never seen to be pending; the deadline cannot fire inside it, and a request
//! that finishes late is still answered with its result rather than replaced
//! with a refusal after the work was done. That is the right outcome for those
//! operations, and it is stated here so nobody expects a 30-second query to be
//! cut short by this setting. Cooperative cancellation inside the engine is a
//! separate decision, deferred in ADR-099.

use std::time::Duration;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::error::ApiError;

/// The body ceiling axum applies when nothing sets one, which is what every
/// release before this setting existed enforced. The default is this value so
/// that adding the knob changes nothing.
pub const DEFAULT_MAX_BODY_BYTES: usize = 2 * 1024 * 1024;

/// How long a request may take before this node abandons it.
///
/// Generous on purpose: measured request latencies are milliseconds
/// (ADR-046), so a request still waiting after this long is one whose client
/// or provider has stalled, not a slow query.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The limits the router applies to every request.
///
/// Held by value rather than behind the shared state because they are
/// parameters of the middleware stack, fixed when the router is built, and a
/// test that wants a small body ceiling or a short deadline should be able to
/// build a router that has one without touching anything else.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestLimits {
    /// Deadline for a request on a timed route. See the module documentation
    /// for what that bounds.
    pub request_timeout: Duration,
    /// Largest request body an extractor will read, in bytes. Over it, the
    /// request is refused with `413 payload_too_large`.
    pub max_body_bytes: usize,
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self { request_timeout: DEFAULT_REQUEST_TIMEOUT, max_body_bytes: DEFAULT_MAX_BODY_BYTES }
    }
}

/// Abandon a request that is still pending at the deadline.
///
/// An `axum::middleware::from_fn_with_state` function rather than
/// `tower_http::timeout::TimeoutLayer`: that layer answers with an empty
/// `408`, and both halves of that are wrong here — the status for the reason
/// [`ApiError::timeout`] gives, and the empty body because every refusal this
/// API makes carries the envelope a client branches on.
pub async fn enforce_timeout(
    State(limits): State<RequestLimits>,
    request: Request,
    next: Next,
) -> Response {
    match tokio::time::timeout(limits.request_timeout, next.run(request)).await {
        Ok(response) => response,
        Err(_elapsed) => ApiError::timeout(limits.request_timeout).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::routing::get;
    use tower::ServiceExt;

    fn router(limits: RequestLimits) -> Router {
        Router::new()
            .route("/quick", get(|| async { "done" }))
            .route(
                "/slow",
                get(|| async {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    "done"
                }),
            )
            .layer(axum::middleware::from_fn_with_state(limits, enforce_timeout))
    }

    async fn body_json(response: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 16).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn a_request_pending_at_the_deadline_is_answered_with_the_envelope() {
        let limits = RequestLimits {
            request_timeout: Duration::from_millis(50),
            ..RequestLimits::default()
        };
        let response = router(limits)
            .oneshot(Request::builder().uri("/slow").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), 503);
        let body = body_json(response).await;
        assert_eq!(body["error"], "timeout");
        assert_eq!(body["retry"], "wait");
        assert!(
            body["message"].as_str().unwrap().contains("request_timeout_secs"),
            "the message should name the setting an operator would change: {body}"
        );
    }

    #[tokio::test]
    async fn a_request_that_finishes_in_time_is_untouched() {
        let limits = RequestLimits {
            request_timeout: Duration::from_millis(50),
            ..RequestLimits::default()
        };
        let response = router(limits)
            .oneshot(Request::builder().uri("/quick").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }

    #[test]
    fn the_defaults_are_what_the_server_enforced_before_the_settings_existed() {
        // Adding a knob must not move anything: axum's own body ceiling is
        // 2 MiB, and the existing 413 test sends a body just over it.
        let limits = RequestLimits::default();
        assert_eq!(limits.max_body_bytes, 2_097_152);
        assert_eq!(limits.request_timeout, Duration::from_secs(30));
    }
}
