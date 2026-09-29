//! What the listener answers before the store is open (ADR-198).
//!
//! The HTTP port is bound before `Engine::open`, so that an open that takes
//! minutes (redb's repair, the schema migration, the walk that verifies the
//! version vector) is a node that answers `/healthz` and says on `/readyz` what
//! it is doing, and not a port nothing listens on that a liveness probe kills.
//! Until the node's router is installed, [`Front`] is the whole service:
//!
//! | Route | Answer |
//! |---|---|
//! | `GET /healthz` | 200 `{"status":"ok"}`, as always |
//! | `GET /readyz` | 503, the error envelope with `status`, `phase`, `phase_age_seconds`, and `done` and `total` when the phase counts |
//! | anything else | 503 `starting`, `retry: elsewhere`, no `Retry-After` |
//!
//! **`/v1/version` and `/metrics` are refused too**, on purpose: a roll polls
//! `/v1/version` to decide a member is up, and a member that answered it while
//! it was still opening would send the roll on to the next one.
//!
//! The answer is built here and logs nothing: a probe every few seconds for a
//! long open would be a line each. The swap is one `OnceLock`: once the router
//! is installed, every request goes to it, at the cost of one atomic load.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use kimmy_storage::{OpenPhase, OpenSnapshot};
use serde_json::json;
use tower::Service;

/// The service the listener serves, from the bind to the swap and after it.
#[derive(Clone, Default)]
pub struct Front {
    app: Arc<OnceLock<axum::Router>>,
}

impl Front {
    /// A front with no router: it answers as an opening node does.
    pub fn new() -> Self {
        Self::default()
    }

    /// A front already serving `app`, for a test with nothing to wait for.
    #[cfg(test)]
    pub fn ready(app: axum::Router) -> Self {
        let front = Self::new();
        front.install(app);
        front
    }

    /// The swap: from now on every request goes to `app`. `false` if a router
    /// was installed already.
    pub fn install(&self, app: axum::Router) -> bool {
        self.app.set(app).is_ok()
    }

    /// Whether the router is installed.
    #[cfg(test)]
    pub fn is_installed(&self) -> bool {
        self.app.get().is_some()
    }
}

impl Service<Request<Body>> for Front {
    type Response = Response;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        match self.app.get() {
            Some(router) => {
                let mut router = router.clone();
                Box::pin(async move { router.call(request).await })
            }
            None => {
                let response = while_opening(&request, kimmy_storage::open_snapshot());
                Box::pin(async move { Ok(response) })
            }
        }
    }
}

/// The answer of a node that has bound its port and not yet installed its
/// router.
fn while_opening(request: &Request<Body>, open: OpenSnapshot) -> Response {
    let get = request.method() == Method::GET || request.method() == Method::HEAD;
    match (get, request.uri().path()) {
        (true, "/healthz") => axum::Json(json!({ "status": "ok" })).into_response(),
        (true, "/readyz") => envelope(
            "the node is starting; it is not ready to serve",
            [("status", json!("opening"))]
                .into_iter()
                .chain(progress_fields(open))
                .collect::<Vec<_>>(),
        ),
        _ => envelope("this node is starting and answers only /healthz and /readyz", Vec::new()),
    }
}

/// `phase`, its age, and its count when it has one.
fn progress_fields(open: OpenSnapshot) -> Vec<(&'static str, serde_json::Value)> {
    // Before the store's open has begun there is no phase to name yet: the
    // node is between the bind and the open, and that is `opening`.
    let phase = match open.phase {
        OpenPhase::Idle => OpenPhase::Opening,
        other => other,
    };
    let mut fields = vec![
        ("phase", json!(phase.label())),
        ("phase_age_seconds", json!(open.phase_age.as_secs_f64())),
    ];
    if open.total > 0 {
        fields.push(("done", json!(open.done)));
        fields.push(("total", json!(open.total)));
    }
    fields
}

/// A 503 in the API's error envelope (`error`, `message`, `retry`), plus
/// `extra`. Built by hand: [`kimmy_api::ApiError`] logs each answer it makes,
/// and this one is asked for on every probe.
fn envelope(message: &str, extra: Vec<(&'static str, serde_json::Value)>) -> Response {
    let code = kimmy_api::error::ErrorCode::Starting;
    let mut body = serde_json::Map::new();
    body.insert("error".into(), json!(code.as_str()));
    body.insert("message".into(), json!(message));
    body.insert("retry".into(), json!(code.retry().as_str()));
    for (key, value) in extra {
        body.insert(key.into(), value);
    }
    (StatusCode::SERVICE_UNAVAILABLE, axum::Json(serde_json::Value::Object(body))).into_response()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    fn get(path: &str) -> Request<Body> {
        Request::builder().method(Method::GET).uri(path).body(Body::empty()).unwrap()
    }

    async fn answer(front: &Front, request: Request<Body>) -> (StatusCode, serde_json::Value) {
        let response = front.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
    }

    #[tokio::test]
    async fn an_opening_node_answers_health_and_refuses_everything_else() {
        let front = Front::new();
        let (status, body) = answer(&front, get("/healthz")).await;
        assert_eq!((status, &body["status"]), (StatusCode::OK, &json!("ok")));

        let (status, body) = answer(&front, get("/readyz")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "starting");
        assert_eq!(body["retry"], "elsewhere");
        assert_eq!(body["status"], "opening");
        assert!(body["phase"].is_string() && body["phase_age_seconds"].is_number(), "{body}");

        // Every other route, the two a roll and a scrape use included.
        for path in ["/v1/version", "/metrics", "/v1/db/shop/collections", "/mcp", "/nope"] {
            let (status, body) = answer(&front, get(path)).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
            assert_eq!(body["error"], "starting", "{path}: {body}");
        }
        // A write, too, and a `/healthz` that is not a GET.
        let post = Request::builder().method(Method::POST).uri("/healthz").body(Body::empty());
        let (status, _) = answer(&front, post.unwrap()).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn the_swap_sends_every_route_to_the_router_and_only_once() {
        let front = Front::new();
        assert!(!front.is_installed());
        let app = axum::Router::new().route("/v1/version", axum::routing::get(|| async { "up" }));
        assert!(front.install(app.clone()));
        assert!(!front.install(app), "the swap happens once");
        let response = front.clone().oneshot(get("/v1/version")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // A route the router lacks is the router's own answer, not the front's.
        let response = front.clone().oneshot(get("/nope")).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn a_phase_with_a_count_says_how_far_and_one_without_does_not() {
        let counted = OpenSnapshot {
            phase: OpenPhase::Migrating,
            phase_age: Duration::from_secs(3),
            done: 2,
            total: 7,
        };
        let fields = progress_fields(counted);
        let get = |name: &str| fields.iter().find(|(k, _)| *k == name).map(|(_, v)| v.clone());
        assert_eq!(get("phase"), Some(json!("migrating")));
        assert_eq!((get("done"), get("total")), (Some(json!(2)), Some(json!(7))));

        let plain = OpenSnapshot { phase: OpenPhase::Repairing, total: 0, done: 0, ..counted };
        let fields = progress_fields(plain);
        assert!(fields.iter().all(|(k, _)| *k != "done" && *k != "total"));
        // Before the open has begun: named `opening`, not `idle`.
        let idle = OpenSnapshot { phase: OpenPhase::Idle, ..plain };
        assert_eq!(progress_fields(idle)[0].1, json!("opening"));
    }
}
