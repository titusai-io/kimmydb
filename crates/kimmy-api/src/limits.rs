//! Per-request resource limits: the deadline and the body ceiling (ADR-099).
//!
//! The third limit in that decision, the per-principal rate limit, is not here
//! — it needs the principal, which only the `Auth` extractor has, so it lives
//! in [`crate::state`]. What this module holds is the two limits that need
//! nothing but the request — and, beside the ceiling, the drain that lets a
//! client see the ceiling's refusal.
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

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::{Body, Bytes, HttpBody};
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http_body::{Frame, SizeHint};
use http_body_util::BodyExt;
use tokio::sync::oneshot;
use tracing::debug;

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

/// How much of a refused body is read past the point of refusal before the
/// `413` is written, in bytes.
///
/// The refusal is written while the client may still be sending. Close the
/// socket then and the bytes still arriving are answered with a reset, and a
/// reset can discard a response the client has not read yet — on macOS it
/// does — so the client reports a connection error instead of the `413` it
/// was sent. Reading the remainder first is what lets the refusal be seen.
///
/// Bounded, because an unbounded read is the attack the ceiling exists to
/// prevent: nothing is kept, but a client that never stops sending would
/// hold the connection and the reader for as long as it liked. A few MiB
/// covers the body a client sends by mistake — the batch that grew past the
/// limit — and a client further over than this is closed on, as before.
pub const MAX_BODY_DRAIN_BYTES: usize = 4 * 1024 * 1024;

/// How long the drain may take before the `413` is written regardless.
///
/// The other axis of the same bound: a client that stops sending without
/// closing would otherwise be waited on for a cap it never fills. On loopback
/// the whole cap drains in milliseconds; a client on a slow link gets as much
/// of the courtesy as fits in this.
pub const BODY_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

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

tokio::task_local! {
    /// When the request this task serves must be answered by: set by
    /// [`enforce_timeout`] for the handler it wraps.
    static REQUEST_DEADLINE: std::time::Instant;
}

/// How long the request this task serves has left before [`enforce_timeout`]
/// abandons it, or `None` outside a request with a deadline.
///
/// For a handler with work that can run after its change has committed, the
/// confirmation of a schema change (ADR-140): capped by this, it answers with
/// what it knows rather than being cut off, and a committed change is never
/// reported as an abandoned request.
pub fn request_time_left() -> Option<Duration> {
    REQUEST_DEADLINE
        .try_with(|deadline| deadline.saturating_duration_since(std::time::Instant::now()))
        .ok()
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
    // The same deadline bounds a handler's wait for the storage writer
    // (ADR-151). This timeout alone cannot: a handler blocked waiting for the
    // writer never yields, so the deadline here is only seen once the wait
    // ends — a write that could not get the writer hung for as long as it
    // was held, and the client saw a transport timeout rather than the
    // documented refusal.
    let deadline = std::time::Instant::now() + limits.request_timeout;
    let handler = REQUEST_DEADLINE.scope(
        deadline,
        kimmy_storage::with_write_wait_budget(limits.request_timeout, next.run(request)),
    );
    match tokio::time::timeout(limits.request_timeout, handler).await {
        Ok(response) => response,
        Err(_elapsed) => ApiError::timeout(limits.request_timeout).into_response(),
    }
}

/// Read the rest of a refused body before the `413` is written.
///
/// # Why a layer, and why this shape
///
/// The ceiling is `DefaultBodyLimit`, which does nothing by itself: it leaves
/// a limit in the request extensions, the `Json` extractor wraps the body in
/// `http_body_util::Limited` with it, and the `413` is that wrapper's error
/// surfacing as the extractor's rejection. By the time a response exists the
/// extractor has dropped the body, and a body that has been dropped cannot be
/// drained — unless what it dropped was a wrapper that hands the real body
/// back as it goes. That is [`Retained`]: it forwards every poll and, on
/// drop, sends the inner body and a count of what was read through a channel
/// this layer holds. On a `413` the layer takes the body back and reads on,
/// bounded by the declared `Content-Length`, [`MAX_BODY_DRAIN_BYTES`] and
/// [`BODY_DRAIN_TIMEOUT`]; on any other response it lets go, as before.
///
/// Not a `Content-Length` check up front, although a declared length over the
/// ceiling could be refused without reading a byte. This layer wraps the
/// whole table, `/mcp` included, and rmcp reads its bodies under a ceiling of
/// its own that ADR-099 deliberately left separate; refusing on the header
/// here would make the two one setting by the back door. The declared length
/// bounds the drain instead, so a body that ends soon is read to its end
/// rather than to the cap — and read to its end is what lets the connection
/// close with a FIN rather than a reset.
///
/// The drain is not under the request deadline: this layer sits outside it,
/// beside the ceiling, and wraps the whole table as the ceiling does. It acts
/// only on a `413`, so a route that answers with a connection rather than a
/// document — `/watch`, `/mcp` — passes through it untouched, and a document
/// route's `413` is answered under the drain's own shorter bound rather than
/// under whatever the deadline had left.
pub async fn drain_refused_body(request: Request, next: Next) -> Response {
    let declared = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok());
    let (parts, body) = request.into_parts();
    let (body, mut returned) = Retained::wrap(body);
    let response = next.run(Request::from_parts(parts, Body::new(body))).await;

    if response.status() == StatusCode::PAYLOAD_TOO_LARGE
        && let Ok((body, read)) = returned.try_recv()
        && !body.is_end_stream()
    {
        let bound = declared
            .map_or(MAX_BODY_DRAIN_BYTES, |length| length.saturating_sub(read))
            .min(MAX_BODY_DRAIN_BYTES);
        drain(body, bound).await;
    }
    response
}

/// Read and discard up to `bound` bytes of `body`, for at most
/// [`BODY_DRAIN_TIMEOUT`], stopping early at its end or on an error.
async fn drain(mut body: Body, bound: usize) {
    let mut drained = 0;
    let finished = tokio::time::timeout(BODY_DRAIN_TIMEOUT, async {
        while drained < bound {
            match body.frame().await {
                Some(Ok(frame)) => drained += frame.data_ref().map_or(0, Bytes::len),
                Some(Err(_)) | None => break,
            }
        }
    })
    .await;
    // Nothing to do about either bound being hit — the refusal goes out and
    // the connection closes, as it did before there was a drain — but a
    // client that ran into one is worth a line when someone asks why it saw a
    // reset rather than the 413.
    if finished.is_err() {
        debug!(drained, bound, "request body drain hit its deadline before the 413 was sent");
    } else if drained >= bound && !body.is_end_stream() {
        debug!(drained, bound, "request body drain hit its cap before the 413 was sent");
    }
}

/// A request body that comes back to the layer that wrapped it when whoever
/// was reading it lets go.
///
/// Forwards every poll to the body it wraps, counts the data it passed on,
/// and on drop sends both through the channel it was created with. Sending
/// fails only when the receiver is already gone, which is the layer having
/// answered without wanting the body back, and that is fine: the body drops
/// here instead, exactly as it would have with no wrapper at all.
struct Retained {
    inner: Option<Body>,
    read: usize,
    give_back: Option<oneshot::Sender<(Body, usize)>>,
}

impl Retained {
    fn wrap(body: Body) -> (Self, oneshot::Receiver<(Body, usize)>) {
        let (give_back, returned) = oneshot::channel();
        (Self { inner: Some(body), read: 0, give_back: Some(give_back) }, returned)
    }
}

impl HttpBody for Retained {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        let polled = Pin::new(inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &polled
            && let Some(data) = frame.data_ref()
        {
            this.read += data.len();
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().is_none_or(HttpBody::is_end_stream)
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.as_ref().map(HttpBody::size_hint).unwrap_or_default()
    }
}

impl Drop for Retained {
    fn drop(&mut self) {
        if let (Some(body), Some(give_back)) = (self.inner.take(), self.give_back.take()) {
            let _ = give_back.send((body, self.read));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::routing::{get, post};
    use futures::StreamExt;
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

    /// The route table as [`crate::routes::router_with_limits`] lays it out:
    /// a route that reads its body under the ceiling, with the drain outside.
    fn draining_router(max_body_bytes: usize) -> Router {
        Router::new()
            .route("/docs", post(|body: Bytes| async move { body.len().to_string() }))
            .layer(axum::extract::DefaultBodyLimit::max(max_body_bytes))
            .layer(axum::middleware::from_fn(drain_refused_body))
    }

    /// A body of `chunks` pieces of `chunk` bytes, counting how many the
    /// server pulled: the count is the only evidence of a drain, since the
    /// response looks the same with or without one.
    fn counted_body(chunks: usize, chunk: usize) -> (Body, Arc<AtomicUsize>) {
        let pulled = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&pulled);
        let stream = futures::stream::iter((0..chunks).map(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok::<_, std::convert::Infallible>(Bytes::from(vec![b'x'; chunk]))
        }));
        (Body::from_stream(stream), pulled)
    }

    fn upload(body: Body, content_length: Option<usize>) -> Request {
        let mut request = Request::builder().method("POST").uri("/docs");
        if let Some(length) = content_length {
            request = request.header(header::CONTENT_LENGTH, length);
        }
        request.body(body).unwrap()
    }

    #[tokio::test]
    async fn the_rest_of_a_refused_body_is_read_before_the_413_is_answered() {
        // Sixteen chunks against a ceiling of one and a half: the extractor
        // gives up on the second, and without a drain the other fourteen
        // are never asked for.
        let (body, pulled) = counted_body(16, 1024);
        let response = draining_router(1536).oneshot(upload(body, Some(16 * 1024))).await.unwrap();

        assert_eq!(response.status(), 413);
        assert_eq!(pulled.load(Ordering::SeqCst), 16, "the whole body should have been read");
    }

    #[tokio::test]
    async fn a_body_under_the_ceiling_is_not_touched_by_the_drain() {
        let (body, pulled) = counted_body(4, 1024);
        let response = draining_router(1 << 20).oneshot(upload(body, Some(4096))).await.unwrap();

        assert_eq!(response.status(), 200);
        assert_eq!(pulled.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn the_drain_stops_at_the_cap_rather_than_reading_a_body_forever() {
        // Far more than the cap, declared and undeclared alike: the cap must
        // hold on its own, since a chunked body declares nothing.
        for declared in [None, Some(usize::MAX / 2)] {
            let chunk = 64 * 1024;
            let (body, pulled) = counted_body(1 << 20, chunk);
            let response = draining_router(1024).oneshot(upload(body, declared)).await.unwrap();

            assert_eq!(response.status(), 413);
            let read = pulled.load(Ordering::SeqCst) * chunk;
            assert!(read >= MAX_BODY_DRAIN_BYTES, "drained {read} bytes, under the cap");
            // One chunk for the extractor, one of slack for the last frame
            // the drain pulled before it saw it was over the cap.
            assert!(read <= MAX_BODY_DRAIN_BYTES + 2 * chunk, "drained {read} bytes, over the cap");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_sender_that_stalls_is_answered_at_the_drain_deadline() {
        // One chunk over the ceiling, then nothing, forever. Paused time
        // makes the deadline fire without waiting for it.
        let stalled =
            futures::stream::iter([Ok::<_, std::convert::Infallible>(Bytes::from(vec![
                b'x';
                2048
            ]))])
            .chain(futures::stream::pending());
        let response = draining_router(1024)
            .oneshot(upload(Body::from_stream(stalled), Some(1 << 20)))
            .await
            .unwrap();

        assert_eq!(response.status(), 413);
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
