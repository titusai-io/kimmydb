//! Mounting `/mcp`, and carrying the principal across the rmcp boundary.
//!
//! rmcp hands a tool handler a [`RequestContext`], not an HTTP request, so the
//! authenticated principal has to travel between the two. The route is:
//!
//! ```text
//! Authorization: Bearer …
//!        │
//!        ▼  axum middleware, using the API's own `Auth` extractor
//!   Principal  ──▶ request extensions
//!        │
//!        ▼  rmcp injects http::request::Parts into the tool context
//!   ctx.extensions[Parts].extensions[Principal]
//! ```
//!
//! Authentication happens in the middleware, **before** rmcp sees the request,
//! so an unauthenticated caller is rejected by the transport rather than by
//! twelve separate tools that each have to remember to check.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use kimmy_api::state::Auth;
use kimmy_api::{RequestLimits, SharedState};
use kimmy_auth::Principal;
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::never::NeverSessionManager,
};
use rmcp::{ErrorData, RoleServer};

use crate::tools::KimmyMcp;

/// Build the MCP router, to be merged into the API router.
///
/// `allowed_hosts` restricts the `Host` header rmcp will accept; an empty list
/// disables that check. See [`mcp_router`]'s note on DNS rebinding below.
pub fn mcp_router(state: SharedState, allowed_hosts: Vec<String>, limits: RequestLimits) -> Router {
    // Stateless. A session would pin a conversation to one node's memory, which
    // is exactly the property M4 replication is meant to avoid needing — and it
    // would let a token authenticated once keep working after it expired. Every
    // POST is authenticated on its own.
    let mut config = StreamableHttpServerConfig::default();
    config.legacy_session_mode = false;
    config.json_response = true;
    // rmcp defaults this to loopback only, as DNS-rebinding protection for a
    // locally running server that has no authentication of its own. KimmyDB is
    // neither: it binds to a network address by default, and the middleware
    // below rejects an unauthenticated request *before* rmcp sees it — which a
    // rebinding attack cannot get past, since it cannot forge a bearer token.
    //
    // So the default here is an empty list, meaning no `Host` check. Keeping
    // rmcp's default would reject every client that reached this server by its
    // real hostname, which is the normal case. Operators who want the check can
    // still ask for it by naming their hosts.
    let config = config.with_allowed_hosts(allowed_hosts);

    let service = StreamableHttpService::new(
        {
            let state = Arc::clone(&state);
            move || Ok(KimmyMcp::new(Arc::clone(&state), limits))
        },
        Arc::new(NeverSessionManager::default()),
        config,
    );

    Router::new()
        .route_service("/mcp", service)
        .layer(axum::middleware::from_fn_with_state((Arc::clone(&state), limits), authenticate))
        .with_state(state)
}

/// Authenticate, then stash the principal where a tool can find it.
///
/// Deliberately calls the API's `Auth` extractor rather than re-reading the
/// header: token parsing, the `insecure_no_auth` escape hatch, and the shape of
/// a 401 are all decided in one place, and this is not a second one.
async fn authenticate(
    State((state, limits)): State<(SharedState, RequestLimits)>,
    request: Request,
    next: Next,
) -> Result<Response, kimmy_api::ApiError> {
    use axum::extract::FromRequestParts;

    let (mut parts, body) = request.into_parts();
    let Auth(principal) = Auth::from_request_parts(&mut parts, &state).await?;
    parts.extensions.insert(principal);

    // rmcp reads its own bodies, with no time bound of its own that this server can see
    // (it takes the stream and reads it in its service); a client that sends the headers
    // and then trickles the body would hold the connection, as it could on REST before
    // ADR-099. The body gets `request_timeout_secs` to arrive. This bounds the *reading*
    // only, not the call: once the body is in, the call runs under the deadline its tool
    // handler sets (`tools::within_deadline`).
    let body = axum::body::Body::new(TimedBody::new(body, limits.request_timeout));
    Ok(next.run(Request::from_parts(parts, body)).await)
}

/// A request body that must arrive within a time: the stream ends with an error if it
/// has not been read to its end by then. Unlike a whole-request deadline it says nothing
/// once the body is complete.
struct TimedBody {
    inner: axum::body::Body,
    timer: std::pin::Pin<Box<tokio::time::Sleep>>,
}

impl TimedBody {
    fn new(inner: axum::body::Body, within: std::time::Duration) -> Self {
        Self { inner, timer: Box::pin(tokio::time::sleep(within)) }
    }
}

#[derive(Debug)]
struct BodyTimedOut;

impl std::fmt::Display for BodyTimedOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the request body did not arrive within server.request_timeout_secs")
    }
}

impl std::error::Error for BodyTimedOut {}

impl http_body::Body for TimedBody {
    type Data = axum::body::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        use std::future::Future;
        use std::task::Poll;
        match std::pin::Pin::new(&mut self.inner).poll_frame(cx) {
            Poll::Ready(frame) => Poll::Ready(frame),
            Poll::Pending => {
                if self.timer.as_mut().poll(cx).is_ready() {
                    Poll::Ready(Some(Err(axum::Error::new(BodyTimedOut))))
                } else {
                    Poll::Pending
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// Recover the principal a tool call is running as.
///
/// A failure here is a wiring bug, not a caller error: the middleware inserts
/// the principal unconditionally, so its absence means `/mcp` was mounted
/// without it. Failing closed is the only safe response — the alternative is a
/// tool that runs with no principal at all.
pub(crate) fn principal(ctx: &RequestContext<RoleServer>) -> Result<Auth, ErrorData> {
    ctx.extensions
        .get::<http::request::Parts>()
        .and_then(|parts| parts.extensions.get::<Principal>())
        .cloned()
        .map(Auth)
        .ok_or_else(|| {
            tracing::error!(
                "MCP tool call carried no principal; /mcp is mounted without its auth layer"
            );
            ErrorData::internal_error("no authenticated principal for this request", None)
        })
}
