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
use kimmy_api::SharedState;
use kimmy_api::state::Auth;
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
pub fn mcp_router(state: SharedState, allowed_hosts: Vec<String>) -> Router {
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
            move || Ok(KimmyMcp::new(Arc::clone(&state)))
        },
        Arc::new(NeverSessionManager::default()),
        config,
    );

    Router::new()
        .route_service("/mcp", service)
        .layer(axum::middleware::from_fn_with_state(Arc::clone(&state), authenticate))
        .with_state(state)
}

/// Authenticate, then stash the principal where a tool can find it.
///
/// Deliberately calls the API's `Auth` extractor rather than re-reading the
/// header: token parsing, the `insecure_no_auth` escape hatch, and the shape of
/// a 401 are all decided in one place, and this is not a second one.
async fn authenticate(
    State(state): State<SharedState>,
    request: Request,
    next: Next,
) -> Result<Response, kimmy_api::ApiError> {
    use axum::extract::FromRequestParts;

    let (mut parts, body) = request.into_parts();
    let Auth(principal) = Auth::from_request_parts(&mut parts, &state).await?;
    parts.extensions.insert(principal);

    Ok(next.run(Request::from_parts(parts, body)).await)
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
