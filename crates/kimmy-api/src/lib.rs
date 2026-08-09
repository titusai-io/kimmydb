//! HTTP and WebSocket API for KimmyDB.
//!
//! Documents cross this boundary as JSON, using MongoDB's Extended JSON v2
//! conventions for the types JSON cannot express. Everything below this layer
//! speaks BSON.
//!
//! Authorization is an extractor rather than middleware, so a route that needs
//! a principal takes one and a route that does not is visibly public.

#![allow(dead_code)]

pub mod audit;
pub mod error;
pub mod exec;
pub mod json;
pub mod metrics;
pub mod ratelimit;
pub mod routes;
pub mod schema;
pub mod state;
pub mod users;
pub mod vectors;
pub mod watch;

use std::sync::Arc;

use axum::Router;
use kimmy_auth::{TokenIssuer, UserStore};
use kimmy_storage::Engine;
use kimmy_vector::IndexCache;

pub use audit::AuditMode;
pub use error::ApiError;
pub use metrics::Metrics;
pub use ratelimit::{Limiter, RateLimit, RateLimits};
pub use state::{AppState, SharedState};

/// Assemble the shared server state.
///
/// Separate from [`build`] because the MCP server mounts onto the same router
/// and must share *this* state — the same engine handle and the same vector
/// index cache. Handing it a second `AppState` would give the two edges
/// different caches and, worse, make it possible for them to differ in what
/// they consider authenticated.
pub fn state(
    engine: Arc<Engine>,
    tokens: TokenIssuer,
    insecure_no_auth: bool,
    limits: RateLimits,
) -> Result<SharedState, kimmy_auth::AuthError> {
    let users = UserStore::open(&engine)?;
    Ok(Arc::new(AppState {
        engine,
        users,
        tokens,
        vectors: IndexCache::new(),
        insecure_no_auth,
        limits,
        metrics: Metrics::default(),
    }))
}

/// Build the HTTP router for an existing state.
pub fn router(state: SharedState) -> Router {
    routes::router(state)
}

/// Build the application router.
pub fn build(
    engine: Arc<Engine>,
    tokens: TokenIssuer,
    insecure_no_auth: bool,
    limits: RateLimits,
) -> Result<Router, kimmy_auth::AuthError> {
    Ok(routes::router(state(engine, tokens, insecure_no_auth, limits)?))
}
