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
pub mod dispatch;
pub mod egress;
pub mod error;
pub mod exec;
pub mod json;
pub mod metrics;
pub mod ownership;
pub mod ratelimit;
pub mod routes;
pub mod schema;
pub mod sessions;
pub mod state;
pub mod users;
pub mod vectors;
pub mod watch;
pub mod webhooks;

use std::sync::Arc;

use axum::Router;
use kimmy_auth::{TokenIssuer, UserStore};
use kimmy_storage::Engine;
use kimmy_vector::IndexCache;

pub use audit::AuditMode;
pub use error::ApiError;
pub use metrics::Metrics;
pub use ratelimit::{Limiter, RateLimit, RateLimits};
pub use sessions::Sessions;
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
    state_with_egress(engine, tokens, insecure_no_auth, limits, egress::EgressPolicy::default())
}

/// As [`state`], with an egress policy for webhooks.
pub fn state_with_egress(
    engine: Arc<Engine>,
    tokens: TokenIssuer,
    insecure_no_auth: bool,
    limits: RateLimits,
    egress: egress::EgressPolicy,
) -> Result<SharedState, kimmy_auth::AuthError> {
    let users = UserStore::open(&engine)?;
    // Its own handle: the cache reads users on a miss, and threading the
    // state's copy back into itself is not expressible.
    let sessions = sessions::Sessions::new(UserStore::open(&engine)?);
    // Snapshots live beside the database file: the graph is state derived
    // from that file, so it belongs on the same volume, in the same backup
    // story (deliberately not *in* the backup — a restore rebuilds, which is
    // always correct), and dies with the same disk.
    let vectors = match engine.path().parent() {
        Some(dir) => IndexCache::with_snapshot_dir(dir.join("hnsw")),
        None => IndexCache::new(),
    };
    Ok(Arc::new(AppState {
        engine,
        users,
        tokens,
        vectors,
        insecure_no_auth,
        limits,
        metrics: Metrics::default(),
        egress,
        sessions,
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
