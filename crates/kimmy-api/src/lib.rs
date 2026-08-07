//! HTTP and WebSocket API for KimmyDB.
//!
//! Documents cross this boundary as JSON, using MongoDB's Extended JSON v2
//! conventions for the types JSON cannot express. Everything below this layer
//! speaks BSON.
//!
//! Authorization is an extractor rather than middleware, so a route that needs
//! a principal takes one and a route that does not is visibly public.

#![allow(dead_code)]

pub mod error;
pub mod json;
pub mod routes;
pub mod state;
pub mod users;
pub mod vectors;
pub mod watch;

use std::sync::Arc;

use axum::Router;
use kimmy_auth::{TokenIssuer, UserStore};
use kimmy_storage::Engine;
use kimmy_vector::IndexCache;

pub use error::ApiError;
pub use state::{AppState, SharedState};

/// Build the application router.
pub fn build(
    engine: Arc<Engine>,
    tokens: TokenIssuer,
    insecure_no_auth: bool,
) -> Result<Router, kimmy_auth::AuthError> {
    let users = UserStore::open(&engine)?;
    let state: SharedState =
        Arc::new(AppState { engine, users, tokens, vectors: IndexCache::new(), insecure_no_auth });
    Ok(routes::router(state))
}
