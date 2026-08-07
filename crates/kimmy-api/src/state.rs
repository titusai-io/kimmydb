//! Shared server state and the authenticated-principal extractor.

use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use kimmy_auth::{Action, Principal, TokenIssuer, UserStore};
use kimmy_storage::Engine;

use crate::error::ApiError;

pub struct AppState {
    pub engine: Arc<Engine>,
    pub users: UserStore,
    pub tokens: TokenIssuer,
    /// When set, every request runs as a superuser. Guarded at startup so it
    /// cannot combine with a non-loopback bind.
    pub insecure_no_auth: bool,
}

pub type SharedState = Arc<AppState>;

/// An authenticated caller, extracted from the `Authorization: Bearer` header.
///
/// Implemented as an extractor so a handler cannot forget it: a route that
/// needs a principal takes one, and a route that does not is visibly public.
pub struct Auth(pub Principal);

impl FromRequestParts<SharedState> for Auth {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &SharedState,
    ) -> Result<Self, Self::Rejection> {
        if state.insecure_no_auth {
            return Ok(Auth(Principal::insecure_root()));
        }

        let header = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| ApiError::unauthorized("missing Authorization header"))?;

        let token = header
            .strip_prefix("Bearer ")
            .ok_or_else(|| ApiError::unauthorized("expected an Authorization: Bearer token"))?;

        Ok(Auth(state.tokens.verify(token.trim())?))
    }
}

impl Auth {
    /// Require an action, or fail with a uniform 403.
    pub fn require(
        &self,
        action: Action,
        db: &str,
        collection: Option<&str>,
    ) -> Result<(), ApiError> {
        if self.0.can(action, db, collection) {
            return Ok(());
        }
        Err(ApiError::forbidden())
    }

    pub fn principal(&self) -> &Principal {
        &self.0
    }
}
