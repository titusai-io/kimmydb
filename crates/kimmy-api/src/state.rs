//! Shared server state and the authenticated-principal extractor.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use axum::extract::{ConnectInfo, FromRequestParts};
use axum::http::request::Parts;
use kimmy_auth::{Action, Principal, TokenIssuer, UserStore};
use kimmy_storage::Engine;
use kimmy_vector::IndexCache;
use tracing::warn;

use crate::error::ApiError;
use crate::ratelimit::RateLimits;

pub struct AppState {
    pub engine: Arc<Engine>,
    pub users: UserStore,
    pub tokens: TokenIssuer,
    /// Approximate vector indexes, built lazily and shared across requests.
    /// Held here rather than rebuilt per query — a graph is O(n log n) to
    /// construct, which would cost more than the exact scan it replaces.
    pub vectors: IndexCache,
    /// When set, every request runs as a superuser. Guarded at startup so it
    /// cannot combine with a non-loopback bind.
    pub insecure_no_auth: bool,
    /// Per-caller budgets. Held in shared state rather than per-connection
    /// because a limit that resets when a client reconnects is not a limit.
    pub limits: RateLimits,
    /// Process counters behind `/metrics`.
    pub metrics: crate::metrics::Metrics,
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

/// The address a request appears to come from, as a rate-limiting key.
///
/// A string rather than an [`IpAddr`] because it is only ever a map key, and
/// because there is a case — no connect info at all — where the honest answer is
/// not an address.
pub struct ClientAddr(pub String);

impl ClientAddr {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The key used when the peer address cannot be determined.
    ///
    /// Every such caller shares one budget. That is deliberately the strict
    /// direction: an unknown caller being limited alongside other unknown
    /// callers is a degraded service, whereas handing each one its own budget
    /// would be no limit at all while looking like one.
    const UNKNOWN: &'static str = "unknown";
}

impl FromRequestParts<SharedState> for ClientAddr {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &SharedState,
    ) -> Result<Self, Self::Rejection> {
        if let Some(header) = state.limits.trusted_proxy_header.as_deref()
            && let Some(addr) = forwarded_for(parts, header)
        {
            return Ok(ClientAddr(addr.to_string()));
        }

        let peer = parts.extensions.get::<ConnectInfo<SocketAddr>>().map(|ConnectInfo(a)| a.ip());

        Ok(ClientAddr(match peer {
            Some(ip) => ip.to_string(),
            None => {
                warn_missing_connect_info();
                Self::UNKNOWN.to_string()
            }
        }))
    }
}

/// Read the client address from a forwarded header.
///
/// Takes the **last** entry, not the first. A proxy appends the peer it saw, so
/// the rightmost value is the one written by the hop nearest this server — the
/// only one not supplied by the client. The leftmost is the conventional
/// "original client", and is exactly what an attacker sets to whatever they
/// like; keying a limit on it would let anyone have unlimited budgets by
/// varying a header.
fn forwarded_for(parts: &Parts, header: &str) -> Option<IpAddr> {
    let raw = parts.headers.get(header)?.to_str().ok()?;
    let last = raw.rsplit(',').next()?.trim();
    // `X-Forwarded-For` carries bare addresses, but `Forwarded`-style values and
    // some proxies append a port. Both parse forms are accepted; anything else
    // falls through to the socket peer rather than becoming a key of its own.
    last.parse::<IpAddr>().ok().or_else(|| last.parse::<SocketAddr>().ok().map(|a| a.ip()))
}

/// Warn once, not per request: this is a deployment mistake that would otherwise
/// print on every call for as long as the server runs.
fn warn_missing_connect_info() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        warn!(
            "no connection info on the request, so every caller shares one rate-limit budget; \
             serve the router with into_make_service_with_connect_info::<SocketAddr>()"
        );
    });
}

impl Auth {
    /// Require an action, or fail with a uniform 403.
    ///
    /// This is where the audit record is written, rather than at each caller.
    /// Every authorization in the server — REST, MCP, the change-stream
    /// upgrade, the vector endpoints — funnels through here, and a log each
    /// route has to remember to write is a log with invisible holes in it.
    pub fn require(
        &self,
        action: Action,
        db: &str,
        collection: Option<&str>,
    ) -> Result<(), ApiError> {
        let allowed = self.0.can(action, db, collection);
        crate::audit::record(&self.0, action, db, collection, allowed);
        if allowed {
            return Ok(());
        }
        Err(ApiError::forbidden())
    }

    pub fn principal(&self) -> &Principal {
        &self.0
    }
}
