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
    /// Where a webhook may be pointed. Held in state rather than read per
    /// request so the policy cannot differ between two calls.
    pub egress: crate::egress::EgressPolicy,
    /// Whether a token is still good beyond its signature: the user still
    /// exists, is still enabled, and has not had its tokens invalidated.
    /// Cached, and kept honest by an oplog consumer (ADR-052).
    pub sessions: crate::sessions::Sessions,
    /// The live member set, once clustering has started.
    ///
    /// Set late rather than passed in, because the router is built before the
    /// cluster task exists. A `OnceLock` rather than a lock that can be
    /// rewritten: there is one member set for the life of the process, and
    /// making that expressible is worth more than the flexibility.
    ///
    /// **It holds peers, never this node.** Anything derived from it has to
    /// add `me` explicitly — the omission that silently undelivered every
    /// clustered webhook (ADR-051).
    pub(crate) members: std::sync::OnceLock<kimmy_cluster::Members>,
    /// The external identity provider, when one is configured.
    ///
    /// Set late for the same reason `members` is: the router is built before
    /// the task that keeps the key set fresh exists. A `OnceLock` because
    /// there is one issuer for the life of the process (ADR-064) — federation
    /// is not something a running node starts or stops doing.
    pub(crate) federation: std::sync::OnceLock<Arc<crate::federation::Federation>>,
}

impl AppState {
    /// Hand the state the live member set. Called once, after the cluster
    /// starts; a second call is ignored.
    pub fn set_members(&self, members: kimmy_cluster::Members) {
        let _ = self.members.set(members);
    }

    /// The live member set, if this node is clustered.
    ///
    /// Reading it is safe **because every member is authenticated** — every
    /// membership datagram carries an HMAC over `cluster_secret`, verified
    /// before foca sees it (ADR-053). That invariant now protects more than
    /// webhook ownership: an unauthenticated peer in this set would be
    /// advertised to clients as a node to send credentials to.
    pub fn members(&self) -> Option<&kimmy_cluster::Members> {
        self.members.get()
    }

    /// Hand the state the external identity provider. Called once, at startup;
    /// a second call is ignored.
    pub fn set_federation(&self, federation: Arc<crate::federation::Federation>) {
        let _ = self.federation.set(federation);
    }

    /// The external identity provider, if this node federates with one.
    pub fn federation(&self) -> Option<&Arc<crate::federation::Federation>> {
        self.federation.get()
    }
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

        let token = token.trim();

        // Two verifiers, chosen by the issuer the token *claims* (ADR-064).
        //
        // Reading an unverified claim is safe because of what it decides:
        // which verifier gets to say yes, never whether the answer is yes.
        // Both verifiers pin their own algorithm and their own key, so a
        // forged `iss` only sends a token to a verifier that refuses it — and
        // a token is never offered to both, which is what leaves no
        // algorithm-confusion surface between them.
        let principal = match state.federation() {
            Some(federation) if federation.claims_this_issuer(token) => {
                let mut principal = federation.verify(token)?;
                // The one asymmetry between the two paths. A local token was
                // issued from a user record, so its grants were resolved once
                // at login and are already complete. A federated principal has
                // no record here, so the roles its claims named are still just
                // names — and they are resolved *here*, on every request,
                // because this is the first place that holds both the names and
                // the engine (ADR-073).
                resolve_roles(state, federation, &mut principal)?;
                principal
            }
            _ => state.tokens.verify(token)?,
        };

        // The signature proves the token was issued here and has not expired.
        // It cannot prove the account still exists, is still enabled, or has
        // not been logged out since — which is this (ADR-052). A federated
        // principal has no local record to check, and the check knows that.
        state.sessions.check(&state.engine, &principal)?;
        Ok(Auth(principal))
    }
}

/// Resolve a federated principal's named roles into grants.
///
/// Per request, and that is the whole point: the alternative — resolving the
/// mapping table once when the verifier is built — looks like an obvious cache
/// and silently freezes every federated principal's permissions at startup, so
/// editing a role would change nothing until the node was restarted. That is
/// the opposite of what naming a stored role is for.
///
/// A name that resolves to nothing contributes nothing, which is how a deleted
/// role behaves for the users still naming it.
fn resolve_roles(
    state: &SharedState,
    federation: &crate::federation::Federation,
    principal: &mut Principal,
) -> Result<(), ApiError> {
    if principal.roles.is_empty() {
        return Ok(());
    }
    let mut grants = state.users.roles().grants_for(&state.engine, &principal.roles)?;

    // The named-role half of ADR-067's break-glass boundary. The inline half is
    // a startup refusal, but a *stored* role can be edited to include `admin`
    // at any time, so the only place this can be enforced honestly is here.
    //
    // Filtered rather than refused: a role is shared with the local users who
    // hold it, and some of them legitimately have `admin`. Failing the whole
    // request would take away a federated caller's unrelated, legitimate grants
    // to punish a permission it was never going to be given anyway.
    if !federation.allow_federated_admin() {
        for grant in &mut grants {
            if grant.actions.contains(&Action::Admin) {
                warn_federated_admin_dropped(&principal.user);
                grant.actions.retain(|a| *a != Action::Admin);
            }
        }
        // A grant stripped down to nothing is dropped rather than kept as an
        // empty rule, so `can` has nothing to iterate that could never match.
        grants.retain(|g| !g.actions.is_empty());
    }

    principal.extend_grants(grants);
    Ok(())
}

/// Warn once per process, not per request.
///
/// This fires on a request path, and an operator who has mapped a claim to a
/// role that carries `admin` would otherwise get one line per call for as long
/// as the node runs. Once is enough to explain why a caller is being told no.
fn warn_federated_admin_dropped(user: &str) {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        warn!(
            user = %user,
            "a role held by a federated principal grants the `admin` action, which was dropped: \
             `admin` is reserved to local users (ADR-067). Set auth.oidc.allow_federated_admin = \
             true to permit it."
        );
    });
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
