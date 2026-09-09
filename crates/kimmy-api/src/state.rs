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

/// What confirming a schema change on the cluster's live members found
/// (ADR-140): who applied it or already held it, who could not apply it and
/// skipped it, and who did not answer before the deadline.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DdlConfirmation {
    /// Members that applied the change, or already held it.
    pub confirmed: Vec<kimmy_core::NodeId>,
    /// Members that could not apply the change and skipped it — counted in
    /// their `kimmy_sync_ddl_refused_total` — or that do not hold the
    /// collection it names.
    pub refused: Vec<kimmy_core::NodeId>,
    /// Members that did not answer before the deadline, or that were too far
    /// behind for one push to reach (ADR-143), and why. Each will receive the
    /// change through anti-entropy; the response cannot say when.
    pub pending: Vec<(kimmy_core::NodeId, String)>,
}

/// How this node confirms a schema change on its live members: handed an
/// entry it just minted, pushes it to each member and reports what became
/// of it. Installed by the daemon once clustering is up, since only it holds
/// the member set and the cluster secret; a node with no confirmer says
/// nothing about its peers rather than something false.
pub type DdlConfirmer = Arc<
    dyn Fn(
            kimmy_core::OplogEntry,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = DdlConfirmation> + Send>>
        + Send
        + Sync,
>;

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
    /// What an embedding provider may be handed and where it may be sent
    /// (ADR-115). Consulted when a collection's vector configuration is
    /// accepted and again when a search embeds a query; the worker holds the
    /// same policy for the documents.
    pub providers: kimmy_vector::ProviderPolicy,
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
    /// How a schema change minted here is confirmed on the live members
    /// before its request answers (ADR-140). Set late, like `members`, and
    /// for the same reason; `None` on a node that is not clustered.
    pub(crate) ddl_confirm: std::sync::OnceLock<DdlConfirmer>,
    /// The external identity provider, when one is configured.
    ///
    /// Set late for the same reason `members` is: the router is built before
    /// the task that keeps the key set fresh exists. A `OnceLock` because
    /// there is one issuer for the life of the process (ADR-064) — federation
    /// is not something a running node starts or stops doing.
    pub(crate) federation: std::sync::OnceLock<Arc<crate::federation::Federation>>,
    /// Peers the replication loop has reported as trailing this node by more
    /// than tombstone retention (ADR-085), for `/v1/topology`.
    pub(crate) stale_peers:
        parking_lot::Mutex<std::collections::BTreeMap<kimmy_core::NodeId, StalePeer>>,
    /// Where a local token may be minted from (ADR-100).
    ///
    /// A `OnceLock` like `federation`, and for the same reason: there is one
    /// answer for the life of the process, and a mode that could be flipped
    /// while serving would be one more thing a request could race. Unset reads
    /// as [`LocalLogin::Always`], which is exactly the behaviour that shipped.
    pub(crate) local_login: std::sync::OnceLock<crate::local_login::LocalLogin>,
}

/// A peer that has been away longer than tombstone retention.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StalePeer {
    /// When this node first noticed, in milliseconds since the epoch.
    pub since_ms: u64,
    /// How far the peer trailed at the last round, in milliseconds.
    pub behind_ms: u64,
}

impl AppState {
    /// What the replication loop learned about a peer this round: `Some`
    /// with how far it trails when that exceeds tombstone retention, `None`
    /// when it is within the window. The first report starts the clock;
    /// later ones only refresh the distance, so `since` says how long the
    /// condition has held rather than when it was last seen.
    pub fn report_peer_staleness(&self, node: kimmy_core::NodeId, behind_ms: Option<u64>) {
        let mut stale = self.stale_peers.lock();
        match behind_ms {
            Some(behind_ms) => {
                let since_ms =
                    stale.get(&node).map_or_else(kimmy_storage::physical_now_ms, |s| s.since_ms);
                stale.insert(node, StalePeer { since_ms, behind_ms });
            }
            None => {
                stale.remove(&node);
            }
        }
    }

    /// The stale-rejoiner record for a peer, if it has one.
    pub fn stale_peer(&self, node: kimmy_core::NodeId) -> Option<StalePeer> {
        self.stale_peers.lock().get(&node).copied()
    }

    /// Hand the state the live member set. Called once, after the cluster
    /// starts; a second call is ignored.
    pub fn set_members(&self, members: kimmy_cluster::Members) {
        let _ = self.members.set(members);
    }

    /// What the engine and the vector cache report right now: the block
    /// `/metrics` renders ahead of the process counters, and the one the
    /// OTLP bridge exports beside them (ADR-142). A fresh reading per
    /// caller, so neither surface reports a window the other measured.
    ///
    /// Counts, never names: exposing collection names here would leak the
    /// schema to anything that can reach the metrics port.
    pub fn storage_readings(&self) -> Result<crate::metrics::StorageReadings, ApiError> {
        let databases = self.engine.list_databases()?;
        let mut collections = 0u64;
        for db in &databases {
            collections += self.engine.list_collections(&db.name)?.len() as u64;
        }
        // The kernel's figure for the whole process, beside the two byte
        // gauges that each bound one part of it (ADR-147). Read here, per
        // scrape and per export, for the same reason the engine's numbers
        // are: a gauge that lags is a gauge an alert fires late on.
        let memory = crate::metrics::ProcessMemory::read();
        Ok(crate::metrics::StorageReadings {
            databases: databases.len() as u64,
            collections,
            // Surfaced here, not only on a change stream, so the condition is
            // visible without anyone having been subscribed when it happened.
            unique_violations: self.engine.unique_violations(),
            // redb has a single writer and every commit is an fsync, so this
            // over the request count is what a write actually costs.
            commits: self.engine.commits(),
            // Under `coalesced` the two diverge, and the gap is the win:
            // commits that reached the disk without their own fsync (ADR-088).
            fsyncs: self.engine.fsyncs(),
            commits_grouped: self.engine.grouped_commits(),
            storage_bytes: self.engine.storage_bytes(),
            // An estimate from node count and width, not a heap measurement;
            // what the budget evicts against, so the two agree by construction.
            vector_index_cache_bytes: self.vectors.resident_bytes(),
            process_resident_bytes: memory.resident_bytes,
            process_resident_peak_bytes: memory.peak_resident_bytes,
            index_unkeyed: self.engine.unkeyed_writes(),
            // What a write costs *before* it starts (ADR-151): the wait for
            // the single writer, which no latency figure separates from the
            // work, and the two summaries of it an alert can be written on.
            writer_wait: self.engine.writer_wait(),
            writer_wait_timeouts: self.engine.writer_wait_timeouts(),
            writer_hold_max_us: u64::try_from(self.engine.writer_hold_max().as_micros())
                .unwrap_or(u64::MAX),
            // And what it cost *while* it ran, by what was running
            // (ADR-159): the maximum above is one number about one moment,
            // and says nothing about which path an operator should go and
            // look at.
            writer_hold: self.engine.writer_hold(),
        })
    }

    /// Install the schema-change confirmer, once, when clustering is up.
    pub fn set_ddl_confirmer(&self, confirmer: DdlConfirmer) {
        let _ = self.ddl_confirm.set(confirmer);
    }

    /// The schema-change confirmer, if this node has peers to confirm on.
    pub fn ddl_confirmer(&self) -> Option<&DdlConfirmer> {
        self.ddl_confirm.get()
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

    /// Say where a local token may be minted from. Called once, at startup; a
    /// second call is ignored.
    pub fn set_local_login(&self, mode: crate::local_login::LocalLogin) {
        let _ = self.local_login.set(mode);
    }

    /// Where a local token may be minted from. `Always` until told otherwise.
    pub fn local_login(&self) -> crate::local_login::LocalLogin {
        self.local_login.get().copied().unwrap_or_default()
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

        // The per-principal budget (ADR-099), spent only by a request that has
        // fully authenticated: a refused token is a 401 and never a 429, so
        // the limiter counts principals rather than guesses. Here rather than
        // in a layer because this is the first place the key exists — and
        // because every surface that takes a principal passes through here,
        // so `/mcp` and the change-stream upgrade are covered without a layer
        // each of them would have to remember.
        let limiter = &state.limits.per_principal;
        if !limiter.limit().is_disabled() {
            let issuer =
                principal.federated.then(|| state.federation().map(|f| f.issuer())).flatten();
            let key = principal_key(&principal, issuer.as_deref());
            if let crate::ratelimit::Decision::Limited { retry_after } = limiter.acquire(&key) {
                state.metrics.record_principal_rate_limited();
                warn!(
                    user = %principal.user,
                    federated = principal.federated,
                    "rate-limited an authenticated request by principal"
                );
                return Err(crate::ratelimit::too_many_requests(retry_after));
            }
        }
        Ok(Auth(principal))
    }
}

/// The key a principal's request budget is kept under.
///
/// A local user is its name, which is the token's subject. A federated
/// principal is its issuer *and* its subject, under a different prefix: a
/// provider's `sub` of `root` is not this cluster's `root`, and a limiter that
/// let the two share a budget would let whoever controls one name at the
/// provider spend the other's. The issuer is included even though a node
/// federates with one provider today (ADR-064), so the key stays right if that
/// ever changes, and so the two prefixes can never collide with each other.
fn principal_key(principal: &Principal, issuer: Option<&str>) -> String {
    if principal.federated {
        format!("oidc:{}:{}", issuer.unwrap_or_default(), principal.user)
    } else {
        format!("local:{}", principal.user)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_federated_subject_and_a_local_user_of_the_same_name_have_different_keys() {
        // Whoever controls the name `root` at the provider must not be able to
        // spend, or be charged for, the local root's budget.
        let local = Principal::new("root", Vec::new());
        let mut federated = Principal::new("root", Vec::new());
        federated.federated = true;

        let local_key = principal_key(&local, None);
        let federated_key = principal_key(&federated, Some("https://auth.example.com"));
        assert_ne!(local_key, federated_key);
        assert!(federated_key.contains("https://auth.example.com"), "{federated_key}");
    }

    #[test]
    fn the_same_local_user_always_gets_the_same_key() {
        // Otherwise a principal spread across many addresses would draw on
        // many budgets, which is the case keying on the principal exists for.
        let a = Principal::new("ada", Vec::new());
        let b = Principal::new(
            "ada",
            vec![kimmy_auth::Grant::new("shop", "*", vec![kimmy_auth::Action::Read])],
        );
        assert_eq!(principal_key(&a, None), principal_key(&b, None), "grants are not identity");
    }
}
