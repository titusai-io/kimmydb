//! HTTP routes.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, Path, Query, State};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use opentelemetry_semantic_conventions::attribute as semconv;
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::{Instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::error::ApiError;
use crate::exec;
use crate::json::JsonBody;
use crate::ratelimit::{self, Decision};
use crate::state::{Auth, SharedState};
use crate::watch;

pub fn router(state: SharedState) -> Router {
    router_with(state, None)
}

/// Build the router, optionally merging routes served on the same listener.
///
/// `extra` exists so that anything mounted beside the REST API — `/mcp` is the
/// only caller — is merged **before** [`count_request`] wraps the table, and so
/// ends up inside it. A router merged after the layer keeps its own (empty)
/// middleware stack: `merge` combines route tables, and a layer already applied
/// stays with the routes it was applied to.
///
/// That is not a subtlety worth rediscovering. `/mcp` was merged afterwards, so
/// it answered 401 with no `WWW-Authenticate` — the one case RFC 9728 exists to
/// serve, since an MCP client has no other way to discover its authorization
/// server — and every MCP request was invisible to `/metrics` and to tracing.
pub fn router_with(state: SharedState, extra: Option<Router>) -> Router {
    let mut app = routes(state.clone());
    if let Some(extra) = extra {
        app = app.merge(extra);
    }
    // Counting happens in one layer rather than in each handler: a counter
    // beside a handler is a counter the next route forgets. It wraps
    // everything including `/metrics` itself, so a scrape is visible as
    // traffic rather than being invisible to the thing it scrapes.
    app.layer(axum::middleware::from_fn_with_state(state, count_request))
}

/// The route table, before instrumentation.
fn routes(state: SharedState) -> Router {
    Router::new()
        // Health endpoints are unauthenticated on purpose: a load balancer
        // probing them should not need credentials.
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        // Unauthenticated by specification, not by oversight: RFC 9728 §3 is
        // how a client that holds no credentials yet finds out where to get
        // some. Both shapes are registered because §3 inserts the well-known
        // segment *between* the authority and the path, so a resource
        // identifier with a path is served one level down; the handler refuses
        // anything that is not this node's own resource.
        //
        // Written out rather than built from `PROTECTED_RESOURCE_METADATA_PATH`
        // because the documentation contract in `tests/openapi.rs` scans this
        // file for route *literals* — a computed path is a route that silently
        // escapes it. `the_well_known_route_matches_the_shared_constant` holds
        // the two together instead.
        .route("/.well-known/oauth-protected-resource", get(protected_resource_metadata))
        .route(
            "/.well-known/oauth-protected-resource/{*resource_path}",
            get(protected_resource_metadata),
        )
        .route("/v1/auth/login", post(login))
        .route("/v1/auth/refresh", post(refresh))
        // Public for the same reason the health routes are: a client has to be
        // able to ask what a node supports before it holds a token.
        .route("/v1/version", get(crate::version::version))
        // Authenticated, unlike /v1/version: a version is a fact about
        // software, this is a map of where a deployment's data lives.
        .route("/v1/topology", get(crate::topology::topology))
        .route("/v1/admin/backup", get(backup))
        .route("/v1/auth/whoami", get(crate::users::whoami))
        .route("/v1/users", get(crate::users::list_users).post(crate::users::create_user))
        .route("/v1/users/{name}", get(crate::users::get_user).delete(crate::users::delete_user))
        .route("/v1/users/{name}/password", post(crate::users::set_password))
        .route("/v1/users/{name}/grants", post(crate::users::set_grants))
        .route("/v1/users/{name}/disabled", post(crate::users::set_disabled))
        .route("/v1/users/{name}/roles", post(crate::roles::set_user_roles))
        // Written out as literals, like every route above, because the
        // documentation contract in `tests/openapi.rs` scans this file for
        // route *literals* — a path built from a constant is a route that
        // silently escapes it while the test still passes.
        .route("/v1/roles", get(crate::roles::list_roles).post(crate::roles::create_role))
        .route("/v1/roles/{name}", get(crate::roles::get_role).delete(crate::roles::delete_role))
        .route("/v1/roles/{name}/grants", post(crate::roles::set_role_grants))
        .route("/v1/databases", get(list_databases))
        .route("/v1/db/{db}/collections", get(list_collections).post(create_collection))
        .route("/v1/db/{db}/coll/{coll}", delete(drop_collection))
        .route("/v1/db/{db}/coll/{coll}/docs", post(insert_doc).get(find_docs))
        .route("/v1/db/{db}/coll/{coll}/bulk", post(bulk_insert_docs))
        .route("/v1/db/{db}/coll/{coll}/find", post(find_docs_post))
        .route("/v1/db/{db}/coll/{coll}/count", post(count_docs))
        .route("/v1/db/{db}/coll/{coll}/aggregate", post(aggregate_docs))
        .route("/v1/db/{db}/coll/{coll}/webhooks", get(list_webhooks).post(register_webhook))
        .route("/v1/db/{db}/coll/{coll}/webhooks/{id}", delete(remove_webhook))
        .route("/v1/db/{db}/coll/{coll}/update", post(update_docs))
        .route("/v1/db/{db}/coll/{coll}/find_and_modify", post(find_and_modify))
        .route("/v1/db/{db}/coll/{coll}/delete", post(delete_docs))
        .route(
            "/v1/db/{db}/coll/{coll}/docs/{id}",
            get(get_doc).put(replace_doc).delete(delete_doc),
        )
        .route("/v1/db/{db}/coll/{coll}/describe", get(describe_collection))
        .route("/v1/db/{db}/coll/{coll}/indexes", get(list_indexes).post(create_index))
        .route("/v1/db/{db}/coll/{coll}/indexes/{name}", delete(drop_index))
        .route(
            "/v1/db/{db}/coll/{coll}/vector",
            get(crate::vectors::get_vector_config)
                .post(crate::vectors::configure_vectors)
                .delete(crate::vectors::disable_vectors),
        )
        .route(
            "/v1/db/{db}/coll/{coll}/docs/{id}/vectors",
            get(crate::vectors::get_document_vectors)
                .put(crate::vectors::put_document_vectors)
                .delete(crate::vectors::delete_document_vectors),
        )
        .route("/v1/db/{db}/coll/{coll}/vector_search", post(crate::vectors::vector_search))
        .route("/v1/db/{db}/coll/{coll}/hybrid_search", post(crate::vectors::hybrid_search))
        .route("/v1/db/{db}/coll/{coll}/watch", get(watch::watch_collection))
        .with_state(state)
}

/// Count every response by status, and time the ones that are real traffic.
///
/// Also where a request's trace span is opened, for the same reason the
/// counting is here: a span beside a handler is a span the next route forgets.
async fn count_request(
    State(state): State<SharedState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    // Health probes and scrapes are excluded from the *histogram* — every few
    // seconds forever, they would crowd the buckets the real traffic lands in
    // — but still counted as requests, so a scrape stays visible as traffic.
    // The same predicate excludes them from tracing, and the argument is the
    // same one: a trace every few seconds forever, of a request nobody is
    // debugging, is cost and noise in equal measure.
    let timed = !matches!(request.uri().path(), "/healthz" | "/readyz" | "/metrics");
    let started = std::time::Instant::now();

    // Read before the request is consumed, for the challenge below. RFC 6750 §3
    // makes the two cases different, and *whether credentials were offered* is
    // the only thing that tells them apart — which is knowable here and nowhere
    // downstream.
    let offered_credentials = request.headers().contains_key(axum::http::header::AUTHORIZATION);
    // `/v1/auth/login` answers 401 for a bad password, and it is not a
    // bearer-protected resource. Challenging there would tell a client to come
    // back with a token, which is the opposite of what it should do.
    let challengeable = request.uri().path() != "/v1/auth/login";

    let span = timed.then(|| request_span(&request));
    let mut response = match &span {
        Some(span) => next.run(request).instrument(span.clone()).await,
        None => next.run(request).await,
    };

    if challengeable {
        add_challenge(&state, offered_credentials, &mut response);
    }

    if let Some(span) = &span {
        // `i64`, not `u16`: `tracing-opentelemetry` has no `record_u64`, so an
        // unsigned value falls through to `record_debug` and arrives at the
        // collector as the *string* "200". The semantic conventions say this
        // attribute is an integer, and a backend filtering on it would match
        // nothing.
        span.record(semconv::HTTP_RESPONSE_STATUS_CODE, i64::from(response.status().as_u16()));
    }
    if timed {
        state.metrics.record_latency(started.elapsed());
    }
    state.metrics.record_request(response.status().as_u16());
    response
}

/// Answer a refusal with the challenge RFC 6750 §3 requires.
///
/// # Why this is a layer and not part of `ApiError`
///
/// A 401 arrives here from two unrelated places: the `Auth` extractor building
/// one directly, and `From<AuthError>` converting whatever a verifier returned.
/// The conversion has no access to state, so it cannot know the resource
/// metadata URL, and threading state into it would mean touching every call
/// site to carry something only this header wants. One layer that already wraps
/// every route, and already holds the request, answers all of it — including
/// the part `ApiError` fundamentally cannot see, which is whether the caller
/// offered credentials at all.
///
/// # The two 401s are deliberately different
///
/// A request with **no** credentials gets a bare challenge and **no** `error`
/// code: RFC 6750 §3 says a client that has not yet tried should not be told it
/// failed. A request with a **bad** one gets `invalid_token`, because that is
/// actionable — it means refresh and retry rather than "you are not welcome".
/// 403 takes `insufficient_scope` (§3.1), which says nothing the body does not
/// already say: it still does not distinguish a collection that is missing from
/// one the caller may not have, and a test holds that.
fn add_challenge(
    state: &SharedState,
    offered_credentials: bool,
    response: &mut axum::response::Response,
) {
    use axum::http::header::WWW_AUTHENTICATE;

    let error = match response.status() {
        axum::http::StatusCode::UNAUTHORIZED if offered_credentials => {
            Some((r#"error="invalid_token""#, "the access token is expired, revoked or malformed"))
        }
        axum::http::StatusCode::UNAUTHORIZED => None,
        axum::http::StatusCode::FORBIDDEN => Some((
            r#"error="insufficient_scope""#,
            "the authenticated principal holds no grant covering this operation",
        )),
        _ => return,
    };
    // Never clobber one a handler set for itself. Nothing does today, and a
    // silent overwrite is the kind of thing that stays invisible until it is
    // the bug.
    if response.headers().contains_key(WWW_AUTHENTICATE) {
        return;
    }

    let mut challenge = String::from(r#"Bearer realm="kimmydb""#);
    if let Some((code, description)) = error {
        challenge.push_str(", ");
        challenge.push_str(code);
        challenge.push_str(&format!(r#", error_description="{description}""#));
    }
    // RFC 9728 §5.1. Absent when the audience is an opaque string, which is a
    // configuration this node supports rather than a gap: there is simply no
    // document to point at.
    if let Some(url) = state.federation().and_then(|f| f.resource_metadata_url()) {
        challenge.push_str(&format!(r#", resource_metadata="{url}""#));
    }

    match axum::http::HeaderValue::from_str(&challenge) {
        Ok(value) => {
            response.headers_mut().insert(WWW_AUTHENTICATE, value);
        }
        // Only reachable through a configured resource identifier containing a
        // character no header may carry. Dropping the header beats panicking on
        // the error path, and the refusal itself is unaffected.
        Err(e) => warn!(error = %e, "could not encode the WWW-Authenticate challenge"),
    }
}

/// The span for one HTTP request, parented to whatever sent the request.
///
/// **Named from axum's `MatchedPath`, not the URI.** The matched path is the
/// route *template* — `/v1/db/{db}/coll/{coll}/docs` — which is low-cardinality
/// (a trace backend groups by span name, and a name carrying an `_id` produces
/// one group per document) *and* carries no database or collection name, so the
/// default span name is private by construction rather than by redaction. The
/// raw path has the names in it, which is why `url.path` is behind
/// `telemetry.include_names` (ADR-068).
fn request_span(request: &axum::extract::Request) -> tracing::Span {
    let route = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|m| m.as_str().to_string())
        // A request that matched no route still gets a span, under a constant
        // rather than under its URI: the 404 path is exactly where an attacker
        // chooses the string, and a span name is a dashboard dimension.
        .unwrap_or_else(|| "unmatched".to_string());

    let span = tracing::info_span!(
        "http.request",
        otel.name = %route,
        otel.kind = "server",
        { semconv::HTTP_REQUEST_METHOD } = %request.method(),
        { semconv::HTTP_ROUTE } = %route,
        { semconv::HTTP_RESPONSE_STATUS_CODE } = tracing::field::Empty,
        { semconv::CLIENT_ADDRESS } = tracing::field::Empty,
        { semconv::URL_PATH } = tracing::field::Empty,
    );

    if let Some(ConnectInfo(peer)) = request.extensions().get::<ConnectInfo<SocketAddr>>() {
        span.record(semconv::CLIENT_ADDRESS, peer.ip().to_string());
    }
    if crate::telemetry::include_names() {
        span.record(semconv::URL_PATH, request.uri().path());
    }

    // Continue the caller's trace rather than starting a new one. Without a
    // propagator installed — which is every build that has not configured a
    // collector — `extract` returns an empty context, and with no OTel layer in
    // the subscriber `set_parent` answers `LayerNotFound`. Both are the ordinary
    // telemetry-off path rather than a failure, which is why the result is
    // dropped: there is nothing to report and nobody to report it to.
    let parent = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(request.headers()))
    });
    let _ = span.set_parent(parent);
    span
}

/// Read `traceparent` and `tracestate` out of a request's headers.
///
/// Hand-written rather than taken from `opentelemetry-http`: that crate exists
/// to carry an HTTP *client*, and pulling it in would put a second `reqwest`
/// major version into this crate's tree for two accessor methods.
struct HeaderExtractor<'a>(&'a axum::http::HeaderMap);

impl opentelemetry::propagation::Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|name| name.as_str()).collect()
    }
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

async fn healthz() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// Prometheus-format metrics.
///
/// Unauthenticated like the health endpoints, and deliberately limited to
/// counts: exposing collection *names* here would leak the schema to anything
/// that can reach the port.
async fn metrics(State(state): State<SharedState>) -> Result<String, ApiError> {
    let databases = state.engine.list_databases()?;
    let mut collections = 0usize;
    for db in &databases {
        collections += state.engine.list_collections(&db.name)?.len();
    }

    Ok(format!(
        "# HELP kimmy_databases Number of databases.\n\
         # TYPE kimmy_databases gauge\n\
         kimmy_databases {databases_count}\n\
         # HELP kimmy_collections Number of collections across all databases.\n\
         # TYPE kimmy_collections gauge\n\
         kimmy_collections {collections}\n\
         # HELP kimmy_unique_violations Unique constraints broken by merging replicated writes.\n\
         # TYPE kimmy_unique_violations counter\n\
         kimmy_unique_violations {violations}\n\
         # HELP kimmy_commits Durable write transactions committed by the storage engine.\n\
         # TYPE kimmy_commits counter\n\
         kimmy_commits {commits}\n\
         # HELP kimmy_storage_bytes Size of the database file on disk.\n\
         # TYPE kimmy_storage_bytes gauge\n\
         kimmy_storage_bytes {storage}\n\
         # HELP kimmy_up Always 1; presence indicates the node is serving.\n\
         # TYPE kimmy_up gauge\n\
         kimmy_up 1\n\
         {process}",
        databases_count = databases.len(),
        // Surfaced here, not only on a change stream, so the condition is
        // visible without anyone having been subscribed when it happened.
        violations = state.engine.unique_violations(),
        // redb has a single writer and every commit is an fsync, so this over
        // the request count is what a write actually costs. A client-visible
        // write that costs two commits costs twice as much as one that costs
        // one, and no latency figure says which of those is happening.
        commits = state.engine.commits(),
        storage = state.engine.storage_bytes(),
        process = state.metrics.render(),
    ))
}

/// This node, described as an OAuth 2.0 protected resource (RFC 9728).
///
/// Served only when federation is configured *and* the audience is an https
/// URI, because only then does this node have a name an authorization server
/// knows it by. A bare-string audience gets a 404 — there is nothing truthful
/// to publish, and publishing an identifier no token will ever carry would send
/// every client to ask for a `resource` the provider refuses.
///
/// The payoff is `kimmy login --oidc` with nothing but `--url`: the client
/// reads this document off the node it is about to use and learns both who to
/// authenticate with and what to ask the token to be for. It is also exactly
/// how the MCP authorization specification says an MCP client discovers where
/// to authenticate, which matters here because `/mcp` is on this same listener.
async fn protected_resource_metadata(
    State(state): State<SharedState>,
    uri: axum::http::Uri,
) -> Result<Json<Value>, ApiError> {
    let federation = state.federation().ok_or_else(not_a_protected_resource)?;
    let served_at = federation.resource_metadata_path().ok_or_else(not_a_protected_resource)?;
    // The wildcard route means this handler also sees requests for *another*
    // resource's metadata on the same host. Answering those with this node's
    // document would be a lie, and one a client would act on.
    if uri.path().trim_end_matches('/') != served_at.trim_end_matches('/') {
        return Err(not_a_protected_resource());
    }
    let metadata = federation.protected_resource_metadata().ok_or_else(not_a_protected_resource)?;
    Ok(Json(metadata))
}

fn not_a_protected_resource() -> ApiError {
    ApiError::not_found("this node publishes no protected resource metadata")
}

/// Readiness differs from liveness: it proves the storage engine responds, so
/// a node with a wedged database is taken out of rotation rather than served.
async fn readyz(State(state): State<SharedState>) -> Result<Json<Value>, ApiError> {
    state.engine.list_databases()?;
    Ok(Json(json!({ "status": "ready", "node": state.engine.node_id().to_string() })))
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct LoginRequest {
    user: String,
    password: String,
}

/// Exchange credentials for a token.
///
/// Rate-limited, and the limit is checked *before* `authenticate` rather than
/// after: every attempt runs a full Argon2id verification — including for a user
/// that does not exist, which is what stops timing from revealing whether one
/// does — so an unthrottled endpoint hands an anonymous caller ~19 MB and
/// milliseconds of CPU per request. Checking afterwards would return 429 while
/// still doing all the work it was meant to prevent.
///
/// Only *failed* attempts are recorded. A caller with correct credentials is not
/// the thing being defended against, and a fleet re-authenticating on a short
/// `token_ttl_secs` must not be throttled for succeeding.
async fn login(
    State(state): State<SharedState>,
    client: crate::state::ClientAddr,
    JsonBody(body): JsonBody<LoginRequest>,
) -> Result<Json<Value>, ApiError> {
    let limits = &state.limits;
    if let Decision::Limited { retry_after } = limits.login_ip.check(client.as_str()) {
        warn!(client = client.as_str(), "rate-limited a login attempt by source address");
        return Err(ratelimit::too_many_requests(retry_after));
    }
    // Keyed on the name as typed. Normalizing would let `Root` and `root` be
    // told apart by the limiter while the user store treats them as one.
    if let Decision::Limited { retry_after } = limits.login_user.check(&body.user) {
        warn!(user = %body.user, "rate-limited a login attempt by username");
        return Err(ratelimit::too_many_requests(retry_after));
    }

    let principal = match state.users.authenticate(&state.engine, &body.user, &body.password) {
        Ok(principal) => principal,
        Err(e) => {
            limits.login_ip.record(client.as_str());
            limits.login_user.record(&body.user);
            return Err(e.into());
        }
    };

    let token = state.tokens.issue(&principal)?;
    Ok(Json(json!({
        "token": token,
        "user": principal.user,
        "expiresIn": state.tokens.ttl_secs(),
    })))
}

/// Exchange a valid token for a fresh one.
///
/// **Sliding re-issue, not a second credential** (ADR-059). A client that is
/// using the API never has to hold credentials past login; one that has been
/// idle longer than the lifetime logs in again, which is a thing a library may
/// ask of an application where doing it every hour is not.
///
/// # This is not a way back into a revoked session
///
/// The route takes `Auth`, so the presented token goes through the whole check
/// every other route does — signature, expiry, and then the storage read that
/// ADR-052 exists for. A token whose account was deleted, disabled, or had its
/// password or grants changed is refused *before this runs*. Refresh cannot
/// launder any of that, which is the failure this route most needed not to
/// have.
///
/// The new token is built from a **fresh read of the user record**, not from
/// the old token's claims: grants and token version come from storage. Today
/// those cannot have changed — a change bumps the version and the extractor
/// would have refused — but the day that stops being true, this route must not
/// be the place that carries stale authority forward.
///
/// Not rate-limited. The login limiter exists to bound Argon2 work, and there
/// is none here; this route verifies a signature and reads one record.
async fn refresh(State(state): State<SharedState>, auth: Auth) -> Result<Json<Value>, ApiError> {
    if auth.principal().unauthenticated {
        return Err(ApiError::bad_request(
            "this node runs with authentication disabled, so there is no token to refresh",
        ));
    }
    // A federated token is the identity provider's to renew, not this node's.
    // Refused explicitly rather than left to fail on the user lookup below,
    // which would report "this token is no longer valid" — true of nothing
    // here, and it would send the caller to log in again by the wrong route.
    // Minting a local token from a federated principal is also the one way an
    // IdP identity could shed its origin flag and outlive the provider's say
    // in it (ADR-065).
    if auth.principal().federated {
        return Err(ApiError::bad_request(
            "this token came from the external identity provider, which is what renews it; \
             this node cannot issue a replacement",
        ));
    }

    let name = &auth.principal().user;
    let user = state
        .users
        .get(&state.engine, name)?
        // Unreachable through the extractor, which checks the same record —
        // but the record is the authority and this is the read that uses it.
        .filter(|u| !u.disabled)
        .ok_or_else(|| ApiError::unauthorized("this token is no longer valid; log in again"))?;

    let principal =
        kimmy_auth::Principal::new(user.name, user.grants).at_version(user.token_version);
    let token = state.tokens.issue(&principal)?;
    Ok(Json(json!({
        "token": token,
        "user": principal.user,
        "expiresIn": state.tokens.ttl_secs(),
    })))
}

// ---------------------------------------------------------------------------
// Backup
// ---------------------------------------------------------------------------

/// Stream a consistent backup of this node.
///
/// Requires `admin` over `*` — the same bar as managing users, and for the same
/// reason: a backup is *every* document on the node, so anything less would let
/// a database-scoped administrator read past their own grants. RBAC is not
/// consulted per collection here; there is no filtered backup, because a partial
/// backup that looks like a whole one is a restore that silently loses data.
///
/// Buffered rather than streamed as it is produced: the backup runs inside a
/// read transaction, and holding that open across a slow client's socket would
/// pin redb's MVCC pages for as long as the client cared to dawdle. Memory is
/// the cheaper cost, and it is bounded by the database rather than by the
/// caller.
async fn backup(
    State(state): State<SharedState>,
    auth: Auth,
) -> Result<axum::response::Response, ApiError> {
    auth.require(kimmy_auth::Action::Admin, "*", None)?;

    let mut buf = Vec::new();
    let info = state.engine.backup_to(&mut buf)?;
    state.metrics.record_backup();
    warn!(
        records = info.records,
        bytes = info.bytes,
        user = %auth.principal().user,
        "served a backup"
    );

    let filename = format!("kimmy-{}-{}.backup", state.engine.node_id(), info.created_ms);
    Ok((
        [
            (axum::http::header::CONTENT_TYPE, "application/octet-stream".to_string()),
            (
                axum::http::header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
        ],
        buf,
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// Databases and collections
// ---------------------------------------------------------------------------

async fn list_databases(
    State(state): State<SharedState>,
    auth: Auth,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::list_databases(&state, &auth)?))
}

async fn list_collections(
    State(state): State<SharedState>,
    auth: Auth,
    Path(db): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::list_collections(&state, &auth, &db)?))
}

#[derive(Deserialize)]
struct CreateCollectionRequest {
    name: String,
}

async fn create_collection(
    State(state): State<SharedState>,
    auth: Auth,
    Path(db): Path<String>,
    JsonBody(body): JsonBody<CreateCollectionRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::create_collection(&state, &auth, &db, &body.name)?))
}

async fn drop_collection(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::drop_collection(&state, &auth, &db, &coll)?))
}

// ---------------------------------------------------------------------------
// Documents
// ---------------------------------------------------------------------------

async fn insert_doc(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    JsonBody(body): JsonBody<Value>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::insert(&state, &auth, &db, &coll, &body)?))
}

/// Insert an array of documents in one commit.
///
/// A sibling of the other multi-document verbs rather than a child of `/docs`,
/// which is already the single-document path — `/docs/bulk` would shadow the
/// document whose `_id` is `"bulk"`.
async fn bulk_insert_docs(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    // `JsonBody` rather than the `Result<Json<_>, JsonRejection>` this route
    // used to spell out: the envelope now comes from the extractor, so it is
    // not something eighteen other handlers can forget.
    JsonBody(documents): JsonBody<Vec<Value>>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::insert_many(&state, &auth, &db, &coll, &documents)?))
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct FindRequest {
    filter: Option<Value>,
    sort: Option<Value>,
    projection: Option<Value>,
    limit: Option<usize>,
    skip: Option<usize>,
    /// Report how the query was answered alongside the results.
    explain: bool,
    /// Resume after a previous page, using the `nextCursor` it returned.
    cursor: Option<String>,
}

impl From<FindRequest> for exec::FindParams {
    fn from(r: FindRequest) -> Self {
        exec::FindParams {
            filter: r.filter,
            sort: r.sort,
            projection: r.projection,
            limit: r.limit,
            skip: r.skip,
            explain: r.explain,
            cursor: r.cursor,
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct FindQuery {
    limit: Option<usize>,
    skip: Option<usize>,
}

async fn find_docs(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    Query(q): Query<FindQuery>,
) -> Result<Json<Value>, ApiError> {
    let params = exec::FindParams { limit: q.limit, skip: q.skip, ..Default::default() };
    Ok(Json(exec::find(&state, &auth, &db, &coll, params)?))
}

async fn find_docs_post(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    JsonBody(body): JsonBody<FindRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::find(&state, &auth, &db, &coll, body.into())?))
}

async fn count_docs(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    JsonBody(body): JsonBody<FindRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::count(&state, &auth, &db, &coll, body.into())?))
}

#[derive(Deserialize)]
struct AggregateRequest {
    pipeline: Value,
}

async fn aggregate_docs(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    JsonBody(body): JsonBody<AggregateRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::aggregate(&state, &auth, &db, &coll, &body.pipeline)?))
}

// ---------------------------------------------------------------------------
// Webhooks
// ---------------------------------------------------------------------------

async fn register_webhook(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    JsonBody(body): JsonBody<crate::webhooks::RegisterRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(crate::webhooks::register(&state, &auth, &db, &coll, &body, &state.egress)?))
}

async fn list_webhooks(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(crate::webhooks::list(&state, &auth, &db, &coll)?))
}

async fn remove_webhook(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll, id)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(crate::webhooks::remove(&state, &auth, &db, &coll, &id)?))
}

async fn get_doc(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll, id)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::get_doc(&state, &auth, &db, &coll, &id)?))
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ReplaceQuery {
    upsert: bool,
}

async fn replace_doc(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll, id)): Path<(String, String, String)>,
    Query(q): Query<ReplaceQuery>,
    JsonBody(body): JsonBody<Value>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::replace(&state, &auth, &db, &coll, &id, &body, q.upsert)?))
}

async fn delete_doc(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll, id)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::delete_by_id(&state, &auth, &db, &coll, &id)?))
}

#[derive(Deserialize)]
struct FindAndModifyRequest {
    #[serde(default)]
    filter: Option<Value>,
    /// Chooses which document when several match. Without it the choice is the
    /// scan's own order, which is unspecified.
    #[serde(default)]
    sort: Option<Value>,
    /// Operators, or a whole replacement document.
    #[serde(default)]
    update: Option<Value>,
    #[serde(default)]
    remove: bool,
    #[serde(default)]
    upsert: bool,
    /// `"before"` (default) or `"after"`.
    #[serde(default, rename = "returnDocument")]
    return_document: Option<String>,
    #[serde(default)]
    projection: Option<Value>,
}

async fn find_and_modify(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    JsonBody(body): JsonBody<FindAndModifyRequest>,
) -> Result<Json<Value>, ApiError> {
    let return_document = match body.return_document.as_deref() {
        None | Some("before") => exec::ReturnDocument::Before,
        Some("after") => exec::ReturnDocument::After,
        Some(other) => {
            return Err(ApiError::bad_request(format!(
                "unknown returnDocument {other:?}: expected \"before\" or \"after\""
            )));
        }
    };

    let spec = exec::FindAndModifySpec {
        filter: body.filter,
        sort: body.sort,
        update: body.update,
        remove: body.remove,
        upsert: body.upsert,
        return_document,
        projection: body.projection,
    };
    Ok(Json(exec::find_and_modify(&state, &auth, &db, &coll, spec)?))
}

#[derive(Deserialize)]
struct UpdateRequest {
    #[serde(default)]
    filter: Option<Value>,
    update: Value,
    #[serde(default)]
    multi: bool,
    /// Report how the targets were found, as `find` does.
    #[serde(default)]
    explain: bool,
}

async fn update_docs(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    JsonBody(body): JsonBody<UpdateRequest>,
) -> Result<Json<Value>, ApiError> {
    let params =
        exec::WriteParams { filter: body.filter, multi: body.multi, explain: body.explain };
    Ok(Json(exec::update(&state, &auth, &db, &coll, &body.update, params)?))
}

#[derive(Deserialize)]
struct DeleteRequest {
    #[serde(default)]
    filter: Option<Value>,
    #[serde(default)]
    multi: bool,
    /// Report how the targets were found, as `find` does.
    #[serde(default)]
    explain: bool,
}

async fn delete_docs(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    JsonBody(body): JsonBody<DeleteRequest>,
) -> Result<Json<Value>, ApiError> {
    let params =
        exec::WriteParams { filter: body.filter, multi: body.multi, explain: body.explain };
    Ok(Json(exec::delete(&state, &auth, &db, &coll, params)?))
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
struct DescribeQuery {
    sample: Option<usize>,
    /// Include one example value per field.
    examples: bool,
}

async fn describe_collection(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    Query(q): Query<DescribeQuery>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(crate::schema::describe_collection(&state, &auth, &db, &coll, q.sample, q.examples)?))
}

// ---------------------------------------------------------------------------
// Indexes
// ---------------------------------------------------------------------------

/// One field of an index definition.
///
/// An *array* rather than a `{field: 1}` object, deliberately: field order
/// decides which queries a compound index can answer, and JSON object key
/// order is not something a client can rely on surviving serialization.
#[derive(Deserialize)]
struct IndexFieldSpec {
    path: String,
    #[serde(default)]
    descending: bool,
}

#[derive(Deserialize)]
struct CreateIndexRequest {
    fields: Vec<IndexFieldSpec>,
    #[serde(default)]
    unique: bool,
    #[serde(default)]
    name: Option<String>,
    /// `"local"` (default) or `"coordinated"`. See the storage docs — a
    /// coordinated unique constraint needs clustering and is refused until M4.
    #[serde(default)]
    enforcement: Option<String>,
    /// Present makes this a TTL index: documents are deleted this many seconds
    /// after the single indexed date field.
    #[serde(default, rename = "expireAfterSeconds")]
    expire_after_seconds: Option<i64>,
    /// Present makes this a partial index: only matching documents are held,
    /// and the planner uses it only for queries provably contained by it.
    #[serde(default, rename = "partialFilterExpression")]
    partial_filter_expression: Option<Value>,
}

async fn create_index(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    JsonBody(body): JsonBody<CreateIndexRequest>,
) -> Result<Json<Value>, ApiError> {
    let spec = exec::IndexSpec {
        fields: body
            .fields
            .into_iter()
            .map(|f| exec::IndexFieldSpec { path: f.path, descending: f.descending })
            .collect(),
        unique: body.unique,
        name: body.name,
        enforcement: body.enforcement,
        expire_after_seconds: body.expire_after_seconds,
        partial_filter_expression: body.partial_filter_expression,
    };
    Ok(Json(exec::create_index(&state, &auth, &db, &coll, spec)?))
}

async fn list_indexes(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::list_indexes(&state, &auth, &db, &coll)?))
}

async fn drop_index(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll, name)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::drop_index(&state, &auth, &db, &coll, &name)?))
}

// What this file's registrations are checked against lives in
// `tests/openapi.rs`: that every route here appears in `docs/openapi.yaml`,
// that every operation the specification describes is registered here, and
// that every route also appears in the prose reference — one scanner for all
// three. The check that used to sit in this module matched `.route("` at the
// start of a line, which silently skipped the registrations rustfmt breaks
// across lines.
