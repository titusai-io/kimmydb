//! HTTP routes.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, Path, State};
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
use crate::json::{JsonBody, QueryParams};
use crate::limits::RequestLimits;
use crate::ratelimit::{self, Decision};
use crate::state::{Auth, SharedState};
use crate::watch;

pub fn router(state: SharedState) -> Router {
    router_with(state, None)
}

/// [`router_with_limits`] with the defaults, which are what the server
/// enforced before the limits were settings.
pub fn router_with(state: SharedState, extra: Option<Router>) -> Router {
    router_with_limits(state, extra, RequestLimits::default())
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
///
/// `extra` is deliberately **outside** the request deadline (ADR-099): `/mcp`
/// is a streaming transport whose responses may be held open, and it is merged
/// here after [`routes`] has already applied the deadline to the routes that
/// take one. It is inside the body ceiling, though rmcp reads its own bodies
/// under its own limit, so the ceiling reaches it only in name.
pub fn router_with_limits(
    state: SharedState,
    extra: Option<Router>,
    limits: RequestLimits,
) -> Router {
    let mut app = routes(state.clone(), limits);
    if let Some(extra) = extra {
        app = app.merge(extra);
    }
    app
        // The body ceiling is one layer over the whole table rather than an
        // argument to each `JsonBody`, for the reason the counter below is: a
        // limit set beside a handler is a limit the next route forgets. The
        // default is exactly what axum applied when nothing set one, so an
        // operator who never touches the setting sees no change (ADR-099).
        .layer(axum::extract::DefaultBodyLimit::max(limits.max_body_bytes))
        // Outside the ceiling, so it sees the ceiling's refusal: a 413 is
        // written while the client may still be sending, and closing on the
        // unread rest answers it with a reset that can take the 413 with it.
        // The drain reads a bounded remainder first. Beside the ceiling
        // rather than beside the deadline for the reason the ceiling is
        // here: it belongs to every route the ceiling reaches.
        .layer(axum::middleware::from_fn(crate::limits::drain_refused_body))
        // Counting happens in one layer rather than in each handler: a counter
        // beside a handler is a counter the next route forgets. It wraps
        // everything including `/metrics` itself, so a scrape is visible as
        // traffic rather than being invisible to the thing it scrapes. It is
        // outermost, so a refusal the deadline or the ceiling makes is counted
        // and traced like any other response.
        .layer(axum::middleware::from_fn_with_state(state, count_request))
}

/// The route table, before instrumentation.
///
/// Two groups, because the request deadline is applied per group rather than
/// to the table as a whole (ADR-099): [`timed_routes`] answer a request with a
/// document and take the deadline; [`streaming_routes`] answer with a
/// connection and are exempt. Kept as two functions rather than one table
/// with a per-route layer so the exemption is a place a route is registered,
/// visible in a diff, rather than an attribute on one line of forty.
fn routes(state: SharedState, limits: RequestLimits) -> Router {
    let timed = timed_routes()
        .layer(axum::middleware::from_fn_with_state(limits, crate::limits::enforce_timeout));
    Router::new()
        .merge(timed)
        .merge(streaming_routes())
        // Over the whole REST table, timed and streaming alike, and applied
        // here rather than in `router_with_limits` so that `/mcp` — merged
        // there, afterwards — stays outside it: MCP is a separate transport
        // with query semantics of its own, and rmcp is the one that reads
        // them (ADR-124). A layer applied to a router stays with the routes
        // it was applied to when that router is merged into another, which
        // is the property `router_with_limits` relies on in the other
        // direction.
        .layer(axum::middleware::from_fn(refuse_unread_query_string))
        .with_state(state)
}

/// Routes whose response is a connection rather than a document, and which
/// therefore carry no request deadline.
///
/// Only the change-stream upgrade today. `/mcp` is the other streaming
/// surface, and it is exempt by being merged after the deadline is applied —
/// see [`router_with_limits`]. Nothing else on this server long-polls or
/// streams: `/v1/admin/backup` is buffered before it is sent, and every
/// document route answers in one piece.
///
/// The exemption is the contract rather than a mechanism the upgrade needs
/// today: axum hands the upgraded socket to a task of its own once the `101`
/// is written, so the handler future the deadline wraps has already finished
/// when the stream begins. Registering the route here is what keeps that true
/// if the upgrade ever moves into the handler, and what a test can hold.
fn streaming_routes() -> Router<SharedState> {
    Router::new().route("/v1/db/{db}/coll/{coll}/watch", get(watch::watch_collection))
}

/// Every route that answers with a document, public and authenticated alike.
fn timed_routes() -> Router<SharedState> {
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
        .route("/v1/db/{db}", delete(drop_database))
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
        .route("/v1/db/{db}/coll/{coll}/violations", get(list_violations))
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
}

/// The routes that read their query string, as `(method, route template)`,
/// each template exactly the literal it is registered with above.
///
/// **This table only opens routes; it never closes one.** The guard,
/// [`refuse_unread_query_string`], answers `400` to any query string on a
/// `(method, route)` that is not listed, so a route added above is closed to
/// query strings until someone lists it here — and listing it is only right
/// once its handler takes a `QueryParams<T>` that reads them. That is the
/// direction that makes ADR-121 hold by construction rather than by
/// diligence: forgetting a route produces one that refuses too much, which
/// the first request with a parameter reports by name, instead of one that
/// reads `?if_stamp=…` as nothing and rewrites the document (ADR-124).
///
/// Every handler that takes `QueryParams<T>` is here, and the contract test in
/// `tests/openapi.rs` holds this table equal to the set of operations
/// `docs/openapi.yaml` gives a query parameter, so a parameter documented on a
/// route this table does not open, or opened here and documented nowhere, is
/// a failing test rather than a route that quietly behaves otherwise.
pub const QUERY_STRING_ROUTES: [(&str, &str); 7] = [
    ("GET", "/v1/db/{db}/coll/{coll}/docs"),
    ("PUT", "/v1/db/{db}/coll/{coll}/docs/{id}"),
    ("DELETE", "/v1/db/{db}/coll/{coll}/docs/{id}"),
    ("GET", "/v1/db/{db}/coll/{coll}/describe"),
    ("GET", "/v1/db/{db}/coll/{coll}/violations"),
    ("DELETE", "/v1/db/{db}/coll/{coll}/vector"),
    ("GET", "/v1/db/{db}/coll/{coll}/watch"),
];

/// Refuse a query string on a route that does not read one.
///
/// ADR-121 closed query strings through `QueryParams<T>`, but an extractor
/// only runs on a handler that takes it, and a handler with no query
/// parameters never looked at its query string at all. So
/// `POST .../update?if_stamp=<stale>` answered `200` and rewrote the document
/// — `if_stamp` is a body field there, the parameter was never read, and the
/// write the caller had made conditional was not — while the same typo on
/// `GET .../docs` was the documented `400`. Found by a test round against a
/// three-member cluster running 0.20.0.
///
/// A `QueryParams<NoParams>` on every handler would have closed the routes
/// that have one today and left the next handler open, which is the failure
/// mode this replaces. One layer over the table, deciding by the matched
/// route against [`QUERY_STRING_ROUTES`], is closed for every route by
/// default and open only for the ones the table names (ADR-124).
///
/// The decision is made on axum's `MatchedPath` — the route *template* —
/// rather than the URI, for the reason `request_span` uses it: the template
/// is what the table holds, and a comparison on the raw path would have to
/// re-implement routing. A request that matched no route carries no
/// `MatchedPath` and passes through to the `404` it is about to get; with
/// `Router::layer` that request never reaches this layer at all, and the
/// check is here so that the property does not depend on it.
///
/// The refusal names the first parameter and says the route takes none,
/// after the shape of the serde message `QueryParams` answers with —
/// `` unknown field `limt`, expected `limit` or `skip` `` — so a client sees
/// the same kind of sentence whichever way its query string was refused. A
/// bare `?` names nothing and is not a query string for this purpose; a
/// parameter with no name, `?=1`, is called malformed rather than named.
///
/// # It answers before authentication
///
/// This is a `Router::layer`, and authentication is the `Auth` extractor a
/// handler takes, so a query string is refused before any token is looked
/// at: `POST /v1/users?zz=1` with no token is `400`, not `401`. That is
/// acceptable because nothing is learned and nothing is touched — the route
/// templates are public in `docs/openapi.yaml`, the message echoes only the
/// caller's own input, and no handler runs. It also means a request the
/// guard refuses never reaches the per-principal budget in
/// [`crate::state`], which is spent inside `Auth`, nor a login limiter,
/// which is spent inside the login handler: harmless, since the guard is
/// cheaper than either and examined no credential, but an ordering fact
/// worth stating, as ADR-099 stated the deadline's. A test in
/// `tests/api.rs` holds the placement.
pub async fn refuse_unread_query_string(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let Some(first) = request.uri().query().and_then(first_query_parameter) else {
        return next.run(request).await;
    };
    let Some(route) = request.extensions().get::<axum::extract::MatchedPath>() else {
        return next.run(request).await;
    };
    let method = request.method().as_str();
    if QUERY_STRING_ROUTES.iter().any(|(m, r)| *m == method && *r == route.as_str()) {
        return next.run(request).await;
    }
    let message = if first.is_empty() {
        "malformed query string: a parameter with no name; this route takes none".to_string()
    } else {
        format!("unknown query parameter `{first}`; this route takes none")
    };
    ApiError::bad_request(message).into_response()
}

/// The name of the first parameter in a query string, percent-decoded, or
/// `None` when the string carries no parameter at all — a bare `?`, or one
/// made only of separators, names nothing. `?a` with no `=` is the parameter
/// `a`; `?=1` is a parameter whose name is empty, and the caller says so.
fn first_query_parameter(query: &str) -> Option<String> {
    let pair = query.split('&').find(|pair| !pair.is_empty())?;
    let name = pair.split('=').next().unwrap_or(pair);
    Some(percent_decode(name))
}

/// Decode a query-string component: `%XX` to its byte, `+` to a space, and
/// anything malformed left as written rather than refused — this is for a
/// message that names the parameter, not for reading it.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let escaped = (bytes[i] == b'%' && i + 2 < bytes.len()).then(|| {
            let hi = (bytes[i + 1] as char).to_digit(16)?;
            let lo = (bytes[i + 2] as char).to_digit(16)?;
            u8::try_from((hi << 4) | lo).ok()
        });
        match (bytes[i], escaped.flatten()) {
            (_, Some(byte)) => {
                out.push(byte);
                i += 3;
            }
            (b'+', None) => {
                out.push(b' ');
                i += 1;
            }
            (byte, None) => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
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
    // `/v1/auth/refresh` never refuses on grants — it takes no `require` — so
    // a 403 from it can only be `auth.local.login` saying the peer is off the
    // host (ADR-100). An `insufficient_scope` challenge there would describe a
    // refusal that did not happen. Its 401s are still challenged: an expired
    // token is exactly what a challenge is for.
    let is_refresh = request.uri().path() == "/v1/auth/refresh";

    let span = timed.then(|| request_span(&request));
    let mut response = match &span {
        Some(span) => next.run(request).instrument(span.clone()).await,
        None => next.run(request).await,
    };

    let mode_refusal = is_refresh && response.status() == axum::http::StatusCode::FORBIDDEN;
    if challengeable && !mode_refusal {
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
            // A refusal may say more precisely why, through an extension
            // rather than by setting the header itself, so that a specific
            // description never costs the `resource_metadata` pointer below.
            // A federated token refused for its lifetime is the one case
            // (ADR-096); everything else stays deliberately generic.
            let description = response
                .extensions()
                .get::<crate::error::ChallengeDescription>()
                .map(|d| quoted_string(&d.0))
                .unwrap_or_else(|| "the access token is expired, revoked or malformed".to_string());
            Some((r#"error="invalid_token""#, description))
        }
        axum::http::StatusCode::UNAUTHORIZED => None,
        axum::http::StatusCode::FORBIDDEN => Some((
            r#"error="insufficient_scope""#,
            "the authenticated principal holds no grant covering this operation".to_string(),
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

/// A description made safe to carry as an RFC 6750 §3 quoted-string.
///
/// The grammar admits printable ASCII without `"` or `\`. Every description
/// this node writes already satisfies it, so this is a guard rather than a
/// transformation — but the header is assembled by string formatting, and a
/// stray quote in a message would otherwise end the parameter early.
fn quoted_string(text: &str) -> String {
    text.chars()
        .map(|c| match c {
            '"' | '\\' => '\'',
            ' '..='~' => c,
            _ => '?',
        })
        .collect()
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
         # HELP kimmy_fsyncs Times the disk was asked to make something durable: one per commit under durable, one per shared flush under coalesced.\n\
         # TYPE kimmy_fsyncs counter\n\
         kimmy_fsyncs {fsyncs}\n\
         # HELP kimmy_commits_grouped_total Commits made durable by a shared flush rather than their own fsync.\n\
         # TYPE kimmy_commits_grouped_total counter\n\
         kimmy_commits_grouped_total {grouped}\n\
         # HELP kimmy_storage_bytes Size of the database file on disk.\n\
         # TYPE kimmy_storage_bytes gauge\n\
         kimmy_storage_bytes {storage}\n\
         # HELP kimmy_vector_index_cache_bytes Estimated bytes of HNSW graphs held in memory across vector collections. Bounded by vector.index_cache.max_bytes; a graph larger than the whole budget is held anyway.\n\
         # TYPE kimmy_vector_index_cache_bytes gauge\n\
         kimmy_vector_index_cache_bytes {index_cache}\n\
         # HELP kimmy_up Always 1; presence indicates the node is serving.\n\
         # TYPE kimmy_up gauge\n\
         kimmy_up 1\n\
         {process}",
        databases_count = databases.len(),
        // An estimate from node count and width, not a heap measurement;
        // what the budget evicts against, so the two agree by construction.
        index_cache = state.vectors.resident_bytes(),
        // Surfaced here, not only on a change stream, so the condition is
        // visible without anyone having been subscribed when it happened.
        violations = state.engine.unique_violations(),
        // redb has a single writer and every commit is an fsync, so this over
        // the request count is what a write actually costs. A client-visible
        // write that costs two commits costs twice as much as one that costs
        // one, and no latency figure says which of those is happening.
        commits = state.engine.commits(),
        // Under `coalesced` the two diverge, and the gap is the win: commits
        // that reached the disk without paying for their own fsync (ADR-088).
        fsyncs = state.engine.fsyncs(),
        grouped = state.engine.grouped_commits(),
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
#[serde(deny_unknown_fields)]
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
///
/// `LocalMinting` comes first, before the body is even read: under
/// `auth.local.login = "loopback_only"` a caller off the host is told no
/// before any password crosses into the handler, and a refusal by mode is not
/// a failed attempt for the limiter to count (ADR-100).
async fn login(
    _mint: crate::local_login::LocalMinting,
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
///
/// Bound by `auth.local.login` exactly as `login` is, because this mints a
/// local token too (ADR-100): a mode that closed login and left refresh open
/// would let a session opened from the host be extended forever from anywhere.
/// `LocalMinting` is declared before `Auth` so that under `disabled` the
/// answer is the mode's 404 for everyone, not a 401 inviting a token fetch.
async fn refresh(
    _mint: crate::local_login::LocalMinting,
    State(state): State<SharedState>,
    auth: Auth,
) -> Result<Json<Value>, ApiError> {
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
#[serde(deny_unknown_fields)]
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

async fn drop_database(
    State(state): State<SharedState>,
    auth: Auth,
    Path(db): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::drop_database(&state, &auth, &db)?))
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
#[serde(default, deny_unknown_fields)]
struct FindRequest {
    #[serde(deserialize_with = "crate::json::non_null_field")]
    filter: Option<Value>,
    #[serde(deserialize_with = "crate::json::non_null_field")]
    sort: Option<Value>,
    #[serde(deserialize_with = "crate::json::non_null_field")]
    projection: Option<Value>,
    #[serde(deserialize_with = "crate::json::non_null_field")]
    limit: Option<usize>,
    #[serde(deserialize_with = "crate::json::non_null_field")]
    skip: Option<usize>,
    /// Report how the query was answered alongside the results.
    explain: bool,
    /// Resume after a previous page, using the `nextCursor` it returned.
    #[serde(deserialize_with = "crate::json::non_null_field")]
    cursor: Option<String>,
    /// Return each document's stamp in a parallel `stamps` array.
    stamps: bool,
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
            stamps: r.stamps,
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct FindQuery {
    limit: Option<usize>,
    skip: Option<usize>,
}

async fn find_docs(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    QueryParams(q): QueryParams<FindQuery>,
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
#[serde(deny_unknown_fields)]
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
) -> Result<impl IntoResponse, ApiError> {
    let (stamp, document) = exec::get_doc_stamped(&state, &auth, &db, &coll, &id)?;
    // The document's stamp as an entity tag, so a read by id pairs with a
    // conditional write by id without a `find`. The body stays the document
    // exactly as stored — a version is not one of its fields (ADR-084).
    let etag = [(axum::http::header::ETAG, format!("\"{stamp}\""))];
    Ok((etag, Json(document)))
}

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct ReplaceQuery {
    upsert: bool,
    /// Replace only if the document is at this stamp.
    if_stamp: Option<String>,
}

async fn replace_doc(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll, id)): Path<(String, String, String)>,
    QueryParams(q): QueryParams<ReplaceQuery>,
    JsonBody(body): JsonBody<Value>,
) -> Result<Json<Value>, ApiError> {
    let params = exec::ReplaceParams { upsert: q.upsert, if_stamp: q.if_stamp };
    Ok(Json(exec::replace(&state, &auth, &db, &coll, &id, &body, params)?))
}

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct DeleteQuery {
    /// Delete only if the document is at this stamp.
    if_stamp: Option<String>,
}

async fn delete_doc(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll, id)): Path<(String, String, String)>,
    QueryParams(q): QueryParams<DeleteQuery>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::delete_by_id(&state, &auth, &db, &coll, &id, q.if_stamp.as_deref())?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FindAndModifyRequest {
    #[serde(default, deserialize_with = "crate::json::non_null_field")]
    filter: Option<Value>,
    /// Chooses which document when several match. Without it the choice is the
    /// scan's own order, which is unspecified.
    #[serde(default, deserialize_with = "crate::json::non_null_field")]
    sort: Option<Value>,
    /// Operators, or a whole replacement document.
    #[serde(default, deserialize_with = "crate::json::non_null_field")]
    update: Option<Value>,
    #[serde(default)]
    remove: bool,
    #[serde(default)]
    upsert: bool,
    /// `"before"` (default) or `"after"`.
    #[serde(default, rename = "returnDocument", deserialize_with = "crate::json::non_null_field")]
    return_document: Option<String>,
    #[serde(default, deserialize_with = "crate::json::non_null_field")]
    projection: Option<Value>,
    /// Write only if the chosen document is at this stamp.
    #[serde(default, deserialize_with = "crate::json::non_null_field")]
    if_stamp: Option<String>,
    /// Which array elements the update's `$[<identifier>]` segments address.
    #[serde(default, rename = "arrayFilters")]
    array_filters: Vec<Value>,
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
        if_stamp: body.if_stamp,
        array_filters: body.array_filters,
    };
    Ok(Json(exec::find_and_modify(&state, &auth, &db, &coll, spec)?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateRequest {
    #[serde(default, deserialize_with = "crate::json::non_null_field")]
    filter: Option<Value>,
    update: Value,
    #[serde(default)]
    multi: bool,
    /// Report how the targets were found, as `find` does.
    #[serde(default)]
    explain: bool,
    /// Write only if the matched document is at this stamp.
    #[serde(default, deserialize_with = "crate::json::non_null_field")]
    if_stamp: Option<String>,
    /// Which array elements the update's `$[<identifier>]` segments address.
    #[serde(default, rename = "arrayFilters")]
    array_filters: Vec<Value>,
}

async fn update_docs(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    JsonBody(body): JsonBody<UpdateRequest>,
) -> Result<Json<Value>, ApiError> {
    let params = exec::WriteParams {
        filter: body.filter,
        multi: body.multi,
        explain: body.explain,
        if_stamp: body.if_stamp,
        array_filters: body.array_filters,
    };
    Ok(Json(exec::update(&state, &auth, &db, &coll, &body.update, params)?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteRequest {
    #[serde(default, deserialize_with = "crate::json::non_null_field")]
    filter: Option<Value>,
    #[serde(default)]
    multi: bool,
    /// Report how the targets were found, as `find` does.
    #[serde(default)]
    explain: bool,
    /// Delete only if the matched document is at this stamp.
    #[serde(default, deserialize_with = "crate::json::non_null_field")]
    if_stamp: Option<String>,
}

async fn delete_docs(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    JsonBody(body): JsonBody<DeleteRequest>,
) -> Result<Json<Value>, ApiError> {
    let params = exec::WriteParams {
        filter: body.filter,
        multi: body.multi,
        explain: body.explain,
        if_stamp: body.if_stamp,
        array_filters: Vec::new(),
    };
    Ok(Json(exec::delete(&state, &auth, &db, &coll, params)?))
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct DescribeQuery {
    sample: Option<usize>,
    /// Include one example value per field.
    examples: bool,
}

async fn describe_collection(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    QueryParams(q): QueryParams<DescribeQuery>,
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
#[serde(deny_unknown_fields)]
struct IndexFieldSpec {
    path: String,
    #[serde(default)]
    descending: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateIndexRequest {
    fields: Vec<IndexFieldSpec>,
    #[serde(default)]
    unique: bool,
    #[serde(default, deserialize_with = "crate::json::non_null_field")]
    name: Option<String>,
    /// `"local"` (default) or `"coordinated"`. See the storage docs — a
    /// coordinated unique constraint needs clustering and is refused until M4.
    #[serde(default, deserialize_with = "crate::json::non_null_field")]
    enforcement: Option<String>,
    /// Present makes this a TTL index: documents are deleted this many seconds
    /// after the single indexed date field.
    #[serde(
        default,
        rename = "expireAfterSeconds",
        deserialize_with = "crate::json::non_null_field"
    )]
    expire_after_seconds: Option<i64>,
    /// Present makes this a partial index: only matching documents are held,
    /// and the planner uses it only for queries provably contained by it.
    #[serde(
        default,
        rename = "partialFilterExpression",
        deserialize_with = "crate::json::non_null_field"
    )]
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

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct ViolationsQuery {
    /// Name the index to get its colliding groups rather than counts.
    index: Option<String>,
}

async fn list_violations(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    QueryParams(q): QueryParams<ViolationsQuery>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::violations(&state, &auth, &db, &coll, q.index.as_deref())?))
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

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::{get, post};
    use tower::ServiceExt;

    use super::*;

    /// The REST table's shape in miniature: one route the table opens, one it
    /// does not, under the same layer `routes` applies.
    ///
    /// The paths are real registered routes on purpose: the scanner in
    /// `tests/openapi.rs` reads every `.route(` literal in this file, this
    /// module included, and an invented path here would be reported as a
    /// route the specification does not describe.
    fn guarded() -> Router {
        Router::new()
            .route("/v1/db/{db}/coll/{coll}/docs", get(|| async { "listed" }))
            .route("/v1/db/{db}/coll/{coll}/find", post(|| async { "found" }))
            .layer(axum::middleware::from_fn(refuse_unread_query_string))
    }

    async fn send(method: &str, uri: &str) -> (u16, Value) {
        let request = Request::builder().method(method).uri(uri).body(Body::empty()).unwrap();
        let response = guarded().oneshot(request).await.unwrap();
        let status = response.status().as_u16();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 16).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    #[tokio::test]
    async fn a_query_string_on_a_route_outside_the_table_is_refused_by_name() {
        let (status, body) = send("POST", "/v1/db/shop/coll/orders/find?bogus=1&limit=2").await;
        assert_eq!(status, 400, "{body}");
        assert_eq!(body["error"], "bad_request");
        assert_eq!(body["retry"], "no");
        let message = body["message"].as_str().unwrap();
        assert!(message.contains("`bogus`"), "the first parameter is named: {message}");
        assert!(message.contains("takes none"), "and the route is said to take none: {message}");
    }

    #[tokio::test]
    async fn a_route_the_table_opens_reads_its_own_query_string() {
        let (status, _) = send("GET", "/v1/db/shop/coll/orders/docs?limit=2").await;
        assert_eq!(status, 200);
        // Opened for its handler to judge, not for this layer to: a parameter
        // the handler does not define is `QueryParams`'s refusal, not ours.
        let (status, _) = send("GET", "/v1/db/shop/coll/orders/docs?limt=2").await;
        assert_eq!(status, 200, "the layer defers to the handler on an opened route");
    }

    #[tokio::test]
    async fn a_bare_question_mark_is_not_a_query_string() {
        for uri in ["/v1/db/shop/coll/orders/find?", "/v1/db/shop/coll/orders/find?&&"] {
            let (status, body) = send("POST", uri).await;
            assert_eq!(status, 200, "{uri}: {body}");
        }
    }

    #[tokio::test]
    async fn a_parameter_without_a_value_is_still_a_parameter() {
        let (status, body) = send("POST", "/v1/db/shop/coll/orders/find?a").await;
        assert_eq!(status, 400, "{body}");
        assert!(body["message"].as_str().unwrap().contains("`a`"), "{body}");
    }

    #[tokio::test]
    async fn a_parameter_without_a_name_is_called_malformed_rather_than_named() {
        let (status, body) = send("POST", "/v1/db/shop/coll/orders/find?=1").await;
        assert_eq!(status, 400, "{body}");
        let message = body["message"].as_str().unwrap();
        assert!(message.contains("malformed"), "{message}");
        assert!(!message.contains("``"), "an empty name is not quoted: {message}");
    }

    #[tokio::test]
    async fn a_wrong_method_with_a_query_string_is_refused_for_the_query_string() {
        // The layer wraps the method router, whose own fallback is the 405,
        // so the query string is judged first. Without one the 405 stands.
        let (status, _) = send("PATCH", "/v1/db/shop/coll/orders/find?zz=1").await;
        assert_eq!(status, 400);
        let (status, _) = send("PATCH", "/v1/db/shop/coll/orders/find").await;
        assert_eq!(status, 405);
    }

    #[tokio::test]
    async fn the_named_parameter_is_percent_decoded() {
        let (_, body) = send("POST", "/v1/db/shop/coll/orders/find?if%5Fstamp+x=1").await;
        let message = body["message"].as_str().unwrap();
        assert!(message.contains("`if_stamp x`"), "{message}");
        // Malformed escapes are left as written rather than refused twice.
        let (_, body) = send("POST", "/v1/db/shop/coll/orders/find?a%zz=1").await;
        assert!(body["message"].as_str().unwrap().contains("`a%zz`"), "{body}");
    }

    #[tokio::test]
    async fn a_request_that_matched_no_route_is_left_to_the_fallback() {
        let (status, _) = send("GET", "/nowhere?zz=1").await;
        assert_eq!(status, 404);
    }
}
