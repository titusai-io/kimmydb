//! HTTP routes.

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::warn;

use crate::error::ApiError;
use crate::exec;
use crate::ratelimit::{self, Decision};
use crate::state::{Auth, SharedState};
use crate::watch;

pub fn router(state: SharedState) -> Router {
    Router::new()
        // Health endpoints are unauthenticated on purpose: a load balancer
        // probing them should not need credentials.
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/v1/auth/login", post(login))
        .route("/v1/admin/backup", get(backup))
        .route("/v1/auth/whoami", get(crate::users::whoami))
        .route("/v1/users", get(crate::users::list_users).post(crate::users::create_user))
        .route("/v1/users/{name}", get(crate::users::get_user).delete(crate::users::delete_user))
        .route("/v1/users/{name}/password", post(crate::users::set_password))
        .route("/v1/users/{name}/grants", post(crate::users::set_grants))
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
        // Counting happens in one layer rather than in each handler: a counter
        // beside a handler is a counter the next route forgets. It wraps
        // everything including `/metrics` itself, so a scrape is visible as
        // traffic rather than being invisible to the thing it scrapes.
        .layer(axum::middleware::from_fn_with_state(state.clone(), count_request))
        .with_state(state)
}

/// Count every response by status, and time the ones that are real traffic.
async fn count_request(
    State(state): State<SharedState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    // Health probes and scrapes are excluded from the *histogram* — every few
    // seconds forever, they would crowd the buckets the real traffic lands in
    // — but still counted as requests, so a scrape stays visible as traffic.
    let timed = !matches!(request.uri().path(), "/healthz" | "/readyz" | "/metrics");
    let started = std::time::Instant::now();
    let response = next.run(request).await;
    if timed {
        state.metrics.record_latency(started.elapsed());
    }
    state.metrics.record_request(response.status().as_u16());
    response
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
        storage = state.engine.storage_bytes(),
        process = state.metrics.render(),
    ))
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
    Json(body): Json<LoginRequest>,
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
    Ok(Json(json!({ "token": token, "user": principal.user })))
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
    Json(body): Json<CreateCollectionRequest>,
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
    Json(body): Json<Value>,
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
    body: Result<Json<Vec<Value>>, JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    let Json(documents) = body?;
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
    Json(body): Json<FindRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(exec::find(&state, &auth, &db, &coll, body.into())?))
}

async fn count_docs(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<FindRequest>,
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
    Json(body): Json<AggregateRequest>,
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
    Json(body): Json<crate::webhooks::RegisterRequest>,
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
    Json(body): Json<Value>,
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
    Json(body): Json<FindAndModifyRequest>,
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
    Json(body): Json<UpdateRequest>,
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
    Json(body): Json<DeleteRequest>,
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
    Json(body): Json<CreateIndexRequest>,
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
