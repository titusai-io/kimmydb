//! HTTP routes.

use axum::extract::{Path, Query, State};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use kimmy_auth::Action;
use kimmy_core::DocId;
use kimmy_query::{filter, plan, shape, update};
use kimmy_storage::CollectionMeta;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::ApiError;
use crate::json::{document_to_json, json_to_bson, json_to_document};
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
        .route("/v1/auth/whoami", get(crate::users::whoami))
        .route("/v1/users", get(crate::users::list_users).post(crate::users::create_user))
        .route("/v1/users/{name}", get(crate::users::get_user).delete(crate::users::delete_user))
        .route("/v1/users/{name}/password", post(crate::users::set_password))
        .route("/v1/users/{name}/grants", post(crate::users::set_grants))
        .route("/v1/databases", get(list_databases))
        .route("/v1/db/{db}/collections", get(list_collections).post(create_collection))
        .route("/v1/db/{db}/coll/{coll}", delete(drop_collection))
        .route("/v1/db/{db}/coll/{coll}/docs", post(insert_doc).get(find_docs))
        .route("/v1/db/{db}/coll/{coll}/find", post(find_docs_post))
        .route("/v1/db/{db}/coll/{coll}/count", post(count_docs))
        .route("/v1/db/{db}/coll/{coll}/update", post(update_docs))
        .route("/v1/db/{db}/coll/{coll}/delete", post(delete_docs))
        .route(
            "/v1/db/{db}/coll/{coll}/docs/{id}",
            get(get_doc).put(replace_doc).delete(delete_doc),
        )
        .route("/v1/db/{db}/coll/{coll}/indexes", get(list_indexes).post(create_index))
        .route("/v1/db/{db}/coll/{coll}/indexes/{name}", delete(drop_index))
        .route("/v1/db/{db}/coll/{coll}/watch", get(watch::watch_collection))
        .with_state(state)
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
         # HELP kimmy_up Always 1; presence indicates the node is serving.\n\
         # TYPE kimmy_up gauge\n\
         kimmy_up 1\n",
        databases_count = databases.len(),
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

async fn login(
    State(state): State<SharedState>,
    Json(body): Json<LoginRequest>,
) -> Result<Json<Value>, ApiError> {
    let principal = state.users.authenticate(&state.engine, &body.user, &body.password)?;
    let token = state.tokens.issue(&principal)?;
    Ok(Json(json!({ "token": token, "user": principal.user })))
}

// ---------------------------------------------------------------------------
// Databases and collections
// ---------------------------------------------------------------------------

async fn list_databases(
    State(state): State<SharedState>,
    auth: Auth,
) -> Result<Json<Value>, ApiError> {
    let names: Vec<String> = state
        .engine
        .list_databases()?
        .into_iter()
        .map(|d| d.name)
        // Hide databases the caller cannot read, rather than revealing that
        // they exist.
        .filter(|name| auth.principal().can(Action::Read, name, None))
        .collect();
    Ok(Json(json!({ "databases": names })))
}

async fn list_collections(
    State(state): State<SharedState>,
    auth: Auth,
    Path(db): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let all = state.engine.list_collections(&db)?;
    let names: Vec<&str> =
        auth.principal().visible(Action::Read, &db, all.iter().map(|c| c.name.as_str()));
    Ok(Json(json!({ "collections": names })))
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
    auth.require(Action::Admin, &db, Some(&body.name))?;
    let meta = state.engine.create_collection(&db, &body.name)?;
    Ok(Json(json!({ "created": meta.name, "id": meta.id.0 })))
}

async fn drop_collection(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Action::Admin, &db, Some(&coll))?;
    let dropped = state.engine.drop_collection(&db, &coll)?;
    Ok(Json(json!({ "dropped": dropped })))
}

// ---------------------------------------------------------------------------
// Documents
// ---------------------------------------------------------------------------

/// Resolve a collection after checking the caller may act on it.
///
/// The authorization check comes first so that a denied request cannot
/// distinguish "forbidden" from "does not exist" by its status code.
fn authorize(
    state: &SharedState,
    auth: &Auth,
    action: Action,
    db: &str,
    coll: &str,
) -> Result<CollectionMeta, ApiError> {
    auth.require(action, db, Some(coll))?;
    Ok(state.engine.get_collection(db, coll)?)
}

async fn insert_doc(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let meta = authorize(&state, &auth, Action::Write, &db, &coll)?;
    let doc = json_to_document(&body)?;
    let id = state.engine.insert(&meta, doc)?;
    Ok(Json(json!({ "insertedId": crate::json::bson_to_json(&id.to_bson()) })))
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
    let request = FindRequest { limit: q.limit, skip: q.skip, ..Default::default() };
    run_find(&state, &auth, &db, &coll, request).await
}

async fn find_docs_post(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<FindRequest>,
) -> Result<Json<Value>, ApiError> {
    run_find(&state, &auth, &db, &coll, body).await
}

/// Default page size, so an unbounded `find` cannot be used to pull an entire
/// collection into memory by accident.
const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 10_000;

async fn run_find(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    request: FindRequest,
) -> Result<Json<Value>, ApiError> {
    let meta = authorize(state, auth, Action::Read, db, coll)?;

    let filter = parse_filter(request.filter.as_ref())?;
    let sort = match &request.sort {
        Some(v) => shape::parse_sort(&json_to_document(v)?)?,
        None => Vec::new(),
    };
    let projection = match &request.projection {
        Some(v) => shape::parse_projection(&json_to_document(v)?)?,
        None => None,
    };
    let limit = request.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let skip = request.skip.unwrap_or(0);

    // A sort has to see every match before it can page, so early exit is only
    // safe for unsorted queries.
    let stop_after = sort.is_empty().then_some(skip + limit);
    let (mut matched, stats) = collect_matching(state, &meta, &filter, stop_after)?;

    shape::sort(&sort, &mut matched);

    let page: Vec<Value> = matched
        .into_iter()
        .skip(skip)
        .take(limit)
        .map(|doc| document_to_json(&shape::project(projection.as_ref(), &doc)))
        .collect();

    let mut body = json!({ "documents": page, "count": page.len() });
    if request.explain {
        body["explain"] = stats.to_json();
    }
    Ok(Json(body))
}

/// How a query was answered, for `explain`.
struct QueryStats {
    index: Option<String>,
    fields_used: usize,
    examined: usize,
    matched: usize,
}

impl QueryStats {
    fn to_json(&self) -> Value {
        json!({
            "strategy": if self.index.is_some() { "index" } else { "collectionScan" },
            "index": self.index,
            "indexFieldsUsed": self.fields_used,
            "documentsExamined": self.examined,
            "documentsMatched": self.matched,
        })
    }
}

/// Gather the documents matching a filter, through an index when one applies.
///
/// The index only narrows the candidate set — **every candidate is re-checked
/// against the full filter**, because an index answers "might match" and only
/// the filter decides. Skipping that recheck is how index-backed queries start
/// returning documents that do not match.
fn collect_matching(
    state: &SharedState,
    meta: &kimmy_storage::CollectionMeta,
    filter: &filter::Filter,
    stop_after: Option<usize>,
) -> Result<(Vec<bson::Document>, QueryStats), ApiError> {
    let plan = plan::choose(filter, &meta.indexes);
    let mut matched = Vec::new();
    let mut examined = 0usize;

    match &plan {
        Some(p) => {
            let candidates = state.engine.index_candidates(meta, p.index_id, &p.lower, &p.upper)?;
            for key in candidates {
                let Some(doc) = state.engine.get_by_encoded_key(meta, &key)? else {
                    continue;
                };
                examined += 1;
                if filter::matches(filter, &doc) {
                    matched.push(doc);
                    if stop_after.is_some_and(|n| matched.len() >= n) {
                        break;
                    }
                }
            }
        }
        None => {
            state.engine.for_each_doc(meta, |_, doc| {
                examined += 1;
                if filter::matches(filter, &doc) {
                    matched.push(doc);
                }
                Ok(!stop_after.is_some_and(|n| matched.len() >= n))
            })?;
        }
    }

    let stats = QueryStats {
        index: plan.as_ref().map(|p| p.index_name.clone()),
        fields_used: plan.as_ref().map_or(0, |p| p.fields_used),
        examined,
        matched: matched.len(),
    };
    Ok((matched, stats))
}

async fn count_docs(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    Json(request): Json<FindRequest>,
) -> Result<Json<Value>, ApiError> {
    let meta = authorize(&state, &auth, Action::Read, &db, &coll)?;
    let filter = parse_filter(request.filter.as_ref())?;

    // No early exit: a count must see every match.
    let (matched, stats) = collect_matching(&state, &meta, &filter, None)?;

    let mut body = json!({ "count": matched.len() });
    if request.explain {
        body["explain"] = stats.to_json();
    }
    Ok(Json(body))
}

async fn get_doc(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll, id)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    let meta = authorize(&state, &auth, Action::Read, &db, &coll)?;
    let doc_id = parse_id(&id)?;
    match state.engine.get(&meta, &doc_id)? {
        Some(doc) => Ok(Json(document_to_json(&doc))),
        None => Err(ApiError::not_found(format!("no document with _id {id}"))),
    }
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
    let meta = authorize(&state, &auth, Action::Write, &db, &coll)?;
    let doc_id = parse_id(&id)?;
    let doc = json_to_document(&body)?;
    let outcome = state.engine.replace(&meta, &doc_id, doc, q.upsert)?;
    Ok(Json(json!({
        "matched": outcome.matched,
        "modified": outcome.modified,
        "upserted": outcome.upserted,
    })))
}

async fn delete_doc(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll, id)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    let meta = authorize(&state, &auth, Action::Write, &db, &coll)?;
    let doc_id = parse_id(&id)?;
    let deleted = state.engine.delete(&meta, &doc_id)?;
    Ok(Json(json!({ "deleted": u8::from(deleted) })))
}

#[derive(Deserialize)]
struct UpdateRequest {
    #[serde(default)]
    filter: Option<Value>,
    update: Value,
    #[serde(default)]
    multi: bool,
}

async fn update_docs(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<UpdateRequest>,
) -> Result<Json<Value>, ApiError> {
    let meta = authorize(&state, &auth, Action::Write, &db, &coll)?;
    let filter = parse_filter(body.filter.as_ref())?;
    let update = update::parse(&json_to_document(&body.update)?)?;

    // Collect the targets first: mutating while scanning would mean the read
    // transaction and the write transaction disagree about what matched.
    let mut targets = Vec::new();
    state.engine.for_each_doc(&meta, |id, doc| {
        if filter::matches(&filter, &doc) {
            targets.push((id, doc));
        }
        Ok(body.multi || targets.is_empty())
    })?;

    let now = now_millis();
    let mut modified = 0u64;
    for (id, mut doc) in targets {
        update::apply(&update, &mut doc, now)?;
        state.engine.replace(&meta, &id, doc, false)?;
        modified += 1;
    }

    Ok(Json(json!({ "matched": modified, "modified": modified })))
}

#[derive(Deserialize)]
struct DeleteRequest {
    #[serde(default)]
    filter: Option<Value>,
    #[serde(default)]
    multi: bool,
}

async fn delete_docs(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<DeleteRequest>,
) -> Result<Json<Value>, ApiError> {
    let meta = authorize(&state, &auth, Action::Write, &db, &coll)?;
    let filter = parse_filter(body.filter.as_ref())?;

    let mut targets = Vec::new();
    state.engine.for_each_doc(&meta, |id, doc| {
        if filter::matches(&filter, &doc) {
            targets.push(id);
        }
        Ok(body.multi || targets.is_empty())
    })?;

    let mut deleted = 0u64;
    for id in targets {
        if state.engine.delete(&meta, &id)? {
            deleted += 1;
        }
    }
    Ok(Json(json!({ "deleted": deleted })))
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
}

async fn create_index(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<CreateIndexRequest>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Action::Admin, &db, Some(&coll))?;

    let fields: Vec<kimmy_storage::IndexField> = body
        .fields
        .into_iter()
        .map(|f| kimmy_storage::IndexField { path: f.path, descending: f.descending })
        .collect();

    let enforcement = match body.enforcement.as_deref() {
        None | Some("local") => kimmy_storage::Enforcement::Local,
        Some("coordinated") => kimmy_storage::Enforcement::Coordinated,
        Some(other) => {
            return Err(ApiError::bad_request(format!(
                "unknown enforcement {other:?}: expected \"local\" or \"coordinated\""
            )));
        }
    };

    let index =
        state.engine.create_index_with(&db, &coll, fields, body.unique, enforcement, body.name)?;
    Ok(Json(index_to_json(&index)))
}

async fn list_indexes(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &auth, Action::Read, &db, &coll)?;
    let indexes: Vec<Value> =
        state.engine.list_indexes(&db, &coll)?.iter().map(index_to_json).collect();
    Ok(Json(json!({ "indexes": indexes })))
}

async fn drop_index(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll, name)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Action::Admin, &db, Some(&coll))?;
    Ok(Json(json!({ "dropped": state.engine.drop_index(&db, &coll, &name)? })))
}

fn index_to_json(index: &kimmy_storage::IndexMeta) -> Value {
    json!({
        "name": index.name,
        "fields": index.fields.iter().map(|f| json!({
            "path": f.path,
            "descending": f.descending,
        })).collect::<Vec<_>>(),
        "unique": index.unique,
        "enforcement": match index.enforcement {
            kimmy_storage::Enforcement::Local => "local",
            kimmy_storage::Enforcement::Coordinated => "coordinated",
        },
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub(crate) fn parse_filter(value: Option<&Value>) -> Result<filter::Filter, ApiError> {
    match value {
        Some(v) => Ok(filter::parse(&json_to_document(v)?)?),
        None => Ok(filter::Filter::AlwaysTrue),
    }
}

/// Interpret a path segment as a document id.
///
/// A 24-character hex string is read as an ObjectId and an integer as an
/// integer, matching how ids are most often written; anything else is a string.
fn parse_id(raw: &str) -> Result<DocId, ApiError> {
    if raw.len() == 24
        && raw.chars().all(|c| c.is_ascii_hexdigit())
        && let Ok(oid) = raw.parse::<bson::oid::ObjectId>()
    {
        return Ok(DocId::ObjectId(oid));
    }
    if let Ok(n) = raw.parse::<i64>() {
        return Ok(DocId::Int64(n));
    }
    Ok(DocId::String(raw.to_string()))
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Convert a JSON value into BSON for internal use.
pub(crate) fn value_to_bson(value: &Value) -> Result<bson::Bson, ApiError> {
    json_to_bson(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_parsed_by_shape() {
        let oid = bson::oid::ObjectId::new();
        assert_eq!(parse_id(&oid.to_hex()).unwrap(), DocId::ObjectId(oid));
        assert_eq!(parse_id("42").unwrap(), DocId::Int64(42));
        assert_eq!(parse_id("-7").unwrap(), DocId::Int64(-7));
        assert_eq!(parse_id("hello").unwrap(), DocId::String("hello".into()));
        // 24 characters but not hex: a string, not a failed ObjectId.
        assert_eq!(
            parse_id("zzzzzzzzzzzzzzzzzzzzzzzz").unwrap(),
            DocId::String("zzzzzzzzzzzzzzzzzzzzzzzz".into())
        );
    }

    #[test]
    fn an_absent_filter_matches_everything() {
        assert_eq!(parse_filter(None).unwrap(), filter::Filter::AlwaysTrue);
    }
}
