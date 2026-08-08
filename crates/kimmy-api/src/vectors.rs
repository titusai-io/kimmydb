//! Vector configuration and search routes.

use std::collections::HashSet;

use axum::extract::{Path, State};
use axum::{Json, http::StatusCode};
use kimmy_auth::Action;
use kimmy_core::VectorConfig;
use kimmy_vector::Access;
use kimmy_vector::search::{self, Hit, SearchOptions};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::ApiError;
use crate::json::json_to_document;
use crate::state::{Auth, SharedState};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

pub async fn configure_vectors(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<VectorConfig>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Action::Admin, &db, Some(&coll))?;
    let meta = state.engine.configure_vectors(&db, &coll, body)?;
    // A changed dimension or metric makes any cached graph meaningless.
    invalidate_index(&state, &db, &coll);
    Ok(Json(json!({
        "collection": meta.name,
        "vector": meta.vector,
        "shadow": kimmy_core::vector_meta::shadow_name(&meta.name),
    })))
}

pub async fn get_vector_config(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Action::Read, &db, Some(&coll))?;
    let meta = state.engine.get_collection(&db, &coll)?;
    Ok(Json(json!({ "vector": meta.vector })))
}

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct DisableQuery {
    /// Discard the stored vectors as well as the configuration.
    drop_vectors: bool,
}

pub async fn disable_vectors(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    axum::extract::Query(q): axum::extract::Query<DisableQuery>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Action::Admin, &db, Some(&coll))?;
    // Resolved *before* the call: dropping the vectors also drops the shadow
    // collection, and afterwards there is no id left to forget the graph under.
    let shadow = state.engine.vector_collection(&db, &coll).ok().flatten().map(|s| s.id);
    let disabled = state.engine.disable_vectors(&db, &coll, q.drop_vectors)?;
    if let Some(id) = shadow {
        state.vectors.invalidate(id);
    }
    Ok(Json(json!({ "disabled": disabled, "droppedVectors": q.drop_vectors })))
}

// ---------------------------------------------------------------------------
// Client-supplied vectors
// ---------------------------------------------------------------------------

/// One chunk of a document, embedded by the client.
#[derive(Deserialize)]
pub struct ChunkInput {
    /// Chunk number within the document, in split order.
    pub chunk: u32,
    pub vector: Vec<f32>,
    /// The text this vector was produced from. Used by the keyword half of
    /// hybrid search, and shown to explain *why* a chunk matched.
    #[serde(default)]
    pub text: String,
}

/// Store vectors a client computed itself.
///
/// This is what makes the `byo` provider usable. `byo` is the default — it is
/// what you get with no external service and no bundled model — but until now
/// there was no way to supply the vectors it expects, so search on such a
/// collection returned nothing, always.
///
/// **Replace-all, per document.** The body is the complete set of chunks for
/// this document; anything previously stored under it and not named here is
/// removed. That mirrors what the embedding worker does, and it is the only
/// semantics that keeps a shortened document from leaving orphan chunks
/// matching text it no longer contains.
///
/// The server supplies `source` and `source_hlc` from the document it already
/// holds, so a client never has to know the internal record shape — and
/// staleness detection keeps working, because the HLC is the document's own
/// rather than something a client could get wrong.
pub async fn put_document_vectors(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll, id)): Path<(String, String, String)>,
    Json(body): Json<Vec<ChunkInput>>,
) -> Result<Json<Value>, ApiError> {
    // Writing derived data about a document is a write on that collection, not
    // an administrative act.
    let meta = authorize_write(&state, &auth, &db, &coll)?;

    let Some(config) = meta.vector.clone() else {
        return Err(ApiError::bad_request(format!(
            "collection {coll:?} has no vector configuration; POST to \
             /v1/db/{db}/coll/{coll}/vector to enable it"
        )));
    };

    for chunk in &body {
        if chunk.vector.len() != config.dim {
            return Err(ApiError::bad_request(format!(
                "chunk {} has {} dimensions, but this collection stores {}",
                chunk.chunk,
                chunk.vector.len(),
                config.dim
            )));
        }
    }

    let mut numbers: Vec<u32> = body.iter().map(|c| c.chunk).collect();
    numbers.sort_unstable();
    numbers.dedup();
    if numbers.len() != body.len() {
        return Err(ApiError::bad_request(
            "two chunks share a number; each chunk of a document must be numbered once",
        ));
    }

    // The document has to exist: `source_hlc` comes from it, and without one
    // there is nothing for staleness to compare against.
    let doc_id = crate::exec::parse_id(&id)?;
    let stamp = state
        .engine
        .document_stamp(&meta, &doc_id)?
        .ok_or_else(|| ApiError::not_found(format!("no document with _id {id}")))?;

    let shadow = state
        .engine
        .vector_collection(&db, &coll)?
        .ok_or_else(|| ApiError::not_found("vector collection is missing"))?;

    let records: Vec<kimmy_core::VectorRecord> = body
        .into_iter()
        .map(|c| kimmy_core::VectorRecord {
            source: doc_id.clone(),
            chunk: c.chunk,
            source_hlc: stamp.hlc,
            vector: c.vector,
            text: c.text,
        })
        .collect();

    let stored = records.len();
    state.engine.put_vectors(&shadow, &doc_id, &records)?;
    state.vectors.invalidate(shadow.id);

    Ok(Json(json!({ "stored": stored, "_id": id })))
}

/// Read back the vectors stored for one document.
pub async fn get_document_vectors(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll, id)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Action::Read, &db, Some(&coll))?;
    let doc_id = crate::exec::parse_id(&id)?;
    let Some(shadow) = state.engine.vector_collection(&db, &coll)? else {
        return Err(ApiError::not_found("vector collection is missing"));
    };

    let chunks: Vec<Value> = state
        .engine
        .get_vectors(&shadow, &doc_id)?
        .into_iter()
        .map(|r| json!({ "chunk": r.chunk, "vector": r.vector, "text": r.text }))
        .collect();
    Ok(Json(json!({ "count": chunks.len(), "chunks": chunks })))
}

/// Delete every vector stored for one document.
pub async fn delete_document_vectors(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll, id)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    let _ = authorize_write(&state, &auth, &db, &coll)?;
    let doc_id = crate::exec::parse_id(&id)?;
    let Some(shadow) = state.engine.vector_collection(&db, &coll)? else {
        return Err(ApiError::not_found("vector collection is missing"));
    };
    let removed = state.engine.delete_vectors(&shadow, &doc_id)?;
    state.vectors.invalidate(shadow.id);
    Ok(Json(json!({ "deleted": removed })))
}

/// Explain an unsearchable collection in terms of what to do about it.
///
/// The remedy differs by provider, and saying which one applies is most of the
/// value: with `byo` nothing will ever populate the collection unless the
/// client does it, whereas with a server-side provider the worker simply has
/// not caught up — or cannot reach its provider.
fn empty_collection_message(db: &str, coll: &str, config: &VectorConfig) -> String {
    if config.provider.embeds_server_side() {
        format!(
            "collection {coll:?} has embeddings configured but none stored yet. The embedding \
             worker fills these in behind writes, so retry shortly; if it stays empty, check \
             the server log for embedding provider errors."
        )
    } else {
        format!(
            "collection {coll:?} uses client-supplied vectors and none have been stored, so \
             search cannot match anything. PUT them to \
             /v1/db/{db}/coll/{coll}/docs/<id>/vectors."
        )
    }
}

/// Authorize a write against the source collection and resolve it.
fn authorize_write(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
) -> Result<kimmy_storage::CollectionMeta, ApiError> {
    crate::exec::authorize(state, auth, Action::Write, db, coll)
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct SearchRequest {
    /// Query text. Embedded server-side, so it needs an embedding provider.
    pub query: Option<String>,
    /// A pre-computed query vector. Required when the provider is `byo`.
    pub vector: Option<Vec<f32>>,
    /// Restrict results to documents matching this filter.
    pub filter: Option<Value>,
    pub k: Option<usize>,
    /// Chunks per document allowed into the results.
    pub per_document: Option<usize>,
}

const DEFAULT_K: usize = 10;
const MAX_K: usize = 1_000;

pub async fn vector_search(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<SearchRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(run_vector_search(&state, &auth, &db, &coll, &body).await?))
}

/// Vector search, independent of the wire format that asked for it.
///
/// Shared with the MCP `vector_search` tool so the two cannot diverge — in
/// particular on the `Search`-then-`Read` authorization pair that filtering
/// requires.
pub async fn run_vector_search(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    body: &SearchRequest,
) -> Result<Value, ApiError> {
    let (shadow, config, options) = prepare(state, auth, db, coll, body)?;
    let query = resolve_query_vector(&config, body).await?;
    let allowed = allowed_ids(state, auth, db, coll, body.filter.as_ref())?;

    let hits = knn(state, &shadow, &config, &query, &options, allowed.as_ref())?;
    Ok(render(&hits))
}

/// k-NN by whichever path the index cache selects.
///
/// The two paths return the same shape and score the same way — the exact scan
/// is exhaustive, the graph walk is approximate — so callers do not branch.
fn knn(
    state: &SharedState,
    shadow: &kimmy_storage::CollectionMeta,
    config: &VectorConfig,
    query: &[f32],
    options: &SearchOptions,
    allowed: Option<&HashSet<String>>,
) -> Result<Vec<Hit>, ApiError> {
    match state.vectors.access(&state.engine, shadow, config.metric, config.dim) {
        Access::Approximate(index) => index.search(&state.engine, shadow, query, options, allowed),
        Access::Exact => search::vector_search(&state.engine, shadow, query, options, allowed),
    }
    .map_err(vector_error)
}

pub async fn hybrid_search(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<SearchRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(run_hybrid_search(&state, &auth, &db, &coll, &body).await?))
}

/// Hybrid search, independent of the wire format that asked for it.
pub async fn run_hybrid_search(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    body: &SearchRequest,
) -> Result<Value, ApiError> {
    let (shadow, config, options) = prepare(state, auth, db, coll, body)?;

    // Hybrid needs the *text* for the lexical half; a bare vector cannot
    // produce one, so the ambiguity is refused rather than silently degrading
    // to plain vector search.
    let Some(text) = body.query.clone() else {
        return Err(ApiError::bad_request(
            "hybrid_search needs `query` text for its keyword half; use vector_search \
             to search by a pre-computed vector alone",
        ));
    };

    let query = resolve_query_vector(&config, body).await?;
    let allowed = allowed_ids(state, auth, db, coll, body.filter.as_ref())?;

    // Each half is retrieved wider than k, so fusion has enough to work with:
    // a document ranked modestly by both should be able to beat one ranked
    // first by only one.
    let wide = SearchOptions { k: (options.k * 4).min(MAX_K), ..options.clone() };
    let dense = knn(state, &shadow, &config, &query, &wide, allowed.as_ref())?;
    let lexical =
        search::keyword_search(&state.engine, &shadow, &text, &wide).map_err(vector_error)?;

    let fused = search::reciprocal_rank_fusion(&[dense, lexical], options.k);
    Ok(render(&fused))
}

/// Shared setup: authorize, resolve the shadow collection, read the options.
fn prepare(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    body: &SearchRequest,
) -> Result<(kimmy_storage::CollectionMeta, VectorConfig, SearchOptions), ApiError> {
    // `search` is its own action; it is implied by `read` but can be granted
    // alone. See docs/security.md.
    auth.require(Action::Search, db, Some(coll))?;

    let meta = state.engine.get_collection(db, coll)?;
    let Some(config) = meta.vector.clone() else {
        return Err(ApiError::bad_request(format!(
            "collection {coll:?} has no vector configuration; POST to \
             /v1/db/{db}/coll/{coll}/vector to enable embedding"
        )));
    };
    let shadow = state
        .engine
        .vector_collection(db, coll)?
        .ok_or_else(|| ApiError::not_found("vector collection is missing"))?;

    // A collection with no vectors at all can only ever return an empty result,
    // and an empty result is indistinguishable from "nothing matched". That is
    // the difference between a caller refining its query forever and a caller
    // learning that ingestion never happened — which is the whole failure mode
    // of `byo` being the default provider.
    if state.engine.count(&shadow)? == 0 {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "no_vectors",
            empty_collection_message(db, coll, &config),
        ));
    }

    let options = SearchOptions {
        k: body.k.unwrap_or(DEFAULT_K).clamp(1, MAX_K),
        metric: config.metric,
        per_document: body.per_document.unwrap_or(1).max(1),
    };
    Ok((shadow, config, options))
}

/// Turn the request into a query vector.
async fn resolve_query_vector(
    config: &VectorConfig,
    body: &SearchRequest,
) -> Result<Vec<f32>, ApiError> {
    if let Some(vector) = &body.vector {
        // A wrong width would score against nothing and return an empty result
        // that looks like "no matches" rather than "wrong input".
        if vector.len() != config.dim {
            return Err(ApiError::bad_request(format!(
                "query vector has {} dimensions, but this collection stores {}",
                vector.len(),
                config.dim
            )));
        }
        return Ok(vector.clone());
    }

    let Some(text) = &body.query else {
        return Err(ApiError::bad_request("provide either `query` text or a `vector`"));
    };
    if !config.provider.embeds_server_side() {
        return Err(ApiError::bad_request(
            "this collection uses client-supplied vectors, so the server cannot embed \
             query text; send a `vector` instead",
        ));
    }

    let provider = kimmy_vector::build(&config.provider, config.dim).map_err(vector_error)?;
    let mut vectors = provider.embed(std::slice::from_ref(text)).await.map_err(vector_error)?;
    vectors.pop().ok_or_else(|| {
        ApiError::new(
            StatusCode::BAD_GATEWAY,
            "provider_error",
            "the embedding provider returned no vector for the query",
        )
    })
}

/// Run the filter, if any, and collect the ids it matched.
///
/// This is what lets vector search compose with the ordinary query language.
fn allowed_ids(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    filter: Option<&Value>,
) -> Result<Option<HashSet<String>>, ApiError> {
    let Some(filter) = filter else {
        return Ok(None);
    };
    // Reading the source documents is a read, distinct from searching.
    auth.require(Action::Read, db, Some(coll))?;

    let parsed = kimmy_query::filter::parse(&json_to_document(filter)?)?;
    let source = state.engine.get_collection(db, coll)?;

    let mut ids = HashSet::new();
    state.engine.for_each_doc(&source, |id, doc| {
        if kimmy_query::filter::matches(&parsed, &doc) {
            ids.insert(id.to_string());
        }
        Ok(true)
    })?;
    Ok(Some(ids))
}

/// Drop a collection's cached graph.
///
/// Best-effort: if the shadow collection cannot be resolved there is nothing
/// cached under it to forget, so a lookup failure is not worth failing the
/// request that triggered it.
fn invalidate_index(state: &SharedState, db: &str, coll: &str) {
    if let Ok(Some(shadow)) = state.engine.vector_collection(db, coll) {
        state.vectors.invalidate(shadow.id);
    }
}

fn render(hits: &[Hit]) -> Value {
    json!({
        "count": hits.len(),
        "matches": hits.iter().map(|h| json!({
            "_id": crate::json::bson_to_json(&h.id.to_bson()),
            "score": h.score,
            "chunk": h.chunk,
            "text": h.text,
        })).collect::<Vec<_>>(),
    })
}

/// Map a vector-pipeline failure onto a status.
///
/// A provider failure is the *upstream's* fault, not the caller's, so it is a
/// 502 rather than a 500 or a 400.
fn vector_error(e: kimmy_vector::VectorError) -> ApiError {
    use kimmy_vector::VectorError as V;
    match e {
        V::NoProvider | V::DimensionMismatch { .. } => ApiError::bad_request(e.to_string()),
        V::LocalUnavailable | V::ModelUnavailable { .. } => {
            ApiError::new(StatusCode::NOT_IMPLEMENTED, "not_implemented", e.to_string())
        }
        V::MissingApiKey { .. } => {
            ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "misconfigured", e.to_string())
        }
        V::Transport { .. } | V::ProviderRejected { .. } | V::MalformedResponse { .. } => {
            ApiError::new(StatusCode::BAD_GATEWAY, "provider_error", e.to_string())
        }
        V::Storage(inner) => inner.into(),
        V::Core(inner) => inner.into(),
    }
}
