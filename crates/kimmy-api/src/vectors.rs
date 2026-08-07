//! Vector configuration and search routes.

use std::collections::HashSet;

use axum::extract::{Path, State};
use axum::{Json, http::StatusCode};
use kimmy_auth::Action;
use kimmy_core::VectorConfig;
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
    let disabled = state.engine.disable_vectors(&db, &coll, q.drop_vectors)?;
    Ok(Json(json!({ "disabled": disabled, "droppedVectors": q.drop_vectors })))
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct SearchRequest {
    /// Query text. Embedded server-side, so it needs an embedding provider.
    query: Option<String>,
    /// A pre-computed query vector. Required when the provider is `byo`.
    vector: Option<Vec<f32>>,
    /// Restrict results to documents matching this filter.
    filter: Option<Value>,
    k: Option<usize>,
    /// Chunks per document allowed into the results.
    per_document: Option<usize>,
}

const DEFAULT_K: usize = 10;
const MAX_K: usize = 1_000;

pub async fn vector_search(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<SearchRequest>,
) -> Result<Json<Value>, ApiError> {
    let (shadow, config, options) = prepare(&state, &auth, &db, &coll, &body)?;
    let query = resolve_query_vector(&config, &body).await?;
    let allowed = allowed_ids(&state, &auth, &db, &coll, body.filter.as_ref())?;

    let hits = search::vector_search(&state.engine, &shadow, &query, &options, allowed.as_ref())
        .map_err(vector_error)?;
    Ok(Json(render(&hits)))
}

pub async fn hybrid_search(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<SearchRequest>,
) -> Result<Json<Value>, ApiError> {
    let (shadow, config, options) = prepare(&state, &auth, &db, &coll, &body)?;

    // Hybrid needs the *text* for the lexical half; a bare vector cannot
    // produce one, so the ambiguity is refused rather than silently degrading
    // to plain vector search.
    let Some(text) = body.query.clone() else {
        return Err(ApiError::bad_request(
            "hybrid_search needs `query` text for its keyword half; use vector_search \
             to search by a pre-computed vector alone",
        ));
    };

    let query = resolve_query_vector(&config, &body).await?;
    let allowed = allowed_ids(&state, &auth, &db, &coll, body.filter.as_ref())?;

    // Each half is retrieved wider than k, so fusion has enough to work with:
    // a document ranked modestly by both should be able to beat one ranked
    // first by only one.
    let wide = SearchOptions { k: (options.k * 4).min(MAX_K), ..options.clone() };
    let dense = search::vector_search(&state.engine, &shadow, &query, &wide, allowed.as_ref())
        .map_err(vector_error)?;
    let lexical =
        search::keyword_search(&state.engine, &shadow, &text, &wide).map_err(vector_error)?;

    let fused = search::reciprocal_rank_fusion(&[dense, lexical], options.k);
    Ok(Json(render(&fused)))
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
