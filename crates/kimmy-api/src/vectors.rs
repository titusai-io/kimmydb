//! Vector configuration and search routes.

use std::collections::HashSet;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::{Json, http::StatusCode};
use kimmy_auth::Action;
use kimmy_core::{
    ChunkConfig, CollectionId, DocId, Metric, OpKind, ProviderConfig, VectorConfig, vector_meta,
};
use kimmy_vector::Access;
use kimmy_vector::search::{self, Hit, SearchOptions};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::warn;

use crate::error::{ApiError, ErrorCode, LogLevel};
use crate::exec::QueryStats;
use crate::json::{JsonBody, json_to_document};
use crate::state::{Auth, SharedState};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// A request-only mirror of [`VectorConfig`], for the same reason
/// `GrantInput` mirrors `kimmy_auth::Grant`: `VectorConfig` is not only the
/// request body here, it is also the stored and replicated form — carried
/// through `CollectionMeta` on disk (JSON) and through `VectorSet` on the
/// wire (BSON) — and several of its fields have always serialized an unset
/// `None` as an explicit `null` rather than omitting the key, because they
/// predate ADR-128 and were never given `skip_serializing_if`. Applying
/// [`crate::json::non_null_field`] to `VectorConfig` itself would refuse to
/// load or apply exactly the records this field's own absence already
/// produced — turning an upgrade into a node that cannot read its own
/// configuration. So the refusal lives here instead, on a type this route
/// alone deserializes from a fresh HTTP body, and converts into the real
/// type afterwards; the stored and replicated form is untouched.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorConfigInput {
    fields: Vec<String>,
    provider: ProviderConfigInput,
    dim: usize,
    #[serde(default)]
    metric: Metric,
    #[serde(default)]
    chunk: ChunkConfigInput,
    #[serde(default, deserialize_with = "crate::json::non_null_field")]
    document_prefix: Option<String>,
    #[serde(default, deserialize_with = "crate::json::non_null_field")]
    query_prefix: Option<String>,
}

impl From<VectorConfigInput> for VectorConfig {
    fn from(input: VectorConfigInput) -> Self {
        VectorConfig {
            fields: input.fields,
            provider: input.provider.into(),
            dim: input.dim,
            metric: input.metric,
            chunk: input.chunk.into(),
            document_prefix: input.document_prefix,
            query_prefix: input.query_prefix,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChunkConfigInput {
    max_chars: usize,
    overlap: usize,
    #[serde(deserialize_with = "crate::json::non_null_field")]
    max_tokens: Option<usize>,
}

impl Default for ChunkConfigInput {
    fn default() -> Self {
        let d = ChunkConfig::default();
        Self { max_chars: d.max_chars, overlap: d.overlap, max_tokens: d.max_tokens }
    }
}

impl From<ChunkConfigInput> for ChunkConfig {
    fn from(input: ChunkConfigInput) -> Self {
        ChunkConfig {
            max_chars: input.max_chars,
            overlap: input.overlap,
            max_tokens: input.max_tokens,
        }
    }
}

fn default_openai_key_env() -> String {
    "OPENAI_API_KEY".to_string()
}

fn default_cohere_key_env() -> String {
    "COHERE_API_KEY".to_string()
}

fn default_gemini_key_env() -> String {
    "GEMINI_API_KEY".to_string()
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderConfigInput {
    /// An empty body, not a unit variant, for the reason spelled out on
    /// [`ProviderConfig::Byo`]: serde has nowhere to apply `deny_unknown_fields`
    /// on an internally-tagged unit variant, so `{"kind":"byo","nosuch":1}`
    /// reached the handler as a plain `byo` with the typo dropped. Both types
    /// need the fix — this mirror is what the route deserializes, the core
    /// type is what the operator's profile map and the stored form do.
    Byo {},
    OpenAi {
        model: String,
        #[serde(default, deserialize_with = "crate::json::non_null_field")]
        endpoint: Option<String>,
        #[serde(default = "default_openai_key_env")]
        api_key_env: String,
        #[serde(default, deserialize_with = "crate::json::non_null_field")]
        dimensions: Option<usize>,
    },
    Ollama {
        model: String,
        endpoint: String,
    },
    CustomHttp {
        endpoint: String,
        #[serde(default, deserialize_with = "crate::json::non_null_field")]
        api_key_env: Option<String>,
    },
    Cohere {
        model: String,
        #[serde(default, deserialize_with = "crate::json::non_null_field")]
        endpoint: Option<String>,
        #[serde(default = "default_cohere_key_env")]
        api_key_env: String,
    },
    Gemini {
        model: String,
        #[serde(default, deserialize_with = "crate::json::non_null_field")]
        endpoint: Option<String>,
        #[serde(default = "default_gemini_key_env")]
        api_key_env: String,
    },
    Local {
        model: String,
    },
    Profile {
        name: String,
    },
}

impl From<ProviderConfigInput> for ProviderConfig {
    fn from(input: ProviderConfigInput) -> Self {
        match input {
            ProviderConfigInput::Byo {} => ProviderConfig::Byo {},
            ProviderConfigInput::OpenAi { model, endpoint, api_key_env, dimensions } => {
                ProviderConfig::OpenAi { model, endpoint, api_key_env, dimensions }
            }
            ProviderConfigInput::Ollama { model, endpoint } => {
                ProviderConfig::Ollama { model, endpoint }
            }
            ProviderConfigInput::CustomHttp { endpoint, api_key_env } => {
                ProviderConfig::CustomHttp { endpoint, api_key_env }
            }
            ProviderConfigInput::Cohere { model, endpoint, api_key_env } => {
                ProviderConfig::Cohere { model, endpoint, api_key_env }
            }
            ProviderConfigInput::Gemini { model, endpoint, api_key_env } => {
                ProviderConfig::Gemini { model, endpoint, api_key_env }
            }
            ProviderConfigInput::Local { model } => ProviderConfig::Local { model },
            ProviderConfigInput::Profile { name } => ProviderConfig::Profile { name },
        }
    }
}

pub async fn configure_vectors(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    JsonBody(body): JsonBody<VectorConfigInput>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Action::Ddl, &db, Some(&coll))?;
    let body: VectorConfig = body.into();
    admit_provider(&state, &body.provider)?;
    let meta = state.engine.configure_vectors(&db, &coll, body)?;
    // A changed dimension or metric makes any cached graph meaningless.
    invalidate_index(&state, &db, &coll);
    crate::audit::record_vectors(
        auth.principal(),
        "ConfigureVectors",
        &db,
        &coll,
        meta.vector.as_ref().map(|v| &v.provider),
    );
    Ok(Json(json!({
        "collection": meta.name,
        "vector": meta.vector,
        "shadow": kimmy_core::vector_meta::shadow_name(&meta.name),
    })))
}

/// This node's provider policy, asked before a configuration is stored.
///
/// Asked here, while the person who typed it is watching, and again when the
/// provider is built — the second because a configuration also arrives by
/// replication. A refusal is a `400` naming the variable or the host, never
/// a value: the policy refuses the *name* before anything reads it (ADR-115).
fn admit_provider(
    state: &SharedState,
    provider: &kimmy_core::ProviderConfig,
) -> Result<(), ApiError> {
    state.providers.check_configure(provider).map_err(|e| ApiError::bad_request(e.to_string()))
}

pub async fn get_vector_config(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Action::Read, &db, Some(&coll))?;
    let meta = crate::exec::collection(&state, &db, &coll)?;
    Ok(Json(json!({ "vector": meta.vector })))
}

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct DisableQuery {
    /// Discard the stored vectors as well as the configuration.
    drop_vectors: bool,
}

pub async fn disable_vectors(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    crate::json::QueryParams(q): crate::json::QueryParams<DisableQuery>,
) -> Result<Json<Value>, ApiError> {
    auth.require(Action::Ddl, &db, Some(&coll))?;
    // Resolved *before* the call: dropping the vectors also drops the shadow
    // collection, and afterwards there is no id left to forget the graph under.
    let shadow = state.engine.vector_collection(&db, &coll).ok().flatten().map(|s| s.id);
    let disabled = state.engine.disable_vectors(&db, &coll, q.drop_vectors)?;
    if let Some(id) = shadow {
        state.vectors.invalidate(id);
    }
    if disabled {
        crate::audit::record_vectors(auth.principal(), "DisableVectors", &db, &coll, None);
    }
    Ok(Json(json!({ "disabled": disabled, "droppedVectors": q.drop_vectors })))
}

// ---------------------------------------------------------------------------
// Forgetting a dropped collection's graph
// ---------------------------------------------------------------------------

/// Forget a dropped collection's vector index, however the drop reached this
/// node.
///
/// [`crate::exec::drop_collection`], [`crate::exec::drop_database`] and
/// [`disable_vectors`] forget synchronously, which is what makes a single node
/// correct with no wiring at all. This is the other half, and the split is the
/// one ADR-052 draws for token state. A drop that arrives by replication is
/// applied by the sync path on the member that receives it, and that member
/// never runs the route — so without this it holds the graph in memory and the
/// snapshot on disk for a collection that no longer exists, for the life of
/// the process. Seen on a three-member cluster twelve hours after every user
/// collection had been dropped: 353 MiB of resident graphs and 280 MiB of
/// snapshots on the member that had held the vectors, against nothing on its
/// two peers, and stable rather than draining.
///
/// It reads the entry stream the apply path already publishes and touches
/// nothing else. It cannot fail a sync round, change what an apply decides, or
/// change what is witnessed: an entry reaches here only once it is committed
/// and published, and what happens to it afterwards is this task's alone.
///
/// Subscribing happens **here**, not inside the returned future, so the
/// caller's `tokio::spawn` cannot miss an entry published between the call and
/// the task being polled for the first time.
pub fn invalidator(state: &SharedState) -> impl std::future::Future<Output = ()> + use<> {
    let mut events = state.engine.subscribe();
    let state = Arc::clone(state);

    async move {
        loop {
            match events.recv().await {
                Ok(entry) if entry.kind == OpKind::DropCollection => {
                    // The entry names what was dropped: the shadow itself when
                    // the vectors were discarded on their own, the parent when
                    // a whole collection went. A parent's drop takes its shadow
                    // in the same transaction and mints no entry for it — but
                    // ids are derived from names, so the shadow's is computable
                    // from the name this entry does carry. Checking an id that
                    // never held a graph costs a metadata read, so both go.
                    forget_if_gone(&state, entry.collection);
                    if let Some(body) = &entry.body
                        && let Ok(target) =
                            bson::deserialize_from_slice::<kimmy_core::CollectionRef>(body)
                    {
                        forget_if_gone(
                            &state,
                            CollectionId::derive(
                                &target.db,
                                &vector_meta::shadow_name(&target.name),
                            ),
                        );
                    }
                }
                Ok(_) => {}
                // Missed entries: which collections were dropped is unknown, so
                // the cache is reconciled against what this node actually holds
                // rather than trusted. Clearing it outright, as the token-state
                // consumer does, is not the equivalent here — that would delete
                // every snapshot on the node and charge a rebuild for
                // collections that are perfectly alive.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    // A read transaction per database, on a task that is
                    // otherwise only ever waiting: off the worker, like every
                    // other storage step.
                    match kimmy_storage::blocking(|| live_collections(&state.engine)) {
                        Ok(live) => {
                            let forgotten = state.vectors.forget_absent(&live);
                            warn!(
                                missed,
                                forgotten,
                                "vector index consumer fell behind; reconciled the cache \
                                 against the collections this node holds"
                            );
                        }
                        Err(e) => warn!(
                            missed,
                            error = %e,
                            "vector index consumer fell behind and could not list this \
                             node's collections; a collection dropped in the meantime may \
                             keep its index and its snapshot until this node restarts"
                        ),
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    }
}

/// Forget a collection's graph, but **only if the collection really is gone**.
///
/// A drop entry reaches this task as history, not as news: the feed is a
/// bounded ring, and a burst of writes — five hundred chunks of one document
/// is five hundred entries — can put a drop arbitrarily far behind the writer.
/// By the time it arrives the name may have been created again, and because a
/// collection id is derived from its name the new collection has the same id,
/// the same cache key and the same snapshot path. Acting on the entry alone
/// would then delete a live index that nobody dropped.
///
/// So the entry only says which id to *look at*; what decides is whether this
/// node still holds a collection under it. That is a read of state the drop
/// has already committed, and it makes this task's correctness independent of
/// how far behind it is — and of whether any particular drop is announced at
/// all, which is not this crate's decision to depend on.
fn forget_if_gone(state: &SharedState, id: CollectionId) {
    match kimmy_storage::blocking(|| state.engine.collection_by_id(id)) {
        Ok(None) => state.vectors.invalidate(id),
        Ok(Some(_)) => {}
        Err(e) => warn!(
            error = %e,
            collection = id.0,
            "could not tell whether a dropped collection is gone; leaving its vector index \
             in place, which costs memory rather than answers"
        ),
    }
}

/// Every collection id this node holds, shadow collections included.
///
/// Deliberately not `Engine::all_collection_ids`, which hides a shadow whose
/// parent is present (ADR-138). The ids it hides are exactly the ones a vector
/// index is keyed by, so reconciling against it would forget every live graph
/// on the node.
pub fn live_collections(
    engine: &kimmy_storage::Engine,
) -> Result<HashSet<CollectionId>, kimmy_storage::StorageError> {
    let mut ids = HashSet::new();
    for db in engine.list_databases()? {
        for coll in engine.list_collections(&db.name)? {
            ids.insert(coll.id);
        }
    }
    Ok(ids)
}

// ---------------------------------------------------------------------------
// Client-supplied vectors
// ---------------------------------------------------------------------------

/// One chunk of a document, embedded by the client.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
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
    JsonBody(body): JsonBody<Vec<ChunkInput>>,
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
    // No cache invalidation: `put_vectors` bumps the collection's vector
    // generation, which is what the search path reads to notice a write, and
    // it is all the embedding worker does for the identical write. Forgetting
    // the entry outright also deleted the snapshot on every stored document,
    // which cost a full rebuild on the next search rather than the bounded
    // staleness the index is designed around. The snapshot that stays behind
    // is not trusted past this write: the cache remembers the generation it
    // wrote it at and will not call it fresh again (`IndexCache::try_snapshot`).
    state.engine.put_vectors(&shadow, &doc_id, &records)?;

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
    // No cache invalidation, for the reason `put_document_vectors` gives:
    // `delete_vectors` bumps the generation when it removed anything, and the
    // snapshot left behind is not trusted past that.
    let removed = state.engine.delete_vectors(&shadow, &doc_id)?;
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
#[serde(default, deny_unknown_fields)]
pub struct SearchRequest {
    /// Query text. Embedded server-side, so it needs an embedding provider.
    #[serde(deserialize_with = "crate::json::non_null_field")]
    pub query: Option<String>,
    /// A pre-computed query vector. Required when the provider is `byo`.
    #[serde(deserialize_with = "crate::json::non_null_field")]
    pub vector: Option<Vec<f32>>,
    /// Restrict results to documents matching this filter.
    #[serde(deserialize_with = "crate::json::non_null_field")]
    pub filter: Option<Value>,
    #[serde(deserialize_with = "crate::json::non_null_field")]
    pub k: Option<usize>,
    /// Chunks per document allowed into the results.
    #[serde(deserialize_with = "crate::json::non_null_field")]
    pub per_document: Option<usize>,
    /// How much each half of `hybrid_search` counts in fusion (ADR-094).
    /// Ignored by `vector_search`, which has one half.
    #[serde(deserialize_with = "crate::json::non_null_field")]
    pub weights: Option<FusionWeights>,
    /// Distinct query terms a chunk must share with the query to count as
    /// lexical evidence in `hybrid_search` (ADR-094). Ignored by
    /// `vector_search`.
    #[serde(deserialize_with = "crate::json::non_null_field")]
    pub min_overlap: Option<usize>,
}

/// The authority each half of a hybrid search carries in fusion.
///
/// Applied as `dense / (60 + rank_dense) + lexical / (60 + rank_lexical)`.
/// Only the ratio matters to the resulting order. The default is equal
/// weights — plain reciprocal rank fusion, which is what every request got
/// before the field existed.
#[derive(Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct FusionWeights {
    pub dense: f64,
    pub lexical: f64,
}

impl Default for FusionWeights {
    fn default() -> Self {
        Self { dense: 1.0, lexical: 1.0 }
    }
}

/// Read the fusion controls off a request and refuse the ones that cannot
/// mean anything.
///
/// A negative weight would *subtract* a half's evidence, both weights at zero
/// leaves nothing to rank by, and a `min_overlap` of zero would admit chunks
/// that share no term with the query as lexical evidence. None of these is a
/// setting anyone wants; each is a mistake worth naming.
fn fusion_controls(body: &SearchRequest) -> Result<(FusionWeights, usize), ApiError> {
    let weights = body.weights.unwrap_or_default();
    for (name, weight) in [("dense", weights.dense), ("lexical", weights.lexical)] {
        if !weight.is_finite() || weight < 0.0 {
            return Err(ApiError::bad_request(format!(
                "weights.{name} must be a finite number of at least 0, got {weight}"
            )));
        }
    }
    if weights.dense == 0.0 && weights.lexical == 0.0 {
        return Err(ApiError::bad_request(
            "weights.dense and weights.lexical cannot both be 0; that leaves nothing to rank by",
        ));
    }
    let min_overlap = body.min_overlap.unwrap_or(1);
    if min_overlap == 0 {
        return Err(ApiError::bad_request(
            "min_overlap must be at least 1: a chunk sharing no term with the query is not \
             lexical evidence",
        ));
    }
    Ok((weights, min_overlap))
}

/// Hits a search returns when the request names no `k`.
///
/// Public because `docs/openapi.yaml` and `docs/vectors.md` state it and the
/// tests hold them to it.
pub const DEFAULT_K: usize = 10;
/// The most hits a search returns. A `k` above it is clamped rather than
/// refused, and so is a `k` of `0` (to `1`): a search has no "window of
/// nothing" the way `find`'s `limit: 0` does. Public for the same reason as
/// [`DEFAULT_K`].
pub const MAX_K: usize = 1_000;

/// The largest admitted set for which a filtered search reads the admitted
/// documents' chunks by key rather than searching everything and discarding.
///
/// The keyed join costs the size of the set and is exact; the discarding join
/// costs the size of the collection and, on the graph, is approximate twice
/// over — widened eightfold for a filter and still short of `k` when the set
/// is small. So the keyed join wins whenever the set is small, and the
/// discarding join when the set is most of the collection, because then the
/// graph's candidates are mostly admitted anyway. A count rather than a
/// fraction, because the keyed join's cost does not depend on the collection
/// — a thousand documents' chunks read by key is the same work over a
/// million documents as over two thousand — and because a fraction would need
/// the collection's size, which is a scan to learn. A thousand is past the
/// widest window a request can ask for (`MAX_K`, and hybrid's halves at
/// `4k`), so an admitted set at or under it is at most a few reads per hit
/// returned. ADR-102.
const SELECTIVE_JOIN_MAX: usize = 1_000;

/// Matches taken per planner call while a filter's ids are collected.
///
/// The executor returns the documents it matched, and an unselective filter
/// over a large collection would otherwise hold every one of them at once —
/// the failure ADR-098 names. Paging by encoded key, as a cursor does, holds
/// one page and the ids. Wide, because on an index plan each page gathers the
/// range's candidate keys again; `exec::visit_matching` makes a page a seek,
/// and `filter_ids` is written to become one call to it — see the note there.
const FILTER_PAGE: usize = 10_000;

pub async fn vector_search(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    JsonBody(body): JsonBody<SearchRequest>,
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
    let (source, shadow, config, options) = prepare(state, auth, db, coll, body)?;
    let query = resolve_query_vector(state, &config, body).await?;
    let allowed = allowed_ids(state, auth, db, coll, body.filter.as_ref())?;

    let hits = knn(state, &shadow, &config, &query, &options, allowed.as_ref())?;
    Ok(render(&only_live(state, &source, hits)?))
}

/// k-NN by whichever path the index cache selects, joined with the filter's
/// admitted set in whichever direction its size warrants.
///
/// The paths return the same shape and score the same way — the exact scan
/// and the keyed join are exhaustive, the graph walk is approximate — so
/// callers do not branch.
fn knn(
    state: &SharedState,
    shadow: &kimmy_storage::CollectionMeta,
    config: &VectorConfig,
    query: &[f32],
    options: &SearchOptions,
    allowed: Option<&Allowed>,
) -> Result<Vec<Hit>, ApiError> {
    knn_joined(state, shadow, config, query, options, allowed.map(|a| (a, a.join())))
}

/// [`knn`] with the join direction chosen by the caller, so a test can run
/// both over one fixture and hold them to the same answer.
fn knn_joined(
    state: &SharedState,
    shadow: &kimmy_storage::CollectionMeta,
    config: &VectorConfig,
    query: &[f32],
    options: &SearchOptions,
    allowed: Option<(&Allowed, Join)>,
) -> Result<Vec<Hit>, ApiError> {
    if let Some((allowed, Join::Keyed)) = allowed {
        return search::vector_search_among(&state.engine, shadow, query, options, &allowed.ids)
            .map_err(vector_error);
    }
    let set = allowed.map(|(a, _)| &a.set);
    match state.vectors.access(&state.engine, shadow, config.metric, config.dim) {
        Access::Approximate(index) => index.search(&state.engine, shadow, query, options, set),
        Access::Exact => search::vector_search(&state.engine, shadow, query, options, set),
    }
    .map_err(vector_error)
}

pub async fn hybrid_search(
    State(state): State<SharedState>,
    auth: Auth,
    Path((db, coll)): Path<(String, String)>,
    JsonBody(body): JsonBody<SearchRequest>,
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
    let (source, shadow, config, options) = prepare(state, auth, db, coll, body)?;

    // Hybrid needs the *text* for the lexical half; a bare vector cannot
    // produce one, so the ambiguity is refused rather than silently degrading
    // to plain vector search.
    let Some(text) = body.query.clone() else {
        return Err(ApiError::bad_request(
            "hybrid_search needs `query` text for its keyword half; use vector_search \
             to search by a pre-computed vector alone",
        ));
    };

    let (weights, min_overlap) = fusion_controls(body)?;

    let query = resolve_query_vector(state, &config, body).await?;
    let allowed = allowed_ids(state, auth, db, coll, body.filter.as_ref())?;

    // Each half is retrieved wider than k, so fusion has enough to work with:
    // a document ranked modestly by both should be able to beat one ranked
    // first by only one.
    let wide = SearchOptions { k: (options.k * 4).min(MAX_K), ..options.clone() };
    let dense = knn(state, &shadow, &config, &query, &wide, allowed.as_ref())?;
    // The overlap gate applies to the lexical half only. A document it drops
    // here is still in `dense` if the dense half ranked it, and keeps that
    // contribution: the gate removes lexical evidence, not documents.
    let lexical = search::keyword_search(&state.engine, &shadow, &text, &wide, min_overlap)
        .map_err(vector_error)?;

    let fused = search::weighted_reciprocal_rank_fusion(
        &[(dense.as_slice(), weights.dense as f32), (lexical.as_slice(), weights.lexical as f32)],
        options.k,
    );
    Ok(render(&only_live(state, &source, fused)?))
}

/// Shared setup: authorize, resolve the shadow collection, read the options.
fn prepare(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    body: &SearchRequest,
) -> Result<
    (kimmy_storage::CollectionMeta, kimmy_storage::CollectionMeta, VectorConfig, SearchOptions),
    ApiError,
> {
    // `search` is its own action; it is implied by `read` but can be granted
    // alone. See docs/security.md.
    auth.require(Action::Search, db, Some(coll))?;

    let meta = crate::exec::collection(state, db, coll)?;
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
            ErrorCode::NoVectors,
            empty_collection_message(db, coll, &config),
        ));
    }

    let options = SearchOptions {
        k: body.k.unwrap_or(DEFAULT_K).clamp(1, MAX_K),
        metric: config.metric,
        per_document: body.per_document.unwrap_or(1).max(1),
    };
    Ok((meta, shadow, config, options))
}

/// Keep only the hits whose source document still exists.
///
/// The shadow collection is maintained *after* the write that changes it: a
/// delete commits, and the embedding worker removes the chunks when it reaches
/// that entry in the stream. Between the two, the chunks are still there to be
/// scored — and ADR-022's promise that "a deleted document cannot surface" was
/// only ever kept for a missing chunk record, not a missing document. The
/// search paths score from the shadow alone, so the check belongs here, once,
/// on whatever they ranked: one point read per hit, after ranking, against the
/// collection the caller actually asked about.
///
/// A hit lost here is not replaced, so a result can be shorter than `k` by the
/// number of deletions the worker has not caught up with. That is the honest
/// answer; padding it would mean ranking again.
fn only_live(
    state: &SharedState,
    source: &kimmy_storage::CollectionMeta,
    hits: Vec<Hit>,
) -> Result<Vec<Hit>, ApiError> {
    let mut live = Vec::with_capacity(hits.len());
    for hit in hits {
        if state.engine.document_stamp(source, &hit.id)?.is_some() {
            live.push(hit);
        }
    }
    Ok(live)
}

/// Turn the request into a query vector.
async fn resolve_query_vector(
    state: &SharedState,
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

    // Under this node's policy, as the worker builds it: the configuration
    // may have arrived by replication and never passed this node's API.
    let provider = kimmy_vector::build(&config.provider, config.dim, &state.providers)
        .map_err(vector_error)?;
    // The query prefix is applied here and nowhere else: a caller-supplied
    // vector was embedded by the caller, prefix and all, or not at all.
    let text = match &config.query_prefix {
        Some(prefix) => format!("{prefix}{text}"),
        None => text.clone(),
    };
    let mut vectors = provider.embed(std::slice::from_ref(&text)).await.map_err(vector_error)?;
    vectors.pop().ok_or_else(|| {
        // The upstream's fault like every other `provider_error` — a
        // well-formed answer with the vector missing from it — so it takes
        // that code's level unchanged, and no override belongs here.
        ApiError::new(
            StatusCode::BAD_GATEWAY,
            ErrorCode::ProviderError,
            "the embedding provider returned no vector for the query",
        )
    })
}

/// The documents a `filter` admitted, in the two shapes the join needs.
struct Allowed {
    /// Every admitted id, for the join that reads their chunks by key.
    ids: Vec<DocId>,
    /// The same ids as the strings a chunk's `source` renders to, for the
    /// join that searches everything and discards.
    set: HashSet<String>,
    /// How the filter was answered — which access path, how much it examined
    /// — so a test can hold the planner to having run. Summed across pages.
    stats: QueryStats,
}

/// The direction a filtered search joins the admitted set with the chunks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Join {
    /// Read the admitted documents' chunks by key and score exactly those.
    Keyed,
    /// Search as if unfiltered and discard hits outside the set.
    Discard,
}

impl Allowed {
    /// Which way round to join, by the size of the set alone.
    fn join(&self) -> Join {
        join_for(self.ids.len())
    }
}

/// The join direction for a set of `admitted` documents. See
/// [`SELECTIVE_JOIN_MAX`].
fn join_for(admitted: usize) -> Join {
    if admitted <= SELECTIVE_JOIN_MAX { Join::Keyed } else { Join::Discard }
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
) -> Result<Option<Allowed>, ApiError> {
    let Some(filter) = filter else {
        return Ok(None);
    };
    // Reading the source documents is a read, distinct from searching.
    auth.require(Action::Read, db, Some(coll))?;

    let parsed = kimmy_query::filter::parse(&json_to_document(filter)?)?;
    let source = crate::exec::collection(state, db, coll)?;
    Ok(Some(filter_ids(state, &source, &parsed, FILTER_PAGE)?))
}

/// The ids a filter matches, through the executor's planner-backed read.
///
/// `collect_matching_after` is the path `find` takes: the primary key when the
/// filter pins `_id`, a secondary index when one applies, a collection scan
/// otherwise — every candidate rechecked against the full filter, whichever
/// it was. So an index on the filtered field serves a search exactly as it
/// serves a `find`, and `explain` on a `find` with the same filter says what
/// the search will get.
///
/// Only the ids are kept. The executor hands back documents, so they are
/// taken a page at a time by encoded key — the way a cursor resumes, and
/// sound for the same reason: every access path delivers `_id` order — and
/// let go once their ids are recorded.
///
/// `exec::visit_matching` now offers the streaming visitor this was written to
/// wait for, so the loop and `page` can collapse into one call that never holds
/// a document. Left as it is here because that is a change to make on its own,
/// with the paging test below turned into whatever replaces it.
fn filter_ids(
    state: &SharedState,
    source: &kimmy_storage::CollectionMeta,
    filter: &kimmy_query::filter::Filter,
    page: usize,
) -> Result<Allowed, ApiError> {
    let mut ids = Vec::new();
    let mut set = HashSet::new();
    let mut stats: Option<QueryStats> = None;
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (docs, page_stats) = crate::exec::collect_matching_stamped_after(
            state,
            source,
            filter,
            Some(page),
            after.as_deref(),
        )?;
        let more = docs.len() >= page;
        let mut last = None;
        // The stamp is the executor's to hand back; admission turns on the
        // filter alone, so it is dropped here.
        for (_, doc) in docs {
            let Some(raw) = doc.get(kimmy_storage::ID_FIELD) else {
                continue;
            };
            let id = DocId::try_from_bson(raw)?;
            last = Some(kimmy_core::keyenc::encode(&id.to_bson())?);
            set.insert(id.to_string());
            ids.push(id);
        }
        match stats.as_mut() {
            // The plan is the same on every page; the counts add up.
            Some(total) => {
                total.examined += page_stats.examined;
                total.matched += page_stats.matched;
            }
            None => stats = Some(page_stats),
        }
        match last {
            Some(key) if more => after = Some(key),
            _ => break,
        }
    }
    let stats = stats.expect("the loop runs at least once");
    Ok(Allowed { ids, set, stats })
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
        // The one place a `not_implemented` is the operator's, so the one
        // place that overrides the code's level (ADR-136). `not_implemented`
        // defaults to `INFO` for its commoner source — a caller asking for a
        // reserved capability, which no operator can grant — but a node that
        // cannot build local embeddings the rest of the cluster is configured
        // to use is a cluster built inconsistently: some member accepted this
        // vector configuration, and this one was compiled or provisioned
        // without what it needs to honour it. Every search of that collection
        // that lands here fails, and it is invisible behind a load balancer
        // because the other members answer. That should page, so it is raised
        // to `ERROR` here rather than the code being lowered around it.
        V::LocalUnavailable | V::ModelUnavailable { .. } => {
            ApiError::new(StatusCode::NOT_IMPLEMENTED, ErrorCode::NotImplemented, e.to_string())
                .at_level(LogLevel::Error)
        }
        // A stored configuration this node's policy refuses, or a profile it
        // does not define, is the deployment's to fix, not the caller's: the
        // configuration passed some member's API, and this member's policy or
        // profiles differ from that one's.
        V::MissingApiKey { .. } | V::PolicyRefused(_) | V::UnknownProfile { .. } => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::Misconfigured,
            e.to_string(),
        ),
        V::Transport { .. } | V::ProviderRejected { .. } | V::MalformedResponse { .. } => {
            ApiError::new(StatusCode::BAD_GATEWAY, ErrorCode::ProviderError, e.to_string())
        }
        V::Storage(inner) => inner.into(),
        V::Core(inner) => inner.into(),
        // Unreachable from a request path: the cache discards a bad snapshot
        // and rebuilds rather than letting the error escape. Mapped anyway,
        // because a match that must be total should not guess.
        V::Snapshot(_) => {
            ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::Snapshot, e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use kimmy_core::{ChunkConfig, Metric, ProviderConfig, VectorRecord, similarity};

    use super::*;

    fn live_state(dir: &tempfile::TempDir) -> SharedState {
        live_state_with(dir, kimmy_vector::ProviderPolicy::default())
    }

    fn live_state_with(
        dir: &tempfile::TempDir,
        providers: kimmy_vector::ProviderPolicy,
    ) -> SharedState {
        let engine = std::sync::Arc::new(
            kimmy_storage::Engine::open(&dir.path().join("kimmy.redb")).unwrap(),
        );
        let tokens =
            kimmy_auth::TokenIssuer::new("an-adequately-long-test-secret-of-32", 3600).unwrap();
        crate::state_with_policies(
            engine,
            tokens,
            false,
            crate::RateLimits::disabled(),
            crate::egress::EgressPolicy::public_only(crate::egress::WEBHOOKS),
            providers,
        )
        .unwrap()
    }

    fn openai(endpoint: Option<&str>, api_key_env: &str) -> ProviderConfig {
        ProviderConfig::OpenAi {
            model: "text-embedding-3-small".into(),
            endpoint: endpoint.map(Into::into),
            api_key_env: api_key_env.into(),
            dimensions: None,
        }
    }

    #[test]
    fn configure_time_refuses_a_node_secret_and_a_private_endpoint_by_name() {
        // The finding, at the door: a `400` that names the variable or the
        // host and the setting that governs it, so the person who typed the
        // configuration learns why while they are still watching.
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);

        for var in [
            "KIMMY_JWT_SECRET",
            "KIMMY_CLUSTER_SECRET",
            "KIMMY_ROOT_PASSWORD",
            "KIMMY_JWT_PREVIOUS_SECRET",
        ] {
            let err =
                admit_provider(&state, &openai(Some("https://93.184.216.34"), var)).expect_err(var);
            assert_eq!(err.status, StatusCode::BAD_REQUEST);
            assert!(err.message.contains(var), "{var}: {}", err.message);
            assert!(err.message.contains("api_key_env"), "{}", err.message);
        }

        let err = admit_provider(&state, &openai(None, "SOMEBODY_ELSES_KEY")).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("SOMEBODY_ELSES_KEY"), "{}", err.message);
        assert!(err.message.contains("vector.provider.allowed_key_env"), "{}", err.message);

        let lan =
            ProviderConfig::Ollama { model: "m".into(), endpoint: "http://10.0.0.5:11434".into() };
        let err = admit_provider(&state, &lan).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("10.0.0.5"), "{}", err.message);
        assert!(err.message.contains("vector.provider.allowed_hosts"), "{}", err.message);

        // The defaults, and the provider namespace, pass with nothing set.
        admit_provider(&state, &openai(None, "OPENAI_API_KEY")).unwrap();
        admit_provider(&state, &openai(Some("https://93.184.216.34"), "KIMMY_PROVIDER_ACME"))
            .unwrap();
        admit_provider(&state, &ProviderConfig::Byo {}).unwrap();
    }

    #[test]
    fn a_locked_node_accepts_only_profiles_byo_and_local() {
        let mut profiles = std::collections::BTreeMap::new();
        profiles.insert(
            "corp".to_string(),
            openai(Some("https://93.184.216.34"), "KIMMY_PROVIDER_CORP"),
        );
        let policy = kimmy_vector::ProviderPolicy::new(
            kimmy_vector::policy::default_allowed_key_env(),
            Vec::new(),
            true,
            profiles,
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let state = live_state_with(&dir, policy);

        let err = admit_provider(&state, &openai(None, "OPENAI_API_KEY")).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("endpoints_locked"), "{}", err.message);

        admit_provider(&state, &ProviderConfig::Profile { name: "corp".into() }).unwrap();
        admit_provider(&state, &ProviderConfig::Byo {}).unwrap();
        let err =
            admit_provider(&state, &ProviderConfig::Profile { name: "nope".into() }).unwrap_err();
        assert!(err.message.contains("vector.providers.nope"), "{}", err.message);

        // The accepted configuration stores the profile's *name* and reads
        // back as it was written; the operator's endpoint stays theirs.
        state.engine.create_collection("app", "docs").unwrap();
        let mut c = config();
        c.provider = ProviderConfig::Profile { name: "corp".into() };
        let meta = state.engine.configure_vectors("app", "docs", c).unwrap();
        let stored = serde_json::to_value(meta.vector.unwrap().provider).unwrap();
        assert_eq!(stored, json!({ "kind": "profile", "name": "corp" }));
    }

    fn config() -> VectorConfig {
        VectorConfig {
            fields: vec!["text".into()],
            provider: ProviderConfig::Byo {},
            dim: 2,
            metric: Metric::Cosine,
            document_prefix: None,
            query_prefix: None,
            chunk: ChunkConfig::default(),
        }
    }

    /// `n` documents `{_id: i, tag: i % 3, text}` with two chunks each,
    /// vectors spread around the unit circle so no two chunks tie.
    fn fixture(
        state: &SharedState,
        n: i64,
    ) -> (kimmy_storage::CollectionMeta, kimmy_storage::CollectionMeta) {
        state.engine.create_collection("app", "docs").unwrap();
        state.engine.configure_vectors("app", "docs", config()).unwrap();
        let source = state.engine.get_collection("app", "docs").unwrap();
        let shadow = state.engine.vector_collection("app", "docs").unwrap().unwrap();
        for i in 0..n {
            state
                .engine
                .insert(&source, bson::doc! { "_id": i, "tag": i % 3, "text": format!("d{i}") })
                .unwrap();
            let records: Vec<VectorRecord> = (0..2u32)
                .map(|chunk| {
                    let angle = (i as f32) * 0.37 + (chunk as f32) * 0.11;
                    VectorRecord {
                        source: DocId::Int64(i),
                        chunk,
                        source_hlc: kimmy_core::Hlc::new(1, 0),
                        vector: vec![angle.cos(), angle.sin()],
                        text: format!("d{i}c{chunk}"),
                    }
                })
                .collect();
            state.engine.put_vectors(&shadow, &DocId::Int64(i), &records).unwrap();
        }
        (source, shadow)
    }

    fn parse(filter: bson::Document) -> kimmy_query::filter::Filter {
        kimmy_query::filter::parse(&filter).unwrap()
    }

    fn sorted_ids(allowed: &Allowed) -> Vec<i64> {
        let mut out: Vec<i64> = allowed
            .ids
            .iter()
            .map(|id| match id {
                DocId::Int64(n) => *n,
                other => panic!("unexpected id {other:?}"),
            })
            .collect();
        out.sort_unstable();
        out
    }

    #[test]
    fn a_filter_on_an_indexed_field_goes_through_the_index() {
        // The whole point of routing the filter through the executor: an
        // index on the filtered field is used, and the planner says so. The
        // same filter without the index scans, and finds the same documents.
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        let (source, _shadow) = fixture(&state, 30);

        let scanned = filter_ids(&state, &source, &parse(bson::doc! { "tag": 1 }), 1_000).unwrap();
        assert!(scanned.stats.index.is_none(), "no index exists yet");
        assert_eq!(scanned.stats.examined, 30, "a scan examines everything");
        assert_eq!(sorted_ids(&scanned), (0..30).filter(|i| i % 3 == 1).collect::<Vec<_>>());

        state
            .engine
            .create_index(
                "app",
                "docs",
                vec![kimmy_storage::IndexField::ascending("tag")],
                false,
                None,
            )
            .unwrap();
        let source = state.engine.get_collection("app", "docs").unwrap();
        let indexed = filter_ids(&state, &source, &parse(bson::doc! { "tag": 1 }), 1_000).unwrap();
        assert!(indexed.stats.index.is_some(), "the index must be planned");
        assert_eq!(indexed.stats.examined, 10, "and only its candidates examined");
        assert_eq!(sorted_ids(&indexed), sorted_ids(&scanned));
        assert_eq!(indexed.set, scanned.set);

        // A filter that pins `_id` is a primary-key read, as it is for `find`.
        let by_id =
            filter_ids(&state, &source, &parse(bson::doc! { "_id": { "$in": [3, 4, 99] } }), 1_000)
                .unwrap();
        assert!(by_id.stats.id_lookup);
        assert_eq!(sorted_ids(&by_id), vec![3, 4]);
    }

    #[test]
    fn a_filters_ids_are_gathered_across_pages() {
        // Paging by key must see every match once, in `_id` order, and add
        // the pages' counts up — including when the last page is exactly
        // full, which is the case that would loop or lose a page if the
        // resume bound were wrong.
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        let (source, _shadow) = fixture(&state, 30);

        for page in [1, 3, 4, 7, 10, 30, 1_000] {
            let all = filter_ids(&state, &source, &parse(bson::doc! {}), page).unwrap();
            let ids: Vec<i64> = all
                .ids
                .iter()
                .map(|id| match id {
                    DocId::Int64(n) => *n,
                    other => panic!("{other:?}"),
                })
                .collect();
            assert_eq!(ids, (0..30).collect::<Vec<_>>(), "page size {page}");
            assert_eq!(all.set.len(), 30);
            assert_eq!(all.stats.matched, 30, "page size {page}");
            assert_eq!(all.stats.examined, 30, "page size {page}");
        }

        let none = filter_ids(&state, &source, &parse(bson::doc! { "tag": 9 }), 4).unwrap();
        assert!(none.ids.is_empty());
        assert_eq!(none.stats.matched, 0);
    }

    #[test]
    fn the_join_direction_turns_on_the_set_size() {
        assert_eq!(join_for(0), Join::Keyed, "an empty set reads nothing");
        assert_eq!(join_for(1), Join::Keyed);
        assert_eq!(join_for(SELECTIVE_JOIN_MAX), Join::Keyed, "the boundary is inclusive");
        assert_eq!(join_for(SELECTIVE_JOIN_MAX + 1), Join::Discard);

        // And the set consults the same rule.
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        let (source, _shadow) = fixture(&state, 3);
        let small = filter_ids(&state, &source, &parse(bson::doc! {}), 1_000).unwrap();
        assert_eq!(small.join(), Join::Keyed);
    }

    /// Every chunk of every admitted document scored by hand, sorted by score
    /// then key, capped per document, cut to `k`.
    fn brute_force(
        state: &SharedState,
        shadow: &kimmy_storage::CollectionMeta,
        query: &[f32],
        options: &SearchOptions,
        allowed: &HashSet<String>,
    ) -> Vec<(String, u32, f32, String)> {
        let mut all = Vec::new();
        state
            .engine
            .for_each_vector(shadow, |r| {
                if allowed.contains(&r.source.to_string()) {
                    let score = similarity(query, &r.vector, options.metric);
                    all.push((r.source.to_string(), r.chunk, score, r.text));
                }
                Ok(true)
            })
            .unwrap();
        all.sort_by(|a, b| {
            b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)).then_with(|| a.1.cmp(&b.1))
        });
        let mut per_doc: HashMap<String, usize> = HashMap::new();
        let mut out = Vec::new();
        for row in all {
            let seen = per_doc.entry(row.0.clone()).or_insert(0);
            if *seen >= options.per_document {
                continue;
            }
            *seen += 1;
            out.push(row);
            if out.len() >= options.k {
                break;
            }
        }
        out
    }

    #[test]
    fn both_join_directions_return_the_filtered_top_k() {
        // The keyed join and the discarding join over the same admitted set
        // must both equal brute force — ids, chunks, scores and text — and so
        // each other. The collection is under the graph threshold, so the
        // discarding join is the exact scan and the comparison is exact.
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        let (source, shadow) = fixture(&state, 40);
        let config = config();
        let query = [0.2f32, -0.98];

        let allowed =
            filter_ids(&state, &source, &parse(bson::doc! { "tag": { "$in": [0, 2] } }), 1_000)
                .unwrap();
        assert_eq!(allowed.ids.len(), 27);

        for (k, per_document) in [(1, 1), (5, 1), (5, 2), (10, 2), (200, 2)] {
            let options = SearchOptions { k, metric: config.metric, per_document };
            let expected = brute_force(&state, &shadow, &query, &options, &allowed.set);
            for join in [Join::Keyed, Join::Discard] {
                let hits =
                    knn_joined(&state, &shadow, &config, &query, &options, Some((&allowed, join)))
                        .unwrap();
                let got: Vec<(String, u32, f32, String)> = hits
                    .into_iter()
                    .map(|h| (h.id.to_string(), h.chunk, h.score, h.text))
                    .collect();
                assert_eq!(got, expected, "{join:?}, k {k}, per_document {per_document}");
            }
        }

        // An empty admitted set: nothing, by either direction.
        let none = filter_ids(&state, &source, &parse(bson::doc! { "tag": 7 }), 1_000).unwrap();
        for join in [Join::Keyed, Join::Discard] {
            let options = SearchOptions { k: 5, metric: config.metric, per_document: 1 };
            let hits = knn_joined(&state, &shadow, &config, &query, &options, Some((&none, join)))
                .unwrap();
            assert!(hits.is_empty(), "{join:?}");
        }
    }

    fn request(weights: Option<(f64, f64)>, min_overlap: Option<usize>) -> SearchRequest {
        SearchRequest {
            weights: weights.map(|(dense, lexical)| FusionWeights { dense, lexical }),
            min_overlap,
            ..Default::default()
        }
    }

    #[test]
    fn absent_fusion_controls_are_equal_weights_and_a_gate_of_one() {
        // The whole promise of ADR-094: a request that does not ask ranks
        // exactly as it did before the fields existed.
        let (weights, min_overlap) = fusion_controls(&request(None, None)).unwrap();
        assert_eq!(weights, FusionWeights { dense: 1.0, lexical: 1.0 });
        assert_eq!(min_overlap, 1);
    }

    #[test]
    fn a_missing_weight_defaults_to_one() {
        let body: SearchRequest =
            serde_json::from_value(json!({ "weights": { "lexical": 0.25 } })).unwrap();
        let (weights, _) = fusion_controls(&body).unwrap();
        assert_eq!(weights, FusionWeights { dense: 1.0, lexical: 0.25 });
    }

    #[test]
    fn meaningless_fusion_controls_are_refused_by_name() {
        for (body, expected) in [
            (request(Some((-1.0, 1.0)), None), "weights.dense"),
            (request(Some((1.0, -0.5)), None), "weights.lexical"),
            (request(Some((0.0, 0.0)), None), "both be 0"),
            (request(None, Some(0)), "min_overlap"),
        ] {
            let error = fusion_controls(&body).expect_err(expected);
            assert_eq!(error.status, StatusCode::BAD_REQUEST);
            assert!(error.message.contains(expected), "{expected} not named in: {}", error.message);
        }
    }

    #[test]
    fn a_zero_weight_on_one_half_is_allowed() {
        // Switching a half off is a legitimate way to see what the other one
        // contributes; only switching *both* off is meaningless.
        assert!(fusion_controls(&request(Some((1.0, 0.0)), None)).is_ok());
        assert!(fusion_controls(&request(Some((0.0, 1.0)), None)).is_ok());
    }

    #[test]
    fn the_two_sources_of_not_implemented_do_not_log_at_the_same_level() {
        // This is the whole reason the level is a per-instance override and
        // not only a per-code property (ADR-136): both of these answer `501
        // not_implemented`, a client cannot tell them apart and should not
        // have to, and they have opposite owners. A caller asking for a
        // reserved capability is nobody's problem to fix. A node that cannot
        // build the embeddings its cluster's stored configuration calls for
        // is a member provisioned unlike its peers — every search of that
        // collection that lands here fails, and behind a load balancer that
        // is invisible, which is exactly the shape of fault that must page.
        //
        // Reachable from here and only from here: one source is `error.rs`'s
        // `CoreError` mapping, the other is this module's `vector_error`, and
        // this is the one test module that can see both.
        let reserved: ApiError =
            kimmy_core::Error::Unsupported("coordinated unique enforcement".into()).into();
        let no_model = vector_error(kimmy_vector::VectorError::LocalUnavailable);

        assert_eq!(reserved.code, ErrorCode::NotImplemented);
        assert_eq!(no_model.code, ErrorCode::NotImplemented);
        assert_eq!(reserved.status, no_model.status, "the wire answer is deliberately the same");

        assert_eq!(reserved.log_level(), Some(LogLevel::Info));
        assert_eq!(no_model.log_level(), Some(LogLevel::Error));

        // And the unbuilt-model case is raised at construction rather than
        // the code being lowered around it, so a third source added later
        // inherits the documented refusal's level and has to argue its way up.
        assert!(reserved.level_override.is_none());
        assert_eq!(no_model.level_override, Some(LogLevel::Error));

        // The other arm of the same match, which is the same condition
        // reported by a provider that names a model it cannot load.
        let unknown_model = vector_error(kimmy_vector::VectorError::ModelUnavailable {
            model: "a-model-this-build-does-not-have".into(),
            detail: "not present on this node".into(),
        });
        assert_eq!(unknown_model.log_level(), Some(LogLevel::Error));
    }
}
