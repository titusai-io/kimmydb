//! Operations shared by the HTTP and MCP edges.
//!
//! Both edges are thin: they parse their own wire format and then call in here.
//! That is deliberate. The MCP server exists in-process precisely so that an
//! agent tool cannot end up more permissive than the REST route beside it, and
//! the cheapest way to guarantee that is to give them one body of code with the
//! authorization check inside it rather than beside it.
//!
//! So every function here takes an [`Auth`] and checks it *first*. A caller
//! cannot reach the engine without passing through one of these.

use kimmy_auth::Action;
use kimmy_core::DocId;
use kimmy_query::{aggregate, filter, plan, shape, update};
use kimmy_storage::CollectionMeta;
use serde_json::{Value, json};

use crate::error::ApiError;
use crate::json::{document_to_json, json_to_document};
use crate::state::{Auth, SharedState};

/// Default page size, so an unbounded `find` cannot be used to pull an entire
/// collection into memory by accident.
pub const DEFAULT_LIMIT: usize = 100;
pub const MAX_LIMIT: usize = 10_000;

// ---------------------------------------------------------------------------
// Authorization
// ---------------------------------------------------------------------------

/// Resolve a collection after checking the caller may act on it.
///
/// The authorization check comes first so that a denied request cannot
/// distinguish "forbidden" from "does not exist" by its status code.
pub fn authorize(
    state: &SharedState,
    auth: &Auth,
    action: Action,
    db: &str,
    coll: &str,
) -> Result<CollectionMeta, ApiError> {
    auth.require(action, db, Some(coll))?;
    Ok(state.engine.get_collection(db, coll)?)
}

// ---------------------------------------------------------------------------
// Databases and collections
// ---------------------------------------------------------------------------

pub fn list_databases(state: &SharedState, auth: &Auth) -> Result<Value, ApiError> {
    let names: Vec<String> = state
        .engine
        .list_databases()?
        .into_iter()
        .map(|d| d.name)
        // Hide databases the caller cannot read, rather than revealing that
        // they exist.
        .filter(|name| auth.principal().can(Action::Read, name, None))
        .collect();
    Ok(json!({ "databases": names }))
}

pub fn list_collections(state: &SharedState, auth: &Auth, db: &str) -> Result<Value, ApiError> {
    let all = state.engine.list_collections(db)?;
    let names: Vec<&str> =
        auth.principal().visible(Action::Read, db, all.iter().map(|c| c.name.as_str()));
    Ok(json!({ "collections": names }))
}

pub fn create_collection(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    name: &str,
) -> Result<Value, ApiError> {
    auth.require(Action::Admin, db, Some(name))?;
    let meta = state.engine.create_collection(db, name)?;
    Ok(json!({ "created": meta.name, "id": meta.id.0 }))
}

pub fn drop_collection(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
) -> Result<Value, ApiError> {
    auth.require(Action::Admin, db, Some(coll))?;
    Ok(json!({ "dropped": state.engine.drop_collection(db, coll)? }))
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/// Everything `find` and `count` can be asked to do.
#[derive(Default)]
pub struct FindParams {
    pub filter: Option<Value>,
    pub sort: Option<Value>,
    pub projection: Option<Value>,
    pub limit: Option<usize>,
    pub skip: Option<usize>,
    /// Report how the query was answered alongside the results.
    pub explain: bool,
}

pub fn find(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    params: FindParams,
) -> Result<Value, ApiError> {
    let meta = authorize(state, auth, Action::Read, db, coll)?;

    let filter = parse_filter(params.filter.as_ref())?;
    let sort = match &params.sort {
        Some(v) => shape::parse_sort(&json_to_document(v)?)?,
        None => Vec::new(),
    };
    let projection = match &params.projection {
        Some(v) => shape::parse_projection(&json_to_document(v)?)?,
        None => None,
    };
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let skip = params.skip.unwrap_or(0);

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
    if params.explain {
        body["explain"] = stats.to_json();
    }
    Ok(body)
}

pub fn count(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    params: FindParams,
) -> Result<Value, ApiError> {
    let meta = authorize(state, auth, Action::Read, db, coll)?;
    let filter = parse_filter(params.filter.as_ref())?;

    // No early exit: a count must see every match.
    let (matched, stats) = collect_matching(state, &meta, &filter, None)?;

    let mut body = json!({ "count": matched.len() });
    if params.explain {
        body["explain"] = stats.to_json();
    }
    Ok(body)
}

pub fn get_doc(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    id: &str,
) -> Result<Value, ApiError> {
    let meta = authorize(state, auth, Action::Read, db, coll)?;
    let doc_id = parse_id(id)?;
    match state.engine.get(&meta, &doc_id)? {
        Some(doc) => Ok(document_to_json(&doc)),
        None => Err(ApiError::not_found(format!("no document with _id {id}"))),
    }
}

/// How a query was answered, for `explain`.
pub struct QueryStats {
    pub index: Option<String>,
    pub fields_used: usize,
    pub examined: usize,
    pub matched: usize,
}

impl QueryStats {
    pub fn to_json(&self) -> Value {
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
pub fn collect_matching(
    state: &SharedState,
    meta: &CollectionMeta,
    filter: &filter::Filter,
    stop_after: Option<usize>,
) -> Result<(Vec<bson::Document>, QueryStats), ApiError> {
    let mut plan = plan::choose(filter, &meta.indexes);
    let mut matched = Vec::new();
    let mut examined = 0usize;

    // A plan that intersected both ends of a range is only sound while the
    // index is not multikey — and it was chosen from a metadata read that is
    // already stale. The checked scan re-reads the flag in the same snapshot
    // as the scan; `None` means a write flipped it in between, and the honest
    // answer is to fall back to scanning the collection. That can happen at
    // most once per index, ever, since the flag never clears.
    let candidates = match &plan {
        Some(p) if p.both_bounds => {
            let checked = state
                .engine
                .index_candidates_unless_multikey(meta, p.index_id, &p.lower, &p.upper)?;
            if checked.is_none() {
                plan = None;
            }
            checked
        }
        Some(p) => Some(state.engine.index_candidates(meta, p.index_id, &p.lower, &p.upper)?),
        None => None,
    };

    match candidates {
        Some(candidates) => {
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

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

pub fn insert(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    document: &Value,
) -> Result<Value, ApiError> {
    let meta = authorize(state, auth, Action::Write, db, coll)?;
    let doc = json_to_document(document)?;
    let id = state.engine.insert(&meta, doc)?;
    Ok(json!({ "insertedId": crate::json::bson_to_json(&id.to_bson()) }))
}

pub fn replace(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    id: &str,
    document: &Value,
    upsert: bool,
) -> Result<Value, ApiError> {
    let meta = authorize(state, auth, Action::Write, db, coll)?;
    let doc_id = parse_id(id)?;
    let doc = json_to_document(document)?;
    let outcome = state.engine.replace(&meta, &doc_id, doc, upsert)?;
    Ok(json!({
        "matched": outcome.matched,
        "modified": outcome.modified,
        "upserted": outcome.upserted,
    }))
}

pub fn delete_by_id(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    id: &str,
) -> Result<Value, ApiError> {
    let meta = authorize(state, auth, Action::Write, db, coll)?;
    let doc_id = parse_id(id)?;
    let deleted = state.engine.delete(&meta, &doc_id)?;
    Ok(json!({ "deleted": u8::from(deleted) }))
}

pub fn update(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    filter_json: Option<&Value>,
    update_json: &Value,
    multi: bool,
) -> Result<Value, ApiError> {
    let meta = authorize(state, auth, Action::Write, db, coll)?;
    let filter = parse_filter(filter_json)?;
    let update = update::parse(&json_to_document(update_json)?)?;

    // Collect the targets first: mutating while scanning would mean the read
    // transaction and the write transaction disagree about what matched.
    let mut targets = Vec::new();
    state.engine.for_each_doc(&meta, |id, doc| {
        if filter::matches(&filter, &doc) {
            targets.push((id, doc));
        }
        Ok(multi || targets.is_empty())
    })?;

    let now = now_millis();
    let mut modified = 0u64;
    for (id, mut doc) in targets {
        update::apply(&update, &mut doc, now)?;
        state.engine.replace(&meta, &id, doc, false)?;
        modified += 1;
    }

    Ok(json!({ "matched": modified, "modified": modified }))
}

pub fn delete(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    filter_json: Option<&Value>,
    multi: bool,
) -> Result<Value, ApiError> {
    let meta = authorize(state, auth, Action::Write, db, coll)?;
    let filter = parse_filter(filter_json)?;

    let mut targets = Vec::new();
    state.engine.for_each_doc(&meta, |id, doc| {
        if filter::matches(&filter, &doc) {
            targets.push(id);
        }
        Ok(multi || targets.is_empty())
    })?;

    let mut deleted = 0u64;
    for id in targets {
        if state.engine.delete(&meta, &id)? {
            deleted += 1;
        }
    }
    Ok(json!({ "deleted": deleted }))
}

// ---------------------------------------------------------------------------
// Indexes
// ---------------------------------------------------------------------------

/// One field of an index definition.
pub struct IndexFieldSpec {
    pub path: String,
    pub descending: bool,
}

/// An index to create.
#[derive(Default)]
pub struct IndexSpec {
    pub fields: Vec<IndexFieldSpec>,
    pub unique: bool,
    pub name: Option<String>,
    /// `"local"` (default) or `"coordinated"`.
    pub enforcement: Option<String>,
}

pub fn create_index(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    spec: IndexSpec,
) -> Result<Value, ApiError> {
    auth.require(Action::Admin, db, Some(coll))?;

    let fields: Vec<kimmy_storage::IndexField> = spec
        .fields
        .into_iter()
        .map(|f| kimmy_storage::IndexField { path: f.path, descending: f.descending })
        .collect();

    let enforcement = match spec.enforcement.as_deref() {
        None | Some("local") => kimmy_storage::Enforcement::Local,
        Some("coordinated") => kimmy_storage::Enforcement::Coordinated,
        Some(other) => {
            return Err(ApiError::bad_request(format!(
                "unknown enforcement {other:?}: expected \"local\" or \"coordinated\""
            )));
        }
    };

    let index =
        state.engine.create_index_with(db, coll, fields, spec.unique, enforcement, spec.name)?;
    Ok(index_to_json(&index))
}

pub fn list_indexes(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
) -> Result<Value, ApiError> {
    authorize(state, auth, Action::Read, db, coll)?;
    let indexes: Vec<Value> =
        state.engine.list_indexes(db, coll)?.iter().map(index_to_json).collect();
    Ok(json!({ "indexes": indexes }))
}

pub fn drop_index(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    name: &str,
) -> Result<Value, ApiError> {
    auth.require(Action::Admin, db, Some(coll))?;
    Ok(json!({ "dropped": state.engine.drop_index(db, coll, name)? }))
}

pub fn index_to_json(index: &kimmy_storage::IndexMeta) -> Value {
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
        // Surfaced so an operator can see *why* a two-sided range on this
        // index does not stop at its upper bound.
        "multikey": index.multikey,
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn parse_filter(value: Option<&Value>) -> Result<filter::Filter, ApiError> {
    match value {
        Some(v) => Ok(filter::parse(&json_to_document(v)?)?),
        None => Ok(filter::Filter::AlwaysTrue),
    }
}

/// Interpret a path segment as a document id.
///
/// A 24-character hex string is read as an ObjectId and an integer as an
/// integer, matching how ids are most often written; anything else is a string.
pub fn parse_id(raw: &str) -> Result<DocId, ApiError> {
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

pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

/// Run an aggregation pipeline.
///
/// # Authorization
///
/// The source collection is checked like any read. **`$lookup` is checked
/// separately, against the collection it names**, because a join reads a second
/// collection and a caller granted `read` on `orders` must not be able to pull
/// `users` through it. That would be a privilege escalation shaped like a
/// query, and it is exactly the kind of second path around
/// [`authorize`](self::authorize) that ADR-024 exists to prevent.
///
/// A denied `$lookup` returns the same uniform 403 as any other refusal, so the
/// pipeline cannot be used to probe which collections exist.
///
/// # Consistency
///
/// Each stage reads storage when it runs, so a `$lookup` sees the foreign
/// collection as of *its own* execution rather than a snapshot taken when the
/// pipeline began. In a leaderless store with no multi-document transactions
/// there is no cross-collection snapshot to take — see ADR-006 — so this is
/// inherent rather than an omission.
pub fn aggregate(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    pipeline: &Value,
) -> Result<Value, ApiError> {
    let meta = authorize(state, auth, Action::Read, db, coll)?;

    let stages = parse_pipeline(pipeline)?;
    let limits = aggregate::Limits::default();

    // The whole collection is the pipeline's input, bounded by the same cap
    // every stage is held to, so a pipeline over an oversized collection fails
    // at the source rather than after allocating it.
    let mut docs: Vec<bson::Document> = Vec::new();
    state.engine.for_each_doc(&meta, |_id, doc| {
        docs.push(doc);
        Ok(true)
    })?;
    aggregate::check_limit("the source collection", docs.len(), &limits)?;

    for stage in &stages {
        docs = match stage {
            aggregate::Stage::Lookup { from, local_field, foreign_field, as_field } => {
                lookup(state, auth, db, from, local_field, foreign_field, as_field, docs, &limits)?
            }
            other => aggregate::apply(other, docs, &limits)?,
        };
    }

    let documents: Vec<Value> = docs.iter().map(document_to_json).collect();
    Ok(json!({ "documents": documents, "count": documents.len() }))
}

fn parse_pipeline(pipeline: &Value) -> Result<Vec<aggregate::Stage>, ApiError> {
    let Some(array) = pipeline.as_array() else {
        return Err(ApiError::bad_request("pipeline must be an array of stages"));
    };
    let stages: Vec<bson::Document> =
        array.iter().map(json_to_document).collect::<Result<_, _>>()?;
    Ok(aggregate::parse(&stages)?)
}

/// Join against another collection, in one pass over it.
///
/// The foreign side is scanned **once** and indexed in memory by the join key,
/// rather than queried per input document. A per-document join is O(n·m), which
/// on any real pair of collections is the difference between a query and an
/// outage.
#[allow(clippy::too_many_arguments)]
fn lookup(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    from: &str,
    local_field: &str,
    foreign_field: &str,
    as_field: &str,
    input: Vec<bson::Document>,
    limits: &aggregate::Limits,
) -> Result<Vec<bson::Document>, ApiError> {
    // The second authorization point. See this function's caller.
    let foreign = authorize(state, auth, Action::Read, db, from)?;

    let wanted: std::collections::HashSet<Vec<u8>> =
        aggregate::lookup_keys(&input, local_field).iter().filter_map(encode_key).collect();

    let mut matches: std::collections::HashMap<Vec<u8>, Vec<bson::Bson>> =
        std::collections::HashMap::new();
    let mut held = 0usize;
    state.engine.for_each_doc(&foreign, |_id, doc| {
        let Some(value) = kimmy_core::path::resolve(&doc, foreign_field).into_iter().next() else {
            return Ok(true);
        };
        let Some(key) = encode_key(value) else {
            return Ok(true);
        };
        if wanted.contains(&key) {
            matches.entry(key).or_default().push(bson::Bson::Document(doc));
            held += 1;
        }
        Ok(true)
    })?;
    // The joined documents are held in memory alongside the input, so they are
    // subject to the same ceiling.
    aggregate::check_limit("$lookup", held, limits)?;

    let mut out = Vec::with_capacity(input.len());
    for mut doc in input {
        let key =
            kimmy_core::path::resolve(&doc, local_field).into_iter().next().and_then(encode_key);
        let joined = key.and_then(|k| matches.get(&k).cloned()).unwrap_or_default();
        // Always an array, even when empty: a field whose type depends on
        // whether anything matched forces every caller to handle two shapes.
        doc.insert(as_field.to_string(), bson::Bson::Array(joined));
        out.push(doc);
    }
    Ok(out)
}

fn encode_key(value: &bson::Bson) -> Option<Vec<u8>> {
    kimmy_core::keyenc::encode(value).ok()
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
