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

use bson::Document;
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

/// Documents accepted by one bulk insert.
///
/// A batch is one transaction, so the whole of it is held in memory and then
/// published as one event per document. The ceiling is well past where the
/// per-commit saving flattens out, and in practice the request body limit binds
/// first for anything but tiny documents.
pub const MAX_BULK_INSERT: usize = 1000;

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
    /// Index ranges scanned: 1 for a plain index plan, several for a `$in`
    /// union, 0 for a collection scan.
    pub probes: usize,
}

impl QueryStats {
    pub fn to_json(&self) -> Value {
        let strategy = match (&self.index, self.probes) {
            (None, _) => "collectionScan",
            // A union of equality probes is a different shape of work from
            // one range scan, and the difference is what `$in` planning
            // bought — so it is named rather than folded into "index".
            (Some(_), n) if n > 1 => "indexUnion",
            (Some(_), _) => "index",
        };
        let mut out = json!({
            "strategy": strategy,
            "index": self.index,
            "indexFieldsUsed": self.fields_used,
            "documentsExamined": self.examined,
            "documentsMatched": self.matched,
        });
        if self.probes > 1 {
            out["probes"] = json!(self.probes);
        }
        out
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
            // A both-bounds plan is always a single intersected range.
            let (lower, upper) = &p.ranges[0];
            let checked =
                state.engine.index_candidates_unless_multikey(meta, p.index_id, lower, upper)?;
            if checked.is_none() {
                plan = None;
            }
            checked
        }
        Some(p) => {
            // One range for a plain plan, several for a `$in` union. The set
            // deduplicates across probes: one document can appear under two of
            // them when an array holds two of the listed values, and examining
            // it twice would double-count it in the result.
            let mut union = std::collections::BTreeSet::new();
            for (lower, upper) in &p.ranges {
                union.extend(state.engine.index_candidates(meta, p.index_id, lower, upper)?);
            }
            Some(union.into_iter().collect())
        }
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
        probes: plan.as_ref().map_or(0, |p| p.ranges.len()),
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

/// Insert many documents in one durable commit, or none of them.
///
/// One `authorize` call for the batch, which is one audit record: a bulk load
/// is one thing the principal asked for, not N things.
pub fn insert_many(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    documents: &[Value],
) -> Result<Value, ApiError> {
    let meta = authorize(state, auth, Action::Write, db, coll)?;

    if documents.len() > MAX_BULK_INSERT {
        return Err(ApiError::bad_request(format!(
            "a bulk insert takes at most {MAX_BULK_INSERT} documents, got {}",
            documents.len()
        )));
    }

    // Convert everything before opening a transaction, so a malformed document
    // is a 400 that never touched the engine.
    let docs = documents
        .iter()
        .enumerate()
        .map(|(i, value)| json_to_document(value).map_err(|e| at_index(i, e)))
        .collect::<Result<Vec<_>, _>>()?;

    let ids = state.engine.insert_many(&meta, docs).map_err(|e| match e.index {
        Some(i) => at_index(i, e.source.into()),
        None => e.source.into(),
    })?;

    Ok(json!({
        "inserted": ids.len(),
        "insertedIds": ids
            .iter()
            .map(|id| crate::json::bson_to_json(&id.to_bson()))
            .collect::<Vec<_>>(),
    }))
}

/// Name the offending document's position without changing the error envelope.
///
/// A batch is all-or-nothing, so there is no partial result to point at — the
/// position is the only thing that tells the caller what to fix.
fn at_index(index: usize, e: ApiError) -> ApiError {
    ApiError { message: format!("document at index {index}: {}", e.message), ..e }
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

/// A filtered write's parameters, mirroring [`FindParams`].
///
/// A struct rather than four positional flags for the same reason `find` has
/// one: `update(state, auth, db, coll, json, true, false)` reads as a puzzle at
/// every call site.
#[derive(Default)]
pub struct WriteParams {
    pub filter: Option<Value>,
    /// Change every match rather than only the first.
    pub multi: bool,
    /// Report how the targets were found, as `find` does.
    pub explain: bool,
}

pub fn update(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    update_json: &Value,
    params: WriteParams,
) -> Result<Value, ApiError> {
    let (multi, explain) = (params.multi, params.explain);
    let meta = authorize(state, auth, Action::Write, db, coll)?;
    let filter = parse_filter(params.filter.as_ref())?;
    let update = update::parse(&json_to_document(update_json)?)?;

    // Collect the targets first: mutating while scanning would mean the read
    // transaction and the write transaction disagree about what matched.
    //
    // Through `collect_matching`, which is what `find` uses — so an index
    // applies here exactly as it does to a read. This path used to call
    // `for_each_doc` directly and scan the whole collection however selective
    // the filter was.
    let stop_after = if multi { None } else { Some(1) };
    let (targets, stats) = collect_matching(state, &meta, &filter, stop_after)?;
    let matched = targets.len() as u64;

    let now = now_millis();
    let mut modified = 0u64;
    for mut doc in targets {
        let id = document_id(&doc)?;
        update::apply(&update, &mut doc, now)?;
        // Counted from the write's own answer rather than assumed. The match
        // came from a read transaction, so a document deleted between the two
        // reports `matched: false` and writes nothing — claiming it as
        // modified would be a number nobody could reconcile with the data.
        if state.engine.replace(&meta, &id, doc, false)?.modified {
            modified += 1;
        }
    }

    let mut body = json!({ "matched": matched, "modified": modified });
    if explain {
        body["explain"] = stats.to_json();
    }
    Ok(body)
}

/// The `_id` of a document that came out of storage.
///
/// Every stored document has one — `insert` assigns it when absent — so this
/// failing means the record is corrupt rather than the request being wrong.
fn document_id(doc: &bson::Document) -> Result<DocId, ApiError> {
    let value = doc
        .get(kimmy_storage::ID_FIELD)
        .ok_or_else(|| ApiError::internal("a stored document has no _id".to_string()))?;
    Ok(DocId::try_from_bson(value)?)
}

// ---------------------------------------------------------------------------
// find_and_modify
// ---------------------------------------------------------------------------

/// Which image the caller wants back.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ReturnDocument {
    #[default]
    Before,
    After,
}

/// A `find_and_modify` request, already parsed.
#[derive(Default)]
pub struct FindAndModifySpec {
    pub filter: Option<Value>,
    pub sort: Option<Value>,
    /// Update operators, or a whole replacement document.
    pub update: Option<Value>,
    pub remove: bool,
    pub upsert: bool,
    pub return_document: ReturnDocument,
    pub projection: Option<Value>,
}

/// The caller's half of [`kimmy_storage::ModifySpec`] — pure functions over
/// documents, evaluated inside the engine's write transaction.
struct Modify<'a> {
    filter: &'a filter::Filter,
    sort: &'a [shape::SortKey],
    /// `None` means remove.
    update: Option<&'a update::Update>,
    upsert: Option<Document>,
    now: i64,
}

impl kimmy_storage::ModifySpec for Modify<'_> {
    fn matches(&self, doc: &Document) -> bool {
        filter::matches(self.filter, doc)
    }

    fn compare(&self, a: &Document, b: &Document) -> std::cmp::Ordering {
        shape::compare(self.sort, a, b)
    }

    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
        let Some(update) = self.update else {
            return Ok(None);
        };
        let mut next = doc.clone();
        update::apply(update, &mut next, self.now).map_err(|e| e.to_string())?;
        Ok(Some(next))
    }

    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        self.upsert.clone().map(Ok)
    }
}

/// Equality constraints a match necessarily satisfies, for seeding an upsert.
///
/// Only `$and`-reachable `$eq` on a plain path counts. An equality inside `$or`
/// is **not** implied by a match, so seeding from it would invent a field the
/// caller never asked for — the kind of quiet wrongness that is hard to notice
/// in a document that otherwise looks right.
fn implied_equalities(filter: &filter::Filter, out: &mut Document) {
    match filter {
        filter::Filter::And(branches) => {
            for branch in branches {
                implied_equalities(branch, out);
            }
        }
        filter::Filter::Field { path, conditions } => {
            for condition in conditions {
                if let filter::Condition::Eq(value) = condition {
                    // Dotted paths go through `path::set` so `{"a.b": 1}`
                    // seeds a nested document rather than a literal key.
                    let _ = kimmy_core::path::set(out, path, value.clone());
                }
            }
        }
        _ => {}
    }
}

pub fn find_and_modify(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    spec: FindAndModifySpec,
) -> Result<Value, ApiError> {
    let meta = authorize(state, auth, Action::Write, db, coll)?;

    // Mutually exclusive rather than silently preferring one: a request that
    // asks to both change and remove a document has no defensible reading.
    if spec.remove && spec.update.is_some() {
        return Err(ApiError::bad_request(
            "find_and_modify takes either `update` or `remove: true`, not both",
        ));
    }
    if !spec.remove && spec.update.is_none() {
        return Err(ApiError::bad_request("find_and_modify needs an `update`, or `remove: true`"));
    }
    if spec.remove && spec.upsert {
        return Err(ApiError::bad_request("`remove` cannot be combined with `upsert`"));
    }
    if spec.remove && spec.return_document == ReturnDocument::After {
        return Err(ApiError::bad_request(
            "`remove` has no document after it; use returnDocument \"before\"",
        ));
    }

    let filter = parse_filter(spec.filter.as_ref())?;
    let sort = match &spec.sort {
        Some(value) => shape::parse_sort(&json_to_document(value)?)?,
        None => Vec::new(),
    };
    let projection = match &spec.projection {
        Some(value) => shape::parse_projection(&json_to_document(value)?)?,
        None => None,
    };
    let update = match &spec.update {
        Some(value) => Some(update::parse(&json_to_document(value)?)?),
        None => None,
    };

    let now = now_millis();

    // The upsert image is built here rather than in the engine, because it
    // needs the filter and the update operators — both query-language things.
    let upsert_doc = if spec.upsert {
        let mut seed = Document::new();
        implied_equalities(&filter, &mut seed);
        if let Some(update) = &update {
            update::apply(update, &mut seed, now)?;
        }
        Some(seed)
    } else {
        None
    };

    // The planner runs out here; the engine re-validates a both-bounds plan
    // inside the transaction that scans, which is stricter than the read path
    // can be.
    let candidates = match plan::choose(&filter, &meta.indexes) {
        Some(p) => kimmy_storage::Candidates::Index {
            index_id: p.index_id,
            ranges: p.ranges.clone(),
            both_bounds: p.both_bounds,
        },
        None => kimmy_storage::Candidates::Scan,
    };

    let modify =
        Modify { filter: &filter, sort: &sort, update: update.as_ref(), upsert: upsert_doc, now };

    let outcome = state.engine.find_and_modify(&meta, &candidates, &modify)?;

    let returned = match spec.return_document {
        ReturnDocument::Before => outcome.before.clone(),
        ReturnDocument::After => outcome.after.clone(),
    };
    let document = match returned {
        Some(doc) => document_to_json(&shape::project(projection.as_ref(), &doc)),
        None => Value::Null,
    };

    let mut body = json!({
        "document": document,
        "matched": u64::from(outcome.matched),
    });
    if let Some(id) = outcome.upserted {
        body["upsertedId"] = crate::json::bson_to_json(&id.to_bson());
    }
    Ok(body)
}

pub fn delete(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    params: WriteParams,
) -> Result<Value, ApiError> {
    let (multi, explain) = (params.multi, params.explain);
    let meta = authorize(state, auth, Action::Write, db, coll)?;
    let filter = parse_filter(params.filter.as_ref())?;

    // Planner-aware, for the same reason `update` is.
    let stop_after = if multi { None } else { Some(1) };
    let (targets, stats) = collect_matching(state, &meta, &filter, stop_after)?;

    let mut deleted = 0u64;
    for doc in targets {
        let id = document_id(&doc)?;
        if state.engine.delete(&meta, &id)? {
            deleted += 1;
        }
    }

    let mut body = json!({ "deleted": deleted });
    if explain {
        body["explain"] = stats.to_json();
    }
    Ok(body)
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
    /// Present makes this a TTL index — see [`kimmy_storage::IndexMeta`].
    pub expire_after_seconds: Option<i64>,
    /// Present makes this a partial index, holding only matching documents.
    pub partial_filter_expression: Option<Value>,
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

    let partial_filter = match &spec.partial_filter_expression {
        Some(value) => Some(json_to_document(value)?),
        None => None,
    };

    let index = state.engine.create_index_with(
        db,
        coll,
        fields,
        spec.unique,
        enforcement,
        spec.name,
        spec.expire_after_seconds,
        partial_filter,
    )?;
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
    let mut out = json!({
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
    });
    // Added only when set, so listing ordinary indexes does not suggest every
    // one of them carries an expiry policy that happens to be null.
    if let Some(secs) = index.expire_after_secs {
        out["expireAfterSeconds"] = json!(secs);
    }
    if let Some(filter) = &index.partial_filter {
        out["partialFilterExpression"] = document_to_json(filter);
    }
    out
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

    // -----------------------------------------------------------------------
    // explain rendering — every branch found unwatched by mutation testing
    // -----------------------------------------------------------------------

    fn stats(index: Option<&str>, probes: usize) -> QueryStats {
        QueryStats {
            index: index.map(str::to_string),
            fields_used: usize::from(index.is_some()),
            examined: 0,
            matched: 0,
            probes,
        }
    }

    #[test]
    fn explain_names_the_strategy_by_its_shape() {
        // Found by mutation testing: both guards in `to_json` survived every
        // test, because nothing asserted the rendered JSON.
        assert_eq!(stats(None, 0).to_json()["strategy"], "collectionScan");
        assert_eq!(stats(Some("i"), 1).to_json()["strategy"], "index");
        assert_eq!(stats(Some("i"), 2).to_json()["strategy"], "indexUnion");
    }

    #[test]
    fn explain_reports_a_probe_count_only_for_unions() {
        // One range is not a union: a "probes": 1 on every indexed query
        // would be noise, and a missing count on a union would hide the one
        // number that distinguishes the shape.
        assert!(stats(Some("i"), 1).to_json().get("probes").is_none());
        assert_eq!(stats(Some("i"), 3).to_json()["probes"], 3);
    }

    // -----------------------------------------------------------------------
    // collect_matching routing — the guards that decide which scan runs
    // -----------------------------------------------------------------------

    fn live_state(dir: &tempfile::TempDir) -> SharedState {
        let engine = std::sync::Arc::new(
            kimmy_storage::Engine::open(&dir.path().join("kimmy.redb")).unwrap(),
        );
        let tokens = kimmy_auth::TokenIssuer::new("an-adequately-long-test-secret", 3600).unwrap();
        crate::state_with_egress(
            engine,
            tokens,
            false,
            crate::RateLimits::disabled(),
            crate::egress::EgressPolicy::default(),
        )
        .unwrap()
    }

    #[test]
    fn a_union_scans_every_probe_not_just_the_first() {
        // Found by mutation testing: forcing the `both_bounds` guard to true
        // sent every plan — unions included — down the single-range checked
        // scan, which reads `ranges[0]` alone. A two-probe `$in` silently
        // lost every match under the second probe, and no test noticed,
        // because none ran a union through `collect_matching`.
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        state.engine.create_collection("app", "docs").unwrap();
        state
            .engine
            .create_index(
                "app",
                "docs",
                vec![kimmy_storage::IndexField::ascending("n")],
                false,
                None,
            )
            .unwrap();
        let meta = state.engine.get_collection("app", "docs").unwrap();
        for i in 0..10i64 {
            state.engine.insert(&meta, bson::doc! { "_id": i, "n": i }).unwrap();
        }

        let filter = filter::parse(&bson::doc! { "n": { "$in": [2, 8] } }).unwrap();
        let (matched, stats) = collect_matching(&state, &meta, &filter, None).unwrap();

        assert_eq!(stats.probes, 2, "the union must be planned");
        let mut ids: Vec<i64> = matched.iter().map(|d| d.get_i64("_id").unwrap()).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![2, 8], "every probe's matches must arrive, not just the first's");
    }

    #[test]
    fn a_stale_both_bounds_plan_falls_back_rather_than_scanning_narrow() {
        // The other half of the same guard: forcing it to false sends a
        // both-bounds plan down the unchecked scan. This test manufactures
        // the exact race the checked scan exists for — metadata fetched
        // while the index was scalar-only, an array arriving before the
        // scan — and the straddling document must still be found.
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        state.engine.create_collection("app", "docs").unwrap();
        state
            .engine
            .create_index(
                "app",
                "docs",
                vec![kimmy_storage::IndexField::ascending("n")],
                false,
                None,
            )
            .unwrap();
        // Fetched while the index is scalar-only: a both-bounds plan.
        let stale = state.engine.get_collection("app", "docs").unwrap();
        state.engine.insert(&stale, bson::doc! { "_id": 1i64, "n": 3 }).unwrap();
        // The flip, after the metadata read: {n: [9, 0]} matches the range
        // below through *different elements*, so a narrow scan loses it.
        state.engine.insert(&stale, bson::doc! { "_id": 2i64, "n": [9, 0] }).unwrap();

        let filter = filter::parse(&bson::doc! { "n": { "$gte": 1, "$lte": 5 } }).unwrap();
        let (matched, stats) = collect_matching(&state, &stale, &filter, None).unwrap();

        let mut ids: Vec<i64> = matched.iter().map(|d| d.get_i64("_id").unwrap()).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2], "the straddling document must not be lost to a stale plan");
        assert!(stats.index.is_none(), "the fallback is a collection scan, and explain says so");
    }
}
