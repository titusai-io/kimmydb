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
// Tracing
// ---------------------------------------------------------------------------

/// Open the span for one executor operation.
///
/// Here rather than at the two edges, for the same reason [`authorize`] is
/// here: the REST route and the MCP tool both call these functions, so a span
/// opened in this file covers both, and one opened beside a handler is a span
/// the next edge forgets. The two lines sit together at the top of each
/// function on purpose — what was asked for, and whether it was allowed.
///
/// **The span name carries no data name.** It is `db.operation.name` — `find`,
/// `insert`, `aggregate` — which is low-cardinality and says nothing about what
/// a deployment stores. `db.namespace` and `db.collection.name` do, so they are
/// recorded only when the operator turned `telemetry.include_names` on
/// (ADR-068); declared `Empty` and filled afterwards, because a field never
/// filled is not exported at all, whereas an empty string would be an attribute
/// asserting the collection is called "".
fn op_span(operation: &'static str, db: &str, coll: Option<&str>) -> tracing::Span {
    use opentelemetry_semantic_conventions::attribute as semconv;

    use crate::telemetry::{DB_SYSTEM, include_names};

    let span = tracing::info_span!(
        "db.operation",
        // The span's real name, set at runtime. The macro bakes a literal into
        // a static callsite, so the name has to arrive as this field instead —
        // `otel.name` is what `tracing-opentelemetry` reads it from.
        otel.name = operation,
        { semconv::DB_SYSTEM_NAME } = DB_SYSTEM,
        { semconv::DB_OPERATION_NAME } = operation,
        { semconv::DB_NAMESPACE } = tracing::field::Empty,
        { semconv::DB_COLLECTION_NAME } = tracing::field::Empty,
    );
    if include_names() {
        span.record(semconv::DB_NAMESPACE, db);
        if let Some(coll) = coll {
            span.record(semconv::DB_COLLECTION_NAME, coll);
        }
    }
    span
}

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
    collection(state, db, coll)
}

/// Resolve a collection, with a retry hint that is true for a cluster.
///
/// A collection created through a load balancer lands on one member and
/// reaches the others a sync round later (measured at ~4.5 s on a
/// three-member cluster); a request that arrives on another member in
/// between is a `404` here. Answering `retry: "no"` to that told a client
/// which had just created the collection to give up. On a node with peers
/// the honest hint is `elsewhere`: another member has it, and this one will
/// shortly. A node with no peers keeps `no` — there is nowhere else.
pub fn collection(state: &SharedState, db: &str, coll: &str) -> Result<CollectionMeta, ApiError> {
    match state.engine.get_collection(db, coll) {
        Ok(meta) => Ok(meta),
        Err(kimmy_storage::StorageError::Core(kimmy_core::Error::CollectionNotFound {
            ..
        })) if state.members().is_some_and(|m| !m.is_empty()) => Err(ApiError::not_found(format!(
            "collection \"{db}\".\"{coll}\" not found on this member; if it was just \
                 created, another member has it and this one will within a sync round"
        ))
        .with_retry(crate::error::Retry::Elsewhere)),
        Err(e) => Err(e.into()),
    }
}

// ---------------------------------------------------------------------------
// Databases and collections
// ---------------------------------------------------------------------------

pub fn list_databases(state: &SharedState, auth: &Auth) -> Result<Value, ApiError> {
    let _span = op_span("list_databases", "*", None).entered();
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
    let _span = op_span("list_collections", db, None).entered();
    // A database that does not exist is an error, not an empty list: the list
    // is filtered by grant, so `[]` already means "nothing you can see", and
    // a caller that mistyped the name would otherwise read it as exactly
    // that. What stays `[]` is a database that exists and in which the
    // caller can read nothing — zero grants is not a refusal (ADR-066).
    if !state.engine.database_exists(db)? {
        return Err(kimmy_core::Error::DatabaseNotFound(db.to_string()).into());
    }
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
    let _span = op_span("create_collection", db, Some(name)).entered();
    auth.require(Action::Ddl, db, Some(name))?;
    let meta = state.engine.create_collection(db, name)?;
    Ok(json!({ "created": meta.name, "id": meta.id.0 }))
}

/// Drop every collection in a database. Each drop is a replicated entry and
/// the last one takes the database with it on every member, so this needs
/// no replication of its own. System databases are refused: the node keeps
/// its own bookkeeping there.
pub fn drop_database(state: &SharedState, auth: &Auth, db: &str) -> Result<Value, ApiError> {
    let _span = op_span("drop_database", db, None).entered();
    if db.starts_with("__") {
        return Err(ApiError::bad_request(format!(
            "{db} is a system database and cannot be dropped"
        )));
    }
    auth.require(Action::Ddl, db, None)?;
    Ok(json!({ "dropped": state.engine.drop_database(db)? }))
}

pub fn drop_collection(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
) -> Result<Value, ApiError> {
    let _span = op_span("drop_collection", db, Some(coll)).entered();
    auth.require(Action::Ddl, db, Some(coll))?;
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
    /// Resume after a previous page. See [`kimmy_core::Cursor`].
    pub cursor: Option<String>,
    /// Return each document's stamp alongside it, for a conditional write
    /// that follows (ADR-084).
    pub stamps: bool,
}

pub fn find(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    params: FindParams,
) -> Result<Value, ApiError> {
    let _span = op_span("find", db, Some(coll)).entered();
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

    // A cursor pages in `_id` order, which is the order both access paths
    // already produce. Combining it with anything that reorders or re-offsets
    // the result is refused rather than silently ignored: a page that quietly
    // dropped the sort it was given would be wrong in a way the caller would
    // read as data.
    let cursor = match &params.cursor {
        Some(raw) => {
            if skip > 0 {
                return Err(ApiError::bad_request(
                    "`cursor` and `skip` cannot be combined: a cursor already says where to \
                     resume",
                ));
            }
            if !sort.is_empty() && !sort_is_id_ascending(&sort) {
                return Err(ApiError::bad_request(
                    "`cursor` pages in _id order, so it takes no `sort` other than {\"_id\": 1}. \
                     Sorting by another field still uses `skip`",
                ));
            }
            Some(kimmy_core::Cursor::decode(raw)?)
        }
        None => None,
    };

    // A sort has to see every match before it can page, so early exit is only
    // safe for unsorted queries. A cursor is `_id`-ordered, which the scan
    // already delivers, so it may stop early too.
    let stop_after = (sort.is_empty() || cursor.is_some()).then_some(skip + limit);
    let (mut matched, stats) = collect_matching_stamped_after(
        state,
        &meta,
        &filter,
        stop_after,
        cursor.as_ref().map(|c| c.key()),
    )?;

    // The stamp travels with its document through the sort. Stable, so the
    // scan's order holds for documents the sort does not separate — the
    // same rule `shape::sort` follows.
    if !sort.is_empty() {
        matched.sort_by(|a, b| shape::compare(&sort, &a.1, &b.1));
    }

    let (page_stamps, page_docs): (Vec<kimmy_core::Stamp>, Vec<bson::Document>) =
        matched.into_iter().skip(skip).take(limit).unzip();

    // Offered whenever this query *could* be continued, so a caller's first
    // request needs no cursor and no flag — it asks for a page, and the reply
    // says how to get the next one. Requiring a cursor to receive a cursor
    // would leave a client with no way to start except a magic constant.
    //
    // Three conditions, and each of them is a way of being wrong otherwise:
    // a short page is the end; a `skip` means the caller is offsetting rather
    // than paging; and a sort other than `_id` ascending would hand back a
    // token that silently pages in a different order from the one asked for.
    let continuable = skip == 0 && (sort.is_empty() || sort_is_id_ascending(&sort));
    let next =
        (continuable && page_docs.len() == limit).then(|| next_cursor(page_docs.last())).flatten();

    let page: Vec<Value> = page_docs
        .iter()
        .map(|doc| document_to_json(&shape::project(projection.as_ref(), doc)))
        .collect();

    let mut body = json!({ "documents": page, "count": page.len() });
    if params.stamps {
        // Parallel to `documents` rather than a field inside each one: the
        // document is the caller's data and comes back exactly as stored.
        let stamps: Vec<String> = page_stamps.iter().map(kimmy_core::Stamp::encode).collect();
        body["stamps"] = json!(stamps);
    }
    if let Some(next) = next {
        body["nextCursor"] = json!(next.encode());
    }
    if params.explain {
        body["explain"] = stats.to_json();
    }
    Ok(body)
}

/// A caller's `if_stamp`, decoded — `None` when the write is unconditional.
fn parse_if_stamp(raw: Option<&str>) -> Result<Option<kimmy_core::Stamp>, ApiError> {
    raw.map(|s| kimmy_core::Stamp::decode(s).map_err(ApiError::from)).transpose()
}

/// Whether a sort specification is exactly `_id` ascending.
fn sort_is_id_ascending(sort: &[shape::SortKey]) -> bool {
    matches!(sort, [key] if key.path == kimmy_storage::ID_FIELD && !key.descending)
}

/// The cursor pointing just past a page's last document.
fn next_cursor(last: Option<&bson::Document>) -> Option<kimmy_core::Cursor> {
    let id = DocId::try_from_bson(last?.get(kimmy_storage::ID_FIELD)?).ok()?;
    let key = kimmy_core::keyenc::encode(&id.to_bson()).ok()?;
    Some(kimmy_core::Cursor::from_key(key))
}

pub fn count(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    params: FindParams,
) -> Result<Value, ApiError> {
    let _span = op_span("count", db, Some(coll)).entered();
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
    get_doc_stamped(state, auth, db, coll, id).map(|(_, doc)| doc)
}

/// [`get_doc`], with the document's stamp — what the HTTP edge serves as
/// `ETag`, so a read by id and a conditional write by id pair up without a
/// `find`.
pub fn get_doc_stamped(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    id: &str,
) -> Result<(String, Value), ApiError> {
    let _span = op_span("get_doc", db, Some(coll)).entered();
    let meta = authorize(state, auth, Action::Read, db, coll)?;
    let doc_id = parse_id(id)?;
    match state.engine.get_stamped(&meta, &doc_id)? {
        Some((stamp, doc)) => Ok((stamp.encode(), document_to_json(&doc))),
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
    /// Whether the filter pinned `_id` and was answered by primary-key reads.
    ///
    /// Reported separately from `index` because the primary key is not one: no
    /// index was consulted and none needs to exist. A client tuning a query
    /// wants to see that it took the fast path, not to be told an index it
    /// never created was used.
    pub id_lookup: bool,
}

impl QueryStats {
    pub fn to_json(&self) -> Value {
        let strategy = match (&self.index, self.probes) {
            _ if self.id_lookup => "idLookup",
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
    collect_matching_after(state, meta, filter, stop_after, None)
}

/// [`collect_matching`], resuming strictly after an encoded document key.
pub fn collect_matching_after(
    state: &SharedState,
    meta: &CollectionMeta,
    filter: &filter::Filter,
    stop_after: Option<usize>,
    after: Option<&[u8]>,
) -> Result<(Vec<bson::Document>, QueryStats), ApiError> {
    let (matched, stats) = collect_matching_stamped_after(state, meta, filter, stop_after, after)?;
    Ok((matched.into_iter().map(|(_, doc)| doc).collect(), stats))
}

/// The one read scan, carrying each document's stamp.
///
/// Both access paths are already in `_id` order — the documents table by its
/// key, an index candidate list because `scan_range_in` sorts by document key —
/// so resuming is a bound rather than a filter, and a page costs its own size
/// rather than everything before it.
pub fn collect_matching_stamped_after(
    state: &SharedState,
    meta: &CollectionMeta,
    filter: &filter::Filter,
    stop_after: Option<usize>,
    after: Option<&[u8]>,
) -> Result<(Vec<(kimmy_core::Stamp, bson::Document)>, QueryStats), ApiError> {
    let mut matched = Vec::new();
    let mut examined = 0usize;

    // The primary key first: it is the tightest access path there is, needs no
    // index to exist, and beats anything a secondary index could offer for the
    // same predicate. The keys are already document keys, so this produces the
    // same candidate shape an index scan does and the filter is re-applied to
    // each exactly as it is there.
    let primary = plan::choose_primary_key(filter);
    if let Some(pk) = &primary {
        for key in &pk.keys {
            if after.is_some_and(|bound| key.as_slice() <= bound) {
                continue;
            }
            // A key naming no document is an ordinary miss, not an error: the
            // filter asked for an `_id` nothing was stored under.
            let Some((stamp, doc)) = state.engine.get_record_by_encoded_key(meta, key)? else {
                continue;
            };
            examined += 1;
            if filter::matches(filter, &doc) {
                matched.push((stamp, doc));
                if stop_after.is_some_and(|n| matched.len() >= n) {
                    break;
                }
            }
        }
        let stats = QueryStats {
            index: None,
            fields_used: 0,
            examined,
            matched: matched.len(),
            probes: pk.keys.len(),
            id_lookup: true,
        };
        return Ok((matched, stats));
    }

    let mut plan = plan::choose(filter, &meta.indexes);

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
                // Candidates arrive in document-key order, so this skips a
                // prefix rather than filtering the whole list.
                if after.is_some_and(|bound| key.as_slice() <= bound) {
                    continue;
                }
                let Some((stamp, doc)) = state.engine.get_record_by_encoded_key(meta, &key)? else {
                    continue;
                };
                examined += 1;
                if filter::matches(filter, &doc) {
                    matched.push((stamp, doc));
                    if stop_after.is_some_and(|n| matched.len() >= n) {
                        break;
                    }
                }
            }
        }
        None => {
            state.engine.for_each_record_after(meta, after, |_, stamp, doc| {
                examined += 1;
                if filter::matches(filter, &doc) {
                    matched.push((stamp, doc));
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
        id_lookup: false,
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
    let _span = op_span("insert", db, Some(coll)).entered();
    let meta = authorize(state, auth, Action::Write, db, coll)?;
    let doc = json_to_document(document)?;
    let (id, stamp) = state.engine.insert_stamped(&meta, doc)?;
    Ok(json!({
        "insertedId": crate::json::bson_to_json(&id.to_bson()),
        "stamp": stamp.encode(),
    }))
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
    let _span = op_span("insert_many", db, Some(coll)).entered();
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

    let stamped = state.engine.insert_many_stamped(&meta, docs).map_err(|e| match e.index {
        Some(i) => at_index(i, e.source.into()),
        None => e.source.into(),
    })?;

    // Positionally parallel to `insertedIds`: every document gets its own
    // stamp, and a caller following one up with a conditional write needs
    // the version it landed at, exactly as `insert` reports.
    Ok(json!({
        "inserted": stamped.len(),
        "insertedIds": stamped
            .iter()
            .map(|(id, _)| crate::json::bson_to_json(&id.to_bson()))
            .collect::<Vec<_>>(),
        "stamps": stamped.iter().map(|(_, stamp)| stamp.encode()).collect::<Vec<_>>(),
    }))
}

/// Name the offending document's position without changing the error envelope.
///
/// A batch is all-or-nothing, so there is no partial result to point at — the
/// position is the only thing that tells the caller what to fix.
fn at_index(index: usize, e: ApiError) -> ApiError {
    ApiError { message: format!("document at index {index}: {}", e.message), ..e }
}

/// A replace-by-id's options, mirroring [`WriteParams`].
#[derive(Default)]
pub struct ReplaceParams {
    /// Create the document when none exists.
    pub upsert: bool,
    /// Replace only if the document is at this stamp (ADR-084).
    pub if_stamp: Option<String>,
}

pub fn replace(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    id: &str,
    document: &Value,
    params: ReplaceParams,
) -> Result<Value, ApiError> {
    let _span = op_span("replace", db, Some(coll)).entered();
    let meta = authorize(state, auth, Action::Write, db, coll)?;
    let doc_id = parse_id(id)?;
    let doc = json_to_document(document)?;
    let expected = parse_if_stamp(params.if_stamp.as_deref())?;
    let outcome = state.engine.replace_if(&meta, &doc_id, doc, params.upsert, expected)?;
    // Counts, not booleans, even though a replace touches at most one document.
    //
    // `WriteOutcome` is three bools and this route used to serialize them
    // straight through, so `matched` was `true` here and `3` on `/update` —
    // the same field name carrying two types on one protocol. Nothing stated
    // the type, so nothing disagreed until the specification's contract test
    // drove both routes and compared them (ADR-056). `upserted` stays a
    // boolean because it genuinely is one.
    let mut body = json!({
        "matched": u8::from(outcome.matched),
        "modified": u8::from(outcome.modified),
        "upserted": outcome.upserted,
    });
    if let Some(stamp) = outcome.stamp {
        body["stamp"] = json!(stamp.encode());
    }
    Ok(body)
}

pub fn delete_by_id(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    id: &str,
    if_stamp: Option<&str>,
) -> Result<Value, ApiError> {
    let _span = op_span("delete_by_id", db, Some(coll)).entered();
    let meta = authorize(state, auth, Action::Write, db, coll)?;
    let doc_id = parse_id(id)?;
    let expected = parse_if_stamp(if_stamp)?;
    let deleted = state.engine.delete_if(&meta, &doc_id, expected)?;
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
    /// Write only if the matched document is at this stamp (ADR-084).
    /// Single-document only: a version names one document.
    pub if_stamp: Option<String>,
}

impl WriteParams {
    /// The caller's condition, decoded — and refused alongside `multi`,
    /// because one stamp cannot describe several documents.
    fn expected(&self) -> Result<Option<kimmy_core::Stamp>, ApiError> {
        if self.multi && self.if_stamp.is_some() {
            return Err(ApiError::bad_request(
                "`if_stamp` names one document's version, so it cannot be combined with \
                 `multi: true`",
            ));
        }
        parse_if_stamp(self.if_stamp.as_deref())
    }
}

pub fn update(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    update_json: &Value,
    params: WriteParams,
) -> Result<Value, ApiError> {
    let _span = op_span("update", db, Some(coll)).entered();
    let (multi, explain) = (params.multi, params.explain);
    let meta = authorize(state, auth, Action::Write, db, coll)?;
    let filter = parse_filter(params.filter.as_ref())?;
    let update = update::parse(&json_to_document(update_json)?)?;
    let expected = params.expected()?;

    // Match and write in one transaction. This used to collect the targets
    // in a read transaction and `replace` each in its own write transaction,
    // which applied the operators to an image another writer could have
    // already moved on from: two concurrent `$inc`s both read 5 and both
    // stored 6. The engine now runs the same in-transaction body
    // `find_and_modify` has, over every match.
    let stop_after = if multi { None } else { Some(1) };
    let (candidates, planned) = candidates_for(&filter, &meta);
    let modify = Modify {
        filter: &filter,
        sort: &[],
        update: Some(&update),
        upsert: None,
        now: now_millis(),
        expected,
    };
    let outcome = state.engine.modify_where(&meta, &candidates, &modify, stop_after)?;

    let mut body = json!({
        "matched": outcome.matched,
        "modified": outcome.modified,
        "commits": outcome.commits,
    });
    if let Some(stamp) = single_stamp(multi, &outcome) {
        body["stamp"] = json!(stamp.encode());
    }
    if explain {
        body["explain"] = planned.stats(&outcome).to_json();
    }
    Ok(body)
}

/// The stamp a filtered write produced, when there is exactly one to report.
///
/// A single-document write names one version, the one a caller passes back
/// as `if_stamp` next time — the same thing `insert` and `replace` report. A
/// `multi` write has no single version by construction (ADR-084 refuses
/// `if_stamp` alongside it for the same reason), so it reports none rather
/// than the last chunk's last document, which would look like the answer
/// and not be it.
fn single_stamp(
    multi: bool,
    outcome: &kimmy_storage::ModifyManyOutcome,
) -> Option<kimmy_core::Stamp> {
    (!multi && outcome.modified == 1).then_some(outcome.stamp).flatten()
}

/// Where a filtered write looks, in the engine's terms, plus what `explain`
/// should say about it.
///
/// The same planner `find` runs, in the same order — primary key first, then
/// an index, then a scan — so an update is found exactly the way a read is.
/// The engine re-checks a both-bounds index plan inside the transaction that
/// scans, which is stricter than the read path can be.
fn candidates_for(
    filter: &filter::Filter,
    meta: &CollectionMeta,
) -> (kimmy_storage::Candidates, PlannedAccess) {
    if let Some(pk) = plan::choose_primary_key(filter) {
        let probes = pk.keys.len();
        return (kimmy_storage::Candidates::Keys(pk.keys), PlannedAccess::PrimaryKey { probes });
    }
    match plan::choose(filter, &meta.indexes) {
        Some(p) => (
            kimmy_storage::Candidates::Index {
                index_id: p.index_id,
                ranges: p.ranges.clone(),
                both_bounds: p.both_bounds,
            },
            PlannedAccess::Index {
                name: p.index_name.clone(),
                fields_used: p.fields_used,
                probes: p.ranges.len(),
            },
        ),
        None => (kimmy_storage::Candidates::Scan, PlannedAccess::Scan),
    }
}

/// The access path a filtered write was planned to, for `explain`.
///
/// Reports the plan as chosen. The one case where the engine departs from it
/// — a both-bounds index plan found multikey inside the transaction, which
/// falls back to a scan — is not reflected, exactly as `find` reports the
/// plan it chose rather than the scan it fell back to.
enum PlannedAccess {
    PrimaryKey { probes: usize },
    Index { name: String, fields_used: usize, probes: usize },
    Scan,
}

impl PlannedAccess {
    fn stats(&self, outcome: &kimmy_storage::ModifyManyOutcome) -> QueryStats {
        let (index, fields_used, probes, id_lookup) = match self {
            PlannedAccess::PrimaryKey { probes } => (None, 0, *probes, true),
            PlannedAccess::Index { name, fields_used, probes } => {
                (Some(name.clone()), *fields_used, *probes, false)
            }
            PlannedAccess::Scan => (None, 0, 0, false),
        };
        QueryStats {
            index,
            fields_used,
            examined: outcome.examined as usize,
            matched: outcome.matched as usize,
            probes,
            id_lookup,
        }
    }
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
    /// Write only if the chosen document is at this stamp (ADR-084).
    pub if_stamp: Option<String>,
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
    /// The version the write is conditional on, if any.
    expected: Option<kimmy_core::Stamp>,
}

impl kimmy_storage::ModifySpec for Modify<'_> {
    fn expected_stamp(&self) -> Option<kimmy_core::Stamp> {
        self.expected
    }

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
    let _span = op_span("find_and_modify", db, Some(coll)).entered();
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

    // Planned the way `update` is — which now includes the primary key: a
    // `find_and_modify` on `_id` used to scan the collection under the
    // writer, because only the index planner ran here.
    let (candidates, _) = candidates_for(&filter, &meta);

    let expected = parse_if_stamp(spec.if_stamp.as_deref())?;
    if expected.is_some() && spec.upsert {
        // An upsert says "create it if it is not there"; a stamp says "it
        // must be there, at this version". Both at once has no reading.
        return Err(ApiError::bad_request("`if_stamp` cannot be combined with `upsert`"));
    }
    let modify = Modify {
        filter: &filter,
        sort: &sort,
        update: update.as_ref(),
        upsert: upsert_doc,
        now,
        expected,
    };

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
    if let Some(stamp) = outcome.stamp {
        // The version the write produced — what the next conditional write
        // names. A removal produces a tombstone's stamp, which nothing can
        // be conditional on, so it is reported only for a document that
        // still exists.
        if outcome.after.is_some() {
            body["stamp"] = json!(stamp.encode());
        }
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
    let _span = op_span("delete", db, Some(coll)).entered();
    let (multi, explain) = (params.multi, params.explain);
    let meta = authorize(state, auth, Action::Write, db, coll)?;
    let filter = parse_filter(params.filter.as_ref())?;
    let expected = params.expected()?;

    // The same one-transaction path as `update`, with the spec removing
    // rather than replacing: what the filter matched is exactly what is
    // tombstoned, with no read-then-write gap for another writer.
    let stop_after = if multi { None } else { Some(1) };
    let (candidates, planned) = candidates_for(&filter, &meta);
    let modify = Modify {
        filter: &filter,
        sort: &[],
        update: None,
        upsert: None,
        now: now_millis(),
        expected,
    };
    let outcome = state.engine.modify_where(&meta, &candidates, &modify, stop_after)?;

    let mut body = json!({ "deleted": outcome.modified, "commits": outcome.commits });
    if let Some(stamp) = single_stamp(multi, &outcome) {
        body["stamp"] = json!(stamp.encode());
    }
    if explain {
        body["explain"] = planned.stats(&outcome).to_json();
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
    let _span = op_span("create_index", db, Some(coll)).entered();
    auth.require(Action::Ddl, db, Some(coll))?;

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
    let _span = op_span("list_indexes", db, Some(coll)).entered();
    authorize(state, auth, Action::Read, db, coll)?;
    let indexes: Vec<Value> =
        state.engine.list_indexes(db, coll)?.iter().map(index_to_json).collect();
    Ok(json!({ "indexes": indexes }))
}

/// Unique violations standing on a collection — the ones a client has to
/// resolve, not the ones that were ever recorded (ADR-087).
///
/// Without `index`: a count per index. With it: the colliding groups on that
/// index, each with its documents, so the caller can decide which to keep.
/// Authorised as `read`: the documents are readable already, and so is the
/// change-stream event that announced the collision. `/metrics` keeps its
/// name-free count.
pub fn violations(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    index: Option<&str>,
) -> Result<Value, ApiError> {
    let _span = op_span("violations", db, Some(coll)).entered();
    let meta = authorize(state, auth, Action::Read, db, coll)?;
    let live = state.engine.live_unique_violations(&meta)?;

    let Some(name) = index else {
        let mut per_index: std::collections::BTreeMap<&str, u64> = Default::default();
        for v in &live {
            *per_index.entry(v.index.as_str()).or_default() += 1;
        }
        let indexes: Vec<Value> =
            per_index.iter().map(|(name, count)| json!({ "name": name, "count": count })).collect();
        return Ok(json!({ "count": live.len(), "indexes": indexes }));
    };

    let mut groups = Vec::new();
    for v in live.iter().filter(|v| v.index == name).take(MAX_LIMIT) {
        let mut documents = Vec::with_capacity(v.ids.len());
        for id in &v.ids {
            if let Some(doc) = state.engine.get(&meta, id)? {
                documents.push(document_to_json(&doc));
            }
        }
        groups.push(json!({
            "ids": v.ids.iter().map(|id| crate::json::bson_to_json(&id.to_bson())).collect::<Vec<_>>(),
            "merged": crate::json::bson_to_json(&v.merged.to_bson()),
            "documents": documents,
        }));
    }
    Ok(json!({ "index": name, "count": groups.len(), "groups": groups }))
}

pub fn drop_index(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    name: &str,
) -> Result<Value, ApiError> {
    let _span = op_span("drop_index", db, Some(coll)).entered();
    auth.require(Action::Ddl, db, Some(coll))?;
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
    let _span = op_span("aggregate", db, Some(coll)).entered();
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
            id_lookup: false,
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
    fn a_primary_key_lookup_is_named_as_one_and_claims_no_index() {
        // `idLookup` wins over every other strategy, including a probe count
        // that would otherwise read as a union — the work is primary-key reads
        // whether there is one key or ten.
        let one = QueryStats { probes: 1, id_lookup: true, ..stats(None, 0) };
        assert_eq!(one.to_json()["strategy"], "idLookup");
        let many = QueryStats { probes: 5, id_lookup: true, ..stats(None, 0) };
        assert_eq!(many.to_json()["strategy"], "idLookup");

        // And it reports no index, because none was consulted and none needs
        // to exist. Naming one would send a reader looking for it.
        assert_eq!(one.to_json()["index"], Value::Null);
        assert_eq!(one.to_json()["indexFieldsUsed"], 0);
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
        let tokens =
            kimmy_auth::TokenIssuer::new("an-adequately-long-test-secret-for-hs256", 3600).unwrap();
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
