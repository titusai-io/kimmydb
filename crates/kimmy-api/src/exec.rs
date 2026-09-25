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
use kimmy_query::expr::Binding;
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

/// The most documents a sorted `find` may hold: `skip + limit`.
///
/// A sort has to see every match before it knows which come first, and what
/// it keeps while looking is what it will return plus what it will skip. The
/// same number as [`MAX_LIMIT`] because it bounds the same thing — how many
/// documents one request may make the node hold — and a deeper offset was
/// never a good way to page: a cursor costs a page, and a range on the sort
/// field costs a page (ADR-098). Where `limit` is clamped this is refused,
/// because a clamped `skip` would return a different page from the one asked
/// for and say nothing.
pub const MAX_SORT_WINDOW: usize = MAX_LIMIT;

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
    // Off the async worker, as every schema change here is: an index build or
    // drop takes as long as the data under it, and any of them can wait for
    // the single writer for the whole of the request's budget. A handler that
    // holds a worker stalls every task queued on it (ADR-153).
    let meta = kimmy_storage::blocking(|| state.engine.create_collection(db, name))?;
    Ok(json!({ "created": meta.name, "id": meta.id.0 }))
}

/// The id a drop of `db.coll` strands a vector index under.
///
/// The index is keyed by the **shadow** collection's id, and the shadow is
/// removed in the same transaction as its parent, so the id has to be in hand
/// before the drop — the reason `vectors::disable_vectors` resolves its own
/// first. It is not looked up, though: a collection id is derived from its
/// name (ADR-031), so the shadow's is `derive(db, shadow_name(coll))`, or
/// `derive(db, coll)` for a shadow named directly, with no read at all. That
/// is also why there is no `Option`: a collection that never had vectors, or
/// had its configuration removed without them, derives an id all the same,
/// and `IndexCache::invalidate` on an id that never held a graph is a no-op.
fn shadow_of(db: &str, coll: &str) -> kimmy_core::CollectionId {
    if kimmy_core::vector_meta::is_shadow(coll) {
        kimmy_core::CollectionId::derive(db, coll)
    } else {
        kimmy_core::CollectionId::derive(db, &kimmy_core::vector_meta::shadow_name(coll))
    }
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
    // Every vector-enabled collection in the database, not one: this drops
    // them all, and a graph left behind for any of them is resident memory
    // and disk for data that no longer exists. Listed before the drop for the
    // reason `shadow_of` gives, and every shadow in the database is listed
    // here by name — beside its parent or, for an orphan (ADR-138), without
    // one — so this is the whole set the drop takes.
    //
    // Check-then-act, and known to be: listed, then dropped, then forgotten,
    // with nothing holding the catalogue still in between. A shadow created
    // after the listing is dropped by `drop_database` and not forgotten here;
    // a name recreated after the drop and before the invalidate loses the
    // graph its first search built. Both are the class
    // `IndexCache::forget_absent`'s doc names and accepts, and cost a rebuild,
    // never a wrong answer: `serve`, `HnswIndex::load` and the `created`
    // checks refuse a graph of the other incarnation, and the change-feed
    // consumer (`vectors::invalidator`) reconciles the dropped one again from
    // the entry the drop mints.
    let shadows: Vec<kimmy_core::CollectionId> = state
        .engine
        .list_collections(db)?
        .into_iter()
        .filter(|c| kimmy_core::vector_meta::is_shadow(&c.name))
        .map(|c| c.id)
        .collect();
    let dropped = kimmy_storage::blocking(|| state.engine.drop_database(db))?;
    for shadow in shadows {
        state.vectors.invalidate(shadow);
    }
    Ok(json!({ "dropped": dropped }))
}

pub fn drop_collection(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
) -> Result<Value, ApiError> {
    let _span = op_span("drop_collection", db, Some(coll)).entered();
    auth.require(Action::Ddl, db, Some(coll))?;
    // Derived, then dropped, then forgotten: the same check-then-act window
    // `drop_database` documents, one collection wide. A recreate of the name
    // landing between the drop and the invalidate has the graph its first
    // search built forgotten with its predecessor's — a rebuild, never a wrong
    // answer, for the reasons given there.
    let shadow = shadow_of(db, coll);
    let dropped = kimmy_storage::blocking(|| state.engine.drop_collection(db, coll))?;
    state.vectors.invalidate(shadow);
    Ok(json!({ "dropped": dropped }))
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

    // `_id` ascending is the order every access path already delivers, so it
    // is not a sort the executor has to perform — and, like no sort at all,
    // the scan can stop once it has `skip + limit` matches.
    let sorted = !sort.is_empty() && !sort_is_id_ascending(&sort);
    let window = skip.saturating_add(limit);

    // Offered whenever this query *could* be continued, so a caller's first
    // request needs no cursor and no flag — it asks for a page, and the reply
    // says how to get the next one. Requiring a cursor to receive a cursor
    // would leave a client with no way to start except a magic constant.
    //
    // Three conditions, and each of them is a way of being wrong otherwise:
    // a short page is the end; a `skip` means the caller is offsetting rather
    // than paging; and a sort other than `_id` ascending would hand back a
    // token that silently pages in a different order from the one asked for.
    // Decided here, before the scan, because it says whether the scan has to
    // keep the last `_id` it visited — a projection may drop `_id` from the
    // page, and a cursor read out of the projected document would then be a
    // walk that stopped after one page.
    let continuable = skip == 0 && (sort.is_empty() || sort_is_id_ascending(&sort));

    // The `_id` of the last document put into the page, kept beside it rather
    // than read back out of it (ADR-150).
    let mut last_id: Option<bson::Bson> = None;

    // What a page holds is what it returns: the projection is applied where a
    // document is visited, so the vector below never holds a stored document
    // (ADR-150). How many documents each path shapes to fill it differs, and
    // each branch says which.
    let (page_stamps, page_docs, stats): (Vec<kimmy_core::Stamp>, Vec<bson::Document>, _) =
        if sorted {
            // A sort has to see every match before it can page, but it need
            // not *hold* every match: the `skip + limit` least are all it can
            // return, so that is all it keeps (ADR-098). The window has a
            // ceiling for the same reason `limit` has one, and it is refused
            // rather than clamped because a clamped `skip` would silently
            // return a different page from the one asked for.
            if window > MAX_SORT_WINDOW {
                return Err(ApiError::bad_request(format!(
                    "a sorted find holds `skip + limit` documents while it sorts, which may not \
                     exceed {MAX_SORT_WINDOW} (got {window}). To page deeper, sort by \
                     {{\"_id\": 1}} and follow `nextCursor`, or narrow the filter on the sort \
                     field to where the last page ended"
                )));
            }
            // Ties are broken by `_id`, which is the order the scan used to
            // feed a stable sort — so the page is the one the full sort gave,
            // whatever order the matches arrive in.
            let order = with_id_tiebreak(&sort);
            let mut top = TopK::new(window, &order, projection.as_ref());
            // A match the order has no position for — a Decimal128 where the
            // sort would read it — refuses the whole query by name, after
            // the visit: the visitor cannot fail, and offering nothing more
            // once one is found costs one pass it was making anyway.
            let mut unsortable: Option<String> = None;
            let stats = visit_matching(state, &meta, &filter, Order::Any, None, |stamp, doc| {
                if unsortable.is_some() {
                    return;
                }
                // Asked of the stored document, before the projection: a
                // Decimal128 at a sort path refuses the query whether or not
                // the page would have carried it.
                match shape::unsortable(&order, &doc) {
                    None => top.offer(stamp, doc),
                    Some(why) => unsortable = Some(why),
                }
            })?;
            if let Some(why) = unsortable {
                return Err(ApiError::bad_request(why));
            }
            // The window shaped `skip + limit` documents and `skip` of them
            // are dropped here. Nothing cheaper is available: which matches
            // the offset steps over is not known until the sort is done, and
            // the alternative — holding stored documents and projecting the
            // survivors — is the thing this record is about.
            let (stamps, docs) = top.into_sorted().into_iter().skip(skip).unzip();
            (stamps, docs, stats)
        } else {
            // What is skipped is counted past, not held — and not shaped
            // either: matches arrive in the order they will be returned in,
            // so this path knows a match is skipped before it would project
            // it, and projects exactly the documents the page returns.
            let mut seen = 0usize;
            let (mut stamps, mut docs) = (Vec::new(), Vec::new());
            let stats = visit_matching(
                state,
                &meta,
                &filter,
                Order::ById { after: cursor.as_ref().map(|c| c.key()) },
                Some(window),
                |stamp, doc| {
                    seen += 1;
                    if seen > skip {
                        if continuable {
                            last_id = doc.get(kimmy_storage::ID_FIELD).cloned();
                        }
                        stamps.push(stamp);
                        // Moved rather than shaped when there is no
                        // projection: `shape::project(None, ..)` is a clone,
                        // and an unprojected page has no reason to copy the
                        // document it was handed.
                        docs.push(match projection.as_ref() {
                            None => doc,
                            Some(projection) => shape::project(Some(projection), &doc),
                        });
                    }
                },
            )?;
            (stamps, docs, stats)
        };

    let next =
        (continuable && page_docs.len() == limit).then(|| next_cursor(last_id.as_ref())).flatten();

    let page: Vec<Value> = page_docs.iter().map(document_to_json).collect();

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
///
/// Takes the `_id` rather than the document, because the page holds projected
/// documents and a projection may have dropped `_id` from them (ADR-150).
/// Reading it back out of the page would have made `{"_id": 0}` a walk that
/// ended after one page, silently and looking like the end of the collection.
fn next_cursor(last: Option<&bson::Bson>) -> Option<kimmy_core::Cursor> {
    let id = DocId::try_from_bson(last?).ok()?;
    let key = kimmy_core::keyenc::encode(&id.to_bson()).ok()?;
    Some(kimmy_core::Cursor::from_key(key))
}

/// A sort with `_id` ascending as its final key, unless it already sorts by
/// `_id`.
///
/// That makes the order total — `_id` is unique — and it is exactly the order
/// the old path produced: a stable sort over matches that arrived in `_id`
/// order. Stated as a key rather than relied on as a property of the input,
/// so the bounded sort can take its matches in whatever order is cheapest.
fn with_id_tiebreak(sort: &[shape::SortKey]) -> Vec<shape::SortKey> {
    let mut order = sort.to_vec();
    if !order.iter().any(|key| key.path == kimmy_storage::ID_FIELD) {
        order.push(shape::SortKey { path: kimmy_storage::ID_FIELD.to_string(), descending: false });
    }
    order
}

/// The `n` least of what it is offered, under a total order.
///
/// A sorted `find` used to collect every match and sort the lot, so its
/// memory was the size of the match set. This holds `skip + limit`: the
/// largest held sits at the top of a max-heap, and an offer that would not
/// displace it is dropped on the spot. The key list the caller has made total
/// with [`with_id_tiebreak`] is what orders it — a heap under a partial order
/// would return a page that depended on arrival order.
///
/// **What it holds of a match is its sort keys and its projected document**,
/// never the stored one (ADR-150). The keys are `shape::sort_keys`, which is
/// what `shape::compare` reads out of a document and nothing more, so
/// `shape::compare_keys` over them is the same ordering the document
/// comparison gave. The projection is applied only once a match is being
/// kept: an offer the heap turns away costs the keys and nothing else.
struct TopK<'a> {
    n: usize,
    order: &'a [shape::SortKey],
    projection: Option<&'a shape::Projection>,
    heap: std::collections::BinaryHeap<Ranked<'a>>,
}

/// One held match: what ranks it, and what it will return.
struct Ranked<'a> {
    order: &'a [shape::SortKey],
    stamp: kimmy_core::Stamp,
    keys: Vec<bson::Bson>,
    doc: bson::Document,
}

impl Ord for Ranked<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        shape::compare_keys(self.order, &self.keys, &other.keys)
    }
}

impl PartialOrd for Ranked<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Ranked<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for Ranked<'_> {}

impl<'a> TopK<'a> {
    fn new(
        n: usize,
        order: &'a [shape::SortKey],
        projection: Option<&'a shape::Projection>,
    ) -> Self {
        Self {
            n,
            order,
            projection,
            heap: std::collections::BinaryHeap::with_capacity(n.min(MAX_LIMIT) + 1),
        }
    }

    fn offer(&mut self, stamp: kimmy_core::Stamp, doc: bson::Document) {
        if self.n == 0 {
            return;
        }
        let keys = shape::sort_keys(self.order, &doc);
        if self.heap.len() == self.n {
            // Would not displace the largest held, so it is dropped where it
            // stands — unprojected, and never a document this window holds.
            let displaces = self.heap.peek().is_some_and(|largest| {
                shape::compare_keys(self.order, &keys, &largest.keys).is_lt()
            });
            if !displaces {
                return;
            }
            self.heap.pop();
        }
        // Moved rather than shaped when there is no projection:
        // `shape::project(None, ..)` is a clone, and the window may as well
        // hold the document it was handed.
        let doc = match self.projection {
            None => doc,
            Some(projection) => shape::project(Some(projection), &doc),
        };
        self.heap.push(Ranked { order: self.order, stamp, keys, doc });
    }

    /// Everything held, least first.
    fn into_sorted(self) -> Vec<(kimmy_core::Stamp, bson::Document)> {
        self.heap.into_sorted_vec().into_iter().map(|r| (r.stamp, r.doc)).collect()
    }
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

    // No early exit — a count must see every match — and nothing kept: each
    // match is counted as it passes the recheck and dropped, in whatever
    // order the access path finds cheapest. Counting used to collect every
    // matching document first, so a count over a large collection cost the
    // memory of the collection (ADR-098).
    let stats = visit_matching(state, &meta, &filter, Order::Any, None, |_, _| {})?;

    let mut body = json!({ "count": stats.matched });
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
    /// Index entries read, when an index was consulted.
    ///
    /// The measure of how much of the index a query touched, as distinct
    /// from how many documents it examined: an exact probe stopped by
    /// `limit` reads as many entries as documents, while a range that had
    /// to be put in `_id` order reads the whole range however few it
    /// returns. `None` for a scan and for a primary-key lookup. `update` and
    /// `delete` read this the same way `find` and `count` do — their
    /// `explain` runs the same read-only scan (ADR-131) — so it appears
    /// there too whenever an index answers.
    pub index_entries: Option<usize>,
    /// Of `index_entries`, the ones read from the index's unkeyed run:
    /// documents the index could not key, which every scan of it reads and
    /// rechecks whatever the filter asked for (ADR-139). Reported whenever
    /// `index_entries` is, so a client can see the cost of leaving such
    /// documents under the index — zero for an index that keys everything
    /// it holds.
    pub unkeyed: Option<usize>,
    /// Of `index_entries`, the ones read from the index's **undecidable** run:
    /// documents held because a partial filter could not decide them, which
    /// every scan of that index reads and rechecks (ADR-185). Reported and read
    /// exactly as `unkeyed` is, and separate from it for the reason the two
    /// counters are separate: that one is a fault an owner can fix, this one is
    /// the documented cost of a `Decimal128` at a filtered path, and an owner
    /// tuning a query needs to see which of the two they are paying for.
    pub undecidable: Option<usize>,
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
        if let Some(entries) = self.index_entries {
            out["indexEntriesRead"] = json!(entries);
        }
        if let Some(unkeyed) = self.unkeyed {
            out["unkeyedCandidates"] = json!(unkeyed);
        }
        if let Some(undecidable) = self.undecidable {
            out["undecidableCandidates"] = json!(undecidable);
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
///
/// A convenience over [`visit_matching`] for a caller that wants the matches
/// as a list; `find` and `count` do not, and go to the visitor directly.
pub fn collect_matching(
    state: &SharedState,
    meta: &CollectionMeta,
    filter: &filter::Filter,
    stop_after: Option<usize>,
) -> Result<(Vec<bson::Document>, QueryStats), ApiError> {
    let (matched, stats) = collect_matching_stamped_after(state, meta, filter, stop_after, None)?;
    Ok((matched.into_iter().map(|(_, doc)| doc).collect(), stats))
}

/// [`collect_matching`], carrying each document's stamp and resuming strictly
/// after an encoded document key, in `_id` order.
pub fn collect_matching_stamped_after(
    state: &SharedState,
    meta: &CollectionMeta,
    filter: &filter::Filter,
    stop_after: Option<usize>,
    after: Option<&[u8]>,
) -> Result<(Vec<(kimmy_core::Stamp, bson::Document)>, QueryStats), ApiError> {
    let mut matched = Vec::new();
    let stats =
        visit_matching(state, meta, filter, Order::ById { after }, stop_after, |stamp, doc| {
            matched.push((stamp, doc));
        })?;
    Ok((matched, stats))
}

/// The order a read wants its matches in.
#[derive(Clone, Copy, Debug)]
pub enum Order<'a> {
    /// `_id` ascending, resuming strictly after an encoded document key — the
    /// order a page is returned in and a cursor resumes from.
    ById { after: Option<&'a [u8]> },
    /// Whichever order the access path finds cheapest. For a count, which
    /// has no order, or a sort, which imposes its own.
    Any,
}

/// The recheck, the count, and the stop — one place, whichever path feeds it.
struct Recheck<'a, F> {
    filter: &'a filter::Filter,
    stop_after: Option<usize>,
    examined: usize,
    matched: usize,
    visit: F,
}

impl<F: FnMut(kimmy_core::Stamp, bson::Document)> Recheck<'_, F> {
    /// Examine one candidate; whether the scan should go on.
    ///
    /// The bound is checked **before** a candidate is considered, not after
    /// one has already been handed to `visit`. Checked after, a `stop_after`
    /// of zero let the first match through before the bound could ever
    /// refuse it — a `limit: 0` page came back holding one document instead
    /// of none, whenever the very first candidate the scan examined happened
    /// to match.
    fn take(&mut self, stamp: kimmy_core::Stamp, doc: bson::Document) -> bool {
        if self.stop_after == Some(self.matched) {
            return false;
        }
        self.examined += 1;
        if filter::matches(self.filter, &doc) {
            self.matched += 1;
            (self.visit)(stamp, doc);
        }
        !self.stop_after.is_some_and(|n| self.matched >= n)
    }
}

/// The one read scan. Every match is handed to `visit` as it is found and
/// nothing is kept here, so what a read holds is decided by the caller — a
/// page, a bounded sort window, or nothing at all for a count (ADR-098).
///
/// Three access paths, tried in order of how little they touch: the primary
/// key when the filter pins `_id`, an index when one applies, and otherwise
/// the collection. All three deliver `_id` order when asked for it — the
/// documents table by its key, primary-key probes because the planner sorts
/// them, and an index scan by [`kimmy_storage::CandidateOrder::ById`] — so
/// resuming after a cursor is a bound rather than a filter, and a page costs
/// its own size rather than everything before it. `stop_after` ends the scan
/// once that many matches have been visited.
pub fn visit_matching<F>(
    state: &SharedState,
    meta: &CollectionMeta,
    filter: &filter::Filter,
    order: Order<'_>,
    stop_after: Option<usize>,
    visit: F,
) -> Result<QueryStats, ApiError>
where
    F: FnMut(kimmy_core::Stamp, bson::Document),
{
    let after = match order {
        Order::ById { after } => after,
        Order::Any => None,
    };
    let mut recheck = Recheck { filter, stop_after, examined: 0, matched: 0, visit };

    // The primary key first: it is the tightest access path there is, needs no
    // index to exist, and beats anything a secondary index could offer for the
    // same predicate. The keys are already document keys, so this produces the
    // same candidate shape an index scan does and the filter is re-applied to
    // each exactly as it is there.
    if let Some(pk) = plan::choose_primary_key(filter) {
        for key in &pk.keys {
            if after.is_some_and(|bound| key.as_slice() <= bound) {
                continue;
            }
            // A key naming no document is an ordinary miss, not an error: the
            // filter asked for an `_id` nothing was stored under.
            let Some((stamp, doc)) = state.engine.get_record_by_encoded_key(meta, key)? else {
                continue;
            };
            if !recheck.take(stamp, doc) {
                break;
            }
        }
        return Ok(QueryStats {
            index: None,
            fields_used: 0,
            examined: recheck.examined,
            matched: recheck.matched,
            probes: pk.keys.len(),
            index_entries: None,
            unkeyed: None,
            undecidable: None,
            id_lookup: true,
        });
    }

    // From here the read is a walk — of an index range or of the collection —
    // and it runs under `blocking` for the reason a write's wait for the
    // writer does (ADR-153): a walk is as long as the collection, it never
    // yields, and one per worker is every worker gone. Sixty-four `count`
    // clients over 300,000 documents on a two-worker runtime held every
    // scrape of `/metrics`, every `/v1/version` and every one-document `find`
    // past a 5 s timeout; the scan was the only thing running. Under
    // `block_in_place` the runtime hands the worker's queue to another thread
    // and the walk keeps the thread it is on. The primary-key probes above
    // stay inline: each is one page read, and the cost of giving up a worker
    // for it would be paid on the fastest path there is.
    let mut plan = plan::choose(filter, &meta.indexes);
    let mut entries = None;
    let mut unkeyed = None;
    let mut undecidable = None;
    if let Some(p) = &plan {
        // Candidates stream out of the index and are rechecked as they come,
        // so stopping stops the read; nothing proportional to the range is
        // gathered first. A plan that intersected both ends of a range is
        // only sound while the index is not multikey — and it was chosen
        // from a metadata read that is already stale. The engine re-reads
        // the flag in the same snapshot as the scan; `None` means a write
        // flipped it in between, and the honest answer is to fall back to
        // scanning the collection. That can happen at most once per index,
        // ever, since the flag never clears.
        let scan = kimmy_storage::IndexScan {
            index_id: p.index_id,
            ranges: &p.ranges,
            both_bounds: p.both_bounds,
            exact: p.exact,
        };
        let delivery = match order {
            Order::ById { after } => {
                kimmy_storage::CandidateOrder::ById { after, want: stop_after }
            }
            Order::Any => kimmy_storage::CandidateOrder::Any,
        };
        let outcome = kimmy_storage::blocking(|| {
            state.engine.visit_index_candidates(meta, &scan, delivery, |_, stamp, doc| {
                Ok(recheck.take(stamp, doc))
            })
        })?;
        match outcome {
            Some(outcome) => {
                entries = Some(outcome.entries);
                unkeyed = Some(outcome.unkeyed);
                undecidable = Some(outcome.undecidable);
            }
            None => plan = None,
        }
    }
    if plan.is_none() {
        kimmy_storage::blocking(|| {
            state
                .engine
                .for_each_record_after(meta, after, |_, stamp, doc| Ok(recheck.take(stamp, doc)))
        })?;
    }

    Ok(QueryStats {
        index: plan.as_ref().map(|p| p.index_name.clone()),
        fields_used: plan.as_ref().map_or(0, |p| p.fields_used),
        examined: recheck.examined,
        matched: recheck.matched,
        probes: plan.as_ref().map_or(0, |p| p.ranges.len()),
        index_entries: entries,
        unkeyed,
        undecidable,
        id_lookup: false,
    })
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
    // The tombstone's stamp, reported as every other write reports the
    // version it produced (ADR-084): `POST .../delete` of one document
    // already did, and a by-id delete that said only `{"deleted": 1}` was the
    // one write a client could not follow with the version it made.
    let stamp = state.engine.delete_if(&meta, &doc_id, expected)?;
    let mut body = json!({ "deleted": u8::from(stamp.is_some()) });
    if let Some(stamp) = stamp {
        body["stamp"] = json!(stamp.encode());
    }
    Ok(body)
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
    /// The request's `arrayFilters`: which elements the update's
    /// `$[<identifier>]` segments address (ADR-104). Meaningful to `update`
    /// only; a delete has no paths.
    pub array_filters: Vec<Value>,
}

impl WriteParams {
    /// The caller's condition, decoded — and refused alongside `multi`,
    /// because one stamp cannot describe several documents, and alongside
    /// `explain`, because a plan cannot honestly answer "would this write
    /// happen" without checking a version it never reads (ADR-131).
    fn expected(&self) -> Result<Option<kimmy_core::Stamp>, ApiError> {
        if self.multi && self.if_stamp.is_some() {
            return Err(ApiError::bad_request(
                "`if_stamp` names one document's version, so it cannot be combined with \
                 `multi: true`",
            ));
        }
        if self.explain && self.if_stamp.is_some() {
            return Err(ApiError::bad_request(
                "`if_stamp` makes the write conditional on a version explain does not check, \
                 so it cannot be combined with `explain: true`",
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
    let update = parse_update(update_json, &params.array_filters)?;
    let expected = params.expected()?;
    let stop_after = if multi { None } else { Some(1) };

    // `explain: true` plans the write and reports it, exactly as `find`
    // reports a read — it does not perform the write (ADR-131). The same
    // read-only scan `find` and `count` use already produces everything
    // `explain` reports, so this goes through it rather than through the
    // engine's write transaction: nothing is written and no commit is
    // spent. `matched`/`modified`/`commits` stay at the values that mean
    // "nothing happened", because nothing did; what the write *would* touch
    // is `explain.documentsMatched`, the same field a real write's `explain`
    // has always carried.
    if explain {
        let stats = visit_matching(state, &meta, &filter, Order::Any, stop_after, |_, _| {})?;
        return Ok(json!({
            "matched": 0,
            "modified": 0,
            "commits": 0,
            "explain": stats.to_json(),
        }));
    }

    // Match and write in one transaction. This used to collect the targets
    // in a read transaction and `replace` each in its own write transaction,
    // which applied the operators to an image another writer could have
    // already moved on from: two concurrent `$inc`s both read 5 and both
    // stored 6. The engine now runs the same in-transaction body
    // `find_and_modify` has, over every match.
    let candidates = candidates_for(&filter, &meta);
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
    Ok(body)
}

/// An update body and the request's `arrayFilters`, parsed together: the
/// identifiers the paths use and the filters that define them are checked
/// against each other, so neither is meaningful alone.
fn parse_update(update_json: &Value, array_filters: &[Value]) -> Result<update::Update, ApiError> {
    let filters = array_filters.iter().map(json_to_document).collect::<Result<Vec<_>, _>>()?;
    Ok(update::parse_with_filters(&json_to_document(update_json)?, &filters)?)
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

/// Where a filtered write looks, in the engine's terms.
///
/// The same planner `find` runs, in the same order — primary key first, then
/// an index, then a scan — so an update is found exactly the way a read is.
/// The engine re-checks a both-bounds index plan inside the transaction that
/// scans, which is stricter than the read path can be.
///
/// `explain` no longer reads this plan back out (ADR-131): a write's
/// `explain` now runs the read-only [`visit_matching`] instead of the write
/// transaction, which plans and reports for itself exactly as `find` does.
fn candidates_for(filter: &filter::Filter, meta: &CollectionMeta) -> kimmy_storage::Candidates {
    if let Some(pk) = plan::choose_primary_key(filter) {
        return kimmy_storage::Candidates::Keys(pk.keys);
    }
    match plan::choose(filter, &meta.indexes) {
        Some(p) => kimmy_storage::Candidates::Index {
            index_id: p.index_id,
            ranges: p.ranges.clone(),
            both_bounds: p.both_bounds,
        },
        None => kimmy_storage::Candidates::Scan,
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
    /// The request's `arrayFilters`, as on `update` (ADR-104).
    pub array_filters: Vec<Value>,
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

    fn unsortable(&self, doc: &Document) -> Option<String> {
        shape::unsortable(self.sort, doc)
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
        Some(value) => Some(parse_update(value, &spec.array_filters)?),
        None => None,
    };

    let now = now_millis();

    // The upsert image is built here rather than in the engine, because it
    // needs the filter and the update operators — both query-language things.
    let upsert_doc = if spec.upsert {
        let mut seed = Document::new();
        implied_equalities(&filter, &mut seed);
        if let Some(update) = &update {
            update::apply_on_insert(update, &mut seed, now)?;
        }
        Some(seed)
    } else {
        None
    };

    // Planned the way `update` is — which now includes the primary key: a
    // `find_and_modify` on `_id` used to scan the collection under the
    // writer, because only the index planner ran here.
    let candidates = candidates_for(&filter, &meta);

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
    let stop_after = if multi { None } else { Some(1) };

    // Plans without deleting, exactly as `update` does above (ADR-131).
    if explain {
        let stats = visit_matching(state, &meta, &filter, Order::Any, stop_after, |_, _| {})?;
        return Ok(json!({ "deleted": 0, "commits": 0, "explain": stats.to_json() }));
    }

    // The same one-transaction path as `update`, with the spec removing
    // rather than replacing: what the filter matched is exactly what is
    // tombstoned, with no read-then-write gap for another writer.
    let candidates = candidates_for(&filter, &meta);
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
    create_index_stamped(state, auth, db, coll, spec).map(|(out, _)| out)
}

/// [`create_index`], then confirmed on every live member before answering
/// (ADR-140): the response carries `confirmation` naming who holds the
/// definition now, who refused it, and who has not answered. A node with no
/// peers to confirm on answers exactly as before.
pub async fn create_index_confirmed(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    spec: IndexSpec,
) -> Result<Value, ApiError> {
    let (mut out, stamp) = create_index_stamped(state, auth, db, coll, spec)?;
    if let Some(stamp) = stamp
        && let Some(confirmation) = confirm_ddl(state, stamp).await?
    {
        out["confirmation"] = confirmation;
    }
    Ok(out)
}

/// The body of [`create_index`], with the creation stamp of the index the
/// response describes — the entry a confirmation pushes.
fn create_index_stamped(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    spec: IndexSpec,
) -> Result<(Value, Option<kimmy_core::Stamp>), ApiError> {
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

    // The build files every document of the collection in the transaction
    // that creates the index, so it runs off the async worker.
    let index = kimmy_storage::blocking(|| {
        state.engine.create_index_with(
            db,
            coll,
            fields,
            spec.unique,
            enforcement,
            spec.name,
            spec.expire_after_seconds,
            partial_filter,
        )
    })?;
    // What the backfill could not key, read back from the index it just
    // built: the one number a client creating an index over existing data
    // most wants beside `multikey`, and the listing reports the same field.
    let meta = state.engine.get_collection(db, coll)?;
    let unkeyed = state.engine.unkeyed_count(&meta, index.id)?;
    let undecidable = state.engine.undecidable_count(&meta, index.id)?;
    Ok((index_to_json(&index, unkeyed, undecidable), index.created))
}

/// Confirm a schema change this node just minted on every live member
/// (ADR-140), rendered for a response.
///
/// `None` when this node has no confirmer — clustering or membership off —
/// or when no entry stands under `stamp`, which is an index that was already
/// here before this request and whose creation has since aged out of the
/// oplog. The caller then says nothing about the peers rather than
/// something false.
pub async fn confirm_ddl(
    state: &SharedState,
    stamp: kimmy_core::Stamp,
) -> Result<Option<Value>, ApiError> {
    let Some(confirm) = state.ddl_confirmer() else {
        return Ok(None);
    };
    let Some(entry) = state.engine.oplog_entry(&stamp)? else {
        return Ok(None);
    };
    // The change has committed. Whatever the confirmation has not heard when
    // the request's deadline comes is reported as pending, with the margin
    // left to write the answer: cut off by the deadline instead, the request
    // was answered "abandoned" for a change that exists and replicates.
    let cap = crate::limits::request_time_left().map(|left| left.saturating_sub(ANSWER_MARGIN));
    Ok(Some(confirmation_to_json(&confirm(entry, cap).await)))
}

/// What a schema change's confirmation leaves of the request's deadline for
/// writing the answer.
const ANSWER_MARGIN: std::time::Duration = std::time::Duration::from_millis(250);

/// A confirmation as a response carries it: node ids as strings, the way
/// `/v1/topology` names them, and each pending member with its reason.
pub fn confirmation_to_json(found: &crate::state::DdlConfirmation) -> Value {
    let ids = |nodes: &[kimmy_core::NodeId]| -> Vec<String> {
        nodes.iter().map(ToString::to_string).collect()
    };
    json!({
        "confirmed": ids(&found.confirmed),
        "refused": ids(&found.refused),
        "pending": found
            .pending
            .iter()
            .map(|(node, reason)| json!({ "node": node.to_string(), "reason": reason }))
            .collect::<Vec<_>>(),
    })
}

pub fn list_indexes(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
) -> Result<Value, ApiError> {
    let _span = op_span("list_indexes", db, Some(coll)).entered();
    authorize(state, auth, Action::Read, db, coll)?;
    let meta = state.engine.get_collection(db, coll)?;
    let mut indexes = Vec::with_capacity(meta.indexes.len());
    for index in &meta.indexes {
        indexes.push(index_to_json(
            index,
            state.engine.unkeyed_count(&meta, index.id)?,
            state.engine.undecidable_count(&meta, index.id)?,
        ));
    }
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
    // A pass over the retained oplog, as long as retention makes it, and so a
    // walk like any other (ADR-153). Skipped by the engine when no unique index
    // could answer, which includes an `index` this collection does not have.
    let live = kimmy_storage::blocking(|| state.engine.live_unique_violations(&meta, index))?;

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
    drop_index_stamped(state, auth, db, coll, name).map(|(out, _)| out)
}

/// [`drop_index`], then confirmed on every live member before answering
/// (ADR-140), as [`create_index_confirmed`] is. A drop that found nothing
/// here mints no entry and confirms nothing: there is no drop to carry.
pub async fn drop_index_confirmed(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    name: &str,
) -> Result<Value, ApiError> {
    let (mut out, stamp) = drop_index_stamped(state, auth, db, coll, name)?;
    if let Some(stamp) = stamp
        && let Some(confirmation) = confirm_ddl(state, stamp).await?
    {
        out["confirmation"] = confirmation;
    }
    Ok(out)
}

fn drop_index_stamped(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    name: &str,
) -> Result<(Value, Option<kimmy_core::Stamp>), ApiError> {
    let _span = op_span("drop_index", db, Some(coll)).entered();
    auth.require(Action::Ddl, db, Some(coll))?;
    // `dropped` says whether this member held the index; the drop is recorded
    // and replicated either way, and its stamp is what the confirmation
    // pushes (ADR-140, ADR-141).
    // One transaction that removes every entry of the index, off the worker.
    let dropped = kimmy_storage::blocking(|| state.engine.drop_index_stamped(db, coll, name))?;
    Ok((json!({ "dropped": dropped.removed }), dropped.stamp))
}

/// `unkeyed` is how many documents the index holds that it could not key, and
/// `undecidable` how many it holds because its partial filter could not decide
/// them — both read from the index, since the definition carries neither.
///
/// **Two figures rather than one.** They were one for a while, and it made
/// `unkeyed` mean something other than what every place it is documented says:
/// a collection with a `Decimal128` at a filtered path reported `unkeyed: 2`
/// while `kimmy_index_unkeyed_total` read 0 (ADR-185).
pub fn index_to_json(index: &kimmy_storage::IndexMeta, unkeyed: u64, undecidable: u64) -> Value {
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
        // Documents the index holds and could not key: arrays at two of a
        // compound index's paths, more than 1,000 keys, a Decimal128. Every
        // scan of the index rechecks them (ADR-139). Zero is the index doing
        // its whole job; anything else names work for the collection's owner,
        // who can see it here without access to the server's logs.
        "unkeyed": unkeyed,
        // Documents the index holds because its partial filter could not decide
        // them: a `Decimal128` at a filtered path ranks equal to every number, so
        // the filter's answer is not an answer and the index holds the document
        // for the scan to re-check (ADR-185). Every scan pays for these, so an
        // owner needs the standing number — on a money field it may be most of
        // the collection.
        //
        // **Always present, like `unkeyed` beside it.** Rendering it only when
        // non-zero would be a second convention in one object: a client would
        // have to know that absent means zero, and a typed client would break the
        // first time it appeared.
        "undecidable": undecidable,
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
    aggregate_with_limits(state, auth, db, coll, pipeline, aggregate::Limits::default())
}

/// [`aggregate`] under an explicit ceiling.
///
/// Split out so the source ceiling can be exercised without inserting a
/// hundred thousand documents; every caller outside a test goes through
/// [`aggregate`] and the default.
pub fn aggregate_with_limits(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    pipeline: &Value,
    limits: aggregate::Limits,
) -> Result<Value, ApiError> {
    let _span = op_span("aggregate", db, Some(coll)).entered();
    let meta = authorize(state, auth, Action::Read, db, coll)?;

    let stages = parse_pipeline(pipeline)?;

    // The source. A pipeline that begins with `$match` is read the way `find`
    // reads: the leading filter goes through `collect_matching`, so an indexed
    // equality or range fetches its candidates rather than the whole
    // collection, and the ceiling applies to what the filter *admits* rather
    // than to what the collection holds. Only the leading run of `$match`
    // stages is taken — `aggregate::leading_match` says why — and every later
    // stage runs exactly as it did, on exactly the input it had. A pipeline
    // with no leading `$match` is the collection, through the same scan.
    let (filter, consumed, what) = match aggregate::leading_match(&stages) {
        Some((filter, consumed)) => (filter, consumed, "the leading $match"),
        None => (filter::Filter::AlwaysTrue, 0, "the source collection"),
    };
    // One past the ceiling: the scan stops as soon as it is over, rather than
    // materialising everything the filter admits in order to refuse it.
    let (docs, _stats) = collect_matching(state, &meta, &filter, Some(limits.max_documents + 1))?;
    if docs.len() > limits.max_documents {
        return Err(ApiError::bad_request(format!(
            "{what} admits more than {} documents, the pipeline limit. Narrow it with a more \
             selective $match",
            limits.max_documents
        )));
    }

    let docs = run_stages(state, auth, db, &stages[consumed..], docs, &limits, &[])?;

    let documents: Vec<Value> = docs.iter().map(document_to_json).collect();
    Ok(json!({ "documents": documents, "count": documents.len() }))
}

/// Run stages in order: the two `$lookup` forms go to the functions here that
/// hold a storage handle, everything else to the pure pipeline.
///
/// `vars` is what an enclosing `$lookup` `let` bound — empty at the top level,
/// and the reason this is a function rather than a loop in `aggregate`: a
/// sub-pipeline runs through the same dispatch, so a `$lookup` nested inside
/// another's pipeline is authorized and executed exactly as an outer one is.
fn run_stages(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    stages: &[aggregate::Stage],
    mut docs: Vec<bson::Document>,
    limits: &aggregate::Limits,
    vars: &[Binding<'_>],
) -> Result<Vec<bson::Document>, ApiError> {
    for stage in stages {
        docs = match stage {
            aggregate::Stage::Lookup {
                from,
                as_field,
                join: aggregate::Join::Equality { local_field, foreign_field },
            } => lookup(state, auth, db, from, local_field, foreign_field, as_field, docs, limits)?,
            aggregate::Stage::Lookup {
                from,
                as_field,
                join: aggregate::Join::Pipeline { vars: let_vars, stages: sub },
            } => {
                lookup_pipeline(state, auth, db, from, let_vars, sub, as_field, docs, limits, vars)?
            }
            other => aggregate::apply_with_vars(other, docs, limits, vars)?,
        };
    }
    Ok(docs)
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
    // The whole foreign collection, so a walk (ADR-153).
    kimmy_storage::blocking(|| {
        state.engine.for_each_doc(&foreign, |_id, doc| {
            // `foreignField` and `localField` are field paths, not expressions:
            // they name the key on each side, read the same way here and in
            // `aggregate::lookup_keys`, and neither fans out across an array.
            let Some(value) = kimmy_core::path::resolve(&doc, foreign_field).into_iter().next()
            else {
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
        })
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

/// The `let`/`pipeline` form: the sub-pipeline runs over the foreign collection
/// once **per input document**, with that document's `let` bound.
///
/// That is a nested loop — O(n·m) in the two collection sizes — and inherent to
/// the form: the sub-pipeline may do anything at all with the variables, so
/// there is no single key to index the foreign side by. What keeps it
/// tolerable: the foreign collection is read from storage once and held for
/// the duration, and a leading `$match` is applied once before the loop, since
/// a filter cannot read `let`. That is still true now that `$expr` has put
/// variables within a filter's reach (ADR-106 over ADR-105), but the reason is
/// narrower than "the filter language has no variables": `$match` is parsed by
/// `filter::parse`, which parses its `$expr` with no names bound, so the only
/// variables it can name are `$$ROOT` and `$$CURRENT` — and those are the
/// foreign document under consideration whether the stage runs here or inside
/// the loop. A `$$name` from `let` is a parse error, not a wrong answer. If
/// `$match` ever parses with the sub-pipeline's names in scope, this hoist must
/// be conditioned on the filter not using them. A join on one key belongs in
/// the `localField`/`foreignField` form, which is a single pass.
#[allow(clippy::too_many_arguments)]
fn lookup_pipeline(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    from: &str,
    let_vars: &[(String, kimmy_query::Expr)],
    stages: &[aggregate::Stage],
    as_field: &str,
    input: Vec<bson::Document>,
    limits: &aggregate::Limits,
    outer: &[Binding<'_>],
) -> Result<Vec<bson::Document>, ApiError> {
    // The second authorization point, exactly as for the equality form.
    let foreign_meta = authorize(state, auth, Action::Read, db, from)?;

    let mut foreign: Vec<bson::Document> = Vec::new();
    // The whole foreign collection, so a walk (ADR-153).
    kimmy_storage::blocking(|| {
        state.engine.for_each_doc(&foreign_meta, |_id, doc| {
            foreign.push(doc);
            Ok(true)
        })
    })?;
    aggregate::check_limit("$lookup", foreign.len(), limits)?;

    let (base, rest) = match stages.split_first() {
        Some((first @ aggregate::Stage::Match(_), rest)) => {
            (aggregate::apply(first, foreign, limits)?, rest)
        }
        _ => (foreign, stages),
    };

    let mut held = 0usize;
    let mut out = Vec::with_capacity(input.len());
    for mut doc in input {
        let bound = aggregate::bind_let(let_vars, &doc, outer)?;
        // Inner bindings after outer ones, so a name rebound here shadows.
        let vars: Vec<Binding<'_>> =
            outer.iter().copied().chain(bound.iter().map(|(n, v)| (n.as_str(), v))).collect();
        let joined = run_stages(state, auth, db, rest, base.clone(), limits, &vars)?;
        // Joined documents are held alongside the input, so the total across
        // every input document is what the ceiling bounds.
        held += joined.len();
        aggregate::check_limit("$lookup", held, limits)?;
        doc.insert(
            as_field.to_string(),
            bson::Bson::Array(joined.into_iter().map(bson::Bson::Document).collect()),
        );
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

    #[test]
    fn the_shadow_a_drop_strands_is_derived_to_the_id_the_catalogue_holds() {
        // `shadow_of` reads nothing, so the only thing that can make it wrong
        // is deriving from the wrong name: the parent's, or a shadow's name
        // suffixed a second time. Pinned against the id the engine actually
        // filed the shadow under, for a configured collection and for the
        // shadow named directly.
        let dir = tempfile::tempdir().unwrap();
        let engine = kimmy_storage::Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let shadow_name = kimmy_core::vector_meta::shadow_name("docs");
        let docs = engine.create_collection("app", "docs").unwrap();
        let shadow = engine.create_system_collection("app", &shadow_name).unwrap();
        assert_eq!(engine.get_collection("app", &shadow_name).unwrap().id, shadow.id);

        assert_eq!(shadow_of("app", "docs"), shadow.id, "a parent's drop strands its shadow");
        assert_eq!(shadow_of("app", &shadow_name), shadow.id, "a shadow named directly is itself");
        assert_ne!(shadow_of("app", "docs"), docs.id, "never the parent: it holds no graph");
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
            index_entries: None,
            unkeyed: None,
            undecidable: None,
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
            crate::egress::EgressPolicy::public_only(crate::egress::WEBHOOKS),
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

    #[test]
    fn the_write_planner_and_the_read_planner_choose_the_same_access_path() {
        // `candidates_for` plans a real `update`/`delete`; `visit_matching`
        // plans a read, and — since ADR-131 — also an `explain` on a write.
        // They are two implementations of one policy, not one shared code
        // path, so nothing else in the repository holds them to the same
        // answer: `candidates_for` reduced to always `Candidates::Scan`
        // compiles, and every existing test still passes, because every
        // other test that touches a write's access path drives it through
        // `explain`, which (correctly, and only) exercises the *read*
        // planner. This is the guard on the write planner itself, and it is
        // the one `update_uses_an_index_when_one_applies` and its siblings
        // used to be before their `explain` case moved to the read path.
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
            state.engine.insert(&meta, bson::doc! { "_id": i, "n": i % 3 }).unwrap();
        }

        let id_lookup = filter::parse(&bson::doc! { "_id": 3 }).unwrap();
        let indexed = filter::parse(&bson::doc! { "n": 1 }).unwrap();
        let indexed_in = filter::parse(&bson::doc! { "n": { "$in": [1, 2] } }).unwrap();
        let scanned = filter::parse(&bson::doc! { "missing": 1 }).unwrap();

        assert!(
            matches!(candidates_for(&id_lookup, &meta), kimmy_storage::Candidates::Keys(_)),
            "an _id filter must plan through the primary key"
        );
        assert!(
            matches!(candidates_for(&indexed, &meta), kimmy_storage::Candidates::Index { .. }),
            "an indexed equality must plan through the index"
        );
        assert!(
            matches!(candidates_for(&indexed_in, &meta), kimmy_storage::Candidates::Index { .. }),
            "an indexed $in must plan through the index"
        );
        assert!(
            matches!(candidates_for(&scanned, &meta), kimmy_storage::Candidates::Scan),
            "a filter on an unindexed field must plan a scan"
        );

        // The read planner `explain` now reports must agree with the choice
        // above, filter for filter — this is what makes a plan a caller
        // reads through `explain` the plan the real write will use.
        for (filter, strategy) in [
            (&id_lookup, "idLookup"),
            (&indexed, "index"),
            (&indexed_in, "indexUnion"),
            (&scanned, "collectionScan"),
        ] {
            let stats = visit_matching(&state, &meta, filter, Order::Any, None, |_, _| {}).unwrap();
            assert_eq!(stats.to_json()["strategy"], strategy, "{filter:?}");
        }
    }

    // -----------------------------------------------------------------------
    // aggregate — the leading $match reads through the planner
    // -----------------------------------------------------------------------

    fn superuser() -> Auth {
        Auth(kimmy_auth::Principal::superuser("test"))
    }

    /// `count` documents `{_id: i, n: i % 5, k: i}` in `app.<coll>`, indexed
    /// on `n` when asked.
    fn seed_pipeline_source(state: &SharedState, coll: &str, count: i64, indexed: bool) {
        state.engine.create_collection("app", coll).unwrap();
        if indexed {
            state
                .engine
                .create_index(
                    "app",
                    coll,
                    vec![kimmy_storage::IndexField::ascending("n")],
                    false,
                    None,
                )
                .unwrap();
        }
        let meta = state.engine.get_collection("app", coll).unwrap();
        for i in 0..count {
            state.engine.insert(&meta, bson::doc! { "_id": i, "n": i % 5, "k": i }).unwrap();
        }
    }

    // -----------------------------------------------------------------------
    // Bounded reads (ADR-098)
    // -----------------------------------------------------------------------

    /// A collection of `n` documents `{_id: i, a: i % 10, b: i % 3}`, some
    /// without `a` — the ties and gaps a sort has to get right.
    fn seeded(n: i64) -> (SharedState, CollectionMeta, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        state.engine.create_collection("app", "docs").unwrap();
        let meta = state.engine.get_collection("app", "docs").unwrap();
        for i in 0..n {
            let doc = if i % 13 == 0 {
                bson::doc! { "_id": i, "b": i % 3 }
            } else {
                bson::doc! { "_id": i, "a": i % 10, "b": i % 3 }
            };
            state.engine.insert(&meta, doc).unwrap();
        }
        (state, meta, dir)
    }

    #[test]
    fn a_count_visits_every_match_and_keeps_none_of_them() {
        // `count` used to be `collect_matching(..).len()`: every matching
        // document decoded into a vector and then counted. The visitor sees
        // each once and holds nothing; the stats are the count.
        let (state, meta, _dir) = seeded(250);
        let filter = filter::parse(&bson::doc! { "b": 1 }).unwrap();

        let mut visited = 0usize;
        let stats = visit_matching(&state, &meta, &filter, Order::Any, None, |_, doc| {
            assert_eq!(doc.get_i64("b").unwrap(), 1, "only matches reach the visitor");
            visited += 1;
        })
        .unwrap();

        let expected = (0..250i64).filter(|i| i % 3 == 1).count();
        assert_eq!(visited, expected);
        assert_eq!(stats.matched, expected, "the count is the stats, not a vector's length");
        assert_eq!(stats.examined, 250, "a scan examines everything");
    }

    #[test]
    fn a_bounded_sort_window_gives_the_page_the_full_sort_gave() {
        // The reference is the old path exactly: every match, in `_id`
        // order, through a stable `sort_by` on the caller's keys, then
        // `skip`/`take`. The heap is fed the same matches in a scrambled
        // order and must produce the same page — which is what the `_id`
        // tie-break exists to guarantee.
        let (state, meta, _dir) = seeded(300);
        let filter = filter::parse(&bson::doc! {}).unwrap();
        let (all, _) = collect_matching_stamped_after(&state, &meta, &filter, None, None).unwrap();

        // A fixed-seed shuffle, so a failure reproduces.
        let mut scrambled = all.clone();
        let mut seed = 0x2545_F491_4F6C_DD1Du64;
        for i in (1..scrambled.len()).rev() {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            scrambled.swap(i, (seed % (i as u64 + 1)) as usize);
        }

        let sorts = [
            bson::doc! { "a": 1 },
            bson::doc! { "a": -1 },
            bson::doc! { "a": 1, "b": -1 },
            bson::doc! { "b": 1 },
            bson::doc! { "_id": -1 },
            bson::doc! { "missing": 1 },
        ];
        let windows = [(0usize, 10usize), (0, 1), (7, 5), (95, 10), (290, 20), (300, 5), (0, 300)];

        for spec in &sorts {
            let sort = shape::parse_sort(spec).unwrap();
            let mut reference = all.clone();
            reference.sort_by(|x, y| shape::compare(&sort, &x.1, &y.1));

            for &(skip, limit) in &windows {
                let expected: Vec<i64> = reference
                    .iter()
                    .skip(skip)
                    .take(limit)
                    .map(|(_, d)| d.get_i64("_id").unwrap())
                    .collect();

                let order = with_id_tiebreak(&sort);
                let mut top = TopK::new(skip + limit, &order, None);
                for (stamp, doc) in scrambled.iter().cloned() {
                    top.offer(stamp, doc);
                }
                let got: Vec<i64> = top
                    .into_sorted()
                    .into_iter()
                    .skip(skip)
                    .map(|(_, d)| d.get_i64("_id").unwrap())
                    .collect();
                assert_eq!(got, expected, "sort {spec:?}, skip {skip}, limit {limit}");
            }
        }
    }

    #[test]
    fn a_leading_match_is_held_to_the_ceiling_not_the_collection() {
        // Thirty documents under a ceiling of ten. Without a leading `$match`
        // the source is the collection and is refused; with one that admits
        // six, the pipeline runs. The same shape at scale is a collection
        // past 100,000 documents with an indexed `$match` in front.
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed_pipeline_source(&state, "docs", 30, true);
        let limits = aggregate::Limits { max_documents: 10 };
        let auth = superuser();

        let whole = json!([{ "$count": "c" }]);
        let err = aggregate_with_limits(&state, &auth, "app", "docs", &whole, limits)
            .expect_err("the whole collection is over the ceiling");
        assert!(format!("{err:?}").contains("source collection"), "{err:?}");

        let narrowed = json!([{ "$match": { "n": 3 } }, { "$count": "c" }]);
        let out = aggregate_with_limits(&state, &auth, "app", "docs", &narrowed, limits).unwrap();
        assert_eq!(out["documents"][0]["c"], 6);

        // A leading `$match` that admits too many is refused too, and the
        // refusal names it rather than the collection.
        let wide = json!([{ "$match": { "n": { "$gte": 0 } } }, { "$count": "c" }]);
        let err = aggregate_with_limits(&state, &auth, "app", "docs", &wide, limits)
            .expect_err("the match admits thirty");
        assert!(format!("{err:?}").contains("leading $match"), "{err:?}");
    }

    #[test]
    fn a_match_after_another_stage_is_not_pushed_down() {
        // `$project` then `$match` must mean what it says: the filter reads
        // the projected document, and the source is still the collection —
        // so under the ceiling it is refused exactly as before.
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed_pipeline_source(&state, "docs", 30, true);
        let limits = aggregate::Limits { max_documents: 10 };

        let pipeline = json!([{ "$project": { "n": 1 } }, { "$match": { "n": 3 } }]);
        let err = aggregate_with_limits(&state, &superuser(), "app", "docs", &pipeline, limits)
            .expect_err("the source is the whole collection");
        assert!(format!("{err:?}").contains("source collection"), "{err:?}");

        // And with room to run, the projection is what the match sees:
        // `k` is gone, so a match on it finds nothing.
        let roomy = aggregate::Limits::default();
        let on_projected_away = json!([{ "$project": { "n": 1 } }, { "$match": { "k": 3 } }]);
        let out =
            aggregate_with_limits(&state, &superuser(), "app", "docs", &on_projected_away, roomy)
                .unwrap();
        assert_eq!(out["count"], 0);
    }

    #[test]
    fn an_indexed_and_an_unindexed_source_give_the_same_answer() {
        // The index only narrows the candidates; the filter decides. The
        // two access paths must agree document for document, including on a
        // range and on consecutive leading matches merged into one.
        let dir = tempfile::tempdir().unwrap();
        let state = live_state(&dir);
        seed_pipeline_source(&state, "indexed", 40, true);
        seed_pipeline_source(&state, "plain", 40, false);
        let auth = superuser();

        for pipeline in [
            json!([{ "$match": { "n": 2 } }, { "$sort": { "_id": 1 } }]),
            json!([{ "$match": { "n": { "$gte": 3 } } }, { "$sort": { "_id": 1 } }]),
            json!([
                { "$match": { "n": 1 } },
                { "$match": { "k": { "$gt": 10 } } },
                { "$group": { "_id": Value::Null, "total": { "$sum": "$k" } } }
            ]),
        ] {
            let indexed = aggregate(&state, &auth, "app", "indexed", &pipeline).unwrap();
            let plain = aggregate(&state, &auth, "app", "plain", &pipeline).unwrap();
            assert_eq!(indexed, plain, "{pipeline}");
            assert!(indexed["count"].as_u64().unwrap() > 0, "the pipeline must find something");
        }
    }

    #[test]
    fn the_tiebreak_is_only_added_when_id_is_not_already_a_key() {
        let by_a = shape::parse_sort(&bson::doc! { "a": 1 }).unwrap();
        let order = with_id_tiebreak(&by_a);
        assert_eq!(order.len(), 2);
        assert_eq!(order[1].path, "_id");
        assert!(!order[1].descending);

        // A sort that already decides by `_id` is total as it stands, and
        // appending an ascending `_id` behind a descending one would be
        // harmless but wrong in spirit.
        let by_id_desc = shape::parse_sort(&bson::doc! { "_id": -1 }).unwrap();
        assert_eq!(with_id_tiebreak(&by_id_desc), by_id_desc);
        let mixed = shape::parse_sort(&bson::doc! { "a": 1, "_id": -1 }).unwrap();
        assert_eq!(with_id_tiebreak(&mixed), mixed);
    }

    #[test]
    fn a_window_of_zero_holds_nothing() {
        // `limit: 0` is a legal request for an empty page; the heap must not
        // treat a capacity of zero as unbounded.
        let (state, meta, _dir) = seeded(3);
        let filter = filter::parse(&bson::doc! {}).unwrap();
        let (all, _) = collect_matching_stamped_after(&state, &meta, &filter, None, None).unwrap();

        let order = with_id_tiebreak(&[]);
        let mut top = TopK::new(0, &order, None);
        for (stamp, doc) in all {
            top.offer(stamp, doc);
        }
        assert!(top.into_sorted().is_empty());
    }

    #[test]
    fn a_stop_after_of_zero_visits_nothing() {
        // The unsorted twin of `a_window_of_zero_holds_nothing`: `Recheck`
        // used to hand the first match to its visitor before checking
        // `stop_after`, so a `stop_after` of zero still let one document
        // through. Seeded so `_id: 0`'s document is the very first candidate
        // the scan examines and matches an empty filter — the exact case
        // that leaked.
        let (state, meta, _dir) = seeded(3);
        let filter = filter::parse(&bson::doc! {}).unwrap();

        let mut visited = 0usize;
        let stats = visit_matching(&state, &meta, &filter, Order::Any, Some(0), |_, _| {
            visited += 1;
        })
        .unwrap();

        assert_eq!(visited, 0, "a stop_after of zero must visit nothing");
        assert_eq!(stats.matched, 0, "{:?}", stats.matched);
    }
}

#[cfg(test)]
mod decimal128_at_the_edge {
    //! The member that accepts a write files a Decimal128 document exactly as
    //! the backfill and the replicated apply do, because the JSON edge now
    //! reads `$numberDecimal` (ADR-139). Storage-level, over the document the
    //! edge produces rather than one built with a Decimal128 in hand.

    use super::*;
    use crate::json::json_to_document;
    use kimmy_storage::IndexField;
    use serde_json::json;

    fn engine() -> (std::sync::Arc<kimmy_storage::Engine>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = kimmy_storage::Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        engine.create_collection("app", "docs").unwrap();
        (std::sync::Arc::new(engine), dir)
    }

    /// A compound index over `a` and `v`, so a `{a: 1}` range reads every
    /// entry filed for the document under `a = 1` — whatever `v` was keyed
    /// as, a Decimal128's nested-document form included.
    fn index(engine: &kimmy_storage::Engine) -> kimmy_storage::CollectionMeta {
        engine
            .create_index(
                "app",
                "docs",
                vec![IndexField::ascending("a"), IndexField::ascending("v")],
                false,
                Some("av".into()),
            )
            .unwrap();
        engine.get_collection("app", "docs").unwrap()
    }

    fn decimal_document() -> Document {
        json_to_document(&json!({ "_id": 1, "a": 1, "v": { "$numberDecimal": "1.5" } })).unwrap()
    }

    #[test]
    fn the_two_orders_a_definition_and_a_document_can_meet_land_in_one_state() {
        // Insert-then-createIndex files through the backfill, which re-decodes
        // the stored bytes; createIndex-then-insert files the document the
        // edge produced. The count used to differ — 1 and 0 — because the
        // edge left `$numberDecimal` a nested document the index could key.
        let (first, _d1) = engine();
        let coll = first.get_collection("app", "docs").unwrap();
        first.insert(&coll, decimal_document()).unwrap();
        let coll = index(&first);
        let backfilled = first.unkeyed_count(&coll, coll.index("av").unwrap().id).unwrap();

        let (second, _d2) = engine();
        let coll = index(&second);
        second.insert(&coll, decimal_document()).unwrap();
        let written = second.unkeyed_count(&coll, coll.index("av").unwrap().id).unwrap();

        assert_eq!(backfilled, 1, "the backfill files the Decimal128 unkeyed");
        assert_eq!(written, backfilled, "and so does the write path, on the same member");
        assert_eq!(second.unkeyed_writes(), 1, "counted, as every unkeyed filing is");
    }

    #[test]
    fn reshaping_the_document_leaves_no_entry_behind() {
        // The old image is re-decoded from its bytes when it is replaced, and
        // classified unkeyed; the acceptor used to have filed it keyed, so the
        // unfiling missed and a document-typed key leaked. A full range over
        // the document's `a` now reads exactly the one entry the new image
        // made.
        let dir = tempfile::tempdir().unwrap();
        let state = live_state_for(&dir);
        let coll = index(&state.engine);
        state.engine.insert(&coll, decimal_document()).unwrap();
        state
            .engine
            .replace(&coll, &kimmy_core::DocId::Int64(1), bson::doc! { "a": 1, "v": 1 }, false)
            .unwrap();
        let coll = state.engine.get_collection("app", "docs").unwrap();
        assert_eq!(state.engine.unkeyed_count(&coll, coll.index("av").unwrap().id).unwrap(), 0);

        let filter = filter::parse(&bson::doc! { "a": 1 }).unwrap();
        let (matched, stats) = collect_matching(&state, &coll, &filter, None).unwrap();
        assert_eq!(stats.index.as_deref(), Some("av"), "read through the index");
        assert_eq!(matched.len(), 1);
        assert_eq!(stats.index_entries, Some(1), "one document, one entry, nothing leaked");
        assert_eq!(stats.unkeyed, Some(0));
    }

    #[test]
    fn an_entry_filed_before_the_fix_outlives_every_later_write() {
        // What the changelog has to say plainly. Through 0.24.0 the edge
        // handed storage a nested document where a Decimal128 was meant, and
        // the index keyed it. Unfiling recomputes the old image's keys from
        // its bytes, which decode to a Decimal128 and classify unkeyed, so
        // every later write removes an unkeyed entry that was never there and
        // leaves the keyed one. Only dropping and recreating the index
        // clears it. Pinned here so the statement stays true.
        let dir = tempfile::tempdir().unwrap();
        let state = live_state_for(&dir);
        let coll = index(&state.engine);
        // The document as the pre-fix edge produced it: the wrapper, unread.
        state
            .engine
            .insert(&coll, bson::doc! { "_id": 1i64, "a": 1, "v": { "$numberDecimal": "1.5" } })
            .unwrap();
        state
            .engine
            .replace(&coll, &kimmy_core::DocId::Int64(1), bson::doc! { "a": 1, "v": 1 }, false)
            .unwrap();

        let filter = filter::parse(&bson::doc! { "a": 1 }).unwrap();
        let coll = state.engine.get_collection("app", "docs").unwrap();
        let (matched, stats) = collect_matching(&state, &coll, &filter, None).unwrap();
        assert_eq!(matched.len(), 1, "the document itself is unaffected");
        assert_eq!(stats.index_entries, Some(2), "the pre-fix entry survives the write");

        state.engine.drop_index("app", "docs", "av").unwrap();
        let coll = index(&state.engine);
        let (_, stats) = collect_matching(&state, &coll, &filter, None).unwrap();
        assert_eq!(stats.index_entries, Some(1), "recreating the index is what clears it");
    }

    fn live_state_for(dir: &tempfile::TempDir) -> SharedState {
        let engine = std::sync::Arc::new(
            kimmy_storage::Engine::open(&dir.path().join("kimmy.redb")).unwrap(),
        );
        engine.create_collection("app", "docs").unwrap();
        let tokens =
            kimmy_auth::TokenIssuer::new("an-adequately-long-test-secret-for-hs256", 3600).unwrap();
        crate::state_with_egress(
            engine,
            tokens,
            false,
            crate::RateLimits::disabled(),
            crate::egress::EgressPolicy::public_only(crate::egress::WEBHOOKS),
        )
        .unwrap()
    }
}
