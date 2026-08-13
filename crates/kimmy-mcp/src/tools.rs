//! The tool surface.
//!
//! Each tool is a thin adapter: take typed arguments, hand them to
//! [`kimmy_api::exec`] along with the caller's principal, render the result.
//! There is no authorization logic here — see the crate documentation for why
//! that is load-bearing rather than merely tidy.
//!
//! Tool *descriptions* are written for a reader that has never seen this
//! database. They are the only documentation an agent gets, so they say what a
//! tool is for and which other tool to reach for instead, not just what its
//! arguments are named.

use kimmy_api::{ApiError, SharedState, exec, schema, vectors};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Implementation, ServerCapabilities, ServerInfo};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, tool, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;

use crate::auth::principal;

#[derive(Clone)]
pub struct KimmyMcp {
    pub(crate) state: SharedState,
    pub(crate) tool_router: ToolRouter<Self>,
}

impl KimmyMcp {
    pub fn new(state: SharedState) -> Self {
        Self { state, tool_router: Self::tool_router() }
    }

    pub(crate) fn server_info() -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().enable_resources().build())
            .with_server_info(Implementation::new("kimmydb", env!("CARGO_PKG_VERSION")))
            .with_instructions(
                "KimmyDB is a document database with vector and hybrid search. \
                 Documents are schemaless BSON, queried with a MongoDB-style filter \
                 language.\n\n\
                 Start with `list_databases`, then `list_collections`, then \
                 `describe_collection` — the last one samples documents and reports the \
                 field paths, their types, and how often each appears. Guessing field \
                 names without it is the usual cause of an empty result.\n\n\
                 Use `find` for exact and range conditions, `vector_search` for \
                 meaning-based retrieval, and `hybrid_search` when the query has both \
                 (a specific term plus a general intent).\n\n\
                 Tools you are not authorized for still appear in this list; calling one \
                 returns an authorization error rather than being hidden.",
            )
    }
}

// ---------------------------------------------------------------------------
// Argument types
//
// Defined here rather than reused from the HTTP layer because a tool schema is
// a contract an agent reads: every field carries a doc comment, and that
// comment becomes the description the model sees.
// ---------------------------------------------------------------------------

#[derive(Deserialize, JsonSchema)]
pub struct DatabaseArgs {
    /// Database name.
    pub database: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct DescribeArgs {
    /// Database name.
    pub database: String,
    /// Collection name.
    pub collection: String,
    /// How many documents to sample. Defaults to 100.
    #[serde(default)]
    pub sample: Option<usize>,
}

#[derive(Deserialize, JsonSchema)]
pub struct FindArgs {
    /// Database name.
    pub database: String,
    /// Collection name.
    pub collection: String,
    /// MongoDB-style query filter, for example
    /// `{"status": "open", "total": {"$gt": 100}}`. Omit to match everything.
    #[serde(default)]
    pub filter: Option<Value>,
    /// Sort specification, for example `{"created_at": -1}`.
    #[serde(default)]
    pub sort: Option<Value>,
    /// Fields to return, for example `{"name": 1, "total": 1}`. Returning only
    /// what you need keeps large documents from crowding out the rest of the
    /// answer.
    #[serde(default)]
    pub projection: Option<Value>,
    /// Maximum documents to return. Defaults to 100.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Documents to skip, for paging.
    #[serde(default)]
    pub skip: Option<usize>,
    /// Also report whether an index was used and how many documents were
    /// examined. Useful when a query is slow.
    #[serde(default)]
    pub explain: bool,
}

#[derive(Deserialize, JsonSchema)]
pub struct CountArgs {
    /// Database name.
    pub database: String,
    /// Collection name.
    pub collection: String,
    /// MongoDB-style query filter. Omit to count the whole collection.
    #[serde(default)]
    pub filter: Option<Value>,
}

#[derive(Deserialize, JsonSchema)]
pub struct AggregateArgs {
    /// Database name.
    pub database: String,
    /// Collection the pipeline reads from.
    pub collection: String,
    /// The pipeline: an array of stage documents, applied in order.
    pub pipeline: Value,
}

#[derive(Deserialize, JsonSchema)]
pub struct SearchArgs {
    /// Database name.
    pub database: String,
    /// Collection name.
    pub collection: String,
    /// Text to search for. The server embeds it, so this requires the
    /// collection to use a server-side embedding provider.
    #[serde(default)]
    pub query: Option<String>,
    /// A pre-computed query vector. Required when the collection's provider is
    /// `byo`, which supplies its own vectors.
    #[serde(default)]
    pub vector: Option<Vec<f32>>,
    /// Restrict results to documents matching this filter, so semantic search
    /// composes with ordinary conditions.
    #[serde(default)]
    pub filter: Option<Value>,
    /// How many results to return. Defaults to 10.
    #[serde(default)]
    pub k: Option<usize>,
}

#[derive(Deserialize, JsonSchema)]
pub struct InsertArgs {
    /// Database name.
    pub database: String,
    /// Collection name.
    pub collection: String,
    /// The document to insert. An `_id` is generated if you omit one.
    pub document: Value,
}

#[derive(Deserialize, JsonSchema)]
pub struct BulkInsertArgs {
    /// Database name.
    pub database: String,
    /// Collection name.
    pub collection: String,
    /// The documents to insert. An `_id` is generated for any that omit one.
    pub documents: Vec<Value>,
}

#[derive(Deserialize, JsonSchema)]
pub struct UpdateArgs {
    /// Database name.
    pub database: String,
    /// Collection name.
    pub collection: String,
    /// Which documents to update. **Omitting this matches every document in
    /// the collection**, so pass one unless that is genuinely what you want.
    #[serde(default)]
    pub filter: Option<Value>,
    /// Update operators, for example `{"$set": {"status": "closed"}}`.
    pub update: Value,
    /// Update every match rather than only the first.
    #[serde(default)]
    pub multi: bool,
}

#[derive(Deserialize, JsonSchema)]
pub struct DeleteArgs {
    /// Database name.
    pub database: String,
    /// Collection name.
    pub collection: String,
    /// Which documents to delete. **Omitting this matches every document in
    /// the collection.**
    #[serde(default)]
    pub filter: Option<Value>,
    /// Delete every match rather than only the first.
    #[serde(default)]
    pub multi: bool,
}

#[derive(Deserialize, JsonSchema)]
pub struct CreateCollectionArgs {
    /// Database name. Created if it does not exist.
    pub database: String,
    /// Name for the new collection.
    pub name: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct IndexFieldArgs {
    /// Dotted field path, for example `customer.id`.
    pub path: String,
    /// Index this field in descending order.
    #[serde(default)]
    pub descending: bool,
}

#[derive(Deserialize, JsonSchema)]
pub struct CreateIndexArgs {
    /// Database name.
    pub database: String,
    /// Collection name.
    pub collection: String,
    /// Fields to index, in order. Order matters: a compound index can answer a
    /// query on a leading prefix of its fields, not on an arbitrary subset.
    pub fields: Vec<IndexFieldArgs>,
    /// Reject documents that duplicate an existing key.
    #[serde(default)]
    pub unique: bool,
    /// Optional index name. Derived from the fields when omitted.
    #[serde(default)]
    pub name: Option<String>,
}

// ---------------------------------------------------------------------------
// Tools
// ---------------------------------------------------------------------------

#[tool_router(router = tool_router)]
impl KimmyMcp {
    /// List the databases on this node.
    #[tool(description = "List the databases this node holds. Only databases you are \
                       authorized to read are returned.")]
    async fn list_databases(
        &self,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = principal(&ctx)?;
        render(exec::list_databases(&self.state, &auth))
    }

    /// List collections in a database.
    #[tool(description = "List the collections in a database. Only collections you are \
                       authorized to read are returned.")]
    async fn list_collections(
        &self,
        Parameters(args): Parameters<DatabaseArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = principal(&ctx)?;
        render(exec::list_collections(&self.state, &auth, &args.database))
    }

    /// Sampled schema of a collection.
    #[tool(description = "Infer a collection's shape by sampling documents: the field \
                       paths that occur, the types found at each, how often each \
                       appears, and an example value. Also reports the indexes and any \
                       vector configuration. Call this before writing a filter — this \
                       database is schemaless, so field names cannot be guessed. \
                       Presence is a fraction of the sample, not a guarantee: a field \
                       missing from the sample may still exist.")]
    async fn describe_collection(
        &self,
        Parameters(args): Parameters<DescribeArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = principal(&ctx)?;
        render(schema::describe_collection(
            &self.state,
            &auth,
            &args.database,
            &args.collection,
            args.sample,
            true,
        ))
    }

    /// Query documents.
    #[tool(description = "Find documents matching a filter. Supports the MongoDB-style \
                       operators $eq $ne $gt $gte $lt $lte $in $nin $exists $type $regex \
                       $all $size $elemMatch $and $or $not $nor. Use this for exact and \
                       range conditions; use vector_search when the question is about \
                       meaning rather than a value.")]
    async fn find(
        &self,
        Parameters(args): Parameters<FindArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = principal(&ctx)?;
        let params = exec::FindParams {
            filter: args.filter,
            sort: args.sort,
            projection: args.projection,
            limit: args.limit,
            skip: args.skip,
            explain: args.explain,
        };
        render(exec::find(&self.state, &auth, &args.database, &args.collection, params))
    }

    /// Count matching documents.
    #[tool(description = "Count the documents matching a filter, without returning them. \
                       Prefer this over find when you only need the number — it does \
                       not spend context on documents you will discard.")]
    async fn count(
        &self,
        Parameters(args): Parameters<CountArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = principal(&ctx)?;
        let params = exec::FindParams { filter: args.filter, ..Default::default() };
        render(exec::count(&self.state, &auth, &args.database, &args.collection, params))
    }

    /// Group, reshape and summarise.
    #[tool(description = "Run an aggregation pipeline: an array of stages applied in order. \
                       Use this instead of find when you need totals, averages, counts \
                       per group, or documents joined from another collection — it does \
                       the work in the database rather than returning rows for you to \
                       reduce. Stages: $match (filter, put it first so later stages see \
                       less), $group (with $sum, $avg, $min, $max, $first, $last, $push, \
                       $addToSet), $unwind, $project, $sort, $skip, $limit, $count, and \
                       $lookup (join; you need read access to the joined collection too). \
                       Field references are written \"$field\". There are no computed \
                       expressions such as $add. A pipeline that would hold too many \
                       documents is refused, naming the stage — add an earlier $match.")]
    async fn aggregate(
        &self,
        Parameters(args): Parameters<AggregateArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = principal(&ctx)?;
        render(exec::aggregate(
            &self.state,
            &auth,
            &args.database,
            &args.collection,
            &args.pipeline,
        ))
    }

    /// Semantic search.
    #[tool(description = "Search a collection by meaning rather than by matching values. \
                       Requires the collection to have vector embeddings configured; \
                       describe_collection reports whether it does. Returns matching \
                       chunks with their source document id and a similarity score. \
                       Pass a filter to combine semantic ranking with ordinary \
                       conditions.")]
    async fn vector_search(
        &self,
        Parameters(args): Parameters<SearchArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = principal(&ctx)?;
        let (db, coll, request) = search_request(args);
        render(vectors::run_vector_search(&self.state, &auth, &db, &coll, &request).await)
    }

    /// Combined semantic and keyword search.
    #[tool(description = "Search by meaning and by keyword at once, fusing the two \
                       rankings. Use this when the query contains a specific term that \
                       must appear — a product code, a name, an error string — as well \
                       as a general intent; pure vector search can rank an exact term \
                       below a paraphrase. Requires `query` text, since the keyword \
                       half needs words.")]
    async fn hybrid_search(
        &self,
        Parameters(args): Parameters<SearchArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = principal(&ctx)?;
        let (db, coll, request) = search_request(args);
        render(vectors::run_hybrid_search(&self.state, &auth, &db, &coll, &request).await)
    }

    /// Insert a document.
    #[tool(description = "Insert one document into a collection.")]
    async fn insert(
        &self,
        Parameters(args): Parameters<InsertArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = principal(&ctx)?;
        render(exec::insert(&self.state, &auth, &args.database, &args.collection, &args.document))
    }

    /// Insert many documents at once.
    #[tool(description = "Insert many documents into a collection in one durable commit. \
                       All of them are written or none are: if any document is rejected \
                       the whole batch fails and the error names its position. At most \
                       1000 documents.")]
    async fn insert_many(
        &self,
        Parameters(args): Parameters<BulkInsertArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = principal(&ctx)?;
        render(exec::insert_many(
            &self.state,
            &auth,
            &args.database,
            &args.collection,
            &args.documents,
        ))
    }

    /// Update documents.
    #[tool(description = "Apply update operators ($set, $unset, $inc, $mul, $min, $max, \
                       $rename, $currentDate, $push, $pull, $addToSet, $pop) to \
                       documents matching a filter. Only the first match is updated \
                       unless `multi` is true.")]
    async fn update(
        &self,
        Parameters(args): Parameters<UpdateArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = principal(&ctx)?;
        render(exec::update(
            &self.state,
            &auth,
            &args.database,
            &args.collection,
            args.filter.as_ref(),
            &args.update,
            args.multi,
        ))
    }

    /// Delete documents.
    #[tool(description = "Delete documents matching a filter. Only the first match is \
                       deleted unless `multi` is true. Deletes are not reversible.")]
    async fn delete(
        &self,
        Parameters(args): Parameters<DeleteArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = principal(&ctx)?;
        render(exec::delete(
            &self.state,
            &auth,
            &args.database,
            &args.collection,
            args.filter.as_ref(),
            args.multi,
        ))
    }

    /// Create a collection.
    #[tool(description = "Create a collection, and its database if that does not exist \
                       yet. Call this before inserting: writing to a collection that \
                       does not exist fails rather than creating it.")]
    async fn create_collection(
        &self,
        Parameters(args): Parameters<CreateCollectionArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = principal(&ctx)?;
        render(exec::create_collection(&self.state, &auth, &args.database, &args.name))
    }

    /// Create a secondary index.
    #[tool(description = "Create a secondary index. Building one blocks until existing \
                       documents are indexed, so it is not free on a large collection. \
                       Check `find` with explain first: a query that already reports \
                       strategy \"index\" does not need another one.")]
    async fn create_index(
        &self,
        Parameters(args): Parameters<CreateIndexArgs>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = principal(&ctx)?;
        let spec = exec::IndexSpec {
            fields: args
                .fields
                .into_iter()
                .map(|f| exec::IndexFieldSpec { path: f.path, descending: f.descending })
                .collect(),
            unique: args.unique,
            name: args.name,
            // `coordinated` needs clustering and is refused until M4, so there
            // is nothing for an agent to choose between yet.
            enforcement: None,
            // Deliberately not exposed. A TTL index arms a background pass
            // that *deletes documents*, and this tool is described to an agent
            // as a query-performance aid — an agent reaching for an index to
            // make a query faster must not be able to schedule data loss as a
            // side effect. Creating one stays an administrative act over HTTP.
            expire_after_seconds: None,
        };
        render(exec::create_index(&self.state, &auth, &args.database, &args.collection, spec))
    }
}

// ---------------------------------------------------------------------------
// Result rendering
// ---------------------------------------------------------------------------

fn search_request(args: SearchArgs) -> (String, String, vectors::SearchRequest) {
    let request = vectors::SearchRequest {
        query: args.query,
        vector: args.vector,
        filter: args.filter,
        k: args.k,
        per_document: None,
    };
    (args.database, args.collection, request)
}

/// Turn an operation's outcome into a tool result.
///
/// Structured content carries the JSON; the text block carries the same thing
/// serialized, because not every client renders structured content and an
/// answer no client shows is not an answer.
pub(crate) fn render(result: Result<Value, ApiError>) -> Result<CallToolResult, ErrorData> {
    match result {
        Ok(value) => Ok(ok(value)),
        Err(e) => Err(error(e)),
    }
}

pub(crate) fn ok(value: Value) -> CallToolResult {
    let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
    let mut result = CallToolResult::success(vec![rmcp::model::ContentBlock::text(text)]);
    result.structured_content = Some(value);
    result
}

/// Map an API failure onto an MCP error.
///
/// The distinction rmcp draws is *whose problem it is*. A rejected filter or a
/// denied authorization is the caller's, and the caller — an agent that may be
/// able to correct itself — needs to read the message, so those become
/// `invalid_params` with the text intact. A storage fault is ours, and its text
/// has already been reduced to something safe to return.
pub(crate) fn error(e: ApiError) -> ErrorData {
    if e.status.is_server_error() {
        ErrorData::internal_error(e.message, None)
    } else {
        ErrorData::invalid_params(e.message, None)
    }
}
