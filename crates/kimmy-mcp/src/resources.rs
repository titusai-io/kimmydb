//! Collections as MCP resources, and the [`ServerHandler`] implementation.
//!
//! A resource is context an agent can attach without spending a tool call, so
//! each collection is published as one: reading `kimmy://{db}/{collection}`
//! returns its inferred schema plus a few whole documents. That is the same
//! material `describe_collection` returns, offered through the channel clients
//! use for "here is what you are working with" rather than "go and do this".
//!
//! Listing is filtered by the caller's grants, exactly as `list_collections`
//! is: a resource list that named collections the caller cannot read would leak
//! their existence.
//!
//! It also omits KimmyDB's own internals — the `__kimmy` system database and
//! the `.__vectors` shadow collections. Not as an access control (a superuser
//! can still read them through `find`, exactly as through the REST API) but
//! because a resource is *material an agent attaches to its context*, and
//! offering it the password-hash collection to attach is the wrong default by
//! any measure. Shadow collections are excluded for a duller reason: they hold
//! float arrays that would consume an enormous amount of context and say
//! nothing the source collection does not.

use kimmy_auth::Action;
use rmcp::model::{
    ListResourcesResult, PaginatedRequestParams, ReadResourceRequestParams, ReadResourceResponse,
    ReadResourceResult, Resource, ResourceContents, ServerInfo,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler, tool_handler};
use serde_json::json;

use crate::auth::principal;
use crate::tools::KimmyMcp;

/// URI scheme for a collection resource.
const SCHEME: &str = "kimmy://";

/// Whole documents included alongside the inferred schema.
///
/// Few, deliberately: the schema is the useful part, and a resource that pastes
/// a hundred documents into the context window crowds out the conversation it
/// was meant to inform.
const SAMPLE_DOCUMENTS: usize = 3;

#[tool_handler(router = self.tool_router)]
impl ServerHandler for KimmyMcp {
    fn get_info(&self) -> ServerInfo {
        Self::server_info()
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let auth = principal(&ctx)?;

        let databases =
            self.state.engine.list_databases().map_err(|e| crate::tools::error(e.into()))?;

        let mut resources = Vec::new();
        for database in databases {
            if is_internal_database(&database.name) {
                continue;
            }
            let collections = self
                .state
                .engine
                .list_collections(&database.name)
                .map_err(|e| crate::tools::error(e.into()))?;

            for collection in collections {
                if is_internal_collection(&collection.name) {
                    continue;
                }
                if !auth.principal().can(Action::Read, &database.name, Some(&collection.name)) {
                    continue;
                }
                let uri = format!("{SCHEME}{}/{}", database.name, collection.name);
                let mut resource =
                    Resource::new(uri, format!("{}.{}", database.name, collection.name))
                        .with_mime_type("application/json")
                        .with_description(describe_briefly(&collection));
                resource.title = Some(collection.name.clone());
                resources.push(resource);
            }
        }

        Ok(ListResourcesResult { resources, ..Default::default() })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        ctx: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        let auth = principal(&ctx)?;
        let (db, coll) = parse_uri(&request.uri)?;

        let schema =
            kimmy_api::schema::describe_collection(&self.state, &auth, db, coll, None, true)
                .map_err(crate::tools::error)?;

        let samples =
            kimmy_api::schema::sample_documents(&self.state, &auth, db, coll, SAMPLE_DOCUMENTS)
                .map_err(crate::tools::error)?;

        let body = json!({ "schema": schema, "samples": samples });
        let text = serde_json::to_string_pretty(&body)
            .map_err(|e| ErrorData::internal_error(e.to_string(), None))?;

        Ok(ReadResourceResult::new(vec![ResourceContents::TextResourceContents {
            uri: request.uri.clone(),
            mime_type: Some("application/json".into()),
            text,
            meta: None,
        }])
        .into())
    }
}

/// Whether a database holds KimmyDB's own bookkeeping rather than user data.
fn is_internal_database(name: &str) -> bool {
    name == kimmy_auth::users::SYSTEM_DB
}

/// Whether a collection holds machinery rather than user data.
///
/// The `__` prefix is reserved for system objects, so this covers the shadow
/// collections that back vector search as well as anything added later.
fn is_internal_collection(name: &str) -> bool {
    kimmy_core::vector_meta::is_shadow(name)
        || name.split('.').any(|segment| segment.starts_with("__"))
}

/// One line telling an agent whether a collection is worth opening.
fn describe_briefly(collection: &kimmy_storage::CollectionMeta) -> String {
    let mut parts = vec![format!("{} index(es)", collection.indexes.len())];
    if let Some(vector) = &collection.vector {
        parts.push(format!("vector search enabled ({} dimensions)", vector.dim));
    }
    format!(
        "Collection {}: {}. Read for inferred schema and sample documents.",
        collection.name,
        parts.join(", ")
    )
}

/// Split `kimmy://db/collection` into its two names.
fn parse_uri(uri: &str) -> Result<(&str, &str), ErrorData> {
    let rest = uri.strip_prefix(SCHEME).ok_or_else(|| {
        ErrorData::invalid_params(
            format!(
                "unsupported resource URI {uri:?}: expected {SCHEME}{{database}}/{{collection}}"
            ),
            None,
        )
    })?;
    // `split_once` rather than `split`: a collection name is a single segment,
    // so a URI with more slashes is malformed rather than something to
    // reinterpret.
    match rest.split_once('/') {
        Some((db, coll)) if !db.is_empty() && !coll.is_empty() && !coll.contains('/') => {
            Ok((db, coll))
        }
        _ => Err(ErrorData::invalid_params(
            format!(
                "unsupported resource URI {uri:?}: expected {SCHEME}{{database}}/{{collection}}"
            ),
            None,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_collection_uri_splits_into_database_and_collection() {
        assert_eq!(parse_uri("kimmy://sales/orders").unwrap(), ("sales", "orders"));
    }

    #[test]
    fn a_uri_without_the_scheme_is_rejected() {
        assert!(parse_uri("https://sales/orders").is_err());
        assert!(parse_uri("sales/orders").is_err());
    }

    #[test]
    fn a_uri_missing_either_name_is_rejected() {
        assert!(parse_uri("kimmy://sales").is_err());
        assert!(parse_uri("kimmy://sales/").is_err());
        assert!(parse_uri("kimmy:///orders").is_err());
    }

    #[test]
    fn internal_objects_are_not_offered_as_resources() {
        // The user store holds password hashes; a resource is something an
        // agent attaches to its context. Those two facts must not meet.
        assert!(is_internal_database(kimmy_auth::users::SYSTEM_DB));
        assert!(is_internal_collection("__users"));
        assert!(is_internal_collection("orders.__vectors"));

        assert!(!is_internal_database("shop"));
        assert!(!is_internal_collection("orders"));
        // A name that merely *contains* underscores is ordinary user data.
        assert!(!is_internal_collection("my__orders"));
        assert!(!is_internal_collection("orders_2024"));
    }

    #[test]
    fn extra_path_segments_are_rejected_rather_than_reinterpreted() {
        // Silently reading `kimmy://a/b/c` as `a`.`b` would answer a question
        // the caller did not ask.
        assert!(parse_uri("kimmy://sales/orders/extra").is_err());
    }
}
