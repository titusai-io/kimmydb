//! Webhook subscriptions: the registry and its API.
//!
//! Delivery is not here — this is registration only. A subscription written
//! here is what a dispatcher will later read.
//!
//! # The registry is an ordinary collection
//!
//! `__kimmy.__webhooks`, alongside `__users`. That is not laziness: being a
//! collection means a subscription **replicates** to every node, is included in
//! a **backup**, comes back on **restore**, and is visible to the same
//! tooling as any other document — none of which a bespoke table would get
//! without writing each one again.
//!
//! Replication is also load-bearing rather than incidental: every node needs to
//! see every subscription, because ownership is derived from the live member
//! set and any node may end up delivering this one.
//!
//! # The secret is generated, never supplied
//!
//! Each subscription gets a signing secret so a receiver can tell a genuine
//! delivery from anything else that can reach its URL. It is returned **once**,
//! at registration, and never again — listing subscriptions does not include
//! it. A caller who loses it re-registers. Storing something a caller chose
//! would invite reuse of a password.

use bson::{Document, doc};
use kimmy_auth::Action;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::egress::EgressPolicy;
use crate::error::ApiError;
use crate::state::{Auth, SharedState};

/// System database and collection holding subscriptions.
pub const WEBHOOKS_DB: &str = "__kimmy";
pub const WEBHOOKS_COLLECTION: &str = "__webhooks";

/// Which operations a subscription wants.
///
/// Absent means "all of them", which is what a caller who did not think about
/// it almost certainly wants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WebhookOperation {
    Insert,
    Update,
    Replace,
    Delete,
}

impl WebhookOperation {
    fn parse(name: &str) -> Result<Self, ApiError> {
        match name {
            "insert" => Ok(Self::Insert),
            "update" => Ok(Self::Update),
            "replace" => Ok(Self::Replace),
            "delete" => Ok(Self::Delete),
            other => Err(ApiError::bad_request(format!(
                "unknown operation {other:?}; expected insert, update, replace or delete"
            ))),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Replace => "replace",
            Self::Delete => "delete",
        }
    }
}

/// A registered subscription, as stored.
#[derive(Clone, Debug, PartialEq)]
pub struct Subscription {
    pub id: String,
    pub database: String,
    pub collection: String,
    pub url: String,
    /// Empty means every operation.
    pub operations: Vec<WebhookOperation>,
    /// HMAC key for signing deliveries. Never returned after registration.
    pub secret: String,
    /// Who registered it, for the audit trail. A subscription outlives the
    /// token that created it, so the record of who asked for it matters.
    pub created_by: String,
    pub created_ms: u64,
}

impl Subscription {
    fn to_document(&self) -> Document {
        doc! {
            "_id": self.id.clone(),
            "database": self.database.clone(),
            "collection": self.collection.clone(),
            "url": self.url.clone(),
            "operations": self.operations.iter().map(|o| o.name()).collect::<Vec<_>>(),
            "secret": self.secret.clone(),
            "createdBy": self.created_by.clone(),
            "createdMs": self.created_ms as i64,
        }
    }

    /// The public view: everything except the secret.
    fn to_json(doc: &Document) -> Value {
        json!({
            "id": doc.get_str("_id").unwrap_or_default(),
            "database": doc.get_str("database").unwrap_or_default(),
            "collection": doc.get_str("collection").unwrap_or_default(),
            "url": doc.get_str("url").unwrap_or_default(),
            "operations": doc
                .get_array("operations")
                .map(|a| a.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                .unwrap_or_default(),
            "createdBy": doc.get_str("createdBy").unwrap_or_default(),
            "createdMs": doc.get_i64("createdMs").unwrap_or_default(),
        })
    }
}

/// What a caller sends to register one.
#[derive(Deserialize)]
pub struct RegisterRequest {
    pub url: String,
    #[serde(default)]
    pub operations: Option<Vec<String>>,
}

/// Ensure the registry collection exists, and hand back its metadata.
fn registry(state: &SharedState) -> Result<kimmy_storage::CollectionMeta, ApiError> {
    match state.engine.get_collection(WEBHOOKS_DB, WEBHOOKS_COLLECTION) {
        Ok(meta) => Ok(meta),
        // `create_system_collection`, not `create_collection`: the `__` prefix
        // is reserved and the ordinary path refuses it ([ADR-017]), which is
        // what stops a user creating something that collides with an internal
        // collection. This is the sanctioned way in, the same one the user
        // store uses.
        //
        // Created on first use rather than at startup, so a node that never
        // registers a webhook never grows the collection.
        Err(_) => Ok(state.engine.create_system_collection(WEBHOOKS_DB, WEBHOOKS_COLLECTION)?),
    }
}

/// A subscription id, unique without coordination.
fn new_id() -> String {
    format!("wh_{}", uuid::Uuid::new_v4().simple())
}

/// A signing secret.
fn new_secret() -> String {
    // Two v4 UUIDs' worth of randomness, hex-encoded: 256 bits from the same
    // CSPRNG the node ids come from, with no new dependency.
    format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple())
}

/// Register a webhook on a collection.
pub fn register(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    request: &RegisterRequest,
    policy: &EgressPolicy,
) -> Result<Value, ApiError> {
    // The `webhook` action, not `watch`: this hands out an ongoing egress path
    // rather than a stream the caller holds. See `Action::Webhook`.
    auth.require(Action::Webhook, db, Some(coll))?;
    // The collection has to exist. Registering against a name that does not
    // would otherwise sit silently, delivering nothing, until someone noticed.
    let _ = state.engine.get_collection(db, coll)?;

    // Checked here so a bad URL fails while the person who typed it is
    // watching. It is checked again before every delivery, because a name that
    // resolves publicly now can resolve inward later.
    policy.check(&request.url).map_err(|e| ApiError::bad_request(e.to_string()))?;

    let operations = match &request.operations {
        Some(names) => {
            names.iter().map(|n| WebhookOperation::parse(n)).collect::<Result<Vec<_>, _>>()?
        }
        None => Vec::new(),
    };

    let subscription = Subscription {
        id: new_id(),
        database: db.to_string(),
        collection: coll.to_string(),
        url: request.url.clone(),
        operations,
        secret: new_secret(),
        created_by: auth.principal().user.clone(),
        created_ms: kimmy_storage::physical_now_ms(),
    };

    let meta = registry(state)?;
    state.engine.insert(&meta, subscription.to_document())?;

    tracing::warn!(
        target: "kimmy::audit",
        user = %subscription.created_by,
        action = "RegisterWebhook",
        db = %db,
        collection = %coll,
        url = %subscription.url,
        subscription = %subscription.id,
        decision = "allow",
        "webhook registered"
    );

    // The secret appears here and nowhere else, ever.
    Ok(json!({
        "id": subscription.id,
        "url": subscription.url,
        "secret": subscription.secret,
        "note": "the secret is shown once and cannot be retrieved later",
    }))
}

/// List the subscriptions on a collection. Secrets are never included.
pub fn list(state: &SharedState, auth: &Auth, db: &str, coll: &str) -> Result<Value, ApiError> {
    auth.require(Action::Webhook, db, Some(coll))?;

    let meta = registry(state)?;
    let mut out = Vec::new();
    state.engine.for_each_doc(&meta, |_id, document| {
        let matches = document.get_str("database").is_ok_and(|d| d == db)
            && document.get_str("collection").is_ok_and(|c| c == coll);
        if matches {
            out.push(Subscription::to_json(&document));
        }
        Ok(true)
    })?;

    Ok(json!({ "webhooks": out, "count": out.len() }))
}

/// Remove a subscription.
pub fn remove(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    id: &str,
) -> Result<Value, ApiError> {
    auth.require(Action::Webhook, db, Some(coll))?;

    let meta = registry(state)?;
    let key = kimmy_core::DocId::String(id.to_string());
    // Read before deleting, so a subscription registered on *another*
    // collection cannot be deleted by guessing its id from a collection the
    // caller happens to hold the grant on.
    let existing = state.engine.get(&meta, &key)?;
    let belongs = existing.as_ref().is_some_and(|d| {
        d.get_str("database").is_ok_and(|x| x == db)
            && d.get_str("collection").is_ok_and(|x| x == coll)
    });
    if !belongs {
        return Err(ApiError::not_found(format!("no webhook {id} on {db}.{coll}")));
    }

    let removed = state.engine.delete(&meta, &key)?;
    tracing::warn!(
        target: "kimmy::audit",
        user = %auth.principal().user,
        action = "RemoveWebhook",
        db = %db,
        collection = %coll,
        subscription = %id,
        decision = "allow",
        "webhook removed"
    );
    Ok(json!({ "removed": removed }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stored_subscription_never_shows_its_secret() {
        // The listing view is built by hand rather than by serialising the
        // stored document, precisely so a field added later cannot leak by
        // default. This is the test that keeps that true.
        let subscription = Subscription {
            id: "wh_1".into(),
            database: "shop".into(),
            collection: "orders".into(),
            url: "https://example.com/hook".into(),
            operations: vec![WebhookOperation::Insert],
            secret: "SUPERSECRETVALUE".into(),
            created_by: "root".into(),
            created_ms: 1,
        };
        let rendered = Subscription::to_json(&subscription.to_document()).to_string();

        assert!(!rendered.contains("SUPERSECRETVALUE"), "the secret leaked: {rendered}");
        assert!(!rendered.contains("secret"), "not even the field name: {rendered}");
        assert!(rendered.contains("wh_1") && rendered.contains("example.com"));
    }

    #[test]
    fn secrets_do_not_repeat() {
        // A shared signing key would let one subscription's receiver forge
        // deliveries for another's.
        let a = new_secret();
        assert_ne!(a, new_secret());
        assert!(a.len() >= 64, "256 bits of hex, got {} chars", a.len());
    }

    #[test]
    fn an_unknown_operation_lists_the_valid_ones() {
        let err = WebhookOperation::parse("upsert").unwrap_err();
        assert!(err.message.contains("upsert"), "{}", err.message);
        assert!(err.message.contains("insert"), "should list valid ones: {}", err.message);
    }
}
