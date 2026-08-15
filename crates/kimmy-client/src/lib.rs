//! The Rust client for KimmyDB.
//!
//! ```no_run
//! # async fn example() -> kimmy_client::Result<()> {
//! use kimmy_client::{Client, Query};
//! use serde_json::json;
//!
//! let client = Client::builder("http://localhost:7878")
//!     .credentials("root", "hunter2")
//!     .discover_nodes(true)
//!     .connect()
//!     .await?;
//!
//! client.insert("shop", "orders", &json!({ "sku": "widget", "qty": 5 })).await?;
//!
//! let mut pages = client.pages("shop", "orders", Query::new().limit(100));
//! while let Some(page) = pages.next().await? {
//!     for document in page {
//!         println!("{document}");
//!     }
//! }
//! # Ok(()) }
//! ```
//!
//! # It is a client of the protocol, not of the server
//!
//! This crate depends on no `kimmy-*` crate, and a test keeps it that way. It
//! sees exactly what the Python and Go clients see: `docs/openapi.yaml` and the
//! bytes on the wire. Sharing a type with the server would let it rely on
//! something the specification never promised — and the first sign of that
//! would be a bug the other two clients have and this one does not.
//!
//! # What it does for you, and what it deliberately does not
//!
//! - **Keeps a token alive.** Built with credentials, it logs in, and it
//!   refreshes before expiry using `expiresIn` rather than by decoding a token
//!   it is told to treat as opaque. Built with a token instead, it uses it
//!   until the server stops accepting it and then says so.
//! - **Fails over between nodes.** Every node accepts writes, so selection is
//!   round-robin plus retry — no primary to find. With `discover_nodes`, the
//!   node list comes from `/v1/topology`.
//! - **Retries only what is safe to retry.** A read is repeated on another node
//!   when the failure says `elsewhere`. **A write is not**, unless the caller
//!   says it is idempotent: an insert that failed after the commit but before
//!   the answer arrived would be applied twice by a helpful retry, and no
//!   status code distinguishes that from one that never landed.
//! - **Pages with cursors**, which is the difference between reading a
//!   collection and reading its first hundred documents.
//! - **Resumes change streams.** A dropped socket reconnects from the last
//!   resume token seen, and tokens are portable, so the reconnect may land on
//!   a different node.

mod error;
mod page;
mod watch;

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::sync::RwLock;

pub use error::{Error, ErrorCode, Result, Retry};

/// An HTTP method, for [`Client::request`].
///
/// This crate's own rather than `reqwest`'s. Converting `kimmy-cli` is what
/// showed why: taking `reqwest::Method` made every consumer of this library
/// depend on `reqwest` to name a verb, which puts the HTTP stack in the public
/// API and makes changing it a breaking change for everyone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Put,
    Delete,
}

impl From<Method> for reqwest::Method {
    fn from(method: Method) -> Self {
        match method {
            Method::Get => reqwest::Method::GET,
            Method::Post => reqwest::Method::POST,
            Method::Put => reqwest::Method::PUT,
            Method::Delete => reqwest::Method::DELETE,
        }
    }
}
pub use page::{Pages, Query};
pub use watch::{ChangeEvent, ChangeStream, WatchOptions};

/// How long before a token expires the client renews it.
///
/// Not zero, because a token that expires between the check and the server
/// reading it is a request that fails for a reason the client could have
/// avoided; and not minutes, because it would spend most of a short lifetime
/// refreshing.
const RENEW_BEFORE: Duration = Duration::from_secs(60);

/// Whether a request may be repeated after a failure.
///
/// The distinction the protocol cannot make for you: `retry: elsewhere` says
/// *this node* could not answer, not that the work did not happen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Safety {
    /// Repeating it cannot change the outcome — reads, and writes whose effect
    /// is the same applied twice.
    Idempotent,
    /// Repeating it might apply the work twice. Failures are returned to the
    /// caller, who knows what the request meant.
    Unsafe,
}

struct Session {
    token: String,
    /// When to renew, not when it expires — computed once, from `expiresIn`.
    renew_at: Instant,
}

struct Inner {
    http: reqwest::Client,
    /// Node endpoints, this client's own first. Rotated on failover.
    endpoints: RwLock<Vec<String>>,
    credentials: Option<(String, String)>,
    session: RwLock<Option<Session>>,
}

/// A connection to a KimmyDB cluster.
///
/// Cheap to clone: everything is shared, including the token, so cloning does
/// not double the logins.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

/// Assembles a [`Client`].
pub struct Builder {
    endpoints: Vec<String>,
    credentials: Option<(String, String)>,
    token: Option<String>,
    discover: bool,
    timeout: Duration,
    accept_invalid_certs: bool,
}

impl Client {
    /// Start building a client against one node.
    ///
    /// One address is all a client needs: the rest of the cluster comes from
    /// `/v1/topology` when `discover_nodes` is on.
    pub fn builder(endpoint: impl Into<String>) -> Builder {
        Builder {
            endpoints: vec![normalize(endpoint.into())],
            credentials: None,
            token: None,
            discover: false,
            timeout: Duration::from_secs(30),
            accept_invalid_certs: false,
        }
    }

    /// The endpoints this client will use, in the order it will try them.
    pub async fn endpoints(&self) -> Vec<String> {
        self.inner.endpoints.read().await.clone()
    }

    /// The token in use, if there is one.
    pub async fn token(&self) -> Option<String> {
        self.inner.session.read().await.as_ref().map(|s| s.token.clone())
    }

    // -----------------------------------------------------------------------
    // The node itself
    // -----------------------------------------------------------------------

    /// What this node is and what it can do.
    ///
    /// Ask before assuming a feature exists: in a cluster mid-upgrade the node
    /// answering the next request may be older than this one.
    pub async fn version(&self) -> Result<Value> {
        self.get("/v1/version").await
    }

    /// Whether the node that answered has a named capability.
    pub async fn has_capability(&self, capability: &str) -> Result<bool> {
        let version = self.version().await?;
        Ok(version["capabilities"]
            .as_array()
            .is_some_and(|list| list.iter().any(|c| c.as_str() == Some(capability))))
    }

    /// The nodes this one knows about.
    pub async fn topology(&self) -> Result<Value> {
        self.get("/v1/topology").await
    }

    /// Re-read the cluster's node list and adopt it.
    ///
    /// Entries with no advertised endpoint are skipped — a node that has not
    /// been told what to advertise cannot be dialled — and so are entries whose
    /// status is not `live`, since the point of the list is somewhere to go
    /// *now*. The current endpoint stays first either way.
    pub async fn refresh_topology(&self) -> Result<Vec<String>> {
        let body = self.topology().await?;
        let mut discovered: Vec<String> = body["nodes"]
            .as_array()
            .map(|nodes| {
                nodes
                    .iter()
                    .filter(|n| n["status"].as_str() == Some("live"))
                    .filter_map(|n| n["endpoint"].as_str())
                    .map(|e| normalize(e.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let mut endpoints = self.inner.endpoints.write().await;
        if let Some(current) = endpoints.first().cloned() {
            discovered.retain(|e| *e != current);
            discovered.insert(0, current);
        }
        if !discovered.is_empty() {
            *endpoints = discovered;
        }
        Ok(endpoints.clone())
    }

    // -----------------------------------------------------------------------
    // Documents
    // -----------------------------------------------------------------------

    /// Insert one document.
    ///
    /// **Not retried automatically.** An insert whose answer was lost may have
    /// landed, and repeating it would insert a second document with a new
    /// `_id`. Give the document an `_id` and retry it yourself if you want
    /// that: a repeat then fails with `duplicate_key`, which is a fact rather
    /// than a guess.
    pub async fn insert(&self, db: &str, collection: &str, document: &Value) -> Result<Value> {
        self.send(
            reqwest::Method::POST,
            &format!("/v1/db/{db}/coll/{collection}/docs"),
            Some(document.clone()),
            Safety::Unsafe,
        )
        .await
    }

    /// Insert many documents in one commit — all of them, or none.
    pub async fn insert_many(
        &self,
        db: &str,
        collection: &str,
        documents: &[Value],
    ) -> Result<Value> {
        self.send(
            reqwest::Method::POST,
            &format!("/v1/db/{db}/coll/{collection}/bulk"),
            Some(json!(documents)),
            Safety::Unsafe,
        )
        .await
    }

    /// One document by `_id`, or `None` when there is none.
    ///
    /// A missing document is not an error here: asking whether something exists
    /// is an ordinary thing to do, and making the caller match on a status to
    /// find out is how `not_found` ends up being treated as a failure.
    pub async fn get_document(
        &self,
        db: &str,
        collection: &str,
        id: &str,
    ) -> Result<Option<Value>> {
        match self.get(&format!("/v1/db/{db}/coll/{collection}/docs/{id}")).await {
            Ok(document) => Ok(Some(document)),
            Err(e) if e.code() == Some(ErrorCode::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// One page of a query.
    pub async fn find(&self, db: &str, collection: &str, query: &Query) -> Result<Value> {
        self.send(
            reqwest::Method::POST,
            &format!("/v1/db/{db}/coll/{collection}/find"),
            Some(query.to_body()),
            Safety::Idempotent,
        )
        .await
    }

    /// Walk a collection page by page.
    ///
    /// The whole reason the client exists rather than a `find` call: a `find`
    /// with no limit returns 100 documents and says nothing about the rest.
    pub fn pages(&self, db: &str, collection: &str, query: Query) -> Pages {
        Pages::new(self.clone(), db.to_string(), collection.to_string(), query)
    }

    /// How many documents match. No page cap — a count sees everything.
    pub async fn count(&self, db: &str, collection: &str, filter: &Value) -> Result<u64> {
        let body = self
            .send(
                reqwest::Method::POST,
                &format!("/v1/db/{db}/coll/{collection}/count"),
                Some(json!({ "filter": filter })),
                Safety::Idempotent,
            )
            .await?;
        match body["count"].as_u64() {
            Some(count) => Ok(count),
            None => Err(Error::Protocol {
                endpoint: self.primary().await,
                detail: "count did not return a number".into(),
            }),
        }
    }

    pub async fn update(
        &self,
        db: &str,
        collection: &str,
        filter: &Value,
        update: &Value,
        multi: bool,
    ) -> Result<Value> {
        self.send(
            reqwest::Method::POST,
            &format!("/v1/db/{db}/coll/{collection}/update"),
            Some(json!({ "filter": filter, "update": update, "multi": multi })),
            Safety::Unsafe,
        )
        .await
    }

    pub async fn delete(
        &self,
        db: &str,
        collection: &str,
        filter: &Value,
        multi: bool,
    ) -> Result<Value> {
        self.send(
            reqwest::Method::POST,
            &format!("/v1/db/{db}/coll/{collection}/delete"),
            Some(json!({ "filter": filter, "multi": multi })),
            Safety::Unsafe,
        )
        .await
    }

    pub async fn aggregate(&self, db: &str, collection: &str, pipeline: &Value) -> Result<Value> {
        self.send(
            reqwest::Method::POST,
            &format!("/v1/db/{db}/coll/{collection}/aggregate"),
            Some(json!({ "pipeline": pipeline })),
            Safety::Idempotent,
        )
        .await
    }

    // -----------------------------------------------------------------------
    // Change streams
    // -----------------------------------------------------------------------

    /// Open a change stream over WebSocket.
    ///
    /// Reconnects on its own, resuming from the last token it saw.
    pub async fn watch(
        &self,
        db: &str,
        collection: &str,
        options: WatchOptions,
    ) -> Result<ChangeStream> {
        ChangeStream::open(self.clone(), db.to_string(), collection.to_string(), options).await
    }

    // -----------------------------------------------------------------------
    // The escape hatch
    // -----------------------------------------------------------------------

    /// Any route, by path.
    ///
    /// Present because a client that covers a subset of an API and cannot reach
    /// the rest sends people back to `curl` for one call. Everything above is a
    /// convenience over this.
    pub async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        safety: Safety,
    ) -> Result<Value> {
        self.send(method.into(), path, body, safety).await
    }

    /// Raw bytes, for the routes that are not JSON — the backup.
    pub async fn download(&self, path: &str) -> Result<Vec<u8>> {
        self.authenticate().await?;
        let endpoint = self.primary().await;
        let token = self.token().await;
        let mut builder = self.inner.http.get(format!("{endpoint}{path}"));
        if let Some(token) = token {
            builder = builder.bearer_auth(token);
        }
        let response = builder
            .send()
            .await
            .map_err(|source| Error::Transport { endpoint: endpoint.clone(), source })?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|source| Error::Transport { endpoint: endpoint.clone(), source })?;
        if !status.is_success() {
            let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            return Err(error::from_response(status.as_u16(), None, &body));
        }
        Ok(bytes.to_vec())
    }

    async fn get(&self, path: &str) -> Result<Value> {
        self.send(reqwest::Method::GET, path, None, Safety::Idempotent).await
    }

    /// The document routes, for callers that need a verb this crate does not
    /// wrap in a named method.
    pub async fn replace_document(
        &self,
        db: &str,
        collection: &str,
        id: &str,
        document: &Value,
        upsert: bool,
    ) -> Result<Value> {
        let query = if upsert { "?upsert=true" } else { "" };
        self.send(
            reqwest::Method::PUT,
            &format!("/v1/db/{db}/coll/{collection}/docs/{id}{query}"),
            Some(document.clone()),
            // Replacing a document by `_id` with a whole body *is* idempotent:
            // applying it twice leaves the same document. Unlike an insert,
            // which invents an `_id` when none is given.
            Safety::Idempotent,
        )
        .await
    }

    /// Delete one document by `_id`.
    pub async fn delete_document(&self, db: &str, collection: &str, id: &str) -> Result<Value> {
        self.send(
            reqwest::Method::DELETE,
            &format!("/v1/db/{db}/coll/{collection}/docs/{id}"),
            None,
            Safety::Idempotent,
        )
        .await
    }

    pub(crate) async fn primary(&self) -> String {
        self.inner.endpoints.read().await.first().cloned().unwrap_or_default()
    }

    // -----------------------------------------------------------------------
    // Authentication
    // -----------------------------------------------------------------------

    /// Ensure there is a usable token, logging in or refreshing as needed.
    ///
    /// Refresh is preferred over a fresh login because it costs no password
    /// verification — the login limiter exists to bound Argon2 work — and
    /// because an application that stored credentials should be able to forget
    /// them for as long as it stays connected.
    async fn authenticate(&self) -> Result<()> {
        let needs = {
            let session = self.inner.session.read().await;
            match session.as_ref() {
                None => self.inner.credentials.is_some(),
                Some(s) => Instant::now() >= s.renew_at,
            }
        };
        if !needs {
            return Ok(());
        }

        let held = self.token().await;
        if held.is_some() {
            // A refresh that fails is not fatal: the token may still be good,
            // and if it is not, the next request says so with the server's own
            // reason rather than one invented here.
            if self.refresh().await.is_ok() {
                return Ok(());
            }
        }
        if self.inner.credentials.is_some() {
            return self.login().await;
        }
        Ok(())
    }

    async fn login(&self) -> Result<()> {
        let Some((user, password)) = &self.inner.credentials else {
            return Err(Error::NotAuthenticated);
        };
        let body = self
            .send_any(
                reqwest::Method::POST,
                "/v1/auth/login",
                Some(json!({ "user": user, "password": password })),
                None,
            )
            .await?;
        self.adopt(body).await
    }

    async fn refresh(&self) -> Result<()> {
        let token = self.token().await;
        let body = self
            .send_any(reqwest::Method::POST, "/v1/auth/refresh", None, token.as_deref())
            .await?;
        self.adopt(body).await
    }

    /// Take a token from a login or refresh response.
    async fn adopt(&self, body: Value) -> Result<()> {
        let endpoint = self.primary().await;
        let token = body["token"].as_str().ok_or_else(|| Error::Protocol {
            endpoint: endpoint.clone(),
            detail: "no token in the response".into(),
        })?;
        // `expiresIn` rather than decoding the token: it is opaque, and a
        // client that parses one depends on a shape nothing promised it.
        let lifetime = Duration::from_secs(body["expiresIn"].as_u64().unwrap_or(3600));
        let renew_in = lifetime.saturating_sub(RENEW_BEFORE).max(Duration::from_secs(1));

        *self.inner.session.write().await =
            Some(Session { token: token.to_string(), renew_at: Instant::now() + renew_in });
        Ok(())
    }

    // -----------------------------------------------------------------------
    // The request path
    // -----------------------------------------------------------------------

    /// Send a request, renewing the token and failing over as the failure and
    /// the request's safety allow.
    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
        safety: Safety,
    ) -> Result<Value> {
        self.authenticate().await?;

        let endpoints = self.inner.endpoints.read().await.clone();
        let mut tried = Vec::new();
        let mut last: Option<Error> = None;
        let mut relogged = false;

        for endpoint in &endpoints {
            tried.push(endpoint.clone());
            // Per endpoint, because the bound is on how long *this* node is
            // given to recover; the next one has said nothing yet.
            let mut waited = false;
            loop {
                let token = self.token().await;
                let result = self
                    .send_to(endpoint, method.clone(), path, body.clone(), token.as_deref())
                    .await;

                let error = match result {
                    Ok(value) => {
                        // The node that answered goes to the front, so the next
                        // request starts where the last one succeeded rather
                        // than re-walking the dead ones.
                        self.promote(endpoint).await;
                        return Ok(value);
                    }
                    Err(e) => e,
                };

                // A token that the server has stopped accepting: log in again
                // once, in case it merely expired or was revoked by a change
                // this client can recover from. Once, because a loop here is
                // how a client hammers a login endpoint forever.
                if error.is_unauthorized() && !relogged && self.inner.credentials.is_some() {
                    relogged = true;
                    if self.login().await.is_ok() {
                        continue;
                    }
                }

                match error.retry() {
                    // The same node, after the delay it named — which is what
                    // separates `wait` from `elsewhere`. `wait` says *this*
                    // node will serve the request shortly, so failing over
                    // abandons the one node that told you how long to wait,
                    // and with a single endpoint there is nowhere to go at all.
                    //
                    // Bounded to one wait per endpoint: a client that sleeps
                    // repeatedly on a rate limit is an application that has
                    // stopped responding. A second refusal falls through to the
                    // next node, since this one has now said no twice.
                    Retry::Wait if safety == Safety::Idempotent && !waited => {
                        let Error::Api { retry_after, .. } = &error else { break };
                        let delay = Duration::from_secs(retry_after.unwrap_or(1).min(30));
                        tokio::time::sleep(delay).await;
                        waited = true;
                        last = Some(error);
                        continue;
                    }
                    Retry::Wait if safety == Safety::Idempotent => {
                        last = Some(error);
                        break;
                    }
                    Retry::Elsewhere if safety == Safety::Idempotent => {
                        last = Some(error);
                        break;
                    }
                    // Either nothing to retry, or a write, which is the
                    // caller's to decide about. Returning the server's own
                    // error is more useful than a client-invented one.
                    _ => return Err(error),
                }
            }
        }

        Err(last.unwrap_or(Error::NoNodeAvailable { tried }))
    }

    /// A request that must reach *some* node, tried against each in turn.
    ///
    /// Used by login and refresh, and it has to fail over: a client handed a
    /// list whose first address is dead could otherwise not authenticate at
    /// all, which is the one failure that makes every other endpoint useless.
    /// Found by a test that put a dead address in front of a live one — the
    /// situation a client meets whenever a node stops.
    ///
    /// Only transport failures move on. A *refusal* is the same everywhere:
    /// one cluster, one signing secret, one user store, so a password the
    /// cluster rejects is not worth asking three nodes about.
    async fn send_any(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
        token: Option<&str>,
    ) -> Result<Value> {
        let endpoints = self.inner.endpoints.read().await.clone();
        let mut tried = Vec::new();
        let mut last = None;

        for endpoint in &endpoints {
            tried.push(endpoint.clone());
            match self.send_to(endpoint, method.clone(), path, body.clone(), token).await {
                Ok(value) => {
                    self.promote(endpoint).await;
                    return Ok(value);
                }
                Err(e @ Error::Transport { .. }) => last = Some(e),
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or(Error::NoNodeAvailable { tried }))
    }

    async fn send_to(
        &self,
        endpoint: &str,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
        token: Option<&str>,
    ) -> Result<Value> {
        let mut builder = self.inner.http.request(method, format!("{endpoint}{path}"));
        if let Some(token) = token {
            builder = builder.bearer_auth(token);
        }
        if let Some(body) = body {
            builder = builder.json(&body);
        }

        let response = builder
            .send()
            .await
            .map_err(|source| Error::Transport { endpoint: endpoint.to_string(), source })?;

        let status = response.status();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());
        let text = response
            .text()
            .await
            .map_err(|source| Error::Transport { endpoint: endpoint.to_string(), source })?;

        if !status.is_success() {
            let body = serde_json::from_str(&text).unwrap_or(Value::Null);
            return Err(error::from_response(status.as_u16(), retry_after, &body));
        }
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text).map_err(|e| Error::Protocol {
            endpoint: endpoint.to_string(),
            detail: format!("the body is not JSON: {e}"),
        })
    }

    /// Move an endpoint to the front of the list.
    async fn promote(&self, endpoint: &str) {
        let mut endpoints = self.inner.endpoints.write().await;
        if endpoints.first().map(String::as_str) == Some(endpoint) {
            return;
        }
        endpoints.retain(|e| e != endpoint);
        endpoints.insert(0, endpoint.to_string());
    }
}

impl Builder {
    /// Log in with these credentials, and keep the token renewed.
    pub fn credentials(mut self, user: impl Into<String>, password: impl Into<String>) -> Self {
        self.credentials = Some((user.into(), password.into()));
        self
    }

    /// Use a token that was obtained elsewhere.
    ///
    /// Without credentials there is nothing to log in with, so when the token
    /// stops being accepted the client says so rather than recovering.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Additional endpoints to try, before any are discovered.
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoints.push(normalize(endpoint.into()));
        self
    }

    /// Learn the rest of the cluster from `/v1/topology` at connect time.
    pub fn discover_nodes(mut self, discover: bool) -> Self {
        self.discover = discover;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Accept any certificate. For a test against a self-signed node, and
    /// named so that it cannot be enabled without saying what it is.
    pub fn danger_accept_invalid_certs(mut self, accept: bool) -> Self {
        self.accept_invalid_certs = accept;
        self
    }

    /// Build the client, logging in if it was given credentials.
    pub async fn connect(self) -> Result<Client> {
        let http = reqwest::Client::builder()
            .timeout(self.timeout)
            .danger_accept_invalid_certs(self.accept_invalid_certs)
            .build()
            .map_err(|source| Error::Transport { endpoint: self.endpoints[0].clone(), source })?;

        let session = self.token.map(|token| Session {
            token,
            // A supplied token has no stated lifetime, so nothing is renewed on
            // a schedule. If it expires, the server says so and the client
            // reports it — which is the honest outcome for a credential this
            // client did not obtain and cannot obtain again.
            renew_at: Instant::now() + Duration::from_secs(86_400 * 365),
        });

        let client = Client {
            inner: Arc::new(Inner {
                http,
                endpoints: RwLock::new(self.endpoints),
                credentials: self.credentials,
                session: RwLock::new(session),
            }),
        };

        client.authenticate().await?;
        if self.discover {
            client.refresh_topology().await?;
        }
        Ok(client)
    }
}

/// Trim a trailing slash, so `{endpoint}{path}` never doubles one.
fn normalize(endpoint: String) -> String {
    endpoint.trim_end_matches('/').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_endpoint_keeps_one_slash_between_it_and_a_path() {
        assert_eq!(normalize("http://localhost:7878/".into()), "http://localhost:7878");
        assert_eq!(normalize("http://localhost:7878".into()), "http://localhost:7878");
    }

    #[test]
    fn the_shipped_crate_depends_on_no_kimmy_crate() {
        // The property this client's usefulness as a *check* rests on: it sees
        // what the Python and Go clients see. A shared type would let it rely
        // on something the specification never promised, and the first sign
        // would be a bug the other two have and this one does not.
        const MANIFEST: &str = include_str!("../Cargo.toml");
        let shipped = MANIFEST
            .split("[dev-dependencies]")
            .next()
            .expect("the manifest has a dependencies section");
        assert!(
            !shipped.contains("\nkimmy-"),
            "kimmy-client has grown a dependency on a server crate:\n{shipped}"
        );
    }
}
