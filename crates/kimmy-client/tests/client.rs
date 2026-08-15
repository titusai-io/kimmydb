//! The client, against a real server on a real socket.
//!
//! Nothing here calls a handler directly. The client's whole job is to be
//! correct about what comes back over a connection, so a test that skipped the
//! connection would be testing the half that was never in doubt.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::response::IntoResponse;
use kimmy_client::{Client, ErrorCode, Method, Query, Retry, Safety, WatchOptions};
use kimmy_storage::Engine;
use serde_json::{Value, json};

const SECRET: &str = "an-adequately-long-test-secret";
const ROOT_PASSWORD: &str = "root-password";

struct Server {
    base: String,
    /// The server's own state, for the few tests whose subject is something a
    /// request cannot reach — the live member set, which exists only once a
    /// cluster has started.
    state: kimmy_api::SharedState,
    _dir: tempfile::TempDir,
}

impl Server {
    async fn start() -> Self {
        Self::with_ttl(3600).await
    }

    /// A server whose tokens expire after `ttl` seconds.
    async fn with_ttl(ttl: u64) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let users = kimmy_auth::UserStore::open(&engine).unwrap();
        users.bootstrap_root(&engine, "root", ROOT_PASSWORD).unwrap();

        let tokens = kimmy_auth::TokenIssuer::new(SECRET, ttl).unwrap();
        let state =
            kimmy_api::state(Arc::clone(&engine), tokens, false, kimmy_api::RateLimits::disabled())
                .unwrap();
        let app = kimmy_api::router(Arc::clone(&state));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
                .await;
        });

        Self { base: format!("http://{addr}"), state, _dir: dir }
    }

    /// Write a peer's record into the node registry directly.
    ///
    /// Standing in for the replication that would carry it in a real cluster:
    /// the registry is an ordinary collection, so a replicated record and a
    /// locally written one are the same thing by the time topology reads it.
    async fn register_peer(&self, node: &kimmy_core::NodeId, endpoint: &str) {
        let meta = self
            .state
            .engine
            .create_system_collection(
                kimmy_api::topology::NODES_DB,
                kimmy_api::topology::NODES_COLLECTION,
            )
            .unwrap();
        self.state
            .engine
            .insert(
                &meta,
                bson::doc! {
                    "_id": node.to_string(),
                    "endpoint": endpoint,
                    "version": "0.0.1-peer",
                    "updatedMs": 0i64,
                },
            )
            .unwrap();
    }
}

/// A server that answers `429` a fixed number of times, then succeeds.
///
/// Not `kimmy_api::router`: a real node's rate limiter bounds *login*, so no
/// arrangement of it makes an ordinary request answer `rate_limited` on
/// demand. The `wait` arm of the retry taxonomy (ADR-057) is public surface a
/// caller depends on, and it had no test that reached it at all — the M10
/// mutation pass found four surviving mutants sitting in this one branch.
///
/// It counts the requests it received, which is the observation that separates
/// `wait` from `elsewhere`: the question is not only whether the call
/// eventually succeeds but *which node* it succeeded on.
struct Stalling {
    base: String,
    hits: Arc<AtomicUsize>,
}

impl Stalling {
    /// Refuse the first `refusals` requests with `429`, then answer them.
    ///
    /// `Retry-After: 0` because the client honours the header and a test that
    /// slept for the default second per attempt would pay for nothing: the
    /// subject is which endpoint the retry goes to, not the arithmetic.
    async fn start(refusals: usize) -> Self {
        let hits = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&hits);

        let app = axum::Router::new().fallback(axum::routing::any(move || {
            let seen = Arc::clone(&seen);
            async move {
                let n = seen.fetch_add(1, Ordering::SeqCst);
                if n < refusals {
                    (
                        axum::http::StatusCode::TOO_MANY_REQUESTS,
                        [(axum::http::header::RETRY_AFTER, "0")],
                        axum::Json(json!({
                            "error": "rate_limited",
                            "message": "slow down",
                            "retry": "wait",
                        })),
                    )
                        .into_response()
                } else {
                    axum::Json(json!({ "documents": [], "served_by": "stalling" })).into_response()
                }
            }
        }));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Self { base: format!("http://{addr}"), hits }
    }

    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
}

/// A client holding a token, so no stub has to serve the auth routes.
///
/// A supplied token is never renewed on a schedule, so `authenticate` makes no
/// request and every hit a stub counts is the request under test.
async fn with_token(endpoints: &[&str]) -> Client {
    let mut builder = Client::builder(endpoints[0]).token("opaque-to-this-client");
    for extra in &endpoints[1..] {
        builder = builder.endpoint(*extra);
    }
    builder.connect().await.expect("connecting")
}

async fn connected() -> (Server, Client) {
    let server = Server::start().await;
    let client = Client::builder(&server.base)
        .credentials("root", ROOT_PASSWORD)
        .connect()
        .await
        .expect("connecting");
    (server, client)
}

async fn seeded(client: &Client, n: i64) {
    client
        .request(
            Method::Post,
            "/v1/db/shop/collections",
            Some(json!({ "name": "orders" })),
            Safety::Unsafe,
        )
        .await
        .expect("creating the collection");

    let documents: Vec<Value> =
        (0..n).map(|i| json!({ "_id": i, "sku": format!("s{}", i % 3), "qty": i })).collect();
    client.insert_many("shop", "orders", &documents).await.expect("seeding");
}

#[tokio::test]
async fn a_client_built_with_credentials_holds_a_token() {
    let (_server, client) = connected().await;
    assert!(client.token().await.is_some(), "connecting logs in");

    // And the token works, which is the only claim that matters about it.
    let databases = client
        .request(Method::Get, "/v1/databases", None, Safety::Idempotent)
        .await
        .expect("an authenticated request");
    assert!(databases["databases"].is_array());
}

#[tokio::test]
async fn documents_round_trip() {
    let (_server, client) = connected().await;
    seeded(&client, 5).await;

    let one = client.get_document("shop", "orders", "3").await.unwrap();
    assert_eq!(one.expect("a document")["qty"], 3);

    // A missing document is `None`, not an error: asking whether something
    // exists is an ordinary thing to do.
    assert!(client.get_document("shop", "orders", "999").await.unwrap().is_none());

    assert_eq!(client.count("shop", "orders", &json!({})).await.unwrap(), 5);
}

#[tokio::test]
async fn paging_walks_the_whole_collection() {
    // The reason the client exists rather than a `find` call: an unlimited
    // `find` returns 100 documents and says nothing about the rest.
    let (_server, client) = connected().await;
    seeded(&client, 250).await;

    let mut pages = client.pages("shop", "orders", Query::new().limit(50));
    let mut seen = Vec::new();
    let mut count = 0;
    while let Some(page) = pages.next().await.unwrap() {
        count += 1;
        seen.extend(page.iter().map(|d| d["_id"].as_i64().unwrap()));
    }

    assert_eq!(seen.len(), 250, "the walk saw {} documents in {count} pages", seen.len());
    assert_eq!(seen, (0..250).collect::<Vec<_>>(), "in order, exactly once each");
}

#[tokio::test]
async fn a_walk_ends_on_an_empty_page_not_a_missing_token() {
    // A collection whose size is an exact multiple of the page size: the last
    // full page still carries a token, so a loop that stopped when the token
    // stopped arriving would read one page too few. The client handles it;
    // this is the test that says so.
    let (_server, client) = connected().await;
    seeded(&client, 100).await;

    let mut pages = client.pages("shop", "orders", Query::new().limit(100));
    let first = pages.next().await.unwrap().expect("a full page");
    assert_eq!(first.len(), 100);
    assert!(pages.next().await.unwrap().is_none(), "the walk ends here, without an error");
}

#[tokio::test]
async fn a_query_a_cursor_cannot_page_is_refused_before_the_walk() {
    let (_server, client) = connected().await;
    seeded(&client, 10).await;

    let mut pages = client.pages("shop", "orders", Query::new().sort(json!({ "qty": 1 })));
    let err = pages.next().await.expect_err("a sorted walk cannot be paged");
    assert!(err.to_string().contains("_id order"), "{err}");
}

#[tokio::test]
async fn a_refusal_arrives_typed() {
    let (_server, client) = connected().await;
    seeded(&client, 1).await;

    let err =
        client.insert("shop", "orders", &json!({ "_id": 0 })).await.expect_err("a duplicate _id");
    assert_eq!(err.code(), Some(ErrorCode::DuplicateKey));
    assert_eq!(err.retry(), Retry::No, "a duplicate does not become un-duplicate");
    assert_eq!(err.status(), Some(409));
}

#[tokio::test]
async fn a_client_with_a_bad_token_and_no_credentials_says_so() {
    let server = Server::start().await;
    let client = Client::builder(&server.base).token("not-a-token").connect().await.unwrap();

    let err = client
        .request(Method::Get, "/v1/databases", None, Safety::Idempotent)
        .await
        .expect_err("a bad token");
    assert!(err.is_unauthorized(), "{err}");
    assert_eq!(err.retry(), Retry::No, "there is nothing to retry without credentials");
}

#[tokio::test]
async fn an_expired_token_is_replaced_without_the_caller_noticing() {
    // The point of holding credentials. A one-second lifetime makes the
    // renewal happen on the second request rather than in an hour.
    let server = Server::with_ttl(1).await;
    let client =
        Client::builder(&server.base).credentials("root", ROOT_PASSWORD).connect().await.unwrap();
    let first = client.token().await.expect("a token");

    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;

    let databases = client
        .request(Method::Get, "/v1/databases", None, Safety::Idempotent)
        .await
        .expect("the client recovers from its own token expiring");
    assert!(databases["databases"].is_array());
    assert_ne!(client.token().await.expect("a token"), first, "it got a new one");
}

#[tokio::test]
async fn an_unreachable_node_is_skipped_for_one_that_answers() {
    // Failover, without a cluster: a dead address in front of a live one is
    // the same situation as a node that stopped, and it is the one worth
    // testing because a client that dies on the first bad endpoint is useless
    // exactly when it is needed.
    let server = Server::start().await;
    // Port 1 is reserved and nothing listens there.
    let client = Client::builder("http://127.0.0.1:1")
        .endpoint(&server.base)
        .credentials("root", ROOT_PASSWORD)
        .connect()
        .await;

    // Connecting logs in against the first endpoint, which is dead — so this
    // is a client that must survive its own construction failing over.
    let client = match client {
        Ok(client) => client,
        Err(e) => panic!("the builder gave up on a dead first endpoint: {e}"),
    };

    let databases = client
        .request(Method::Get, "/v1/databases", None, Safety::Idempotent)
        .await
        .expect("the live node answers");
    assert!(databases["databases"].is_array());

    // And the node that answered is now first, so the next request does not
    // re-walk the dead one.
    assert_eq!(client.endpoints().await.first().unwrap(), &server.base);
}

#[tokio::test]
async fn a_write_is_not_retried_elsewhere_automatically() {
    // The distinction the protocol cannot make: `elsewhere` says this node
    // could not answer, not that the work did not happen. A helpful retry of
    // an insert would apply it twice, and no status distinguishes that from
    // one that never landed.
    let server = Server::start().await;
    let client =
        Client::builder(&server.base).credentials("root", ROOT_PASSWORD).connect().await.unwrap();
    seeded(&client, 1).await;

    // A write to a node that cannot answer at all: the client reports it
    // rather than moving on with a request it cannot know the fate of.
    let dead = Client::builder("http://127.0.0.1:1")
        .endpoint(&server.base)
        .token(client.token().await.unwrap())
        .connect()
        .await
        .unwrap();

    let err = dead
        .insert("shop", "orders", &json!({ "_id": 99 }))
        .await
        .expect_err("an unsafe request is not moved to another node");
    assert_eq!(err.retry(), Retry::Elsewhere, "the advice is there for the caller: {err}");

    // The caller decides — and here it can, because the document carries an
    // `_id`, so a repeat is a fact rather than a guess.
    let ok = dead
        .request(
            Method::Post,
            "/v1/db/shop/coll/orders/docs",
            Some(json!({ "_id": 99 })),
            Safety::Idempotent,
        )
        .await
        .expect("declared idempotent, so it moves to the live node");
    assert!(ok["insertedId"].is_number() || ok["insertedId"].is_string());
}

#[tokio::test]
async fn topology_and_capabilities_come_back() {
    let (server, client) = connected().await;

    let version = client.version().await.unwrap();
    assert_eq!(version["protocol"], "v1");
    assert!(client.has_capability("cursor-paging").await.unwrap());
    assert!(!client.has_capability("a-capability-nobody-has").await.unwrap());

    // A single node with no advertised endpoint still lists itself, so
    // discovery cannot leave a client with nowhere to go.
    let topology = client.topology().await.unwrap();
    assert_eq!(topology["count"], 1);
    let endpoints = client.refresh_topology().await.unwrap();
    assert_eq!(endpoints, vec![server.base.clone()], "an unadvertised node is not dialled");
}

#[tokio::test]
async fn a_change_stream_delivers_and_carries_a_resume_token() {
    let (_server, client) = connected().await;
    seeded(&client, 1).await;

    let mut stream = client
        .watch("shop", "orders", WatchOptions::new().full_document(true))
        .await
        .expect("opening a change stream");

    let writer = client.clone();
    tokio::spawn(async move {
        for id in 100..103 {
            let _ = writer.insert("shop", "orders", &json!({ "_id": id, "sku": "x" })).await;
        }
    });

    let mut seen = Vec::new();
    for _ in 0..3 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("an event within five seconds")
            .expect("the stream is live")
            .expect("an event");
        assert_eq!(event.operation, "insert");
        assert!(event.full_document().is_some(), "full_document was asked for");
        seen.push(event.document_id().cloned());
    }

    assert_eq!(seen.len(), 3);
    assert!(stream.resume_token().is_some(), "the stream knows where it got to");
}

#[tokio::test]
async fn a_dropped_collection_ends_the_stream() {
    // Until the server invalidated on a drop, this hung: no event, no close,
    // no error, and a collection recreated under the same name would silently
    // adopt the stream. The client's job is to surface the end rather than
    // reconnect into nothing, which is what `is_invalidate` is for.
    let (_server, client) = connected().await;
    seeded(&client, 1).await;

    let mut stream = client.watch("shop", "orders", WatchOptions::new()).await.unwrap();

    client
        .request(Method::Delete, "/v1/db/shop/coll/orders", None, Safety::Idempotent)
        .await
        .expect("dropping the collection");

    let event = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
        .await
        .expect("a dropped collection ends the stream rather than stalling it")
        .expect("an event")
        .expect("the invalidate");
    assert!(event.is_invalidate(), "{event:?}");
    assert_eq!(event.raw["reason"], "CollectionDropped");

    // And it is the end: nothing after it, and no reconnection attempt.
    assert!(stream.next().await.unwrap().is_none());
}

#[tokio::test]
async fn a_change_stream_resumes_from_where_it_stopped() {
    // What makes reconnection safe: a token is portable and carries no server
    // state, so a second stream started from it sees what the first missed
    // rather than everything since the beginning.
    let (_server, client) = connected().await;
    seeded(&client, 1).await;

    let mut first = client.watch("shop", "orders", WatchOptions::new()).await.unwrap();
    client.insert("shop", "orders", &json!({ "_id": 200 })).await.unwrap();
    let event = tokio::time::timeout(std::time::Duration::from_secs(5), first.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let token = event.resume_token.clone().expect("a resume token");
    first.close().await;

    // Written while nothing is listening.
    client.insert("shop", "orders", &json!({ "_id": 201 })).await.unwrap();

    let mut resumed =
        client.watch("shop", "orders", WatchOptions::new().resume_after(token)).await.unwrap();
    let missed = tokio::time::timeout(std::time::Duration::from_secs(5), resumed.next())
        .await
        .expect("the write made while disconnected is delivered")
        .unwrap()
        .unwrap();
    assert_eq!(missed.document_id().and_then(Value::as_i64), Some(201));
}

/// The convenience methods, which the mutation pass found untested.
///
/// `find`, `update`, `delete`, `aggregate`, `replace_document`,
/// `delete_document` and `download` could each be replaced with a stub that
/// returns a default, and nothing failed: the rest of the suite reaches the
/// server through `pages`, `insert`, `count` and `request`. Convenience
/// wrappers are exactly where a wrong path or a wrong verb hides, because they
/// are one line each and look obviously right.
#[tokio::test]
async fn the_convenience_methods_reach_the_routes_they_name() {
    let (_server, client) = connected().await;
    seeded(&client, 5).await;

    let page = client
        .find("shop", "orders", &Query::new().filter(json!({ "qty": { "$gte": 3 } })))
        .await
        .expect("find");
    assert_eq!(page["count"], 2, "find must apply the filter it was given: {page}");

    let updated = client
        .update(
            "shop",
            "orders",
            &json!({ "_id": 1 }),
            &json!({ "$set": { "sku": "changed" } }),
            false,
        )
        .await
        .expect("update");
    assert_eq!(updated["modified"], 1);
    assert_eq!(
        client.get_document("shop", "orders", "1").await.unwrap().expect("a document")["sku"],
        "changed",
        "update must reach the document it named"
    );

    let replaced = client
        .replace_document("shop", "orders", "1", &json!({ "sku": "replaced" }), false)
        .await
        .expect("replace");
    assert_eq!(replaced["matched"], 1);
    assert_eq!(
        client.get_document("shop", "orders", "1").await.unwrap().expect("a document")["sku"],
        "replaced"
    );

    let grouped = client
        .aggregate(
            "shop",
            "orders",
            &json!([{ "$group": { "_id": null, "total": { "$sum": "$qty" } } }]),
        )
        .await
        .expect("aggregate");
    // 0+2+3+4: document 1 lost its `qty` to the replace above, because a
    // replace is not a merge — unnamed fields are dropped. Worth having a test
    // notice that rather than a caller.
    assert_eq!(grouped["documents"][0]["total"], 9, "{grouped}");

    assert_eq!(
        client.delete_document("shop", "orders", "0").await.expect("delete one")["deleted"],
        1
    );
    let deleted = client
        .delete("shop", "orders", &json!({ "qty": { "$gte": 3 } }), true)
        .await
        .expect("delete many");
    assert_eq!(deleted["deleted"], 2);
    assert_eq!(client.count("shop", "orders", &json!({})).await.unwrap(), 2);

    // A backup is bytes rather than JSON, and an empty one would still be
    // "successful" to a caller that only checked the status.
    let backup = client.download("/v1/admin/backup").await.expect("backup");
    assert!(backup.len() > 100, "a backup of a seeded node is not a token amount of bytes");
}

/// Topology filtering, which the mutation pass also found untested.
///
/// The single-node test could not exercise it: with one node and nothing
/// advertised there is nothing to filter, so inverting every comparison in
/// `refresh_topology` changed nothing observable. This gives it three nodes to
/// choose between.
#[tokio::test]
async fn refresh_topology_keeps_the_live_advertised_nodes_and_stays_where_it_is() {
    let (server, client) = connected().await;

    let live = kimmy_core::NodeId::generate();
    let unknown = kimmy_core::NodeId::generate();
    let unadvertised = kimmy_core::NodeId::generate();
    server.register_peer(&live, "http://10.0.0.7:7878").await;
    server.register_peer(&unknown, "http://10.0.0.8:7878").await;
    server.register_peer(&unadvertised, "").await;

    let members = kimmy_cluster::Members::default();
    members.insert_for_test("10.0.0.7:7900".parse().unwrap(), live);
    server.state.set_members(members);

    let endpoints = client.refresh_topology().await.expect("topology");

    assert_eq!(
        endpoints.first().map(String::as_str),
        Some(server.base.as_str()),
        "the node answering stays first: a client should not be moved off it"
    );
    assert!(endpoints.contains(&"http://10.0.0.7:7878".to_string()), "a live peer is usable");
    assert!(
        !endpoints.iter().any(|e| e.contains("10.0.0.8")),
        "a peer this node cannot vouch for is not somewhere to send requests now"
    );
    assert_eq!(endpoints.len(), 2, "and a node with no advertised endpoint cannot be dialled");
}

/// A stream reports the token it would resume from.
///
/// The accessor was untested: every other test reads `event.resume_token`, so
/// `ChangeStream::resume_token()` could have returned any string at all —
/// which the mutation pass demonstrated by making it return `"xyzzy"`.
#[tokio::test]
async fn a_stream_reports_the_token_it_would_resume_from() {
    let (_server, client) = connected().await;
    seeded(&client, 1).await;

    let mut stream = client.watch("shop", "orders", WatchOptions::new()).await.unwrap();
    client.insert("shop", "orders", &json!({ "_id": 300 })).await.unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .expect("an event");
    assert_eq!(
        stream.resume_token(),
        event.resume_token.as_deref(),
        "the stream's position is the last event it delivered"
    );
    assert!(stream.resume_token().is_some_and(|t| !t.is_empty()));
}

/// `retry: wait` waits, then asks the *same* node again.
///
/// The distinction from `elsewhere` is the whole reason the taxonomy has three
/// values rather than a boolean (ADR-057): `wait` says this node will serve the
/// request shortly, so moving to a peer abandons the node that just told you
/// how long to wait. With one endpoint there is nowhere else to go, which makes
/// this the sharpest statement of the rule — a client that fails over on `wait`
/// has nothing to fail over to and reports an error the server never meant.
#[tokio::test]
async fn a_wait_retries_the_same_node() {
    let stalling = Stalling::start(1).await;
    let client = with_token(&[&stalling.base]).await;

    let page = client
        .find("shop", "orders", &Query::new())
        .await
        .expect("a rate limit that names a delay is recoverable, not an error the caller sees");

    assert_eq!(page["served_by"], "stalling", "the eventual success is the server's own answer");
    assert_eq!(stalling.hits(), 2, "the refusal, then the same node again after the delay");
}

/// And a `wait` that keeps refusing is bounded rather than infinite.
///
/// The bound is what keeps a rate limit from turning a client into an
/// application that has stopped responding. It gives up and reports the
/// server's own error, which is more useful than a client-invented one.
#[tokio::test]
async fn a_wait_that_keeps_refusing_gives_up() {
    // More refusals than any bounded client could absorb.
    let stalling = Stalling::start(100).await;
    let client = with_token(&[&stalling.base]).await;

    let error = client.find("shop", "orders", &Query::new()).await.expect_err("still refusing");

    assert_eq!(error.retry(), Retry::Wait, "the class the server sent survives to the caller");
    assert!(
        stalling.hits() < 10,
        "a bounded number of attempts, not a client that sleeps forever: {} hits",
        stalling.hits()
    );
}

/// A node that has said `wait` twice is one the client moves on from.
///
/// The bound and the fall-through are one decision seen from two sides, and
/// with a single endpoint they are indistinguishable — giving up and moving to
/// a node that does not exist produce the same error. It takes a second node to
/// tell them apart, which is why the mutation pass could rewrite this guard
/// either way without a test noticing.
#[tokio::test]
async fn a_node_that_keeps_saying_wait_is_left_for_one_that_answers() {
    let stalling = Stalling::start(100).await;
    let healthy = Stalling::start(0).await;
    let client = with_token(&[&stalling.base, &healthy.base]).await;

    let page = client.find("shop", "orders", &Query::new()).await.expect("the second node answers");

    assert_eq!(page["served_by"], "stalling", "the answer came from a node, not from the client");
    assert_eq!(stalling.hits(), 2, "the refusal and one wait, and then no more");
    assert_eq!(healthy.hits(), 1, "then the next node, asked once");
}

/// A write is never repeated on `wait` — not to this node, and not to another.
///
/// `429` is answered before the work in every case this stub produces, but
/// nothing in the response says so, and no status distinguishes a request that
/// failed before its commit from one that failed after. So the write goes
/// exactly once and the caller decides.
///
/// **The second endpoint is what makes this a test.** With one node, "reported
/// the refusal" and "failed over and found nowhere to go" produce the same
/// error, and the mutation pass showed the guard could be rewritten to fail
/// over without anything noticing. A peer that receives a write the first node
/// may already have committed is a duplicate, silently.
#[tokio::test]
async fn a_wait_does_not_repeat_a_write_anywhere() {
    let stalling = Stalling::start(1).await;
    let healthy = Stalling::start(0).await;
    let client = with_token(&[&stalling.base, &healthy.base]).await;

    let error = client
        .request(Method::Post, "/v1/db/shop/coll/orders/insert", Some(json!({})), Safety::Unsafe)
        .await
        .expect_err("an unsafe request reports the refusal rather than repeating it");

    assert_eq!(error.retry(), Retry::Wait, "the class is reported, not acted on");
    assert_eq!(stalling.hits(), 1, "sent once");
    assert_eq!(healthy.hits(), 0, "and never to a peer that might commit it a second time");
}

/// The query builders and `collect_all`, which nothing else used.
///
/// `projection` and `explain` could each be replaced with a default and no
/// test noticed, because every other test builds a query with a filter and a
/// limit. A builder method that silently discards what it was given is the
/// kind of defect that looks like the server ignoring a field.
#[tokio::test]
async fn the_query_builders_send_what_they_were_given() {
    let (_server, client) = connected().await;
    seeded(&client, 3).await;

    let page = client
        .find(
            "shop",
            "orders",
            &Query::new().projection(json!({ "_id": 1 })).explain(true).limit(2),
        )
        .await
        .expect("find");

    assert!(page["explain"].is_object(), "explain(true) must ask for it: {page}");
    let first = &page["documents"][0];
    assert!(first["_id"].is_i64(), "the projection keeps _id");
    assert!(first.get("qty").is_none(), "and drops what it did not name: {first}");

    // `collect_all` is the shape a caller reaches for when the result is known
    // to be small, and it was reachable only through code nothing ran.
    let all = client
        .pages("shop", "orders", Query::new().limit(2))
        .collect_all()
        .await
        .expect("collect_all");
    assert_eq!(all.len(), 3, "every document, across pages of two");
}
