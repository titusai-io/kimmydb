//! The client, against a real server on a real socket.
//!
//! Nothing here calls a handler directly. The client's whole job is to be
//! correct about what comes back over a connection, so a test that skipped the
//! connection would be testing the half that was never in doubt.

use std::net::SocketAddr;
use std::sync::Arc;

use kimmy_client::{Client, ErrorCode, Method, Query, Retry, Safety, WatchOptions};
use kimmy_storage::Engine;
use serde_json::{Value, json};

const SECRET: &str = "an-adequately-long-test-secret";
const ROOT_PASSWORD: &str = "root-password";

struct Server {
    base: String,
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
        let app =
            kimmy_api::build(Arc::clone(&engine), tokens, false, kimmy_api::RateLimits::disabled())
                .unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
                .await;
        });

        Self { base: format!("http://{addr}"), _dir: dir }
    }
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
