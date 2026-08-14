//! The Rust client's conformance driver.
//!
//! One of three programs that answer the same questions in three languages.
//! The runner (`clients/conformance/run.py`) executes every scenario against
//! every driver and compares what comes back to expectations declared once, in
//! `clients/conformance/scenarios.json`.
//!
//! **A driver reports observations; it does not decide whether they are
//! right.** That is the whole design: three clients that each judged themselves
//! would be three opinions, and what is wanted is one oracle and three answers.
//!
//! ```text
//! conformance list
//! conformance run <scenario> <base-url> [dead-url]
//! ```
//!
//! Output is a single JSON object on stdout. Anything else — logs, progress —
//! goes to stderr, so the runner can parse stdout without a protocol.

use std::process::ExitCode;

use kimmy_client::{Client, ErrorCode, Method, Query, Retry, Safety, WatchOptions};
use serde_json::{Value, json};

/// Every scenario this driver implements. The runner checks this against the
/// declared list, so a client that quietly stops covering one is a failure
/// rather than a silence.
const SCENARIOS: [&str; 16] = [
    "capabilities",
    "documents_round_trip",
    "unlimited_find_is_a_page",
    "paging_walks_everything",
    "walk_ends_on_empty_page",
    "cursor_refuses_what_it_cannot_page",
    "creating_a_collection_twice_is_a_conflict",
    "duplicate_key_is_typed",
    "token_is_renewed",
    "failover_past_a_dead_endpoint",
    "write_is_not_retried_elsewhere",
    "change_stream_delivers",
    "change_stream_resumes",
    "dropped_collection_ends_stream",
    "recreated_collection_serves_its_own_history",
    "stale_resume_token_is_refused",
];

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("list") => {
            println!("{}", json!(SCENARIOS));
            ExitCode::SUCCESS
        }
        Some("run") => {
            let scenario = args.get(1).cloned().unwrap_or_default();
            let base = args.get(2).cloned().unwrap_or_default();
            let dead = args.get(3).cloned().unwrap_or_else(|| "http://127.0.0.1:1".into());
            match run(&scenario, &base, &dead).await {
                Ok(observations) => {
                    println!("{observations}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    println!("{}", json!({ "error": e }));
                    ExitCode::FAILURE
                }
            }
        }
        _ => {
            eprintln!("usage: conformance list | conformance run <scenario> <base-url> [dead-url]");
            ExitCode::FAILURE
        }
    }
}

fn password() -> String {
    std::env::var("KIMMY_ROOT_PASSWORD").unwrap_or_else(|_| "conformance-password".into())
}

async fn connect(base: &str) -> Result<Client, String> {
    Client::builder(base).credentials("root", password()).connect().await.map_err(|e| e.to_string())
}

async fn seeded(client: &Client, n: i64) -> Result<(), String> {
    client
        .request(
            Method::Post,
            "/v1/db/shop/collections",
            Some(json!({ "name": "orders" })),
            Safety::Idempotent,
        )
        .await
        .map_err(|e| e.to_string())?;
    let documents: Vec<Value> = (0..n).map(|i| json!({ "_id": i, "qty": i })).collect();
    if documents.is_empty() {
        return Ok(());
    }
    client.insert_many("shop", "orders", &documents).await.map_err(|e| e.to_string())?;
    Ok(())
}

async fn run(scenario: &str, base: &str, dead: &str) -> Result<Value, String> {
    match scenario {
        "capabilities" => {
            let client = connect(base).await?;
            let version = client.version().await.map_err(|e| e.to_string())?;
            Ok(json!({
                "protocol": version["protocol"],
                "has_cursor_paging": client.has_capability("cursor-paging").await.map_err(|e| e.to_string())?,
                "has_invented_capability": client.has_capability("a-capability-nobody-has").await.map_err(|e| e.to_string())?,
            }))
        }

        "documents_round_trip" => {
            let client = connect(base).await?;
            seeded(&client, 5).await?;
            let found =
                client.get_document("shop", "orders", "3").await.map_err(|e| e.to_string())?;
            let missing =
                client.get_document("shop", "orders", "999").await.map_err(|e| e.to_string())?;
            Ok(json!({
                "qty": found.as_ref().map(|d| d["qty"].clone()).unwrap_or(Value::Null),
                "missing_is_absent": missing.is_none(),
                "count": client.count("shop", "orders", &json!({})).await.map_err(|e| e.to_string())?,
            }))
        }

        "unlimited_find_is_a_page" => {
            let client = connect(base).await?;
            seeded(&client, 150).await?;
            let page =
                client.find("shop", "orders", &Query::new()).await.map_err(|e| e.to_string())?;
            Ok(json!({
                "page": page["count"],
                "offers_cursor": page["nextCursor"].is_string(),
                "total": client.count("shop", "orders", &json!({})).await.map_err(|e| e.to_string())?,
            }))
        }

        "paging_walks_everything" => {
            let client = connect(base).await?;
            seeded(&client, 250).await?;
            let mut pages = client.pages("shop", "orders", Query::new().limit(50));
            let mut ids: Vec<i64> = Vec::new();
            while let Some(page) = pages.next().await.map_err(|e| e.to_string())? {
                ids.extend(page.iter().filter_map(|d| d["_id"].as_i64()));
            }
            Ok(json!({
                "documents_seen": ids.len(),
                "first_id": ids.first().copied().unwrap_or(-1),
                "last_id": ids.last().copied().unwrap_or(-1),
                "ordered": ids.windows(2).all(|w| w[0] < w[1]),
            }))
        }

        "walk_ends_on_empty_page" => {
            let client = connect(base).await?;
            seeded(&client, 100).await?;
            let mut pages = client.pages("shop", "orders", Query::new().limit(100));
            let (mut count, mut seen) = (0, 0);
            while let Some(page) = pages.next().await.map_err(|e| e.to_string())? {
                count += 1;
                seen += page.len();
            }
            Ok(json!({ "pages": count, "documents_seen": seen }))
        }

        "cursor_refuses_what_it_cannot_page" => {
            let client = connect(base).await?;
            seeded(&client, 10).await?;
            let mut sorted = client.pages("shop", "orders", Query::new().sort(json!({ "qty": 1 })));
            let refused = sorted.next().await.is_err();
            let mut by_id =
                client.pages("shop", "orders", Query::new().sort(json!({ "_id": 1 })).limit(5));
            Ok(json!({
                "sorted_walk_refused": refused,
                "id_sort_allowed": by_id.next().await.is_ok(),
            }))
        }

        "creating_a_collection_twice_is_a_conflict" => {
            let client = connect(base).await?;
            let first = client
                .request(
                    Method::Post,
                    "/v1/db/shop/collections",
                    Some(json!({ "name": "orders" })),
                    Safety::Idempotent,
                )
                .await
                .map_err(|e| e.to_string())?;
            let second = client
                .request(
                    Method::Post,
                    "/v1/db/shop/collections",
                    Some(json!({ "name": "orders" })),
                    Safety::Idempotent,
                )
                .await
                .expect_err("creating an existing collection is a conflict");
            Ok(json!({
                "first_created": first["created"] == "orders",
                "second_code": second.code().map(|c| c.to_string()),
                "second_status": second.status(),
            }))
        }

        "duplicate_key_is_typed" => {
            let client = connect(base).await?;
            seeded(&client, 1).await?;
            let error = client
                .insert("shop", "orders", &json!({ "_id": 0 }))
                .await
                .expect_err("a duplicate _id must be refused");
            Ok(json!({
                "code": error.code().map(|c| c.to_string()),
                "retry": retry_name(error.retry()),
                "status": error.status(),
            }))
        }

        "token_is_renewed" => {
            let client = connect(base).await?;
            let first = client.token().await.unwrap_or_default();
            // The node this scenario runs against issues one-second tokens.
            tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
            let ok = client
                .request(Method::Get, "/v1/databases", None, Safety::Idempotent)
                .await
                .is_ok();
            Ok(json!({
                "token_changed": client.token().await.unwrap_or_default() != first,
                "request_succeeded": ok,
            }))
        }

        "failover_past_a_dead_endpoint" => {
            // The dead address is first, so even logging in has to move on.
            let client = Client::builder(dead)
                .endpoint(base)
                .credentials("root", password())
                .connect()
                .await
                .map_err(|e| e.to_string())?;
            let answered = client
                .request(Method::Get, "/v1/databases", None, Safety::Idempotent)
                .await
                .is_ok();
            Ok(json!({
                "answered": answered,
                "live_endpoint_first_after": client.endpoints().await.first() == Some(&base.to_string()),
            }))
        }

        "write_is_not_retried_elsewhere" => {
            let live = connect(base).await?;
            seeded(&live, 1).await?;
            let token = live.token().await.unwrap_or_default();
            let client = Client::builder(dead)
                .endpoint(base)
                .token(token)
                .connect()
                .await
                .map_err(|e| e.to_string())?;

            let error = client
                .insert("shop", "orders", &json!({ "_id": 99 }))
                .await
                .expect_err("an unsafe write must not move to another node");
            let idempotent = client
                .request(
                    Method::Post,
                    "/v1/db/shop/coll/orders/docs",
                    Some(json!({ "_id": 99 })),
                    Safety::Idempotent,
                )
                .await
                .is_ok();
            Ok(json!({
                "write_failed": true,
                "retry_class": retry_name(error.retry()),
                "idempotent_retry_succeeded": idempotent,
            }))
        }

        "change_stream_delivers" => {
            let client = connect(base).await?;
            seeded(&client, 1).await?;
            let mut stream = client
                .watch("shop", "orders", WatchOptions::new().full_document(true))
                .await
                .map_err(|e| e.to_string())?;

            let writer = client.clone();
            tokio::spawn(async move {
                for id in 100..103 {
                    let _ = writer.insert("shop", "orders", &json!({ "_id": id })).await;
                }
            });

            let mut ids = Vec::new();
            let mut all_inserts = true;
            let mut full = true;
            for _ in 0..3 {
                let event = next_event(&mut stream).await?;
                all_inserts &= event.operation == "insert";
                full &= event.full_document().is_some();
                ids.push(event.document_id().and_then(Value::as_i64).unwrap_or(-1));
            }
            Ok(json!({
                "events": ids.len(),
                "ids": ids,
                "all_inserts": all_inserts,
                "has_full_document": full,
            }))
        }

        "change_stream_resumes" => {
            let client = connect(base).await?;
            seeded(&client, 1).await?;
            let mut first = client
                .watch("shop", "orders", WatchOptions::new())
                .await
                .map_err(|e| e.to_string())?;
            client
                .insert("shop", "orders", &json!({ "_id": 200 }))
                .await
                .map_err(|e| e.to_string())?;
            let token = next_event(&mut first).await?.resume_token.unwrap_or_default();
            first.close().await;

            // Written while nothing is listening.
            client
                .insert("shop", "orders", &json!({ "_id": 201 }))
                .await
                .map_err(|e| e.to_string())?;

            let mut resumed = client
                .watch("shop", "orders", WatchOptions::new().resume_after(token))
                .await
                .map_err(|e| e.to_string())?;
            let missed = next_event(&mut resumed).await?;
            Ok(json!({
                "resumed_id": missed.document_id().and_then(Value::as_i64).unwrap_or(-1),
            }))
        }

        "dropped_collection_ends_stream" => {
            let client = connect(base).await?;
            seeded(&client, 1).await?;
            let mut stream = client
                .watch("shop", "orders", WatchOptions::new())
                .await
                .map_err(|e| e.to_string())?;
            client
                .request(Method::Delete, "/v1/db/shop/coll/orders", None, Safety::Idempotent)
                .await
                .map_err(|e| e.to_string())?;
            let event = next_event(&mut stream).await?;
            Ok(json!({
                "operation": event.operation,
                "reason": event.raw["reason"],
            }))
        }

        "recreated_collection_serves_its_own_history" => {
            let client = connect(base).await?;
            seeded(&client, 1).await?;
            drop_collection(&client).await?;
            seeded(&client, 0).await?;
            client
                .insert("shop", "orders", &json!({ "_id": 99 }))
                .await
                .map_err(|e| e.to_string())?;

            let mut stream = client
                .watch("shop", "orders", WatchOptions::new().replay_from_start(true))
                .await
                .map_err(|e| e.to_string())?;
            let event = next_event(&mut stream).await?;
            Ok(json!({ "first_id": event.document_id().and_then(Value::as_i64).unwrap_or(-1) }))
        }

        "stale_resume_token_is_refused" => {
            let client = connect(base).await?;
            seeded(&client, 1).await?;
            let mut stream = client
                .watch("shop", "orders", WatchOptions::new())
                .await
                .map_err(|e| e.to_string())?;
            client
                .insert("shop", "orders", &json!({ "_id": 5 }))
                .await
                .map_err(|e| e.to_string())?;
            let token = next_event(&mut stream).await?.resume_token.unwrap_or_default();
            stream.close().await;

            drop_collection(&client).await?;
            seeded(&client, 0).await?;

            let refused = client
                .watch("shop", "orders", WatchOptions::new().resume_after(token))
                .await
                .err()
                .and_then(|e| e.code())
                .map(|c| c.to_string());
            Ok(json!({ "code": refused }))
        }

        other => Err(format!("unknown scenario {other:?}")),
    }
}

async fn drop_collection(client: &Client) -> Result<(), String> {
    client
        .request(Method::Delete, "/v1/db/shop/coll/orders", None, Safety::Idempotent)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// The next event, or a failure rather than a hang.
async fn next_event(
    stream: &mut kimmy_client::ChangeStream,
) -> Result<kimmy_client::ChangeEvent, String> {
    tokio::time::timeout(std::time::Duration::from_secs(15), stream.next())
        .await
        .map_err(|_| "timed out waiting for a change event".to_string())?
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "the stream ended without an event".to_string())
}

fn retry_name(retry: Retry) -> &'static str {
    match retry {
        Retry::No => "no",
        Retry::Wait => "wait",
        Retry::Elsewhere => "elsewhere",
    }
}

/// Referenced so an unused import is not the thing that breaks the build when
/// a scenario is edited.
#[allow(dead_code)]
fn codes_are_typed(code: ErrorCode) -> String {
    code.to_string()
}
