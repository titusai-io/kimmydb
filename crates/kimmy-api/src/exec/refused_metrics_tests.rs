//! `kimmy_api_refused_total{code}` at the two places a probe route can reach through the real router and its middleware (ADR-272,
//! decision 7; Q6.5, C1). The HTTP tests of the three codes are in `tests/api.rs`, the MCP exit's in `kimmy-mcp`'s `tests/mcp.rs`; these
//! are the two refusals a real request cannot make on demand: the sink's end check, which needs a swap to land in the middle of a
//! request, and a refusal that is not one of the three. Each is built by a probe route merged beside the table, so it passes through
//! `count_request` as every route does, and the counter is read from the rendered `/metrics` page.

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

use super::resolve_memo_tests::{flip, state_with_orders};
use super::*;
use crate::error::ErrorCode;

/// A request and its answer: the status, and the body as text.
async fn ask(app: &Router, path: &str) -> (StatusCode, String) {
    let request = Request::get(path).body(Body::empty()).expect("the request builds");
    let response = app.clone().oneshot(request).await.expect("the router answers");
    let status = response.status();
    let bytes = response.into_body().collect().await.expect("the body is read").to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// `[stage_collection, swap_in_progress, clock_ahead]` off the rendered `/metrics` page.
async fn refused(app: &Router) -> [u64; 3] {
    let (status, page) = ask(app, "/metrics").await;
    assert_eq!(status, StatusCode::OK, "the scrape is answered: {page}");
    let read = |code: &str| -> u64 {
        let series = format!("kimmy_api_refused_total{{code=\"{code}\"}} ");
        page.lines()
            .find_map(|line| line.strip_prefix(&series))
            .unwrap_or_else(|| panic!("`{series}` is missing from /metrics"))
            .trim()
            .parse()
            .expect("the series reads as a number")
    };
    [read("stage_collection"), read("swap_in_progress"), read("clock_ahead")]
}

/// A sink about to write finds that a swap in the middle of its request buried a physical it read: the end check refuses with
/// `swap_in_progress` (resolve_memo.rs, `end_check`).
async fn probe_sink(State(state): State<SharedState>) -> Result<Json<Value>, ApiError> {
    let source = state.engine.resolve("app", "orders")?.meta;
    let _scope = resolve_memo::Scope::begin("app", "orders", &source);
    flip(&state);
    resolve_memo::end_check(&state)?;
    Ok(Json(json!({ "written": true })))
}

#[tokio::test(flavor = "current_thread")]
async fn a_sink_that_finds_a_physical_buried_in_the_middle_of_its_request_moves_swap_in_progress_by_exactly_one()
 {
    let (state, _dir) = state_with_orders();
    let probe = Router::new().route("/probe/sink", get(probe_sink).with_state(state.clone()));
    let app = crate::router_with(state, Some(probe));
    let before = refused(&app).await;
    let (status, body) = ask(&app, "/probe/sink").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "premise: the end check refused: {body}");
    assert!(
        body.contains("\"swap_in_progress\""),
        "premise: it is the refusal this test counts, built at the sink's end check: {body}"
    );
    let after = refused(&app).await;
    assert_eq!(
        [after[0] - before[0], after[1] - before[1], after[2] - before[2]],
        [0, 1, 0],
        "the second site that builds swap_in_progress is counted by the same exit"
    );
}

/// A refusal of another code, a `503` and a `400` among them, is counted by none of the three: the series is keyed on the code and not on
/// the status.
#[tokio::test(flavor = "current_thread")]
async fn a_different_503_and_a_different_400_move_no_refused_series() {
    let (state, _dir) = state_with_orders();
    async fn busy() -> Result<Json<Value>, ApiError> {
        Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, ErrorCode::CollectionPurging, "purging"))
    }
    async fn invalid() -> Result<Json<Value>, ApiError> {
        Err(ApiError::new(StatusCode::BAD_REQUEST, ErrorCode::BadRequest, "no"))
    }
    let probe = Router::new().route("/probe/503", get(busy)).route("/probe/400", get(invalid));
    let app = crate::router_with(state, Some(probe));
    let before = refused(&app).await;
    let (status, body) = ask(&app, "/probe/503").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "premise: a 503 of another code: {body}");
    assert!(body.contains("\"collection_purging\""), "premise: and it is that code: {body}");
    let (status, body) = ask(&app, "/probe/400").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "premise: a 400 of another code: {body}");
    assert_eq!(refused(&app).await, before, "neither refusal is one of the three");
}
