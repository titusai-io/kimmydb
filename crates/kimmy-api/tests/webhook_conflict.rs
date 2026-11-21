//! The `conflictResolved` webhook event (Q7 step 4, S4b): a subscription opts in by listing the operation, on a collection that can
//! produce it; what it is delivered is the loss records the owner holds, under the `eventId` of the lost write.

use std::sync::Arc;

use kimmy_api::egress::{EgressPolicy, WEBHOOKS};
use kimmy_auth::TokenIssuer;
use kimmy_core::MergePolicy;
use kimmy_storage::Engine;

const SECRET: &str = "an-adequately-long-test-secret-for-hs256";

/// An engine plus API state, with an egress policy that permits the loopback receiver.
fn state_for(dir: &tempfile::TempDir) -> kimmy_api::SharedState {
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let tokens = TokenIssuer::new(SECRET, 3600).unwrap();
    kimmy_api::state_with_egress(
        engine,
        tokens,
        false,
        kimmy_api::RateLimits::disabled(),
        EgressPolicy::new(WEBHOOKS, vec!["127.0.0.1".into()]),
    )
    .unwrap()
}

fn root() -> kimmy_api::state::Auth {
    kimmy_api::state::Auth(kimmy_auth::Principal::new("root", vec![kimmy_auth::Grant::superuser()]))
}

async fn register(
    state: &kimmy_api::SharedState,
    operations: Option<Vec<&str>>,
) -> Result<serde_json::Value, kimmy_api::error::ApiError> {
    let request = kimmy_api::webhooks::RegisterRequest {
        url: "http://127.0.0.1:9/hook".into(),
        operations: operations.map(|names| names.into_iter().map(String::from).collect()),
    };
    kimmy_api::webhooks::register(
        state,
        &root(),
        "shop",
        "orders",
        &request,
        &EgressPolicy::new(WEBHOOKS, vec!["127.0.0.1".into()]),
    )
    .await
}

#[tokio::test]
async fn a_subscription_may_list_conflict_resolved() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    state.engine.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();

    let registered = register(&state, Some(vec!["insert", "conflictResolved"])).await;
    let id = registered.expect("a field collection can be subscribed to conflictResolved")["id"]
        .as_str()
        .unwrap()
        .to_string();

    let listing = kimmy_api::webhooks::list(&state, &root(), "shop", "orders").unwrap();
    let listed = listing["webhooks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["id"] == id.as_str())
        .expect("the subscription is listed");
    assert_eq!(
        listed["operations"],
        serde_json::json!(["insert", "conflictResolved"]),
        "the listing names the operation as it was asked for"
    );
}

#[tokio::test]
async fn a_document_collection_cannot_be_subscribed_to_conflict_resolved() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    state
        .engine
        .create_collection_with_policy("shop", "orders", MergePolicy::Document, None)
        .unwrap();

    let err = register(&state, Some(vec!["conflictResolved"]))
        .await
        .expect_err("a document collection never produces a loss record");

    assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
    assert!(
        err.message.contains("mergePolicy"),
        "the refusal names the policy that cannot produce the event: {}",
        err.message
    );
    // And the other operations stay available there.
    register(&state, Some(vec!["insert"])).await.expect("insert on a document collection");
}
