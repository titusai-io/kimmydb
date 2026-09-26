//! A vector drop that a partial disable left behind is audited (ADR-192).
//!
//! Its own test binary, with one test, because the check reads a log line
//! through a thread-local subscriber: in a binary where other tests run at
//! the same time, a callsite first hit on another thread with no subscriber
//! caches that nothing listens, and the line is never seen here.

use std::sync::Arc;

use kimmy_auth::TokenIssuer;
use kimmy_storage::Engine;
use tower::ServiceExt;

/// Captures what a subscriber formats.
#[derive(Clone, Default)]
struct Captured(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[test]
fn dropping_vectors_a_partial_disable_left_behind_is_audited() {
    let captured = Captured::default();
    let subscriber =
        tracing_subscriber::fmt().with_writer(captured.clone()).with_ansi(false).finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    engine.create_collection("shop", "docs").unwrap();
    let config = kimmy_core::vector_meta::VectorConfig {
        fields: vec!["text".into()],
        provider: kimmy_core::ProviderConfig::Byo {},
        dim: 3,
        metric: Default::default(),
        document_prefix: None,
        query_prefix: None,
        chunk: Default::default(),
    };
    engine.configure_vectors("shop", "docs", config).unwrap();
    // What an earlier build's partial disable left: the configuration off,
    // the stored vectors still there.
    assert!(engine.disable_vectors("shop", "docs", false).unwrap());

    let tokens = TokenIssuer::new("an-adequately-long-test-secret-value", 3600).unwrap();
    let state =
        kimmy_api::state(Arc::clone(&engine), tokens, true, kimmy_api::RateLimits::disabled())
            .unwrap();
    let app = kimmy_api::router(state);
    let request = axum::http::Request::builder()
        .method("DELETE")
        .uri("/v1/db/shop/coll/docs/vector?drop_vectors=true")
        .body(axum::body::Body::empty())
        .unwrap();
    let response = runtime.block_on(app.oneshot(request)).unwrap();
    assert_eq!(response.status(), 200);

    let log = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    assert!(log.contains("action=DropVectors"), "no audit line for the drop:\n{log}");
}
