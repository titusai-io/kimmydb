//! The join retention `R` of a field collection across members with different local retentions: the creator's value
//! is the one every member holds, whichever way it arrives, so every member computes the same horizon.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bson::doc;
use kimmy_core::MergePolicy;
use kimmy_storage::Engine;

use crate::transport::{serve, sync_once};

const SECRET: &str = "a-join-retention-test-secret";
const HOUR: Duration = Duration::from_secs(3_600);

fn engine() -> (Arc<Engine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap()), dir)
}

async fn serving(engine: &Arc<Engine>) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(Arc::clone(engine), listener, SECRET.into()));
    addr
}

/// Three members with three different local retentions: a field collection created on one is held by the others with
/// the creator's `R`, and a document written on each converges with the same version identity everywhere.
#[tokio::test(flavor = "multi_thread")]
async fn three_members_with_different_local_retentions_hold_the_creators_join_retention() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let (c, _dc) = engine();
    a.set_default_join_retention(HOUR);
    b.set_default_join_retention(48 * HOUR);
    c.set_default_join_retention(7 * HOUR);
    let (a_addr, b_addr) = (serving(&a).await, serving(&b).await);
    let orders =
        a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    a.insert(&orders, doc! { "_id": 1 }).unwrap();

    sync_once(&b, a_addr, SECRET, None).await.unwrap();
    sync_once(&c, b_addr, SECRET, None).await.unwrap();

    for engine in [&a, &b, &c] {
        let held = engine.get_collection("shop", "orders").unwrap();
        assert_eq!(
            held.join_retention_ms,
            Some(3_600_000),
            "every member computes the same horizon: R is the creator's"
        );
    }
}
