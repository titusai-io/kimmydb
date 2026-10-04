//! A member that is catching up in a healthy cluster is not "nobody can expire":
//! a peer holds the TTL index and expires the collection, so the member reads
//! `owed_elsewhere`, and neither the gauge nor the log says otherwise (ADR-201,
//! ADR-202). `unowned_catching_up` is for a collection whose every holder is
//! catching up, which `unowned_ttl_catching_up.rs` pins.
//!
//! Its own test binary, with one test, because the check reads a log line through
//! a thread-local subscriber: in a binary where other tests run at the same time,
//! a callsite first hit on another thread with no subscriber caches that nothing
//! listens, and the line is never seen here.
//!
//! The state exists from the marker: a member that is itself catching up expires
//! nothing, so a collection it holds the only index of is unowned until it has
//! caught up, and expiry being silent is the failure both the gauge and the line
//! exist to make audible.

use std::sync::Arc;
use std::time::Duration;

use bson::doc;
use kimmy_api::ownership::TtlState;
use kimmy_cluster::catchup::CatchUp;
use kimmy_cluster::{CatchUpReason, Facts, Members, TtlHeld};
use kimmy_storage::Engine;

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
fn a_catching_up_member_whose_peer_holds_the_index_reads_owed_elsewhere_and_warns_of_nothing() {
    let captured = Captured::default();
    let subscriber =
        tracing_subscriber::fmt().with_writer(captured.clone()).with_ansi(false).finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    engine.create_collection("app", "sessions").unwrap();
    engine
        .create_index_with(
            "app",
            "sessions",
            vec![kimmy_storage::IndexField::ascending("seen")],
            false,
            Default::default(),
            Some("ttl_seen".into()),
            Some(60),
            None,
        )
        .unwrap();
    let meta = engine.get_collection("app", "sessions").unwrap();
    engine
        .insert_many(
            &meta,
            (0..3i64).map(|i| doc! { "_id": i, "seen": bson::DateTime::from_millis(i) }).collect(),
        )
        .unwrap();
    let state = kimmy_api::state(
        Arc::clone(&engine),
        kimmy_auth::TokenIssuer::new("a-healthy-cluster-test-signing-secret", 3600).unwrap(),
        false,
        kimmy_api::RateLimits::disabled(),
    )
    .unwrap();

    // The only peer holds the index and is not catching up. This member holds it
    // too, and is catching up: its marker is set and its block says so, as the
    // daemon's does.
    let peer = kimmy_core::NodeId::from_bytes([7; 16]);
    let members = Members::default();
    members.insert_for_test("127.0.0.1:7007".parse().unwrap(), peer);
    let holder = Facts { boot: vec![7; 16], ..Facts::default() }
        .with_ttl(vec![TtlHeld { collection: meta.id, digest: vec![9; 8] }]);
    members.record_peer_facts_for_test(peer, holder, Duration::ZERO);
    let mine = Facts { boot: vec![1; 16], catching_up: true, ..Facts::default() }
        .with_ttl(vec![TtlHeld { collection: meta.id, digest: vec![9; 8] }]);
    members.set_facts_source(Arc::new(move || mine.clone()));
    state.set_members(members.clone());
    let marker_dir = tempfile::tempdir().unwrap();
    let catch_up = CatchUp::open(marker_dir.path(), Duration::from_secs(120));
    catch_up.mark(CatchUpReason::SeededEmpty).unwrap();
    state.set_catch_up(Arc::clone(&catch_up));

    let readings = state.storage_readings().unwrap();
    assert_eq!(
        readings.ownership.ttl_collections[TtlState::OwedElsewhere.slot()],
        1,
        "a peer expires it: {:?}",
        readings.ownership
    );
    assert_eq!(readings.ownership.ttl_collections[TtlState::UnownedCatchingUp.slot()], 0);
    assert_eq!(readings.ownership.ttl_collections[TtlState::UnownedNoHolder.slot()], 0);

    runtime.block_on(async {
        let looping = tokio::spawn(kimmy_api::expiry::run(
            Arc::clone(&state),
            engine.node_id(),
            Some(members),
            Duration::from_millis(20),
            kimmy_task::Shutdown::new(),
        ));
        tokio::time::sleep(Duration::from_millis(400)).await;
        looping.abort();
    });
    let log = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    assert!(
        !log.contains("no member can expire"),
        "a peer expires it, so nobody is warned that nobody can:\n{log}"
    );
    assert_eq!(
        engine.count(&meta, kimmy_storage::WalkScope::Request).unwrap(),
        3,
        "this member expires nothing while it is catching up"
    );
}
