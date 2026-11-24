//! A request resolves each name it reads once, and a sink that is about to write checks that nothing it read has been buried since
//! (ADR-272, decision 11; Q6.4 item 4). The flip is driven through the engine between two uses of a name, as a swap that lands in
//! the middle of an aggregation would.

use kimmy_core::{CollectionBind, CollectionId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp};
use kimmy_storage::{PeerWindow, SyncOutcome, WalkScope};
use serde_json::json;

use super::resolve_memo::{self, Scope};
use super::tests::{agg, live_state, seed_docs};
use super::*;

fn root() -> Auth {
    Auth(kimmy_auth::Principal::superuser("test"))
}

/// Flip `orders` to a stage on this member, the way a replicated swap does: the stage's creation, then the binding in a window.
pub(super) fn flip(state: &SharedState) -> String {
    let stage = "orders.__stg.memo";
    let engine = &state.engine;
    engine
        .create_stage(
            "app",
            stage,
            MergePolicy::Document,
            Hlc::new(kimmy_storage::physical_now_ms() + 60_000, 0),
        )
        .unwrap();
    let creation = engine
        .entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive("app", stage)
        })
        .unwrap();
    // a binding from another member's origin, delivered in a window
    let bind = OplogEntry {
        stamp: Stamp::new(
            Hlc::new(creation.stamp.hlc.wall_ms + 1, 0),
            kimmy_core::NodeId::from_bytes([9; 16]),
            kimmy_core::Epoch::ZERO,
        ),
        kind: OpKind::BindCollection,
        collection: CollectionId::derive("app", "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: "app".into(),
                name: "orders".into(),
                physical: stage.into(),
            })
            .unwrap(),
        ),
        meta: None,
    };
    let mut theirs = kimmy_core::VersionVector::default();
    theirs.observe(bind.stamp);
    let mine = engine.witnessed_vector().unwrap();
    let mut outcome = SyncOutcome::default();
    engine
        .apply_peer_window_into(
            &theirs,
            Some(&mine),
            std::slice::from_ref(&bind),
            PeerWindow {
                scanned_to: bind.stamp.hlc,
                exhausted: true,
                passed_through: None,
                asked_partial: false,
            },
            &mut outcome,
        )
        .unwrap();
    assert_eq!(
        engine.resolve("app", "orders").unwrap().physical,
        stage,
        "premise: the flip is visible"
    );
    stage.to_string()
}

pub(super) fn state_with_orders() -> (SharedState, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let state = live_state(&dir);
    seed_docs(&state, "orders", vec![bson::doc! { "_id": 1, "v": "a" }]);
    seed_docs(&state, "src", vec![bson::doc! { "_id": 1 }]);
    (state, dir)
}

#[test]
fn a_name_read_twice_in_one_request_is_resolved_once() {
    let (state, _dir) = state_with_orders();
    // the request's source is another collection, so `orders` is a name the request reads (a `$lookup` of it, twice)
    let source = state.engine.resolve("app", "src").unwrap().meta;
    let stage;
    {
        let _scope = Scope::begin("app", "src", &source);
        let first = foreign_collection(&state, &root(), "app", "orders").unwrap().unwrap();
        stage = flip(&state);
        let second = foreign_collection(&state, &root(), "app", "orders").unwrap().unwrap();
        assert_eq!(first.name, "orders", "premise: the first use resolved the identity");
        assert_eq!(
            second.name, "orders",
            "the second use of the name answers from the request's memo, not from the physical the flip made visible ({stage})"
        );
    }
    // and a request that has not begun a memo resolves afresh, as ever
    let afresh = foreign_collection(&state, &root(), "app", "orders").unwrap().unwrap();
    assert_eq!(
        afresh.name, stage,
        "outside a request's memo the name answers from what is visible now"
    );
}

#[test]
fn the_source_of_a_request_is_resolved_once_too() {
    let (state, _dir) = state_with_orders();
    let source = state.engine.resolve("app", "orders").unwrap().meta;
    let _scope = Scope::begin("app", "orders", &source);
    flip(&state);
    let again = foreign_collection(&state, &root(), "app", "orders").unwrap().unwrap();
    assert_eq!(
        again.name, "orders",
        "a `$lookup` of the source reads the physical the source read"
    );
}

#[test]
fn a_sink_about_to_write_finds_a_physical_the_request_read_buried_and_writes_nothing() {
    let (state, _dir) = state_with_orders();
    let source = state.engine.resolve("app", "orders").unwrap().meta;
    let _scope = Scope::begin("app", "orders", &source);
    assert!(
        resolve_memo::end_check(&state).is_ok(),
        "nothing has been buried: the write may go on"
    );
    flip(&state); // the swap lands in the middle of the request and buries `orders`
    let refused =
        resolve_memo::end_check(&state).expect_err("the physical the request read is gone");
    assert_eq!(refused.status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert!(format!("{refused:?}").contains("SwapInProgress"), "{refused:?}");
}

#[test]
fn a_read_only_request_is_never_refused_for_a_physical_that_was_buried_after_its_last_read() {
    let (state, _dir) = state_with_orders();
    // no sink, no end check: an aggregation that only reads answers from what it read
    let answer = agg(&state, &root(), "app", "orders", &json!([{ "$match": {} }])).unwrap();
    assert_eq!(answer["count"], 1, "{answer:?}");
}

/// The wiring, by source: an aggregation begins the memo in BOTH of its synchronous entries (the `$vectorSearch` source and the scan),
/// after their awaits, and the sink writer checks before anything is written. A flip in the middle of a request cannot be injected
/// from here, so the order of these calls is what is asserted.
#[test]
fn the_memo_is_begun_in_both_entries_and_the_sink_checks_before_it_writes() {
    let exec = include_str!("../exec.rs");
    let body = |name: &str| -> String {
        let start = exec.find(&format!("fn {name}(")).unwrap_or_else(|| panic!("no fn {name}"));
        let rest = &exec[start + 1..];
        let end = rest.find("\nfn ").or_else(|| rest.find("\npub fn ")).unwrap_or(rest.len());
        exec[start..start + 1 + end].to_string()
    };
    let run = body("aggregate_run_held");
    assert_eq!(
        run.matches("resolve_memo::Scope::begin(").count(),
        2,
        "the memo is begun after the `$vectorSearch` await and before the scan"
    );
    let sink = body("write_sink");
    let check = sink
        .find("resolve_memo::end_check(state)?")
        .expect("the sink writer asks the memo's end check");
    let write = sink.find("deadline::check()?").expect("the writer's last look at the deadline");
    assert!(check < write, "the end check comes before anything is created or written");
}

/// The memo is the thread's: its guard must not be holdable across an await (see [`Scope`]). `Scope: Send` would make this ambiguous
/// and fail to compile.
#[test]
fn the_scope_cannot_be_held_across_an_await() {
    trait AmbiguousIfSend<A> {
        fn check() {}
    }
    impl<T: ?Sized> AmbiguousIfSend<()> for T {}
    impl<T: ?Sized + Send> AmbiguousIfSend<u8> for T {}
    <Scope as AmbiguousIfSend<_>>::check();
}
