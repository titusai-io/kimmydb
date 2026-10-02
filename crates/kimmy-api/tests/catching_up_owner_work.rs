//! While a member's catching-up marker is set, no owner work of any class runs on
//! it (ADR-202).
//!
//! The hazard: a member that expires a document from a stale copy mints a delete
//! stamped *now*, which beats a refreshed copy that arrived from a peer with an
//! earlier stamp under last-writer-wins. An embedding written from a stale copy
//! and a webhook dispatched from stale state are the same mistake. The marker
//! turns all three off here, whatever the member set says, and whether the gate
//! is closed (`seeded_empty`, `restored`, `snapshot`) or has opened as `unknown`.

use std::sync::Arc;
use std::time::Duration;

use bson::doc;
use kimmy_api::expiry::pass_with;
use kimmy_api::ownership::{Owners, TtlState};
use kimmy_cluster::catchup::CatchUp;
use kimmy_cluster::{CatchUpReason, Facts, Members};
use kimmy_storage::{CollectionMeta, Engine, physical_now_ms};

const DOCS: u64 = 4;

/// An engine holding `app.sessions` with a TTL index and `DOCS` documents that
/// expired long ago.
fn fixture(dir: &tempfile::TempDir) -> (Engine, CollectionMeta) {
    let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
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
    let docs = (0..DOCS as i64)
        .map(|i| doc! { "_id": i, "seen": bson::DateTime::from_millis(i) })
        .collect();
    engine.insert_many(&meta, docs).unwrap();
    (engine, meta)
}

/// A marker in its own directory, set for `reason`.
fn marker(reason: CatchUpReason) -> (Arc<CatchUp>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let catch_up = CatchUp::open(dir.path(), Duration::from_secs(120));
    catch_up.mark(reason).unwrap();
    (catch_up, dir)
}

fn remaining(engine: &Engine, meta: &CollectionMeta) -> u64 {
    engine.count(meta, kimmy_storage::WalkScope::Request).unwrap()
}

/// TTL expiry: nothing is deleted while the marker is set, for every reason, and
/// the whole backlog is deleted once it clears.
#[test]
fn expiry_deletes_nothing_while_the_marker_is_set() {
    for reason in CatchUpReason::ALL {
        let dir = tempfile::tempdir().unwrap();
        let (engine, meta) = fixture(&dir);
        let (catch_up, _marker_dir) = marker(reason);
        let owners = Owners::over(engine.node_id(), None).gated_by(Some(Arc::clone(&catch_up)));

        assert_eq!(pass_with(&engine, &owners, physical_now_ms()).deleted, 0, "{reason:?}");
        assert_eq!(remaining(&engine, &meta), DOCS, "{reason:?}");
        let seen = owners.ttl_view(&engine.all_collections().unwrap());
        assert_eq!(seen.counts[TtlState::UnownedCatchingUp.slot()], 1, "{reason:?}: {seen:?}");

        catch_up.clear("the test");
        assert_eq!(pass_with(&engine, &owners, physical_now_ms()).deleted, DOCS, "{reason:?}");
        assert_eq!(remaining(&engine, &meta), 0);
    }
}

/// Embeddings: the worker's ownership question is answered no while the marker is
/// set, for the live worker as much as the backfill (both ask it), and yes again
/// once it clears.
#[test]
fn embeddings_are_not_owned_while_the_marker_is_set() {
    for reason in CatchUpReason::ALL {
        let dir = tempfile::tempdir().unwrap();
        let (engine, _meta) = fixture(&dir);
        let (catch_up, _marker_dir) = marker(reason);
        let owners = Owners::over(engine.node_id(), None).gated_by(Some(Arc::clone(&catch_up)));
        for key in ["app/docs", "app/notes", "shop/items"] {
            assert!(!owners.owns_embedding(key), "{reason:?}: {key}");
        }
        catch_up.clear("the test");
        for key in ["app/docs", "app/notes", "shop/items"] {
            assert!(owners.owns_embedding(key), "{reason:?}: {key} after the clear");
        }
    }
}

/// Webhooks: no subscription is delivered from here while the marker is set.
#[test]
fn webhooks_are_not_delivered_while_the_marker_is_set() {
    for reason in CatchUpReason::ALL {
        let dir = tempfile::tempdir().unwrap();
        let (engine, _meta) = fixture(&dir);
        let (catch_up, _marker_dir) = marker(reason);
        let owners = Owners::over(engine.node_id(), None).gated_by(Some(Arc::clone(&catch_up)));
        for id in ["wh_a", "wh_b", "wh_c"] {
            assert!(!owners.owns_subscription(id), "{reason:?}: {id}");
        }
        catch_up.clear("the test");
        for id in ["wh_a", "wh_b", "wh_c"] {
            assert!(owners.owns_subscription(id), "{reason:?}: {id} after the clear");
        }
    }
}

/// The marker outranks the member set: a clustered member whose own block says it
/// is fine, and that the rendezvous names for every key, still owns nothing.
#[test]
fn the_marker_outranks_the_member_set() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, meta) = fixture(&dir);
    let (catch_up, _marker_dir) = marker(CatchUpReason::SeededEmpty);
    let members = Members::default();
    let block = Facts { boot: vec![1; 16], ..Facts::default() }
        .with_ttl(vec![kimmy_cluster::TtlHeld { collection: meta.id, digest: vec![9; 8] }]);
    members.set_facts_source(Arc::new(move || block.clone()));
    let owners = Owners::over(engine.node_id(), Some(members)).gated_by(Some(catch_up));

    assert!(!owners.owns_ttl(&meta));
    assert!(!owners.owns_embedding("app/sessions"));
    assert!(!owners.owns_subscription("wh_a"));
    assert_eq!(pass_with(&engine, &owners, physical_now_ms()).deleted, 0);
}

/// Past the bound the gate opens and the state reads `unknown`, but the marker is
/// still set, so owner work stays off: the bound is about refusing requests on a
/// guess, not about expiring from a copy nobody has checked.
#[test]
fn owner_work_stays_off_past_the_bound() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, meta) = fixture(&dir);
    let marker_dir = tempfile::tempdir().unwrap();
    let catch_up = CatchUp::open(marker_dir.path(), Duration::from_millis(1));
    catch_up.first_tick(std::time::Instant::now());
    catch_up.mark(CatchUpReason::SeededEmpty).unwrap();
    std::thread::sleep(Duration::from_millis(30));
    assert!(catch_up.unknown(std::time::Instant::now()), "the wait has run out");

    let owners = Owners::over(engine.node_id(), None).gated_by(Some(catch_up));
    assert!(!owners.owns_ttl(&meta));
    assert!(!owners.owns_embedding("app/sessions"));
    assert!(!owners.owns_subscription("wh_a"));
    assert_eq!(pass_with(&engine, &owners, physical_now_ms()).deleted, 0);
    assert_eq!(remaining(&engine, &meta), DOCS);
}

/// The operator's clear is live: deleting the file and one refresh (the sync tick
/// makes it) puts owner work back, with no restart.
#[test]
fn the_operators_clear_puts_owner_work_back() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, meta) = fixture(&dir);
    let marker_dir = tempfile::tempdir().unwrap();
    let catch_up = CatchUp::open(marker_dir.path(), Duration::from_secs(120));
    catch_up.mark(CatchUpReason::Restored).unwrap();
    let owners = Owners::over(engine.node_id(), None).gated_by(Some(Arc::clone(&catch_up)));
    assert_eq!(pass_with(&engine, &owners, physical_now_ms()).deleted, 0);

    std::fs::remove_file(marker_dir.path().join(kimmy_cluster::catchup::FILE)).unwrap();
    catch_up.refresh();
    assert!(owners.owns_ttl(&meta));
    assert_eq!(pass_with(&engine, &owners, physical_now_ms()).deleted, DOCS);
}

/// A member with no marker at all (clustering off, or nothing to gate on) behaves
/// as it always has.
#[test]
fn a_member_with_no_marker_owns_as_before() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, meta) = fixture(&dir);
    let marker_dir = tempfile::tempdir().unwrap();
    let never_set = CatchUp::open(marker_dir.path(), Duration::from_secs(120));
    for owners in [
        Owners::over(engine.node_id(), None),
        Owners::over(engine.node_id(), None).gated_by(None),
        Owners::over(engine.node_id(), None).gated_by(Some(never_set)),
    ] {
        assert!(owners.owns_ttl(&meta));
        assert!(owners.owns_embedding("app/sessions"));
        assert!(owners.owns_subscription("wh_a"));
    }
    assert_eq!(remaining(&engine, &meta), DOCS);
}

/// The expiry loop as the daemon runs it takes the marker from the state: with it
/// set the loop deletes nothing however many passes run, and when it clears the
/// backlog goes. (The tests above ask `Owners` directly; this is the wiring.)
#[tokio::test]
async fn the_expiry_loop_reads_the_marker_from_the_state() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, meta) = fixture(&dir);
    let engine = Arc::new(engine);
    let state = kimmy_api::state(
        Arc::clone(&engine),
        kimmy_auth::TokenIssuer::new("a-catching-up-owner-work-secret-value", 3600).unwrap(),
        false,
        kimmy_api::RateLimits::disabled(),
    )
    .unwrap();
    let (catch_up, _marker_dir) = marker(CatchUpReason::SeededEmpty);
    state.set_catch_up(Arc::clone(&catch_up));
    let looping = tokio::spawn(kimmy_api::expiry::run(
        state,
        engine.node_id(),
        None,
        Duration::from_millis(50),
    ));

    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(remaining(&engine, &meta), DOCS, "the loop expired while the marker was set");

    catch_up.clear("the test");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while remaining(&engine, &meta) != 0 {
        assert!(std::time::Instant::now() < deadline, "the loop never resumed after the clear");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    looping.abort();
}

/// Drive `catch_up` into `unknown (owed)` with virtual instants (ADR-212): B
/// (node 1) reached and answered, C (node 2) listed live and never answering,
/// judged five seconds after the arming and past C's hold.
fn into_owed_replay(catch_up: &CatchUp) {
    use kimmy_cluster::catchup::{Decision, Reached, ReplayTick, Tick, UnknownCause};
    use kimmy_core::{Hlc, NodeId, VersionVector};
    let id = |n: u8| NodeId::from_bytes([n; 16]);
    let (me, b, c) = (id(9), id(1), id(2));
    let members = Members::default();
    members.insert_for_test("127.0.0.1:7001".parse().unwrap(), b);
    members.insert_for_test("127.0.0.1:7002".parse().unwrap(), c);
    let live = members.node_ids();
    let mut servable = VersionVector::new();
    servable.insert(b, Hlc::new(100, 0));
    let mut mine = servable.clone();
    mine.insert(me, Hlc::new(200, 0));
    let reached = [Reached { node: b, servable, witnessed: None, facts: None }];
    let t0 = std::time::Instant::now();
    catch_up.arm_replay_at(Hlc::new(50, 0), None, t0).unwrap();
    catch_up.replay_finished(b);
    for at in [5, 130] {
        let now = t0 + Duration::from_secs(at);
        catch_up.replay_note_members(&ReplayTick {
            me,
            members: Some(&members),
            reached: &reached,
            expected_members: None,
            now,
        });
        catch_up.replay_settle();
        let decision = catch_up.evaluate(&Tick {
            me,
            reached: &reached,
            mine_witnessed: &mine,
            mine_servable: &mine,
            snapshot_pending: false,
            live: Some(&live),
            expected_members: None,
            now,
        });
        assert_eq!(decision, Decision::Kept, "{at} s");
    }
    assert_eq!(
        catch_up.unknown_because(t0 + Duration::from_secs(130)),
        Some(UnknownCause::OwedReplay),
        "premise: C owes the replay past its hold"
    );
}

/// A restored member serving as `unknown` because a member owes it the replay.
fn owed() -> (Arc<CatchUp>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    kimmy_cluster::catchup::write_marker(dir.path(), CatchUpReason::Restored).unwrap();
    let catch_up = CatchUp::open(dir.path(), Duration::from_secs(120));
    into_owed_replay(&catch_up);
    (catch_up, dir)
}

/// Serving as `unknown (owed)` (ADR-212), the gate is open but the marker is set,
/// so TTL expiry deletes nothing.
#[test]
fn expiry_deletes_nothing_while_owed_the_replay() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, meta) = fixture(&dir);
    let (catch_up, _marker_dir) = owed();
    assert!(!catch_up.gated(std::time::Instant::now()), "premise: the gate is open");
    let owners = Owners::over(engine.node_id(), None).gated_by(Some(catch_up));
    assert!(!owners.owns_ttl(&meta));
    assert_eq!(pass_with(&engine, &owners, physical_now_ms()).deleted, 0);
    assert_eq!(remaining(&engine, &meta), DOCS);
}

/// The same for embeddings.
#[test]
fn embeddings_are_not_owned_while_owed_the_replay() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _meta) = fixture(&dir);
    let (catch_up, _marker_dir) = owed();
    let owners = Owners::over(engine.node_id(), None).gated_by(Some(catch_up));
    for key in ["app/docs", "app/notes", "shop/items"] {
        assert!(!owners.owns_embedding(key), "{key}");
    }
}

/// The same for webhook dispatch.
#[test]
fn webhooks_are_not_delivered_while_owed_the_replay() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _meta) = fixture(&dir);
    let (catch_up, _marker_dir) = owed();
    let owners = Owners::over(engine.node_id(), None).gated_by(Some(catch_up));
    for id in ["wh_a", "wh_b", "wh_c"] {
        assert!(!owners.owns_subscription(id), "{id}");
    }
}
