//! Who expires a collection is decided among the members known to hold its TTL
//! index (ADR-201), not among every live member.
//!
//! The defect this pins: ownership was the rendezvous winner over the live set,
//! so a top-ranked member that did not hold the collection's TTL index (or had
//! expiry off, or was catching up) owned it and expired nothing, and no member
//! expired the collection, silently.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bson::doc;
use kimmy_api::expiry::{key, pass_with};
use kimmy_api::ownership::{Owners, TtlState, owner};
use kimmy_cluster::{Facts, Members, TtlHeld};
use kimmy_core::NodeId;
use kimmy_storage::{CollectionMeta, Engine, physical_now_ms};

const DOCS: u64 = 5;

fn node(n: u8) -> NodeId {
    NodeId::from_bytes([n; 16])
}

fn addr(n: u8) -> std::net::SocketAddr {
    format!("127.0.0.1:{}", 7000 + u16::from(n)).parse().unwrap()
}

/// An engine holding `app.sessions` with a TTL index and `DOCS` documents it
/// says expired long ago.
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

/// A peer id that outranks `me` for this collection's expiry when both are
/// candidates, so that a rule which only looks at ranking hands the collection
/// to the peer.
fn outranking(me: NodeId) -> u8 {
    let k = key("app", "sessions");
    (1..=254u8)
        .find(|n| {
            let both: BTreeSet<NodeId> = [me, node(*n)].into();
            owner(&k, &both) == Some(node(*n))
        })
        .expect("some id outranks any other")
}

fn holder_block(meta: &CollectionMeta) -> Facts {
    Facts { boot: vec![1; 16], ..Facts::default() }
        .with_ttl(vec![TtlHeld { collection: meta.id, digest: vec![9; 8] }])
}

/// This member's view: `peer` live and said `said` `age` ago; this member's own
/// block is `mine`.
fn owners(me: NodeId, peer: u8, said: Facts, age: Duration, mine: Facts) -> Owners {
    let members = Members::default();
    members.insert_for_test(addr(peer), node(peer));
    members.record_peer_facts_for_test(node(peer), said, age);
    members.set_facts_source(Arc::new(move || mine.clone()));
    Owners::over(me, Some(members))
}

fn remaining(engine: &Engine, meta: &CollectionMeta) -> u64 {
    engine.count(meta, kimmy_storage::WalkScope::Request).unwrap()
}

/// The member that ranks first but holds no TTL index hands the collection on: the
/// one that holds it expires it.
#[test]
fn a_top_ranked_member_without_the_index_hands_expiry_on() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, meta) = fixture(&dir);
    let me = engine.node_id();
    let peer = outranking(me);
    let said = Facts { boot: vec![2; 16], ..Facts::default() }; // holds nothing
    let view = owners(me, peer, said, Duration::ZERO, holder_block(&meta));
    let outcome = pass_with(&engine, &view, physical_now_ms());
    assert_eq!(outcome.deleted, DOCS, "the only holder expires it");
    assert_eq!(remaining(&engine, &meta), 0);
}

/// And when the top-ranked member does hold it, it stays the owner: this member
/// leaves the collection alone.
#[test]
fn a_top_ranked_member_that_holds_the_index_keeps_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, meta) = fixture(&dir);
    let me = engine.node_id();
    let peer = outranking(me);
    let view = owners(me, peer, holder_block(&meta), Duration::ZERO, holder_block(&meta));
    let outcome = pass_with(&engine, &view, physical_now_ms());
    assert_eq!(outcome.deleted, 0);
    assert_eq!(remaining(&engine, &meta), DOCS);
}

/// A holder whose block is stale is not positive knowledge: a member that dropped
/// the index while its contacts fail must not keep the collection for as long as it
/// is quiet.
#[test]
fn a_stale_listing_does_not_keep_the_collection() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, meta) = fixture(&dir);
    let me = engine.node_id();
    let peer = outranking(me);
    let view =
        owners(me, peer, holder_block(&meta), Duration::from_secs(3_600), holder_block(&meta));
    let outcome = pass_with(&engine, &view, physical_now_ms());
    assert_eq!(outcome.deleted, DOCS, "the stale holder is not a candidate");
}

/// A holder that is catching up does not expire, so this member does.
#[test]
fn a_top_ranked_holder_that_is_catching_up_hands_expiry_on() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, meta) = fixture(&dir);
    let me = engine.node_id();
    let peer = outranking(me);
    let said = Facts { catching_up: true, ..holder_block(&meta) };
    let view = owners(me, peer, said, Duration::ZERO, holder_block(&meta));
    assert_eq!(pass_with(&engine, &view, physical_now_ms()).deleted, DOCS);
}

/// A member that is itself catching up expires nothing, whatever it holds and
/// whoever else is live: the work waits, and the view says why.
#[test]
fn a_catching_up_member_expires_nothing_and_says_why() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, meta) = fixture(&dir);
    let me = engine.node_id();
    let peer = outranking(me);
    let mine = Facts { catching_up: true, ..holder_block(&meta) };
    // The only other member does not hold the index.
    let view =
        owners(me, peer, Facts { boot: vec![2; 16], ..Facts::default() }, Duration::ZERO, mine);
    assert_eq!(pass_with(&engine, &view, physical_now_ms()).deleted, 0);
    assert_eq!(remaining(&engine, &meta), DOCS);
    let collections = engine.all_collections().unwrap();
    let seen = view.ttl_view(&collections);
    assert_eq!(seen.counts[TtlState::UnownedCatchingUp.slot()], 1, "{seen:?}");
    assert_eq!(seen.unowned, vec![("app.sessions".to_string(), TtlState::UnownedCatchingUp)]);
}

/// With expiry switched off here, this member expires nothing and another holder
/// owns the collection; with no other holder it is unowned, and says so.
#[test]
fn a_member_with_expiry_off_owes_the_collection_elsewhere_or_says_nobody_can() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, meta) = fixture(&dir);
    let me = engine.node_id();
    let peer = outranking(me);
    let off = Facts { ttl_disabled: true, ..holder_block(&meta) };
    let collections = engine.all_collections().unwrap();

    let elsewhere = owners(me, peer, holder_block(&meta), Duration::ZERO, off.clone());
    assert_eq!(pass_with(&engine, &elsewhere, physical_now_ms()).deleted, 0);
    assert_eq!(elsewhere.ttl_view(&collections).counts[TtlState::OwedElsewhere.slot()], 1);

    let nobody =
        owners(me, peer, Facts { boot: vec![2; 16], ..Facts::default() }, Duration::ZERO, off);
    let seen = nobody.ttl_view(&collections);
    assert_eq!(seen.counts[TtlState::UnownedNoHolder.slot()], 1, "{seen:?}");
    assert_eq!(pass_with(&engine, &nobody, physical_now_ms()).deleted, 0);
}

/// A member that holds the index and is the only one able owns the collection in
/// its own view, so a view is never empty for a collection it holds.
#[test]
fn a_view_is_never_empty_for_a_collection_this_member_can_expire() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, meta) = fixture(&dir);
    let me = engine.node_id();
    let alone = Owners::over(me, None);
    assert_eq!(alone.ttl_view(&engine.all_collections().unwrap()).counts, [1, 0, 0, 0]);
    assert!(alone.owns_ttl(&meta));
}

/// A peer that has said nothing (an older build, in a mixed cluster) is not known
/// to hold the index, so this member, which holds it, expires the collection even
/// when the older peer would outrank it. If the older peer holds it too it expires
/// as well: a duplicate delete, which converges, and never a gap.
#[test]
fn an_older_peer_that_says_nothing_is_not_known_to_hold_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, meta) = fixture(&dir);
    let me = engine.node_id();
    let peer = outranking(me);
    let members = Members::default();
    members.insert_for_test(addr(peer), node(peer));
    let block = holder_block(&meta);
    members.set_facts_source(Arc::new(move || block.clone()));
    let view = Owners::over(me, Some(members));
    assert_eq!(pass_with(&engine, &view, physical_now_ms()).deleted, DOCS);
}
