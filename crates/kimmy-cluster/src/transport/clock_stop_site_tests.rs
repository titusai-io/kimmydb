//! U1 at the transport sites that decide "stop, or move on" (package 3.0): a repair replay, the replay of
//! this member's own lost writes, and the tick's pulls. A window that ended at an entry whose clock is not
//! due yet took nothing from the stop on, so none of them may move past it, and none may ask again inside
//! the tick.
//!
//! The receiver's clock is moved back per engine (`set_clock_offset_for_tests`), so what the donor wrote just
//! now is far-future to it, and forward again to make it due: no test waits.

use std::net::SocketAddr;
use std::time::Duration;

use bson::doc;
use kimmy_core::{DocId, Hlc, MergePolicy, NodeId, OplogEntry, VersionVector};
use kimmy_storage::Engine;
use tokio::io::DuplexStream;

use super::*;
use crate::catchup::CatchUp;
use crate::protocol::ReplayStall;

const SKEW_MS: i64 = 300_000;

fn node(n: u128) -> NodeId {
    NodeId::from_bytes(n.to_be_bytes())
}

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

/// A donor holding the field collection `shop.orders` and `n` documents written after its creation: the creation's
/// time (the floor a replay is planned from) and the document entries in stamp order.
fn donor(n: i64) -> (Engine, tempfile::TempDir, Hlc, Vec<OplogEntry>) {
    let (a, dir) = engine();
    let orders =
        a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let creation = a
        .entries_for_peer(Hlc::ZERO, MAX_BATCH, kimmy_storage::WalkScope::Background)
        .unwrap()
        .entries;
    let cut = creation.last().unwrap().stamp;
    for i in 1..=n {
        std::thread::sleep(Duration::from_millis(3));
        a.insert(&orders, doc! { "_id": i }).unwrap();
    }
    let documents = a
        .entries_for_peer(Hlc::ZERO, MAX_BATCH, kimmy_storage::WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .filter(|e| e.stamp > cut)
        .collect();
    (a, dir, cut.hlc, documents)
}

/// The receiver: its own field collection `shop.orders` (the same id as the donor's), and its clock behind the
/// donor's by more than the bound.
fn receiver() -> (Engine, tempfile::TempDir, kimmy_storage::CollectionMeta) {
    let (b, dir) = engine();
    let orders =
        b.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    b.set_clock_offset_for_tests(-(SKEW_MS + 10_000));
    (b, dir, orders)
}

/// A peer that serves `window` to every `AskEntries` it is sent, and says which positions were asked from, in
/// order, until the round ends (the receiver drops its end).
async fn serve_window(
    mut stream: DuplexStream,
    theirs: VersionVector,
    window: Vec<OplogEntry>,
) -> Vec<Hlc> {
    assert!(matches!(read_frame(&mut stream).await.unwrap(), Message::AskVersions { .. }));
    let answer = Message::Vectors {
        servable: theirs.clone(),
        witnessed: theirs,
        facts: None,
        facts_gen: None,
        echo: None,
        replay_from: None,
    };
    write_frame(&mut stream, &answer).await.unwrap();
    let mut asked = Vec::new();
    while let Ok(Message::AskEntries { from, .. }) = read_frame(&mut stream).await {
        asked.push(from);
        let scanned_to = window.last().unwrap().stamp.hlc;
        let entries = Message::Entries {
            entries: window.clone(),
            scanned_to,
            exhausted: false,
            passed_through: None,
        };
        if write_frame(&mut stream, &entries).await.is_err() {
            break;
        }
    }
    asked
}

/// One round of `b` against the peer that serves `window`, and what the peer was asked from.
async fn round(
    b: &Engine,
    stalls: &mut PeerStalls,
    theirs: &VersionVector,
    window: &[OplogEntry],
) -> (SyncOutcome, Vec<Hlc>) {
    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let (ours, peer_end) = tokio::io::duplex(MAX_FRAME);
    let peer = tokio::spawn(serve_window(peer_end, theirs.clone(), window.to_vec()));
    let outcome = sync_over(b, ours, addr, node(9), None, stalls).await.unwrap();
    (outcome, peer.await.unwrap())
}

/// A repair's replay that stopped at an entry whose clock is not due yet resumes from where it was, not from the end
/// of the window: the refused entry and everything after it would be skipped, and never asked for again.
#[tokio::test]
async fn a_repair_replay_stopped_at_a_clock_refused_entry_does_not_skip_it_and_applies_it_once_due()
{
    let (a, _da, floor, documents) = donor(2);
    let theirs = a.version_vector().unwrap();
    let (b, _db, orders) = receiver();
    let mut stalls = PeerStalls::new();
    assert!(stalls.plan_repair(node(9), orders.id, Repair::Replay { from: floor }));

    let (outcome, asked) = round(&b, &mut stalls, &theirs, &documents).await;
    assert!(
        outcome.stopped(),
        "a refused entry is not skipped: the window did not stop at it: {outcome:?}"
    );
    assert_eq!(asked.len(), 1, "one request in the round: {asked:?}");
    let start = asked[0];

    // Due: the next round asks from the same place, so the refused entry is served again and applies.
    b.set_clock_offset_for_tests(0);
    let (outcome, asked) = round(&b, &mut stalls, &theirs, &documents).await;
    assert_eq!(
        asked.first(),
        Some(&start),
        "a refused entry is not skipped: the replay resumed from {asked:?}, not from {start:?}"
    );
    assert_eq!(outcome.clock_refused, 0, "{outcome:?}");
    for i in 1..=2i64 {
        assert!(
            b.get(&orders, &DocId::Int64(i)).unwrap().is_some(),
            "applied once due: document {i}"
        );
    }
}

/// The replay of this member's own lost writes: a page that stopped at an entry whose clock is not due yet stalls, and
/// is not counted as done, so the replay asks for it again rather than moving past it.
#[test]
fn a_replay_of_lost_writes_stalls_at_a_clock_refused_entry_and_is_not_counted_as_done() {
    let writer_dir = tempfile::tempdir().unwrap();
    let writer = Engine::open(&writer_dir.path().join("kimmy.redb")).unwrap();
    let orders =
        writer.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    writer.insert_many(&orders, (0..2).map(|i| doc! { "_id": i }).collect()).unwrap();
    let mut backup = Vec::new();
    writer.backup_to(&mut backup, kimmy_storage::WalkScope::Request).unwrap();
    std::thread::sleep(Duration::from_millis(15));
    writer.insert_many(&orders, (2..7).map(|i| doc! { "_id": i }).collect()).unwrap();

    let (c, _dc) = engine();
    let everything = writer
        .entries_for_peer(Hlc::ZERO, MAX_BATCH, kimmy_storage::WalkScope::Background)
        .unwrap();
    c.apply_batch(&everything.entries).unwrap();

    let a_dir = tempfile::tempdir().unwrap();
    let path = a_dir.path().join("kimmy.redb");
    kimmy_storage::backup::restore(&path, &mut backup.as_slice()).unwrap();
    let a = Engine::open(&path).unwrap();
    assert_eq!(a.node_id(), writer.node_id(), "premise: A is the restored writer");
    a.create_collection("shop", "startup").unwrap();
    let marker_dir = tempfile::tempdir().unwrap();
    let catch_up = CatchUp::open(marker_dir.path(), Duration::from_secs(120));
    catch_up.arm_replay(a.own_position_at_open(), None).unwrap();

    let c_node = c.node_id();
    let from = catch_up.replay_from(c_node, a.node_id());
    let window =
        c.entries_for_peer(from.hlc, MAX_BATCH, kimmy_storage::WalkScope::Background).unwrap();
    let page: Vec<_> =
        window.entries.iter().filter(|e| e.stamp.node == a.node_id()).cloned().collect();
    assert!(!page.is_empty(), "premise: the writes A lost");
    a.set_clock_offset_for_tests(-(SKEW_MS + 10_000));

    let stalled =
        replay_apply(&a, &catch_up, &"c", c_node, from, page.clone(), from, true, Via::Inbound)
            .unwrap();
    assert_eq!(stalled, ReplayPage::Stalled(ReplayStall::ClockRefused), "the page stalls");
    assert!(!catch_up.replay_answered(c_node), "and is not counted as done");

    a.set_clock_offset_for_tests(0);
    let done =
        replay_apply(&a, &catch_up, &"c", c_node, from, page, from, true, Via::Inbound).unwrap();
    assert!(matches!(done, ReplayPage::Done { .. }), "applied once due: {done:?}");
    assert!(catch_up.replay_answered(c_node));
}

/// A pull that ended at a refused entry is not pulled again inside the tick: the window is retried on the next one,
/// and the tick's budget is not spent asking the same question.
#[tokio::test]
async fn a_refused_window_is_retried_on_the_tick_and_not_pulled_again_in_it() {
    let (a, _da, _floor, documents) = donor(3);
    let theirs = a.version_vector().unwrap();
    let (b, _db, _orders) = receiver();
    let mut stalls = PeerStalls::new();

    let (outcome, asked) = round(&b, &mut stalls, &theirs, &documents).await;

    assert!(
        !outcome.truncated,
        "one request per tick: a stopped window is not truncated, so the tick pulls no more: {outcome:?}"
    );
    assert_eq!(asked.len(), 1, "one request per tick: {asked:?}");
    assert_eq!(outcome.clock_refused, 1, "{outcome:?}");
}
