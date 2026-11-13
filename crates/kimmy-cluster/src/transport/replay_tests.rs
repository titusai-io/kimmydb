//! The replay a restored member's peer serves on the contact the peer opened
//! (ADR-212's addendum): the ask on `Vectors`, the pages and their acks, what the
//! member accepts and refuses, and that the ordinary round goes on.

use kimmy_core::Epoch;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use kimmy_core::{Hlc, NodeId, OpKind, OplogEntry, Stamp};
use kimmy_storage::Engine;

use super::*;
use crate::catchup::CatchUp;
use crate::protocol::ReplayStall;

const SECRET: &str = "a-replay-inbound-secret";
const BINDING: &[u8] = b"a-replay-inbound-binding";

fn addr() -> SocketAddr {
    "127.0.0.1:7199".parse().unwrap()
}

/// A restored member A (its backup lacks the tail), its armed catch-up state and
/// `Members`, and a holder C of the whole tail.
struct Fixture {
    a: Engine,
    c: Engine,
    catch_up: Arc<CatchUp>,
    members: Members,
    lost: i64,
    _dirs: Vec<tempfile::TempDir>,
}

/// `lost` documents written after the backup, `big` bytes each.
fn fixture(lost: i64, big: usize) -> Fixture {
    fixture_with_tail(lost, big, 0)
}

/// [`fixture`], and C also holds `tail` entries of a third origin that sort between
/// the backup and the lost writes: what a serve's walk must read past to reach them.
fn fixture_with_tail(lost: i64, big: usize, tail: usize) -> Fixture {
    let writer_dir = tempfile::tempdir().unwrap();
    let writer = Engine::open(&writer_dir.path().join("kimmy.redb")).unwrap();
    let orders = writer.create_collection("shop", "orders").unwrap();
    writer.insert_many(&orders, (0..2).map(|i| bson::doc! { "_id": i }).collect()).unwrap();
    let mut backup = Vec::new();
    writer.backup_to(&mut backup, kimmy_storage::WalkScope::Request).unwrap();
    let backup_at = writer.version_vector().unwrap().get(writer.origin());
    // The lost writes are stamped after the third origin's entries.
    std::thread::sleep(Duration::from_millis(15));
    let pad = "x".repeat(big);
    for chunk in (2..2 + lost).collect::<Vec<_>>().chunks(100) {
        writer
            .insert_many(
                &orders,
                chunk.iter().map(|i| bson::doc! { "_id": *i, "pad": pad.clone() }).collect(),
            )
            .unwrap();
    }
    let c_dir = tempfile::tempdir().unwrap();
    let c = Engine::open(&c_dir.path().join("kimmy.redb")).unwrap();
    let mut from = Hlc::ZERO;
    loop {
        let window =
            writer.entries_for_peer(from, MAX_BATCH, kimmy_storage::WalkScope::Background).unwrap();
        c.apply_batch(&window.entries).unwrap();
        if window.exhausted {
            break;
        }
        from = window.entries.last().unwrap().stamp.hlc;
    }
    if tail > 0 {
        let third = NodeId::generate();
        let entries: Vec<OplogEntry> = (0..tail)
            .map(|k| OplogEntry {
                stamp: Stamp::new(Hlc::new(backup_at.wall_ms + 1, k as u16), third, Epoch::ZERO),
                kind: OpKind::Insert,
                collection: orders.id,
                doc_id: Some(kimmy_core::DocId::String(format!("third{k}"))),
                body: Some(
                    bson::serialize_to_vec(&bson::doc! { "_id": format!("third{k}") }).unwrap(),
                ),
            })
            .collect();
        for chunk in entries.chunks(500) {
            c.apply_batch(chunk).unwrap();
        }
    }
    let a_dir = tempfile::tempdir().unwrap();
    let path = a_dir.path().join("kimmy.redb");
    kimmy_storage::backup::restore(&path, &mut backup.as_slice()).unwrap();
    let a = Engine::open(&path).unwrap();
    assert_eq!(a.node_id(), writer.node_id());
    a.create_collection("shop", "startup").unwrap();
    let marker_dir = tempfile::tempdir().unwrap();
    let catch_up = CatchUp::open(marker_dir.path(), Duration::from_secs(120));
    catch_up.arm_replay(a.own_position_at_open(), None).unwrap();
    let members = Members::default();
    members.set_catch_up(Arc::clone(&catch_up));
    Fixture { a, c, catch_up, members, lost, _dirs: vec![writer_dir, c_dir, a_dir, marker_dir] }
}

impl Fixture {
    fn orders(&self) -> u64 {
        let held = self.a.get_collection("shop", "orders").unwrap();
        self.a.count(&held, kimmy_storage::WalkScope::Request).unwrap()
    }

    /// One round: C dials A, over the real serve and sync code.
    async fn round(&self) -> Result<SyncOutcome, ProtocolError> {
        let (mut ours, far) = tokio::io::duplex(MAX_FRAME);
        let serving = async {
            let _ = serve_peer(
                &self.a,
                far,
                SECRET,
                BINDING,
                None,
                ServeBudgets::serving(),
                Some(&self.members),
            )
            .await;
        };
        let mut stalls = PeerStalls::new();
        let asking = async {
            open_handshake(&self.c, &mut ours, SECRET, BINDING).await.unwrap();
            let outcome =
                sync_over(&self.c, &mut ours, addr(), self.a.node_id(), None, &mut stalls).await;
            drop(ours);
            outcome
        };
        let ((), outcome) = tokio::join!(serving, asking);
        outcome
    }
}

/// C dials A once, and A's side is served until the stream closes; returns what the
/// scripted dialer saw. For tests that send frames by hand.
async fn scripted<F, Fut>(f: &Fixture, script: F) -> Result<(), ProtocolError>
where
    F: FnOnce(tokio::io::DuplexStream) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let (mut ours, far) = tokio::io::duplex(MAX_FRAME);
    let serving =
        serve_peer(&f.a, far, SECRET, BINDING, None, ServeBudgets::serving(), Some(&f.members));
    let asking = async {
        open_handshake(&f.c, &mut ours, SECRET, BINDING).await.unwrap();
        f_script(&mut ours, f).await;
        script(ours).await;
    };
    let (served, ()) = tokio::join!(serving, asking);
    // The script ending closes the stream, which is how a peer's contact ends.
    match served {
        Err(ProtocolError::Closed) => Ok(()),
        Err(ProtocolError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(()),
        other => other,
    }
}

/// Nothing: the hook point every scripted test starts from.
async fn f_script(_stream: &mut tokio::io::DuplexStream, _f: &Fixture) {}

async fn ask_versions(stream: &mut tokio::io::DuplexStream) -> Option<Stamp> {
    write_frame(stream, &Message::AskVersions { facts: None, facts_gen: None, echo: None })
        .await
        .unwrap();
    match read_frame(stream).await.unwrap() {
        Message::Vectors { replay_from, .. } => replay_from,
        other => panic!("{other:?}"),
    }
}

/// The replay position just after `hlc` on A's node, in every epoch.
fn pos(f: &Fixture, hlc: Hlc) -> Stamp {
    crate::catchup::replay_after(hlc, f.a.node_id())
}

/// An entry of A's origin above `hlc`, in a collection A has, to build pages by hand.
fn own_entry(f: &Fixture, hlc: Hlc, id: &str) -> OplogEntry {
    let orders = f.a.get_collection("shop", "orders").unwrap();
    OplogEntry {
        stamp: Stamp::new(hlc, f.a.node_id(), f.a.epoch()),
        kind: OpKind::Insert,
        collection: orders.id,
        doc_id: Some(kimmy_core::DocId::String(id.into())),
        body: Some(bson::serialize_to_vec(&bson::doc! { "_id": id }).unwrap()),
    }
}

/// The whole replay rides C's own contact: A asks on its `Vectors`, C serves
/// the pages before its ordinary pull, A applies and acks, and the replay is
/// answered, with no dial from A at all.
#[tokio::test]
async fn an_inbound_contact_serves_the_whole_replay() {
    let f = fixture(MAX_BATCH as i64 * 2 + 50, 0);
    let before = f.orders();
    let served = replay_counters().get(true, ReplayResult::Served);
    // The serve has a time budget a loaded runner can spend before the last page; the
    // replay then goes on at the next contact, so the test allows a few.
    for _ in 0..8 {
        f.round().await.unwrap();
        if f.catch_up.replay_answered(f.c.node_id()) {
            break;
        }
    }
    assert!(f.catch_up.replay_answered(f.c.node_id()), "the replay is answered");
    assert_eq!(f.orders(), before + f.lost as u64, "every lost write is back");
    assert_eq!(f.catch_up.reason(), Some(crate::CatchUpReason::Restored));
    assert!(replay_counters().get(true, ReplayResult::Served) >= served + 3, "pages were served");
}

/// An unmarked member asks as well: it is armed, and nothing says it was restored
/// until the replay proves it.
#[tokio::test]
async fn an_armed_member_with_no_marker_asks() {
    let f = fixture(5, 0);
    assert_eq!(f.catch_up.reason(), None, "premise: unmarked");
    scripted(&f, |mut stream| async move {
        assert!(ask_versions(&mut stream).await.is_some(), "it asks");
    })
    .await
    .unwrap();
}

/// The ask is made only while the replay is armed and C has not answered it.
#[tokio::test]
async fn the_ask_is_made_only_while_armed_and_unanswered() {
    let f = fixture(5, 0);
    let c = f.c.node_id();
    let cursor = f.catch_up.replay_from(c, f.a.node_id());
    scripted(&f, |mut stream| async move {
        assert_eq!(ask_versions(&mut stream).await, Some(cursor));
    })
    .await
    .unwrap();
    f.catch_up.replay_finished(c);
    scripted(&f, |mut stream| async move {
        assert_eq!(ask_versions(&mut stream).await, None, "answered: no ask");
    })
    .await
    .unwrap();
    let off = fixture(5, 0);
    off.catch_up.forget_replay();
    scripted(&off, |mut stream| async move {
        assert_eq!(ask_versions(&mut stream).await, None, "not armed: no ask");
    })
    .await
    .unwrap();
}

/// A page nobody asked for is refused, and so is every page that is not what was
/// asked: the wrong start, too many entries, out of order, another origin's, or
/// below where it began.
#[tokio::test]
async fn pages_that_were_not_asked_for_are_refused() {
    let f = fixture(5, 0);
    let from = f.catch_up.replay_from(f.c.node_id(), f.a.node_id());
    let refused_before = replay_counters().get(true, ReplayResult::Refused);
    // No ask on this connection.
    let r = scripted(&f, |mut stream| async move {
        write_frame(
            &mut stream,
            &Message::ReplayEntries {
                from,
                entries: Vec::new(),
                exhausted: true,
                horizon: false,
                scanned_to: from,
            },
        )
        .await
        .unwrap();
    })
    .await;
    assert!(matches!(r, Err(ProtocolError::Malformed(_))), "{r:?}");

    let at = |n: u64| Hlc::new(from.hlc.wall_ms + n, 0);
    let pages: Vec<(&str, Message)> = vec![
        (
            "a different start",
            Message::ReplayEntries {
                from: pos(&f, at(1)),
                entries: Vec::new(),
                exhausted: true,
                horizon: false,
                scanned_to: pos(&f, at(1)),
            },
        ),
        (
            "scanned to before its own last entry",
            Message::ReplayEntries {
                from,
                entries: vec![own_entry(&f, at(5), "a")],
                exhausted: false,
                horizon: false,
                scanned_to: pos(&f, at(4)),
            },
        ),
        (
            "out of order",
            Message::ReplayEntries {
                from,
                entries: vec![own_entry(&f, at(5), "b"), own_entry(&f, at(4), "a")],
                exhausted: false,
                horizon: false,
                scanned_to: pos(&f, at(9)),
            },
        ),
        (
            "at or below where it began",
            Message::ReplayEntries {
                from,
                entries: vec![own_entry(&f, from.hlc, "a")],
                exhausted: false,
                horizon: false,
                scanned_to: pos(&f, at(9)),
            },
        ),
        (
            "another origin's entry",
            Message::ReplayEntries {
                from,
                entries: vec![OplogEntry {
                    stamp: Stamp::new(at(5), NodeId::generate(), Epoch::ZERO),
                    ..own_entry(&f, at(5), "a")
                }],
                exhausted: false,
                horizon: false,
                scanned_to: pos(&f, at(9)),
            },
        ),
        (
            "the same start's HLC in another epoch",
            Message::ReplayEntries {
                from: Stamp { epoch: Epoch::ZERO, ..from },
                entries: Vec::new(),
                exhausted: true,
                horizon: false,
                scanned_to: pos(&f, at(9)),
            },
        ),
        (
            "two epochs at one HLC out of stamp order",
            Message::ReplayEntries {
                from,
                entries: vec![
                    own_entry(&f, at(5), "b"),
                    OplogEntry {
                        stamp: Stamp::new(at(5), f.a.node_id(), Epoch::ZERO),
                        ..own_entry(&f, at(5), "a")
                    },
                ],
                exhausted: false,
                horizon: false,
                scanned_to: pos(&f, at(9)),
            },
        ),
        (
            "scanned to another member's position",
            Message::ReplayEntries {
                from,
                entries: vec![own_entry(&f, at(5), "a")],
                exhausted: false,
                horizon: false,
                scanned_to: Stamp::new(at(9), NodeId::generate(), Epoch::ZERO),
            },
        ),
        (
            "more than a batch",
            Message::ReplayEntries {
                from,
                entries: (1..=MAX_BATCH as u64 + 1)
                    .map(|n| own_entry(&f, at(n), &n.to_string()))
                    .collect(),
                exhausted: false,
                horizon: false,
                scanned_to: pos(&f, at(MAX_BATCH as u64 + 9)),
            },
        ),
    ];
    for (what, page) in pages {
        let r = scripted(&f, |mut stream| async move {
            assert!(ask_versions(&mut stream).await.is_some());
            write_frame(&mut stream, &page).await.unwrap();
        })
        .await;
        assert!(matches!(r, Err(ProtocolError::Malformed(_))), "{what}: {r:?}");
        assert!(!f.catch_up.replay_answered(f.c.node_id()), "{what}: nothing is answered");
    }
    assert!(replay_counters().get(true, ReplayResult::Refused) >= refused_before + 9);
}

/// **An empty page that is not the end answers nothing.** A budget cut or a batch
/// cut to fit a frame says nothing of what lies beyond it; only `exhausted` finishes.
#[tokio::test]
async fn an_empty_page_that_is_not_the_end_does_not_finish_the_replay() {
    let f = fixture(5, 0);
    let c = f.c.node_id();
    let from = f.catch_up.replay_from(c, f.a.node_id());
    scripted(&f, |mut stream| async move {
        assert!(ask_versions(&mut stream).await.is_some());
        write_frame(
            &mut stream,
            &Message::ReplayEntries {
                from,
                entries: Vec::new(),
                exhausted: false,
                horizon: false,
                scanned_to: from,
            },
        )
        .await
        .unwrap();
        match read_frame(&mut stream).await.unwrap() {
            Message::ReplayAck { next_from, done, stalled } => {
                assert_eq!((next_from, done, stalled), (from, false, None));
            }
            other => panic!("{other:?}"),
        }
        // The end: empty and exhausted.
        write_frame(
            &mut stream,
            &Message::ReplayEntries {
                from,
                entries: Vec::new(),
                exhausted: true,
                horizon: false,
                scanned_to: from,
            },
        )
        .await
        .unwrap();
        match read_frame(&mut stream).await.unwrap() {
            Message::ReplayAck { done, .. } => assert!(done),
            other => panic!("{other:?}"),
        }
    })
    .await
    .unwrap();
    assert!(f.catch_up.replay_answered(c));
}

/// A horizon page is not an answer: it records the horizon, acks without
/// finishing, and the replay stays owed.
#[tokio::test]
async fn a_horizon_page_is_recorded_and_answers_nothing() {
    let f = fixture(5, 0);
    let c = f.c.node_id();
    let from = f.catch_up.replay_from(c, f.a.node_id());
    scripted(&f, |mut stream| async move {
        assert!(ask_versions(&mut stream).await.is_some());
        write_frame(
            &mut stream,
            &Message::ReplayEntries {
                from,
                entries: Vec::new(),
                exhausted: false,
                horizon: true,
                scanned_to: from,
            },
        )
        .await
        .unwrap();
        match read_frame(&mut stream).await.unwrap() {
            Message::ReplayAck { done, .. } => assert!(!done),
            other => panic!("{other:?}"),
        }
    })
    .await
    .unwrap();
    assert!(!f.catch_up.replay_answered(c));
    assert!(
        matches!(
            f.catch_up.replay_beyond_horizon(c, std::time::Instant::now()),
            crate::catchup::Horizon::Again
        ),
        "the first horizon was already recorded"
    );
}

/// **No livelock.** A page naming a collection A lacks stalls the replay for this
/// contact only: the ordinary pull in the same round brings the collection (C created
/// it), and the next contact finishes. The connection is never closed for it.
#[tokio::test]
async fn a_stalled_page_still_lets_the_ordinary_pull_run_and_the_next_contact_finishes() {
    let writer_dir = tempfile::tempdir().unwrap();
    let writer = Engine::open(&writer_dir.path().join("kimmy.redb")).unwrap();
    writer.create_collection("shop", "orders").unwrap();
    let mut backup = Vec::new();
    writer.backup_to(&mut backup, kimmy_storage::WalkScope::Request).unwrap();

    // C holds a collection its own origin created, and a write of A's origin in it.
    let c_dir = tempfile::tempdir().unwrap();
    let c = Engine::open(&c_dir.path().join("kimmy.redb")).unwrap();
    let elsewhere = c.create_collection("shop", "peer_only").unwrap();
    // Just after the backup, so that A's own later writes sort above it and C still
    // lacks them.
    let backup_at = writer.version_vector().unwrap().get(writer.origin());
    let lost = OplogEntry {
        stamp: Stamp::new(Hlc::new(backup_at.wall_ms + 1, 0), writer.node_id(), writer.epoch()),
        kind: OpKind::Insert,
        collection: elsewhere.id,
        doc_id: Some(kimmy_core::DocId::String("lost".into())),
        body: Some(bson::serialize_to_vec(&bson::doc! { "_id": "lost" }).unwrap()),
    };
    c.apply_batch(std::slice::from_ref(&lost)).unwrap();

    let a_dir = tempfile::tempdir().unwrap();
    let path = a_dir.path().join("kimmy.redb");
    kimmy_storage::backup::restore(&path, &mut backup.as_slice()).unwrap();
    let a = Engine::open(&path).unwrap();
    std::thread::sleep(Duration::from_millis(10));
    a.create_collection("shop", "startup").unwrap();
    let marker_dir = tempfile::tempdir().unwrap();
    let catch_up = CatchUp::open(marker_dir.path(), Duration::from_secs(120));
    catch_up.arm_replay(a.own_position_at_open(), None).unwrap();
    let members = Members::default();
    members.set_catch_up(Arc::clone(&catch_up));
    let f = Fixture {
        a,
        c,
        catch_up,
        members,
        lost: 0,
        _dirs: vec![writer_dir, c_dir, a_dir, marker_dir],
    };
    assert!(f.a.get_collection("shop", "peer_only").is_err(), "premise: A lacks the collection");

    // First contact: the page stalls and nothing is answered, but the contact is not
    // spent: C's ordinary round on the same connection ran (it pulled A's own
    // collection), and the next contact is not blocked.
    let stalled = replay_counters().get(true, ReplayResult::Stalled);
    f.round().await.unwrap();
    assert!(
        replay_counters().get(true, ReplayResult::Stalled) > stalled,
        "the stalled page is counted, by way"
    );
    assert!(!f.catch_up.replay_answered(f.c.node_id()), "stalled: nothing is answered");
    assert!(
        f.c.get_collection("shop", "startup").is_ok(),
        "the stalled contact still ran C's ordinary pull from A"
    );
    // A contact the peer opens does not push its own collections to A, so A learns
    // the collection as it always did: by its own round with a member that has it
    // (here a member B, which A can dial).
    assert!(f.a.get_collection("shop", "peer_only").is_err());
    let b_dir = tempfile::tempdir().unwrap();
    let b = Engine::open(&b_dir.path().join("kimmy.redb")).unwrap();
    let window =
        f.c.entries_for_peer(Hlc::ZERO, MAX_BATCH, kimmy_storage::WalkScope::Background).unwrap();
    let theirs: Vec<_> =
        window.entries.iter().filter(|e| e.stamp.node == f.c.node_id()).cloned().collect();
    b.apply_batch(&theirs).unwrap();
    let (mut ours, far) = tokio::io::duplex(MAX_FRAME);
    let serving =
        serve_peer(&b, far, SECRET, BINDING, None, ServeBudgets::serving(), None::<&Members>);
    let asking = async {
        open_handshake(&f.a, &mut ours, SECRET, BINDING).await.unwrap();
        let mut stalls = PeerStalls::new();
        let outcome = sync_over(&f.a, &mut ours, addr(), b.node_id(), None, &mut stalls).await;
        drop(ours);
        outcome
    };
    let (_, outcome) = tokio::join!(serving, asking);
    outcome.unwrap();
    assert!(f.a.get_collection("shop", "peer_only").is_ok(), "A's own round brought it");
    // Second contact: the collection is here, the page applies, and it is answered.
    f.round().await.unwrap();
    assert!(f.catch_up.replay_answered(f.c.node_id()));
    assert_eq!(f.catch_up.reason(), Some(crate::CatchUpReason::Restored));
}

/// The wire: the stall rides the ack, and a page never carries what an older
/// build would take for something else.
#[test]
fn the_new_frames_round_trip_and_an_older_build_ignores_the_ask() {
    use serde::Deserialize;
    let node = NodeId::from_bytes([4; 16]);
    let tied = Stamp::new(Hlc::new(4, 0), node, Epoch::new(Hlc::new(2, 0), [7; 8]));
    let frames = vec![
        Message::ReplayEntries {
            from: crate::catchup::replay_after(Hlc::new(3, 1), node),
            entries: Vec::new(),
            exhausted: true,
            horizon: false,
            scanned_to: Stamp::new(Hlc::new(3, 1), node, Epoch::from_u64(5)),
        },
        Message::ReplayAck { next_from: tied, done: false, stalled: None },
        Message::ReplayAck {
            next_from: tied,
            done: false,
            stalled: Some(ReplayStall::PurgePending),
        },
    ];
    for frame in frames {
        let doc = bson::serialize_to_document(&frame).unwrap();
        let back: Message = bson::deserialize_from_document(doc).unwrap();
        assert_eq!(back, frame);
    }
    // A `Vectors` with the ask, read by a build that predates it: the key is dropped.
    #[derive(Debug, Deserialize)]
    enum Before {
        Vectors {
            servable: kimmy_core::VersionVector,
            witnessed: kimmy_core::VersionVector,
        },
        #[allow(dead_code)]
        Versions(kimmy_core::VersionVector),
    }
    let with_ask = Message::Vectors {
        servable: Default::default(),
        witnessed: Default::default(),
        facts: None,
        facts_gen: None,
        echo: None,
        replay_from: Some(tied),
    };
    let back: Message =
        bson::deserialize_from_document(bson::serialize_to_document(&with_ask).unwrap()).unwrap();
    assert_eq!(back, with_ask, "the ask round-trips as the whole stamp");
    let doc = bson::serialize_to_document(&with_ask).unwrap();
    assert!(matches!(bson::deserialize_from_document::<Before>(doc), Ok(Before::Vectors { .. })));
    // A frame with no ask has no key at all.
    let without = Message::Vectors {
        servable: Default::default(),
        witnessed: Default::default(),
        facts: None,
        facts_gen: None,
        echo: None,
        replay_from: None,
    };
    let doc = bson::serialize_to_document(&without).unwrap();
    assert!(!doc.get_document("Vectors").unwrap().contains_key("replay_from"));
    // A build that predates the pages cannot read one: it is malformed to it.
    #[derive(Debug, Deserialize)]
    enum Older {
        #[allow(dead_code)]
        Versions(kimmy_core::VersionVector),
    }
    let page = bson::serialize_to_document(&frames_page()).unwrap();
    assert!(bson::deserialize_from_document::<Older>(page).is_err());
}

fn frames_page() -> Message {
    let zero = Stamp::new(Hlc::ZERO, NodeId::from_bytes([0; 16]), Epoch::ZERO);
    Message::ReplayEntries {
        from: zero,
        entries: Vec::new(),
        exhausted: true,
        horizon: false,
        scanned_to: zero,
    }
}

/// The dialer's side, against a scripted answerer: it ends its replay for the
/// contact when an ack does not advance, however the answerer words it.
#[tokio::test]
async fn an_ack_that_does_not_advance_ends_the_replay_for_the_contact() {
    // More than one page, so that a dialler that kept going would send a second.
    let f = fixture(MAX_BATCH as i64 * 2 + 5, 0);
    let (mut ours, mut theirs) = tokio::io::duplex(MAX_FRAME);
    let from = crate::catchup::replay_after(Hlc::new(1, 0), f.a.node_id());
    // UNSUPERVISED: a scripted peer or racing applier in a test, joined below, whose panic fails it
    let scripted = tokio::spawn(async move {
        let mut pages = 0usize;
        while let Ok(Message::ReplayEntries { from, .. }) = read_frame(&mut theirs).await {
            pages += 1;
            // A constant ack: the position it was asked from, never moving.
            write_frame(
                &mut theirs,
                &Message::ReplayAck { next_from: from, done: false, stalled: None },
            )
            .await
            .unwrap();
        }
        pages
    });
    // The dialer holds the whole tail; the peer (A) is the origin it serves.
    let theirs_vector = kimmy_core::VersionVector::default();
    let served =
        serve_replay(&f.c, &mut ours, addr(), f.a.node_id(), &theirs_vector, from).await.unwrap();
    drop(ours);
    assert_eq!(served, ReplayServed::InStep);
    assert_eq!(scripted.await.unwrap(), 1, "one page, then it stopped: it does not trust the ack");
}

/// A page that is never acked ends the round quietly: nothing more can be read from
/// a stream that may still hold the ack, so the contact is over without a failure.
#[tokio::test(start_paused = true)]
async fn a_missing_ack_ends_the_contact_without_failing_it() {
    let f = fixture(30, 0);
    let (mut ours, mut theirs) = tokio::io::duplex(MAX_FRAME);
    // UNSUPERVISED: a scripted peer or racing applier in a test, joined below, whose panic fails it
    let silent = tokio::spawn(async move {
        let _ = read_frame(&mut theirs).await;
        // Holds the stream open and says nothing.
        tokio::time::sleep(Duration::from_secs(3600)).await;
    });
    let before = replay_counters().get(true, ReplayResult::AckTimeout);
    let served = serve_replay(
        &f.c,
        &mut ours,
        addr(),
        f.a.node_id(),
        &kimmy_core::VersionVector::default(),
        crate::catchup::replay_after(Hlc::new(1, 0), f.a.node_id()),
    )
    .await
    .unwrap();
    assert_eq!(served, ReplayServed::OutOfStep);
    assert_eq!(replay_counters().get(true, ReplayResult::AckTimeout), before + 1);
    silent.abort();
}

/// Serving the replay is a read: with C's writer held the whole time, the replay is
/// served and applied, and does not wait for it. A watchdog releases the writer after
/// twenty seconds, so a replay that needed it fails this test and does not hang it.
#[test]
fn the_replay_is_served_while_the_holders_writer_is_held() {
    let f = fixture(20, 0);
    let c = f.c.node_id();
    let (held, is_held) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let holder = &f.c;
    let (done_in_time, answered) = std::thread::scope(|scope| {
        // UNSUPERVISED: a scripted peer or racing applier in a test, joined below, whose panic fails it
        let watcher = scope.spawn(move || {
            let hold = holder.hold_writer(kimmy_storage::WriterHolder::Bulk);
            held.send(()).unwrap();
            let in_time = released.recv_timeout(Duration::from_secs(20)).is_ok();
            drop(hold);
            in_time
        });
        is_held.recv().unwrap();
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(
            async {
                let (mut ours, far) = tokio::io::duplex(MAX_FRAME);
                let serving = serve_peer(
                    &f.a,
                    far,
                    SECRET,
                    BINDING,
                    None,
                    ServeBudgets::serving(),
                    Some(&f.members),
                );
                let asking = async {
                    open_handshake(&f.c, &mut ours, SECRET, BINDING).await.unwrap();
                    let asked = ask_versions(&mut ours).await.expect("the ask");
                    let theirs = kimmy_core::VersionVector::default();
                    let served =
                        serve_replay(&f.c, &mut ours, addr(), f.a.node_id(), &theirs, asked).await;
                    drop(ours);
                    served
                };
                let (_, served) = tokio::join!(serving, asking);
                assert_eq!(served.unwrap(), ReplayServed::InStep);
            },
        );
        release.send(()).unwrap();
        (watcher.join().unwrap(), f.catch_up.replay_answered(c))
    });
    assert!(done_in_time, "the replay waited for the holder's writer");
    assert!(answered, "the replay finished with the holder's writer held");
}

/// The same page applied twice, and from two places at once, is safe: the second
/// finds everything held, the clock only moves forward and the end is idempotent.
#[test]
fn applying_a_page_twice_and_at_once_is_safe() {
    let f = fixture(40, 0);
    let c = f.c.node_id();
    let from = f.catch_up.replay_from(c, f.a.node_id());
    let window =
        f.c.entries_for_peer(from.hlc, MAX_BATCH, kimmy_storage::WalkScope::Background).unwrap();
    let page: Vec<_> =
        window.entries.iter().filter(|e| e.stamp.node == f.a.node_id()).cloned().collect();
    assert!(!page.is_empty());
    let apply = |via| {
        replay_apply(&f.a, &f.catch_up, &"c", c, from, page.clone(), from, true, via).unwrap()
    };
    let (one, two) = std::thread::scope(|scope| {
        // UNSUPERVISED: a scripted peer or racing applier in a test, joined below, whose panic fails it
        let t1 = scope.spawn(|| apply(Via::Inbound));
        // UNSUPERVISED: a scripted peer or racing applier in a test, joined below, whose panic fails it
        let t2 = scope.spawn(|| apply(Via::Outbound));
        (t1.join().unwrap(), t2.join().unwrap())
    });
    assert!(matches!(one, ReplayPage::Done { .. }) && matches!(two, ReplayPage::Done { .. }));
    assert!(f.catch_up.replay_answered(c));
    assert_eq!(f.orders(), 2 + 40);
    // And once more after: nothing changes.
    assert!(matches!(apply(Via::Inbound), ReplayPage::Done { .. }));
    assert_eq!(f.orders(), 2 + 40);
}

/// Entries larger than a frame can carry a batch of: the page is sized by C, and the
/// replay still finishes across several.
#[tokio::test]
async fn pages_of_large_entries_are_sized_by_the_holder() {
    // Five entries of 14 MiB each are more than one frame of MAX_FRAME (64 MiB), and
    // far fewer than MAX_BATCH entries: the holder sizes the page, the member cannot.
    let f = fixture(5, 14 * 1024 * 1024);
    TEST_REPLAY_SERVE_TIME.with(|t| t.set(Some(Duration::from_secs(3600))));
    TEST_REPLAY_PAGES_SENT.with(|n| n.set(0));
    f.round().await.unwrap();
    let pages = TEST_REPLAY_PAGES_SENT.with(std::cell::Cell::get);
    TEST_REPLAY_SERVE_TIME.with(|t| t.set(None));
    // `write_frame` refuses a frame over MAX_FRAME, so every page that went out fits,
    // and the tail needed more than one of them.
    assert!(pages > 1, "the tail went in {pages} pages, each under MAX_FRAME ({MAX_FRAME})");
    assert!(f.catch_up.replay_answered(f.c.node_id()));
    assert_eq!(f.orders(), 2 + 5);
}

/// One contact serves at most the page cap, and the replay goes on at the next.
#[tokio::test]
async fn one_contact_serves_at_most_the_page_cap() {
    let f = fixture(MAX_BATCH as i64 * 3 + 5, 0);
    // The time box is out of play (an hour), so only the cap can end the serve.
    TEST_REPLAY_SERVE_TIME.with(|t| t.set(Some(Duration::from_secs(3600))));
    TEST_REPLAY_MAX_PAGES.with(|cap| cap.set(Some(2)));
    TEST_REPLAY_PAGES_SENT.with(|n| n.set(0));
    f.round().await.unwrap();
    let sent = TEST_REPLAY_PAGES_SENT.with(std::cell::Cell::get);
    TEST_REPLAY_MAX_PAGES.with(|cap| cap.set(None));
    assert_eq!(sent, 2, "it stopped at the cap: two pages of the four the tail needs");
    assert!(!f.catch_up.replay_answered(f.c.node_id()), "two pages are not the whole tail");
    f.round().await.unwrap();
    f.round().await.unwrap();
    TEST_REPLAY_SERVE_TIME.with(|t| t.set(None));
    assert!(f.catch_up.replay_answered(f.c.node_id()), "the next contacts finish it");
}

/// A holder that has collected its oplog past where the replay asks cannot say what
/// it holds of the member's origin: it answers with the horizon, never "nothing".
#[tokio::test]
async fn a_holder_past_its_horizon_does_not_answer_the_replay() {
    let f = fixture(5, 0);
    let far = kimmy_storage::physical_now_ms() + 365 * 24 * 3600 * 1000;
    f.c.collect_garbage_at(far, kimmy_storage::RetentionPolicy::new(1, 1)).unwrap();
    f.round().await.unwrap();
    assert!(!f.catch_up.replay_answered(f.c.node_id()), "a horizon is not an answer");
}

/// The ack of a page that moved the cursor names where it moved to, and the next
/// page is validated against that.
#[tokio::test]
async fn the_ack_names_the_cursor_the_page_reached() {
    let f = fixture(5, 0);
    let c = f.c.node_id();
    let from = f.catch_up.replay_from(c, f.a.node_id());
    let at = |n: u64| Hlc::new(from.hlc.wall_ms + n, 0);
    let page = vec![own_entry(&f, at(1), "p1"), own_entry(&f, at(2), "p2")];
    let reached = pos(&f, at(2));
    let last = page[1].stamp;
    scripted(&f, |mut stream| async move {
        assert!(ask_versions(&mut stream).await.is_some());
        write_frame(
            &mut stream,
            &Message::ReplayEntries {
                from,
                entries: page,
                exhausted: false,
                horizon: false,
                scanned_to: reached,
            },
        )
        .await
        .unwrap();
        match read_frame(&mut stream).await.unwrap() {
            Message::ReplayAck { next_from, done, stalled } => {
                assert_eq!(
                    next_from, last,
                    "the ack names the last entry applied, its whole stamp"
                );
                assert!(!done && stalled.is_none());
            }
            other => panic!("{other:?}"),
        }
    })
    .await
    .unwrap();
}

/// **Liveness.** The holder's walk reads past a long tail of another origin before it
/// reaches the member's entries, and its row budget cuts the walk short. Each contact
/// still moves the member's cursor to where the walk got (`scanned_to`), so the replay
/// finishes over several contacts instead of repeating the first one for ever.
#[tokio::test]
async fn a_walk_cut_short_by_the_budget_still_finishes_over_contacts() {
    let f = fixture_with_tail(10, 0, 400);
    let c = f.c.node_id();
    TEST_REPLAY_EXAMINE_TIME.with(|t| t.set(Some(Duration::ZERO)));
    let mut contacts = 0;
    let mut advanced_each_time = true;
    while !f.catch_up.replay_answered(c) && contacts < 600 {
        let before = f.catch_up.replay_from(c, f.a.node_id());
        f.round().await.unwrap();
        contacts += 1;
        if !f.catch_up.replay_answered(c) && f.catch_up.replay_from(c, f.a.node_id()) <= before {
            advanced_each_time = false;
        }
    }
    TEST_REPLAY_EXAMINE_TIME.with(|t| t.set(None));
    assert!(f.catch_up.replay_answered(c), "answered within {contacts} contacts");
    assert!(advanced_each_time, "every contact that did not finish moved the cursor");
    assert!(contacts > 1, "the budget really cut the first walk short");
    assert_eq!(f.orders(), 2 + f.lost as u64, "every lost write is back");
}

/// **A page the member cannot apply in time ends the contact, not the member's
/// standing with the peer.** With A's writer held, the apply waits, C's two-second
/// wait for the ack runs out, and the contact ends quietly: no error comes back (so
/// no sync failure is counted and no back-off starts), `ack_timeout` is counted, and
/// the replay finishes on a later contact once the writer is free. A late ack would
/// desynchronise the stream, so the contact is over rather than waited on.
#[test]
fn a_member_that_cannot_apply_in_time_ends_the_contact_without_failing_it() {
    let f = fixture(10, 0);
    let c = f.c.node_id();
    let before = replay_counters().get(true, ReplayResult::AckTimeout);
    let (held, is_held) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let member = &f.a;
    let outcome = std::thread::scope(|scope| {
        // UNSUPERVISED: a scripted peer or racing applier in a test, joined below, whose panic fails it
        let watcher = scope.spawn(move || {
            let hold = member.hold_writer(kimmy_storage::WriterHolder::Bulk);
            held.send(()).unwrap();
            let _ = released.recv_timeout(Duration::from_secs(30));
            drop(hold);
        });
        is_held.recv().unwrap();
        let (mut ours, far) = tokio::io::duplex(MAX_FRAME);
        // The member's side runs on a thread of its own: its apply blocks on the
        // writer, and must not hold the dialler's timer with it.
        // UNSUPERVISED: a scripted peer or racing applier in a test, joined below, whose panic fails it
        let server = scope.spawn(|| {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(
                serve_peer(
                    &f.a,
                    far,
                    SECRET,
                    BINDING,
                    None,
                    ServeBudgets::serving(),
                    Some(&f.members),
                ),
            )
        });
        let mut stalls = PeerStalls::new();
        let outcome =
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(
                async {
                    open_handshake(&f.c, &mut ours, SECRET, BINDING).await.unwrap();
                    let outcome =
                        sync_over(&f.c, &mut ours, addr(), f.a.node_id(), None, &mut stalls).await;
                    drop(ours);
                    outcome
                },
            );
        release.send(()).unwrap();
        watcher.join().unwrap();
        let _ = server.join().unwrap();
        outcome
    });
    assert!(outcome.is_ok(), "the contact ended without an error: {outcome:?}");
    assert!(
        replay_counters().get(true, ReplayResult::AckTimeout) > before,
        "the missing ack was counted"
    );
    // The writer is free: a later contact finishes the replay.
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    for _ in 0..4 {
        if f.catch_up.replay_answered(c) {
            break;
        }
        rt.block_on(f.round()).unwrap();
    }
    assert!(f.catch_up.replay_answered(c), "a later contact finished it");
    assert_eq!(f.orders(), 2 + f.lost as u64);
}

/// This member's own stop, met while serving, ends the contact as a stop: neither the
/// peer's failure nor the round's (`ProtocolError::Stopping`), and nothing is sent.
#[tokio::test]
async fn a_stop_during_the_serve_ends_the_contact_as_a_stop() {
    let f = fixture(10, 0);
    f.c.set_stopping();
    let (mut ours, mut theirs) = tokio::io::duplex(MAX_FRAME);
    let from = f.catch_up.replay_from(f.c.node_id(), f.a.node_id());
    let served = serve_replay(
        &f.c,
        &mut ours,
        addr(),
        f.a.node_id(),
        &kimmy_core::VersionVector::new(),
        from,
    )
    .await;
    drop(ours);
    assert!(matches!(served, Err(ProtocolError::Stopping(_))), "{served:?}");
    assert_eq!(ServeFailure::of(&served.unwrap_err()), None, "a stop is no failure");
    assert!(read_frame(&mut theirs).await.is_err(), "no page was sent");
}

/// A frame as the wire carries it: the length, then the BSON.
fn framed(doc: &bson::Document) -> Vec<u8> {
    let body = bson::serialize_to_vec(doc).unwrap();
    let mut out = (body.len() as u32).to_be_bytes().to_vec();
    out.extend(body);
    out
}

/// Through the real decoder, an ask that is not a position reads as no ask: it costs
/// the field, never the frame, and a later well-formed ask still reads.
#[tokio::test]
async fn a_malformed_replay_from_decodes_as_absent_through_the_real_decoder() {
    let ask = Stamp::new(Hlc::new(9, 0), NodeId::from_bytes([4; 16]), Epoch::from_u64(3));
    let mut sent = bson::serialize_to_document(&Message::Vectors {
        servable: Default::default(),
        witnessed: Default::default(),
        facts: None,
        facts_gen: None,
        echo: None,
        replay_from: Some(ask),
    })
    .unwrap();
    let mut short_epoch = bson::serialize_to_document(&ask).unwrap();
    short_epoch.insert(
        "epoch",
        bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![1; 17] },
    );
    for bad in [
        bson::Bson::String("not a position".into()),
        bson::Bson::Int32(7),
        bson::Bson::Document(bson::doc! { "wall_ms": "x" }),
        bson::Bson::Array(vec![]),
        // An HLC alone, the form before a replay position was a stamp.
        bson::serialize_to_bson(&Hlc::new(9, 0)).unwrap(),
        // A stamp whose epoch is not 18 bytes.
        bson::Bson::Document(short_epoch),
    ] {
        sent.get_document_mut("Vectors").unwrap().insert("replay_from", bad.clone());
        let bytes = framed(&sent);
        match read_frame(&mut &bytes[..]).await.unwrap() {
            Message::Vectors { replay_from, .. } => {
                assert_eq!(replay_from, None, "{bad:?} reads as no ask")
            }
            other => panic!("{other:?}"),
        }
    }
    sent.get_document_mut("Vectors")
        .unwrap()
        .insert("replay_from", bson::serialize_to_bson(&ask).unwrap());
    let bytes = framed(&sent);
    assert!(matches!(
        read_frame(&mut &bytes[..]).await.unwrap(),
        Message::Vectors { replay_from: Some(h), .. } if h == ask
    ));
}

/// A build that predates the pages meets `ReplayEntries` as a variant it does not
/// know, and the real decoder says that is `Malformed` (a protocol error that ends the
/// contact), which is what the ask's being sent only to a peer that sent it makes
/// unreachable in practice. Proven here on the decoder, with a name no build knows.
#[tokio::test]
async fn a_frame_a_build_does_not_know_is_malformed_through_the_real_decoder() {
    let unknown = bson::doc! { "ReplayEntriesFromAFutureBuild": { "from": 1 } };
    let bytes = framed(&unknown);
    let err = read_frame(&mut &bytes[..]).await.unwrap_err();
    assert!(matches!(err, ProtocolError::Malformed(_)), "{err:?}");
    assert_eq!(ServeFailure::of(&err), Some(ServeFailure::Malformed));
}

/// A holder that has collected another origin's entries, which the member does not
/// hold, still answers the replay of the member's own: the horizon is judged for the
/// member's origin alone, so the member is not told its own writes are out of reach
/// because of someone else's.
#[tokio::test]
async fn another_origins_collected_entries_do_not_put_the_replay_out_of_reach() {
    let f = fixture_with_tail(10, 0, 50);
    let c = f.c.node_id();
    let backup_at = f.catch_up.replay_from(c, f.a.node_id());
    // A cutoff between the third origin's entries (just after the backup) and the
    // member's lost writes (fifteen milliseconds later).
    let now = backup_at.hlc.wall_ms + 6 + 1_000;
    f.c.collect_garbage_at(now, kimmy_storage::RetentionPolicy::new(1, 1)).unwrap();
    f.round().await.unwrap();
    assert!(f.catch_up.replay_answered(c), "answered, not put beyond the horizon");
    assert_eq!(f.orders(), 2 + f.lost as u64);
}

/// **The sync loop's own books, through a real listener.** C's loop dials A over TLS;
/// A's writer is held, so its apply of the replay page waits and C's two seconds for
/// the ack run out. The contact ends quietly: the tick's report counts no failed
/// round and no peer backing off, and the peer is not in back-off afterwards either:
/// the next tick dials it again at once, finds the writer free, and finishes the replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_missing_ack_is_no_failed_round_and_no_back_off_in_the_loop() {
    use crate::discovery::SeedSource;
    use crate::peers::{ReplicationConfig, RoundReport, replicate};

    let Fixture { a, c, catch_up, members, lost, _dirs } = fixture(10, 0);
    let (a, c) = (Arc::new(a), Arc::new(c));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a_addr = listener.local_addr().unwrap();
    let tls = Arc::new(crate::tls::ClusterTls::new().unwrap());
    // UNSUPERVISED: the member's listener in a test, aborted below; a panic in it fails the round the test makes
    let serving = tokio::spawn(serve_with(
        Arc::clone(&a),
        listener,
        SECRET.to_string(),
        None,
        None,
        None,
        tls,
        Some(members),
    ));

    // The member's writer is held until the first tick has reported.
    let (held, is_held) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let holder = Arc::clone(&a);
    // UNSUPERVISED: holds a writer for a test, released and joined below, and its panic fails the test
    let watcher = std::thread::spawn(move || {
        let hold = holder.hold_writer(kimmy_storage::WriterHolder::Bulk);
        held.send(()).unwrap();
        let _ = released.recv_timeout(Duration::from_secs(30));
        drop(hold);
    });
    is_held.recv().unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let mut config = ReplicationConfig::new(
        vec![SeedSource::Static(vec![a_addr])],
        SECRET.to_string(),
        "127.0.0.1:1".parse().unwrap(),
    );
    config.sync_interval = Duration::from_secs(1);
    config.discovery_interval = Duration::from_secs(600);
    config.members = Some(Members::default());
    config.on_round = Some(Arc::new(move |report| {
        let _ = tx.send(report);
    }));
    let before = replay_counters().get(true, ReplayResult::AckTimeout);
    // UNSUPERVISED: the dialler's loop in a test, aborted below; its reports are what the test reads
    let looping = tokio::spawn(replicate(Arc::clone(&c), config));
    async fn next(rx: &mut tokio::sync::mpsc::UnboundedReceiver<RoundReport>) -> RoundReport {
        tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("the loop ticks")
            .expect("it is running")
    }

    // The first tick that reached the peer: the ack timed out, and nothing failed.
    let mut report = next(&mut rx).await;
    while report.peers_known == 0 {
        report = next(&mut rx).await;
    }
    assert_eq!(report.failed, 0, "no failed round: {report:?}");
    assert_eq!(report.backing_off, 0, "no peer backing off: {report:?}");
    assert!(
        replay_counters().get(true, ReplayResult::AckTimeout) > before,
        "the missing ack was counted"
    );
    release.send(()).unwrap();
    watcher.join().unwrap();

    // Not in back-off: the next ticks dial it, and the replay finishes.
    for _ in 0..6 {
        if catch_up.replay_answered(c.node_id()) {
            break;
        }
        let report = next(&mut rx).await;
        assert_eq!(report.failed, 0, "{report:?}");
        assert_eq!(report.backing_off, 0, "{report:?}");
    }
    assert!(catch_up.replay_answered(c.node_id()), "a later contact finished it");
    let held_orders = a.get_collection("shop", "orders").unwrap();
    assert_eq!(a.count(&held_orders, kimmy_storage::WalkScope::Request).unwrap(), 2 + lost as u64);
    looping.abort();
    serving.abort();
}

/// What a test's subscriber saw: the level and message of each event.
#[derive(Clone, Default)]
struct Seen(Arc<std::sync::Mutex<Vec<(tracing::Level, String)>>>);

impl tracing::Subscriber for Seen {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::Id {
        tracing::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::Id, _: &tracing::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Message(String);
        impl tracing::field::Visit for Message {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut message = Message(String::new());
        event.record(&mut message);
        self.0.lock().unwrap().push((*event.metadata().level(), message.0));
    }
    fn enter(&self, _: &tracing::Id) {}
    fn exit(&self, _: &tracing::Id) {}
}

/// A replay of many pages from one peer says once that the peer holds writes the member
/// lost, and once that it is finished: the same for the pages a peer serves on its own
/// contact and the ones the member asks for.
#[tokio::test]
async fn a_replay_of_many_pages_is_reported_once_not_once_per_page() {
    let f = fixture(10, 0);
    let c = f.c.node_id();
    let from = f.catch_up.replay_from(c, f.a.node_id());
    let at = |n: u64| Hlc::new(from.hlc.wall_ms + n, 0);
    let pages: Vec<(Vec<OplogEntry>, bool, Stamp)> = vec![
        ((1..=3).map(|n| own_entry(&f, at(n), &format!("p{n}"))).collect(), false, pos(&f, at(3))),
        ((4..=6).map(|n| own_entry(&f, at(n), &format!("p{n}"))).collect(), false, pos(&f, at(6))),
        ((7..=9).map(|n| own_entry(&f, at(n), &format!("p{n}"))).collect(), true, pos(&f, at(9))),
    ];
    let seen = Seen::default();
    let log = Arc::clone(&seen.0);
    let _recording = tracing::subscriber::set_default(seen);
    // A callsite first hit on a thread with no subscriber is cached as uninteresting for
    // every thread; rebuild so this test's subscriber sees the events of a parallel run.
    tracing::callsite::rebuild_interest_cache();
    scripted(&f, |mut stream| async move {
        assert!(ask_versions(&mut stream).await.is_some());
        let mut cursor = from;
        for (entries, exhausted, scanned_to) in pages {
            write_frame(
                &mut stream,
                &Message::ReplayEntries {
                    from: cursor,
                    entries,
                    exhausted,
                    horizon: false,
                    scanned_to,
                },
            )
            .await
            .unwrap();
            match read_frame(&mut stream).await.unwrap() {
                Message::ReplayAck { next_from, .. } => cursor = next_from,
                other => panic!("{other:?}"),
            }
        }
    })
    .await
    .unwrap();
    let log = log.lock().unwrap();
    let said = |needle: &str, level: tracing::Level| {
        log.iter().filter(|(l, m)| *l == level && m.contains(needle)).count()
    };
    assert_eq!(
        said("holds writes this member made and no longer holds", tracing::Level::WARN),
        1,
        "three pages, one warning: {log:?}"
    );
    assert_eq!(
        said("replay of this member's own origin is finished", tracing::Level::INFO),
        1,
        "and one line for the end: {log:?}"
    );
    // A later episode (a new arming) says it again.
    f.catch_up.arm_replay(f.a.own_position_at_open(), None).unwrap();
    assert!(f.catch_up.replay_first_loss_from(c), "armed afresh, it is the first again");
}

/// The serving side of the replay matches the asker **by node, across epochs**: the
/// writes a restored member lost sit in its old epoch, while its vector also names
/// the new one it writes under now. The server holds every other node's origins at
/// `Hlc::MAX` and every epoch of the asker's node where `from` leaves it, so the page it
/// serves holds exactly the asker's old-epoch writes and nothing of the third node's.
///
/// What this test pins is the `held` vector: `ours` is read at transport.rs:2485 and
/// every origin of another node is put at `Hlc::MAX` in `held` at transport.rs:2495, so
/// with the node match broken there the old-epoch writes are not served.
///
/// It does not reach the final `entry.stamp.node == their_node` filter on the served
/// entries (transport.rs:2542), and that filter is **not** redundant. `ours` and the
/// walk are separate reads: the version vector is read in its own read transaction
/// (`Engine::read_versions`, kimmy-storage engine.rs:2903, which begins and ends within
/// that call) and the walk of `serve_entries_to_peer` (transport.rs:2526) opens another
/// (kimmy-storage sync.rs:998, in `entries_for_peer_counting`). An entry of an origin
/// that another contact applies to the server between the two is in the walk's snapshot
/// and not in `held`, so only the filter keeps it off the page. A single-task test
/// cannot interleave an apply between those two calls, so none reaches it.
#[tokio::test]
async fn the_replay_a_peer_serves_holds_every_epoch_of_the_askers_node_and_no_other() {
    let f = fixture_with_tail(5, 0, 3);
    let theirs = f.a.version_vector().unwrap();
    let new_origin = f.a.origin();
    let epochs_of_a = theirs.iter().filter(|(o, _)| o.node == f.a.node_id()).count();
    assert!(
        epochs_of_a >= 2 && theirs.get(new_origin) > Hlc::ZERO,
        "premise: the asker's vector names its old epoch and the one it writes under now"
    );
    let from = f.a.own_position_at_open();
    let held_by_server = f.c.version_vector().unwrap();
    assert!(
        held_by_server.iter().any(|(o, _)| o.node != f.a.node_id()),
        "premise: the server holds a third node's origin"
    );
    assert_eq!(
        held_by_server.get(new_origin),
        Hlc::ZERO,
        "premise: the server lacks the new epoch"
    );

    let (mut server_end, mut client_end) = tokio::io::duplex(MAX_FRAME);
    let asked = crate::catchup::replay_after(from, f.a.node_id());
    let serving = serve_replay(&f.c, &mut server_end, addr(), f.a.node_id(), &theirs, asked);
    let asking = async {
        let Message::ReplayEntries { entries, exhausted, horizon, scanned_to, .. } =
            read_frame(&mut client_end).await.unwrap()
        else {
            panic!("a page of the replay")
        };
        write_frame(
            &mut client_end,
            &Message::ReplayAck { next_from: scanned_to, done: exhausted, stalled: None },
        )
        .await
        .unwrap();
        (entries, exhausted, horizon)
    };
    let (served, (entries, exhausted, horizon)) = tokio::join!(serving, asking);
    served.unwrap();
    assert!(!horizon && exhausted, "the whole answer in one page");
    assert_eq!(entries.len(), 5, "the five writes the backup lacks: {entries:?}");
    assert!(
        entries.iter().all(|e| e.stamp.node == f.a.node_id() && e.stamp.hlc > from),
        "only the asker's node, above where it asked"
    );
    assert!(
        entries.iter().all(|e| e.stamp.origin() != new_origin),
        "and they are the old epoch's writes, not the new one's"
    );
}

/// Two lost writes of A's node at one HLC under two of its earlier epochs, put on C after `before`
/// lost writes of `big` bytes each: the shape a restored member's clock behind its lost writes
/// gives (Q7 Step 1 review, F3). Returns the fixture and the two ids.
fn tied_fixture(before: i64, big: usize, tied_big: usize) -> (Fixture, [String; 2]) {
    let f = fixture(before, big);
    let orders = f.c.get_collection("shop", "orders").unwrap();
    let node = f.a.node_id();
    let old =
        f.a.version_vector()
            .unwrap()
            .iter()
            .map(|(origin, _)| origin)
            .find(|origin| origin.node == node && *origin != f.a.origin())
            .expect("premise: A's vector names the epoch the backup was written under");
    let other = Epoch::new(Hlc::new(1, 0), [9; 8]);
    assert_ne!(other, old.epoch);
    // Above every lost write C holds and below anything A writes later.
    let tie = Hlc::new(kimmy_storage::physical_now_ms() + 1_000, 0);
    let pad = "y".repeat(tied_big);
    let ids = [format!("tied-{}", 0), format!("tied-{}", 1)];
    let entries: Vec<OplogEntry> = [other, old.epoch]
        .into_iter()
        .zip(&ids)
        .map(|(epoch, id)| OplogEntry {
            stamp: Stamp::new(tie, node, epoch),
            kind: OpKind::Insert,
            collection: orders.id,
            doc_id: Some(kimmy_core::DocId::String(id.clone())),
            body: Some(bson::serialize_to_vec(&bson::doc! { "_id": id, "pad": &pad }).unwrap()),
        })
        .collect();
    for entry in &entries {
        f.c.apply_batch(std::slice::from_ref(entry)).unwrap();
    }
    assert_eq!(f.c.read_oplog_from(tie, 10, kimmy_storage::WalkScope::Request).unwrap().len(), 2);
    (f, ids)
}

impl Fixture {
    fn holds(&self, id: &str) -> bool {
        let held = self.a.get_collection("shop", "orders").unwrap();
        self.a.get(&held, &kimmy_core::DocId::String(id.into())).unwrap().is_some()
    }
}

/// Two epochs of A's node at one HLC in one page of the replay C serves: the page is in
/// stamp order, not refused as out of order, and both writes come back.
#[tokio::test]
async fn a_page_with_two_epochs_of_the_members_node_at_one_hlc_is_applied_whole() {
    let (f, ids) = tied_fixture(5, 0, 0);
    let c = f.c.node_id();
    let mut contacts = 0;
    let mut errors = Vec::new();
    while !f.catch_up.replay_answered(c) && contacts < 5 {
        if let Err(e) = f.round().await {
            errors.push(e.to_string());
        }
        contacts += 1;
    }
    assert!(f.catch_up.replay_answered(c), "answered within {contacts} contacts: {errors:?}");
    assert!(ids.iter().all(|id| f.holds(id)), "both tied writes are back: {errors:?}");
    assert_eq!(f.orders(), 2 + 5 + 2);
}

/// The same tie with a page boundary between the two, on the replay C serves on its own
/// contact: entries of 14 MiB, four to a frame, three before the tie, so the holder's first
/// page holds the three and the first of the tie. The second is after the cursor the first left,
/// and is served.
#[tokio::test]
async fn a_page_boundary_between_two_epochs_at_one_hlc_drops_neither_inbound() {
    const BIG: usize = 14 * 1024 * 1024;
    let (f, ids) = tied_fixture(3, BIG, BIG);
    let c = f.c.node_id();
    TEST_REPLAY_SERVE_TIME.with(|t| t.set(Some(Duration::from_secs(3600))));
    let mut contacts = 0;
    let mut errors = Vec::new();
    while !f.catch_up.replay_answered(c) && contacts < 10 {
        if let Err(e) = f.round().await {
            errors.push(e.to_string());
        }
        contacts += 1;
    }
    TEST_REPLAY_SERVE_TIME.with(|t| t.set(None));
    assert!(f.catch_up.replay_answered(c), "answered within {contacts} contacts: {errors:?}");
    let held: Vec<bool> = ids.iter().map(|id| f.holds(id)).collect();
    assert_eq!(
        held,
        [true, true],
        "both tied writes are back after {contacts} contacts: {errors:?}"
    );
}

/// The same tie on the replay A asks for itself (`replay_own_origin`), with a page boundary
/// between the two: entries of 14 MiB, four to a frame, three before the tie. The page holds
/// the three and the first of the tie, and the next ask must still be served the second.
#[tokio::test]
async fn a_page_boundary_between_two_epochs_at_one_hlc_drops_neither_outbound() {
    const BIG: usize = 14 * 1024 * 1024;
    let (f, ids) = tied_fixture(3, BIG, BIG);
    let c = f.c.node_id();
    let theirs = f.c.version_vector().unwrap();
    let mut contacts = 0;
    while !f.catch_up.replay_answered(c) && contacts < 10 {
        let (mut ours, served) = tokio::io::duplex(MAX_FRAME);
        let serving =
            serve_peer(&f.c, served, SECRET, BINDING, None, ServeBudgets::serving(), None);
        let asking = async {
            open_handshake(&f.a, &mut ours, SECRET, BINDING).await.unwrap();
            let done = replay_own_origin(
                &f.a,
                &mut ours,
                addr(),
                c,
                &theirs,
                &f.catch_up,
                Instant::now() + Duration::from_secs(3600),
                &RoundClock::default(),
            )
            .await;
            drop(ours);
            done
        };
        let (_, done) = tokio::join!(serving, asking);
        done.unwrap();
        contacts += 1;
    }
    assert!(f.catch_up.replay_answered(c), "answered within {contacts} contacts");
    let held: Vec<bool> = ids.iter().map(|id| f.holds(id)).collect();
    assert_eq!(held, [true, true], "both tied writes are back after {contacts} contacts");
}

/// A replay cursor leaves each epoch of its node where a window that ends `Through` it leaves it
/// (`coverage_up_to`): an epoch at or before the cursor's read through its HLC, one after it
/// through the HLC before, so the walk the `held` drives serves an entry of a later epoch at
/// the cursor's HLC. The floor as a cursor holds every epoch at the floor.
#[test]
fn the_held_a_replay_cursor_names_is_the_coverage_of_a_window_through_it() {
    let me = NodeId::from_bytes([5; 16]);
    let epochs = [Epoch::from_u64(1), Epoch::from_u64(2), Epoch::from_u64(3)];
    let tie = Hlc::new(1_000, 4);
    let mut theirs = kimmy_core::VersionVector::new();
    for epoch in epochs {
        theirs.insert(kimmy_core::Origin::new(me, epoch), Hlc::new(2_000, 0));
    }
    for cursor in [
        Stamp::new(tie, me, epochs[0]),
        Stamp::new(tie, me, epochs[1]),
        Stamp::new(tie, me, epochs[2]),
        crate::catchup::replay_after(tie, me),
    ] {
        let window = kimmy_storage::sync::coverage_up_to(
            &theirs,
            kimmy_storage::sync::WindowEnd::Through(cursor),
        );
        for epoch in epochs {
            let origin = kimmy_core::Origin::new(me, epoch);
            assert_eq!(
                replay_held_at(origin, cursor),
                window.get(origin),
                "{epoch:?} under the cursor {cursor:?}"
            );
            assert_eq!(
                Stamp::new(tie, me, epoch) > cursor,
                replay_held_at(origin, cursor) < tie,
                "an entry at the cursor's HLC is served exactly when it is after the cursor"
            );
        }
        assert!(holds_after(&theirs, cursor), "every epoch holds more above the tie");
    }
    let mut at_tie = kimmy_core::VersionVector::new();
    at_tie.insert(kimmy_core::Origin::new(me, epochs[2]), tie);
    assert!(holds_after(&at_tie, Stamp::new(tie, me, epochs[1])), "a later epoch at the tie");
    assert!(!holds_after(&at_tie, Stamp::new(tie, me, epochs[2])), "read through it");
    assert!(!holds_after(&at_tie, crate::catchup::replay_after(tie, me)));
}

/// **Decode is the trust boundary.** Every replay position on the wire is a whole stamp: a
/// page or an ack whose position is not one (an HLC alone, an epoch of the wrong length) is
/// refused by the real decoder as malformed, with nothing applied; it cannot reach the cursor.
#[tokio::test]
async fn a_replay_position_that_is_not_a_stamp_is_refused_by_the_decoder() {
    let node = NodeId::from_bytes([4; 16]);
    let at = Stamp::new(Hlc::new(9, 0), node, Epoch::from_u64(3));
    let frames = [
        (
            "ReplayEntries",
            "from",
            Message::ReplayEntries {
                from: at,
                entries: Vec::new(),
                exhausted: false,
                horizon: false,
                scanned_to: at,
            },
        ),
        (
            "ReplayEntries",
            "scanned_to",
            Message::ReplayEntries {
                from: at,
                entries: Vec::new(),
                exhausted: false,
                horizon: false,
                scanned_to: at,
            },
        ),
        (
            "ReplayAck",
            "next_from",
            Message::ReplayAck { next_from: at, done: false, stalled: None },
        ),
    ];
    for (variant, field, frame) in frames {
        let good = bson::serialize_to_document(&frame).unwrap();
        assert!(read_frame(&mut &framed(&good)[..]).await.is_ok(), "premise: {variant} reads");
        let mut short_epoch = bson::serialize_to_document(&at).unwrap();
        short_epoch.insert(
            "epoch",
            bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![1; 17] },
        );
        for bad in [
            bson::serialize_to_bson(&Hlc::new(9, 0)).unwrap(),
            bson::Bson::Document(short_epoch),
            bson::Bson::String("not a position".into()),
        ] {
            let mut sent = good.clone();
            sent.get_document_mut(variant).unwrap().insert(field, bad.clone());
            let read = read_frame(&mut &framed(&sent)[..]).await;
            assert!(
                matches!(read, Err(ProtocolError::Malformed(_))),
                "{variant}.{field} = {bad:?}: {read:?}"
            );
        }
    }
}

/// A replay position that decodes and names another member is refused where it is used: an
/// ack of a page that names a position of another node than the one being served ends the
/// serve as malformed, and so does an ask on `Vectors` for a replay of another node's origin.
#[tokio::test]
async fn a_replay_position_of_another_member_is_refused() {
    let f = fixture(30, 0);
    let from = crate::catchup::replay_after(Hlc::new(1, 0), f.a.node_id());
    let (mut ours, mut theirs) = tokio::io::duplex(MAX_FRAME);
    // UNSUPERVISED: a scripted peer or racing applier in a test, joined below, whose panic fails it
    let scripted = tokio::spawn(async move {
        if let Ok(Message::ReplayEntries { scanned_to, .. }) = read_frame(&mut theirs).await {
            let elsewhere = Stamp { node: NodeId::from_bytes([8; 16]), ..scanned_to };
            write_frame(
                &mut theirs,
                &Message::ReplayAck { next_from: elsewhere, done: false, stalled: None },
            )
            .await
            .unwrap();
        }
    });
    let served = serve_replay(
        &f.c,
        &mut ours,
        addr(),
        f.a.node_id(),
        &kimmy_core::VersionVector::default(),
        from,
    )
    .await;
    drop(ours);
    scripted.await.unwrap();
    assert!(
        matches!(&served, Err(ProtocolError::Malformed(why)) if why.contains("not the peer's own")),
        "{served:?}"
    );

    // The ask: A's `Vectors` asks C for a replay from a position of a third node.
    let (ours, mut theirs) = tokio::io::duplex(MAX_FRAME);
    let third = Stamp::new(Hlc::new(1, 0), NodeId::from_bytes([8; 16]), Epoch::ZERO);
    // UNSUPERVISED: a scripted peer or racing applier in a test, joined below, whose panic fails it
    let answering = tokio::spawn(async move {
        while let Ok(frame) = read_frame(&mut theirs).await {
            if let Message::AskVersions { .. } = frame {
                let vectors = Message::Vectors {
                    servable: Default::default(),
                    witnessed: Default::default(),
                    facts: None,
                    facts_gen: None,
                    echo: None,
                    replay_from: Some(third),
                };
                write_frame(&mut theirs, &vectors).await.unwrap();
            }
        }
    });
    let mut stalls = PeerStalls::new();
    let outcome = sync_over(&f.c, ours, addr(), f.a.node_id(), None, &mut stalls).await;
    answering.await.unwrap();
    assert!(
        matches!(&outcome, Err(ProtocolError::Malformed(why)) if why.contains("not the peer's own")),
        "{outcome:?}"
    );
}

/// The holder's walk is cut by its budget at a row of another node at the HLC of A's two tied
/// entries, which sorts just before them: the page is empty, and the walk examined nothing of
/// A's at that HLC. The position the page leaves must be where that row leaves A's node, the HLC
/// before, so both tied entries are still served; read as "past that HLC in every epoch" it
/// would pass over them.
#[tokio::test]
async fn a_budget_cut_just_before_two_epochs_at_one_hlc_drops_neither() {
    // No lost write before the tie, so the page the budget cuts holds no entry.
    let (f, ids) = tied_fixture(0, 0, 0);
    let c = f.c.node_id();
    let orders = f.c.get_collection("shop", "orders").unwrap();
    let tie =
        f.c.read_oplog_from(Hlc::ZERO, usize::MAX, kimmy_storage::WalkScope::Request).unwrap();
    let tie = tie.iter().find(|e| e.doc_id == Some(kimmy_core::DocId::String(ids[0].clone())));
    let tie = tie.expect("the first tied entry").stamp.hlc;
    let lowest = NodeId::from_bytes([0; 16]);
    assert!(lowest < f.a.node_id(), "premise: the other node's row sorts before A's at the tie");
    f.c.apply_batch(&[OplogEntry {
        stamp: Stamp::new(tie, lowest, Epoch::ZERO),
        kind: OpKind::Insert,
        collection: orders.id,
        doc_id: Some(kimmy_core::DocId::String("lowest".into())),
        body: Some(bson::serialize_to_vec(&bson::doc! { "_id": "lowest" }).unwrap()),
    }])
    .unwrap();
    // The walk starts at the replay's floor, and the budget is the rows up to and including
    // the other node's row at the tie, so the cut lands on it.
    let floor = f.catch_up.replay_from(c, f.a.node_id()).hlc;
    let rows = f.c.read_oplog_from(floor, usize::MAX, kimmy_storage::WalkScope::Request).unwrap();
    let cut = rows.iter().position(|e| e.stamp.node == lowest).expect("the other node's row");
    assert_eq!(rows[cut + 1].stamp.hlc, tie, "premise: A's tied entries follow it: {rows:?}");
    assert!(
        rows[..cut].iter().all(|e| e.stamp.hlc == floor),
        "premise: nothing the walk would serve comes before the cut: {rows:?}"
    );
    TEST_REPLAY_EXAMINE_ROWS.with(|r| r.set(Some(cut as u64 + 1)));
    TEST_REPLAY_SERVE_TIME.with(|t| t.set(Some(Duration::from_secs(3600))));
    let mut contacts = 0;
    let mut errors = Vec::new();
    while !f.catch_up.replay_answered(c) && contacts < 5 {
        if let Err(e) = f.round().await {
            errors.push(e.to_string());
        }
        contacts += 1;
    }
    TEST_REPLAY_EXAMINE_ROWS.with(|r| r.set(None));
    TEST_REPLAY_SERVE_TIME.with(|t| t.set(None));
    assert!(f.catch_up.replay_answered(c), "answered within {contacts} contacts: {errors:?}");
    let held: Vec<bool> = ids.iter().map(|id| f.holds(id)).collect();
    assert_eq!(
        held,
        [true, true],
        "both tied writes are back after {contacts} contacts: {errors:?}"
    );
}

/// Where a walk that examined through `p` leaves the node it serves: every entry of that node
/// at or below `p` in stamp order is before the position, and every one after `p` is after it.
#[test]
fn the_position_a_cut_walk_leaves_is_exact_in_stamp_order() {
    let node = NodeId::from_bytes([5; 16]);
    let at = Hlc::new(1_000, 3);
    let rows = [
        Stamp::new(at, NodeId::from_bytes([1; 16]), Epoch::ZERO),
        Stamp::new(at, node, Epoch::from_u64(1)),
        Stamp::new(at, node, Epoch::from_u64(2)),
        Stamp::new(at, NodeId::from_bytes([9; 16]), Epoch::MAX),
    ];
    let mine = [
        Stamp::new(at.predecessor(), node, Epoch::MAX),
        Stamp::new(at, node, Epoch::ZERO),
        Stamp::new(at, node, Epoch::from_u64(1)),
        Stamp::new(at, node, Epoch::from_u64(2)),
        Stamp::new(at, node, Epoch::MAX),
        Stamp::new(at.successor(), node, Epoch::ZERO),
    ];
    for p in rows {
        let position = replay_position_through(p, node);
        assert_eq!(position.node, node, "a position of the served node");
        for entry in mine {
            assert_eq!(entry > position, entry > p, "{entry:?} against the cut at {p:?}");
        }
    }
}

/// A `Vectors` answer whose `replay_from` is not a replay position is read as no ask: the
/// dialler's round goes on (failing it would fail every round with that peer, and the pull with
/// it), and the dialler says so once for that peer, not once a round. A later answer with a
/// well-formed ask is served as before.
#[tokio::test]
async fn a_malformed_replay_from_is_no_ask_said_once_per_peer_and_the_round_goes_on() {
    use tokio::io::AsyncWriteExt;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    let their = NodeId::generate();
    let seen = Seen::default();
    let log = Arc::clone(&seen.0);
    let _recording = tracing::subscriber::set_default(seen);
    tracing::callsite::rebuild_interest_cache();

    let vectors = |replay_from: bson::Bson| {
        let mut doc = bson::serialize_to_document(&Message::Vectors {
            servable: Default::default(),
            witnessed: Default::default(),
            facts: None,
            facts_gen: None,
            echo: None,
            replay_from: None,
        })
        .unwrap();
        doc.get_document_mut("Vectors").unwrap().insert("replay_from", replay_from);
        doc
    };
    // A peer that answers the round with `answer` for its vectors and serves nothing; returns
    // how many replay pages it was sent.
    async fn peer(mut stream: tokio::io::DuplexStream, answer: bson::Document) -> usize {
        let mut pages = 0;
        while let Ok(frame) = read_frame(&mut stream).await {
            match frame {
                Message::AskVersions { .. } => stream.write_all(&framed(&answer)).await.unwrap(),
                Message::ReplayEntries { scanned_to, .. } => {
                    pages += 1;
                    let ack =
                        Message::ReplayAck { next_from: scanned_to, done: true, stalled: None };
                    write_frame(&mut stream, &ack).await.unwrap();
                }
                Message::AskDivergence { .. } => {
                    let none = Message::Divergence {
                        collections: Vec::new(),
                        probe_count: None,
                        incarnations: Vec::new(),
                        digests: None,
                        digest_busy: false,
                        definitions: None,
                    };
                    write_frame(&mut stream, &none).await.unwrap();
                }
                other => panic!("an empty peer is asked nothing else: {other:?}"),
            }
        }
        pages
    }
    let round = |answer: bson::Document| {
        let engine = &engine;
        async move {
            let (ours, theirs) = tokio::io::duplex(MAX_FRAME);
            // UNSUPERVISED: a scripted peer or racing applier in a test, joined below, whose panic fails it
            let answering = tokio::spawn(peer(theirs, answer));
            let mut stalls = PeerStalls::new();
            let outcome = sync_over(engine, ours, addr(), their, None, &mut stalls).await;
            (outcome, answering.await.unwrap())
        }
    };

    let bad = bson::Bson::String("not a position".into());
    for n in 0..2 {
        let (outcome, pages) = round(vectors(bad.clone())).await;
        assert!(outcome.is_ok(), "round {n} goes on: {outcome:?}");
        assert_eq!(pages, 0, "round {n}: no ask, so no replay is served");
    }
    let said = log
        .lock()
        .unwrap()
        .iter()
        .filter(|(level, m)| *level == tracing::Level::WARN && m.contains("not a replay position"))
        .count();
    assert_eq!(said, 1, "said once for the peer across two rounds: {:?}", log.lock().unwrap());

    let good =
        bson::serialize_to_bson(&crate::catchup::replay_after(Hlc::new(1, 0), their)).unwrap();
    let (outcome, pages) = round(vectors(good)).await;
    assert!(outcome.is_ok(), "{outcome:?}");
    assert_eq!(pages, 1, "a well-formed ask is served as before");
}
