//! The replay a restored member's peer serves on the contact the peer opened
//! (ADR-212's addendum): the ask on `Vectors`, the pages and their acks, what the
//! member accepts and refuses, and that the ordinary round goes on.

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
    fixture_of(lost, big, tail, false)
}

/// The scope of the idempotency key the `i`th lost write is made under, when
/// they are keyed.
fn lost_scope(i: i64) -> kimmy_core::ScopeHash {
    let mut scope = [0u8; 32];
    scope[..8].copy_from_slice(&i.to_be_bytes());
    kimmy_core::ScopeHash(scope)
}

fn lost_key(i: i64) -> kimmy_storage::KeyedWrite {
    kimmy_storage::KeyedWrite::new(lost_scope(i), kimmy_core::Fingerprint([7; 32]))
}

/// [`fixture_with_tail`]; with `keyed`, each lost write is a keyed insert (under
/// [`lost_key`]), and C pulled it with its record.
fn fixture_of(lost: i64, big: usize, tail: usize, keyed: bool) -> Fixture {
    let writer_dir = tempfile::tempdir().unwrap();
    let writer = Engine::open(&writer_dir.path().join("kimmy.redb")).unwrap();
    let orders = writer.create_collection("shop", "orders").unwrap();
    writer.insert_many(&orders, (0..2).map(|i| bson::doc! { "_id": i }).collect()).unwrap();
    let mut backup = Vec::new();
    writer.backup_to(&mut backup, kimmy_storage::WalkScope::Request).unwrap();
    let backup_at = writer.version_vector().unwrap().get(writer.node_id());
    // The lost writes are stamped after the third origin's entries.
    std::thread::sleep(Duration::from_millis(15));
    let pad = "x".repeat(big);
    if keyed {
        for i in 2..2 + lost {
            let doc = bson::doc! { "_id": i, "pad": pad.clone() };
            let kimmy_storage::Keyed::Fresh(_) =
                writer.insert_keyed(&orders, doc, &lost_key(i)).unwrap()
            else {
                panic!("a fresh key")
            };
        }
    } else {
        for chunk in (2..2 + lost).collect::<Vec<_>>().chunks(100) {
            writer
                .insert_many(
                    &orders,
                    chunk.iter().map(|i| bson::doc! { "_id": *i, "pad": pad.clone() }).collect(),
                )
                .unwrap();
        }
    }
    let c_dir = tempfile::tempdir().unwrap();
    let c = Engine::open(&c_dir.path().join("kimmy.redb")).unwrap();
    let mut from = Hlc::ZERO;
    loop {
        let window =
            writer.entries_for_peer(from, MAX_BATCH, kimmy_storage::WalkScope::Background).unwrap();
        c.apply_batch_carrying(&window.entries, &window.records, kimmy_storage::RecordsVia::Pull)
            .unwrap();
        if window.exhausted {
            break;
        }
        from = window.entries.last().unwrap().stamp.hlc;
    }
    if tail > 0 {
        let third = NodeId::generate();
        let entries: Vec<OplogEntry> = (0..tail)
            .map(|k| OplogEntry {
                stamp: Stamp::new(Hlc::new(backup_at.wall_ms + 1, k as u16), third),
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

/// C's servable vector, as a round reads it.
async fn ask_versions_servable(
    stream: &mut tokio::io::DuplexStream,
) -> Option<kimmy_core::VersionVector> {
    write_frame(
        stream,
        &Message::AskVersions { witnessed: true, facts: None, facts_gen: None, echo: None },
    )
    .await
    .ok()?;
    match read_frame(stream).await.ok()? {
        Message::Vectors { servable, .. } => Some(servable),
        _ => None,
    }
}

async fn ask_versions(stream: &mut tokio::io::DuplexStream) -> Option<Hlc> {
    write_frame(
        stream,
        &Message::AskVersions { witnessed: true, facts: None, facts_gen: None, echo: None },
    )
    .await
    .unwrap();
    match read_frame(stream).await.unwrap() {
        Message::Vectors { replay_from, .. } => replay_from,
        other => panic!("{other:?}"),
    }
}

/// An entry of A's origin above `hlc`, in a collection A has, to build pages by hand.
fn own_entry(f: &Fixture, hlc: Hlc, id: &str) -> OplogEntry {
    let orders = f.a.get_collection("shop", "orders").unwrap();
    OplogEntry {
        stamp: Stamp::new(hlc, f.a.node_id()),
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

/// A replay brings back the records of the keyed writes it brings back (ADR-240): a
/// member restored from a backup that lacks its own keyed writes takes them
/// from C with their records, so a retry on it is answered from the record its
/// own first application wrote rather than applied a second time.
#[tokio::test]
async fn a_replay_brings_back_the_records_of_the_keyed_writes_it_restores() {
    let f = fixture_of(40, 0, 0, true);
    let orders = f.a.get_collection("shop", "orders").unwrap();
    assert_eq!(f.a.idempotency_record(orders.id, &lost_scope(2)).unwrap(), None, "premise");
    let replayed = f.a.idempotency_records_received()[2];
    for _ in 0..8 {
        f.round().await.unwrap();
        if f.catch_up.replay_answered(f.c.node_id()) {
            break;
        }
    }
    assert!(f.catch_up.replay_answered(f.c.node_id()), "the replay is answered");
    assert_eq!(f.a.idempotency_records_received()[2], replayed + 40, "counted, by replay");
    for i in 2..2 + f.lost {
        let doc = bson::doc! { "_id": i, "pad": "" };
        match f.a.insert_keyed(&orders, doc, &lost_key(i)).unwrap() {
            kimmy_storage::Keyed::Replayed(record) => {
                assert_eq!(record.origin, f.a.node_id(), "its own first application")
            }
            kimmy_storage::Keyed::Fresh(_) => panic!("the retry of {i} applied again"),
        }
    }
}

/// `$inc` of `n`, for G5.
struct IncN;

impl kimmy_storage::ModifySpec for IncN {
    fn matches(&self, doc: &bson::Document) -> kimmy_core::Result<bool> {
        Ok(doc.contains_key("n"))
    }
    fn compare(&self, _: &bson::Document, _: &bson::Document) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
    fn apply(&self, doc: &bson::Document) -> Result<Option<bson::Document>, String> {
        let mut next = doc.clone();
        next.insert("n", doc.get_i64("n").map_err(|e| e.to_string())? + 1);
        Ok(Some(next))
    }
    fn upsert(&self) -> Option<Result<bson::Document, String>> {
        None
    }
}

/// `to` pulls everything `from` holds that it lacks, window by window, records and all.
fn pull_all(to: &Engine, from: &Engine) {
    for _ in 0..16 {
        let theirs = from.version_vector().unwrap();
        let mine = to.witnessed_vector().unwrap();
        let Some(start) = mine.behind(&theirs) else { return };
        let window = from
            .entries_for_peer_holding(
                start,
                MAX_BATCH,
                Some(&mine),
                kimmy_storage::WalkScope::Background,
            )
            .unwrap();
        let mut outcome = SyncOutcome::default();
        to.apply_peer_window_carrying_into(
            &theirs,
            Some(&mine),
            &window.entries,
            &window.records,
            kimmy_storage::RecordsVia::Pull,
            kimmy_storage::PeerWindow {
                scanned_to: window.scanned_to,
                exhausted: window.exhausted,
                passed_through: None,
                asked_partial: false,
            },
            &mut outcome,
        )
        .unwrap();
        if window.exhausted {
            return;
        }
    }
}

/// G5: a member restored from a backup that lacks its own keyed `$inc` and the
/// write it made on top, replayed by a peer whose copy of the `$inc` lost there
/// (C wrote the document in between) and which holds the write on top. C's oplog
/// has no entry at the `$inc`'s stamp, so the page carries only the write on top;
/// the record rides the page all the same, by the row C holds inside the page's
/// own advance, and a retry on the restored member is answered from it rather
/// than applied over the write on top.
#[tokio::test]
async fn g5_a_replay_from_a_peer_whose_copy_of_the_keyed_entry_lost_carries_its_record() {
    let writer_dir = tempfile::tempdir().unwrap();
    let writer = Engine::open(&writer_dir.path().join("kimmy.redb")).unwrap();
    let orders = writer.create_collection("shop", "orders").unwrap();
    writer.insert(&orders, bson::doc! { "_id": 1i64, "n": 10i64 }).unwrap();
    let c_dir = tempfile::tempdir().unwrap();
    let c = Engine::open(&c_dir.path().join("kimmy.redb")).unwrap();
    pull_all(&c, &writer);
    let mut backup = Vec::new();
    writer.backup_to(&mut backup, kimmy_storage::WalkScope::Request).unwrap();
    std::thread::sleep(Duration::from_millis(15));

    let k = lost_key(1);
    let kimmy_storage::Keyed::Fresh(_) = writer
        .modify_where_keyed(&orders, &kimmy_storage::Candidates::Scan, &IncN, Some(1), &k)
        .unwrap()
    else {
        panic!("a fresh key")
    };
    let s1 = writer.idempotency_record(orders.id, &lost_scope(1)).unwrap().unwrap().first;
    let corders = c.get_collection("shop", "orders").unwrap();
    let later = |ms: u64| Stamp::new(Hlc::new(s1.hlc.wall_ms + ms, 0), NodeId::from_bytes([0; 16]));
    c.advance_clock_past(&later(10));
    c.replace(&corders, &kimmy_core::DocId::Int64(1), bson::doc! { "n": 100i64 }, false).unwrap();
    writer.advance_clock_past(&later(20));
    writer
        .replace(
            &orders,
            &kimmy_core::DocId::Int64(1),
            bson::doc! { "n": 11i64, "tag": "w" },
            false,
        )
        .unwrap();
    pull_all(&c, &writer);
    let held = |engine: &Engine| {
        let coll = engine.get_collection("shop", "orders").unwrap();
        engine.get(&coll, &kimmy_core::DocId::Int64(1)).unwrap().unwrap().get_i64("n").unwrap()
    };
    assert_eq!(held(&c), 11, "premise: the write on top won on C");
    assert!(!c.has_oplog_entry(&s1).unwrap(), "premise: the $inc lost on C");
    assert!(c.idempotency_record(corders.id, &lost_scope(1)).unwrap().is_some(), "premise");

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
    let f = Fixture {
        a,
        c,
        catch_up,
        members,
        lost: 0,
        _dirs: vec![writer_dir, c_dir, a_dir, marker_dir],
    };
    for _ in 0..8 {
        f.round().await.unwrap();
        if f.catch_up.replay_answered(f.c.node_id()) {
            break;
        }
    }
    assert!(f.catch_up.replay_answered(f.c.node_id()), "the replay is answered");
    assert_eq!(held(&f.a), 11, "the restored member holds the write on top");
    let aorders = f.a.get_collection("shop", "orders").unwrap();
    let retry =
        f.a.modify_where_keyed(&aorders, &kimmy_storage::Candidates::Scan, &IncN, Some(1), &k)
            .unwrap();
    assert!(matches!(retry, kimmy_storage::Keyed::Replayed(_)), "{retry:?}");
    assert_eq!(held(&f.a), 11, "applied once");
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
    let cursor = f.catch_up.replay_from(c);
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
    let from = f.catch_up.replay_from(f.c.node_id());
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
                records: Vec::new(),
                replay_records: None,
            },
        )
        .await
        .unwrap();
    })
    .await;
    assert!(matches!(r, Err(ProtocolError::Malformed(_))), "{r:?}");

    let at = |n: u64| Hlc::new(from.wall_ms + n, 0);
    let pages: Vec<(&str, Message)> = vec![
        (
            "a different start",
            Message::ReplayEntries {
                from: at(1),
                entries: Vec::new(),
                exhausted: true,
                horizon: false,
                scanned_to: at(1),
                records: Vec::new(),
                replay_records: None,
            },
        ),
        (
            "scanned to before its own last entry",
            Message::ReplayEntries {
                from,
                entries: vec![own_entry(&f, at(5), "a")],
                exhausted: false,
                horizon: false,
                scanned_to: at(4),
                records: Vec::new(),
                replay_records: None,
            },
        ),
        (
            "out of order",
            Message::ReplayEntries {
                from,
                entries: vec![own_entry(&f, at(5), "b"), own_entry(&f, at(4), "a")],
                exhausted: false,
                horizon: false,
                scanned_to: at(9),
                records: Vec::new(),
                replay_records: None,
            },
        ),
        (
            "at or below where it began",
            Message::ReplayEntries {
                from,
                entries: vec![own_entry(&f, from, "a")],
                exhausted: false,
                horizon: false,
                scanned_to: at(9),
                records: Vec::new(),
                replay_records: None,
            },
        ),
        (
            "another origin's entry",
            Message::ReplayEntries {
                from,
                entries: vec![OplogEntry {
                    stamp: Stamp::new(at(5), NodeId::generate()),
                    ..own_entry(&f, at(5), "a")
                }],
                exhausted: false,
                horizon: false,
                scanned_to: at(9),
                records: Vec::new(),
                replay_records: None,
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
                scanned_to: at(MAX_BATCH as u64 + 9),
                records: Vec::new(),
                replay_records: None,
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
    assert!(replay_counters().get(true, ReplayResult::Refused) >= refused_before + 6);
}

/// **An empty page that is not the end answers nothing.** A budget cut or a batch
/// cut to fit a frame says nothing of what lies beyond it; only `exhausted` finishes.
#[tokio::test]
async fn an_empty_page_that_is_not_the_end_does_not_finish_the_replay() {
    let f = fixture(5, 0);
    let c = f.c.node_id();
    let from = f.catch_up.replay_from(c);
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
                records: Vec::new(),
                replay_records: None,
            },
        )
        .await
        .unwrap();
        match read_frame(&mut stream).await.unwrap() {
            Message::ReplayAck { next_from, done, stalled, .. } => {
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
                records: Vec::new(),
                replay_records: None,
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
    let from = f.catch_up.replay_from(c);
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
                records: Vec::new(),
                replay_records: None,
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
    let backup_at = writer.version_vector().unwrap().get(writer.node_id());
    let lost = OplogEntry {
        stamp: Stamp::new(Hlc::new(backup_at.wall_ms + 1, 0), writer.node_id()),
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
    let frames = vec![
        Message::ReplayEntries {
            from: Hlc::new(3, 1),
            entries: Vec::new(),
            exhausted: true,
            horizon: false,
            scanned_to: Hlc::new(3, 1),
            records: Vec::new(),
            replay_records: None,
        },
        Message::ReplayAck {
            next_from: Hlc::new(4, 0),
            done: false,
            stalled: None,
            replay_records: None,
        },
        Message::ReplayAck {
            next_from: Hlc::new(4, 0),
            done: false,
            stalled: Some(ReplayStall::PurgePending),
            replay_records: None,
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
        replay_from: Some(Hlc::new(9, 0)),
        replay_records: None,
    };
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
        replay_records: None,
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
    Message::ReplayEntries {
        from: Hlc::ZERO,
        entries: Vec::new(),
        exhausted: true,
        horizon: false,
        scanned_to: Hlc::ZERO,
        records: Vec::new(),
        replay_records: None,
    }
}

/// The dialer's side, against a scripted answerer: it ends its replay for the
/// contact when an ack does not advance, however the answerer words it.
#[tokio::test]
async fn an_ack_that_does_not_advance_ends_the_replay_for_the_contact() {
    // More than one page, so that a dialler that kept going would send a second.
    let f = fixture(MAX_BATCH as i64 * 2 + 5, 0);
    let (mut ours, mut theirs) = tokio::io::duplex(MAX_FRAME);
    let from = Hlc::new(1, 0);
    // UNSUPERVISED: a scripted peer or racing applier in a test, joined below, whose panic fails it
    let scripted = tokio::spawn(async move {
        let mut pages = 0usize;
        while let Ok(Message::ReplayEntries { from, .. }) = read_frame(&mut theirs).await {
            pages += 1;
            // A constant ack: the position it was asked from, never moving.
            write_frame(
                &mut theirs,
                &Message::ReplayAck {
                    next_from: from,
                    done: false,
                    stalled: None,
                    replay_records: None,
                },
            )
            .await
            .unwrap();
        }
        pages
    });
    // The dialer holds the whole tail; the peer (A) is the origin it serves.
    let theirs_vector = kimmy_core::VersionVector::default();
    let served = serve_replay(&f.c, &mut ours, addr(), f.a.node_id(), &theirs_vector, from, None)
        .await
        .unwrap();
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
        Hlc::new(1, 0),
        None,
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
                        serve_replay(&f.c, &mut ours, addr(), f.a.node_id(), &theirs, asked, None)
                            .await;
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
    let from = f.catch_up.replay_from(c);
    let window =
        f.c.entries_for_peer(from, MAX_BATCH, kimmy_storage::WalkScope::Background).unwrap();
    let page: Vec<_> =
        window.entries.iter().filter(|e| e.stamp.node == f.a.node_id()).cloned().collect();
    assert!(!page.is_empty());
    let apply = |via| {
        replay_apply(&f.a, &f.catch_up, &"c", c, from, page.clone(), &[], from, true, via).unwrap()
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
    let from = f.catch_up.replay_from(c);
    let at = |n: u64| Hlc::new(from.wall_ms + n, 0);
    let page = vec![own_entry(&f, at(1), "p1"), own_entry(&f, at(2), "p2")];
    scripted(&f, |mut stream| async move {
        assert!(ask_versions(&mut stream).await.is_some());
        write_frame(
            &mut stream,
            &Message::ReplayEntries {
                from,
                entries: page,
                exhausted: false,
                horizon: false,
                scanned_to: at(2),
                records: Vec::new(),
                replay_records: None,
            },
        )
        .await
        .unwrap();
        match read_frame(&mut stream).await.unwrap() {
            Message::ReplayAck { next_from, done, stalled, .. } => {
                assert_eq!(next_from, at(2), "the ack names the last entry applied");
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
        let before = f.catch_up.replay_from(c);
        f.round().await.unwrap();
        contacts += 1;
        if !f.catch_up.replay_answered(c) && f.catch_up.replay_from(c) <= before {
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
    let from = f.catch_up.replay_from(f.c.node_id());
    let served = serve_replay(
        &f.c,
        &mut ours,
        addr(),
        f.a.node_id(),
        &kimmy_core::VersionVector::new(),
        from,
        None,
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
    let mut sent = bson::serialize_to_document(&Message::Vectors {
        servable: Default::default(),
        witnessed: Default::default(),
        facts: None,
        facts_gen: None,
        echo: None,
        replay_from: Some(Hlc::new(9, 0)),
        replay_records: None,
    })
    .unwrap();
    for bad in [
        bson::Bson::String("not a position".into()),
        bson::Bson::Int32(7),
        bson::Bson::Document(bson::doc! { "wall_ms": "x" }),
        bson::Bson::Array(vec![]),
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
        .insert("replay_from", bson::serialize_to_bson(&Hlc::new(9, 0)).unwrap());
    let bytes = framed(&sent);
    assert!(matches!(
        read_frame(&mut &bytes[..]).await.unwrap(),
        Message::Vectors { replay_from: Some(h), .. } if h == Hlc::new(9, 0)
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
    let backup_at = f.catch_up.replay_from(c);
    // A cutoff between the third origin's entries (just after the backup) and the
    // member's lost writes (fifteen milliseconds later).
    let now = backup_at.wall_ms + 6 + 1_000;
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
    let from = f.catch_up.replay_from(c);
    let at = |n: u64| Hlc::new(from.wall_ms + n, 0);
    let pages: Vec<(Vec<OplogEntry>, bool, Hlc)> = vec![
        ((1..=3).map(|n| own_entry(&f, at(n), &format!("p{n}"))).collect(), false, at(3)),
        ((4..=6).map(|n| own_entry(&f, at(n), &format!("p{n}"))).collect(), false, at(6)),
        ((7..=9).map(|n| own_entry(&f, at(n), &format!("p{n}"))).collect(), true, at(9)),
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
                    records: Vec::new(),
                    replay_records: None,
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
    // The member logs the end after it has acked the last page, which the script
    // returns on: wait for the line rather than race the task that writes it.
    for _ in 0..500 {
        let finished = log.lock().unwrap().iter().any(|(l, m)| {
            *l == tracing::Level::INFO
                && m.contains("replay of this member's own origin is finished")
        });
        if finished {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
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

// -- B2 and m1 of the B5 build review: what a replay page carries, both ways -----------------

impl Fixture {
    /// One round the other way: A dials C, and its own round asks C for the replay
    /// of its origin (`replay_own_origin`, the outbound way), over the real serve
    /// and sync code.
    async fn round_outbound(&self) -> Result<SyncOutcome, ProtocolError> {
        let (mut ours, far) = tokio::io::duplex(MAX_FRAME);
        let serving = async {
            let _ = serve_peer(&self.c, far, SECRET, BINDING, None, ServeBudgets::serving(), None)
                .await;
        };
        let mut stalls = PeerStalls::new();
        stalls.set_catch_up(Some(Arc::clone(&self.catch_up)));
        let asking = async {
            open_handshake(&self.a, &mut ours, SECRET, BINDING).await.unwrap();
            let outcome =
                sync_over(&self.a, &mut ours, addr(), self.c.node_id(), None, &mut stalls).await;
            drop(ours);
            outcome
        };
        let ((), outcome) = tokio::join!(serving, asking);
        outcome
    }

    /// The outbound replay alone, and not the ordinary pull its round goes on
    /// to: A dials C, reads C's vectors and asks for the replay of its origin
    /// (`replay_own_origin`), so what A holds after is what the replay brought.
    async fn replay_outbound_only(&self) -> Result<(), ProtocolError> {
        self.replay_outbound_for(Duration::from_secs(20)).await
    }

    /// [`Self::replay_outbound_only`], its deadline `time` after it starts: a zero
    /// one asks for one page a contact.
    async fn replay_outbound_for(&self, time: Duration) -> Result<(), ProtocolError> {
        let (mut ours, far) = tokio::io::duplex(MAX_FRAME);
        let serving = async {
            let _ = serve_peer(&self.c, far, SECRET, BINDING, None, ServeBudgets::serving(), None)
                .await;
        };
        let asking = async {
            open_handshake(&self.a, &mut ours, SECRET, BINDING).await.unwrap();
            let theirs = match ask_versions_servable(&mut ours).await {
                Some(theirs) => theirs,
                None => return Err(ProtocolError::Malformed("no vectors".into())),
            };
            let deadline = Instant::now() + time;
            let replayed = replay_own_origin(
                &self.a,
                &mut ours,
                addr(),
                self.c.node_id(),
                &theirs,
                &self.catch_up,
                deadline,
            )
            .await;
            drop(ours);
            replayed
        };
        let ((), replayed) = tokio::join!(serving, asking);
        replayed
    }

    /// The outbound replay alone, contact after contact, until C has answered it,
    /// at most eight.
    async fn replay_outbound_only_through(&self) {
        for _ in 0..8 {
            self.replay_outbound_only().await.unwrap();
            if self.catch_up.replay_answered(self.c.node_id()) {
                return;
            }
        }
        panic!("the outbound replay is not answered after eight contacts");
    }

    /// Rounds one way until C has answered A's replay, at most eight.
    async fn replay_through(&self, outbound: bool) {
        for _ in 0..8 {
            let round = if outbound { self.round_outbound().await } else { self.round().await };
            round.unwrap();
            if self.catch_up.replay_answered(self.c.node_id()) {
                return;
            }
        }
        panic!("the replay is not answered after eight rounds");
    }
}

/// The value of `n` in document `id` of `shop.orders` on `engine`.
fn n_of(engine: &Engine, id: i64) -> i64 {
    let coll = engine.get_collection("shop", "orders").unwrap();
    engine.get(&coll, &kimmy_core::DocId::Int64(id)).unwrap().unwrap().get_i64("n").unwrap()
}

/// G5 with the record's rows at another origin (B2 of the build review). Q's keyed
/// update-many writes chunk 1 (d1, d2); before Q's chunk 2, the client's retry
/// runs on R, which lacks Q's record: a fresh attempt writes d1..d4 in one chunk.
/// Q's chunk 2 then writes d3, d4 later. The merged record's `first` (Q's first
/// chunk) and `last` (Q's second) are both of Q's origin. R pulls Q, and C pulls R:
/// C holds R's entries and the record, with no row of R's origin. R is restored
/// from a backup that predates all of it, and C replays R's own entries to it.
/// With `f3`, R also writes d3, d4 after Q's chunk 2, so none of Q's entries is
/// in R's oplog: C holds Q's rows above its servable vector, and the page carries
/// the record by F3 (X4 of the build review's M1).
fn other_origins_record(f3: bool) -> (Fixture, kimmy_storage::KeyedWrite) {
    let writer_dir = tempfile::tempdir().unwrap();
    let writer = Arc::new(Engine::open(&writer_dir.path().join("kimmy.redb")).unwrap());
    let mut q_dirs = Vec::new();
    // R is the writer that is later restored; Q must sort before it.
    let q = loop {
        let dir = tempfile::tempdir().unwrap();
        let q = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        q_dirs.push(dir);
        if q.node_id() < writer.node_id() {
            break Arc::new(q);
        }
    };
    let orders = writer.create_collection("shop", "orders").unwrap();
    for i in 1..=4i64 {
        writer.insert(&orders, bson::doc! { "_id": i, "n": 0i64 }).unwrap();
    }
    pull_all(&q, &writer);
    let c_dir = tempfile::tempdir().unwrap();
    let c = Engine::open(&c_dir.path().join("kimmy.redb")).unwrap();
    pull_all(&c, &writer);
    let mut backup = Vec::new();
    writer.backup_to(&mut backup, kimmy_storage::WalkScope::Request).unwrap();
    std::thread::sleep(Duration::from_millis(15));

    let k = lost_key(1);
    let qorders = q.get_collection("shop", "orders").unwrap();
    q.set_multi_chunk_docs(2);
    let (w1, q1) = (Arc::clone(&writer), Arc::clone(&q));
    q.before_next_continuing_write(move || {
        let worders = w1.get_collection("shop", "orders").unwrap();
        let first_q = q1.idempotency_record(worders.id, &k.scope).unwrap().unwrap().first;
        w1.advance_clock_past(&Stamp::new(
            Hlc::new(first_q.hlc.wall_ms + 5, 0),
            NodeId::from_bytes([0; 16]),
        ));
        let kimmy_storage::Keyed::Fresh(out) = w1
            .modify_where_keyed(&worders, &kimmy_storage::Candidates::Scan, &IncN, None, &k)
            .unwrap()
        else {
            panic!("R lacks Q's record: a fresh attempt")
        };
        assert_eq!(out.modified, 4, "{out:?}");
        let last_r = w1.idempotency_record(worders.id, &k.scope).unwrap().unwrap().last;
        q1.advance_clock_past(&Stamp::new(
            Hlc::new(last_r.hlc.wall_ms + 5, 0),
            NodeId::from_bytes([0; 16]),
        ));
    });
    let kimmy_storage::Keyed::Fresh(out) =
        q.modify_where_keyed(&qorders, &kimmy_storage::Candidates::Scan, &IncN, None, &k).unwrap()
    else {
        panic!("a fresh key on Q")
    };
    assert_eq!(out.modified, 4, "{out:?}");
    if f3 {
        let q_last = q.idempotency_record(qorders.id, &k.scope).unwrap().unwrap().last;
        writer.advance_clock_past(&Stamp::new(
            Hlc::new(q_last.hlc.wall_ms + 5, 0),
            NodeId::from_bytes([0; 16]),
        ));
        for id in [3i64, 4] {
            writer
                .replace(&orders, &kimmy_core::DocId::Int64(id), bson::doc! { "n": 7i64 }, false)
                .unwrap();
        }
    }
    pull_all(&writer, &q);
    let merged = writer.idempotency_record(orders.id, &k.scope).unwrap().unwrap();
    assert_eq!(merged.first.node, q.node_id(), "premise: first is Q's: {merged:?}");
    assert_eq!(merged.last.node, q.node_id(), "premise: last is Q's: {merged:?}");
    if f3 {
        let q_last = q.idempotency_record(qorders.id, &k.scope).unwrap().unwrap().last;
        assert!(!writer.has_oplog_entry(&q_last).unwrap(), "premise: Q's entries lost on R");
    }
    pull_all(&c, &writer);
    for id in 1..=2 {
        assert_eq!(n_of(&c, id), 1, "premise: once on C");
    }
    let corders = c.get_collection("shop", "orders").unwrap();
    assert!(c.idempotency_record(corders.id, &k.scope).unwrap().is_some(), "premise: C holds it");

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
    let mut dirs = vec![writer_dir, c_dir, a_dir, marker_dir];
    dirs.extend(q_dirs);
    (Fixture { a, c, catch_up, members, lost: 0, _dirs: dirs }, k)
}

/// After the replay, the restored member holds the request's effects with its
/// record, and a retry there replays rather than applying again.
fn the_restored_member_holds_the_effect_with_its_record(
    f: &Fixture,
    k: &kimmy_storage::KeyedWrite,
) {
    let effects: Vec<i64> = (1..=4).map(|id| n_of(&f.a, id)).collect();
    assert!(effects.contains(&1), "premise: the replay brought effects back: {effects:?}");
    let aorders = f.a.get_collection("shop", "orders").unwrap();
    let record = f.a.idempotency_record(aorders.id, &k.scope).unwrap();
    let retry =
        f.a.modify_where_keyed(&aorders, &kimmy_storage::Candidates::Scan, &IncN, None, k).unwrap();
    let after: Vec<i64> = (1..=4).map(|id| n_of(&f.a, id)).collect();
    assert!(
        record.is_some() && after == effects,
        "the restored member holds the request's effect {effects:?} with record {record:?}; \
         the retry answered {retry:?} and the documents are now {after:?}"
    );
}

/// B2, the inbound way: C serves the replay on the contact it opened, and the
/// page carries the record of the page's collection whose request spans it.
#[tokio::test]
async fn b2_an_inbound_replay_carries_a_record_with_no_row_of_the_requesters_origin() {
    let (f, k) = other_origins_record(false);
    f.replay_through(false).await;
    the_restored_member_holds_the_effect_with_its_record(&f, &k);
}

/// B2, the outbound way: A dials C and asks for its replay (`AskEntries` with
/// `replay`), and the answer carries the record the same way.
#[tokio::test]
async fn b2_an_outbound_replay_carries_a_record_with_no_row_of_the_requesters_origin() {
    let (f, k) = other_origins_record(false);
    // The replay alone: the ordinary pull that follows it in a round would bring
    // Q's entries, and the record with them, by coverage.
    f.replay_outbound_only_through().await;
    the_restored_member_holds_the_effect_with_its_record(&f, &k);
}

/// X4 of the build review's M1: the F3 half of a replay page. C holds Q's rows
/// above its servable vector, so the record rides the page by F3.
#[tokio::test]
async fn a_replay_page_carries_a_record_its_sender_cannot_vouch_for() {
    let (f, k) = other_origins_record(true);
    f.replay_through(false).await;
    the_restored_member_holds_the_effect_with_its_record(&f, &k);
}

/// Plant on `engine` a row of `idempotency_by_stamp` at `stamp` naming a key with
/// no record, as a retention pass stopped between chunks leaves one, through a
/// frame's store of a record and the record's removal.
fn plant_orphan_row(engine: &Engine, stamp: Stamp) -> kimmy_core::ScopeHash {
    let orders = engine.get_collection("shop", "orders").unwrap();
    let scope = kimmy_core::ScopeHash([0x0f; 32]);
    let record = kimmy_storage::IdempotencyRecord {
        fingerprint: kimmy_core::Fingerprint([0x0f; 32]),
        first: stamp,
        last: stamp,
        origin: stamp.node,
        state: kimmy_storage::RecordState::Done,
        contested: false,
        outcome: kimmy_storage::RecordOutcome::Insert { stamp },
    };
    let carried = kimmy_storage::CarriedRecord {
        collection: orders.id,
        scope,
        carrying: Some(stamp),
        record: record.encode(),
    };
    engine.apply_batch_carrying(&[], &[carried], kimmy_storage::RecordsVia::Pull).unwrap();
    assert!(engine.idempotency_record(orders.id, &scope).unwrap().is_some(), "premise: stored");
    assert!(engine.remove_idempotency_record_for_test(orders.id, &scope).unwrap(), "premise");
    scope
}

/// An orphan row inside a replay page's range (its record gone, as a retention
/// pass stopped between chunks leaves it) is passed over without error and ships
/// nothing, on both ways.
async fn an_orphan_row_inside_the_page_ships_nothing(outbound: bool) {
    let f = fixture_of(6, 0, 0, true);
    let from = f.a.own_position_at_open();
    let orders = f.c.get_collection("shop", "orders").unwrap();
    let at = Stamp::new(Hlc::new(from.wall_ms + 1, 0), f.a.node_id());
    let scope = plant_orphan_row(&f.c, at);
    f.replay_through(outbound).await;
    assert_eq!(f.orders(), 2 + 6, "every lost write is back");
    let aorders = f.a.get_collection("shop", "orders").unwrap();
    assert_eq!(
        f.a.idempotency_record(aorders.id, &scope).unwrap(),
        None,
        "the orphan shipped nothing"
    );
    assert_eq!(aorders.id, orders.id);
    for i in 2..8 {
        assert!(f.a.idempotency_record(aorders.id, &lost_scope(i)).unwrap().is_some(), "{i}");
    }
}

#[tokio::test]
async fn an_orphan_row_inside_an_inbound_replay_page_ships_nothing() {
    an_orphan_row_inside_the_page_ships_nothing(false).await;
}

#[tokio::test]
async fn an_orphan_row_inside_an_outbound_replay_page_ships_nothing() {
    an_orphan_row_inside_the_page_ships_nothing(true).await;
}

/// A frame's budget for this test thread, put back when dropped.
struct EntryBudget;

impl EntryBudget {
    fn set(bytes: usize) -> Self {
        TEST_ENTRY_BUDGET.with(|budget| budget.set(Some(bytes)));
        EntryBudget
    }
}

impl Drop for EntryBudget {
    fn drop(&mut self) {
        TEST_ENTRY_BUDGET.with(|budget| budget.set(None));
    }
}

/// `count` records on C of requests that span the replay's start (`first` at or
/// below it, `last` above it, both of A's origin, at stamps with no entry), each
/// a `find_and_modify` record carrying a document of about a kilobyte: what
/// every page of the replay carries at its `first`, so all ride its first entry.
fn plant_spanning_records(f: &Fixture, count: u16) -> Vec<kimmy_core::ScopeHash> {
    let from = f.a.own_position_at_open();
    let orders = f.c.get_collection("shop", "orders").unwrap();
    let node = f.a.node_id();
    let pad = "p".repeat(1_000);
    (0..count)
        .map(|k| {
            let first = Stamp::new(Hlc::new(from.wall_ms - 1, k), node);
            let last = Stamp::new(Hlc::new(from.wall_ms + 1, k), node);
            let mut scope = [0x5a; 32];
            scope[30..].copy_from_slice(&k.to_be_bytes());
            let scope = kimmy_core::ScopeHash(scope);
            let record = kimmy_storage::IdempotencyRecord {
                fingerprint: kimmy_core::Fingerprint([0x5a; 32]),
                first,
                last,
                origin: node,
                state: kimmy_storage::RecordState::Done,
                contested: false,
                outcome: kimmy_storage::RecordOutcome::FindAndModify {
                    matched: true,
                    modified: true,
                    removed: false,
                    upserted_id: None,
                    stamp: Some(last),
                    document: Some(bson::doc! { "pad": pad.clone() }),
                },
            };
            let carried = kimmy_storage::CarriedRecord {
                collection: orders.id,
                scope,
                carrying: Some(first),
                record: record.encode(),
            };
            f.c.apply_batch_carrying(&[], &[carried], kimmy_storage::RecordsVia::Pull).unwrap();
            scope
        })
        .collect()
}

/// m1 of the build review, and the residual its ruling did not accept: the
/// records a replay's first entry needs, all of requests that span the replay's
/// start (carried at a `first` at or below it, so no page of records alone can
/// move the position past them), fill more than one page. They cross on pages of
/// records alone at the same position, each going on after the last key the one
/// before sent, and then the entries follow: the replay is answered, with every
/// record and every lost write.
async fn the_records_at_the_replay_start_cross_on_several_pages(outbound: bool) {
    let f = fixture(5, 0);
    let scopes = plant_spanning_records(&f, 40);
    let _budget = EntryBudget::set(8 * 1024);
    TEST_REPLAY_PAGES_SENT.with(|n| n.set(0));
    f.replay_through(outbound).await;
    assert_eq!(f.orders(), 2 + 5, "every lost write is back");
    let orders = f.a.get_collection("shop", "orders").unwrap();
    for scope in &scopes {
        assert!(f.a.idempotency_record(orders.id, scope).unwrap().is_some(), "every record");
    }
    // Nothing was left unfit: the replay is answered with every record, which a
    // page that carried nothing would have stopped (the process-wide counter is
    // shared with the tests that do leave a page unfit, in parallel).
    if !outbound {
        let pages = TEST_REPLAY_PAGES_SENT.with(std::cell::Cell::get);
        assert!(pages >= 5, "pages of records alone, then the entries: {pages}");
    }
}

#[tokio::test]
async fn the_records_at_an_inbound_replays_start_cross_on_several_pages() {
    the_records_at_the_replay_start_cross_on_several_pages(false).await;
}

#[tokio::test]
async fn the_records_at_an_outbound_replays_start_cross_on_several_pages() {
    the_records_at_the_replay_start_cross_on_several_pages(true).await;
}

/// B2's records ride at their `first`: a replay page that stops between a
/// request's effects and its `last` stores the record with the effects. As
/// [`other_origins_record`], with R writing into a collection Q created after
/// the backup, between its attempt and Q's chunk 2, so the record's `last` (Q's
/// chunk 2) lies past an entry the restored member cannot take yet: the page
/// stops there, at a collection it lacks. Carried at `last`, the record would be
/// stored with nothing the stopped batch took; carried at `first`, it rides with
/// the effects.
#[tokio::test]
async fn a_replay_page_that_stops_between_the_effects_and_last_stores_the_record() {
    let writer_dir = tempfile::tempdir().unwrap();
    let writer = Arc::new(Engine::open(&writer_dir.path().join("kimmy.redb")).unwrap());
    let mut q_dirs = Vec::new();
    let q = loop {
        let dir = tempfile::tempdir().unwrap();
        let q = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        q_dirs.push(dir);
        if q.node_id() < writer.node_id() {
            break Arc::new(q);
        }
    };
    let orders = writer.create_collection("shop", "orders").unwrap();
    for i in 1..=4i64 {
        writer.insert(&orders, bson::doc! { "_id": i, "n": 0i64 }).unwrap();
    }
    pull_all(&q, &writer);
    let c_dir = tempfile::tempdir().unwrap();
    let c = Engine::open(&c_dir.path().join("kimmy.redb")).unwrap();
    pull_all(&c, &writer);
    let mut backup = Vec::new();
    writer.backup_to(&mut backup, kimmy_storage::WalkScope::Request).unwrap();
    std::thread::sleep(Duration::from_millis(15));
    // A collection the restored member will not have heard of.
    q.create_collection("shop", "x").unwrap();
    pull_all(&writer, &q);

    let k = lost_key(1);
    let qorders = q.get_collection("shop", "orders").unwrap();
    q.set_multi_chunk_docs(2);
    let (w1, q1) = (Arc::clone(&writer), Arc::clone(&q));
    q.before_next_continuing_write(move || {
        let worders = w1.get_collection("shop", "orders").unwrap();
        let first_q = q1.idempotency_record(worders.id, &k.scope).unwrap().unwrap().first;
        w1.advance_clock_past(&Stamp::new(
            Hlc::new(first_q.hlc.wall_ms + 5, 0),
            NodeId::from_bytes([0; 16]),
        ));
        let kimmy_storage::Keyed::Fresh(_) = w1
            .modify_where_keyed(&worders, &kimmy_storage::Candidates::Scan, &IncN, None, &k)
            .unwrap()
        else {
            panic!("R lacks Q's record: a fresh attempt")
        };
        let wx = w1.get_collection("shop", "x").unwrap();
        w1.insert(&wx, bson::doc! { "_id": 1i64 }).unwrap();
        let stop = w1.version_vector().unwrap().get(w1.node_id());
        q1.advance_clock_past(&Stamp::new(
            Hlc::new(stop.wall_ms + 5, 0),
            NodeId::from_bytes([0; 16]),
        ));
    });
    let kimmy_storage::Keyed::Fresh(_) =
        q.modify_where_keyed(&qorders, &kimmy_storage::Candidates::Scan, &IncN, None, &k).unwrap()
    else {
        panic!("a fresh key on Q")
    };
    pull_all(&writer, &q);
    pull_all(&c, &writer);
    let a_dir = tempfile::tempdir().unwrap();
    let path = a_dir.path().join("kimmy.redb");
    kimmy_storage::backup::restore(&path, &mut backup.as_slice()).unwrap();
    let a = Engine::open(&path).unwrap();
    a.create_collection("shop", "startup").unwrap();
    assert!(a.get_collection("shop", "x").is_err(), "premise: the restored member lacks x");
    let marker_dir = tempfile::tempdir().unwrap();
    let catch_up = CatchUp::open(marker_dir.path(), Duration::from_secs(120));
    catch_up.arm_replay(a.own_position_at_open(), None).unwrap();
    let members = Members::default();
    members.set_catch_up(Arc::clone(&catch_up));
    let mut dirs = vec![writer_dir, c_dir, a_dir, marker_dir];
    dirs.extend(q_dirs);
    let f = Fixture { a, c, catch_up, members, lost: 0, _dirs: dirs };
    let stalled = replay_counters().get(true, ReplayResult::Stalled);
    for _ in 0..2 {
        f.round().await.unwrap();
    }
    assert!(!f.catch_up.replay_answered(f.c.node_id()), "premise: the page stops at x");
    assert!(replay_counters().get(true, ReplayResult::Stalled) > stalled, "premise: stalled");
    the_restored_member_holds_the_effect_with_its_record(&f, &k);
}

/// One pull of `from` by `to` over the real serve and sync code.
async fn pull_round(to: &Engine, from: &Engine) -> Result<SyncOutcome, ProtocolError> {
    let (mut ours, far) = tokio::io::duplex(MAX_FRAME);
    let serving = async {
        let _ = serve_peer(from, far, SECRET, BINDING, None, ServeBudgets::serving(), None).await;
    };
    let mut stalls = PeerStalls::new();
    let asking = async {
        open_handshake(to, &mut ours, SECRET, BINDING).await.unwrap();
        let outcome = sync_over(to, &mut ours, addr(), from.node_id(), None, &mut stalls).await;
        drop(ours);
        outcome
    };
    let ((), outcome) = tokio::join!(serving, asking);
    outcome
}

/// m1 of the build review, on a pull: the records riding a window's first entry
/// overfill a frame with it. Before, the answer was `fits: 0`, which the
/// requester reads as an entry that can never replicate, every round. Now it is a
/// partial window of records alone that ends before the first record left out:
/// the requester stores those, its coverage moves up to them, and the next window
/// carries the rest, then the entry.
#[tokio::test]
async fn a_pull_whose_first_entry_cannot_cross_with_its_records_takes_them_alone_first() {
    let s_dir = tempfile::tempdir().unwrap();
    let s = Engine::open(&s_dir.path().join("kimmy.redb")).unwrap();
    let orders = s.create_collection("shop", "orders").unwrap();
    let m_dir = tempfile::tempdir().unwrap();
    let m = Engine::open(&m_dir.path().join("kimmy.redb")).unwrap();
    pull_all(&m, &s);
    let me = s.node_id();
    let base = s.version_vector().unwrap().get(me);
    let pad = "p".repeat(1_000);
    let scopes: Vec<_> = (0..40u16)
        .map(|k| {
            let at = Stamp::new(Hlc::new(base.wall_ms + 1, k), me);
            let record = kimmy_storage::IdempotencyRecord {
                fingerprint: kimmy_core::Fingerprint([4; 32]),
                first: at,
                last: at,
                origin: me,
                state: kimmy_storage::RecordState::Done,
                contested: false,
                outcome: kimmy_storage::RecordOutcome::FindAndModify {
                    matched: true,
                    modified: true,
                    removed: false,
                    upserted_id: None,
                    stamp: Some(at),
                    document: Some(bson::doc! { "pad": pad.clone() }),
                },
            };
            let mut scope = [4u8; 32];
            scope[30..].copy_from_slice(&k.to_be_bytes());
            let scope = kimmy_core::ScopeHash(scope);
            let carried = kimmy_storage::CarriedRecord {
                collection: orders.id,
                scope,
                carrying: Some(at),
                record: record.encode(),
            };
            s.apply_batch_carrying(&[], &[carried], kimmy_storage::RecordsVia::Pull).unwrap();
            scope
        })
        .collect();
    s.insert(&orders, bson::doc! { "_id": 99i64, "n": 0i64 }).unwrap();
    let unfit = records_unfit().get(UnfitPath::Pull);
    let _budget = EntryBudget::set(8 * 1024);
    let morders = m.get_collection("shop", "orders").unwrap();
    let mut rounds = 0;
    while m.get(&morders, &kimmy_core::DocId::Int64(99)).unwrap().is_none() {
        rounds += 1;
        assert!(rounds <= 16, "the entry never crossed");
        pull_round(&m, &s).await.unwrap();
    }
    assert!(rounds >= 3, "windows of records alone came first: {rounds}");
    for scope in &scopes {
        assert!(m.idempotency_record(morders.id, scope).unwrap().is_some(), "every record");
    }
    assert_eq!(records_unfit().get(UnfitPath::Pull), unfit, "nothing was left unfit");
}

/// The records cursor outlives a contact: a peer serving two pages a contact
/// inbound, and a member asking one page a contact outbound, still carry the
/// records at the replay's start over several contacts, each going on where the
/// last left off, rather than starting the position's records over each time.
#[tokio::test]
async fn the_records_at_an_inbound_replays_start_cross_over_several_contacts() {
    let f = fixture(5, 0);
    let scopes = plant_spanning_records(&f, 30);
    let _budget = EntryBudget::set(8 * 1024);
    TEST_REPLAY_MAX_PAGES.with(|cap| cap.set(Some(2)));
    let mut contacts = 0;
    while !f.catch_up.replay_answered(f.c.node_id()) {
        contacts += 1;
        assert!(contacts <= 8, "the replay is not answered after eight contacts");
        f.round().await.unwrap();
    }
    TEST_REPLAY_MAX_PAGES.with(|cap| cap.set(None));
    assert!(contacts >= 3, "premise: the records took more than one contact: {contacts}");
    let orders = f.a.get_collection("shop", "orders").unwrap();
    for scope in &scopes {
        assert!(f.a.idempotency_record(orders.id, scope).unwrap().is_some(), "every record");
    }
    assert_eq!(f.orders(), 2 + 5, "every lost write is back");
}

#[tokio::test]
async fn the_records_at_an_outbound_replays_start_cross_over_several_contacts() {
    let f = fixture(5, 0);
    let scopes = plant_spanning_records(&f, 30);
    let _budget = EntryBudget::set(8 * 1024);
    let mut contacts = 0;
    while !f.catch_up.replay_answered(f.c.node_id()) {
        contacts += 1;
        assert!(contacts <= 8, "the replay is not answered after eight contacts");
        f.replay_outbound_for(Duration::ZERO).await.unwrap();
    }
    assert!(contacts >= 3, "premise: the records took more than one contact: {contacts}");
    let orders = f.a.get_collection("shop", "orders").unwrap();
    for scope in &scopes {
        assert!(f.a.idempotency_record(orders.id, scope).unwrap().is_some(), "every record");
    }
    assert_eq!(f.orders(), 2 + 5, "every lost write is back");
}

/// A record on C, changed now (a frame's store logs it), of `bytes` padding: a
/// `find_and_modify` record of A's origin inside the replay's span.
fn plant_large_change(f: &Fixture, bytes: usize) {
    let from = f.a.own_position_at_open();
    let orders = f.c.get_collection("shop", "orders").unwrap();
    let node = f.a.node_id();
    let at = Stamp::new(Hlc::new(from.wall_ms + 1, 900), node);
    let record = kimmy_storage::IdempotencyRecord {
        fingerprint: kimmy_core::Fingerprint([0x6b; 32]),
        first: at,
        last: at,
        origin: node,
        state: kimmy_storage::RecordState::Done,
        contested: false,
        outcome: kimmy_storage::RecordOutcome::FindAndModify {
            matched: true,
            modified: true,
            removed: false,
            upserted_id: None,
            stamp: Some(at),
            document: Some(bson::doc! { "pad": "q".repeat(bytes) }),
        },
    };
    let carried = kimmy_storage::CarriedRecord {
        collection: orders.id,
        scope: kimmy_core::ScopeHash([0x6b; 32]),
        carrying: Some(at),
        record: record.encode(),
    };
    f.c.apply_batch_carrying(&[], &[carried], kimmy_storage::RecordsVia::Pull).unwrap();
}

/// The lines of `level` in `log` that say `needle`.
fn said(
    log: &std::sync::Mutex<Vec<(tracing::Level, String)>>,
    level: tracing::Level,
    needle: &str,
) -> usize {
    log.lock()
        .unwrap()
        .iter()
        .filter(|(l, message)| *l == level && message.contains(needle))
        .count()
}

/// What the cut says when a changed record is too large for a replay's delta.
const STARTS_OVER: &str = "the position's records start over from a new base";
/// What the cut says when a page cannot carry what it must.
const UNFIT: &str = "do not fit one replay page";

/// A replay with a records cursor at its position, a page of records alone
/// served for it one way, and then, on C, a change of `bytes` since that
/// cursor's base, with this thread's log recorded from there on.
async fn a_cursor_then_a_change(
    outbound: bool,
    bytes: usize,
) -> (
    Fixture,
    EntryBudget,
    Arc<std::sync::Mutex<Vec<(tracing::Level, String)>>>,
    tracing::subscriber::DefaultGuard,
) {
    let f = fixture(5, 0);
    plant_spanning_records(&f, 30);
    let budget = EntryBudget::set(8 * 1024);
    let c = f.c.node_id();
    if outbound {
        f.replay_outbound_for(Duration::ZERO).await.unwrap();
        assert!(f.catch_up.replay_records(c).is_some(), "premise: a page of records alone");
    } else {
        TEST_REPLAY_MAX_PAGES.with(|cap| cap.set(Some(1)));
        f.round().await.unwrap();
        TEST_REPLAY_MAX_PAGES.with(|cap| cap.set(None));
        let at = f.catch_up.replay_from(c);
        assert_eq!(f.catch_up.replay_records(c).map(|r| r.at), Some(at), "premise: a cursor");
    }
    plant_large_change(&f, bytes);
    let seen = Seen::default();
    let log = Arc::clone(&seen.0);
    let recording = tracing::subscriber::set_default(seen);
    tracing::callsite::rebuild_interest_cache();
    (f, budget, log, recording)
}

/// A record changed since a replay's records cursor that is larger than the
/// delta's half of a page (refused by name in storage) and no larger than a
/// page: the position's records start over from a base above the change, the
/// record crosses as an ordinary record on a page of records alone, and the
/// replay finishes with it and every other record, nothing left unfit.
async fn a_changed_record_too_large_for_the_delta_starts_its_position_over(outbound: bool) {
    let (f, _budget, log, _recording) = a_cursor_then_a_change(outbound, 5 * 1024).await;
    if outbound {
        f.replay_outbound_only_through().await;
    } else {
        f.replay_through(false).await;
    }
    assert!(said(&log, tracing::Level::INFO, STARTS_OVER) >= 1, "it started over");
    assert_eq!(said(&log, tracing::Level::WARN, UNFIT), 0, "nothing was left unfit");
    let orders = f.a.get_collection("shop", "orders").unwrap();
    let big = kimmy_core::ScopeHash([0x6b; 32]);
    assert!(f.a.idempotency_record(orders.id, &big).unwrap().is_some(), "the large record crossed");
    assert_eq!(f.orders(), 2 + 5, "every lost write is back");
}

#[tokio::test]
async fn a_changed_record_too_large_for_an_inbound_replays_delta_starts_its_position_over() {
    a_changed_record_too_large_for_the_delta_starts_its_position_over(false).await;
}

#[tokio::test]
async fn a_changed_record_too_large_for_an_outbound_replays_delta_starts_its_position_over() {
    a_changed_record_too_large_for_the_delta_starts_its_position_over(true).await;
}

/// What still stalls: a changed record larger than a whole page, which no page
/// can carry (the class of a document no page can carry). The position starts
/// over, the pages reach the record and can go no further: each such page
/// carries nothing, is said at `WARN` and counted, the contact ends in step, and
/// the replay stays owed at its position.
async fn a_changed_record_larger_than_a_page_stalls_counted(outbound: bool) {
    let (f, _budget, log, _recording) = a_cursor_then_a_change(outbound, 9 * 1024).await;
    let c = f.c.node_id();
    let at = f.catch_up.replay_from(c);
    let unfit = records_unfit().get(UnfitPath::Replay);
    for _ in 0..3 {
        let contact = if outbound {
            f.replay_outbound_for(Duration::from_secs(20)).await
        } else {
            f.round().await.map(drop)
        };
        assert!(contact.is_ok(), "the contact is no failure: {contact:?}");
    }
    assert!(records_unfit().get(UnfitPath::Replay) > unfit, "counted");
    let warned = said(&log, tracing::Level::WARN, UNFIT);
    assert!(
        (1..=3).contains(&warned),
        "said once a contact at most, the contact going no further: {warned}"
    );
    assert!(!f.catch_up.replay_answered(c), "the replay stays owed");
    assert_eq!(f.catch_up.replay_from(c), at, "at its position");
    let orders = f.a.get_collection("shop", "orders").unwrap();
    let big = kimmy_core::ScopeHash([0x6b; 32]);
    assert!(f.a.idempotency_record(orders.id, &big).unwrap().is_none(), "it did not cross");
}

#[tokio::test]
async fn a_changed_record_larger_than_a_page_stalls_an_inbound_replay_counted() {
    a_changed_record_larger_than_a_page_stalls_counted(false).await;
}

#[tokio::test]
async fn a_changed_record_larger_than_a_page_stalls_an_outbound_replay_counted() {
    a_changed_record_larger_than_a_page_stalls_counted(true).await;
}

/// The pages a test's replay cut on this thread since it last looked.
fn take_cuts() -> Vec<TestReplayCut> {
    TEST_REPLAY_CUTS.with(|cuts| std::mem::take(&mut *cuts.borrow_mut()))
}

fn key_of(cursor: &kimmy_storage::ReplayRecords) -> (u64, [u8; 32]) {
    (cursor.collection.0, cursor.scope.0)
}

/// Every page of records alone a replay cuts **strictly advances its cursor's
/// last key sent**, so the pages at a position end (the ruling on m1(c)): each
/// leaves a cursor of its own position whose key is past the key of the cursor
/// it came with. (The one page of records alone that keeps the key, a delta that
/// fills it, is said and counted, below.)
async fn every_page_of_records_alone_advances_its_key(outbound: bool) {
    let f = fixture(5, 0);
    plant_spanning_records(&f, 40);
    let _budget = EntryBudget::set(8 * 1024);
    take_cuts();
    // The pages first, whether or not the replay finished: a page that kept its
    // key is named here rather than as a replay that never ends.
    for _ in 0..8 {
        let round = if outbound { f.round_outbound().await } else { f.round().await };
        round.unwrap();
        if f.catch_up.replay_answered(f.c.node_id()) {
            break;
        }
    }
    let cuts = take_cuts();
    let alone: Vec<_> = cuts.iter().filter(|cut| cut.records_alone).collect();
    assert!(alone.len() >= 4, "premise: pages of records alone: {}", alone.len());
    for cut in alone {
        assert!(!cut.unfit, "{cut:?}");
        let went = cut.went.as_ref().expect("a page of records alone leaves a cursor");
        assert_eq!(went.at, cut.from, "of its own position");
        if let Some(came) = cut.came.as_ref().filter(|came| came.at == cut.from) {
            assert!(key_of(went) > key_of(came), "the key strictly advances: {cut:?}");
        }
    }
    assert!(f.catch_up.replay_answered(f.c.node_id()), "and the replay finishes");
}

#[tokio::test]
async fn every_page_of_records_alone_of_an_inbound_replay_advances_its_key() {
    every_page_of_records_alone_advances_its_key(false).await;
}

#[tokio::test]
async fn every_page_of_records_alone_of_an_outbound_replay_advances_its_key() {
    every_page_of_records_alone_advances_its_key(true).await;
}

/// The page of records alone that does not advance the key: records changed
/// since the cursor's base fill it (each small enough for the delta, together
/// more than it carries), so it carries those, keeps its key and moves its
/// base. It is said at `WARN` and counted, and the replay still finishes, with
/// every record.
#[tokio::test]
async fn a_page_filled_by_what_changed_since_the_cursor_keeps_its_key_and_is_counted() {
    let f = fixture(5, 0);
    let mut scopes = plant_spanning_records(&f, 30);
    let _budget = EntryBudget::set(8 * 1024);
    TEST_REPLAY_MAX_PAGES.with(|cap| cap.set(Some(1)));
    f.round().await.unwrap();
    TEST_REPLAY_MAX_PAGES.with(|cap| cap.set(None));
    let c = f.c.node_id();
    assert!(f.catch_up.replay_records(c).is_some(), "premise: the member holds a cursor");
    // Twelve changes of about 600 bytes each: more than the delta's 4 KiB.
    let from = f.a.own_position_at_open();
    let orders = f.c.get_collection("shop", "orders").unwrap();
    let node = f.a.node_id();
    for k in 0..12u16 {
        let at = Stamp::new(Hlc::new(from.wall_ms + 1, 700 + k), node);
        let record = kimmy_storage::IdempotencyRecord {
            fingerprint: kimmy_core::Fingerprint([0x7c; 32]),
            first: at,
            last: at,
            origin: node,
            state: kimmy_storage::RecordState::Done,
            contested: false,
            outcome: kimmy_storage::RecordOutcome::FindAndModify {
                matched: true,
                modified: true,
                removed: false,
                upserted_id: None,
                stamp: Some(at),
                document: Some(bson::doc! { "pad": "c".repeat(500) }),
            },
        };
        let mut scope = [0x7c; 32];
        scope[30..].copy_from_slice(&k.to_be_bytes());
        let scope = kimmy_core::ScopeHash(scope);
        let carried = kimmy_storage::CarriedRecord {
            collection: orders.id,
            scope,
            carrying: Some(at),
            record: record.encode(),
        };
        f.c.apply_batch_carrying(&[], &[carried], kimmy_storage::RecordsVia::Pull).unwrap();
        scopes.push(scope);
    }
    let seen = Seen::default();
    let log = Arc::clone(&seen.0);
    let _recording = tracing::subscriber::set_default(seen);
    tracing::callsite::rebuild_interest_cache();
    let unfit = records_unfit().get(UnfitPath::Replay);
    take_cuts();
    f.replay_through(false).await;
    let cuts = take_cuts();
    let kept: Vec<_> = cuts
        .iter()
        .filter(|cut| {
            cut.records_alone
                && cut.came.as_ref().zip(cut.went.as_ref()).is_some_and(|(came, went)| {
                    key_of(came) == key_of(went) && went.base.key > came.base.key
                })
        })
        .collect();
    assert!(!kept.is_empty(), "premise: a page the delta filled: {cuts:#?}");
    assert!(records_unfit().get(UnfitPath::Replay) >= unfit + kept.len() as u64, "counted");
    let said = log
        .lock()
        .unwrap()
        .iter()
        .filter(|(level, message)| {
            *level == tracing::Level::WARN
                && message.contains("carries only the idempotency records changed since")
        })
        .count();
    assert_eq!(said, kept.len(), "said for each");
    let aorders = f.a.get_collection("shop", "orders").unwrap();
    for scope in &scopes {
        assert!(f.a.idempotency_record(aorders.id, scope).unwrap().is_some(), "every record");
    }
    assert_eq!(f.orders(), 2 + 5, "every lost write is back");
}

/// The inbound replay's records cursor is held by the member replayed to, and
/// nowhere else: a member restored again (here, the records it stored from the
/// pages removed, as a store restored from the same backup holds none of them,
/// and its replay armed again, as a start arms it) asks from the same position
/// with no cursor, and its peer starts that position's records over. Kept by the
/// peer, the cursor would pass those records as sent, and the member would take
/// its lost keyed writes back without them.
#[tokio::test]
async fn an_inbound_records_cursor_does_not_outlive_a_second_restore_of_the_member() {
    let f = fixture_of(5, 0, 0, true);
    let planted = plant_spanning_records(&f, 30);
    let _budget = EntryBudget::set(8 * 1024);
    TEST_REPLAY_MAX_PAGES.with(|cap| cap.set(Some(2)));
    let floor = f.a.own_position_at_open();
    // Contact 1: two pages of records alone, stored and acknowledged.
    f.round().await.unwrap();
    assert!(!f.catch_up.replay_answered(f.c.node_id()), "premise: more pages are owed");
    let orders = f.a.get_collection("shop", "orders").unwrap();
    let keyed: Vec<_> = (2..2 + f.lost).map(lost_scope).collect();
    assert!(
        keyed.iter().chain(&planted).any(|s| f
            .a
            .idempotency_record(orders.id, s)
            .unwrap()
            .is_some()),
        "premise: records crossed on contact 1"
    );
    assert_eq!(f.orders(), 2, "premise: no lost write is back yet");
    // Restored again from the same backup: none of those records, and armed again.
    for s in keyed.iter().chain(&planted) {
        let _ = f.a.remove_idempotency_record_for_test(orders.id, s).unwrap();
    }
    f.catch_up.arm_replay(floor, None).unwrap();
    assert_eq!(f.a.own_position_at_open(), floor, "premise: the same replay position");
    let mut contacts = 0;
    while !f.catch_up.replay_answered(f.c.node_id()) {
        contacts += 1;
        assert!(contacts <= 12, "the replay is not answered after twelve contacts");
        f.round().await.unwrap();
    }
    TEST_REPLAY_MAX_PAGES.with(|cap| cap.set(None));
    assert_eq!(f.orders(), 2 + 5, "every lost write is back");
    let missing_keyed =
        keyed.iter().filter(|s| f.a.idempotency_record(orders.id, s).unwrap().is_none()).count();
    let missing_planted =
        planted.iter().filter(|s| f.a.idempotency_record(orders.id, s).unwrap().is_none()).count();
    assert_eq!(
        (missing_keyed, missing_planted),
        (0, 0),
        "after the second restore every lost keyed write and every spanning record is back"
    );
}

/// The same with a keyed `$inc`, whose retry shows what a cursor that outlived
/// the second restore would cost: the `$inc` taken back without its record, and
/// applied a second time.
#[tokio::test]
async fn a_second_restore_during_an_inbound_records_cursor_does_not_apply_a_keyed_inc_twice() {
    let writer_dir = tempfile::tempdir().unwrap();
    let writer = Engine::open(&writer_dir.path().join("kimmy.redb")).unwrap();
    let orders = writer.create_collection("shop", "orders").unwrap();
    for i in 0..2i64 {
        writer.insert(&orders, bson::doc! { "_id": i, "n": 0i64 }).unwrap();
    }
    let mut backup = Vec::new();
    writer.backup_to(&mut backup, kimmy_storage::WalkScope::Request).unwrap();
    std::thread::sleep(Duration::from_millis(15));
    let k = lost_key(1);
    let kimmy_storage::Keyed::Fresh(out) = writer
        .modify_where_keyed(&orders, &kimmy_storage::Candidates::Scan, &IncN, None, &k)
        .unwrap()
    else {
        panic!("a fresh key")
    };
    assert_eq!(out.modified, 2);
    let c_dir = tempfile::tempdir().unwrap();
    let c = Engine::open(&c_dir.path().join("kimmy.redb")).unwrap();
    pull_all(&c, &writer);
    let corders = c.get_collection("shop", "orders").unwrap();
    assert!(c.idempotency_record(corders.id, &k.scope).unwrap().is_some(), "premise: C holds it");
    let a_dir = tempfile::tempdir().unwrap();
    let path = a_dir.path().join("kimmy.redb");
    kimmy_storage::backup::restore(&path, &mut backup.as_slice()).unwrap();
    let a = Engine::open(&path).unwrap();
    assert_eq!(a.node_id(), writer.node_id());
    a.create_collection("shop", "startup").unwrap();
    let marker_dir = tempfile::tempdir().unwrap();
    let catch_up = CatchUp::open(marker_dir.path(), Duration::from_secs(120));
    let floor = a.own_position_at_open();
    catch_up.arm_replay(floor, None).unwrap();
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
    let planted = plant_spanning_records(&f, 30);
    let _budget = EntryBudget::set(8 * 1024);
    TEST_REPLAY_MAX_PAGES.with(|cap| cap.set(Some(2)));
    f.round().await.unwrap();
    assert!(!f.catch_up.replay_answered(f.c.node_id()), "premise: more pages are owed");
    let aorders = f.a.get_collection("shop", "orders").unwrap();
    assert!(
        f.a.idempotency_record(aorders.id, &k.scope).unwrap().is_some(),
        "premise: the $inc's record crossed on contact 1"
    );
    assert_eq!((n_of(&f.a, 0), n_of(&f.a, 1)), (0, 0), "premise: no effect is back yet");
    // Restored again from the same backup.
    let _ = f.a.remove_idempotency_record_for_test(aorders.id, &k.scope).unwrap();
    for s in &planted {
        let _ = f.a.remove_idempotency_record_for_test(aorders.id, s).unwrap();
    }
    f.catch_up.arm_replay(floor, None).unwrap();
    let mut contacts = 0;
    while !f.catch_up.replay_answered(f.c.node_id()) {
        contacts += 1;
        assert!(contacts <= 12, "the replay is not answered after twelve contacts");
        f.round().await.unwrap();
    }
    TEST_REPLAY_MAX_PAGES.with(|cap| cap.set(None));
    let effects = (n_of(&f.a, 0), n_of(&f.a, 1));
    let record = f.a.idempotency_record(aorders.id, &k.scope).unwrap();
    let retry =
        f.a.modify_where_keyed(&aorders, &kimmy_storage::Candidates::Scan, &IncN, None, &k)
            .unwrap();
    let after = (n_of(&f.a, 0), n_of(&f.a, 1));
    assert!(
        record.is_some() && after == effects,
        "the restored member holds the $inc's effect {effects:?} with record {record:?}; the \
         retry answered {retry:?} and the documents are now {after:?}"
    );
}

/// A's lost entries as C holds them, of A's origin above the replay's start, and
/// how far C's walk to them reached.
fn lost_entries_on_c(f: &Fixture) -> (Vec<OplogEntry>, Hlc) {
    let from = f.a.own_position_at_open();
    let window =
        f.c.entries_for_peer(from, MAX_BATCH, kimmy_storage::WalkScope::Background).unwrap();
    let entries = window
        .entries
        .into_iter()
        .filter(|entry| entry.stamp.node == f.a.node_id() && entry.stamp.hlc > from)
        .collect();
    (entries, window.scanned_to)
}

/// A record of `orders` on C from `first` to `last`, as stored there.
fn plant_record_on_c(
    f: &Fixture,
    n: u8,
    first: Stamp,
    last: Stamp,
) -> kimmy_storage::CarriedRecord {
    let orders = f.c.get_collection("shop", "orders").unwrap();
    let record = kimmy_storage::IdempotencyRecord {
        fingerprint: kimmy_core::Fingerprint([n; 32]),
        first,
        last,
        origin: first.node,
        state: kimmy_storage::RecordState::Done,
        contested: false,
        outcome: kimmy_storage::RecordOutcome::Insert { stamp: last },
    };
    let carried = kimmy_storage::CarriedRecord {
        collection: orders.id,
        scope: kimmy_core::ScopeHash([n; 32]),
        carrying: Some(first),
        record: record.encode(),
    };
    f.c.apply_batch_carrying(&[], std::slice::from_ref(&carried), kimmy_storage::RecordsVia::Pull)
        .unwrap();
    carried
}

/// A record a replay page carries twice, by the window (F3, or a row of the
/// requester's origin inside the page) and by B2 at its `first`, is carried at
/// the lower of the two stamps: carried at the higher, a page that stops between
/// the two would store the effect there without its record.
#[test]
fn a_record_a_replay_page_carries_twice_is_carried_at_the_lower_of_its_stamps() {
    let f = fixture(5, 0);
    let from = f.a.own_position_at_open();
    let node = f.a.node_id();
    let first = |k: u16| Stamp::new(Hlc::new(from.wall_ms + 1, k), node);
    let row = |k: u16| Stamp::new(Hlc::new(from.wall_ms + 3, k), node);
    // One the window sends by F3, one by a row of A's origin inside the page.
    let mut f3 = plant_record_on_c(&f, 0x71, first(1), row(1));
    let mut own = plant_record_on_c(&f, 0x72, first(2), row(2));
    f3.carrying = Some(row(1));
    own.carrying = Some(row(2));
    let (entries, scanned_to) = lost_entries_on_c(&f);
    assert!(!entries.is_empty(), "premise: lost entries");
    let cut = cut_replay_page(
        &f.c,
        &"a test",
        node,
        from,
        entries,
        vec![f3, own],
        1,
        scanned_to,
        false,
        None,
    )
    .unwrap();
    let carried = |n: u8| {
        cut.records.iter().find(|r| r.scope == kimmy_core::ScopeHash([n; 32])).map(|r| r.carrying)
    };
    assert_eq!(carried(0x71), Some(Some(first(1))), "the F3 record at its first");
    assert_eq!(carried(0x72), Some(Some(first(2))), "the window's other record at its first");
}

/// The strict-advance guard: a page of records alone under a cursor of this
/// position whose records in key order all lie outside the cursor's set and at
/// or before its key (here a cursor of another collection's set, keyed past every
/// record) cannot move the key past it, and sent, it would be sent again for
/// ever. Such a page carries nothing and is counted, never sent.
#[test]
fn a_page_of_records_alone_that_cannot_move_its_cursors_key_is_not_sent() {
    let f = fixture(5, 0);
    plant_spanning_records(&f, 30);
    let _budget = EntryBudget::set(8 * 1024);
    let from = f.a.own_position_at_open();
    let orders = f.c.get_collection("shop", "orders").unwrap();
    let base = f.c.replay_page_records(&[orders.id], from, Hlc::MAX, None, 1 << 20).unwrap().base;
    let cursor = kimmy_storage::ReplayRecords {
        at: from,
        through: Hlc::MAX,
        collections: vec![kimmy_core::CollectionId(1)],
        collection: kimmy_core::CollectionId(u64::MAX),
        scope: kimmy_core::ScopeHash([0xff; 32]),
        base,
    };
    let (entries, scanned_to) = lost_entries_on_c(&f);
    let unfit = records_unfit().get(UnfitPath::Replay);
    let cut = cut_replay_page(
        &f.c,
        &"a test",
        f.a.node_id(),
        from,
        entries,
        Vec::new(),
        0,
        scanned_to,
        false,
        Some(cursor.clone()),
    )
    .unwrap();
    assert!(cut.unfit && cut.records.is_empty() && cut.entries.is_empty(), "nothing sent: {cut:?}");
    assert!(records_unfit().get(UnfitPath::Replay) > unfit, "counted");
    if let Some(went) = &cut.replay_records {
        assert_eq!(went, &cursor, "the cursor stays as it came");
    }
}

/// A replay page the sender truncates to fit its frame keeps every record riding
/// its kept entries, the one riding its last entry included: the next page
/// starts after that entry, so B2's rule (`last` above the page's start) never
/// selects that record again. Ten keyed inserts of about a kilobyte each under
/// an 8 KiB frame, so every page is cut.
async fn a_truncated_replay_page_keeps_its_last_entrys_record(outbound: bool) {
    let f = fixture_of(10, 1_000, 0, true);
    let _budget = EntryBudget::set(8 * 1024);
    f.replay_through(outbound).await;
    assert_eq!(f.orders(), 2 + 10, "every lost write is back");
    let orders = f.a.get_collection("shop", "orders").unwrap();
    let missing: Vec<i64> = (2..12)
        .filter(|i| f.a.idempotency_record(orders.id, &lost_scope(*i)).unwrap().is_none())
        .collect();
    assert!(missing.is_empty(), "lost keyed inserts back without their records: {missing:?}");
}

#[tokio::test]
async fn a_truncated_inbound_replay_page_keeps_its_last_entrys_record() {
    a_truncated_replay_page_keeps_its_last_entrys_record(false).await;
}

#[tokio::test]
async fn a_truncated_outbound_replay_page_keeps_its_last_entrys_record() {
    a_truncated_replay_page_keeps_its_last_entrys_record(true).await;
}

/// A pulled window's first entry and the record riding at that entry's own stamp
/// (a keyed `find_and_modify`, whose record holds the document it returned)
/// together overfill a frame, though each alone fits. No partial window of
/// records alone can carry the record, which rides at the entry and not below
/// it: the record goes ahead of the entry, with nothing covered, and the window
/// asked again naming that entry leaves it out, so the entry crosses.
#[test]
fn an_entry_and_its_own_record_that_overfill_a_frame_send_the_record_ahead() {
    let node = NodeId::from_bytes([3; 16]);
    let at = Stamp::new(Hlc::new(100, 0), node);
    let entry = OplogEntry {
        stamp: at,
        kind: OpKind::Update,
        collection: kimmy_core::CollectionId(1),
        doc_id: Some(kimmy_core::DocId::Int64(1)),
        body: Some(
            bson::serialize_to_vec(&bson::doc! { "_id": 1i64, "pad": "e".repeat(5_000) }).unwrap(),
        ),
    };
    let record = kimmy_storage::IdempotencyRecord {
        fingerprint: kimmy_core::Fingerprint([3; 32]),
        first: at,
        last: at,
        origin: node,
        state: kimmy_storage::RecordState::Done,
        contested: false,
        outcome: kimmy_storage::RecordOutcome::FindAndModify {
            matched: true,
            modified: true,
            removed: false,
            upserted_id: None,
            stamp: Some(at),
            document: Some(bson::doc! { "_id": 1i64, "pad": "e".repeat(5_000) }),
        },
    };
    let carried = kimmy_storage::CarriedRecord {
        collection: kimmy_core::CollectionId(1),
        scope: kimmy_core::ScopeHash([3; 32]),
        carrying: Some(at),
        record: record.encode(),
    };
    let _budget = EntryBudget::set(8 * 1024);
    assert!(record_bytes(&carried).unwrap() < 8 * 1024, "premise: the record alone fits");
    let entries = [entry];
    let records = [carried.clone()];
    assert!(matches!(how_many_fit(&entries, &records, 0), Fits::Only(0)), "premise");
    assert!(records_alone_window(&entries, &records, 0, Hlc::new(1, 0)).is_none(), "premise");
    let (ahead_of, ahead) = records_ahead(&entries, &records, 0).expect("the record goes ahead");
    assert_eq!((ahead_of, ahead), (at.hlc, vec![carried.clone()]));
    // Asked again naming the entry: the record is left out, and the entry fits.
    let (mut records, mut unanchored) = (vec![carried], 0);
    leave_out_records_ahead(&entries, &mut records, &mut unanchored, Some(at.hlc));
    assert!(records.is_empty());
    assert!(matches!(how_many_fit(&entries, &records, unanchored), Fits::All));
}

/// The same over the real serve and pull: a keyed `find_and_modify` on S whose
/// entry and record each hold a document of about 5 KiB, under an 8 KiB frame.
/// M's pull takes the record ahead and then the entry, in one contact, and a
/// retry on M is answered from the record.
#[tokio::test]
async fn a_pull_takes_a_find_and_modifys_record_ahead_of_its_entry_in_one_contact() {
    let s_dir = tempfile::tempdir().unwrap();
    let s = Engine::open(&s_dir.path().join("kimmy.redb")).unwrap();
    let orders = s.create_collection("shop", "orders").unwrap();
    s.insert(&orders, bson::doc! { "_id": 1i64, "n": 0i64, "pad": "x".repeat(5_000) }).unwrap();
    let m_dir = tempfile::tempdir().unwrap();
    let m = Engine::open(&m_dir.path().join("kimmy.redb")).unwrap();
    pull_all(&m, &s);
    let k = lost_key(9);
    let kimmy_storage::Keyed::Fresh(_) = s
        .find_and_modify_keyed(
            &orders,
            &kimmy_storage::Candidates::Scan,
            &IncN,
            &k,
            kimmy_storage::ReturnedImage::After,
        )
        .unwrap()
    else {
        panic!("a fresh key")
    };
    let _budget = EntryBudget::set(8 * 1024);
    pull_round(&m, &s).await.expect("one contact takes the record and then the entry");
    let morders = m.get_collection("shop", "orders").unwrap();
    assert_eq!(n_of(&m, 1), 1, "the entry crossed");
    assert!(m.idempotency_record(morders.id, &k.scope).unwrap().is_some(), "with its record");
    let retry = m
        .find_and_modify_keyed(
            &morders,
            &kimmy_storage::Candidates::Scan,
            &IncN,
            &k,
            kimmy_storage::ReturnedImage::After,
        )
        .unwrap();
    assert!(matches!(retry, kimmy_storage::Keyed::Replayed(_)), "{retry:?}");
    assert_eq!(n_of(&m, 1), 1, "applied once");
}

/// Whether `message` is an answer of records ahead of an entry: no entries, not
/// the tail, no partial end, and records.
fn is_records_ahead(message: &Message) -> bool {
    matches!(
        message,
        Message::Entries { entries, exhausted: false, passed_through: None, records, .. }
            if entries.is_empty() && !records.is_empty()
    )
}

/// One pull of `s` by `m` through a relay that ends the contact as soon as an
/// answer of records ahead has reached `m`, as a sender stopped between that
/// answer and the next would. Returns the pull's outcome and whether the relay
/// cut it there.
async fn pull_round_cut_after_records_ahead(
    m: &Engine,
    s: &Engine,
) -> (Result<SyncOutcome, ProtocolError>, bool) {
    let (mut ours, relay_m) = tokio::io::duplex(MAX_FRAME);
    let (relay_s, far) = tokio::io::duplex(MAX_FRAME);
    let serving = async {
        let _ = serve_peer(s, far, SECRET, BINDING, None, ServeBudgets::serving(), None).await;
    };
    let cut = std::sync::atomic::AtomicBool::new(false);
    let relay = async {
        let (mut m_read, mut m_write) = tokio::io::split(relay_m);
        let (mut s_read, mut s_write) = tokio::io::split(relay_s);
        let up = async {
            while let Ok(frame) = read_frame(&mut m_read).await {
                if write_frame(&mut s_write, &frame).await.is_err() {
                    break;
                }
            }
        };
        let down = async {
            while let Ok(frame) = read_frame(&mut s_read).await {
                let ahead = is_records_ahead(&frame);
                if write_frame(&mut m_write, &frame).await.is_err() || ahead {
                    cut.store(ahead, std::sync::atomic::Ordering::SeqCst);
                    break;
                }
            }
        };
        tokio::select! {
            () = up => {}
            () = down => {}
        }
    };
    let mut stalls = PeerStalls::new();
    let asking = async {
        open_handshake(m, &mut ours, SECRET, BINDING).await.unwrap();
        let outcome = sync_over(m, &mut ours, addr(), s.node_id(), None, &mut stalls).await;
        drop(ours);
        outcome
    };
    let ((), (), outcome) = tokio::join!(serving, relay, asking);
    (outcome, cut.load(std::sync::atomic::Ordering::SeqCst))
}

/// Records sent ahead of their entry grant nothing, and a contact cut between
/// them and the entry costs one more round, never a loop. S holds a keyed
/// `find_and_modify` whose entry and record each hold a document of about 5 KiB,
/// under an 8 KiB frame. M's first contact ends as soon as the records ahead have
/// reached it: M stored the record and nothing else (no vector, coverage or held
/// mark moved, no document written). Its next contact is sent the records ahead
/// once more, asks again, and takes the entry.
#[tokio::test]
async fn records_sent_ahead_grant_nothing_and_a_cut_contact_sends_them_again_then_the_entry() {
    let s_dir = tempfile::tempdir().unwrap();
    let s = Engine::open(&s_dir.path().join("kimmy.redb")).unwrap();
    let orders = s.create_collection("shop", "orders").unwrap();
    s.insert(&orders, bson::doc! { "_id": 1i64, "n": 0i64, "pad": "x".repeat(5_000) }).unwrap();
    let m_dir = tempfile::tempdir().unwrap();
    let m = Engine::open(&m_dir.path().join("kimmy.redb")).unwrap();
    pull_all(&m, &s);
    let k = lost_key(9);
    let kimmy_storage::Keyed::Fresh(_) = s
        .find_and_modify_keyed(
            &orders,
            &kimmy_storage::Candidates::Scan,
            &IncN,
            &k,
            kimmy_storage::ReturnedImage::After,
        )
        .unwrap()
    else {
        panic!("a fresh key")
    };
    let _budget = EntryBudget::set(8 * 1024);
    let morders = m.get_collection("shop", "orders").unwrap();
    let witnessed = m.witnessed_vector().unwrap();
    let servable = m.version_vector().unwrap();
    let held = m.held_ranges_covered_by(&witnessed).unwrap();
    assert!(m.idempotency_record(morders.id, &k.scope).unwrap().is_none(), "premise");

    let (first, cut) = pull_round_cut_after_records_ahead(&m, &s).await;
    assert!(cut, "premise: the contact was cut right after the records ahead");
    assert!(first.is_err(), "the contact ended there: {first:?}");
    assert!(m.idempotency_record(morders.id, &k.scope).unwrap().is_some(), "the record is stored");
    assert_eq!(m.witnessed_vector().unwrap(), witnessed, "nothing covered");
    assert_eq!(m.version_vector().unwrap(), servable, "nothing taken");
    assert_eq!(m.held_ranges_covered_by(&witnessed).unwrap(), held, "no held mark moved");
    assert_eq!(n_of(&m, 1), 0, "the entry not taken");

    let seen = Seen::default();
    let log = Arc::clone(&seen.0);
    let recording = tracing::subscriber::set_default(seen);
    tracing::callsite::rebuild_interest_cache();
    pull_round(&m, &s).await.expect("the next contact takes the records again, then the entry");
    drop(recording);
    assert_eq!(said(&log, tracing::Level::INFO, "they go ahead of it, alone"), 1, "once, no loop");
    assert_eq!(n_of(&m, 1), 1, "the entry crossed");
    let retry = m
        .find_and_modify_keyed(
            &morders,
            &kimmy_storage::Candidates::Scan,
            &IncN,
            &k,
            kimmy_storage::ReturnedImage::After,
        )
        .unwrap();
    assert!(matches!(retry, kimmy_storage::Keyed::Replayed(_)), "{retry:?}");
    assert_eq!(n_of(&m, 1), 1, "applied once");
}
