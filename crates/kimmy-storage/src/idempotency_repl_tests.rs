//! Idempotency records across members (B5, "Replication and the cross-member
//! retry"): what a window carries, what a receiver stores and when, what a
//! snapshot and a converge page carry, between engines in one process through
//! the storage halves of the real paths (`entries_for_peer_*`, the apply of a
//! peer's window, `snapshot_page` and `apply_snapshot_page`). The transport's
//! own half is tested over sockets in `kimmy-cluster/tests/idempotency.rs`.

use std::cmp::Ordering;
use std::sync::Arc;

use bson::{Document, doc};
use kimmy_core::{
    CollectionId, DocId, Fingerprint, Hlc, NodeId, OpKind, OplogEntry, ScopeHash, Stamp,
};

use crate::idempotency::{
    CarriedRecord, IdempotencyRecord, Keyed, KeyedWrite, RecordOutcome, RecordState, RecordsVia,
};
use crate::{
    Candidates, CollectionMeta, Engine, ModifySpec, PeerWindow, SnapshotProgress, SyncOutcome,
    WalkScope,
};

// -- Fixtures -------------------------------------------------------------------------------

struct Member {
    engine: Arc<Engine>,
    _dir: tempfile::TempDir,
}

fn member() -> Member {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    Member { engine, _dir: dir }
}

/// A and B, with `shop.orders` created on A and pulled by B.
fn pair() -> (Member, Member, CollectionMeta, CollectionMeta) {
    let (a, b) = (member(), member());
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    pull(&b.engine, &a.engine);
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    (a, b, ca, cb)
}

fn keyed(n: u8) -> KeyedWrite {
    KeyedWrite::new(ScopeHash([n; 32]), Fingerprint([n; 32]))
}

fn fresh<T: std::fmt::Debug>(answer: Keyed<T>) -> T {
    match answer {
        Keyed::Fresh(value) => value,
        Keyed::Replayed(record) => panic!("a fresh write was expected; replayed {record:?}"),
    }
}

fn replayed<T: std::fmt::Debug>(answer: Keyed<T>) -> IdempotencyRecord {
    match answer {
        Keyed::Replayed(record) => record,
        Keyed::Fresh(value) => panic!("a replay was expected; wrote again: {value:?}"),
    }
}

/// `to` pulls one window from `from`, as the transport's pull does: from the
/// requester's witnessed position, the sender's records with it.
fn pull(to: &Engine, from: &Engine) -> SyncOutcome {
    let theirs = from.version_vector().unwrap();
    let mine = to.witnessed_vector().unwrap();
    let window =
        from.entries_for_peer_holding(Hlc::ZERO, 1024, Some(&mine), WalkScope::Background).unwrap();
    apply_window(to, &theirs, &mine, &window)
}

fn apply_window(
    to: &Engine,
    theirs: &kimmy_core::VersionVector,
    mine: &kimmy_core::VersionVector,
    window: &crate::OplogWindow,
) -> SyncOutcome {
    let mut outcome = SyncOutcome::default();
    to.apply_peer_window_carrying_into(
        theirs,
        Some(mine),
        &window.entries,
        &window.records,
        RecordsVia::Pull,
        PeerWindow {
            scanned_to: window.scanned_to,
            exhausted: window.exhausted,
            passed_through: None,
            asked_partial: false,
        },
        &mut outcome,
    )
    .unwrap();
    outcome
}

fn count(engine: &Engine, coll: &CollectionMeta) -> u64 {
    engine.count(coll, WalkScope::Request).unwrap()
}

fn n_of(engine: &Engine, coll: &CollectionMeta, id: i64) -> i64 {
    engine.get(coll, &DocId::Int64(id)).unwrap().unwrap().get_i64("n").unwrap()
}

/// `$inc` of `n` on every document that has one.
struct Inc;

impl ModifySpec for Inc {
    fn matches(&self, doc: &Document) -> kimmy_core::Result<bool> {
        Ok(doc.contains_key("n"))
    }
    fn compare(&self, _: &Document, _: &Document) -> Ordering {
        Ordering::Equal
    }
    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
        let mut next = doc.clone();
        next.insert("n", doc.get_i64("n").map_err(|e| e.to_string())? + 1);
        Ok(Some(next))
    }
    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        None
    }
}

fn stamp(ms: u64, node: u8) -> Stamp {
    Stamp::new(Hlc::new(ms, 0), NodeId::from_bytes([node; 16]))
}

fn insert_entry(coll: CollectionId, id: i64, at: Stamp) -> OplogEntry {
    OplogEntry {
        stamp: at,
        kind: OpKind::Insert,
        collection: coll,
        doc_id: Some(DocId::Int64(id)),
        body: Some(bson::serialize_to_vec(&doc! { "_id": id, "n": 0i64 }).unwrap()),
    }
}

fn wire(
    coll: CollectionId,
    k: &KeyedWrite,
    carrying: Option<Stamp>,
    r: &IdempotencyRecord,
) -> CarriedRecord {
    CarriedRecord { collection: coll, scope: k.scope, carrying, record: r.encode() }
}

fn record_of(k: &KeyedWrite, first: Stamp, last: Stamp) -> IdempotencyRecord {
    IdempotencyRecord {
        fingerprint: k.fingerprint,
        first,
        last,
        origin: first.node,
        state: RecordState::Done,
        outcome: RecordOutcome::Insert { stamp: first },
    }
}

// -- Windows and receivers ------------------------------------------------------------------

/// C1, in storage: B pulls A's keyed write with its record, and a retry on B is
/// answered from A's record (a peer replay: its origin is A), with one document.
#[test]
fn c1_a_pulled_window_carries_the_record_and_a_retry_there_replays() {
    let (a, b, ca, cb) = pair();
    let k = keyed(1);
    let (id, first) = fresh(a.engine.insert_keyed(&ca, doc! { "v": 1 }, &k).unwrap());
    let window = a
        .engine
        .entries_for_peer_holding(
            Hlc::ZERO,
            1024,
            Some(&b.engine.witnessed_vector().unwrap()),
            WalkScope::Background,
        )
        .unwrap();
    assert_eq!(window.records.len(), 1, "the window carries the entry's record");
    assert_eq!(window.records[0].carrying, Some(first));
    pull(&b.engine, &a.engine);
    assert_eq!(b.engine.idempotency_records_received()[0], 1, "counted, by pull");
    let again = replayed(b.engine.insert_keyed(&cb, doc! { "v": 1 }, &k).unwrap());
    assert_eq!(again.origin, a.engine.node_id(), "a peer replay");
    assert_eq!(again.outcome, RecordOutcome::Insert { stamp: first });
    assert_eq!(count(&b.engine, &cb), 1);
    assert!(b.engine.get(&cb, &id).unwrap().is_some());
    // Rowed at its `last` and at the carrying stamp, which are one here.
    assert!(b.engine.next_stamp() > first, "witnessed");
}

/// C5, in storage: B holds a newer, independent version of the document, so A's
/// keyed entry loses there and is not appended; its record is stored all the
/// same, and a retry on B is answered from it rather than applied after the
/// version that beat it.
#[test]
fn c5_a_record_is_stored_when_its_entry_loses() {
    let (a, b, ca, cb) = pair();
    let k = keyed(2);
    a.engine.insert(&ca, doc! { "_id": 1i64, "n": 0i64 }).unwrap();
    pull(&b.engine, &a.engine);
    let out =
        fresh(a.engine.modify_where_keyed(&ca, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    assert_eq!(out.modified, 1);
    // B's own write to the document, stamped after A's (its clock moved past it).
    let ahead = Stamp::new(Hlc::new(crate::physical_now_ms() + 60_000, 0), b.engine.node_id());
    b.engine.advance_clock_past(&ahead);
    b.engine.replace(&cb, &DocId::Int64(1), doc! { "n": 10i64 }, false).unwrap();
    let outcome = pull(&b.engine, &a.engine);
    assert_eq!(outcome.superseded, 1, "premise: A's entry lost there");
    assert_eq!(n_of(&b.engine, &cb, 1), 10);
    let again =
        replayed(b.engine.modify_where_keyed(&cb, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    assert_eq!(again.origin, a.engine.node_id());
    assert_eq!(n_of(&b.engine, &cb, 1), 10, "not applied after the version that beat it");
}

/// A frame's record is stored with any entry of its collection the run takes,
/// not only the one that carries it: here the carrying entry is left above the
/// vector the sender advertised, and a later entry of the collection, from
/// another origin, is taken.
#[test]
fn a_record_is_stored_with_any_entry_of_its_collection() {
    let b = member();
    let cb = b.engine.create_collection("shop", "orders").unwrap();
    let k = keyed(3);
    let base = crate::physical_now_ms();
    let (e1, e2) = (stamp(base + 1, 0xA1), stamp(base + 2, 0xB2));
    let entries = [insert_entry(cb.id, 1, e1), insert_entry(cb.id, 2, e2)];
    let r = record_of(&k, e1, e1);
    let records = [wire(cb.id, &k, Some(e1), &r)];
    // The sender advertises origin 0xB2 through e2 and origin 0xA1 below e1.
    let mut theirs = kimmy_core::VersionVector::new();
    theirs.insert(e2.node, e2.hlc);
    theirs.insert(e1.node, Hlc::new(base, 0));
    let mine = b.engine.witnessed_vector().unwrap();
    let window = crate::OplogWindow {
        unanchored: 0,
        entries: entries.to_vec(),
        scanned_to: e2.hlc,
        exhausted: true,
        passed_through: None,
        records: records.to_vec(),
    };
    let outcome = apply_window(&b.engine, &theirs, &mine, &window);
    assert_eq!((outcome.deferred, outcome.applied), (1, 1), "premise: e1 left, e2 taken");
    assert_eq!(b.engine.idempotency_record(cb.id, &k.scope).unwrap(), Some(r));
}

/// A record lands with the run that takes an entry of its collection, not only
/// with the batch's last transaction: when the batch fails after that run
/// committed (here at its last commit, after a schema change ended the run),
/// the effect the run committed has its record with it, and a retry replays.
#[test]
fn a_record_lands_with_the_run_that_takes_its_collections_entry() {
    let (a, b, ca, cb) = pair();
    let k = keyed(31);
    let (id, _) = fresh(a.engine.insert_keyed(&ca, doc! { "v": 1 }, &k).unwrap());
    a.engine.create_collection("shop", "later").unwrap();
    let theirs = a.engine.version_vector().unwrap();
    let mine = b.engine.witnessed_vector().unwrap();
    let window = a
        .engine
        .entries_for_peer_holding(Hlc::ZERO, 1024, Some(&mine), WalkScope::Background)
        .unwrap();
    crate::sync::count_hooks::fail_next(crate::sync::count_hooks::Fail::BeforeLastCommit);
    let mut outcome = SyncOutcome::default();
    let failed = b.engine.apply_peer_window_carrying_into(
        &theirs,
        Some(&mine),
        &window.entries,
        &window.records,
        RecordsVia::Pull,
        PeerWindow {
            scanned_to: window.scanned_to,
            exhausted: window.exhausted,
            passed_through: None,
            asked_partial: false,
        },
        &mut outcome,
    );
    assert!(failed.is_err(), "premise: the batch failed at its last commit");
    assert!(b.engine.get(&cb, &id).unwrap().is_some(), "premise: the run took the insert");
    assert!(b.engine.idempotency_record(cb.id, &k.scope).unwrap().is_some(), "and its record");
    replayed(b.engine.insert_keyed(&cb, doc! { "v": 1 }, &k).unwrap());
    assert_eq!(count(&b.engine, &cb), 1);
}

/// The same when the entry loses (C5): the record is stored with the run that
/// takes its entry on either outcome of the merge. Here the run is committed by
/// a schema change of the same origin after it, which raises B's coverage of A
/// past the keyed entry, and the batch then fails at its last commit. The
/// record stands with the run; held only for the last commit, it would be lost
/// for good, since no later window of A covers the entry again, and a retry on B
/// would apply the request a second time, on top of the version that beat it.
#[test]
fn a_record_lands_with_the_run_that_takes_its_entry_when_the_entry_loses() {
    let (a, b, ca, cb) = pair();
    let k = keyed(32);
    a.engine.insert(&ca, doc! { "_id": 1i64, "n": 0i64 }).unwrap();
    pull(&b.engine, &a.engine);
    fresh(a.engine.modify_where_keyed(&ca, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    let keyed_at = a.engine.idempotency_record(ca.id, &k.scope).unwrap().unwrap().last;
    a.engine.create_collection("shop", "later").unwrap();
    // B's own write to the document, stamped after A's keyed one.
    let ahead = Stamp::new(Hlc::new(crate::physical_now_ms() + 60_000, 0), b.engine.node_id());
    b.engine.advance_clock_past(&ahead);
    b.engine.replace(&cb, &DocId::Int64(1), doc! { "n": 10i64 }, false).unwrap();

    let theirs = a.engine.version_vector().unwrap();
    let mine = b.engine.witnessed_vector().unwrap();
    let window = a
        .engine
        .entries_for_peer_holding(Hlc::ZERO, 1024, Some(&mine), WalkScope::Background)
        .unwrap();
    crate::sync::count_hooks::fail_next(crate::sync::count_hooks::Fail::BeforeLastCommit);
    let mut outcome = SyncOutcome::default();
    let failed = b.engine.apply_peer_window_carrying_into(
        &theirs,
        Some(&mine),
        &window.entries,
        &window.records,
        RecordsVia::Pull,
        PeerWindow {
            scanned_to: window.scanned_to,
            exhausted: window.exhausted,
            passed_through: None,
            asked_partial: false,
        },
        &mut outcome,
    );
    assert!(failed.is_err(), "premise: the batch failed at its last commit");
    assert_eq!(n_of(&b.engine, &cb, 1), 10, "premise: A's keyed entry lost on B");
    let covered = b.engine.witnessed_vector().unwrap().get(a.engine.node_id());
    assert!(covered > keyed_at.hlc, "premise: B's coverage of A passed the keyed entry");
    assert!(b.engine.idempotency_record(cb.id, &k.scope).unwrap().is_some(), "with the run");

    pull(&b.engine, &a.engine);
    replayed(b.engine.modify_where_keyed(&cb, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    assert_eq!(n_of(&b.engine, &cb, 1), 10, "not applied again");
}

/// A record whose row's entry is not in the frame rides with the first later
/// entry of its collection the batch takes. A member whose own write beat the
/// keyed entry never appended it, and serves its windows without it, while it
/// holds the record at that row and the later writes of the origin. Here A's
/// window to C, without the keyed `$inc`, carries A's later write to the same
/// document and a schema change after it; the batch fails at its last commit,
/// after the schema change committed the run and raised C's coverage of A past
/// the keyed entry. The record stands with the run; held for the last commit,
/// it would be lost for good, and a retry on C would `$inc` again a document
/// whose version already holds the first one.
#[test]
fn a_record_whose_entry_is_not_in_the_frame_rides_with_a_later_entry() {
    let (a, c, ca, cc) = pair();
    let k = keyed(33);
    a.engine.insert(&ca, doc! { "_id": 1i64, "n": 0i64 }).unwrap();
    pull(&c.engine, &a.engine);
    fresh(a.engine.modify_where_keyed(&ca, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    let keyed_at = a.engine.idempotency_record(ca.id, &k.scope).unwrap().unwrap().last;
    a.engine.replace(&ca, &DocId::Int64(1), doc! { "n": 1i64, "seen": true }, false).unwrap();
    a.engine.create_collection("shop", "later").unwrap();

    let theirs = a.engine.version_vector().unwrap();
    let mine = c.engine.witnessed_vector().unwrap();
    let mut window = a
        .engine
        .entries_for_peer_holding(Hlc::ZERO, 1024, Some(&mine), WalkScope::Background)
        .unwrap();
    window.entries.retain(|entry| entry.stamp != keyed_at);
    assert_eq!(window.entries.len(), 2, "premise: the later write and the schema change");
    assert!(
        window.records.iter().any(|r| r.scope == k.scope && r.carrying == Some(keyed_at)),
        "premise: the record rides at the keyed entry's row"
    );
    crate::sync::count_hooks::fail_next(crate::sync::count_hooks::Fail::BeforeLastCommit);
    let mut outcome = SyncOutcome::default();
    let failed = c.engine.apply_peer_window_carrying_into(
        &theirs,
        Some(&mine),
        &window.entries,
        &window.records,
        RecordsVia::Pull,
        PeerWindow {
            scanned_to: window.scanned_to,
            exhausted: window.exhausted,
            passed_through: None,
            asked_partial: false,
        },
        &mut outcome,
    );
    assert!(failed.is_err(), "premise: the batch failed at its last commit");
    assert_eq!(n_of(&c.engine, &cc, 1), 1, "premise: C took the later write");
    let covered = c.engine.witnessed_vector().unwrap().get(a.engine.node_id());
    assert!(covered > keyed_at.hlc, "premise: C's coverage of A passed the keyed entry");
    assert!(c.engine.idempotency_record(cc.id, &k.scope).unwrap().is_some(), "with the run");

    pull(&c.engine, &a.engine);
    replayed(c.engine.modify_where_keyed(&cc, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    assert_eq!(n_of(&c.engine, &cc, 1), 1, "not applied again");
}

/// A frame's records are stored in the transaction that raises the receiver's
/// coverage, **whether or not an entry of their collection is taken** and
/// whether or not the collection stands here (B5, records travel with
/// coverage): here the only entry is left above the vector the sender
/// advertised, and a second record names a collection this member has never
/// heard of. Both are stored, and counted by the way they came; the same frame
/// again writes nothing.
#[test]
fn a_frames_records_are_stored_whether_or_not_an_entry_of_their_collection_is_taken() {
    let b = member();
    let cb = b.engine.create_collection("shop", "orders").unwrap();
    let (k, elsewhere) = (keyed(4), keyed(14));
    let e1 = stamp(crate::physical_now_ms() + 1, 0xA1);
    let e2 = stamp(crate::physical_now_ms() + 2, 0xA1);
    let (r, r2) = (record_of(&k, e1, e1), record_of(&elsewhere, e2, e2));
    let unknown = CollectionId::derive("shop", "not-here-yet");
    let window = crate::OplogWindow {
        unanchored: 0,
        entries: vec![insert_entry(cb.id, 1, e1)],
        scanned_to: e1.hlc,
        exhausted: true,
        passed_through: None,
        records: vec![wire(cb.id, &k, Some(e1), &r), wire(unknown, &elsewhere, Some(e2), &r2)],
    };
    let mine = b.engine.witnessed_vector().unwrap();
    let below = kimmy_core::VersionVector::new();
    let outcome = apply_window(&b.engine, &below, &mine, &window);
    assert_eq!(outcome.deferred, 1, "premise: the entry was left");
    assert_eq!(b.engine.idempotency_record(cb.id, &k.scope).unwrap(), Some(r.clone()));
    assert_eq!(b.engine.idempotency_record(unknown, &elsewhere.scope).unwrap(), Some(r2));
    assert_eq!(b.engine.idempotency_records_received()[0], 2, "counted, by pull");
    let commits = b.engine.commits();
    b.engine
        .apply_batch_carrying(&[], &window.records, RecordsVia::Pull)
        .expect("a frame of records held already");
    assert_eq!(b.engine.commits(), commits, "records held already cost no commit");
    assert_eq!(b.engine.idempotency_records_received()[0], 2, "and are not counted again");
}

/// A record of a life the receiver has buried is not stored, and one this build
/// cannot decode is dropped and counted while its entries are applied.
#[test]
fn a_history_record_is_not_stored_and_an_undecodable_one_is_dropped() {
    let b = member();
    b.engine.create_collection("shop", "orders").unwrap();
    b.engine.drop_collection("shop", "orders").unwrap();
    b.engine.finish_purges_now().unwrap();
    let cb = b.engine.create_collection("shop", "orders").unwrap();
    let floor = cb.incarnation_floor.expect("a floor");
    let k = keyed(5);
    let old = Stamp::new(Hlc::new(floor.wall_ms - 5, 0), NodeId::from_bytes([0xA1; 16]));
    let new = Stamp::new(Hlc::new(crate::physical_now_ms() + 5, 0), NodeId::from_bytes([0xA1; 16]));
    let history = record_of(&k, old, old);
    let mut undecodable = record_of(&keyed(6), new, new).encode();
    undecodable[0] = crate::idempotency::RECORD_FORMAT + 1;
    let records = vec![
        wire(cb.id, &k, Some(old), &history),
        CarriedRecord {
            collection: cb.id,
            scope: keyed(6).scope,
            carrying: Some(new),
            record: undecodable,
        },
    ];
    let outcome = b
        .engine
        .apply_batch_carrying(
            &[insert_entry(cb.id, 1, old), insert_entry(cb.id, 2, new)],
            &records,
            RecordsVia::Pull,
        )
        .unwrap();
    assert_eq!((outcome.superseded, outcome.applied), (1, 1));
    assert_eq!(b.engine.idempotency_record(cb.id, &k.scope).unwrap(), None, "history");
    assert_eq!(b.engine.idempotency_record(cb.id, &keyed(6).scope).unwrap(), None);
    assert_eq!(b.engine.idempotency_records_undecodable(), 1);
    assert!(b.engine.get(&cb, &DocId::Int64(2)).unwrap().is_some(), "its entry applied");
}

/// A window's records are read in the read transaction that read its entries:
/// a record removed between the two reads is still the window's, as it stood
/// with the entries it describes.
#[test]
fn a_windows_records_are_read_in_its_own_transaction() {
    let (a, b, ca, _) = pair();
    // Without `held` the linear walk reads the window, with it the key walk.
    for (n, held) in [(7, None), (17, Some(b.engine.witnessed_vector().unwrap()))] {
        let k = keyed(n);
        fresh(a.engine.insert_keyed(&ca, doc! { "v": 1 }, &k).unwrap());
        let remover = Arc::clone(&a.engine);
        let scope = k.scope;
        let id = ca.id;
        crate::sync::race_hooks::at(crate::sync::race_hooks::Race::WindowRecords, move || {
            let db = remover.db();
            let txn = db.begin_write().unwrap();
            txn.open_table(crate::tables::IDEMPOTENCY)
                .unwrap()
                .remove((id.0, scope.as_bytes().as_slice()))
                .unwrap();
            txn.commit().unwrap();
        });
        let window = a
            .engine
            .entries_for_peer_holding(Hlc::ZERO, 1024, held.as_ref(), WalkScope::Background)
            .unwrap();
        crate::sync::race_hooks::clear();
        assert!(window.records.iter().any(|r| r.scope == k.scope), "held {held:?}");
        assert_eq!(a.engine.idempotency_record(ca.id, &k.scope).unwrap(), None, "premise: removed");
    }
}

/// C4, in storage: B pulls A's keyed write at the instant after its commit, and
/// holds its record with it, so a retry on B replays. A record written in a
/// transaction of its own after the write would be missing here.
#[test]
fn c4_a_pull_right_after_the_commit_carries_the_record() {
    let (a, b, ca, cb) = pair();
    let k = keyed(8);
    let (to, from) = (Arc::clone(&b.engine), Arc::clone(&a.engine));
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::KeyedCommitted, move || {
        pull(&to, &from);
    });
    fresh(a.engine.insert_keyed(&ca, doc! { "_id": 1i64, "v": 1 }, &k).unwrap());
    crate::sync::race_hooks::clear();
    assert!(b.engine.get(&cb, &DocId::Int64(1)).unwrap().is_some(), "premise: B pulled it");
    replayed(b.engine.insert_keyed(&cb, doc! { "_id": 1i64, "v": 1 }, &k).unwrap());
}

/// Two applications of one keyed request on two members, neither having
/// received the other's: after the pulls both hold one document under the
/// derived `_id`, and one record, the later application's outcome with the
/// earlier one's `first` (C2's convergence, in storage).
#[test]
fn c2_two_applications_converge_to_one_document_and_one_record() {
    let (a, b, ca, cb) = pair();
    let k = keyed(9);
    let (id_a, first_a) = fresh(a.engine.insert_keyed(&ca, doc! { "v": 1 }, &k).unwrap());
    std::thread::sleep(std::time::Duration::from_millis(2));
    let (id_b, first_b) = fresh(b.engine.insert_keyed(&cb, doc! { "v": 1 }, &k).unwrap());
    assert_eq!(id_a, id_b, "the derived `_id`");
    assert!(first_b > first_a);
    pull(&a.engine, &b.engine);
    pull(&b.engine, &a.engine);
    for (engine, coll) in [(&a.engine, &ca), (&b.engine, &cb)] {
        assert_eq!(count(engine, coll), 1);
        let r = engine.idempotency_record(coll.id, &k.scope).unwrap().unwrap();
        assert_eq!(r.first, first_a, "the earlier application's first");
        assert_eq!(r.last, first_b);
        assert_eq!(r.outcome, RecordOutcome::Insert { stamp: first_b }, "the later outcome");
    }
    assert_eq!(
        a.engine.digest_row(ca.id).unwrap().map(|r| r.digest),
        b.engine.digest_row(cb.id).unwrap().map(|r| r.digest),
        "the cut digest compares equal once settled"
    );
}

// -- Snapshots ------------------------------------------------------------------------------

/// Every page of a whole-database snapshot from `from`, from the start.
fn pages(from: &Engine, scope: Option<CollectionId>) -> Vec<crate::SnapshotPage> {
    let mut out = Vec::new();
    let mut after = None;
    loop {
        let page = from.snapshot_page(after, scope).unwrap();
        after = page.next.clone();
        out.push(page);
        if after.is_none() {
            return out;
        }
    }
}

/// C6, in storage: a member that takes the database by snapshot takes each
/// collection's records ahead of its documents, and a retry there replays. No
/// page holds one collection's records after another's documents.
#[test]
fn c6_a_snapshot_carries_each_collections_records_before_its_documents() {
    let a = member();
    let names = ["c1", "c2", "c3", "c4"];
    let mut colls = Vec::new();
    for name in names {
        colls.push(a.engine.create_collection("shop", name).unwrap());
    }
    for (i, coll) in colls.iter().enumerate() {
        for d in 0..3i64 {
            a.engine.insert(coll, doc! { "_id": d, "n": 0i64 }).unwrap();
        }
        // c3 is written without keys.
        if i != 2 {
            fresh(a.engine.insert_keyed(coll, doc! { "v": 1 }, &keyed(20 + i as u8)).unwrap());
        }
    }
    let pages = pages(&a.engine, None);
    let mut seen_docs: std::collections::HashSet<CollectionId> = Default::default();
    for page in &pages {
        let recorded: std::collections::HashSet<CollectionId> =
            page.records.iter().map(|r| r.collection).collect();
        for collection in &recorded {
            assert!(!seen_docs.contains(collection), "records after its documents: {page:?}");
        }
        let documented: std::collections::HashSet<CollectionId> =
            page.documents.iter().map(|d| d.collection).collect();
        // A page ends at the boundary into a collection with records: no page
        // holds a collection's records after another collection's documents
        // (a collection without records may follow on the page).
        if let Some(from) = recorded.iter().max() {
            assert!(
                documented.iter().all(|c| c >= from),
                "a page holds records after another collection's documents: {page:?}"
            );
        }
        seen_docs.extend(documented);
    }
    let b = member();
    let mut progress = SnapshotProgress::whole_database();
    for page in &pages {
        b.engine.apply_snapshot_page(a.engine.node_id(), &mut progress, page).unwrap();
    }
    assert!(progress.is_complete());
    assert_eq!(b.engine.idempotency_records_received()[3], 3, "by snapshot");
    for (i, name) in names.iter().enumerate() {
        let cb = b.engine.get_collection("shop", name).unwrap();
        if i == 2 {
            continue;
        }
        let k = keyed(20 + i as u8);
        let again = replayed(b.engine.insert_keyed(&cb, doc! { "v": 1 }, &k).unwrap());
        assert_eq!(again.origin, a.engine.node_id());
        assert_eq!(count(&b.engine, &cb), 4);
    }
}

/// A collection with more records than a page sends them over several pages,
/// the cursor naming its records position, before any of its documents; an
/// older receiver, which drops the position and echoes the rest, is sent the
/// documents straight away.
#[test]
fn a_records_phase_spans_pages_and_an_older_receiver_skips_it() {
    let a = member();
    let coll = a.engine.create_collection("shop", "orders").unwrap();
    let n = crate::idempotency::RECORD_PAGE + 88;
    for i in 0..n {
        let mut scope = [0u8; 32];
        scope[..8].copy_from_slice(&(i as u64).to_be_bytes());
        let k = KeyedWrite::new(ScopeHash(scope), Fingerprint([1; 32]));
        fresh(a.engine.insert_keyed(&coll, doc! { "_id": i as i64 }, &k).unwrap());
    }
    let first = a.engine.snapshot_page(None, None).unwrap();
    assert_eq!(first.records.len(), crate::idempotency::RECORD_PAGE);
    assert!(first.documents.is_empty(), "records first");
    let next = first.next.clone().expect("more");
    assert!(next.records.as_ref().is_some_and(|at| at.len() == 32), "a records position: {next}");
    // The position survives the wire's BSON and the JSON a requester persists
    // its progress in.
    let wire: crate::SnapshotCursor =
        bson::deserialize_from_slice(&bson::serialize_to_vec(&next).unwrap()).unwrap();
    assert_eq!(wire, next);
    let persisted: crate::SnapshotCursor =
        serde_json::from_slice(&serde_json::to_vec(&next).unwrap()).unwrap();
    assert_eq!(persisted, next);
    let second = a.engine.snapshot_page(Some(next.clone()), None).unwrap();
    assert_eq!(second.records.len(), 88);
    assert!(!second.documents.is_empty(), "then the documents, on the same page");
    // An older receiver echoes the cursor without the position.
    let older = crate::SnapshotCursor { records: None, ..next };
    let skipped = a.engine.snapshot_page(Some(older), None).unwrap();
    assert!(skipped.records.is_empty());
    assert_eq!(skipped.documents.first().map(|d| d.id.clone()), Some(DocId::Int64(0)));

    // A receiver of this build asks from where its progress says, and so takes
    // every record.
    let b = member();
    let mut progress = SnapshotProgress::whole_database();
    while !progress.is_complete() {
        let page = a.engine.snapshot_page(progress.after().cloned(), None).unwrap();
        b.engine.apply_snapshot_page(a.engine.node_id(), &mut progress, &page).unwrap();
    }
    assert_eq!(b.engine.idempotency_records().unwrap(), n as u64);
}

/// A scoped repair snapshot and a converge page carry the collection's records,
/// and the receiver stores them in the page's transaction, counted as a repair.
#[test]
fn a_repair_and_a_converge_page_carry_the_records() {
    let (a, b, ca, cb) = pair();
    let k = keyed(30);
    fresh(a.engine.insert_keyed(&ca, doc! { "_id": 1i64, "v": 1 }, &k).unwrap());
    let scoped = a.engine.snapshot_page(None, Some(ca.id)).unwrap();
    assert_eq!(scoped.records.len(), 1);
    let mut progress = SnapshotProgress::of_collection(ca.id);
    b.engine.apply_snapshot_page(a.engine.node_id(), &mut progress, &scoped).unwrap();
    assert!(b.engine.idempotency_record(cb.id, &k.scope).unwrap().is_some());
    assert_eq!(b.engine.idempotency_records_received()[4], 1, "by repair");

    let c = member();
    pull(&c.engine, &a.engine);
    let cc = c.engine.get_collection("shop", "orders").unwrap();
    // C holds the document but has lost the record: the converge page brings it.
    {
        let db = c.engine.db();
        let txn = db.begin_write().unwrap();
        txn.open_table(crate::tables::IDEMPOTENCY)
            .unwrap()
            .remove((cc.id.0, k.scope.as_bytes().as_slice()))
            .unwrap();
        txn.commit().unwrap();
    }
    let mut converge = SnapshotProgress::of_collection_converging(cc.id, Default::default());
    c.engine.apply_snapshot_page(a.engine.node_id(), &mut converge, &scoped).unwrap();
    replayed(c.engine.insert_keyed(&cc, doc! { "_id": 1i64, "v": 1 }, &k).unwrap());
}

// -- Retention of a record this build cannot read --------------------------------------------

/// A record this build cannot decode (a newer build's, left by a rollback) does
/// not outlive the window: it is aged by the stamp of its row, kept until that
/// row expires and removed with it.
#[test]
fn an_undecodable_record_goes_when_its_row_expires() {
    let (a, _b, ca, _) = pair();
    let k = keyed(40);
    let at = Stamp::new(Hlc::new(5_000, 0), a.engine.node_id());
    let mut bytes = record_of(&k, at, at).encode();
    bytes[0] = crate::idempotency::RECORD_FORMAT + 1;
    {
        let db = a.engine.db();
        let txn = db.begin_write().unwrap();
        txn.open_table(crate::tables::IDEMPOTENCY)
            .unwrap()
            .insert((ca.id.0, k.scope.as_bytes().as_slice()), bytes.as_slice())
            .unwrap();
        let mut row = ca.id.0.to_be_bytes().to_vec();
        row.extend_from_slice(k.scope.as_bytes());
        txn.open_table(crate::tables::IDEMPOTENCY_BY_STAMP)
            .unwrap()
            .insert(crate::idempotency::row_key(&at).as_slice(), row.as_slice())
            .unwrap();
        txn.commit().unwrap();
    }
    let raw = |engine: &Engine| {
        use redb::{ReadableDatabase, ReadableTableMetadata};
        let txn = engine.db().begin_read().unwrap();
        let records = txn.open_table(crate::tables::IDEMPOTENCY).unwrap();
        let rows = txn.open_table(crate::tables::IDEMPOTENCY_BY_STAMP).unwrap();
        (
            records.get((ca.id.0, k.scope.as_bytes().as_slice())).unwrap().is_some(),
            rows.len().unwrap(),
        )
    };
    assert_eq!(a.engine.idempotency_record(ca.id, &k.scope).unwrap(), None, "absent to lookups");
    const DAY_MS: u64 = 24 * 60 * 60 * 1000;
    let policy = crate::RetentionPolicy::new(24 * 60 * 60, 24 * 60 * 60);
    let kept = a.engine.collect_garbage_at(5_000 + DAY_MS, policy).unwrap();
    assert_eq!(kept.records_removed, 0, "not before its row expires");
    assert_eq!(raw(&a.engine), (true, 1));
    let gone = a.engine.collect_garbage_at(5_000 + DAY_MS + 1, policy).unwrap();
    assert_eq!(gone.records_removed, 1);
    assert_eq!(raw(&a.engine), (false, 0), "the record went with its row");
}

// -- Lives: a record of the life after a drop -----------------------------------------------

/// A window ships a record at a row of its own, never at a row another record
/// under its key left: one of a life a drop ended (its record purged, its rows
/// left to expire), here below the record's `first`. Shipped at that row, the
/// record of the later life rode with the earlier life's entry.
#[test]
fn a_window_ships_a_record_at_its_own_row_not_at_one_an_earlier_life_left() {
    let (a, _b, ca, _) = pair();
    let k = keyed(41);
    fresh(a.engine.insert_keyed(&ca, doc! { "v": 1 }, &k).unwrap());
    let old = a.engine.idempotency_record(ca.id, &k.scope).unwrap().expect("premise").first;
    a.engine.drop_collection("shop", "orders").unwrap();
    a.engine.finish_purges_now().unwrap();
    let again = a.engine.create_collection("shop", "orders").unwrap();
    fresh(a.engine.insert_keyed(&again, doc! { "v": 1 }, &k).unwrap());
    let now = a.engine.idempotency_record(again.id, &k.scope).unwrap().expect("premise");
    assert!(old < now.first, "premise: the earlier life's row is below the record");
    let empty = kimmy_core::VersionVector::new();
    let window = a
        .engine
        .entries_for_peer_holding(Hlc::ZERO, 1024, Some(&empty), WalkScope::Background)
        .unwrap();
    let shipped: Vec<_> = window.records.iter().filter(|r| r.scope == k.scope).collect();
    assert_eq!(shipped.len(), 1, "one per key: {shipped:?}");
    assert_eq!(shipped[0].carrying, Some(now.first), "at its own row, not the earlier life's");
}

/// A frame's record is stored with the entry at or after its row, never in the
/// run of an earlier entry of its collection: the batch takes entries in stamp
/// order, and one before the row can be of a life a drop between them ends, a
/// drop this member has not taken yet. Here B holds its own record of the key
/// from the earlier life, and A's window carries the dropped life's write, the
/// drop, the creation and the later life's write with its record. Stored with
/// the first entry, the later life's record merged with B's, still alive then,
/// and B held the earlier life's `first` under it.
#[test]
fn a_frames_record_rides_with_the_entry_at_its_row_not_an_earlier_one() {
    let (a, b, ca, cb) = pair();
    let k = keyed(42);
    let tick = || std::thread::sleep(std::time::Duration::from_millis(3));
    fresh(a.engine.insert_keyed(&ca, doc! { "v": 1 }, &k).unwrap());
    tick();
    fresh(b.engine.insert_keyed(&cb, doc! { "v": 1 }, &k).unwrap());
    tick();
    pull(&a.engine, &b.engine);
    a.engine.drop_collection("shop", "orders").unwrap();
    a.engine.finish_purges_now().unwrap();
    let again = a.engine.create_collection("shop", "orders").unwrap();
    fresh(a.engine.insert_keyed(&again, doc! { "v": 1 }, &k).unwrap());
    let theirs = a.engine.idempotency_record(again.id, &k.scope).unwrap().expect("premise");
    let mine = b.engine.idempotency_record(cb.id, &k.scope).unwrap().expect("premise");
    assert!(mine.last < theirs.first, "premise: B's record is the earlier life's");
    pull(&b.engine, &a.engine);
    b.engine.finish_purges_now().unwrap();
    pull(&b.engine, &a.engine);
    assert_eq!(
        b.engine.idempotency_record(again.id, &k.scope).unwrap(),
        Some(theirs),
        "the later life's record, not merged with the earlier life's"
    );
}

/// A batch that ends at an entry stores none of the frame's records whose row is at
/// or past it: the window grants no coverage there and serves them again. Here B's
/// batch ends at the creation of `shop.other` again, its earlier life's purge still
/// owed, before the drop of `orders` and the later life's write with its record;
/// B still holds the earlier life's record under the key. Stored at the batch's end,
/// the later life's record merged with it.
#[test]
fn a_batch_that_ends_early_leaves_the_records_past_its_end() {
    let (a, b, ca, cb) = pair();
    a.engine.create_collection("shop", "other").unwrap();
    let k = keyed(43);
    fresh(a.engine.insert_keyed(&ca, doc! { "v": 1 }, &k).unwrap());
    pull(&b.engine, &a.engine);
    let earlier = b.engine.idempotency_record(cb.id, &k.scope).unwrap().expect("premise");
    a.engine.drop_collection("shop", "other").unwrap();
    a.engine.finish_purges_now().unwrap();
    a.engine.create_collection("shop", "other").unwrap();
    a.engine.drop_collection("shop", "orders").unwrap();
    a.engine.finish_purges_now().unwrap();
    let again = a.engine.create_collection("shop", "orders").unwrap();
    fresh(a.engine.insert_keyed(&again, doc! { "v": 1 }, &k).unwrap());
    let later = a.engine.idempotency_record(again.id, &k.scope).unwrap().expect("premise");
    let outcome = pull(&b.engine, &a.engine);
    assert_eq!(outcome.purge_pending, 1, "premise: the batch ended at the creation");
    assert_eq!(
        b.engine.idempotency_record(cb.id, &k.scope).unwrap(),
        Some(earlier),
        "the later life's record is left for the window that takes its row"
    );
    for _ in 0..4 {
        b.engine.finish_purges_now().unwrap();
        pull(&b.engine, &a.engine);
    }
    assert_eq!(b.engine.idempotency_record(again.id, &k.scope).unwrap(), Some(later));
}
