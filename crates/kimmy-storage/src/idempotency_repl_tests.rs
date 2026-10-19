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

/// Past the millisecond: the next stamp on any member is above every one before.
fn tick() {
    std::thread::sleep(std::time::Duration::from_millis(3));
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
        contested: false,
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

/// The same when the entry is judged history here (ruling A: a record outlives a
/// drop of its collection for its window): B has dropped `orders` and made it
/// again since A's keyed insert, so A's entry is of a life B has buried, and its
/// record is stored with the run that takes it as history. A schema change of
/// A's after it commits that run and raises B's coverage of A past the entry,
/// and the batch then fails at its last commit. Held for the last commit, the
/// record would be lost for good, though B's coverage passed its row, and a retry
/// on B would apply afresh where the record says the request was answered.
#[test]
fn a_record_lands_with_the_run_that_takes_its_entry_as_history() {
    let (a, b, ca, _) = pair();
    let k = keyed(33);
    let (id, at) = fresh(a.engine.insert_keyed(&ca, doc! { "_id": 1i64, "v": 1 }, &k).unwrap());
    a.engine.create_collection("shop", "later").unwrap();
    // B's drop at or above A's keyed entry, and its next life.
    b.engine.advance_clock_past(&at);
    b.engine.drop_collection("shop", "orders").unwrap();
    b.engine.finish_purges_now().unwrap();
    let cb = b.engine.create_collection("shop", "orders").unwrap();

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
    assert!(b.engine.get(&cb, &id).unwrap().is_none(), "premise: the entry was history on B");
    let covered = b.engine.witnessed_vector().unwrap().get(a.engine.node_id());
    assert!(covered > at.hlc, "premise: B's coverage of A passed the keyed entry");
    assert!(b.engine.idempotency_record(cb.id, &k.scope).unwrap().is_some(), "with the run");
    replayed(b.engine.insert_keyed(&cb, doc! { "_id": 1i64, "v": 1 }, &k).unwrap());
    assert_eq!(count(&b.engine, &cb), 0, "nothing written to B's next life");
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

/// A record of a life the receiver has buried is stored all the same (ruling A:
/// a record outlives a drop for its window), and a retry in the life standing
/// replays it; one this build cannot decode is dropped and counted while its
/// entries are applied. Its entry is history here, so the record rides with an
/// entry judged history.
#[test]
fn a_dropped_lifes_record_is_stored_and_an_undecodable_one_is_dropped() {
    let b = member();
    b.engine.create_collection("shop", "orders").unwrap();
    b.engine.drop_collection("shop", "orders").unwrap();
    b.engine.finish_purges_now().unwrap();
    let cb = b.engine.create_collection("shop", "orders").unwrap();
    let floor = cb.incarnation_floor.expect("a floor");
    let k = keyed(5);
    let old = Stamp::new(Hlc::new(floor.wall_ms - 5, 0), NodeId::from_bytes([0xA1; 16]));
    let new = Stamp::new(Hlc::new(crate::physical_now_ms() + 5, 0), NodeId::from_bytes([0xA1; 16]));
    let dropped_life = record_of(&k, old, old);
    let mut undecodable = record_of(&keyed(6), new, new).encode();
    undecodable[0] = crate::idempotency::RECORD_FORMAT + 1;
    let records = vec![
        wire(cb.id, &k, Some(old), &dropped_life),
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
    assert_eq!(
        b.engine.idempotency_record(cb.id, &k.scope).unwrap(),
        Some(dropped_life),
        "stored, though its life is buried here"
    );
    assert_eq!(b.engine.idempotency_record(cb.id, &keyed(6).scope).unwrap(), None);
    assert_eq!(b.engine.idempotency_records_undecodable(), 1);
    assert!(b.engine.get(&cb, &DocId::Int64(2)).unwrap().is_some(), "its entry applied");
    let replay = replayed(b.engine.insert_keyed(&cb, doc! { "_id": 1i64 }, &k).unwrap());
    assert_eq!(replay.first, old);
    assert!(b.engine.get(&cb, &DocId::Int64(1)).unwrap().is_none(), "nothing written");
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

// -- Rows another record left; records across a drop ------------------------------------

/// A window ships a record at a row of its own, never at a row another record
/// under its key left: here one past its window that a new request's record
/// replaced, its rows not yet collected, below the new record's `first`. Shipped
/// at that row, the new record would ride with an entry of the request before.
#[test]
fn a_window_ships_a_record_at_its_own_row_not_at_one_an_earlier_record_left() {
    let (a, _b, ca, _) = pair();
    a.engine.set_idempotency_window(3_600);
    let k = keyed(41);
    let (_, old) = fresh(a.engine.insert_keyed(&ca, doc! { "_id": 1i64 }, &k).unwrap());
    a.engine.set_idempotency_clock_for_test(Some(old.hlc.wall_ms + 3_600_001));
    fresh(a.engine.insert_keyed(&ca, doc! { "_id": 2i64 }, &k).unwrap());
    a.engine.set_idempotency_clock_for_test(None);
    let now = a.engine.idempotency_record(ca.id, &k.scope).unwrap().expect("premise");
    assert!(old < now.first, "premise: the earlier record's row is below the record");
    let empty = kimmy_core::VersionVector::new();
    let window = a
        .engine
        .entries_for_peer_holding(Hlc::ZERO, 1024, Some(&empty), WalkScope::Background)
        .unwrap();
    let shipped: Vec<_> = window.records.iter().filter(|r| r.scope == k.scope).collect();
    assert_eq!(shipped.len(), 1, "one per key: {shipped:?}");
    assert_eq!(shipped[0].carrying, Some(now.first), "at its own row, not the earlier one's");
}

/// A record found again at a lower row is carried there (B1 of the build
/// review), but only at a row of its own: a row another record under its key
/// left below its `first` lowers nothing. Here that row is planted at an origin
/// that sorts after the record's own, so F2 reads it after the record was
/// shipped at its own row.
#[test]
fn a_row_below_the_records_first_found_later_does_not_lower_its_carrying_row() {
    let a = member();
    let z = loop {
        let z = member();
        if z.engine.node_id() > a.engine.node_id() {
            break z;
        }
    };
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    pull(&z.engine, &a.engine);
    let cz = z.engine.get_collection("shop", "orders").unwrap();
    z.engine.insert(&cz, doc! { "_id": 7i64 }).unwrap();
    pull(&a.engine, &z.engine);
    let k = keyed(43);
    fresh(a.engine.insert_keyed(&ca, doc! { "v": 1 }, &k).unwrap());
    let own = a.engine.idempotency_record(ca.id, &k.scope).unwrap().expect("premise").first;
    let z_node = z.engine.node_id();
    let stray = Stamp::new(a.engine.version_vector().unwrap().get(z_node), z_node);
    assert!(stray < own, "premise: the planted row is below the record's first");
    {
        let db = a.engine.db();
        let txn = db.begin_write().unwrap();
        let mut row = ca.id.0.to_be_bytes().to_vec();
        row.extend_from_slice(k.scope.as_bytes());
        txn.open_table(crate::tables::IDEMPOTENCY_BY_STAMP)
            .unwrap()
            .insert(crate::idempotency::row_key(&stray).as_slice(), row.as_slice())
            .unwrap();
        txn.commit().unwrap();
    }
    let empty = kimmy_core::VersionVector::new();
    let window = a
        .engine
        .entries_for_peer_holding(Hlc::ZERO, 1024, Some(&empty), WalkScope::Background)
        .unwrap();
    let shipped: Vec<_> = window.records.iter().filter(|r| r.scope == k.scope).collect();
    assert_eq!(shipped.len(), 1, "one per key: {shipped:?}");
    assert_eq!(shipped[0].carrying, Some(own), "at its own row, not the lower one");
}

/// A record rides through a drop and a recreation (ruling A). A and B each
/// applied the same keyed write before A dropped the collection; A's retry in
/// the next life replays, and B, taking the drop, the creation and A's record
/// in one window, keeps its record merged with A's, so a retry there replays
/// too and nothing is written to the next life on either.
#[test]
fn a_record_rides_through_a_drop_and_a_recreation_to_a_member_that_holds_its_own() {
    let (a, b, ca, cb) = pair();
    let k = keyed(42);
    fresh(a.engine.insert_keyed(&ca, doc! { "v": 1 }, &k).unwrap());
    tick();
    fresh(b.engine.insert_keyed(&cb, doc! { "v": 1 }, &k).unwrap());
    tick();
    pull(&a.engine, &b.engine);
    a.engine.drop_collection("shop", "orders").unwrap();
    a.engine.finish_purges_now().unwrap();
    let again = a.engine.create_collection("shop", "orders").unwrap();
    replayed(a.engine.insert_keyed(&again, doc! { "v": 1 }, &k).unwrap());
    let theirs = a.engine.idempotency_record(again.id, &k.scope).unwrap().expect("premise");
    let mine = b.engine.idempotency_record(cb.id, &k.scope).unwrap().expect("premise");
    pull(&b.engine, &a.engine);
    b.engine.finish_purges_now().unwrap();
    pull(&b.engine, &a.engine);
    let life = b.engine.get_collection("shop", "orders").unwrap();
    assert_eq!(
        b.engine.idempotency_record(life.id, &k.scope).unwrap(),
        Some(mine.merge(&theirs)),
        "B's record and A's, merged across the drop"
    );
    replayed(b.engine.insert_keyed(&life, doc! { "v": 1 }, &k).unwrap());
    assert_eq!((count(&a.engine, &again), count(&b.engine, &life)), (0, 0));
}

/// A batch that ends at an entry stores none of the frame's records whose row is at
/// or past it: the window grants no coverage there and serves them again. Here B's
/// batch ends at the creation of `shop.other` again, its earlier life's purge still
/// owed, before the drop of `orders`, its creation again and a keyed write there with
/// its record. B's record of a write before the drop stays throughout (ruling A).
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
    let k2 = keyed(44);
    fresh(a.engine.insert_keyed(&again, doc! { "v": 2 }, &k2).unwrap());
    let later = a.engine.idempotency_record(again.id, &k2.scope).unwrap().expect("premise");
    let outcome = pull(&b.engine, &a.engine);
    assert_eq!(outcome.purge_pending, 1, "premise: the batch ended at the creation");
    assert_eq!(
        b.engine.idempotency_record(cb.id, &k2.scope).unwrap(),
        None,
        "the later write's record is left for the window that takes its row"
    );
    for _ in 0..4 {
        b.engine.finish_purges_now().unwrap();
        pull(&b.engine, &a.engine);
    }
    assert_eq!(b.engine.idempotency_record(again.id, &k2.scope).unwrap(), Some(later));
    assert_eq!(b.engine.idempotency_record(again.id, &k.scope).unwrap(), Some(earlier));
}

// -- D2, D3 and KR across members (ruling A, the `contested` flag) -------------------------------

/// `$inc` of every counter from 0 to `n`, on `coll` at `on`, one chunk a pair.
fn counters_on(on: &Engine, coll: &CollectionMeta, n: i64) {
    for i in 0..n {
        on.insert(coll, doc! { "_id": i, "n": 0i64 }).unwrap();
    }
}

fn ns(engine: &Engine, coll: &CollectionMeta, n: i64) -> Vec<i64> {
    (0..n).map(|i| n_of(engine, coll, i)).collect()
}

/// Stop the keyed request this thread runs next after its first commit, as a
/// drain deadline does: the next chunk is refused and the record left
/// `applying`.
fn stop_after_the_first_chunk(engine: &Arc<Engine>) {
    let engine = Arc::clone(engine);
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
        engine.set_stopping();
    });
}

/// A member's restart, after a stop: the store opened again.
fn restart(m: Member) -> Member {
    crate::sync::race_hooks::clear();
    let Member { engine, _dir } = m;
    let path = _dir.path().join("kimmy.redb");
    drop(Arc::into_inner(engine).expect("the only handle"));
    Member { engine: Arc::new(Engine::open(&path).unwrap()), _dir }
}

/// D2 under ruling A. A applies an `$inc` of every counter and B takes it with its
/// record; A drops `orders`; B, before it hears of the drop, writes counter 0
/// (unkeyed) on top of its version, which carries the `$inc`, stamped after the
/// drop. B then takes the drop, which keeps that write (ADR-236). A retry on B
/// replays the record: the request never lands twice on counter 0. Under the life
/// rule the record was history there, and the retry incremented counter 0 again.
#[test]
fn d2_a_retry_after_a_drop_replays_and_never_doubles() {
    let (a, b, ca, cb) = pair();
    counters_on(&a.engine, &ca, 3);
    a.engine.set_multi_chunk_docs(2);
    let k = keyed(70);
    fresh(a.engine.modify_where_keyed(&ca, &Candidates::Scan, &Inc, None, &k).unwrap());
    pull(&b.engine, &a.engine);
    assert_eq!(ns(&b.engine, &cb, 3), vec![1, 1, 1], "premise: B took the $inc");
    a.engine.drop_collection("shop", "orders").unwrap();
    tick();
    b.engine
        .replace(&cb, &DocId::Int64(0), doc! { "_id": 0i64, "n": 1i64, "touched": true }, false)
        .unwrap();
    pull(&b.engine, &a.engine);
    let life = b.engine.get_collection("shop", "orders").expect("the touched write's life");
    assert_eq!(count(&b.engine, &life), 1, "premise: the drop took counters 1 and 2");
    b.engine.set_multi_chunk_docs(2);
    let again =
        replayed(b.engine.modify_where_keyed(&life, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!(again.origin, a.engine.node_id());
    assert_eq!(n_of(&b.engine, &life, 0), 1, "the $inc landed once on counter 0");
}

/// D3 under ruling A, the stamp rule's contract across a drop. A applies an
/// `$inc` of every counter; B drops `orders` and creates it again with fresh
/// counters; C, before it hears of either, applies the same request, stamped
/// after the drop, and stops after its first chunk. C takes A's effect and
/// record (merged: A's `first`, before the drop), then the drop and the new
/// life: C's first chunk stands (ADR-236), and counters 2 to 4 are B's fresh
/// ones. A retry on C resumes on the merged `first` and leaves every match, each
/// stored at or above it, counted in `skipped`: nothing is written twice, and the
/// new life's counters are left as they are (Chris's 2026-10-18 rule).
#[test]
fn d3_a_resume_after_a_drop_leaves_what_is_at_or_above_its_merged_first() {
    let (a, b, ca, _) = pair();
    let c = member();
    counters_on(&a.engine, &ca, 5);
    pull(&b.engine, &a.engine);
    pull(&c.engine, &a.engine);
    let k = keyed(71);
    a.engine.set_multi_chunk_docs(2);
    fresh(a.engine.modify_where_keyed(&ca, &Candidates::Scan, &Inc, None, &k).unwrap());
    tick();
    b.engine.drop_collection("shop", "orders").unwrap();
    b.engine.finish_purges_now().unwrap();
    let fresh_life = b.engine.create_collection("shop", "orders").unwrap();
    counters_on(&b.engine, &fresh_life, 5);
    tick();
    let cc = c.engine.get_collection("shop", "orders").unwrap();
    c.engine.set_multi_chunk_docs(2);
    stop_after_the_first_chunk(&c.engine);
    let stopped = c.engine.modify_where_keyed(&cc, &Candidates::Scan, &Inc, None, &k).unwrap_err();
    assert!(matches!(stopped, crate::StorageError::PartiallyApplied { .. }), "{stopped:?}");
    let c = restart(c);
    pull(&c.engine, &a.engine);
    for _ in 0..3 {
        c.engine.finish_purges_now().unwrap();
        pull(&c.engine, &b.engine);
    }
    let life = c.engine.get_collection("shop", "orders").unwrap();
    assert_eq!(ns(&c.engine, &life, 5), vec![1, 1, 0, 0, 0], "premise: C's chunk and B's life");
    let record = c.engine.idempotency_record(life.id, &k.scope).unwrap().unwrap();
    assert_eq!(record.state, RecordState::Applying, "premise: C's attempt, merged with A's");
    c.engine.set_multi_chunk_docs(2);
    let out = fresh(c.engine.modify_where_keyed(&life, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert!(out.resumed);
    assert_eq!(ns(&c.engine, &life, 5), vec![1, 1, 0, 0, 0], "nothing written twice or afresh");
    assert_eq!(out.modified, 2, "C's first chunk, and nothing since");
    assert!(out.skipped >= 3, "the new life's counters, left and counted: {out:?}");
}

/// KR: r0 and r1 are two `$inc`s of every counter under one key (two
/// fingerprints). C applies r0; A, without r0's record, applies r1 and stops
/// after its first chunk; A then takes r0's effect and record from C, and the
/// two records merge contested. A retry of r1 on A is refused (422), and nothing
/// completes on r0's `first`; so is r0's. And the other way round: C, taking A's
/// record into r0's, holds a contested record too.
#[test]
fn kr_a_reused_keys_merged_records_are_contested_and_refused() {
    let (a, c, ca, cc) = pair();
    counters_on(&a.engine, &ca, 5);
    pull(&c.engine, &a.engine);
    let r0 = keyed(72);
    let r1 = KeyedWrite::new(r0.scope, Fingerprint([0x73; 32]));
    c.engine.set_multi_chunk_docs(2);
    fresh(c.engine.modify_where_keyed(&cc, &Candidates::Scan, &Inc, None, &r0).unwrap());
    tick();
    a.engine.set_multi_chunk_docs(2);
    stop_after_the_first_chunk(&a.engine);
    a.engine.modify_where_keyed(&ca, &Candidates::Scan, &Inc, None, &r1).unwrap_err();
    let a = restart(a);
    a.engine.set_multi_chunk_docs(2);
    pull(&a.engine, &c.engine);
    let merged = a.engine.idempotency_record(ca.id, &r1.scope).unwrap().unwrap();
    assert!(merged.contested, "{merged:?}");
    let before = ns(&a.engine, &ca, 5);
    for k in [&r1, &r0] {
        let refused = a.engine.modify_where_keyed(&ca, &Candidates::Scan, &Inc, None, k);
        assert!(matches!(refused, Err(crate::StorageError::IdempotencyKeyReused)), "{refused:?}");
    }
    assert_eq!(ns(&a.engine, &ca, 5), before, "nothing completes on r0's first");
    pull(&c.engine, &a.engine);
    assert!(c.engine.idempotency_record(cc.id, &r0.scope).unwrap().unwrap().contested);
    let refused = c.engine.modify_where_keyed(&cc, &Candidates::Scan, &Inc, None, &r0);
    assert!(matches!(refused, Err(crate::StorageError::IdempotencyKeyReused)), "{refused:?}");
}

/// A whole-database snapshot's page carries the record of a collection dropped
/// and not created again, as the sender's records phase walks the ids under its
/// tombstones; the receiver stores it, though nothing stands under the id there,
/// and a retry once the name is created replays and writes nothing. The page is
/// the sender's with that record added: the receiver's half.
#[test]
fn a_snapshot_page_leaves_a_dropped_collections_record_and_a_retry_replays() {
    let a = member();
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.create_collection("shop", "items").unwrap();
    let k = keyed(74);
    fresh(a.engine.insert_keyed(&ca, doc! { "_id": 1i64 }, &k).unwrap());
    let record = a.engine.idempotency_record(ca.id, &k.scope).unwrap().unwrap();
    a.engine.drop_collection("shop", "orders").unwrap();
    a.engine.finish_purges_now().unwrap();
    let mut pages = pages(&a.engine, None);
    pages[0].records.insert(0, wire(ca.id, &k, None, &record));
    let b = member();
    let mut progress = SnapshotProgress::whole_database();
    for page in &pages {
        b.engine.apply_snapshot_page(a.engine.node_id(), &mut progress, page).unwrap();
    }
    assert!(progress.is_complete());
    assert!(b.engine.get_collection("shop", "orders").is_err(), "premise: not there");
    assert_eq!(b.engine.idempotency_record(ca.id, &k.scope).unwrap(), Some(record.clone()));
    let again = b.engine.create_collection("shop", "orders").unwrap();
    let replay = replayed(b.engine.insert_keyed(&again, doc! { "_id": 1i64 }, &k).unwrap());
    assert_eq!(replay.first, record.first);
    assert_eq!(count(&b.engine, &again), 0, "nothing written");
}

/// The same, end to end: the sender's records phase must carry the dropped
/// collection's record itself.
///
/// Needs the dropped-collection records of a snapshot: its records phase walks
/// the ids under `COLLECTIONS_DROPPED`, and its delta and pages do not filter
/// records by whether their collection stands.
#[test]
fn a_member_rebuilt_by_snapshot_after_a_drop_replays_the_old_key() {
    let a = member();
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.create_collection("shop", "items").unwrap();
    let k = keyed(75);
    fresh(a.engine.insert_keyed(&ca, doc! { "_id": 1i64 }, &k).unwrap());
    a.engine.drop_collection("shop", "orders").unwrap();
    a.engine.finish_purges_now().unwrap();
    let b = member();
    let mut progress = SnapshotProgress::whole_database();
    for page in &pages(&a.engine, None) {
        b.engine.apply_snapshot_page(a.engine.node_id(), &mut progress, page).unwrap();
    }
    assert!(progress.is_complete());
    let again = b.engine.create_collection("shop", "orders").unwrap();
    replayed(b.engine.insert_keyed(&again, doc! { "_id": 1i64 }, &k).unwrap());
    assert_eq!(count(&b.engine, &again), 0, "nothing written");
}

/// A converge page whose first page is of a life this member has not buried (the
/// sender dropped the collection and made it again, and this member still holds
/// the life before) applies none of its documents, and stores its records: they
/// outlive the drop. Here the record is put on the page by hand, as a sender's
/// records phase over its dropped collections puts it.
#[test]
fn a_converge_page_of_a_life_this_member_has_not_buried_keeps_its_records() {
    let (a, b, ca, cb) = pair();
    let k = keyed(78);
    fresh(a.engine.insert_keyed(&ca, doc! { "_id": 1i64 }, &k).unwrap());
    let record = a.engine.idempotency_record(ca.id, &k.scope).unwrap().unwrap();
    a.engine.drop_collection("shop", "orders").unwrap();
    a.engine.finish_purges_now().unwrap();
    let next = a.engine.create_collection("shop", "orders").unwrap();
    let mut page = a.engine.snapshot_page(None, Some(next.id)).unwrap();
    assert!(page.dropped.is_none(), "premise: the collection stands on the sender");
    page.records = vec![wire(next.id, &k, None, &record)];
    let mut converge = SnapshotProgress::of_collection_converging(cb.id, Default::default());
    assert!(
        b.engine.first_page_is_a_life_this_member_has_not_buried(&converge, &page).unwrap(),
        "premise: B holds the life before"
    );
    let applied = b.engine.apply_snapshot_page(a.engine.node_id(), &mut converge, &page).unwrap();
    assert!(applied.history);
    assert_eq!(b.engine.idempotency_record(cb.id, &k.scope).unwrap(), Some(record));
    assert_eq!(b.engine.idempotency_records_received()[4], 1, "by repair");
}

/// A converge page from a sender that dropped the collection applies none of its
/// documents, and stores its records: they outlive the drop. Here the record is
/// put on the page by hand, as a sender's records phase over its dropped
/// collections puts it.
#[test]
fn a_converge_page_from_a_sender_that_dropped_keeps_its_records() {
    let (a, b, ca, cb) = pair();
    let k = keyed(76);
    fresh(a.engine.insert_keyed(&ca, doc! { "_id": 1i64 }, &k).unwrap());
    let record = a.engine.idempotency_record(ca.id, &k.scope).unwrap().unwrap();
    a.engine.drop_collection("shop", "orders").unwrap();
    let mut page = a.engine.snapshot_page(None, Some(ca.id)).unwrap();
    assert!(page.dropped.is_some(), "premise: the sender's page names its drop");
    page.records = vec![wire(ca.id, &k, None, &record)];
    let mut converge = SnapshotProgress::of_collection_converging(cb.id, Default::default());
    let applied = b.engine.apply_snapshot_page(a.engine.node_id(), &mut converge, &page).unwrap();
    assert!(applied.sender_dropped);
    assert_eq!(b.engine.idempotency_record(cb.id, &k.scope).unwrap(), Some(record));
    assert_eq!(b.engine.idempotency_records_received()[4], 1, "by repair");
}

// -- A rewind between two attempts that merged ----------------------------------------------

/// Three members: A and B each apply one attempt of a keyed single-document
/// request, from the same starting version, B before it holds A's record; R
/// takes A's, then B's, so it holds the merged record (`first` A's stamp,
/// `last` B's) and B's version of the document. Returns the members, R's
/// collection and A's stamp, the rewind's target.
fn two_attempts_merged_on_r(
    write: impl Fn(&Engine, &CollectionMeta),
    k: &KeyedWrite,
) -> (Member, Member, Member, CollectionMeta, Hlc) {
    let (a, b, r) = (member(), member(), member());
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": 1i64, "n": 0i64 }).unwrap();
    pull(&b.engine, &a.engine);
    pull(&r.engine, &a.engine);
    let (cb, cr) = (
        b.engine.get_collection("shop", "orders").unwrap(),
        r.engine.get_collection("shop", "orders").unwrap(),
    );
    tick();
    write(&a.engine, &ca);
    let first = a.engine.idempotency_record(ca.id, &k.scope).unwrap().unwrap().first;
    tick();
    write(&b.engine, &cb);
    pull(&r.engine, &a.engine);
    pull(&r.engine, &b.engine);
    let merged = r.engine.idempotency_record(cr.id, &k.scope).unwrap().unwrap();
    assert!(merged.first == first && merged.last.hlc > first.hlc, "premise: merged {merged:?}");
    (a, b, r, cr, first.hlc)
}

/// A rewind between two attempts of a keyed `find_and_modify` that merged
/// reopens the record it cuts, and a retry replays: the document is back at the
/// first attempt's version, which holds its `$inc`, and a fresh retry would
/// increment on top of it, at a stamp above the rewind that wins everywhere.
#[test]
fn a_rewind_between_two_attempts_of_a_find_and_modify_replays_the_retry() {
    let k = keyed(90);
    let fam = |e: &Engine, c: &CollectionMeta| {
        let out = e
            .find_and_modify_keyed(
                c,
                &Candidates::Scan,
                &Inc,
                &k,
                crate::idempotency::ReturnedImage::After,
            )
            .unwrap();
        fresh(out);
    };
    let (_a, _b, r, cr, until) = two_attempts_merged_on_r(fam, &k);
    assert_eq!(n_of(&r.engine, &cr, 1), 1, "premise: one $inc, as last-writer-wins keeps it");
    let out = r.engine.rewind_to(until).unwrap();
    assert_eq!((out.records_removed, out.records_reopened), (0, 1), "{out:?}");
    assert_eq!(n_of(&r.engine, &cr, 1), 1, "premise: the first attempt's version, its $inc in it");
    let record = r.engine.idempotency_record(cr.id, &k.scope).unwrap().unwrap();
    assert_eq!(record.state, RecordState::Applying);
    replayed(
        r.engine
            .find_and_modify_keyed(
                &cr,
                &Candidates::Scan,
                &Inc,
                &k,
                crate::idempotency::ReturnedImage::After,
            )
            .unwrap(),
    );
    assert_eq!(n_of(&r.engine, &cr, 1), 1, "the request landed once");
}

/// The same for a keyed insert: the retry replays its first answer, not
/// `409 duplicate_key` on the first attempt's document the rewind kept.
#[test]
fn a_rewind_between_two_attempts_of_an_insert_replays_the_retry() {
    let k = keyed(91);
    let insert = |e: &Engine, c: &CollectionMeta| {
        fresh(e.insert_keyed(c, doc! { "_id": 5i64, "v": 1 }, &k).unwrap());
    };
    let (_a, _b, r, cr, until) = two_attempts_merged_on_r(insert, &k);
    let out = r.engine.rewind_to(until).unwrap();
    assert_eq!((out.records_removed, out.records_reopened), (0, 1), "{out:?}");
    assert!(r.engine.get(&cr, &DocId::Int64(5)).unwrap().is_some(), "premise: the first attempt's");
    replayed(r.engine.insert_keyed(&cr, doc! { "_id": 5i64, "v": 1 }, &k).unwrap());
    assert_eq!(count(&r.engine, &cr), 2);
}

/// The same for a keyed delete by id: the retry replays `{"deleted": 1}`, not a
/// fresh `{"deleted": 0}` against the first attempt's tombstone, which the
/// rewind kept below the merged record's winning delete.
#[test]
fn a_rewind_between_two_attempts_of_a_delete_by_id_replays_the_retry() {
    let k = keyed(92);
    let delete = |e: &Engine, c: &CollectionMeta| {
        let deleted = fresh(e.delete_keyed(c, &DocId::Int64(1), None, &k).unwrap());
        assert!(deleted.is_some(), "premise: each attempt deleted the document");
    };
    let (_a, _b, r, cr, until) = two_attempts_merged_on_r(delete, &k);
    let out = r.engine.rewind_to(until).unwrap();
    assert_eq!((out.records_removed, out.records_reopened), (0, 1), "{out:?}");
    assert!(r.engine.get(&cr, &DocId::Int64(1)).unwrap().is_none(), "premise: still deleted");
    let replay = replayed(r.engine.delete_keyed(&cr, &DocId::Int64(1), None, &k).unwrap());
    assert!(
        matches!(replay.outcome, RecordOutcome::Delete { stamp: Some(_), .. }),
        "the first answer, a document deleted: {replay:?}"
    );
}

// -- A past-window record of the same request -----------------------------------------------

/// Two attempts of one keyed `$inc` of every counter, on two members that have
/// not met: A's first chunk (counters 1 and 2), then B's (counters 0 and 1, B
/// holding a counter 0 of its own), each stopped after it. B pulls A once A's
/// record is past its window on B's clock while A, its retention not yet run,
/// still serves the entries. Returns the counters after B's resume.
fn a_resume_after_the_first_attempts_window(past_the_window: bool) -> Vec<i64> {
    let (a, b, ca, cb) = pair();
    for i in 1..=4i64 {
        a.engine.insert(&ca, doc! { "_id": i, "n": 0i64 }).unwrap();
    }
    pull(&b.engine, &a.engine);
    b.engine.insert(&cb, doc! { "_id": 0i64, "n": 0i64 }).unwrap();
    let k = keyed(93);
    tick();
    a.engine.set_multi_chunk_docs(2);
    stop_after_the_first_chunk(&a.engine);
    a.engine.modify_where_keyed(&ca, &Candidates::Scan, &Inc, None, &k).unwrap_err();
    let a_last = a.engine.idempotency_record(ca.id, &k.scope).unwrap().unwrap().last;
    let a = restart(a);
    tick();
    b.engine.set_multi_chunk_docs(2);
    stop_after_the_first_chunk(&b.engine);
    b.engine.modify_where_keyed(&cb, &Candidates::Scan, &Inc, None, &k).unwrap_err();
    let b = restart(b);
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    b.engine.set_idempotency_window(60);
    if past_the_window {
        b.engine.set_idempotency_clock_for_test(Some(a_last.hlc.wall_ms + 60_001));
    }
    pull(&b.engine, &a.engine);
    let held = b.engine.idempotency_record(cb.id, &k.scope).unwrap().unwrap();
    let a_first = a.engine.idempotency_record(ca.id, &k.scope).unwrap().unwrap().first;
    assert_eq!(held.first, a_first, "A's record merged into B's own: the earlier first");
    b.engine.set_multi_chunk_docs(2);
    let out = b.engine.modify_where_keyed(&cb, &Candidates::Scan, &Inc, None, &k).unwrap();
    assert!(matches!(out, Keyed::Fresh(_)), "a resume: {out:?}");
    (0..=4i64).map(|i| n_of(&b.engine, &cb, i)).collect()
}

/// A record past its window whose request is the live record's here (the same
/// fingerprint) is merged, not refused: B's resume leaves what A's attempt
/// wrote, which B took from A's entries, and every counter is incremented once.
#[test]
fn a_past_window_record_of_the_live_records_own_request_merges() {
    assert_eq!(a_resume_after_the_first_attempts_window(true), vec![1, 1, 1, 1, 1]);
}

/// The control, inside the window: the same.
#[test]
fn a_record_of_the_live_records_own_request_inside_its_window_merges() {
    assert_eq!(a_resume_after_the_first_attempts_window(false), vec![1, 1, 1, 1, 1]);
}
