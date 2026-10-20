//! Idempotency records (ADR-240): the codec, the merge, the keyed write paths, a
//! record across a drop, the coalesced replay barrier, the multi-chunk record and
//! resume, retention, the purge and the sweep (which leave records), backups, a
//! rewind, and what replication will read and store. The storage, crash and
//! fsync-fault tests F1 to F7 of the design are here, named by their row.

use std::cmp::Ordering;
use std::path::PathBuf;
use std::sync::Arc;

use bson::{Bson, Document, doc};
use kimmy_core::{CollectionId, DocId, Fingerprint, Hlc, NodeId, ScopeHash, Stamp};
use proptest::prelude::*;

use crate::idempotency::{
    self, IdempotencyRecord, Keyed, KeyedWrite, ModifyRecorded, RecordOutcome, RecordState,
    ReturnedImage,
};
use crate::{
    Candidates, CollectionMeta, DurabilityClass, Engine, ModifySpec, RetentionPolicy, StorageError,
    WalkScope, tables,
};

const DAY: u64 = 24 * 60 * 60;
const DAY_MS: u64 = DAY * 1000;

// -- Fixtures -------------------------------------------------------------------------------

fn open() -> (Engine, CollectionMeta, tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let engine = Engine::open(&path).unwrap();
    let coll = engine.create_collection("shop", "orders").unwrap();
    (engine, coll, dir, path)
}

fn reopen(path: &std::path::Path) -> (Engine, CollectionMeta) {
    let engine = Engine::open(path).unwrap();
    let coll = engine.get_collection("shop", "orders").unwrap();
    (engine, coll)
}

/// A key of its own for each `n`: scope and fingerprint both from it.
fn keyed(n: u8) -> KeyedWrite {
    KeyedWrite::new(ScopeHash([n; 32]), Fingerprint([n; 32]))
}

/// The same key with another request's fingerprint.
fn reused(k: &KeyedWrite) -> KeyedWrite {
    KeyedWrite::new(k.scope, Fingerprint([0xEE; 32]))
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

fn count(engine: &Engine, coll: &CollectionMeta) -> u64 {
    engine.count(coll, WalkScope::Request).unwrap()
}

fn record(engine: &Engine, coll: &CollectionMeta, k: &KeyedWrite) -> Option<IdempotencyRecord> {
    engine.idempotency_record(coll.id, &k.scope).unwrap()
}

/// Every row of `IDEMPOTENCY_BY_STAMP`, as (stamp, collection, scope).
fn rows(engine: &Engine) -> Vec<(Stamp, u64, [u8; 32])> {
    use redb::{ReadableDatabase, ReadableTable};
    let txn = engine.db().begin_read().unwrap();
    let table = txn.open_table(tables::IDEMPOTENCY_BY_STAMP).unwrap();
    table
        .iter()
        .unwrap()
        .map(|row| {
            let (key, value) = row.unwrap();
            let stamp = idempotency::decode_row_key(key.value()).unwrap();
            let value = value.value();
            let id = u64::from_be_bytes(value[..8].try_into().unwrap());
            (stamp, id, value[8..].try_into().unwrap())
        })
        .collect()
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
        let n = doc.get_i64("n").map_err(|e| e.to_string())?;
        next.insert("n", n + 1);
        Ok(Some(next))
    }
    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        None
    }
}

/// A delete by filter of every document that has an `n`.
struct Remove;

impl ModifySpec for Remove {
    fn matches(&self, doc: &Document) -> kimmy_core::Result<bool> {
        Ok(doc.contains_key("n"))
    }
    fn compare(&self, _: &Document, _: &Document) -> Ordering {
        Ordering::Equal
    }
    fn apply(&self, _: &Document) -> std::result::Result<Option<Document>, String> {
        Ok(None)
    }
    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        None
    }
}

/// `{k: <k>}` set to `v`, upserting `{k, v}` when nothing matches.
struct SetOrUpsert {
    k: &'static str,
    v: i64,
}

impl ModifySpec for SetOrUpsert {
    fn matches(&self, doc: &Document) -> kimmy_core::Result<bool> {
        Ok(doc.get_str("k").is_ok_and(|k| k == self.k))
    }
    fn compare(&self, _: &Document, _: &Document) -> Ordering {
        Ordering::Equal
    }
    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
        let mut next = doc.clone();
        next.insert("v", self.v);
        Ok(Some(next))
    }
    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        Some(Ok(doc! { "k": self.k, "v": self.v }))
    }
}

/// `count` documents `{_id: i, n: 0}`, chunked one per commit.
fn counters(engine: &Engine, coll: &CollectionMeta, count: i64) {
    engine.set_multi_chunk_docs(1);
    let docs: Vec<Document> = (0..count).map(|i| doc! { "_id": i, "n": 0i64 }).collect();
    engine.insert_many(coll, docs).unwrap();
}

fn ns(engine: &Engine, coll: &CollectionMeta, count: i64) -> Vec<i64> {
    (0..count)
        .map(|i| {
            engine.get(coll, &DocId::Int64(i)).unwrap().map_or(-1, |d| d.get_i64("n").unwrap())
        })
        .collect()
}

fn modify(record: &IdempotencyRecord) -> &ModifyRecorded {
    match &record.outcome {
        RecordOutcome::Modify(m) => m,
        other => panic!("a filtered write's record, not {other:?}"),
    }
}

// -- The codec ------------------------------------------------------------------------------

fn stamp(ms: u64, counter: u16, node: u8) -> Stamp {
    Stamp::new(Hlc::new(ms, counter), NodeId::from_bytes([node; 16]))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn base(fp: u8, first: Stamp, last: Stamp, origin: u8, state: RecordState) -> IdempotencyRecord {
    IdempotencyRecord {
        fingerprint: Fingerprint([fp; 32]),
        first,
        last,
        origin: NodeId::from_bytes([origin; 16]),
        state,
        outcome: RecordOutcome::Insert { stamp: first },
        contested: false,
    }
}

/// The bytes, pinned against the layout `idempotency.rs` states, computed
/// independently of the codec (Python, from the layout): a record written by
/// one build must read the same in every other while it lives, and it is
/// shipped to peers as these bytes.
#[test]
fn the_record_codec_is_pinned() {
    let s1 = stamp(1000, 1, 0x11);
    let insert = base(0xAB, s1, s1, 0x22, RecordState::Done);
    assert_eq!(
        hex(&insert.encode()),
        concat!(
            "01",
            "abababababababababababababababababababababababababababababababab",
            "00000000000003e8000111111111111111111111111111111111",
            "00000000000003e8000111111111111111111111111111111111",
            "22222222222222222222222222222222",
            "01",
            "01",
            "00000000000003e8000111111111111111111111111111111111",
        )
    );

    let mut modify = base(0xCD, s1, stamp(2000, 0, 0x11), 0x11, RecordState::Applying);
    modify.outcome = RecordOutcome::Modify(ModifyRecorded {
        matched: 3,
        modified: 2,
        skipped: 1,
        commits: 2,
        stamp: Some(stamp(2000, 1, 0x11)),
        cursor: Some(vec![0x10, 0x01]),
        resumed: true,
    });
    assert_eq!(
        hex(&modify.encode()),
        concat!(
            "01",
            "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
            "00000000000003e8000111111111111111111111111111111111",
            "00000000000007d0000011111111111111111111111111111111",
            "11111111111111111111111111111111",
            "00",
            "05",
            "0000000000000003",
            "0000000000000002",
            "0000000000000001",
            "0000000000000002",
            "01",
            "0100000000000007d0000111111111111111111111111111111111",
            "000000021001",
        )
    );

    let mut fam = base(0xEF, s1, s1, 0x22, RecordState::Done);
    fam.outcome = RecordOutcome::FindAndModify {
        matched: false,
        modified: false,
        removed: false,
        upserted_id: Some(Bson::String("x".into())),
        stamp: Some(s1),
        document: Some(doc! { "a": 1i32 }),
    };
    assert_eq!(
        hex(&fam.encode()),
        concat!(
            "01",
            "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef",
            "00000000000003e8000111111111111111111111111111111111",
            "00000000000003e8000111111111111111111111111111111111",
            "22222222222222222222222222222222",
            "01",
            "06",
            "00",
            "0100000000000003e8000111111111111111111111111111111111",
            "0000000e",
            "0e00000002760002000000780000",
            "0000000c",
            "0c0000001061000100000000",
        )
    );

    let mut bulk = base(0xAB, s1, s1, 0x22, RecordState::Done);
    bulk.outcome = RecordOutcome::Bulk { stamps: vec![s1, stamp(1000, 2, 0x11)] };
    assert_eq!(
        hex(&bulk.encode()),
        concat!(
            "01",
            "abababababababababababababababababababababababababababababababab",
            "00000000000003e8000111111111111111111111111111111111",
            "00000000000003e8000111111111111111111111111111111111",
            "22222222222222222222222222222222",
            "01",
            "02",
            "00000002",
            "00000000000003e8000111111111111111111111111111111111",
            "00000000000003e8000211111111111111111111111111111111",
        )
    );

    let mut replace = base(0xAB, s1, s1, 0x22, RecordState::Done);
    replace.outcome =
        RecordOutcome::Replace { matched: true, modified: true, upserted: false, stamp: None };
    assert!(hex(&replace.encode()).ends_with("2222222222222222222222222222222201030300"));

    let mut delete = base(0xAB, s1, s1, 0x22, RecordState::Done);
    delete.outcome = RecordOutcome::Delete { deleted: 1, stamp: Some(s1) };
    assert!(
        hex(&delete.encode()).ends_with(
            "010400000000000000010100000000000003e8000111111111111111111111111111111111"
        )
    );

    // The contested bit, beside the state: a record written before it reads as not
    // contested (every record above), and either state carries it.
    let contested =
        IdempotencyRecord { contested: true, ..base(0xAB, s1, s1, 0x22, RecordState::Done) };
    let state_at = 2 * (1 + 32 + 26 + 26 + 16);
    let bytes = hex(&contested.encode());
    assert_eq!(&bytes[state_at..state_at + 2], "81", "done, contested");
    let applying = IdempotencyRecord { state: RecordState::Applying, ..contested.clone() };
    assert_eq!(&hex(&applying.encode())[state_at..state_at + 2], "80", "applying, contested");

    for record in [insert, modify, fam, bulk, replace, delete, contested, applying] {
        let back = IdempotencyRecord::decode(&record.encode()).unwrap().unwrap();
        assert_eq!(back, record, "every outcome round-trips");
    }
}

/// A newer build's record leads with a higher version byte and is skipped,
/// never misread; one this format cannot be is refused, never guessed at.
#[test]
fn a_newer_record_is_skipped_and_a_malformed_one_refused() {
    let s1 = stamp(1000, 1, 0x11);
    let bytes = base(0xAB, s1, s1, 0x22, RecordState::Done).encode();

    let mut newer = bytes.clone();
    newer[0] = idempotency::RECORD_FORMAT + 1;
    newer.extend_from_slice(b"whatever a later format holds");
    assert_eq!(IdempotencyRecord::decode(&newer).unwrap(), None);

    let mut zero = bytes.clone();
    zero[0] = 0;
    assert!(IdempotencyRecord::decode(&zero).is_err());
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(IdempotencyRecord::decode(&trailing).is_err(), "nothing may follow");
    assert!(IdempotencyRecord::decode(&bytes[..bytes.len() - 1]).is_err(), "truncated");
    assert!(IdempotencyRecord::decode(&[]).is_err());
    let mut state = bytes.clone();
    state[1 + 32 + 26 + 26 + 16] = 7;
    assert!(IdempotencyRecord::decode(&state).is_err(), "an unknown state");
    state[1 + 32 + 26 + 26 + 16] = 0x87;
    assert!(IdempotencyRecord::decode(&state).is_err(), "an unknown state, contested");
    let mut outcome = bytes.clone();
    outcome[1 + 32 + 26 + 26 + 16 + 1] = 99;
    assert!(IdempotencyRecord::decode(&outcome).is_err(), "an unknown outcome");
    // A bulk that claims more stamps than it holds is refused before anything
    // is allocated for them.
    let mut bulk = base(0xAB, s1, s1, 0x22, RecordState::Done);
    bulk.outcome = RecordOutcome::Bulk { stamps: vec![s1] };
    let mut lying = bulk.encode();
    let at = 1 + 32 + 26 + 26 + 16 + 2;
    lying[at..at + 4].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(IdempotencyRecord::decode(&lying).is_err());
}

// -- The merge ------------------------------------------------------------------------------

fn arb_stamp() -> impl Strategy<Value = Stamp> {
    (0u64..4, 0u16..2, 0u8..2).prop_map(|(ms, c, node)| stamp(ms, c, node))
}

fn arb_record() -> impl Strategy<Value = IdempotencyRecord> {
    (
        0u8..2,
        arb_stamp(),
        arb_stamp(),
        0u8..2,
        prop::bool::ANY,
        0u8..3,
        0u64..3,
        prop::option::of(prop::collection::vec(0u8..3, 0..2)),
        prop::bool::weighted(0.2),
    )
        .prop_map(|(fp, a, b, origin, done, kind, count, cursor, contested)| {
            let (first, last) = if a <= b { (a, b) } else { (b, a) };
            let outcome = match kind {
                0 => RecordOutcome::Insert { stamp: first },
                1 => RecordOutcome::Delete { deleted: count, stamp: Some(last) },
                _ => RecordOutcome::Modify(ModifyRecorded {
                    matched: count,
                    modified: count,
                    skipped: count / 2,
                    commits: count,
                    stamp: Some(last),
                    cursor,
                    resumed: done,
                }),
            };
            IdempotencyRecord {
                fingerprint: Fingerprint([fp; 32]),
                first,
                last,
                origin: NodeId::from_bytes([origin; 16]),
                state: if done { RecordState::Done } else { RecordState::Applying },
                outcome,
                contested,
            }
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    /// The merge is a join: order and repetition of arrival change nothing.
    #[test]
    fn records_merge_as_a_join(a in arb_record(), b in arb_record(), c in arb_record()) {
        prop_assert_eq!(a.merge(&b), b.merge(&a), "commutative");
        prop_assert_eq!(a.merge(&b).merge(&c), a.merge(&b.merge(&c)), "associative");
        prop_assert_eq!(a.merge(&a), a.clone(), "idempotent");
        let m = a.merge(&b);
        prop_assert_eq!(m.first, a.first.min(b.first), "first is the smaller");
        prop_assert_eq!(m.last, a.last.max(b.last), "last is the greater");
        // State, outcome, origin and fingerprint are one record's, the one with
        // the greater last; at an equal last, done beats applying.
        let from_a = (a.last, a.state) > (b.last, b.state);
        let from_b = (b.last, b.state) > (a.last, a.state);
        if from_a {
            prop_assert_eq!((&m.outcome, m.state, m.origin), (&a.outcome, a.state, a.origin));
        }
        if from_b {
            prop_assert_eq!((&m.outcome, m.state, m.origin), (&b.outcome, b.state, b.origin));
        }
    }

    /// The contested flag: set when either side is, or the fingerprints differ, so the
    /// merge is still a join with it (commutative, associative and idempotent
    /// above, the flag compared with the rest); and it never picks the winner,
    /// so flipping it on one side changes nothing else of the merge.
    #[test]
    fn the_contested_flag_is_the_union_of_fingerprints_and_never_breaks_a_tie(
        a in arb_record(),
        b in arb_record(),
        c in arb_record(),
    ) {
        let m = a.merge(&b);
        prop_assert_eq!(
            m.contested,
            a.contested || b.contested || a.fingerprint != b.fingerprint
        );
        let all = a.merge(&b).merge(&c);
        let two = |x: &IdempotencyRecord, y: &IdempotencyRecord| x.fingerprint != y.fingerprint;
        prop_assert_eq!(
            all.contested,
            a.contested || b.contested || c.contested || two(&a, &b) || two(&b, &c) || two(&a, &c),
            "contested exactly when a side was or two fingerprints met"
        );
        let flipped = IdempotencyRecord { contested: !a.contested, ..a.clone() };
        let unflag = |r: IdempotencyRecord| IdempotencyRecord { contested: false, ..r };
        prop_assert_eq!(unflag(flipped.merge(&b)), unflag(a.merge(&b)), "the flag is no tie-break");
        prop_assert_eq!(unflag(b.merge(&flipped)), unflag(b.merge(&a)));
    }
}

/// Two contested records, merged in either order: the flag comes from both sides
/// and not from the winner, and at an equal `last` and state the winner is the
/// same whichever side carries the flag.
#[test]
fn a_merge_of_two_fingerprints_is_contested_in_either_order() {
    let r0 = base(1, stamp(100, 0, 0xC), stamp(100, 0, 0xC), 0xC, RecordState::Done);
    let r1 = base(2, stamp(130, 0, 0xA), stamp(130, 0, 0xA), 0xA, RecordState::Applying);
    for merged in [r0.merge(&r1), r1.merge(&r0)] {
        assert!(merged.contested);
        assert_eq!(merged.fingerprint, r1.fingerprint, "the greater last wins, as before");
        assert_eq!(merged.first, r0.first);
    }
    let again = r0.merge(&r1).merge(&r0);
    assert!(again.contested, "a contested record stays contested, whatever merges in");
    // A tie on everything the state byte follows: the outcome's bytes pick the
    // winner (a delete's tag above an insert's), and the flag, which shares the
    // state's byte, does not.
    let x = base(3, stamp(200, 0, 1), stamp(200, 0, 1), 1, RecordState::Done);
    let y = IdempotencyRecord {
        outcome: RecordOutcome::Delete { deleted: 1, stamp: None },
        ..x.clone()
    };
    assert_eq!(x.merge(&y).outcome, y.outcome, "premise: the delete wins the tie");
    let flagged_x = IdempotencyRecord { contested: true, ..x.clone() };
    assert_eq!(flagged_x.merge(&y).outcome, y.outcome);
    assert_eq!(y.merge(&flagged_x).outcome, y.outcome);
    assert!(y.merge(&flagged_x).contested);
}

/// The design's example of why `first` is the smaller and not the winner's:
/// an attempt on A writes at (1002,0) and stops, an independent attempt on B at
/// (1003,0) and (1004,0); merged, a third attempt must write below (1002,0).
#[test]
fn the_merged_first_is_the_earliest_attempts() {
    let a = base(1, stamp(1002, 0, 0xA), stamp(1002, 0, 0xA), 0xA, RecordState::Applying);
    let b = base(1, stamp(1003, 0, 0xB), stamp(1004, 0, 0xB), 0xB, RecordState::Applying);
    let m = a.merge(&b);
    assert_eq!(m.first, stamp(1002, 0, 0xA));
    assert_eq!(m.last, stamp(1004, 0, 0xB));
    assert_eq!(m.origin, NodeId::from_bytes([0xB; 16]), "B's state, the greater last");
    let done = IdempotencyRecord { state: RecordState::Done, ..b.clone() };
    assert_eq!(b.merge(&done).state, RecordState::Done, "at an equal last, done wins");
    assert_eq!(done.merge(&b).state, RecordState::Done);
}

// -- The keyed write paths ------------------------------------------------------------------

/// A keyed insert's retry gets the first answer, the same stamp and the same
/// `_id`, and nothing is written again: one document, one commit, one record
/// with its row at the insert's stamp.
#[test]
fn a_keyed_insert_replays_the_same_stamp_and_leaves_one_document() {
    let (engine, coll, _dir, _path) = open();
    let k = keyed(1);
    let commits = engine.commits();
    let (id, first) = fresh(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    assert_eq!(engine.commits(), commits + 1, "the record rides in the write's own commit");
    // The derived `_id`, stored as its string, for position 0.
    assert_eq!(id, DocId::String(k.derived_id(coll.id, 0)));
    let stored = engine.get(&coll, &id).unwrap().unwrap();
    assert_eq!(stored.get("_id"), Some(&Bson::String(k.derived_id(coll.id, 0))));

    let entries = engine.read_oplog_from(Hlc::ZERO, 100, WalkScope::Request).unwrap().len();
    let again = replayed(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    assert_eq!(again.outcome, RecordOutcome::Insert { stamp: first });
    assert_eq!((again.first, again.last, again.state), (first, first, RecordState::Done));
    assert_eq!(again.origin, engine.node_id());
    assert_eq!(engine.commits(), commits + 1, "a replay commits nothing");
    assert_eq!(engine.read_oplog_from(Hlc::ZERO, 100, WalkScope::Request).unwrap().len(), entries);
    assert_eq!(count(&engine, &coll), 1);
    assert_eq!(rows(&engine), vec![(first, coll.id.0, k.scope.0)], "one row, at last");
}

#[test]
fn a_keyed_insert_with_its_own_id_keeps_it() {
    let (engine, coll, _dir, _path) = open();
    let k = keyed(2);
    let (id, _) = fresh(engine.insert_keyed(&coll, doc! { "_id": 7i64, "v": 1 }, &k).unwrap());
    assert_eq!(id, DocId::Int64(7));
    replayed(engine.insert_keyed(&coll, doc! { "_id": 7i64, "v": 1 }, &k).unwrap());
    assert_eq!(count(&engine, &coll), 1);
}

/// The same key with another request is refused and writes nothing; so is the
/// same fingerprint on another kind of request.
#[test]
fn a_key_reused_for_another_request_is_refused_and_writes_nothing() {
    let (engine, coll, _dir, _path) = open();
    let k = keyed(3);
    fresh(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    let before = record(&engine, &coll, &k);
    let commits = engine.commits();
    let refused = engine.insert_keyed(&coll, doc! { "v": 2 }, &reused(&k)).unwrap_err();
    assert!(matches!(refused, StorageError::IdempotencyKeyReused), "{refused:?}");
    let refused =
        engine.replace_keyed(&coll, &DocId::Int64(1), doc! { "v": 1 }, true, None, &k).unwrap_err();
    assert!(matches!(refused, StorageError::IdempotencyKeyReused), "another kind: {refused:?}");
    let refused =
        engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &reused(&k)).unwrap_err();
    assert!(matches!(refused, StorageError::IdempotencyKeyReused), "{refused:?}");
    assert_eq!(engine.commits(), commits);
    assert_eq!(count(&engine, &coll), 1);
    assert_eq!(record(&engine, &coll, &k), before, "the record is the first request's");
    // Another principal's scope is its own.
    let other = KeyedWrite::new(ScopeHash([0x99; 32]), k.fingerprint);
    fresh(engine.insert_keyed(&coll, doc! { "v": 1 }, &other).unwrap());
    assert_eq!(count(&engine, &coll), 2);
}

/// A bulk's record keeps its stamps, and each document without `_id` is given
/// the derived one for its position.
#[test]
fn a_keyed_bulk_replays_its_stamps_and_derives_each_position() {
    let (engine, coll, _dir, _path) = open();
    let k = keyed(4);
    let docs = vec![doc! { "a": 0 }, doc! { "_id": "mine", "a": 1 }, doc! { "a": 2 }];
    let written = fresh(engine.insert_many_keyed(&coll, docs.clone(), &k).unwrap());
    assert_eq!(written[0].0, DocId::String(k.derived_id(coll.id, 0)));
    assert_eq!(written[1].0, DocId::String("mine".into()));
    assert_eq!(written[2].0, DocId::String(k.derived_id(coll.id, 2)));
    let again = replayed(engine.insert_many_keyed(&coll, docs, &k).unwrap());
    let stamps: Vec<Stamp> = written.iter().map(|(_, s)| *s).collect();
    assert_eq!(again.outcome, RecordOutcome::Bulk { stamps: stamps.clone() });
    assert_eq!(again.first, stamps[0], "with the batch's first entry");
    assert_eq!(count(&engine, &coll), 3);
    // A refused batch writes no record: a key reused in it is refused first,
    // and a batch refused for a document is not remembered.
    let k2 = keyed(5);
    let refused = engine.insert_many_keyed(&coll, vec![doc! { "_id": "mine" }], &k2).unwrap_err();
    assert_eq!(refused.index, Some(0));
    assert_eq!(record(&engine, &coll, &k2), None);
}

/// A keyed replace's retry is answered from the record, and a retry whose
/// `if_stamp` the first attempt moved on from is the first answer and not
/// `Stale` (the lookup comes before the version check).
#[test]
fn a_keyed_replace_replays_over_a_stale_if_stamp() {
    let (engine, coll, _dir, _path) = open();
    let (_, at) = engine.insert_stamped(&coll, doc! { "_id": 1i64, "v": 0 }).unwrap();
    let k = keyed(6);
    let id = DocId::Int64(1);
    let first =
        fresh(engine.replace_keyed(&coll, &id, doc! { "v": 1 }, false, Some(at), &k).unwrap());
    assert!(first.modified);
    let again =
        replayed(engine.replace_keyed(&coll, &id, doc! { "v": 1 }, false, Some(at), &k).unwrap());
    assert_eq!(
        again.outcome,
        RecordOutcome::Replace {
            matched: true,
            modified: true,
            upserted: false,
            stamp: first.stamp
        }
    );
    // Unchanged and unmatched replaces change nothing and are not remembered.
    let k2 = keyed(7);
    let unchanged =
        fresh(engine.replace_keyed(&coll, &id, doc! { "v": 1 }, false, None, &k2).unwrap());
    assert!(!unchanged.modified);
    let k3 = keyed(8);
    let missing =
        fresh(engine.replace_keyed(&coll, &DocId::Int64(9), doc! {}, false, None, &k3).unwrap());
    assert!(!missing.matched);
    assert_eq!((record(&engine, &coll, &k2), record(&engine, &coll, &k3)), (None, None));
}

/// A keyed delete is replayed while its own tombstone stands; once retention
/// has collected the tombstone the retry runs afresh, and deletes a document a
/// peer brought back under it: a version older than the request's first write
/// makes the record absent.
#[test]
fn a_keyed_delete_replays_only_while_its_tombstone_stands() {
    let (engine, coll, _dir, _path) = open();
    let id = DocId::Int64(1);
    let (_, inserted) = engine.insert_stamped(&coll, doc! { "_id": 1i64 }).unwrap();
    let k = keyed(9);
    let deleted = fresh(engine.delete_keyed(&coll, &id, None, &k).unwrap()).unwrap();
    let again = replayed(engine.delete_keyed(&coll, &id, None, &k).unwrap());
    assert_eq!(again.outcome, RecordOutcome::Delete { deleted: 1, stamp: Some(deleted) });

    // Tombstones kept an hour, records a day: a bare engine allows what the
    // daemon's start refuses, to reach the case.
    let two_hours = deleted.hlc.wall_ms + 2 * 3_600_000;
    engine.collect_garbage_at(two_hours, RetentionPolicy::new(DAY, 3_600)).unwrap();
    assert!(record(&engine, &coll, &k).is_some(), "the record is in its window");
    assert_eq!(fresh(engine.delete_keyed(&coll, &id, None, &k).unwrap()), None, "afresh");

    // The older insert comes back from a peer that never saw the delete.
    let entry = engine.oplog_entry(&inserted).unwrap().expect("the insert's entry");
    assert!(engine.apply_remote(&coll, &entry).unwrap());
    let again = fresh(engine.delete_keyed(&coll, &id, None, &k).unwrap()).expect("deleted again");
    assert!(engine.get(&coll, &id).unwrap().is_none());
    assert_eq!(record(&engine, &coll, &k).unwrap().last, again, "a new record replaced it");
}

/// A keyed delete whose tombstone a drop took replays in the next life (ruling
/// A): the row is gone, and the drop is at or above the delete's stamp, so the
/// delete stands as everything of its life does. With the id written again in
/// the next life, the row is stamped above the delete's and it replays too,
/// leaving that document alone.
#[test]
fn a_keyed_delete_replays_after_a_drop_took_its_tombstone() {
    let (engine, coll, _dir, _path) = open();
    let id = DocId::Int64(1);
    engine.insert_stamped(&coll, doc! { "_id": 1i64 }).unwrap();
    let k = keyed(10);
    let deleted = fresh(engine.delete_keyed(&coll, &id, None, &k).unwrap()).unwrap();
    engine.drop_collection("shop", "orders").unwrap();
    engine.finish_purges_now().unwrap();
    let again = engine.create_collection("shop", "orders").unwrap();
    let replay = replayed(engine.delete_keyed(&again, &id, None, &k).unwrap());
    assert_eq!(replay.outcome, RecordOutcome::Delete { deleted: 1, stamp: Some(deleted) });
    engine.insert_stamped(&again, doc! { "_id": 1i64 }).unwrap();
    replayed(engine.delete_keyed(&again, &id, None, &k).unwrap());
    assert!(engine.get(&again, &id).unwrap().is_some(), "the next life's document stands");
}

/// A keyed `find_and_modify` keeps its flags, the upserted `_id` as the stored
/// body holds it (a rendered value), and the image the caller answers with.
#[test]
fn a_keyed_find_and_modify_records_its_answer() {
    let (engine, coll, _dir, _path) = open();
    let k = keyed(10);
    let spec = SetOrUpsert { k: "a", v: 1 };
    let upserted = fresh(
        engine
            .find_and_modify_keyed(&coll, &Candidates::Scan, &spec, &k, ReturnedImage::After)
            .unwrap(),
    );
    let derived = Bson::String(k.derived_id(coll.id, 0));
    assert_eq!(upserted.upserted, Some(DocId::String(k.derived_id(coll.id, 0))));
    let again = replayed(
        engine
            .find_and_modify_keyed(&coll, &Candidates::Scan, &spec, &k, ReturnedImage::After)
            .unwrap(),
    );
    assert_eq!(
        again.outcome,
        RecordOutcome::FindAndModify {
            matched: false,
            modified: false,
            removed: false,
            upserted_id: Some(derived.clone()),
            stamp: upserted.stamp,
            document: upserted.after.clone(),
        }
    );
    assert_eq!(count(&engine, &coll), 1);

    // A change returning the image before it.
    let k2 = keyed(11);
    let set = SetOrUpsert { k: "a", v: 2 };
    let changed = fresh(
        engine
            .find_and_modify_keyed(&coll, &Candidates::Scan, &set, &k2, ReturnedImage::Before)
            .unwrap(),
    );
    let again = replayed(
        engine
            .find_and_modify_keyed(&coll, &Candidates::Scan, &set, &k2, ReturnedImage::Before)
            .unwrap(),
    );
    let RecordOutcome::FindAndModify { matched, modified, removed, upserted_id, document, .. } =
        again.outcome
    else {
        panic!("a find_and_modify record");
    };
    assert!(matched && modified && !removed && upserted_id.is_none());
    assert_eq!(document, changed.before, "the image the request answered with");
    // Unchanged: no record.
    let k3 = keyed(12);
    fresh(
        engine
            .find_and_modify_keyed(&coll, &Candidates::Scan, &set, &k3, ReturnedImage::After)
            .unwrap(),
    );
    assert_eq!(record(&engine, &coll, &k3), None);
}

/// The upserted `_id` a record keeps is the stored body's own value, never the
/// `DocId` rendered back: an `Int32` the seed carried stays an `Int32`.
#[test]
fn a_keyed_upsert_records_the_stored_id_value() {
    struct Seeded;
    impl ModifySpec for Seeded {
        fn matches(&self, _: &Document) -> kimmy_core::Result<bool> {
            Ok(false)
        }
        fn compare(&self, _: &Document, _: &Document) -> Ordering {
            Ordering::Equal
        }
        fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
            Ok(Some(doc.clone()))
        }
        fn upsert(&self) -> Option<std::result::Result<Document, String>> {
            Some(Ok(doc! { "_id": 5i32, "v": 1 }))
        }
    }
    let (engine, coll, _dir, _path) = open();
    let k = keyed(93);
    let first = fresh(
        engine
            .find_and_modify_keyed(&coll, &Candidates::Scan, &Seeded, &k, ReturnedImage::Before)
            .unwrap(),
    );
    assert_eq!(first.upserted, Some(DocId::Int64(5)), "premise: the DocId is an Int64");
    let again = replayed(
        engine
            .find_and_modify_keyed(&coll, &Candidates::Scan, &Seeded, &k, ReturnedImage::Before)
            .unwrap(),
    );
    let RecordOutcome::FindAndModify { upserted_id, document, .. } = again.outcome else {
        panic!("a find_and_modify record");
    };
    assert_eq!(upserted_id, Some(Bson::Int32(5)));
    assert_eq!(document, None, "the image before an upsert is none");
}

/// A request that changed nothing is not remembered: a retry is evaluated
/// again, and a refusal leaves no record.
#[test]
fn a_request_that_changed_nothing_writes_no_record() {
    let (engine, coll, _dir, _path) = open();
    let k = keyed(13);
    let commits = engine.commits();
    let nothing =
        fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!((nothing.matched, nothing.commits), (0, 0));
    assert_eq!(fresh(engine.delete_keyed(&coll, &DocId::Int64(1), None, &k).unwrap()), None);
    let stale = engine.delete_keyed(&coll, &DocId::Int64(1), Some(stamp(1, 0, 1)), &k);
    assert!(matches!(stale, Err(StorageError::Stale { current: None })), "{stale:?}");
    assert_eq!(engine.commits(), commits, "nothing committed");
    assert_eq!(record(&engine, &coll, &k), None);
    assert!(rows(&engine).is_empty());
    // A refused insert (a duplicate `_id`) writes no record either.
    engine.insert(&coll, doc! { "_id": 1i64 }).unwrap();
    assert!(engine.insert_keyed(&coll, doc! { "_id": 1i64 }, &k).is_err());
    assert_eq!(record(&engine, &coll, &k), None);
}

/// The derived `_id` belongs to the request, not to the member: the same keyed
/// request on two separate engines lands on one document key.
#[test]
fn the_derived_id_is_the_same_on_two_engines() {
    let (a, ca, _da, _) = open();
    let (b, cb, _db, _) = open();
    assert_ne!(a.node_id(), b.node_id());
    let k = keyed(14);
    let (on_a, _) = fresh(a.insert_keyed(&ca, doc! { "v": 1 }, &k).unwrap());
    let (on_b, _) = fresh(b.insert_keyed(&cb, doc! { "v": 1 }, &k).unwrap());
    assert_eq!(on_a, on_b);
    let docs = vec![doc! {}, doc! {}];
    let bulk_a = fresh(a.insert_many_keyed(&ca, docs.clone(), &keyed(15)).unwrap());
    let bulk_b = fresh(b.insert_many_keyed(&cb, docs, &keyed(15)).unwrap());
    assert_eq!(
        bulk_a.iter().map(|(id, _)| id).collect::<Vec<_>>(),
        bulk_b.iter().map(|(id, _)| id).collect::<Vec<_>>()
    );
    // Another request under the same key derives another `_id` (I7).
    let (other, _) = fresh(b.insert_keyed(&cb, doc! { "v": 2 }, &reused(&keyed(16))).unwrap());
    assert_ne!(other, on_b);
}

// -- Across a drop, and the window ----------------------------------------------------------

/// A record outlives a drop of its collection for its window: the
/// drop's purge leaves it, re-creation is not held up by it, and a retry in the
/// next life replays the first answer and writes nothing there. Another request
/// under the key is refused across the drop, as before it.
#[test]
fn a_record_outlives_a_drop_and_a_retry_in_the_next_life_replays() {
    let (engine, coll, _dir, _path) = open();
    let k = keyed(17);
    let (_, first) = fresh(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    let old = record(&engine, &coll, &k).unwrap();
    engine.drop_collection("shop", "orders").unwrap();
    engine.finish_purges_now().unwrap();
    assert_eq!(record(&engine, &coll, &k), Some(old.clone()), "the purge left it");
    let again = engine.create_collection("shop", "orders").expect("records hold nothing up");
    assert!(again.incarnation_floor.is_some_and(|floor| floor >= old.last.hlc));
    let replay = replayed(engine.insert_keyed(&again, doc! { "v": 1 }, &k).unwrap());
    assert_eq!(replay.first, first, "the first answer");
    assert_eq!(count(&engine, &again), 0, "nothing written to the next life");
    let refused = engine.insert_keyed(&again, doc! { "v": 2 }, &reused(&k)).unwrap_err();
    assert!(matches!(refused, StorageError::IdempotencyKeyReused), "{refused:?}");
    assert_eq!(record(&engine, &again, &k), Some(old), "unchanged");
}

/// A record of a dropped life that a peer sends is stored as any other, and a
/// retry in the next life replays it (a drop's way in by F2: the record reaches a
/// member after the drop did). Refused, the retry wrote afresh there.
#[test]
fn a_dropped_lifes_record_from_a_peer_is_stored_and_replayed() {
    let (engine, _coll, _dir, _path) = open();
    engine.drop_collection("shop", "orders").unwrap();
    let again = engine.create_collection("shop", "orders").unwrap();
    let floor = again.incarnation_floor.expect("a life after a drop has a floor");
    let k = keyed(91);
    let below = Stamp::new(Hlc::new(floor.wall_ms - 10, 0), NodeId::from_bytes([9; 16]));
    let r = IdempotencyRecord {
        fingerprint: k.fingerprint,
        first: below,
        last: below,
        origin: below.node,
        state: RecordState::Done,
        outcome: RecordOutcome::Modify(ModifyRecorded { matched: 2, ..Default::default() }),
        contested: false,
    };
    {
        let txn = engine.begin_write(crate::WriterHolder::Replication).unwrap();
        let stored = engine.store_record(&txn, again.id, &k.scope, &r, None).unwrap();
        assert_eq!(stored, Some(r.clone()), "stored, though a drop is above it");
        txn.commit().unwrap();
    }
    counters(&engine, &again, 2);
    let replay =
        replayed(engine.modify_where_keyed(&again, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!(replay.first, r.first);
    assert_eq!(ns(&engine, &again, 2), vec![0, 0], "nothing written to the next life");
}

/// A record of a chunk of several documents runs from the first entry its
/// transaction wrote to the last (L1), with a row at each, as for a bulk.
#[test]
fn a_chunks_record_runs_from_its_first_entry_to_its_last() {
    let (engine, coll, _dir, _path) = open();
    counters(&engine, &coll, 3);
    engine.set_multi_chunk_docs(10);
    let k = keyed(92);
    let out = fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!(out.commits, 1);
    let written: Vec<Stamp> = engine
        .read_oplog_from(Hlc::ZERO, 100, WalkScope::Request)
        .unwrap()
        .iter()
        .filter(|e| e.kind == kimmy_core::OpKind::Replace)
        .map(|e| e.stamp)
        .collect();
    assert_eq!(written.len(), 3);
    let stored = record(&engine, &coll, &k).unwrap();
    assert_eq!((stored.first, stored.last), (written[0], written[2]));
    assert_eq!(
        rows(&engine).iter().map(|(s, ..)| *s).collect::<Vec<_>>(),
        vec![written[0], written[2]]
    );
    // A chunk that found fewer than its budget ends the request: done, in its
    // own commit, and a retry replays it rather than resuming it.
    assert_eq!(stored.state, RecordState::Done);
    let commits = engine.commits();
    let again =
        replayed(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!(again, stored);
    assert_eq!(engine.commits(), commits);
    assert_eq!(ns(&engine, &coll, 3), vec![1, 1, 1]);
}

/// The single-document form of a keyed update or delete by filter is one
/// transaction: its record is done when it commits, and a retry replays it,
/// with the stamp the single form reports.
#[test]
fn a_single_document_keyed_update_replays() {
    let (engine, coll, _dir, _path) = open();
    counters(&engine, &coll, 2);
    let k = keyed(94);
    let out =
        fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    assert_eq!((out.matched, out.modified, out.commits), (1, 1, 1));
    let stored = record(&engine, &coll, &k).unwrap();
    assert_eq!(stored.state, RecordState::Done);
    assert_eq!(modify(&stored).stamp, out.stamp);
    let again =
        replayed(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    assert_eq!(again, stored);
    assert_eq!(ns(&engine, &coll, 2), vec![1, 0], "one document, once");
    let k2 = keyed(95);
    fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Remove, Some(1), &k2).unwrap());
    replayed(engine.modify_where_keyed(&coll, &Candidates::Scan, &Remove, Some(1), &k2).unwrap());
    assert_eq!(count(&engine, &coll), 1, "one deleted, once");
}

/// A record is absent on lookup once its `last` is past the window, at
/// exactly the cutoff retention collects it at.
#[test]
fn a_record_past_its_window_is_absent() {
    let (engine, coll, _dir, _path) = open();
    engine.set_idempotency_window(3_600);
    let k = keyed(18);
    let (_, at) = fresh(engine.insert_keyed(&coll, doc! { "_id": 1i64, "v": 1 }, &k).unwrap());
    engine.set_idempotency_clock_for_test(Some(at.hlc.wall_ms + 3_600_000));
    replayed(engine.insert_keyed(&coll, doc! { "_id": 1i64, "v": 1 }, &k).unwrap());
    engine.set_idempotency_clock_for_test(Some(at.hlc.wall_ms + 3_600_001));
    // A new request now: the document stands, so it is the duplicate an
    // unkeyed insert would be.
    let duplicate = engine.insert_keyed(&coll, doc! { "_id": 1i64, "v": 1 }, &k).unwrap_err();
    assert!(matches!(duplicate, StorageError::Core(_)), "{duplicate:?}");
    assert_eq!(count(&engine, &coll), 1);
}

/// A peer's record past its window is not stored, past the window being the one
/// test of a record's life, when the live record here is another request's or
/// there is none. Here a key reused after its window holds the new request's
/// live record, and a peer that has not yet run retention ships the old
/// request's, with another body: merged, it would lower the live record's
/// `first` to the old request's, so a resume would leave every match changed
/// since, and mark the new request contested. Nothing is written for it, a row
/// included; nor for a past-window record under a key with no record here. (One
/// of the live record's own request, the same fingerprint, is merged: the
/// replication tests' `a_past_window_record_of_the_live_records_own_request_merges`.)
#[test]
fn a_peer_record_past_its_window_is_not_stored() {
    let (engine, coll, _dir, _path) = open();
    engine.set_idempotency_window(DAY);
    let k = keyed(31);
    fresh(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    let live = record(&engine, &coll, &k).unwrap();
    let rows_before = rows(&engine);
    let now = crate::physical_now_ms();
    let elsewhere = keyed(32);
    for (scope, fingerprint) in
        [(k.scope, reused(&k).fingerprint), (elsewhere.scope, elsewhere.fingerprint)]
    {
        let old = IdempotencyRecord {
            fingerprint,
            first: stamp(now - 2 * DAY_MS, 0, 0x77),
            last: stamp(now - 2 * DAY_MS + 1_000, 0, 0x77),
            origin: NodeId::from_bytes([0x77; 16]),
            state: RecordState::Done,
            outcome: RecordOutcome::Modify(ModifyRecorded::default()),
            contested: false,
        };
        let txn = engine.begin_write(crate::WriterHolder::Replication).unwrap();
        let (stored, wrote) =
            engine.store_record_noting(&txn, coll.id, &scope, &old, Some(old.last)).unwrap();
        assert_eq!((stored, wrote), (None, false));
        txn.commit().unwrap();
    }
    assert_eq!(record(&engine, &coll, &k), Some(live), "its first and flag as they were");
    assert_eq!(engine.idempotency_record(coll.id, &elsewhere.scope).unwrap(), None);
    assert_eq!(rows(&engine), rows_before, "no row for either");
    replayed(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
}

// -- Durability ------------------------------------------------------------------------------

/// F3. Under `coalesced` a duplicate that finds the record before its flush
/// waits for that flush, and when it fails both get `OutcomeUnknown`. A replay
/// answered without the barrier would be told `200` for a write the flush then
/// failed to make durable.
#[test]
fn f3_a_coalesced_replay_shares_its_records_failed_flush() {
    let (engine, coll, _dir, _path) = open();
    let engine = Arc::new(engine);
    engine.set_durability(DurabilityClass::Coalesced, std::time::Duration::from_millis(400));
    let k = keyed(19);
    assert!(engine.arm_test_storage_failure("sync_data"));
    let first = {
        let (engine, coll) = (Arc::clone(&engine), coll.clone());
        std::thread::spawn(move || engine.insert_keyed(&coll, doc! { "v": 1 }, &k))
    };
    // The first attempt's commit is visible before its flush.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while record(&engine, &coll, &k).is_none() {
        assert!(std::time::Instant::now() < deadline, "premise: the commit landed");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let duplicate = engine.insert_keyed(&coll, doc! { "v": 1 }, &k);
    let first = first.join().unwrap();
    assert!(matches!(first, Err(StorageError::OutcomeUnknown(_))), "{first:?}");
    assert!(matches!(duplicate, Err(StorageError::OutcomeUnknown(_))), "{duplicate:?}");
}

/// F3's other half: a replay that found the record before its flush answers
/// only once a flush has made it durable.
#[test]
fn f3_a_coalesced_replay_answers_only_after_the_flush() {
    let (engine, coll, _dir, _path) = open();
    let engine = Arc::new(engine);
    engine.set_durability(DurabilityClass::Coalesced, std::time::Duration::from_millis(300));
    let k = keyed(20);
    let fsyncs = engine.fsyncs();
    let first = {
        let (engine, coll) = (Arc::clone(&engine), coll.clone());
        std::thread::spawn(move || engine.insert_keyed(&coll, doc! { "v": 1 }, &k))
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while record(&engine, &coll, &k).is_none() {
        assert!(std::time::Instant::now() < deadline, "premise: the commit landed");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(engine.fsyncs(), fsyncs, "premise: visible and not yet flushed");
    replayed(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    assert!(engine.fsyncs() > fsyncs, "the replay waited for the flush that covers the record");
    fresh(first.join().unwrap().unwrap());
}

/// Under `durable` a replay finds only a record whose commit has fsynced, and
/// so pays no fsync of its own.
#[test]
fn a_durable_replay_pays_no_fsync() {
    let (engine, coll, _dir, _path) = open();
    let k = keyed(21);
    fresh(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    let fsyncs = engine.fsyncs();
    replayed(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    assert_eq!(engine.fsyncs(), fsyncs);
}

/// F1. A keyed insert whose fsync fails is `OutcomeUnknown`; after the reopen
/// the document and its record are both there, and the retry replays.
#[test]
fn f1_a_keyed_insert_whose_fsync_fails_replays_after_the_repair() {
    let (engine, coll, _dir, path) = open();
    let k = keyed(22);
    assert!(engine.arm_test_storage_failure("sync_data"));
    let failed = engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap_err();
    assert!(matches!(failed, StorageError::OutcomeUnknown(_)), "{failed:?}");
    drop(engine);
    let (engine, coll) = reopen(&path);
    assert!(record(&engine, &coll, &k).is_some(), "the record survived with the write");
    let again = replayed(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    assert!(matches!(again.outcome, RecordOutcome::Insert { .. }));
    assert_eq!(count(&engine, &coll), 1, "one document");
}

/// F2. A keyed insert whose commit's first write fails is a plain failure;
/// after the reopen neither the document nor the record is there, and the
/// retry applies once.
#[test]
fn f2_a_keyed_insert_whose_first_write_fails_applies_once_after() {
    let (engine, coll, _dir, path) = open();
    let k = keyed(23);
    assert!(engine.arm_test_storage_failure("write"));
    let failed = engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap_err();
    assert!(!matches!(failed, StorageError::OutcomeUnknown(_)), "{failed:?}");
    drop(engine);
    let (engine, coll) = reopen(&path);
    assert_eq!(record(&engine, &coll, &k), None);
    assert_eq!(count(&engine, &coll), 0);
    fresh(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    replayed(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    assert_eq!(count(&engine, &coll), 1);
}

/// F6. Records survive a restart under both durability classes, and a replay
/// after it answers what it answered before.
#[test]
fn f6_records_survive_a_restart_under_both_classes() {
    for class in [DurabilityClass::Durable, DurabilityClass::Coalesced] {
        let (engine, coll, _dir, path) = open();
        engine.set_durability(class, std::time::Duration::from_millis(2));
        let k = keyed(24);
        counters(&engine, &coll, 3);
        fresh(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
        let k2 = keyed(25);
        fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k2).unwrap());
        let before = (
            replayed(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap()),
            replayed(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k2).unwrap()),
        );
        drop(engine);
        let (engine, coll) = reopen(&path);
        engine.set_durability(class, std::time::Duration::from_millis(2));
        let after = (
            replayed(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap()),
            replayed(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k2).unwrap()),
        );
        assert_eq!(before, after, "{class:?}");
        assert_eq!(ns(&engine, &coll, 3), vec![1, 1, 1], "{class:?}");
    }
}

// -- Multi-chunk requests --------------------------------------------------------------------

/// The record follows the chunks: `applying` with cumulative counts and the
/// cursor after each, a row at each chunk's first entry, `done` at the end; and
/// a retry of a done request replays its counts.
#[test]
fn a_multi_chunk_record_follows_its_chunks() {
    let (engine, coll, _dir, _path) = open();
    counters(&engine, &coll, 3);
    let k = keyed(26);
    let out = fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!((out.matched, out.modified, out.commits, out.skipped), (3, 3, 3, 0));
    assert!(!out.resumed);
    let stored = record(&engine, &coll, &k).unwrap();
    assert_eq!(stored.state, RecordState::Done);
    let m = modify(&stored);
    assert_eq!((m.matched, m.modified, m.commits, m.skipped), (3, 3, 3, 0));
    assert_eq!(m.cursor, Some(crate::docs::doc_key(&DocId::Int64(2)).unwrap()));
    assert_eq!(rows(&engine).len(), 3, "one row per chunk's entry");
    assert!(stored.first < stored.last);
    let again =
        replayed(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!(again, stored);
    assert_eq!(ns(&engine, &coll, 3), vec![1, 1, 1]);
}

/// The end of a keyed request past a full chunk is found by that chunk's
/// lookahead (G8), which writes `done` in the chunk's own record: no
/// record-only commit follows, and the record's `last` is the chunk's last
/// entry.
#[test]
fn the_end_past_a_full_chunk_is_found_by_its_lookahead_and_written_with_it() {
    let (engine, coll, _dir, _path) = open();
    counters(&engine, &coll, 2);
    // Two chunks of one, both full, then a third that finds nothing.
    let k = keyed(27);
    let commits = engine.commits();
    let out = fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!(out.commits, 2, "two chunks wrote");
    assert_eq!(engine.commits(), commits + 2, "and no commit of the record alone");
    let stored = record(&engine, &coll, &k).unwrap();
    assert_eq!(stored.state, RecordState::Done);
    assert_eq!(modify(&stored).commits, 2);
    let last_entry = engine
        .read_oplog_from(Hlc::ZERO, 100, WalkScope::Request)
        .unwrap()
        .iter()
        .map(|e| e.stamp)
        .max()
        .unwrap();
    assert_eq!(stored.last, last_entry, "`done` with the last chunk's entry");
}

/// L1. A record's `last` is the stamp of the last entry its commit wrote. A drop
/// from another member at the HLC of a keyed update's first entry (a tie goes to
/// the drop) takes that entry's document with the earlier life, and leaves the
/// later entry's document standing in the life the drop re-dates; the record,
/// judged by its `last`, stands too, so a retry is answered from it and does
/// not increment the standing document a second time.
#[test]
fn l1_a_drop_at_a_commits_first_entry_leaves_the_record_of_its_later_entry_standing() {
    let (engine, coll, _dir, _path) = open();
    engine
        .insert_many(&coll, vec![doc! { "_id": 1i64, "n": 0i64 }, doc! { "_id": 2i64, "n": 0i64 }])
        .unwrap();
    let k = keyed(90);
    let out = fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!((out.modified, out.commits), (2, 1), "premise: one commit wrote both");
    let stored = record(&engine, &coll, &k).unwrap();
    let mut written: Vec<Stamp> = engine
        .read_oplog_from(Hlc::ZERO, 100, WalkScope::Request)
        .unwrap()
        .iter()
        .filter(|e| e.kind == kimmy_core::OpKind::Replace)
        .map(|e| e.stamp)
        .collect();
    written.sort();
    let (first, last) = (written[0], written[written.len() - 1]);
    assert!(first < last, "premise: two entries");
    assert_eq!((stored.first, stored.last), (first, last), "`last` is the last entry's");
    let drop = crate::engine::ddl_entry(
        Stamp::new(first.hlc, NodeId::from_bytes([0xD1; 16])),
        kimmy_core::OpKind::DropCollection,
        coll.id,
        &kimmy_core::CollectionRef::new("shop", "orders"),
    )
    .unwrap();
    engine.apply_batch(std::slice::from_ref(&drop)).unwrap();
    let life = engine.get_collection("shop", "orders").expect("the later entry's life stands");
    let doc = |id: i64| engine.get(&life, &DocId::Int64(id)).unwrap();
    assert!(doc(1).is_none(), "premise: the drop took the first entry's document");
    assert_eq!(doc(2).unwrap().get_i64("n").unwrap(), 1, "premise: the later entry stands");
    let again =
        replayed(engine.modify_where_keyed(&life, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!(again.last, last);
    assert_eq!(doc(2).unwrap().get_i64("n").unwrap(), 1, "applied once");
}

/// L1 for a keyed bulk insert: the record runs from its first entry to its last.
#[test]
fn l1_a_bulk_inserts_record_runs_from_its_first_entry_to_its_last() {
    let (engine, coll, _dir, _path) = open();
    let k = keyed(91);
    let ids = fresh(
        engine
            .insert_many_keyed(&coll, vec![doc! { "v": 1 }, doc! { "v": 2 }, doc! { "v": 3 }], &k)
            .unwrap(),
    );
    let stored = record(&engine, &coll, &k).unwrap();
    let stamps: Vec<Stamp> = ids.iter().map(|(_, stamp)| *stamp).collect();
    assert_eq!((stored.first, stored.last), (stamps[0], stamps[2]));
    let at: Vec<Stamp> = rows(&engine).iter().map(|(s, ..)| *s).collect();
    assert!(at.contains(&stamps[0]) && at.contains(&stamps[2]), "rows at both: {at:?}");
}

/// The lookahead never ends a request that has a match it could still write:
/// two-document chunks over three documents, the first chunk's lookahead finds
/// the third, the request goes on and writes it, and `done` comes with it.
#[test]
fn the_lookahead_does_not_end_a_request_with_a_match_it_could_write() {
    let (engine, coll, _dir, _path) = open();
    counters(&engine, &coll, 3);
    engine.set_multi_chunk_docs(2);
    let k = keyed(93);
    let out = fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!((out.modified, out.skipped, out.commits), (3, 0, 2), "{out:?}");
    assert_eq!(ns(&engine, &coll, 3), vec![1, 1, 1], "every match written");
    assert_eq!(record(&engine, &coll, &k).unwrap().state, RecordState::Done);
}

/// The lookahead stops at the first match past the chunk that the request
/// could write: what it adds to a chunk's hold on the writer, where the request
/// goes on, is one match. Two-document chunks over four documents read each
/// document once, and the third once more, for the first chunk's lookahead.
#[test]
fn the_lookahead_reads_one_match_where_the_request_goes_on() {
    struct Counted(std::sync::atomic::AtomicUsize);
    impl ModifySpec for Counted {
        fn matches(&self, doc: &Document) -> kimmy_core::Result<bool> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(doc.contains_key("n"))
        }
        fn compare(&self, _: &Document, _: &Document) -> Ordering {
            Ordering::Equal
        }
        fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
            Inc.apply(doc)
        }
        fn upsert(&self) -> Option<std::result::Result<Document, String>> {
            None
        }
    }
    let (engine, coll, _dir, _path) = open();
    counters(&engine, &coll, 4);
    engine.set_multi_chunk_docs(2);
    let spec = Counted(std::sync::atomic::AtomicUsize::new(0));
    let out = fresh(
        engine.modify_where_keyed(&coll, &Candidates::Scan, &spec, None, &keyed(94)).unwrap(),
    );
    assert_eq!((out.modified, out.commits), (4, 2), "{out:?}");
    assert_eq!(spec.0.load(std::sync::atomic::Ordering::Relaxed), 5, "four reads, and one ahead");
}

/// F4. A three-chunk `$inc` that fails at chunk two's commit: after the reopen
/// the retry resumes after chunk one, and every document is incremented once.
#[test]
fn f4_a_three_chunk_inc_that_failed_at_chunk_two_resumes() {
    for call in ["write", "sync_data"] {
        let (engine, coll, _dir, path) = open();
        counters(&engine, &coll, 3);
        let engine = Arc::new(engine);
        let k = keyed(28);
        let armer = Arc::clone(&engine);
        crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
            assert!(armer.arm_test_storage_failure(call));
        });
        let failed = engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k);
        assert!(matches!(failed, Err(StorageError::PartiallyApplied { .. })), "{call}: {failed:?}");
        drop(engine);
        let (engine, coll) = reopen(&path);
        engine.set_multi_chunk_docs(1);
        let stored = record(&engine, &coll, &k).unwrap();
        assert_eq!(stored.state, RecordState::Applying, "{call}");
        let out =
            fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
        assert!(out.resumed, "{call}");
        assert_eq!(out.modified, 3, "{call}: cumulative");
        assert_eq!(out.skipped, 0, "{call}: resumed after its own cursor");
        assert_eq!(ns(&engine, &coll, 3), vec![1, 1, 1], "{call}: each once");
        assert_eq!(record(&engine, &coll, &k).unwrap().state, RecordState::Done);
        replayed(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    }
}

/// F5. The drain-deadline stop between chunks, keyed: the retry on the
/// restarted node resumes.
#[test]
fn f5_a_keyed_request_stopped_between_chunks_resumes() {
    let (engine, coll, _dir, path) = open();
    counters(&engine, &coll, 3);
    let engine = Arc::new(engine);
    let k = keyed(29);
    let stopper = Arc::clone(&engine);
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
        stopper.set_stopping();
    });
    let failed = engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap_err();
    let StorageError::PartiallyApplied { applied, .. } = failed else {
        panic!("a stop after a commit is partly applied: {failed:?}");
    };
    assert_eq!(
        applied,
        crate::Applied::Modify { matched: 1, modified: 1, commits: 1, in_doubt: 0 }
    );
    drop(engine);
    let (engine, coll) = reopen(&path);
    engine.set_multi_chunk_docs(1);
    let out = fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert!(out.resumed);
    assert_eq!((out.matched, out.modified, out.commits), (3, 3, 3));
    assert_eq!(ns(&engine, &coll, 3), vec![1, 1, 1]);
}

/// A short chunk that writes nothing ends a keyed request whose record stands,
/// and marks it `done` in a commit of the record alone. Here the first attempt
/// stopped after its first chunk, two documents of three, and the third was
/// written by another write before the retry: the retry resumes, its one chunk
/// finds the third and leaves it by the stamp rule, and the request ends. Left
/// `applying`, every later retry would resume again rather than replay.
#[test]
fn a_resume_whose_short_last_chunk_writes_nothing_marks_the_record_done() {
    let (engine, coll, _dir, path) = open();
    counters(&engine, &coll, 3);
    engine.set_multi_chunk_docs(2);
    let engine = Arc::new(engine);
    let k = keyed(98);
    let stopper = Arc::clone(&engine);
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
        stopper.set_stopping();
    });
    let failed = engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k);
    assert!(matches!(failed, Err(StorageError::PartiallyApplied { .. })), "{failed:?}");
    drop(engine);
    let (engine, coll) = reopen(&path);
    engine.set_multi_chunk_docs(2);
    assert_eq!(record(&engine, &coll, &k).unwrap().state, RecordState::Applying, "premise");
    // Another write to the third document, after the request's first.
    engine.replace(&coll, &DocId::Int64(2), doc! { "n": 10i64 }, false).unwrap();

    let out = fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert!(out.resumed);
    assert_eq!((out.matched, out.modified, out.skipped), (2, 2, 1));
    assert_eq!(record(&engine, &coll, &k).unwrap().state, RecordState::Done);
    replayed(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!(ns(&engine, &coll, 3), vec![1, 1, 10]);
}

/// The two lines `docs/operations.md` publishes for a keyed write, at the
/// levels it gives them: a resume at `INFO`, with the collection and the counts
/// it carries on from, and a replay at `DEBUG`, below what an operator's
/// default `INFO` shows.
#[test]
fn a_resume_logs_at_info_and_a_replay_below_it() {
    let (engine, coll, _dir, path) = open();
    counters(&engine, &coll, 3);
    let engine = Arc::new(engine);
    let k = keyed(96);
    let stopper = Arc::clone(&engine);
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
        stopper.set_stopping();
    });
    let failed = engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k);
    assert!(matches!(failed, Err(StorageError::PartiallyApplied { .. })), "{failed:?}");
    drop(engine);
    let (engine, coll) = reopen(&path);
    engine.set_multi_chunk_docs(1);

    let (out, logs) = crate::log_capture::logged(|| {
        fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap())
    });
    assert!(out.resumed);
    let line = logs
        .lines()
        .find(|line| line.contains("resumed a keyed write that had stopped"))
        .unwrap_or_else(|| panic!("a resume logs its line at INFO:\n{logs}"));
    assert!(line.contains(" INFO "), "{line}");
    for field in ["db=shop", "collection=orders", "matched=1", "modified=1", "skipped=0"] {
        assert!(line.contains(field), "{field} in {line}");
    }

    let (_, logs) = crate::log_capture::logged(|| {
        replayed(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap())
    });
    assert!(!logs.contains("replayed a keyed write"), "a replay logs at DEBUG:\n{logs}");
}

/// How a test crashes a request between two of its commits.
#[derive(Clone, Copy, Debug)]
enum Crash {
    /// The drain deadline passes after the commit: the next chunk is refused.
    AfterCommit,
    /// The next chunk's commit fails at its first write: it is not there.
    BeforeCommit,
    /// The next chunk's fsync fails: it may be there, and after the repair is.
    FsyncFails,
}

/// Crash the request this thread runs next, at its `k`-th point between two
/// commits.
fn crash_at(engine: &Arc<Engine>, k: usize, how: Crash) {
    let engine = Arc::clone(engine);
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
        if k > 1 {
            crash_at(&engine, k - 1, how);
            return;
        }
        match how {
            Crash::AfterCommit => engine.set_stopping(),
            Crash::BeforeCommit => assert!(engine.arm_test_storage_failure("write")),
            Crash::FsyncFails => assert!(engine.arm_test_storage_failure("sync_data")),
        }
    });
}

/// Drop a crash hook the request never reached (it ended before the point),
/// and the engine it holds with it, so the store can be opened again.
fn disarm() {
    crate::sync::race_hooks::clear();
}

/// I3 on one member, across crashes: a keyed multi-chunk request crashed after
/// any of its commits, or at any commit, retried after the restart and crashed
/// again, writes each document at most once at every point, and finishes with
/// each written once.
#[test]
fn a_multi_chunk_request_across_crashes_writes_each_document_once() {
    for k in 1..=3usize {
        for how in [Crash::AfterCommit, Crash::BeforeCommit, Crash::FsyncFails] {
            let (engine, coll, _dir, path) = open();
            counters(&engine, &coll, 4);
            let key = keyed(90);
            let engine = Arc::new(engine);
            crash_at(&engine, k, how);
            let failed = engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &key);
            assert!(failed.is_err(), "{k} {how:?}: premise: it crashed");
            disarm();
            drop(engine);
            let (engine, coll) = reopen(&path);
            assert!(ns(&engine, &coll, 4).iter().all(|n| (0..=1).contains(n)), "{k} {how:?}");
            engine.set_multi_chunk_docs(1);
            // The resume crashes too, one commit in.
            let engine = Arc::new(engine);
            crash_at(&engine, 1, Crash::AfterCommit);
            let _ = engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &key);
            disarm();
            drop(engine);
            let (engine, coll) = reopen(&path);
            assert!(ns(&engine, &coll, 4).iter().all(|n| (0..=1).contains(n)), "{k} {how:?}");
            engine.set_multi_chunk_docs(1);
            let out =
                engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &key).unwrap();
            assert_eq!(ns(&engine, &coll, 4), vec![1, 1, 1, 1], "{k} {how:?}: {out:?}");
            let done = record(&engine, &coll, &key).unwrap();
            assert_eq!(done.state, RecordState::Done, "{k} {how:?}");
            assert_eq!(modify(&done).modified, 4, "{k} {how:?}: every write counted once");
            replayed(
                engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &key).unwrap(),
            );
        }
    }
}

/// A duplicate while a keyed multi-chunk request runs here is refused, on any
/// keyed route; the place is given up on every way out, an error and a panic
/// included.
#[test]
fn a_duplicate_while_a_multi_chunk_request_runs_is_in_flight() {
    let (engine, coll, _dir, _path) = open();
    counters(&engine, &coll, 3);
    let engine = Arc::new(engine);
    let k = keyed(30);
    let (inner, c) = (Arc::clone(&engine), coll.clone());
    let answers = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let seen = Arc::clone(&answers);
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
        let multi = inner.modify_where_keyed(&c, &Candidates::Scan, &Inc, None, &k);
        let insert = inner.insert_keyed(&c, doc! {}, &k);
        let single = inner.modify_where_keyed(&c, &Candidates::Scan, &Inc, Some(1), &k);
        seen.lock().extend([
            matches!(multi, Err(StorageError::IdempotencyKeyInFlight)),
            matches!(insert, Err(StorageError::IdempotencyKeyInFlight)),
            matches!(single, Err(StorageError::IdempotencyKeyInFlight)),
        ]);
    });
    fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!(*answers.lock(), vec![true, true, true]);
    replayed(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());

    // An early return: a failure in chunk two.
    struct FailSecond(std::sync::atomic::AtomicUsize);
    impl ModifySpec for FailSecond {
        fn matches(&self, doc: &Document) -> kimmy_core::Result<bool> {
            Ok(doc.contains_key("n"))
        }
        fn compare(&self, _: &Document, _: &Document) -> Ordering {
            Ordering::Equal
        }
        fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
            match self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                1 => Err("the second document is refused".into()),
                3 => panic!("a panic in chunk three, on purpose"),
                _ => Inc.apply(doc),
            }
        }
        fn upsert(&self) -> Option<std::result::Result<Document, String>> {
            None
        }
    }
    let k2 = keyed(31);
    let spec = FailSecond(Default::default());
    let failed = engine.modify_where_keyed(&coll, &Candidates::Scan, &spec, None, &k2);
    assert!(matches!(failed, Err(StorageError::PartiallyApplied { .. })), "{failed:?}");
    // Resumed, and panicking in its own later chunk.
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        engine.modify_where_keyed(&coll, &Candidates::Scan, &spec, None, &k2)
    }));
    assert!(unwound.is_err(), "premise: the spec panicked");
    // Not in flight after either: the retry resumes.
    let out = fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k2).unwrap());
    assert!(out.resumed);
}

/// I3 on one member: a resume of another member's attempt starts from the
/// beginning and writes only matches stored below the merged `first`, so a
/// document the first attempt wrote is skipped and counted, and every document
/// is written once across all attempts.
#[test]
fn a_resume_of_another_members_attempt_writes_each_document_once() {
    let (a, ca, _da, _) = open();
    let (b, cb, _db, _) = open();
    // A document B alone holds, keyed below every one A's attempt reached: a
    // cursor from A would pass over it, and that attempt never saw it.
    b.insert(&cb, doc! { "_id": -1i64, "n": 0i64 }).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(2));
    counters(&a, &ca, 3);
    // B holds the same three documents, as A's bulk replicated.
    for entry in a.read_oplog_from(Hlc::ZERO, 100, WalkScope::Request).unwrap() {
        if entry.doc_id.is_some() {
            b.apply_remote(&cb, &entry).unwrap();
        }
    }
    b.set_multi_chunk_docs(1);
    let a = Arc::new(a);
    let k = keyed(32);
    let stopper = Arc::clone(&a);
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
        stopper.set_stopping();
    });
    assert!(a.modify_where_keyed(&ca, &Candidates::Scan, &Inc, None, &k).is_err());
    a.lift_stops_for_test();
    // B pulls A's chunk and its record, in one transaction, as a frame does.
    let record = a.idempotency_record(ca.id, &k.scope).unwrap().unwrap();
    let chunk_one: Vec<_> = a
        .read_oplog_from(Hlc::ZERO, 100, WalkScope::Request)
        .unwrap()
        .into_iter()
        .filter(|e| e.stamp >= record.first)
        .collect();
    assert_eq!(chunk_one.len(), 1);
    {
        let txn = b.begin_write(crate::WriterHolder::Replication).unwrap();
        b.apply_remote_in_txn(&txn, &cb, &chunk_one[0], crate::engine::Position::Raise).unwrap();
        b.store_record(&txn, cb.id, &k.scope, &record, Some(chunk_one[0].stamp)).unwrap();
        txn.commit().unwrap();
    }
    assert_eq!(ns(&b, &cb, 3), vec![1, 0, 0]);
    let out = fresh(b.modify_where_keyed(&cb, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert!(out.resumed);
    assert_eq!(out.skipped, 1, "A's write, left");
    assert_eq!(ns(&b, &cb, 3), vec![1, 1, 1], "each once");
    let below = b.get(&cb, &DocId::Int64(-1)).unwrap().unwrap();
    assert_eq!(below.get_i64("n").unwrap(), 1, "from the beginning, not from A's cursor");
    let merged = b.idempotency_record(cb.id, &k.scope).unwrap().unwrap();
    assert_eq!(merged.first, record.first, "the merged first is A's");
    assert_eq!(merged.origin, b.node_id());
    assert_eq!(merged.state, RecordState::Done);
}

/// Every chunk after a record exists holds to the stamp rule, a fresh
/// attempt's later chunks included: another attempt's write that arrived
/// between two chunks, with its record, is not written again.
#[test]
fn a_later_chunk_leaves_what_another_attempt_wrote_meanwhile() {
    let (engine, coll, _dir, _path) = open();
    counters(&engine, &coll, 3);
    let engine = Arc::new(engine);
    let k = keyed(33);
    let (inner, c) = (Arc::clone(&engine), coll.clone());
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
        // Another member's attempt wrote document 2 above this attempt's
        // `first`, and its record merged in with that entry.
        let live = inner.idempotency_record(c.id, &k.scope).unwrap().unwrap();
        let other = NodeId::from_bytes([0x42; 16]);
        let at = Stamp::new(live.last.hlc.successor().successor(), other);
        let body = bson::serialize_to_vec(&doc! { "_id": 2i64, "n": 1i64 }).unwrap();
        let entry = kimmy_core::OplogEntry {
            stamp: at,
            kind: kimmy_core::OpKind::Replace,
            collection: c.id,
            doc_id: Some(DocId::Int64(2)),
            body: Some(body),
        };
        let theirs = IdempotencyRecord {
            first: at,
            last: at,
            origin: other,
            state: RecordState::Applying,
            ..live
        };
        let txn = inner.begin_write(crate::WriterHolder::Replication).unwrap();
        inner.apply_remote_in_txn(&txn, &c, &entry, crate::engine::Position::Raise).unwrap();
        inner.store_record(&txn, c.id, &k.scope, &theirs, Some(at)).unwrap();
        txn.commit().unwrap();
    });
    let out = fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!(ns(&engine, &coll, 3), vec![1, 1, 1], "document 2 once, by the other attempt");
    assert_eq!(out.skipped, 1);
    assert!(!out.resumed);
}

// -- A contested key ------------------------------------------------------------------------

/// Merge into `k`'s record, as a peer's store does, the record of another
/// request under the key, written a millisecond before this one's `first`: the
/// stored record turns contested, its winner still this request's.
fn contest(engine: &Engine, coll: &CollectionMeta, k: &KeyedWrite) {
    let live = engine.idempotency_record(coll.id, &k.scope).unwrap().expect("a record to contest");
    let other = NodeId::from_bytes([0x43; 16]);
    let at = Stamp::new(Hlc::new(live.first.hlc.wall_ms - 1, 0), other);
    let theirs = IdempotencyRecord {
        fingerprint: reused(k).fingerprint,
        first: at,
        last: at,
        origin: other,
        ..live
    };
    let txn = engine.begin_write(crate::WriterHolder::Replication).unwrap();
    let merged = engine.store_record(&txn, coll.id, &k.scope, &theirs, Some(at)).unwrap().unwrap();
    txn.commit().unwrap();
    assert!(merged.contested, "premise: two fingerprints met");
    assert_eq!(merged.fingerprint, k.fingerprint, "premise: this request's record wins");
}

fn refused(failed: StorageError) {
    assert!(matches!(failed, StorageError::IdempotencyKeyReused), "{failed:?}");
}

/// A keyed write refused as a reused key, having committed nothing.
fn refused_writing_nothing<T: std::fmt::Debug>(
    engine: &Engine,
    write: impl FnOnce() -> crate::Result<Keyed<T>>,
) {
    let commits = engine.commits();
    refused(write().unwrap_err());
    assert_eq!(engine.commits(), commits, "nothing committed");
}

/// A contested record is refused (422) on every keyed path, before a replay,
/// a resume or a delete's tombstone test, for the fingerprint that won the merge
/// as for the other: neither client's retry is answered from it. Nothing is
/// written.
#[test]
fn a_contested_key_is_refused_on_every_keyed_path() {
    let (engine, coll, _dir, _path) = open();
    engine.insert_stamped(&coll, doc! { "_id": 8i64 }).unwrap();
    engine.insert_stamped(&coll, doc! { "_id": 9i64, "n": 0i64 }).unwrap();

    let k = keyed(110);
    fresh(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    contest(&engine, &coll, &k);
    refused_writing_nothing(&engine, || engine.insert_keyed(&coll, doc! { "v": 1 }, &k));
    refused_writing_nothing(&engine, || engine.insert_keyed(&coll, doc! { "v": 2 }, &reused(&k)));

    let k = keyed(111);
    fresh(engine.insert_many_keyed(&coll, vec![doc! { "v": 1 }], &k).unwrap());
    contest(&engine, &coll, &k);
    refused_writing_nothing(&engine, || {
        engine.insert_many_keyed(&coll, vec![doc! { "v": 1 }], &k).map_err(|e| e.source)
    });

    let k = keyed(112);
    fresh(engine.replace_keyed(&coll, &DocId::Int64(7), doc! { "v": 1 }, true, None, &k).unwrap());
    contest(&engine, &coll, &k);
    refused_writing_nothing(&engine, || {
        engine.replace_keyed(&coll, &DocId::Int64(7), doc! { "v": 1 }, true, None, &k)
    });

    let k = keyed(113);
    fresh(engine.delete_keyed(&coll, &DocId::Int64(8), None, &k).unwrap());
    contest(&engine, &coll, &k);
    refused_writing_nothing(&engine, || engine.delete_keyed(&coll, &DocId::Int64(8), None, &k));

    let k = keyed(114);
    fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    contest(&engine, &coll, &k);
    refused_writing_nothing(&engine, || {
        engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k)
    });

    let k = keyed(115);
    fresh(
        engine
            .find_and_modify_keyed(&coll, &Candidates::Scan, &Inc, &k, ReturnedImage::After)
            .unwrap(),
    );
    contest(&engine, &coll, &k);
    refused_writing_nothing(&engine, || {
        engine.find_and_modify_keyed(&coll, &Candidates::Scan, &Inc, &k, ReturnedImage::After)
    });
}

/// A contested key in flight: a multi-chunk request whose record turns
/// contested between its chunks stops, partially applied (ADR-192) with the
/// chunk it committed, and writes nothing more: the merged `first` is another
/// request's, no bound for this one's stamp rule. Every retry is then refused,
/// a resume included.
#[test]
fn kr_a_request_whose_record_turns_contested_between_chunks_ends_partially_applied() {
    let (engine, coll, _dir, _path) = open();
    counters(&engine, &coll, 3);
    let engine = Arc::new(engine);
    let k = keyed(116);
    let (inner, c) = (Arc::clone(&engine), coll.clone());
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
        contest(&inner, &c, &k);
    });
    let failed = engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap_err();
    let StorageError::PartiallyApplied { applied, cause } = failed else {
        panic!("partially applied was expected: {failed:?}");
    };
    refused(*cause);
    assert!(
        matches!(applied, crate::Applied::Modify { matched: 1, modified: 1, commits: 1, .. }),
        "{applied:?}"
    );
    assert_eq!(ns(&engine, &coll, 3), vec![1, 0, 0], "nothing after the first chunk");
    let record = record(&engine, &coll, &k).unwrap();
    assert_eq!((record.state, record.contested), (RecordState::Applying, true));
    refused(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap_err());
    assert_eq!(ns(&engine, &coll, 3), vec![1, 0, 0]);
}

// -- Retention ------------------------------------------------------------------------------

/// Store in one transaction while the window's clock reads `now_ms`, then give
/// the window back the real clock: a record older than one window at the real
/// clock is refused on store, and a retention test must hold one
/// first. Retention itself takes its cutoff from the pass, not from this clock.
fn store_while_the_window_reads(
    engine: &Engine,
    now_ms: u64,
    store: impl FnOnce(&crate::engine::WriteTxn<'_>),
) {
    engine.set_idempotency_clock_for_test(Some(now_ms));
    let txn = engine.begin_write(crate::WriterHolder::Repair).unwrap();
    store(&txn);
    txn.commit().unwrap();
    engine.set_idempotency_clock_for_test(None);
}

/// F7. A record goes one window after its `last`, not before; one a later
/// chunk changed survives its first row's expiry; one stored from a page goes
/// by its row at `last`.
#[test]
fn f7_retention_collects_a_record_one_window_after_its_last() {
    let (engine, coll, _dir, _path) = open();
    let policy = RetentionPolicy::new(DAY, DAY);
    let k = keyed(34);
    let (_, at) = fresh(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    let kept = engine.collect_garbage_at(at.hlc.wall_ms + DAY_MS, policy).unwrap();
    assert_eq!(kept.records_removed, 0, "not before its time");
    assert!(record(&engine, &coll, &k).is_some());
    let gone = engine.collect_garbage_at(at.hlc.wall_ms + DAY_MS + 1, policy).unwrap();
    assert_eq!(gone.records_removed, 1);
    assert_eq!(record(&engine, &coll, &k), None);
    assert!(rows(&engine).is_empty());
    assert_eq!(engine.idempotency_records_collected(), 1);

    // A multi-chunk request whose second chunk is ten seconds after its first.
    counters(&engine, &coll, 2);
    let engine = Arc::new(engine);
    let k2 = keyed(35);
    let mover = Arc::clone(&engine);
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
        let now = crate::physical_now_ms() + 10_000;
        mover.advance_clock_past(&Stamp::new(Hlc::new(now, 0), mover.node_id()));
    });
    fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k2).unwrap());
    let stored = record(&engine, &coll, &k2).unwrap();
    let first_row = rows(&engine).iter().map(|(s, ..)| *s).min().unwrap();
    assert!(stored.last.hlc.wall_ms >= first_row.hlc.wall_ms + 10_000, "premise");
    let between = first_row.hlc.wall_ms + DAY_MS + 1;
    let held_rows = rows(&engine);
    let pass = engine.collect_garbage_at(between, policy).unwrap();
    assert_eq!(pass.records_removed, 0, "a later chunk changed it");
    assert_eq!(record(&engine, &coll, &k2), Some(stored.clone()));
    assert_eq!(stored.first, first_row, "premise: the expired row is the record's first");
    assert_eq!(rows(&engine), held_rows, "a live record's row at `first` stays (F1)");
    let pass = engine.collect_garbage_at(stored.last.hlc.wall_ms + DAY_MS + 1, policy).unwrap();
    assert_eq!(pass.records_removed, 1);

    // Stored from a page: no entry here, only its row at `last`. Stored while
    // the window's clock reads its `last`, since a record past its window is
    // refused.
    let page_record = base(3, stamp(5_000, 0, 9), stamp(6_000, 0, 9), 9, RecordState::Done);
    store_while_the_window_reads(&engine, 6_000, |txn| {
        engine.store_record(txn, coll.id, &ScopeHash([3; 32]), &page_record, None).unwrap();
    });
    assert_eq!(rows(&engine).iter().filter(|(s, ..)| *s == page_record.last).count(), 1);
    let pass = engine.collect_garbage_at(6_000 + DAY_MS + 1, policy).unwrap();
    assert_eq!(pass.records_removed, 1);
}

/// A record is judged by its `last` at whichever of its rows retention reaches,
/// not only at its `first`: a three-chunk request, ten seconds between chunks,
/// keeps a row at its second chunk's entry that is neither its `first` nor its
/// `last`. When that row expires, the record, whose `last` is still inside the
/// window, stays, and a retry replays.
#[test]
fn a_record_stays_when_a_row_between_its_first_and_last_expires() {
    fn later_by(engine: &Engine, ms: u64) {
        let now = crate::physical_now_ms() + ms;
        engine.advance_clock_past(&Stamp::new(Hlc::new(now, 0), engine.node_id()));
    }
    let (engine, coll, _dir, _path) = open();
    let policy = RetentionPolicy::new(DAY, DAY);
    counters(&engine, &coll, 3);
    let engine = Arc::new(engine);
    let k = keyed(97);
    let mover = Arc::clone(&engine);
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
        later_by(&mover, 10_000);
        crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
            later_by(&mover, 20_000);
        });
    });
    fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    crate::sync::race_hooks::clear();
    let stored = record(&engine, &coll, &k).unwrap();
    let held: Vec<Stamp> = rows(&engine).iter().map(|(s, ..)| *s).collect();
    assert_eq!(held.len(), 3, "premise: a row at each chunk's entry: {held:?}");
    let (first, middle, last) = (held[0], held[1], held[2]);
    assert_eq!((stored.first, stored.last), (first, last), "premise");
    assert!(last.hlc.wall_ms >= middle.hlc.wall_ms + 10_000, "premise: {held:?}");

    let pass = engine.collect_garbage_at(middle.hlc.wall_ms + DAY_MS + 1, policy).unwrap();
    assert_eq!(pass.records_removed, 0, "its `last` is inside the window");
    assert_eq!(record(&engine, &coll, &k), Some(stored));
    let left: Vec<Stamp> = rows(&engine).iter().map(|(s, ..)| *s).collect();
    assert_eq!(left, [first, last], "the expired row goes, the one at `first` stays (F1)");
    replayed(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!(ns(&engine, &coll, 3), vec![1, 1, 1]);
}

/// The records step runs whatever the oplog step found:
/// here the oplog step removes nothing, every entry being inside its window,
/// and a record a peer sent with an older `last` still goes; and on a store
/// with no oplog at all, where the oplog step ends at once.
#[test]
fn the_records_step_runs_when_the_oplog_step_has_nothing_to_do() {
    let (engine, coll, _dir, _path) = open();
    let now = crate::physical_now_ms();
    let old = stamp(now - 2 * DAY_MS, 0, 9);
    let peers = base(4, old, old, 9, RecordState::Done);
    // Stored while it was inside its window: a record past it is refused.
    store_while_the_window_reads(&engine, old.hlc.wall_ms, |txn| {
        engine.store_record(txn, coll.id, &ScopeHash([4; 32]), &peers, None).unwrap();
    });
    assert_eq!(engine.idempotency_records().unwrap(), 1, "premise: stored");
    let pass = engine.collect_garbage_at(now, RetentionPolicy::new(DAY, 365 * DAY)).unwrap();
    assert_eq!(pass.oplog_removed, 0, "premise: nothing in the oplog has expired");
    assert_eq!(pass.records_removed, 1, "and the records step still ran");
    assert!(!pass.is_empty());

    let dir = tempfile::tempdir().unwrap();
    let bare = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    assert_eq!(bare.oplog_entries().unwrap(), 0, "premise: no oplog");
    store_while_the_window_reads(&bare, old.hlc.wall_ms, |txn| {
        bare.store_record(txn, CollectionId(77), &ScopeHash([4; 32]), &peers, None).unwrap();
    });
    assert_eq!(bare.idempotency_records().unwrap(), 1, "premise: stored");
    let pass = bare.collect_garbage_at(now, RetentionPolicy::new(DAY, DAY)).unwrap();
    assert_eq!((pass.oplog_removed, pass.records_removed), (0, 1));
}

/// A stop in the records step ends it between two chunks: what the first chunk
/// committed stands and is counted, and the next pass finds the rest.
#[test]
fn a_stop_ends_the_records_step_between_chunks() {
    let (engine, coll, _dir, _path) = open();
    let n = idempotency::RECORD_COLLECT_CHUNK + 500;
    // Stored while the window's clock reads the first of them: a record past its
    // window is refused.
    store_while_the_window_reads(&engine, 1_000, |txn| {
        for i in 0..n {
            let mut scope = [0u8; 32];
            scope[..8].copy_from_slice(&(i as u64).to_be_bytes());
            let s = stamp(1_000 + i as u64, 0, 9);
            let r = base(5, s, s, 9, RecordState::Done);
            engine.store_record(txn, coll.id, &ScopeHash(scope), &r, None).unwrap();
        }
    });
    assert_eq!(engine.idempotency_records().unwrap(), n as u64, "premise: all stored");
    let cutoff = Hlc::new(1_000_000_000, 0);
    // A chunk's rows, and the one seek that finds the origin they belong to.
    engine.stop_walks_after_rows(idempotency::RECORD_COLLECT_CHUNK as u64 + 2);
    let stopped = engine.collect_idempotency_records(cutoff);
    assert!(matches!(stopped, Err(StorageError::Stopping(_))), "{stopped:?}");
    assert_eq!(engine.idempotency_records().unwrap(), 500, "the first chunk stands");
    assert_eq!(engine.idempotency_records_collected(), idempotency::RECORD_COLLECT_CHUNK as u64);
    engine.lift_stops_for_test();
    assert_eq!(engine.collect_idempotency_records(cutoff).unwrap(), 500);
    assert_eq!(engine.idempotency_records().unwrap(), 0);
}

/// A record whose own row at `last` is missing is still collected, by any row
/// below the cutoff that names it: its `last` is below that row's stamp, so it
/// is past its window.
#[test]
fn a_record_whose_row_at_last_is_missing_is_still_collected() {
    let (engine, coll, _dir, _path) = open();
    let r = base(6, stamp(1_000, 0, 9), stamp(1_000, 0, 9), 9, RecordState::Done);
    // Stored while it was inside its window: a record past it is refused.
    store_while_the_window_reads(&engine, 1_000, |txn| {
        engine
            .store_record(txn, coll.id, &ScopeHash([6; 32]), &r, Some(stamp(2_000, 0, 9)))
            .unwrap();
        txn.open_table(tables::IDEMPOTENCY_BY_STAMP)
            .unwrap()
            .remove(idempotency::row_key(&r.last).as_slice())
            .unwrap();
    });
    assert_eq!(engine.idempotency_records().unwrap(), 1, "premise: stored");
    assert_eq!(engine.collect_idempotency_records(Hlc::new(3_000, 0)).unwrap(), 1);
}

// -- The clock at open -----------------------------------------------------------------------

/// A record stored from a sender whose clock is ahead is folded into the clock,
/// and after a restart the clock resumes above it: the last key of the rows by
/// stamp is in the high water at open. After a restart the change log's high
/// water is above it too (its key was minted after the record was witnessed); after
/// a restore, which does not carry the log, only the rows' is.
#[test]
fn the_clock_resumes_above_every_record_after_a_restart() {
    let (engine, coll, _dir, path) = open();
    let ahead =
        Stamp::new(Hlc::new(crate::physical_now_ms() + 3_600_000, 0), NodeId::from_bytes([9; 16]));
    let r = base(7, ahead, ahead, 9, RecordState::Applying);
    {
        let txn = engine.begin_write(crate::WriterHolder::Repair).unwrap();
        engine.store_record(&txn, coll.id, &ScopeHash([7; 32]), &r, None).unwrap();
        txn.commit().unwrap();
    }
    assert!(engine.next_stamp() > ahead, "witnessed while it runs");
    drop(engine);
    let (engine, _) = reopen(&path);
    assert!(engine.next_stamp() > ahead, "and after a restart");
    // A restore carries the records and not the change log, which begins again: the
    // records' own high water is what the clock resumes above there.
    let mut buf = Vec::new();
    engine.backup_to(&mut buf, WalkScope::Request).unwrap();
    let dest = tempfile::tempdir().unwrap();
    let restored = dest.path().join("restored.redb");
    crate::backup::restore(&restored, &mut buf.as_slice()).unwrap();
    let (engine, _) = reopen(&restored);
    assert!(engine.next_stamp() > ahead, "and after a restore");
}

/// The change log's keys are minted stamps, and a page's base is one of them: the clock
/// resumes above the last key after a restart, even where the key is the only stamp
/// this member holds that high (its clock moved ahead, then it stored a record whose own
/// stamps are behind). Below it, the next change's key could fall under a base already
/// handed out, and that page's delta would miss it.
#[test]
fn the_clock_resumes_above_the_change_logs_keys_after_a_restart() {
    let (engine, coll, _dir, path) = open();
    let now = Stamp::new(Hlc::new(crate::physical_now_ms(), 0), NodeId::from_bytes([9; 16]));
    let ahead = Stamp::new(Hlc::new(crate::physical_now_ms() + 3_600_000, 0), engine.node_id());
    engine.advance_clock_past(&ahead);
    let r = base(8, now, now, 9, RecordState::Done);
    {
        let txn = engine.begin_write(crate::WriterHolder::Repair).unwrap();
        engine.store_record(&txn, coll.id, &ScopeHash([8; 32]), &r, None).unwrap();
        txn.commit().unwrap();
    }
    let key = {
        use redb::ReadableDatabase;
        let txn = engine.db().begin_read().unwrap();
        crate::idempotency::changes_high_water(&txn).unwrap().expect("a change was logged")
    };
    assert!(key > ahead, "premise: the log's key is minted on the clock moved ahead");
    drop(engine);
    let (engine, _) = reopen(&path);
    assert!(engine.next_stamp() > key, "the clock resumes above the log's last key");
}

// -- Purge, sweep, rewind, backup --------------------------------------------------------------

/// A drop's purge leaves the collection's records (they outlive the drop): it
/// removes the documents and index entries, and retention alone removes a
/// record, one window after its `last`.
#[test]
fn a_drops_purge_leaves_the_collections_records() {
    let (engine, coll, _dir, _path) = open();
    let other = engine.create_collection("shop", "items").unwrap();
    for n in 0..5u8 {
        fresh(engine.insert_keyed(&coll, doc! {}, &keyed(40 + n)).unwrap());
    }
    fresh(engine.insert_keyed(&other, doc! {}, &keyed(50)).unwrap());
    engine.drop_collection("shop", "orders").unwrap();
    engine.finish_purges_now().unwrap();
    assert_eq!(count(&engine, &coll), 0, "premise: the purge ran");
    assert_eq!(engine.idempotency_records().unwrap(), 6, "every record stands");
    for n in 0..5u8 {
        assert!(record(&engine, &coll, &keyed(40 + n)).is_some());
    }
    assert_eq!(rows(&engine).len(), 6, "and every row");
}

/// The purge leaves a record of either life: the dropped life's, and one a
/// window stored for a write stamped after the drop, a write the next life
/// keeps (ADR-236). A retry of either after the name is created again replays.
#[test]
fn a_drops_purge_leaves_a_record_of_either_life() {
    let (engine, coll, _dir, _path) = open();
    let old = keyed(60);
    fresh(engine.insert_keyed(&coll, doc! { "_id": 1i64 }, &old).unwrap());
    engine.drop_collection("shop", "orders").unwrap();
    let dropped = engine.collection_dropped_at(coll.id).unwrap().expect("a tombstone");
    // As a window brings it, the collection absent here and its purge pending.
    let k = keyed(61);
    let at = Stamp::new(dropped.hlc.successor(), NodeId::from_bytes([9; 16]));
    let later = IdempotencyRecord {
        fingerprint: k.fingerprint,
        first: at,
        last: at,
        origin: at.node,
        state: RecordState::Done,
        outcome: RecordOutcome::Insert { stamp: at },
        contested: false,
    };
    {
        let txn = engine.begin_write(crate::WriterHolder::Replication).unwrap();
        engine.store_record(&txn, coll.id, &k.scope, &later, Some(at)).unwrap();
        txn.commit().unwrap();
    }
    engine.finish_purges_now().unwrap();
    assert!(record(&engine, &coll, &old).is_some(), "the dropped life's record stays");
    assert_eq!(record(&engine, &coll, &k), Some(later), "the later life's stays");
    let again = engine.create_collection("shop", "orders").unwrap();
    let replay = replayed(engine.insert_keyed(&again, doc! { "_id": 2i64 }, &k).unwrap());
    assert_eq!(replay.first, at);
    replayed(engine.insert_keyed(&again, doc! { "_id": 1i64 }, &old).unwrap());
    assert_eq!(count(&engine, &again), 0, "nothing written again");
}

/// A database drop buries each collection the same way, and the purge that
/// follows leaves each one's records: a retry once the names stand again
/// replays.
#[test]
fn a_database_drop_leaves_every_collections_records() {
    let (engine, coll, _dir, _path) = open();
    let items = engine.create_collection("shop", "items").unwrap();
    fresh(engine.insert_keyed(&coll, doc! {}, &keyed(51)).unwrap());
    fresh(engine.insert_keyed(&items, doc! {}, &keyed(52)).unwrap());
    assert!(engine.drop_database("shop").unwrap());
    engine.finish_purges_now().unwrap();
    assert_eq!(engine.idempotency_records().unwrap(), 2);
    let again = engine.create_collection("shop", "items").unwrap();
    replayed(engine.insert_keyed(&again, doc! {}, &keyed(52)).unwrap());
    assert_eq!(count(&engine, &again), 0);
}

/// A late drop's sweep leaves the records of the life before (they outlive the
/// drop). Here a peer's drop lands between two of this member's writes, a keyed
/// one before it and an unkeyed one after: the sweep takes the keyed write's
/// document with the life before and keeps the later one, and the keyed write's
/// record stays, so a retry replays and writes nothing to the life the drop
/// re-dated.
#[test]
fn a_late_drops_sweep_leaves_the_records_of_the_life_before() {
    let (engine, coll, _dir, _path) = open();
    let k = keyed(53);
    let (_, before) =
        fresh(engine.insert_keyed(&coll, doc! { "_id": 1i64, "n": 0i64 }, &k).unwrap());
    let (_, after) = engine.insert_stamped(&coll, doc! { "_id": 2i64, "n": 0i64 }).unwrap();
    assert!(before.hlc < after.hlc, "premise: two milliseconds or counters apart");
    let drop = crate::engine::ddl_entry(
        Stamp::new(before.hlc, NodeId::from_bytes([0xD1; 16])),
        kimmy_core::OpKind::DropCollection,
        coll.id,
        &kimmy_core::CollectionRef::new("shop", "orders"),
    )
    .unwrap();
    engine.apply_batch(std::slice::from_ref(&drop)).unwrap();
    let life = engine.get_collection("shop", "orders").expect("the later write's life stands");
    assert!(engine.get(&life, &DocId::Int64(1)).unwrap().is_none(), "premise: swept");
    assert!(engine.get(&life, &DocId::Int64(2)).unwrap().is_some(), "premise: kept");
    assert!(record(&engine, &life, &k).is_some(), "the record stays");
    let replay = replayed(engine.insert_keyed(&life, doc! { "_id": 1i64, "n": 0i64 }, &k).unwrap());
    assert_eq!(replay.first, before);
    assert!(engine.get(&life, &DocId::Int64(1)).unwrap().is_none(), "nothing written again");
}

/// A rewind removes the record of a request it discards whole, with every row
/// by stamp that names it, and reopens one it cuts part-way, its rows kept (the
/// one at its `last` included), which a retry then resumes from the beginning:
/// what the rewind undid is written again, and nothing the request still holds.
#[test]
fn a_rewind_removes_or_reopens_the_records_it_cuts() {
    let (engine, coll, _dir, _path) = open();
    counters(&engine, &coll, 3);
    let engine = Arc::new(engine);
    let k = keyed(60);
    let until = Arc::new(parking_lot::Mutex::new(None));
    let (mover, mark) = (Arc::clone(&engine), Arc::clone(&until));
    let once = std::sync::atomic::AtomicBool::new(false);
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
        if !once.swap(true, std::sync::atomic::Ordering::SeqCst) {
            *mark.lock() = Some(mover.clock_last());
            let later = crate::physical_now_ms() + 10_000;
            mover.advance_clock_past(&Stamp::new(Hlc::new(later, 0), mover.node_id()));
        }
    });
    fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    let k2 = keyed(61);
    fresh(engine.insert_keyed(&coll, doc! { "_id": 9i64 }, &k2).unwrap());
    let until = until.lock().expect("the hook ran");
    let naming = |engine: &Engine, k: &KeyedWrite| -> Vec<Stamp> {
        rows(engine).into_iter().filter(|(_, _, scope)| *scope == k.scope.0).map(|r| r.0).collect()
    };
    let (kept_rows, gone_rows) = (naming(&engine, &k), naming(&engine, &k2));
    assert!(kept_rows.len() >= 2 && !gone_rows.is_empty(), "premise: {kept_rows:?} {gone_rows:?}");
    let out = engine.rewind_to(until).unwrap();
    assert_eq!((out.records_removed, out.records_reopened), (1, 1));
    assert_eq!(record(&engine, &coll, &k2), None, "discarded whole");
    assert_eq!(naming(&engine, &k2), Vec::<Stamp>::new(), "and no row names it");
    assert_eq!(naming(&engine, &k), kept_rows, "the reopened record's rows stay");
    let open = record(&engine, &coll, &k).unwrap();
    assert_eq!(open.state, RecordState::Applying);
    assert_eq!(modify(&open).cursor, None);
    assert_eq!(ns(&engine, &coll, 3), vec![1, 0, 0], "premise: chunks two and three undone");
    engine.set_multi_chunk_docs(1);
    let again = fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert!(again.resumed);
    assert_eq!(ns(&engine, &coll, 3), vec![1, 1, 1]);
}

/// A rewind that cuts a keyed bulk insert part-way removes its record: a bulk
/// minted in the millisecond the rewind is to keeps a prefix, and nothing
/// resumes a bulk, so a reopened record would replay "inserted 3" while two
/// documents stand. The retry is a fresh attempt, which meets the first
/// document that stands as a duplicate and writes nothing.
#[test]
fn a_rewind_that_cuts_a_bulk_part_way_removes_its_record() {
    let (engine, coll, _dir, _path) = open();
    let k = keyed(64);
    let docs = vec![doc! { "v": 0 }, doc! { "v": 1 }, doc! { "v": 2 }];
    let ids = fresh(engine.insert_many_keyed(&coll, docs.clone(), &k).unwrap());
    let stamps: Vec<Stamp> = ids.iter().map(|(_, stamp)| *stamp).collect();
    let until = stamps[1].hlc;
    assert!(stamps[0].hlc <= until && until < stamps[2].hlc, "premise: {stamps:?}");
    let out = engine.rewind_to(until).unwrap();
    assert_eq!((out.records_removed, out.records_reopened), (1, 0));
    assert_eq!(record(&engine, &coll, &k), None, "cut part-way, and not reopened");
    assert!(
        rows(&engine).into_iter().all(|(_, _, scope)| scope != k.scope.0),
        "no row by stamp names the record the rewind removed"
    );
    assert_eq!(count(&engine, &coll), 2, "premise: the rewind kept two of three");
    let retry = engine.insert_many_keyed(&coll, docs, &k);
    assert!(
        matches!(&retry, Err(e) if e.index == Some(0) && matches!(e.source, StorageError::Core(_))),
        "a duplicate at the first document: {retry:?}"
    );
    assert_eq!(count(&engine, &coll), 2, "the retry wrote nothing");
    assert_eq!(record(&engine, &coll, &k), None, "and stored no record");
}

/// A backup carries the records and their rows (tags 12 and 13), and a
/// restored member replays from them.
#[test]
fn a_backup_carries_the_records() {
    let (engine, coll, _dir, _path) = open();
    let k = keyed(62);
    fresh(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    counters(&engine, &coll, 2);
    let k2 = keyed(63);
    fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &k2).unwrap());
    let mut buf = Vec::new();
    engine.backup_to(&mut buf, WalkScope::Request).unwrap();
    let dest = tempfile::tempdir().unwrap();
    let path = dest.path().join("restored.redb");
    crate::backup::restore(&path, &mut buf.as_slice()).unwrap();
    let (restored, rcoll) = reopen(&path);
    assert_eq!(record(&restored, &rcoll, &k), record(&engine, &coll, &k));
    assert_eq!(record(&restored, &rcoll, &k2), record(&engine, &coll, &k2));
    assert_eq!(rows(&restored), rows(&engine));
    replayed(restored.insert_keyed(&rcoll, doc! { "v": 1 }, &k).unwrap());
    replayed(restored.modify_where_keyed(&rcoll, &Candidates::Scan, &Inc, None, &k2).unwrap());
    assert_eq!(count(&restored, &rcoll), 3);
}

/// A peer's live record over one stored here that is past its window (retention
/// has not run yet) replaces it: an absent record is overwritten by the next
/// store, never merged with. Merged, the new request's record would take the old
/// request's lower `first` and, with another body, turn contested, so a retry of
/// the new request here would be refused `422` instead of replaying.
#[test]
fn a_peer_record_over_a_stored_one_past_its_window_replaces_it() {
    let (engine, coll, _dir, _path) = open();
    engine.set_idempotency_window(3_600);
    let k = keyed(41);
    let (_, at) = fresh(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    let old = record(&engine, &coll, &k).expect("premise: the old request's record");
    // Past the old record's window; no retention pass has run.
    let later = at.hlc.wall_ms + 3_600_000 + 10_000;
    engine.set_idempotency_clock_for_test(Some(later));
    let theirs = IdempotencyRecord {
        fingerprint: reused(&k).fingerprint,
        first: stamp(later - 1_000, 0, 0x77),
        last: stamp(later - 1_000, 0, 0x77),
        origin: NodeId::from_bytes([0x77; 16]),
        state: RecordState::Done,
        outcome: RecordOutcome::Insert { stamp: stamp(later - 1_000, 0, 0x77) },
        contested: false,
    };
    let txn = engine.begin_write(crate::WriterHolder::Replication).unwrap();
    let (stored, wrote) =
        engine.store_record_noting(&txn, coll.id, &k.scope, &theirs, Some(theirs.first)).unwrap();
    txn.commit().unwrap();
    assert!(wrote, "premise: a change");
    assert!(old.first < theirs.first, "premise: the old request's first is lower");
    assert_eq!(stored, Some(theirs.clone()), "replaced, not merged with the stale record");
    assert_eq!(record(&engine, &coll, &k), Some(theirs));
}

/// A rewind that removes two records leaves no row by stamp of either, the
/// lower `first`'s included: its row removal ranges from the lowest `first` it
/// removes. Ranged from a higher one, the earlier record's rows would stay above
/// the rewound servable vector, read as F3 candidates by every frame until
/// retention collected them.
#[test]
fn a_rewind_removing_two_records_leaves_no_row_of_either() {
    let (engine, coll, _dir, _path) = open();
    engine.insert(&coll, doc! { "_id": 1i64 }).unwrap();
    let until = engine.clock_last();
    let (k1, k2) = (keyed(71), keyed(72));
    fresh(engine.insert_keyed(&coll, doc! { "_id": 2i64 }, &k1).unwrap());
    fresh(engine.insert_keyed(&coll, doc! { "_id": 3i64 }, &k2).unwrap());
    let first = |k: &KeyedWrite| record(&engine, &coll, k).unwrap().first;
    assert!(first(&k1) < first(&k2), "premise: two firsts");
    let out = engine.rewind_to(until).unwrap();
    assert_eq!(out.records_removed, 2, "premise: both discarded whole");
    for k in [&k1, &k2] {
        assert!(
            rows(&engine).into_iter().all(|(_, _, scope)| scope != k.scope.0),
            "a row by stamp still names a removed record: {:?}",
            rows(&engine)
        );
    }
}

/// The change log's rows, its beginning (the empty key) aside.
fn log_rows(engine: &Engine) -> usize {
    use redb::{ReadableDatabase, ReadableTable};
    let txn = engine.db().begin_read().unwrap();
    let log = txn.open_table(tables::IDEMPOTENCY_CHANGES).unwrap();
    log.iter().unwrap().filter(|row| !row.as_ref().unwrap().0.value().is_empty()).count()
}

/// A store whose merge changes only the contested flag is not a no-op: it
/// writes the contested record and logs the change, so a page's delta carries
/// it; the same store again is a no-op that logs nothing. Taken for a no-op,
/// this member would go on replaying the winner while every other refuses.
#[test]
fn a_store_that_only_contests_is_written_and_logged() {
    let (engine, coll, _dir, _path) = open();
    let now = crate::physical_now_ms();
    let scope = ScopeHash([3; 32]);
    let mk = |fp: u8, last_ms: u64| IdempotencyRecord {
        fingerprint: Fingerprint([fp; 32]),
        first: stamp(now - 10, 0, 9),
        last: stamp(last_ms, 0, 9),
        origin: NodeId::from_bytes([9; 16]),
        state: RecordState::Done,
        outcome: RecordOutcome::Insert { stamp: stamp(now - 10, 0, 9) },
        contested: false,
    };
    let store = |r: &IdempotencyRecord, carrying: Option<Stamp>| {
        let txn = engine.begin_write(crate::WriterHolder::Replication).unwrap();
        let (stored, wrote) =
            engine.store_record_noting(&txn, coll.id, &scope, r, carrying).unwrap();
        txn.commit().unwrap();
        (stored.unwrap(), wrote)
    };
    let winner = mk(1, now - 5);
    assert!(store(&winner, Some(winner.last)).1);
    let logged = log_rows(&engine);
    assert!(!store(&winner, Some(winner.last)).1, "premise: the same store again is a no-op");
    assert_eq!(log_rows(&engine), logged);
    // Another request's record, a lower `last` and the same `first`: the winner and
    // every row stay as they are, and only the flag moves.
    let (stored, wrote) = store(&mk(2, now - 8), None);
    assert!(wrote && stored.contested, "{stored:?}");
    assert_eq!(IdempotencyRecord { contested: false, ..stored.clone() }, winner, "the winner");
    assert_eq!(log_rows(&engine), logged + 1, "and the change is logged");
    let (again, wrote) = store(&stored, Some(stored.last));
    assert!(!wrote && again.contested, "a contested record held already is a no-op");
    assert_eq!(engine.idempotency_record(coll.id, &scope).unwrap(), Some(stored));
}

// -- For replication ------------------------------------------------------------------------

/// What a sender reads: the records whose rows lie inside the coverage a window
/// grants (F2), the ones whose rows its servable vector does not reach (F3)
/// leading, one per key; and a collection's records page by page.
#[test]
fn a_sender_finds_the_records_of_the_coverage_it_grants() {
    use redb::ReadableDatabase;
    let (engine, coll, _dir, _path) = open();
    let k = keyed(64);
    let (_, at) = fresh(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    let stored = record(&engine, &coll, &k).unwrap();
    let me = engine.node_id();
    let servable = engine.version_vector().unwrap();
    let txn = engine.db().begin_read().unwrap();
    let nothing = |_: NodeId| Hlc::ZERO;
    // The whole history granted: the record, once, at its row.
    let (all, unanchored) =
        idempotency::records_for_coverage(&txn, &nothing, &servable, &servable).unwrap();
    assert_eq!((all.len(), unanchored), (1, 0));
    assert_eq!((all[0].collection, all[0].scope, all[0].carrying), (coll.id, k.scope, Some(at)));
    assert_eq!(IdempotencyRecord::decode(&all[0].record).unwrap(), Some(stored.clone()));
    // A requester that holds the write already: nothing.
    let held = |_: NodeId| at.hlc;
    let (none, _) = idempotency::records_for_coverage(&txn, &held, &servable, &servable).unwrap();
    assert!(none.is_empty(), "{none:?}");
    // A window that grants coverage only up to before the write: nothing.
    let mut short = kimmy_core::VersionVector::new();
    short.insert(me, at.hlc.predecessor());
    let (none, _) = idempotency::records_for_coverage(&txn, &nothing, &short, &servable).unwrap();
    assert!(none.is_empty(), "{none:?}");
    // A servable vector below the record's row: shipped whatever the window
    // grants, as one this node cannot vouch for, ahead of the rest.
    let (f3, unanchored) =
        idempotency::records_for_coverage(&txn, &nothing, &short, &short).unwrap();
    assert_eq!((f3.len(), unanchored), (1, 1));

    drop(txn);
    let other = engine.create_collection("shop", "items").unwrap();
    for n in 0..5u8 {
        fresh(engine.insert_keyed(&other, doc! {}, &keyed(70 + n)).unwrap());
    }
    let txn = engine.db().begin_read().unwrap();
    let mut after = None;
    let mut pages = Vec::new();
    loop {
        let (page, more) = idempotency::records_for_collection_page(
            &txn,
            other.id,
            after.as_ref(),
            2,
            usize::MAX,
            true,
        )
        .unwrap();
        pages.push(page.len());
        match page.last() {
            Some((scope, _)) if more => after = Some(*scope),
            _ => break,
        }
    }
    assert_eq!(pages, vec![2, 2, 1], "five records, two a page, none of another collection");
}

/// A row by stamp whose record is gone (a retention pass stopped between its
/// chunks can leave one until the next pass) is passed over by every sender's
/// read, without an error: inside a window's advance (F2), above the sender's
/// servable vector (F3), and a change-log row naming it in a page's delta. The
/// records phase reads the records themselves and never meets it. Only the
/// record that stands ships.
#[test]
fn a_row_whose_record_is_gone_ships_nothing() {
    use redb::ReadableDatabase;
    let (engine, coll, _dir, _path) = open();
    let k = keyed(65);
    let (_, at) = fresh(engine.insert_keyed(&coll, doc! { "v": 1 }, &k).unwrap());
    let me = engine.node_id();
    let servable = engine.version_vector().unwrap();
    let gone = ScopeHash([66; 32]);
    let mut names_gone = coll.id.0.to_be_bytes().to_vec();
    names_gone.extend_from_slice(&gone.0);
    let base;
    {
        let txn = engine.begin_write(crate::WriterHolder::Repair).unwrap();
        // The delta's base, minted under the writer as every log key is.
        base = engine.next_stamp();
        let mut by_stamp = txn.open_table(tables::IDEMPOTENCY_BY_STAMP).unwrap();
        // Inside the advance a whole-history window grants, below the live row.
        let inside = Stamp::new(at.hlc.predecessor(), me);
        by_stamp.insert(idempotency::row_key(&inside).as_slice(), names_gone.as_slice()).unwrap();
        // Above what the servable vector reaches: an F3 candidate.
        let above = stamp(crate::physical_now_ms() + 3_600_000, 0, 9);
        by_stamp.insert(idempotency::row_key(&above).as_slice(), names_gone.as_slice()).unwrap();
        drop(by_stamp);
        // A change-log row after the delta's base, naming it.
        let logged = engine.next_stamp();
        txn.open_table(tables::IDEMPOTENCY_CHANGES)
            .unwrap()
            .insert(crate::codec::oplog_key(&logged).as_slice(), names_gone.as_slice())
            .unwrap();
        txn.commit().unwrap();
    }
    assert_eq!(engine.idempotency_record(coll.id, &gone).unwrap(), None, "premise: no record");
    assert_eq!(rows(&engine).len(), 3, "premise: the live row and the two left");

    let txn = engine.db().begin_read().unwrap();
    let nothing = |_: NodeId| Hlc::ZERO;
    let (shipped, unanchored) =
        idempotency::records_for_coverage(&txn, &nothing, &servable, &servable).unwrap();
    assert_eq!((shipped.len(), unanchored), (1, 0), "F2 and F3 pass the rows over: {shipped:?}");
    assert_eq!((shipped[0].scope, shipped[0].carrying), (k.scope, Some(at)));
    let delta = idempotency::records_changed_since(
        &txn,
        base,
        &|_| true,
        &|_, _| true,
        idempotency::DeltaBudget::PAGE,
    )
    .unwrap();
    assert!(delta.records.iter().all(|r| r.scope != gone), "{:?}", delta.records);
    assert_eq!(delta.stopped_at, None, "read to the end, without an error");
    let (page, more) =
        idempotency::records_for_collection_page(&txn, coll.id, None, 10, 1 << 20, false).unwrap();
    assert_eq!((page.len(), page[0].0, more), (1, k.scope, false), "the records phase");
}

/// What a receiver stores: merged into the live record, rows at the merged
/// `last` and the carrying stamp, its stamps witnessed; refused as history;
/// overwriting what is not alive.
#[test]
fn a_receiver_merges_and_witnesses_what_it_is_sent() {
    let (engine, coll, _dir, _path) = open();
    let scope = ScopeHash([80; 32]);
    let ahead = crate::physical_now_ms() + 60_000;
    let a = base(8, stamp(ahead, 0, 1), stamp(ahead, 0, 1), 1, RecordState::Applying);
    let b = base(8, stamp(ahead + 5, 0, 2), stamp(ahead + 9, 0, 2), 2, RecordState::Done);
    let store = |r: &IdempotencyRecord, carrying| {
        let txn = engine.begin_write(crate::WriterHolder::Replication).unwrap();
        let stored = engine.store_record(&txn, coll.id, &scope, r, carrying).unwrap();
        txn.commit().unwrap();
        stored
    };
    store(&a, None);
    let merged = store(&b, Some(stamp(ahead + 7, 0, 2))).unwrap();
    assert_eq!(merged, a.merge(&b));
    assert_eq!(engine.idempotency_record(coll.id, &scope).unwrap(), Some(merged.clone()));
    let at: Vec<Stamp> = rows(&engine).iter().map(|(s, ..)| *s).collect();
    assert_eq!(at, vec![a.last, stamp(ahead + 7, 0, 2), b.last]);
    assert!(engine.next_stamp().hlc > merged.last.hlc, "witnessed");

    // A dropped life's record is stored: at or below the drop's tombstone,
    // within its window.
    engine.drop_collection("shop", "orders").unwrap();
    let dropped = engine.collection_dropped_at(coll.id).unwrap().expect("a tombstone");
    let at = Stamp::new(Hlc::new(dropped.hlc.wall_ms - 1, 0), NodeId::from_bytes([1; 16]));
    let old = IdempotencyRecord { last: at, first: at, ..base(9, at, at, 1, RecordState::Done) };
    let other = ScopeHash([81; 32]);
    let txn = engine.begin_write(crate::WriterHolder::Replication).unwrap();
    assert_eq!(engine.store_record(&txn, coll.id, &other, &old, None).unwrap(), Some(old.clone()));
    txn.commit().unwrap();
    assert_eq!(engine.idempotency_record(coll.id, &other).unwrap(), Some(old));
}
