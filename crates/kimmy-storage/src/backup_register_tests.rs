//! The backup carries the register (ADR-272, decisions 4, 6 and 13; Q6.6, unit A). A backup is the logical form of a store: every table that
//! decides what the store means. REGISTER, REGISTER_INFO, REGISTER_STAGES and the post-drop row decide which physical a swapped name answers
//! from, what is still only information, and what a name's next physical takes, and none of them was carried: a restored member that had
//! swapped a name lost the swap (the buried identity was not in the backup and nothing named the stage), or held a stage nothing could bind.
//!
//! Each test makes a swap, takes the backup, restores it into a new store and asks what the restored member says. The premises are asserted
//! before the backup: a restore that "works" on a store with no swap proves nothing.

use std::sync::Arc;

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, DocId, Enforcement, Hlc, IndexCreate, IndexField, LossLimits,
    MergePolicy, OpKind, OplogEntry, Stamp, VersionVector,
};
use redb::{ReadableDatabase, ReadableTable};

use crate::register::{BaseOptions, Carried};
use crate::{Engine, PeerWindow, SyncOutcome, WalkScope, physical_now_ms};

const DB: &str = "app";
/// A stage that is never bound.
const UNBOUND: &str = "orders.__stg.a0";
const BOUND: &str = "orders.__stg.a1";

fn engine() -> (Arc<Engine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    (Arc::new(Engine::open(&dir.path().join("kimmy.redb")).expect("an engine opens")), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background)
        .expect("the oplog is readable")
        .entries
}

fn window(b: &Engine, entries: &[OplogEntry]) -> SyncOutcome {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = b.witnessed_vector().expect("the vector reads");
    let mut outcome = SyncOutcome::default();
    b.apply_peer_window_into(
        &theirs,
        Some(&mine),
        entries,
        PeerWindow {
            scanned_to: entries.last().map_or(Hlc::ZERO, |e| e.stamp.hlc),
            exhausted: true,
            passed_through: None,
            asked_partial: false,
        },
        &mut outcome,
    )
    .expect("the window applies");
    outcome
}

fn orders_id() -> CollectionId {
    CollectionId::derive(DB, "orders")
}

fn bind_of(by: &Engine, wall_ms: u64, physical: &str) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        kind: OpKind::BindCollection,
        collection: orders_id(),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: physical.into(),
            })
            .expect("the binding encodes"),
        ),
        meta: None,
    }
}

fn drop_of(by: &Engine, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        kind: OpKind::DropCollection,
        collection: orders_id(),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&kimmy_core::CollectionRef::new(DB, "orders"))
                .expect("the drop encodes"),
        ),
        meta: None,
    }
}

/// An index made to the logical name `orders` by `by` at `wall_ms`.
fn index_entry(by: &Engine, name: &str, path: &str, wall_ms: u64) -> OplogEntry {
    let index = kimmy_core::IndexMeta {
        id: kimmy_core::IndexMeta::derive_id(name),
        name: name.into(),
        fields: vec![IndexField::ascending(path)],
        unique: false,
        enforcement: Enforcement::Local,
        multikey: false,
        expire_after_secs: None,
        partial_filter: None,
        created: None,
    };
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        kind: OpKind::CreateIndex,
        collection: orders_id(),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&IndexCreate {
                db: DB.into(),
                collection: "orders".into(),
                index,
            })
            .expect("the creation body encodes"),
        ),
        meta: None,
    }
}

/// The author of the swap: both stages' creations and the entries, not yet delivered to anyone.
struct World {
    creations: Vec<OplogEntry>,
    bind: OplogEntry,
    bind_second: OplogEntry,
    author: Engine,
    _dir: tempfile::TempDir,
}

fn world() -> World {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let author = Engine::open(&dir.path().join("kimmy.redb")).expect("the author's engine opens");
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    author.create_stage(DB, UNBOUND, MergePolicy::Document, deadline).expect("the unbound stage");
    author.create_stage(DB, BOUND, MergePolicy::Document, deadline).expect("the bound stage");
    let creations: Vec<OplogEntry> = everything(&author)
        .into_iter()
        .filter(|e| e.kind == OpKind::CreateCollection)
        .filter(|e| {
            e.collection == CollectionId::derive(DB, UNBOUND)
                || e.collection == CollectionId::derive(DB, BOUND)
        })
        .collect();
    assert_eq!(creations.len(), 2, "premise: both stages' creations");
    // far enough above the real clock that no member built in a test is made after the author's stamps (the margin of `join_tests`)
    let wall = creations
        .iter()
        .map(|e| e.stamp.hlc.wall_ms)
        .max()
        .expect("two creations")
        .max(physical_now_ms())
        + 30_000;
    let bind = bind_of(&author, wall, BOUND);
    let bind_second = bind_of(&author, wall + 10, UNBOUND);
    World { creations, bind, bind_second, author, _dir: dir }
}

fn at(w: &World, offset: u64) -> u64 {
    w.bind.stamp.hlc.wall_ms + offset
}

/// A member holding `orders` with two documents and both stages' creations; the bound stage holds three documents of its own.
fn member(w: &World) -> (Arc<Engine>, tempfile::TempDir) {
    let (m, d) = engine();
    let orders = m.create_collection(DB, "orders").expect("the identity is created");
    m.insert(&orders, doc! { "_id": 1, "v": 1, "t": 1 }).expect("a document");
    m.insert(&orders, doc! { "_id": 2, "v": 2, "t": 2 }).expect("a document");
    m.apply_batch(&w.creations).expect("the stages' creations apply");
    let stage = m.get_collection(DB, BOUND).expect("premise: the bound stage is held");
    for id in 10..13 {
        m.insert(&stage, doc! { "_id": id, "v": id, "t": id }).expect("the job's output");
    }
    (m, d)
}

fn flip(m: &Engine, w: &World) {
    window(m, std::slice::from_ref(&w.bind));
    assert_eq!(served(m), BOUND, "premise: the flip is visible");
}

fn served(m: &Engine) -> String {
    m.resolve(DB, "orders").expect("the name resolves").physical
}

/// What the name resolves to, as a value an assertion can compare: a restored member that cannot resolve it fails ON the assertion's message
/// and not in a helper's `expect`.
fn resolves(m: &Engine) -> std::result::Result<String, String> {
    m.resolve(DB, "orders").map(|resolved| resolved.physical).map_err(|e| e.to_string())
}

/// What the register holds of `orders`: the events in effect, and the ones known ahead of their position.
fn in_effect(m: &Engine) -> usize {
    let txn = m.db().begin_read().expect("a read");
    crate::register::timeline_read(&txn, DB, "orders").expect("the register reads").events().len()
}

fn parked(m: &Engine) -> usize {
    let txn = m.db().begin_read().expect("a read");
    crate::register::parked_read(&txn, DB, "orders").expect("the register reads").events().len()
}

fn logical_drops(m: &Engine) -> usize {
    let txn = m.db().begin_read().expect("a read");
    crate::register::timeline_read(&txn, DB, "orders")
        .expect("the register reads")
        .events()
        .iter()
        .filter(|(_, event)| event.physical().is_none())
        .count()
}

fn backup_of(e: &Engine) -> Vec<u8> {
    let mut bytes = Vec::new();
    e.backup_to(&mut bytes, WalkScope::Request).expect("the backup is written");
    bytes
}

fn restored(bytes: &[u8]) -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("kimmy.redb");
    crate::backup::restore(&path, &mut &bytes[..]).expect("the backup restores");
    (Engine::open(&path).expect("the restored store opens"), dir)
}

fn has_doc(m: &Engine, physical: &str, id: i64) -> bool {
    let coll = m.get_collection(DB, physical).expect("the physical is held");
    m.get(&coll, &DocId::Int64(id)).expect("the physical reads").is_some()
}

fn holds(m: &Engine, physical: &str, index: &str) -> bool {
    m.get_collection(DB, physical).is_ok_and(|c| c.index(index).is_some())
}

// -- Tests 1 to 5: what the restored member says ------------------------------------------------------------------------------------------

#[test]
fn a_swap_whose_binding_retention_has_collected_resolves_to_the_stage_after_a_restore() {
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w);
    m.collect_oplog_entry_for_test(&w.bind.stamp);
    assert!(
        everything(&m).iter().all(|e| e.stamp != w.bind.stamp),
        "premise: the binding's entry is no longer in the oplog"
    );
    assert_eq!(served(&m), BOUND, "premise: the name still resolves to the stage");
    let (r, _dr) = restored(&backup_of(&m));
    assert_eq!(
        resolves(&r),
        Ok(BOUND.to_string()),
        "the restored member serves the stage, as the member it came from did"
    );
    assert!(has_doc(&r, BOUND, 10), "with the job's documents");
    assert!(r.get_collection(DB, "orders").is_err(), "and the identity is still buried");
}

#[test]
fn a_restore_rebuilds_the_names_row_as_the_source_member_held_it() {
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w);
    let source = crate::register::visible_row_for_tests(&m, DB, "orders").expect("the row reads");
    assert!(source.is_some(), "premise: the source member's pass wrote the name's row");
    let (r, _dr) = restored(&backup_of(&m));
    assert_eq!(
        crate::register::visible_row_for_tests(&r, DB, "orders").expect("the row reads"),
        source,
        "the restore wrote the row from what it computed, and it is the source's"
    );
    assert_eq!(r.register_resolve_fallbacks(), 0, "premise: nothing has fallen back yet");
    assert_eq!(resolves(&r), Ok(BOUND.to_string()), "the restored member answers from the row");
    assert_eq!(r.register_resolve_fallbacks(), 0, "and it did not fall back to compute it");
}

#[test]
fn a_restore_of_a_name_with_only_a_stage_row_and_no_event_writes_no_row() {
    // `check_restored` names every nameable name (the stage rows name it) and writes the row from what it computes; a name with no event
    // has nothing to cache, so the writer keeps no row for it, whichever route asks
    let w = world();
    let (m, _d) = member(&w);
    {
        let txn = m.db().begin_read().expect("a read");
        let stage = crate::register::stage_row_named_read(&txn, CollectionId::derive(DB, BOUND));
        let row = stage.expect("the row reads");
        assert!(row.is_some(), "premise: the register holds the stage's row");
    }
    assert_eq!(in_effect(&m), 0, "premise: the name has no event in effect");
    assert_eq!(parked(&m), 0, "premise: and none ahead of its position");
    let (r, _dr) = restored(&backup_of(&m));
    assert_eq!(
        crate::register::visible_row_for_tests(&r, DB, "orders").expect("the row reads"),
        None,
        "the restore wrote no row for a name with no event"
    );
    assert_eq!(resolves(&r), Ok("orders".to_string()), "and the name answers as the identity does");
    assert_eq!(r.register_resolve_fallbacks(), 0, "with no fallback: there was no row to be stale");
}

#[test]
fn a_dropped_swapped_name_reads_absent_after_a_restore_with_the_same_events_and_recreates_as_a_recreation()
 {
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w);
    window(&m, std::slice::from_ref(&drop_of(&w.author, at(&w, 5))));
    assert!(m.resolve(DB, "orders").is_err(), "premise: the name reads absent");
    assert_eq!(
        (in_effect(&m), logical_drops(&m)),
        (2, 1),
        "premise: the register holds the binding and the drop"
    );
    let (r, _dr) = restored(&backup_of(&m));
    assert!(r.resolve(DB, "orders").is_err(), "the restored name reads absent");
    assert_eq!(
        (in_effect(&r), logical_drops(&r)),
        (2, 1),
        "the restored register holds the same two events"
    );
    r.create_collection(DB, "orders").expect("the name is made again");
    assert!(
        kimmy_core::is_recreation_name(&served(&r)),
        "a create of a swapped name that was dropped is a recreation, not an ordinary collection"
    );
}

#[test]
fn a_binding_that_is_only_parked_is_still_parked_and_held_after_a_restore_and_a_window_releases_it()
{
    let w = world();
    let (m, _d) = member(&w);
    m.apply_batch(std::slice::from_ref(&w.bind)).expect("the binding is stored ahead");
    assert_eq!(
        (parked(&m), in_effect(&m)),
        (1, 0),
        "premise: the binding is information only, and the identity still answers"
    );
    let (r, _dr) = restored(&backup_of(&m));
    assert_eq!(
        (parked(&r), in_effect(&r)),
        (1, 0),
        "still parked after the restore, and not in effect"
    );
    assert_eq!(served(&r), "orders", "the identity still answers");
    window(&r, std::slice::from_ref(&w.bind));
    assert_eq!(served(&r), BOUND, "a window releases it and the flip happens");
}

#[test]
fn the_post_drop_row_survives_a_restore_and_the_next_recreate_takes_it() {
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w);
    window(&m, &[index_entry(&w.author, "late_1", "v", at(&w, 7))]);
    window(&m, std::slice::from_ref(&drop_of(&w.author, at(&w, 5))));
    assert!(m.resolve(DB, "orders").is_err(), "premise: the name is absent");
    assert!(
        crate::register::post_row_for_tests(&m, DB, "orders").expect("readable").is_some(),
        "premise: the post-drop row exists"
    );
    let (r, _dr) = restored(&backup_of(&m));
    let row = crate::register::post_row_for_tests(&r, DB, "orders").expect("readable");
    assert!(
        row.as_ref().is_some_and(|row| row.indexes.iter().any(|i| i.name == "late_1")),
        "the restored member holds the row with its definition"
    );
    r.create_collection(DB, "orders").expect("the name is made again");
    assert!(holds(&r, &served(&r), "late_1"), "the recreation takes the row's definition");
}

#[test]
fn a_drop_the_seed_put_in_the_register_is_carried_and_in_effect_after_a_restore() {
    let w = world();
    // the member applied the drop as a plain drop first (it knew of no swap), then met the swap: the seed made the tombstone a logical drop
    let (m, _d) = engine();
    m.create_collection(DB, "orders").expect("the identity is created");
    let drop = drop_of(&w.author, at(&w, 5));
    window(&m, std::slice::from_ref(&drop));
    assert_eq!(
        m.collection_dropped_at(orders_id()).expect("reads"),
        Some(drop.stamp),
        "premise: the drop is the identity's tombstone"
    );
    m.apply_batch(&w.creations).expect("the stages' creations apply");
    window(&m, std::slice::from_ref(&w.bind_second));
    assert_eq!(served(&m), UNBOUND, "premise: the flip over the drop");
    assert_eq!(
        logical_drops(&m),
        1,
        "premise: the drop is in REGISTER, put there by the seed, at the tombstone's stamp"
    );
    let (r, _dr) = restored(&backup_of(&m));
    assert_eq!(logical_drops(&r), 1, "the seeded drop is carried and in effect after the restore");
    assert_eq!(
        resolves(&r),
        Ok(UNBOUND.to_string()),
        "and the name answers from the stage that flipped over it"
    );
}

// -- Test 10: the definition floor across a backup (the manager's addition to unit A; the B4 note's carry line) ----------------------------
//
// 10a: a stage that holds a floor, restored, holds the same floor (a pin), and through the restored name a definition at or below it is history.
// 10b: a name whose post-drop row holds a floor, restored, holds the same floor in the row, and the next physical takes it, and the first
// life's index delivered after that is history there. Mutant (both): the metadata or the row re-encoded without the field.

/// A flipped member whose bound stage holds the floor `W + 3`.
fn a_member_whose_stage_holds_a_floor() -> (World, Arc<Engine>, tempfile::TempDir, Hlc) {
    let w = world();
    let (m, d) = member(&w);
    flip(&m, &w);
    let floor = Hlc::new(at(&w, 3), 0);
    let mut stage = m.get_collection(DB, BOUND).expect("the stage is held");
    stage.definition_floor = Some(floor);
    let txn = m.begin_write(crate::WriterHolder::Ddl).expect("a writer");
    Engine::put_collection_meta(&txn, &stage).expect("the metadata is written");
    txn.commit().expect("the commit lands");
    (w, m, d, floor)
}

#[test]
fn a_stages_definition_floor_is_equal_after_a_restore() {
    // PINS a carry that COLLECTIONS already makes (the field rides in the collection's metadata): green before the register is carried, and
    // keeps the floor from being lost when the carry is built. What the floor DOES after the restore is the next test, which needs the carry.
    let (_w, m, _d, floor) = a_member_whose_stage_holds_a_floor();
    let (r, _dr) = restored(&backup_of(&m));
    assert_eq!(
        r.get_collection(DB, BOUND).expect("the stage is held").definition_floor,
        Some(floor),
        "the restored stage holds the floor it held"
    );
}

#[test]
fn a_definition_at_or_below_a_restored_stages_floor_is_history_through_the_name_and_one_above_it_is_applied()
 {
    let (w, m, _d, _floor) = a_member_whose_stage_holds_a_floor();
    let (r, _dr) = restored(&backup_of(&m));
    assert_eq!(
        resolves(&r),
        Ok(BOUND.to_string()),
        "premise: the restored name resolves to the stage (the register is carried)"
    );
    window(
        &r,
        &[
            index_entry(&w.author, "at_floor", "t", at(&w, 3)),
            index_entry(&w.author, "above_floor", "v", at(&w, 6)),
        ],
    );
    assert!(
        !holds(&r, BOUND, "at_floor"),
        "a definition at the floor is history on the restored member"
    );
    assert!(
        holds(&r, BOUND, "above_floor"),
        "and one above it is applied: the floor is not a wall"
    );
}

#[test]
fn the_post_drop_rows_floor_survives_a_restore() {
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w);
    let floor = Hlc::new(at(&w, 3), 0);
    {
        let mut stage = m.get_collection(DB, BOUND).expect("the stage is held");
        stage.definition_floor = Some(floor);
        let txn = m.begin_write(crate::WriterHolder::Ddl).expect("a writer");
        Engine::put_collection_meta(&txn, &stage).expect("the metadata is written");
        txn.commit().expect("the commit lands");
    }
    window(&m, std::slice::from_ref(&drop_of(&w.author, at(&w, 5))));
    let row = crate::register::post_row_for_tests(&m, DB, "orders").expect("readable");
    assert_eq!(
        row.and_then(|row| row.definition_floor),
        Some(floor),
        "premise: the burial wrote the stage's floor into the row"
    );
    let (r, _dr) = restored(&backup_of(&m));
    let row = crate::register::post_row_for_tests(&r, DB, "orders").expect("readable");
    assert_eq!(
        row.and_then(|row| row.definition_floor),
        Some(floor),
        "the restored row carries its floor, and the next physical takes it"
    );
    r.create_collection(DB, "orders").expect("the name is made again");
    let recreation = served(&r);
    assert_eq!(
        r.get_collection(DB, &recreation).expect("the recreation is held").definition_floor,
        Some(floor),
        "the recreation took the row's floor"
    );
    window(&r, &[index_entry(&w.author, "first_life", "t", at(&w, 3))]);
    assert!(
        !holds(&r, &recreation, "first_life"),
        "the first life's index, delivered after the restore, is history on the recreation"
    );
}

// -- Test 6: the refusals --------------------------------------------------------------------------------------------------------------
//
// A backup is a header and a run of records (tag, key, value), ended by 0xFF. Each refusal edits the records of a good backup and restores
// it: the restore is refused by the check's own message and writes nothing. The tags of the four register tables are 13 to 16 (never
// reused). The refusals that need a row the source never held (above the vector, at or below it, a stray stage row) are built by copying a
// real record under a key of their own: a row's key is the logical name's id (8 bytes, big-endian) then the stamp's key bytes.

const HEADER: usize = 8 + 1 + 16 + 8;
const T_REGISTER: u8 = 13;
const T_REGISTER_INFO: u8 = 14;
const T_REGISTER_STAGES: u8 = 15;
const T_REGISTER_POST: u8 = 16;
const T_OPLOG: u8 = 6;
const T_OPLOG_VERSIONS: u8 = 10;

fn records(bytes: &[u8]) -> Vec<(u8, Vec<u8>, Vec<u8>)> {
    let mut at = HEADER;
    let mut out = Vec::new();
    while bytes[at] != 0xFF {
        let tag = bytes[at];
        at += 1;
        let klen = u32::from_be_bytes(bytes[at..at + 4].try_into().expect("4 bytes")) as usize;
        at += 4;
        let key = bytes[at..at + klen].to_vec();
        at += klen;
        let vlen = u32::from_be_bytes(bytes[at..at + 4].try_into().expect("4 bytes")) as usize;
        at += 4;
        let value = bytes[at..at + vlen].to_vec();
        at += vlen;
        out.push((tag, key, value));
    }
    out
}

fn rebuilt(bytes: &[u8], records: &[(u8, Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut out = bytes[..HEADER].to_vec();
    for (tag, key, value) in records {
        out.push(*tag);
        out.extend_from_slice(&(key.len() as u32).to_be_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(value);
    }
    out.push(0xFF);
    out
}

/// The restore of `bytes`, which must be refused: the error's text, and the proof that nothing was written.
fn refused(bytes: &[u8]) -> String {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("kimmy.redb");
    let error = crate::backup::restore(&path, &mut &bytes[..])
        .expect_err("the backup is refused")
        .to_string();
    assert!(!path.exists(), "a refused restore wrote nothing: no file at the path ({error})");
    error
}

fn a_flipped_backup() -> (Vec<u8>, World, Arc<Engine>, tempfile::TempDir) {
    let w = world();
    let (m, d) = member(&w);
    flip(&m, &w);
    (backup_of(&m), w, m, d)
}

#[test]
fn a_register_row_that_does_not_decode_refuses_the_restore() {
    let (bytes, _w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    let row = records
        .iter_mut()
        .find(|(tag, _, _)| *tag == T_REGISTER)
        .expect("premise: the backup carries a REGISTER row");
    row.2 = vec![0xFF; 3];
    let error = refused(&rebuilt(&bytes, &records));
    assert!(
        error.contains("a register row does not decode"),
        "a register row that does not decode is refused by its own check, not a later one: {error}"
    );
}

#[test]
fn a_binding_without_its_stage_row_refuses_the_restore() {
    let (bytes, _w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    assert!(
        records.iter().any(|(tag, _, _)| *tag == T_REGISTER),
        "premise: the backup carries the binding"
    );
    records.retain(|(tag, _, _)| *tag != T_REGISTER_STAGES);
    let error = refused(&rebuilt(&bytes, &records));
    assert!(
        error.contains("binding without its stage row"),
        "the refusal names the check: {error}"
    );
}

#[test]
fn a_hidden_collection_the_register_does_not_know_refuses_the_restore() {
    let (bytes, _w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    records.retain(|(tag, _, _)| {
        ![T_REGISTER, T_REGISTER_INFO, T_REGISTER_STAGES, T_REGISTER_POST].contains(tag)
    });
    let error = refused(&rebuilt(&bytes, &records));
    assert!(
        error.contains("unknown to the register"),
        "a stage alive in the backup that the register has no row for: {error}"
    );
}

#[test]
fn a_parked_row_with_no_entry_in_the_backup_refuses_the_restore() {
    let w = world();
    let (m, _d) = member(&w);
    m.apply_batch(std::slice::from_ref(&w.bind)).expect("the binding is stored ahead");
    let bytes = backup_of(&m);
    let mut records = records(&bytes);
    assert!(
        records.iter().any(|(tag, _, _)| *tag == T_REGISTER_INFO),
        "premise: the backup carries the parked row"
    );
    let entry_key = crate::codec::oplog_key(&w.bind.stamp);
    let before = records.len();
    records.retain(|(tag, key, _)| !(*tag == T_OPLOG && key.as_slice() == entry_key.as_slice()));
    assert_eq!(
        records.len(),
        before - 1,
        "premise: the binding's entry was removed from the oplog records"
    );
    let error = refused(&rebuilt(&bytes, &records));
    assert!(error.contains("has no entry"), "the refusal names the check: {error}");
}

// -- Test 7: the classification --------------------------------------------------------------------------------------------------------

#[test]
fn the_register_tables_are_carried_and_none_is_classed_as_not_carried() {
    let source = include_str!("backup.rs");
    let carried = &source[source.find("const CARRIED").expect("the carried list")
        ..source.find("const NOT_CARRIED").expect("the not-carried list")];
    let not_carried = &source[source.find("const NOT_CARRIED").expect("the not-carried list")..];
    let not_carried = &not_carried[..not_carried.find("];").expect("its end")];
    // The names table (unit N) is the fifth: a physical's id to its database and names.
    let tables =
        ["REGISTER", "REGISTER_INFO", "REGISTER_STAGES", "REGISTER_POST", "REGISTER_NAMES"];
    for table in tables {
        let quoted = format!("\"{table}\"");
        assert!(carried.contains(&quoted), "{table} is in CARRIED");
        assert!(
            !not_carried.contains(&format!("({quoted},")),
            "{table} is not named in NOT_CARRIED, whose reasons said 're-derived' or 'not yet'"
        );
    }
}

/// The `COLLECTIONS` record of `physical` in a backup, as JSON: key `(db, name)` is two length-prefixed parts.
fn collection_record<'a>(
    records: &'a mut [(u8, Vec<u8>, Vec<u8>)],
    physical: &str,
) -> &'a mut (u8, Vec<u8>, Vec<u8>) {
    records
        .iter_mut()
        .find(|(tag, key, _)| *tag == 3 && key.ends_with(physical.as_bytes()))
        .expect("premise: the backup carries the collection's record")
}

#[test]
fn a_visible_physical_holding_a_definition_at_or_below_its_own_floor_refuses_the_restore_as_damaged()
 {
    // consistency check 4 (the tester's finding, ruled): what a flip's floor leaves cannot be below the floor, so a backup that says so was
    // damaged or hand-made, and is named and refused
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w);
    window(&m, &[index_entry(&w.author, "x_1", "t", at(&w, 7))]);
    assert!(holds(&m, BOUND, "x_1"), "premise: the index is on the visible stage");
    let bytes = backup_of(&m);
    let mut records = records(&bytes);
    let stage = collection_record(&mut records, BOUND);
    let mut meta: serde_json::Value =
        serde_json::from_slice(&stage.2).expect("the metadata is JSON");
    meta["definition_floor"] =
        serde_json::to_value(Hlc::new(at(&w, 9), 0)).expect("the floor encodes");
    stage.2 = serde_json::to_vec(&meta).expect("the metadata encodes");
    let error = refused(&rebuilt(&bytes, &records));
    assert!(error.contains("at or below its own floor"), "the refusal names the check: {error}");
}

#[test]
fn a_visible_physical_holding_a_validator_changed_at_or_below_its_own_floor_refuses_the_restore_as_damaged()
 {
    // check 4 covers the validator as the flip's floor does: a validator changed at or below the floor is the dropped life's
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w);
    let stamp = Stamp::new(Hlc::new(at(&w, 7), 0), w.author.node_id(), w.author.epoch());
    let entry = crate::engine::ddl_entry(
        stamp,
        OpKind::ConfigureValidator,
        orders_id(),
        &kimmy_core::ValidatorSet {
            db: DB.into(),
            collection: "orders".into(),
            validator: Some(kimmy_core::ValidatorSpec {
                schema: doc! { "bsonType": "object" },
                action: Default::default(),
                level: Default::default(),
            }),
        },
    )
    .expect("the validator entry encodes");
    window(&m, std::slice::from_ref(&entry));
    assert!(
        m.get_collection(DB, BOUND).expect("the stage is held").validator.is_some(),
        "premise: the visible stage holds the validator"
    );
    let bytes = backup_of(&m);
    let mut records = records(&bytes);
    let stage = collection_record(&mut records, BOUND);
    let mut meta: serde_json::Value =
        serde_json::from_slice(&stage.2).expect("the metadata is JSON");
    meta["definition_floor"] =
        serde_json::to_value(Hlc::new(at(&w, 9), 0)).expect("the floor encodes");
    stage.2 = serde_json::to_vec(&meta).expect("the metadata encodes");
    let error = refused(&rebuilt(&bytes, &records));
    assert!(
        error.contains("validator changed at or below its own floor"),
        "the refusal names the check: {error}"
    );
}

#[test]
fn a_post_drop_row_left_while_a_newer_physical_is_visible_refuses_the_restore_as_damaged() {
    // the next physical that becomes visible takes the row and deletes it in the same transaction, so a row beside a visible physical is damage
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w);
    let bytes = backup_of(&m);
    let mut records = records(&bytes);
    let row = serde_json::to_vec(&crate::register::PostRow::default()).expect("the row encodes");
    records.push((T_REGISTER_POST, orders_id().0.to_be_bytes().to_vec(), row));
    let error = refused(&rebuilt(&bytes, &records));
    assert!(
        error.contains("post-drop row left while a newer physical is visible"),
        "the refusal names the check: {error}"
    );
}

/// The key of a register row of `orders` stamped `stamp`: the name's id then the stamp's key bytes.
fn row_key(stamp: &Stamp) -> Vec<u8> {
    let mut key = orders_id().0.to_be_bytes().to_vec();
    key.extend_from_slice(&stamp.to_key_bytes());
    key
}

#[test]
fn a_register_row_above_the_carried_vector_refuses_the_restore() {
    let (bytes, w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    let binding = records
        .iter()
        .find(|(tag, _, _)| *tag == T_REGISTER)
        .cloned()
        .expect("premise: the backup carries the binding's row");
    // a copy of the binding's event stamped above everything this member had covered of its origin
    let above = Stamp::new(
        Hlc::new(w.bind.stamp.hlc.wall_ms + 500, 0),
        w.author.node_id(),
        w.author.epoch(),
    );
    records.push((T_REGISTER, row_key(&above), binding.2));
    let error = refused(&rebuilt(&bytes, &records));
    assert!(error.contains("above the carried vector"), "the refusal names the check: {error}");
}

#[test]
fn a_parked_row_at_or_below_the_carried_vector_refuses_the_restore() {
    let (bytes, w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    let binding = records
        .iter()
        .find(|(tag, _, _)| *tag == T_REGISTER)
        .cloned()
        .expect("premise: the backup carries the binding's row");
    // information only, yet at a stamp the vector already covers: it would never be released
    let below =
        Stamp::new(Hlc::new(w.bind.stamp.hlc.wall_ms - 1, 0), w.author.node_id(), w.author.epoch());
    records.push((T_REGISTER_INFO, row_key(&below), binding.2));
    let error = refused(&rebuilt(&bytes, &records));
    assert!(
        error.contains("at or below the carried vector"),
        "the refusal names the check: {error}"
    );
}

#[test]
fn a_stage_row_whose_names_derive_another_key_refuses_the_restore() {
    let (bytes, _w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    let stage = records
        .iter()
        .find(|(tag, _, value)| *tag == T_REGISTER_STAGES && value.ends_with(BOUND.as_bytes()))
        .cloned()
        .expect("premise: the backup carries the bound stage's row");
    // the binding keeps its own stage row; a second one names the bound stage under an id that is not its own
    let stray = CollectionId::derive(DB, "orders.__stg.zz").0.to_be_bytes().to_vec();
    assert_ne!(stray, stage.1, "premise: the key is not the stage's own");
    records.push((T_REGISTER_STAGES, stray, stage.2));
    let error = refused(&rebuilt(&bytes, &records));
    assert!(error.contains("derive another key"), "the refusal names the check: {error}");
}

// -- Tests 8 and 9: what a restore must NOT do ------------------------------------------------------------------------------------------
//
// Both are GUARDS for the code commit, green before it (nothing yet carries the register, so nothing yet judges it): they keep the carry from
// being built with a pass or a purge deferral in it. Test 8 is red on its premise at the parent (it needs a carried stage row); the code
// commit turns it green. Test 9 is green before the code commit: it keeps the carry from adding a deferral.

#[test]
fn a_restore_runs_no_settle_pass_a_stage_a_pass_would_bury_is_still_alive() {
    let (bytes, _w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    // a hand-made backup: the unbound stage's deadline is far below the visible binding's stamp, so a settle pass would bury it as doomed
    let stage = records
        .iter_mut()
        .find(|(tag, _, value)| *tag == T_REGISTER_STAGES && value.ends_with(UNBOUND.as_bytes()))
        .expect("premise: the backup carries the unbound stage's row");
    let at = Stamp::KEY_LEN;
    let low = Hlc::new(1, 0).to_bytes();
    stage.2[at..at + low.len()].copy_from_slice(&low);
    let (r, _dr) = restored(&rebuilt(&bytes, &records));
    assert!(
        r.get_collection(DB, UNBOUND).is_ok(),
        "the restore transcribes and does not judge: the stage is still alive"
    );
    assert_eq!(served(&r), BOUND, "and the name still answers from the bound stage");
}

#[test]
fn a_restore_leaves_no_purge_deferral_whatever_the_source_held() {
    let w = world();
    let (m, _d) = member(&w);
    m.set_purge_deferral(std::time::Duration::from_secs(60));
    flip(&m, &w);
    let rows = |e: &Engine| -> usize {
        let txn = e.db().begin_read().expect("a read");
        match txn.open_table(crate::tables::PURGE_NOT_BEFORE) {
            Ok(table) => table.iter().expect("the table reads").count(),
            Err(_) => 0,
        }
    };
    assert!(rows(&m) > 0, "premise: the source holds a deferral for the identity it buried");
    let (r, _dr) = restored(&backup_of(&m));
    assert_eq!(rows(&r), 0, "a restore has no request in flight: nothing is deferred");
}

// -- The restore writes to a temporary path and renames it over the target only after every check has passed (ruled) -----------------------

#[test]
fn a_backup_cut_mid_way_leaves_no_file_at_the_target_and_no_temporary_file() {
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w);
    let bytes = backup_of(&m);
    let cut = &bytes[..bytes.len() * 2 / 3];
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("kimmy.redb");
    let error = crate::backup::restore(&path, &mut &cut[..]).expect_err("a cut backup is refused");
    assert!(!path.exists(), "nothing is at the target after a stream cut mid-way: {error}");
    assert_eq!(
        std::fs::read_dir(dir.path()).expect("the directory reads").count(),
        0,
        "and no temporary file is left beside it ({error})"
    );
}

#[test]
fn a_restore_that_succeeds_leaves_the_store_at_the_target_and_nothing_named_for_the_temporary_path()
{
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w);
    let bytes = backup_of(&m);
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("kimmy.redb");
    crate::backup::restore(&path, &mut &bytes[..]).expect("the backup restores");
    assert!(path.exists(), "the store is at the target");
    let names: Vec<String> = std::fs::read_dir(dir.path())
        .expect("the directory reads")
        .map(|e| e.expect("an entry").file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        !names.iter().any(|n| n.contains("restoring")),
        "nothing named for the temporary path is left: {names:?}"
    );
}

#[test]
fn a_target_that_appears_between_the_checks_and_the_publish_is_left_untouched_and_nothing_is_left_beside_it()
 {
    // the restore never overwrites: the store is published by a hard link, which fails if the target exists, so a concurrent restore (or
    // anything else) that made the target after the check at the top is not clobbered
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w);
    let bytes = backup_of(&m);
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("kimmy.redb");
    let target = path.clone();
    crate::backup::before_publish_for_tests(move || {
        std::fs::write(&target, b"another restore's store").expect("the target appears");
    });
    let error =
        crate::backup::restore(&path, &mut &bytes[..]).expect_err("the restore does not overwrite");
    assert_eq!(
        std::fs::read(&path).expect("the target reads"),
        b"another restore's store".to_vec(),
        "the target is untouched ({error})"
    );
    let names: Vec<String> = std::fs::read_dir(dir.path())
        .expect("the directory reads")
        .map(|e| e.expect("an entry").file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names, vec!["kimmy.redb".to_string()], "and no temporary file is left beside it");
}

// -- The refusals no other test reaches, and the stale temporary file (a guard commit: the code is unchanged) --------------------------------

#[test]
fn a_register_row_whose_key_is_the_wrong_length_refuses_the_restore() {
    let (bytes, _w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    let row = records
        .iter_mut()
        .find(|(tag, _, _)| *tag == T_REGISTER)
        .expect("premise: the backup carries a REGISTER row");
    row.1.pop();
    let error = refused(&rebuilt(&bytes, &records));
    assert!(
        error.contains("wrong length"),
        "a register row key of the wrong length is refused by its own check: {error}"
    );
}

#[test]
fn a_stage_row_of_the_wrong_shape_refuses_the_restore() {
    let (bytes, _w, _m, _d) = a_flipped_backup();
    let stage = records(&bytes)
        .into_iter()
        .find(|(tag, _, value)| *tag == T_REGISTER_STAGES && value.ends_with(BOUND.as_bytes()))
        .expect("premise: the backup carries the bound stage's row");
    let fixed = Stamp::KEY_LEN + kimmy_core::HLC_ENCODED_LEN;
    let mut overrun = stage.2.clone();
    overrun[fixed..fixed + 2].copy_from_slice(&u16::MAX.to_be_bytes());
    let cases = [
        ("a key that is not eight bytes", stage.1[..7].to_vec(), stage.2.clone()),
        ("a value that ends before its names", stage.1.clone(), stage.2[..fixed].to_vec()),
        ("a database name longer than the row", stage.1.clone(), overrun),
    ];
    for (what, key, value) in cases {
        let mut records = records(&bytes);
        records.retain(|(tag, k, _)| *tag != T_REGISTER_STAGES || *k != stage.1);
        records.push((T_REGISTER_STAGES, key, value));
        let error = refused(&rebuilt(&bytes, &records));
        assert!(
            error.contains("a stage row does not decode"),
            "a stage row of the wrong shape is refused by its own check ({what}): {error}"
        );
    }
}

#[test]
fn a_binding_whose_stage_row_names_another_logical_id_refuses_the_restore() {
    let (bytes, _w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    let row = records
        .iter_mut()
        .find(|(tag, _, _)| *tag == T_REGISTER)
        .expect("premise: the backup carries the binding's row");
    // the binding's row is filed under another name's id: its stage's names derive `orders`, not that
    let elsewhere = CollectionId::derive(DB, "elsewhere").0.to_be_bytes();
    row.1[..8].copy_from_slice(&elsewhere);
    let error = refused(&rebuilt(&bytes, &records));
    assert!(
        error.contains("binding without its stage row"),
        "a binding whose stage row names another logical id is refused as having none: {error}"
    );
}

#[test]
fn a_hidden_recreation_the_register_has_no_row_for_refuses_the_restore() {
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w);
    window(&m, std::slice::from_ref(&drop_of(&w.author, at(&w, 5))));
    m.create_collection(DB, "orders").expect("the name is made again");
    let recreation = served(&m);
    assert!(kimmy_core::is_recreation_name(&recreation), "premise: a recreation holds the name");
    let bytes = backup_of(&m);
    let mut records = records(&bytes);
    records.retain(|(tag, _, _)| *tag != T_REGISTER);
    let error = refused(&rebuilt(&bytes, &records));
    assert!(
        error.contains(&recreation),
        "a hidden recreation the register has no row for is refused, by name: {error}"
    );
}

/// The records of a backup: tag, key, value.
type Records = Vec<(u8, Vec<u8>, Vec<u8>)>;

/// A backup of a member that dropped a swapped name and made it again: the bytes, the records, the
/// recreation's name, and the stamp of the `Create` event that made it (read from its `REGISTER`
/// row, which the caller removes).
fn a_backup_with_a_recreation() -> (Vec<u8>, Records, String, Stamp) {
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w);
    window(&m, std::slice::from_ref(&drop_of(&w.author, at(&w, 5))));
    m.create_collection(DB, "orders").expect("the name is made again");
    let recreation = served(&m);
    assert!(kimmy_core::is_recreation_name(&recreation), "premise: a recreation holds the name");
    let bytes = backup_of(&m);
    let records = records(&bytes);
    let created = records
        .iter()
        .filter(|(tag, _, _)| *tag == T_REGISTER)
        .find(|(_, _, value)| {
            matches!(
                kimmy_core::RegisterEvent::decode(value),
                Ok(kimmy_core::RegisterEvent::Create { physical }) if physical == recreation
            )
        })
        .map(|(_, key, _)| Stamp::from_key_bytes(key[8..].try_into().expect("a stamp's key")))
        .expect("premise: the backup carries the recreation's Create event");
    (bytes, records, recreation, created)
}

/// The versions the backup carries, with the one of `origin` set to `hlc`.
fn with_the_version_of(
    records: &mut [(u8, Vec<u8>, Vec<u8>)],
    origin: kimmy_core::Origin,
    hlc: Hlc,
) {
    let mut set = 0;
    for (_, key, value) in records.iter_mut().filter(|(tag, _, _)| *tag == T_OPLOG_VERSIONS) {
        if key.as_slice() == origin.to_bytes().as_slice() {
            *value = hlc.to_bytes().to_vec();
            set += 1;
        }
    }
    assert_eq!(set, 1, "premise: the backup carries one version for the recreation's origin");
}

#[test]
fn a_hidden_recreation_with_no_create_event_restores_when_the_carried_vector_does_not_cover_its_stamp()
 {
    // A backup taken mid-way, or a join: the recreation was made after the vector, so the register
    // does not hold its event yet and the event will arrive by a window. The check that every
    // hidden collection is known to the register must not refuse it.
    let (bytes, mut records, recreation, created) = a_backup_with_a_recreation();
    records.retain(|(tag, _, _)| *tag != T_REGISTER);
    with_the_version_of(&mut records, created.origin(), created.hlc.predecessor());
    let (r, _dr) = restored(&rebuilt(&bytes, &records));
    assert!(
        r.get_collection(DB, &recreation).is_ok(),
        "the recreation is alive in the restored store, as the backup held it"
    );
}

#[test]
fn a_hidden_recreation_with_no_create_event_is_refused_when_the_vector_covers_its_stamp_exactly() {
    // the boundary: a vector AT the stamp covers it, so the register must hold the event
    let (bytes, mut records, recreation, created) = a_backup_with_a_recreation();
    records.retain(|(tag, _, _)| *tag != T_REGISTER);
    with_the_version_of(&mut records, created.origin(), created.hlc);
    let error = refused(&rebuilt(&bytes, &records));
    assert!(
        error.contains("unknown to the register") && error.contains(&recreation),
        "a recreation the vector covers has its event in the register, or it is damaged: {error}"
    );
}

#[test]
fn a_hidden_recreation_whose_name_carries_no_stamp_is_refused_whatever_the_vector_says() {
    let (bytes, mut records, recreation, created) = a_backup_with_a_recreation();
    records.retain(|(tag, _, _)| *tag != T_REGISTER);
    with_the_version_of(&mut records, created.origin(), created.hlc.predecessor());
    // the record is filed under a name with the mark and a stamp that is not one: too short
    let renamed = "orders.__rc.0a1b";
    let record = collection_record(&mut records, &recreation);
    record.1.clear();
    for part in [DB, renamed] {
        record.1.extend_from_slice(&(part.len() as u32).to_be_bytes());
        record.1.extend_from_slice(part.as_bytes());
    }
    let error = refused(&rebuilt(&bytes, &records));
    assert!(
        error.contains("unknown to the register") && error.contains(renamed),
        "a name whose stamp does not parse is unknown, below the vector or not: {error}"
    );
}

#[test]
fn a_stale_temporary_file_from_an_earlier_crash_does_not_stop_a_restore() {
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w);
    let bytes = backup_of(&m);
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("kimmy.redb");
    let stale = dir.path().join("kimmy.redb.restoring");
    std::fs::write(&stale, b"half of an earlier restore").expect("the stale file is made");
    crate::backup::restore(&path, &mut &bytes[..])
        .expect("a stale temporary file from an earlier crash does not stop a restore");
    assert!(!stale.exists(), "the stale temporary file is gone after the restore");
    let r = Engine::open(&path).expect("the restored store opens");
    assert!(
        resolves(&r) == Ok(BOUND.to_string()),
        "the restored store answers as the member it came from did"
    );
}

#[test]
fn the_visible_physical_is_the_newest_binding_whose_collection_is_alive_in_the_backup() {
    // check 4 judges the physical the name resolves to, which is the newest binding whose collection is ALIVE: here the newest binding names
    // a stage whose collection is not in the backup, so the older one is visible and its floor is judged
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w);
    window(&m, &[index_entry(&w.author, "x_1", "t", at(&w, 7))]);
    assert!(holds(&m, BOUND, "x_1"), "premise: the index is on the visible stage");
    let bytes = backup_of(&m);
    let mut records = records(&bytes);
    let newer = Stamp::new(Hlc::new(at(&w, 20), 0), w.author.node_id(), w.author.epoch());
    records.retain(|(tag, key, _)| !(*tag == 3 && key.ends_with(UNBOUND.as_bytes())));
    let event = kimmy_core::RegisterEvent::Bind { physical: UNBOUND.into() };
    records.push((T_REGISTER, row_key(&newer), event.encode()));
    for (_, _, value) in records.iter_mut().filter(|(tag, _, _)| *tag == T_OPLOG_VERSIONS) {
        let covered = Hlc::from_bytes(value.as_slice().try_into().expect("a stamp"));
        *value = covered.max(newer.hlc).to_bytes().to_vec();
    }
    // the control: a dead newest binding is accepted, and the older stage answers
    let (r, _dr) = restored(&rebuilt(&bytes, &records));
    assert!(
        resolves(&r) == Ok(BOUND.to_string()),
        "premise: the older stage answers while the newest binding's collection is not alive"
    );
    let stage = collection_record(&mut records, BOUND);
    let mut meta: serde_json::Value =
        serde_json::from_slice(&stage.2).expect("the metadata is JSON");
    meta["definition_floor"] =
        serde_json::to_value(Hlc::new(at(&w, 9), 0)).expect("the floor encodes");
    stage.2 = serde_json::to_vec(&meta).expect("the metadata encodes");
    let error = refused(&rebuilt(&bytes, &records));
    assert!(
        error.contains("at or below its own floor"),
        "the older, alive stage is the visible physical, and its floor is judged: {error}"
    );
}

// -- Unit N: the names table and the base options in a backup -----------------------------------------------------------------------------
//
// The names table and the base options are carried by a backup (tags 17 and 18), copied as stored, and the restore refuses a row whose key
// does not derive from its names or that does not decode. The refusals are modelled on
// `a_stage_row_whose_names_derive_another_key_refuses_the_restore`: a record is added under a key that is not its own, and the restore
// is refused by the check's own message with nothing written.

const T_REGISTER_NAMES: u8 = 17;
const T_COLLECTION_BASE_OPTIONS: u8 = 18;

fn rows_of(e: &Engine, which: Carried) -> Vec<(Vec<u8>, Vec<u8>)> {
    let txn = e.db().begin_read().expect("a read");
    crate::register::carried_rows(&txn, which).expect("the table reads")
}

/// The source member's two tables are in the backup and a restore keeps them as they were: row for row, key and value.
#[test]
fn a_backup_carries_the_names_and_the_options_and_a_restore_keeps_them() {
    let (bytes, _w, m, _d) = a_flipped_backup();
    let carried = records(&bytes);
    assert!(
        carried.iter().any(|(tag, _, _)| *tag == T_REGISTER_NAMES),
        "premise: the backup carries the names table"
    );
    assert!(
        carried.iter().any(|(tag, _, _)| *tag == T_COLLECTION_BASE_OPTIONS),
        "premise: and the base options"
    );
    let (r, _dr) = restored(&bytes);
    assert_eq!(rows_of(&r, Carried::Names), rows_of(&m, Carried::Names), "the names, row for row");
    assert_eq!(
        rows_of(&r, Carried::Options),
        rows_of(&m, Carried::Options),
        "the options, row for row"
    );
}

/// A recreation's physical has no stage row, and a collection that is gone leaves nothing else that names its id: the names table is what
/// does, and the backup carries it.
#[test]
fn a_backup_names_a_recreation_that_has_no_stage_row_of_its_own() {
    let (_bytes, records, recreation, _created) = a_backup_with_a_recreation();
    let key = CollectionId::derive(DB, &recreation).0.to_be_bytes().to_vec();
    assert!(
        records.iter().any(|(tag, k, v)| {
            *tag == T_REGISTER_NAMES && *k == key && v.ends_with(recreation.as_bytes())
        }),
        "the recreation {recreation} is named in the backup, by its own id"
    );
}

#[test]
fn a_names_row_whose_key_does_not_derive_from_its_names_refuses_the_restore() {
    let (bytes, _w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    let names = records
        .iter()
        .find(|(tag, _, _)| *tag == T_REGISTER_NAMES)
        .cloned()
        .expect("premise: the backup carries a names row");
    let stray = CollectionId::derive(DB, "orders.__stg.zz").0.to_be_bytes().to_vec();
    assert_ne!(stray, names.1, "premise: the key is not the row's own");
    records.push((T_REGISTER_NAMES, stray, names.2));
    let error = refused(&rebuilt(&bytes, &records));
    assert!(error.contains("derive another key"), "the refusal names the check: {error}");
}

#[test]
fn a_names_row_of_the_wrong_shape_refuses_the_restore() {
    let (bytes, _w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    let key = CollectionId::derive(DB, "orders.__stg.zz").0.to_be_bytes().to_vec();
    records.push((T_REGISTER_NAMES, key, vec![0]));
    let error = refused(&rebuilt(&bytes, &records));
    assert!(error.contains("wrong shape"), "the refusal names the check: {error}");
}

#[test]
fn a_names_row_wrong_only_in_its_logical_name_refuses_the_restore() {
    let (bytes, _w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    let physical = "orders.__stg.zz";
    let key = CollectionId::derive(DB, physical).0.to_be_bytes().to_vec();
    // db length || db || logical length || logical || physical, with the logical name of another name and the key as the names derive it
    let mut value = Vec::new();
    value.extend_from_slice(&(DB.len() as u16).to_be_bytes());
    value.extend_from_slice(DB.as_bytes());
    value.extend_from_slice(&("another".len() as u16).to_be_bytes());
    value.extend_from_slice(b"another");
    value.extend_from_slice(physical.as_bytes());
    records.push((T_REGISTER_NAMES, key, value));
    let error = refused(&rebuilt(&bytes, &records));
    assert!(error.contains("logical name is not its physical's"), "the check is named: {error}");
}

#[test]
fn a_base_options_row_whose_key_does_not_derive_from_its_names_refuses_the_restore() {
    let (bytes, _w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    let options = records
        .iter()
        .find(|(tag, _, _)| *tag == T_COLLECTION_BASE_OPTIONS)
        .cloned()
        .expect("premise: the backup carries an options row");
    let stray = CollectionId::derive(DB, "orders.__stg.zz").0.to_be_bytes().to_vec();
    assert_ne!(stray, options.1, "premise: the key is not the row's own");
    records.push((T_COLLECTION_BASE_OPTIONS, stray, options.2));
    let error = refused(&rebuilt(&bytes, &records));
    assert!(error.contains("derive another key"), "the refusal names the check: {error}");
}

/// The backup of a flipped world, with one more options row: the shadow's, as `forged`, under the key its names derive (so the key check passes). No collection of that name
/// stands in the backup, which is allowed (the identity the flip buried keeps its row with no collection to need it).
fn refused_with_the_shadows_row(forged: BaseOptions) -> String {
    let (bytes, _w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    let key = CollectionId::derive(DB, "orders.__vectors").0.to_be_bytes().to_vec();
    assert!(
        !records.iter().any(|(tag, k, _)| *tag == T_COLLECTION_BASE_OPTIONS && *k == key),
        "premise: the backup holds no row for the shadow"
    );
    assert_eq!(forged.db, DB, "premise: the row's names derive the key");
    records.push((T_COLLECTION_BASE_OPTIONS, key, serde_json::to_vec(&forged).unwrap()));
    refused(&rebuilt(&bytes, &records))
}

fn named_by_the_new_check(error: &str) {
    assert!(error.contains("base-options row"), "the refusal is about the row: {error}");
    assert!(!error.contains("derive another key"), "and is not the key check: {error}");
    assert!(!error.contains("does not decode"), "and is not the decode check: {error}");
}

/// (a) The shadow's row says `field`, with an `R` and a `K`. DESIGNED RED today (the restore is accepted, so `refused` fails its `expect_err`). Mutant PF4-M2: the normal-form
/// check removed at the restore check.
#[test]
fn pf4a_designed_red_a_backup_with_a_shadow_row_that_says_field_is_refused() {
    named_by_the_new_check(&refused_with_the_shadows_row(BaseOptions {
        db: DB.to_string(),
        name: "orders.__vectors".to_string(),
        at: Hlc::ZERO,
        merge_policy: MergePolicy::Field,
        join_retention_ms: Some(86_400_000),
        loss_limits: Some(LossLimits::DEFAULT),
        stage: None,
    }));
}

/// (b) The shadow's row has an `at` above `Hlc::ZERO`. DESIGNED RED today. Mutant PF4-M2.
#[test]
fn pf4b_designed_red_a_backup_with_a_shadow_row_with_an_at_above_zero_is_refused() {
    named_by_the_new_check(&refused_with_the_shadows_row(BaseOptions {
        db: DB.to_string(),
        name: "orders.__vectors".to_string(),
        at: Hlc::new(5, 0),
        merge_policy: MergePolicy::Document,
        join_retention_ms: None,
        loss_limits: None,
        stage: None,
    }));
}

#[test]
fn a_base_options_row_that_does_not_decode_refuses_the_restore() {
    let (bytes, _w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    let key = CollectionId::derive(DB, "orders.__stg.zz").0.to_be_bytes().to_vec();
    records.push((T_COLLECTION_BASE_OPTIONS, key, b"not json".to_vec()));
    let error = refused(&rebuilt(&bytes, &records));
    assert!(error.contains("does not decode"), "the refusal names the check: {error}");
}

// -- The restore check's two clauses (ADR-272, unit N): every register event id and stage row is NAMED, and every collection has an options row ----

/// An event of a name that no names row holds is a backup that would restore a register nothing can read by name: refused, whole.
#[test]
fn a_backup_with_an_event_of_a_name_no_names_row_holds_is_refused() {
    let (bytes, _w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    let orders = CollectionId::derive(DB, "orders").0.to_be_bytes().to_vec();
    let before = records.len();
    records.retain(|(tag, key, _)| !(*tag == T_REGISTER_NAMES && *key == orders));
    assert!(records.len() < before, "premise: the backup carries the identity's names row");
    let error = refused(&rebuilt(&bytes, &records));
    assert!(error.contains("no names row"), "the refusal names the check: {error}");
}

/// A stage row whose physical no names row holds is refused the same way.
#[test]
fn a_backup_with_a_stage_row_no_names_row_holds_is_refused() {
    let (bytes, _w, _m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    let stage = CollectionId::derive(DB, BOUND).0.to_be_bytes().to_vec();
    let before = records.len();
    records.retain(|(tag, key, _)| !(*tag == T_REGISTER_NAMES && *key == stage));
    assert!(records.len() < before, "premise: the backup carries the stage's names row");
    let error = refused(&rebuilt(&bytes, &records));
    assert!(error.contains("no names row"), "the refusal names the check: {error}");
}

/// A collection with no options row: the invariant of every creation path (a creation writes its row, whatever its route). Refused, and the
/// restore invents no row to satisfy it.
#[test]
fn a_backup_with_a_collection_that_has_no_options_row_is_refused() {
    let (bytes, _w, m, _d) = a_flipped_backup();
    let mut records = records(&bytes);
    // the row of a collection that STANDS in the backup (the identity the flip buried keeps its row, with no collection to need it)
    let standing = m.get_collection(DB, BOUND).expect("premise: the bound stage stands");
    let key = standing.id.0.to_be_bytes().to_vec();
    let before = records.len();
    records.retain(|(tag, k, _)| !(*tag == T_COLLECTION_BASE_OPTIONS && *k == key));
    assert_eq!(records.len(), before - 1, "premise: the backup carries the stage's options row");
    let error = refused(&rebuilt(&bytes, &records));
    assert!(error.contains("no options row"), "the refusal names the check: {error}");
}

/// w1g (the 6:12 AM ruling, "an options row with no collection is valid", one-way ON PURPOSE): a backup with a GONE collection (its tombstone, no collection) and NO options row for it is ACCEPTED. `check_restored` clause 2 refuses a STANDING collection with no row
/// (the test beside it) and says nothing of a gone one: P12's collector may have taken the row of a collection whose tombstones are collected, and the member's own backup must restore. EXPECTED: green at 30f3fc04 (read: clause 2 walks the collections that stand);
/// it changes at NO P commit and must stay green through P4 (whose normal-form check on the carried and restored ROWS must not add a "row for every tombstone" demand) and P12. Mutant (P4): clause 2 also demands a row for every tombstoned id.
/// If it is RED at the first run, that is a finding for P (the ruling is not what the code does), not a test bug.
#[test]
fn w1g_a_backup_with_a_gone_collection_and_no_options_row_is_accepted() {
    let (m, _d) = engine();
    let orders = m.create_collection(DB, "orders").unwrap();
    m.insert(&orders, bson::doc! { "_id": 1, "v": "old" }).unwrap();
    assert!(m.drop_collection(DB, "orders").unwrap());
    let bytes = backup_of(&m);
    let mut records = records(&bytes);
    let key = orders_id().0.to_be_bytes().to_vec();
    let before = records.len();
    records.retain(|(tag, k, _)| !(*tag == T_COLLECTION_BASE_OPTIONS && *k == key));
    assert_eq!(
        records.len(),
        before - 1,
        "premise: the backup carried the gone collection's row, and it is taken out"
    );
    let (restored_member, _dir) = restored(&rebuilt(&bytes, &records));
    assert!(
        restored_member.get_collection(DB, "orders").is_err(),
        "the collection is gone after the restore"
    );
    assert!(
        restored_member.collection_dropped_at(orders_id()).unwrap().is_some(),
        "and its tombstone came with the backup"
    );
}

/// The names ride the FIRST pages of a join and the register's events the LAST. A name whose first event the sender stores AFTER the names
/// walk has passed its key, and before the last page, reaches the joiner without a names row, and with no stage row or collection to name it
/// by: so the last page carries the names rows of the ids its register unit holds. The joiner holds the row, and a backup of it restores.
#[test]
fn a_name_the_sender_stores_after_the_names_walk_is_named_by_the_last_page() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (s, _ds) = engine();
    for i in 0..12 {
        s.create_collection(DB, &format!("c{i}")).unwrap();
    }
    // a room that splits the carry over several pages and still holds the register's carry
    s.set_snapshot_room_for_test(Some(1_200));
    let stored = Arc::new(AtomicBool::new(false));
    let stamp = Stamp::new(Hlc::new(1_000, 0), s.node_id(), s.epoch());
    let once = Arc::clone(&stored);
    s.set_snapshot_reader_hook_for_test(Some(Arc::new(move |e, after| {
        if after.is_some() && !once.swap(true, Ordering::SeqCst) {
            let txn = e.begin_write(crate::WriterHolder::Ddl).unwrap();
            crate::register::put(&txn, DB, "late", stamp, kimmy_core::RegisterEvent::LogicalDrop)
                .unwrap();
            txn.commit().unwrap();
        }
    })));
    let (j, _dj) = engine();
    let mut progress = crate::SnapshotProgress::whole_database();
    let mut pages = 0;
    while !progress.is_complete() {
        assert!(pages < 100, "the pull does not end");
        let page = s.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        j.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
        pages += 1;
    }
    assert!(stored.load(Ordering::SeqCst) && pages >= 3, "premise: stored between pages {pages}");
    let key = CollectionId::derive(DB, "late").0.to_be_bytes().to_vec();
    let named = {
        let txn = j.db().begin_read().unwrap();
        crate::register::carried_rows(&txn, crate::register::Carried::Names).unwrap()
    };
    assert!(named.iter().any(|(k, _)| *k == key), "the joiner holds the name's row");
    let (_restored, _dr) = restored(&backup_of(&j));
}

/// The twin of the race above with a recreation's `Create` event: the sender stores a recreation of "late" after the names walk, and `store`
/// writes TWO names rows for it (the identity's and the recreation's physical's), which the joiner can never get later (a row is written when
/// an event is newly stored, and the carry stores it). The last page carries both, in the transaction the event is read in.
#[test]
fn a_recreation_stored_after_the_names_walk_is_named_by_the_last_page_with_its_physical() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (s, _ds) = engine();
    for i in 0..12 {
        s.create_collection(DB, &format!("c{i}")).unwrap();
    }
    s.set_snapshot_room_for_test(Some(1_400));
    let stored = Arc::new(AtomicBool::new(false));
    let stamp = Stamp::new(Hlc::new(1_000, 0), s.node_id(), s.epoch());
    let physical = kimmy_core::recreation_name("late", &stamp);
    let (once, event) = (Arc::clone(&stored), physical.clone());
    s.set_snapshot_reader_hook_for_test(Some(Arc::new(move |e, after| {
        if after.is_some() && !once.swap(true, Ordering::SeqCst) {
            let txn = e.begin_write(crate::WriterHolder::Ddl).unwrap();
            let create = kimmy_core::RegisterEvent::Create { physical: event.clone() };
            crate::register::put(&txn, DB, "late", stamp, create).unwrap();
            txn.commit().unwrap();
        }
    })));
    let (j, _dj) = engine();
    let mut progress = crate::SnapshotProgress::whole_database();
    let mut pages = 0;
    while !progress.is_complete() {
        assert!(pages < 100, "the pull does not end");
        let page = s.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        j.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
        pages += 1;
    }
    assert!(stored.load(Ordering::SeqCst) && pages >= 3, "premise: stored between pages {pages}");
    let named = {
        let txn = j.db().begin_read().unwrap();
        crate::register::carried_rows(&txn, Carried::Names).unwrap()
    };
    let held = |name: &str| {
        let key = CollectionId::derive(DB, name).0.to_be_bytes().to_vec();
        named.iter().any(|(k, _)| *k == key)
    };
    assert!(held("late"), "the joiner holds the identity's names row");
    assert!(held(&physical), "and the recreation's physical's");
}

/// The gauge of the register's carry (`kimmy_snapshot_register_carry_bytes`) and the warning at half the cap measure what the unit carries:
/// the register AND the names rows of its events' ids that ride the same page.
#[test]
fn the_register_carry_gauge_counts_the_names_that_ride_with_it() {
    let (_bytes, _w, m, _d) = a_flipped_backup();
    let page = m.snapshot_page(None, None).unwrap();
    let register = page.register.as_ref().expect("premise: the one page carries the register");
    let ids: Vec<Vec<u8>> = register.events.iter().map(|(key, _)| key[..8].to_vec()).collect();
    assert!(!ids.is_empty(), "premise: the register holds events");
    let names: usize = {
        let txn = m.db().begin_read().unwrap();
        let held = crate::register::carried_rows(&txn, Carried::Names).unwrap();
        let mut seen = std::collections::BTreeSet::new();
        held.iter()
            .filter(|(key, _)| ids.contains(key) && seen.insert(key.clone()))
            .map(|(key, value)| key.len() + value.len() + crate::snapshot::CARRIED_ROW_FRAME)
            .sum()
    };
    assert!(names > 0, "premise: the events' names are held");
    assert!(
        m.snapshot_register_carry_bytes() as usize >= register.encoded_len() + names,
        "the gauge reads {} for a register of {} and names of {names}",
        m.snapshot_register_carry_bytes(),
        register.encoded_len()
    );
}
