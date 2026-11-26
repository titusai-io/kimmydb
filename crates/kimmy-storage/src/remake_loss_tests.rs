//! Unit P, commit P7c: a Q7 loss record for a collection that is GONE here is no fact that makes it again (ruled 1:07 PM). The entries
//! route judges it by the name's row: above the floor of a `field` name it is appended and PARKED by collection id and document key with
//! the row's `R`, no collection made (pf1 to pf3 of the page-and-order file pin that, and the order with a write); a `document` name's
//! record is ignored; with no row the window stops and fetches; with the drop's rows still to be purged the entry is left untaken and
//! parks once the purge is done.

use bson::doc;
use kimmy_core::merge_state::{
    LossKey, LossKind, LossValue, MStamp, MergeState, Path, Value, WriterId,
};
use kimmy_core::{CollectionId, DocId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp, VersionVector};
use redb::ReadableDatabase;

use crate::{Engine, PeerWindow, SyncOutcome, WalkScope, physical_now_ms};

const DB: &str = "app";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn window(b: &Engine, entries: &[OplogEntry]) -> SyncOutcome {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = b.witnessed_vector().unwrap();
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
    .unwrap();
    outcome
}

fn pause() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn id() -> CollectionId {
    CollectionId::derive(DB, "orders")
}

/// A relayed loss record for document 1 of `orders`, by `from`, stamped `wall + 1` (the loser's stamp is `wall`).
fn loss_entry(from: &Engine, wall: u64) -> OplogEntry {
    let key = LossKey::field(
        LossKind::Clash,
        Path::flat("x").unwrap(),
        MStamp {
            hlc: Hlc::new(wall, 0),
            writer: WriterId { node: from.node_id(), epoch: from.epoch() },
        },
    );
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall + 1, 0), from.node_id(), from.epoch()),
        kind: OpKind::LossRecord,
        collection: id(),
        doc_id: Some(DocId::Int64(1)),
        body: Some(MergeState::encode_loss_record(&key, &LossValue::Value(Value::Int64(7)))),
        meta: None,
    }
}

/// How many records are parked for document 1 of `orders`.
fn parked(e: &Engine) -> usize {
    let key = crate::docs::doc_key(&DocId::Int64(1)).unwrap();
    let txn = e.db().begin_read().unwrap();
    let table = txn.open_table(crate::tables::LOSS_PENDING).unwrap();
    table
        .range((id().0, key.as_slice(), &[][..])..=(id().0, key.as_slice(), &[0xff; 64][..]))
        .unwrap()
        .count()
}

fn is_held(e: &Engine, stamp: Stamp) -> bool {
    everything(e).iter().any(|entry| entry.stamp == stamp)
}

/// `x` made `orders` (under `policy`) with document 1; `z` took it and dropped it (D); the returned member `m` took the collection, then D;
/// `purged` says whether its rows are purged. Returns (m, D, the loss record stamped above D).
fn the_world(
    policy: MergePolicy,
    purged: bool,
) -> (Engine, tempfile::TempDir, OplogEntry, OplogEntry, [tempfile::TempDir; 2]) {
    let (x, dx) = engine();
    let orders = x.create_collection_with_policy(DB, "orders", policy, None).unwrap();
    x.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    let (z, dz) = engine();
    window(&z, &everything(&x));
    let (m, dm) = engine();
    window(&m, &everything(&x));
    pause();
    assert!(z.drop_collection(DB, "orders").unwrap());
    let d = everything(&z)
        .into_iter()
        .find(|e| e.kind == OpKind::DropCollection && e.collection == id())
        .expect("the drop");
    window(&m, std::slice::from_ref(&d));
    if purged {
        m.finish_purges_now().unwrap();
    }
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the collection is gone");
    let loss = loss_entry(&x, d.stamp.hlc.wall_ms + 1_000);
    assert!(d.stamp.hlc < loss.stamp.hlc, "premise: the record is stamped above the drop");
    (m, dm, d, loss, [dx, dz])
}

/// A name whose row says `document`: the record is ignored, as `relay_loss_record` ignores one of a collection that is not `field`. Nothing
/// is parked or appended, no collection is made and the window is not stopped. Mutant: it is parked whatever the row says.
#[test]
fn a_loss_record_above_the_floor_of_a_document_name_is_ignored() {
    let (m, _dm, _d, loss, _dirs) = the_world(MergePolicy::Document, true);
    let outcome = window(&m, std::slice::from_ref(&loss));
    assert_eq!((outcome.unknown_collection, outcome.purge_pending), (0, 0), "{outcome:?}");
    assert!(m.get_collection(DB, "orders").is_err(), "no collection is made");
    assert_eq!(parked(&m), 0, "nothing is parked");
    assert!(!is_held(&m, loss.stamp), "and it is not appended");
}

/// A member that holds the drop and no row of the name cannot say the name was never swapped, nor what its options are: a record above the
/// floor stops the window, and the scoped page of the fetch brings the row. Mutant: it is judged history (the old rule).
#[test]
fn a_loss_record_above_a_tombstone_for_a_member_with_no_row_stops_the_window() {
    let (x, _dx) = engine();
    let orders = x.create_collection_with_policy(DB, "orders", MergePolicy::Field, None).unwrap();
    x.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    let (z, _dz) = engine();
    window(&z, &everything(&x));
    pause();
    assert!(z.drop_collection(DB, "orders").unwrap());
    let d = everything(&z)
        .into_iter()
        .find(|e| e.kind == OpKind::DropCollection && e.collection == id())
        .expect("the drop");
    let (m, _dm) = engine();
    window(&m, std::slice::from_ref(&d));
    assert_eq!(m.collection_dropped_at(id()).unwrap(), Some(d.stamp), "premise: the tombstone");
    let loss = loss_entry(&x, d.stamp.hlc.wall_ms + 1_000);
    let outcome = window(&m, std::slice::from_ref(&loss));
    assert_eq!(outcome.unknown_collection, 1, "the window stops and asks: {outcome:?}");
    assert_eq!(parked(&m), 0, "nothing is parked");
    assert!(m.get_collection(DB, "orders").is_err(), "and no collection is made");
}

/// The drop's rows are not purged yet: the record is left UNTAKEN (`purge_pending`, not appended, not parked) and is taken, appended and parked
/// once the purge is done. Mutant: it is parked over the rows the purge will remove.
#[test]
fn a_loss_record_above_the_floor_that_meets_a_pending_purge_is_left_untaken_and_parks_after_it() {
    let (m, _dm, _d, loss, _dirs) = the_world(MergePolicy::Field, false);
    assert!(m.rows_under(id()).unwrap() > 0, "premise: the drop's rows are not purged");
    let first = window(&m, std::slice::from_ref(&loss));
    assert!(first.purge_pending > 0, "untaken, waiting for the purge: {first:?}");
    assert!(!is_held(&m, loss.stamp), "not appended");
    assert_eq!(parked(&m), 0, "not parked");
    m.finish_purges_now().unwrap();
    let again = window(&m, std::slice::from_ref(&loss));
    assert_eq!(again.purge_pending, 0, "{again:?}");
    assert!(is_held(&m, loss.stamp), "appended after the purge");
    assert_eq!(parked(&m), 1, "and parked");
    assert!(m.get_collection(DB, "orders").is_err(), "still no collection");
}

/// A name this member swapped keeps the OLD rule: its tombstone is the register's burial mark, and a record above it is a swap's late record, not
/// a fact of a plain collection. The row is held and says `field`, and the identity's rows are still to be purged, yet nothing is parked, nothing
/// is appended, no collection is made, and the window is neither stopped nor waiting on the purge. Mutant: the swap gate of the plan removed (the
/// record is then judged as a plain name's: it waits on the purge, or parks).
#[test]
fn a_loss_record_above_the_floor_of_a_once_swapped_gone_name_keeps_the_old_rule() {
    let stage = "orders.__stg.l1";
    let now = physical_now_ms();
    let (a, _da) = engine();
    a.create_stage(DB, stage, MergePolicy::Field, Hlc::new(now + 120_000, 0)).expect("the stage");
    let creation = everything(&a)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, stage)
        })
        .expect("the stage's creation");
    let bind = OplogEntry {
        stamp: Stamp::new(Hlc::new(now + 20_000, 0), a.node_id(), a.epoch()),
        kind: OpKind::BindCollection,
        collection: id(),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&kimmy_core::CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: stage.into(),
            })
            .expect("the binding encodes"),
        ),
        meta: None,
    };
    let (m, _dm) = engine();
    let orders = m.create_collection_with_policy(DB, "orders", MergePolicy::Field, None).unwrap();
    m.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    window(&m, std::slice::from_ref(&creation));
    window(&m, std::slice::from_ref(&bind));
    assert_eq!(
        m.resolve(DB, "orders").expect("the name resolves").physical,
        stage,
        "premise: bound"
    );
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the identity is buried");
    assert!(m.rows_under(id()).unwrap() > 0, "premise: its rows are still to be purged");
    let loss = loss_entry(&a, bind.stamp.hlc.wall_ms + 1_000);
    assert!(bind.stamp.hlc < loss.stamp.hlc, "premise: the record is above the burial mark");
    let outcome = window(&m, std::slice::from_ref(&loss));
    assert_eq!(
        (outcome.unknown_collection, outcome.purge_pending),
        (0, 0),
        "the record is taken as before, not stopped and not waiting: {outcome:?}"
    );
    assert_eq!(parked(&m), 0, "nothing is parked");
    assert!(!is_held(&m, loss.stamp), "and nothing is appended");
    assert!(m.get_collection(DB, "orders").is_err(), "and no identity is made beside the stage");
}

/// The DATABASE's drop is the only floor: the member holds the row (`field`) and the database's tombstone D, and its own tombstone is collected
/// (the state w1e (a) builds). A record at or below D is history and is not parked; one above D is parked with the row's R and appended, and no
/// collection is made. Mutant: the floor read as the collection's own tombstone only (the old gone arm: the record above D then stops the window).
#[test]
fn a_loss_record_for_a_member_whose_only_floor_is_the_databases_drop_is_history_below_it_and_parked_above_it()
 {
    let (m, _dm) = engine();
    let life = m.create_collection_with_policy(DB, "orders", MergePolicy::Field, None).unwrap();
    let t0 = life.created.wall_ms;
    let (x, _dx) = engine();
    let own_drop = OplogEntry {
        stamp: Stamp::new(Hlc::new(t0 + 10_000, 0), x.node_id(), x.epoch()),
        kind: OpKind::DropCollection,
        collection: id(),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&kimmy_core::CollectionRef::new(DB, "orders"))
                .expect("the drop encodes"),
        ),
        meta: None,
    };
    let database_drop = crate::engine::ddl_entry(
        Stamp::new(Hlc::new(t0 + 20_000, 0), x.node_id(), x.epoch()),
        OpKind::DropCollection,
        kimmy_core::database_drop_id(DB),
        &kimmy_core::CollectionRef::new(DB, kimmy_core::DATABASE_DROP_NAME),
    )
    .expect("the database drop encodes");
    window(&m, std::slice::from_ref(&own_drop));
    window(&m, std::slice::from_ref(&database_drop));
    m.finish_purges_now().expect("the purges finish");
    m.collect_garbage_at(
        own_drop.stamp.hlc.wall_ms + 1_000 + 5_000,
        crate::RetentionPolicy::new(31_536_000, 1),
    )
    .expect("the retention pass runs");
    assert_eq!(m.collection_dropped_at(id()).unwrap(), None, "premise: no tombstone of its own");
    assert_eq!(
        m.database_dropped_at(DB).unwrap(),
        Some(database_drop.stamp),
        "premise: the database's tombstone is kept"
    );
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the collection is gone");

    let below = loss_entry(&x, t0 + 15_000);
    assert!(below.stamp.hlc < database_drop.stamp.hlc, "premise: below the database's drop");
    let outcome = window(&m, std::slice::from_ref(&below));
    assert_eq!(outcome.unknown_collection, 0, "history at once, not a stop: {outcome:?}");
    assert_eq!(parked(&m), 0, "and not parked");
    assert!(!is_held(&m, below.stamp), "nor appended");

    let above = loss_entry(&x, t0 + 30_000);
    assert!(above.stamp.hlc > database_drop.stamp.hlc, "premise: above the database's drop");
    let outcome = window(&m, std::slice::from_ref(&above));
    assert_eq!(outcome.unknown_collection, 0, "taken, not a stop: {outcome:?}");
    assert_eq!(parked(&m), 1, "parked");
    assert!(is_held(&m, above.stamp), "and appended");
    assert!(m.get_collection(DB, "orders").is_err(), "and no collection is made");
}
