//! The register in storage (ADR-272): the stage's creation row, the one door every event passes, the events in effect and
//! the events ahead of their position.

use kimmy_core::register_state::{Put, Ready, Visible};
use kimmy_core::{
    CollectionId, Epoch, Hlc, MergePolicy, NodeId, OpKind, RegisterEvent, StageInfo, StageMeta,
    Stamp,
};

use crate::{Engine, WalkScope, WriterHolder, physical_now_ms};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

const DB: &str = "app";
const STAGE: &str = "orders.__stg.a1";

fn st(hlc: u64, origin: u8) -> Stamp {
    Stamp::new(Hlc::new(hlc, 0), NodeId::from_bytes([origin; 16]), Epoch::ZERO)
}

fn everything(from: &Engine) -> Vec<kimmy_core::OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

/// A member with a stage made by a local `create_stage` whose deadline is a minute away.
fn with_a_stage() -> (Engine, tempfile::TempDir, Hlc) {
    let (e, dir) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    e.create_stage(DB, STAGE, MergePolicy::Document, deadline).unwrap();
    (e, dir, deadline)
}

fn bind() -> RegisterEvent {
    RegisterEvent::Bind { physical: STAGE.into() }
}

/// Run `f` in a write transaction of the engine and commit it.
fn in_txn<T>(e: &Engine, f: impl FnOnce(&redb::WriteTransaction) -> T) -> T {
    let txn = e.begin_write(WriterHolder::Ddl).unwrap();
    let out = f(&txn);
    txn.commit().unwrap();
    out
}

#[test]
fn a_stage_creation_keeps_its_stamp_and_deadline_in_the_register_in_the_transaction_of_the_creation()
 {
    let (e, _dir, deadline) = with_a_stage();
    let creation = everything(&e)
        .into_iter()
        .find(|x| {
            x.kind == OpKind::CreateCollection && x.collection == CollectionId::derive(DB, STAGE)
        })
        .expect("the stage's creation entry");
    let kept =
        in_txn(&e, |txn| crate::register::stage_row(txn, CollectionId::derive(DB, STAGE)).unwrap());
    assert_eq!(
        kept,
        Some(StageMeta { created: creation.stamp, info: StageInfo { deadline } }),
        "the row is the creation entry's stamp and deadline"
    );
    let meta = e.get_collection(DB, STAGE).unwrap();
    assert_eq!(meta.stage, kept, "the collection's own definition says the same");
}

#[test]
fn a_replicated_stage_creation_keeps_the_stamp_of_the_entry_not_the_receivers() {
    let (a, _da, deadline) = with_a_stage();
    let (b, _db) = engine();
    b.apply_batch(&everything(&a)).unwrap();
    let origin_stamp = everything(&a)
        .into_iter()
        .find(|x| {
            x.kind == OpKind::CreateCollection && x.collection == CollectionId::derive(DB, STAGE)
        })
        .unwrap()
        .stamp;
    let kept =
        in_txn(&b, |txn| crate::register::stage_row(txn, CollectionId::derive(DB, STAGE)).unwrap());
    assert_eq!(kept, Some(StageMeta { created: origin_stamp, info: StageInfo { deadline } }));
    assert_ne!(origin_stamp.node, b.node_id(), "premise: the creation is another member's");
}

#[test]
fn the_row_outlives_the_stage_physical() {
    let (e, _dir, _deadline) = with_a_stage();
    assert!(
        in_txn(&e, |txn| crate::register::stage_row(txn, CollectionId::derive(DB, STAGE)).unwrap())
            .is_some()
    );
    // the system path that buries a stage: a client's drop of a stage name is refused on the mark (ADR-272, decision 15)
    e.drop_collection_inner(DB, STAGE, None).unwrap();
    assert!(e.get_collection(DB, STAGE).is_err(), "premise: the stage's collection is gone");
    assert!(
        in_txn(&e, |txn| crate::register::stage_row(txn, CollectionId::derive(DB, STAGE)).unwrap())
            .is_some(),
        "the creation row stays: a later binding's validity reads it"
    );
}

#[test]
fn an_ordinary_collection_leaves_no_stage_row() {
    let (e, _dir) = engine();
    let orders = e.create_collection(DB, "orders").unwrap();
    assert!(in_txn(&e, |txn| crate::register::stage_row(txn, orders.id).unwrap()).is_none());
}

#[test]
fn every_event_goes_through_the_one_door() {
    let (e, _dir, deadline) = with_a_stage();
    in_txn(&e, |txn| {
        let put = |stamp, event| crate::register::put(txn, DB, "orders", stamp, event).unwrap();
        // valid: at the deadline
        assert_eq!(put(st(deadline.wall_ms, 1), bind()), Put::Stored);
        assert_eq!(put(st(deadline.wall_ms, 1), bind()), Put::Duplicate, "the same event again");
        // above the deadline: history, never stored
        let other = RegisterEvent::Bind { physical: "orders.__stg.a2".into() };
        assert_eq!(put(st(deadline.wall_ms + 1, 2), bind()), Put::InvalidBinding);
        // a stage whose creation this member never saw
        assert_eq!(put(st(10, 1), other), Put::UnknownStage);
        // a recreation and a logical drop need no stage
        assert_eq!(
            put(st(5, 1), RegisterEvent::Create { physical: "orders.r1".into() }),
            Put::Stored
        );
        assert_eq!(put(st(6, 1), RegisterEvent::LogicalDrop), Put::Stored);
        let timeline = crate::register::timeline(txn, DB, "orders").unwrap();
        let stamps: Vec<Stamp> = timeline.events().iter().map(|(s, _)| *s).collect();
        assert_eq!(
            stamps,
            [st(5, 1), st(6, 1), st(deadline.wall_ms, 1)],
            "only what the door let in, in stamp order"
        );
    });
}

#[test]
fn what_is_in_effect_survives_the_transaction_and_decides_what_the_name_serves() {
    let (e, _dir, deadline) = with_a_stage();
    in_txn(&e, |txn| {
        crate::register::put(txn, DB, "orders", st(deadline.wall_ms, 1), bind()).unwrap();
    });
    let visible = in_txn(&e, |txn| {
        let timeline = crate::register::timeline(txn, DB, "orders").unwrap();
        timeline.visible(|p| if p == STAGE { Ready::Alive } else { Ready::Buried })
    });
    assert_eq!(visible, Visible::Bound { physical: STAGE.into(), stamp: st(deadline.wall_ms, 1) });
}

#[test]
fn an_event_ahead_of_its_position_is_known_and_not_in_effect_until_released() {
    let (e, _dir, deadline) = with_a_stage();
    let at = st(deadline.wall_ms, 1);
    in_txn(&e, |txn| {
        assert_eq!(crate::register::park(txn, DB, "orders", at, bind()).unwrap(), Put::Stored);
        assert!(
            crate::register::timeline(txn, DB, "orders").unwrap().events().is_empty(),
            "not in effect"
        );
        assert_eq!(
            crate::register::parked(txn, DB, "orders").unwrap().events().len(),
            1,
            "but known"
        );
        let refusal = crate::register::refusal(txn, DB, "orders", "orders")
            .unwrap()
            .expect("a refusal on information");
        assert!(refusal.parked && refusal.event == at, "{refusal:?}");
        // an invalid binding is not parked either
        assert_eq!(
            crate::register::park(txn, DB, "orders", st(deadline.wall_ms + 5, 2), bind()).unwrap(),
            Put::InvalidBinding
        );
    });
    in_txn(&e, |txn| {
        assert_eq!(crate::register::release(txn, DB, "orders", &at).unwrap(), Some(Put::Stored));
        assert!(
            crate::register::parked(txn, DB, "orders").unwrap().events().is_empty(),
            "no longer information"
        );
        assert_eq!(
            crate::register::timeline(txn, DB, "orders").unwrap().events().len(),
            1,
            "in effect now"
        );
        assert_eq!(
            crate::register::release(txn, DB, "orders", &at).unwrap(),
            None,
            "nothing left to release"
        );
        let refusal = crate::register::refusal(txn, DB, "orders", "orders")
            .unwrap()
            .expect("known, not parked");
        assert!(!refusal.parked, "{refusal:?}");
    });
}

#[test]
fn an_event_already_in_effect_is_not_parked_again() {
    let (e, _dir, deadline) = with_a_stage();
    let at = st(deadline.wall_ms, 1);
    in_txn(&e, |txn| {
        crate::register::put(txn, DB, "orders", at, bind()).unwrap();
        assert_eq!(crate::register::park(txn, DB, "orders", at, bind()).unwrap(), Put::Duplicate);
        assert!(crate::register::parked(txn, DB, "orders").unwrap().events().is_empty());
    });
}

/// Only `register.rs` opens the three register tables: the door cannot be walked around by a route that writes a row itself.
#[test]
fn only_this_module_opens_the_register_tables() {
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|x| x == "rs") {
                out.push(path);
            }
        }
    }
    let mut files = Vec::new();
    walk(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"), &mut files);
    assert!(files.len() > 50, "premise: the sources were found");
    let mut offenders = Vec::new();
    for file in files {
        let name = file.file_name().unwrap().to_string_lossy().into_owned();
        if name == "register.rs" || name == "tables.rs" || name == "register_tests.rs" {
            continue;
        }
        let text = std::fs::read_to_string(&file).unwrap();
        for table in [
            "tables::REGISTER_STAGES",
            "tables::REGISTER_POST",
            "tables::REGISTER_VISIBLE",
            "tables::REGISTER_INFO",
            "tables::REGISTER;",
            "tables::REGISTER)",
            "tables::REGISTER,",
        ] {
            if text.contains(table) {
                offenders.push(format!("{name}: {table}"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "the register tables are opened outside register.rs: {offenders:?}"
    );
}

/// The one trigger of a staging write, and its one `Hold` test (decision 4, rule S; decision 9): a snapshot document is state and not
/// history, so a staging write that arrives under `Hold` is never owed, for a stage this member holds and one it does not.
#[test]
fn a_staging_write_is_never_owed_under_hold_and_is_owed_in_a_window() {
    use crate::engine::Position;
    let (author, _da) = engine();
    author
        .create_stage(DB, STAGE, MergePolicy::Document, Hlc::new(physical_now_ms() + 60_000, 0))
        .unwrap();
    let creation =
        everything(&author).into_iter().find(|e| e.kind == OpKind::CreateCollection).unwrap();
    let stage = author.get_collection(DB, STAGE).unwrap();
    author.insert(&stage, bson::doc! { "_id": 1 }).unwrap();
    let row = everything(&author).into_iter().find(|e| e.kind == OpKind::Insert).unwrap();
    let (m, _dm) = engine();
    m.apply_batch(&[creation]).unwrap();
    let txn = m.begin_write(WriterHolder::Replication).unwrap();
    assert!(
        !m.staging_write_is_owed(&txn, &row, Position::Hold, None).unwrap(),
        "under Hold a staging write is never owed: a snapshot document is state, with no entry to serve"
    );
    assert!(
        m.staging_write_is_owed(&txn, &row, Position::InWindow, None).unwrap(),
        "in a window the author's write, below any binding and not in the oplog, is owed"
    );
    let other = kimmy_core::OplogEntry {
        stamp: Stamp::new(row.stamp.hlc, NodeId::from_bytes([3; 16]), row.stamp.epoch),
        ..row.clone()
    };
    assert!(
        !m.staging_write_is_owed(&txn, &other, Position::InWindow, None).unwrap(),
        "a write of another origin is not a staging write"
    );
    txn.abort().unwrap();
}

/// F6 of the review of items 6 and 6b: a stage row of the form before it named its stage is a development store of schema 12 written by an earlier build.
/// It is refused by name, with the wipe, and not read as corruption of unknown cause.
#[test]
fn a_stage_row_of_the_form_before_it_named_its_stage_is_refused_with_the_wipe() {
    let (e, _d) = engine();
    let id = CollectionId::derive(DB, STAGE);
    crate::register::write_old_form_stage_row_for_test(&e, id);
    let txn = e.begin_write(WriterHolder::Ddl).unwrap();
    let refused = crate::register::stage_row(&txn, id).unwrap_err().to_string();
    txn.abort().unwrap();
    assert!(
        refused.contains("wipe this data directory") && refused.contains("stage"),
        "the refusal names what it is and the wipe: {refused}"
    );
}
