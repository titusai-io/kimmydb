//! Unit P, P9 (tests first) and P10: the drop purger's COMPLETION for an id makes the collection again when the options row the id keeps is above
//! its floor, as a caller of the one remake function (`Engine::remake_from_the_row`).
//!
//! The case: a creation of a plain name stamped above its drop reaches a member whose purge is not done, by entries. The creation arm keeps the options
//! row (above the floor) and then meets the old rows, so the creation waits for the purge and is left untaken; if its sender no longer serves it, the
//! row stands above the floor with no collection. The purger's completion makes the collection from the row: the creation's life (created = the
//! row's `at`, no marker) with the drop as its floor. The hold the register puts on the physicals it buries (`PURGE_NOT_BEFORE`) is not a plain
//! name's and is not extended to one (ruled 5:35 PM: a hold to a join's grant would deadlock the join with the page's wait): a name the register
//! holds is a swapped name, which is never made again.

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp, VersionVector,
};

use crate::{Engine, PeerWindow, WalkScope, physical_now_ms};

const DB: &str = "app";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn window(b: &Engine, entries: &[OplogEntry]) -> crate::SyncOutcome {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = b.witnessed_vector().unwrap();
    let mut outcome = crate::SyncOutcome::default();
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

/// `m` took a collection `orders` with a document from `x`, and the drop of it (D, by `z`): it holds the tombstone and the dropped life's rows are still
/// filed under the id. Returns (m, D, the dirs).
fn the_member_with_a_pending_purge() -> (Engine, OplogEntry, [tempfile::TempDir; 3]) {
    let (x, dx) = engine();
    let first = x.create_collection(DB, "orders").unwrap();
    x.insert(&first, doc! { "_id": 1, "v": "old" }).unwrap();
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
    assert!(m.rows_under(id()).unwrap() > 0, "premise: the dropped life's rows are not purged");
    (m, d, [dx, dz, dm])
}

/// P9/P10: a creation of the name stamped above the drop reaches a member whose purge is not done, by entries. The creation arm keeps the options row
/// (above the drop) and then meets the old rows, so the creation waits for the purge (`purge_pending`, left untaken). Its sender never offers it again
/// (retention), so nothing re-offers it. The purger then completes, and with it the completion makes the collection from the row: the creation's life,
/// dated by the row's `at`, unmarked, with the drop as its floor, holding none of the dropped rows. RED today (the collection stays absent: the row
/// stands above the floor and nothing remakes it); GREEN at P10. Mutants (P10): the completion remakes nothing; it remakes whenever a row exists.
///
/// NOT built, on purpose: "the purge completes in the MIDDLE of a whole join, after a page brought its row above the floor". The page route cannot
/// leave the row above the floor while the old rows stand: its pre-check (`page_waits_on_a_purge`) reads the page's carried options rows and the
/// states, and waits whole BEFORE `restore_collection` keeps any row, and a carry merge that meets the pending purge aborts its transaction with
/// the merged rows. So a join never holds a row above the floor with the old rows still filed; only the entries route's creation does (the row
/// is kept, then the creation waits), and that is the row below.
#[test]
fn a_purge_that_completes_after_a_creation_waited_for_it_makes_the_collection_from_its_row() {
    let (m, d, _dirs) = the_member_with_a_pending_purge();
    pause();
    let (y, _dy) = engine();
    y.create_collection(DB, "orders").unwrap();
    let c4 = everything(&y)
        .into_iter()
        .find(|e| e.kind == OpKind::CreateCollection && e.collection == id())
        .expect("the later creation");
    assert!(d.stamp.hlc < c4.stamp.hlc, "premise: the creation is stamped above the drop");
    let outcome = window(&m, std::slice::from_ref(&c4));
    assert!(outcome.purge_pending > 0, "premise: the creation waits for the purge: {outcome:?}");
    assert!(
        m.get_collection(DB, "orders").is_err(),
        "premise: no collection is made over the rows"
    );

    // the purger completes; nothing is offered again
    m.purge_dropped_collection(id()).unwrap();
    assert_eq!(m.rows_under(id()).unwrap(), 0, "the purge completed");
    let made = m.get_collection(DB, "orders").expect(
        "the purge's completion made the collection from the row, with nothing offered again",
    );
    assert_eq!(made.created, c4.stamp.hlc, "the creation's life, dated by the row's `at`");
    assert!(!made.is_redated(), "and not marked as the remains of the dropped life");
    assert_eq!(made.incarnation_floor, Some(d.stamp.hlc), "with the drop as its floor");
    assert_eq!(m.count(&made, WalkScope::Background).unwrap(), 0, "and none of the dropped rows");
}

/// A name the REGISTER holds is a swapped name: its identity is buried by the binding, and the burial records the hold on its purge
/// (`PURGE_NOT_BEFORE`, the deferral). When the hold passes and the purge completes, the completion makes NOTHING, though the identity's options
/// row is above its floor (a later creation of the name was seen): the name answers from the stage, and the remake never fires for a name with swap
/// history. Built through a real stage and binding, with the real deferral. Mutant (P10): the swap gate of the remake removed.
#[test]
fn a_purge_that_completes_for_a_name_the_register_held_makes_nothing() {
    let stage = "orders.__stg.p1";
    let now = physical_now_ms();
    let (a, _da) = engine();
    a.create_stage(DB, stage, MergePolicy::Document, Hlc::new(now + 120_000, 0))
        .expect("the stage");
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
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: stage.into(),
            })
            .expect("the binding encodes"),
        ),
        meta: None,
    };
    let (m, _dm) = engine();
    let orders = m.create_collection(DB, "orders").unwrap();
    m.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    m.set_purge_deferral(std::time::Duration::from_millis(200));
    window(&m, std::slice::from_ref(&creation));
    window(&m, std::slice::from_ref(&bind));
    assert_eq!(
        m.resolve(DB, "orders").expect("the name resolves").physical,
        stage,
        "premise: bound"
    );
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the identity is buried");
    assert!(m.rows_under(id()).unwrap() > 0, "premise: its rows are held for the purger");
    assert_eq!(m.purge_dropped_collection(id()).unwrap(), 0, "the register's hold is honoured");
    // a later creation of the name, stamped above the burial mark: the options row rises above the floor
    let later = OplogEntry {
        stamp: Stamp::new(Hlc::new(now + 21_000, 0), a.node_id(), a.epoch()),
        kind: OpKind::CreateCollection,
        collection: id(),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&kimmy_core::CollectionCreate {
                db: DB.into(),
                name: "orders".into(),
                merge_policy: MergePolicy::Document,
                validator: None,
                join_retention_ms: None,
                loss_limits: None,
                stage: None,
            })
            .expect("the creation encodes"),
        ),
        meta: None,
    };
    window(&m, std::slice::from_ref(&later));
    std::thread::sleep(std::time::Duration::from_millis(300));
    m.purge_dropped_collection(id()).unwrap();
    assert_eq!(m.rows_under(id()).unwrap(), 0, "the purge completed once the hold passed");
    assert!(m.get_collection(DB, "orders").is_err(), "and made nothing: the name is the stage's");
    assert_eq!(m.resolve(DB, "orders").expect("the name resolves").physical, stage, "still bound");
}

/// A purge that completes with no row above the floor makes nothing: a drop with no later creation leaves the collection gone. A guard (green today
/// and after P10). Mutant (P10): the completion remakes whenever a row exists.
#[test]
fn a_purge_that_completes_with_no_row_above_the_floor_makes_nothing() {
    let (m, _d, _dirs) = the_member_with_a_pending_purge();
    m.purge_dropped_collection(id()).unwrap();
    assert_eq!(m.rows_under(id()).unwrap(), 0, "the purge completed");
    assert!(m.get_collection(DB, "orders").is_err(), "no row above the drop: nothing is made");
}
