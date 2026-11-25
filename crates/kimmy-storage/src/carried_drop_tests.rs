//! The drop a snapshot carries of a swapped name (ADR-272, decisions 5 and 6; Q6.4 item 1b-2, routes R4 and R6).
//!
//! A page carries a collection's burial as a bare `(id, stamp)` tombstone. For a name that was swapped the page cannot say
//! whether the burial of `orders` is a drop of the name or the loser of a swap (the flip buries the identity at the binding's
//! stamp), nor whether the burial of a stage is the drop or a later binding's. Applied by the old stamp rule to a member that
//! has not made the binding visible, it takes the identity it serves; applied to a member that has, it takes the stage it
//! serves: the name is lost, and the entry that would have put it right is covered by the grant and never served. Such a drop
//! is not applied here: the member keeps what it holds, and the deferral is counted
//! (`kimmy_register_carried_drop_deferred_total`, which must read 0 once the snapshot carries the register, Q6.6). A name with
//! no event and no stage is judged as it always was.

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp, VersionVector,
};

use crate::{Engine, PeerWindow, SnapshotProgress, SyncOutcome, WalkScope, physical_now_ms};

const DB: &str = "app";
const STAGE1: &str = "orders.__stg.a1";
const STAGE2: &str = "orders.__stg.a2";

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

fn creation_of(a: &Engine, physical: &str) -> OplogEntry {
    everything(a)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, physical)
        })
        .unwrap()
}

fn bind_of(a: &Engine, physical: &str, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), a.node_id(), a.epoch()),
        kind: OpKind::BindCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: physical.into(),
            })
            .unwrap(),
        ),
        meta: None,
    }
}

fn life_wall(m: &Engine) -> u64 {
    m.get_collection(DB, "orders").unwrap().created.wall_ms
}

/// A member holding `orders` with a document.
fn with_orders() -> (Engine, tempfile::TempDir) {
    let (m, d) = engine();
    let orders = m.create_collection(DB, "orders").unwrap();
    m.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    (m, d)
}

/// The world of one swap: the author `a` of the stage and its creation, a SENDER that has made the binding visible, and the
/// binding, stamped above every life that exists (so the incarnation rule can never be what turns a tombstone away) and above
/// the creation.
struct Swap {
    a: Engine,
    sender: Engine,
    creation: OplogEntry,
    bind: OplogEntry,
    _dirs: Vec<tempfile::TempDir>,
}

fn swap(joiners: &[&Engine]) -> Swap {
    let (a, da) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE1, MergePolicy::Document, deadline).unwrap();
    let creation = creation_of(&a, STAGE1);
    let (sender, ds) = with_orders();
    let wall = joiners
        .iter()
        .map(|m| life_wall(m))
        .chain([life_wall(&sender), creation.stamp.hlc.wall_ms, physical_now_ms()])
        .max()
        .unwrap()
        + 1;
    let bind = bind_of(&a, STAGE1, wall);
    sender.apply_batch(std::slice::from_ref(&creation)).unwrap();
    window(&sender, std::slice::from_ref(&bind));
    assert_eq!(
        sender.resolve(DB, "orders").unwrap().physical,
        STAGE1,
        "premise: the sender flipped"
    );
    Swap { a, sender, creation, bind, _dirs: vec![da, ds] }
}

/// The sender's whole-database page, applied to `joiner`.
fn join(joiner: &Engine, sender: &Engine) {
    let page = sender.snapshot_page(None, None).unwrap();
    assert!(
        page.dropped_collections.iter().any(|(id, _)| *id == CollectionId::derive(DB, "orders")),
        "premise: the page carries the burial of the identity"
    );
    joiner
        .apply_snapshot_page(sender.node_id(), &mut SnapshotProgress::whole_database(), &page)
        .unwrap();
}

#[test]
fn a_carried_burial_of_the_identity_is_not_applied_to_a_member_that_has_not_made_the_binding_visible()
 {
    // The joiner holds `orders` and the stage's creation, and not the binding (which the sender's grant covers and will never
    // serve it). By the old rule the burial takes `orders`, and nothing puts it back.
    let (j, _dj) = with_orders();
    let s = swap(&[&j]);
    j.apply_batch(std::slice::from_ref(&s.creation)).unwrap();
    join(&j, &s.sender);
    assert!(j.get_collection(DB, "orders").is_ok(), "the member keeps the collection it serves");
    assert_eq!(j.register_carried_drops_deferred(), 1, "and counts the drop it did not apply");
    assert_eq!(
        j.collection_dropped_at(CollectionId::derive(DB, "orders")).unwrap(),
        None,
        "no tombstone is recorded for a burial that was not applied"
    );
}

#[test]
fn a_carried_burial_of_the_stage_a_member_serves_is_not_applied() {
    // The joiner has made the first binding visible (stage 1 serves). The sender went on: a second swap buried stage 1 at its
    // binding. The page carries stage 1's burial; by the old rule the joiner buries the stage it serves, and the second binding
    // is covered by the grant and never served: the name is lost.
    let (j, _dj) = with_orders();
    let s = swap(&[&j]);
    j.apply_batch(std::slice::from_ref(&s.creation)).unwrap();
    window(&j, std::slice::from_ref(&s.bind));
    assert_eq!(
        j.resolve(DB, "orders").unwrap().physical,
        STAGE1,
        "premise: the joiner serves stage 1"
    );
    // the second swap, on the sender
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    s.a.create_stage(DB, STAGE2, MergePolicy::Document, deadline).unwrap();
    let creation2 = creation_of(&s.a, STAGE2);
    let bind2 =
        bind_of(&s.a, STAGE2, creation2.stamp.hlc.wall_ms.max(s.bind.stamp.hlc.wall_ms) + 1);
    s.sender.apply_batch(std::slice::from_ref(&creation2)).unwrap();
    window(&s.sender, std::slice::from_ref(&bind2));
    assert_eq!(
        s.sender.resolve(DB, "orders").unwrap().physical,
        STAGE2,
        "premise: the sender went on"
    );
    join(&j, &s.sender);
    assert!(
        j.get_collection(DB, STAGE1).is_ok(),
        "the stage the member serves is not buried by a bare tombstone"
    );
    assert_eq!(j.resolve(DB, "orders").unwrap().physical, STAGE1, "and the name still answers");
    assert_eq!(j.register_carried_drops_deferred(), 1);
}

#[test]
fn a_carried_drop_of_a_name_that_was_never_swapped_is_applied_as_it_always_was() {
    let (j, _dj) = engine();
    j.create_collection(DB, "plain").unwrap();
    let (s, _ds) = engine();
    s.create_collection(DB, "plain").unwrap();
    assert!(s.drop_collection(DB, "plain").unwrap());
    let page = s.snapshot_page(None, None).unwrap();
    j.apply_snapshot_page(s.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    assert!(j.get_collection(DB, "plain").is_err(), "the drop took the collection");
    assert_eq!(j.register_carried_drops_deferred(), 0, "and nothing was deferred");
}

#[test]
fn a_scoped_pull_of_the_identity_does_not_apply_the_burial_either() {
    // Route R6: the repair of `orders` against a sender that has flipped. Its `dropped` answer is the identity's burial at the
    // binding's stamp.
    let (j, _dj) = with_orders();
    let s = swap(&[&j]);
    j.apply_batch(std::slice::from_ref(&s.creation)).unwrap();
    let id = CollectionId::derive(DB, "orders");
    let page = s.sender.snapshot_page(None, Some(id)).unwrap();
    assert_eq!(page.dropped, Some(s.bind.stamp), "premise: the sender answers with the burial");
    j.apply_snapshot_page(s.sender.node_id(), &mut SnapshotProgress::of_collection(id), &page)
        .unwrap();
    assert!(j.get_collection(DB, "orders").is_ok(), "the member keeps the collection it serves");
    assert_eq!(j.register_carried_drops_deferred(), 1);
}

#[test]
fn a_converging_repair_never_applies_the_senders_burial() {
    // Route R6, the converge policy: `page.dropped` abandons the repair and applies nothing (a pin: this route was already safe).
    let (j, _dj) = with_orders();
    let s = swap(&[&j]);
    j.apply_batch(std::slice::from_ref(&s.creation)).unwrap();
    let id = CollectionId::derive(DB, "orders");
    let page = s.sender.snapshot_page(None, Some(id)).unwrap();
    let applied = j
        .apply_snapshot_page(
            s.sender.node_id(),
            &mut SnapshotProgress::of_collection_converging(id, VersionVector::new()),
            &page,
        )
        .unwrap();
    assert!(applied.sender_dropped, "the repair is abandoned");
    assert!(j.get_collection(DB, "orders").is_ok(), "and the member keeps what it serves");
    assert_eq!(
        j.register_carried_drops_deferred(),
        0,
        "nothing was carried into the register's drop path"
    );
}

// -- Unit B (Q6.6): a tombstone the register owns, for an id this member holds no collection under ----------------------------------------
//
// The held branch above defers the burial of a collection the register judges. A member that holds NO collection under the carried id used
// to record the sender's tombstone without asking: for a stage whose row it keeps (the creation is known, the collection is gone) or the
// identity of a name whose binding is only parked here, that tombstone is the register's to decide, and a recorded one refuses what the
// register would have served. The register is asked by id; if it owns the id, nothing is recorded and the deferral is counted.

/// Whether this member keeps the creation row of the stage `physical`.
fn holds_a_stage_row(m: &Engine, physical: CollectionId) -> bool {
    use redb::ReadableDatabase;
    let txn = m.db().begin_read().unwrap();
    crate::register::stage_row_named_read(&txn, physical).unwrap().is_some()
}

/// Whether the name `orders` has register events here and none in effect: its binding is known ahead of its position.
fn the_binding_is_parked_only(m: &Engine) -> bool {
    use redb::ReadableDatabase;
    let txn = m.db().begin_read().unwrap();
    crate::register::has_events(&txn, DB, "orders").unwrap()
        && !crate::register::has_events_in_effect(&txn, DB, "orders").unwrap()
}

/// A receiver that knows the creation of `STAGE1` and holds no collection of it, and the whole-database page of a sender that has gone
/// on to a second swap (so the page carries the burial of `STAGE1`, the identity's, and a plain collection's).
struct Known {
    j: Engine,
    s: Swap,
    page: crate::SnapshotPage,
    _dj: tempfile::TempDir,
}

fn a_page_that_buries_a_stage_the_receiver_knows() -> Known {
    let (j, dj) = engine();
    let s = swap(&[]);
    j.apply_batch(std::slice::from_ref(&s.creation)).unwrap();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    s.a.create_stage(DB, STAGE2, MergePolicy::Document, deadline).unwrap();
    let creation2 = creation_of(&s.a, STAGE2);
    let wall = creation2.stamp.hlc.wall_ms.max(s.bind.stamp.hlc.wall_ms) + 1;
    let bind2 = bind_of(&s.a, STAGE2, wall);
    s.sender.apply_batch(std::slice::from_ref(&creation2)).unwrap();
    window(&s.sender, std::slice::from_ref(&bind2));
    s.sender.create_collection(DB, "plain").unwrap();
    assert!(s.sender.drop_collection(DB, "plain").unwrap());
    // by hand: the stage's row stays, its collection and its tombstone go (the state of a member whose tombstone was collected)
    j.forget_collection_leaving_rows(DB, STAGE1).unwrap();
    let page = s.sender.snapshot_page(None, None).unwrap();
    Known { j, s, page, _dj: dj }
}

#[test]
fn a_carried_burial_of_a_stage_the_receiver_knows_but_holds_no_collection_of_is_not_recorded() {
    let k = a_page_that_buries_a_stage_the_receiver_knows();
    let stage1 = CollectionId::derive(DB, STAGE1);
    assert!(
        holds_a_stage_row(&k.j, stage1),
        "premise: the receiver keeps the stage's creation row"
    );
    assert!(k.j.get_collection(DB, STAGE1).is_err(), "premise: and holds no collection of it");
    assert!(
        k.j.collection_dropped_at(stage1).unwrap().is_none(),
        "premise: and no tombstone of it"
    );
    assert!(
        k.page.dropped_collections.iter().any(|(id, _)| *id == stage1),
        "premise: the page carries the stage's tombstone"
    );
    k.j.apply_snapshot_page(k.s.sender.node_id(), &mut SnapshotProgress::whole_database(), &k.page)
        .unwrap();
    assert_eq!(
        k.j.collection_dropped_at(stage1).unwrap(),
        None,
        "no tombstone is recorded under the stage's id: the register decides its burial"
    );
    assert_eq!(k.j.register_carried_drops_deferred(), 1, "and the deferral is counted");
}

#[test]
fn a_carried_burial_of_a_plain_collection_on_the_same_page_is_recorded_as_it_always_was() {
    let k = a_page_that_buries_a_stage_the_receiver_knows();
    let plain = CollectionId::derive(DB, "plain");
    assert!(
        k.page.dropped_collections.iter().any(|(id, _)| *id == plain),
        "premise: the page carries the plain collection's tombstone"
    );
    k.j.apply_snapshot_page(k.s.sender.node_id(), &mut SnapshotProgress::whole_database(), &k.page)
        .unwrap();
    assert!(
        k.j.collection_dropped_at(plain).unwrap().is_some(),
        "the plain collection's tombstone is recorded"
    );
}

#[test]
fn a_scoped_burial_of_the_identity_whose_binding_is_only_parked_here_is_not_recorded() {
    // Route R6 (a scoped pull, which grants nothing: nothing is released while the page applies). The receiver never held `orders`; it
    // knows the stage's creation and holds the binding ahead of its position.
    let (j, _dj) = engine();
    let s = swap(&[]);
    j.apply_batch(std::slice::from_ref(&s.creation)).unwrap();
    j.apply_batch(std::slice::from_ref(&s.bind)).unwrap();
    let id = CollectionId::derive(DB, "orders");
    let page = s.sender.snapshot_page(None, Some(id)).unwrap();
    assert!(j.get_collection(DB, "orders").is_err(), "premise: the identity is not held here");
    assert!(
        page.dropped == Some(s.bind.stamp),
        "premise: the page carries the identity's burial at the binding"
    );
    assert!(the_binding_is_parked_only(&j), "premise: the binding is known ahead of its position");
    j.apply_snapshot_page(s.sender.node_id(), &mut SnapshotProgress::of_collection(id), &page)
        .unwrap();
    assert_eq!(
        j.collection_dropped_at(id).unwrap(),
        None,
        "no tombstone is recorded under the identity of a name whose binding is parked"
    );
    assert_eq!(j.register_carried_drops_deferred(), 1, "and the deferral is counted");
}

// -- The count is per delivery: a deferral records no tombstone, so a page delivered again is deferred (and counted) again -------------------

#[test]
fn a_whole_database_page_delivered_twice_defers_the_stage_twice_and_records_no_tombstone() {
    let k = a_page_that_buries_a_stage_the_receiver_knows();
    let stage1 = CollectionId::derive(DB, STAGE1);
    for delivery in 1..=2u64 {
        k.j.apply_snapshot_page(
            k.s.sender.node_id(),
            &mut SnapshotProgress::whole_database(),
            &k.page,
        )
        .unwrap();
        assert_eq!(
            k.j.register_carried_drops_deferred(),
            delivery,
            "the deferral is counted per delivery: the page came {delivery} time(s)"
        );
        assert_eq!(
            k.j.collection_dropped_at(stage1).unwrap(),
            None,
            "and still no tombstone is recorded under the stage's id (delivery {delivery})"
        );
    }
}

#[test]
fn a_scoped_page_delivered_twice_defers_the_identity_twice_and_records_no_tombstone() {
    let (j, _dj) = engine();
    let s = swap(&[]);
    j.apply_batch(std::slice::from_ref(&s.creation)).unwrap();
    j.apply_batch(std::slice::from_ref(&s.bind)).unwrap();
    let id = CollectionId::derive(DB, "orders");
    let page = s.sender.snapshot_page(None, Some(id)).unwrap();
    for delivery in 1..=2u64 {
        j.apply_snapshot_page(s.sender.node_id(), &mut SnapshotProgress::of_collection(id), &page)
            .unwrap();
        assert_eq!(
            j.register_carried_drops_deferred(),
            delivery,
            "the deferral is counted per delivery: the page came {delivery} time(s)"
        );
        assert_eq!(
            j.collection_dropped_at(id).unwrap(),
            None,
            "and still no tombstone is recorded under the identity's id (delivery {delivery})"
        );
    }
}
