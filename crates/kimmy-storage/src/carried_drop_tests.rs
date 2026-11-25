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

/// The page of a sender that lists every tombstone it holds, the register-owned ones too: what a sender before the register's carry
/// sends (its tombstones are put back into the page by hand), which the receiver's own deferral still has to meet.
fn with_the_registers_tombstones(
    sender: &Engine,
    mut page: crate::SnapshotPage,
) -> crate::SnapshotPage {
    for (id, stamp) in sender.collections_dropped().unwrap() {
        if !page.dropped_collections.iter().any(|(listed, _)| *listed == id) {
            page.dropped_collections.push((id, stamp));
        }
    }
    page
}

/// The sender's whole-database page, applied to `joiner`: the page of a sender before the
/// register's carry, which lists the register's tombstones and carries no register (the receiver's
/// own deferral is what these tests pin). With the carry, the joiner takes the swap from the
/// register, and the identity or the stage it served is replaced as on the sender.
fn join(joiner: &Engine, sender: &Engine) {
    let mut page = sender.snapshot_page(None, None).unwrap();
    page.register = None;
    let page = with_the_registers_tombstones(sender, page);
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

/// What the name answers on `j` after a pull, as a value an assertion can compare.
fn served_by(j: &Engine) -> Result<String, String> {
    j.resolve(DB, "orders").map(|resolved| resolved.physical).map_err(|e| e.to_string())
}

#[test]
fn a_joiner_that_held_the_identity_serves_the_stage_the_carry_binds_and_holds_it() {
    // With the carry the joiner takes the swap from the register: the binding its window never
    // delivered makes the stage visible and binds the identity out, as on the sender. The name is
    // served throughout, by a physical the member holds.
    let (j, _dj) = with_orders();
    let s = swap(&[&j]);
    j.apply_batch(std::slice::from_ref(&s.creation)).unwrap();
    pull(&j, &s.sender);
    assert_eq!(
        served_by(&j),
        Ok(STAGE1.to_string()),
        "the name answers from the stage the sender serves"
    );
    assert!(j.get_collection(DB, STAGE1).is_ok(), "and the member holds the physical it serves");
}

#[test]
fn a_joiner_that_served_the_first_stage_serves_the_second_the_carry_binds_and_holds_it() {
    // The joiner served stage 1; the sender went on to stage 2. The second binding is covered by
    // the grant and no window serves it, so the carry is what delivers it: the name is served by
    // stage 2, which the page made here with its creation and its row.
    let (j, _dj) = with_orders();
    let s = swap(&[&j]);
    j.apply_batch(std::slice::from_ref(&s.creation)).unwrap();
    window(&j, std::slice::from_ref(&s.bind));
    assert_eq!(served_by(&j), Ok(STAGE1.to_string()), "premise: the joiner serves stage 1");
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    s.a.create_stage(DB, STAGE2, MergePolicy::Document, deadline).unwrap();
    let creation2 = creation_of(&s.a, STAGE2);
    let bind2 =
        bind_of(&s.a, STAGE2, creation2.stamp.hlc.wall_ms.max(s.bind.stamp.hlc.wall_ms) + 1);
    s.sender.apply_batch(std::slice::from_ref(&creation2)).unwrap();
    window(&s.sender, std::slice::from_ref(&bind2));
    assert_eq!(served_by(&s.sender), Ok(STAGE2.to_string()), "premise: the sender went on");
    pull(&j, &s.sender);
    assert_eq!(served_by(&j), Ok(STAGE2.to_string()), "the name answers from the second stage");
    assert!(j.get_collection(DB, STAGE2).is_ok(), "and the member holds the physical it serves");
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

/// A sender that flipped to `STAGE1`, went on to a second swap (`STAGE2`, which buries `STAGE1`), and dropped a plain collection.
fn a_sender_that_went_on_to_a_second_swap() -> Swap {
    let s = swap(&[]);
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    s.a.create_stage(DB, STAGE2, MergePolicy::Document, deadline).unwrap();
    let creation2 = creation_of(&s.a, STAGE2);
    let wall = creation2.stamp.hlc.wall_ms.max(s.bind.stamp.hlc.wall_ms) + 1;
    let bind2 = bind_of(&s.a, STAGE2, wall);
    s.sender.apply_batch(std::slice::from_ref(&creation2)).unwrap();
    window(&s.sender, std::slice::from_ref(&bind2));
    s.sender.create_collection(DB, "plain").unwrap();
    assert!(s.sender.drop_collection(DB, "plain").unwrap());
    s
}

fn a_page_that_buries_a_stage_the_receiver_knows() -> Known {
    let (j, dj) = engine();
    let s = a_sender_that_went_on_to_a_second_swap();
    j.apply_batch(std::slice::from_ref(&s.creation)).unwrap();
    // by hand: the stage's row stays, its collection and its tombstone go (the state of a member whose tombstone was collected)
    j.forget_collection_leaving_rows(DB, STAGE1).unwrap();
    let page =
        with_the_registers_tombstones(&s.sender, s.sender.snapshot_page(None, None).unwrap());
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

// -- Unit C3: the sender leaves out of its carried drops every id the register owns --------------------------------------------------------

fn ids_of(page: &crate::SnapshotPage) -> Vec<CollectionId> {
    page.dropped_collections.iter().map(|(id, _)| *id).collect()
}

#[test]
fn a_page_lists_a_plain_collections_tombstone_and_none_the_register_owns() {
    let s = a_sender_that_went_on_to_a_second_swap();
    let (identity, stage1) = (CollectionId::derive(DB, "orders"), CollectionId::derive(DB, STAGE1));
    let plain = CollectionId::derive(DB, "plain");
    let held: Vec<CollectionId> =
        s.sender.collections_dropped().unwrap().into_iter().map(|(id, _)| id).collect();
    assert!(
        [identity, stage1, plain].iter().all(|id| held.contains(id)),
        "premise: the sender holds a tombstone for each of the three"
    );
    let page = s.sender.snapshot_page(None, None).unwrap();
    let listed = ids_of(&page);
    assert!(!listed.contains(&identity), "the bound-out identity's tombstone is not listed");
    assert!(!listed.contains(&stage1), "nor the buried stage's");
    assert!(listed.contains(&plain), "and the plain collection's is");
}

#[test]
fn a_buried_recreations_tombstone_is_not_listed_either() {
    let s = swap(&[]);
    assert!(s.sender.drop_collection(DB, "orders").unwrap(), "premise: a logical drop of the name");
    s.sender.create_collection(DB, "orders").expect("the name is made again");
    let recreation = s.sender.resolve(DB, "orders").unwrap().physical;
    assert!(kimmy_core::is_recreation_name(&recreation), "premise: a recreation holds the name");
    assert!(s.sender.drop_collection(DB, "orders").unwrap(), "the second drop buries it");
    let id = CollectionId::derive(DB, &recreation);
    let held = s.sender.collections_dropped().unwrap();
    assert!(
        held.iter().any(|(held, _)| *held == id),
        "premise: the sender holds the recreation's tombstone"
    );
    let page = s.sender.snapshot_page(None, None).unwrap();
    assert!(!ids_of(&page).contains(&id), "the buried recreation's tombstone is not listed");
}

#[test]
fn an_identity_dropped_while_its_binding_is_only_parked_is_still_listed_and_buries_the_receivers() {
    // The state F3 made: the drop is only in the tombstone (REGISTER holds nothing of the name; the binding is parked in REGISTER_INFO), so
    // leaving it out would leave a receiver that holds the identity holding it for good.
    let (j, _dj) = with_orders();
    let (a, _da) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE1, MergePolicy::Document, deadline).unwrap();
    let creation = creation_of(&a, STAGE1);
    let (sender, _ds) = engine();
    sender.apply_batch(std::slice::from_ref(&creation)).unwrap();
    let wall = creation.stamp.hlc.wall_ms.max(physical_now_ms()) + 10;
    sender.apply_batch(std::slice::from_ref(&bind_of(&a, STAGE1, wall))).unwrap();
    assert!(
        the_binding_is_parked_only(&sender),
        "premise: the sender's binding is known ahead of its position"
    );
    // by hand, at a stamp the sender's own coverage names (the creation's) and above the receiver's identity
    let identity = CollectionId::derive(DB, "orders");
    sender.record_collection_drops(&[(identity, creation.stamp)]).unwrap();
    let page = sender.snapshot_page(None, None).unwrap();
    assert!(
        ids_of(&page).contains(&identity),
        "the identity's tombstone is listed: only parked events do not own it"
    );
    j.apply_snapshot_page(sender.node_id(), &mut SnapshotProgress::whole_database(), &page)
        .unwrap();
    assert!(j.get_collection(DB, "orders").is_err(), "and the receiver's identity is buried by it");
}

// -- Unit C2a: a stage arrives by a snapshot page with its creation and its row ---------------------------------------------------------------

fn the_stage_row_of(m: &Engine, physical: &str) -> Option<kimmy_core::StageMeta> {
    use redb::ReadableDatabase;
    let txn = m.db().begin_read().unwrap();
    crate::register::stage_row_named_read(&txn, CollectionId::derive(DB, physical))
        .unwrap()
        .map(|row| row.meta)
}

fn covered_through(stamp: Stamp) -> VersionVector {
    let mut vector = VersionVector::default();
    vector.observe(stamp);
    vector
}

fn the_logical_drops_of_orders(m: &Engine) -> usize {
    use redb::ReadableDatabase;
    let txn = m.db().begin_read().unwrap();
    let timeline = crate::register::timeline_read(&txn, DB, "orders").unwrap();
    timeline.events().iter().filter(|(_, event)| event.physical().is_none()).count()
}

/// The scoped page of `STAGE1` that the sender serves, and the sender's own creation of it.
fn the_page_of_the_stage(s: &Swap) -> (CollectionId, crate::SnapshotPage, kimmy_core::StageMeta) {
    let id = CollectionId::derive(DB, STAGE1);
    let page = s.sender.snapshot_page(None, Some(id)).unwrap();
    let stage = s.sender.get_collection(DB, STAGE1).unwrap().stage;
    (id, page, stage.expect("premise: the sender's stage holds its creation"))
}

#[test]
fn a_stage_a_page_made_holds_its_creation_and_its_row_after_a_reopen() {
    let s = swap(&[]);
    let (j, dj) = engine();
    let (id, page, theirs) = the_page_of_the_stage(&s);
    j.apply_snapshot_page(s.sender.node_id(), &mut SnapshotProgress::of_collection(id), &page)
        .unwrap();
    let path = dj.path().join("kimmy.redb");
    drop(j);
    let j = Engine::open(&path).unwrap();
    assert_eq!(
        j.get_collection(DB, STAGE1).unwrap().stage,
        Some(theirs),
        "the stage holds the creation the page carried, after a reopen"
    );
    assert_eq!(
        the_stage_row_of(&j, STAGE1),
        Some(theirs),
        "and its row, written with the collection"
    );
}

#[test]
fn a_binding_of_a_stage_a_page_made_takes_effect_when_it_arrives_in_a_window() {
    let s = swap(&[]);
    let (j, _dj) = engine();
    let (id, page, _) = the_page_of_the_stage(&s);
    j.apply_snapshot_page(s.sender.node_id(), &mut SnapshotProgress::of_collection(id), &page)
        .unwrap();
    // the grant covers the creation, so no entry will ever write the row: the page's own is the only one
    j.absorb_version_vector(&covered_through(s.creation.stamp)).unwrap();
    window(&j, std::slice::from_ref(&s.bind));
    let served = j.resolve(DB, "orders").map(|resolved| resolved.physical);
    assert_eq!(
        served.map_err(|e| e.to_string()),
        Ok(STAGE1.to_string()),
        "the binding takes effect: the page gave the stage its row"
    );
}

#[test]
fn a_held_stage_whose_creation_differs_from_the_pages_refuses_the_page_by_name() {
    let s = swap(&[]);
    let (j, _dj) = engine();
    j.apply_batch(std::slice::from_ref(&s.creation)).unwrap();
    let (id, mut page, theirs) = the_page_of_the_stage(&s);
    let other = Stamp::new(Hlc::new(theirs.created.hlc.wall_ms + 1, 0), s.a.node_id(), s.a.epoch());
    page.collections[0].stage = Some(kimmy_core::StageMeta { created: other, ..theirs });
    let outcome =
        j.apply_snapshot_page(s.sender.node_id(), &mut SnapshotProgress::of_collection(id), &page);
    let error = outcome.expect_err("an inconsistent sender's page is refused").to_string();
    assert!(error.contains("two creations"), "a stage cannot have two creations: {error}");
}

#[test]
fn a_stage_a_page_made_below_a_drop_the_member_holds_as_a_tombstone_is_judged_as_the_creation_arm_judges_it()
 {
    // the creation arm seeds the name's drop from the identity's tombstone when the first thing the register learns is a stage created
    // below it (review of items 6 and 6b, F1); a stage made by a page is judged the same way
    let s = swap(&[]);
    let (j, _dj) = with_orders();
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(j.drop_collection(DB, "orders").unwrap(), "premise: the identity is dropped here");
    let tombstone = j.collection_dropped_at(CollectionId::derive(DB, "orders")).unwrap();
    assert!(tombstone.is_some(), "premise: the member holds the identity's tombstone");
    assert_eq!(
        the_logical_drops_of_orders(&j),
        0,
        "premise: the register holds no drop of the name yet"
    );
    let (id, page, _) = the_page_of_the_stage(&s);
    let passes = crate::sync::race_hooks::settle_passes();
    j.apply_snapshot_page(s.sender.node_id(), &mut SnapshotProgress::of_collection(id), &page)
        .unwrap();
    assert!(
        crate::sync::race_hooks::settle_passes() > passes,
        "the seed's drop ends the identity's remains by the settle pass, in the same transaction"
    );
    assert_eq!(
        the_logical_drops_of_orders(&j),
        1,
        "the stage's creation is judged: the tombstone above it is the name's logical drop"
    );
}

// -- Unit C2b: the register travels on the last page, held to the grant, and takes effect in the last page's transaction -----------------------

type Patch<'a> = &'a dyn Fn(&mut crate::SnapshotPage);

/// A whole-database pull of `sender` into `joiner`, each page patched by `patch` before it is applied, until it completes or a page fails.
fn pull_patched(
    joiner: &Engine,
    sender: &Engine,
    patch: Patch<'_>,
) -> (SnapshotProgress, crate::Result<()>) {
    let mut progress = SnapshotProgress::whole_database();
    for _ in 0..50 {
        if progress.is_complete() {
            return (progress, Ok(()));
        }
        let mut page = sender.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        patch(&mut page);
        if let Err(e) = joiner.apply_snapshot_page(sender.node_id(), &mut progress, &page) {
            return (progress, Err(e));
        }
    }
    panic!("the pull did not complete in 50 pages");
}

fn pull(joiner: &Engine, sender: &Engine) {
    let (progress, result) = pull_patched(joiner, sender, &|_| {});
    result.expect("every page applies");
    assert!(progress.is_complete(), "the pull completes");
}

fn the_events_of_orders(m: &Engine) -> Vec<(Stamp, kimmy_core::RegisterEvent)> {
    use redb::ReadableDatabase;
    let txn = m.db().begin_read().unwrap();
    crate::register::timeline_read(&txn, DB, "orders").unwrap().events().to_vec()
}

/// A `REGISTER` row of `orders` as a page carries it.
fn an_event_row(
    stamp: &Stamp,
    event: &kimmy_core::RegisterEvent,
) -> (serde_bytes::ByteBuf, serde_bytes::ByteBuf) {
    let mut key = CollectionId::derive(DB, "orders").0.to_be_bytes().to_vec();
    key.extend_from_slice(&stamp.to_key_bytes());
    (serde_bytes::ByteBuf::from(key), serde_bytes::ByteBuf::from(event.encode()))
}

fn bind_event() -> kimmy_core::RegisterEvent {
    kimmy_core::RegisterEvent::Bind { physical: STAGE1.into() }
}

#[test]
fn a_fresh_member_that_joins_after_a_swap_holds_the_senders_register_and_serves_the_stage() {
    let s = swap(&[]);
    s.sender.collect_oplog_entry_for_test(&s.bind.stamp);
    assert!(
        everything(&s.sender).iter().all(|entry| entry.stamp != s.bind.stamp),
        "premise: the sender's oplog no longer holds the binding"
    );
    let (j, _dj) = engine();
    pull(&j, &s.sender);
    let served = j.resolve(DB, "orders").map(|resolved| resolved.physical);
    assert_eq!(
        served.map_err(|e| e.to_string()),
        Ok(STAGE1.to_string()),
        "the joined member serves the stage"
    );
    assert_eq!(
        the_events_of_orders(&j),
        vec![(s.bind.stamp, bind_event())],
        "and its register holds exactly the sender's events"
    );
    assert_eq!(
        j.register_carried_drops_deferred(),
        0,
        "no carried burial was deferred: the sender no longer lists what the register owns"
    );
}

#[test]
fn an_event_above_the_grant_is_not_taken_and_one_at_it_is() {
    let s = swap(&[]);
    let (j, _dj) = engine();
    let origin = s.a.origin();
    let above = |page: &crate::SnapshotPage| {
        Stamp::new(page.versions.get(origin).successor(), s.a.node_id(), s.a.epoch())
    };
    let patch = |page: &mut crate::SnapshotPage| {
        let stamp = above(page);
        if let Some(carry) = &mut page.register {
            carry.events.push(an_event_row(&stamp, &kimmy_core::RegisterEvent::LogicalDrop));
        }
    };
    let (_, result) = pull_patched(&j, &s.sender, &patch);
    result.expect("the pull applies");
    let events = the_events_of_orders(&j);
    assert_eq!(events.len(), 1, "the event above the grant is not taken (it arrives by windows)");
    assert_eq!(events[0].0, s.bind.stamp, "and the one at the grant, the binding, is");
}

#[test]
fn a_senders_parked_event_is_not_on_the_page_and_never_takes_effect() {
    let s = swap(&[]);
    let (parked_here, _dp) = engine();
    parked_here.apply_batch(std::slice::from_ref(&s.creation)).unwrap();
    parked_here.apply_batch(std::slice::from_ref(&s.bind)).unwrap();
    assert!(
        the_binding_is_parked_only(&parked_here),
        "premise: the sender holds the binding only ahead of its position"
    );
    let page = parked_here.snapshot_page(None, None).unwrap();
    let carry = page.register.clone().expect("the last page carries the register");
    assert!(carry.events.is_empty(), "a parked event is not carried");
    assert!(!carry.stages.is_empty(), "though the stage's row is");
    let (j, _dj) = engine();
    pull(&j, &parked_here);
    assert!(!the_binding_is_parked_only(&j), "nothing of it is parked on the joined member");
    assert!(the_events_of_orders(&j).is_empty(), "and nothing of it is in effect there");
}

#[test]
fn a_binding_of_a_stage_the_page_does_not_know_fails_the_page_and_moves_nothing() {
    let s = swap(&[]);
    let (j, _dj) = engine();
    let before = j.witnessed_vector().unwrap();
    let patch = |page: &mut crate::SnapshotPage| {
        let earlier = Hlc::new(s.bind.stamp.hlc.wall_ms - 1, 0);
        let stamp = Stamp::new(earlier, s.a.node_id(), s.a.epoch());
        let unknown = kimmy_core::RegisterEvent::Bind { physical: "orders.__stg.zz".into() };
        if let Some(carry) = &mut page.register {
            carry.events.push(an_event_row(&stamp, &unknown));
        }
    };
    let (progress, result) = pull_patched(&j, &s.sender, &patch);
    assert!(result.is_err(), "a binding of a stage no row names fails the page");
    assert!(!progress.is_complete(), "the progress did not move to complete");
    assert!(the_events_of_orders(&j).is_empty(), "nothing of the register was applied");
    assert_eq!(j.witnessed_vector().unwrap(), before, "and the grant was not absorbed");
}

#[test]
fn a_binding_above_its_stages_deadline_fails_the_page() {
    let s = swap(&[]);
    let (j, _dj) = engine();
    let deadline = s.sender.get_collection(DB, STAGE1).unwrap().stage.unwrap().info.deadline;
    let late = Stamp::new(Hlc::new(deadline.wall_ms + 1, 0), s.a.node_id(), s.a.epoch());
    let patch = |page: &mut crate::SnapshotPage| {
        page.versions.observe(late);
        if let Some(carry) = &mut page.register {
            carry.events.push(an_event_row(&late, &bind_event()));
        }
    };
    let (_, result) = pull_patched(&j, &s.sender, &patch);
    assert!(result.is_err(), "a binding above its stage's deadline fails the page");
    assert!(the_events_of_orders(&j).is_empty(), "and nothing of the register was applied");
}

#[test]
fn a_join_buries_what_the_binding_binds_out_and_counts_no_swap() {
    let s = swap(&[]);
    let (j, _dj) = with_orders();
    pull(&j, &s.sender);
    assert_eq!(
        j.out_swaps(),
        0,
        "a join counts no swap: this member never served what it replaced"
    );
    let buried = j.out_buried().into_iter().find(|(cause, _)| *cause == "bound_out");
    assert!(buried.is_some_and(|(_, n)| n >= 1), "but the burial happened here, and is counted");
    assert!(
        j.get_collection(DB, "orders").is_err(),
        "the identity the binding binds out is buried"
    );
}

#[test]
fn a_name_the_receiver_cannot_name_has_its_rows_stored_by_id_and_nothing_else() {
    let s = swap(&[]);
    let (j, _dj) = engine();
    let ghost = CollectionId::derive(DB, "ghost");
    let stamp = Stamp::new(s.bind.stamp.hlc.predecessor(), s.a.node_id(), s.a.epoch());
    let patch = |page: &mut crate::SnapshotPage| {
        let mut key = ghost.0.to_be_bytes().to_vec();
        key.extend_from_slice(&stamp.to_key_bytes());
        let row = serde_bytes::ByteBuf::from(kimmy_core::RegisterEvent::LogicalDrop.encode());
        if let Some(carry) = &mut page.register {
            carry.events.push((serde_bytes::ByteBuf::from(key), row));
        }
    };
    let (_, result) = pull_patched(&j, &s.sender, &patch);
    result.expect("the pull applies");
    use redb::ReadableDatabase;
    let txn = j.db().begin_read().unwrap();
    let stored = crate::register::has_events(&txn, DB, "ghost").unwrap();
    assert!(stored, "the rows of a name no stage row names are stored by id");
}

#[test]
fn a_post_drop_row_is_taken_only_for_a_name_that_reads_dropped() {
    let s = swap(&[]);
    let (j, _dj) = engine();
    let patch = |page: &mut crate::SnapshotPage| {
        let row = serde_json::to_vec(&crate::register::PostRow::default()).unwrap();
        let key = CollectionId::derive(DB, "orders").0.to_be_bytes().to_vec();
        if let Some(carry) = &mut page.register {
            carry.posts.push((serde_bytes::ByteBuf::from(key), serde_bytes::ByteBuf::from(row)));
        }
    };
    let (_, result) = pull_patched(&j, &s.sender, &patch);
    result.expect("the pull applies");
    let row = crate::register::post_row_for_tests(&j, DB, "orders").unwrap();
    assert!(row.is_none(), "a post-drop row beside a visible physical is not taken");
}

#[test]
fn the_receivers_own_tombstone_seeds_a_drop_only_above_the_grant() {
    for (above, seeded) in [(true, 1usize), (false, 0usize)] {
        let s = swap(&[]);
        let (j, _dj) = engine();
        let id = CollectionId::derive(DB, "orders");
        let stamp = if above {
            Stamp::new(Hlc::new(s.bind.stamp.hlc.wall_ms + 5, 0), j.node_id(), j.epoch())
        } else {
            Stamp::new(s.bind.stamp.hlc.predecessor(), s.a.node_id(), s.a.epoch())
        };
        j.record_collection_drops(&[(id, stamp)]).unwrap();
        pull(&j, &s.sender);
        let drops = the_events_of_orders(&j)
            .iter()
            .filter(|(_, event)| matches!(event, kimmy_core::RegisterEvent::LogicalDrop))
            .count();
        assert_eq!(
            drops, seeded,
            "a tombstone is seeded as a drop only when it is above the grant"
        );
    }
}

// -- Unit C2b-2: a drop the page carries below the stage a stale receiver already serves (D2) -----

/// How many entry rows the index `name` has under `physical`, counted by the collection's id and
/// the index's id, whether or not the physical still holds the definition (a count through the
/// definition answers 0 for an index that is gone).
fn index_rows(m: &Engine, physical: &str, name: &str) -> usize {
    let coll = m.get_collection(DB, physical).unwrap();
    crate::index::scan_range(
        m,
        WalkScope::Request,
        coll.id,
        kimmy_core::IndexMeta::derive_id(name),
        &[],
        None,
        crate::index::Unkeyed::Include,
    )
    .unwrap()
    .len()
}

/// An index on `physical` made by hand at `wall_ms`, by the origin of `by`: through the restored-
/// definition door, which asks nothing of the register, and the rows of the documents already there
/// are built under it.
fn index_by_hand(m: &Engine, by: &Engine, physical: &str, name: &str, wall_ms: u64) {
    let created = Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch());
    m.create_index_inner(
        DB,
        physical,
        vec![kimmy_core::IndexField::ascending("t")],
        false,
        kimmy_core::Enforcement::Local,
        Some(name.into()),
        None,
        None,
        crate::index::CreateOrigin::Restored(created),
        &|_, _| Ok(false),
    )
    .unwrap();
}

#[test]
fn a_drop_the_page_carries_below_the_stage_a_stale_receiver_serves_floors_what_the_stage_holds() {
    // The receiver applied the stage's creation and the binding and serves the stage; it never
    // heard of the two logical drops that the sender's register holds below the binding. Each index
    // it holds on the stage was made at a stamp: `y` below both drops, `x` between them, `z` above
    // both. The page's drops take effect in the receiver's last-page transaction: nothing buries
    // the stage, so the pass is told the NEWEST drop it stored and floors the stage there, and `y`
    // and `x` go with their entry rows while `z` stays.
    let (j, _dj) = with_orders();
    let s = swap(&[&j]);
    j.apply_batch(std::slice::from_ref(&s.creation)).unwrap();
    window(&j, std::slice::from_ref(&s.bind));
    assert_eq!(j.resolve(DB, "orders").unwrap().physical, STAGE1, "premise: the receiver serves");
    let stage = j.get_collection(DB, STAGE1).unwrap();
    j.insert(&stage, doc! { "_id": 1, "t": 1 }).unwrap();
    let wall = s.bind.stamp.hlc.wall_ms;
    for (name, below_bind_by) in [("y", 5), ("x", 3), ("z", 1)] {
        index_by_hand(&j, &s.a, STAGE1, name, wall - below_bind_by);
        assert_eq!(index_rows(&j, STAGE1, name), 1, "premise: {name} is on the stage with a row");
    }
    let first = Stamp::new(Hlc::new(wall - 4, 0), s.a.node_id(), s.a.epoch());
    let second = Stamp::new(Hlc::new(wall - 2, 0), s.a.node_id(), s.a.epoch());
    let patch = |page: &mut crate::SnapshotPage| {
        if let Some(carry) = &mut page.register {
            for drop in [first, second] {
                carry.events.push(an_event_row(&drop, &kimmy_core::RegisterEvent::LogicalDrop));
            }
        }
    };
    let (progress, result) = pull_patched(&j, &s.sender, &patch);
    result.expect("the pull applies");
    assert!(progress.is_complete(), "the pull completes");
    assert_eq!(
        the_events_of_orders(&j).iter().map(|(stamp, _)| *stamp).collect::<Vec<_>>(),
        vec![first, second, s.bind.stamp],
        "premise: the receiver holds both drops, below the binding it already had"
    );
    assert_eq!(j.resolve(DB, "orders").unwrap().physical, STAGE1, "the stage still serves");
    let stage = j.get_collection(DB, STAGE1).unwrap();
    assert!(stage.index("y").is_none(), "y, below both drops, is not on the stage");
    assert!(stage.index("x").is_none(), "x, below the newest drop, is not on the stage");
    assert!(stage.index("z").is_some(), "z, above both, is");
    assert_eq!(index_rows(&j, STAGE1, "y"), 0, "no entry row of y is left under the stage's id");
    assert_eq!(index_rows(&j, STAGE1, "x"), 0, "no entry row of x is left under the stage's id");
    assert_eq!(index_rows(&j, STAGE1, "z"), 1, "z keeps its row");
}
