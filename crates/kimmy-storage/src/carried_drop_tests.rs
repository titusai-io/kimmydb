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
    swap_over(joiners, &|_| {})
}

/// [`swap`] over a sender that `before_the_swap` has written to first (an index made on `orders`, say): the binding is stamped above it.
fn swap_over(joiners: &[&Engine], before_the_swap: &dyn Fn(&Engine)) -> Swap {
    let (a, da) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE1, MergePolicy::Document, deadline).unwrap();
    let creation = creation_of(&a, STAGE1);
    let (sender, ds) = with_orders();
    before_the_swap(&sender);
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

/// `page` with the tombstone it carries for `stage` moved one tick below the stamp the sender holds it at, and that stamp: so that a
/// tombstone this member records under the stage's id can be told from the sender's, whatever the sender's stamp and this member's own
/// judgement (the loser is buried at the binding's stamp, on both) have in common.
fn with_the_stage_tombstone_one_tick_lower(
    page: &crate::SnapshotPage,
    stage: CollectionId,
) -> (crate::SnapshotPage, Stamp) {
    let mut lowered = page.clone();
    let entry = lowered.dropped_collections.iter_mut().find(|(id, _)| *id == stage);
    let (_, stamp) = entry.expect("premise: the page carries the stage's tombstone");
    stamp.hlc = stamp.hlc.predecessor();
    let carried = *stamp;
    (lowered, carried)
}

/// What a tombstone under the stage's id may be after the join (Q6.6, unit D): none, or this member's OWN, the loser rule asked of its own
/// events (a stage that no later binding keeps), recorded at the stamp of the event visible here, and never the stamp the sender carried.
/// The register still decides the burial; it now decides it from this member's events, and records its own tombstone.
fn assert_the_tombstone_is_this_members_own(
    j: &Engine,
    stage: CollectionId,
    carried: Stamp,
    when: &str,
) {
    let visible = j.resolve(DB, "orders").unwrap().visible;
    assert!(visible.is_some(), "premise: a binding is visible here ({when})");
    assert_ne!(
        j.collection_dropped_at(stage).unwrap(),
        Some(carried),
        "the sender's carried stamp is not recorded under the stage's id ({when})"
    );
    assert_eq!(
        j.collection_dropped_at(stage).unwrap(),
        visible,
        "any tombstone under the stage's id is this member's own, at the stamp of the event visible here ({when})"
    );
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
    let (page, carried) = with_the_stage_tombstone_one_tick_lower(&k.page, stage1);
    k.j.apply_snapshot_page(k.s.sender.node_id(), &mut SnapshotProgress::whole_database(), &page)
        .unwrap();
    assert_the_tombstone_is_this_members_own(&k.j, stage1, carried, "the register decides");
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
    // the page of a sender before the register's carry (as `join` makes it): nothing of this member's own tests runs, so no tombstone is
    // ever recorded and each delivery meets the carried burial again, which is deferred and counted again
    let k = a_page_that_buries_a_stage_the_receiver_knows();
    let stage1 = CollectionId::derive(DB, STAGE1);
    let mut page = k.page.clone();
    page.register = None;
    for delivery in 1..=2u64 {
        k.j.apply_snapshot_page(
            k.s.sender.node_id(),
            &mut SnapshotProgress::whole_database(),
            &page,
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
fn a_page_with_the_carry_delivered_twice_defers_once_and_keeps_the_members_own_tombstone() {
    // With the carry, delivery 1 defers the carried burial and the receiver's own tests bury the stage (unit D): its own tombstone is
    // recorded at the stamp of its visible event. On delivery 2 the carried burial is not above that tombstone, and a drop at or below
    // the one held is skipped before the register is asked (`apply_carried_drops`), so it is not deferred: counting a no-op as deferred
    // would be wrong.
    let k = a_page_that_buries_a_stage_the_receiver_knows();
    let stage1 = CollectionId::derive(DB, STAGE1);
    let (page, carried) = with_the_stage_tombstone_one_tick_lower(&k.page, stage1);
    for delivery in 1..=2u64 {
        k.j.apply_snapshot_page(
            k.s.sender.node_id(),
            &mut SnapshotProgress::whole_database(),
            &page,
        )
        .unwrap();
        assert_eq!(
            k.j.register_carried_drops_deferred(),
            1,
            "deferred once, by the first delivery, and not again by the second (delivery {delivery})"
        );
        let when = format!("delivery {delivery}");
        assert_the_tombstone_is_this_members_own(&k.j, stage1, carried, &when);
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
    assert_eq!(
        joiner.snapshot_documents_skipped(),
        vec![("true", 0), ("false", 0)],
        "no document of the pull was skipped for want of its collection"
    );
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
fn a_join_buries_the_identity_the_binding_binds_out_whether_or_not_it_is_counted() {
    // The state, asserted alone: the test above reads the counter first, and a mutant that skips the burial dies there before it
    // reaches the identity. Here nothing is read before the identity.
    let s = swap(&[]);
    let (j, _dj) = with_orders();
    assert!(j.get_collection(DB, "orders").is_ok(), "premise: the joiner holds the identity");
    pull(&j, &s.sender);
    assert_eq!(served_by(&j), Ok(STAGE1.to_string()), "premise: the stage serves the name");
    assert!(
        j.get_collection(DB, "orders").is_err(),
        "the identity the binding binds out is buried by the join"
    );
}

// This is the case that no real member is in: every name that has an event also has a stage row (a binding needs its stage's, and stage
// rows outlive their collections and ride every page and backup), so a name can be named whenever it has events. The resolve row's
// property test keeps it as an explicit operation (`an event with no stage row`) so that the answers stay proven right if that is wrong.
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

// A stage row the carry holds is named by the shape check `stage_row_names` makes (a restore has a loop of its own that refuses a damaged
// row first, so only a page reaches this one). A row whose key is not eight bytes, or whose value ends before its names, fails the page by
// name, and nothing of it is applied.

/// What a test makes of a carried stage row: the key and the value that take the place of the row's own.
type ReShape = dyn Fn(&[u8], &[u8]) -> (Vec<u8>, Vec<u8>);

/// A pull of the swap's sender into a fresh member, the carry's first stage row replaced by what `shape` makes of its key and value. Asserts
/// the premises (the sender's snapshot is one page; the unpatched row is of the right shape and the unpatched pull applies; the patch ran and
/// changed the row) and that the page failed with the carry's transaction moved nothing; returns the text of its error. The stage row the
/// receiver holds afterwards is the one its page's own definition made, in the collection's transaction before the carry's (an earlier
/// transaction of the same page): it equals the row the unpatched pull took.
fn the_error_of_a_carried_stage_row_shaped(shape: &ReShape) -> String {
    let s = swap(&[]);
    assert!(
        s.sender.snapshot_page(None, None).unwrap().next.is_none(),
        "premise: the sender's snapshot is one page, so the definitions and the carry ride the same page"
    );
    let (whole, _dw) = engine();
    pull(&whole, &s.sender);
    assert!(
        holds_a_stage_row(&whole, CollectionId::derive(DB, STAGE1)),
        "premise: a pull that is not patched takes the stage row"
    );
    let (j, _dj) = engine();
    let before = j.witnessed_vector().unwrap();
    let patched = std::cell::Cell::new(false);
    let patch = |page: &mut crate::SnapshotPage| {
        if let Some(carry) = &mut page.register {
            let (key, value) = carry.stages[0].clone();
            assert!(key.len() == 8, "premise: the row the carry holds has a key of eight bytes");
            let (key2, value2) = shape(&key, &value);
            assert!(
                (key2.as_slice(), value2.as_slice()) != (key.as_slice(), value.as_slice()),
                "premise: the row was changed"
            );
            carry.stages[0] =
                (serde_bytes::ByteBuf::from(key2), serde_bytes::ByteBuf::from(value2));
            patched.set(true);
        }
    };
    let (progress, result) = pull_patched(&j, &s.sender, &patch);
    assert!(patched.get(), "premise: the carry's page was patched");
    let error = result.expect_err("a stage row of the wrong shape fails the page").to_string();
    assert!(!progress.is_complete(), "the progress did not move to complete");
    assert_eq!(
        the_stage_row_of(&j, STAGE1),
        the_stage_row_of(&whole, STAGE1),
        "the stage row held is the one the page's definition made, equal to the sender's, and not the damaged one"
    );
    assert!(the_events_of_orders(&j).is_empty(), "nothing of the register was applied");
    assert_eq!(j.witnessed_vector().unwrap(), before, "and the grant was not absorbed");
    error
}

#[test]
fn a_carried_stage_row_whose_key_is_not_eight_bytes_fails_the_page_by_name() {
    let error =
        the_error_of_a_carried_stage_row_shaped(&|key, value| (key[..7].to_vec(), value.to_vec()));
    assert!(
        error.contains("a stage row of the wrong shape"),
        "refused by the shape check: {error}"
    );
}

#[test]
fn a_carried_stage_row_whose_value_ends_before_its_names_fails_the_page_by_name() {
    let error = the_error_of_a_carried_stage_row_shaped(&|key, value| {
        (key.to_vec(), value[..value.len().min(40)].to_vec())
    });
    assert!(
        error.contains("a stage row of the wrong shape"),
        "refused by the shape check: {error}"
    );
}

/// A post-drop row of `orders` holding one index, `carried_ix`, made at `made`.
fn a_post_row_with_an_index(made: Stamp) -> crate::register::PostRow {
    let index = kimmy_core::IndexMeta {
        id: kimmy_core::IndexMeta::derive_id("carried_ix"),
        name: "carried_ix".into(),
        fields: vec![kimmy_core::IndexField::ascending("t")],
        unique: false,
        enforcement: kimmy_core::Enforcement::Local,
        multikey: false,
        expire_after_secs: None,
        partial_filter: None,
        created: Some(made),
    };
    crate::register::PostRow { indexes: vec![index], ..Default::default() }
}

#[test]
fn a_post_drop_row_is_taken_only_for_a_name_that_reads_dropped() {
    // The row's definitions are what the guard protects: a row taken beside a visible physical is joined into it by the settle pass and
    // deleted in the same transaction, so the absence of the row proves nothing. The stage the name serves must not hold its index.
    let s = swap(&[]);
    let (j, _dj) = engine();
    let made = Stamp::new(Hlc::new(s.bind.stamp.hlc.wall_ms - 1, 0), s.a.node_id(), s.a.epoch());
    let carried = a_post_row_with_an_index(made);
    assert!(
        s.sender.get_collection(DB, STAGE1).unwrap().index("carried_ix").is_none(),
        "premise: the sender's stage does not hold the index, so only the post-drop row has it"
    );
    assert!(j.get_collection(DB, STAGE1).is_err(), "premise: the joiner holds no stage yet");
    let patched = std::cell::Cell::new(false);
    let patch = |page: &mut crate::SnapshotPage| {
        let row = serde_json::to_vec(&carried).unwrap();
        let key = CollectionId::derive(DB, "orders").0.to_be_bytes().to_vec();
        if let Some(carry) = &mut page.register {
            carry.posts.push((serde_bytes::ByteBuf::from(key), serde_bytes::ByteBuf::from(row)));
            patched.set(true);
        }
    };
    let (_, result) = pull_patched(&j, &s.sender, &patch);
    result.expect("the pull applies");
    assert!(
        patched.get(),
        "premise: the last page carried the register and the patch pushed the row"
    );
    assert_eq!(served_by(&j), Ok(STAGE1.to_string()), "premise: the stage serves the name");
    assert!(j.get_collection(DB, STAGE1).is_ok(), "premise: the member holds the stage it serves");
    let row = crate::register::post_row_for_tests(&j, DB, "orders").unwrap();
    assert!(row.is_none(), "a post-drop row beside a visible physical is not taken");
    assert!(
        j.get_collection(DB, STAGE1).unwrap().index("carried_ix").is_none(),
        "and its definitions did not join the stage the name serves"
    );
}

#[test]
fn a_post_drop_row_is_taken_for_a_name_that_reads_dropped() {
    // The page carries a logical drop above the binding and a post-drop row holding an index made
    // after the drop (both at or below the grant): the name reads dropped on the receiver, so the
    // row is taken. The burial of the stage writes a post-drop row of its own in the same
    // transaction, so the row's CONTENT shows that the carried one was taken: the index the stage
    // never held.
    let s = swap(&[]);
    let (j, _dj) = engine();
    let wall = s.bind.stamp.hlc.wall_ms;
    let at = |ms: u64| Stamp::new(Hlc::new(wall + ms, 0), s.a.node_id(), s.a.epoch());
    let (dropped, made) = (at(5), at(9));
    let carried = a_post_row_with_an_index(made);
    let patched = std::cell::Cell::new(false);
    let patch = |page: &mut crate::SnapshotPage| {
        page.versions.observe(made);
        if let Some(carry) = &mut page.register {
            carry.events.push(an_event_row(&dropped, &kimmy_core::RegisterEvent::LogicalDrop));
            let key = CollectionId::derive(DB, "orders").0.to_be_bytes().to_vec();
            let row = serde_json::to_vec(&carried).unwrap();
            carry.posts.push((serde_bytes::ByteBuf::from(key), serde_bytes::ByteBuf::from(row)));
            patched.set(true);
        }
    };
    let (progress, result) = pull_patched(&j, &s.sender, &patch);
    result.expect("the pull applies");
    assert!(progress.is_complete(), "the pull completes");
    assert!(
        patched.get(),
        "premise: the last page carried the register and the patch pushed the drop and the row"
    );
    assert!(j.resolve(DB, "orders").is_err(), "premise: the name reads dropped on the receiver");
    let row = crate::register::post_row_for_tests(&j, DB, "orders").unwrap();
    assert!(
        row.is_some_and(|row| row.indexes.iter().any(|i| i.name == "carried_ix")),
        "a post-drop row for a name that reads dropped is taken, with its index"
    );
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

#[test]
fn a_binding_the_joiner_held_parked_takes_effect_in_the_page_that_covers_it_though_the_carry_adds_nothing()
 {
    // The joiner serves stage 1 and holds the second binding only as information (its stage row is
    // held, the binding is ahead of the joiner's position). The page's grant covers the binding,
    // and the carry's events for the name are all held already, so the carry changes nothing and
    // names nothing: the grant's release of the parked binding is what puts it in effect, and the
    // settle pass that release runs is what buries stage 1, in the page's own transaction.
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
    j.apply_batch(std::slice::from_ref(&creation2)).unwrap();
    j.apply_batch(std::slice::from_ref(&bind2)).unwrap();
    s.sender.apply_batch(std::slice::from_ref(&creation2)).unwrap();
    window(&s.sender, std::slice::from_ref(&bind2));
    assert_eq!(served_by(&s.sender), Ok(STAGE2.to_string()), "premise: the sender went on");
    let held: Vec<Stamp> = the_events_of_orders(&j).iter().map(|(stamp, _)| *stamp).collect();
    assert_eq!(held, vec![s.bind.stamp], "premise: the joiner holds only the first binding");
    assert_eq!(
        served_by(&j),
        Ok(STAGE1.to_string()),
        "premise: the second binding is not in effect"
    );
    let parked = {
        use redb::ReadableDatabase;
        let txn = j.db().begin_read().unwrap();
        crate::register::parked_read(&txn, DB, "orders").unwrap().events().len()
    };
    assert_eq!(parked, 1, "premise: the second binding is held as information only");
    assert!(j.get_collection(DB, STAGE2).is_ok(), "premise: the joiner holds the second stage");
    assert!(j.get_collection(DB, STAGE1).is_ok(), "premise: the joiner holds stage 1");
    let kept = std::cell::Cell::new(0usize);
    let patch = |page: &mut crate::SnapshotPage| {
        if let Some(carry) = &mut page.register {
            carry
                .events
                .retain(|(key, _)| held.iter().any(|stamp| key[8..] == stamp.to_key_bytes()));
            kept.set(carry.events.len());
        }
    };
    let (progress, result) = pull_patched(&j, &s.sender, &patch);
    result.expect("the pull applies");
    assert!(progress.is_complete(), "the pull completes");
    assert_eq!(kept.get(), 1, "premise: the carry's events for the name are the one already held");
    assert_eq!(served_by(&j), Ok(STAGE2.to_string()), "the name answers from the second stage");
    assert!(j.get_collection(DB, STAGE2).is_ok(), "and the member holds the physical it serves");
    assert!(
        j.get_collection(DB, STAGE1).is_err(),
        "the stage the second binding replaced is buried in the page's own transaction"
    );
}

// -- Unit C2c: a restored index is logged under the logical name; a document with no collection is counted when it is skipped --------------

/// The `CreateIndex` entry at `stamp` in `m`'s oplog.
fn the_index_entry_at(m: &Engine, stamp: &Stamp) -> Option<OplogEntry> {
    everything(m).into_iter().find(|e| e.kind == OpKind::CreateIndex && e.stamp == *stamp)
}

/// The sender's own `CreateIndex` entry of the index `x` made on `orders` before the swap.
fn the_index_entry(m: &Engine) -> OplogEntry {
    everything(m)
        .into_iter()
        .find(|e| e.kind == OpKind::CreateIndex)
        .expect("premise: the sender logged the creation of its index")
}

fn make_x_on_orders(sender: &Engine) {
    sender
        .create_index(
            DB,
            "orders",
            vec![kimmy_core::IndexField::ascending("v")],
            false,
            Some("x".into()),
        )
        .unwrap();
}

#[test]
fn the_index_of_a_stage_a_page_restored_is_logged_under_the_logical_name_as_the_origin_logged_it() {
    // The index the client made on `orders` is copied to the stage at the flip, and a member that joins builds it on the stage from the
    // page: the entry it logs for it is the entry at the index's stamp, which must be the bytes the origin logged (addressed to
    // `orders`), not an entry addressed to the stage, which a peer that never held the stage cannot place.
    let s = swap_over(&[], &make_x_on_orders);
    let original = the_index_entry(&s.sender);
    assert_eq!(
        original.collection,
        CollectionId::derive(DB, "orders"),
        "premise: the origin's entry is addressed to the logical name"
    );
    assert!(
        s.sender.get_collection(DB, STAGE1).unwrap().index("x").is_some(),
        "premise: the sender's stage holds the index"
    );
    let (j, _dj) = engine();
    pull(&j, &s.sender);
    assert!(
        j.get_collection(DB, STAGE1).unwrap().index("x").is_some(),
        "premise: the joined member built the index on the stage"
    );
    let joined = the_index_entry_at(&j, &original.stamp)
        .expect("premise: the joined member logged an entry at the index's stamp");
    assert_eq!(
        joined.collection, original.collection,
        "the entry is addressed to the logical name"
    );
    assert_eq!(joined.body, original.body, "and its body is the origin's, byte for byte");
}

#[test]
fn the_index_of_a_recreation_a_page_restored_is_logged_under_the_logical_name_too() {
    // The same for a recreation: the name was swapped, dropped and made again, and the index was made on the name after that. The origin
    // logged it for `orders`; the member that joins builds it on the recreation, whose own name carries a stamp, and logs the same entry.
    let s = swap(&[]);
    assert!(
        s.sender.drop_collection(DB, "orders").unwrap(),
        "premise: the swapped name is dropped"
    );
    s.sender.create_collection(DB, "orders").unwrap();
    let recreation = s.sender.resolve(DB, "orders").unwrap().physical;
    assert!(
        kimmy_core::is_recreation_name(&recreation),
        "premise: a recreation holds the name on the sender"
    );
    make_x_on_orders(&s.sender);
    let original = the_index_entry(&s.sender);
    assert_eq!(
        original.collection,
        CollectionId::derive(DB, "orders"),
        "premise: the origin's entry is addressed to the logical name"
    );
    let (j, _dj) = engine();
    pull(&j, &s.sender);
    assert!(
        j.get_collection(DB, &recreation).unwrap().index("x").is_some(),
        "premise: the joined member built the index on the recreation"
    );
    let joined = the_index_entry_at(&j, &original.stamp)
        .expect("premise: the joined member logged an entry at the index's stamp");
    assert_eq!(
        joined.collection, original.collection,
        "the entry is addressed to the logical name"
    );
    assert_eq!(joined.body, original.body, "and its body is the origin's, byte for byte");
}

#[test]
fn a_peer_that_never_held_the_stage_takes_the_joined_members_index_entry_without_stopping() {
    let s = swap_over(&[], &make_x_on_orders);
    let stamp = the_index_entry(&s.sender).stamp;
    let (j, _dj) = engine();
    pull(&j, &s.sender);
    let joined = the_index_entry_at(&j, &stamp)
        .expect("premise: the joined member logged an entry at the index's stamp");
    let (k, _dk) = engine();
    k.create_collection(DB, "orders").unwrap();
    assert!(k.get_collection(DB, STAGE1).is_err(), "premise: the peer never held the stage");
    let outcome = window(&k, std::slice::from_ref(&joined));
    assert!(
        !outcome.stopped() && outcome.unknown_collection == 0,
        "the entry does not address a physical the peer cannot place"
    );
    assert!(
        k.get_collection(DB, "orders").unwrap().index("x").is_some(),
        "and the peer took the index on the name it holds"
    );
}

#[test]
fn the_restored_index_of_an_ordinary_collection_is_logged_as_the_origin_logged_it() {
    let (a, _da) = with_orders();
    make_x_on_orders(&a);
    let original = the_index_entry(&a);
    let (j, _dj) = engine();
    pull(&j, &a);
    assert!(
        j.get_collection(DB, "orders").unwrap().index("x").is_some(),
        "premise: the joined member built the index"
    );
    let joined = the_index_entry_at(&j, &original.stamp)
        .expect("premise: the joined member logged an entry at the index's stamp");
    assert_eq!(joined.collection, original.collection, "the entry is addressed to the collection");
    assert_eq!(joined.body, original.body, "and its body is the origin's, byte for byte");
}

/// A pull of a sender's one collection whose definition the page leaves out, so that its documents have no collection here and no
/// tombstone to say why. With `covered` the page's vector covers their stamps (the grant), without it nothing does.
fn pull_documents_that_have_no_collection(covered: bool) -> (Engine, tempfile::TempDir) {
    let (sender, _ds) = with_orders();
    let (j, dj) = engine();
    let patched = std::cell::Cell::new(0usize);
    let patch = |page: &mut crate::SnapshotPage| {
        patched.set(patched.get() + page.documents.len());
        page.collections.retain(|state| state.name != "orders");
        if !covered {
            page.versions = VersionVector::default();
        }
    };
    let (progress, result) = pull_patched(&j, &sender, &patch);
    result.expect("the page applies: a skipped document does not fail it");
    assert!(progress.is_complete(), "the pull completes");
    assert!(patched.get() > 0, "premise: the page carried documents");
    assert!(j.get_collection(DB, "orders").is_err(), "premise: no collection was made for them");
    assert_eq!(
        j.collection_dropped_at(CollectionId::derive(DB, "orders")).unwrap(),
        None,
        "premise: and no tombstone says it is gone"
    );
    (j, dj)
}

#[test]
fn a_document_with_no_collection_that_the_grant_covers_is_counted_true_and_the_page_applies() {
    let (j, _dj) = pull_documents_that_have_no_collection(true);
    assert_eq!(j.snapshot_documents_skipped(), vec![("true", 1), ("false", 0)]);
}

#[test]
fn a_document_with_no_collection_that_the_grant_does_not_cover_is_counted_false_and_the_page_applies()
 {
    let (j, _dj) = pull_documents_that_have_no_collection(false);
    assert_eq!(j.snapshot_documents_skipped(), vec![("true", 0), ("false", 1)]);
}

// -- Unit C2a, the stage's stamp in the seed: an identity that stands as the remains of the drop ----------------------------------------------

/// A `DropCollection` of `orders` stamped at `wall_ms` by `by`.
fn drop_of_orders(by: &Engine, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        kind: OpKind::DropCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(bson::serialize_to_vec(&kimmy_core::CollectionRef::new(DB, "orders")).unwrap()),
        meta: None,
    }
}

/// The creation of `orders` and one write stamped a few milliseconds above it, both made on a third member.
fn a_life_and_a_later_write() -> (Vec<OplogEntry>, tempfile::TempDir) {
    let (s, d) = engine();
    let orders = s.create_collection(DB, "orders").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    s.insert(&orders, doc! { "_id": 1, "v": "after the drop" }).unwrap();
    (everything(&s), d)
}

#[test]
fn a_stage_a_page_makes_is_judged_by_its_creation_against_the_drop_the_identity_stands_as_the_remains_of()
 {
    // The seed reads the stage's creation only when the identity STANDS as the remains of the drop (a write stamped above the drop
    // kept it, and the survivors path re-dated it): the swap began below the drop, and the drop is the name's, or it began above it,
    // and the drop was an ordinary one of an earlier life. (a) the stage is created below the drop: the seed writes the logical drop
    // and the remains are ended in the page's transaction. (b) above it: nothing is seeded and the identity goes on serving.
    for (stage_below_the_drop, seeded) in [(true, 1usize), (false, 0usize)] {
        let (s, life, _ds) = if stage_below_the_drop {
            let s = swap(&[]);
            let (life, ds) = a_life_and_a_later_write();
            (s, life, ds)
        } else {
            let (life, ds) = a_life_and_a_later_write();
            (swap(&[]), life, ds)
        };
        let created = life
            .iter()
            .find(|e| e.kind == OpKind::CreateCollection)
            .expect("the life's creation")
            .stamp
            .hlc
            .wall_ms;
        let drop = drop_of_orders(&s.a, created + 1);
        let write = life.iter().find(|e| e.kind == OpKind::Insert).expect("the write");
        assert!(write.stamp.hlc > drop.stamp.hlc, "premise: the write is stamped after the drop");
        assert_eq!(
            s.creation.stamp < drop.stamp,
            stage_below_the_drop,
            "premise: the stage was created below the drop in case (a) and above it in case (b)"
        );
        let (m, _dm) = engine();
        window(&m, &life);
        window(&m, std::slice::from_ref(&drop));
        let kept = m.get_collection(DB, "orders").expect("premise: the identity stands");
        assert!(
            kept.is_redated(),
            "premise: the drop that the write survived re-dated the identity"
        );
        assert_eq!(
            kept.created,
            drop.stamp.hlc.successor(),
            "premise: its creation is the successor of the drop"
        );
        assert_eq!(the_logical_drops_of_orders(&m), 0, "premise: the register holds no drop yet");
        let (id, page, _) = the_page_of_the_stage(&s);
        m.apply_snapshot_page(s.sender.node_id(), &mut SnapshotProgress::of_collection(id), &page)
            .unwrap();
        assert_eq!(
            the_logical_drops_of_orders(&m),
            seeded,
            "the drop is the name's logical drop only when the stage was created below it"
        );
        assert_eq!(
            m.get_collection(DB, "orders").is_ok(),
            seeded == 0,
            "the remains of the drop are ended with the seed, and the identity serves without it"
        );
    }
}

// -- Unit C2d: the carry's measure, where it rides, and its cap ------------------------------------------------------------------------------

/// The name of the plain collection the carry-shape tests use: the first of `plain0`, `plain1`, ... whose id is among the highest the
/// hash gives. The walk visits collections in id order and an early end resumes after the last row it sent, so the collection that
/// holds the row must be the LAST one visited, or the cursor of the early end points at an empty collection's start and a wrong cursor
/// is not seen (the mutant that clears it changes nothing there).
fn plain_name() -> String {
    (0u32..)
        .map(|n| format!("plain{n}"))
        .find(|name| CollectionId::derive(DB, name).0 > u64::MAX - (1 << 52))
        .expect("a name with a high id")
}

fn plain_id() -> CollectionId {
    CollectionId::derive(DB, &plain_name())
}

/// A plain collection holding one document on `sender`: a row the walk sends, whatever the swap did.
fn with_a_plain_document(sender: &Engine) {
    let plain = sender.create_collection(DB, &plain_name()).unwrap();
    sender.insert(&plain, doc! { "_id": 1, "v": "a document the walk sends" }).unwrap();
}

/// A sender with a plain document and as many swaps as it takes for its carry to be larger than the row: the size of that carry, the sender
/// (the page it builds is the whole snapshot) and the last stage it bound. Measured, not computed: a row costs its bytes, the overhead of
/// a row and the cursor the page would cut at it, and the room hook the tests use must hold the row when it holds the carry and not both,
/// so swaps are added (at most 32) until a scoped snapshot of the row alone fits a room one byte under the carry.
fn a_sender_and_the_size_of_its_carry() -> (Swap, usize, String) {
    let s = swap(&[]);
    with_a_plain_document(&s.sender);
    let plain = plain_id();
    let mut last = STAGE1.to_string();
    let mut previous = s.bind.stamp.hlc.wall_ms;
    let mut swaps = 1;
    let size = loop {
        let whole = s.sender.snapshot_page(None, None).unwrap();
        assert_eq!(whole.documents.len(), 1, "premise: the page carries the plain document");
        let carry = whole
            .register
            .expect("premise: the one page that exhausts the walk carries the register");
        let size = carry.encoded_len();
        s.sender.set_snapshot_room_for_test(Some(size - 1));
        let probe = s.sender.snapshot_page(None, Some(plain)).unwrap();
        s.sender.set_snapshot_room_for_test(None);
        if probe.documents.len() == 1 {
            break size;
        }
        assert!(swaps < 32, "premise: {swaps} swaps make the carry larger than the row");
        swaps += 1;
        let stage = format!("orders.__stg.a{swaps}");
        let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
        s.a.create_stage(DB, &stage, MergePolicy::Document, deadline).unwrap();
        let creation = creation_of(&s.a, &stage);
        let wall = creation.stamp.hlc.wall_ms.max(previous) + 1;
        let bind = bind_of(&s.a, &stage, wall);
        s.sender.apply_batch(std::slice::from_ref(&creation)).unwrap();
        window(&s.sender, std::slice::from_ref(&bind));
        assert_eq!(served_by(&s.sender), Ok(stage.clone()), "premise: the sender swapped again");
        previous = wall;
        last = stage;
    };
    let highest = s.sender.all_collections().unwrap().iter().map(|meta| meta.id.0).max();
    assert_eq!(
        highest,
        Some(plain.0),
        "premise: the plain collection is the last one the walk visits"
    );
    (s, size, last)
}

#[test]
fn a_carry_that_fits_rides_the_page_that_walks_the_last_row() {
    let (s, size, _) = a_sender_and_the_size_of_its_carry();
    let page = s.sender.snapshot_page(None, None).unwrap();
    assert_eq!(page.documents.len(), 1, "premise: the page holds the row");
    assert!(page.next.is_none(), "the snapshot ends on the page that walks its last row");
    assert_eq!(
        page.register.as_ref().map(|carry| carry.encoded_len()),
        Some(size),
        "and that page carries the register"
    );
}

#[test]
fn a_carry_that_does_not_fit_beside_the_rows_rides_the_empty_page_after_them() {
    let (s, size, last) = a_sender_and_the_size_of_its_carry();
    // room for the carry alone, and for the row, but not for both
    s.sender.set_snapshot_room_for_test(Some(size + 1));
    let probe = s.sender.snapshot_page(None, Some(plain_id())).unwrap();
    assert_eq!(probe.documents.len(), 1, "premise: the row fits the room");
    let first = s.sender.snapshot_page(None, None).unwrap();
    assert_eq!(first.documents.len(), 1, "premise: the row fits the room");
    assert!(first.register.is_none(), "the carry is not on the page that cannot hold it");
    let cursor = first.next.clone();
    assert!(cursor.is_some(), "the page ends early, after its last row, and the walk goes on");
    let second = s.sender.snapshot_page(cursor, None).unwrap();
    assert!(
        second.documents.is_empty() && second.collections.is_empty(),
        "the carry rides a page with nothing else on it, and no row is sent twice"
    );
    assert_eq!(
        first.documents.len() + second.documents.len(),
        1,
        "the two pages together hold the source's one row: none lost, none sent twice"
    );
    assert!(second.register.is_some() && second.next.is_none(), "which is the last page");
    // and the pull completes with the register applied
    let (j, _dj) = engine();
    pull(&j, &s.sender);
    assert_eq!(
        served_by(&j),
        Ok(last),
        "the joiner took the register: the last stage the sender bound serves"
    );
    assert!(j.get_collection(DB, &plain_name()).is_ok(), "and the row came with it");
}

#[test]
fn a_carry_that_does_not_fit_a_page_of_its_own_is_refused_not_cut() {
    let (s, size, _) = a_sender_and_the_size_of_its_carry();
    s.sender.set_snapshot_room_for_test(Some(size - 1));
    // the same walk of the row alone, on a page with no carry (a scoped one, whose cursor reserves more): it fits the room, so the walk of
    // the whole database exhausts on its page and only the carry, which does not fit a page of its own, can refuse
    let probe = s.sender.snapshot_page(None, Some(plain_id())).unwrap();
    assert_eq!(probe.documents.len(), 1, "premise: the row fits the room");
    match s.sender.snapshot_page(None, None) {
        Err(crate::StorageError::SnapshotRegisterCarryTooLarge { bytes, cap }) => {
            assert_eq!((bytes, cap), (size, size - 1), "the error names the size and the room");
        }
        other => panic!("a carry no page can hold is refused, never cut: {other:?}"),
    }
}

#[test]
fn the_gauge_reads_the_size_of_the_carry_last_built_and_zero_before_any() {
    let (e, _d) = engine();
    assert_eq!(e.snapshot_register_carry_bytes(), 0, "premise: nothing built a carry yet");
    let page = e.snapshot_page(None, None).unwrap();
    let empty = page.register.expect("the last page carries the register").encoded_len();
    assert_eq!(e.snapshot_register_carry_bytes() as usize, empty, "an empty register has a size");
    let (s, size, _) = a_sender_and_the_size_of_its_carry();
    assert_eq!(s.sender.snapshot_register_carry_bytes() as usize, size, "the measure of the swaps");
    assert!(size > empty, "premise: a swap makes the carry larger");
}

#[test]
fn the_warning_fires_at_half_the_cap_and_at_most_once_an_hour() {
    // no document on the sender: with a cap this small a row would end the first page before the carry is reached
    let s = swap(&[]);
    let whole = s.sender.snapshot_page(None, None).unwrap();
    assert!(whole.documents.is_empty(), "premise: the sender has no row to send");
    let size = whole.register.expect("premise: the page carries the register").encoded_len();
    assert_eq!(s.sender.register_carry_warnings(), 0, "premise: no warning so far");
    // a cap four times the carry: half of it is twice the carry
    s.sender.set_snapshot_page_bytes_for_test(Some(4 * size));
    assert!(s.sender.snapshot_page(None, None).is_ok(), "premise: the page is served");
    assert_eq!(s.sender.register_carry_warnings(), 0, "below half the cap: no warning");
    // a cap just above the carry: the carry is more than half of it
    s.sender.set_snapshot_page_bytes_for_test(Some(size + 1));
    let _ = s.sender.snapshot_page(None, None);
    assert_eq!(s.sender.register_carry_warnings(), 1, "at half the cap or more: one warning");
    let _ = s.sender.snapshot_page(None, None);
    assert_eq!(s.sender.register_carry_warnings(), 1, "and not again within the hour");
}

// -- The resolve row: what the settle pass decided is kept, and a resolution reads it (ADR-272; Q6.6) --------------------------------------

fn the_row(m: &Engine) -> Option<crate::register::VisibleRow> {
    crate::register::visible_row_for_tests(m, DB, "orders").unwrap()
}

#[test]
fn the_pass_writes_the_row_it_decided_for_a_flip() {
    let s = swap(&[]);
    assert_eq!(served_by(&s.sender), Ok(STAGE1.to_string()), "premise: the flip is visible");
    assert_eq!(
        the_row(&s.sender),
        Some(crate::register::VisibleRow {
            kind: crate::register::VisibleKind::Bound,
            event: Some(s.bind.stamp),
            physical: STAGE1.to_string(),
            newest_drop: None,
            newest_event: Some(s.bind.stamp),
        }),
        "the row names the visible binding, its stage and the newest event it was decided against"
    );
}

#[test]
fn a_logical_drop_in_effect_is_a_dropped_row_and_the_name_is_absent() {
    let s = swap(&[]);
    let drop = drop_of_orders(&s.a, s.bind.stamp.hlc.wall_ms + 5);
    window(&s.sender, std::slice::from_ref(&drop));
    assert!(s.sender.resolve(DB, "orders").is_err(), "premise: the name reads absent");
    assert_eq!(s.sender.register_resolve_fallbacks(), 0, "the row answered: nothing fell back");
    assert_eq!(
        the_row(&s.sender),
        Some(crate::register::VisibleRow {
            kind: crate::register::VisibleKind::Dropped,
            event: Some(drop.stamp),
            physical: String::new(),
            newest_drop: Some(drop.stamp),
            newest_event: Some(drop.stamp),
        }),
        "the row says dropped, at the drop's stamp, which is also the newest drop in effect"
    );
}

#[test]
fn a_resolution_with_a_current_row_reads_no_register_row() {
    use redb::ReadableDatabase;
    let s = swap(&[]);
    assert!(the_row(&s.sender).is_some(), "premise: the name has its row");
    let before = crate::register::rows_read_for_tests();
    s.sender.resolve(DB, "orders").unwrap();
    assert_eq!(
        crate::register::rows_read_for_tests(),
        before,
        "the row answers: no event of the name is read"
    );
    assert_eq!(s.sender.register_resolve_fallbacks(), 0, "and nothing fell back");
    // the counter counts: the full computation reads the name's events
    let txn = s.sender.db().begin_read().unwrap();
    crate::register::timeline_read(&txn, DB, "orders").unwrap();
    assert!(
        crate::register::rows_read_for_tests() > before,
        "premise: reading the timeline moves the counter, so a 0 above is a measurement"
    );
}

#[test]
fn a_row_naming_a_collection_that_is_gone_falls_back_to_the_events_and_counts_it() {
    let s = swap(&[]);
    assert!(the_row(&s.sender).is_some(), "premise: the name has its row");
    assert_eq!(s.sender.register_resolve_fallbacks(), 0, "premise: nothing has fallen back");
    // a burial the pass did not run: the stage's collection removed by hand
    let txn = s.sender.begin_write(crate::WriterHolder::Ddl).unwrap();
    {
        let mut collections = txn.open_table(crate::tables::COLLECTIONS).unwrap();
        let removed = collections.remove((DB, STAGE1)).unwrap();
        assert!(removed.is_some(), "premise: the stage's collection was held");
    }
    txn.commit().unwrap();
    assert_eq!(
        the_row(&s.sender).map(|row| row.physical),
        Some(STAGE1.to_string()),
        "premise: the row still names the stage, so it is stale"
    );
    let before = crate::register::rows_read_for_tests();
    let answered = s.sender.resolve(DB, "orders");
    assert!(
        answered.is_err(),
        "the answer is the computation's: neither the stage nor the identity is held"
    );
    assert!(crate::register::rows_read_for_tests() > before, "it was computed from the events");
    assert_eq!(s.sender.register_resolve_fallbacks(), 1, "and the fallback is counted");
}

#[test]
fn a_name_with_no_event_has_no_row_and_resolves_as_get_collection_does() {
    let (m, _dm) = with_orders();
    assert_eq!(the_row(&m), None, "a name with no event has no row");
    let resolved = m.resolve(DB, "orders").unwrap();
    let held = m.get_collection(DB, "orders").unwrap();
    assert_eq!((resolved.physical.as_str(), resolved.meta.id), ("orders", held.id));
    assert_eq!(resolved.visible, None, "no event made it visible");
    assert_eq!(m.register_resolve_fallbacks(), 0, "and it is not a fallback: no row was stale");
}

#[test]
fn an_event_stored_without_the_pass_makes_the_row_stale_and_the_name_is_computed() {
    let s = swap(&[]);
    assert_eq!(
        the_row(&s.sender).map(|row| row.kind),
        Some(crate::register::VisibleKind::Bound),
        "premise: the name has its row, and it says the binding is visible"
    );
    let stamp = Stamp::new(Hlc::new(s.bind.stamp.hlc.wall_ms + 5, 0), s.a.node_id(), s.a.epoch());
    let txn = s.sender.begin_write(crate::WriterHolder::Ddl).unwrap();
    let put =
        crate::register::put(&txn, DB, "orders", stamp, kimmy_core::RegisterEvent::LogicalDrop);
    assert_eq!(put.unwrap(), kimmy_core::register_state::Put::Stored, "premise: it is stored");
    txn.commit().unwrap();
    assert_eq!(
        the_row(&s.sender).map(|row| row.kind),
        Some(crate::register::VisibleKind::Bound),
        "premise: no pass ran, so the row still says the binding is visible"
    );
    assert!(s.sender.resolve(DB, "orders").is_err(), "the name reads dropped, as the events say");
    assert_eq!(s.sender.register_resolve_fallbacks(), 1, "and the stale row is counted");
}

#[test]
fn a_collection_that_arrives_deletes_the_names_row_in_its_transaction() {
    let s = swap(&[]);
    assert!(the_row(&s.sender).is_some(), "premise: the name has its row");
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    s.sender.create_stage(DB, "orders.__stg.a9", MergePolicy::Document, deadline).unwrap();
    assert_eq!(the_row(&s.sender), None, "a collection of the name arrived: no stale row is left");
    assert_eq!(served_by(&s.sender), Ok(STAGE1.to_string()), "the name still answers, computed");
    assert_eq!(s.sender.register_resolve_fallbacks(), 1, "and a name with events, no row, counts");
}

#[test]
fn forgetting_a_collection_leaving_its_rows_deletes_the_names_row() {
    let s = swap(&[]);
    assert!(the_row(&s.sender).is_some(), "premise: the name has its row");
    s.sender.forget_collection_leaving_rows(DB, STAGE1).unwrap();
    assert!(
        s.sender.get_collection(DB, STAGE1).is_err(),
        "premise: the stage's definition is gone"
    );
    assert_eq!(the_row(&s.sender), None, "and the name's row went with it");
}

#[test]
fn a_name_bound_to_a_stage_the_member_no_longer_holds_answers_not_found_from_its_row() {
    // The visible answer skips an event whose physical is not alive, down to the identity, and with no identity the name is absent: the
    // pass writes an `Identity` row, and that row is CORRECT (the computation says not found). The reader must use it, with no fallback
    // and no count. It cannot go stale: a collection that arrives (the identity, a stage, a recreation of the name) deletes the row in
    // its own transaction (`forget_visible_row` maps a stage's or a recreation's name to the logical name).
    let (a, _da) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE1, MergePolicy::Document, deadline).unwrap();
    let creation = creation_of(&a, STAGE1);
    let (m, _dm) = engine();
    m.apply_batch(std::slice::from_ref(&creation)).unwrap();
    let wall = physical_now_ms() + 5_000;
    window(&m, std::slice::from_ref(&bind_of(&a, STAGE1, wall)));
    m.forget_collection_leaving_rows(DB, STAGE1).unwrap();
    let second = bind_of(&a, STAGE1, wall + 2);
    window(&m, std::slice::from_ref(&second));
    assert!(m.get_collection(DB, STAGE1).is_err(), "premise: the member no longer holds the stage");
    assert!(m.get_collection(DB, "orders").is_err(), "premise: and it holds no identity");
    assert_eq!(the_events_of_orders(&m).len(), 2, "premise: both bindings are in effect");
    assert_eq!(
        the_row(&m),
        Some(crate::register::VisibleRow {
            kind: crate::register::VisibleKind::Identity,
            event: None,
            physical: String::new(),
            newest_drop: None,
            newest_event: Some(second.stamp),
        }),
        "premise: the pass decided the identity, against the newest event"
    );
    assert_eq!(m.register_resolve_fallbacks(), 0, "premise: nothing has fallen back");
    assert!(m.resolve(DB, "orders").is_err(), "the name is absent, as the computation says");
    assert_eq!(m.register_resolve_fallbacks(), 0, "and the row said so: no fallback, no count");
}

#[test]
fn a_stage_creation_delivered_by_a_window_leaves_the_name_with_no_row() {
    // The creation of a stage that arrives in a window runs the settle pass for its name (it is judged at once against an event above its
    // deadline), with no event stored: the pass must not write a row for a name that has none (the row is a cache of the events).
    let (a, _da) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE1, MergePolicy::Document, deadline).unwrap();
    let creation = creation_of(&a, STAGE1);
    let (m, _dm) = engine();
    let passes = crate::sync::race_hooks::settle_passes();
    window(&m, std::slice::from_ref(&creation));
    assert!(m.get_collection(DB, STAGE1).is_ok(), "premise: the member holds the stage");
    assert!(the_stage_row_of(&m, STAGE1).is_some(), "premise: the register holds its creation");
    assert!(the_events_of_orders(&m).is_empty(), "premise: the name has no event in effect");
    assert!(
        crate::sync::race_hooks::settle_passes() > passes,
        "premise: the creation ran the settle pass for the name"
    );
    assert_eq!(the_row(&m), None, "a name with no event has no row, whatever ran the pass");
}

// -- The resolve row, R2: the newest logical drop in effect is read from the row (rule 4) ------------------------------------------------

/// A member that swapped, dropped the name and made it again: a recreation holds the name, and the row says what the newest drop was.
fn a_swapped_name_dropped_and_made_again() -> (Swap, Stamp, crate::meta::CollectionMeta) {
    let s = swap(&[]);
    assert!(
        s.sender.drop_collection(DB, "orders").unwrap(),
        "premise: the swapped name is dropped"
    );
    s.sender.create_collection(DB, "orders").unwrap();
    let served = served_by(&s.sender).unwrap();
    assert!(kimmy_core::is_recreation_name(&served), "premise: a recreation holds the name");
    let drop = the_row(&s.sender)
        .and_then(|row| row.newest_drop)
        .expect("premise: the row holds the newest logical drop in effect");
    let standing = s.sender.get_collection(DB, &served).unwrap();
    (s, drop, standing)
}

#[test]
fn rule_four_reads_the_newest_drop_from_the_row_and_no_register_row() {
    let (s, drop, standing) = a_swapped_name_dropped_and_made_again();
    let above = Stamp::new(drop.hlc.successor(), drop.node, drop.epoch);
    let at = Stamp::new(drop.hlc, drop.node, drop.epoch);
    let before = crate::register::rows_read_for_tests();
    assert!(
        !s.sender.definition_of_an_ended_life(&standing, above).unwrap(),
        "a definition above the drop belongs to the present life"
    );
    assert!(
        s.sender.definition_of_an_ended_life(&standing, at).unwrap(),
        "one at the drop is the drop's: a tie goes to the drop"
    );
    assert_eq!(
        crate::register::rows_read_for_tests(),
        before,
        "and the row answered: no event of the name was read"
    );
    assert_eq!(s.sender.register_resolve_fallbacks(), 0, "nothing fell back");
}

#[test]
fn rule_four_with_a_stale_row_is_computed_and_counted() {
    let s = swap(&[]);
    let stage = s.sender.get_collection(DB, STAGE1).unwrap();
    assert!(the_row(&s.sender).is_some(), "premise: the name has its row, with no drop in it");
    let stamp = Stamp::new(Hlc::new(s.bind.stamp.hlc.wall_ms + 5, 0), s.a.node_id(), s.a.epoch());
    let txn = s.sender.begin_write(crate::WriterHolder::Ddl).unwrap();
    let put =
        crate::register::put(&txn, DB, "orders", stamp, kimmy_core::RegisterEvent::LogicalDrop);
    assert_eq!(put.unwrap(), kimmy_core::register_state::Put::Stored, "premise: it is stored");
    txn.commit().unwrap();
    assert_eq!(
        the_row(&s.sender).and_then(|row| row.newest_drop),
        None,
        "premise: no pass ran, so the row knows of no drop"
    );
    assert!(
        s.sender.definition_of_an_ended_life(&stage, stamp).unwrap(),
        "a definition at the drop that is stored is the drop's, whatever the stale row says"
    );
    assert_eq!(s.sender.register_resolve_fallbacks(), 1, "and the stale row is counted");
}

// -- Unit C2e: a name is nameable from the hidden collections the receiver holds, not only from the carry's stage rows ---------------------

/// A member `m` made by a page from an OLDER sender: it holds the stage's collection and no stage row for it (the page had no `stage`
/// field and carried no register), and then met the logical drop of the name by a window, which it stored (the name is swapped: a
/// collection `orders.__stg.*` is held). Returns the author, the member and the drop.
fn a_member_with_a_stage_and_no_stage_row_that_met_the_drop()
-> (Engine, Engine, OplogEntry, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE1, MergePolicy::Document, deadline).unwrap();
    let mut page = a.snapshot_page(None, None).unwrap();
    let has_the_stage = page.collections.iter().any(|state| state.stage.is_some());
    assert!(has_the_stage, "premise: the page has the stage");
    for state in &mut page.collections {
        state.stage = None;
    }
    page.register = None;
    let (m, dm) = engine();
    m.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    let drop = drop_of_orders(&a, physical_now_ms() + 5_000);
    window(&m, std::slice::from_ref(&drop));
    (a, m, drop, [da, dm])
}

#[test]
fn a_name_whose_only_hidden_collection_has_no_stage_row_is_named_from_that_collection() {
    let (_a, m, drop, _dirs) = a_member_with_a_stage_and_no_stage_row_that_met_the_drop();
    assert!(m.get_collection(DB, STAGE1).is_ok(), "premise: the source member holds the stage");
    assert!(the_stage_row_of(&m, STAGE1).is_none(), "premise: and no stage row for it");
    assert_eq!(
        the_events_of_orders(&m).iter().map(|(stamp, _)| *stamp).collect::<Vec<_>>(),
        vec![drop.stamp],
        "premise: the source member stored the logical drop"
    );
    let source_row = the_row(&m);
    assert_eq!(
        source_row.as_ref().map(|row| row.kind),
        Some(crate::register::VisibleKind::Dropped),
        "premise: the source member's pass wrote the dropped row"
    );
    let (j, _dj) = engine();
    pull(&j, &m);
    assert!(j.get_collection(DB, STAGE1).is_ok(), "premise: the joiner holds the stage as well");
    assert!(the_stage_row_of(&j, STAGE1).is_none(), "premise: and no stage row, as the source");
    assert_eq!(
        the_events_of_orders(&j).iter().map(|(stamp, _)| *stamp).collect::<Vec<_>>(),
        vec![drop.stamp],
        "the joiner holds the drop"
    );
    assert_eq!(the_row(&j), source_row, "and the row the pass wrote for it is the source's");
    assert!(j.resolve(DB, "orders").is_err(), "the name is absent, as on the source");
    assert_eq!(j.register_resolve_fallbacks(), 0, "and the row answered: no fallback");
    assert_eq!(
        j.get_collection(DB, STAGE1).is_ok(),
        m.get_collection(DB, STAGE1).is_ok(),
        "the stage is in the state the source holds it in (no pass buries a stage with no row, on either)"
    );
}

/// A probe of events of a name with no stage row among members of the current version, not by an older page: A
/// authors a stage, B takes the SCOPED page of it (the stage and its row, no coverage), B's client drops `orders` and makes it again
/// (and, with `dropped_again`, drops it once more), and C, below A's creation, takes a window from B. C holds the name's events and no
/// stage row at all. A stale D that still holds the identity `orders` then joins from C by a whole-database pull.
fn a_stale_member_joins_from_one_whose_events_have_no_stage_row(
    dropped_again: bool,
) -> (Engine, Engine, [tempfile::TempDir; 4]) {
    let (a, da) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE1, MergePolicy::Document, deadline).unwrap();
    let creation = creation_of(&a, STAGE1);
    let (b, db) = with_orders();
    let (c, dc) = engine();
    let (d, dd) = engine();
    window(&c, &everything(&b));
    window(&d, &everything(&b));
    assert!(
        c.get_collection(DB, "orders").is_ok() && d.get_collection(DB, "orders").is_ok(),
        "premise: C and D hold the identity by B's creation"
    );
    let id = CollectionId::derive(DB, STAGE1);
    let page = a.snapshot_page(None, Some(id)).unwrap();
    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::of_collection(id), &page).unwrap();
    assert!(the_stage_row_of(&b, STAGE1).is_some(), "premise: B holds the stage row");
    assert!(
        b.witnessed_vector().unwrap().get(creation.stamp.origin()) < creation.stamp.hlc,
        "premise: B's vector does not cover the stage's creation"
    );
    assert!(b.drop_collection(DB, "orders").unwrap(), "premise: B's client drops the name");
    b.create_collection(DB, "orders").unwrap();
    if dropped_again {
        assert!(b.drop_collection(DB, "orders").unwrap(), "premise: and drops it again");
    }
    let held = c.witnessed_vector().unwrap();
    let mine = b.version_vector().unwrap();
    assert_eq!(b.serve_verdict(&held).unwrap(), crate::ServeVerdict::Serve, "premise: B serves C");
    let from = held.behind(&mine).expect("B is ahead of C");
    let w = b.entries_for_peer_holding(from, 10_000, Some(&held), WalkScope::Background).unwrap();
    let mut outcome = SyncOutcome::default();
    let window = PeerWindow {
        scanned_to: w.scanned_to,
        exhausted: w.exhausted,
        passed_through: None,
        asked_partial: false,
    };
    c.apply_peer_window_into(&mine, Some(&held), &w.entries, window, &mut outcome).unwrap();
    assert!(!the_events_of_orders(&c).is_empty(), "premise: C holds register events of the name");
    let stage_rows = {
        use redb::ReadableDatabase;
        let txn = c.db().begin_read().unwrap();
        crate::register::carried_rows(&txn, crate::register::Carried::Stages).unwrap().len()
    };
    assert_eq!(stage_rows, 0, "premise: and no stage row at all");
    (c, d, [da, db, dc, dd])
}

#[test]
fn a_stale_member_holding_the_identity_buries_it_when_it_joins_from_events_with_no_stage_row() {
    let (c, d, _dirs) = a_stale_member_joins_from_one_whose_events_have_no_stage_row(true);
    assert!(d.get_collection(DB, "orders").is_ok(), "premise: D still holds the identity");
    let (progress, result) = pull_patched(&d, &c, &|_| {});
    result.expect("the pull applies");
    assert!(progress.is_complete(), "the pull completes");
    assert!(
        d.get_collection(DB, "orders").is_err(),
        "the identity is buried in the join's transaction, as the name's drop says"
    );
    assert_eq!(
        the_row(&d).map(|row| row.kind),
        Some(crate::register::VisibleKind::Dropped),
        "and the dropped row is written"
    );
    assert!(d.resolve(DB, "orders").is_err(), "the name is absent");
    assert_eq!(d.register_resolve_fallbacks(), 0, "from the row: no fallback");
}

#[test]
fn an_unnamed_id_the_receiver_holds_a_tombstone_for_above_the_grant_is_seeded_by_id() {
    // D1 for an id that no stage row and no collection of the receiver names: the receiver's own tombstone, above the grant, is the
    // name's drop, written by id. The carried event is below the grant and below the tombstone.
    let (s, _ds) = engine();
    let (j, _dj) = engine();
    j.create_collection(DB, "orders").unwrap();
    assert!(j.drop_collection(DB, "orders").unwrap(), "premise: the receiver dropped the name");
    let tombstone = j
        .collection_dropped_at(CollectionId::derive(DB, "orders"))
        .unwrap()
        .expect("premise: the receiver holds the identity's tombstone");
    assert!(j.get_collection(DB, "orders").is_err(), "premise: it holds no collection of the name");
    let carried = Stamp::new(Hlc::new(tombstone.hlc.wall_ms - 1_000, 0), s.node_id(), s.epoch());
    let patch = |page: &mut crate::SnapshotPage| {
        page.versions.observe(carried);
        let mut key = CollectionId::derive(DB, "orders").0.to_be_bytes().to_vec();
        key.extend_from_slice(&carried.to_key_bytes());
        let event = kimmy_core::RegisterEvent::Create { physical: "orders.__rc.x".into() };
        if let Some(carry) = &mut page.register {
            let row = (serde_bytes::ByteBuf::from(key), serde_bytes::ByteBuf::from(event.encode()));
            carry.events.push(row);
        }
    };
    let grant = std::cell::Cell::new(None);
    let (_, result) = pull_patched(&j, &s, &|page| {
        patch(page);
        grant.set(Some(page.versions.get(tombstone.origin())));
    });
    result.expect("the pull applies");
    let above = grant.get().is_some_and(|granted| tombstone.hlc > granted);
    assert!(above, "premise: the tombstone is above the grant");
    let events = the_events_of_orders(&j);
    assert!(
        events.iter().any(|(stamp, event)| *stamp == tombstone
            && matches!(event, kimmy_core::RegisterEvent::LogicalDrop)),
        "the receiver's tombstone is the name's drop, stored by id: {events:?}"
    );
    assert!(
        events.iter().any(|(stamp, _)| *stamp == carried),
        "and the carried event is stored too: {events:?}"
    );
}

#[test]
fn an_id_no_hidden_collection_names_is_stored_by_id_with_no_pass_and_no_row() {
    // the scan of the receiver's own hidden collections runs (it holds the swap's stage) and finds no collection of `ghost`
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
    assert!(j.get_collection(DB, STAGE1).is_ok(), "premise: the receiver holds a stage");
    use redb::ReadableDatabase;
    let txn = j.db().begin_read().unwrap();
    assert!(crate::register::has_events(&txn, DB, "ghost").unwrap(), "stored by id");
    assert_eq!(
        crate::register::visible_row_read(&txn, DB, "ghost").unwrap(),
        None,
        "and no pass ran for it, so no row"
    );
}

#[test]
fn a_hand_built_recreation_with_no_stage_row_is_named_from_its_collection_and_joins_as_its_source_holds_it()
 {
    // A STATE BUILT BY HAND, not one a product path makes (the stage's row removed by the test hook): the name's only collection is a
    // recreation, and its events are a logical drop and the recreation's `Create`. It is here for the half of the scan that names an id
    // from a RECREATION's physical name, which the stage half does not exercise.
    let (a, _da) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE1, MergePolicy::Document, deadline).unwrap();
    let (m, _dm) = with_orders();
    m.apply_batch(std::slice::from_ref(&creation_of(&a, STAGE1))).unwrap();
    assert!(m.drop_collection(DB, "orders").unwrap(), "premise: the swapped name is dropped");
    m.create_collection(DB, "orders").unwrap();
    let recreation = served_by(&m).unwrap();
    assert!(kimmy_core::is_recreation_name(&recreation), "premise: a recreation holds the name");
    // the stage's collection and its row are taken away, and the pass runs once so that the source holds the row it would
    m.forget_collection_leaving_rows(DB, STAGE1).unwrap();
    crate::register::remove_stage_row_for_test(&m, CollectionId::derive(DB, STAGE1));
    let txn = m.begin_write(crate::WriterHolder::Ddl).unwrap();
    crate::Engine::settle_register_in_txn(&txn, DB, "orders", None).unwrap();
    txn.commit().unwrap();
    assert!(m.get_collection(DB, STAGE1).is_err(), "premise: the source holds no stage");
    assert!(the_stage_row_of(&m, STAGE1).is_none(), "premise: and no stage row of the name");
    assert_eq!(the_events_of_orders(&m).len(), 2, "premise: the drop and the recreation's Create");
    let source_row = the_row(&m);
    assert_eq!(
        source_row.as_ref().map(|row| row.kind),
        Some(crate::register::VisibleKind::Created),
        "premise: the source's row says the recreation is visible"
    );
    let (j, _dj) = engine();
    pull(&j, &m);
    assert!(j.get_collection(DB, &recreation).is_ok(), "premise: the joiner holds the recreation");
    assert_eq!(
        the_events_of_orders(&j),
        the_events_of_orders(&m),
        "the joiner holds the source's events, the Create among them"
    );
    assert_eq!(the_row(&j), source_row, "and the row the pass wrote for the name is the source's");
    assert_eq!(served_by(&j), served_by(&m), "the name is served by the same recreation");
    assert_eq!(j.register_resolve_fallbacks(), 0, "from the row: no fallback");
}

// -- Unit D (D-1): a doomed burial is never taken from another member; the receiver asks its own tests --------------------------------------

const DOOMED: &str = "orders.__stg.d0";
const LIVE1: &str = "orders.__stg.d1";
const LIVE2: &str = "orders.__stg.d2";

/// A sender that doomed a stage: `a` authors three stages (the first with a deadline one second out, the other two a minute), `b` (another
/// origin) authors two bindings, the first of the second stage and the later of the third. The first binding makes the first stage doomed
/// (it has no binding and its deadline is below the visible event) and the second buries the second as a loser. The sender holds
/// the third alone, and the stage rows of all three.
struct Doomed {
    a: Engine,
    sender: Engine,
    creations: Vec<OplogEntry>,
    bind1: OplogEntry,
    bind2: OplogEntry,
    _dirs: Vec<tempfile::TempDir>,
}

fn a_sender_that_doomed_a_stage() -> Doomed {
    let (a, da) = engine();
    let (b, db) = engine();
    let now = physical_now_ms();
    a.create_stage(DB, DOOMED, MergePolicy::Document, Hlc::new(now + 1_000, 0)).unwrap();
    a.create_stage(DB, LIVE1, MergePolicy::Document, Hlc::new(now + 60_000, 0)).unwrap();
    a.create_stage(DB, LIVE2, MergePolicy::Document, Hlc::new(now + 60_000, 0)).unwrap();
    let creations: Vec<OplogEntry> =
        [DOOMED, LIVE1, LIVE2].iter().map(|name| creation_of(&a, name)).collect();
    let (sender, ds) = engine();
    sender.apply_batch(&creations).unwrap();
    let wall = now + 5_000;
    let bind1 = bind_of(&b, LIVE1, wall);
    let bind2 = bind_of(&b, LIVE2, wall + 10);
    window(&sender, std::slice::from_ref(&bind1));
    window(&sender, std::slice::from_ref(&bind2));
    assert_eq!(
        served_by(&sender),
        Ok(LIVE2.to_string()),
        "premise: the sender serves the last stage"
    );
    assert!(
        sender.get_collection(DB, DOOMED).is_err(),
        "premise: the sender doomed the first stage"
    );
    assert!(sender.get_collection(DB, LIVE1).is_err(), "premise: and buried the second as a loser");
    assert!(
        the_stage_row_of(&sender, DOOMED).is_some(),
        "premise: the sender still holds its stage row"
    );
    Doomed { a, sender, creations, bind1, bind2, _dirs: vec![da, db, ds] }
}

/// The ids a page lists as not alive at its sender.
fn listed_not_alive(page: &crate::SnapshotPage) -> Vec<u64> {
    let carry = page.register.as_ref().expect("premise: the last page carries the register");
    carry
        .not_alive
        .iter()
        .map(|id| u64::from_be_bytes(id.as_slice().try_into().expect("an id is eight bytes")))
        .collect()
}

/// After a join, every stage row this member holds has a live collection or a tombstone under its id (outside an unfinished join, a
/// member that knows a stage's creation holds the stage alive or holds its own tombstone for it).
fn assert_stage_invariant(m: &Engine) {
    use redb::ReadableDatabase;
    let txn = m.db().begin_read().unwrap();
    let rows = crate::register::carried_rows(&txn, crate::register::Carried::Stages).unwrap();
    assert!(!rows.is_empty(), "premise: the member holds stage rows");
    for (key, _) in rows {
        let id = CollectionId(u64::from_be_bytes(key.as_slice().try_into().unwrap()));
        let row = crate::register::stage_row_named_read(&txn, id).unwrap().unwrap();
        let alive = m.get_collection(&row.db, &row.physical).is_ok();
        let tombstone = m.collection_dropped_at(id).unwrap().is_some();
        assert!(
            alive || tombstone,
            "the stage {} has no live collection and no tombstone",
            row.physical
        );
    }
}

#[test]
fn a_doomed_stage_this_member_holds_is_buried_at_the_stamp_of_its_own_visible_event() {
    let w = a_sender_that_doomed_a_stage();
    let (j, _dj) = engine();
    j.apply_batch(std::slice::from_ref(&w.creations[0])).unwrap();
    assert!(
        j.get_collection(DB, DOOMED).is_ok(),
        "premise: the receiver holds the doomed stage alive"
    );
    pull(&j, &w.sender);
    assert!(j.get_collection(DB, DOOMED).is_err(), "the receiver buried it by its own test");
    assert_eq!(
        j.collection_dropped_at(CollectionId::derive(DB, DOOMED)).unwrap(),
        Some(w.bind2.stamp),
        "at the stamp of its own visible event"
    );
    let doomed = j.out_buried().into_iter().find(|(cause, _)| *cause == "doomed");
    assert!(doomed.is_some_and(|(_, n)| n >= 1), "and the burial is counted as doomed");
    assert_stage_invariant(&j);
}

#[test]
fn a_doomed_stage_held_nowhere_here_gets_its_own_tombstone_at_the_stamp_of_its_visible_event() {
    let w = a_sender_that_doomed_a_stage();
    let (j, _dj) = engine();
    let doomed = CollectionId::derive(DB, DOOMED);
    let page = w.sender.snapshot_page(None, None).unwrap();
    assert!(
        listed_not_alive(&page).contains(&doomed.0),
        "premise: the sender lists the doomed stage"
    );
    assert!(
        j.get_collection(DB, DOOMED).is_err(),
        "premise: the receiver holds no collection of it"
    );
    assert_eq!(j.collection_dropped_at(doomed).unwrap(), None, "premise: and no tombstone");
    assert_ne!(
        w.bind1.stamp, w.bind2.stamp,
        "premise: two events were visible in turn on the sender"
    );
    pull(&j, &w.sender);
    assert_eq!(
        j.collection_dropped_at(doomed).unwrap(),
        Some(w.bind2.stamp),
        "its own tombstone, at the stamp of the event visible after the join, not the sender's"
    );
    assert_stage_invariant(&j);
}

#[test]
fn a_stage_alive_at_the_sender_is_not_listed_and_is_not_buried_by_the_listing() {
    let w = a_sender_that_doomed_a_stage();
    let live = CollectionId::derive(DB, LIVE2);
    let page = w.sender.snapshot_page(None, None).unwrap();
    assert!(
        !listed_not_alive(&page).contains(&live.0),
        "a stage alive at the sender is not listed"
    );
    assert!(
        listed_not_alive(&page).contains(&CollectionId::derive(DB, DOOMED).0),
        "premise: the list is not empty: it names the stage the sender doomed"
    );
    let (j, _dj) = engine();
    pull(&j, &w.sender);
    assert!(j.get_collection(DB, LIVE2).is_ok(), "the receiver holds the live stage");
    assert_eq!(j.collection_dropped_at(live).unwrap(), None, "and holds no tombstone for it");
}

#[test]
fn a_listed_stage_whose_creation_is_above_the_grant_is_left_alone_and_the_join_completes() {
    let w = a_sender_that_doomed_a_stage();
    let doomed = CollectionId::derive(DB, DOOMED);
    let below = w.creations[0].stamp.hlc.predecessor();
    let patch = |page: &mut crate::SnapshotPage| {
        // the grant for the author of the stages is below the doomed stage's creation: the creation is not this join's
        page.versions.insert(w.a.origin(), below);
    };
    let page = w.sender.snapshot_page(None, None).unwrap();
    assert!(
        listed_not_alive(&page).contains(&doomed.0),
        "premise: the sender lists the doomed stage"
    );
    let (j, _dj) = engine();
    let (progress, result) = pull_patched(&j, &w.sender, &patch);
    result.expect("the join completes");
    assert!(progress.is_complete(), "the pull completes");
    assert_eq!(
        j.collection_dropped_at(doomed).unwrap(),
        None,
        "a stage whose creation is above the grant is not this join's: no tombstone"
    );
}

#[test]
fn a_listed_id_this_member_has_no_stage_row_for_is_skipped() {
    let w = a_sender_that_doomed_a_stage();
    let ghost = CollectionId::derive(DB, "orders.__stg.zz");
    let patch = |page: &mut crate::SnapshotPage| {
        if let Some(carry) = &mut page.register {
            carry.not_alive.push(serde_bytes::ByteBuf::from(ghost.0.to_be_bytes().to_vec()));
        }
    };
    let (j, _dj) = engine();
    let (progress, result) = pull_patched(&j, &w.sender, &patch);
    result.expect("the join completes");
    assert!(progress.is_complete(), "the pull completes");
    assert_eq!(
        j.collection_dropped_at(ghost).unwrap(),
        None,
        "an id with no stage row gets no tombstone"
    );
}

#[test]
fn the_carrys_measure_counts_the_list_of_stages_not_alive() {
    let w = a_sender_that_doomed_a_stage();
    let page = w.sender.snapshot_page(None, None).unwrap();
    let carry = page.register.expect("the last page carries the register");
    assert!(!carry.not_alive.is_empty(), "premise: the list is not empty");
    let mut without = carry.clone();
    without.not_alive.clear();
    assert!(
        carry.encoded_len() >= without.encoded_len() + 8 * carry.not_alive.len(),
        "the list is in the measure, so the cap and the gauge count it"
    );
}

// -- Unit E (D-4): a join across a burial restarts ----------------------------------------------------------------------------------------

const F: [&str; 3] = ["orders.__stg.f1", "orders.__stg.f2", "orders.__stg.f3"];

/// The room of a page for rows: two documents of these tests (a row costs 631 bytes of it) and not three, and the carry of three swaps
/// (about 1,100 bytes) beside them or alone, as a page that carries it must (`SnapshotRegisterCarryTooLarge`).
const TWO_ROWS_A_PAGE: usize = 1_500;

/// A sender that swaps `orders` again while a member joins from it. It holds six documents in each stage and has made the binding of
/// the first stage visible (`orders` is served by it) before the first page of the join is read. Its page reader (the hook) makes the
/// next swap, `later` of them, before the SECOND page of each pass is read: the stage's creation and documents, then a binding of it
/// above the grant the pass was given (the first page's vector was read before). So the stage the pass saw alive on page one is buried
/// by an event the grant does not cover, before its documents are walked: the case of decision 9. Two documents to a page.
struct Across {
    sender: Engine,
    creations: Vec<OplogEntry>,
    /// How many pages with a cursor the sender's page reader has been asked for.
    cursor_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    _dirs: Vec<tempfile::TempDir>,
}

/// The premise every test that drives a swap through the page reader has: the reader ran with a cursor, so the join had a page after the
/// first and the swap was made (a join of one page fails here, on its real cause, and not on a later premise).
fn the_reader_ran(w: &Across) {
    assert!(
        w.cursor_calls.load(std::sync::atomic::Ordering::SeqCst) >= 1,
        "premise: the sender's page reader ran at least once with a cursor"
    );
}

fn a_sender_that_swaps_again_during_the_join(later: usize) -> Across {
    let (a, da) = engine();
    let (b, db) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    for (k, name) in F[..=later].iter().enumerate() {
        let stage = a.create_stage(DB, name, MergePolicy::Document, deadline).unwrap();
        for i in 1..=6 {
            let id = i64::try_from(k).unwrap() * 10 + i;
            a.insert(&stage, doc! { "_id": id, "v": "x".repeat(40) }).unwrap();
        }
    }
    let entries_of = |name: &str| -> Vec<OplogEntry> {
        let id = CollectionId::derive(DB, name);
        everything(&a).into_iter().filter(|entry| entry.collection == id).collect()
    };
    let creations: Vec<OplogEntry> = F[..=later].iter().map(|name| creation_of(&a, name)).collect();
    let (sender, ds) = with_orders();
    // documents beside the stages, so that the rows exceed a page whatever a stage's rows are or are not walked
    let filler = sender.create_collection(DB, "filler").unwrap();
    for i in 1..=20 {
        sender.insert(&filler, doc! { "_id": i, "v": "x".repeat(40) }).unwrap();
    }
    sender.apply_batch(&entries_of(F[0])).unwrap();
    let base = creations
        .iter()
        .map(|creation| creation.stamp.hlc.wall_ms)
        .chain([life_wall(&sender), physical_now_ms()])
        .max()
        .unwrap()
        + 1;
    window(&sender, std::slice::from_ref(&bind_of(&b, F[0], base)));
    assert_eq!(
        served_by(&sender),
        Ok(F[0].to_string()),
        "premise: the sender serves the first stage"
    );
    let swaps: Vec<(Vec<OplogEntry>, OplogEntry)> = (1..=later)
        .map(|k| (entries_of(F[k]), bind_of(&b, F[k], base + 10 * u64::try_from(k).unwrap())))
        .collect();
    let (page_ones, made) =
        (std::sync::atomic::AtomicUsize::new(0), std::sync::atomic::AtomicUsize::new(0));
    sender.set_snapshot_room_for_test(Some(TWO_ROWS_A_PAGE));
    // read before the hook is set, so that it does not count this page
    assert!(
        sender.snapshot_page(None, None).unwrap().next.is_some(),
        "premise: page one is not the last, so a page with a cursor is asked for"
    );
    let cursor_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = cursor_calls.clone();
    let hook = move |engine: &Engine, after: Option<&crate::SnapshotCursor>| {
        use std::sync::atomic::Ordering::SeqCst;
        if after.is_none() {
            page_ones.fetch_add(1, SeqCst);
            return;
        }
        counted.fetch_add(1, SeqCst);
        // before the second page of pass n: the n-th swap, once
        let due = made.load(SeqCst);
        if due < swaps.len() && page_ones.load(SeqCst) == due + 1 {
            made.fetch_add(1, SeqCst);
            let (entries, bind) = &swaps[due];
            engine.apply_batch(entries).unwrap();
            window(engine, std::slice::from_ref(bind));
            assert_eq!(
                served_by(engine),
                Ok(F[due + 1].to_string()),
                "premise: the sender serves the newer stage, and has buried the one the pass read"
            );
        }
    };
    sender.set_snapshot_reader_hook_for_test(Some(std::sync::Arc::new(hook)));
    Across { sender, creations, cursor_calls, _dirs: vec![da, db, ds] }
}

/// What a join came to, page by page, as `pull_snapshot`'s loop would drive it.
struct Joined {
    progress: SnapshotProgress,
    /// `SnapshotApplied::restarts` of each page, in order.
    restarts: Vec<usize>,
}

fn join_by_pages(joiner: &Engine, sender: &Engine) -> Joined {
    join_by_pages_patched(joiner, sender, &|_| {})
}

fn join_by_pages_patched(joiner: &Engine, sender: &Engine, patch: Patch<'_>) -> Joined {
    let mut progress = SnapshotProgress::whole_database();
    let mut restarts = Vec::new();
    for _ in 0..60 {
        if progress.is_complete() {
            return Joined { progress, restarts };
        }
        let mut page = sender.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        patch(&mut page);
        let done = joiner.apply_snapshot_page(sender.node_id(), &mut progress, &page).unwrap();
        restarts.push(done.restarts);
    }
    panic!("the join did not complete in 60 pages: a restart that does not end");
}

/// Pages of the join applied until one restarts it (or the join completes); the page that did.
fn join_until_it_restarts(joiner: &Engine, sender: &Engine) -> SnapshotProgress {
    let mut progress = SnapshotProgress::whole_database();
    for _ in 0..60 {
        let page = sender.snapshot_page(progress.after().cloned(), None).unwrap();
        let done = joiner.apply_snapshot_page(sender.node_id(), &mut progress, &page).unwrap();
        if done.restarts > 0 {
            return progress;
        }
        assert!(!progress.is_complete(), "premise: the join restarts before it completes");
    }
    panic!("premise: the join restarts within 60 pages");
}

#[test]
fn the_senders_page_reader_hook_runs_before_each_page_with_the_cursor_asked_for() {
    let (sender, _ds) = engine();
    let orders = sender.create_collection(DB, "orders").unwrap();
    for i in 1..=20 {
        sender.insert(&orders, doc! { "_id": i, "v": "x".repeat(40) }).unwrap();
    }
    sender.set_snapshot_room_for_test(Some(TWO_ROWS_A_PAGE));
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Option<String>>::new()));
    let log = seen.clone();
    let hook = move |_: &Engine, after: Option<&crate::SnapshotCursor>| {
        log.lock().unwrap().push(after.map(ToString::to_string));
    };
    sender.set_snapshot_reader_hook_for_test(Some(std::sync::Arc::new(hook)));
    let (j, _dj) = engine();
    let mut progress = SnapshotProgress::whole_database();
    let mut asked = Vec::new();
    while !progress.is_complete() {
        asked.push(progress.after().map(ToString::to_string));
        let page = sender.snapshot_page(progress.after().cloned(), None).unwrap();
        j.apply_snapshot_page(sender.node_id(), &mut progress, &page).unwrap();
        assert!(asked.len() < 20, "premise: the snapshot ends");
    }
    assert!(asked.len() >= 3, "premise: the snapshot has several pages");
    assert_eq!(*seen.lock().unwrap(), asked, "the hook saw each page's cursor, in order, once");
    sender.set_snapshot_reader_hook_for_test(None);
    sender.snapshot_page(None, None).unwrap();
    assert_eq!(seen.lock().unwrap().len(), asked.len(), "a hook taken off is not called");
}

#[test]
fn a_stage_buried_by_an_event_above_the_grant_is_listed_and_the_carry_holds_the_event() {
    let w = a_sender_that_swaps_again_during_the_join(1);
    let stage = CollectionId::derive(DB, F[0]);
    let first = w.sender.snapshot_page(None, None).unwrap();
    assert!(first.next.is_some(), "premise: page one is not the last");
    assert!(w.sender.get_collection(DB, F[0]).is_ok(), "premise: page one is read before the swap");
    let origin = w.creations[0].stamp.origin();
    assert!(
        first.versions.get(origin) >= w.creations[0].stamp.hlc,
        "premise: the grant of page one covers the stage's creation"
    );
    let (mut cursor, mut last) = (first.next.clone(), None);
    while let Some(after) = cursor {
        let page = w.sender.snapshot_page(Some(after), None).unwrap();
        cursor = page.next.clone();
        last = Some(page);
    }
    let last = last.expect("premise: there is a page after the first");
    assert!(listed_not_alive(&last).contains(&stage.0), "the last page lists the buried stage");
    let logical = CollectionId::derive(DB, "orders").0.to_be_bytes();
    let above = last.register.as_ref().expect("the last page carries the register").events.iter();
    let above = above.filter(|(key, _)| key[..8] == logical).any(|(key, _)| {
        let stamp = Stamp::from_key_bytes(key[8..].try_into().unwrap());
        stamp.hlc > first.versions.get(stamp.origin())
    });
    assert!(above, "and the carry holds an event of the name above the first page's grant");
    the_reader_ran(&w);
}

#[test]
fn a_join_across_a_swap_above_its_grant_restarts_once_and_serves_the_newer_stage_whole() {
    let w = a_sender_that_swaps_again_during_the_join(1);
    let (j, _dj) = engine();
    let joined = join_by_pages(&j, &w.sender);
    assert!(joined.progress.is_complete(), "the join completes");
    assert_eq!(
        joined.restarts.iter().sum::<usize>(),
        1,
        "it restarted once: {:?}",
        joined.restarts
    );
    assert_eq!(served_by(&j), Ok(F[1].to_string()), "and the name is served from the newer stage");
    let newer = j.get_collection(DB, F[1]).unwrap();
    assert_eq!(j.count(&newer, WalkScope::Request).unwrap(), 6, "with every document of it");
    assert!(j.get_collection(DB, F[0]).is_err(), "the stage the first pass held partly is buried");
    assert_stage_invariant(&j);
    assert_eq!(
        the_events_of_orders(&j),
        the_events_of_orders(&w.sender),
        "the member's register is the sender's"
    );
    assert_eq!(j.snapshot_join_restarts(), 1, "counted once");
    assert_eq!(
        j.snapshot_restarted_for_test(w.sender.node_id()),
        Vec::<CollectionId>::new(),
        "and what it remembered of the restart is forgotten with the join"
    );
    the_reader_ran(&w);
}

#[test]
fn the_page_that_restarts_the_join_commits_nothing_of_its_carry_and_grants_nothing() {
    let w = a_sender_that_swaps_again_during_the_join(1);
    let (j, _dj) = engine();
    let progress = join_until_it_restarts(&j, &w.sender);
    assert_eq!(progress, SnapshotProgress::whole_database(), "the progress is back at page one");
    assert_eq!(
        j.snapshot_restarted_for_test(w.sender.node_id()),
        vec![CollectionId::derive(DB, F[0])],
        "the stage that restarted the join is remembered, so that it restarts it once"
    );
    let origin = w.creations[0].stamp.origin();
    assert!(
        j.witnessed_vector().unwrap().get(origin) < w.creations[1].stamp.hlc,
        "no grant was absorbed: the member still asks for what the newer stage's creation covers"
    );
    assert!(the_events_of_orders(&j).is_empty(), "no event of the carry was stored");
    assert_eq!(
        j.collection_dropped_at(CollectionId::derive(DB, F[0])).unwrap(),
        None,
        "and no tombstone was recorded for the stage the first pass held"
    );
    assert!(j.get_collection(DB, F[0]).is_ok(), "the pages before it stay applied");
    the_reader_ran(&w);
}

#[test]
fn the_progress_a_restart_leaves_is_the_progress_a_superseding_pull_starts_from() {
    let w = a_sender_that_swaps_again_during_the_join(1);
    let (j, _dj) = engine();
    let peer = w.sender.node_id();
    let mut progress = SnapshotProgress::whole_database();
    loop {
        let page = w.sender.snapshot_page(progress.after().cloned(), None).unwrap();
        let before = progress.clone();
        let done = j.apply_snapshot_page(peer, &mut progress, &page).unwrap();
        if done.restarts > 0 {
            assert_ne!(before, SnapshotProgress::whole_database(), "premise: it was under way");
            break;
        }
        assert!(progress.pages() < 50, "premise: the join restarts");
    }
    // the same fields as a fresh progress, which is what the puller replaces a superseded one with
    assert_eq!(
        progress,
        SnapshotProgress::whole_database(),
        "the restart's reset is the supersede's"
    );
    assert!(
        !j.snapshot_progress_recorded(peer, None).unwrap(),
        "and the cursor the earlier pages persisted is gone with it"
    );
    the_reader_ran(&w);
}

#[test]
fn a_stage_held_none_of_restarts_the_join_when_an_event_of_its_name_is_above_the_grant() {
    // the stage the first pass would have read was buried before page one by a plain drop, so no page defines it and this member never
    // holds it: it is `left`, and the swap the reader makes after page one is above the grant
    let w = a_sender_that_swaps_again_during_the_join(1);
    assert!(w.sender.bury_collection(DB, F[0], None).unwrap().is_some(), "premise: buried first");
    let (j, _dj) = engine();
    let joined = join_by_pages(&j, &w.sender);
    assert!(joined.progress.is_complete(), "the join completes");
    assert_eq!(
        joined.restarts.iter().sum::<usize>(),
        1,
        "it restarted once: {:?}",
        joined.restarts
    );
    assert_eq!(served_by(&j), Ok(F[1].to_string()), "and the name is served from the newer stage");
    assert_stage_invariant(&j);
    assert_eq!(j.snapshot_listed_stages_left(), 0, "nothing is left standing by the second pass");
    the_reader_ran(&w);
}

#[test]
fn a_stage_that_already_restarted_the_join_completes_it_the_second_time_and_is_counted() {
    let w = a_sender_that_swaps_again_during_the_join(1);
    let (j, _dj) = engine();
    j.note_restarted_for_test(w.sender.node_id(), &[CollectionId::derive(DB, F[0])]);
    let joined = join_by_pages(&j, &w.sender);
    assert!(joined.progress.is_complete(), "the join completes");
    assert_eq!(joined.restarts.iter().sum::<usize>(), 0, "without a restart: the stage had one");
    assert_eq!(j.snapshot_join_restarts(), 0, "none counted");
    assert_eq!(j.snapshot_listed_stages_left(), 1, "and the stage it left is counted");
    assert!(
        j.snapshot_restarted_for_test(w.sender.node_id()).is_empty(),
        "what it remembered is forgotten with the join"
    );
    the_reader_ran(&w);
}

#[test]
fn two_stages_buried_during_a_join_give_exactly_two_restarts_and_the_last_serves_the_name() {
    let w = a_sender_that_swaps_again_during_the_join(2);
    let (j, _dj) = engine();
    let joined = join_by_pages(&j, &w.sender);
    assert!(joined.progress.is_complete(), "the join completes");
    assert_eq!(joined.restarts.iter().sum::<usize>(), 2, "{:?}", joined.restarts);
    assert_eq!(served_by(&j), Ok(F[2].to_string()), "the name is served from the last stage");
    let last = j.get_collection(DB, F[2]).unwrap();
    assert_eq!(j.count(&last, WalkScope::Request).unwrap(), 6, "with every document of it");
    assert_eq!(j.snapshot_join_restarts(), 2, "both counted");
    assert_stage_invariant(&j);
    the_reader_ran(&w);
}

#[test]
fn a_listed_stage_whose_creation_is_above_the_grant_does_not_restart_the_join() {
    let w = a_sender_that_swaps_again_during_the_join(1);
    let below = w.creations[0].stamp.hlc.predecessor();
    let origin = w.creations[0].stamp.origin();
    let (j, _dj) = engine();
    let joined = join_by_pages_patched(&j, &w.sender, &|page| {
        page.versions.insert(origin, below);
    });
    assert!(joined.progress.is_complete(), "the join completes");
    assert_eq!(
        joined.restarts.iter().sum::<usize>(),
        0,
        "a creation above the grant is not this join's: no restart"
    );
    the_reader_ran(&w);
}

#[test]
fn a_stage_this_members_own_test_buries_does_not_restart_the_join() {
    let w = a_sender_that_doomed_a_stage();
    let (j, _dj) = engine();
    j.apply_batch(std::slice::from_ref(&w.creations[0])).unwrap();
    let joined = join_by_pages(&j, &w.sender);
    assert!(joined.progress.is_complete(), "the join completes");
    assert_eq!(
        joined.restarts.iter().sum::<usize>(),
        0,
        "and does not restart: the pass buried it"
    );
    assert_stage_invariant(&j);
}

#[test]
fn a_stage_below_a_database_drop_this_member_holds_gets_its_own_tombstone_and_no_restart() {
    let (a, _da) = engine();
    let (d, _dd) = engine();
    d.create_collection(DB, "orders").unwrap();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, F[0], MergePolicy::Document, deadline).unwrap();
    let creation = creation_of(&a, F[0]);
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(d.drop_database(DB).unwrap(), "premise: the database is dropped");
    let (sender, _ds) = engine();
    window(&sender, std::slice::from_ref(&creation));
    assert!(sender.get_collection(DB, F[0]).is_ok(), "premise: the sender held the stage alive");
    window(&sender, &everything(&d));
    assert!(sender.get_collection(DB, F[0]).is_err(), "premise: the drop buried it at the sender");
    assert!(the_stage_row_of(&sender, F[0]).is_some(), "premise: the sender holds its stage row");
    let (j, _dj) = engine();
    window(&j, &everything(&d));
    let dropped = j.database_dropped_at(DB).unwrap().expect("premise: this member holds the drop");
    let joined = join_by_pages(&j, &sender);
    assert!(joined.progress.is_complete(), "the join completes");
    assert_eq!(joined.restarts.iter().sum::<usize>(), 0, "and does not restart");
    assert_eq!(
        j.collection_dropped_at(CollectionId::derive(DB, F[0])).unwrap(),
        Some(dropped),
        "this member's own tombstone, at the stamp of the database's drop it holds"
    );
    assert_stage_invariant(&j);
    assert_eq!(j.snapshot_listed_stages_left(), 0, "and it is not a stage left standing");
}

#[test]
fn a_stage_row_only_with_no_event_above_the_grant_completes_the_join_and_is_counted() {
    let (a, _da) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, F[0], MergePolicy::Document, deadline).unwrap();
    let (sender, _ds) = with_orders();
    sender.apply_batch(std::slice::from_ref(&creation_of(&a, F[0]))).unwrap();
    assert!(sender.bury_collection(DB, F[0], None).unwrap().is_some(), "premise: buried by a drop");
    let id = CollectionId::derive(DB, F[0]);
    let (j, _dj) = engine();
    let joined = join_by_pages(&j, &sender);
    assert!(joined.progress.is_complete(), "the join completes");
    assert_eq!(joined.restarts.iter().sum::<usize>(), 0, "and does not restart: no event says why");
    assert_eq!(j.snapshot_listed_stages_left(), 1, "the stage it left is counted");
    // the one state `assert_stage_invariant` does not hold in, named: a stage row with no collection and no tombstone, which no event
    // above the grant and no database drop explains
    assert!(the_stage_row_of(&j, F[0]).is_some(), "it holds the stage row");
    assert!(j.get_collection(DB, F[0]).is_err(), "and no collection of it");
    assert_eq!(j.collection_dropped_at(id).unwrap(), None, "and no tombstone for it");
}

#[test]
fn an_event_exactly_at_the_grant_is_not_above_it() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, F[0], MergePolicy::Document, deadline).unwrap();
    let creation = creation_of(&a, F[0]);
    let (sender, _ds) = with_orders();
    sender.apply_batch(std::slice::from_ref(&creation)).unwrap();
    let wall = creation.stamp.hlc.wall_ms.max(life_wall(&sender)).max(physical_now_ms()) + 1;
    let bind = bind_of(&b, F[0], wall);
    window(&sender, std::slice::from_ref(&bind));
    assert!(sender.bury_collection(DB, F[0], None).unwrap().is_some(), "premise: buried by a drop");
    let first = sender.snapshot_page(None, None).unwrap();
    assert_eq!(
        first.versions.get(bind.stamp.origin()),
        bind.stamp.hlc,
        "premise: the binding is exactly at the grant of the first page"
    );
    let (j, _dj) = engine();
    let joined = join_by_pages(&j, &sender);
    assert!(joined.progress.is_complete(), "the join completes");
    assert_eq!(
        joined.restarts.iter().sum::<usize>(),
        0,
        "an event at the grant is covered by it: no restart"
    );
}

// -- Unit E, pinned: a stage above the receiver's own drop seeds nothing, so the restart has no seed of the receiver's to keep -----------
//
// Page one of the first pass makes a stage of `orders`, created ABOVE this member's own identity tombstone, which is above the first grant
// (`judge_a_stage_a_page_made`). A stage created above a drop is one the drop never concerned (ADR-272, decision 6), so the page seeds no
// logical drop for it, and the restart that follows has none of the receiver's to keep: nothing of the name's register is the receiver's
// own judgement that the sender never made. Before the second pass the sender takes that same drop as an ORDINARY one (the stage of the
// name was created above it, and no event of the name is in effect there), so neither holds an event of the name, the two read the name
// absent, and the registers end EQUAL once the next event of the name reaches both: each seeds the drop from its own tombstone when it
// judges that event, a binding, which asks as one.
//
// The restart itself is caused by another name's swap (`items`), made by the sender's page reader between the first two pages, as in the
// tests above, so that the name under test has no event on the sender when the second pass starts.

const SEEDED_STAGE: &str = "orders.__stg.g1";
const ITEMS_STAGES: [&str; 2] = ["items.__stg.v1", "items.__stg.v2"];

fn bind_of_name(by: &Engine, name: &str, physical: &str, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        kind: OpKind::BindCollection,
        collection: CollectionId::derive(DB, name),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: name.into(),
                physical: physical.into(),
            })
            .unwrap(),
        ),
        meta: None,
    }
}

#[test]
fn a_stage_above_the_receivers_drop_seeds_none_and_the_ends_equal_at_the_names_next_event() {
    let (r, _dr) = engine();
    let (a, _da) = engine();
    let (b, _db) = engine();
    let (sender, _ds) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    let orders_id = CollectionId::derive(DB, "orders");

    // the receiver's own life of `orders`, which the sender holds too, and which the receiver then drops: its tombstone at t
    r.create_collection(DB, "orders").unwrap();
    sender.apply_batch(&everything(&r)).unwrap();
    assert!(r.drop_collection(DB, "orders").unwrap(), "premise: the receiver dropped its identity");
    let t = r.collection_dropped_at(orders_id).unwrap().expect("premise: the receiver's tombstone");
    let drop_entry = everything(&r)
        .into_iter()
        .find(|entry| entry.kind == OpKind::DropCollection && entry.collection == orders_id)
        .expect("premise: the receiver's drop is an entry");
    std::thread::sleep(std::time::Duration::from_millis(5));

    // the stage of `orders` the first page makes, created above t, held alive by the sender; and the swap of `items` that causes the restart
    a.create_stage(DB, SEEDED_STAGE, MergePolicy::Document, deadline).unwrap();
    for (k, name) in ITEMS_STAGES.iter().enumerate() {
        let stage = a.create_stage(DB, name, MergePolicy::Document, deadline).unwrap();
        for i in 1..=30 {
            let id = i64::try_from(k).unwrap() * 100 + i;
            a.insert(&stage, doc! { "_id": id, "v": "x".repeat(40) }).unwrap();
        }
    }
    let entries_of = |name: &str| -> Vec<OplogEntry> {
        let id = CollectionId::derive(DB, name);
        everything(&a).into_iter().filter(|entry| entry.collection == id).collect()
    };
    let seeded_creation = creation_of(&a, SEEDED_STAGE);
    assert!(seeded_creation.stamp > t, "premise: the stage of `orders` was created above the drop");
    sender.apply_batch(&entries_of(SEEDED_STAGE)).unwrap();
    sender.apply_batch(&entries_of(ITEMS_STAGES[0])).unwrap();
    let wall = [seeded_creation.stamp.hlc.wall_ms, t.hlc.wall_ms, physical_now_ms()]
        .into_iter()
        .max()
        .unwrap()
        + 1;
    window(&sender, std::slice::from_ref(&bind_of_name(&b, "items", ITEMS_STAGES[0], wall)));
    assert!(sender.get_collection(DB, "orders").is_ok(), "premise: the sender holds the identity");
    assert!(the_events_of_orders(&sender).is_empty(), "premise: and no event of `orders`");

    // the sender's page reader, before the second page of the first pass: the drop reaches it and is taken as an ordinary one, and `items`
    // is swapped again
    let swap = (entries_of(ITEMS_STAGES[1]), bind_of_name(&b, "items", ITEMS_STAGES[1], wall + 10));
    let (page_ones, made) =
        (std::sync::atomic::AtomicUsize::new(0), std::sync::atomic::AtomicBool::new(false));
    sender.set_snapshot_room_for_test(Some(3_000));
    // read before the hook is set, so that it does not count this page
    assert!(
        sender.snapshot_page(None, None).unwrap().next.is_some(),
        "premise: page one is not the last, so a page with a cursor is asked for"
    );
    let with_a_cursor = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let cursor_calls = with_a_cursor.clone();
    let hook = move |engine: &Engine, after: Option<&crate::SnapshotCursor>| {
        use std::sync::atomic::Ordering::SeqCst;
        if after.is_none() {
            page_ones.fetch_add(1, SeqCst);
        } else {
            cursor_calls.fetch_add(1, SeqCst);
        }
        if after.is_some() && page_ones.load(SeqCst) == 1 && !made.swap(true, SeqCst) {
            window(engine, std::slice::from_ref(&drop_entry));
            engine.apply_batch(&swap.0).unwrap();
            window(engine, std::slice::from_ref(&swap.1));
        }
    };
    sender.set_snapshot_reader_hook_for_test(Some(std::sync::Arc::new(hook)));

    let joined = join_by_pages(&r, &sender);
    assert!(
        with_a_cursor.load(std::sync::atomic::Ordering::SeqCst) >= 1,
        "premise: the sender's page reader ran at least once with a cursor"
    );
    assert!(joined.progress.is_complete(), "the second pass completes");
    assert_eq!(
        joined.restarts.iter().sum::<usize>(),
        1,
        "premise: the join restarted once, for `items`: {:?}",
        joined.restarts
    );
    assert_eq!(
        sender.collection_dropped_at(orders_id).unwrap(),
        Some(t),
        "premise: the sender took the drop, at the receiver's stamp"
    );
    assert!(
        sender.get_collection(DB, "orders").is_err(),
        "premise: as an ordinary drop of the identity"
    );

    // neither holds an event of the name: the stage is above the drop, so page one seeded none, and both read the name absent
    assert!(
        the_events_of_orders(&r).is_empty(),
        "the receiver holds no event of the name: a stage above its drop seeded none"
    );
    assert!(the_events_of_orders(&sender).is_empty(), "the sender holds no event of the name");
    assert!(served_by(&r).is_err(), "the receiver reads the name absent");
    assert!(served_by(&sender).is_err(), "and so does the sender");

    // the next event of the name, a binding of the stage, reaches both
    let bind = bind_of(&b, SEEDED_STAGE, wall + 100);
    window(&r, std::slice::from_ref(&bind));
    window(&sender, std::slice::from_ref(&bind));
    assert_eq!(served_by(&r), Ok(SEEDED_STAGE.to_string()), "the receiver serves the stage");
    assert_eq!(served_by(&sender), Ok(SEEDED_STAGE.to_string()), "and so does the sender");
    assert_eq!(
        the_events_of_orders(&r),
        the_events_of_orders(&sender),
        "and the two registers are equal: the sender seeds the drop from its own tombstone when it judges the binding"
    );
}

// -- A name with no register event resolves through the identity alone (the common case, an unswapped name) ----------------------------------

/// `resolve_computed` for `name`, in a read transaction of its own: the truth the answer of a name with no event must equal.
fn computed(m: &Engine, name: &str) -> crate::Result<crate::Resolved> {
    use redb::ReadableDatabase;
    let txn = m.db().begin_read().unwrap();
    Engine::resolve_computed(&txn, DB, name)
}

/// What an answer says, in a form two answers compare by: the physical, the collection's id and the visible stamp, or the error's text.
fn said(answer: crate::Result<crate::Resolved>) -> Result<(String, u64, Option<Stamp>), String> {
    answer.map(|r| (r.physical, r.meta.id.0, r.visible)).map_err(|e| e.to_string())
}

#[test]
fn a_name_with_no_event_whose_collection_is_held_answers_what_the_computation_answers() {
    let (m, _dm) = with_orders();
    assert!(the_events_of_orders(&m).is_empty(), "premise: the name has no event");
    assert!(
        computed(&m, "orders").is_ok(),
        "premise: the identity is held, so the computation answers it"
    );
    assert_eq!(said(m.resolve(DB, "orders")), said(computed(&m, "orders")));
    assert_eq!(m.register_resolve_fallbacks(), 0, "and it is not a fallback");
}

#[test]
fn a_name_with_no_event_and_no_collection_answers_what_the_computation_answers() {
    let (m, _dm) = with_orders();
    assert!(computed(&m, "ghost").is_err(), "premise: the computation finds the name absent");
    let answer = said(m.resolve(DB, "ghost"));
    assert!(answer.is_err(), "the name is absent: {answer:?}");
    assert_eq!(answer, said(computed(&m, "ghost")), "with the computation's own error");
    assert_eq!(said(m.resolve_for_write(DB, "ghost")), answer, "and a write is refused the same");
    assert_eq!(m.register_resolve_fallbacks(), 0, "and it is not a fallback");
}

#[test]
fn a_name_whose_only_event_is_parked_answers_as_the_computation_does() {
    let s = swap(&[]);
    let (parked_here, _dp) = engine();
    parked_here.apply_batch(std::slice::from_ref(&s.creation)).unwrap();
    parked_here.apply_batch(std::slice::from_ref(&s.bind)).unwrap();
    assert!(
        the_binding_is_parked_only(&parked_here),
        "premise: the binding is known only ahead of its position"
    );
    assert_eq!(said(parked_here.resolve(DB, "orders")), said(computed(&parked_here, "orders")));
    assert!(
        parked_here.resolve(DB, "orders").is_err(),
        "the name is absent: a parked event moves nothing"
    );
}

#[test]
fn a_name_with_no_event_opens_the_register_once() {
    let (m, _dm) = with_orders();
    let before = crate::register::register_opened_for_tests();
    m.resolve(DB, "orders").unwrap();
    assert_eq!(
        crate::register::register_opened_for_tests() - before,
        1,
        "a name with no event is answered by the one probe for its newest event, with no timeline read"
    );
    let before = crate::register::register_opened_for_tests();
    assert!(m.resolve(DB, "ghost").is_err());
    assert_eq!(
        crate::register::register_opened_for_tests() - before,
        1,
        "and so is an absent name"
    );
    // the counter counts: the computation opens the register for the timeline, and a name with events reads its row after the probe
    let before = crate::register::register_opened_for_tests();
    computed(&m, "orders").unwrap();
    assert_eq!(
        crate::register::register_opened_for_tests() - before,
        1,
        "premise: the computation's timeline read moves the counter"
    );
    let s = swap(&[]);
    let before = crate::register::register_opened_for_tests();
    assert_eq!(
        s.sender.resolve(DB, "orders").unwrap().physical,
        STAGE1,
        "a swapped name answers its stage"
    );
    assert_eq!(
        crate::register::register_opened_for_tests() - before,
        1,
        "premise: a name with events is answered by its row after the one probe"
    );
}
