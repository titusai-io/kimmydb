//! The swap, burial and loss counters of a replacing `$out` (ADR-272; Q6.5, C2): `kimmy_out_swaps_total`,
//! `kimmy_out_buried_total{cause}` and `kimmy_out_loss_records_total{kind}`.
//!
//! The rule under test: a count is made by the commit that finished the thing counted, never by the settle pass, which can still abort and
//! is then delivered again. A swap is counted once per binding, when that binding took effect in the transaction and is the event the name
//! answers from when the pass ends. The cause of a burial is read from the pass: a physical the pass buried because a later event
//! superseded it is `bound_out`, a stage that can never be visible is `doomed`, a physical a logical drop ended is `drop`.
//!
//! Each count is this member's own: two members that meet the same events in another order can count differently, and the tests say so by
//! reading one member at a time.

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, CollectionRef, Hlc, MergePolicy, OpKind, OplogEntry, Stamp,
    VersionVector,
};

use crate::{Engine, PeerWindow, SyncOutcome, WalkScope, WriterHolder, physical_now_ms};

const DB: &str = "app";
/// A stage with a short deadline: doomed by any event stamped above it that makes another physical visible.
const S1: &str = "orders.__stg.s1";
/// The stage the tests bind first.
const S2: &str = "orders.__stg.s2";
/// A second long-lived stage.
const S3: &str = "orders.__stg.s3";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    (Engine::open(&dir.path().join("kimmy.redb")).expect("an engine opens"), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background)
        .expect("the oplog is readable")
        .entries
}

fn try_window(b: &Engine, entries: &[OplogEntry]) -> crate::Result<SyncOutcome> {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = b.witnessed_vector()?;
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
    )?;
    Ok(outcome)
}

fn window(b: &Engine, entries: &[OplogEntry]) -> SyncOutcome {
    try_window(b, entries).expect("the window applies")
}

fn creation_of(a: &Engine, physical: &str) -> OplogEntry {
    everything(a)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, physical)
        })
        .expect("the stage's creation is in the author's oplog")
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
            .expect("the binding encodes"),
        ),
        meta: None,
    }
}

/// A drop of `name`, as the author `a` would mint it at `wall_ms`.
fn drop_of(a: &Engine, name: &str, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), a.node_id(), a.epoch()),
        kind: OpKind::DropCollection,
        collection: CollectionId::derive(DB, name),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionRef::new(DB, name)).expect("the drop encodes"),
        ),
        meta: None,
    }
}

/// The author of three stages (S1 with a short deadline, S2 and S3 with long ones), the bindings of S2 and S3, and a drop of the name
/// stamped above both bindings.
struct World {
    a: Engine,
    s1: OplogEntry,
    s2: OplogEntry,
    s3: OplogEntry,
    bind_s2: OplogEntry,
    bind_s3: OplogEntry,
    drop: OplogEntry,
    _dir: tempfile::TempDir,
}

fn world() -> World {
    let (a, dir) = engine();
    let now = physical_now_ms();
    a.create_stage(DB, S1, MergePolicy::Document, Hlc::new(now + 5_000, 0))
        .expect("the short-lived stage is created");
    a.create_stage(DB, S2, MergePolicy::Document, Hlc::new(now + 120_000, 0))
        .expect("the first long-lived stage is created");
    a.create_stage(DB, S3, MergePolicy::Document, Hlc::new(now + 121_000, 0))
        .expect("the second long-lived stage is created");
    let (s1, s2, s3) = (creation_of(&a, S1), creation_of(&a, S2), creation_of(&a, S3));
    let bind_s2 = bind_of(&a, S2, now + 20_000);
    let bind_s3 = bind_of(&a, S3, now + 30_000);
    let drop = drop_of(&a, "orders", now + 40_000);
    World { a, s1, s2, s3, bind_s2, bind_s3, drop, _dir: dir }
}

/// A member holding `orders` with a document.
fn with_orders() -> (Engine, tempfile::TempDir) {
    let (m, d) = engine();
    let orders = m.create_collection(DB, "orders").expect("the identity is created");
    m.insert(&orders, doc! { "_id": 1, "v": "old" }).expect("the identity takes a document");
    (m, d)
}

/// A member holding `orders` and the creations `stages` of the world's stages.
fn member_with(stages: &[&OplogEntry]) -> (Engine, tempfile::TempDir) {
    let (m, d) = with_orders();
    for creation in stages {
        window(&m, std::slice::from_ref(*creation));
    }
    (m, d)
}

/// `[bound_out, doomed, orphan, drop]`, the order of `OUT_BURIAL_CAUSES`.
fn buried(e: &Engine) -> [u64; 4] {
    let counts = e.out_buried();
    assert_eq!(
        counts.iter().map(|(cause, _)| *cause).collect::<Vec<_>>(),
        crate::OUT_BURIAL_CAUSES,
        "the causes read in the order the series renders them"
    );
    std::array::from_fn(|slot| counts[slot].1)
}

fn physical_served(e: &Engine) -> String {
    e.resolve(DB, "orders").expect("the name resolves").physical
}

fn register_events(e: &Engine) -> Vec<Stamp> {
    let txn = e.begin_write(WriterHolder::Ddl).expect("a writer");
    let events = crate::register::timeline(&txn, DB, "orders")
        .expect("the register is readable")
        .events()
        .iter()
        .map(|(stamp, _)| *stamp)
        .collect();
    txn.abort().expect("the read transaction ends");
    events
}

fn granted(stamp: Stamp) -> VersionVector {
    let mut vector = VersionVector::default();
    vector.observe(stamp);
    vector
}

#[test]
fn a_binding_that_becomes_visible_counts_one_swap_and_one_bound_out_burial_and_a_later_stage_creation_counts_none()
 {
    let w = world();
    let (m, _d) = member_with(&[&w.s2]);
    assert_eq!((m.out_swaps(), buried(&m)), (0, [0; 4]), "premise: nothing counted yet");
    window(&m, std::slice::from_ref(&w.bind_s2));
    assert_eq!(physical_served(&m), S2, "premise: the binding is visible");
    assert_eq!(m.out_swaps(), 1, "one binding became visible: one swap");
    assert_eq!(buried(&m), [1, 0, 0, 0], "the identity it buried is `bound_out`, and nothing else");
    // another event of the same name: the creation of a later stage runs the settle pass again over the same visible binding
    window(&m, std::slice::from_ref(&w.s3));
    assert_eq!(physical_served(&m), S2, "premise: the binding is still the one visible");
    assert_eq!(m.out_swaps(), 1, "the same binding, seen by the pass again, is not a second swap");
    assert_eq!(buried(&m), [1, 0, 0, 0], "and buries nothing more");
}

#[test]
fn a_binding_counts_one_swap_whether_a_window_a_later_window_or_a_grant_made_it_take_effect() {
    let w = world();
    // in a window
    let (in_window, _d1) = member_with(&[&w.s2]);
    window(&in_window, std::slice::from_ref(&w.bind_s2));
    // known ahead of its position, then released by a window
    let (by_window, _d2) = member_with(&[&w.s2]);
    by_window.apply_batch(std::slice::from_ref(&w.bind_s2)).expect("the binding is stored ahead");
    assert_eq!(
        (physical_served(&by_window), by_window.out_swaps(), buried(&by_window)),
        ("orders".to_string(), 0, [0; 4]),
        "premise: only information, so nothing is counted"
    );
    window(&by_window, std::slice::from_ref(&w.bind_s2));
    // known ahead of its position, then released by a snapshot grant
    let (by_grant, _d3) = member_with(&[&w.s2]);
    by_grant.apply_batch(std::slice::from_ref(&w.bind_s2)).expect("the binding is stored ahead");
    assert_eq!(by_grant.out_swaps(), 0, "premise: only information, so nothing is counted");
    by_grant.absorb_version_vector(&granted(w.bind_s2.stamp)).expect("the grant is absorbed");
    for (route, m) in
        [("a window", &in_window), ("a later window", &by_window), ("a grant", &by_grant)]
    {
        assert_eq!(physical_served(m), S2, "{route}: the binding is visible");
        assert_eq!(m.out_swaps(), 1, "{route}: one swap");
        assert_eq!(buried(m), [1, 0, 0, 0], "{route}: the identity is buried as `bound_out`");
    }
}

#[test]
fn a_binding_that_is_stored_and_not_visible_counts_no_swap() {
    let w = world();
    let (m, _d) = member_with(&[&w.s2]);
    // the stage is buried before its binding arrives (that burial is a drop's, counted in its own test below, and not the binding's)
    let stage_drop = drop_of(&w.a, S2, w.s2.stamp.hlc.wall_ms + 5);
    window(&m, std::slice::from_ref(&stage_drop));
    assert!(m.get_collection(DB, S2).is_err(), "premise: the stage is buried");
    let (swaps_before, buried_before) = (m.out_swaps(), buried(&m));
    window(&m, std::slice::from_ref(&w.bind_s2));
    assert!(
        register_events(&m).contains(&w.bind_s2.stamp),
        "premise: the binding's row is in the register"
    );
    assert_eq!(
        physical_served(&m),
        "orders",
        "premise: and the name does not resolve to its stage"
    );
    assert_eq!(
        m.out_swaps(),
        swaps_before,
        "a binding that took effect and is not visible is not a swap here"
    );
    assert_eq!(buried(&m), buried_before, "and the binding's window buried nothing");
}

#[test]
fn a_replicated_drop_of_a_stage_by_its_name_counts_one_drop_burial() {
    // `drop_keeping_later_writes` is where every drop of a name or of a database buries a held stage (a window, the database sweep, a
    // snapshot page): a stage ended by a drop is `drop`, by whichever route the drop arrives.
    let w = world();
    let (m, _d) = member_with(&[&w.s2]);
    assert_eq!(buried(&m), [0; 4], "premise: nothing counted yet");
    let stage_drop = drop_of(&w.a, S2, w.s2.stamp.hlc.wall_ms + 5);
    window(&m, std::slice::from_ref(&stage_drop));
    assert!(m.get_collection(DB, S2).is_err(), "premise: the stage is buried by the drop");
    assert_eq!(
        buried(&m),
        [0, 0, 0, 1],
        "a stage buried by a drop of its name counts as `drop`, once"
    );
    assert_eq!(m.out_swaps(), 0, "and a drop is no swap");
}

#[test]
fn a_stage_that_can_never_be_visible_counts_as_doomed_and_not_as_bound_out() {
    let w = world();
    let (m, _d) = member_with(&[&w.s1, &w.s2]);
    assert!(m.get_collection(DB, S1).is_ok(), "premise: the short-lived stage is held");
    window(&m, std::slice::from_ref(&w.bind_s2));
    assert!(m.get_collection(DB, S1).is_err(), "premise: it was buried with the binding");
    assert_eq!(m.out_swaps(), 1, "the binding that buried the doomed stage is one swap");
    assert_eq!(
        buried(&m),
        [1, 1, 0, 0],
        "the identity is `bound_out` and the stage no event can make visible is `doomed`, one each"
    );
}

/// **A stand-in of two engines.** A replay stops behind the first event of an origin it parks, so no member holds two parked events of
/// one origin; the third stage and its binding are another engine's. On a real member two parked events can only be of one node
/// under two epochs: two engines differ in the node as well, which this test does not tell apart.
#[test]
fn a_logical_drop_that_buries_two_physicals_in_one_pass_counts_both_as_drop() {
    let w = world();
    let (b, _db) = engine();
    b.create_stage(DB, S3, MergePolicy::Document, Hlc::new(physical_now_ms() + 121_000, 0))
        .expect("the other engine's stage is created");
    let s3 = creation_of(&b, S3);
    let bind_s3 = bind_of(&b, S3, w.bind_s3.stamp.hlc.wall_ms);
    let (m, _d) = member_with(&[&w.s2, &s3]);
    // both bindings are known ahead of their position; the drop above them takes effect in a window and ends the identity
    m.apply_batch(&[w.bind_s2.clone(), bind_s3.clone()]).expect("the bindings are stored ahead");
    window(&m, std::slice::from_ref(&w.drop));
    let after_the_drop = (m.out_swaps(), buried(&m));
    assert!(
        m.get_collection(DB, S2).is_ok() && m.get_collection(DB, S3).is_ok(),
        "premise: both stages are still held"
    );
    // a grant covers both bindings: one settle pass finds two physicals of the chain held under a drop
    let mut covered = granted(w.bind_s2.stamp);
    covered.observe(bind_s3.stamp);
    m.absorb_version_vector(&covered).expect("the grant is absorbed");
    assert!(
        m.get_collection(DB, S2).is_err() && m.get_collection(DB, S3).is_err(),
        "premise: the pass buried both stages"
    );
    let (swaps, causes) = (m.out_swaps(), buried(&m));
    assert_eq!(swaps, after_the_drop.0, "a drop makes nothing visible: no swap");
    assert_eq!(
        [causes[0], causes[1], causes[2]],
        [after_the_drop.1[0], after_the_drop.1[1], after_the_drop.1[2]],
        "neither `bound_out`, `doomed` nor `orphan` moved"
    );
    assert_eq!(
        causes[3] - after_the_drop.1[3],
        2,
        "two physicals buried by one pass: `drop` moves by two"
    );
}

#[test]
fn a_local_drop_and_a_replicated_drop_of_a_swapped_name_count_alike() {
    let w = world();
    let (local, _d1) = member_with(&[&w.s2]);
    window(&local, std::slice::from_ref(&w.bind_s2));
    let (replicated, _d2) = member_with(&[&w.s2]);
    window(&replicated, std::slice::from_ref(&w.bind_s2));
    let before = (local.out_swaps(), buried(&local));
    assert_eq!(before, (replicated.out_swaps(), buried(&replicated)), "premise: the same start");
    assert!(
        local.drop_collection(DB, "orders").expect("the local drop runs"),
        "the drop found the name"
    );
    window(&replicated, std::slice::from_ref(&w.drop));
    for (route, m) in [("local", &local), ("replicated", &replicated)] {
        assert!(m.resolve(DB, "orders").is_err(), "{route}: the name is dropped");
        assert_eq!(m.out_swaps(), before.0, "{route}: a drop is not a swap");
        assert_eq!(
            buried(m),
            [before.1[0], before.1[1], before.1[2], before.1[3] + 1],
            "{route}: the bound physical is buried as `drop`, by one"
        );
    }
}

#[test]
fn a_transaction_that_fails_after_the_pass_counts_nothing_and_the_redelivery_counts_once() {
    let w = world();
    let (m, _d) = member_with(&[&w.s2]);
    let passes = crate::sync::race_hooks::settle_passes();
    crate::sync::race_hooks::fail_after_the_next_settle();
    let attempt = try_window(&m, std::slice::from_ref(&w.bind_s2));
    assert!(attempt.is_err(), "premise: the transaction failed: {attempt:?}");
    assert_eq!(
        crate::sync::race_hooks::settle_passes(),
        passes + 1,
        "premise: the pass ran before it failed"
    );
    assert_eq!(physical_served(&m), "orders", "premise: nothing of the binding was committed");
    assert_eq!((m.out_swaps(), buried(&m)), (0, [0; 4]), "an aborted transaction counts nothing");
    window(&m, std::slice::from_ref(&w.bind_s2));
    assert_eq!(physical_served(&m), S2, "the redelivery made the binding visible");
    assert_eq!(m.out_swaps(), 1, "and counts it once");
    assert_eq!(buried(&m), [1, 0, 0, 0], "and its burial once");
}

#[test]
fn a_physical_this_member_never_held_is_not_counted_as_buried() {
    // The pass names the identity as a physical the binding buries; a member that never held it buries nothing, and counts nothing.
    let w = world();
    let (m, _d) = engine();
    window(&m, std::slice::from_ref(&w.s2));
    assert!(
        m.get_collection(DB, "orders").is_err(),
        "premise: this member never held the identity"
    );
    window(&m, std::slice::from_ref(&w.bind_s2));
    assert_eq!(physical_served(&m), S2, "premise: the binding is visible");
    assert_eq!(m.out_swaps(), 1, "the swap is this member's all the same");
    assert_eq!(buried(&m), [0; 4], "a burial that buried nothing is not counted");
}

#[test]
fn the_swap_and_loss_series_nothing_mints_yet_stay_at_zero() {
    let w = world();
    let (m, _d) = member_with(&[&w.s2]);
    window(&m, std::slice::from_ref(&w.bind_s2));
    assert_eq!(
        m.out_loss_records(),
        vec![("late_write", 0), ("snapshot_delete", 0)],
        "no code mints a swap's loss record before Q6.7"
    );
    assert_eq!(buried(&m)[2], 0, "and nothing abandons a stage before Q6.7: `orphan` stays 0");
}

// -- A database's drop buries a held stage whole (late_drop.rs) ---------------------------------------------------------------------------

/// A stage with a row stamped after the drop of its database, on a member that has not heard of the drop: the drop buries the stage whole
/// instead of keeping it for the row (a stage never takes the survivors path).
struct StageUnderDrop {
    m: Engine,
    drop_entries: Vec<OplogEntry>,
    _dirs: Vec<tempfile::TempDir>,
}

fn a_member_holding_a_stage_whose_database_was_dropped() -> StageUnderDrop {
    let (a, da) = engine();
    let (d, dd) = engine();
    d.create_collection(DB, "orders").expect("the dropping member holds the identity");
    a.create_stage(DB, S2, MergePolicy::Document, Hlc::new(physical_now_ms() + 60_000, 0))
        .expect("the stage is created");
    let creation = creation_of(&a, S2);
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(d.drop_database(DB).expect("the database drops"), "premise: the database is dropped");
    std::thread::sleep(std::time::Duration::from_millis(5));
    let stage = a.get_collection(DB, S2).expect("the stage is held by its author");
    a.insert(&stage, doc! { "_id": 9, "v": "staged after the drop" })
        .expect("the stage takes a row stamped after the drop");
    let row = everything(&a)
        .into_iter()
        .find(|e| e.kind == OpKind::Insert)
        .expect("the row is in the author's oplog");
    let (m, dm) = engine();
    window(&m, &[creation, row]);
    assert!(m.get_collection(DB, S2).is_ok(), "premise: the stage is alive on the member");
    StageUnderDrop { m, drop_entries: everything(&d), _dirs: vec![da, dd, dm] }
}

#[test]
fn a_database_drop_over_a_held_stage_counts_the_stage_as_drop_by_one() {
    let s = a_member_holding_a_stage_whose_database_was_dropped();
    assert_eq!((s.m.out_swaps(), buried(&s.m)), (0, [0; 4]), "premise: nothing counted yet");
    window(&s.m, &s.drop_entries);
    assert!(s.m.get_collection(DB, S2).is_err(), "premise: the stage is buried whole at the drop");
    assert_eq!(buried(&s.m), [0, 0, 0, 1], "the stage's burial counts as `drop`, once");
    assert_eq!(s.m.out_swaps(), 0, "and a drop is no swap");
}

#[test]
fn a_database_drop_whose_stage_burial_fails_before_its_commit_counts_nothing_and_the_redelivery_counts_once()
 {
    let s = a_member_holding_a_stage_whose_database_was_dropped();
    let reached = crate::sync::race_hooks::stage_burials_before_commit();
    crate::sync::race_hooks::fail_the_next_stage_burial();
    let attempt = try_window(&s.m, &s.drop_entries);
    assert_eq!(
        crate::sync::race_hooks::stage_burials_before_commit(),
        reached + 1,
        "premise: the burial reached the point before its commit ({attempt:?})"
    );
    assert!(
        s.m.get_collection(DB, S2).is_ok(),
        "premise: the transaction did not commit, so the stage stands"
    );
    assert_eq!(buried(&s.m), [0; 4], "a burial that did not commit counts nothing");
    window(&s.m, &s.drop_entries);
    assert!(s.m.get_collection(DB, S2).is_err(), "the redelivered drop buries the stage");
    assert_eq!(buried(&s.m), [0, 0, 0, 1], "and counts it once");
}

#[test]
fn a_database_drop_over_a_plain_collection_counts_no_burial_of_a_swap() {
    // Only a stage's burial by a database's drop is a swap's: a collection that was never staged goes the same way and is not counted.
    // The receiving member's life of `orders` is created BEFORE the drop and holds nothing stamped after it, so the drop buries it whole
    // (the branch of `drop_keeping_later_writes`, late_drop.rs, taken when nothing is stamped after the drop). A life created after the
    // drop would survive it (ADR-229).
    let (m, _dm) = with_orders();
    std::thread::sleep(std::time::Duration::from_millis(5));
    let (d, _dd) = engine();
    d.create_collection(DB, "orders").expect("the dropping member holds the collection");
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(d.drop_database(DB).expect("the database drops"), "premise: the database is dropped");
    window(&m, &everything(&d));
    assert!(
        m.get_collection(DB, "orders").is_err(),
        "premise: the collection is buried by the drop"
    );
    assert_eq!(
        buried(&m),
        [0; 4],
        "a plain collection's burial is not a swap's: counted by no cause"
    );
}
