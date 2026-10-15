//! A life that began after a drop ends the life standing here that the drop ended,
//! when no tombstone arrives to say so (ADR-235, B4 item 1).
//!
//! The case is the sender's drop tombstone aged out: the receiver holds life 1, the
//! sender dropped it and made life 2, and a snapshot of life 2 carries nothing about
//! the drop. Before, the receiver kept life 1 and applied life 2's documents into it:
//! one collection made of two lives. The evidence now is the sender's incarnation
//! floor on the page. With none, or with a floor the standing life was made after,
//! nothing is buried: two members that create one name with no drop between them are
//! not a life ended.

use bson::doc;
use kimmy_core::{CollectionId, DocId, Hlc, OpKind, Stamp};

use crate::{Engine, SnapshotProgress, WalkScope};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn transfer_under(into: &Engine, from: &Engine, mut progress: SnapshotProgress) {
    let mut waited = false;
    while !progress.is_complete() {
        let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        let outcome = into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
        if outcome.purge_pending.is_some() {
            assert!(!waited, "a purge that never finishes");
            waited = true;
            into.finish_purges_now().unwrap();
            continue;
        }
        waited = false;
    }
}

fn transfer(into: &Engine, from: &Engine) {
    transfer_under(into, from, SnapshotProgress::whole_database());
}

fn pull(into: &Engine, from: &Engine) {
    let entries = from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    into.apply_batch(&entries).unwrap();
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn orders_id() -> CollectionId {
    CollectionId::derive("shop", "orders")
}

fn forget_the_tombstone(sender: &Engine) {
    let txn = sender.begin_write(crate::WriterHolder::Ddl).unwrap();
    txn.open_table(crate::tables::COLLECTIONS_DROPPED).unwrap().remove(orders_id().0).unwrap();
    txn.commit().unwrap();
    assert!(sender.collection_dropped_at(orders_id()).unwrap().is_none(), "premise");
}

fn has(e: &Engine, id: i64) -> bool {
    let coll = e.get_collection("shop", "orders").unwrap();
    e.get(&coll, &DocId::Int64(id)).unwrap().is_some()
}

/// The sender dropped life 1 and made life 2; the receiver holds life 1 (taken by
/// snapshot, with document 1). Returns `(sender, receiver, dirs)`.
fn receiver_holds_the_previous_life() -> (Engine, Engine, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    let first = a.create_collection("shop", "orders").unwrap();
    a.insert(&first, doc! { "_id": 1 }).unwrap();
    transfer(&b, &a);
    assert!(has(&b, 1), "premise");
    later();
    a.drop_collection("shop", "orders").unwrap();
    a.finish_purges_now().unwrap();
    later();
    let second = a.create_collection("shop", "orders").unwrap();
    a.insert(&second, doc! { "_id": 2 }).unwrap();
    assert_ne!(first.created, second.created, "premise: two lives");
    (a, b, [da, db])
}

fn assert_holds_the_senders_life(a: &Engine, b: &Engine, what: &str) {
    let held = b.get_collection("shop", "orders").unwrap();
    let want = a.get_collection("shop", "orders").unwrap();
    assert_eq!(held.created, want.created, "{what}: the receiver keeps the life that ended");
    assert!(!has(b, 1), "{what}: the previous life's document is still here");
    assert!(has(b, 2), "{what}: the new life's document did not land");
}

#[test]
fn a_whole_database_snapshot_without_the_tombstone_replaces_the_previous_life() {
    let (a, b, _dirs) = receiver_holds_the_previous_life();
    forget_the_tombstone(&a);
    transfer(&b, &a);
    assert_holds_the_senders_life(&a, &b, "whole-database");
}

#[test]
fn a_scoped_snapshot_without_the_tombstone_replaces_the_previous_life() {
    let (a, b, _dirs) = receiver_holds_the_previous_life();
    forget_the_tombstone(&a);
    transfer_under(&b, &a, SnapshotProgress::of_collection(orders_id()));
    assert_holds_the_senders_life(&a, &b, "scoped");
}

#[test]
fn with_the_tombstone_held_it_still_converges() {
    let (a, b, _dirs) = receiver_holds_the_previous_life();
    transfer(&b, &a);
    assert_holds_the_senders_life(&a, &b, "tombstone carried");
}

#[test]
fn the_floor_the_receiver_ends_with_is_the_senders() {
    let (a, b, _dirs) = receiver_holds_the_previous_life();
    forget_the_tombstone(&a);
    transfer(&b, &a);
    assert_eq!(
        b.get_collection("shop", "orders").unwrap().incarnation_floor,
        a.get_collection("shop", "orders").unwrap().incarnation_floor,
        "stragglers of the ended life stay history on the receiver too"
    );
}

#[test]
fn a_write_the_receiver_made_after_the_drop_is_kept() {
    // ADR-231, extended: the burial is the drop's, so what was written after it stays.
    let (a, b, _dirs) = receiver_holds_the_previous_life();
    forget_the_tombstone(&a);
    let cb = b.get_collection("shop", "orders").unwrap();
    b.insert(&cb, doc! { "_id": 9 }).unwrap();
    transfer(&b, &a);
    assert!(!has(&b, 1), "the previous life's document is still here");
    assert!(has(&b, 9), "a write stamped after the drop was buried with the life");
    assert!(has(&b, 2));
    // And where it ends: the ADR-231 shape, pinned. The life is re-dated above the drop
    // (the receiver keeps its own write), with the sender's floor, not at the sender's
    // `created`.
    let held = b.get_collection("shop", "orders").unwrap();
    let sender = a.get_collection("shop", "orders").unwrap();
    assert!(held.is_redated(), "a life that kept a write after the drop is re-dated");
    assert_eq!(held.incarnation_floor, sender.incarnation_floor, "the sender's floor");
}

#[test]
fn a_page_without_a_floor_buries_nothing() {
    // A sender that predates the field, or a life with no drop behind it.
    let (a, b, _dirs) = receiver_holds_the_previous_life();
    forget_the_tombstone(&a);
    let mut page = a.snapshot_page(None, Some(orders_id())).unwrap();
    for state in &mut page.collections {
        state.incarnation_floor = None;
    }
    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::of_collection(orders_id()), &page)
        .unwrap();
    assert!(has(&b, 1), "a life was buried on no evidence");
}

#[test]
fn a_floor_the_senders_coverage_does_not_name_buries_nothing() {
    let (a, b, _dirs) = receiver_holds_the_previous_life();
    forget_the_tombstone(&a);
    let far = Hlc::new(crate::physical_now_ms() + 86_400_000, 0);
    let mut page = a.snapshot_page(None, Some(orders_id())).unwrap();
    for state in &mut page.collections {
        state.incarnation_floor = Some(far);
        state.created = Some(Hlc::new(crate::physical_now_ms() + 86_400_001, 0));
    }
    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::of_collection(orders_id()), &page)
        .unwrap();
    assert!(has(&b, 1), "a life was buried on a floor nothing vouched for");
}

/// `a` creates `shop.orders` (document 2) and `b`, later, creates it again (document 1):
/// two first lives of one name, with no drop between them.
fn two_members_that_created_one_name() -> (Engine, Engine, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    let on_a = a.create_collection("shop", "orders").unwrap();
    a.insert(&on_a, doc! { "_id": 2 }).unwrap();
    later();
    let on_b = b.create_collection("shop", "orders").unwrap();
    b.insert(&on_b, doc! { "_id": 1 }).unwrap();
    let created = |e: &Engine| e.get_collection("shop", "orders").unwrap().created;
    assert!(created(&a) < created(&b), "premise: a made it first");
    assert!(a.get_collection("shop", "orders").unwrap().incarnation_floor.is_none(), "premise");
    (a, b, [da, db])
}

fn created_of(e: &Engine) -> Hlc {
    e.get_collection("shop", "orders").unwrap().created
}

#[test]
fn two_members_that_create_one_name_with_no_drop_between_them_bury_nothing() {
    let (a, b, _dirs) = two_members_that_created_one_name();
    transfer(&b, &a);
    assert!(has(&b, 1), "a concurrent create was buried");
    assert!(has(&b, 2));
}

#[test]
fn a_page_from_the_newer_creation_leaves_the_older_alone() {
    let (a, b, _dirs) = two_members_that_created_one_name();
    let before = created_of(&a);
    transfer(&a, &b);
    assert_eq!(created_of(&a), before);
    assert!(has(&a, 1) && has(&a, 2));
}

#[test]
fn a_life_made_after_the_drop_the_receiver_already_knows_buries_nothing() {
    // The receiver made its life after the sender's drop (floor below the receiver's
    // creation), however it learned of the drop: that life is not the one the drop ended.
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.create_collection("shop", "orders").unwrap();
    a.drop_collection("shop", "orders").unwrap();
    a.finish_purges_now().unwrap();
    later();
    a.create_collection("shop", "orders").unwrap();
    forget_the_tombstone(&a);
    later();
    let on_b = b.create_collection("shop", "orders").unwrap();
    b.insert(&on_b, doc! { "_id": 1 }).unwrap();
    let floor = a.get_collection("shop", "orders").unwrap().incarnation_floor.unwrap();
    assert!(floor < on_b.created, "premise: the receiver's life is after the drop");
    transfer(&b, &a);
    assert!(has(&b, 1), "a life made after the drop was buried");
}

#[test]
fn a_standing_life_newer_than_the_arriving_one_is_not_buried() {
    // A sender that is behind: its life 2 against the receiver's life 3.
    let (a, b, _dirs) = receiver_holds_the_previous_life();
    forget_the_tombstone(&a);
    transfer(&b, &a);
    later();
    b.drop_collection("shop", "orders").unwrap();
    b.finish_purges_now().unwrap();
    later();
    let third = b.create_collection("shop", "orders").unwrap();
    b.insert(&third, doc! { "_id": 3 }).unwrap();
    forget_the_tombstone(&a);
    transfer(&b, &a);
    assert_eq!(b.get_collection("shop", "orders").unwrap().created, third.created);
    assert!(has(&b, 3), "a newer life was buried by a sender that is behind");
}

#[test]
fn a_life_re_dated_by_the_drop_is_not_mistaken_for_a_newer_one() {
    // ADR-231, extended: `m` keeps a write made after the drop, so its life is dated
    // just above the drop, with the creation it had before in `redated_from`. A member
    // still holding the original life (created at that creation) is looking at the same
    // life continuing, and the drop reaches it by its own route.
    let (a, m) = {
        let (a, _da) = engine();
        let (m, _dm) = engine();
        // Leak the dirs for the test's duration: the engines are returned.
        std::mem::forget(_da);
        std::mem::forget(_dm);
        (a, m)
    };
    let on_a = a.create_collection("shop", "orders").unwrap();
    a.insert(&on_a, doc! { "_id": 1 }).unwrap();
    pull(&m, &a);
    let (p, _dp) = engine();
    pull(&p, &a);
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    let on_m = m.get_collection("shop", "orders").unwrap();
    m.insert(&on_m, doc! { "_id": 3 }).unwrap();
    pull(&m, &a);
    let kept = m.get_collection("shop", "orders").unwrap();
    assert!(kept.is_redated(), "premise");
    assert!(kept.incarnation_floor.is_some(), "premise: the floor is set");
    let page = m.snapshot_page(None, Some(orders_id())).unwrap();
    let state = &page.collections[0];
    assert_eq!(state.redated_from, Some(on_a.created), "premise");
    assert!(state.incarnation_floor.is_some(), "premise: the page says so");
    let before = p.get_collection("shop", "orders").unwrap().created;
    p.apply_snapshot_page(m.node_id(), &mut SnapshotProgress::of_collection(orders_id()), &page)
        .unwrap();
    assert_eq!(
        p.get_collection("shop", "orders").unwrap().created,
        before,
        "a re-dated life was taken for a newer one and the standing life buried"
    );
    // What the page brings is the sender's content (its sweep deleted the document the
    // drop ended, and that delete travels), not a replacement of the life.
    assert!(has(&p, 3), "the page's write after the drop did not land");
    assert!(!has(&p, 1), "the document the drop ended is still here");
}

// -- The entries route ---------------------------------------------------------------------

#[test]
fn a_create_that_arrives_before_the_drop_it_followed_converges_once_the_drop_arrives() {
    // The realistic shape of the entries route (ADR-235): two origins. `y` drops life 1;
    // `x`, having seen the drop, makes life 2 and writes into it; `b` gets x's entries
    // first (the create finds life 1 standing and does nothing) and y's drop later, within
    // the window, and ADR-231's sweep ends the old life and keeps what x wrote after the
    // drop. The drop is never withheld for good: beyond the horizon a member takes a
    // whole-database snapshot, which is the page route above.
    let (y, _dy) = engine();
    let (x, _dx) = engine();
    let (b, _db) = engine();
    let first = y.create_collection("shop", "orders").unwrap();
    y.insert(&first, doc! { "_id": 1 }).unwrap();
    pull(&x, &y);
    pull(&b, &y);
    assert!(has(&b, 1) && has(&x, 1), "premise");
    later();
    assert!(y.drop_collection("shop", "orders").unwrap());
    later();
    pull(&x, &y);
    assert!(x.get_collection("shop", "orders").is_err(), "premise: x heard of the drop");
    x.finish_purges_now().unwrap();
    later();
    let second = x.create_collection("shop", "orders").unwrap();
    x.insert(&second, doc! { "_id": 2 }).unwrap();
    assert_ne!(first.created, second.created, "premise");

    // x's own entries first, y's drop later.
    let own: Vec<_> = x
        .entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .filter(|e| e.stamp.node == x.node_id())
        .collect();
    assert!(own.iter().any(|e| e.kind == OpKind::CreateCollection), "premise");
    b.apply_batch(&own).unwrap();
    assert!(has(&b, 1), "premise: the old life stands until the drop arrives");
    pull(&b, &y);
    b.finish_purges_now().unwrap();

    assert!(b.get_collection("shop", "orders").is_ok(), "the collection is gone on b");
    assert!(!has(&b, 1), "the previous life's document survived its drop");
    assert!(has(&b, 2), "x's write after the drop was lost");
}

// -- The predicate, one clause at a time, with its boundary --------------------------------

mod predicate {
    use kimmy_core::{CollectionId, Hlc};

    use crate::CollectionMeta;
    use crate::meta::Redated;
    use crate::sync::arriving_life_ends_the_standing_one as ends;

    fn h(ms: u64) -> Hlc {
        Hlc::new(ms, 0)
    }

    fn standing(created: u64) -> CollectionMeta {
        CollectionMeta::new(
            CollectionId::derive("shop", "orders"),
            "shop",
            "orders",
            h(created),
            None,
        )
    }

    fn redated(from: u64, created: u64) -> CollectionMeta {
        let mut meta = standing(created);
        meta.redated = Some(Redated {
            from: h(from),
            created: h(created),
            first_arrival: None,
            arrivals_from: None,
        });
        meta
    }

    #[test]
    fn a_life_made_after_a_drop_that_ended_the_standing_one_ends_it() {
        // Standing created at 100; the drop at 200; the arriving life created at 300.
        assert!(ends(&standing(100), Some(h(300)), None, Some(h(200))));
    }

    #[test]
    fn no_floor_is_no_evidence() {
        assert!(!ends(&standing(100), Some(h(300)), None, None));
        assert!(!ends(&standing(100), None, None, Some(h(200))), "and no creation says nothing");
    }

    #[test]
    fn the_arriving_life_must_be_newer() {
        assert!(!ends(&standing(300), Some(h(300)), None, Some(h(200))), "the same life");
        assert!(!ends(&standing(400), Some(h(300)), None, Some(h(200))), "a sender that is behind");
        assert!(
            ends(&standing(100), Some(h(101)), None, Some(h(100))),
            "one tick newer, at the edge"
        );
    }

    #[test]
    fn the_drop_must_be_at_or_after_the_standing_creation() {
        // A floor below the standing creation is a drop the standing life was made after.
        assert!(!ends(&standing(250), Some(h(300)), None, Some(h(200))));
        assert!(
            ends(&standing(200), Some(h(300)), None, Some(h(200))),
            "at it is a drop it followed"
        );
    }

    #[test]
    fn the_drop_must_be_before_the_arriving_creation() {
        assert!(!ends(&standing(100), Some(h(300)), None, Some(h(300))), "a floor at the creation");
        assert!(!ends(&standing(100), Some(h(300)), None, Some(h(400))), "a floor above it");
        assert!(ends(&standing(100), Some(h(300)), None, Some(h(299))));
    }

    #[test]
    fn a_re_dated_life_is_the_standing_life_continuing() {
        // The arriving life was re-dated from the creation the standing one has.
        assert!(!ends(&standing(100), Some(h(201)), Some(h(100)), Some(h(200))));
        // The same life re-dated by another drop: both sides began at 100.
        assert!(!ends(&redated(100, 151), Some(h(251)), Some(h(100)), Some(h(250))));
        // A standing life that was re-dated is compared by where it began, not where it was
        // dated to: an arriving life that began elsewhere is a different life.
        assert!(ends(&redated(100, 151), Some(h(400)), None, Some(h(300))));
        // And re-dated from a different beginning than the standing one's.
        assert!(ends(&standing(100), Some(h(401)), Some(h(350)), Some(h(400))));
    }
}

// -- What the burial leaves, and what survives it ------------------------------------------

#[test]
fn a_database_drop_floor_buries_the_previous_life_too() {
    // The sender's floor is the greater of the collection's and the database's drop
    // tombstone; with both aged out, the page still carries it.
    let (a, _da) = engine();
    let (b, _db) = engine();
    let first = a.create_collection("shop", "orders").unwrap();
    a.insert(&first, doc! { "_id": 1 }).unwrap();
    transfer(&b, &a);
    assert!(has(&b, 1), "premise");
    later();
    a.drop_database("shop").unwrap();
    a.finish_purges_now().unwrap();
    later();
    let second = a.create_collection("shop", "orders").unwrap();
    a.insert(&second, doc! { "_id": 2 }).unwrap();
    let floor = a.get_collection("shop", "orders").unwrap().incarnation_floor;
    assert!(floor.is_some(), "premise: the new life has a floor");
    forget_the_tombstone(&a);
    let txn = a.begin_write(crate::WriterHolder::Ddl).unwrap();
    txn.open_table(crate::tables::COLLECTIONS_DROPPED)
        .unwrap()
        .remove(kimmy_core::database_drop_id("shop").0)
        .unwrap();
    txn.commit().unwrap();
    transfer(&b, &a);
    assert_holds_the_senders_life(&a, &b, "database drop");
    assert_eq!(b.get_collection("shop", "orders").unwrap().incarnation_floor, floor);
}

#[test]
fn an_index_survives_the_burial_and_both_members_agree_on_it() {
    // ADR-233: the receiver's own index tombstone is honoured, and the sender's index
    // of the new life, made after the drop, is restored with its own stamp.
    let (a, _da) = engine();
    let (b, _db) = engine();
    let first = a.create_collection("shop", "orders").unwrap();
    a.insert(&first, doc! { "_id": 1, "k": 1 }).unwrap();
    a.create_index(
        "shop",
        "orders",
        vec![crate::IndexField::ascending("k")],
        false,
        Some("ik".into()),
    )
    .unwrap();
    transfer(&b, &a);
    assert_eq!(b.list_indexes("shop", "orders").unwrap().len(), 1, "premise");
    later();
    a.drop_collection("shop", "orders").unwrap();
    a.finish_purges_now().unwrap();
    later();
    let second = a.create_collection("shop", "orders").unwrap();
    a.insert(&second, doc! { "_id": 2, "k": 2 }).unwrap();
    a.create_index(
        "shop",
        "orders",
        vec![crate::IndexField::ascending("k")],
        false,
        Some("ik".into()),
    )
    .unwrap();
    forget_the_tombstone(&a);
    transfer(&b, &a);
    let by_a: Vec<_> = a
        .list_indexes("shop", "orders")
        .unwrap()
        .iter()
        .map(|i| (i.name.clone(), i.created))
        .collect();
    let by_b: Vec<_> = b
        .list_indexes("shop", "orders")
        .unwrap()
        .iter()
        .map(|i| (i.name.clone(), i.created))
        .collect();
    assert_eq!(by_a, by_b, "the members hold different indexes");
    assert_eq!(by_a.len(), 1);
    assert_eq!(
        crate::definition_digest::definition_digest(&a.get_collection("shop", "orders").unwrap()),
        crate::definition_digest::definition_digest(&b.get_collection("shop", "orders").unwrap()),
        "the definition digests differ"
    );
}

#[test]
fn a_failure_between_the_burial_and_the_restore_leaves_nothing_that_does_not_heal() {
    // The burial is its own transaction and the restore follows. What a failure between
    // them leaves is the burial alone: the standing life ended, the tombstone at the
    // floor, the page's progress not advanced. Modelled by running the burial and
    // stopping, then retrying the whole page.
    let (a, b, _dirs) = receiver_holds_the_previous_life();
    forget_the_tombstone(&a);
    let page = a.snapshot_page(None, Some(orders_id())).unwrap();
    let state = &page.collections[0];
    let floor = state.incarnation_floor.expect("premise: the page carries the floor");
    b.bury_the_life_this_one_followed(state, &page.versions, a.node_id(), None).unwrap();
    assert!(b.get_collection("shop", "orders").is_err(), "the burial left a life standing");
    let tombstone = b.collection_dropped_at(orders_id()).unwrap().expect("a tombstone");
    assert_eq!(tombstone.hlc, floor, "the tombstone is at the floor");
    // The retry: the whole page again, from the progress that never advanced.
    transfer_under(&b, &a, SnapshotProgress::of_collection(orders_id()));
    assert_holds_the_senders_life(&a, &b, "the retry");
    assert_eq!(b.get_collection("shop", "orders").unwrap().incarnation_floor.unwrap(), floor);
}

/// The burial takes the drop's own stamp from the page's tombstone when the page carries
/// one (ADR-236), so the receiver and the drop's maker name one drop by one stamp. C took
/// A's drop by a scoped page (its tombstone, and no coverage of A) and A's new life by
/// another, then wrote into it; B, holding the life before, takes C's whole page. The
/// carried pair is not one C's coverage names, so it is not applied as a drop (ADR-163);
/// the floor is named by C's own write, and the burial it drives was stamped with C's
/// node, a tombstone of the drop that no member which made or applied it holds.
#[test]
fn a_burial_from_a_whole_page_takes_the_stamp_of_the_tombstone_it_carries() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let (c, _dc) = engine();
    let first = a.create_collection("shop", "orders").unwrap();
    a.insert(&first, doc! { "_id": 1 }).unwrap();
    transfer(&b, &a);
    later();
    a.drop_collection("shop", "orders").unwrap();
    a.finish_purges_now().unwrap();
    let dropped = a.collection_dropped_at(orders_id()).unwrap().expect("the drop's tombstone");
    transfer_under(&c, &a, SnapshotProgress::of_collection(orders_id()));
    assert_eq!(c.collection_dropped_at(orders_id()).unwrap(), Some(dropped), "premise");
    later();
    a.create_collection("shop", "orders").unwrap();
    transfer_under(&c, &a, SnapshotProgress::of_collection(orders_id()));
    let on_c = c.get_collection("shop", "orders").unwrap();
    assert_eq!(on_c.incarnation_floor, Some(dropped.hlc), "premise: the floor came with it");
    c.insert(&on_c, doc! { "_id": 2 }).unwrap();
    let page = c.snapshot_page(None, None).unwrap();
    assert!(page.versions.get(a.node_id()) < dropped.hlc, "premise: C cannot vouch for A's drop");

    transfer(&b, &c);
    assert!(!has(&b, 1) && has(&b, 2), "the life before ended and the new one stands");
    assert_eq!(
        b.collection_dropped_at(orders_id()).unwrap(),
        Some(dropped),
        "the burial named the drop by its maker's stamp"
    );
}

/// The carried tombstone names the burial only when it is the drop the floor stands for
/// (the floor's own `Hlc`): one of another drop, which the page carries for the same id,
/// leaves the burial at the floor with the sender's node.
#[test]
fn a_burial_from_a_whole_page_ignores_a_carried_tombstone_of_another_drop() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let (c, _dc) = engine();
    let first = a.create_collection("shop", "orders").unwrap();
    a.insert(&first, doc! { "_id": 1 }).unwrap();
    transfer(&b, &a);
    later();
    a.drop_collection("shop", "orders").unwrap();
    a.finish_purges_now().unwrap();
    let dropped = a.collection_dropped_at(orders_id()).unwrap().expect("the drop's tombstone");
    transfer_under(&c, &a, SnapshotProgress::of_collection(orders_id()));
    later();
    a.create_collection("shop", "orders").unwrap();
    transfer_under(&c, &a, SnapshotProgress::of_collection(orders_id()));
    let on_c = c.get_collection("shop", "orders").unwrap();
    c.insert(&on_c, doc! { "_id": 2 }).unwrap();
    let mut page = c.snapshot_page(None, None).unwrap();
    assert!(page.next.is_none(), "premise: one page");
    let other = Stamp::new(Hlc::new(dropped.hlc.wall_ms + 1, 0), a.node_id());
    for (id, stamp) in page.dropped_collections.iter_mut() {
        if *id == orders_id() {
            *stamp = other;
        }
    }
    let mut progress = SnapshotProgress::whole_database();
    if b.apply_snapshot_page(c.node_id(), &mut progress, &page).unwrap().purge_pending.is_some() {
        // The burial's rows are the purger's before the new life can stand.
        b.finish_purges_now().unwrap();
        b.apply_snapshot_page(c.node_id(), &mut progress, &page).unwrap();
    }
    assert!(!has(&b, 1) && has(&b, 2), "the life before ended and the new one stands");
    assert_eq!(
        b.collection_dropped_at(orders_id()).unwrap(),
        Some(Stamp::new(dropped.hlc, c.node_id())),
        "the burial took a tombstone of another drop"
    );
}

/// The present-life sweep a page's floor drives (a standing life created after the drop,
/// its floor below it) is stamped as the burial is: by the tombstone the page carries for
/// the drop when there is one (ADR-236). B made its own life after A's drop and took in
/// A's index of the life before; C, which took A's drop and A's next life by scoped pages,
/// serves B a whole page that carries A's tombstone without vouching for it. The index
/// drop the sweep records is A's stamp, as it is on every member that applied the drop.
#[test]
fn the_present_life_sweep_from_a_page_takes_the_stamp_of_the_tombstone_it_carries() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let (c, _dc) = engine();
    a.create_collection("shop", "orders").unwrap();
    let field = vec![kimmy_core::IndexField::ascending("k")];
    let index = a.create_index("shop", "orders", field, false, Some("by_k".into())).unwrap();
    let before_the_drop =
        a.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    later();
    a.drop_collection("shop", "orders").unwrap();
    a.finish_purges_now().unwrap();
    let dropped = a.collection_dropped_at(orders_id()).unwrap().expect("the drop's tombstone");
    later();
    b.create_collection("shop", "orders").unwrap();
    b.apply_batch(&before_the_drop).unwrap();
    let standing = b.get_collection("shop", "orders").unwrap();
    assert!(standing.index("by_k").is_some(), "premise: B took in the life before's index");
    assert!(crate::late_drop::owes_a_sweep(&standing, dropped.hlc), "premise: it owes the sweep");

    transfer_under(&c, &a, SnapshotProgress::of_collection(orders_id()));
    later();
    a.create_collection("shop", "orders").unwrap();
    transfer_under(&c, &a, SnapshotProgress::of_collection(orders_id()));
    let on_c = c.get_collection("shop", "orders").unwrap();
    c.insert(&on_c, doc! { "_id": 2 }).unwrap();
    let page = c.snapshot_page(None, None).unwrap();
    assert!(page.versions.get(a.node_id()) < dropped.hlc, "premise: C cannot vouch for A's drop");

    transfer(&b, &c);
    let on_b = b.get_collection("shop", "orders").unwrap();
    assert!(on_b.index("by_k").is_none(), "the sweep removed the life before's index");
    assert_eq!(on_b.incarnation_floor, Some(dropped.hlc), "and took the floor");
    let index_id = kimmy_core::IndexMeta::derive_id(&index.name);
    assert_eq!(
        b.index_dropped_at(orders_id(), index_id).unwrap(),
        Some(dropped),
        "the sweep named the drop by its maker's stamp"
    );
}

// -- A relay passes the floor on (PR #193, break-the-guard review) -------------------------

/// S drops and recreates; R, which holds nothing, takes S's page (whole or scoped, S's
/// tombstone held or forgotten); O, which holds the old life, then takes R's page. The
/// floor must reach O through R, so O's old life ends at it: before, `restore_collection`
/// created the life through `create_collection_marked`, which took the floor only from R's
/// own tombstone, so R ended with none and O ended with one collection made of two lives.
fn relay(
    first_hop_scoped: bool,
    second_hop_scoped: bool,
    forget_on_s: bool,
    r_is_busy: bool,
) -> Result<(), String> {
    let (s, _ds) = engine();
    let (r, _dr) = engine();
    let (o, _do) = engine();
    let first = s.create_collection("shop", "orders").unwrap();
    s.insert(&first, doc! { "_id": 1 }).unwrap();
    transfer(&o, &s);
    assert!(has(&o, 1), "premise");
    later();
    assert!(s.drop_collection("shop", "orders").unwrap());
    s.finish_purges_now().unwrap();
    later();
    let second = s.create_collection("shop", "orders").unwrap();
    s.insert(&second, doc! { "_id": 2 }).unwrap();
    if forget_on_s {
        forget_the_tombstone(&s);
    }
    let hop = |into: &Engine, from: &Engine, scoped: bool| {
        if scoped {
            transfer_under(into, from, SnapshotProgress::of_collection(orders_id()));
        } else {
            transfer(into, from);
        }
    };
    hop(&r, &s, first_hop_scoped);
    if r_is_busy {
        // A member in a live cluster has stamps of its own after the floor (any write of
        // its own, or entries of any origin), which is what lets its coverage name the
        // floor it passes on. A scoped snapshot grants no coverage, so an idle relay that
        // took the life that way has none to vouch with (see the test below).
        later();
        let other = r.create_collection("shop", "heartbeat").unwrap();
        r.insert(&other, doc! { "_id": 1 }).unwrap();
    }
    let floor_s = created_floor(&s);
    let floor_r = created_floor(&r);
    hop(&o, &r, second_hop_scoped);
    o.finish_purges_now().unwrap();
    if has(&o, 1) {
        Err(format!(
            "floor on S {floor_s:?}, on R {floor_r:?}: O holds life 0's document 1 beside life \
             1's document 2"
        ))
    } else if created_floor(&r) != floor_s {
        Err(format!("R's floor {:?} is not S's {floor_s:?}", created_floor(&r)))
    } else if !has(&o, 2) {
        Err("O did not take life 1's document 2".into())
    } else {
        // O ends holding the sender's life: its creation and its floor.
        let (held, want) = (
            o.get_collection("shop", "orders").unwrap(),
            s.get_collection("shop", "orders").unwrap(),
        );
        if held.created != want.created || held.incarnation_floor != want.incarnation_floor {
            Err(format!(
                "O ends with created {:?} and floor {:?}, S with {:?} and {:?}",
                held.created, held.incarnation_floor, want.created, want.incarnation_floor
            ))
        } else {
            Ok(())
        }
    }
}

fn created_floor(e: &Engine) -> Option<Hlc> {
    e.get_collection("shop", "orders").unwrap().incarnation_floor
}

#[test]
fn a_member_that_took_the_life_by_snapshot_passes_the_floor_on() {
    let mut bad = Vec::new();
    for first in [false, true] {
        for second in [false, true] {
            for forget in [false, true] {
                if let Err(e) = relay(first, second, forget, true) {
                    bad.push(format!(
                        "first hop scoped={first}, second hop scoped={second}, forget on S={forget}: {e}"
                    ));
                }
            }
        }
    }
    assert!(bad.is_empty(), "relay failures:\n{}", bad.join("\n"));
}

#[test]
fn a_floor_the_coverage_does_not_name_is_not_adopted_on_a_fresh_create() {
    let (a, _da) = engine();
    let (r, _dr) = engine();
    let first = a.create_collection("shop", "orders").unwrap();
    a.insert(&first, doc! { "_id": 1 }).unwrap();
    later();
    a.drop_collection("shop", "orders").unwrap();
    a.finish_purges_now().unwrap();
    later();
    a.create_collection("shop", "orders").unwrap();
    let real = created_floor(&a).expect("premise: the life has a floor");
    // The page's coverage does not name the floor (an empty vector): not believed.
    let mut page = a.snapshot_page(None, Some(orders_id())).unwrap();
    page.versions = kimmy_core::VersionVector::new();
    r.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::of_collection(orders_id()), &page)
        .unwrap();
    assert_eq!(
        created_floor(&r),
        None,
        "a floor nothing vouched for was adopted (the real one: {real:?})"
    );
}

#[test]
fn a_floor_at_or_above_the_life_it_belongs_to_is_not_adopted() {
    let (a, _da) = engine();
    let (r, _dr) = engine();
    let first = a.create_collection("shop", "orders").unwrap();
    a.insert(&first, doc! { "_id": 1 }).unwrap();
    later();
    a.drop_collection("shop", "orders").unwrap();
    a.finish_purges_now().unwrap();
    later();
    a.create_collection("shop", "orders").unwrap();
    // A floor equal to the life's own creation, named by the coverage: a floor is a drop
    // the life was made after, so it is below `created`.
    let mut page = a.snapshot_page(None, Some(orders_id())).unwrap();
    for state in &mut page.collections {
        state.incarnation_floor = state.created;
    }
    r.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::of_collection(orders_id()), &page)
        .unwrap();
    assert_eq!(created_floor(&r), None, "a floor not below its life was adopted");
}

#[test]
fn an_idle_relay_that_took_the_life_by_a_scoped_snapshot_holds_the_floor_but_cannot_vouch_for_it() {
    // The stated limit of the relay: R adopted S's floor (it carries it), but a scoped
    // snapshot grants no coverage and R has written nothing, so its pages name no stamp at
    // or above the floor and O does not believe it (ADR-163). Nothing is buried on a
    // floor nothing vouches for; the floor is believed once R's coverage reaches it.
    let (s, _ds) = engine();
    let (r, _dr) = engine();
    let (o, _do) = engine();
    let first = s.create_collection("shop", "orders").unwrap();
    s.insert(&first, doc! { "_id": 1 }).unwrap();
    transfer(&o, &s);
    later();
    s.drop_collection("shop", "orders").unwrap();
    s.finish_purges_now().unwrap();
    later();
    let second = s.create_collection("shop", "orders").unwrap();
    s.insert(&second, doc! { "_id": 2 }).unwrap();
    transfer_under(&r, &s, SnapshotProgress::of_collection(orders_id()));
    assert_eq!(created_floor(&r), created_floor(&s), "R carries the floor");
    transfer(&o, &r);
    assert!(has(&o, 1), "a floor nothing vouched for buried a life");
}

#[test]
fn the_pages_floor_never_lowers_the_floor_the_receivers_own_tombstone_gave() {
    // R dropped its copy of the life itself, after S's drop and before S recreated: its own
    // tombstone is the greater floor, and the page's lower one must not replace it.
    let (s, _ds) = engine();
    let (r, _dr) = engine();
    let first = s.create_collection("shop", "orders").unwrap();
    s.insert(&first, doc! { "_id": 1 }).unwrap();
    transfer(&r, &s);
    later();
    s.drop_collection("shop", "orders").unwrap();
    s.finish_purges_now().unwrap();
    later();
    r.drop_collection("shop", "orders").unwrap();
    r.finish_purges_now().unwrap();
    let own = r.collection_dropped_at(orders_id()).unwrap().expect("R's tombstone").hlc;
    later();
    s.create_collection("shop", "orders").unwrap();
    let theirs = created_floor(&s).expect("the page carries a floor");
    assert!(theirs < own, "premise: the page's floor is the lower one");
    transfer_under(&r, &s, SnapshotProgress::of_collection(orders_id()));
    assert_eq!(created_floor(&r), Some(own), "the page's lower floor replaced R's own");
}

// -- Part 3 (review D): a vouched floor reaches the life standing here -----------------------

fn scoped(into: &Engine, from: &Engine) {
    transfer_under(into, from, SnapshotProgress::of_collection(orders_id()));
}

fn named(hlc: Hlc, versions: &kimmy_core::VersionVector) -> bool {
    versions.iter().any(|(_, covered)| hlc <= covered)
}

/// An ordinary pull: the requester's witnessed vector as `held`, as a real pull sends it.
fn pull_from_position(into: &Engine, from: &Engine) {
    let held = into.witnessed_vector().unwrap();
    let entries = from
        .entries_for_peer_holding(Hlc::ZERO, 10_000, Some(&held), WalkScope::Background)
        .unwrap()
        .entries;
    into.apply_batch(&entries).unwrap();
    into.finish_purges_now().unwrap();
}

/// S drops life 0 and makes life 1, with document 2; O holds life 0, with document 1.
/// `forget` ages S's tombstone out (the premise of ADR-235).
fn o_holds_life_0(forget: bool) -> (Engine, Engine, [tempfile::TempDir; 2]) {
    let (s, d_s) = engine();
    let (o, d_o) = engine();
    let first = s.create_collection("shop", "orders").unwrap();
    s.insert(&first, doc! { "_id": 1 }).unwrap();
    transfer(&o, &s);
    assert!(has(&o, 1), "premise");
    later();
    assert!(s.drop_collection("shop", "orders").unwrap());
    s.finish_purges_now().unwrap();
    later();
    let second = s.create_collection("shop", "orders").unwrap();
    s.insert(&second, doc! { "_id": 2 }).unwrap();
    if forget {
        forget_the_tombstone(&s);
    }
    (s, o, [d_s, d_o])
}

/// X takes the life from an idle relay (a scoped snapshot grants nothing, so the floor is
/// not vouched and X adopts none), then takes S's whole-database snapshot. The life stands
/// on X already, so nothing adopted the floor (`restore_collection` adopts only on a fresh
/// create), and X was granted S's coverage past the drop: it was never served the drop
/// again, and its own pages then granted a member holding life 0 that coverage without
/// burying it. A vouched page now gives the life standing here the floor, through the
/// sweep ADR-231 owes it.
#[test]
fn a_member_holding_the_new_life_without_the_floor_takes_it_from_a_vouched_page() {
    let (s, o, _dirs) = o_holds_life_0(true);
    let (r, _dr) = engine();
    let (x, _dx) = engine();
    scoped(&r, &s);
    scoped(&x, &r);
    assert_eq!(created_floor(&x), None, "premise: R's page could not vouch");
    transfer(&x, &s);
    assert_eq!(created_floor(&x), created_floor(&s), "X did not take the vouched floor");
    // O, still on life 0, takes X's whole-database snapshot.
    transfer(&o, &x);
    o.finish_purges_now().unwrap();
    assert!(!has(&o, 1), "O merged life 0 through X");
    assert!(has(&o, 2));
    // And not by luck of the order: O's own pull from S finds nothing left to do.
    pull_from_position(&o, &s);
    assert_holds_the_senders_life(&s, &o, "after the pull");
}

/// The same with life 1 reusing life 0's ids, so the counts agree and the count half never
/// fires: what O would keep of life 0 is its index, which only the definition digest sees,
/// and that plans no repair.
#[test]
fn with_equal_counts_life_0s_index_does_not_survive_on_the_member_that_took_the_life_floorless() {
    let (s, _ds) = engine();
    let (o, _do) = engine();
    let (r, _dr) = engine();
    let (x, _dx) = engine();
    let first = s.create_collection("shop", "orders").unwrap();
    s.insert(&first, doc! { "_id": 1, "v": 0 }).unwrap();
    s.create_index(
        "shop",
        "orders",
        vec![crate::IndexField::ascending("v")],
        true,
        Some("old".into()),
    )
    .unwrap();
    transfer(&o, &s);
    later();
    s.drop_collection("shop", "orders").unwrap();
    s.finish_purges_now().unwrap();
    later();
    let second = s.create_collection("shop", "orders").unwrap();
    s.insert(&second, doc! { "_id": 1, "v": 1 }).unwrap();
    forget_the_tombstone(&s);
    scoped(&r, &s);
    scoped(&x, &r);
    transfer(&x, &s);
    transfer(&o, &x);
    o.finish_purges_now().unwrap();
    pull_from_position(&o, &s);
    let names = |e: &Engine| -> Vec<String> {
        e.list_indexes("shop", "orders").unwrap().into_iter().map(|i| i.name).collect()
    };
    assert_eq!(names(&o), names(&s), "life 0's index survived on O");
    let (co, cs) =
        (o.get_collection("shop", "orders").unwrap(), s.get_collection("shop", "orders").unwrap());
    assert_eq!(
        crate::definition_digest::definition_digest(&co),
        crate::definition_digest::definition_digest(&cs),
        "the definition digests differ"
    );
}

/// The sweep ADR-231 owes removes a straggler of the life before (a write stamped before the
/// drop), and keeps what the life itself wrote.
#[test]
fn the_sweep_a_vouched_floor_owes_removes_a_straggler_and_keeps_the_lifes_own_writes() {
    let (s, o, _dirs) = o_holds_life_0(true);
    let (r, _dr) = engine();
    let (x, _dx) = engine();
    scoped(&r, &s);
    scoped(&x, &r);
    // A straggler of life 0 (document 1, stamped before the drop) from O, which holds it.
    scoped(&x, &o);
    assert!(has(&x, 1), "premise: X took a straggler");
    later();
    let cx = x.get_collection("shop", "orders").unwrap();
    x.insert(&cx, doc! { "_id": 9 }).unwrap();
    transfer(&x, &s);
    assert!(!has(&x, 1), "the straggler stayed");
    assert!(has(&x, 2) && has(&x, 9), "a write of the life went");
    assert_eq!(created_floor(&x), created_floor(&s));
}

/// A life that already took the floor owes no sweep: a second vouched page of it removes
/// nothing, including the write the life made after the drop. The branch is disjoint from
/// the burial and a no-op where the floor stands at the drop.
#[test]
fn a_vouched_page_for_a_life_that_already_holds_the_floor_sweeps_nothing() {
    let (s, _o, _dirs) = o_holds_life_0(true);
    let (x, _dx) = engine();
    transfer(&x, &s);
    assert_eq!(created_floor(&x), created_floor(&s));
    later();
    let cx = x.get_collection("shop", "orders").unwrap();
    x.insert(&cx, doc! { "_id": 9 }).unwrap();
    transfer(&x, &s);
    scoped(&x, &s);
    assert!(has(&x, 2) && has(&x, 9), "a life's own writes were swept");
}

/// A re-dated life (ADR-231 extended) holds the drop as its floor already: a second page of
/// it sweeps nothing.
#[test]
fn a_re_dated_life_takes_its_own_page_again_and_loses_nothing() {
    let (a, _da) = engine();
    let (m, _dm) = engine();
    let on_a = a.create_collection("shop", "orders").unwrap();
    a.insert(&on_a, doc! { "_id": 1 }).unwrap();
    pull(&m, &a);
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    let on_m = m.get_collection("shop", "orders").unwrap();
    m.insert(&on_m, doc! { "_id": 3 }).unwrap();
    pull(&m, &a);
    assert!(m.get_collection("shop", "orders").unwrap().is_redated(), "premise");
    let (p, _dp) = engine();
    let page = m.snapshot_page(None, Some(orders_id())).unwrap();
    p.apply_snapshot_page(m.node_id(), &mut SnapshotProgress::of_collection(orders_id()), &page)
        .unwrap();
    assert!(has(&p, 3), "premise");
    let again = m.snapshot_page(None, Some(orders_id())).unwrap();
    p.apply_snapshot_page(m.node_id(), &mut SnapshotProgress::of_collection(orders_id()), &again)
        .unwrap();
    assert!(has(&p, 3), "a re-dated life's write after the drop was swept");
}

/// A mutant the first tests did not kill: adopting the page's floor only when the receiver
/// has none of its own. R's own tombstone of an EARLIER drop gives it a lower floor than the
/// page's, and its pages would then carry a floor below O's life 0.
#[test]
fn the_pages_higher_floor_replaces_a_lower_one_the_receivers_own_tombstone_gave() {
    let (s, _ds) = engine();
    let (r, _dr) = engine();
    let (o, _do) = engine();
    // R's own earlier life of the name, dropped first: R's tombstone is the lower one.
    let mine = r.create_collection("shop", "orders").unwrap();
    r.insert(&mine, doc! { "_id": 7 }).unwrap();
    later();
    r.drop_collection("shop", "orders").unwrap();
    r.finish_purges_now().unwrap();
    let own = r.collection_dropped_at(orders_id()).unwrap().unwrap().hlc;
    later();
    let first = s.create_collection("shop", "orders").unwrap();
    s.insert(&first, doc! { "_id": 1 }).unwrap();
    transfer(&o, &s);
    later();
    s.drop_collection("shop", "orders").unwrap();
    s.finish_purges_now().unwrap();
    later();
    let second = s.create_collection("shop", "orders").unwrap();
    s.insert(&second, doc! { "_id": 2 }).unwrap();
    let theirs = created_floor(&s).unwrap();
    assert!(own < theirs, "premise");
    scoped(&r, &s);
    assert_eq!(created_floor(&r), Some(theirs), "R kept its own lower floor");
    // And it matters one hop on: R busy, O on life 0.
    later();
    let hb = r.create_collection("shop", "heartbeat").unwrap();
    r.insert(&hb, doc! { "_id": 1 }).unwrap();
    transfer(&o, &r);
    o.finish_purges_now().unwrap();
    assert!(!has(&o, 1), "O merged two lives through R's lower floor");
}

// The idle relay's limit heals (ADR-163's strict gate costs a round of anti-entropy). An
// unvouched floor is never granted, so O is still owed the drop.

/// R's page granted O nothing of S, so O's next ordinary pull from S delivers the
/// `DropCollection` entry (while retained), and the keep-later-writes drop ends life 0 on O.
#[test]
fn the_limit_heals_on_os_next_ordinary_pull_from_the_dropper() {
    for second_scoped in [false, true] {
        for forget in [false, true] {
            let (s, o, _dirs) = o_holds_life_0(forget);
            let (r, _dr) = engine();
            scoped(&r, &s);
            if second_scoped {
                scoped(&o, &r)
            } else {
                transfer(&o, &r)
            }
            assert!(has(&o, 1), "premise: the limit");
            assert!(
                o.witnessed_vector().unwrap().get(s.node_id()) < created_floor(&s).unwrap(),
                "R's page granted O coverage of S past the drop"
            );
            pull_from_position(&o, &s);
            assert!(!has(&o, 1), "second scoped={second_scoped} forget={forget}: life 0 survives");
            assert!(has(&o, 2), "second scoped={second_scoped} forget={forget}: life 1 lost");
            assert_eq!(created_floor(&o), created_floor(&s), "scoped={second_scoped} {forget}");
        }
    }
}

/// With the drop entry aged out (the ADR-235 premise), O is beyond S's horizon and takes S's
/// whole-database snapshot, whose coverage names the floor: the burial runs.
#[test]
fn the_limit_heals_on_the_dropper_s_whole_database_snapshot() {
    let (s, o, _dirs) = o_holds_life_0(true);
    let (r, _dr) = engine();
    scoped(&r, &s);
    transfer(&o, &r);
    assert!(has(&o, 1), "premise: the limit");
    transfer(&o, &s);
    o.finish_purges_now().unwrap();
    assert!(!has(&o, 1));
    assert!(has(&o, 2));
}

/// The realistic first hop: a fresh member catching up by a whole-database snapshot, idle.
/// Its grant names the floor, so it vouches for it whether or not it is busy.
#[test]
fn an_idle_relay_that_took_the_life_by_a_whole_database_snapshot_vouches() {
    let mut bad = Vec::new();
    for second in [false, true] {
        for forget in [false, true] {
            if let Err(e) = relay(false, second, forget, false) {
                bad.push(format!("second hop scoped={second}, forget on S={forget}: {e}"));
            }
        }
    }
    assert!(bad.is_empty(), "relay failures:\n{}", bad.join("\n"));
}

/// A whole-database pull in progress grants nothing until its last page, so a relay part-way
/// through one is in the same state as the idle scoped one; once it completes, it vouches.
#[test]
fn a_relay_part_way_through_a_whole_database_pull_cannot_vouch_either() {
    let (s, o, _dirs) = o_holds_life_0(true);
    let coll = s.get_collection("shop", "orders").unwrap();
    for i in 10..(10 + crate::snapshot::SNAPSHOT_PAGE as i64 + 50) {
        s.insert(&coll, doc! { "_id": i }).unwrap();
    }
    let (r, _dr) = engine();
    let mut progress = SnapshotProgress::whole_database();
    let page = s.snapshot_page(None, None).unwrap();
    assert!(page.next.is_some(), "premise: more than one page");
    r.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
    assert_eq!(created_floor(&r), created_floor(&s), "R adopted the floor from the first page");
    assert!(!named(created_floor(&r).unwrap(), &r.version_vector().unwrap()), "no grant yet");
    transfer(&o, &r);
    o.finish_purges_now().unwrap();
    assert!(has(&o, 1), "a relay mid-pull vouched");
    // R completes: the grant names the floor, and R's next page buries.
    while !progress.is_complete() {
        let page = s.snapshot_page(progress.after().cloned(), None).unwrap();
        r.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
    }
    transfer(&o, &r);
    o.finish_purges_now().unwrap();
    assert!(!has(&o, 1), "the completed relay still cannot vouch");
}

/// The clock passes a floor the member adopts, as it passes the floor a burial restores
/// (ADR-233): the page's coverage may name the floor and not the life's creation, and a
/// member whose clock is behind would otherwise mint writes at or below its own floor.
#[test]
fn the_clock_passes_an_adopted_floor() {
    let (a, _da) = engine();
    let (r, _dr) = engine();
    a.create_collection("shop", "orders").unwrap();
    // A floor an hour ahead of this machine's clock, named by the page's coverage, with the
    // life created above it and not named.
    let ahead = Hlc::new(kimmy_storage_now_ms() + 3_600_000, 0);
    let mut page = a.snapshot_page(None, Some(orders_id())).unwrap();
    for state in &mut page.collections {
        state.incarnation_floor = Some(ahead);
        state.created = Some(ahead.successor());
    }
    page.versions.insert(a.node_id(), ahead);
    r.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::of_collection(orders_id()), &page)
        .unwrap();
    assert_eq!(created_floor(&r), Some(ahead), "premise: the floor was adopted");
    assert!(r.next_stamp().hlc > ahead, "the clock stayed behind the adopted floor");
}

fn kimmy_storage_now_ms() -> u64 {
    crate::physical_now_ms()
}
