//! What the InWindow release does when the window omits an EARLIER entry of the origin that this member still
//! holds as state (ADR-148, ADR-160, ADR-169, ADR-172). The release is safe because a sender omits an entry from
//! a window only when it witnessed that entry as superseded; these tests pin what the receiver then does with
//! its held mark.

use bson::doc;
use kimmy_core::Hlc;

use crate::Engine;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

/// The honest shape: `a` inserts d1 (the earlier entry), replaces d1, then inserts d2. The sender `b` took the
/// replace first, so the insert of d1 arrived superseded and was witnessed away; its window carries the replace
/// and the insert of d2, not the earlier insert. The receiver `c` holds the earlier insert and the insert of d2
/// as state (snapshot-style, under `Hold`).
#[test]
fn a_window_that_omits_a_superseded_earlier_entry_leaves_its_held_mark_and_the_documents_converge()
{
    let (a, _da) = engine();
    let (b, _db) = engine();
    let (c, _dc) = engine();
    a.create_collection("shop", "orders").unwrap();
    let creation = a.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background).unwrap();
    b.apply_batch(&creation.entries).unwrap();
    c.apply_batch(&creation.entries).unwrap();
    let coll_a = a.get_collection("shop", "orders").unwrap();
    let coll_b = b.get_collection("shop", "orders").unwrap();
    let coll_c = c.get_collection("shop", "orders").unwrap();
    later();
    let id1 = a.insert(&coll_a, doc! { "_id": 1, "v": 1 }).unwrap();
    later();
    a.replace(&coll_a, &id1, doc! { "_id": 1, "v": 2 }, false).unwrap();
    later();
    let id2 = a.insert(&coll_a, doc! { "_id": 2, "v": 3 }).unwrap();
    let all = a.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background).unwrap();
    let after: Vec<_> = all
        .entries
        .iter()
        .filter(|e| e.stamp > creation.entries.last().unwrap().stamp)
        .cloned()
        .collect();
    assert_eq!(after.len(), 3, "premise: insert d1, replace d1, insert d2");
    let (earlier, replace, last) = (after[0].clone(), after[1].clone(), after[2].clone());

    // The sender: the replace first, then the earlier insert (superseded on arrival), then the last insert.
    b.apply_batch(std::slice::from_ref(&replace)).unwrap();
    b.apply_batch(std::slice::from_ref(&earlier)).unwrap();
    b.apply_batch(std::slice::from_ref(&last)).unwrap();
    assert!(
        b.oplog_entry(&earlier.stamp).unwrap().is_none(),
        "premise: the sender witnessed it away"
    );
    assert!(b.oplog_entry(&replace.stamp).unwrap().is_some());

    // The receiver holds the earlier insert and the last insert as state.
    c.apply_held_for_test(&coll_c, &earlier).unwrap();
    c.apply_held_for_test(&coll_c, &last).unwrap();
    assert_eq!(c.held_len().unwrap(), 2, "premise: two marks");

    let window = b.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background).unwrap();
    assert!(
        window.entries.iter().all(|e| e.stamp != earlier.stamp),
        "premise: the window omits the earlier entry"
    );
    c.apply_peer_batch(
        &b.witnessed_vector().unwrap(),
        &window.entries,
        window.scanned_to,
        window.exhausted,
    )
    .unwrap();

    // (a) the receiver's documents converge to the origin's.
    assert_eq!(c.get(&coll_c, &id1).unwrap(), a.get(&coll_a, &id1).unwrap());
    assert_eq!(c.get(&coll_c, &id2).unwrap(), a.get(&coll_a, &id2).unwrap());
    assert_eq!(b.get(&coll_b, &id1).unwrap(), a.get(&coll_a, &id1).unwrap());

    // (b) the marks. The last entry arrived in position and its mark is released, raising the servable vector over
    // the earlier insert. The replace (the superseder of the earlier insert) arrived in the same window, so the
    // earlier insert's mark must not outlive it below the vector: a mark there is named in every pull's `marked`
    // spans (ADR-172), the sender cannot serve an entry it witnessed away, and the member would ask again every
    // MARKS_REASK_AFTER (300 s) until retention collects the entry.
    let servable = c.version_vector().unwrap().get(a.node_id());
    assert!(servable >= last.stamp.hlc, "the last entry arrived in position and raised the vector");
    let covered = c.held_marks_covered_by(&c.witnessed_vector().unwrap()).unwrap();
    assert!(
        covered.is_empty(),
        "the member would ask its peers again for an entry that was superseded: {covered:?}"
    );
    assert_eq!(c.held_len().unwrap(), 0, "a mark outlived the supersession of its entry");
}

/// The genuine gap: a covered held mark whose entry is NOT superseded is kept, and the member keeps asking for it.
/// The window omits an entry nothing replaced (a sender that does not have it); the sweep must not drop it.
#[test]
fn a_covered_held_mark_that_is_not_superseded_is_kept_and_still_asked_for() {
    let (a, _da) = engine();
    let (c, _dc) = engine();
    a.create_collection("shop", "orders").unwrap();
    let creation = a.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background).unwrap();
    c.apply_batch(&creation.entries).unwrap();
    let coll_a = a.get_collection("shop", "orders").unwrap();
    let coll_c = c.get_collection("shop", "orders").unwrap();
    later();
    a.insert(&coll_a, doc! { "_id": 1, "v": 1 }).unwrap();
    later();
    a.insert(&coll_a, doc! { "_id": 2, "v": 2 }).unwrap();
    let all = a.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background).unwrap();
    let docs: Vec<_> = all
        .entries
        .iter()
        .filter(|e| e.stamp > creation.entries.last().unwrap().stamp)
        .cloned()
        .collect();
    let (earlier, last) = (docs[0].clone(), docs[1].clone());
    c.apply_held_for_test(&coll_c, &earlier).unwrap();
    c.apply_held_for_test(&coll_c, &last).unwrap();

    c.apply_peer_batch(
        &a.witnessed_vector().unwrap(),
        std::slice::from_ref(&last),
        last.stamp.hlc,
        true,
    )
    .unwrap();

    assert!(c.version_vector().unwrap().get(a.node_id()) >= last.stamp.hlc);
    assert_eq!(
        c.held_len().unwrap(),
        1,
        "the later entry was released; the unsuperseded earlier one stays"
    );
    let covered = c.held_marks_covered_by(&c.witnessed_vector().unwrap()).unwrap();
    assert_eq!(covered.len(), 1, "and the member still asks for it: {covered:?}");
    assert_eq!(covered[0].1, vec![earlier.stamp.hlc]);
}

/// The snapshot-grant path is NOT changed by the sweep: a grant releases every mark it covers, superseded or not (ADR-160), in
/// the transaction that adopts it, so no mark is left below the vector for the sweep to judge. What the sweep adds is only
/// for marks the vector reaches through a window.
#[test]
fn a_grant_releases_every_mark_it_covers_so_the_sweep_has_nothing_to_judge_after_one() {
    let (a, _da) = engine();
    let (c, _dc) = engine();
    a.create_collection("shop", "orders").unwrap();
    let creation = a.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background).unwrap();
    c.apply_batch(&creation.entries).unwrap();
    let coll_a = a.get_collection("shop", "orders").unwrap();
    let coll_c = c.get_collection("shop", "orders").unwrap();
    later();
    let id1 = a.insert(&coll_a, doc! { "_id": 1, "v": 1 }).unwrap();
    later();
    a.replace(&coll_a, &id1, doc! { "_id": 1, "v": 2 }, false).unwrap();
    let all = a.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background).unwrap();
    let docs: Vec<_> = all
        .entries
        .iter()
        .filter(|e| e.stamp > creation.entries.last().unwrap().stamp)
        .cloned()
        .collect();
    let (earlier, replace) = (docs[0].clone(), docs[1].clone());
    c.apply_held_for_test(&coll_c, &earlier).unwrap();
    c.apply_held_for_test(&coll_c, &replace).unwrap();
    assert_eq!(c.held_len().unwrap(), 2, "premise: two marks");

    c.absorb_version_vector(&a.witnessed_vector().unwrap()).unwrap();
    assert_eq!(
        c.held_len().unwrap(),
        0,
        "the grant released the superseded mark and the current one alike"
    );
    assert!(c.held_marks_covered_by(&c.witnessed_vector().unwrap()).unwrap().is_empty());
}
