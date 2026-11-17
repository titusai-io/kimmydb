//! Step 3, the apply of a field collection's entries (S3a): the required case of ruling 9, at the engine.
//! Three engines and two documents: what a member joins below its own newest stamp it also serves onward.

use bson::doc;
use kimmy_core::{DocId, Hlc, MergePolicy, OplogEntry, Stamp};

use crate::Engine;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background).unwrap().entries
}

/// A sets `X.a` at s1; M, not having it, sets `X.b` at s2 > s1; M pulls A (the entry is joined and appended
/// although M's newest stamp out-stamps it); A writes another document Y at s3 and M pulls it, which raises M's
/// servable of A past s1; P, empty, pulls only M. P reads `X = {a, b}` with the state of A and M, by entries
/// alone: no snapshot, no repair, and nothing is applied on a following P-to-A round.
#[test]
fn a_lower_stamped_concurrent_field_write_is_joined_appended_and_served_onward() {
    let (a, _da) = engine();
    let (m, _dm) = engine();
    let (p, _dp) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let creation = everything(&a);
    m.apply_batch(&creation).unwrap();
    p.apply_batch(&creation).unwrap();
    let cm = m.get_collection("shop", "orders").unwrap();
    let cp = p.get_collection("shop", "orders").unwrap();
    assert_eq!(cp.merge_policy, MergePolicy::Field, "premise: the member holds a field collection");

    later();
    let x = a.insert(&ca, doc! { "_id": 1 }).unwrap();
    let after_creation = |e: &Engine| -> Vec<OplogEntry> {
        let last = creation.last().unwrap().stamp;
        everything(e).into_iter().filter(|en| en.stamp > last).collect()
    };
    m.apply_batch(&after_creation(&a)).unwrap();
    assert!(m.get(&cm, &x).unwrap().is_some(), "M applied A's insert of X");
    later();
    // s1 on A: X.a
    a.replace(&ca, &x, doc! { "_id": 1, "a": 1 }, false).unwrap();
    let s1 = after_creation(&a).last().unwrap().stamp;
    later();
    // s2 on M, which has not seen s1: X.b
    m.advance_clock_past(&Stamp::new(Hlc::new(s1.hlc.wall_ms + 100, 0), m.node_id(), m.epoch()));
    m.replace(&cm, &x, doc! { "_id": 1, "b": 2 }, false).unwrap();
    let s2 = everything(&m).last().unwrap().stamp;
    assert!(s2 > s1, "premise: M's write is stamped above A's");

    // M pulls A: the entry below its newest stamp is joined and appended
    m.apply_batch(&after_creation(&a)).unwrap();
    assert!(m.oplog_entry(&s1).unwrap().is_some(), "M holds A's entry to serve it onward");
    later();
    // A writes another document, M pulls it
    a.insert(&ca, doc! { "_id": 2 }).unwrap();
    m.apply_batch(&after_creation(&a)).unwrap();

    // P pulls only M
    let from_m: Vec<OplogEntry> =
        everything(&m).into_iter().filter(|en| en.stamp > creation.last().unwrap().stamp).collect();
    let out = p.apply_batch(&from_m).unwrap();
    assert_eq!(out.superseded, 0, "nothing of M's window was superseded for P");

    let id = DocId::Int64(1);
    let (vp, dp) = p.get_versioned(&cp, &id).unwrap().expect("P holds X");
    assert_eq!(dp, doc! { "_id": 1, "a": 1, "b": 2 }, "P reads both fields");
    let (vm, _) = m.get_versioned(&cm, &id).unwrap().unwrap();
    // A takes M's entries too, and then all three hold one state
    a.apply_batch(
        &everything(&m)
            .into_iter()
            .filter(|en| en.stamp > creation.last().unwrap().stamp)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let (va, _) = a.get_versioned(&ca, &id).unwrap().unwrap();
    assert_eq!(vp, vm, "the same version and state hash as M");
    assert_eq!(vm, va, "and as A");
    // nothing moves on a following round with A (A has nothing P lacks of X)
    let again = p.apply_batch(&after_creation(&a)).unwrap();
    assert_eq!(again.applied, 0, "nothing is applied");
}

/// A field collection on `a`, taken (by its creation entry) by every engine in `others`; the stamp
/// of the creation entry is the cut under which `after` filters.
struct Cluster {
    cut: kimmy_core::Stamp,
}

impl Cluster {
    fn new(a: &Engine, others: &[&Engine]) -> (Self, crate::meta::CollectionMeta) {
        let ca =
            a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
        let creation = everything(a);
        for e in others {
            e.apply_batch(&creation).unwrap();
        }
        (Self { cut: creation.last().unwrap().stamp }, ca)
    }

    fn after(&self, e: &Engine) -> Vec<OplogEntry> {
        everything(e).into_iter().filter(|en| en.stamp > self.cut).collect()
    }
}

/// Applies `batch` and fails by name when the engine did not apply every entry of it: a setup entry the
/// parent drops silently would otherwise surface as an unrelated `unwrap` on `None` further down.
fn premise_applied(e: &Engine, batch: &[OplogEntry], what: &str) {
    let out = e.apply_batch(batch).unwrap();
    assert_eq!(out.applied, batch.len(), "premise: {what}: {out:?}");
}

/// The same two entries in either order give one state, whichever is applied first, and both stay
/// in the oplog to be served on.
#[test]
fn the_same_two_field_entries_in_either_order_give_one_state_hash() {
    let (a, _da) = engine();
    let (m, _dm) = engine();
    let (p1, _d1) = engine();
    let (p2, _d2) = engine();
    let (c, ca) = Cluster::new(&a, &[&m, &p1, &p2]);
    later();
    let x = a.insert(&ca, doc! { "_id": 1 }).unwrap();
    let insert = c.after(&a);
    premise_applied(&m, &insert, "M applied A's insert of X");
    let cm = m.get_collection("shop", "orders").unwrap();
    later();
    a.replace(&ca, &x, doc! { "_id": 1, "a": 1 }, false).unwrap();
    let s1 = c.after(&a).last().unwrap().clone();
    later();
    m.replace(&cm, &x, doc! { "_id": 1, "b": 2 }, false).unwrap();
    let s2 = c.after(&m).last().unwrap().clone();
    assert!(s2.stamp > s1.stamp, "premise: M's write is stamped above A's");

    premise_applied(&p1, &insert, "P1 applied A's insert of X");
    premise_applied(&p2, &insert, "P2 applied A's insert of X");
    p1.apply_batch(&[s2.clone(), s1.clone()]).unwrap();
    p2.apply_batch(&[s1.clone(), s2.clone()]).unwrap();
    let c1 = p1.get_collection("shop", "orders").unwrap();
    let c2 = p2.get_collection("shop", "orders").unwrap();
    let (v1, d1) = p1.get_versioned(&c1, &x).unwrap().unwrap();
    let (v2, d2) = p2.get_versioned(&c2, &x).unwrap().unwrap();
    assert_eq!(d1, doc! { "_id": 1, "a": 1, "b": 2 });
    assert_eq!(d1, d2, "the same document in either order");
    assert_eq!(v1, v2, "the same version identity and state hash");
    for p in [&p1, &p2] {
        assert!(p.oplog_entry(&s1.stamp).unwrap().is_some(), "s1 is kept");
        assert!(p.oplog_entry(&s2.stamp).unwrap().is_some(), "s2 is kept");
    }
}

/// An entry the receiver's version vector already covers moves nothing and publishes nothing, and
/// is still kept in its oplog (always keep): the member serves it onward.
#[test]
fn a_covered_field_entry_arriving_in_window_is_kept_and_moves_nothing() {
    let (a, _da) = engine();
    let (m, _dm) = engine();
    let (p, _dp) = engine();
    let (c, ca) = Cluster::new(&a, &[&m, &p]);
    later();
    let x = a.insert(&ca, doc! { "_id": 1 }).unwrap();
    let insert = c.after(&a);
    premise_applied(&m, &insert, "M applied A's insert of X");
    premise_applied(&p, &insert, "P applied A's insert of X");
    let cm = m.get_collection("shop", "orders").unwrap();
    let cp = p.get_collection("shop", "orders").unwrap();
    later();
    a.replace(&ca, &x, doc! { "_id": 1, "a": 1 }, false).unwrap();
    let s1 = c.after(&a).last().unwrap().clone();
    premise_applied(&m, std::slice::from_ref(&s1), "M applied A's write s1");
    later();
    // M writes again over what it merged: its entry carries a and b
    m.replace(&cm, &x, doc! { "_id": 1, "a": 1, "b": 2 }, false).unwrap();
    let s3 = c.after(&m).last().unwrap().clone();
    // P takes M's entry first: it covers s1
    p.apply_batch(std::slice::from_ref(&s3)).unwrap();
    assert!(p.oplog_entry(&s1.stamp).unwrap().is_none(), "premise: P has not seen s1's entry");
    let (before, _) = p.get_versioned(&cp, &x).unwrap().unwrap();
    // s1 now arrives: covered
    let out = p.apply_batch(std::slice::from_ref(&s1)).unwrap();
    assert_eq!((out.applied, out.superseded), (0, 1), "{out:?}");
    let (after, _) = p.get_versioned(&cp, &x).unwrap().unwrap();
    assert_eq!(before, after, "the state hash did not move");
    assert!(p.oplog_entry(&s1.stamp).unwrap().is_some(), "the covered entry is kept to be served");
}

/// A delete whose stamp is below the record's stamp still kills a life whose writer had not seen it:
/// the join, not the stamp, decides. Either order of arrival.
#[test]
fn a_carried_delete_below_the_record_stamp_still_kills_the_life_that_did_not_see_it() {
    for delete_first in [false, true] {
        let (a, _da) = engine();
        let (m, _dm) = engine();
        let (p, _dp) = engine();
        let (c, ca) = Cluster::new(&a, &[&m, &p]);
        later();
        let x = a.insert(&ca, doc! { "_id": 1 }).unwrap();
        let insert = c.after(&a);
        premise_applied(&m, &insert, "M applied A's insert of X");
        premise_applied(&p, &insert, "P applied A's insert of X");
        let cm = m.get_collection("shop", "orders").unwrap();
        let cp = p.get_collection("shop", "orders").unwrap();
        later();
        // A deletes X; M, which has not seen the delete, writes X.b above its stamp
        a.delete(&ca, &x).unwrap();
        let delete = c.after(&a).last().unwrap().clone();
        later();
        m.replace(&cm, &x, doc! { "_id": 1, "b": 2 }, false).unwrap();
        let write = c.after(&m).last().unwrap().clone();
        assert!(write.stamp > delete.stamp, "premise: the write is stamped above the delete");
        let order = if delete_first { [delete.clone(), write] } else { [write, delete.clone()] };
        p.apply_batch(&order).unwrap();
        assert_eq!(p.get(&cp, &x).unwrap(), None, "the delete kills the life that did not see it");
        m.apply_batch(std::slice::from_ref(&delete)).unwrap();
        assert_eq!(m.get(&cm, &x).unwrap(), None, "and on M, which wrote it");
    }
}
