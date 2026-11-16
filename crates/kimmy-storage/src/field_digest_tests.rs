//! Step 3 (S3b): what a `field` collection's digest folds. The term of a live record is its key and its
//! state hash, not its stamp: two members whose records stand at the same newest stamp over different
//! merged states must not compare equal. `maxw` rises for every origin the state has seen, not only
//! the origin of the newest write.

use bson::doc;
use kimmy_core::{DocId, MergePolicy, OplogEntry};

use crate::Engine;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(kimmy_core::Hlc::ZERO, 10_000, crate::WalkScope::Background)
        .unwrap()
        .entries
}

/// A writes X.a (s1), M writes X.b above it (s2), M takes A's entry and writes X.c (s3): M's record
/// stands at s3 over {a, b, c}. P takes only M's s2 entry, which carries {b}, and stands at s2.
struct Trio {
    a: Engine,
    m: Engine,
    p: Engine,
    _dirs: Vec<tempfile::TempDir>,
    coll: kimmy_core::CollectionId,
    s1: OplogEntry,
    s2: OplogEntry,
    s3: OplogEntry,
}

fn trio() -> Trio {
    let (a, da) = engine();
    let (m, dm) = engine();
    let (p, dp) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let creation = everything(&a);
    m.apply_batch(&creation).unwrap();
    p.apply_batch(&creation).unwrap();
    let cut = creation.last().unwrap().stamp;
    let cm = m.get_collection("shop", "orders").unwrap();
    later();
    let x = a.insert(&ca, doc! { "_id": 1 }).unwrap();
    let insert: Vec<OplogEntry> = everything(&a).into_iter().filter(|e| e.stamp > cut).collect();
    m.apply_batch(&insert).unwrap();
    p.apply_batch(&insert).unwrap();
    later();
    a.replace(&ca, &x, doc! { "_id": 1, "a": 1 }, false).unwrap();
    let s1 = everything(&a).into_iter().rfind(|e| e.stamp > cut).unwrap();
    later();
    m.replace(&cm, &x, doc! { "_id": 1, "b": 2 }, false).unwrap();
    let s2 = everything(&m).into_iter().rfind(|e| e.stamp > cut).unwrap();
    m.apply_batch(std::slice::from_ref(&s1)).unwrap();
    later();
    m.replace(&cm, &x, doc! { "_id": 1, "a": 1, "b": 2, "c": 3 }, false).unwrap();
    let s3 = everything(&m).into_iter().rfind(|e| e.stamp > cut).unwrap();
    assert!(s1.stamp < s2.stamp && s2.stamp < s3.stamp, "premise: the writes are stamped in order");
    Trio { a, m, p, _dirs: vec![da, dm, dp], coll: ca.id, s1, s2, s3 }
}

#[test]
fn two_records_at_one_stamp_over_different_states_do_not_digest_equal() {
    let t = trio();
    // P holds M's s2 entry only: {b}. M's record stands at s3 over {a, b, c}; make the stamps equal by
    // giving P the entry that M's record stands at: P's record is then s3's state too, so first compare
    // a state that is NOT s3's: P with {b} against a member whose newest stamp is the same s2.
    let (q, _dq) = engine();
    let creation = everything(&t.a).into_iter().filter(|e| e.kind.is_ddl()).collect::<Vec<_>>();
    q.apply_batch(&creation).unwrap();
    let insert: Vec<OplogEntry> =
        everything(&t.m).into_iter().filter(|e| e.stamp < t.s1.stamp && !e.kind.is_ddl()).collect();
    q.apply_batch(&insert).unwrap();
    // q holds {b} at s2; r holds {a, b} at s2 (s1 then s2 in either order, no s3)
    q.apply_batch(std::slice::from_ref(&t.s2)).unwrap();
    let (r, _dr) = engine();
    r.apply_batch(&creation).unwrap();
    r.apply_batch(&insert).unwrap();
    r.apply_batch(&[t.s1.clone(), t.s2.clone()]).unwrap();
    let cq = q.get_collection("shop", "orders").unwrap();
    let cr = r.get_collection("shop", "orders").unwrap();
    let id = DocId::Int64(1);
    let (vq, dq_) = q.get_versioned(&cq, &id).unwrap().unwrap();
    let (vr, dr_) = r.get_versioned(&cr, &id).unwrap().unwrap();
    assert_eq!(dq_, doc! { "_id": 1, "b": 2 }, "premise: q holds {{b}}");
    assert_eq!(dr_, doc! { "_id": 1, "a": 1, "b": 2 }, "premise: r holds {{a, b}}");
    assert_ne!(vq, vr, "premise: the versions differ");
    let rq = q.digest_row(t.coll).unwrap().expect("q's digest row");
    let rr = r.digest_row(t.coll).unwrap().expect("r's digest row");
    assert_ne!(
        rq.digest, rr.digest,
        "different states at the same newest stamp digest differently"
    );
}

#[test]
fn members_with_the_same_state_digest_equal_however_it_arrived() {
    let t = trio();
    let (q, _dq) = engine();
    let (r, _dr) = engine();
    let creation = everything(&t.a).into_iter().filter(|e| e.kind.is_ddl()).collect::<Vec<_>>();
    let insert: Vec<OplogEntry> =
        everything(&t.m).into_iter().filter(|e| e.stamp < t.s1.stamp && !e.kind.is_ddl()).collect();
    for e in [&q, &r] {
        e.apply_batch(&creation).unwrap();
        e.apply_batch(&insert).unwrap();
    }
    q.apply_batch(&[t.s1.clone(), t.s2.clone(), t.s3.clone()]).unwrap();
    r.apply_batch(&[t.s3.clone(), t.s2.clone(), t.s1.clone()]).unwrap();
    assert_eq!(
        q.digest_row(t.coll).unwrap().unwrap().digest,
        r.digest_row(t.coll).unwrap().unwrap().digest
    );
    assert_eq!(
        q.digest_row(t.coll).unwrap().unwrap().digest,
        t.m.digest_row(t.coll).unwrap().unwrap().digest,
        "and equal to the member that wrote them"
    );
}

#[test]
fn max_stamp_per_origin_rises_for_every_origin_the_state_has_seen() {
    let t = trio();
    // P takes only M's s3 entry: its state carries A's s1 and M's s2 and s3.
    let (q, _dq) = engine();
    let creation = everything(&t.a).into_iter().filter(|e| e.kind.is_ddl()).collect::<Vec<_>>();
    let insert: Vec<OplogEntry> =
        everything(&t.m).into_iter().filter(|e| e.stamp < t.s1.stamp && !e.kind.is_ddl()).collect();
    q.apply_batch(&creation).unwrap();
    q.apply_batch(&insert).unwrap();
    q.apply_batch(std::slice::from_ref(&t.s3)).unwrap();
    let row = q.digest_row(t.coll).unwrap().unwrap();
    assert_eq!(
        row.maxw.get(&t.s1.stamp.origin()).copied(),
        Some(t.s1.stamp.hlc),
        "A's origin is raised to the write of A that the state holds: {row:?}"
    );
    let _ = (&t.p,);
}
