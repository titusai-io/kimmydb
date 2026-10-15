//! Break-the-guard review C of the repair build (design v3.5): attempts to make the
//! `Converge` and `Definitions` policies resurrect a deleted document, overwrite data,
//! touch a definition, or build an index they must not. Every test drives the real
//! paths (retention passes, restarts, backups, sidecars, drops, sweeps) rather than
//! setting the horizon by hand, so a hole between two mechanisms shows.
//!
//! Naming: `revc_` prefix, so the review's tests run as one filter.

use std::collections::{BTreeMap, BTreeSet};

use bson::doc;
use kimmy_core::{CollectionId, DocId, Hlc, OpKind, OplogEntry, Stamp};

use crate::gc::RetentionPolicy;
use crate::{Engine, IndexField, SnapshotApplied, SnapshotProgress, WalkScope};

fn path_of(dir: &tempfile::TempDir) -> std::path::PathBuf {
    dir.path().join("kimmy.redb")
}

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&path_of(&dir)).unwrap(), dir)
}

fn orders() -> CollectionId {
    CollectionId::derive("shop", "orders")
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn coll(e: &Engine) -> crate::CollectionMeta {
    e.get_collection("shop", "orders").unwrap()
}

/// Every entry `from` holds, applied to `into`.
fn pull(into: &Engine, from: &Engine) {
    let entries = from.entries_for_peer(Hlc::ZERO, 100_000, WalkScope::Background).unwrap().entries;
    into.apply_batch(&entries).unwrap();
}

/// What a retention pass removes, at an exact tombstone cutoff of `cutoff_ms` (and an
/// oplog cutoff a year earlier, so the oplog is kept and only tombstones go).
fn collect_at(e: &Engine, cutoff_ms: u64) -> crate::gc::GcOutcome {
    const YEAR: u64 = 365 * 24 * 3600;
    e.collect_garbage_at(cutoff_ms + 1_000, RetentionPolicy::new(YEAR, 1)).unwrap()
}

fn now() -> u64 {
    crate::physical_now_ms()
}

/// The raw rows of `shop.orders` on `e`: key to (stamp, live).
fn rows(e: &Engine) -> BTreeMap<Vec<u8>, (Stamp, bool)> {
    rows_of(e, orders())
}

fn rows_of(e: &Engine, id: CollectionId) -> BTreeMap<Vec<u8>, (Stamp, bool)> {
    use redb::ReadableDatabase;
    let txn = e.db().begin_read().unwrap();
    let table = txn.open_table(crate::tables::DOCS).unwrap();
    let mut out = BTreeMap::new();
    let lo: &[u8] = &[];
    for row in table.range::<(u64, &[u8])>((id.0, lo)..(id.0 + 1, lo)).unwrap() {
        let (k, v) = row.unwrap();
        let record = crate::codec::decode_doc_record(v.value()).unwrap();
        out.insert(k.value().1.to_vec(), (record.stamp, record.is_live()));
    }
    out
}

fn key(id: i64) -> Vec<u8> {
    crate::docs::doc_key(&DocId::Int64(id)).unwrap()
}

fn live(e: &Engine, id: i64) -> bool {
    rows(e).get(&key(id)).is_some_and(|(_, live)| *live)
}

fn held(e: &Engine, id: i64) -> bool {
    rows(e).contains_key(&key(id))
}

fn sum(total: &mut SnapshotApplied, done: &SnapshotApplied) {
    total.applied += done.applied;
    total.left += done.left;
    total.left_tombstone += done.left_tombstone;
    total.created += done.created;
    total.superseded += done.superseded;
    total.sender_dropped |= done.sender_dropped;
}

/// The sender's whole scoped snapshot of `shop.orders`, applied to `into` under `Converge`.
fn converge(into: &Engine, from: &Engine) -> SnapshotApplied {
    converge_scope(into, from, orders())
}

fn converge_scope(into: &Engine, from: &Engine, scope: CollectionId) -> SnapshotApplied {
    let mut progress =
        SnapshotProgress::of_collection_converging(scope, kimmy_core::VersionVector::new());
    let mut total = SnapshotApplied::default();
    while !progress.is_complete() {
        let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        let done = into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
        assert!(done.shadows_missing.is_empty() && done.shadows_orphaned.is_empty());
        sum(&mut total, &done);
        if done.sender_dropped {
            break;
        }
    }
    total
}

/// `s` holds `x` live; `r` held it, deleted it after, and has not passed the delete on.
/// Returns `(s, r, dirs, the delete's stamp)`.
fn sender_missed_the_requesters_delete(x: i64) -> (Engine, Engine, [tempfile::TempDir; 2], Stamp) {
    let (s, ds) = engine();
    let (r, dr) = engine();
    s.create_collection("shop", "orders").unwrap();
    pull(&r, &s);
    later();
    s.insert(&coll(&s), doc! { "_id": x, "v": 1 }).unwrap();
    pull(&r, &s);
    later();
    assert!(r.delete(&coll(&r), &DocId::Int64(x)).unwrap());
    let t = rows(&r)[&key(x)].0;
    assert!(!rows(&r)[&key(x)].1, "premise: a tombstone on r");
    assert!(live(&s, x), "premise: s still holds it");
    (s, r, [ds, dr], t)
}

// =====================================================================================
// 1. Never resurrect
// =====================================================================================

/// A retention pass on the requester commits between the sender serving the page and the
/// requester applying it: the tombstone the requester deleted `x` under is gone by the
/// apply, so `x` is a row it holds nothing for, and the horizon read inside the apply's
/// own transaction must see the pass.
#[test]
fn revc_a_retention_pass_between_the_page_and_its_apply_leaves_the_deleted_row() {
    let (s, r, _d, t) = sender_missed_the_requesters_delete(7);
    let page = s.snapshot_page(None, Some(orders())).unwrap();
    later();
    let outcome = collect_at(&r, now());
    assert!(outcome.tombstones_removed >= 1, "premise: the tombstone was collected");
    assert!(!held(&r, 7), "premise: no row on r");
    assert!(r.tombstone_horizon().unwrap().unwrap() > t.hlc, "premise: horizon above t");
    let mut progress =
        SnapshotProgress::of_collection_converging(orders(), kimmy_core::VersionVector::new());
    let done = r.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
    assert!(!held(&r, 7), "RESURRECTED: a collected delete came back through Converge");
    assert_eq!((done.created, done.left), (0, 1));
}

/// The same between two pages of one pull: page one is applied while the requester still
/// holds its tombstones (an older live row is refused by stamp), the retention pass
/// collects them, and page two finds rows it holds nothing for.
#[test]
fn revc_a_retention_pass_between_two_converge_pages_resurrects_nothing() {
    let (s, sd) = engine();
    let (r, rd) = engine();
    let _keep = (sd, rd);
    s.create_collection("shop", "orders").unwrap();
    pull(&r, &s);
    later();
    let n = crate::SNAPSHOT_PAGE as i64 * 2 + 17;
    let docs: Vec<_> = (0..n).map(|i| doc! { "_id": i, "v": 1 }).collect();
    s.insert_many(&coll(&s), docs).unwrap();
    pull(&r, &s);
    later();
    for i in 0..n {
        assert!(r.delete(&coll(&r), &DocId::Int64(i)).unwrap());
    }
    let mut progress =
        SnapshotProgress::of_collection_converging(orders(), kimmy_core::VersionVector::new());
    let mut total = SnapshotApplied::default();
    let mut pages = 0;
    while !progress.is_complete() {
        let page = s.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        let done = r.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
        sum(&mut total, &done);
        pages += 1;
        if pages == 1 {
            later();
            assert!(collect_at(&r, now()).tombstones_removed > 0, "premise");
        }
    }
    assert!(pages >= 3, "premise: several pages");
    let resurrected = (0..n).filter(|i| live(&r, *i)).count();
    assert_eq!(resurrected, 0, "RESURRECTED {resurrected} rows; {total:?}");
    assert_eq!(total.created, 0);
}

/// A retention pass under one retention, a restart, and passes under a far longer one:
/// the horizon is persisted and monotone, so a row the first pass made ambiguous stays
/// ambiguous whatever the later retention says.
#[test]
fn revc_a_restart_with_a_raised_retention_admits_nothing_below_the_old_cutoff() {
    let (s, r, dirs, t) = sender_missed_the_requesters_delete(7);
    collect_at(&r, now());
    assert!(!held(&r, 7), "premise");
    let horizon = r.tombstone_horizon().unwrap().unwrap();
    let path = path_of(&dirs[1]);
    drop(r);
    let r = Engine::open(&path).unwrap();
    // A day of retention now, and passes that collect something old under it.
    let day = RetentionPolicy::new(86_400 * 365, 86_400);
    r.collect_garbage_at(now(), day).unwrap();
    assert_eq!(r.tombstone_horizon().unwrap().unwrap(), horizon, "the horizon moved back");
    assert!(horizon > t.hlc);
    let done = converge(&r, &s);
    assert!(!held(&r, 7), "RESURRECTED after a restart with a raised retention");
    assert_eq!((done.created, done.left), (0, 1));
}

/// The requester restores a backup taken before `x` existed, after deleting `x` and
/// collecting the tombstone. The restored identity has forgotten the delete; its horizon
/// is the restore's own time, above the delete.
#[test]
fn revc_a_restore_of_this_members_old_backup_admits_nothing_it_deleted_since() {
    let (s, sd) = engine();
    let (r, rd) = engine();
    s.create_collection("shop", "orders").unwrap();
    pull(&r, &s);
    let mut backup = Vec::new();
    r.backup_to(&mut backup, WalkScope::Request).unwrap();
    later();
    s.insert(&coll(&s), doc! { "_id": 7, "v": 1 }).unwrap();
    pull(&r, &s);
    later();
    assert!(r.delete(&coll(&r), &DocId::Int64(7)).unwrap());
    let t = rows(&r)[&key(7)].0;
    collect_at(&r, now());
    assert!(!held(&r, 7));
    drop(r);
    // The restore, into the member's own path.
    let target = tempfile::tempdir().unwrap();
    crate::backup::restore(&path_of(&target), &mut backup.as_slice()).unwrap();
    let r = Engine::open(&path_of(&target)).unwrap();
    assert!(!held(&r, 7), "premise: the backup predates x");
    assert!(r.tombstone_horizon().unwrap().unwrap() > t.hlc, "premise");
    let done = converge(&r, &s);
    assert!(!live(&r, 7), "RESURRECTED by a Converge after a restore");
    assert_eq!(done.created, 0);
    drop((sd, rd));
}

/// The sender is the one restored: it deleted `x`, collected the tombstone, and restored a
/// backup that holds `x` live. A requester that never held `x` (it lost the insert and the
/// delete) must not take `x` from it: the sender's horizon, reset by the restore, covers it.
#[test]
fn revc_a_sender_restored_from_a_backup_that_holds_a_row_it_deleted_does_not_hand_it_out() {
    let (s, sd) = engine();
    let (r, _rd) = engine();
    s.create_collection("shop", "orders").unwrap();
    pull(&r, &s);
    later();
    s.insert(&coll(&s), doc! { "_id": 7, "v": 1 }).unwrap();
    let mut backup = Vec::new();
    s.backup_to(&mut backup, WalkScope::Request).unwrap();
    later();
    assert!(s.delete(&coll(&s), &DocId::Int64(7)).unwrap());
    let t = rows(&s)[&key(7)].0;
    later();
    collect_at(&s, now());
    drop(s);
    drop(sd);
    let target = tempfile::tempdir().unwrap();
    crate::backup::restore(&path_of(&target), &mut backup.as_slice()).unwrap();
    let s = Engine::open(&path_of(&target)).unwrap();
    assert!(live(&s, 7), "premise: the restored sender holds x live");
    assert!(s.tombstone_horizon().unwrap().unwrap() > t.hlc, "premise");
    let done = converge(&r, &s);
    assert!(!live(&r, 7), "RESURRECTED: a restored sender's stale row was created");
    assert_eq!((done.created, done.left), (0, 1));
}

/// A foreign build (one that does not maintain the key) collected the tombstone; the key
/// still says what this build last wrote, below the delete. The roll back to this build
/// resets the key from the prior sidecar.
#[test]
fn revc_a_roll_back_after_a_foreign_build_collected_admits_nothing_it_collected() {
    let (s, r, dirs, t) = sender_missed_the_requesters_delete(7);
    let before = r.tombstone_horizon().unwrap().unwrap();
    assert!(before < t.hlc, "premise: the key is below the delete");
    // The foreign build's pass: removes the tombstone, leaves the key as it was.
    collect_at(&r, now());
    r.test_set_tombstone_horizon(before).unwrap();
    let path = path_of(&dirs[1]);
    drop(r);
    let mut sidecar = crate::format::read_sidecar(&path).expect("a sidecar");
    sidecar.written_by = "0.48.0".into();
    crate::format::write_sidecar(&path, &sidecar).unwrap();
    let r = Engine::open(&path).unwrap();
    assert!(!held(&r, 7), "premise");
    let done = converge(&r, &s);
    assert!(!held(&r, 7), "RESURRECTED after a roll back from a foreign build");
    assert_eq!(done.created, 0);
}

/// BREAK (Major): the reset is keyed on `written_by`, which is `CARGO_PKG_VERSION`, and this
/// branch still builds as **0.49.0**, the same string the 0.49.0 release writes. A store the
/// repair build wrote, run by the 0.49.0 release (which collects tombstones and does not
/// maintain the key), then rolled forward to the repair build, is trusted: the stale key
/// stands and the young-stamp rule creates a row this member deleted and collected.
/// The rollback gate's row "a store rolled to 0.49, run, and rolled forward" and the lab's
/// mixed roll (row 8) run this branch against the 0.49.0 release, so both see it.
#[test]
fn revc_break_a_roll_through_the_0_49_0_release_is_invisible_to_this_branchs_build() {
    let (s, r, dirs, t) = sender_missed_the_requesters_delete(7);
    let before = r.tombstone_horizon().unwrap().unwrap();
    assert!(before < t.hlc, "premise: the key is below the delete");
    // The 0.49.0 release's pass: removes the tombstone, leaves the key as it was.
    collect_at(&r, now());
    r.test_set_tombstone_horizon(before).unwrap();
    let path = path_of(&dirs[1]);
    drop(r);
    // The 0.49.0 release writes its own version into the sidecar.
    let mut sidecar = crate::format::read_sidecar(&path).expect("a sidecar");
    sidecar.written_by = "0.49.0".into();
    crate::format::write_sidecar(&path, &sidecar).unwrap();
    let r = Engine::open(&path).unwrap();
    let done = converge(&r, &s);
    println!(
        "REVC SAME-VERSION this build writes {:?}; horizon after the roll forward {:?}, delete {:?}; {done:?}",
        kimmy_core::build::VERSION,
        r.tombstone_horizon().unwrap(),
        t.hlc
    );
    assert!(
        !held(&r, 7),
        "RESURRECTED: a roll through a build with the same version string ({}) kept the stale \
         horizon and created a row this member deleted and collected",
        kimmy_core::build::VERSION
    );
}

/// The same, with the sidecar gone altogether (a store copied without it).
#[test]
fn revc_a_store_whose_sidecar_is_gone_resets_and_admits_nothing_it_collected() {
    let (s, r, dirs, _t) = sender_missed_the_requesters_delete(7);
    let before = r.tombstone_horizon().unwrap().unwrap();
    collect_at(&r, now());
    r.test_set_tombstone_horizon(before).unwrap();
    let path = path_of(&dirs[1]);
    drop(r);
    std::fs::remove_file(crate::format::sidecar_path(&path)).unwrap();
    let r = Engine::open(&path).unwrap();
    let done = converge(&r, &s);
    assert!(!held(&r, 7), "RESURRECTED after a start with no sidecar");
    assert_eq!(done.created, 0);
}

/// A kill after the pre-open sidecar write and before the reset commit: the next start
/// must still see the foreign build. Emulated by the pre-open write alone (the open that
/// would commit the reset is never made), then an ordinary open.
#[test]
fn revc_a_kill_between_the_sidecar_write_and_the_reset_still_resets_on_the_next_start() {
    let (s, r, dirs, _t) = sender_missed_the_requesters_delete(7);
    let before = r.tombstone_horizon().unwrap().unwrap();
    collect_at(&r, now());
    r.test_set_tombstone_horizon(before).unwrap();
    let path = path_of(&dirs[1]);
    drop(r);
    let mut sidecar = crate::format::read_sidecar(&path).expect("a sidecar");
    sidecar.written_by = "0.48.0".into();
    crate::format::write_sidecar(&path, &sidecar).unwrap();
    // The start that is killed: it writes the pre-open sidecar and nothing else.
    let cleared = crate::format::check_before_open(&path).unwrap();
    if let Some(write) = cleared.sidecar_before_open() {
        crate::format::write_sidecar(&path, &write).unwrap();
    }
    let r = Engine::open(&path).unwrap();
    let done = converge(&r, &s);
    assert!(!held(&r, 7), "RESURRECTED after a kill in the reset window");
    assert_eq!(done.created, 0);
}

/// A whole-database snapshot from a sender whose horizon is lower than the receiver's
/// leaves the receiver's horizon where it was (adoption is a max), so a row the receiver's
/// own collection made ambiguous stays ambiguous for every later Converge.
#[test]
fn revc_a_whole_database_page_never_lowers_the_horizon_a_converge_reads() {
    let (s, r, _d, t) = sender_missed_the_requesters_delete(7);
    collect_at(&r, now());
    let high = r.tombstone_horizon().unwrap().unwrap();
    assert!(high > t.hlc);
    // A third member, empty but for the collection, with a horizon at its start: lower.
    let (c, _dc) = engine();
    c.create_collection("shop", "orders").unwrap();
    let mut progress = SnapshotProgress::whole_database();
    while !progress.is_complete() {
        let mut page = c.snapshot_page(progress.after().cloned(), None).unwrap();
        page.tombstone_horizon = Some(Hlc::new(1, 0));
        r.apply_snapshot_page(c.node_id(), &mut progress, &page).unwrap();
    }
    assert_eq!(r.tombstone_horizon().unwrap().unwrap(), high, "adoption lowered the horizon");
    let done = converge(&r, &s);
    assert!(!held(&r, 7));
    assert_eq!(done.created, 0);
}

fn insert_entry(collection: CollectionId, id: i64, at: Stamp) -> OplogEntry {
    OplogEntry {
        stamp: at,
        kind: OpKind::Insert,
        collection,
        doc_id: Some(DocId::Int64(id)),
        body: Some(bson::serialize_to_vec(&doc! { "_id": id, "v": 1 }).unwrap()),
    }
}

fn delete_entry(collection: CollectionId, id: i64, at: Stamp) -> OplogEntry {
    OplogEntry {
        stamp: at,
        kind: OpKind::Delete,
        collection,
        doc_id: Some(DocId::Int64(id)),
        body: None,
    }
}

/// Clock skew both ways: the sender's clock is an hour fast when it writes `x`, the
/// requester deletes `x` an hour of its own clock later than that, and collects when its
/// slow clock finally passes the delete. Every comparison is between stamps, so the skew
/// moves when things happen and never the order of `s` and the cutoff.
#[test]
fn revc_clock_skew_cannot_make_a_collected_delete_young() {
    let (s, _sd) = engine();
    let (r, _rd) = engine();
    s.create_collection("shop", "orders").unwrap();
    pull(&r, &s);
    let hour = 3_600_000;
    let fast = Stamp::new(Hlc::new(now() + hour, 0), s.node_id());
    s.apply_batch(&[insert_entry(orders(), 7, fast)]).unwrap();
    pull(&r, &s);
    assert!(live(&r, 7), "premise");
    // The requester's delete: its HLC has witnessed the fast stamp, so the delete is after.
    assert!(r.delete(&coll(&r), &DocId::Int64(7)).unwrap());
    let t = rows(&r)[&key(7)].0;
    assert!(t.hlc > fast.hlc, "premise: the delete sorts after the fast write");
    // Collected only once the cutoff passes the delete, an hour of wall time ahead.
    collect_at(&r, t.hlc.wall_ms + 1);
    assert!(!held(&r, 7), "premise: collected");
    let done = converge(&r, &s);
    assert!(!held(&r, 7), "RESURRECTED under clock skew");
    assert_eq!(done.created, 0);
    // And a write stamped an hour in the past (a slow clock elsewhere): a delete here is
    // still stamped after what this member has seen.
    let (r2, _r2d) = engine();
    pull(&r2, &s);
    let early = Stamp::new(Hlc::new(now() - hour, 0), s.node_id());
    s.apply_batch(&[insert_entry(orders(), 8, early)]).unwrap();
    r2.apply_batch(&[insert_entry(orders(), 8, early)]).unwrap();
    assert!(r2.delete(&r2.get_collection("shop", "orders").unwrap(), &DocId::Int64(8)).unwrap());
    let t2 = rows(&r2)[&key(8)].0;
    collect_at(&r2, t2.hlc.wall_ms + 1);
    assert!(!held(&r2, 8), "premise");
    let done = converge(&r2, &s);
    assert!(!held(&r2, 8), "RESURRECTED under a slow requester clock: {done:?}");
}

/// A third member's delete, within its retention, that neither member of the pair has
/// applied: the requester lost the insert too, so the pair's repair creates `x` (young),
/// and the third member's tombstone, a row, wins it back by the ordinary merge in the next
/// Converge. What must hold: every member ends with `x` deleted.
#[test]
fn revc_a_third_members_delete_within_retention_wins_after_a_young_create() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let (c, _dc) = engine();
    a.create_collection("shop", "orders").unwrap();
    pull(&b, &a);
    pull(&c, &a);
    later();
    a.insert(&coll(&a), doc! { "_id": 7, "v": 1 }).unwrap();
    pull(&c, &a);
    later();
    // C deletes x; nobody hears.
    assert!(c.delete(&coll(&c), &DocId::Int64(7)).unwrap());
    // B never got the insert: its repair from A creates x, young.
    let done = converge(&b, &a);
    assert_eq!(done.created, 1, "premise: the young lost insert is written");
    // Each pair then repairs: B and A from C.
    converge(&b, &c);
    converge(&a, &c);
    for (name, e) in [("a", &a), ("b", &b), ("c", &c)] {
        assert!(!live(e, 7), "RESURRECTED on {name}: a delete within retention lost");
    }
}

/// The pairwise statement, measured: when the third member has COLLECTED its delete and
/// neither member of the pair ever applied it, the repair creates the row. By design (ADR-085,
/// the pairwise guarantee of section 13.3); recorded so the report can say it was seen.
#[test]
fn revc_a_third_members_collected_delete_is_the_documented_pairwise_window() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let (c, _dc) = engine();
    a.create_collection("shop", "orders").unwrap();
    pull(&b, &a);
    pull(&c, &a);
    later();
    a.insert(&coll(&a), doc! { "_id": 7, "v": 1 }).unwrap();
    pull(&c, &a);
    later();
    assert!(c.delete(&coll(&c), &DocId::Int64(7)).unwrap());
    later();
    collect_at(&c, now());
    let done = converge(&b, &a);
    // The pair's horizons are both below the insert: admitted, as the design says.
    assert_eq!(done.created, 1, "the documented window did not open: {done:?}");
}

/// Minor m1 (convergence), ruled: **the ADR-163 coverage gate stays**, and the heal is through
/// the original deleter. C deletes `x`; A and B lose the entry (witnessed, not applied). B's
/// repair from C applies the tombstone over B's live row. A's repair from B then ignores B's
/// tombstone, because B's own coverage (`versions`) never named a row a repair applied under
/// `Hold`: relaying it would grant a member coverage of history it did not itself receive.
/// A's own repair from C, which does cover the stamp, applies it. The finding between A and B
/// stands until then and clears; nothing is resurrected. The cost, documented in ADR-237: with
/// the origin gone, a member that lost the delete is not healed by a member that learned it
/// by repair.
#[test]
fn revc_minor_a_repaired_delete_is_not_relayed_but_heals_through_the_original_deleter() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let (c, _dc) = engine();
    a.create_collection("shop", "orders").unwrap();
    pull(&b, &a);
    pull(&c, &a);
    later();
    a.insert(&coll(&a), doc! { "_id": 7, "v": 1 }).unwrap();
    pull(&b, &a);
    pull(&c, &a);
    later();
    assert!(c.delete(&coll(&c), &DocId::Int64(7)).unwrap());
    // A and B witness C's delete and do not apply it.
    let theirs = c.witnessed_vector().unwrap();
    a.absorb_witnessed(&theirs).unwrap();
    b.absorb_witnessed(&theirs).unwrap();
    let from_c = converge(&b, &c);
    assert_eq!(from_c.applied, 1, "premise: B took the delete from C");
    assert!(!live(&b, 7));
    // The gate: B's coverage of C names nothing, so A takes no tombstone of C's stamp from B.
    converge(&a, &b);
    assert!(live(&a, 7), "the coverage gate (ADR-163) was bypassed by a relayed repair");
    // The heal: A's repair from the original deleter.
    let from_c = converge(&a, &c);
    assert_eq!(from_c.applied, 1, "A took the delete from C: {from_c:?}");
    assert!(!live(&a, 7), "the repair from the original deleter did not heal A");
    for (name, e) in [("a", &a), ("b", &b), ("c", &c)] {
        assert!(!live(e, 7), "RESURRECTED on {name}");
    }
}

/// The requester drops the collection between the finding and the repair.
#[test]
fn revc_a_collection_drop_between_the_finding_and_the_apply_writes_nothing() {
    let (s, r, _d, _t) = sender_missed_the_requesters_delete(7);
    s.insert(&coll(&s), doc! { "_id": 8, "v": 1 }).unwrap();
    let page = s.snapshot_page(None, Some(orders())).unwrap();
    assert!(r.drop_collection("shop", "orders").unwrap());
    let mut progress =
        SnapshotProgress::of_collection_converging(orders(), kimmy_core::VersionVector::new());
    let done = r.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
    r.finish_purges_now().unwrap();
    assert!(r.get_collection("shop", "orders").is_err(), "the repair recreated the collection");
    assert!(rows(&r).is_empty(), "rows were written under a dropped collection: {:?}", rows(&r));
    assert_eq!(done.created + done.applied, 0, "{done:?}");
}

/// Dropped and made again between the finding and the apply: the page is the old life's
/// (the sender never saw the drop), and none of it may land in the new one, young or not.
#[test]
fn revc_a_drop_and_recreate_between_the_finding_and_the_apply_writes_nothing_of_the_old_life() {
    let (s, sd) = engine();
    let (r, rd) = engine();
    let _keep = (sd, rd);
    s.create_collection("shop", "orders").unwrap();
    pull(&r, &s);
    later();
    // Young rows on the sender the requester never got (would be created if admitted).
    for i in 0..5 {
        s.insert(&coll(&s), doc! { "_id": i, "v": 1 }).unwrap();
    }
    let page = s.snapshot_page(None, Some(orders())).unwrap();
    later();
    assert!(r.drop_collection("shop", "orders").unwrap());
    r.finish_purges_now().unwrap();
    later();
    r.create_collection("shop", "orders").unwrap();
    let mut progress =
        SnapshotProgress::of_collection_converging(orders(), kimmy_core::VersionVector::new());
    let done = r.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
    assert!(rows(&r).is_empty(), "the old life's rows landed in the new one: {done:?}");
    assert_eq!(done.created, 0);
}

/// The same through the database's drop (ADR-229).
#[test]
fn revc_a_database_drop_and_recreate_between_the_finding_and_the_apply_writes_nothing() {
    let (s, sd) = engine();
    let (r, rd) = engine();
    let _keep = (sd, rd);
    s.create_collection("shop", "orders").unwrap();
    pull(&r, &s);
    later();
    for i in 0..5 {
        s.insert(&coll(&s), doc! { "_id": i, "v": 1 }).unwrap();
    }
    let page = s.snapshot_page(None, Some(orders())).unwrap();
    later();
    assert!(r.drop_database("shop").unwrap());
    r.finish_purges_now().unwrap();
    later();
    r.create_collection("shop", "orders").unwrap();
    let done = {
        let mut progress =
            SnapshotProgress::of_collection_converging(orders(), kimmy_core::VersionVector::new());
        r.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap()
    };
    assert!(rows(&r).is_empty(), "a life the database drop ended was written back: {done:?}");
    assert_eq!(done.created, 0);
    // And a later full Converge of the same sender.
    let again = converge(&r, &s);
    assert!(rows(&r).is_empty(), "{again:?}");
}

/// The drop's own tombstone collected before the collection is made again: the new life
/// has no floor and no tombstone over the old rows. Only the horizon stands between the
/// old life and the new one.
#[test]
fn revc_an_old_life_after_its_drop_tombstone_was_collected_is_not_written_back() {
    let (s, sd) = engine();
    let (r, rd) = engine();
    let _keep = (sd, rd);
    s.create_collection("shop", "orders").unwrap();
    pull(&r, &s);
    later();
    for i in 0..5 {
        s.insert(&coll(&s), doc! { "_id": i, "v": 1 }).unwrap();
    }
    pull(&r, &s);
    later();
    assert!(r.drop_collection("shop", "orders").unwrap());
    r.finish_purges_now().unwrap();
    later();
    let outcome = collect_at(&r, now());
    assert!(outcome.tombstones_removed >= 1, "premise: the drop's tombstone collected");
    assert!(r.collection_dropped_at(orders()).unwrap().is_none(), "premise");
    later();
    r.create_collection("shop", "orders").unwrap();
    assert!(coll(&r).incarnation_floor.is_none(), "premise: no floor");
    let done = converge(&r, &s);
    assert!(
        rows(&r).is_empty(),
        "RESURRECTED the old life after its tombstone was collected: {done:?}"
    );
}

/// The same through the database's tombstone, collected before the recreate.
#[test]
fn revc_an_old_life_after_its_database_tombstone_was_collected_is_not_written_back() {
    let (s, sd) = engine();
    let (r, rd) = engine();
    let _keep = (sd, rd);
    s.create_collection("shop", "orders").unwrap();
    pull(&r, &s);
    later();
    for i in 0..5 {
        s.insert(&coll(&s), doc! { "_id": i, "v": 1 }).unwrap();
    }
    pull(&r, &s);
    later();
    assert!(r.drop_database("shop").unwrap());
    r.finish_purges_now().unwrap();
    later();
    assert!(collect_at(&r, now()).tombstones_removed >= 1, "premise");
    later();
    r.create_collection("shop", "orders").unwrap();
    assert!(coll(&r).incarnation_floor.is_none(), "premise: no floor");
    let done = converge(&r, &s);
    assert!(rows(&r).is_empty(), "RESURRECTED: {done:?}");
}

/// A Converge page applied between two chunks of ADR-231's sweep. The requester's life
/// began after a drop it learned late (it created the name independently) and holds the
/// old life's stragglers; between two chunks the life owes the sweep. The sender, which
/// never saw the drop, still holds the old life: below the drop it is history by the
/// tombstone, and above it nothing young may be written while the sweep is owed.
#[test]
fn revc_a_converge_page_between_two_sweep_chunks_writes_nothing_of_the_life_before() {
    let (a, _da) = engine();
    let (s, _ds) = engine();
    let (r, _dr) = engine();
    let on_a = a.create_collection("shop", "orders").unwrap();
    // More than a chunk of the old life, so the sweep takes several chunks.
    let docs: Vec<_> = (0..2_500i64).map(|i| doc! { "_id": i, "v": 1 }).collect();
    a.insert_many(&on_a, docs).unwrap();
    pull(&s, &a);
    later();
    // R creates the name on its own, after A's drop.
    assert!(a.drop_collection("shop", "orders").unwrap());
    let drop_stamp = a.collection_dropped_at(orders()).unwrap().unwrap();
    later();
    r.create_collection("shop", "orders").unwrap();
    // The old life's stragglers reach R before the drop (entries arrive in stamp order, so
    // apply the inserts only), then the drop.
    let entries = a.entries_for_peer(Hlc::ZERO, 100_000, WalkScope::Background).unwrap().entries;
    let (before, after): (Vec<_>, Vec<_>) = entries.into_iter().partition(|e| e.stamp < drop_stamp);
    r.apply_batch(&before).unwrap();
    assert!(rows(&r).len() >= 2_500, "premise: stragglers held");
    // The sender also wrote into the old life after the drop it never saw: young rows.
    later();
    for i in 10_000..10_005i64 {
        s.insert(&coll(&s), doc! { "_id": i, "v": 1 }).unwrap();
    }
    let r = std::rc::Rc::new(r);
    let s = std::rc::Rc::new(s);
    let result = std::rc::Rc::new(std::cell::RefCell::new(None));
    {
        let (r2, s2, out) =
            (std::rc::Rc::clone(&r), std::rc::Rc::clone(&s), std::rc::Rc::clone(&result));
        crate::late_drop::hooks::run_after_the_next_chunk(move || {
            let meta = r2.get_collection("shop", "orders").unwrap();
            let owes = crate::late_drop::owes_a_sweep(&meta, drop_stamp.hlc);
            let done = converge(&r2, &s2);
            *out.borrow_mut() = Some((owes, done, rows(&r2)));
        });
    }
    r.apply_batch(&after).unwrap();
    let (owes, done, mid) = result.borrow_mut().take().expect("the sweep ran more than one chunk");
    assert!(owes, "premise: the life owed the sweep between chunks");
    let old_life: Vec<_> = (0..2_500i64)
        .filter(|i| mid.get(&key(*i)).is_some_and(|(stamp, live)| *live && *stamp < drop_stamp))
        .collect();
    // Stragglers the sweep has not reached yet are still there; the page must not have
    // added to them or written the young rows.
    let young: Vec<_> = (10_000..10_005i64).filter(|i| mid.contains_key(&key(*i))).collect();
    assert!(
        young.is_empty(),
        "a young row was written while the sweep was owed: {young:?} {done:?}"
    );
    assert_eq!(done.created, 0, "{done:?}");
    assert_eq!(done.applied, 0, "the page applied old-life rows mid-sweep: {done:?}");
    let _ = old_life;
    // After the sweep: no old-life row survives on R.
    let end = rows(&r);
    let survivors = (0..2_500i64).filter(|i| end.get(&key(*i)).is_some_and(|(_, l)| *l)).count();
    assert_eq!(survivors, 0, "old-life rows survived the sweep");
}

/// A re-dated life (ADR-231 extended, the survivors path): R held the old life, wrote into it
/// after a drop it had not seen, then took the drop. The life is re-dated to just after the
/// drop and swept; the sender never saw the drop and still holds the old life's rows, and one
/// it wrote after the drop. Below the drop nothing may come back; above it the row is a write
/// the drop does not cover, written when young.
#[test]
fn revc_a_redated_life_takes_nothing_of_the_life_before_from_a_sender_that_missed_the_drop() {
    let (a, _da) = engine();
    let (r, _dr) = engine();
    let (s, _ds) = engine();
    let on_a = a.create_collection("shop", "orders").unwrap();
    let docs: Vec<_> = (0..1_500i64).map(|i| doc! { "_id": i, "v": 1 }).collect();
    a.insert_many(&on_a, docs).unwrap();
    pull(&r, &a);
    pull(&s, &a);
    later();
    assert!(a.drop_collection("shop", "orders").unwrap());
    let dropped = a.collection_dropped_at(orders()).unwrap().unwrap();
    later();
    // R and S both write into the old life after the drop, without having seen it.
    r.insert(&coll(&r), doc! { "_id": 5_000i64, "v": 1 }).unwrap();
    s.insert(&coll(&s), doc! { "_id": 6_000i64, "v": 1 }).unwrap();
    // A Converge between two chunks of R's sweep, and one after it.
    let r = std::rc::Rc::new(r);
    let s = std::rc::Rc::new(s);
    let mid = std::rc::Rc::new(std::cell::RefCell::new(None));
    {
        let (r2, s2, out) =
            (std::rc::Rc::clone(&r), std::rc::Rc::clone(&s), std::rc::Rc::clone(&mid));
        crate::late_drop::hooks::run_after_the_next_chunk(move || {
            let done = converge(&r2, &s2);
            *out.borrow_mut() = Some((done, rows(&r2)));
        });
    }
    pull(&r, &a);
    let standing = coll(&r);
    assert!(standing.is_redated(), "premise: the survivors path re-dated the life");
    let (during, rows_mid) = mid.borrow_mut().take().expect("the sweep ran more than one chunk");
    let below = |rows: &BTreeMap<Vec<u8>, (Stamp, bool)>| {
        (0..1_500i64)
            .filter(|i| rows.get(&key(*i)).is_some_and(|(st, l)| *l && *st < dropped))
            .count()
    };
    let swept_mid = below(&rows_mid);
    let after = converge(&r, &s);
    let end = rows(&r);
    assert_eq!(
        below(&end),
        0,
        "RESURRECTED old-life rows in a re-dated life: during={during:?} after={after:?}"
    );
    assert_eq!(
        during.created + during.applied,
        0,
        "the page wrote into a life that owed its sweep: {during:?}"
    );
    assert!(end.get(&key(5_000)).is_some_and(|(_, l)| *l), "R's own later write was lost");
    println!("REVC REDATED stragglers mid-sweep={swept_mid} during={during:?} after={after:?}");
}

// =====================================================================================
// 2. A Converge never touches a definition
// =====================================================================================

fn vector_config_dot(dim: usize) -> kimmy_core::VectorConfig {
    kimmy_core::VectorConfig { metric: kimmy_core::Metric::Dot, ..vector_config(dim) }
}

fn vector_config(dim: usize) -> kimmy_core::VectorConfig {
    kimmy_core::VectorConfig {
        fields: vec!["body".into()],
        provider: kimmy_core::ProviderConfig::Byo {},
        dim,
        metric: kimmy_core::Metric::Cosine,
        document_prefix: None,
        query_prefix: None,
        chunk: Default::default(),
    }
}

/// Everything about definitions this member holds, as bytes: every collection's metadata,
/// the collection and database tombstones, and the index tombstones.
type Definitions = (Vec<u8>, Vec<(u64, Vec<u8>)>, Vec<((u64, u32), Vec<u8>)>);

fn definitions(e: &Engine) -> Definitions {
    use redb::{ReadableDatabase, ReadableTable};
    let txn = e.db().begin_read().unwrap();
    let mut metas = Vec::new();
    for row in txn.open_table(crate::tables::COLLECTIONS).unwrap().iter().unwrap() {
        let (_, v) = row.unwrap();
        metas.extend_from_slice(v.value());
        metas.push(b'\n');
    }
    let dropped = txn
        .open_table(crate::tables::COLLECTIONS_DROPPED)
        .unwrap()
        .iter()
        .unwrap()
        .map(|r| {
            let (k, v) = r.unwrap();
            (k.value(), v.value().to_vec())
        })
        .collect();
    let indexes = txn
        .open_table(crate::tables::INDEXES_DROPPED)
        .unwrap()
        .iter()
        .unwrap()
        .map(|r| {
            let (k, v) = r.unwrap();
            (k.value(), v.value().to_vec())
        })
        .collect();
    (metas, dropped, indexes)
}

/// The requester holds a TTL index, a unique index, a plain index, a vector configuration
/// and a second collection; the sender's page carries a rival TTL, a rival unique, an index
/// the requester lacks, another vector configuration, the drop of the requester's plain
/// index (newer), the tombstone of the requester's second collection, and documents.
/// Every definition must come out byte-identical; only documents change.
#[test]
fn revc_a_converge_leaves_every_definition_byte_identical() {
    let (s, sd) = engine();
    let (r, rd) = engine();
    let _keep = (sd, rd);
    s.create_collection("shop", "orders").unwrap();
    s.create_collection("shop", "other").unwrap();
    pull(&r, &s);
    later();
    let ttl = |e: &Engine, secs: i64| {
        e.create_index_with(
            "shop",
            "orders",
            vec![IndexField::ascending("t")],
            false,
            crate::Enforcement::Local,
            Some("ttl_t".into()),
            Some(secs),
            None,
        )
        .unwrap();
    };
    // The requester's own.
    ttl(&r, 3600);
    r.create_index("shop", "orders", vec![IndexField::ascending("u")], true, Some("uniq_u".into()))
        .unwrap();
    r.create_index("shop", "orders", vec![IndexField::ascending("k")], false, Some("plain".into()))
        .unwrap();
    r.configure_vectors("shop", "orders", vector_config(8)).unwrap();
    r.insert(&coll(&r), doc! { "_id": 1, "v": 1, "t": bson::DateTime::now(), "u": 1, "k": 1 })
        .unwrap();
    pull(&s, &r);
    later();
    // The sender diverges in every way a page can carry.
    s.drop_index("shop", "orders", "ttl_t").unwrap();
    ttl(&s, 5);
    s.drop_index("shop", "orders", "uniq_u").unwrap();
    s.create_index(
        "shop",
        "orders",
        vec![IndexField::ascending("u2")],
        true,
        Some("uniq_u".into()),
    )
    .unwrap();
    s.create_index(
        "shop",
        "orders",
        vec![IndexField::ascending("w")],
        false,
        Some("lacked".into()),
    )
    .unwrap();
    s.drop_index("shop", "orders", "plain").unwrap();
    s.configure_vectors("shop", "orders", vector_config_dot(8)).unwrap();
    s.drop_collection("shop", "other").unwrap();
    s.replace(
        &coll(&s),
        &DocId::Int64(1),
        doc! { "_id": 1, "v": 2, "u": 9, "u2": 1, "k": 9 },
        false,
    )
    .unwrap();
    s.insert(&coll(&s), doc! { "_id": 2, "v": 2, "u2": 2 }).unwrap();
    let page = s.snapshot_page(None, Some(orders())).unwrap();
    assert!(!page.collections.is_empty(), "premise: the page carries definitions");
    assert!(!page.dropped_indexes.is_empty(), "premise: the page carries index drops");
    let before = definitions(&r);
    let mut progress =
        SnapshotProgress::of_collection_converging(orders(), kimmy_core::VersionVector::new());
    let mut done = r.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
    while !progress.is_complete() {
        let page = s.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        done = r.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
    }
    let after = definitions(&r);
    assert_eq!(before.1, after.1, "collection or database tombstones changed");
    assert_eq!(before.2, after.2, "index tombstones changed");
    assert_eq!(
        String::from_utf8_lossy(&before.0),
        String::from_utf8_lossy(&after.0),
        "a collection's metadata changed under Converge ({done:?})"
    );
    assert!(r.get_collection("shop", "other").is_ok(), "the carried collection drop applied");
    // The documents did move.
    assert_eq!(
        r.get(&coll(&r), &DocId::Int64(1)).unwrap().unwrap().get_i32("v").unwrap(),
        2,
        "premise: the newer row landed"
    );
}

/// The same requirement under `Definitions`: one index built, everything else
/// byte-identical, and no document touched.
#[test]
fn revc_a_definitions_page_builds_one_index_and_touches_nothing_else() {
    let (s, sd) = engine();
    let (r, rd) = engine();
    let _keep = (sd, rd);
    s.create_collection("shop", "orders").unwrap();
    pull(&r, &s);
    r.configure_vectors("shop", "orders", vector_config(8)).unwrap();
    r.create_index("shop", "orders", vec![IndexField::ascending("u")], true, Some("uniq_u".into()))
        .unwrap();
    r.insert(&coll(&r), doc! { "_id": 1, "v": 1, "u": 1 }).unwrap();
    pull(&s, &r);
    later();
    s.configure_vectors("shop", "orders", vector_config_dot(8)).unwrap();
    s.create_index(
        "shop",
        "orders",
        vec![IndexField::ascending("w")],
        false,
        Some("lacked".into()),
    )
    .unwrap();
    s.replace(&coll(&s), &DocId::Int64(1), doc! { "_id": 1, "v": 2, "u": 1 }, false).unwrap();
    s.insert(&coll(&s), doc! { "_id": 2, "v": 2 }).unwrap();
    let rows_before = rows(&r);
    let meta_before = coll(&r);
    let mut progress = SnapshotProgress::of_collection_definitions(orders(), 1_000_000);
    let page = s.snapshot_page(None, progress.scope()).unwrap();
    let done = r.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
    assert_eq!(done.definitions.built, 1, "{done:?}");
    let meta_after = coll(&r);
    assert_eq!(rows(&r), rows_before, "a Definitions pull wrote documents");
    assert_eq!(meta_after.vector, meta_before.vector, "the vector configuration moved");
    let mut held: Vec<_> = meta_after.indexes.iter().map(|i| i.name.clone()).collect();
    held.sort();
    assert_eq!(held, vec!["lacked".to_string(), "uniq_u".to_string()]);
    let uniq_before = meta_before.indexes.iter().find(|i| i.name == "uniq_u").unwrap();
    let uniq_after = meta_after.indexes.iter().find(|i| i.name == "uniq_u").unwrap();
    assert_eq!(
        serde_json::to_string(uniq_before).unwrap(),
        serde_json::to_string(uniq_after).unwrap()
    );
}

// =====================================================================================
// 3. The definition repair builds a plain index only, never one this member dropped
// =====================================================================================

fn definitions_repair(into: &Engine, from: &Engine, limit: u64) -> crate::DefinitionRestore {
    let mut progress = SnapshotProgress::of_collection_definitions(orders(), limit);
    let page = from.snapshot_page(None, progress.scope()).unwrap();
    into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap().definitions
}

fn index_names(e: &Engine) -> Vec<String> {
    let mut n: Vec<_> =
        e.list_indexes("shop", "orders").unwrap().into_iter().map(|i| i.name).collect();
    n.sort();
    n
}

/// This member dropped a plain index the sender kept (the drop never reached it), the
/// index tombstone was collected by a real pass, and the member restarted: the index must
/// stay dropped.
#[test]
fn revc_an_index_dropped_here_collected_and_restarted_is_not_rebuilt() {
    let (s, sd) = engine();
    let (r, rd) = engine();
    s.create_collection("shop", "orders").unwrap();
    pull(&r, &s);
    later();
    s.create_index("shop", "orders", vec![IndexField::ascending("v")], false, Some("by_v".into()))
        .unwrap();
    pull(&r, &s);
    later();
    assert!(r.drop_index("shop", "orders", "by_v").unwrap());
    // Within the tombstone: `tombstoned`.
    assert_eq!(definitions_repair(&r, &s, 1_000_000).tombstoned, 1);
    later();
    assert!(collect_at(&r, now()).tombstones_removed >= 1, "premise: the index tombstone went");
    let path = path_of(&rd);
    drop(r);
    let r = Engine::open(&path).unwrap();
    let done = definitions_repair(&r, &s, 1_000_000);
    assert!(index_names(&r).is_empty(), "REBUILT a dropped index: {done:?}");
    assert_eq!(done.ambiguous, 1, "{done:?}");
    drop(sd);
}

/// The collection, index and all, dropped and made again here; its drop tombstone then
/// collected so the new life has no floor. The sender kept the old life and its index.
#[test]
fn revc_an_index_of_a_life_this_member_ended_is_not_built_after_the_tombstone_is_collected() {
    for database in [false, true] {
        let (s, _sd) = engine();
        let (r, _rd) = engine();
        s.create_collection("shop", "orders").unwrap();
        later();
        s.create_index(
            "shop",
            "orders",
            vec![IndexField::ascending("v")],
            false,
            Some("old".into()),
        )
        .unwrap();
        pull(&r, &s);
        later();
        if database {
            assert!(r.drop_database("shop").unwrap());
        } else {
            assert!(r.drop_collection("shop", "orders").unwrap());
        }
        r.finish_purges_now().unwrap();
        later();
        // With the tombstone standing: history by the floor.
        r.create_collection("shop", "orders").unwrap();
        let first = definitions_repair(&r, &s, 1_000_000);
        assert!(index_names(&r).is_empty(), "database={database}: built under a floor: {first:?}");
        assert!(r.drop_collection("shop", "orders").unwrap());
        r.finish_purges_now().unwrap();
        later();
        assert!(collect_at(&r, now()).tombstones_removed >= 1);
        later();
        r.create_collection("shop", "orders").unwrap();
        assert!(coll(&r).incarnation_floor.is_none(), "premise: no floor");
        let done = definitions_repair(&r, &s, 1_000_000);
        assert!(
            index_names(&r).is_empty(),
            "database={database}: REBUILT an old life's index: {done:?}"
        );
    }
}

/// The size gate is the requester's own live count, at the boundary: at the gate the
/// index is built; one document over, it is not.
#[test]
fn revc_the_index_gate_is_the_requesters_count_at_the_boundary() {
    for (docs, limit, built) in [(10u64, 10u64, 1usize), (11, 10, 0)] {
        let (s, _sd) = engine();
        let (r, _rd) = engine();
        s.create_collection("shop", "orders").unwrap();
        pull(&r, &s);
        for i in 0..docs as i64 {
            r.insert(&coll(&r), doc! { "_id": i, "v": i }).unwrap();
        }
        later();
        s.create_index(
            "shop",
            "orders",
            vec![IndexField::ascending("v")],
            false,
            Some("by_v".into()),
        )
        .unwrap();
        let done = definitions_repair(&r, &s, limit);
        assert_eq!(done.built, built, "docs={docs} limit={limit}: {done:?}");
        assert_eq!(done.too_large, 1 - built, "docs={docs} limit={limit}: {done:?}");
    }
}

// =====================================================================================
// 5. Convergence: a random-history oracle over three members
// =====================================================================================

/// A small deterministic generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 =
            self.0.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

struct Member {
    dir: tempfile::TempDir,
    engine: Option<Engine>,
    /// Every delete this member ever held as a tombstone row: key to the stamps.
    ghosts: BTreeMap<Vec<u8>, BTreeSet<Stamp>>,
    /// Entries this member witnessed and never applied (a lost entry, as the lab's hook).
    lost: BTreeSet<Stamp>,
}

impl Member {
    fn e(&self) -> &Engine {
        self.engine.as_ref().unwrap()
    }
    fn note_ghosts(&mut self) {
        for (k, (stamp, live)) in rows(self.e()) {
            if !live {
                self.ghosts.entry(k).or_default().insert(stamp);
            }
        }
    }
}

/// What the oracle saw over a run.
#[derive(Debug, Default)]
struct Seen {
    converges: usize,
    created: usize,
    applied: usize,
    left: usize,
    violations: Vec<String>,
    tombstone_stamp_gaps: usize,
    unvouched_deletes: usize,
    gc_removed: usize,
    restarts: usize,
    foreign: usize,
    restores: usize,
}

/// One random history: inserts, updates and deletes on any member, lossy pulls between any
/// pair, tombstone collection at random cutoffs (unequal per member), restarts, a roll to a
/// foreign build that collects without raising the key and back, restores of a member's own
/// earlier backup, and Converge pulls between every ordered pair. After every Converge the
/// oracle checks, for the pair (requester R, sender S):
///
/// * **ghosts**: every row R did not hold before and holds live after has a stamp above
///   every delete of that id either R or S ever held (the union of both ghost histories);
/// * **no stamp fell** on R;
/// * **pairwise convergence of what S holds live**: every id S holds live at `s` is, on R,
///   either a row at `>= s` or no row at all (left); and every id S holds as a tombstone
///   over a live R row older than it is no longer live on R.
fn random_history(seed: u64, steps: usize) -> Seen {
    let mut rng = Rng(seed);
    let mut seen = Seen::default();
    let mut members: Vec<Member> = (0..3)
        .map(|_| {
            let dir = tempfile::tempdir().unwrap();
            let engine = Some(Engine::open(&path_of(&dir)).unwrap());
            Member { dir, engine, ghosts: BTreeMap::new(), lost: BTreeSet::new() }
        })
        .collect();
    members[0].e().create_collection("shop", "orders").unwrap();
    for i in 1..3 {
        let entries = members[0]
            .e()
            .entries_for_peer(Hlc::ZERO, 100_000, WalkScope::Background)
            .unwrap()
            .entries;
        members[i].e().apply_batch(&entries).unwrap();
    }
    let mut backups: Vec<Option<Vec<u8>>> = vec![None, None, None];
    for _ in 0..steps {
        let m = rng.below(3) as usize;
        match rng.below(20) {
            // A write.
            0..=6 => {
                let id = rng.below(12) as i64;
                let e = members[m].e();
                let c = coll(e);
                let present = e.get(&c, &DocId::Int64(id)).unwrap().is_some();
                if present && rng.below(3) == 0 {
                    e.delete(&c, &DocId::Int64(id)).unwrap();
                } else if present {
                    e.replace(
                        &c,
                        &DocId::Int64(id),
                        doc! { "_id": id, "v": rng.below(100) as i32 },
                        false,
                    )
                    .unwrap();
                } else {
                    e.replace(
                        &c,
                        &DocId::Int64(id),
                        doc! { "_id": id, "v": rng.below(100) as i32 },
                        true,
                    )
                    .unwrap();
                }
                later();
            }
            // A pull, losing an entry now and then (witnessed, not applied).
            7..=10 => {
                let from = (m + 1 + rng.below(2) as usize) % 3;
                let entries = members[from]
                    .e()
                    .entries_for_peer(Hlc::ZERO, 100_000, WalkScope::Background)
                    .unwrap()
                    .entries;
                let mut keep = Vec::new();
                for entry in entries {
                    if members[m].lost.contains(&entry.stamp) {
                        continue;
                    }
                    let document = matches!(
                        entry.kind,
                        OpKind::Insert | OpKind::Update | OpKind::Replace | OpKind::Delete
                    );
                    if document
                        && rng.below(6) == 0
                        && !rows(members[m].e())
                            .get(&crate::docs::doc_key(entry.doc_id.as_ref().unwrap()).unwrap())
                            .is_some_and(|(s, _)| *s >= entry.stamp)
                    {
                        members[m].lost.insert(entry.stamp);
                        continue;
                    }
                    keep.push(entry);
                }
                members[m].e().apply_batch(&keep).unwrap();
                let theirs = members[from].e().witnessed_vector().unwrap();
                members[m].e().absorb_witnessed(&theirs).unwrap();
            }
            // Tombstone collection at a random recent cutoff.
            11..=12 => {
                members[m].note_ghosts();
                let back = rng.below(40);
                let cutoff = now().saturating_sub(back);
                seen.gc_removed += collect_at(members[m].e(), cutoff).tombstones_removed;
            }
            // A restart.
            13 => {
                members[m].note_ghosts();
                drop(members[m].engine.take());
                members[m].engine = Some(Engine::open(&path_of(&members[m].dir)).unwrap());
                seen.restarts += 1;
            }
            // A roll to a foreign build that collects without raising the key, and back.
            14 => {
                members[m].note_ghosts();
                let before = members[m].e().tombstone_horizon().unwrap().unwrap();
                let cutoff = now().saturating_sub(rng.below(20));
                seen.gc_removed += collect_at(members[m].e(), cutoff).tombstones_removed;
                members[m].e().test_set_tombstone_horizon(before).unwrap();
                drop(members[m].engine.take());
                let path = path_of(&members[m].dir);
                let mut sidecar = crate::format::read_sidecar(&path).expect("a sidecar");
                sidecar.written_by =
                    std::env::var("REVC_FOREIGN").unwrap_or_else(|_| "0.48.0".into());
                crate::format::write_sidecar(&path, &sidecar).unwrap();
                members[m].engine = Some(Engine::open(&path).unwrap());
                seen.foreign += 1;
            }
            // Take a backup, or restore the last one taken.
            15 => {
                members[m].note_ghosts();
                match backups[m].take() {
                    None => {
                        let mut out = Vec::new();
                        members[m].e().backup_to(&mut out, WalkScope::Request).unwrap();
                        backups[m] = Some(out);
                    }
                    Some(backup) => {
                        drop(members[m].engine.take());
                        let path = path_of(&members[m].dir);
                        std::fs::remove_file(&path).unwrap();
                        let _ = std::fs::remove_file(crate::format::sidecar_path(&path));
                        crate::backup::restore(&path, &mut backup.as_slice()).unwrap();
                        members[m].engine = Some(Engine::open(&path).unwrap());
                        seen.restores += 1;
                    }
                }
            }
            // A Converge pull, judged.
            _ => {
                let s = (m + 1 + rng.below(2) as usize) % 3;
                for x in members.iter_mut() {
                    x.note_ghosts();
                }
                let before = rows(members[m].e());
                let sender = rows(members[s].e());
                let done = converge(members[m].e(), members[s].e());
                seen.converges += 1;
                seen.created += done.created;
                seen.applied += done.applied;
                seen.left += done.left;
                let after = rows(members[m].e());
                for (k, (stamp, live)) in &after {
                    match before.get(k) {
                        Some((was, _)) if stamp < was => seen.violations.push(format!(
                            "seed {seed}: a stamp fell on {m}: {k:?} {was:?} -> {stamp:?}"
                        )),
                        None if *live => {
                            for ghosts in [&members[m].ghosts, &members[s].ghosts] {
                                if let Some(t) =
                                    ghosts.get(k).and_then(|g| g.iter().find(|t| *t > stamp))
                                {
                                    seen.violations.push(format!(
                                        "seed {seed}: GHOST on {m} from {s}: {k:?} created at {stamp:?} under a delete at {t:?}"
                                    ));
                                }
                            }
                        }
                        _ => {}
                    }
                }
                for (k, (s_stamp, s_live)) in &sender {
                    let mine = after.get(k);
                    if *s_live {
                        if let Some((r_stamp, _)) = mine
                            && r_stamp < s_stamp
                            && !before.get(k).is_some_and(|(b, _)| b > s_stamp)
                        {
                            // A held row older than a live sender row: must have been applied,
                            // unless it is history (a buried life), which this run never makes.
                            seen.violations.push(format!(
                                "seed {seed}: not converged on {m} from {s}: {k:?} held {r_stamp:?} < sender {s_stamp:?}"
                            ));
                        }
                    } else if let Some((r_stamp, r_live)) = mine {
                        if *r_live && r_stamp < s_stamp {
                            let covered =
                                members[s].e().version_vector().unwrap().get(s_stamp.node);
                            if s_stamp.hlc > covered {
                                // The sender's own coverage does not name its tombstone (it
                                // holds it from a repair, under `Hold`): ignored by design
                                // (ADR-163), counted apart.
                                seen.unvouched_deletes += 1;
                            } else {
                                seen.violations.push(format!(
                                    "seed {seed}: a newer sender delete not applied on {m}: {k:?} r={r_stamp:?} s={s_stamp:?} covered={covered:?}"
                                ));
                            }
                        }
                        if !*r_live && r_stamp < s_stamp {
                            // A held older tombstone is replaced by a newer vouched one
                            // (review C m2); one the sender's coverage does not name is
                            // ignored by design, counted apart.
                            let covered =
                                members[s].e().version_vector().unwrap().get(s_stamp.node);
                            if s_stamp.hlc > covered {
                                seen.unvouched_deletes += 1;
                            } else {
                                seen.tombstone_stamp_gaps += 1;
                                seen.violations.push(format!(
                                    "seed {seed}: a newer vouched tombstone did not replace the older on {m}: {k:?} r={r_stamp:?} s={s_stamp:?}"
                                ));
                            }
                        }
                    }
                }
            }
        }
    }
    seen
}

#[test]
fn revc_random_histories_never_let_a_repair_create_a_ghost() {
    let seeds: u64 = std::env::var("REVC_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(40);
    let steps: usize = std::env::var("REVC_STEPS").ok().and_then(|v| v.parse().ok()).unwrap_or(250);
    let mut total = Seen::default();
    for seed in 1..=seeds {
        let seen = random_history(seed, steps);
        total.converges += seen.converges;
        total.created += seen.created;
        total.applied += seen.applied;
        total.left += seen.left;
        total.gc_removed += seen.gc_removed;
        total.restarts += seen.restarts;
        total.foreign += seen.foreign;
        total.restores += seen.restores;
        total.tombstone_stamp_gaps += seen.tombstone_stamp_gaps;
        total.unvouched_deletes += seen.unvouched_deletes;
        total.violations.extend(seen.violations);
    }
    println!(
        "REVC ORACLE seeds={seeds} steps={steps} converges={} created={} applied={} left={} gc_removed={} restarts={} foreign={} restores={} tombstone_stamp_gaps={} unvouched_deletes={} violations={}",
        total.converges,
        total.created,
        total.applied,
        total.left,
        total.gc_removed,
        total.restarts,
        total.foreign,
        total.restores,
        total.tombstone_stamp_gaps,
        total.unvouched_deletes,
        total.violations.len()
    );
    for v in total.violations.iter().take(20) {
        println!("REVC VIOLATION {v}");
    }
    assert!(
        total.violations.is_empty(),
        "{} violations; first: {:?}",
        total.violations.len(),
        total.violations.first()
    );
}
