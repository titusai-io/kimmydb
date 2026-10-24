//! The collected-through horizon (design v3.4, section 13.1): the cutoff of the last
//! retention pass that collected something, kept in `META`, monotone, raised in each
//! removing transaction of every tombstone collector, reset on a start the evidence
//! cannot vouch for, and offered on the pages a member serves.

use bson::doc;
use kimmy_core::Hlc;

use crate::gc::RetentionPolicy;
use crate::{Engine, WalkScope};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn policy() -> RetentionPolicy {
    RetentionPolicy::new(60, 60)
}

fn later_than_the_retention() -> u64 {
    crate::physical_now_ms() + 10 * 60 * 1_000
}

fn cutoff_of(now_ms: u64) -> Hlc {
    Hlc::new(now_ms - 60 * 1_000, 0)
}

#[test]
fn a_fresh_store_starts_with_a_horizon_at_its_start() {
    let before = crate::physical_now_ms();
    let (engine, _dir) = engine();
    let horizon = engine.tombstone_horizon().unwrap().expect("a fresh store has a horizon");
    assert!(horizon.wall_ms >= before, "{horizon:?} is before the start {before}");
}

#[test]
fn a_pass_that_removes_a_document_tombstone_raises_the_horizon_to_its_cutoff() {
    let (engine, _dir) = engine();
    let meta = engine.create_collection("shop", "orders").unwrap();
    engine.insert(&meta, doc! { "_id": 1 }).unwrap();
    engine.delete(&meta, &kimmy_core::DocId::Int64(1)).unwrap();
    let now = later_than_the_retention();
    engine.collect_garbage_at(now, policy()).unwrap();
    assert_eq!(engine.tombstone_horizon().unwrap(), Some(cutoff_of(now)));
}

#[test]
fn a_pass_that_removes_a_collection_tombstone_raises_the_horizon() {
    let (engine, _dir) = engine();
    engine.create_collection("shop", "orders").unwrap();
    engine.drop_collection("shop", "orders").unwrap();
    engine.finish_purges_now().unwrap();
    let start = engine.tombstone_horizon().unwrap().unwrap();
    let now = later_than_the_retention();
    let outcome = engine.collect_garbage_at(now, policy()).unwrap();
    assert!(outcome.tombstones_removed >= 1, "premise: a tombstone was collected");
    assert_eq!(engine.tombstone_horizon().unwrap(), Some(cutoff_of(now)));
    assert!(cutoff_of(now) > start);
}

#[test]
fn a_pass_that_removes_a_database_tombstone_raises_the_horizon() {
    let (engine, _dir) = engine();
    engine.create_collection("shop", "orders").unwrap();
    engine.drop_database("shop").unwrap();
    engine.finish_purges_now().unwrap();
    let now = later_than_the_retention();
    engine.collect_garbage_at(now, policy()).unwrap();
    assert_eq!(engine.tombstone_horizon().unwrap(), Some(cutoff_of(now)));
}

#[test]
fn a_pass_that_removes_an_index_tombstone_raises_the_horizon() {
    let (engine, _dir) = engine();
    engine.create_collection("shop", "orders").unwrap();
    engine
        .create_index(
            "shop",
            "orders",
            vec![crate::IndexField::ascending("k")],
            false,
            Some("ik".into()),
        )
        .unwrap();
    engine.drop_index("shop", "orders", "ik").unwrap();
    let now = later_than_the_retention();
    let outcome = engine.collect_garbage_at(now, policy()).unwrap();
    assert!(outcome.tombstones_removed >= 1, "premise: the index tombstone was collected");
    assert_eq!(engine.tombstone_horizon().unwrap(), Some(cutoff_of(now)));
}

#[test]
fn a_pass_that_removes_nothing_does_not_move_the_horizon() {
    let (engine, _dir) = engine();
    let meta = engine.create_collection("shop", "orders").unwrap();
    engine.insert(&meta, doc! { "_id": 1 }).unwrap();
    let before = engine.tombstone_horizon().unwrap();
    engine.collect_garbage_at(later_than_the_retention(), policy()).unwrap();
    assert_eq!(engine.tombstone_horizon().unwrap(), before, "a no-op pass moved the horizon");
}

#[test]
fn the_horizon_never_goes_down() {
    let (engine, _dir) = engine();
    let meta = engine.create_collection("shop", "orders").unwrap();
    engine.insert(&meta, doc! { "_id": 1 }).unwrap();
    engine.delete(&meta, &kimmy_core::DocId::Int64(1)).unwrap();
    let late = later_than_the_retention() + 3_600_000;
    engine.collect_garbage_at(late, policy()).unwrap();
    let high = engine.tombstone_horizon().unwrap().unwrap();
    // A second tombstone, collected by a pass with an earlier `now`.
    engine.insert(&meta, doc! { "_id": 2 }).unwrap();
    engine.delete(&meta, &kimmy_core::DocId::Int64(2)).unwrap();
    engine.collect_garbage_at(later_than_the_retention(), policy()).unwrap();
    assert_eq!(engine.tombstone_horizon().unwrap(), Some(high), "the horizon was lowered");
}

/// Every burial of a life raises the horizon when the life holds a record above the drop: the
/// purge that follows removes tombstones too, and one stamped after the drop is covered by no
/// floor (the young-stamp model test's YS1). `bury_standing_in_txn` is the one function every
/// burial site passes through (an ordinary drop, a re-dated life's end of sweep, the covered
/// fast path, a database drop's burial of a re-dated life).
#[test]
fn every_burial_raises_the_horizon_to_the_highest_stamp_the_life_held() {
    let source = include_str!("engine.rs");
    let start = source.find("pub(crate) fn bury_standing_in_txn(").expect("the burial");
    let end = source[start..].find("pub(crate) fn finish_burial").map(|n| start + n).unwrap();
    let function = &source[start..end];
    assert!(function.contains("set_at_least_in_txn"), "the burial raises no horizon");
    assert!(function.contains("kept_row"), "and reads the life's highest stamp from the kept row");
}

/// A local drop mints its stamp above everything the life held and so raises nothing: the
/// burial's raise is for a life holding a record above the drop only.
#[test]
fn a_local_drop_raises_no_horizon() {
    let (engine, _dir) = engine();
    let meta = engine.create_collection("shop", "orders").unwrap();
    engine.insert(&meta, doc! { "_id": 1 }).unwrap();
    engine.insert(&meta, doc! { "_id": 2 }).unwrap();
    engine.delete(&meta, &kimmy_core::DocId::Int64(2)).unwrap();
    let before = engine.tombstone_horizon().unwrap();
    assert!(engine.drop_collection("shop", "orders").unwrap());
    assert_eq!(engine.tombstone_horizon().unwrap(), before, "a local drop raised the horizon");
}

/// Every write transaction in `gc.rs` that removes from `DOCS`, `COLLECTIONS_DROPPED` or
/// `INDEXES_DROPPED` raises the horizon in that same transaction: a collector added later
/// that does not is a hole in section 13.3's own-side argument.
#[test]
fn every_tombstone_collector_raises_the_horizon_in_its_removing_transaction() {
    let source = include_str!("gc.rs");
    let product = source.split("#[cfg(test)]").next().unwrap();
    // Split at every `fn` at item depth, whatever its visibility, so a collector added later as
    // `pub(crate) fn` is a function of its own and not part of the one above it.
    let mut starts: Vec<usize> = product
        .match_indices("fn ")
        .map(|(at, _)| at)
        .filter(|at| {
            let line_start = product[..*at].rfind('\n').map_or(0, |n| n + 1);
            let prefix = &product[line_start..*at];
            // An item inside `impl`: four spaces of indent, then at most a visibility.
            prefix.starts_with("    ")
                && !prefix.starts_with("     ")
                && matches!(prefix.trim(), "" | "pub" | "pub(crate)" | "pub(super)" | "const")
        })
        .collect();
    starts.push(product.len());
    let mut checked = 0;
    for window in starts.windows(2) {
        let function = &product[window[0]..window[1]];
        let removes = function.contains("begin_write(WriterHolder::Retention)")
            && (function.contains("tables::DOCS")
                || function.contains("COLLECTIONS_DROPPED")
                || function.contains("INDEXES_DROPPED"));
        if !removes {
            continue;
        }
        checked += 1;
        let name = function.lines().next().unwrap_or("");
        // The raise sits in the removing transaction: after the transaction opens and before
        // the commit that follows it. A raise in a second transaction after the commit leaves
        // the crash window the design forbids (13.1), and a raise before the transaction
        // opens is in nothing.
        let begin = function.find("begin_write(WriterHolder::Retention)").unwrap();
        let commit = function[begin..]
            .find("txn.commit()")
            .map(|at| begin + at)
            .unwrap_or_else(|| panic!("{name}: no commit follows the transaction"));
        let raise = function
            .find("raise_tombstone_horizon_in_txn")
            .unwrap_or_else(|| panic!("a tombstone collector does not raise the horizon: {name}"));
        assert!(
            begin < raise && raise < commit,
            "{name}: the horizon is not raised inside the removing transaction"
        );
    }
    assert_eq!(checked, 3, "the three collectors: documents, collections and databases, indexes");
}

#[test]
fn a_served_page_carries_the_horizon() {
    let (engine, _dir) = engine();
    let meta = engine.create_collection("shop", "orders").unwrap();
    engine.insert(&meta, doc! { "_id": 1 }).unwrap();
    let page = engine
        .snapshot_page(None, Some(kimmy_core::CollectionId::derive("shop", "orders")))
        .unwrap();
    assert_eq!(page.tombstone_horizon, engine.tombstone_horizon().unwrap());
    assert!(page.tombstone_horizon.is_some());
    let _ = WalkScope::Request;
}

// -- The reset at open (design 13.1) ----------------------------------------------------

fn path_of(dir: &tempfile::TempDir) -> std::path::PathBuf {
    dir.path().join("kimmy.redb")
}

/// Raise the stored horizon far above any start time, as a long-running member's would
/// be, and close the engine.
fn with_a_high_horizon(path: &std::path::Path) -> Hlc {
    let engine = Engine::open(path).unwrap();
    let meta = engine.create_collection("shop", "orders").unwrap();
    engine.insert(&meta, doc! { "_id": 1 }).unwrap();
    engine.delete(&meta, &kimmy_core::DocId::Int64(1)).unwrap();
    let now = crate::physical_now_ms() + 60 * 60 * 1_000;
    engine.collect_garbage_at(now, policy()).unwrap();
    let high = engine.tombstone_horizon().unwrap().unwrap();
    assert!(high.wall_ms > crate::physical_now_ms() + 30 * 60 * 1_000, "premise: far ahead");
    high
}

#[test]
fn a_restart_by_the_same_build_keeps_the_horizon() {
    let dir = tempfile::tempdir().unwrap();
    let high = with_a_high_horizon(&path_of(&dir));
    let engine = Engine::open(&path_of(&dir)).unwrap();
    assert_eq!(engine.tombstone_horizon().unwrap(), Some(high));
}

#[test]
fn a_start_after_another_build_wrote_the_store_resets_to_the_start_but_never_lowers() {
    let dir = tempfile::tempdir().unwrap();
    let high = with_a_high_horizon(&path_of(&dir));
    // The prior sidecar says another build wrote the store.
    let mut sidecar = crate::format::read_sidecar(&path_of(&dir)).expect("a sidecar");
    sidecar.written_by = "0.48.0".into();
    crate::format::write_sidecar(&path_of(&dir), &sidecar).unwrap();
    let start = crate::physical_now_ms();
    let engine = Engine::open(&path_of(&dir)).unwrap();
    let horizon = engine.tombstone_horizon().unwrap().unwrap();
    assert!(horizon >= high, "the reset lowered the horizon: {horizon:?} < {high:?}");
    assert!(horizon.wall_ms >= start);
}

#[test]
fn a_foreign_build_that_collected_leaves_the_start_time_when_the_stored_value_is_lower() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::open(&path_of(&dir)).unwrap();
        engine.create_collection("shop", "orders").unwrap();
    }
    let low = {
        // Write a stale low value, as a build that did not maintain the key would leave.
        let engine = Engine::open(&path_of(&dir)).unwrap();
        engine.test_set_tombstone_horizon(Hlc::new(1_000, 0)).unwrap();
        Hlc::new(1_000, 0)
    };
    let mut sidecar = crate::format::read_sidecar(&path_of(&dir)).expect("a sidecar");
    sidecar.written_by = "0.48.0".into();
    crate::format::write_sidecar(&path_of(&dir), &sidecar).unwrap();
    let start = crate::physical_now_ms();
    let engine = Engine::open(&path_of(&dir)).unwrap();
    let horizon = engine.tombstone_horizon().unwrap().unwrap();
    assert!(horizon > low && horizon.wall_ms >= start, "{horizon:?}");
}

#[test]
fn an_absent_key_on_a_store_that_is_not_fresh_is_the_start_time() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::open(&path_of(&dir)).unwrap();
        engine.create_collection("shop", "orders").unwrap();
        engine.test_remove_tombstone_horizon().unwrap();
    }
    let start = crate::physical_now_ms();
    let engine = Engine::open(&path_of(&dir)).unwrap();
    let horizon = engine.tombstone_horizon().unwrap().expect("an absent key is never read as zero");
    assert!(horizon.wall_ms >= start);
}

#[test]
fn a_restore_drops_the_horizon_of_the_backup() {
    let dir = tempfile::tempdir().unwrap();
    let high = with_a_high_horizon(&path_of(&dir));
    let mut backup = Vec::new();
    {
        let engine = Engine::open(&path_of(&dir)).unwrap();
        assert_eq!(engine.tombstone_horizon().unwrap(), Some(high));
        engine.backup_to(&mut backup, WalkScope::Request).unwrap();
    }
    let target = tempfile::tempdir().unwrap();
    let start = crate::physical_now_ms();
    crate::backup::restore(&path_of(&target), &mut backup.as_slice()).unwrap();
    let engine = Engine::open(&path_of(&target)).unwrap();
    let horizon = engine.tombstone_horizon().unwrap().unwrap();
    assert!(horizon < high, "the backup's horizon came back: {horizon:?}");
    assert!(horizon.wall_ms >= start, "and the restored store did not reset to its own time");
}

#[test]
fn the_sidecar_written_before_the_open_keeps_the_prior_build_until_the_reset_lands() {
    // A kill anywhere between the pre-open sidecar write and the reset commit must leave
    // the evidence in place for the next start.
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::open(&path_of(&dir)).unwrap();
        engine.create_collection("shop", "orders").unwrap();
    }
    let mut sidecar = crate::format::read_sidecar(&path_of(&dir)).expect("a sidecar");
    sidecar.written_by = "0.48.0".into();
    sidecar.redb_file_format = crate::format::REDB_FILE_FORMAT - 1;
    crate::format::write_sidecar(&path_of(&dir), &sidecar).unwrap();
    let cleared = crate::format::check_before_open(&path_of(&dir)).unwrap();
    let before_open = cleared.sidecar_before_open().expect("the redb fields are brought up");
    assert_eq!(before_open.written_by, "0.48.0", "the pre-open write named this build");
    assert!(cleared.horizon_is_untrusted());
    // A kill here: the sidecar is on disk as the pre-open write left it. The next start still
    // reads the evidence, and resets: the horizon is at least that start.
    drop(cleared);
    let second_start = crate::physical_now_ms();
    let engine = Engine::open(&path_of(&dir)).unwrap();
    let horizon = engine.tombstone_horizon().unwrap().expect("a horizon");
    assert!(horizon.wall_ms >= second_start, "{horizon:?} is below the second start");
}

// -- A completed whole-database snapshot adopts its sender's forgetting ------------------------

fn whole_database_pull(into: &Engine, from: &Engine, shape: impl Fn(&mut crate::SnapshotPage)) {
    let mut progress = crate::SnapshotProgress::whole_database();
    while !progress.is_complete() {
        let mut page = from.snapshot_page(progress.after().cloned(), None).unwrap();
        shape(&mut page);
        into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
    }
}

#[test]
fn a_completed_whole_database_snapshot_raises_the_receivers_horizon_to_the_senders() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.create_collection("shop", "orders").unwrap();
    let sender = Hlc::new(crate::physical_now_ms() + 3_600_000, 0);
    let before = b.tombstone_horizon().unwrap().unwrap();
    assert!(before < sender, "premise");
    whole_database_pull(&b, &a, |page| page.tombstone_horizon = Some(sender));
    assert_eq!(b.tombstone_horizon().unwrap(), Some(sender));
}

#[test]
fn adoption_never_lowers_the_receivers_horizon() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.create_collection("shop", "orders").unwrap();
    let own = b.tombstone_horizon().unwrap().unwrap();
    // A sender whose horizon is below the receiver's (just reset, or a shorter retention).
    whole_database_pull(&b, &a, |page| page.tombstone_horizon = Some(Hlc::new(1, 0)));
    assert_eq!(b.tombstone_horizon().unwrap(), Some(own));
}

#[test]
fn a_page_that_is_not_the_last_adopts_nothing() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let meta = a.create_collection("shop", "orders").unwrap();
    for i in 0..(crate::snapshot::SNAPSHOT_PAGE as i64 + 5) {
        a.insert(&meta, doc! { "_id": i }).unwrap();
    }
    let sender = Hlc::new(crate::physical_now_ms() + 3_600_000, 0);
    let own = b.tombstone_horizon().unwrap().unwrap();
    let mut progress = crate::SnapshotProgress::whole_database();
    let mut page = a.snapshot_page(None, None).unwrap();
    assert!(page.next.is_some(), "premise: more than one page");
    page.tombstone_horizon = Some(sender);
    b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    assert_eq!(b.tombstone_horizon().unwrap(), Some(own), "adopted before the grant");
    // The last page adopts.
    while !progress.is_complete() {
        let mut page = a.snapshot_page(progress.after().cloned(), None).unwrap();
        page.tombstone_horizon = Some(sender);
        b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    }
    assert_eq!(b.tombstone_horizon().unwrap(), Some(sender));
}

#[test]
fn a_page_without_a_horizon_and_a_scoped_snapshot_adopt_nothing() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.create_collection("shop", "orders").unwrap();
    let own = b.tombstone_horizon().unwrap().unwrap();
    whole_database_pull(&b, &a, |page| page.tombstone_horizon = None);
    assert_eq!(b.tombstone_horizon().unwrap(), Some(own));
    let sender = Hlc::new(crate::physical_now_ms() + 3_600_000, 0);
    let id = kimmy_core::CollectionId::derive("shop", "orders");
    let mut progress = crate::SnapshotProgress::of_collection(id);
    let mut page = a.snapshot_page(None, Some(id)).unwrap();
    page.tombstone_horizon = Some(sender);
    b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    assert_eq!(b.tombstone_horizon().unwrap(), Some(own), "a scoped snapshot grants nothing");
}

// -- The 0.49 line is not this build (review revA M1) ------------------------------------------

fn held_row(e: &Engine, id: i64) -> bool {
    use redb::ReadableDatabase;
    let key = crate::docs::doc_key(&kimmy_core::DocId::Int64(id)).unwrap();
    let txn = e.db().begin_read().unwrap();
    let table = txn.open_table(crate::tables::DOCS).unwrap();
    table
        .get((kimmy_core::CollectionId::derive("shop", "orders").0, key.as_slice()))
        .unwrap()
        .is_some()
}

/// A store that the 0.49.0 release ran between two starts of this build, and whose retention
/// pass collected a tombstone without raising the key, must reset: the real previous release's
/// `written_by` (not an invented string) is untrusted whatever this build calls itself. Without
/// it the young-stamp rule writes back a document this member deleted.
#[test]
fn a_0_49_release_that_collected_between_two_starts_resets_the_horizon() {
    let (a, _da) = engine();
    let dir_b = tempfile::tempdir().unwrap();
    let path_b = dir_b.path().join("kimmy.redb");
    let b = Engine::open(&path_b).unwrap();
    a.create_collection("shop", "orders").unwrap();
    let pull = |into: &Engine, from: &Engine| {
        let entries =
            from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
        into.apply_batch(&entries).unwrap();
    };
    pull(&b, &a);
    std::thread::sleep(std::time::Duration::from_millis(5));
    let ca = a.get_collection("shop", "orders").unwrap();
    a.insert(&ca, doc! { "_id": 1, "v": 1 }).unwrap();
    pull(&b, &a);
    std::thread::sleep(std::time::Duration::from_millis(5));
    // B deletes the document; A never hears of it.
    let cb = b.get_collection("shop", "orders").unwrap();
    b.delete(&cb, &kimmy_core::DocId::Int64(1)).unwrap();
    // The 0.49.0 release runs the store: its retention pass collects the tombstone and, knowing
    // nothing of the key, does not raise it.
    {
        let txn = b.begin_write(crate::WriterHolder::Retention).unwrap();
        {
            let mut docs = txn.open_table(crate::tables::DOCS).unwrap();
            let key = crate::docs::doc_key(&kimmy_core::DocId::Int64(1)).unwrap();
            let id = kimmy_core::CollectionId::derive("shop", "orders").0;
            assert!(crate::live_count::remove_record(&txn, &mut docs, id, &key).unwrap());
        }
        txn.commit().unwrap();
    }
    drop(b);
    // ... and leaves its sidecar, as every 0.49.0 open does.
    let mut sidecar = crate::format::read_sidecar(&path_b).expect("a sidecar");
    sidecar.written_by = "0.49.0".into();
    crate::format::write_sidecar(&path_b, &sidecar).unwrap();
    // This build starts again.
    let before_start = crate::physical_now_ms();
    let b = Engine::open(&path_b).unwrap();
    let horizon = b.tombstone_horizon().unwrap().expect("a horizon");
    assert!(
        horizon.wall_ms >= before_start,
        "the horizon was not reset to this start: {horizon:?}"
    );
    assert!(!held_row(&b, 1), "premise: the tombstone is gone");
    let mut progress = crate::SnapshotProgress::of_collection_converging(
        kimmy_core::CollectionId::derive("shop", "orders"),
        kimmy_core::VersionVector::new(),
    );
    while !progress.is_complete() {
        let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    }
    assert!(!held_row(&b, 1), "a document this member deleted came back through the young rule");
}

// -- Scenarios the first tests left to pieces (review revA m9) ---------------------------------

/// A retention raised across a restart admits nothing below the horizon: the horizon a pass
/// raised under the old retention survives the restart (the same build keeps it), and a later
/// pass under a longer retention does not lower it. A row older than it is `left`, whatever the
/// retention says now.
#[test]
fn a_retention_raised_across_a_restart_admits_nothing_below_the_horizon() {
    let (a, _da) = engine();
    let dir_b = tempfile::tempdir().unwrap();
    let path_b = path_of(&dir_b);
    let b = Engine::open(&path_b).unwrap();
    a.create_collection("shop", "orders").unwrap();
    let pull = |into: &Engine, from: &Engine| {
        let entries =
            from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
        into.apply_batch(&entries).unwrap();
    };
    pull(&b, &a);
    let ca = a.get_collection("shop", "orders").unwrap();
    a.insert(&ca, doc! { "_id": 1, "v": 1 }).unwrap();
    pull(&b, &a);
    std::thread::sleep(std::time::Duration::from_millis(5));
    let cb = b.get_collection("shop", "orders").unwrap();
    b.delete(&cb, &kimmy_core::DocId::Int64(1)).unwrap();
    // A pass under a 60 s retention collects the tombstone and raises the horizon above it.
    let now = later_than_the_retention();
    b.collect_garbage_at(now, policy()).unwrap();
    let raised = b.tombstone_horizon().unwrap().unwrap();
    drop(b);
    // The restart, then a pass under a retention an hour longer: it collects nothing and the
    // horizon does not move down.
    let b = Engine::open(&path_b).unwrap();
    assert_eq!(b.tombstone_horizon().unwrap(), Some(raised), "the restart lowered the horizon");
    b.collect_garbage_at(now, RetentionPolicy::new(3_600, 3_600)).unwrap();
    assert_eq!(b.tombstone_horizon().unwrap(), Some(raised));
    // The row the sender still holds is older than the horizon: left, not created.
    let mut progress = crate::SnapshotProgress::of_collection_converging(
        kimmy_core::CollectionId::derive("shop", "orders"),
        kimmy_core::VersionVector::new(),
    );
    let mut created = 0;
    while !progress.is_complete() {
        let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        created += b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap().created;
    }
    assert_eq!(created, 0);
    assert!(!held_row(&b, 1), "a document this member deleted came back");
}
