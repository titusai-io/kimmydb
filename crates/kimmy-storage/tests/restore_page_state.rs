//! A restore and the rows a member holds as state (ADR-244).
//!
//! B writes `orders` at t+120 and `items` at t+190. D takes `items` by a scoped page,
//! whose row at t+190 D holds as page state: it grants D no coverage of B. D is backed up
//! and restored. Then D pulls B, from its witnessed vector, as the transport's pull does,
//! and it must take B's write at t+120, which no page carried. A backup does not carry the
//! held marks, so the restore re-derives them from the backup's own vector; before it did,
//! the first open raised both vectors over the page's row and D never took the write.
//!
//! Public API only.

use bson::doc;
use kimmy_core::{CollectionId, DocId, Hlc, Stamp};
use kimmy_storage::{Engine, PeerWindow, SnapshotProgress, SyncOutcome, WalkScope};

fn pull_all(to: &Engine, from: &Engine) {
    for _ in 0..8 {
        let theirs = from.version_vector().unwrap();
        let mine = to.witnessed_vector().unwrap();
        let Some(start) = mine.behind(&theirs) else { return };
        let window =
            from.entries_for_peer_holding(start, 1024, Some(&mine), WalkScope::Background).unwrap();
        let mut outcome = SyncOutcome::default();
        to.apply_peer_window_into(
            &theirs,
            Some(&mine),
            &window.entries,
            PeerWindow {
                scanned_to: window.scanned_to,
                exhausted: window.exhausted,
                passed_through: None,
                asked_partial: false,
            },
            &mut outcome,
        )
        .unwrap();
        if window.exhausted {
            return;
        }
    }
}

fn scoped(to: &Engine, from: &Engine, id: CollectionId) {
    let mut progress = SnapshotProgress::of_collection(id);
    while !progress.is_complete() {
        let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        to.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
    }
}

/// An empty directory for one member's store, and the store's path in it.
fn member_dir(root: &tempfile::TempDir, name: &str) -> std::path::PathBuf {
    let dir = root.path().join(name);
    std::fs::create_dir(&dir).unwrap();
    dir.join("kimmy.redb")
}

/// The writes, and D after the scoped page of `items`: (B, D, D's path, the directory).
fn setup() -> (Engine, Engine, std::path::PathBuf, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let b = Engine::open(&dir.path().join("b.redb")).unwrap();
    let d_path = member_dir(&dir, "d");
    let d = Engine::open(&d_path).unwrap();
    let t = (kimmy_storage::physical_now_ms() / 1000 + 3_600) * 1000;
    let orders = b.create_collection("shop", "orders").unwrap();
    let items = b.create_collection("shop", "items").unwrap();
    b.insert(&orders, doc! { "_id": 0i64, "n": 0i64 }).unwrap();
    pull_all(&d, &b);
    b.advance_clock_past(&Stamp::new(Hlc::new(t + 120, 0), b.node_id()));
    b.insert(&orders, doc! { "_id": 1i64, "n": 1i64 }).unwrap();
    b.advance_clock_past(&Stamp::new(Hlc::new(t + 190, 0), b.node_id()));
    b.insert(&items, doc! { "_id": 101i64, "u": 1i64 }).unwrap();
    scoped(&d, &b, items.id);
    (b, d, d_path, dir)
}

fn holds(e: &Engine, collection: &str, id: i64) -> bool {
    let coll = e.get_collection("shop", collection).unwrap();
    e.get(&coll, &DocId::Int64(id)).unwrap().is_some()
}

fn holds_the_write(d: &Engine) -> bool {
    holds(d, "orders", 1)
}

fn count(e: &Engine, collection: &str) -> u64 {
    let coll = e.get_collection("shop", collection).unwrap();
    e.count(&coll, WalkScope::Request).unwrap()
}

/// `d` backed up, its directory emptied, and the backup restored in its place, as
/// `kimmyd restore` does: restored only, not opened.
fn restored_closed(d: Engine, d_path: &std::path::Path) {
    let mut bytes = Vec::new();
    d.backup_to(&mut bytes, WalkScope::Background).unwrap();
    d.close().map_err(|e| format!("{e:?}")).unwrap();
    for entry in std::fs::read_dir(d_path.parent().unwrap()).unwrap() {
        std::fs::remove_file(entry.unwrap().path()).unwrap();
    }
    kimmy_storage::backup::restore(d_path, &mut bytes.as_slice()).unwrap();
}

/// [`restored_closed`], then opened, as the next start does.
fn backed_up_and_restored(d: Engine, d_path: &std::path::Path) -> Engine {
    restored_closed(d, d_path);
    Engine::open(d_path).unwrap()
}

/// The control: without the restore, D's pull takes B's write at t+120.
#[test]
fn a_member_pulls_the_writes_a_scoped_page_jumped_over() {
    let (b, d, _path, _dir) = setup();
    pull_all(&d, &b);
    assert!(holds_the_write(&d), "D, which took items by page, lacks B's write to orders");
}

/// After a backup and a restore, D's witnessed vector must still be where it was, below
/// B's write at t+120, so its pull takes the write.
#[test]
fn a_restored_member_pulls_the_writes_a_scoped_page_jumped_over() {
    let (b, d, d_path, _dir) = setup();
    let before = d.witnessed_vector().unwrap().get(b.node_id());
    let d = backed_up_and_restored(d, &d_path);
    let after = d.witnessed_vector().unwrap().get(b.node_id());
    pull_all(&d, &b);
    assert!(
        holds_the_write(&d),
        "the restored D lacks B's write to orders at t+120: its witnessed vector of B was \
         {before:?} before the restore and {after:?} after it"
    );
}

/// What the restored member claims is the backup's servable vector, for both vectors, and
/// no more: not the page's row it holds as state.
#[test]
fn a_restored_member_claims_the_backups_vector_and_no_more() {
    let (b, d, d_path, _dir) = setup();
    let servable = d.version_vector().unwrap();
    assert!(d.held_marks().unwrap() > 0, "the page's row is held");
    let d = backed_up_and_restored(d, &d_path);
    assert_eq!(d.version_vector().unwrap(), servable, "the servable vector rose");
    assert_eq!(d.witnessed_vector().unwrap(), servable, "the witnessed vector rose");
    assert!(servable.get(b.node_id()) < b.version_vector().unwrap().get(b.node_id()));
}

/// The key-less form, on the real clock: a document B writes once, below the page's row,
/// and never touches again. After the restore B goes on writing to both collections and D
/// pulls until its witnessed vector covers B's servable one (what clears a restored
/// member's catching-up marker by dominance). D must then hold the document too, and
/// hold nothing as state any more: the page's row arrived in a contiguous window and
/// released its mark.
#[test]
fn a_restored_member_takes_a_document_written_once_below_a_scoped_page() {
    let dir = tempfile::tempdir().unwrap();
    let b = Engine::open(&dir.path().join("b.redb")).unwrap();
    let d_path = member_dir(&dir, "d");
    let d = Engine::open(&d_path).unwrap();
    let orders = b.create_collection("shop", "orders").unwrap();
    let items = b.create_collection("shop", "items").unwrap();
    b.insert(&orders, doc! { "_id": 0i64 }).unwrap();
    pull_all(&d, &b);
    // Written once and never again.
    b.insert(&orders, doc! { "_id": 1i64, "once": true }).unwrap();
    b.insert(&items, doc! { "_id": 101i64 }).unwrap();
    scoped(&d, &b, items.id);
    let d = backed_up_and_restored(d, &d_path);
    b.insert(&orders, doc! { "_id": 2i64 }).unwrap();
    b.insert(&items, doc! { "_id": 102i64 }).unwrap();
    pull_all(&d, &b);

    let theirs = b.version_vector().unwrap();
    let mine = d.witnessed_vector().unwrap();
    assert!(mine.covers(&theirs), "D did not converge by position: {mine:?} against {theirs:?}");
    assert!(holds(&d, "orders", 2), "the control: D's pull took B's write after the restore");
    assert!(
        holds(&d, "orders", 1),
        "the restored D lacks the document B wrote once below the page's row, though its \
         witnessed vector covers B's: orders holds {} documents on D and {} on B",
        count(&d, "orders"),
        count(&b, "orders")
    );
    assert_eq!(d.held_marks().unwrap(), 0, "a mark the restore re-derived was not released");
    assert_eq!(d.version_vector().unwrap(), theirs, "D's servable vector is B's");
}

/// A third member C that pulls B's origin from the restored D before it reaches B must not
/// be told D covers what D does not: it takes B's write at t+120, from D or from B.
#[test]
fn a_member_pulling_from_a_restored_member_takes_what_the_page_jumped_over() {
    let dir = tempfile::tempdir().unwrap();
    let b = Engine::open(&dir.path().join("b.redb")).unwrap();
    let c = Engine::open(&dir.path().join("c.redb")).unwrap();
    let d_path = member_dir(&dir, "d");
    let d = Engine::open(&d_path).unwrap();
    let orders = b.create_collection("shop", "orders").unwrap();
    let items = b.create_collection("shop", "items").unwrap();
    b.insert(&orders, doc! { "_id": 0i64 }).unwrap();
    pull_all(&d, &b);
    pull_all(&c, &b);
    b.insert(&orders, doc! { "_id": 1i64 }).unwrap();
    b.insert(&items, doc! { "_id": 101i64 }).unwrap();
    scoped(&d, &b, items.id);
    let d = backed_up_and_restored(d, &d_path);
    pull_all(&c, &d);
    pull_all(&c, &b);
    assert!(c.witnessed_vector().unwrap().covers(&b.version_vector().unwrap()));
    assert!(holds(&c, "orders", 1), "C lacks B's write at t+120, which the restored D skipped");
    assert!(holds(&c, "items", 101), "and the page's row");
}

/// A backup taken part-way through a whole-database snapshot: the pull is not resumed
/// after the restore (its progress is node-local and not in the backup), and the first
/// page's rows are held. The restored member takes the rest by entries, every document
/// the later pages would have carried included, and ends holding nothing as state.
#[test]
fn a_member_restored_part_way_through_a_whole_database_snapshot_takes_the_later_pages() {
    let dir = tempfile::tempdir().unwrap();
    let b = Engine::open(&dir.path().join("b.redb")).unwrap();
    let d_path = member_dir(&dir, "d");
    let d = Engine::open(&d_path).unwrap();
    let orders = b.create_collection("shop", "orders").unwrap();
    let total = kimmy_storage::snapshot::SNAPSHOT_PAGE as i64 + 88;
    // In descending id order, so the first page (the lowest ids) holds the newest stamps.
    for id in (0..total).rev() {
        b.insert(&orders, doc! { "_id": id }).unwrap();
    }
    let mut progress = SnapshotProgress::whole_database();
    let page = b.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
    d.apply_snapshot_page(b.node_id(), &mut progress, &page).unwrap();
    assert!(!progress.is_complete(), "the snapshot is part-way");
    let d = backed_up_and_restored(d, &d_path);
    assert!(d.snapshots_to_resume().unwrap().is_empty(), "the pull is not resumed");
    pull_all(&d, &b);
    assert!(d.witnessed_vector().unwrap().covers(&b.version_vector().unwrap()));
    let missing: Vec<i64> = (0..total).filter(|id| !holds(&d, "orders", *id)).collect();
    assert!(missing.is_empty(), "{} documents of the later pages never arrived", missing.len());
    assert_eq!(d.held_marks().unwrap(), 0);
}

/// `kimmyd restore --until`: restore, open, rewind, as the command does. The rewind's
/// target is above the page's row, and the rewind discards a write of D's own after it.
/// Neither the open nor the rewind's lowering of the vectors may claim the row.
#[test]
fn a_restore_until_does_not_claim_a_row_held_as_state() {
    let dir = tempfile::tempdir().unwrap();
    let b = Engine::open(&dir.path().join("b.redb")).unwrap();
    let d_path = member_dir(&dir, "d");
    let d = Engine::open(&d_path).unwrap();
    let orders = b.create_collection("shop", "orders").unwrap();
    let items = b.create_collection("shop", "items").unwrap();
    b.insert(&orders, doc! { "_id": 0i64 }).unwrap();
    pull_all(&d, &b);
    let servable = d.version_vector().unwrap().get(b.node_id());
    b.insert(&orders, doc! { "_id": 1i64 }).unwrap();
    b.insert(&items, doc! { "_id": 101i64 }).unwrap();
    scoped(&d, &b, items.id);
    std::thread::sleep(std::time::Duration::from_millis(5));
    let until = Hlc::new(kimmy_storage::physical_now_ms(), 0);
    std::thread::sleep(std::time::Duration::from_millis(5));
    // A write of D's own after the target, which the rewind discards.
    let d_orders = d.get_collection("shop", "orders").unwrap();
    d.insert(&d_orders, doc! { "_id": 50i64 }).unwrap();
    restored_closed(d, &d_path);
    {
        let d = Engine::open(&d_path).unwrap();
        let outcome = d.rewind_to(until).unwrap();
        assert_eq!(outcome.removed, 1, "the rewind discarded D's own write");
        d.close().map_err(|e| format!("{e:?}")).unwrap();
    }
    let d = Engine::open(&d_path).unwrap();
    assert_eq!(
        d.version_vector().unwrap().get(b.node_id()),
        servable,
        "the rewound member claims B's origin over the page's row"
    );
    assert!(!holds(&d, "orders", 50));
    pull_all(&d, &b);
    assert!(holds_the_write(&d), "the rewound member lacks B's write below the page's row");
}

/// The member's own origin: a write of D's own that D lost and holds again only as state
/// (D restored from a backup older than it, then a page of B's carrying it back) must not
/// raise D's own position at the next restore's open. That position is where the start's
/// replay of D's own origin begins, so a rise would skip D's own writes below the row.
#[test]
fn a_restored_members_own_position_does_not_rise_over_its_own_rows_held_as_state() {
    let dir = tempfile::tempdir().unwrap();
    let b = Engine::open(&dir.path().join("b.redb")).unwrap();
    let d_path = member_dir(&dir, "d");
    let d = Engine::open(&d_path).unwrap();
    let orders = d.create_collection("shop", "orders").unwrap();
    d.create_collection("shop", "items").unwrap();
    pull_all(&b, &d);
    let mut older = Vec::new();
    d.backup_to(&mut older, WalkScope::Background).unwrap();
    let position = d.version_vector().unwrap().get(d.node_id());
    // Two writes of D's own after that backup, which B takes and the old backup lacks.
    let items = d.get_collection("shop", "items").unwrap();
    d.insert(&items, doc! { "_id": 8i64 }).unwrap();
    d.insert(&orders, doc! { "_id": 7i64 }).unwrap();
    pull_all(&b, &d);
    d.close().map_err(|e| format!("{e:?}")).unwrap();
    for entry in std::fs::read_dir(d_path.parent().unwrap()).unwrap() {
        std::fs::remove_file(entry.unwrap().path()).unwrap();
    }
    kimmy_storage::backup::restore(&d_path, &mut older.as_slice()).unwrap();
    let d = Engine::open(&d_path).unwrap();
    assert_eq!(d.own_position_at_open(), position);
    // B's page of `orders` carries D's own write back, as state.
    scoped(&d, &b, CollectionId::derive("shop", "orders"));
    assert!(holds(&d, "orders", 7) && !holds(&d, "items", 8));
    let d = backed_up_and_restored(d, &d_path);
    assert_eq!(
        d.own_position_at_open(),
        position,
        "the restored member's own position rose over its own write held as state"
    );
    assert_eq!(d.version_vector().unwrap().get(d.node_id()), position);
}
