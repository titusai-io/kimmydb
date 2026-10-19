//! A restore and a scoped page's rows (finding BR1 of the idempotency model, 2026-10-18).
//!
//! B writes `orders` at t+120 and `items` at t+190. D takes `items` by a scoped page,
//! whose row at t+190 D holds as page state: it grants D no coverage of B. D is backed up
//! and restored. Then D pulls B, from its witnessed vector, as the transport's pull does,
//! and it must take B's write at t+120, which no page carried.
//!
//! Public API only, so the same file runs on a tree with or without B5.

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

/// The writes, and D after the scoped page of `items`: (B, D, D's path, B's write at
/// t+120, the directory).
fn setup() -> (Engine, Engine, std::path::PathBuf, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let b = Engine::open(&dir.path().join("b.redb")).unwrap();
    let d_dir = dir.path().join("d");
    std::fs::create_dir(&d_dir).unwrap();
    let d_path = d_dir.join("kimmy.redb");
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

fn holds_the_write(d: &Engine) -> bool {
    let orders = d.get_collection("shop", "orders").unwrap();
    d.get(&orders, &DocId::Int64(1)).unwrap().is_some()
}

/// The control: without the restore, D's pull takes B's write at t+120.
#[test]
fn a_member_pulls_the_writes_a_scoped_page_jumped_over() {
    let (b, d, _path, _dir) = setup();
    pull_all(&d, &b);
    assert!(holds_the_write(&d), "D, which took items by page, lacks B's write to orders");
}

/// D backed up, its directory emptied, and the backup restored and opened in its place,
/// as `kimmyd restore` and the next start do.
fn backed_up_and_restored(d: Engine, d_path: &std::path::Path) -> Engine {
    let mut bytes = Vec::new();
    d.backup_to(&mut bytes, WalkScope::Background).unwrap();
    d.close().map_err(|e| format!("{e:?}")).unwrap();
    for entry in std::fs::read_dir(d_path.parent().unwrap()).unwrap() {
        std::fs::remove_file(entry.unwrap().path()).unwrap();
    }
    kimmy_storage::backup::restore(d_path, &mut bytes.as_slice()).unwrap();
    Engine::open(d_path).unwrap()
}

/// Finding BR1: after a backup and a restore, D's witnessed vector claims B's origin
/// through the page's row at t+190, so its pull starts past B's write at t+120 and never
/// takes it.
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

/// The key-less form, on the real clock: a document B writes once, below the page's row,
/// and never touches again. After the restore B goes on writing to both collections and D
/// pulls until its witnessed vector covers B's servable one (what clears a restored
/// member's catching-up marker by dominance). D then holds B's later writes and not the
/// one the page jumped over: no window from B starts below D's claim again, so only a
/// repair could bring it.
#[test]
fn a_restored_member_takes_a_document_written_once_below_a_scoped_page() {
    let dir = tempfile::tempdir().unwrap();
    let b = Engine::open(&dir.path().join("b.redb")).unwrap();
    let d_dir = dir.path().join("d");
    std::fs::create_dir(&d_dir).unwrap();
    let d_path = d_dir.join("kimmy.redb");
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
    let d_orders = d.get_collection("shop", "orders").unwrap();
    assert!(
        d.get(&d_orders, &DocId::Int64(2)).unwrap().is_some(),
        "the control: D's pull took B's write after the restore"
    );
    let count = |e: &Engine| {
        let coll = e.get_collection("shop", "orders").unwrap();
        e.count(&coll, WalkScope::Request).unwrap()
    };
    assert!(
        d.get(&d_orders, &DocId::Int64(1)).unwrap().is_some(),
        "the restored D lacks the document B wrote once below the page's row, though its \
         witnessed vector covers B's: orders holds {} documents on D and {} on B",
        count(&d),
        count(&b)
    );
}
