//! The re-reads under the writer that decide whether an older tombstone may
//! replace or remove a newer one.
//!
//! Each of these checks is made twice: once outside the writer, cheaply, and
//! once under it, because what the first read can be overtaken by a write that
//! lands before the writer is taken. Every unit test that arrives at the
//! function with a stale value is turned away by the first check, so the second
//! survived its own removal with every test green. Here a newer tombstone is
//! recorded in the window between the two (a hook, or the scan and the removal
//! made two calls), and the older one must not win.

use std::cell::RefCell;

use kimmy_core::{CollectionId, Hlc, IndexField, IndexMeta, Stamp};

use crate::Engine;

thread_local! {
    static BEFORE_THE_WRITER: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
}

/// Called by `record_collection_drop` after its outside check and before it takes
/// the writer, where a concurrent write lands in production. Per thread.
pub(crate) fn between_the_check_and_the_writer() {
    if let Some(write) = BEFORE_THE_WRITER.with(|h| h.borrow_mut().take()) {
        write();
    }
}

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

/// Strictly later than `stamp`, from the same node.
fn after(stamp: Stamp) -> Stamp {
    Stamp::new(Hlc::new(stamp.hlc.wall_ms + 10, 0), stamp.node)
}

/// A cutoff past every stamp written now.
fn much_later() -> Hlc {
    Hlc::new(crate::physical_now_ms() + 365 * 24 * 60 * 60 * 1000, 0)
}

/// A dropped, purged collection, and the stamp of its tombstone.
fn dropped_collection(engine: &Engine, name: &str) -> (CollectionId, Stamp) {
    let coll = engine.create_collection("shop", name).unwrap();
    engine.drop_collection("shop", name).unwrap();
    engine.finish_purges_now().unwrap();
    let stamp = engine.collection_dropped_at(coll.id).unwrap().expect("a tombstone");
    (coll.id, stamp)
}

#[test]
fn a_collection_tombstone_recorded_again_after_the_scan_is_kept() {
    let (engine, _dir) = engine();
    let (kept, kept_stamp) = dropped_collection(&engine, "kept");
    let (plain, _) = dropped_collection(&engine, "plain");
    let cutoff = much_later();
    let expired = engine.scan_dropped_collections(cutoff).unwrap();
    assert_eq!(expired.len(), 2, "premise: the scan offers both: {expired:?}");

    // A drop of one of them lands between the scan and the removal.
    let newer = after(kept_stamp);
    engine.record_collection_drop(kept, newer).unwrap();

    assert_eq!(engine.remove_dropped_collections(&expired, cutoff).unwrap(), 1);
    assert_eq!(engine.collection_dropped_at(kept).unwrap(), Some(newer), "the newer drop stands");
    assert_eq!(engine.collection_dropped_at(plain).unwrap(), None, "the unchanged one is gone");
}

#[test]
fn an_index_tombstone_recorded_again_after_the_scan_is_kept() {
    let (engine, _dir) = engine();
    let coll = engine.create_collection("shop", "orders").unwrap();
    let mut ids = Vec::new();
    for name in ["a", "b"] {
        engine
            .create_index("shop", "orders", vec![IndexField::ascending(name)], false, None)
            .unwrap();
        engine.drop_index("shop", "orders", &format!("{name}_1")).unwrap();
        ids.push(IndexMeta::derive_id(&format!("{name}_1")));
    }
    let cutoff = much_later();
    let expired = engine.scan_dropped_indexes(cutoff).unwrap();
    assert_eq!(expired.len(), 2, "premise: the scan offers both: {expired:?}");

    let held = engine.index_dropped_at(coll.id, ids[0]).unwrap().expect("a tombstone");
    let newer = after(held);
    engine.record_index_drop(coll.id, ids[0], newer).unwrap();

    assert_eq!(engine.remove_dropped_indexes(&expired, cutoff).unwrap(), 1);
    assert_eq!(engine.index_dropped_at(coll.id, ids[0]).unwrap(), Some(newer));
    assert_eq!(engine.index_dropped_at(coll.id, ids[1]).unwrap(), None);
}

#[test]
fn an_older_drop_does_not_replace_one_recorded_between_its_check_and_its_writer() {
    let (engine, _dir) = engine();
    let engine = std::sync::Arc::new(engine);
    let coll = engine.create_collection("shop", "orders").unwrap();
    let base = Stamp::new(Hlc::new(crate::physical_now_ms(), 0), engine.node_id());
    let (older, newer) = (after(base), after(after(base)));

    // Nothing is held, so the outside check lets `older` through; the newer
    // drop lands before the writer is taken.
    let competing = std::sync::Arc::clone(&engine);
    BEFORE_THE_WRITER.with(|h| {
        *h.borrow_mut() = Some(Box::new(move || {
            competing.record_collection_drop(coll.id, newer).unwrap();
        }));
    });
    engine.record_collection_drop(coll.id, older).unwrap();

    assert!(BEFORE_THE_WRITER.with(|h| h.borrow().is_none()), "premise: the window was reached");
    assert_eq!(engine.collection_dropped_at(coll.id).unwrap(), Some(newer));
}

#[test]
fn a_batch_of_drops_does_not_replace_a_newer_one_it_holds() {
    let (engine, _dir) = engine();
    let (stale, stamp) = dropped_collection(&engine, "stale");
    let (fresh, _) = dropped_collection(&engine, "fresh");
    let newer = after(stamp);
    engine.record_collection_drop(stale, newer).unwrap();
    let before = engine.collection_dropped_at(fresh).unwrap().unwrap();

    // The callers filter first; the writer's check is what holds if they did
    // not, or if the table moved since.
    let older = Stamp::new(Hlc::new(stamp.hlc.wall_ms - 5, 0), stamp.node);
    engine.record_collection_drops(&[(stale, older), (fresh, after(before))]).unwrap();

    assert_eq!(engine.collection_dropped_at(stale).unwrap(), Some(newer), "the older one lost");
    assert_eq!(engine.collection_dropped_at(fresh).unwrap(), Some(after(before)), "the newer won");
}
