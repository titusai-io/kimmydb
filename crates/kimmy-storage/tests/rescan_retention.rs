//! What a vector rescan's reads leave in the heap (B6(a), round 0560).
//!
//! A rescan of an owned collection walks its ids, then for each document reads
//! its stamp and its vectors' staleness, and embeds nothing when all are
//! current. Round 0560 saw a member's resident climb 175 MiB around such a
//! rescan of a 2000-document, 11 MB collection. The reads of a rescan can leave
//! in the heap only what redb's page cache keeps of the pages they touch, so
//! what is retained follows **the collection's own pages** and not the size of
//! the store around it. Measured cold, on a store reopened with a cache far
//! larger than the collection, with the other collection growing
//! fortyfold between the two measurements: net live bytes (allocated minus
//! freed) after the walk. The control is a walk of the other collection, which
//! must retain more, or the measurement could not tell them apart.
//!
//! **A measurement, ignored by default** (it builds about 640 MB of store, twice
//! over): `cargo test --release -p kimmy-storage --test rescan_retention --
//! --ignored`. A debug build reads the whole store at open, so the control only
//! means something in release. It showed 16.9 MiB retained by the rescan of the
//! 2,000-document collection, the same with 500 and with 20,000 documents in the
//! other collection, against 66 MB for the control.
//!
//! Its own binary, with a counting `#[global_allocator]`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};

use bson::doc;
use kimmy_storage::{Engine, WalkScope};

static LIVE: AtomicIsize = AtomicIsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const TARGET_DOCS: usize = 2000;
const DOC_BYTES: usize = 5 * 1024;
/// The default `storage.cache_bytes`.
const CACHE: usize = 256 * 1024 * 1024;

fn build(path: &std::path::Path, other_docs: usize) {
    let engine = Engine::open(path).unwrap();
    engine.create_collection("app", "target").unwrap();
    engine.create_collection("app", "other").unwrap();
    let target = engine.get_collection("app", "target").unwrap();
    let other = engine.get_collection("app", "other").unwrap();
    let payload = "x".repeat(DOC_BYTES);
    for i in 0..TARGET_DOCS {
        engine.insert(&target, doc! { "_id": format!("t{i}"), "p": payload.as_str() }).unwrap();
    }
    let big = "y".repeat(16 * 1024);
    for i in 0..other_docs {
        engine.insert(&other, doc! { "_id": format!("o{i}"), "p": big.as_str() }).unwrap();
    }
    engine.close().unwrap();
}

/// Net live bytes a cold reopen's rescan-shaped reads of `which` leave behind.
fn retained(other_docs: usize, which: &str) -> isize {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    build(&path, other_docs);
    let engine = Engine::open_with_cache(&path, Some(CACHE)).unwrap();
    let coll = engine.get_collection("app", which).unwrap();
    let before = LIVE.load(Ordering::Relaxed);
    let mut ids = Vec::new();
    engine
        .for_each_doc(&coll, WalkScope::Background, |id, _| {
            ids.push(id);
            Ok(true)
        })
        .unwrap();
    for id in &ids {
        engine.document_stamp(&coll, id).unwrap().unwrap();
    }
    let after = LIVE.load(Ordering::Relaxed);
    drop(ids);
    after - before
}

#[test]
#[ignore = "a measurement: run with --release -- --ignored"]
fn a_rescans_reads_leave_the_collections_pages_and_not_the_stores_size() {
    let small = retained(500, "target");
    let large = retained(20_000, "target");
    let control = retained(20_000, "other");
    let collection_bytes = (TARGET_DOCS * DOC_BYTES) as isize;
    assert!(large <= collection_bytes * 2, "bounded by the collection: {large}");
    assert!(large <= small + collection_bytes / 4, "flat as the store grows: {small} {large}");
    // A debug build reads the whole store at open (the cache is warm before the
    // walk), so only a release build can tell the control from the rescan.
    if !cfg!(debug_assertions) {
        assert!(control > large * 2, "the control must retain more: {control} vs {large}");
    }
}
