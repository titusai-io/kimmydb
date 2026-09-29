//! What `/violations` costs in heap, measured (ADR-200). The route read the
//! whole retained oplog to find its few records; from the table it reads the
//! records only, so its allocation follows the number of violations and not
//! the size of the oplog.
//!
//! Its own binary, with a counting `#[global_allocator]`, for the reason
//! `count_allocations.rs` gives.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use bson::doc;
use kimmy_core::{DocId, Hlc, NodeId, OpKind, OplogEntry, Stamp};
use kimmy_storage::{Engine, IndexField, WalkScope};

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

static MEASURING: Mutex<()> = Mutex::new(());

const DOC_BYTES: usize = 32 * 1024;

fn allocated_by<T>(f: impl FnOnce() -> T) -> (usize, T) {
    let before = ALLOCATED.load(Ordering::Relaxed);
    let out = f();
    (ALLOCATED.load(Ordering::Relaxed) - before, out)
}

/// An engine whose oplog holds `documents` large entries, with `violations`
/// unique violations spread evenly among them.
fn store(
    documents: usize,
    violations: usize,
) -> (Engine, kimmy_storage::CollectionMeta, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    engine.create_collection("app", "docs").unwrap();
    let field = IndexField { path: "email".into(), descending: false };
    engine.create_index("app", "docs", vec![field], true, None).unwrap();
    let coll = engine.get_collection("app", "docs").unwrap();
    let payload = "x".repeat(DOC_BYTES);
    let every = documents.checked_div(violations).unwrap_or(usize::MAX).max(1);
    let mut made = 0;
    for i in 0..documents {
        engine
            .insert(
                &coll,
                doc! { "_id": format!("d{i}"), "email": format!("d{i}@x"), "p": payload.as_str() },
            )
            .unwrap();
        if i % every == 0 && made < violations {
            // The colliding documents are small: standing re-reads them.
            engine
                .insert(&coll, doc! { "_id": format!("c{made}"), "email": format!("c{made}@x") })
                .unwrap();
            let entry = OplogEntry {
                stamp: Stamp::new(Hlc::new(9_000 + made as u64, 0), NodeId::generate()),
                kind: OpKind::Insert,
                collection: coll.id,
                doc_id: Some(DocId::String(format!("r{made}"))),
                body: Some(
                    bson::serialize_to_vec(
                        &doc! { "_id": format!("r{made}"), "email": format!("c{made}@x") },
                    )
                    .unwrap(),
                ),
            };
            assert!(engine.apply_remote(&coll, &entry).unwrap());
            made += 1;
        }
    }
    assert_eq!(made, violations);
    (engine, coll, dir)
}

fn cost(documents: usize, violations: usize) -> (usize, usize) {
    let (engine, coll, _dir) = store(documents, violations);
    assert!(engine.violations_table_ready());
    // Once, so what is measured is the route and not first-use setup.
    engine.live_unique_violations(&coll, None, WalkScope::Request).unwrap();
    let (bytes, live) =
        allocated_by(|| engine.live_unique_violations(&coll, None, WalkScope::Request).unwrap());
    assert_eq!(live.len(), violations);
    (bytes, live.len())
}

#[test]
fn with_no_violations_the_route_allocates_nothing_that_follows_the_oplog() {
    let _measuring = MEASURING.lock().unwrap();
    let (small, _) = cost(64, 0);
    let (large, _) = cost(256, 0);
    assert!(small < DOC_BYTES && large < DOC_BYTES, "small {small}, large {large}");
    assert!(large <= small + 4_096, "4x the oplog, {small} then {large} bytes");
}

#[test]
fn with_fifty_violations_spread_across_the_oplog_it_follows_the_violations() {
    let _measuring = MEASURING.lock().unwrap();
    let (small, _) = cost(200, 50);
    let (large, _) = cost(800, 50);
    // A body read per record, not per entry: far less than the 200 documents'
    // worth the oplog walk would touch, and the same at 4x the oplog.
    assert!(small < 50 * DOC_BYTES / 4, "{small} bytes for 50 violations");
    assert!(large <= small + small / 8 + 4_096, "4x the oplog, {small} then {large} bytes");
}
