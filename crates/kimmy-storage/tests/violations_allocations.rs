//! What `/violations` costs in heap, measured cold (ADR-200). The route read the
//! whole retained oplog to find its few records; from the table it reads the
//! records only, so what its first call allocates after a reopen follows the
//! number of violations and not the size of the oplog.
//!
//! **Cold, because warm proves nothing**: with the pages in redb's cache a walk
//! of the oplog allocates nothing either. Each measurement is the first call
//! after the store is reopened with the smallest page cache, so every page the
//! route touches is read, and a read allocates. **The oplog walk is the
//! control**, forced by a test switch on the same store: it must grow with the
//! oplog, or the measurement could not tell the two routes apart.
//!
//! **Stamped from a pinned clock**, because what a cold call reads depends on
//! where the store's pages are, and with stamps from the wall clock that
//! depended on the time. redb shortens each branch key to the bytes that tell
//! its two children apart, so a branch page's fill, and with it where the page
//! splits and which pages the store has and where, moved with when the fixture
//! was built; the reopen then leaves in the small cache whatever its own reads
//! left there. The table's first call read a different handful of pages from
//! run to run, and under host load up to 100 KiB more than usual, which failed
//! the flat bound though nothing in the route had changed. A witnessed stamp
//! far past the wall clock makes each local stamp the successor of the one
//! before, and the remote entries come from fixed origins, so every run builds
//! the same store and measures the same bytes.
//!
//! Its own binary, with a counting `#[global_allocator]`, for the reason
//! `count_allocations.rs` gives.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use bson::doc;
use kimmy_core::{DocId, Hlc, NodeId, OpKind, OplogEntry, Stamp};
use kimmy_storage::{Engine, ExamineBudget, IndexField, WalkScope};

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
/// The smallest page cache the store allows.
const CACHE: usize = 8 * 1024 * 1024;

fn allocated_by<T>(f: impl FnOnce() -> T) -> (usize, T) {
    let before = ALLOCATED.load(Ordering::Relaxed);
    let out = f();
    (ALLOCATED.load(Ordering::Relaxed) - before, out)
}

/// Where the fixture's clock is pinned before its first write: far past any
/// wall clock it runs under, so each local stamp is the successor of the one
/// before, whenever and however slowly the store is built.
const PINNED: Hlc = Hlc::new(1 << 44, 0);

/// A store at `path` whose oplog holds `documents` large entries, with
/// `violations` unique violations spread evenly among them, closed.
fn build(path: &std::path::Path, documents: usize, violations: usize) {
    let engine = Engine::open(path).unwrap();
    engine.witness_processed(&Stamp::new(PINNED, NodeId::from_bytes([0xFF; 16]))).unwrap();
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
                stamp: Stamp::new(
                    Hlc::new(9_000 + made as u64, 0),
                    NodeId::from_bytes([made as u8 + 1; 16]),
                ),
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
    engine.close().unwrap();
}

/// What the first `/violations` call allocates on a store reopened cold, by the
/// table (`from_oplog` false) or by the oplog walk (true).
fn cold_first_call(documents: usize, violations: usize, from_oplog: bool) -> usize {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    build(&path, documents, violations);

    // Completing the table takes an open, the backfill, and a close: through is
    // then at the tail, and the next open finds the table ready.
    {
        let engine = Engine::open_with_cache(&path, Some(CACHE)).unwrap();
        let budget = ExamineBudget { time: std::time::Duration::from_secs(600), rows: 1_000 };
        while !engine.violations_backfill_step(budget).unwrap().done {}
        engine.close().unwrap();
    }
    let engine = Engine::open_with_cache(&path, Some(CACHE)).unwrap();
    assert!(engine.violations_table_ready(), "a completed table is ready at the next open");
    let coll = engine.get_collection("app", "docs").unwrap();
    kimmy_storage::violations_table::set_test_violations_from_oplog(from_oplog);
    let (bytes, live) =
        allocated_by(|| engine.live_unique_violations(&coll, None, WalkScope::Request).unwrap());
    kimmy_storage::violations_table::set_test_violations_from_oplog(false);
    assert_eq!(live.len(), violations);
    bytes
}

/// The table's first call is flat as the oplog grows fourfold, with no violations
/// and with fifty spread across it, and the oplog walk on the same stores is not:
/// the control that shows the measurement can tell them apart.
#[test]
fn the_first_call_after_a_reopen_follows_the_violations_and_not_the_oplog() {
    let _measuring = MEASURING.lock().unwrap();
    for (small, large, violations) in [(64, 256, 0), (200, 800, 50)] {
        let table =
            (cold_first_call(small, violations, false), cold_first_call(large, violations, false));
        let oplog =
            (cold_first_call(small, violations, true), cold_first_call(large, violations, true));
        assert!(
            oplog.1 >= oplog.0 * 2,
            "the control must grow with the oplog ({violations} violations): {oplog:?}"
        );
        assert!(
            table.1 <= table.0 + table.0 / 2 + 64 * 1024,
            "the table's first call is flat as the oplog grows fourfold \
             ({violations} violations): {table:?}"
        );
        assert!(
            table.1 * 4 <= oplog.1,
            "the table reads far less than the oplog walk ({violations} violations): \
             table {table:?}, oplog {oplog:?}"
        );
    }
}
