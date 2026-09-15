//! What the divergence check's count costs in heap, measured rather than argued.
//!
//! Both members of every contact count the probed collection: the requester
//! through `count_probe_reading`, the answering peer through `count_by_id`. A
//! count needs one flag per record, and the walk copied every document's body
//! to read it, and on the answering side parsed every document into BSON too
//! (ADR-133's addendum). So this binary counts every byte allocated while a
//! count walks a collection of large documents.
//!
//! Its own binary, with a counting `#[global_allocator]` wrapping
//! `std::alloc::System`, for the reason `kimmy-api/tests/memory.rs` gives: an
//! allocator is a property of the whole test binary.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use bson::doc;
use kimmy_storage::Engine;

/// Every byte handed out since the last reset. Cumulative, not live: a walk
/// that copies a body and frees it before the next one never holds more than
/// one, and still allocates all of them.
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

/// `System`, with every block it hands out counted. `realloc` and
/// `alloc_zeroed` are left to the trait's defaults, which call `alloc`, so one
/// place counts.
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

/// Held for the length of a measurement, because the counter is one counter.
static MEASURING: Mutex<()> = Mutex::new(());

/// Documents in the fixture, and the size of each one's payload.
const N: usize = 64;
const DOC_BYTES: usize = 64 * 1024;

/// Bytes allocated while `f` runs.
fn allocated_by<T>(f: impl FnOnce() -> T) -> (usize, T) {
    let before = ALLOCATED.load(Ordering::Relaxed);
    let out = f();
    (ALLOCATED.load(Ordering::Relaxed) - before, out)
}

#[test]
fn counting_a_collection_of_large_documents_allocates_less_than_one_of_them() {
    let _measuring = MEASURING.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    let coll = engine.create_collection("app", "wide").unwrap();
    let payload = "x".repeat(DOC_BYTES);
    for i in 0..N as i64 {
        engine.insert(&coll, doc! { "_id": i, "payload": payload.as_str() }).unwrap();
    }

    // Once each, so redb's read cache holds every page: what is measured is
    // the walk, not the cache filling.
    assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(N as u64));
    assert_eq!(engine.count_probe_reading(coll.id).unwrap().1, Some(N as u64));

    let (requester, reading) = allocated_by(|| engine.count_probe_reading(coll.id).unwrap());
    assert_eq!(reading.1, Some(N as u64));
    let (answerer, count) = allocated_by(|| engine.count_by_id(coll.id).unwrap());
    assert_eq!(count, Some(N as u64));

    // Less than one document, for N of them: a walk that copies or decodes
    // each record allocates at least N × DOC_BYTES.
    // Both sides are reported before either is judged.
    assert!(
        requester < DOC_BYTES && answerer < DOC_BYTES,
        "over {N} documents of {DOC_BYTES} bytes: count_probe_reading allocated {requester} \
         bytes, count_by_id {answerer}"
    );
}
