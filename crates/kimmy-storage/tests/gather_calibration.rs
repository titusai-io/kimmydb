//! What the keys an indexed write gathers really hold, against the estimate that charges
//! them (`kimmy_query::budget::held_set_key`, ADR-270's second addendum).
//!
//! `update`, `delete` and `find_and_modify` over an index range gather the document key of
//! every entry of the range into a `Vec<Vec<u8>>` inside the write transaction, sort it, and
//! only then read the documents. This runs the engine's own `modify_where` over a range of
//! N documents with a spec that matches none of them, with a counting allocator under it:
//!
//! - **live** is what the gather holds once it is built: the allocator's live bytes when the
//!   spec first sees a document (the sort and the de-duplication are over; the keys are all
//!   there; nothing else is read yet), less what was live before the call;
//! - **peak** is the most the allocator held at any moment of the call, less the same, which
//!   takes in the growth of the array the keys are pushed into, the copy that joins the
//!   ranges, and the scratch of the sort.
//!
//! The estimate has to be **at least the live bytes of a key**: a smaller one would let a
//! write hold more than it is charged. The peak is reported beside it, not held to it: it is
//! transient, it comes and goes inside one gather, and the estimate is the one the pool uses
//! for a key in a set, so one figure serves the documents and the operator.
//!
//! Measured once at ranges up to 262,145 keys (a debug build takes minutes to build them), a
//! key a write gathers held, in bytes, live 56.1 to 57.0 for an `_id` of 7 or 13 bytes (the
//! estimate is 82), 72.1 for 27 bytes (100) and 88.0 for 43 (118); and at the peak 87.5 to
//! 104.1, 103.5 to 120.1 and 119.5 to 136.0: the figure at the high end is a range one past
//! a power of two, where the array the keys are pushed into has just doubled and the copy
//! that joins the ranges (or the scratch of the sort) is alive beside it.
//!
//! Its own binary, with a counting `#[global_allocator]`, for the reason
//! `kimmy-api/tests/memory.rs` gives: an allocator is a property of the whole test binary. One
//! test function, so nothing else allocates while a gather is measured. Run it with
//! `--nocapture` to see the table. Ignored by default, as a debug build takes close
//! to a minute; CI runs it in release with `--ignored`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cmp::Ordering;
use std::sync::atomic::{AtomicIsize, Ordering as Atomic};

use bson::{Bson, Document, doc};
use kimmy_storage::{Candidates, Engine, IndexField, ModifySpec};

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);

/// A block as the allocator really sizes it, plus its 8-byte header (the rule
/// `kimmy-query/tests/held_calibration.rs` counts by).
fn block(ptr: *mut u8) -> isize {
    // SAFETY: `ptr` came from the system allocator and is live.
    (unsafe { libc::malloc_usable_size(ptr.cast()) } + 8) as isize
}

fn grow(by: isize) {
    let now = LIVE.fetch_add(by, Atomic::Relaxed) + by;
    PEAK.fetch_max(now, Atomic::Relaxed);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            grow(block(ptr));
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(block(ptr), Atomic::Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            grow(block(ptr));
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let before = block(ptr);
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            grow(block(new) - before);
        }
        new
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn live() -> isize {
    LIVE.load(Atomic::Relaxed)
}

/// A spec that matches nothing, and notes the allocator's live bytes the first time it is
/// asked, which is after the gather is built and before any document is read.
struct Probe {
    first: std::cell::Cell<Option<isize>>,
}

impl ModifySpec for Probe {
    fn matches(&self, _: &Document) -> kimmy_core::Result<bool> {
        if self.first.get().is_none() {
            self.first.set(Some(live()));
        }
        Ok(false)
    }

    fn compare(&self, _: &Document, _: &Document) -> Ordering {
        Ordering::Equal
    }

    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
        Ok(Some(doc.clone()))
    }

    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        None
    }
}

/// How an `_id` is made, and so how long its encoded key is.
#[derive(Clone, Copy)]
enum Ids {
    Int64,
    ObjectId,
    /// A string of this many characters.
    Text(usize),
}

impl Ids {
    fn name(self) -> String {
        match self {
            Ids::Int64 => "Int64".into(),
            Ids::ObjectId => "ObjectId".into(),
            Ids::Text(n) => format!("string of {n}"),
        }
    }

    fn id(self, i: i64) -> Bson {
        match self {
            Ids::Int64 => Bson::Int64(i),
            Ids::ObjectId => {
                let mut bytes = [0u8; 12];
                bytes[4..].copy_from_slice(&(i as u64).to_be_bytes());
                Bson::ObjectId(bson::oid::ObjectId::from_bytes(bytes))
            }
            Ids::Text(n) => Bson::String(format!("{i:0>n$}")),
        }
    }
}

/// What a gather of `range` entries held, as `(live, peak)` bytes beyond what was held before.
fn gather(
    engine: &Engine,
    coll: &kimmy_storage::CollectionMeta,
    index_id: u32,
    range: i64,
) -> (isize, isize) {
    let key = |i: i64| kimmy_core::keyenc::encode_compound_ordered(&[(Bson::Int64(i), false)]);
    let candidates = Candidates::Index {
        index_id,
        ranges: vec![(key(0).unwrap(), key(range - 1).unwrap())],
        both_bounds: false,
    };
    let probe = Probe { first: Default::default() };
    // Once unmeasured, so that the pages the range reads are in redb's cache and what is
    // measured is the gather and not the cache filling.
    engine.modify_where(coll, &candidates, &probe, None).unwrap();
    probe.first.set(None);

    let before = live();
    PEAK.store(before, Atomic::Relaxed);
    let out = engine.modify_where(coll, &candidates, &probe, None).unwrap();
    assert_eq!(out.examined, range as u64, "the gather found every entry of the range");
    (probe.first.get().expect("the spec was asked") - before, PEAK.load(Atomic::Relaxed) - before)
}

#[test]
#[ignore = "time (54 s in a debug build); run in release by CI's decimal job"]
fn the_estimate_of_a_gathered_key_is_at_least_what_it_holds() {
    // 32,769 is one past a power of two: the array the keys are pushed into has just
    // doubled, which is its worst case against the keys it holds.
    const RANGES: [i64; 3] = [1_000, 10_001, 32_769];
    let mut table = String::new();
    let mut worst_live = 0.0f64;
    for ids in [Ids::Int64, Ids::ObjectId, Ids::Text(24), Ids::Text(40)] {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        engine.create_collection("app", "keys").unwrap();
        let index = engine
            .create_index("app", "keys", vec![IndexField::ascending("t")], false, None)
            .unwrap();
        let coll = engine.get_collection("app", "keys").unwrap();
        let n = RANGES[RANGES.len() - 1];
        for start in (0..n).step_by(16_384) {
            let batch: Vec<Document> =
                (start..(start + 16_384).min(n)).map(|i| doc! {"_id": ids.id(i), "t": i}).collect();
            engine.insert_many(&coll, batch).unwrap();
        }
        let key_len = kimmy_core::keyenc::encode(&ids.id(7)).unwrap().len();
        let estimate = kimmy_query::budget::held_set_key(key_len);
        for range in RANGES {
            let (held, peak) = gather(&engine, &coll, index.id, range);
            let (held, peak) = (held as f64 / range as f64, peak as f64 / range as f64);
            worst_live = worst_live.max(held / estimate as f64);
            table.push_str(&format!(
                "{:<16} key {key_len:>2} B  {range:>7} keys   live {held:>6.1} B/key   \
                 peak {peak:>6.1} B/key   estimate {estimate} B/key\n",
                ids.name(),
            ));
            assert!(
                held <= estimate as f64,
                "{}: {range} keys hold {held:.1} bytes a key, over the estimate of {estimate}\n{table}",
                ids.name(),
            );
        }
    }
    println!("\n{table}\nlive / estimate, at the worst: {worst_live:.3}");
}
