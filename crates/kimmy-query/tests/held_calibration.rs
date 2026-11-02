//! The estimate of a parsed document's memory (`budget::held_bytes`) against what the
//! documents really hold, measured with a counting allocator (ADR-269, the review's B2).
//!
//! For each shape, N documents are parsed from their BSON, as the storage layer reads them, and
//! built field by field, as a stage builds them; the heap each holds is the live bytes the
//! allocator counted (a block as the allocator really sizes it, plus its 8-byte header). The
//! estimate must be **at least** what is held, for every shape (a smaller one would let a
//! request take more than it is charged), and **at most 1.5 times** it (a larger one would
//! take the limits away from documents of long strings). A document of long strings must be
//! charged within 1.0 to 1.35 times its BSON size. A change of the `bson` crate's layout that
//! breaks any of this fails here, loudly.
//!
//! One test function, so nothing else allocates while a shape is measured.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};

use bson::{Bson, Document, doc};
use kimmy_query::budget::{doc_bytes, held_bytes, held_element, held_frame_growth};

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

fn block(ptr: *mut u8) -> isize {
    // SAFETY: `ptr` came from the system allocator and is live.
    (unsafe { libc::malloc_usable_size(ptr.cast()) } + 8) as isize
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE.fetch_add(block(ptr), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(block(ptr), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            LIVE.fetch_add(block(ptr), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let before = block(ptr);
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            LIVE.fetch_add(block(new) - before, Ordering::Relaxed);
        }
        new
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn live() -> isize {
    LIVE.load(Ordering::Relaxed)
}

fn ints(n: usize) -> Document {
    let mut d = Document::new();
    d.insert("_id", 1_i32);
    for i in 0..n {
        d.insert(format!("f{i}"), i as i32);
    }
    d
}

fn strings(n: usize, len: usize) -> Document {
    let mut d = Document::new();
    d.insert("_id", 1_i32);
    for i in 0..n {
        d.insert(format!("s{i}"), "x".repeat(len));
    }
    d
}

fn nested(depth: usize) -> Document {
    if depth == 0 {
        return ints(10);
    }
    let mut d = Document::new();
    for i in 0..10 {
        d.insert(format!("k{i}"), nested(depth - 1));
    }
    d
}

fn shapes() -> Vec<(&'static str, Document)> {
    let mut reviewer100 = Document::new();
    reviewer100.insert("_id", 1_i32);
    for i in 0..50 {
        reviewer100.insert(format!("n{i}"), i);
        reviewer100.insert(format!("s{i}"), "y".repeat(60));
    }
    let mut mixed = Document::new();
    mixed.insert("_id", bson::oid::ObjectId::from_bytes([7; 12]));
    mixed.insert("when", bson::DateTime::from_millis(1_700_000_000_000));
    mixed.insert("ratio", 0.25_f64);
    mixed.insert("exact", bson::Decimal128::from_bytes([3; 16]));
    mixed.insert(
        "blob",
        bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![9; 200] },
    );
    mixed.insert("flag", true);
    mixed.insert("none", Bson::Null);
    mixed.insert("name", "a mid-sized string value of some forty chars");
    mixed.insert("tags", vec!["a", "bb", "ccc", "dddd"]);
    mixed.insert("inner", doc! {"x": 1, "y": "z", "w": {"deep": [1, 2, 3]}});
    vec![
        ("tiny {_id}", doc! {"_id": 1}),
        ("10 int fields", ints(10)),
        ("string 1 KiB", doc! {"_id": 1, "s": "x".repeat(1024)}),
        ("string 4 KiB", doc! {"_id": 1, "pad": "x".repeat(4096)}),
        ("string 100 KiB", doc! {"_id": 1, "s": "x".repeat(100 * 1024)}),
        ("100 int fields", ints(100)),
        ("400 int fields", ints(400)),
        ("1,000 int fields", ints(1000)),
        ("1,100 int fields (past a doubling)", ints(1100)),
        ("100 short strings (10 B)", strings(100, 10)),
        ("reviewer 100 fields (50 ints, 50 x 60 B)", reviewer100),
        ("nested depth 3 x 10 fields", nested(3)),
        ("array of 1,000 ints", doc! {"_id": 1, "a": (0..1000).collect::<Vec<i32>>()}),
        (
            "array of 50 small documents",
            doc! {"_id": 1, "a": (0..50).map(|i| doc! {"x": i, "y": "s"}).collect::<Vec<_>>()},
        ),
        ("mixed types", mixed),
        (
            "20 fields with 40-char names",
            Document::from_iter(
                (0..20).map(|i| (format!("{}{i:02}", "n".repeat(38)), Bson::Int32(i))),
            ),
        ),
    ]
}

/// What `n` copies of the shape hold when read from BSON, per document, and when built field
/// by field.
fn measure(shape: &Document, n: usize) -> (f64, f64) {
    let bytes = bson::serialize_to_vec(shape).unwrap();
    let before = live();
    let mut parsed: Vec<Document> = Vec::with_capacity(n);
    for _ in 0..n {
        parsed.push(bson::deserialize_from_slice(&bytes).unwrap());
    }
    let parsed_held = (live() - before) as f64 / n as f64;
    drop(parsed);

    let before = live();
    let mut built: Vec<Document> = Vec::with_capacity(n);
    for _ in 0..n {
        let mut d = Document::new();
        for (k, v) in shape {
            d.insert(k.clone(), v.clone());
        }
        built.push(d);
    }
    let built_held = (live() - before) as f64 / n as f64;
    drop(built);
    (parsed_held, built_held)
}

#[test]
fn the_estimate_is_at_least_what_documents_hold_and_at_most_one_and_a_half_times_it() {
    let mut table = String::new();
    table.push_str(&format!(
        "{:<42} {:>9} {:>10} {:>10} {:>9} {:>8} {:>8}\n",
        "shape", "BSON", "parsed", "built", "estimate", "est/par", "est/BSON"
    ));
    let mut failures = Vec::new();
    for (name, shape) in shapes() {
        let bson_size = doc_bytes(&shape);
        let n = (2_000_000 / bson_size).clamp(8, 4_000);
        let (parsed, built) = measure(&shape, n);
        let estimate = held_bytes(&shape) as f64;
        table.push_str(&format!(
            "{:<42} {:>9} {:>10.0} {:>10.0} {:>9.0} {:>8.2} {:>8.2}\n",
            name,
            bson_size,
            parsed,
            built,
            estimate,
            estimate / parsed,
            estimate / bson_size as f64,
        ));
        // The estimate covers what a parsed document and a built one hold.
        for (how, held) in [("parsed", parsed), ("built", built)] {
            if estimate < held {
                failures.push(format!(
                    "{name}: estimate {estimate} is under what a {how} document holds, {held}"
                ));
            }
        }
        // The same document charged as the stages that add a field to a document charge it:
        // each field's heap, and what the table grows by to hold it.
        let by_elements = (std::mem::size_of::<Document>()
            + shape
                .iter()
                .enumerate()
                .map(|(len, (k, v))| held_element(k, v) + held_frame_growth(len, 1))
                .sum::<usize>()) as f64;
        table.push_str(&format!(
            "{:<42} {:>9} {:>10} {:>10} {:>9.0} {:>8.2}\n",
            "   charged field by field",
            "",
            "",
            "",
            by_elements,
            by_elements / built,
        ));
        if by_elements < built || by_elements > 1.5 * built {
            failures.push(format!(
                "{name}: fields charged one by one {by_elements}, a built document holds {built}"
            ));
        }
        let most = parsed.max(built);
        if estimate > 1.5 * most {
            failures.push(format!(
                "{name}: estimate {estimate} is over 1.5 x the {most} a document holds"
            ));
        }
        // A document of long strings keeps about its BSON size as its charge, so its limits
        // stay what they were. (A short one is dominated by the cost of being a document.)
        if name == "string 4 KiB" || name == "string 100 KiB" {
            let ratio = estimate / bson_size as f64;
            if !(1.0..=1.35).contains(&ratio) {
                failures.push(format!("{name}: {ratio:.2} x its BSON, not within 1.0 to 1.35"));
            }
        }
    }
    println!("{table}");
    assert!(failures.is_empty(), "{}\n{table}", failures.join("\n"));
}
