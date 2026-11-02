//! What an answer's body holds while it is written (ADR-269): the live bytes against the bytes
//! the lease is charged.
//!
//! The documents of a ~12 MiB answer are written as JSON one at a time, as `exec` writes them,
//! into a `Vec<u8>` (the first design: the vector doubles as it fills) and into a
//! `ChunkWriter` (chunks of 64 KiB, each allocated once at its size). A counting allocator
//! measures the live bytes above where the build started, at its peak, against the length
//! written, which is what the lease is charged. One test, so nothing else allocates.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};

use kimmy_api::limits::{ANSWER_CHUNK_BYTES, ChunkWriter};
use serde_json::{Value, json};

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);

fn grew(by: isize) {
    let now = LIVE.fetch_add(by, Ordering::Relaxed) + by;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            grew(layout.size() as isize);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // A growing vector holds the old block and the new one at once while it copies.
        grew(new_size as isize);
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        if new.is_null() {
            LIVE.fetch_sub(new_size as isize, Ordering::Relaxed);
            LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
        }
        new
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn documents(extra: usize) -> Vec<Value> {
    // 3,000 documents of about 4 KB of JSON: 12 MB; the first has `extra` more characters.
    (0..3_000)
        .map(|i| json!({ "_id": i, "pad": "p".repeat(4_000 + if i == 0 { extra } else { 0 }), "n": i * 7 }))
        .collect()
}

fn build<W: std::io::Write>(documents: &[Value], sink: &mut W) {
    sink.write_all(b"{\"documents\":[").unwrap();
    for (at, doc) in documents.iter().enumerate() {
        if at > 0 {
            sink.write_all(b",").unwrap();
        }
        serde_json::to_writer(&mut *sink, doc).unwrap();
    }
    sink.write_all(b"]}").unwrap();
}

/// (peak live bytes above the start, live bytes above it when the build is done, bytes
/// written) for the answer built as a `Vec<u8>`.
fn measure_vec(documents: &[Value]) -> (isize, isize, usize) {
    let start = LIVE.load(Ordering::Relaxed);
    PEAK.store(start, Ordering::Relaxed);
    let mut body = Vec::<u8>::new();
    build(documents, &mut body);
    let done = LIVE.load(Ordering::Relaxed) - start;
    (PEAK.load(Ordering::Relaxed) - start, done, body.len())
}

/// The same for the answer built as chunks and finished.
fn measure_chunks(documents: &[Value]) -> (isize, isize, usize) {
    let start = LIVE.load(Ordering::Relaxed);
    PEAK.store(start, Ordering::Relaxed);
    let mut writer = ChunkWriter::new();
    build(documents, &mut writer);
    let body = writer.finish();
    let done = LIVE.load(Ordering::Relaxed) - start;
    (PEAK.load(Ordering::Relaxed) - start, done, body.len())
}

#[test]
fn a_chunked_body_holds_at_most_a_chunk_beyond_what_is_written() {
    let documents = documents(0);

    let (vec_peak, vec_done, vec_written) = measure_vec(&documents);
    let (chunk_peak, chunk_done, chunk_written) = measure_chunks(&documents);
    assert_eq!(vec_written, chunk_written);
    let written = chunk_written as isize;
    println!(
        "answer of {written} bytes: Vec<u8> peaks at {vec_peak} live bytes ({:+.1}% over the \
         charge) and holds {vec_done} when done ({:+.1}%); chunks peak at {chunk_peak} \
         ({:+.1}%) and hold {chunk_done} ({:+.1}%)",
        100.0 * (vec_peak - written) as f64 / written as f64,
        100.0 * (vec_done - written) as f64 / written as f64,
        100.0 * (chunk_peak - written) as f64 / written as f64,
        100.0 * (chunk_done - written) as f64 / written as f64,
    );
    assert!(written > 12_000_000);
    // The chunks hold what was written, the list of them (a few KB) and at most one chunk
    // beyond it while the last is being filled.
    assert!(chunk_peak >= written && chunk_done >= written);
    assert!(chunk_peak - written <= (ANSWER_CHUNK_BYTES + 16 * 1024) as isize);
    // The vector held more: it doubles.
    assert!(vec_peak - written > 4 * ANSWER_CHUNK_BYTES as isize, "{vec_peak} for {written}");
    assert!(vec_peak > chunk_peak);

    // A finished body is exactly what was written and the list of its chunks, however short the
    // last: with an answer one byte past a chunk, the last chunk is cut to that byte and not
    // left at the 64 KiB it was allocated.
    let extra = ANSWER_CHUNK_BYTES - (chunk_written % ANSWER_CHUNK_BYTES) + 1;
    let tail = documents_with_a_tail(extra);
    let (_, tail_done, tail_written) = measure_chunks(&tail);
    assert_eq!(tail_written % ANSWER_CHUNK_BYTES, 1);
    let list = 16 * 1024;
    assert!(
        tail_done - (tail_written as isize) < list,
        "{} bytes held beyond {tail_written}: the last chunk was not cut",
        tail_done - tail_written as isize
    );
}

fn documents_with_a_tail(extra: usize) -> Vec<Value> {
    documents(extra)
}
