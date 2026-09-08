//! What one read *holds*, measured rather than argued.
//!
//! [ADR-098](../../../docs/decisions.md) says a read may hold memory
//! proportional to its result and never to the collection it walks, and
//! nothing checked the "proportional to its result" half: every existing test
//! asserts what a page *contains*, which is the same whether the page was
//! assembled from projected documents or from stored ones. It was assembled
//! from stored ones, and over a collection of large documents — a vector
//! shadow collection, whose chunks each carry an embedding — a projected page
//! of 10,000 held gigabytes to return a list of ids (ADR-150).
//!
//! So this binary is its own, with a counting `#[global_allocator]` wrapping
//! `std::alloc::System`. That is why it cannot live in `tests/api.rs`: a
//! global allocator is a property of the whole test binary, and installing one
//! there would put the counter under every other test in the file.
//!
//! The counter is process-wide, so the two measurements take a lock for their
//! whole length rather than only around the request — a fixture built on one
//! thread would otherwise be counted against a measurement running on
//! another.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use bson::{Bson, Document, doc};
use kimmy_api::exec::{self, FindParams};
use kimmy_api::state::Auth;
use kimmy_storage::Engine;
use serde_json::json;

// ---------------------------------------------------------------------------
// The counting allocator
// ---------------------------------------------------------------------------

/// Bytes currently held, and the high-water mark since the last reset.
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// `System`, with every block it hands out and takes back counted.
///
/// `realloc` and `alloc_zeroed` are deliberately left to the trait's default
/// implementations, which are written in terms of `alloc` and `dealloc` — so
/// there is one place that counts, and no way for a growth to escape it.
struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Bytes held right now.
fn live() -> usize {
    LIVE.load(Ordering::Relaxed)
}

/// Forget the high-water mark, so the next reading is about what follows.
fn reset_peak() {
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
}

/// The high-water mark since [`reset_peak`].
fn peak() -> usize {
    PEAK.load(Ordering::Relaxed)
}

/// Held for the length of a measurement, because the counter is one counter.
static MEASURING: Mutex<()> = Mutex::new(());

// ---------------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------------

/// Documents in the fixture, and the width of the array each one carries.
///
/// 4,096 is the widest embedding this server is asked to store, and the width
/// the finding behind ADR-150 was measured at.
const N: usize = 1_000;
const DIM: usize = 4_096;

/// What one of these documents costs once it is decoded.
///
/// A BSON array of doubles is `dim × 8` bytes stored and a `Vec<Bson>` of
/// `dim` elements decoded, and a `Bson` is an enum wide enough for its widest
/// variant — so the decoded form is several times the stored one. This is the
/// unit the bounds below are written in, so that a change to `Bson`'s width
/// moves the bound with it rather than silently loosening it.
fn decoded_bytes() -> usize {
    DIM * std::mem::size_of::<Bson>()
}

/// A document holding `DIM` doubles, whose smallest element is `i`.
///
/// Distinct minima, because that is what the sort orders by: a path through an
/// array sorts by its smallest element, in both directions.
fn wide_doc(i: i64) -> Document {
    let values: Vec<Bson> =
        (0..DIM).map(|k| Bson::Double(i as f64 + (k as f64) / (DIM as f64))).collect();
    doc! { "_id": i, "x": Bson::Array(values) }
}

/// A live state over a fresh database, holding `N` wide documents.
fn fixture(dir: &tempfile::TempDir) -> kimmy_api::SharedState {
    let engine = std::sync::Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let tokens =
        kimmy_auth::TokenIssuer::new("an-adequately-long-test-secret-for-hs256", 3600).unwrap();
    let state = kimmy_api::state(engine, tokens, false, kimmy_api::RateLimits::disabled()).unwrap();

    state.engine.create_collection("app", "docs").unwrap();
    let meta = state.engine.get_collection("app", "docs").unwrap();
    // In batches, so the fixture never holds the whole collection at once —
    // what is being measured is the read, and a build that peaked higher than
    // the request would make the reading meaningless.
    let mut next = 0i64;
    while (next as usize) < N {
        let batch: Vec<Document> = (next..(next + 100).min(N as i64)).map(wide_doc).collect();
        next += batch.len() as i64;
        state.engine.insert_many(&meta, batch).unwrap();
    }
    state
}

fn root() -> Auth {
    Auth(kimmy_auth::Principal::insecure_root())
}

/// A `find` that asks for ids only, over the whole fixture.
fn ids_only(sort: Option<serde_json::Value>) -> FindParams {
    FindParams {
        filter: Some(json!({})),
        sort,
        projection: Some(json!({ "_id": 1 })),
        limit: Some(N),
        ..Default::default()
    }
}

/// What a page of `N` ids may hold: a handful of decoded documents, plus a
/// kilobyte per document returned.
///
/// Generous on both terms and still an order of magnitude below a page that
/// holds its stored documents — which is the point. The eight is headroom for
/// the one document in hand, the bytes it was decoded from, and whatever the
/// read transaction has open; it is not a budget anything is expected to
/// spend.
fn bound() -> usize {
    8 * decoded_bytes() + N * 1024
}

/// Run one `find` and answer with what it held above where it started.
///
/// The same request is made twice and only the second is measured. redb keeps
/// a page cache — up to `storage.cache_bytes`, evicted only for room — and the
/// first pass over a collection fills it, so a single reading counts the
/// storage layer's cache as though the request were holding it. That cache is
/// the node's, bounded by its own setting and shared by every later request;
/// what ADR-098 bounds is the *request*, which is what the second reading is.
fn held_by(state: &kimmy_api::SharedState, params: impl Fn() -> FindParams) -> serde_json::Value {
    drop(exec::find(state, &root(), "app", "docs", params()).unwrap());

    let before = live();
    reset_peak();
    let body = exec::find(state, &root(), "app", "docs", params()).unwrap();
    let held = peak().saturating_sub(before);
    // Carried out with the body so the assertions and the reading are one
    // value: a test that reported a number it did not also check the page of
    // could pass over an empty page.
    json!({ "held": held, "body": body })
}

fn report(what: &str, held: usize) {
    println!(
        "{what}: held {held} bytes ({:.1} MiB) for {N} ids; one decoded document is {} bytes; \
         the bound is {} bytes ({:.1} MiB)",
        held as f64 / (1024.0 * 1024.0),
        decoded_bytes(),
        bound(),
        bound() as f64 / (1024.0 * 1024.0),
    );
}

// ---------------------------------------------------------------------------
// The measurements
// ---------------------------------------------------------------------------

#[test]
fn size_of_bson_is_the_amplification_this_bound_is_written_in() {
    // Takes the lock like the measurements do, though it measures nothing:
    // the counter is one counter, and a test in this file that allocated
    // beside a reading would be an exception to that rule for a reader to
    // discover rather than to read.
    let _measuring = MEASURING.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    // Reported rather than pinned to a number: what matters is that a decoded
    // array of doubles is several times its stored width, which is why an
    // unprojected page over a shadow collection is not the size of the bytes
    // on disk.
    let width = std::mem::size_of::<Bson>();
    println!(
        "size_of::<Bson>() = {width}; a dim-{DIM} vector is {} bytes stored as BSON doubles and \
         {} bytes decoded",
        DIM * 8,
        DIM * width
    );
    assert!(width >= 8, "a Bson holds at least a double");
}

#[test]
fn an_unsorted_page_of_ids_holds_the_ids_and_not_the_documents() {
    let _measuring = MEASURING.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let state = fixture(&dir);

    let measured = held_by(&state, || ids_only(None));
    let held = measured["held"].as_u64().unwrap() as usize;
    let body = &measured["body"];

    assert_eq!(body["count"].as_u64(), Some(N as u64));
    assert_eq!(body["documents"][0], json!({ "_id": 0 }), "ids only, as asked for");
    report("unsorted", held);
    assert!(
        held < bound(),
        "an unsorted page of {N} ids held {held} bytes, over the {} it is allowed: the page is \
         holding stored documents rather than what it returns (ADR-150)",
        bound()
    );
}

#[test]
fn a_sorted_page_of_ids_holds_its_sort_keys_and_not_the_documents() {
    let _measuring = MEASURING.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let state = fixture(&dir);

    let measured = held_by(&state, || ids_only(Some(json!({ "x": 1 }))));
    let held = measured["held"].as_u64().unwrap() as usize;
    let body = &measured["body"];

    assert_eq!(body["count"].as_u64(), Some(N as u64));
    // `x`'s smallest element is the document's `_id`, so the sort is `_id`
    // order arrived at through the array rule — which is what makes the page
    // checkable without holding the documents to check it against.
    assert_eq!(body["documents"][0], json!({ "_id": 0 }));
    assert_eq!(body["documents"][N - 1], json!({ "_id": N as i64 - 1 }));
    report("sorted", held);
    assert!(
        held < bound(),
        "a sorted page of {N} ids held {held} bytes, over the {} it is allowed: the sort window \
         is holding stored documents rather than sort keys and what it returns (ADR-150)",
        bound()
    );
}
