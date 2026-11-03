//! The keys an `update`, a `delete` or a `find_and_modify` gathers from an index range under the
//! writer, and the documents it matches (ADR-270's second addendum): charged to a lease on the
//! node's pool, named `what the write holds`, under the request limit, never waiting for the
//! pool; the first 10,000 keys free; the lease kept at its high-water mark across the chunks of a
//! `multi` write; the gather ended by the deadline and the stop, with nothing written before the
//! first commit.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use bson::{Document, doc};
use kimmy_query::budget::held_set_key;
use serde_json::{Value, json};

use super::tests::live_state;
use super::*;
use crate::error::{ErrorCode, Retry};
use crate::limits::AggregateMemory;

/// The keys a write may gather without asking the pool.
const FREE: usize = kimmy_storage::MAX_CANDIDATES;

fn root() -> Auth {
    Auth(kimmy_auth::Principal::superuser("test"))
}

/// `n` documents `{_id: i, t: i, u: i % 10}` under indexes on `t` and `u`, so a range of `t` is
/// an index range whose entries carry the `_id`s the budget counts. The request limit and the
/// pool are `limit` and `pool`.
fn indexed(dir: &tempfile::TempDir, n: i64, limit: usize, pool: usize) -> SharedState {
    let state = live_state(dir);
    state.set_aggregate_memory(AggregateMemory::new(limit, pool));
    state.engine.create_collection("app", "orders").unwrap();
    for field in ["t", "u"] {
        let index = vec![kimmy_storage::IndexField::ascending(field)];
        state.engine.create_index("app", "orders", index, false, None).unwrap();
    }
    let meta = state.engine.get_collection("app", "orders").unwrap();
    let docs: Vec<Document> = (0..n).map(|i| doc! { "_id": i, "t": i, "u": i % 10 }).collect();
    state.engine.insert_many(&meta, docs).unwrap();
    state
}

/// What the budget counts for the keys of `n` of the `_id`s of `indexed`.
fn key_bytes(n: usize) -> usize {
    n * held_set_key(5)
}

fn pool_of(state: &SharedState) -> kimmy_query::budget::MemoryPool {
    state.aggregate_memory().pool().clone()
}

/// The filter of the keys of `t >= from` in a collection of `n`: `n - from` of them. It rejects
/// every document, so nothing is written.
fn rejecting(from: i64) -> Value {
    json!({ "t": { "$gte": from }, "missing": 1 })
}

/// The filter of every document of `t >= from`, which a write changes.
fn matching(from: i64) -> Value {
    json!({ "t": { "$gte": from } })
}

fn set_x() -> Value {
    json!({ "$set": { "x": 1 } })
}

fn write(filter: Value, multi: bool) -> WriteParams {
    WriteParams { filter: Some(filter), multi, ..Default::default() }
}

fn updated(state: &SharedState, filter: Value) -> Result<Value, ApiError> {
    update(state, &root(), "app", "orders", &set_x(), write(filter, true))
}

fn deleted(state: &SharedState, filter: Value) -> Result<Value, ApiError> {
    delete(state, &root(), "app", "orders", write(filter, true))
}

fn modified(state: &SharedState, filter: Value, sorted: bool) -> Result<Value, ApiError> {
    let spec = FindAndModifySpec {
        filter: Some(filter),
        sort: sorted.then(|| json!({ "t": 1 })),
        update: Some(set_x()),
        ..Default::default()
    };
    find_and_modify(state, &root(), "app", "orders", spec)
}

/// The bytes a refusal says the lease held: "what the write holds is N bytes".
fn held_in(err: &ApiError) -> usize {
    let tail = err.message.split("what the write holds is ").nth(1).expect(&err.message);
    tail.split(' ').next().unwrap().parse().unwrap()
}

/// How many documents carry `x`, which only a write that changed them sets.
fn changed(state: &SharedState) -> u64 {
    let counted = count(
        state,
        &root(),
        "app",
        "orders",
        FindParams { filter: Some(json!({ "x": 1 })), ..Default::default() },
    );
    counted.unwrap()["count"].as_u64().unwrap()
}

/// How many of the first `n` documents carry `x`, by point reads: a read that walks stops at
/// the node's stop, and a test of the stop has to look afterwards.
fn changed_among(state: &SharedState, n: i64) -> usize {
    let meta = state.engine.get_collection("app", "orders").unwrap();
    (0..n)
        .filter(|i| {
            let doc = state.engine.get(&meta, &DocId::Int64(*i)).unwrap().unwrap();
            doc.contains_key("x")
        })
        .count()
}

/// The oplog's entries, which a write that wrote nothing leaves as they were.
fn oplog_len(state: &SharedState) -> usize {
    state
        .engine
        .read_oplog_from(kimmy_core::Hlc::ZERO, 1_000_000, kimmy_storage::WalkScope::Request)
        .unwrap()
        .len()
}

#[test]
fn a_write_over_n_keys_is_answered_and_over_n_plus_one_is_refused_at_the_limit_by_the_byte() {
    // 10,101 entries, a limit of exactly 10,100 keys. A filter that rejects every document of
    // the range of 10,100 is answered, `matched: 0`, as it was before the hold; the range of
    // 10,101 is refused at its last key, for each verb. The lease is made at the 10,001st key
    // and charged for all the keys then held, so the refusal says the bytes of 10,101.
    let n = 10_101;
    let limit = key_bytes(10_100);
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, n, limit, 64 << 20);
    let pool = pool_of(&state);
    let log = oplog_len(&state);

    assert_eq!(updated(&state, rejecting(1)).unwrap()["matched"], 0);
    assert_eq!(deleted(&state, rejecting(1)).unwrap()["deleted"], 0);
    assert_eq!(modified(&state, rejecting(1), true).unwrap()["matched"], 0);
    assert_eq!(modified(&state, rejecting(1), false).unwrap()["matched"], 0);
    assert_eq!((pool.used(), pool.holders()), (0, 0), "given back when answered");

    type Verb = fn(&SharedState, Value) -> Result<Value, ApiError>;
    let verbs: [(&str, Verb); 4] = [
        ("update", updated),
        ("delete", deleted),
        ("find_and_modify, sorted", |s, f| modified(s, f, true)),
        ("find_and_modify", |s, f| modified(s, f, false)),
    ];
    for (verb, run) in verbs {
        let before = pool.limit_refusals();
        let err = run(&state, rejecting(0)).unwrap_err();
        assert_eq!(
            err.status,
            axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            "{verb}: {}",
            err.message
        );
        assert_eq!(err.code, ErrorCode::AnswerTooLarge);
        assert_eq!(err.code.retry(), Retry::No);
        assert_eq!(held_in(&err), key_bytes(10_101), "{verb}: refused at the key past the limit");
        let text = format!(
            "what the write holds is {} bytes, over the limit of {limit} bytes one write \
             may hold at once (aggregate.memory_limit_bytes); ",
            key_bytes(10_101)
        );
        assert!(err.message.starts_with(&text), "{verb}: {}", err.message);
        for remedy in [
            "narrow the filter on the indexed field so that its range is smaller",
            "several writes, each over part of the range",
            "raise aggregate.memory_limit_bytes",
        ] {
            assert!(err.message.contains(remedy), "{verb}: {}", err.message);
        }
        // Not an aggregation's, and not a find's: a write has no `$match`, `cursor` or sort.
        for other in ["$match", "cursor", "sort by"] {
            assert!(!err.message.contains(other), "{verb}: {}", err.message);
        }
        assert_eq!((pool.used(), pool.holders()), (0, 0), "{verb}: and given back when refused");
        assert_eq!(pool.limit_refusals(), before + 1, "{verb}: the shared counter moved");
    }

    // A write that would have written is refused the same, and writes nothing.
    let err = updated(&state, matching(0)).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert_eq!((changed(&state), oplog_len(&state)), (0, log), "no document, no oplog entry");
}

#[test]
fn the_first_ten_thousand_keys_are_free_and_the_next_makes_the_lease_for_all_of_them() {
    // A limit of nothing and a pool of nothing: a lease that charged one byte would be
    // refused. A range of 10,000 keys is answered; the 10,001st makes the lease and is
    // charged with the 10,000 before it.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 10_101, 0, 0);
    let pool = pool_of(&state);
    assert_eq!(updated(&state, rejecting(10_101 - FREE as i64)).unwrap()["matched"], 0);
    assert_eq!((pool.limit_refusals(), pool.pool_refusals(), pool.holders()), (0, 0, 0));

    let err = updated(&state, rejecting(10_101 - FREE as i64 - 1)).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert_eq!(held_in(&err), key_bytes(FREE + 1));

    // And with the pool full: the free window is answered, the key past it is `503`.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 10_101, 64 << 20, 1 << 20);
    let pool = pool_of(&state);
    let other = state.aggregate_memory().limits();
    let mut full = other.buffer.lease("another request");
    full.charge(pool.cap()).unwrap();
    assert_eq!(updated(&state, rejecting(10_101 - FREE as i64)).unwrap()["matched"], 0);
    assert_eq!(pool.pool_refusals(), 0);
    let err = updated(&state, rejecting(10_101 - FREE as i64 - 1)).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::SERVICE_UNAVAILABLE, "{}", err.message);
    assert_eq!(pool.pool_refusals(), 1);
    drop(full);
    assert_eq!(pool.used(), 0);
}

#[test]
fn a_full_pool_refuses_a_write_at_once_with_503_and_retry_wait() {
    // Another request holds the whole pool. The write is the youngest holder, and is refused
    // at its 10,001st key, without a wait, with its own bytes given back.
    let dir = tempfile::tempdir().unwrap();
    let pool_bytes = key_bytes(10_101) + 1_000 * document_bytes(true);
    let state = indexed(&dir, 10_101, 64 << 20, pool_bytes);
    let pool = pool_of(&state);
    let other = state.aggregate_memory().limits();
    let mut held = other.buffer.lease("another request");
    held.charge(pool_bytes).unwrap();

    let started = Instant::now();
    let err = updated(&state, matching(0)).unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
    assert_eq!(err.status, axum::http::StatusCode::SERVICE_UNAVAILABLE, "{}", err.message);
    assert_eq!(err.code, ErrorCode::Timeout);
    assert_eq!(err.code.retry(), Retry::Wait);
    assert_eq!(err.retry_after_secs, Some(crate::error::AGGREGATE_POOL_RETRY_AFTER_SECS));
    assert!(err.message.starts_with("what the write holds needed "), "{}", err.message);
    assert!(err.message.contains("aggregate.memory_pool_bytes"), "{}", err.message);
    assert!(
        err.message.contains("retry later, or hold less: narrow the filter on the indexed field so that its range is smaller"),
        "{}",
        err.message
    );
    assert!(!err.message.contains("$match"), "{}", err.message);
    assert_eq!(pool.used(), pool_bytes, "only the other request's bytes are left");
    assert_eq!((pool.pool_refusals(), pool.limit_refusals()), (1, 0));
    assert_eq!(changed(&state), 0, "nothing was written");
    drop(held);
    assert_eq!(updated(&state, matching(0)).unwrap()["matched"], 10_101);
    assert_eq!(pool.used(), 0);
}

/// Run `update` on a thread of its own, stopped by `key_hooks` once it holds `at` bytes of keys:
/// the thread says so on the first channel and goes on when it is sent something on the second.
struct Gated {
    reached: mpsc::Receiver<()>,
    go: mpsc::Sender<()>,
    thread: std::thread::JoinHandle<(Result<Value, ApiError>, Duration)>,
}

fn gated(state: &SharedState, filter: Value, at: usize) -> Gated {
    let (reached_tx, reached) = mpsc::channel();
    let (go, go_rx) = mpsc::channel();
    let state = Arc::clone(state);
    let thread = std::thread::spawn(move || {
        let mut gate = Some((reached_tx, go_rx));
        key_hooks::after_each_key(move |held| {
            if held >= at
                && let Some((reached, go)) = gate.take()
            {
                reached.send(()).unwrap();
                go.recv().unwrap();
            }
        });
        let started = Instant::now();
        let result = updated(&state, filter);
        (result, started.elapsed())
    });
    Gated { reached, go, thread }
}

#[test]
fn a_write_never_waits_for_the_pool_even_when_it_is_the_oldest_holder() {
    // The write holds half its keys and is stopped; another request takes the rest of the
    // pool. A `find` that was the older holder would wait for it (and is answered when the
    // other lets go). A write holds the single writer, so it is refused `503` at once.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 10_101, 64 << 20, key_bytes(10_101) + key_bytes(100));
    let pool = pool_of(&state);

    let write = gated(&state, rejecting(0), key_bytes(10_050));
    write.reached.recv_timeout(Duration::from_secs(60)).unwrap();
    assert_eq!(pool.holders(), 1);
    let other = state.aggregate_memory().limits();
    let mut young = other.buffer.lease("another request");
    young.charge(pool.cap() - pool.used()).unwrap();
    assert_eq!(pool.used(), pool.cap());

    write.go.send(()).unwrap();
    let (result, took) = write.thread.join().unwrap();
    let err = result.unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::SERVICE_UNAVAILABLE, "{}", err.message);
    assert_eq!(err.code.retry(), Retry::Wait);
    assert!(err.message.starts_with("what the write holds needed "), "{}", err.message);
    assert!(took < Duration::from_secs(4), "{took:?}: it did not wait for the pool");
    assert_eq!(pool.pool_refusals(), 1);
    assert_eq!(pool.used(), young.bytes(), "it gave back all it held");
    drop(young);
    assert_eq!((pool.used(), pool.holders()), (0, 0));
}

fn in_a_request<T>(left: Duration, work: impl FnOnce() -> T) -> T {
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(
        crate::limits::as_a_request(Instant::now() + left, Duration::from_secs(30), async {
            work()
        }),
    )
}

#[test]
fn a_multi_write_keeps_its_lease_at_the_most_it_held_until_the_request_is_over() {
    // 10,101 entries in chunks of 4,000: the whole range is gathered for each of the three
    // chunks. The pool is exactly what the first chunk needs: its keys and 4,000 documents. The lease is made in the first
    // chunk, stands at that between the chunks, is not asked for more by the second and
    // third, and goes back when the request is answered.
    let dir = tempfile::tempdir().unwrap();
    let most = key_bytes(10_101) + 4_000 * document_bytes(true);
    let state = indexed(&dir, 10_101, most, most);
    state.engine.set_multi_chunk_docs(4_000);
    let pool = pool_of(&state);
    let between = Arc::new(AtomicUsize::new(usize::MAX));
    let (seen, watched) = (Arc::clone(&between), pool.clone());
    state.engine.before_next_continuing_write(move || {
        seen.store(watched.used(), Ordering::SeqCst);
    });
    let answered = updated(&state, matching(0)).unwrap();
    assert_eq!(
        (answered["matched"].clone(), answered["modified"].clone(), answered["commits"].clone()),
        (json!(10_101), json!(10_101), json!(3))
    );
    assert_eq!(
        between.load(Ordering::SeqCst),
        most,
        "between the chunks the lease stands at the most the request held"
    );
    assert_eq!(pool.used(), 0, "and it is given back at the end");
    assert_eq!(pool.holders(), 0);
    assert_eq!(changed(&state), 10_101);
}

/// What the budget counts for one document of `indexed` when a write keeps its stored body
/// beside it (a chunk of `update` or `delete`), and when it does not (what a sort keeps).
fn document_bytes(with_body: bool) -> usize {
    let document = doc! { "_id": 5_i64, "t": 5_i64, "u": 5_i64 };
    let body = if with_body { bson::serialize_to_vec(&document).unwrap().len() } else { 0 };
    kimmy_query::budget::held_bytes(&document) + kimmy_query::budget::held_blob(body)
}

#[test]
fn the_documents_a_chunk_matches_are_charged_on_the_lease_by_the_byte() {
    // 10,101 entries in chunks of 1,000: the lease is made at the 10,001st key of the first
    // gather, and the first chunk holds all the keys and, at the most, 1,000 matched documents
    // with their bodies. A limit of exactly that answers the whole request, eleven chunks, since
    // the lease stands at it; a byte under refuses the first chunk's 1,000th document.
    let (keys, one) = (key_bytes(10_101), document_bytes(true));
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 10_101, keys + 1_000 * one, 64 << 20);
    let answered = updated(&state, matching(0)).unwrap();
    assert_eq!((&answered["matched"], &answered["commits"]), (&json!(10_101), &json!(11)));
    assert_eq!(pool_of(&state).used(), 0);

    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 10_101, keys + 1_000 * one - 1, 64 << 20);
    let log = oplog_len(&state);
    let err = updated(&state, matching(0)).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert_eq!(held_in(&err), keys + 1_000 * one, "refused at the document that passes the limit");
    assert_eq!((changed(&state), oplog_len(&state)), (0, log), "nothing was written");
    assert_eq!(pool_of(&state).used(), 0);

    // A delete holds the same.
    let err = deleted(&state, matching(0)).unwrap_err();
    assert_eq!(held_in(&err), keys + 1_000 * one);

    // A range within what a write may match unpooled has no lease, and its documents are held
    // as they always were, uncharged, under a limit of nothing.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 10_101, 0, 0);
    let answered = updated(&state, matching(101)).unwrap();
    assert_eq!(answered["matched"], 10_000);
    assert_eq!((pool_of(&state).limit_refusals(), pool_of(&state).holders()), (0, 0));
}

#[test]
fn a_sort_charges_every_match_it_holds_and_not_their_bodies() {
    // A `find_and_modify` that sorts holds every match to choose one: 9,045 of the 10,051 entries
    // of the range here, without their stored bodies. The keys of the gather are held beside
    // them while they are read.
    let total = 10_051;
    let wanted = json!({ "t": { "$gte": 50 }, "u": { "$ne": 0 } });
    let matches = (50..10_101).filter(|i| i % 10 != 0).count();
    assert_eq!(matches, 9_045);
    let peak = key_bytes(total) + matches * document_bytes(false);
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 10_101, peak, 64 << 20);
    assert_eq!(modified(&state, wanted.clone(), true).unwrap()["matched"], 1);
    assert_eq!(pool_of(&state).used(), 0);

    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 10_101, peak - 1, 64 << 20);
    let err = modified(&state, wanted, true).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert_eq!(held_in(&err), peak, "refused at the last match");
    assert_eq!(changed(&state), 0, "nothing was written");
}

#[test]
fn a_pool_another_request_takes_between_the_chunks_does_not_refuse_the_later_ones() {
    // The pool is what the first chunk needs. Between the first chunk and the second another request
    // takes whatever the pool has free, and keeps it to the end. Were the lease given back
    // between the chunks that would be all of it, and the second chunk, after a commit, would be
    // refused (`500 partially_applied`) for a pool it had already been given; the lease stands,
    // so the second and third chunks ask for nothing.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 10_101, 64 << 20, key_bytes(10_101) + 4_000 * document_bytes(true));
    state.engine.set_multi_chunk_docs(4_000);
    let pool = pool_of(&state);
    let other = state.aggregate_memory().limits();
    let taken = Arc::new(std::sync::Mutex::new(None));
    let (slot, watched) = (Arc::clone(&taken), pool.clone());
    state.engine.before_next_continuing_write(move || {
        let mut lease = other.buffer.lease("another request");
        lease.charge(watched.cap() - watched.used()).unwrap();
        *slot.lock().unwrap() = Some(lease);
    });
    let answered = updated(&state, matching(0)).unwrap();
    assert_eq!(answered["commits"], 3);
    assert_eq!(pool.used(), taken.lock().unwrap().as_ref().unwrap().bytes());
    taken.lock().unwrap().take();
    assert_eq!(pool.used(), 0);
}

#[test]
fn a_later_chunk_whose_range_has_outgrown_the_lease_is_partly_applied_with_the_refusal_as_cause() {
    // Between the first chunk and the second 100 documents enter the range. The lease stands at
    // the keys and documents of the first chunk, and the second chunk's come to 100 keys more,
    // which is over the limit; the refusal is after a commit: `500 partially_applied`, retry
    // `verify`, with the first chunk counted and the refusal as the cause. A range that had not
    // grown asked the pool for nothing (the test before).
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 10_101, key_bytes(10_101) + 4_000 * document_bytes(true), 64 << 20);
    state.engine.set_multi_chunk_docs(4_000);
    let (engine, meta) = (Arc::clone(&state.engine), state.engine.get_collection("app", "orders"));
    let meta = meta.unwrap();
    state.engine.before_next_continuing_write(move || {
        let docs: Vec<Document> =
            (20_000..20_100).map(|i| doc! { "_id": i, "t": i, "u": 0 }).collect();
        engine.insert_many(&meta, docs).unwrap();
    });
    let err = updated(&state, matching(0)).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::INTERNAL_SERVER_ERROR, "{}", err.message);
    assert_eq!(err.code, ErrorCode::PartiallyApplied);
    assert_eq!(err.code.retry(), Retry::Verify);
    let extra = err.extra.as_ref().unwrap();
    assert_eq!(extra["applied"]["matched"], 4_000);
    assert_eq!(extra["applied"]["commits"], 1);
    assert_eq!(extra["cause"]["code"], "answer_too_large");
    assert!(extra["cause"]["message"].as_str().unwrap().starts_with("what the write holds is "));
    assert_eq!(changed(&state), 4_000, "the first chunk stands, and nothing after it");
    assert_eq!(pool_of(&state).used(), 0);
}

#[test]
fn the_other_paths_of_a_write_take_no_lease_and_touch_no_pool() {
    // A request limit of nothing and a pool of nothing: a lease that charged a single byte
    // would be refused. A scan, an `_id` lookup, a lookup of several, the free window of an
    // index range and an `explain` are all answered, no key is held, and the pool is never
    // asked.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 300, 0, 0);
    let taken = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&taken);
    key_hooks::after_each_key(move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    let pool = pool_of(&state);
    let p = |filter: Value| write(filter, true);

    // A collection scan: no index applies to the field.
    assert_eq!(updated(&state, json!({ "w": 1 })).unwrap()["matched"], 0);
    assert_eq!(deleted(&state, json!({ "w": 1 })).unwrap()["deleted"], 0);
    assert!(modified(&state, json!({ "w": 1 }), true).is_ok());
    // An `_id`, and several.
    assert_eq!(updated(&state, json!({ "_id": 5 })).unwrap()["matched"], 1);
    assert_eq!(updated(&state, json!({ "_id": { "$in": [9, 3] } })).unwrap()["matched"], 2);
    assert!(modified(&state, json!({ "_id": 7 }), false).is_ok());
    // An explain, which does not write.
    let explained = WriteParams { explain: true, ..p(matching(0)) };
    assert!(update(&state, &root(), "app", "orders", &set_x(), explained).is_ok());
    let explained = WriteParams { explain: true, ..p(matching(0)) };
    assert!(delete(&state, &root(), "app", "orders", explained).is_ok());
    assert_eq!(taken.load(Ordering::SeqCst), 0, "no key was held");
    assert_eq!(
        (pool.used(), pool.limit_refusals(), pool.pool_refusals(), pool.holders()),
        (0, 0, 0, 0)
    );

    // An index range within the free window takes keys and not the pool.
    assert_eq!(updated(&state, matching(0)).unwrap()["matched"], 300);
    assert!(taken.load(Ordering::SeqCst) >= 300, "the keys were held");
    assert_eq!(
        (pool.used(), pool.limit_refusals(), pool.pool_refusals(), pool.holders()),
        (0, 0, 0, 0)
    );
}

#[test]
fn a_write_gives_its_lease_back_on_every_way_out() {
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 10_101, 64 << 20, 64 << 20);
    let pool = pool_of(&state);
    let quiet = |state: &SharedState| {
        let pool = pool_of(state);
        (pool.used(), pool.holders())
    };

    // Answered, with a lease made.
    let peak = Arc::new(AtomicUsize::new(0));
    let (seen, watched) = (Arc::clone(&peak), pool.clone());
    key_hooks::after_each_key(move |_| {
        seen.fetch_max(watched.used(), Ordering::SeqCst);
    });
    assert_eq!(updated(&state, matching(0)).unwrap()["matched"], 10_101);
    assert!(peak.load(Ordering::SeqCst) >= key_bytes(10_101), "the lease was made and charged");
    assert_eq!(quiet(&state), (0, 0));

    // An update whose operator fails on the first document, with the lease made.
    let failing = json!({ "$inc": { "t": "no" } });
    let err = update(&state, &root(), "app", "orders", &failing, write(matching(0), true));
    assert_eq!(err.unwrap_err().status, axum::http::StatusCode::BAD_REQUEST);
    assert_eq!(quiet(&state), (0, 0));

    // A commit that fails, in the second chunk: the storage failure is armed between the two.
    state.engine.set_multi_chunk_docs(4_000);
    let armer = Arc::clone(&state.engine);
    state.engine.before_next_continuing_write(move || {
        assert!(armer.arm_test_storage_failure("sync_data"));
    });
    let bump = json!({ "$inc": { "u": 1 } });
    let err = update(&state, &root(), "app", "orders", &bump, write(matching(0), true));
    assert_eq!(err.unwrap_err().code, ErrorCode::PartiallyApplied);
    assert_eq!(quiet(&state), (0, 0));
}

#[test]
fn a_panic_with_the_lease_held_gives_it_back() {
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 10_101, 64 << 20, 64 << 20);
    let pool = pool_of(&state);
    let held_when_it_panicked = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&held_when_it_panicked);
    key_hooks::after_each_key(move |held| {
        if held >= key_bytes(10_050) {
            seen.store(held, Ordering::SeqCst);
            panic!("a panic with keys held");
        }
    });
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = updated(&state, matching(0));
    }));
    assert!(panicked.is_err());
    assert!(held_when_it_panicked.load(Ordering::SeqCst) >= key_bytes(10_050));
    assert_eq!((pool.used(), pool.holders()), (0, 0), "a panic gives them back too");
}

#[test]
fn a_gather_is_cut_at_the_deadline_and_ticks_for_each_entry() {
    let (small, big) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (a, b) =
        (indexed(&small, 100, 64 << 20, 64 << 20), indexed(&big, 1_100, 64 << 20, 64 << 20));
    let looks = |state: &SharedState| {
        kimmy_core::deadline::looks_of(|| {
            in_a_request(Duration::from_secs(3600), || updated(state, rejecting(0)))
        })
        .1
    };
    // A thousand more entries cost a look each for the walk that gathers them and a look each
    // for the document read behind each: before the gather ticked it was the documents only.
    let diff = looks(&b) - looks(&a);
    assert_eq!(diff, 2_000, "{diff} looks for a thousand more entries");

    // A request with no time left is `503 timeout`, with nothing written.
    let late = in_a_request(Duration::ZERO, || updated(&b, matching(0)));
    let err = late.unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::SERVICE_UNAVAILABLE, "{}", err.message);
    assert_eq!(err.code, ErrorCode::Timeout);
    assert_eq!(changed(&b), 0);

    // A deadline that passes in the middle of the gather ends it there, before a document is
    // read: the 300th key sleeps until the request's 100 ms are over.
    let counted = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&counted);
    key_hooks::after_each_key(move |_| {
        if seen.fetch_add(1, Ordering::SeqCst) == 299 {
            std::thread::sleep(Duration::from_millis(150));
        }
    });
    let err = in_a_request(Duration::from_millis(100), || updated(&b, matching(0))).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::SERVICE_UNAVAILABLE, "{}", err.message);
    assert_eq!(err.code, ErrorCode::Timeout);
    // The clock is read every so many entries (a stride that has grown over 300 quick ones),
    // so it is not the next entry that ends it, but one well before the range's 1,100.
    let ended_at = counted.load(Ordering::SeqCst);
    assert!((300..1_100).contains(&ended_at), "the gather ended at entry {ended_at}");
    assert_eq!((changed(&b), pool_of(&b).used()), (0, 0));
}

#[test]
fn a_stop_cuts_a_first_chunk_with_nothing_written_and_a_later_one_as_partly_applied() {
    // The drain deadline arrives (the engine's stop) in the middle of a gather. In the first
    // chunk the request is refused `503 node_stopping`, nothing written. In the second it has
    // written the first chunk, and ADR-192 answers `500 partially_applied`, retry `verify`,
    // with the stop as the cause and the first chunk counted; the second chunk is not half
    // done, since it wrote nothing.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 30, 64 << 20, 64 << 20);
    state.engine.set_multi_chunk_docs(10);
    let stopper = Arc::clone(&state.engine);
    let counted = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&counted);
    key_hooks::after_each_key(move |_| {
        // The range is 30 keys: the first gather takes the 1st to the 30th, the second the
        // 31st to the 60th. The 35th is the fifth key of the second.
        if seen.fetch_add(1, Ordering::SeqCst) + 1 == 35 {
            stopper.set_stopping();
        }
    });
    let err = updated(&state, matching(0)).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::INTERNAL_SERVER_ERROR, "{}", err.message);
    assert_eq!(err.code, ErrorCode::PartiallyApplied);
    let extra = err.extra.as_ref().unwrap();
    assert_eq!(
        (&extra["applied"]["matched"], &extra["applied"]["commits"]),
        (&json!(10), &json!(1))
    );
    assert_eq!(extra["cause"]["code"], "stopping");
    assert_eq!(counted.load(Ordering::SeqCst), 35, "the gather ended at the next entry");
    assert_eq!(
        changed_among(&state, 30),
        10,
        "the first chunk stands and the second wrote nothing"
    );

    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 30, 64 << 20, 64 << 20);
    let stopper = Arc::clone(&state.engine);
    let counted = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&counted);
    key_hooks::after_each_key(move |_| {
        if seen.fetch_add(1, Ordering::SeqCst) + 1 == 5 {
            stopper.set_stopping();
        }
    });
    let err = updated(&state, matching(0)).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::SERVICE_UNAVAILABLE, "{}", err.message);
    assert_eq!(err.code, ErrorCode::NodeStopping);
    assert_eq!(counted.load(Ordering::SeqCst), 5);
    assert_eq!(changed_among(&state, 30), 0, "nothing was written");
    assert_eq!(pool_of(&state).used(), 0);
}

#[test]
fn a_deadline_that_passes_after_the_first_commit_does_not_cut_a_later_gather() {
    // ADR-192: a request that has written something keeps going. The request's 100 ms are
    // over before the second chunk, and the second and third chunks gather and land.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 30, 64 << 20, 64 << 20);
    state.engine.set_multi_chunk_docs(10);
    state.engine.before_next_continuing_write(|| std::thread::sleep(Duration::from_millis(250)));
    let answered = in_a_request(Duration::from_millis(100), || updated(&state, matching(0)));
    let answered = answered.unwrap();
    assert_eq!((&answered["matched"], &answered["commits"]), (&json!(30), &json!(3)));
    assert_eq!(changed(&state), 30);
}
