//! The keys a `find` holds to put an inexact index range in `_id` order (ADR-269, ADR-270):
//! they are charged to a lease on the node's pool, named `the index range of the filter`,
//! under the request limit; no other path of a `find` takes a lease; and the lease goes back
//! on every exit.

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

fn root() -> Auth {
    Auth(kimmy_auth::Principal::superuser("test"))
}

/// The state of `n` documents `{_id: i, t: i, u: i % 10}` under an index on `t`, so a range of
/// `t` is an inexact index range, delivered in `_id` order, whose entries carry the `_id`s the
/// budget counts. The request limit and the pool are `limit` and `pool`.
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

/// The most keys any pass holds, for a filter that rejects every candidate of a range of `n`
/// entries read for a window of 100 (the default `limit`): the first pass wants 100 and each
/// wants twice as many as the one before, past the keys the passes before it offered. Not the
/// range, but up to about half of it.
fn largest_pass(n: usize) -> usize {
    let (mut done, mut batch, mut most) = (0, 100, 0);
    while done < n {
        let held = batch.min(n - done);
        most = most.max(held);
        done += held;
        batch *= 2;
    }
    most
}

/// A range that takes most of the collection and a condition that rejects every document of
/// it, so the walk goes from pass to pass, doubling, up to the whole range.
fn rejecting() -> Value {
    json!({ "t": { "$gte": 0 }, "missing": 1 })
}

fn find_of(state: &SharedState, params: FindParams) -> Result<Value, ApiError> {
    find(state, &root(), "app", "orders", params)
}

fn rejected(filter: Value) -> FindParams {
    FindParams { filter: Some(filter), ..Default::default() }
}

fn pool_of(state: &SharedState) -> kimmy_query::budget::MemoryPool {
    state.aggregate_memory().pool().clone()
}

/// The bytes a refusal says the lease held: "the index range of the filter is N bytes".
fn held_in(err: &ApiError) -> usize {
    let tail = err.message.split("the index range of the filter is ").nth(1).expect(&err.message);
    tail.split(' ').next().unwrap().parse().unwrap()
}

#[test]
fn a_pass_of_n_keys_is_answered_and_one_of_n_plus_one_is_refused_at_the_limit_by_the_byte() {
    // A filter that rejects everything goes on in passes of 100, 200, 400 and 800 keys wanted.
    // Over 1,200 entries the last pass holds the 500 that are left, and that is the most any
    // holds; over 1,201 it holds 501. A limit of exactly 500 keys answers the first and
    // refuses the second at its 501st key.
    let limit = key_bytes(500);
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 1_200, limit, 64 << 20);
    assert_eq!((largest_pass(1_200), largest_pass(1_201)), (500, 501));
    let answer = find_of(&state, rejected(rejecting())).unwrap();
    assert_eq!(answer["count"], 0);
    assert_eq!(pool_of(&state).used(), 0, "the keys are given back when it is answered");

    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 1_201, limit, 64 << 20);
    let err = find_of(&state, rejected(rejecting())).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert_eq!(err.code, ErrorCode::AnswerTooLarge);
    assert_eq!(err.code.retry(), Retry::No);
    assert_eq!(held_in(&err), key_bytes(501), "refused at the key that passes the limit");
    let text = format!(
        "the index range of the filter is {} bytes, over the limit of {limit} bytes one find may \
         hold at once (aggregate.memory_limit_bytes); ",
        key_bytes(501)
    );
    assert!(err.message.starts_with(&text), "{}", err.message);
    // And says what to do: the remedies of this path, not an aggregation's.
    for remedy in [
        "narrow the filter on the indexed field",
        "sort by anything but {_id: 1} ({_id: -1} included)",
        "use a smaller skip, paging with `cursor`",
    ] {
        assert!(err.message.contains(remedy), "{}", err.message);
    }
    // The sort it offers is one the find accepts: its window is the sorted find's ceiling.
    let ceiling = format!("when skip + limit is at most {MAX_SORT_WINDOW}");
    assert!(err.message.contains(&ceiling), "{}", err.message);
    assert!(!err.message.contains("$match"), "{}", err.message);
    assert_eq!(pool_of(&state).used(), 0, "and given back when it is refused");
    assert_eq!((pool_of(&state).limit_refusals(), pool_of(&state).pool_refusals()), (1, 0));
}

#[test]
fn the_remedies_it_names_answer_the_same_range_under_the_same_limit() {
    // A limit of 100 keys against a range of 1,500: the unsorted find is refused. A sort on
    // another field reads the range in index order and holds only the page; a narrower range
    // holds fewer keys. Paging with a cursor is not one of them when the filter rejects: the
    // first page is the one that walks the range, and the message does not offer it for that.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 1_500, key_bytes(100), 64 << 20);
    assert!(find_of(&state, rejected(rejecting())).is_err());
    let sorted = FindParams { sort: Some(json!({ "t": 1 })), ..rejected(rejecting()) };
    assert_eq!(find_of(&state, sorted).unwrap()["count"], 0);
    let sorted_by_id_down =
        FindParams { sort: Some(json!({ "_id": -1 })), ..rejected(rejecting()) };
    assert_eq!(find_of(&state, sorted_by_id_down).unwrap()["count"], 0);
    let narrow = json!({ "t": { "$gte": 1_450 }, "missing": 1 });
    assert_eq!(find_of(&state, rejected(narrow)).unwrap()["count"], 0);
    assert_eq!(pool_of(&state).used(), 0);

    let after_cursor = FindParams {
        cursor: Some(
            kimmy_core::Cursor::from_key(
                kimmy_core::keyenc::encode(&bson::Bson::Int64(10)).unwrap(),
            )
            .encode(),
        ),
        ..rejected(rejecting())
    };
    let err = find_of(&state, after_cursor).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
}

#[test]
fn a_large_skip_holds_a_window_of_keys_and_a_cursor_in_its_place_does_not() {
    // `skip + limit` is the first pass's `want`, so a `skip` of 11,000 over a range every
    // document of which matches holds 11,010 keys; the page after a cursor holds ten. The
    // window is not charged up to what a page may hold (10,000 keys and the one a
    // replacement holds for a moment), and past that every key held is: the lease is made
    // at the 10,002nd key, charged for all of them, and refused at the 10,501st.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 12_000, key_bytes(10_500), 64 << 20);
    let all = json!({ "t": { "$gte": 0 } });
    let skipping = FindParams { skip: Some(11_000), limit: Some(10), ..rejected(all.clone()) };
    let err = find_of(&state, skipping).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert_eq!(held_in(&err), key_bytes(10_501));
    assert!(err.message.contains("use a smaller skip, paging with `cursor`"), "{}", err.message);
    assert_eq!(pool_of(&state).used(), 0);
    // A skip that fits what a page may hold is answered under the same limit, uncharged.
    let skipping = FindParams { skip: Some(9_000), limit: Some(10), ..rejected(all.clone()) };
    assert_eq!(find_of(&state, skipping).unwrap()["count"], 10);
    let key = kimmy_core::keyenc::encode(&bson::Bson::Int64(10_999)).unwrap();
    let paged = FindParams {
        cursor: Some(kimmy_core::Cursor::from_key(key).encode()),
        limit: Some(10),
        ..rejected(all)
    };
    let page = find_of(&state, paged).unwrap();
    assert_eq!(page["count"], 10);
    assert_eq!(pool_of(&state).used(), 0);
}

#[test]
fn what_a_find_holds_is_the_pool_and_a_full_pool_refuses_the_youngest() {
    // Another request holds all but 100 keys of a pool the find needs 1,500 of: the find is
    // the youngest holder, and is refused 503 (retry: wait) at once, with its own bytes
    // given back and the other's untouched. When the other ends, the same request is answered.
    let n = 1_500;
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, n as i64, key_bytes(n), key_bytes(n) + key_bytes(100));
    let pool = pool_of(&state);
    let other = state.aggregate_memory().limits();
    let mut held = other.buffer.lease("another request");
    held.charge(key_bytes(n)).unwrap();
    assert_eq!(pool.used(), key_bytes(n));

    let err = find_of(&state, rejected(rejecting())).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::SERVICE_UNAVAILABLE, "{}", err.message);
    assert_eq!(err.code, ErrorCode::Timeout);
    assert_eq!(err.code.retry(), Retry::Wait);
    assert_eq!(err.retry_after_secs, Some(crate::error::AGGREGATE_POOL_RETRY_AFTER_SECS));
    assert!(err.message.starts_with("the index range of the filter needed "), "{}", err.message);
    assert!(err.message.contains("aggregate.memory_pool_bytes"), "{}", err.message);
    assert!(
        err.message.contains("sort by anything but {_id: 1} when skip + limit"),
        "{}",
        err.message
    );
    assert!(!err.message.contains("$match"), "{}", err.message);
    assert_eq!(pool.used(), key_bytes(n), "only the other request's bytes are left");
    assert_eq!((pool.pool_refusals(), pool.limit_refusals()), (1, 0));

    drop(held);
    assert_eq!(pool.used(), 0);
    assert_eq!(find_of(&state, rejected(rejecting())).unwrap()["count"], 0);
    assert_eq!(pool.used(), 0);
}

/// Run `find` on a thread of its own, stopped by `key_hooks` once it holds `at` bytes of keys:
/// the thread says so on the first channel and goes on when it is sent something on the
/// second. The result of the find comes back when the thread is joined.
struct Gated {
    reached: mpsc::Receiver<()>,
    go: mpsc::Sender<()>,
    thread: std::thread::JoinHandle<Result<Value, ApiError>>,
}

fn gated(state: &SharedState, params: FindParams, at: usize) -> Gated {
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
        find_of(&state, params)
    });
    Gated { reached, go, thread }
}

#[test]
fn two_finds_share_the_pool_and_the_younger_is_refused_where_the_older_is_answered() {
    // A pool of 1.25 times what one find holds at its peak (800 keys over 1,500 entries). The
    // first find is stopped when it holds half of that, so it is the older holder; the
    // second, which starts then, takes what is left and is the youngest when the pool is
    // full: 503, wait. The first goes on, takes the rest, and is answered; the pool is empty
    // at the end.
    let (n, peak) = (1_500, largest_pass(1_500));
    assert_eq!(peak, 800);
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, n as i64, key_bytes(peak), key_bytes(peak) + key_bytes(peak) / 4);
    let pool = pool_of(&state);

    let first = gated(&state, rejected(rejecting()), key_bytes(peak / 2));
    first.reached.recv_timeout(Duration::from_secs(30)).unwrap();
    assert_eq!(pool.holders(), 1);
    let held_by_first = pool.used();
    assert!(held_by_first >= key_bytes(peak / 2));

    let second = find_of(&state, rejected(rejecting())).unwrap_err();
    assert_eq!(second.status, axum::http::StatusCode::SERVICE_UNAVAILABLE, "{}", second.message);
    assert_eq!(second.code.retry(), Retry::Wait);
    assert_eq!(pool.used(), held_by_first, "the refused find gave back all it held");

    first.go.send(()).unwrap();
    let answered = first.thread.join().unwrap().unwrap();
    assert_eq!(answered["count"], 0);
    assert_eq!(pool.used(), 0);
    assert_eq!((pool.pool_refusals(), pool.limit_refusals()), (1, 0));
}

#[test]
fn an_older_find_waits_for_a_younger_holder_and_is_answered_when_it_lets_go() {
    // The find holds half its keys and is stopped; another request takes the rest of the pool
    // and the find, which is the older, waits for it rather than being refused. It is still
    // waiting a moment later, and is answered when the other lets go.
    let (n, peak) = (1_500, largest_pass(1_500));
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, n as i64, key_bytes(peak), key_bytes(peak) + key_bytes(100));
    let pool = pool_of(&state);

    let find = gated(&state, rejected(rejecting()), key_bytes(peak / 2));
    find.reached.recv_timeout(Duration::from_secs(30)).unwrap();
    let other = state.aggregate_memory().limits();
    let mut young = other.buffer.lease("another request");
    young.charge(pool.cap() - pool.used()).unwrap();
    assert_eq!(pool.used(), pool.cap());

    find.go.send(()).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert!(!find.thread.is_finished(), "the older holder waits");
    assert_eq!(pool.pool_refusals(), 0);

    drop(young);
    let answered = find.thread.join().unwrap().unwrap();
    assert_eq!(answered["count"], 0);
    assert_eq!(pool.used(), 0);
    assert_eq!(pool.pool_refusals(), 0);
}

fn in_a_request<T>(left: Duration, work: impl FnOnce() -> T) -> T {
    tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(
        crate::limits::as_a_request(Instant::now() + left, Duration::from_secs(30), async {
            work()
        }),
    )
}

#[test]
fn a_find_that_waits_for_the_pool_waits_no_longer_than_the_request_has_left() {
    let (n, peak) = (1_500, largest_pass(1_500));
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, n as i64, key_bytes(peak), key_bytes(peak) + key_bytes(100));
    let pool = pool_of(&state);
    let (reached_tx, reached) = mpsc::channel();
    let (go, go_rx) = mpsc::channel::<()>();
    let thread = {
        let state = Arc::clone(&state);
        std::thread::spawn(move || {
            let mut gate = Some((reached_tx, go_rx));
            key_hooks::after_each_key(move |held| {
                if held >= key_bytes(peak / 2)
                    && let Some((reached, go)) = gate.take()
                {
                    reached.send(()).unwrap();
                    go.recv().unwrap();
                }
            });
            let started = Instant::now();
            let result =
                in_a_request(Duration::from_millis(400), || find_of(&state, rejected(rejecting())));
            (result, started.elapsed())
        })
    };
    reached.recv_timeout(Duration::from_secs(30)).unwrap();
    let other = state.aggregate_memory().limits();
    let mut young = other.buffer.lease("another request");
    young.charge(pool.cap() - pool.used()).unwrap();
    go.send(()).unwrap();
    // The other never lets go: the find is cut at its deadline, not left to wait the five
    // seconds a request without one would.
    let (result, took) = thread.join().unwrap();
    let err = result.unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::SERVICE_UNAVAILABLE, "{}", err.message);
    assert_eq!(err.code, ErrorCode::Timeout);
    assert!(took < Duration::from_secs(3), "{took:?}");
    assert_eq!(pool.used(), young.bytes(), "its keys are given back");
    drop(young);
    assert_eq!(pool.used(), 0);
}

#[test]
fn the_other_paths_of_a_find_take_no_lease_and_touch_no_pool() {
    // A request limit of nothing and a pool of nothing: a lease that charged a single byte
    // would be refused. Every other path of a `find` is answered, no key is held, and the
    // pool is never asked.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 300, 0, 0);
    let taken = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = Arc::clone(&taken);
    key_hooks::after_each_key(move |_| {
        counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    });
    let pool = pool_of(&state);
    let answer = |params: FindParams| find_of(&state, params).unwrap();
    let ids = |value: &Value| -> Vec<i64> {
        value["documents"].as_array().unwrap().iter().map(|d| d["_id"].as_i64().unwrap()).collect()
    };

    // An exact probe of one key (one run), and of several (merged runs).
    assert_eq!(ids(&answer(rejected(json!({ "t": 7 })))), [7]);
    let union = answer(rejected(json!({ "t": { "$in": [40, 20, 30] } })));
    assert_eq!(ids(&union), [20, 30, 40]);
    // An `_id` lookup, and a lookup of several.
    assert_eq!(ids(&answer(rejected(json!({ "_id": 5 })))), [5]);
    assert_eq!(ids(&answer(rejected(json!({ "_id": { "$in": [9, 3] } })))), [3, 9]);
    // A collection scan: no index applies to the field.
    let scan = answer(FindParams { explain: true, ..rejected(json!({ "w": 1 })) });
    assert_eq!(scan["count"], 0);
    assert_eq!(scan["explain"]["strategy"], "collectionScan");
    assert_eq!(answer(FindParams { limit: Some(3), ..Default::default() })["count"], 3);
    // An index scan in index order: a sort on another field takes the range as it is stored.
    let sorted = answer(FindParams {
        sort: Some(json!({ "t": -1 })),
        limit: Some(2),
        ..rejected(json!({ "t": { "$gte": 100 } }))
    });
    assert_eq!(ids(&sorted), [299, 298]);
    // A count walks in index order too.
    let counted =
        count(&state, &root(), "app", "orders", rejected(json!({ "t": { "$gte": 100 } })));
    assert_eq!(counted.unwrap()["count"], 200);

    assert_eq!(taken.load(std::sync::atomic::Ordering::SeqCst), 0, "no key was held");
    assert_eq!((pool.used(), pool.limit_refusals(), pool.pool_refusals()), (0, 0, 0));
    assert_eq!(pool.holders(), 0);

    // The path that does hold keys is refused under the same limit, at the key that passes
    // its window, so the test is not vacuous.
    let err = find_of(&state, rejected(json!({ "t": { "$gte": 0 }, "missing": 1 }))).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert_eq!(held_in(&err), key_bytes(102));
    assert_eq!(pool.limit_refusals(), 1);
}

#[test]
fn a_find_that_ends_early_or_fails_gives_its_keys_back() {
    let n = 1_500;
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, n, key_bytes(n as usize), 64 << 20);
    let pool = pool_of(&state);
    let all = json!({ "t": { "$gte": 0 } });

    // Answered after a full window: the first pass offers its keys and ends.
    let found = find_of(&state, FindParams { limit: Some(5), ..rejected(all.clone()) });
    assert_eq!(found.unwrap()["count"], 5);
    assert_eq!(pool.used(), 0);

    // A recheck that fails on a candidate, with a pass of keys held and charged: the first
    // 300 are rejected, in passes of 100 and 200, and the 301st fails in the pass of 400.
    let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (seen, watched) = (Arc::clone(&peak), pool.clone());
    key_hooks::after_each_key(move |_| {
        seen.fetch_max(watched.used(), std::sync::atomic::Ordering::SeqCst);
    });
    let divide = json!({ "$cond": [{ "$gte": ["$t", 300] }, { "$divide": ["$t", 0] }, false] });
    let failing = rejected(json!({ "t": { "$gte": 0 }, "$expr": divide }));
    let err = find_of(&state, failing).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST, "{}", err.message);
    assert!(peak.load(std::sync::atomic::Ordering::SeqCst) >= key_bytes(300), "charged");
    assert_eq!(pool.used(), 0);

    // A projection that fails on a document of the page.
    let projecting =
        FindParams { projection: Some(json!({ "t": { "$slice": 1 } })), ..rejected(all.clone()) };
    let _ = find_of(&state, projecting);
    assert_eq!(pool.used(), 0);

    // A panic in the middle of the walk, with keys held.
    let held_when_it_panicked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = Arc::clone(&held_when_it_panicked);
    key_hooks::after_each_key(move |held| {
        if held >= key_bytes(300) {
            seen.store(held, std::sync::atomic::Ordering::SeqCst);
            panic!("a panic with keys held");
        }
    });
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = find_of(&state, rejected(rejecting()));
    }));
    assert!(panicked.is_err());
    assert!(held_when_it_panicked.load(std::sync::atomic::Ordering::SeqCst) >= key_bytes(300));
    assert_eq!(pool.used(), 0, "a panic gives them back too");
    assert_eq!(pool.holders(), 0);
}

#[test]
fn a_find_over_a_range_is_cut_at_the_deadline_and_the_walk_ticks_for_each_entry() {
    let (small, big) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (a, b) =
        (indexed(&small, 100, 64 << 20, 64 << 20), indexed(&big, 1_100, 64 << 20, 64 << 20));
    // A window over the range, so one pass reads it all and holds its keys.
    let params = || FindParams { limit: Some(10_000), ..rejected(rejecting()) };
    let looks = |state: &SharedState| {
        kimmy_core::deadline::looks_of(|| {
            in_a_request(Duration::from_secs(3600), || find_of(state, params()))
        })
        .1
    };
    let diff = looks(&b) - looks(&a);
    assert_eq!(diff, 1_000, "{diff} looks for a thousand more entries");
    let late = in_a_request(Duration::ZERO, || find_of(&b, params()));
    let err = late.unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::SERVICE_UNAVAILABLE, "{}", err.message);
    assert_eq!(err.code, ErrorCode::Timeout);
    assert_eq!(pool_of(&b).used(), 0);
}

#[test]
fn the_pool_peak_of_a_find_over_a_range_is_the_keys_of_its_largest_pass() {
    // The measure the operations notes quote: the bytes the pool shows while a find that
    // rejects everything walks a range of 20,000 entries, in passes of 100, 200, ... 12,800
    // wanted, the last of which holds the 7,300 that are left.
    let n = 20_000;
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, n, 64 << 20, 64 << 20);
    let pool = pool_of(&state);
    let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (seen, watched) = (Arc::clone(&peak), pool.clone());
    key_hooks::after_each_key(move |_| {
        seen.fetch_max(watched.used(), std::sync::atomic::Ordering::SeqCst);
    });
    assert_eq!(find_of(&state, rejected(rejecting())).unwrap()["count"], 0);
    let peak = peak.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(peak, key_bytes(7_300), "{peak} bytes for {n} entries");
    assert_eq!(largest_pass(n as usize), 7_300);
    assert_eq!(pool.used(), 0);
}

#[test]
fn a_find_whose_keys_stay_within_its_window_is_answered_with_the_pool_full() {
    // The commonest range query is a small page: `find {t: {$gte: x}} limit 20`. Its keys are
    // the size of its page and are not charged, so it does not ask the pool, and a pool that
    // aggregations have filled does not refuse it.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 1_500, 0, 0);
    let pool = pool_of(&state);
    let all = json!({ "t": { "$gte": 0 } });
    // Accepting what it reads: 20 keys for a page of 20.
    let page = find_of(&state, FindParams { limit: Some(20), ..rejected(all.clone()) });
    assert_eq!(page.unwrap()["count"], 20);
    // Rejecting everything of a range that fits the window (the default 100).
    let narrow = json!({ "t": { "$gte": 1_450 }, "missing": 1 });
    assert_eq!(find_of(&state, rejected(narrow)).unwrap()["count"], 0);
    // The largest window a page may have, over a range that fits it.
    let wide = FindParams { limit: Some(MAX_LIMIT), ..rejected(all.clone()) };
    assert_eq!(find_of(&state, wide).unwrap()["count"], 1_500);
    assert_eq!((pool.limit_refusals(), pool.pool_refusals(), pool.holders()), (0, 0, 0));

    // The same with a pool another request has filled: it is answered and takes nothing.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 1_500, 64 << 20, 1 << 20);
    let pool = pool_of(&state);
    let other = state.aggregate_memory().limits();
    let mut held = other.buffer.lease("another request");
    held.charge(pool.cap()).unwrap();
    let page = find_of(&state, FindParams { limit: Some(20), ..rejected(all.clone()) });
    assert_eq!(page.unwrap()["count"], 20);
    assert_eq!((pool.used(), pool.pool_refusals(), pool.holders()), (pool.cap(), 0, 1));

    // A pass that goes past the window does take the lease: a rejecting filter over the whole
    // range wants 200 keys in its second pass, and the full pool refuses it there.
    let err = find_of(&state, rejected(rejecting())).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::SERVICE_UNAVAILABLE, "{}", err.message);
    assert_eq!(pool.pool_refusals(), 1);
    drop(held);
    assert_eq!(pool.used(), 0);
}

#[test]
fn the_first_key_past_the_window_makes_the_lease_and_is_charged_with_the_ones_before_it() {
    // A window of 100 holds 101 keys unpooled. The 102nd, in the second pass, makes the lease
    // and charges all 102; a limit of 101 keys refuses it there, and one of 102 does not.
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 300, key_bytes(101), 64 << 20);
    // What the pool shows while the keys held are 101 or fewer.
    let within = Arc::new(AtomicUsize::new(0));
    let (seen, watched) = (within.clone(), pool_of(&state));
    key_hooks::after_each_key(move |held| {
        if held <= key_bytes(101) {
            seen.fetch_max(watched.used(), Ordering::SeqCst);
        }
    });
    let err = find_of(&state, rejected(rejecting())).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert_eq!(held_in(&err), key_bytes(102));
    assert_eq!(within.load(Ordering::SeqCst), 0, "the pool is not asked within the window");
    assert_eq!(pool_of(&state).used(), 0);
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 300, key_bytes(200), 64 << 20);
    assert_eq!(find_of(&state, rejected(rejecting())).unwrap()["count"], 0);
}

#[test]
fn a_find_sorted_by_id_ascending_takes_the_hold_as_one_with_no_sort_does() {
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 1_500, key_bytes(100), 64 << 20);
    let by_id = FindParams { sort: Some(json!({ "_id": 1 })), ..rejected(rejecting()) };
    let err = find_of(&state, by_id).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert!(err.message.starts_with("the index range of the filter is "), "{}", err.message);
}

/// `n` documents `{_id: i, t: i * 7919 % n}` under an index on `t`, so the range arrives in `t`
/// order and the `_id`s in it are scattered, as they are when the indexed field does not
/// follow `_id`: a pass that is full meets keys smaller than the largest it holds.
fn scattered(dir: &tempfile::TempDir, n: i64, limit: usize) -> SharedState {
    let state = live_state(dir);
    state.set_aggregate_memory(AggregateMemory::new(limit, 64 << 20));
    state.engine.create_collection("app", "orders").unwrap();
    let index = vec![kimmy_storage::IndexField::ascending("t")];
    state.engine.create_index("app", "orders", index, false, None).unwrap();
    let meta = state.engine.get_collection("app", "orders").unwrap();
    let docs: Vec<Document> = (0..n).map(|i| doc! { "_id": i, "t": i * 7919 % n }).collect();
    state.engine.insert_many(&meta, docs).unwrap();
    state
}

#[test]
fn a_full_pass_that_meets_smaller_keys_holds_one_more_than_it_keeps_for_a_moment() {
    // Over 2,000 entries the fourth pass wants 800 and is full, and a smaller key takes the
    // place of the largest, which is held beside it until it is put back: 801 keys at the
    // most, and the walk charges them. A limit of 800 keys refuses at the 801st; 801 answers.
    let dir = tempfile::tempdir().unwrap();
    let state = scattered(&dir, 2_000, key_bytes(800));
    let err = find_of(&state, rejected(rejecting())).unwrap_err();
    assert_eq!(err.status, axum::http::StatusCode::PAYLOAD_TOO_LARGE, "{}", err.message);
    assert_eq!(held_in(&err), key_bytes(801));
    assert_eq!(pool_of(&state).used(), 0);
    let dir = tempfile::tempdir().unwrap();
    let state = scattered(&dir, 2_000, key_bytes(801));
    assert_eq!(find_of(&state, rejected(rejecting())).unwrap()["count"], 0);
    assert_eq!(pool_of(&state).used(), 0);
}

#[test]
fn the_sort_the_refusal_offers_is_one_a_find_accepts() {
    // The text offers a sort "when skip + limit is at most {MAX_SORT_WINDOW}", and the sorted
    // find refuses a window over that: the two agree.
    assert!(FIND_RANGE_KEYS.over_limit.contains(&format!("at most {MAX_SORT_WINDOW},")));
    assert!(FIND_RANGE_KEYS.pool_busy.contains(&format!("at most {MAX_SORT_WINDOW},")));
    let dir = tempfile::tempdir().unwrap();
    let state = indexed(&dir, 10, 64 << 20, 64 << 20);
    let sorted = |limit| FindParams {
        sort: Some(json!({ "_id": -1 })),
        limit: Some(limit),
        ..Default::default()
    };
    assert!(find_of(&state, sorted(MAX_SORT_WINDOW)).is_ok());
    assert!(find_of(&state, FindParams { skip: Some(1), ..sorted(MAX_SORT_WINDOW) }).is_err());
}
