//! The request's memory budget (ADR-269): what each stage charges, when it is refused,
//! and the pool the budgets draw on.

use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use bson::{Bson, Document, doc};
use kimmy_core::Error;

use crate::aggregate::{self, Limits};
use crate::budget::{
    BufferBudget, DEFAULT_MEMORY_LIMIT_BYTES, DEFAULT_MEMORY_POOL_BYTES, MemoryPool, doc_bytes,
    docs_bytes, held_bytes, held_bytes_bson, held_docs_bytes,
};

fn capped(cap: usize) -> Limits {
    Limits { buffer: BufferBudget::new(cap), ..Default::default() }
}

fn pooled(cap: usize, pool: &MemoryPool) -> Limits {
    Limits { buffer: BufferBudget::pooled(cap, pool), ..Default::default() }
}

/// `n` documents holding a string of `pad` bytes each.
fn padded(n: usize, pad: usize) -> Vec<Document> {
    (0..n as i32).map(|i| doc! {"_id": i, "g": i % 3, "pad": "x".repeat(pad)}).collect()
}

fn run(
    stage: Document,
    input: Vec<Document>,
    limits: &Limits,
) -> kimmy_core::Result<Vec<Document>> {
    let stage = aggregate::parse(&[stage])?.remove(0);
    aggregate::apply(&stage, input, limits)
}

/// The smallest cap under which the stage runs, found by halving; the stage must run
/// under `hi`.
fn smallest_cap(stage: &Document, input: &[Document], mut hi: usize) -> usize {
    let mut lo = 0usize;
    assert!(run(stage.clone(), input.to_vec(), &capped(hi)).is_ok(), "must run under {hi}");
    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        if run(stage.clone(), input.to_vec(), &capped(mid)).is_ok() {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    hi
}

fn is_too_large(err: &Error) -> bool {
    matches!(err, Error::ValueTooLarge { .. })
}

// ---------------------------------------------------------------------------
// The lease: charge, release, refusal
// ---------------------------------------------------------------------------

#[test]
fn a_charge_over_the_cap_is_refused_at_the_byte_and_named() {
    let budget = BufferBudget::new(100);
    let mut lease = budget.lease("the source read");
    lease.charge(100).unwrap();
    let err = lease.charge(1).unwrap_err();
    let Error::ValueTooLarge { what, bytes, max, remedy, .. } = &err else { panic!("{err:?}") };
    assert_eq!((*what, *bytes, *max), ("the source read", 101, 100));
    assert!(remedy.contains("a leading $match narrows what is read"), "{remedy}");
    assert!(remedy.contains("$project, $limit or $match after it reduce what later stages hold"));
    assert!(!remedy.contains("early"), "{remedy}");
    assert!(remedy.contains("raise aggregate.memory_limit_bytes"), "{remedy}");
    let text = err.to_string();
    assert!(text.contains("the source read is 101 bytes"), "{text}");
    assert!(text.contains("limit of 100 bytes"), "{text}");
    assert!(text.contains("aggregate.memory_limit_bytes"), "{text}");
}

#[test]
fn leases_give_their_bytes_back_when_released_and_when_dropped() {
    let budget = BufferBudget::new(1_000);
    let mut a = budget.lease("a");
    let mut b = budget.lease("b");
    a.charge(300).unwrap();
    b.charge(200).unwrap();
    assert_eq!(budget.used(), 500);
    a.release(100);
    assert_eq!((budget.used(), a.bytes()), (400, 200));
    // Releasing more than the lease holds gives back what it holds and no more.
    a.release(10_000);
    assert_eq!((budget.used(), a.bytes()), (200, 0));
    drop(b);
    assert_eq!(budget.used(), 0);
    // A refused charge stays on its lease and goes back when the lease is dropped.
    let mut c = budget.lease("c");
    assert!(c.charge(2_000).is_err());
    assert_eq!(budget.used(), 2_000);
    drop(c);
    assert_eq!(budget.used(), 0);
}

#[test]
fn resize_charges_growth_and_releases_shrinkage() {
    let budget = BufferBudget::new(1_000);
    let mut lease = budget.lease("x");
    lease.resize(0, 400).unwrap();
    lease.resize(400, 150).unwrap();
    assert_eq!(budget.used(), 150);
    assert!(lease.resize(150, 1_001).is_err());
}

#[test]
fn a_documents_size_is_what_it_encodes_to() {
    let d = doc! {"a": 1, "s": "hello", "n": {"x": [1, 2, 3], "y": Bson::Null}, "t": true};
    assert_eq!(doc_bytes(&d), bson::serialize_to_vec(&d).unwrap().len());
    let docs = vec![d.clone(), d.clone()];
    assert_eq!(docs_bytes(&docs), 2 * doc_bytes(&d));
}

// ---------------------------------------------------------------------------
// The pool
// ---------------------------------------------------------------------------

#[test]
fn a_budget_reserves_what_it_holds_from_the_pool_and_returns_it() {
    let pool = MemoryPool::new(100);
    let budget = BufferBudget::pooled(100, &pool);
    let mut lease = budget.lease("a");
    lease.charge(60).unwrap();
    assert_eq!((pool.used(), budget.reserved()), (60, 60));
    lease.release(20);
    assert_eq!((pool.used(), budget.reserved()), (40, 40));
    drop(lease);
    assert_eq!((pool.used(), budget.reserved()), (0, 0));
}

#[test]
fn a_request_the_pool_cannot_cover_is_refused_with_a_retry_and_reserves_nothing() {
    let pool = MemoryPool::new(100);
    let first = BufferBudget::pooled(100, &pool);
    let mut held = first.lease("first");
    held.charge(60).unwrap();

    let second = BufferBudget::pooled(100, &pool);
    let mut lease = second.lease("the source read");
    let err = lease.charge(50).unwrap_err();
    assert!(
        matches!(
            &err,
            Error::MemoryBusy { what: "the source read", bytes: 50, pool: 100, in_use: 60 }
        ),
        "{err:?}"
    );
    let text = err.to_string();
    assert!(text.contains("aggregate.memory_pool_bytes"), "{text}");
    assert!(text.contains("retry later"), "{text}");
    // `in_use` is the pool's total, the refused request's own reservation included, and the
    // message says so rather than calling it what other requests hold.
    assert!(text.contains("60 reserved now by all requests, this one's included"), "{text}");
    assert!(!text.contains("other requests hold"), "{text}");
    assert_eq!((pool.pool_refusals(), pool.limit_refusals()), (1, 0));
    // The refused bytes were not reserved, and the first request still holds its own.
    assert_eq!((second.reserved(), pool.used()), (0, 60));
    drop(lease);
    assert_eq!(pool.used(), 60);
    // When the first ends the same charge is covered.
    drop(held);
    assert_eq!(pool.used(), 0);
    let mut again = second.lease("the source read");
    again.charge(50).unwrap();
    assert_eq!(pool.used(), 50);
}

#[test]
fn the_pool_total_in_a_refusal_includes_what_the_refused_request_holds() {
    let pool = MemoryPool::new(100);
    let other = BufferBudget::pooled(100, &pool);
    let mut theirs = other.lease("other");
    theirs.charge(40).unwrap();
    let mine = BufferBudget::pooled(100, &pool);
    let mut lease = mine.lease("the input of $sort");
    lease.charge(30).unwrap();
    // Another 50 does not fit: 70 are reserved, 30 of them this request's own.
    let err = lease.charge(50).unwrap_err();
    assert!(matches!(&err, Error::MemoryBusy { bytes: 50, pool: 100, in_use: 70, .. }), "{err:?}");
}

#[test]
fn a_request_over_its_own_limit_is_counted_apart_and_takes_nothing_from_the_pool() {
    let pool = MemoryPool::new(1_000);
    let budget = BufferBudget::pooled(10, &pool);
    let mut lease = budget.lease("the answer");
    lease.charge(10).unwrap();
    assert!(is_too_large(&lease.charge(1).unwrap_err()));
    assert_eq!((pool.limit_refusals(), pool.pool_refusals()), (1, 0));
    assert_eq!((pool.used(), budget.reserved()), (10, 10));
    drop(lease);
    assert_eq!(pool.used(), 0);
}

#[test]
fn the_pool_gets_everything_back_when_a_request_fails_or_panics() {
    let pool = MemoryPool::new(1_000_000);
    // A stage that fails with the budget partly charged.
    let limits = pooled(1_000_000, &pool);
    let err =
        run(doc! {"$addFields": {"x": {"$divide": [1, 0]}}}, padded(3, 100), &limits).unwrap_err();
    assert!(matches!(err, Error::InvalidQuery(_)), "{err:?}");
    assert_eq!((limits.buffer.used(), pool.used()), (0, 0));
    // One refused by its limit.
    let limits = pooled(300, &pool);
    assert!(is_too_large(
        &run(doc! {"$addFields": {"x": 1}}, padded(3, 100), &limits).unwrap_err()
    ));
    assert_eq!((limits.buffer.used(), pool.used()), (0, 0));
    // One that panics with a lease held: the unwinding drops it.
    let budget = BufferBudget::pooled(1_000_000, &pool);
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut lease = budget.lease("x");
        lease.charge(500).unwrap();
        assert_eq!(pool.used(), 500);
        panic!("a stage panicked");
    }));
    assert!(unwound.is_err());
    assert_eq!(pool.used(), 0);
}

#[test]
fn two_requests_cannot_both_take_the_last_of_the_pool() {
    for _ in 0..50 {
        let pool = MemoryPool::new(100);
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let (pool, barrier) = (pool.clone(), Arc::clone(&barrier));
                std::thread::spawn(move || {
                    let budget = BufferBudget::pooled(100, &pool);
                    // Whichever loses may be the older, which would wait: not for this.
                    budget.wait_until(Instant::now());
                    let mut lease = budget.lease("x");
                    barrier.wait();
                    let won = lease.charge(60).is_ok();
                    // Hold the bytes until both have tried.
                    barrier.wait();
                    won
                })
            })
            .collect();
        let won: Vec<bool> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(won.iter().filter(|w| **w).count(), 1, "exactly one takes it: {won:?}");
        assert_eq!(pool.used(), 0, "and both give it back");
        assert_eq!(pool.pool_refusals(), 1);
    }
}

// ---------------------------------------------------------------------------
// The pool is granted oldest first (ADR-269, the review's m6)
// ---------------------------------------------------------------------------

/// A request on `pool` that holds `bytes` now: its budget and the lease that holds them.
fn holder(pool: &MemoryPool, bytes: usize) -> (BufferBudget, crate::budget::Lease) {
    let budget = BufferBudget::pooled(pool.cap(), pool);
    let mut lease = budget.lease("a request");
    lease.charge(bytes).unwrap();
    (budget, lease)
}

fn is_busy(err: &Error) -> bool {
    matches!(err, Error::MemoryBusy { .. })
}

#[test]
fn an_older_request_waits_for_the_pool_and_is_woken_by_a_release() {
    let pool = MemoryPool::new(100);
    let (_b, mut older) = holder(&pool, 10);
    let (_a, younger) = holder(&pool, 60);
    // The older asks for 50 more: the pool has 30. It waits, and the younger's release wakes it.
    let started = Instant::now();
    let waiter = std::thread::spawn(move || {
        let result = older.charge(50);
        (result, older, started.elapsed())
    });
    std::thread::sleep(Duration::from_millis(150));
    assert!(!waiter.is_finished(), "the older request is waiting, not refused");
    assert_eq!(pool.pool_refusals(), 0);
    drop(younger);
    let (result, older, waited) = waiter.join().unwrap();
    result.unwrap();
    assert!(waited >= Duration::from_millis(100), "it waited for the release: {waited:?}");
    assert_eq!(pool.used(), 60, "its 10 and the 50 it waited for");
    assert_eq!((pool.pool_refusals(), pool.holders()), (0, 1));
    drop(older);
    assert_eq!((pool.used(), pool.holders()), (0, 0));
}

#[test]
fn the_younger_request_is_refused_while_the_older_waits_and_the_older_then_completes() {
    let pool = MemoryPool::new(100);
    let (_b, mut older) = holder(&pool, 10);
    let (_a, mut younger) = holder(&pool, 60);
    let waiter = std::thread::spawn(move || {
        let result = older.charge(50);
        (result, older)
    });
    std::thread::sleep(Duration::from_millis(100));
    // The younger finds the pool full too and is the one refused, at once; dropping its lease
    // (the stage unwinding) gives its 60 back, which is what the older was waiting for.
    let started = Instant::now();
    let err = younger.charge(40).unwrap_err();
    assert!(is_busy(&err), "{err:?}");
    assert!(started.elapsed() < Duration::from_millis(50), "refused at once");
    drop(younger);
    let (result, older) = waiter.join().unwrap();
    result.unwrap();
    assert_eq!(pool.used(), 60);
    assert_eq!(pool.pool_refusals(), 1, "only the refusal is counted, not the wait");
    drop(older);
    assert_eq!((pool.used(), pool.holders()), (0, 0));
}

#[test]
fn a_waiting_request_is_ended_by_its_deadline_and_the_pool_comes_back() {
    let pool = MemoryPool::new(100);
    let (budget, mut older) = holder(&pool, 10);
    let (_a, younger) = holder(&pool, 80);
    budget.wait_until(Instant::now() + Duration::from_millis(120));
    let started = Instant::now();
    let err = older.charge(50).unwrap_err();
    let waited = started.elapsed();
    assert!(is_busy(&err), "{err:?}: the deadline's 503");
    assert!(waited >= Duration::from_millis(100) && waited < Duration::from_secs(2), "{waited:?}");
    assert_eq!(pool.pool_refusals(), 1);
    drop(older);
    drop(budget);
    assert_eq!(pool.used(), 80, "only the younger holds now");
    drop(younger);
    assert_eq!((pool.used(), pool.holders()), (0, 0));
}

#[test]
fn a_waiting_request_is_ended_by_the_deadline_scope_it_runs_under() {
    // The wait is a synchronous sleep, so the request's deadline fires inside it: under a
    // scope of 80 ms the older request's wait ends there with the deadline's own error, not
    // the pool's refusal and not at the (much later) bound of its own budget.
    let pool = MemoryPool::new(100);
    let (budget, mut older) = holder(&pool, 10);
    budget.wait_until(Instant::now() + Duration::from_secs(30));
    let (_a, younger) = holder(&pool, 80);
    let started = Instant::now();
    let err = {
        let _scope = kimmy_core::deadline::Deadline::scope(Some(Duration::from_millis(80)));
        older.charge(50).unwrap_err()
    };
    let waited = started.elapsed();
    assert!(matches!(err, Error::DeadlineExceeded), "{err:?}");
    assert!(waited >= Duration::from_millis(60) && waited < Duration::from_secs(5), "{waited:?}");
    assert_eq!(pool.pool_refusals(), 0, "a deadline is not a refusal of the pool");
    drop(older);
    drop(budget);
    drop(younger);
    assert_eq!((pool.used(), pool.holders()), (0, 0));
    // With no scope the wait runs to the bound of its own budget, as before.
    let (budget, mut older) = holder(&pool, 10);
    budget.wait_until(Instant::now() + Duration::from_millis(80));
    let (_a, _younger) = holder(&pool, 80);
    assert!(is_busy(&older.charge(50).unwrap_err()));
}

#[test]
fn a_wait_that_reaches_the_request_deadline_is_the_deadlines_answer_not_the_pools() {
    // `AggregateMemory::limits` sets the budget's own bound to the request's deadline, so the
    // two coincide: the bound is the deadline's, and must not be tested first.
    for lead in [Duration::from_millis(100), Duration::ZERO] {
        let pool = MemoryPool::new(100);
        let (budget, mut older) = holder(&pool, 10);
        let (_a, younger) = holder(&pool, 80);
        let end = Instant::now() + lead;
        budget.wait_until(end);
        let scope = kimmy_core::deadline::Deadline::scope(Some(lead));
        let err = older.charge(50).unwrap_err();
        drop(scope);
        assert!(matches!(err, Error::DeadlineExceeded), "lead {lead:?}: {err:?}");
        assert_eq!(pool.pool_refusals(), 0, "the deadline is not a refusal of the pool");
        drop(older);
        drop(budget);
        drop(younger);
        assert_eq!((pool.used(), pool.holders()), (0, 0));
    }
    // The bound of the budget with no deadline in scope is still the pool's own refusal.
    let pool = MemoryPool::new(100);
    let (budget, mut older) = holder(&pool, 10);
    budget.wait_until(Instant::now() + Duration::from_millis(60));
    let (_a, _younger) = holder(&pool, 80);
    assert!(is_busy(&older.charge(50).unwrap_err()));
    assert_eq!(pool.pool_refusals(), 1);
}

#[test]
fn a_request_that_alone_would_pass_the_whole_pool_is_refused_at_once() {
    let pool = MemoryPool::new(100);
    let budget = BufferBudget::pooled(1_000, &pool);
    budget.wait_until(Instant::now() + Duration::from_secs(30));
    let mut lease = budget.lease("a request");
    lease.charge(10).unwrap();
    // A younger request holds bytes, so this one, the older, would wait for the pool...
    let (_other, _held) = holder(&pool, 40);
    let started = Instant::now();
    // ...but 150 bytes would pass the whole pool of 100 whatever is freed: refused at once.
    let err = lease.charge(150).unwrap_err();
    assert!(is_busy(&err), "{err:?}");
    assert!(started.elapsed() < Duration::from_millis(500), "waiting cannot help it");
    assert_eq!(pool.pool_refusals(), 1);
}

#[test]
fn the_order_is_cleaned_when_a_request_errors_drops_or_panics() {
    let pool = MemoryPool::new(100);
    // Dropped.
    let (budget, lease) = holder(&pool, 10);
    assert_eq!(pool.holders(), 1);
    drop(lease);
    assert_eq!(pool.holders(), 0, "a lease that gave everything back is no holder");
    drop(budget);
    // Refused: the stage unwinds with its leases.
    let (_other, _held) = holder(&pool, 80);
    {
        let budget = BufferBudget::pooled(100, &pool);
        let mut lease = budget.lease("refused");
        assert!(is_busy(&lease.charge(50).unwrap_err()));
        assert_eq!(pool.holders(), 1, "the refused one never became a holder");
    }
    assert_eq!((pool.used(), pool.holders()), (80, 1));
    // Panic with a lease held: the unwinding drops it.
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let (_budget, _lease) = holder(&pool, 10);
        assert_eq!(pool.holders(), 2);
        panic!("a stage panicked");
    }));
    assert!(unwound.is_err());
    assert_eq!((pool.used(), pool.holders()), (80, 1));
    // A partial release leaves the holder; releasing all of it does not.
    let budget = BufferBudget::pooled(100, &pool);
    let mut lease = budget.lease("partial");
    lease.charge(10).unwrap();
    lease.release(4);
    assert_eq!(pool.holders(), 2);
    lease.release(6);
    assert_eq!(pool.holders(), 1);
}

#[test]
fn three_requests_ramping_together_two_are_answered_and_the_youngest_is_refused() {
    // The review's m6: three equal requests ramping up together against a pool that holds two
    // all got `503`, each refused while holding a share. A pool of 900, each request going to
    // 400 in steps of 100 with a barrier at each step: at the last step the pool is full, the
    // youngest is refused and gives its 300 back, and the two older ones are answered.
    let pool = MemoryPool::new(900);
    let budgets: Vec<_> = (0..3).map(|_| BufferBudget::pooled(900, &pool)).collect();
    let mut leases: Vec<_> = budgets.iter().map(|b| b.lease("a request")).collect();
    // The first step, in order, so the requests are born in the order they are numbered.
    for lease in &mut leases {
        lease.charge(100).unwrap();
    }
    let barrier = Arc::new(Barrier::new(3));
    let handles: Vec<_> = leases
        .into_iter()
        .map(|mut lease| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                for _ in 0..3 {
                    barrier.wait();
                    lease.charge(100)?;
                }
                // Answered: the bytes are held until the test has looked at the pool.
                Ok(lease)
            })
        })
        .collect();
    let mut results: Vec<Result<crate::budget::Lease, Error>> =
        handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(
        results.iter().map(|r| r.is_ok()).collect::<Vec<_>>(),
        [true, true, false],
        "{results:?}"
    );
    assert!(matches!(&results[2], Err(Error::MemoryBusy { .. })));
    assert_eq!(results[0].as_ref().unwrap().bytes(), 400);
    assert_eq!((pool.used(), pool.holders(), pool.pool_refusals()), (800, 2, 1));
    results.clear();
    assert_eq!((pool.used(), pool.holders()), (0, 0));
}

// ---------------------------------------------------------------------------
// What each stage charges
// ---------------------------------------------------------------------------

#[test]
fn a_stage_that_builds_documents_holds_its_input_and_its_output_at_once() {
    let input = padded(20, 200);
    let held_in = held_docs_bytes(&input);
    for stage in [
        doc! {"$addFields": {"extra": "$pad"}},
        doc! {"$set": {"extra": "$pad"}},
        doc! {"$project": {"p": "$pad", "g": 1}},
        doc! {"$replaceRoot": {"newRoot": {"p": "$pad"}}},
        doc! {"$replaceWith": {"p": "$pad"}},
    ] {
        let out = run(stage.clone(), input.clone(), &capped(usize::MAX / 4)).unwrap();
        let held_out = held_docs_bytes(&out);
        // The stage runs with exactly its input and its output, and not a byte fewer.
        let limits = capped(held_in + held_out);
        assert_eq!(run(stage.clone(), input.clone(), &limits).unwrap(), out, "{stage}");
        assert_eq!(limits.buffer.used(), 0, "{stage}: gives everything back");
        let err = run(stage.clone(), input.clone(), &capped(held_in + held_out - 1)).unwrap_err();
        let Error::ValueTooLarge { what, bytes, .. } = &err else { panic!("{stage}: {err:?}") };
        assert_eq!(*bytes, held_in + held_out, "{stage}: refused at the last byte");
        assert!(what.starts_with("the output of $"), "{stage}: {what}");
    }
}

#[test]
fn a_stage_that_only_moves_its_input_holds_the_input_and_builds_nothing() {
    let input = padded(20, 200);
    let held_in = held_docs_bytes(&input);
    for stage in [
        doc! {"$match": {"g": 1}},
        doc! {"$sort": {"g": -1}},
        doc! {"$limit": 5},
        doc! {"$skip": 5},
        doc! {"$count": "n"},
        doc! {"$sample": {"size": 4}},
    ] {
        let limits = capped(held_in);
        assert!(run(stage.clone(), input.clone(), &limits).is_ok(), "{stage}");
        assert_eq!(limits.buffer.used(), 0, "{stage}");
        let err = run(stage.clone(), input.clone(), &capped(held_in - 1)).unwrap_err();
        let Error::ValueTooLarge { what, bytes, .. } = &err else { panic!("{stage}: {err:?}") };
        assert_eq!(*bytes, held_in, "{stage}");
        assert!(what.starts_with("the input of $"), "{stage}: {what}");
    }
}

#[test]
fn unwind_charges_each_copy_it_makes() {
    let input: Vec<Document> =
        (0..10).map(|i| doc! {"_id": i, "tags": vec!["aaaaaaaaaaaaaaaaaaaaaaaa"; 8]}).collect();
    let held_in = held_docs_bytes(&input);
    let stage = doc! {"$unwind": "$tags"};
    let out = run(stage.clone(), input.clone(), &capped(usize::MAX / 4)).unwrap();
    assert_eq!(out.len(), 80);
    let both = held_in + held_docs_bytes(&out);
    assert!(run(stage.clone(), input.clone(), &capped(both)).is_ok());
    let err = run(stage, input, &capped(both - 1)).unwrap_err();
    assert!(
        matches!(&err, Error::ValueTooLarge { what, bytes, .. }
            if what.starts_with("the output of $unwind") && *bytes == both),
        "{err:?}"
    );
}

#[test]
fn group_holds_its_buckets_and_what_push_collects_beside_its_input() {
    let input = padded(30, 300);
    let held_in = held_docs_bytes(&input);
    let stage = doc! {"$group": {"_id": "$g", "all": {"$push": "$pad"}}};
    // The pushed values are most of what the buckets hold: the estimate of a 300-byte string
    // in its slot each, and a little for each of the three buckets.
    let pushed = 30 * held_bytes_bson(&Bson::String("x".repeat(300)));
    let smallest = smallest_cap(&stage, &input, held_in + 4 * pushed);
    assert!(smallest >= held_in + pushed, "the pushed values are charged: {smallest}");
    assert!(smallest <= held_in + pushed + 3 * 1_200, "and little else is: {smallest}");
    // Refused at the value that passes the limit, naming the buckets.
    let err = run(stage, input, &capped(smallest - 1)).unwrap_err();
    assert!(
        matches!(&err, Error::ValueTooLarge { what, .. } if what.starts_with("the buckets of $group")),
        "{err:?}"
    );
}

#[test]
fn the_accumulators_that_keep_values_are_charged_as_they_keep_them() {
    // One group, so the state is the whole of what is kept beyond the input.
    let input = padded(50, 400);
    let held_in = held_docs_bytes(&input);
    let keeps = |acc: Document| doc! {"$group": {"_id": null, "k": acc}};
    for (acc, kept_all) in [
        (doc! {"$push": "$pad"}, true),
        (doc! {"$addToSet": "$pad"}, false),
        (doc! {"$firstN": {"input": "$pad", "n": 50}}, true),
        (doc! {"$lastN": {"input": "$pad", "n": 50}}, true),
        (doc! {"$maxN": {"input": "$_id", "n": 50}}, false),
        (doc! {"$topN": {"output": "$pad", "sortBy": {"_id": 1}, "n": 50}}, true),
        (doc! {"$mergeObjects": {"p": "$pad"}}, false),
        (doc! {"$max": "$pad"}, false),
    ] {
        let stage = keeps(acc.clone());
        let smallest = smallest_cap(&stage, &input, held_in + 100_000);
        let kept = smallest - held_in;
        if kept_all {
            // 50 distinct values of 400 bytes: all of them are held.
            assert!(kept >= 50 * 400, "{acc}: {kept}");
        } else {
            assert!(
                kept < 50 * 400,
                "{acc}: only what is kept is charged, not what is read: {kept}"
            );
        }
    }
}

#[test]
fn top_n_charges_the_rows_it_keeps_and_not_the_rows_it_reads() {
    let input = padded(200, 500);
    let held_in = held_docs_bytes(&input);
    let stage = doc! {"$group": {"_id": null, "k": {"$topN": {"output": "$pad", "sortBy": {"_id": 1}, "n": 2}}}};
    let smallest = smallest_cap(&stage, &input, held_in + 100_000);
    // Two rows of 500 bytes are kept at a time, however many are read: a few KB, where the
    // 200 rows read would be over 100 KB.
    assert!(smallest - held_in < 6_000, "{}", smallest - held_in);
}

#[test]
fn bucket_and_bucket_auto_and_sort_by_count_hold_their_buckets() {
    let input = padded(60, 200);
    let held_in = held_docs_bytes(&input);
    for stage in [
        doc! {"$bucket": {"groupBy": "$_id", "boundaries": [0, 20, 40, 60], "output": {"all": {"$push": "$pad"}}}},
        doc! {"$bucketAuto": {"groupBy": "$_id", "buckets": 3, "output": {"all": {"$push": "$pad"}}}},
    ] {
        let smallest = smallest_cap(&stage, &input, held_in + 100_000);
        assert!(smallest >= held_in + 60 * 200, "{stage}: {smallest}");
        let err = run(stage.clone(), input.clone(), &capped(smallest - 1)).unwrap_err();
        assert!(is_too_large(&err), "{stage}: {err:?}");
    }
    let stage = doc! {"$sortByCount": "$g"};
    let out = run(stage.clone(), input.clone(), &capped(held_in + 10_000)).unwrap();
    assert_eq!(out.len(), 3);
    let err = run(stage, input, &capped(held_in)).unwrap_err();
    assert!(
        matches!(&err, Error::ValueTooLarge { what, .. } if what.starts_with("the buckets of $sortByCount")),
        "{err:?}"
    );
}

#[test]
fn redact_charges_the_copies_it_keeps() {
    let input = padded(20, 200);
    let held_in = held_docs_bytes(&input);
    let stage = doc! {"$redact": "$$KEEP"};
    let out = run(stage.clone(), input.clone(), &capped(usize::MAX / 4)).unwrap();
    let both = held_in + held_docs_bytes(&out);
    assert!(run(stage.clone(), input.clone(), &capped(both)).is_ok());
    assert!(is_too_large(&run(stage, input, &capped(both - 1)).unwrap_err()));
}

#[test]
fn the_blocking_stages_charge_their_buffers_once_not_twice() {
    // `$setWindowFields` holds the documents it buffers and the values it adds; the input
    // is not charged a second time.
    let input: Vec<Document> =
        (0..20).map(|i| doc! {"_id": i, "g": i % 2, "pad": "x".repeat(100)}).collect();
    let held_in = held_docs_bytes(&input);
    let stage =
        doc! {"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"n": {"$documentNumber": {}}}}};
    let smallest = smallest_cap(&stage, &input, 10 * held_in);
    assert!(smallest < 2 * held_in, "the input is charged once: {smallest} vs {held_in}");
    let stage = doc! {"$densify": {"field": "_id", "range": {"step": 1, "bounds": "full"}}};
    let smallest = smallest_cap(&stage, &input, 10 * held_in);
    assert!(smallest < 2 * held_in, "{smallest} vs {held_in}");
}

#[test]
fn an_early_refusal_ends_a_stage_at_the_document_that_passes_the_limit() {
    // The third document would raise a different error if the stage got to it. Its
    // output passes the limit at the first document, so the refusal comes first.
    let mut input = padded(5, 1_000);
    input[2].insert("z", 0);
    let stage =
        doc! {"$addFields": {"q": {"$divide": [1, {"$ifNull": ["$z", 1]}]}, "copy": "$pad"}};
    let err =
        run(stage.clone(), input.clone(), &capped(held_docs_bytes(&input) + 1_500)).unwrap_err();
    assert!(is_too_large(&err), "{err:?}");
    // With room, the stage does reach it.
    let err = run(stage, input, &capped(usize::MAX / 4)).unwrap_err();
    assert!(matches!(err, Error::InvalidQuery(_)), "{err:?}");
}

// ---------------------------------------------------------------------------
// $facet
// ---------------------------------------------------------------------------

#[test]
fn a_facet_holds_its_input_and_a_copy_for_each_branch_but_the_last() {
    let input = padded(20, 200);
    let held_in = held_docs_bytes(&input);
    let two = doc! {"$facet": {"a": [{"$count": "n"}], "b": [{"$count": "n"}]}};
    // The input, and the first branch's copy of it as that branch's first stage holds it.
    let limits = capped(2 * held_in);
    assert_eq!(run(two.clone(), input.clone(), &limits).unwrap().len(), 1);
    assert_eq!(limits.buffer.used(), 0);
    let err = run(two, input.clone(), &capped(2 * held_in - 1)).unwrap_err();
    assert!(is_too_large(&err), "{err:?}");
    // One branch takes the input itself: nothing is copied.
    let one = doc! {"$facet": {"a": [{"$count": "n"}]}};
    assert!(run(one, input, &capped(held_in)).is_ok());
}

#[test]
fn the_copy_a_facet_makes_is_charged_before_it_is_made() {
    let input = padded(20, 200);
    let held_in = held_docs_bytes(&input);
    let two = doc! {"$facet": {"a": [{"$count": "n"}], "b": [{"$count": "n"}]}};
    // Room for the input and not for a copy: refused naming the copy.
    let err = run(two, input, &capped(held_in + held_in / 2)).unwrap_err();
    assert!(
        matches!(&err, Error::ValueTooLarge { what, .. } if what.contains("a copy of the input of $facet")),
        "{err:?}"
    );
}

#[test]
fn the_branches_of_a_facet_share_one_budget_with_the_stages_inside_them() {
    let input = padded(20, 200);
    let held_in = held_docs_bytes(&input);
    let branch = doc! {"$facet": {"a": [{"$addFields": {"e": "$pad"}}, {"$count": "n"}]}};
    // The branch's `$addFields` holds its input and its output, and the facet nothing more
    // (it hands over its only copy).
    let out =
        run(doc! {"$addFields": {"e": "$pad"}}, input.clone(), &capped(usize::MAX / 4)).unwrap();
    let need = held_in + held_docs_bytes(&out);
    assert!(run(branch.clone(), input.clone(), &capped(need)).is_ok());
    assert!(is_too_large(&run(branch, input, &capped(need - 1)).unwrap_err()));
}

// ---------------------------------------------------------------------------
// $fill (the review's B1) and the sweep of every stage
// ---------------------------------------------------------------------------

/// The element a `big` field of `size` bytes adds to a document.
fn big_element(size: usize) -> usize {
    crate::window::element_bytes("big", &Bson::String("x".repeat(size)))
}

#[test]
fn fill_value_charges_its_input_once_and_each_value_it_writes() {
    let input = padded(50, 200);
    let held_in = held_docs_bytes(&input);
    let stage = doc! {"$fill": {"output": {"big": {"value": "y".repeat(2_000)}}}};
    let out = run(stage.clone(), input.clone(), &capped(usize::MAX / 4)).unwrap();
    // The documents have three fields, so the fourth takes the table a doubling.
    let one = big_element(2_000) + crate::budget::held_frame_growth(3, 1);
    // What the stage charges: its input once and, for each document, the element it writes
    // (the estimate of what the field adds, not the difference of two whole documents).
    let held_out = held_in + 50 * one;
    assert!(held_docs_bytes(&out) <= held_out + 50 * 64, "the estimate covers what was built");
    // The stage holds exactly its input and the values it wrote: a budget of that passes and
    // gives everything back; a budget of one byte less is refused at the last byte. A
    // charge of the input twice would have refused the first.
    let limits = capped(held_out);
    assert_eq!(run(stage.clone(), input.clone(), &limits).unwrap(), out);
    assert_eq!(limits.buffer.used(), 0);
    let err = run(stage.clone(), input.clone(), &capped(held_out - 1)).unwrap_err();
    let Error::ValueTooLarge { what, bytes, .. } = &err else { panic!("{err:?}") };
    assert_eq!((*what, *bytes), ("the documents $fill buffers", held_out));
    // And it stops at the document whose value passes the limit, not after the fiftieth:
    // room for the input and three values refuses at the fourth.
    let err = run(stage, input, &capped(held_in + 3 * one + one / 2)).unwrap_err();
    let Error::ValueTooLarge { bytes, .. } = &err else { panic!("{err:?}") };
    assert_eq!(*bytes, held_in + 4 * one);
}

#[test]
fn fill_with_a_method_and_with_both_charges_its_input_once_and_every_value() {
    // Even documents hold `v`; the odd ones are filled from the one before.
    let input: Vec<Document> = (0..50)
        .map(|i| {
            if i % 2 == 0 {
                doc! {"_id": i, "v": i, "pad": "x".repeat(200)}
            } else {
                doc! {"_id": i, "pad": "x".repeat(200)}
            }
        })
        .collect();
    let held_in = held_docs_bytes(&input);
    let int_v = crate::window::element_bytes("v", &Bson::Int32(0));
    // The big value is a fourth field of a three-field document: its table doubles.
    let growth = crate::budget::held_frame_growth(3, 1);
    let one = big_element(2_000) + growth;
    let locf = doc! {"v": {"method": "locf"}};
    let both = doc! {"v": {"method": "locf"}, "big": {"value": "y".repeat(2_000)}};
    for (name, output, added) in [("locf", locf, 25 * int_v), ("both", both, 25 * int_v + 50 * one)]
    {
        let stage = doc! {"$fill": {"sortBy": {"_id": 1}, "output": output}};
        let out = run(stage.clone(), input.clone(), &capped(usize::MAX / 4)).unwrap();
        assert!(held_docs_bytes(&out) <= held_in + added + 50 * 64, "{name}");
        let slack = 10_000 + if name == "both" { 50 * growth } else { 0 };
        // The input once, the values written, and the window's sort keys and cells (a slot
        // and a key for each of the 50 documents, under 10 KB): a second charge of the input
        // would be 50 KB more.
        let smallest = smallest_cap(&stage, &input, 10 * held_in);
        assert!(smallest >= held_in + added, "{name}: {smallest}");
        assert!(smallest < held_in + added + slack, "{name}: {smallest}");
        let limits = capped(smallest);
        assert!(run(stage.clone(), input.clone(), &limits).is_ok(), "{name}");
        assert_eq!(limits.buffer.used(), 0, "{name}");
        let err = run(stage, input.clone(), &capped(smallest - 1)).unwrap_err();
        assert!(is_too_large(&err), "{name}: {err:?}");
    }
}

#[test]
fn the_reviewers_fill_shapes_are_refused_in_the_budget_they_were_not_in() {
    // 50 documents and a 1 MiB budget, as the review measured, with a value of 100 KB: the
    // output is 5 MB, five times the budget. Each shape was answered; each is refused now.
    let input: Vec<Document> = (0..50).map(|i| doc! {"_id": i, "v": i}).collect();
    let big = "z".repeat(100_000);
    let budget = capped(1 << 20);
    let value = doc! {"$fill": {"output": {"big": {"value": big.as_str()}}}};
    let with_locf = doc! {"$fill": {"sortBy": {"_id": 1},
    "output": {"big": {"value": big.as_str()}, "v": {"method": "locf"}}}};
    for stage in [value.clone(), with_locf] {
        let err = run(stage.clone(), input.clone(), &budget).unwrap_err();
        let Error::ValueTooLarge { what, bytes, max, .. } = &err else { panic!("{err:?}") };
        assert_eq!(*what, "the documents $fill buffers", "{stage}");
        // Refused at the eleventh value (10 x 100 KB fits in 1 MiB), not after all fifty.
        assert!(*bytes > *max && *bytes < *max + 100_100, "{stage}: {bytes}");
        assert_eq!(budget.buffer.used(), 0, "{stage}");
    }
    // `$fill` and then `$count`: the fill is what is refused, before it built 5 MB.
    let pipeline = [value, doc! {"$count": "n"}];
    let stages = aggregate::parse(&pipeline).unwrap();
    let mut docs = input;
    let mut seen = Vec::new();
    for stage in &stages {
        match aggregate::apply(stage, docs.clone(), &budget) {
            Ok(out) => docs = out,
            Err(e) => {
                seen.push((stage.name(), e));
                break;
            }
        }
    }
    let (name, err) = seen.pop().expect("refused");
    assert_eq!(name, "$fill");
    assert!(matches!(&err, Error::ValueTooLarge { what, .. } if what.contains("$fill")), "{err:?}");
}

/// A stage of every kind the pure pipeline runs, over documents of `padded(20, 200)`.
fn every_pure_stage() -> Vec<Document> {
    vec![
        doc! {"$match": {"g": 1}},
        doc! {"$project": {"p": "$pad"}},
        doc! {"$addFields": {"e": "$pad"}},
        doc! {"$set": {"e": "$pad"}},
        doc! {"$unset": "pad"},
        doc! {"$replaceRoot": {"newRoot": {"p": "$pad"}}},
        doc! {"$replaceWith": {"p": "$pad"}},
        doc! {"$group": {"_id": "$g", "n": {"$sum": 1}}},
        doc! {"$bucket": {"groupBy": "$_id", "boundaries": [0, 10, 100]}},
        doc! {"$bucketAuto": {"groupBy": "$_id", "buckets": 3}},
        doc! {"$sort": {"g": -1}},
        doc! {"$sortByCount": "$g"},
        doc! {"$limit": 5},
        doc! {"$skip": 5},
        doc! {"$count": "n"},
        doc! {"$unwind": "$pad"},
        doc! {"$facet": {"a": [{"$count": "n"}]}},
        doc! {"$redact": "$$KEEP"},
        doc! {"$sample": {"size": 4}},
        doc! {"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"n": {"$documentNumber": {}}}}},
        doc! {"$densify": {"field": "_id", "range": {"step": 1, "bounds": "full"}}},
        doc! {"$fill": {"output": {"x": {"value": 1}}}},
        doc! {"$fill": {"sortBy": {"_id": 1}, "output": {"pad": {"method": "locf"}}}},
    ]
}

#[test]
fn every_stage_is_refused_with_a_413_when_its_input_is_over_the_budget() {
    let input = padded(20, 200);
    let held_in = held_docs_bytes(&input);
    for stage in every_pure_stage() {
        let name = stage.keys().next().unwrap().clone();
        // The stage runs with room for its input and is refused one byte under it.
        let err = run(stage.clone(), input.clone(), &capped(held_in - 1)).unwrap_err();
        assert!(is_too_large(&err), "{name} ({stage}): {err:?}");
        let limits = capped(held_in - 1);
        let _ = run(stage.clone(), input.clone(), &limits);
        assert_eq!(limits.buffer.used(), 0, "{name}: gives everything back when refused");
        // A budget of nothing refuses it too, whatever it is.
        assert!(is_too_large(&run(stage, input.clone(), &capped(0)).unwrap_err()), "{name}");
    }
}

#[test]
fn every_stage_that_builds_is_refused_at_the_document_that_passes_the_budget() {
    // Each input fits with room for two built values and no more; the stage would build 40
    // of 1 KB or more, so a stage that finished would end far over the cap. The refusal's
    // total says where it stopped: within one built value of the cap.
    let big = "q".repeat(20_000);
    let input: Vec<Document> =
        (0..40).map(|i| doc! {"_id": i * 10, "g": i, "tags": [1, 2, 3], "pad": "x"}).collect();
    let held_in = held_docs_bytes(&input);
    let value = Bson::String(big.clone());
    // The unit of each: what one built value or document is charged. A stage that builds a
    // document per input document is charged that document whole (its estimate, found by
    // running the stage once without a cap); one that collects a value is charged the value.
    let pushed = Some(held_bytes_bson(&value));
    let cases: Vec<(&str, Document, Option<usize>)> = vec![
        ("$addFields", doc! {"$addFields": {"big": big.as_str()}}, None),
        ("$project", doc! {"$project": {"big": big.as_str()}}, None),
        ("$replaceRoot", doc! {"$replaceRoot": {"newRoot": {"big": big.as_str()}}}, None),
        ("$fill", doc! {"$fill": {"output": {"big": {"value": big.as_str()}}}}, None),
        (
            "$setWindowFields",
            doc! {"$setWindowFields": {"sortBy": {"_id": 1},
            "output": {"all": {"$push": "$pad", "window": {"documents": ["unbounded", "current"]}},
            "big": {"$first": value.clone()}}}},
            None,
        ),
        (
            "$densify",
            doc! {"$densify": {"field": "_id", "range": {"step": 1, "bounds": "full"}}},
            None,
        ),
        ("$unwind", doc! {"$unwind": {"path": "$tags"}}, None),
        ("$group", doc! {"$group": {"_id": null, "all": {"$push": big.as_str()}}}, pushed),
        (
            "$bucket",
            doc! {"$bucket": {"groupBy": "$_id", "boundaries": [0, 1000],
            "output": {"all": {"$push": big.as_str()}}}},
            pushed,
        ),
        (
            "$facet",
            doc! {"$facet": {"a": [{"$addFields": {"big": big.as_str()}}], "b": [{"$count": "n"}]}},
            None,
        ),
    ];
    for (name, stage, unit) in cases {
        let unit = unit.unwrap_or_else(|| {
            let out = run(stage.clone(), input.clone(), &capped(usize::MAX / 4)).unwrap();
            // A `$facet` answers one document holding its branches' documents.
            let docs: Vec<Document> = match out.first().and_then(|d| d.get_array("a").ok()) {
                Some(a) => a.iter().filter_map(|d| d.as_document().cloned()).collect(),
                None => out,
            };
            docs.iter().map(held_bytes).max().unwrap()
        });
        // Room for the input, the stage's own copy of it, and two built values.
        let cap = 2 * held_in + 2 * unit;
        let limits = capped(cap);
        let err = run(stage.clone(), input.clone(), &limits)
            .expect_err(&format!("{name} finished inside {cap} bytes"));
        let Error::ValueTooLarge { bytes, max, .. } = &err else { panic!("{name}: {err:?}") };
        assert_eq!(*max, cap, "{name}");
        assert!(*bytes > cap && *bytes <= cap + 3 * unit, "{name}: stopped at {bytes} of {cap}");
        assert_eq!(limits.buffer.used(), 0, "{name}");
    }
}

// ---------------------------------------------------------------------------
// The numbers at the limits a member runs under, without allocating them
//
// A member derives its limits from the memory it runs under (a pool of a tenth of it, a
// request may hold half the pool: `kimmyd`'s `aggmem`), so the figures below are those of a
// 2, an 8 and a 16 GiB container. The constants in `budget.rs` are only what a `Limits`
// made with `Default` falls back on, for tests and embedders.
// ---------------------------------------------------------------------------

/// The per-request limit and the pool of a container of `memory` bytes, as `aggmem` derives
/// them: a pool of a tenth, a request a half of the pool.
fn derived(memory: usize) -> (usize, usize) {
    let pool = memory / 10;
    (pool / 2, pool)
}

#[test]
fn the_library_default_is_a_fresh_fallback_budget_and_not_what_a_member_runs_on() {
    // `Default` is for tests and embedders: a budget of the fallback constant, new each time.
    assert_eq!(Limits::default().buffer.cap(), DEFAULT_MEMORY_LIMIT_BYTES);
    assert_eq!(DEFAULT_MEMORY_POOL_BYTES, 2 * DEFAULT_MEMORY_LIMIT_BYTES);
    let (a, b) = (Limits::default(), Limits::default());
    let mut held = a.buffer.lease("a");
    held.charge(10).unwrap();
    assert_eq!((a.buffer.used(), b.buffer.used()), (10, 0), "each default is its own budget");
    // The figures a member gets are not the fallback: 2 GiB of memory is a tenth for the pool
    // and half of that for a request.
    assert_eq!(derived(2 << 30), (107_374_182, 214_748_364));
    assert!(derived(2 << 30).0 < DEFAULT_MEMORY_LIMIT_BYTES / 9);
}

/// `{_id, pad}` with a string of `pad` characters, and the same with one boolean field added by
/// `$addFields`: the two sizes a document is charged at, input and output.
fn add_fields_pair(pad: usize) -> (Document, Document) {
    let one = doc! {"_id": 1_i32, "pad": "x".repeat(pad)};
    let mut grown = one.clone();
    grown.insert("flag", true);
    (one, grown)
}

/// The most `$addFields` holds over 100,000 of them: every input and every output.
fn add_fields_peak(pad: usize) -> usize {
    let (one, grown) = add_fields_pair(pad);
    100_000 * (held_bytes(&one) + held_bytes(&grown))
}

#[test]
fn a_hundred_thousand_documents_of_four_kib_run_through_add_fields_at_an_explicit_limit() {
    // The review's acceptance check 3: 100,000 documents of 4 KiB through `$addFields`. The
    // documents of 4,094 bytes of BSON (a string of 4,070 characters) are charged 5,336 each and
    // 5,372 with the field added: a peak of 1,070,800,000, under 1 GiB (1,073,741,824) by 0.3%.
    // With a string of 4,096 characters (4,120 bytes) they are charged 5,372 and 5,408: a peak
    // of 1,078,000,000, over 1 GiB and under 2 GiB. Worked out from real documents, charged
    // through the real accounts, with nothing allocated.
    let n = 100_000usize;
    for (pad, peak_expected, runs_at_one_gib) in
        [(4_070usize, 1_070_800_000usize, true), (4_096, 1_078_000_000, false)]
    {
        let (one, grown) = add_fields_pair(pad);
        let (size_in, size_out) = (held_bytes(&one), held_bytes(&grown));
        assert_eq!(n * (size_in + size_out), peak_expected, "pad {pad}");
        for (cap, runs) in [(1usize << 30, runs_at_one_gib), (2 << 30, true)] {
            let limits = Limits { buffer: BufferBudget::new(cap), ..Default::default() };
            let mut input = limits.buffer.lease("the input of $addFields");
            let mut output = limits.buffer.lease("the output of $addFields");
            let ran = (0..n).all(|_| input.charge(size_in).is_ok())
                && (0..n).all(|_| output.charge(size_out).is_ok());
            assert_eq!(ran, runs, "pad {pad} under {cap}: peak {peak_expected}");
            if ran {
                assert_eq!(limits.buffer.used(), peak_expected);
                // The answer is then the documents and their JSON: still under the limit.
                drop(input);
                let json = format!(r#"{{"_id":1,"pad":"{}","flag":true}}"#, "x".repeat(pad)).len();
                let mut answer = limits.buffer.lease("the answer");
                for _ in 0..n {
                    answer.charge(json).unwrap();
                }
                assert!(limits.buffer.used() < cap, "{}", limits.buffer.used());
            }
        }
        // The plain BSON bytes of the same would have fit 1 GiB either way: the estimate is
        // what makes the second not.
        assert!(n * (doc_bytes(&one) + doc_bytes(&grown)) < 1 << 30);
    }
}

#[test]
fn a_hundred_thousand_documents_of_four_kib_are_refused_at_the_derived_limits() {
    // The same 100,000 documents against the limit a member derives. The source read is
    // charged whole before any stage runs and stops at the document that passes the limit;
    // `$addFields` holds input plus output, so it needs twice the source.
    let (one, grown) = add_fields_pair(4_096);
    let (size, size_out) = (held_bytes(&one), held_bytes(&grown));
    assert_eq!((size, size_out), (5_372, 5_408));
    let n = 100_000usize;
    // The peak is 1,078,000,000 (1,070,800,000 for the 4,094-byte documents): `$addFields`
    // runs from a limit that high, a container 20 times it: 20.1 GiB (19.95).
    for (memory, runs) in [
        (2usize << 30, false),
        (8 << 30, false),
        (16 << 30, false),
        (20 << 30, false),
        (21 << 30, true),
        (24 << 30, true),
    ] {
        let (limit, pool) = derived(memory);
        let budget = BufferBudget::pooled(limit, &MemoryPool::new(pool));
        // The source read: the document that passes the limit is the first refused.
        let mut read = budget.lease("the source read");
        let mut at = None;
        for i in 1..=n {
            if read.charge(size).is_err() {
                at = Some(i);
                break;
            }
        }
        let read_fits = at.is_none();
        // `$addFields` then holds the input again beside what it builds. The read is the
        // stage's input here.
        let mut built = budget.lease("the output of $addFields");
        let output_fits = read_fits && (0..n).all(|_| built.charge(size_out).is_ok());
        assert_eq!(output_fits, runs, "{} GiB: read refused at {at:?}", memory >> 30);
        if let Some(i) = at {
            assert_eq!(i, limit / size + 1, "{} GiB", memory >> 30);
        }
    }
    // The review's documents (4,094 bytes) run in a container of 20 GiB, where these do not.
    let peak = add_fields_peak(4_070);
    assert!(peak <= derived(20 << 30).0 && peak > derived(19 << 30).0, "{peak}");
    assert!(add_fields_peak(4_096) > derived(20 << 30).0);
    // A 2 GiB member refuses the read itself, at the 19,988th document of the 100,000.
    assert_eq!(derived(2 << 30).0 / size + 1, 19_988);
}

/// The estimate of a document of `n` integer fields, as the reviewer's wide shape.
fn ints_doc(n: usize) -> Document {
    let mut d = doc! {"_id": 1_i32};
    for i in 0..n {
        d.insert(format!("f{i}"), i as i32);
    }
    d
}

#[test]
fn the_reviewers_wide_documents_are_refused_at_the_source_read_at_the_derived_limits() {
    // The review's B2: 400 integer fields take 19 times their BSON, so 25,000 of them (100 MB
    // of BSON, inside the old 102 MiB limit) ended a 2 GiB member. Charged at the estimate,
    // the read stops after about a thousand. Worked out from one representative document and
    // the limit arithmetic, with nothing allocated.
    let wide = ints_doc(400);
    let mut hundred = doc! {"_id": 1_i32};
    for i in 0..50 {
        hundred.insert(format!("n{i}"), i);
        hundred.insert(format!("s{i}"), "y".repeat(60));
    }
    for (name, doc, count, bson_total_mb) in
        [("400 int fields", &wide, 25_000usize, 97usize), ("100 fields", &hundred, 100_000, 394)]
    {
        let (bson, charged) = (doc_bytes(doc), held_bytes(doc));
        // The charge is what the document costs parsed: 5 to 24 times its BSON.
        assert!(charged > 5 * bson, "{name}: {charged} for {bson}");
        assert!(count * bson / 1_000_000 >= bson_total_mb, "{name}");
        for memory in [2usize << 30, 8 << 30] {
            let (limit, pool) = derived(memory);
            let budget = BufferBudget::pooled(limit, &MemoryPool::new(pool));
            let mut read = budget.lease("the source read");
            let refused = (1..=count).find(|_| read.charge(charged).is_err());
            // Refused at the first document past the limit, long before the read ends.
            assert_eq!(refused, Some(limit / charged + 1), "{name} in {} GiB", memory >> 30);
            assert!(refused.unwrap() < count / 4, "{name} in {} GiB", memory >> 30);
            // The BSON bytes of what was read before the refusal are far under the limit: the
            // old accounting would have gone on to read all of it.
            assert!(refused.unwrap() * bson < limit / 4, "{name} in {} GiB", memory >> 30);
        }
    }
    // The figures the review used: 400 int fields refused after 1,162 documents in 2 GiB and
    // 4,650 in 8 GiB.
    assert_eq!(derived(2 << 30).0 / held_bytes(&wide) + 1, 1_163);
    assert_eq!(derived(8 << 30).0 / held_bytes(&wide) + 1, 4_651);
}

#[test]
fn wide_documents_are_refused_end_to_end_under_a_small_limit_they_would_have_fit_as_bson() {
    // The same shape scaled down: 60 documents of 400 integer fields are 234 KB of BSON and
    // 5.5 MB parsed. Under a limit of 2 MiB the old accounting let `$count` through; the
    // estimate refuses it at the input, a few documents past the limit, having allocated what
    // the documents hold and no more.
    let input: Vec<Document> = (0..60).map(|_| ints_doc(400)).collect();
    assert!(docs_bytes(&input) < 300_000);
    let limits = capped(2 << 20);
    let err = run(doc! {"$count": "n"}, input.clone(), &limits).unwrap_err();
    let Error::ValueTooLarge { what, bytes, max, .. } = &err else { panic!("{err:?}") };
    assert_eq!((*what, *max), ("the input of $count", 2 << 20));
    let each = held_bytes(&input[0]);
    assert_eq!(*bytes, (2 << 20) / each * each + each, "refused at the document that passes it");
    assert_eq!(limits.buffer.used(), 0);
    // Under a limit that holds them as parsed it runs.
    assert!(run(doc! {"$count": "n"}, input.clone(), &capped(60 * each)).is_ok());
    assert!(run(doc! {"$count": "n"}, input, &capped(60 * each - 1)).is_err());
}

#[test]
fn a_thousand_documents_of_eight_mib_are_refused_at_the_limit_whichever_it_is() {
    // The reviewer's Z1: 1,000 documents each given an 8 MiB string. The input is small; the
    // output passes the limit long before 8 GB. The first document refused, for the explicit
    // 1 GiB of the first design and for the limits a 2 GiB and an 8 GiB member derive.
    let size = 8 * 1024 * 1024 + 20;
    for (limit, expected) in
        [(1usize << 30, 128), (derived(2 << 30).0, 13), (derived(8 << 30).0, 52)]
    {
        let limits = Limits { buffer: BufferBudget::new(limit), ..Default::default() };
        let mut output =
            limits.buffer.lease("the output of $addFields (with its input, still held)");
        let mut built = 0usize;
        let err = loop {
            built += 1;
            if let Err(e) = output.charge(size) {
                break e;
            }
            assert!(built < 1_000, "never refused");
        };
        assert_eq!(built, expected, "the document that passes {limit}");
        assert!(is_too_large(&err), "{err:?}");
    }
}

#[test]
fn at_the_derived_ratio_two_requests_cannot_exhaust_the_pool_and_a_third_waits() {
    // A 2 GiB container: a pool of 214,748,364 and a request limit of half of it. Two requests
    // each at their limit fill the pool exactly, so one more request, a third, is the first to
    // be told to wait; with only two, no request ever meets the pool.
    let (limit, pool_bytes) = derived(2 << 30);
    assert_eq!(2 * limit, pool_bytes);
    let pool = MemoryPool::new(pool_bytes);
    let a = BufferBudget::pooled(limit, &pool);
    let b = BufferBudget::pooled(limit, &pool);
    let c = BufferBudget::pooled(limit, &pool);
    let (mut la, mut lb, mut lc) = (a.lease("a"), b.lease("b"), c.lease("c"));
    la.charge(limit).unwrap();
    lb.charge(limit).unwrap();
    assert_eq!(pool.used(), pool_bytes);
    // Two requests are within the pool, and a request cannot go over its own limit to meet it.
    assert!(is_too_large(&la.charge(1).unwrap_err()));
    assert_eq!(pool.pool_refusals(), 0);
    // The third is refused with a retry and nothing reserved, and is answered when one ends.
    assert!(matches!(lc.charge(1).unwrap_err(), Error::MemoryBusy { .. }));
    assert_eq!((pool.pool_refusals(), c.reserved()), (1, 0));
    drop(la);
    let mut lc2 = c.lease("c");
    lc2.charge(1).unwrap();
}

#[test]
fn with_an_explicit_pool_under_twice_the_limit_two_requests_meet_the_pool() {
    // An explicit pool of one and a half limits: the second request is the one told to wait.
    let limit = 1_000_000usize;
    let pool = MemoryPool::new(limit + limit / 2);
    let (a, b) = (BufferBudget::pooled(limit, &pool), BufferBudget::pooled(limit, &pool));
    let (mut la, mut lb) = (a.lease("a"), b.lease("b"));
    la.charge(limit).unwrap();
    assert!(matches!(lb.charge(limit).unwrap_err(), Error::MemoryBusy { .. }));
    assert_eq!(pool.pool_refusals(), 1);
}

// ---------------------------------------------------------------------------
// A group's output document, and the value bounds beside the budget (ADR-267, ADR-269)
// ---------------------------------------------------------------------------

fn with_document_limit(max_document_bytes: usize) -> Limits {
    Limits { max_document_bytes, ..Default::default() }
}

#[test]
fn a_group_document_is_held_to_its_limit_as_a_whole_not_per_accumulator() {
    // Each array is within the limit on its own (about 2.1 KB against 3,000); the two
    // together make a document of about 4.2 KB, which is refused at 3,000.
    let input = padded(10, 200);
    let one = doc! {"$group": {"_id": null, "a": {"$push": "$pad"}}};
    let two = doc! {"$group": {"_id": null, "a": {"$push": "$pad"}, "b": {"$push": "$pad"}}};
    let limits = with_document_limit(3_000);
    assert_eq!(run(one, input.clone(), &limits).unwrap().len(), 1);
    let err = run(two.clone(), input.clone(), &limits).unwrap_err();
    let Error::ValueTooLarge { what, bytes, max, .. } = &err else { panic!("{err:?}") };
    assert_eq!((*what, *max), ("the document $group builds", 3_000));
    assert!(*bytes > 4_000 && *bytes < 4_500, "{bytes}");
    // Exactly at the limit it is built, one byte under it is refused.
    let built = run(two.clone(), input.clone(), &with_document_limit(usize::MAX)).unwrap();
    let size = doc_bytes(&built[0]);
    assert!(run(two.clone(), input.clone(), &with_document_limit(size)).is_ok());
    assert!(is_too_large(&run(two, input, &with_document_limit(size - 1)).unwrap_err()));
}

#[test]
fn a_bucket_and_a_bucket_auto_output_document_are_held_to_the_limit_as_a_whole() {
    let input = padded(30, 200);
    for stage in [
        doc! {"$bucket": {"groupBy": "$_id", "boundaries": [0, 30],
        "output": {"a": {"$push": "$pad"}, "b": {"$push": "$pad"}}}},
        doc! {"$bucketAuto": {"groupBy": "$_id", "buckets": 1,
        "output": {"a": {"$push": "$pad"}, "b": {"$push": "$pad"}}}},
    ] {
        let name = if stage.contains_key("$bucket") { "$bucket" } else { "$bucketAuto" };
        let built = run(stage.clone(), input.clone(), &with_document_limit(usize::MAX)).unwrap();
        let size = doc_bytes(&built[0]);
        assert!(run(stage.clone(), input.clone(), &with_document_limit(size)).is_ok(), "{name}");
        let err = run(stage, input.clone(), &with_document_limit(size - 1)).unwrap_err();
        assert!(
            matches!(&err, Error::ValueTooLarge { what, .. } if what.contains(name)),
            "{name}: {err:?}"
        );
    }
}

/// The stage adds fields that each fit and together make a document over the bound: the
/// document the stage completes is refused as a whole, as `$addFields` refuses it, at the
/// byte (`size` passes, `size - 1` is refused naming `the document the stage builds` and
/// the total), while one of the fields alone passes under the same bound.
fn assert_built_document_is_held_whole(
    name: &str,
    stage: Document,
    one: Option<Document>,
    input: &[Document],
) {
    let built = run(stage.clone(), input.to_vec(), &with_document_limit(usize::MAX)).unwrap();
    let size = built.iter().map(doc_bytes).max().unwrap();
    assert!(run(stage.clone(), input.to_vec(), &with_document_limit(size)).is_ok(), "{name}");
    let err = run(stage, input.to_vec(), &with_document_limit(size - 1)).unwrap_err();
    let Error::ValueTooLarge { what, bytes, max, .. } = &err else { panic!("{name}: {err:?}") };
    assert_eq!(
        (*what, *bytes, *max),
        ("the document the stage builds", size, size - 1),
        "{name}: {err:?}"
    );
    // Each value fits alone: the same bound lets the stage with one of them through.
    if let Some(one) = one {
        assert!(run(one, input.to_vec(), &with_document_limit(size - 1)).is_ok(), "{name}: one");
    }
}

#[test]
fn fill_and_set_window_fields_hold_a_completed_document_to_the_limit_whole() {
    let input = padded(3, 1_000);
    // (`$addFields` is the reference for the refusal: the same helper and words, held to the
    // 16 MiB constant, which `aggregate.rs`'s own tests drive through `set_computed_within`.)
    // `$fill` with values.
    assert_built_document_is_held_whole(
        "$fill value",
        doc! {"$fill": {"output": {"a": {"value": "$pad"}, "b": {"value": "$pad"}}}},
        Some(doc! {"$fill": {"output": {"a": {"value": "$pad"}}}}),
        &input,
    );
    // `$fill` with values, one of which removes a field that holds a null: the document is
    // checked all the same.
    let nulled: Vec<Document> =
        (0..3).map(|i| doc! {"_id": i, "pad": "p".repeat(1_000), "b": Bson::Null}).collect();
    assert_built_document_is_held_whole(
        "$fill value and a removal",
        doc! {"$fill": {"output": {"a": {"value": "$pad"}, "b": {"value": "$nowhere"},
        "c": {"value": "$pad"}}}},
        Some(doc! {"$fill": {"output": {"a": {"value": "$pad"}, "b": {"value": "$nowhere"}}}}),
        &nulled,
    );
    // `$fill` with methods: the third document is given what the first two carried.
    let carried = vec![
        doc! {"_id": 0, "a": "a".repeat(1_200)},
        doc! {"_id": 1, "b": "b".repeat(1_200)},
        doc! {"_id": 2},
    ];
    assert_built_document_is_held_whole(
        "$fill locf",
        doc! {"$fill": {"sortBy": {"_id": 1},
        "output": {"a": {"method": "locf"}, "b": {"method": "locf"}}}},
        // (No one-field variant: the second document is given the first's value and holds
        // its own, as large as the third with both.)
        None,
        &carried,
    );
    // `$fill` with both kinds in one stage: the values come after the methods.
    assert_built_document_is_held_whole(
        "$fill locf and value",
        doc! {"$fill": {"sortBy": {"_id": 1},
        "output": {"a": {"method": "locf"}, "c": {"value": "$pad"}}}},
        Some(doc! {"$fill": {"sortBy": {"_id": 1}, "output": {"a": {"method": "locf"}}}}),
        &[
            doc! {"_id": 0, "a": "a".repeat(1_200), "pad": "p".repeat(1_000)},
            doc! {"_id": 1, "pad": "p".repeat(1_000)},
        ],
    );
    // `$setWindowFields`.
    assert_built_document_is_held_whole(
        "$setWindowFields",
        doc! {"$setWindowFields": {"sortBy": {"_id": 1},
        "output": {"a": {"$first": "$pad"}, "b": {"$first": "$pad"}}}},
        Some(
            doc! {"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"a": {"$first": "$pad"}}}},
        ),
        &input,
    );
}

#[test]
fn unwind_with_an_index_field_holds_the_document_it_completes_to_the_limit_whole() {
    // A document at the bound before the index field is added: `$unwind` without the index
    // passes it, and with it the document is over by the field, refused at the byte.
    let docs = [
        // An array to fan out, a scalar that unwinds to itself, and the empty and null cases
        // that `preserveNullAndEmptyArrays` keeps (the null index is added to those too).
        ("array", doc! {"_id": 1, "tags": ["a"], "pad": "p".repeat(300)}, false),
        ("scalar", doc! {"_id": 1, "tags": "a", "pad": "p".repeat(300)}, false),
        ("empty", doc! {"_id": 1, "tags": Bson::Array(vec![]), "pad": "p".repeat(300)}, true),
        ("null", doc! {"_id": 1, "tags": Bson::Null, "pad": "p".repeat(300)}, true),
        ("missing", doc! {"_id": 1, "pad": "p".repeat(300)}, true),
    ];
    for (name, input, preserve) in docs {
        let plain = doc! {"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": preserve}};
        let indexed = doc! {"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": preserve,
        "includeArrayIndex": "idx"}};
        let without =
            run(plain.clone(), vec![input.clone()], &with_document_limit(usize::MAX)).unwrap();
        let with =
            run(indexed.clone(), vec![input.clone()], &with_document_limit(usize::MAX)).unwrap();
        let (before, size) = (doc_bytes(&without[0]), doc_bytes(&with[0]));
        assert!(size > before, "{name}: the index adds a field");
        // The document at the limit before the index passes without it; size passes with it,
        // and size + 1 over refuses at the byte.
        assert!(run(plain, vec![input.clone()], &with_document_limit(before)).is_ok(), "{name}");
        assert!(run(indexed.clone(), vec![input.clone()], &with_document_limit(size)).is_ok());
        let err =
            run(indexed.clone(), vec![input.clone()], &with_document_limit(size - 1)).unwrap_err();
        let Error::ValueTooLarge { what, bytes, max, .. } = &err else { panic!("{name}: {err:?}") };
        assert_eq!(
            (*what, *bytes, *max),
            ("the document the stage builds", size, size - 1),
            "{name}"
        );
        // Under a bound the document before the index fits and the index does not.
        assert!(run(indexed, vec![input], &with_document_limit(before)).is_err(), "{name}");
    }
}

#[test]
fn the_value_bound_and_the_budget_each_refuse_alone_and_count_the_same_bytes_once() {
    let input = padded(40, 1_000);
    let held_in = held_docs_bytes(&input);
    let stage = doc! {"$group": {"_id": null, "all": {"$push": "$pad"}}};
    // The array of 40 strings is 40 KB. The budget is what holds it: charged once beside the
    // input (the array's own size plus the bucket), not twice.
    let smallest = smallest_cap(&stage, &input, 4 * held_in);
    let array = 40 * held_bytes_bson(&Bson::String("x".repeat(1_000)));
    assert!(smallest >= held_in + array && smallest < held_in + array + 1_500, "{smallest}");
    // A budget that fits is refused by nothing; a budget one byte short names the buckets.
    let err = run(stage, input, &capped(smallest - 1)).unwrap_err();
    assert!(
        matches!(&err, Error::ValueTooLarge { what, .. } if what.starts_with("the buckets of $group"))
    );
}

#[test]
fn a_window_output_that_makes_a_documents_table_grow_is_charged_the_growth() {
    // Documents of three fields have a table that holds three: the fourth, `n`, takes it to
    // seven. Each document is charged its input, the sort key taken, the cell and the growth.
    let input: Vec<Document> = (0..10).map(|i| doc! {"_id": i, "a": 1, "b": 2}).collect();
    let held_in = held_docs_bytes(&input);
    let stage = doc! {"$setWindowFields": {"sortBy": {"_id": 1},
    "output": {"n": {"$documentNumber": {}}}}};
    let key = held_bytes_bson(&Bson::Int32(0));
    let cell = crate::budget::held_element("n", &Bson::Int32(0));
    let growth = crate::budget::held_frame_growth(3, 1);
    assert!(growth > 200, "a doubling of a small table is a few hundred bytes: {growth}");
    let exact = held_in + 10 * (key + cell + growth);
    let smallest = smallest_cap(&stage, &input, 4 * held_in);
    assert_eq!(smallest, exact);
}

#[test]
fn a_group_of_many_small_buckets_is_charged_at_what_a_bucket_costs_parsed() {
    // A bucket is an output document, its key twice over and an index entry: 400 distinct
    // keys with nothing collected still cost what 400 parsed documents of two fields do, about
    // a KB each, where their BSON is twenty bytes.
    let input: Vec<Document> = (0..400).map(|i| doc! {"_id": i}).collect();
    let held_in = held_docs_bytes(&input);
    let stage = doc! {"$group": {"_id": "$_id", "n": {"$sum": 1}}};
    let smallest = smallest_cap(&stage, &input, 4 * held_in + 4_000_000);
    let buckets = smallest - held_in;
    let bucket = buckets / 400;
    assert!((900..3_000).contains(&bucket), "{bucket} a bucket");
    assert!(bucket > 20 * 20, "far over the 20 bytes of BSON an output document is");
}

#[test]
fn what_a_group_collects_is_charged_at_what_the_values_cost_parsed() {
    // `$push: "$$ROOT"` of documents of 100 integer fields keeps each whole: a document that
    // is 904 bytes of BSON and about 23 KB parsed. The bucket is charged the parsed size of
    // every value it collects, beside the input (which is charged the same, once).
    let input: Vec<Document> = (0..10)
        .map(|i| {
            let mut d = ints_doc(100);
            d.insert("_id", i);
            d
        })
        .collect();
    let held_in = held_docs_bytes(&input);
    let each = held_bytes_bson(&Bson::Document(input[0].clone()));
    for (name, stage) in [
        ("$push", doc! {"$group": {"_id": null, "all": {"$push": "$$ROOT"}}}),
        ("$addToSet", doc! {"$group": {"_id": null, "all": {"$addToSet": "$$ROOT"}}}),
        (
            "$firstN",
            doc! {"$group": {"_id": null, "all": {"$firstN": {"input": "$$ROOT", "n": 10}}}},
        ),
        ("$last", doc! {"$group": {"_id": null, "last": {"$last": "$$ROOT"}}}),
    ] {
        let collected = if name == "$last" { each } else { 10 * each };
        let smallest = smallest_cap(&stage, &input, 4 * held_in + 4 * collected);
        assert!(smallest >= held_in + collected, "{name}: {smallest} for {collected} collected");
        // (An `$addToSet` also keeps the identity of each member: about 1 KB a member.)
        assert!(smallest < held_in + collected + 20_000, "{name}: {smallest}");
    }
    // The BSON bytes of the same would be a twenty-fifth of it.
    assert!(10 * doc_bytes(&input[0]) * 20 < 10 * each);
}

#[test]
fn merge_objects_is_charged_by_the_document_it_builds_not_by_the_documents_it_reads() {
    // Forty documents each with a field of its own: the merged document holds all forty.
    let input: Vec<Document> = (0..40).map(|i| doc! {format!("f{i}"): "y".repeat(300)}).collect();
    let held_in = held_docs_bytes(&input);
    let stage = doc! {"$group": {"_id": null, "m": {"$mergeObjects": "$$ROOT"}}};
    let smallest = smallest_cap(&stage, &input, 4 * held_in);
    // What the merged document holds: forty fields of a 300-byte string, and its table.
    let merged = 40 * crate::budget::held_element("f00", &Bson::String("y".repeat(300)))
        + crate::budget::held_frame_growth(0, 40);
    assert!(smallest >= held_in + merged, "the merged fields are charged: {smallest}");
    assert!(smallest < held_in + merged + 2_500, "and once: {smallest}");
    // The same field set over and over is one field, however many documents set it.
    let same: Vec<Document> = (0..40).map(|_| doc! {"f": "y".repeat(300)}).collect();
    let held_same = held_docs_bytes(&same);
    let smallest = smallest_cap(&stage, &same, 4 * held_same);
    assert!(smallest < held_same + 3_000, "{smallest}");
}

// ---------------------------------------------------------------------------
// The stages that stream (ADR-270): what a run of them holds
// ---------------------------------------------------------------------------

/// A chain over `stages`, each stage run as the pure pipeline runs it.
fn chain_of<'a>(
    stages: &'a [aggregate::Stage],
    limits: &'a Limits,
) -> crate::pipeline::Chain<'a, Error> {
    // Boxed and leaked: the runner has to outlive the chain it is given to, and a test is short.
    let run: crate::pipeline::Runner<'static, Error> = Box::leak(Box::new({
        let limits = limits.clone();
        move |stage: &aggregate::Stage, input, arrived| {
            aggregate::apply_charged(stage, input, &limits, &[], arrived)
        }
    }));
    crate::pipeline::build(stages, run, limits, &[])
}

/// The stages that stream, each one that keeps no field of its own the tail looks for, then a
/// `$match` that drops every document, so that what a run holds is what is measured.
fn streaming_stages() -> Vec<Document> {
    vec![
        doc! {"$match": {"g": {"$gte": 0}}},
        doc! {"$project": {"p": "$pad", "g": 1}},
        doc! {"$addFields": {"e": "$pad"}},
        doc! {"$set": {"e": "$pad"}},
        doc! {"$unset": "g"},
        doc! {"$replaceRoot": {"newRoot": {"p": "$pad"}}},
        doc! {"$replaceWith": {"p": "$pad"}},
        doc! {"$redact": "$$KEEP"},
        doc! {"$unwind": "$pad"},
        doc! {"$skip": 0},
        doc! {"$limit": 1_000_000},
    ]
}

#[test]
fn a_stage_that_streams_holds_one_document_and_charges_nothing_that_lasts() {
    let nothing = doc! {"$match": {"nothing": 1}};
    for stage in streaming_stages() {
        let stages = aggregate::parse(&[stage.clone(), nothing.clone()]).unwrap();
        // A budget of nothing: no document is held, so none is charged.
        let limits = capped(0);
        let mut chain = chain_of(&stages, &limits);
        for doc in padded(200, 200) {
            assert!(chain.push(doc).is_ok(), "{stage}");
            assert_eq!(limits.buffer.used(), 0, "{stage}");
        }
        assert!(chain.finish().unwrap().is_empty(), "{stage}");
    }
}

#[test]
fn a_vec_given_to_a_stage_that_streams_is_charged_as_its_input_and_given_back_as_it_is_taken() {
    let input = padded(20, 200);
    let held_in = held_docs_bytes(&input);
    let nothing = doc! {"$match": {"nothing": 1}};
    for stage in streaming_stages() {
        let stages = aggregate::parse(&[stage.clone(), nothing.clone()]).unwrap();
        let name = crate::budget::input_of(stages[0].name());
        // Room for the input and no more: it is drained and the budget is empty again.
        let limits = capped(held_in);
        assert!(chain_of(&stages, &limits).push_all(input.clone()).is_ok(), "{stage}");
        assert_eq!(limits.buffer.used(), 0, "{stage}");
        // A byte under it is refused on arrival, naming the stage the main loop named.
        let limits = capped(held_in - 1);
        let err = chain_of(&stages, &limits).push_all(input.clone()).unwrap_err();
        let Error::ValueTooLarge { what, bytes, .. } = &err else { panic!("{stage}: {err:?}") };
        assert_eq!((*what, *bytes), (name, held_in), "{stage}");
        assert_eq!(limits.buffer.used(), 0, "{stage}: gives everything back when refused");
    }
}

/// One document of about 1 MiB with `levels` arrays of two numbers, `a0` to `a<levels-1>`.
fn deep(levels: usize) -> Document {
    let mut doc = doc! {"_id": 1, "pad": "x".repeat(1 << 20)};
    for level in 0..levels {
        doc.insert(format!("a{level}"), vec![1, 2]);
    }
    doc
}

#[test]
fn a_fan_of_unwind_holds_its_document_and_its_elements_and_is_charged_for_them() {
    // Fifty `$unwind`s over fifty arrays: the first descent stands fifty fans, each holding
    // the document its array is unwound from. Charged, they are refused at the budget; they
    // were fifty uncounted copies, 50 MiB here and a member's memory with a larger document.
    let doc = deep(50);
    let one = held_bytes(&doc);
    let mut specs: Vec<Document> =
        (0..50).map(|level| doc! {"$unwind": format!("$a{level}")}).collect();
    specs.push(doc! {"$limit": 1});
    let stages = aggregate::parse(&specs).unwrap();
    let cap = 10 * one;
    let limits = capped(cap);
    let mut chain = chain_of(&stages, &limits);
    let err = chain.push(doc.clone()).unwrap_err();
    let Error::ValueTooLarge { what, bytes, max, .. } = &err else { panic!("{err:?}") };
    assert_eq!(*what, "the input of $unwind");
    assert_eq!(*max, cap);
    // Refused at the fan that took the hold over the cap: the eleventh, not the fiftieth.
    assert!(*bytes > cap && *bytes <= cap + one, "held {bytes} of {cap}, a fan is {one}");
    drop(chain);
    assert_eq!(limits.buffer.used(), 0, "gives everything back when refused");
    // Room for all fifty fans, and the same pipeline answers one document and holds nothing
    // after it.
    let limits = capped(60 * one);
    let mut chain = chain_of(&stages, &limits);
    assert_eq!(chain.push(doc).unwrap(), crate::pipeline::Flow::Done);
    // What is held is the answer's one document, and no fan.
    let held = limits.buffer.used();
    let out = chain.finish().unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(held, held_bytes(&out[0]), "the fans are given back when the limit is full");
    assert_eq!(limits.buffer.used(), 0);
}

#[test]
fn unwinds_that_do_not_fan_out_hold_nothing_however_many_there_are() {
    // 998 `$unwind`s of a path the document does not have, keeping it: each hands the
    // document on, and none holds a copy. The reviewer's shape, at a megabyte.
    let doc = deep(0);
    let one = held_bytes(&doc);
    let mut specs: Vec<Document> = (0..998)
        .map(|_| doc! {"$unwind": {"path": "$none", "preserveNullAndEmptyArrays": true}})
        .collect();
    specs.push(doc! {"$limit": 1});
    let stages = aggregate::parse(&specs).unwrap();
    // Room for the one document the answer holds, and no more.
    let limits = capped(one);
    let mut chain = chain_of(&stages, &limits);
    assert_eq!(chain.push(doc).unwrap(), crate::pipeline::Flow::Done);
    assert_eq!(chain.finish().unwrap().len(), 1);
    assert_eq!(limits.buffer.used(), 0);
}

#[test]
fn a_descent_of_single_element_unwinds_holds_one_document_as_the_loop_did() {
    // Fifty arrays of one element each, fifty `$unwind`s: each fan hands its one element out
    // with the document itself, and keeps nothing, so the descent holds the document once
    // (the loop held it twice, as a stage's input and its output, and so did not hold fifty).
    let mut doc = deep(0);
    for level in 0..50 {
        doc.insert(format!("a{level}"), vec![1]);
    }
    let one = held_bytes(&doc);
    let mut specs: Vec<Document> =
        (0..50).map(|level| doc! {"$unwind": format!("$a{level}")}).collect();
    specs.push(doc! {"$limit": 1});
    let stages = aggregate::parse(&specs).unwrap();
    // Room for the document in flight, and the one the answer holds: a few documents' worth.
    let limits = capped(3 * one);
    let mut chain = chain_of(&stages, &limits);
    assert_eq!(chain.push(doc).unwrap(), crate::pipeline::Flow::Done);
    assert_eq!(chain.finish().unwrap().len(), 1);
    assert_eq!(limits.buffer.used(), 0);
}

#[test]
fn a_fan_is_charged_for_what_it_still_holds_and_nothing_after_its_elements_are_out() {
    // Two elements: the fan keeps the document for the second and is charged for it and the
    // element still to come; once the second is out, the document went with it. A deeper
    // fan's descent is therefore charged one document, not one for each level.
    let mut doc = deep(0);
    for level in 0..50 {
        doc.insert(format!("a{level}"), vec![1, 2]);
    }
    let one = held_bytes(&doc);
    let mut specs: Vec<Document> =
        (0..50).map(|level| doc! {"$unwind": format!("$a{level}")}).collect();
    specs.push(doc! {"$limit": 2});
    let stages = aggregate::parse(&specs).unwrap();
    // Fifty fans stand during the first descent (each is on its first of two elements), and
    // only the last element of each hands the document on: 60 documents is room for it.
    let limits = capped(60 * one);
    let mut chain = chain_of(&stages, &limits);
    assert_eq!(chain.push(doc).unwrap(), crate::pipeline::Flow::Done);
    assert_eq!(chain.finish().unwrap().len(), 2);
    assert_eq!(limits.buffer.used(), 0);
}

// ---------------------------------------------------------------------------
// The stages that hold a state and not their input (ADR-270)
// ---------------------------------------------------------------------------

/// The stages that keep their groups, each keeping the padding of what it is given.
fn grouping_stages() -> Vec<Document> {
    vec![
        doc! {"$group": {"_id": "$g", "p": {"$push": "$pad"}}},
        doc! {"$bucket": {"groupBy": "$g", "boundaries": [0, 1, 2, 3],
        "output": {"p": {"$push": "$pad"}}}},
        doc! {"$sortByCount": "$_id"},
    ]
}

#[test]
fn a_count_holds_a_counter_and_charges_the_budget_and_the_pool_nothing() {
    let pool = MemoryPool::new(1 << 30);
    let limits = pooled(0, &pool);
    let stages = aggregate::parse(&[doc! {"$count": "n"}]).unwrap();
    let mut chain = chain_of(&stages, &limits);
    for doc in padded(500, 200) {
        let _ = chain.push(doc).unwrap();
        assert_eq!((limits.buffer.used(), pool.used()), (0, 0));
    }
    assert_eq!(chain.finish().unwrap(), [doc! {"n": 500}]);
    assert_eq!((limits.buffer.used(), pool.used()), (0, 0));
}

/// [`chain_of`] with every stage a barrier, as the chain was built before the stages that keep
/// a state were sinks (ADR-270, P1).
fn barrier_chain_of<'a>(
    stages: &'a [aggregate::Stage],
    limits: &'a Limits,
) -> crate::pipeline::Chain<'a, Error> {
    let run: crate::pipeline::Runner<'static, Error> = Box::leak(Box::new({
        let limits = limits.clone();
        move |stage: &aggregate::Stage, input, arrived| {
            aggregate::apply_charged(stage, input, &limits, &[], arrived)
        }
    }));
    crate::pipeline::build_barriers(stages, run, limits, &[])
}

#[test]
fn a_state_holds_what_the_barrier_it_replaces_held_its_input_beside() {
    // Five hundred documents streamed to a `$count`, a `$group` of three buckets and a `$sort`
    // with a `$limit` of three: the barrier charged every document as it arrived (the whole
    // input, in the lease and in the pool), and the sink holds the counter, which is nothing,
    // the three buckets, and three documents with their keys.
    let input = padded(500, 300);
    let held_in = held_docs_bytes(&input);
    let cases = [
        vec![doc! {"$count": "n"}],
        vec![doc! {"$group": {"_id": "$g", "n": {"$sum": 1}}}],
        vec![doc! {"$sort": {"g": 1}}, doc! {"$limit": 3}],
    ];
    for specs in cases {
        let stages = aggregate::parse(&specs).unwrap();
        // What the chain holds, in its lease and in the pool, once the input has been pushed.
        let held = |barriers: bool| {
            let pool = MemoryPool::new(1 << 30);
            let limits = pooled(1 << 30, &pool);
            let mut chain = if barriers {
                barrier_chain_of(&stages, &limits)
            } else {
                chain_of(&stages, &limits)
            };
            for doc in &input {
                let _ = chain.push(doc.clone()).unwrap();
            }
            assert_eq!(pool.used(), limits.buffer.used(), "{specs:?}");
            limits.buffer.used()
        };
        let (barrier, sink) = (held(true), held(false));
        assert_eq!(barrier, held_in, "{specs:?}: the barrier held its input");
        if stages[0].name() == "$count" {
            assert_eq!(sink, 0, "a counter holds nothing");
        } else {
            assert!(sink > 0 && sink * 50 < held_in, "{specs:?}: {sink} of {held_in}");
        }
    }
}

#[test]
fn the_buckets_of_a_group_are_charged_as_they_grow_exactly_as_the_stage_charged_them() {
    let input = padded(60, 300);
    let held_in = held_docs_bytes(&input);
    for spec in grouping_stages() {
        let pool = MemoryPool::new(1 << 30);
        let limits = pooled(1 << 30, &pool);
        let stages = aggregate::parse(std::slice::from_ref(&spec)).unwrap();
        let mut chain = chain_of(&stages, &limits);
        let mut seen = Vec::new();
        for doc in &input {
            let _ = chain.push(doc.clone()).unwrap();
            seen.push(limits.buffer.used());
            assert_eq!(pool.used(), limits.buffer.used(), "{spec}");
        }
        // It grows with the buckets and the values they keep, and never holds the input.
        assert!(seen.windows(2).all(|w| w[1] >= w[0]) && seen[59] > seen[0], "{spec}: {seen:?}");
        let held = limits.buffer.used();
        // What the stage charged for its buckets when it held the whole input: the cheapest
        // budget it ran in was its input, and the buckets beside it.
        let barrier = smallest_cap(&spec, &input, 4 * held_in);
        assert_eq!(held, barrier - held_in, "{spec}: the state is what the buckets were");
        let out = chain.finish().unwrap();
        assert!(!out.is_empty(), "{spec}");
        assert_eq!((limits.buffer.used(), pool.used()), (0, 0), "{spec}: given back");
    }
}

#[test]
fn the_buckets_of_a_group_are_refused_at_the_byte_past_the_limit_and_the_pool_is_given_back() {
    let input = padded(60, 300);
    for spec in grouping_stages() {
        let stages = aggregate::parse(std::slice::from_ref(&spec)).unwrap();
        let name = crate::budget::state_of(stages[0].name());
        // The state at the thirtieth document, found by running to it.
        let pool = MemoryPool::new(1 << 30);
        let limits = pooled(1 << 30, &pool);
        let mut chain = chain_of(&stages, &limits);
        for doc in &input[..30] {
            let _ = chain.push(doc.clone()).unwrap();
        }
        let cap = limits.buffer.used();
        drop(chain);
        assert_eq!((limits.buffer.used(), pool.used()), (0, 0));
        // A budget of exactly that holds thirty; the thirty-first is refused at its byte.
        let limits = pooled(cap, &pool);
        let mut chain = chain_of(&stages, &limits);
        for doc in &input[..30] {
            let _ = chain.push(doc.clone()).unwrap();
        }
        let err = chain.push(input[30].clone()).unwrap_err();
        let Error::ValueTooLarge { what, bytes, max, .. } = &err else { panic!("{err:?}") };
        assert_eq!((*what, *max), (name, cap), "{spec}");
        assert!(*bytes > cap, "{spec}: refused at {bytes}");
        drop(chain);
        assert_eq!((limits.buffer.used(), pool.used()), (0, 0), "{spec}: gives it all back");
    }
}

#[test]
fn the_heap_of_a_sort_holds_the_documents_it_keeps_and_not_the_input() {
    let input = padded(100, 300);
    let sort = doc! {"$sort": {"g": -1, "_id": 1}};
    let pool = MemoryPool::new(1 << 30);
    // What one document costs the heap: itself and its two keys.
    let entry = {
        let limits = pooled(1 << 30, &pool);
        let stages = aggregate::parse(&[sort.clone(), doc! {"$limit": 4}]).unwrap();
        let mut chain = chain_of(&stages, &limits);
        let _ = chain.push(input[0].clone()).unwrap();
        limits.buffer.used()
    };
    assert!(entry > held_docs_bytes(&input[..1]), "the keys are charged with the document");
    // Room for five: the four it keeps and the one it takes before it turns another out. A
    // `$sort` of the hundred and a `$limit` of four is answered, and the budget and the pool
    // are empty afterwards.
    let limits = pooled(5 * entry + entry / 2, &pool);
    let stages = aggregate::parse(&[sort.clone(), doc! {"$limit": 4}]).unwrap();
    let mut chain = chain_of(&stages, &limits);
    let mut most = 0;
    for doc in &input {
        let _ = chain.push(doc.clone()).unwrap();
        most = most.max(limits.buffer.used());
        assert_eq!(pool.used(), limits.buffer.used());
    }
    // Four held, each with its keys: not the hundred, which no budget here holds.
    assert!(most > 3 * entry && most <= 4 * entry + entry / 2, "held {most}, an entry is {entry}");
    let out = chain.finish().unwrap();
    assert_eq!(out.iter().map(|d| d.get_i32("_id").unwrap()).collect::<Vec<_>>(), [2, 5, 8, 11]);
    assert_eq!((limits.buffer.used(), pool.used()), (0, 0));
    // The same sort with a `$skip` holds the documents it skips as well: seven, and a budget of
    // six is refused at the byte of the seventh.
    let limits = pooled(6 * entry, &pool);
    let stages = aggregate::parse(&[sort, doc! {"$skip": 3}, doc! {"$limit": 4}]).unwrap();
    let mut chain = chain_of(&stages, &limits);
    let err = input
        .iter()
        .find_map(|doc| chain.push(doc.clone()).err())
        .expect("seven documents do not fit in six");
    let Error::ValueTooLarge { what, bytes, max, .. } = &err else { panic!("{err:?}") };
    assert_eq!((*what, *max), ("the heap of $sort", 6 * entry));
    assert!(*bytes > 6 * entry && *bytes < 7 * entry + entry / 2, "refused at {bytes}");
    drop(chain);
    assert_eq!((limits.buffer.used(), pool.used()), (0, 0), "gives it all back when refused");
}

#[test]
fn the_reservoir_of_a_sample_holds_the_documents_it_keeps_and_gives_them_back() {
    let input = padded(200, 300);
    let pool = MemoryPool::new(1 << 30);
    let limits = pooled(1 << 30, &pool);
    let stages = aggregate::parse(&[doc! {"$sample": {"size": 5}}]).unwrap();
    let one = {
        let mut chain = chain_of(&stages, &limits);
        let _ = chain.push(input[0].clone()).unwrap();
        limits.buffer.used()
    };
    // Room for the five it keeps and nothing else: two hundred documents go through it, the
    // ones that replace another are charged and the ones they replace given back one for one.
    let limits = pooled(6 * one, &pool);
    let mut chain = chain_of(&stages, &limits);
    let mut most = 0;
    for doc in &input {
        let _ = chain.push(doc.clone()).unwrap();
        most = most.max(limits.buffer.used());
        assert_eq!(pool.used(), limits.buffer.used());
    }
    assert!(most > 4 * one && most < 6 * one, "held {most}, a document is {one}");
    assert_eq!(chain.finish().unwrap().len(), 5);
    assert_eq!((limits.buffer.used(), pool.used()), (0, 0));
    // One document less of room and the sixth is refused at its byte.
    let limits = pooled(4 * one + one / 2, &pool);
    let mut chain = chain_of(&stages, &limits);
    let err = input.iter().find_map(|doc| chain.push(doc.clone()).err()).expect("refused");
    let Error::ValueTooLarge { what, max, .. } = &err else { panic!("{err:?}") };
    assert_eq!((*what, *max), ("the reservoir of $sample", 4 * one + one / 2));
    drop(chain);
    assert_eq!((limits.buffer.used(), pool.used()), (0, 0), "gives it all back when refused");
}
