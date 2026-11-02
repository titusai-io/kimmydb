//! The request's memory budget (ADR-269): what each stage charges, when it is refused,
//! and the pool the budgets draw on.

use std::sync::{Arc, Barrier};

use bson::{Bson, Document, doc};
use kimmy_core::Error;

use crate::aggregate::{self, Limits};
use crate::budget::{
    BufferBudget, DEFAULT_MEMORY_LIMIT_BYTES, DEFAULT_MEMORY_POOL_BYTES, MemoryPool, doc_bytes,
    docs_bytes,
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
    let pool = MemoryPool::new(1_000);
    // A stage that fails with the budget partly charged.
    let limits = pooled(1_000, &pool);
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
    let budget = BufferBudget::pooled(1_000, &pool);
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
// What each stage charges
// ---------------------------------------------------------------------------

#[test]
fn a_stage_that_builds_documents_holds_its_input_and_its_output_at_once() {
    let input = padded(20, 200);
    let held_in = docs_bytes(&input);
    for stage in [
        doc! {"$addFields": {"extra": "$pad"}},
        doc! {"$set": {"extra": "$pad"}},
        doc! {"$project": {"p": "$pad", "g": 1}},
        doc! {"$replaceRoot": {"newRoot": {"p": "$pad"}}},
        doc! {"$replaceWith": {"p": "$pad"}},
    ] {
        let out = run(stage.clone(), input.clone(), &capped(usize::MAX / 4)).unwrap();
        let held_out = docs_bytes(&out);
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
    let held_in = docs_bytes(&input);
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
    let held_in = docs_bytes(&input);
    let stage = doc! {"$unwind": "$tags"};
    let out = run(stage.clone(), input.clone(), &capped(usize::MAX / 4)).unwrap();
    assert_eq!(out.len(), 80);
    let both = held_in + docs_bytes(&out);
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
    let held_in = docs_bytes(&input);
    let stage = doc! {"$group": {"_id": "$g", "all": {"$push": "$pad"}}};
    // The pushed values are most of what the buckets hold: a 305-byte string and a type byte
    // each, and a little for each of the three buckets.
    let pushed = 30 * (1 + 5 + 300);
    let smallest = smallest_cap(&stage, &input, held_in + 4 * pushed);
    assert!(smallest >= held_in + pushed, "the pushed values are charged: {smallest}");
    assert!(smallest <= held_in + pushed + 3 * 200, "and little else is: {smallest}");
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
    let held_in = docs_bytes(&input);
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
    let held_in = docs_bytes(&input);
    let stage = doc! {"$group": {"_id": null, "k": {"$topN": {"output": "$pad", "sortBy": {"_id": 1}, "n": 2}}}};
    let smallest = smallest_cap(&stage, &input, held_in + 100_000);
    // Two rows of 500 bytes are kept at a time, however many are read.
    assert!(smallest - held_in < 4 * 600, "{}", smallest - held_in);
}

#[test]
fn bucket_and_bucket_auto_and_sort_by_count_hold_their_buckets() {
    let input = padded(60, 200);
    let held_in = docs_bytes(&input);
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
    let held_in = docs_bytes(&input);
    let stage = doc! {"$redact": "$$KEEP"};
    let out = run(stage.clone(), input.clone(), &capped(usize::MAX / 4)).unwrap();
    let both = held_in + docs_bytes(&out);
    assert!(run(stage.clone(), input.clone(), &capped(both)).is_ok());
    assert!(is_too_large(&run(stage, input, &capped(both - 1)).unwrap_err()));
}

#[test]
fn the_blocking_stages_charge_their_buffers_once_not_twice() {
    // `$setWindowFields` holds the documents it buffers and the values it adds; the input
    // is not charged a second time.
    let input: Vec<Document> =
        (0..20).map(|i| doc! {"_id": i, "g": i % 2, "pad": "x".repeat(100)}).collect();
    let held_in = docs_bytes(&input);
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
    let err = run(stage.clone(), input.clone(), &capped(docs_bytes(&input) + 1_500)).unwrap_err();
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
    let held_in = docs_bytes(&input);
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
    let held_in = docs_bytes(&input);
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
    let held_in = docs_bytes(&input);
    let branch = doc! {"$facet": {"a": [{"$addFields": {"e": "$pad"}}, {"$count": "n"}]}};
    // The branch's `$addFields` holds its input and its output, and the facet nothing more
    // (it hands over its only copy).
    let out =
        run(doc! {"$addFields": {"e": "$pad"}}, input.clone(), &capped(usize::MAX / 4)).unwrap();
    let need = held_in + docs_bytes(&out);
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
    let held_in = docs_bytes(&input);
    let stage = doc! {"$fill": {"output": {"big": {"value": "y".repeat(2_000)}}}};
    let out = run(stage.clone(), input.clone(), &capped(usize::MAX / 4)).unwrap();
    let held_out = docs_bytes(&out);
    let one = big_element(2_000);
    assert_eq!(held_out, held_in + 50 * one);
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
    let held_in = docs_bytes(&input);
    let int_v = crate::window::element_bytes("v", &Bson::Int32(0));
    let one = big_element(2_000);
    let locf = doc! {"v": {"method": "locf"}};
    let both = doc! {"v": {"method": "locf"}, "big": {"value": "y".repeat(2_000)}};
    for (name, output, added) in [("locf", locf, 25 * int_v), ("both", both, 25 * int_v + 50 * one)]
    {
        let stage = doc! {"$fill": {"sortBy": {"_id": 1}, "output": output}};
        let out = run(stage.clone(), input.clone(), &capped(usize::MAX / 4)).unwrap();
        assert_eq!(docs_bytes(&out), held_in + added, "{name}");
        // The input once, the values written, and the window's sort keys and cells (under
        // 1 KB for these 50 documents): a second charge of the input would be 11 KB more.
        let smallest = smallest_cap(&stage, &input, 10 * held_in);
        assert!(smallest >= held_in + added, "{name}: {smallest}");
        assert!(smallest < held_in + added + 1_000, "{name}: {smallest}");
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
    let held_in = docs_bytes(&input);
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
    let big = "q".repeat(1_000);
    let input: Vec<Document> =
        (0..40).map(|i| doc! {"_id": i * 10, "g": i, "tags": [1, 2, 3], "pad": "x"}).collect();
    let held_in = docs_bytes(&input);
    let value = Bson::String(big.clone());
    let cases: Vec<(&str, Document, usize)> = vec![
        ("$addFields", doc! {"$addFields": {"big": big.as_str()}}, big_element(1_000)),
        ("$project", doc! {"$project": {"big": big.as_str()}}, big_element(1_000)),
        ("$replaceRoot", doc! {"$replaceRoot": {"newRoot": {"big": big.as_str()}}}, 1_040),
        ("$fill", doc! {"$fill": {"output": {"big": {"value": big.as_str()}}}}, big_element(1_000)),
        (
            "$setWindowFields",
            doc! {"$setWindowFields": {"sortBy": {"_id": 1},
            "output": {"all": {"$push": "$pad", "window": {"documents": ["unbounded", "current"]}},
            "big": {"$first": value.clone()}}}},
            // The largest value it adds to one document: the last one's array.
            big_element(1_000),
        ),
        (
            "$densify",
            doc! {"$densify": {"field": "_id", "range": {"step": 1, "bounds": "full"}}},
            // A generated document: `_id` only, but there are 391 of them, 5 bytes in an
            // input charged beside them.
            40,
        ),
        (
            "$unwind",
            doc! {"$unwind": {"path": "$tags"}},
            // One copy of a document.
            120,
        ),
        ("$group", doc! {"$group": {"_id": null, "all": {"$push": big.as_str()}}}, 1_050),
        (
            "$bucket",
            doc! {"$bucket": {"groupBy": "$_id", "boundaries": [0, 1000],
            "output": {"all": {"$push": big.as_str()}}}},
            1_050,
        ),
        (
            "$facet",
            doc! {"$facet": {"a": [{"$addFields": {"big": big.as_str()}}], "b": [{"$count": "n"}]}},
            big_element(1_000),
        ),
    ];
    for (name, stage, unit) in cases {
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

#[test]
fn a_hundred_thousand_documents_of_four_kib_run_through_add_fields_at_an_explicit_limit_of_one_gib()
{
    // 100,000 documents of 4 KiB are about 400 MB; `$addFields` holds them and its output,
    // about 800 MB: under an explicit limit of 1 GiB, which is what this check needs (the
    // derived limit of a container under about 15.4 GiB refuses it; see the next test). Worked out
    // from real documents, charged through the real accounts, with nothing allocated.
    let n = 100_000usize;
    let one = doc! {"_id": 1_i32, "pad": "x".repeat(4_096)};
    let mut grown = one.clone();
    grown.insert("flag", true);
    let (size_in, size_out) = (doc_bytes(&one), doc_bytes(&grown));
    let limits = Limits { buffer: BufferBudget::new(1 << 30), ..Default::default() };
    let mut input = limits.buffer.lease("the input of $addFields");
    let mut output = limits.buffer.lease("the output of $addFields");
    for _ in 0..n {
        input.charge(size_in).unwrap();
    }
    for _ in 0..n {
        output.charge(size_out).unwrap();
    }
    let peak = limits.buffer.used();
    assert_eq!(peak, n * (size_in + size_out));
    assert!(peak > 800_000_000 && peak < 840_000_000, "{peak}");
    assert!(peak < limits.buffer.cap(), "{peak} runs under {}", limits.buffer.cap());
    // The answer is then the documents and their JSON: still under it.
    drop(input);
    // `{"_id":1,"pad":"<4096 bytes>","flag":true}`
    let json = r#"{"_id":1,"pad":""#.len() + 4_096 + r#"","flag":true}"#.len();
    let mut answer = limits.buffer.lease("the answer");
    for _ in 0..n {
        answer.charge(json).unwrap();
    }
    assert!(limits.buffer.used() < limits.buffer.cap(), "{}", limits.buffer.used());
}

#[test]
fn a_hundred_thousand_documents_of_four_kib_are_refused_at_the_derived_limits() {
    // The same 100,000 documents, 410 MB of BSON, against the limit a member derives. The
    // source read is charged whole before any stage runs and stops at the document that
    // passes the limit; `$addFields` holds input plus output, so it needs twice the source.
    let one = doc! {"_id": 1_i32, "pad": "x".repeat(4_096)};
    let mut grown = one.clone();
    grown.insert("flag", true);
    let (size, size_out) = (doc_bytes(&one), doc_bytes(&grown));
    assert_eq!((size, size_out), (4_120, 4_127));
    let n = 100_000usize;
    // About 824 MB are held at the peak, so `$addFields` runs from a container of about
    // 15.4 GiB (824 MB x 20), and not in 2 or 8 GiB.
    for (memory, runs) in
        [(2usize << 30, false), (8 << 30, false), (15 << 30, false), (16 << 30, true)]
    {
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
        // `$addFields` then holds the input again beside what it builds (7 bytes a document
        // more: the `flag` field). The read is the stage's input here.
        let mut built = budget.lease("the output of $addFields");
        let output_fits = read_fits && (0..n).all(|_| built.charge(size_out).is_ok());
        assert_eq!(output_fits, runs, "{} GiB: read refused at {at:?}", memory >> 30);
        if let Some(i) = at {
            assert_eq!(i, limit / size + 1, "{} GiB", memory >> 30);
        }
    }
    // A 2 GiB member refuses the read itself, at the 26,062nd document of the 100,000.
    assert_eq!(derived(2 << 30).0 / size + 1, 26_062);
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

#[test]
fn the_value_bound_and_the_budget_each_refuse_alone_and_count_the_same_bytes_once() {
    let input = padded(40, 1_000);
    let held_in = docs_bytes(&input);
    let stage = doc! {"$group": {"_id": null, "all": {"$push": "$pad"}}};
    // The array of 40 strings is 40 KB. The budget is what holds it: charged once beside the
    // input (the array's own size plus the bucket), not twice.
    let smallest = smallest_cap(&stage, &input, 4 * held_in);
    let array = 5 + 40 * (1 + 2 + 5 + 1_000);
    assert!(smallest >= held_in + array && smallest < held_in + array + 400, "{smallest}");
    // A budget that fits is refused by nothing; a budget one byte short names the buckets.
    let err = run(stage, input, &capped(smallest - 1)).unwrap_err();
    assert!(
        matches!(&err, Error::ValueTooLarge { what, .. } if what.starts_with("the buckets of $group"))
    );
}

#[test]
fn merge_objects_is_charged_by_the_document_it_builds_not_by_the_documents_it_reads() {
    // Forty documents each with a field of its own: the merged document holds all forty.
    let input: Vec<Document> = (0..40).map(|i| doc! {format!("f{i}"): "y".repeat(300)}).collect();
    let held_in = docs_bytes(&input);
    let stage = doc! {"$group": {"_id": null, "m": {"$mergeObjects": "$$ROOT"}}};
    let smallest = smallest_cap(&stage, &input, 4 * held_in);
    assert!(smallest >= held_in + 40 * 300, "the merged fields are charged: {smallest}");
    assert!(smallest < held_in + 40 * 330 + 400, "and once: {smallest}");
    // The same field set over and over is one field, however many documents set it.
    let same: Vec<Document> = (0..40).map(|_| doc! {"f": "y".repeat(300)}).collect();
    let held_same = docs_bytes(&same);
    let smallest = smallest_cap(&stage, &same, 4 * held_same);
    assert!(smallest < held_same + 600, "{smallest}");
}
