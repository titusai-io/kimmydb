//! A request waiting in the aggregation pool is ended by the node's stop, and answered as the
//! shutdown answers any request it cut (ADR-266, ADR-269): `503 node_stopping`, with the pool
//! at 0 afterwards.
//!
//! The stop is a flag for the whole process that nothing clears, so this is a binary of its
//! own with one test.

use std::time::{Duration, Instant};

use kimmy_api::AggregateMemory;
use kimmy_api::error::{ApiError, ErrorCode};
use kimmy_core::deadline::{self, Deadline};

#[test]
fn a_request_waiting_in_the_pool_is_ended_by_the_stop_and_answered_node_stopping() {
    // A pool of 100: two older requests hold 10 each and ask for 50 more; a younger one holds
    // 70 and never lets go, so both older ones wait. One waits under a deadline of an hour,
    // the other under none at all.
    let memory = AggregateMemory::new(100, 100);
    let pool = memory.pool().clone();
    let lease_of = |bytes: usize| {
        let limits = memory.limits();
        let mut lease = limits.buffer.lease("the source read");
        lease.charge(bytes).unwrap();
        (limits, lease)
    };
    let (limits_a, mut first) = lease_of(10);
    let (limits_b, mut second) = lease_of(10);
    // Give the waits a distant bound of their own: only the stop can end them.
    limits_a.buffer.wait_until(Instant::now() + Duration::from_secs(60));
    limits_b.buffer.wait_until(Instant::now() + Duration::from_secs(60));
    let (_limits_y, held) = lease_of(70);

    let started = Instant::now();
    let under_a_deadline = std::thread::spawn(move || {
        let _hour = Deadline::scope(Some(Duration::from_secs(3600)));
        let result = first.charge(50);
        (result, first)
    });
    let under_none = std::thread::spawn(move || {
        let result = second.charge(50);
        (result, second)
    });
    std::thread::sleep(Duration::from_millis(100));
    assert!(!under_a_deadline.is_finished() && !under_none.is_finished(), "both are waiting");
    assert_eq!(pool.pool_refusals(), 0);

    deadline::stop_requests();
    for waiter in [under_a_deadline, under_none] {
        let (result, lease) = waiter.join().unwrap();
        let error = result.expect_err("the stop ended the wait");
        assert!(matches!(error, kimmy_core::Error::DeadlineExceeded), "{error:?}");
        let answered = ApiError::from(error);
        assert_eq!(answered.status.as_u16(), 503);
        assert_eq!(answered.code, ErrorCode::NodeStopping);
        assert!(answered.message.contains("shutting down"), "{}", answered.message);
        drop(lease);
    }
    assert!(started.elapsed() < Duration::from_secs(10), "ended by the stop, not the bound");
    assert_eq!(pool.pool_refusals(), 0, "a stop is not a refusal of the pool");
    drop(held);
    drop((limits_a, limits_b));
    assert_eq!((pool.used(), pool.holders()), (0, 0));
}
