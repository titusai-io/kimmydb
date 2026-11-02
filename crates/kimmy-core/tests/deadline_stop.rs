//! The node's stop ends requests at their next check of the clock (ADR-266).
//!
//! The stop is a flag for the whole process that nothing clears, so it is set in a binary of
//! its own, by one test: set in the library's unit tests it would end every deadline test
//! that happens to be running beside it.

use std::time::Duration;

use kimmy_core::Error;
use kimmy_core::deadline::{
    Deadline, MAX_STRIDE, Ticker, check, clock_reads, requests_are_stopped, stop_requests, tick,
    tick_n,
};

#[test]
fn a_stop_ends_the_next_check_of_every_scope_and_spares_a_suspended_step() {
    assert!(!requests_are_stopped());
    // Before the stop: a deadline an hour away passes every check.
    let hour = Deadline::scope(Some(Duration::from_secs(3600)));
    check().unwrap();
    // Cheap ticks teach the shared count that ticks cost nothing, so that its stride is the
    // most; run to the read that follows, so that a full stride is left after it.
    for _ in 0..100_000 {
        tick().unwrap();
    }
    let reads = clock_reads();
    while clock_reads() == reads {
        tick().unwrap();
    }
    let mut ticker = Ticker::new();
    for _ in 0..10_000 {
        ticker.tick().unwrap();
    }

    stop_requests();
    assert!(requests_are_stopped());

    // The next tick that reads the clock fails, and it comes within a stride of ticks: the
    // ticks in between do not look, which is what makes a tick cheap.
    let mut ticks = 0u32;
    let outcome = loop {
        ticks += 1;
        if let Err(e) = tick() {
            break e;
        }
        assert!(ticks <= MAX_STRIDE, "no tick failed in a stride of {MAX_STRIDE}");
    };
    assert!(matches!(outcome, Error::DeadlineExceeded));
    // The same for a ticker, a bulk charge and an explicit check, which looks at once.
    let mut ticks = 0u32;
    while ticker.tick().is_ok() {
        ticks += 1;
        assert!(ticks <= MAX_STRIDE, "the ticker never failed");
    }
    assert!(matches!(tick_n(1_000_000), Err(Error::DeadlineExceeded)));
    assert!(matches!(check(), Err(Error::DeadlineExceeded)));
    // A scope begun after the stop fails at its first tick, whatever time it has.
    {
        let _later = Deadline::scope(Some(Duration::from_secs(3600)));
        assert!(matches!(tick(), Err(Error::DeadlineExceeded)));
        assert!(matches!(Ticker::new().tick(), Err(Error::DeadlineExceeded)));
    }
    // A step set aside (a write that has committed, ADR-192) is not stopped, and neither is
    // work with no deadline: nothing there looks at the clock.
    {
        let _written = Deadline::suspend();
        check().unwrap();
        for _ in 0..5_000 {
            tick().unwrap();
        }
        tick_n(1_000_000).unwrap();
    }
    drop(hour);
    check().unwrap();
    tick().unwrap();
    // And the guard still restores: the stop is not a deadline of this thread.
    assert!(!Deadline::is_set());
}
