//! Cooperative cancellation of synchronous work at the request's deadline (ADR-266).
//!
//! The request timeout (ADR-099) fires when a request's future is still *pending*
//! at the deadline. Work that never yields, such as a pipeline running over its
//! documents on the worker thread, is never seen to be pending, so the timeout
//! could not end it. This module is how that work ends itself: the handler that
//! calls it sets a deadline for the duration of the call, and the loops under it
//! look at the clock now and then and stop with [`Error::DeadlineExceeded`], which
//! the API answers as the `503 timeout` the middleware gives.
//!
//! # The pieces
//!
//! - [`Deadline::scope`] sets a **thread-local** deadline and puts the previous
//!   one back when the guard drops, so scopes nest and a function that sets one
//!   cannot leak it into the next request that runs on the same thread. A nested
//!   scope never *extends* the deadline: the earlier of the two holds.
//! - [`check`] is the clock read: `Ok` with no deadline set, `Ok` before the
//!   deadline, [`Error::DeadlineExceeded`] at or after it.
//! - [`Ticker`] is what a loop holds: [`Ticker::tick`] is a counter increment on
//!   most calls and a [`check`] on the first and on every [`CADENCE`]-th after it.
//! - [`tick`] is the same cadence on a counter shared by the thread, for code with
//!   nowhere to keep a `Ticker`: the rows a walk yields, the elements an
//!   expression iterates.
//!
//! Outside a scope there is no deadline and every call here is a no-op.
//!
//! # Adding a check to a loop
//!
//! ```
//! use kimmy_core::deadline::{Deadline, Ticker};
//!
//! fn sum(values: &[u64]) -> kimmy_core::Result<u64> {
//!     let mut ticker = Ticker::new();
//!     let mut total = 0;
//!     for value in values {
//!         ticker.tick()?;
//!         total += value;
//!     }
//!     Ok(total)
//! }
//!
//! assert_eq!(sum(&[1, 2, 3]).unwrap(), 6);
//! let _expired = Deadline::scope(Some(std::time::Duration::ZERO));
//! assert!(sum(&[1, 2, 3]).is_err());
//! ```
//!
//! One `Ticker` per loop, made before it, with `ticker.tick()?;` as the first
//! statement of the body. A loop nested in another takes its own `Ticker`, or
//! calls [`tick`] when its iterations are cheap and short. The cost is a few
//! nanoseconds per iteration (see ADR-266 for the measurement), so a loop whose
//! iteration costs more than that has no reason to go without.
//!
//! # What this cannot do
//!
//! It is thread-local, so it holds for the thread that set it. A call that moves
//! work to another thread must set the scope inside that thread's closure. It
//! ends work only at a check: a single iteration that itself runs long is not cut.

use std::cell::Cell;
use std::marker::PhantomData;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};

/// How many ticks pass between two reads of the clock, the first tick counting.
pub const CADENCE: u32 = 1024;

thread_local! {
    /// When the synchronous call this thread is in must be done by, if it has one.
    static DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
    /// The ticks [`tick`] has counted since the deadline was last set.
    static TICKS: Cell<u32> = const { Cell::new(0) };
    /// Reads of the clock [`check`] has made on this thread, for tests that count looks.
    static CLOCK_READS: Cell<u64> = const { Cell::new(0) };
}

/// A deadline set for this thread, put back to what it was when dropped.
///
/// Not `Send`: the deadline is the thread's, and a guard dropped on another
/// thread would restore the wrong one. Do not hold one across an `.await`.
#[must_use = "the deadline is removed when the guard drops"]
pub struct Deadline {
    previous: Option<Instant>,
    _thread_bound: PhantomData<*const ()>,
}

impl Deadline {
    /// Set a deadline `remaining` from now for this thread, until the guard drops.
    ///
    /// `None` sets nothing and leaves any deadline already in force. A scope inside
    /// another keeps the earlier of the two deadlines, so a callee cannot give a
    /// request more time than its caller had. `Some(Duration::ZERO)` is a deadline
    /// that has already passed. A duration too long to add to the clock is no
    /// deadline.
    pub fn scope(remaining: Option<Duration>) -> Deadline {
        let previous = DEADLINE.get();
        let wanted = remaining.and_then(|left| Instant::now().checked_add(left));
        let next = match (previous, wanted) {
            (Some(held), Some(wanted)) => Some(held.min(wanted)),
            (held, wanted) => held.or(wanted),
        };
        DEADLINE.set(next);
        TICKS.set(0);
        Deadline { previous, _thread_bound: PhantomData }
    }

    /// Remove the thread's deadline until the guard drops, for a step that must not be
    /// cut once it is begun: a bulk write that has committed a chunk finishes rather
    /// than stop half way (ADR-192, ADR-266).
    pub fn suspend() -> Deadline {
        let previous = DEADLINE.get();
        DEADLINE.set(None);
        TICKS.set(0);
        Deadline { previous, _thread_bound: PhantomData }
    }

    /// Whether this thread has a deadline in force.
    pub fn is_set() -> bool {
        DEADLINE.get().is_some()
    }
}

impl Drop for Deadline {
    fn drop(&mut self) {
        DEADLINE.set(self.previous);
    }
}

/// How many times [`check`] has read the clock on this thread (only a check made while a
/// deadline is set reads it). For a test that wants to know a loop ticks without waiting
/// for a deadline to pass: with a deadline far away, run the loop over `n` iterations and
/// compare the reads with what `n` ticks make, one on the first tick and one on every
/// [`CADENCE`]-th after it.
pub fn clock_reads() -> u64 {
    CLOCK_READS.get()
}

/// `Ok` unless this thread's deadline has passed, in which case
/// [`Error::DeadlineExceeded`]. A read of the clock only when a deadline is set.
#[inline]
pub fn check() -> Result<()> {
    match DEADLINE.get() {
        None => Ok(()),
        Some(deadline) => {
            CLOCK_READS.set(CLOCK_READS.get() + 1);
            if Instant::now() >= deadline { Err(Error::DeadlineExceeded) } else { Ok(()) }
        }
    }
}

/// What a loop holds to look at the clock every [`CADENCE`] iterations.
#[derive(Debug, Default)]
pub struct Ticker {
    count: u32,
}

impl Ticker {
    pub const fn new() -> Self {
        Ticker { count: 0 }
    }

    /// Count one iteration; read the clock on the first and on every [`CADENCE`]-th
    /// after it (the 1st, 1025th, 2049th, ...), so a loop of a single iteration is
    /// checked and one of millions costs a counter increment on most of them.
    #[inline]
    pub fn tick(&mut self) -> Result<()> {
        let due = self.count == 0;
        self.count = if self.count + 1 == CADENCE { 0 } else { self.count + 1 };
        if due { check() } else { Ok(()) }
    }
}

/// [`Ticker::tick`] on a counter shared by the thread, which restarts when a scope
/// is entered. For a loop that is entered many times and runs a few iterations each
/// (an expression's `$map` over a short array), where a `Ticker` of its own would
/// never reach its cadence; and for code that cannot keep a `Ticker`. Nothing is
/// counted while no deadline is set.
#[inline]
pub fn tick() -> Result<()> {
    if !Deadline::is_set() {
        return Ok(());
    }
    let count = TICKS.get();
    TICKS.set(if count + 1 == CADENCE { 0 } else { count + 1 });
    if count == 0 { check() } else { Ok(()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reads() -> u64 {
        clock_reads()
    }

    #[test]
    fn without_a_scope_there_is_no_deadline_and_everything_passes() {
        assert!(!Deadline::is_set());
        assert!(check().is_ok());
        let mut ticker = Ticker::new();
        for _ in 0..5000 {
            ticker.tick().unwrap();
            tick().unwrap();
        }
        assert_eq!(reads(), 0, "no deadline, no clock read");
    }

    #[test]
    fn a_scope_with_time_left_passes_and_one_without_fails() {
        let _guard = Deadline::scope(Some(Duration::from_secs(3600)));
        assert!(Deadline::is_set());
        assert!(check().is_ok());
        drop(_guard);
        let _expired = Deadline::scope(Some(Duration::ZERO));
        assert!(matches!(check(), Err(Error::DeadlineExceeded)));
    }

    #[test]
    fn the_previous_deadline_is_restored_on_drop() {
        assert!(!Deadline::is_set());
        {
            let _outer = Deadline::scope(Some(Duration::ZERO));
            assert!(check().is_err());
            {
                let _inner = Deadline::scope(Some(Duration::from_secs(3600)));
                // The earlier deadline holds: an inner scope cannot extend it.
                assert!(check().is_err());
            }
            assert!(check().is_err(), "the outer deadline is back after the inner drops");
        }
        assert!(!Deadline::is_set(), "and no deadline once the outer drops");
        assert!(check().is_ok());
    }

    #[test]
    fn an_inner_scope_may_shorten_and_the_outer_is_restored_after_it() {
        let _outer = Deadline::scope(Some(Duration::from_secs(3600)));
        {
            let _inner = Deadline::scope(Some(Duration::ZERO));
            assert!(check().is_err());
        }
        assert!(check().is_ok(), "the outer scope had time left and has it again");
    }

    #[test]
    fn a_suspended_deadline_is_back_when_the_guard_drops() {
        let _outer = Deadline::scope(Some(Duration::ZERO));
        {
            let _suspended = Deadline::suspend();
            assert!(!Deadline::is_set());
            assert!(check().is_ok());
            tick().unwrap();
        }
        assert!(matches!(check(), Err(Error::DeadlineExceeded)));
    }

    #[test]
    fn a_scope_of_none_changes_nothing() {
        let _outer = Deadline::scope(Some(Duration::ZERO));
        let _none = Deadline::scope(None);
        assert!(check().is_err());
        drop(_none);
        assert!(check().is_err());
        drop(_outer);
        let _only_none = Deadline::scope(None);
        assert!(!Deadline::is_set());
    }

    #[test]
    fn a_duration_too_long_for_the_clock_is_no_deadline() {
        let _guard = Deadline::scope(Some(Duration::MAX));
        assert!(!Deadline::is_set());
        assert!(check().is_ok());
    }

    #[test]
    fn a_guard_is_released_when_the_scope_unwinds() {
        let caught = std::panic::catch_unwind(|| {
            let _guard = Deadline::scope(Some(Duration::ZERO));
            panic!("unwinds through the guard");
        });
        assert!(caught.is_err());
        assert!(!Deadline::is_set());
    }

    #[test]
    fn the_deadline_belongs_to_the_thread_that_set_it() {
        let _guard = Deadline::scope(Some(Duration::ZERO));
        std::thread::spawn(|| {
            assert!(!Deadline::is_set());
            assert!(check().is_ok());
        })
        .join()
        .unwrap();
        assert!(check().is_err());
    }

    #[test]
    fn a_ticker_reads_the_clock_on_the_first_tick_and_every_cadence_after() {
        let _guard = Deadline::scope(Some(Duration::from_secs(3600)));
        let before = reads();
        let mut ticker = Ticker::new();
        ticker.tick().unwrap();
        assert_eq!(reads() - before, 1, "the first tick reads the clock");
        for _ in 1..CADENCE {
            ticker.tick().unwrap();
        }
        assert_eq!(reads() - before, 1, "ticks 2..=1024 do not");
        ticker.tick().unwrap();
        assert_eq!(reads() - before, 2, "the 1025th does");
        for _ in 0..(3 * CADENCE) {
            ticker.tick().unwrap();
        }
        // Ticks 1026..=4097: the 2049th, 3073rd and 4097th read it.
        assert_eq!(reads() - before, 5);
    }

    #[test]
    fn a_ticker_fails_on_its_first_tick_when_the_deadline_has_passed() {
        let _guard = Deadline::scope(Some(Duration::ZERO));
        assert!(matches!(Ticker::new().tick(), Err(Error::DeadlineExceeded)));
    }

    #[test]
    fn a_ticker_notices_a_deadline_that_passes_in_the_middle_of_a_loop() {
        let _guard = Deadline::scope(Some(Duration::from_millis(30)));
        let mut ticker = Ticker::new();
        let started = Instant::now();
        let mut iterations = 0u64;
        let outcome = loop {
            if let Err(e) = ticker.tick() {
                break e;
            }
            iterations += 1;
            std::hint::black_box(iterations);
            if started.elapsed() > Duration::from_secs(10) {
                panic!("the loop was never stopped");
            }
        };
        assert!(matches!(outcome, Error::DeadlineExceeded));
        assert!(started.elapsed() >= Duration::from_millis(30));
    }

    #[test]
    fn the_shared_tick_has_the_same_cadence_and_restarts_with_a_scope() {
        let _guard = Deadline::scope(Some(Duration::from_secs(3600)));
        let before = reads();
        tick().unwrap();
        assert_eq!(reads() - before, 1);
        for _ in 1..CADENCE {
            tick().unwrap();
        }
        assert_eq!(reads() - before, 1);
        tick().unwrap();
        assert_eq!(reads() - before, 2);
        // A new scope restarts the count, so its first tick reads the clock.
        let _inner = Deadline::scope(Some(Duration::ZERO));
        assert!(matches!(tick(), Err(Error::DeadlineExceeded)));
    }

    /// The cost of a check, measured and bounded generously (the unit tests run
    /// unoptimised; ADR-266 records the optimised figures).
    #[test]
    fn the_cost_of_a_tick_and_of_a_check_is_small() {
        const N: u32 = 2_000_000;
        let _guard = Deadline::scope(Some(Duration::from_secs(3600)));
        let started = Instant::now();
        let mut ticker = Ticker::new();
        for _ in 0..N {
            ticker.tick().unwrap();
        }
        let per_tick = started.elapsed().as_nanos() as f64 / f64::from(N);

        let started = Instant::now();
        for _ in 0..N / 10 {
            check().unwrap();
        }
        let per_check = started.elapsed().as_nanos() as f64 / f64::from(N / 10);

        let started = Instant::now();
        for _ in 0..N {
            tick().unwrap();
        }
        let per_shared = started.elapsed().as_nanos() as f64 / f64::from(N);
        eprintln!(
            "deadline cost: Ticker::tick {per_tick:.1} ns, check {per_check:.1} ns, \
             shared tick {per_shared:.1} ns (per call, this build profile)"
        );
        assert!(per_tick < 500.0, "a tick costs {per_tick} ns");
        assert!(per_shared < 500.0, "a shared tick costs {per_shared} ns");
        assert!(per_check < 5_000.0, "a check costs {per_check} ns");
    }
}
