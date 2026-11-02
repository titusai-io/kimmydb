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
//! - [`Ticker`] is what a loop holds: [`Ticker::tick`] is a counter decrement on
//!   most calls and a [`check`] when the counter runs out. How many ticks that is
//!   adapts to what they cost (see below).
//! - [`tick`] is the same on a counter shared by the thread, for code with
//!   nowhere to keep a `Ticker`: the rows a walk yields, the elements an
//!   expression iterates. [`tick_n`] charges a number of ticks at once, for an
//!   operation that touches `n` elements in one call.
//!
//! # The cadence is driven by time
//!
//! A count of iterations says nothing about work: an iteration can cost a
//! nanosecond or a quarter of a second. So the clock is read every `N` ticks, with
//! `N` starting at 1, **doubling** (up to [`MAX_STRIDE`]) while the gap between two
//! reads stays under [`FAST`] (1 ms) and **halving** when it passes [`SLOW`] (10 ms).
//! Cheap ticks settle at about a read a millisecond (so the cost is a decrement per
//! tick and a 25 ns read per [`MAX_STRIDE`]), expensive ones at a read per tick, and
//! in between a read about every 1 to 10 ms of work, whatever a tick costs. What is
//! not covered is the first stretch after the cost jumps: a read a thousand cheap
//! ticks apart is followed by that many expensive ones before the next read, so a
//! new phase of work should start with [`check`] (which also restarts the shared
//! count at 1), as every pipeline stage does on entry.
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
//! ends work only at a check: a single operation that itself runs long without
//! ticking (one regular expression over a 16 MiB string) is not cut, and the bulk
//! array and string operators charge their size with [`tick_n`] before they run.

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};

/// Whether the node is stopping and its requests must end (ADR-266): set once by the
/// shutdown path at the drain deadline, read by every look at the clock.
static STOPPING: AtomicBool = AtomicBool::new(false);

/// The node is stopping: from now on every look at the clock made under a deadline fails
/// with [`Error::DeadlineExceeded`], so that a request thread running synchronous work ends
/// at its next check instead of holding the storage engine past the drain. Called by the
/// shutdown path at the drain deadline (the same moment `Engine::set_stopping` is, ADR-192),
/// not at the signal: a request that can finish inside the drain is let finish. Irreversible
/// for the life of the process. A step run under [`Deadline::suspend`] (a write that has
/// committed, ADR-192) has no deadline and is not stopped by this either.
pub fn stop_requests() {
    STOPPING.store(true, Ordering::SeqCst);
}

/// Whether [`stop_requests`] has been called: the reason a [`Error::DeadlineExceeded`]
/// is a shutdown and not a deadline that passed, for the API to answer it as the node
/// stopping.
pub fn requests_are_stopped() -> bool {
    STOPPING.load(Ordering::Relaxed)
}

/// Whether the work that looked at the clock at `now` must end: its deadline has come, or
/// the node is stopping.
#[inline]
fn over(now: Instant, deadline: Instant) -> bool {
    now >= deadline || STOPPING.load(Ordering::Relaxed)
}

/// The most ticks between two reads of the clock.
pub const MAX_STRIDE: u32 = 512;

/// A gap between two reads shorter than this doubles the stride.
pub const FAST: Duration = Duration::from_millis(1);

/// A gap between two reads longer than this halves the stride.
pub const SLOW: Duration = Duration::from_millis(10);

/// How the stride of one counter adapts: ticks left before the next read, the stride in
/// force, and when the clock was last read.
#[derive(Clone, Copy, Debug)]
struct Cadence {
    left: u32,
    stride: u32,
    last: Option<Instant>,
}

impl Cadence {
    const fn new() -> Self {
        Cadence { left: 1, stride: 1, last: None }
    }

    /// Charge `n` ticks; whether the clock is to be read now.
    #[inline]
    fn charge(&mut self, n: u32) -> bool {
        if self.left > n {
            self.left -= n;
            false
        } else {
            true
        }
    }

    /// The clock was read at `now`: adapt the stride to the gap since the last read and
    /// start counting again.
    fn read(&mut self, now: Instant) {
        if let Some(last) = self.last {
            let gap = now.saturating_duration_since(last);
            if gap < FAST {
                self.stride = (self.stride * 2).min(MAX_STRIDE);
            } else if gap > SLOW {
                self.stride = (self.stride / 2).max(1);
            }
        }
        if EVERY_TICK.get() {
            self.stride = 1;
        }
        self.last = Some(now);
        self.left = self.stride;
    }
}

thread_local! {
    /// When the synchronous call this thread is in must be done by, if it has one.
    static DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
    /// The cadence [`tick`] shares: ticks left, the stride, the last read. Restarted by a
    /// scope and by [`check`].
    static SHARED_LEFT: Cell<u32> = const { Cell::new(1) };
    static SHARED_STRIDE: Cell<u32> = const { Cell::new(1) };
    static SHARED_LAST: Cell<Option<Instant>> = const { Cell::new(None) };
    /// Ticks the shared count has taken in all, up to the last read: see [`ticks_taken`].
    static SHARED_TAKEN: Cell<u64> = const { Cell::new(0) };
    /// What `SHARED_LEFT` was set to at the last read.
    static SHARED_SET: Cell<u32> = const { Cell::new(1) };
    /// Reads of the clock [`check`] has made on this thread, for tests that count looks.
    static CLOCK_READS: Cell<u64> = const { Cell::new(0) };
    /// Whether every tick reads the clock: see [`look_at_every_tick`].
    static EVERY_TICK: Cell<bool> = const { Cell::new(false) };
}

/// Makes every tick on this thread read the clock until the guard drops, so that the number
/// of reads ([`clock_reads`]) is the number of ticks and checks, exactly. For a test that wants
/// to know that a loop ticks: the adaptive stride makes the count of reads depend on how fast
/// the loop runs, and a loop with a [`Ticker`] of its own has no other count (the shared
/// count's is [`ticks_taken`]). Compare the reads of two runs that differ only in the size of
/// the thing the loop walks, under a deadline that is far away.
#[must_use = "the stride is restored when the guard drops"]
pub fn look_at_every_tick() -> EveryTick {
    EveryTick { previous: EVERY_TICK.replace(true), _thread_bound: PhantomData }
}

/// Run `work` under a deadline an hour away with every tick reading the clock
/// ([`look_at_every_tick`]), and say how many reads of the clock it made: the number of ticks and
/// checks it took, exactly. A test that wants to know a loop ticks runs the work over two inputs
/// that differ in the one thing the loop walks, and holds the difference in reads to what each
/// walked item costs, which removing the loop's tick lowers.
pub fn looks_of<T>(work: impl FnOnce() -> T) -> (T, u64) {
    let _deadline = Deadline::scope(Some(Duration::from_secs(3600)));
    let _exact = look_at_every_tick();
    let before = clock_reads();
    let out = work();
    (out, clock_reads() - before)
}

/// The guard of [`look_at_every_tick`].
pub struct EveryTick {
    previous: bool,
    _thread_bound: PhantomData<*const ()>,
}

impl Drop for EveryTick {
    fn drop(&mut self) {
        EVERY_TICK.set(self.previous);
    }
}

fn restart_shared() {
    SHARED_LEFT.set(1);
    SHARED_STRIDE.set(1);
    SHARED_LAST.set(None);
    SHARED_SET.set(1);
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
        restart_shared();
        Deadline { previous, _thread_bound: PhantomData }
    }

    /// Remove the thread's deadline until the guard drops, for a step that must not be
    /// cut once it is begun: a bulk write that has committed a chunk finishes rather
    /// than stop half way (ADR-192, ADR-266).
    pub fn suspend() -> Deadline {
        let previous = DEADLINE.get();
        DEADLINE.set(None);
        restart_shared();
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
        restart_shared();
    }
}

/// How many times the clock has been read on this thread by a check made while a deadline
/// is set. For a test that wants to know a check was made.
pub fn clock_reads() -> u64 {
    CLOCK_READS.get()
}

/// How many ticks the shared count ([`tick`], [`tick_n`]) has taken on this thread, in all.
/// Exact, and independent of the clock: for a test that wants to know that a loop ticks
/// once for each of its iterations (compare it before and after, under a deadline that is far
/// away; nothing is counted while no deadline is set).
pub fn ticks_taken() -> u64 {
    SHARED_TAKEN.get() + u64::from(SHARED_SET.get().saturating_sub(SHARED_LEFT.get()))
}

/// Read the clock for `cadence`: adapt it and compare with the deadline.
fn look(cadence: &mut Cadence) -> Result<()> {
    match DEADLINE.get() {
        // No deadline: nothing to look at, and no reason to look again soon.
        None => {
            cadence.left = MAX_STRIDE;
            Ok(())
        }
        Some(deadline) => {
            CLOCK_READS.set(CLOCK_READS.get() + 1);
            let now = Instant::now();
            cadence.read(now);
            if over(now, deadline) { Err(Error::DeadlineExceeded) } else { Ok(()) }
        }
    }
}

/// `Ok` unless this thread's deadline has passed, in which case
/// [`Error::DeadlineExceeded`]. A read of the clock only when a deadline is set. It
/// also restarts the shared count at a stride of 1: a check marks the start of a new
/// phase of work, whose cost the count has not learned.
#[inline]
pub fn check() -> Result<()> {
    match DEADLINE.get() {
        None => Ok(()),
        Some(deadline) => {
            CLOCK_READS.set(CLOCK_READS.get() + 1);
            SHARED_TAKEN.set(SHARED_TAKEN.get() + u64::from(SHARED_SET.get() - SHARED_LEFT.get()));
            restart_shared();
            if over(Instant::now(), deadline) { Err(Error::DeadlineExceeded) } else { Ok(()) }
        }
    }
}

/// What a loop holds to look at the clock as often as its iterations' cost asks for.
#[derive(Debug)]
pub struct Ticker {
    cadence: Cadence,
}

impl Default for Ticker {
    fn default() -> Self {
        Ticker::new()
    }
}

impl Ticker {
    pub const fn new() -> Self {
        Ticker { cadence: Cadence::new() }
    }

    /// Count one iteration; read the clock when the count runs out: on the first tick, then
    /// after a stride that doubles while the reads are under a millisecond apart and
    /// halves when they are over ten (see the module documentation).
    #[inline]
    pub fn tick(&mut self) -> Result<()> {
        if self.cadence.charge(1) { look(&mut self.cadence) } else { Ok(()) }
    }

    /// Count `n` iterations at once, for one operation that touches `n` elements; reads
    /// the clock if that runs the count out.
    #[inline]
    pub fn tick_n(&mut self, n: usize) -> Result<()> {
        if self.cadence.charge(u32::try_from(n).unwrap_or(u32::MAX)) {
            look(&mut self.cadence)
        } else {
            Ok(())
        }
    }
}

/// [`Ticker::tick`] on a count shared by the thread, which restarts when a scope is entered
/// or [`check`] is called. For a loop that is entered many times and runs a few iterations
/// each (an expression's `$map` over a short array), where a `Ticker` of its own would
/// never learn what a tick costs; and for code that cannot keep a `Ticker`. Nothing is
/// counted while no deadline is set.
#[inline]
pub fn tick() -> Result<()> {
    tick_n(1)
}

/// [`tick`] for `n` iterations at once: the clock is read if that runs the count out. How a
/// bulk operation (`$range`, a set operator, a regular expression over a long string) makes
/// the work it is about to do count, before it does it.
#[inline]
pub fn tick_n(n: usize) -> Result<()> {
    if !Deadline::is_set() {
        return Ok(());
    }
    let n = u32::try_from(n).unwrap_or(u32::MAX);
    let left = SHARED_LEFT.get();
    if left > n {
        SHARED_LEFT.set(left - n);
        return Ok(());
    }
    shared_look(n)
}

#[cold]
fn shared_look(n: u32) -> Result<()> {
    // What the window took before this call, and this call's ticks in full.
    let taken = SHARED_SET.get() - SHARED_LEFT.get();
    SHARED_TAKEN.set(SHARED_TAKEN.get() + u64::from(taken) + u64::from(n));
    let mut cadence = Cadence { left: 0, stride: SHARED_STRIDE.get(), last: SHARED_LAST.get() };
    let outcome = look(&mut cadence);
    SHARED_STRIDE.set(cadence.stride);
    SHARED_LAST.set(cadence.last);
    SHARED_LEFT.set(cadence.left);
    SHARED_SET.set(cadence.left);
    outcome
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

    /// The ticks at which a cadence reads the clock when every tick costs `cost` on a fake
    /// clock, with the time of each read.
    fn simulate(
        cadence: &mut Cadence,
        base: Instant,
        now: &mut Duration,
        cost: Duration,
        ticks: u32,
    ) -> Vec<(u32, Duration)> {
        let mut reads = Vec::new();
        for i in 1..=ticks {
            *now += cost;
            if cadence.charge(1) {
                cadence.read(base + *now);
                reads.push((i, *now));
            }
        }
        reads
    }

    #[test]
    fn cheap_ticks_double_the_stride_up_to_the_most_and_read_at_the_first_tick() {
        let (base, mut now, mut c) = (Instant::now(), Duration::ZERO, Cadence::new());
        let reads = simulate(&mut c, base, &mut now, Duration::from_nanos(100), 20_000);
        let at: Vec<u32> = reads.iter().map(|r| r.0).collect();
        // The first tick, then the stride doubles: ticks 1, 2, 4, 8, ... 512 ...
        assert_eq!(&at[..5], &[1, 2, 4, 8, 16]);
        assert_eq!(c.stride, MAX_STRIDE);
        // And from there on one read in `MAX_STRIDE` ticks.
        let tail: Vec<u32> = at.windows(2).rev().take(5).map(|w| w[1] - w[0]).collect();
        assert!(tail.iter().all(|&gap| gap == MAX_STRIDE), "{tail:?}");
    }

    #[test]
    fn expensive_ticks_keep_the_stride_at_one_and_read_at_every_tick() {
        let (base, mut now, mut c) = (Instant::now(), Duration::ZERO, Cadence::new());
        let reads = simulate(&mut c, base, &mut now, Duration::from_millis(50), 100);
        assert_eq!(reads.len(), 100);
        assert_eq!(c.stride, 1);
    }

    #[test]
    fn the_stride_shrinks_when_the_cost_jumps_and_a_read_comes_within_a_stride_of_it() {
        let (base, mut now, mut c) = (Instant::now(), Duration::ZERO, Cadence::new());
        simulate(&mut c, base, &mut now, Duration::from_nanos(100), 20_000);
        assert_eq!(c.stride, MAX_STRIDE);
        // Each tick now costs 20 ms. The first read comes after what is left of the stride,
        // and every read after it halves the stride until it is 1.
        let reads = simulate(&mut c, base, &mut now, Duration::from_millis(20), 3_000);
        assert!(
            reads[0].0 <= MAX_STRIDE,
            "the first read after the jump is at tick {}",
            reads[0].0
        );
        assert_eq!(c.stride, 1);
        let strides_to_one = reads.iter().take_while(|r| r.0 <= 3_000).count();
        assert!(strides_to_one >= 11, "{strides_to_one} reads");
        let last = &reads[reads.len() - 10..];
        assert!(last.windows(2).all(|w| w[1].0 - w[0].0 == 1), "reads at every tick once it is 1");
    }

    #[test]
    fn whatever_a_tick_costs_the_clock_is_read_about_every_ten_milliseconds_of_work() {
        for cost in [
            Duration::from_nanos(10),
            Duration::from_nanos(500),
            Duration::from_micros(3),
            Duration::from_micros(40),
            Duration::from_micros(300),
            Duration::from_millis(1),
            Duration::from_millis(4),
            Duration::from_millis(9),
            Duration::from_millis(30),
            Duration::from_millis(400),
        ] {
            let (base, mut now, mut c) = (Instant::now(), Duration::ZERO, Cadence::new());
            // Enough ticks for an hour of the cheapest and a few hundred reads of the dearest.
            let ticks = if cost < Duration::from_micros(100) { 400_000 } else { 2_000 };
            let reads = simulate(&mut c, base, &mut now, cost, ticks);
            // After the stride has settled (the warm-up is a dozen reads), no gap between two reads is
            // longer than the cost of a tick or ten milliseconds, whichever is longer.
            let limit = cost.max(SLOW);
            let longest = reads.windows(2).skip(20).map(|w| w[1].1 - w[0].1).max().unwrap();
            assert!(longest <= limit, "cost {cost:?}: the longest gap is {longest:?}");
        }
    }

    #[test]
    fn a_charge_larger_than_what_is_left_reads_the_clock() {
        let mut c = Cadence { left: 500, stride: 500, last: None };
        assert!(!c.charge(499));
        assert!(c.charge(1), "the count runs out");
        let mut c = Cadence { left: 500, stride: 500, last: None };
        assert!(c.charge(10_000));
    }

    #[test]
    fn a_real_ticker_over_cheap_ticks_reads_the_clock_a_small_share_of_the_time() {
        let _guard = Deadline::scope(Some(Duration::from_secs(3600)));
        let before = reads();
        let mut ticker = Ticker::new();
        for _ in 0..2_000_000u32 {
            ticker.tick().unwrap();
        }
        let made = reads() - before;
        assert!(made >= 1);
        assert!(made < 2_000_000 / 100, "{made} reads for two million cheap ticks");
    }

    #[test]
    fn the_shared_count_counts_every_tick_and_a_check_restarts_its_stride() {
        let _guard = Deadline::scope(Some(Duration::from_secs(3600)));
        let before = ticks_taken();
        for _ in 0..5000 {
            tick().unwrap();
        }
        tick_n(700).unwrap();
        tick_n(3).unwrap();
        assert_eq!(ticks_taken() - before, 5703);
        // The count has learned that ticks are cheap and reads once in many. A check restarts
        // it at a stride of 1, so the next few hundred ticks read at 1, 2, 4, 8, ... 256.
        check().unwrap();
        let reads_before = reads();
        for _ in 0..600 {
            tick().unwrap();
        }
        assert!(reads() - reads_before >= 8, "{} reads", reads() - reads_before);
    }

    #[test]
    fn the_shared_tick_reads_the_clock_on_the_first_tick_of_a_scope() {
        let _guard = Deadline::scope(Some(Duration::from_secs(3600)));
        let before = reads();
        tick().unwrap();
        assert_eq!(reads() - before, 1);
        // A new scope restarts the count, so its first tick reads the clock.
        let _inner = Deadline::scope(Some(Duration::ZERO));
        assert!(matches!(tick(), Err(Error::DeadlineExceeded)));
    }

    #[test]
    fn a_bulk_charge_on_the_shared_count_reads_the_clock_at_once() {
        let _guard = Deadline::scope(Some(Duration::from_secs(3600)));
        tick().unwrap();
        for _ in 0..5 {
            tick().unwrap();
        }
        let before = reads();
        tick_n(1_000_000).unwrap();
        assert_eq!(reads() - before, 1);
        let _expired = Deadline::scope(Some(Duration::ZERO));
        assert!(matches!(tick_n(1_000_000), Err(Error::DeadlineExceeded)));
    }

    #[test]
    fn under_the_exact_guard_every_tick_reads_the_clock_and_the_stride_is_back_after_it() {
        let _guard = Deadline::scope(Some(Duration::from_secs(3600)));
        {
            let _exact = look_at_every_tick();
            let before = reads();
            let mut ticker = Ticker::new();
            for _ in 0..1_000 {
                ticker.tick().unwrap();
            }
            for _ in 0..1_000 {
                tick().unwrap();
            }
            assert_eq!(reads() - before, 2_000, "a read for every tick");
            // A bulk charge is one look, as ever.
            let before = reads();
            tick_n(5_000).unwrap();
            assert_eq!(reads() - before, 1);
        }
        let before = reads();
        let mut ticker = Ticker::new();
        for _ in 0..1_000 {
            ticker.tick().unwrap();
        }
        assert!(reads() - before < 100, "{} reads once the guard is gone", reads() - before);
    }

    #[test]
    fn a_ticker_fails_on_its_first_tick_when_the_deadline_has_passed() {
        let _guard = Deadline::scope(Some(Duration::ZERO));
        assert!(matches!(Ticker::new().tick(), Err(Error::DeadlineExceeded)));
    }

    #[test]
    fn a_ticker_notices_a_deadline_that_passes_in_the_middle_of_a_loop() {
        // The start is taken before the scope, so the deadline cannot come before 30 ms of it.
        let started = Instant::now();
        let _guard = Deadline::scope(Some(Duration::from_millis(30)));
        let mut ticker = Ticker::new();
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
