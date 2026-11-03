//! Counters of the work the merge state does, for tests that pin its complexity without a
//! clock. Compiled in tests only: the production code carries no counter.

use std::cell::Cell;

#[derive(Clone, Copy)]
pub(super) enum Counter {
    /// A walk over the whole state to list the writers it mentions.
    WriterScans,
}

thread_local! {
    static COUNTS: [Cell<usize>; 1] = const { [Cell::new(0)] };
}

pub(super) fn bump(c: Counter, n: usize) {
    COUNTS.with(|a| a[c as usize].set(a[c as usize].get() + n));
}

/// Run `f` and return what it counted.
pub(super) fn count<T>(f: impl FnOnce() -> T) -> (T, Counts) {
    COUNTS.with(|a| a.iter().for_each(|c| c.set(0)));
    let t = f();
    let c = COUNTS.with(|a| Counts { writer_scans: a[Counter::WriterScans as usize].get() });
    (t, c)
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Counts {
    pub writer_scans: usize,
}
