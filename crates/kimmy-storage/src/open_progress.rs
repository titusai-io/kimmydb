//! What a store's open is doing, for a probe to answer while it runs.
//!
//! `Engine::open` can take minutes on a large store: redb's repair after an
//! unclean stop, the walk that verifies the version vector and the live
//! counts' rebuild all run before it returns. The daemon
//! listens before it opens (ADR-198), and this is what its `/readyz` says in
//! the meantime: the phase, how long it has lasted, and how far through it is.
//!
//! One store is opened at a time in a daemon, so the state is process-wide:
//! an atomic phase, its start, a done and a total. It changes no behaviour of
//! the open, except that a stop asked for during it ([`request_open_stop`]) is
//! honoured where it can be taken safely: in the verification walk, which is
//! abandoned and simply not recorded, and between the chunks of the counts'
//! rebuild and the sweeps that bury what a drop covers.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

/// The phase of an open, in the order they occur.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum OpenPhase {
    /// No open is running, or the last has finished and the node is serving.
    Idle = 0,
    /// redb is repairing the file after an unclean stop: no count, about
    /// 0.4 s per GiB.
    Repairing = 1,
    /// Opening the file and reading its tables.
    Opening = 2,
    /// The walk that verifies the version vector against the oplog: rows
    /// read of the oplog's rows.
    Verifying = 3,
    /// Rebuilding the live document counts.
    Counting = 4,
    /// The store is open and the node is starting its tasks, before it
    /// serves.
    Starting = 5,
    /// Burying what a database's drop covers (ADR-229): `done` of `total`
    /// databases, before the node serves.
    Reconciling = 6,
}

impl OpenPhase {
    /// Every phase a probe reports, `Idle` (no open running) left out. A test
    /// holds this to the numbering and to the phases the OpenAPI document lists.
    pub const fn reported() -> [OpenPhase; 6] {
        [
            Self::Repairing,
            Self::Opening,
            Self::Verifying,
            Self::Counting,
            Self::Starting,
            Self::Reconciling,
        ]
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Repairing => "repairing",
            Self::Opening => "opening",
            Self::Verifying => "verifying",
            Self::Counting => "counting",
            Self::Starting => "starting",
            Self::Reconciling => "reconciling",
        }
    }

    fn from_u8(n: u8) -> Self {
        match n {
            1 => Self::Repairing,
            2 => Self::Opening,
            3 => Self::Verifying,
            4 => Self::Counting,
            5 => Self::Starting,
            6 => Self::Reconciling,
            _ => Self::Idle,
        }
    }
}

/// A reading of the open in progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenSnapshot {
    pub phase: OpenPhase,
    /// How long the phase has lasted.
    pub phase_age: Duration,
    /// How far through it, when it has a count (`total > 0`).
    pub done: u64,
    pub total: u64,
}

static PHASE: AtomicU8 = AtomicU8::new(0);
/// When the phase began, in milliseconds since the first reading of the clock.
static SINCE_MS: AtomicU64 = AtomicU64::new(0);
static DONE: AtomicU64 = AtomicU64::new(0);
static TOTAL: AtomicU64 = AtomicU64::new(0);
static STOP: AtomicBool = AtomicBool::new(false);
fn now_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    u64::try_from(START.get_or_init(Instant::now).elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Enter `phase`, with no count.
pub fn set_open_phase(phase: OpenPhase) {
    #[cfg(test)]
    tests::THIS_THREAD_PHASE.with(|seen| seen.set(Some((phase, 0))));
    #[cfg(test)]
    tests::THIS_THREAD_DONE.with(|seen| seen.set(0));
    DONE.store(0, Relaxed);
    TOTAL.store(0, Relaxed);
    SINCE_MS.store(now_ms(), Relaxed);
    PHASE.store(phase as u8, Relaxed);
}

/// Enter `phase` with a total to count towards.
pub(crate) fn set_open_phase_of(phase: OpenPhase, total: u64) {
    set_open_phase(phase);
    #[cfg(test)]
    tests::THIS_THREAD_PHASE.with(|seen| seen.set(Some((phase, total))));
    TOTAL.store(total, Relaxed);
}

/// How far through the current phase.
pub(crate) fn set_open_done(done: u64) {
    #[cfg(test)]
    tests::THIS_THREAD_DONE.with(|seen| seen.set(done));
    DONE.store(done, Relaxed);
}

/// The open in progress, as a probe reads it.
pub fn open_snapshot() -> OpenSnapshot {
    let phase = OpenPhase::from_u8(PHASE.load(Relaxed));
    let since = SINCE_MS.load(Relaxed);
    OpenSnapshot {
        phase,
        phase_age: Duration::from_millis(now_ms().saturating_sub(since)),
        done: DONE.load(Relaxed),
        total: TOTAL.load(Relaxed),
    }
}

/// Ask the open in progress to stop at its next safe point. Idempotent.
pub fn request_open_stop() {
    STOP.store(true, Relaxed);
}

/// Whether a stop has been asked for.
pub fn open_stop_requested() -> bool {
    #[cfg(test)]
    if tests::THIS_THREAD_STOPS.with(std::cell::Cell::get) {
        return true;
    }
    STOP.load(Relaxed)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::cell::Cell;

    use bson::doc;

    use crate::Engine;

    /// A phase added to the numbering and not to [`OpenPhase::reported`] is one
    /// the OpenAPI test never looks for.
    #[test]
    fn every_phase_the_numbering_knows_is_reported() {
        use super::OpenPhase;
        let reported = OpenPhase::reported();
        for n in 0..=u8::MAX {
            let phase = OpenPhase::from_u8(n);
            if phase != OpenPhase::Idle {
                assert!(
                    reported.contains(&phase),
                    "phase {n} ({}) is not in `reported`",
                    phase.label()
                );
                assert_eq!(phase as u8, n);
            }
        }
        for phase in reported {
            assert_eq!(OpenPhase::from_u8(phase as u8), phase);
        }
    }

    thread_local! {
        /// The phase and total this thread last entered. The real state is one
        /// set of atomics for the process, and an open running in another test
        /// moves it, so a test that asks what its own open is doing reads this.
        pub(crate) static THIS_THREAD_PHASE: Cell<Option<(super::OpenPhase, u64)>> =
            const { Cell::new(None) };
        /// A stop asked for on this thread only. The real request is one flag for
        /// the process, and a test that raised it would stop every open running
        /// beside it in the same test binary.
        pub(crate) static THIS_THREAD_STOPS: Cell<bool> = const { Cell::new(false) };
        /// How far this thread's open said it was, beside its phase.
        pub(crate) static THIS_THREAD_DONE: Cell<u64> = const { Cell::new(0) };
    }

    /// The verification walk checks for a stop every 8,192 rows, and a stop asked
    /// for abandons it there: nothing is recorded, and the walk read only what it
    /// had reached. The same store with no stop is read to the end.
    #[test]
    fn a_stop_asked_for_during_the_open_abandons_the_verification_walk() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let coll = engine.create_collection("shop", "orders").unwrap();
        for batch in 0..5 {
            let docs =
                (0..2_000).map(|i| doc! { "_id": format!("d{batch}-{i}"), "n": i }).collect();
            engine.insert_many(&coll, docs).unwrap();
        }
        let rows = engine
            .serve_entries_to_peer(kimmy_core::Hlc::ZERO, usize::MAX, None, &[], None)
            .unwrap()
            .entries
            .len() as u64;
        assert!(rows > 8_192, "the fixture must be longer than one check interval: {rows}");

        let (_, whole, _) = Engine::rebuild_version_vector_if_stale(engine.db()).unwrap();
        assert_eq!(whole.rows, rows, "with no stop the walk reads the whole oplog");

        THIS_THREAD_STOPS.with(|stops| stops.set(true));
        let (raised, abandoned, _) = Engine::rebuild_version_vector_if_stale(engine.db()).unwrap();
        THIS_THREAD_STOPS.with(|stops| stops.set(false));
        assert!(!raised);
        assert_eq!(abandoned.rows, 8_192, "abandoned at the first check");
    }
}
