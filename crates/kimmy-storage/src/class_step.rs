//! The per-class heartbeat (ADR-213, section 6.3): one cell for each class of
//! owned work, made of atomics, that the class's own task beats as it works and
//! the yield evaluator reads from its own thread.
//!
//! # Scope: a task-local, not a thread-local
//!
//! `CLASS_CELL` is set by [`scope`] around a class task's whole future at spawn
//! (`ttl_expiry`, `webhook_dispatcher`, `embedding_worker`). Synchronous code the
//! task polls sees it, including inside [`crate::blocking`], which is
//! `block_in_place` and so runs on the same poll. **No other task sees it**, so
//! another task's commit on the same worker thread can never beat this class or
//! move it to `WriterGate`, which a thread-local would do across an `.await`.
//! Code reads it with `try_with`, a no-op outside a class task, so a request pays
//! one failed lookup.
//!
//! # What a cell says
//!
//! A [`Phase`] and when it began, packed into one `AtomicU64` so a reader can
//! never pair a new phase with an old time; when the class last beat; how many
//! local successes, local faults, remote faults and config faults it counted; how
//! many cycles it completed and how many of them failed locally with no success;
//! and how many items it owned at its last owner check. The evaluator reads
//! deltas. A class is *overdue* when it has gone longer than its phase's bound
//! without a beat or a change of phase; [`ClassCell::reading`] gives it both ages.
//!
//! Timestamps come from a [`StepClock`] a test injects, so a crate test can make
//! thirty seconds pass without waiting.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use kimmy_core::CollectionId;

/// Where a cell's timestamps come from: milliseconds on any monotonic origin.
pub trait StepClock: Send + Sync + 'static {
    fn now_ms(&self) -> u64;
}

/// The real clock: milliseconds since it was made.
#[derive(Debug)]
pub struct MonotonicClock {
    origin: std::time::Instant,
}

impl MonotonicClock {
    pub fn new() -> Self {
        Self { origin: std::time::Instant::now() }
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl StepClock for MonotonicClock {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

/// A class's phase: what it is doing, which decides how long it may go without a
/// beat.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Between turns of its loop, at its own wait.
    Waiting = 0,
    /// A step of local work: a scan, a flush, a pass over the store.
    Local = 1,
    /// Waiting on something remote, under a timer of ours: a delivery, one
    /// provider attempt. Stamped **per attempt**.
    Remote = 2,
    /// Waiting for the writer gate: never overdue, and never counted in another
    /// phase, so gate time needs no field of its own.
    WriterGate = 3,
}

impl Phase {
    fn from_bits(bits: u64) -> Self {
        match bits & 0b11 {
            0 => Self::Waiting,
            1 => Self::Local,
            2 => Self::Remote,
            _ => Self::WriterGate,
        }
    }
}

/// The three classes a cell can be for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Class {
    Ttl,
    Webhooks,
    Embeddings,
}

impl Class {
    pub const ALL: [Self; 3] = [Self::Ttl, Self::Webhooks, Self::Embeddings];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Ttl => "ttl",
            Self::Webhooks => "webhooks",
            Self::Embeddings => "embeddings",
        }
    }
}

const PHASE_SHIFT: u32 = 62;
const SINCE_MASK: u64 = (1 << PHASE_SHIFT) - 1;

/// One class's heartbeat. See the module documentation.
pub struct ClassCell {
    clock: Arc<dyn StepClock>,
    beat_at: AtomicU64,
    /// The phase in the top two bits, over the milliseconds it began at.
    phase_word: AtomicU64,
    owned: AtomicU64,
    /// Ttl only: the collections owned at the last completed pass, `None` until
    /// the first pass completes.
    owned_ttl: ArcSwapOption<Vec<CollectionId>>,
    local_ok: AtomicU64,
    local_fault: AtomicU64,
    remote_fault: AtomicU64,
    config_fault: AtomicU64,
    cycles: AtomicU64,
    cycles_bad: AtomicU64,
}

/// What a cell said at one moment: counters as they stood, and the two ages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reading {
    pub owned: u64,
    pub phase: Phase,
    /// Time since the last beat.
    pub since_beat: Duration,
    /// Time since the phase last changed.
    pub since_phase: Duration,
    pub local_ok: u64,
    pub local_fault: u64,
    pub remote_fault: u64,
    pub config_fault: u64,
    pub cycles: u64,
    pub cycles_bad: u64,
}

impl ClassCell {
    pub fn new(clock: Arc<dyn StepClock>) -> Self {
        let now = clock.now_ms();
        Self {
            clock,
            beat_at: AtomicU64::new(now),
            phase_word: AtomicU64::new(now & SINCE_MASK),
            owned: AtomicU64::new(0),
            owned_ttl: ArcSwapOption::empty(),
            local_ok: AtomicU64::new(0),
            local_fault: AtomicU64::new(0),
            remote_fault: AtomicU64::new(0),
            config_fault: AtomicU64::new(0),
            cycles: AtomicU64::new(0),
            cycles_bad: AtomicU64::new(0),
        }
    }

    /// A cell that lives for the life of the process, as the task-local needs.
    pub fn leak(clock: Arc<dyn StepClock>) -> &'static ClassCell {
        Box::leak(Box::new(Self::new(clock)))
    }

    fn now(&self) -> u64 {
        self.clock.now_ms()
    }

    /// Switch phase, stamping `since = now`. A beat too: a change of phase is
    /// progress.
    pub fn set_phase(&self, phase: Phase) {
        let now = self.now();
        self.phase_word
            .store(((phase as u64) << PHASE_SHIFT) | (now & SINCE_MASK), Ordering::SeqCst);
        self.beat_at.store(now, Ordering::SeqCst);
    }

    pub fn phase(&self) -> Phase {
        Phase::from_bits(self.phase_word.load(Ordering::SeqCst) >> PHASE_SHIFT)
    }

    pub fn beat(&self) {
        self.beat_at.store(self.now(), Ordering::SeqCst);
    }

    /// A local step succeeded.
    pub fn ok(&self) {
        self.local_ok.fetch_add(1, Ordering::SeqCst);
        self.beat();
    }

    /// A local step failed: the storage engine, the registry, a list.
    pub fn local_fault(&self) {
        self.local_fault.fetch_add(1, Ordering::SeqCst);
        self.beat();
    }

    /// A remote step failed: neutral, since a down endpoint is not this member's
    /// health.
    pub fn remote_fault(&self) {
        self.remote_fault.fetch_add(1, Ordering::SeqCst);
        self.beat();
    }

    /// A per-node configuration fault: neutral, and not health either.
    pub fn config_fault(&self) {
        self.config_fault.fetch_add(1, Ordering::SeqCst);
        self.beat();
    }

    /// A cycle completed (a pass, a dispatcher pass, a flush or ownership tick);
    /// `bad` when local faults rose in it and no local step succeeded.
    pub fn cycle(&self, bad: bool) {
        self.cycles.fetch_add(1, Ordering::SeqCst);
        if bad {
            self.cycles_bad.fetch_add(1, Ordering::SeqCst);
        }
        self.beat();
    }

    /// Items the class owned at its last owner check.
    pub fn set_owned(&self, owned: u64) {
        self.owned.store(owned, Ordering::SeqCst);
    }

    /// Ttl only: the collections owned at the pass that just completed.
    pub fn set_owned_ttl(&self, owned: Vec<CollectionId>) {
        self.owned_ttl.store(Some(Arc::new(owned)));
    }

    /// Ttl only: a pass ran without knowing what it owned (it ran while the member
    /// was catching up), so there is nothing to say. Reads as before the first
    /// pass: the target rule falls back on the member's own holder list.
    pub fn forget_owned_ttl(&self) {
        self.owned_ttl.store(None);
    }

    /// The collections owned at the last completed pass; `None` before the first,
    /// or after a pass that ran gated.
    pub fn owned_ttl(&self) -> Option<Arc<Vec<CollectionId>>> {
        self.owned_ttl.load_full()
    }

    /// Local successes so far, for a crate test.
    pub fn local_oks(&self) -> u64 {
        self.local_ok.load(Ordering::SeqCst)
    }

    pub fn reading(&self) -> Reading {
        let now = self.now();
        let word = self.phase_word.load(Ordering::SeqCst);
        let since = word & SINCE_MASK;
        let ms = |then: u64| Duration::from_millis(now.saturating_sub(then));
        Reading {
            owned: self.owned.load(Ordering::SeqCst),
            phase: Phase::from_bits(word >> PHASE_SHIFT),
            since_beat: ms(self.beat_at.load(Ordering::SeqCst)),
            since_phase: ms(since),
            local_ok: self.local_ok.load(Ordering::SeqCst),
            local_fault: self.local_fault.load(Ordering::SeqCst),
            remote_fault: self.remote_fault.load(Ordering::SeqCst),
            config_fault: self.config_fault.load(Ordering::SeqCst),
            cycles: self.cycles.load(Ordering::SeqCst),
            cycles_bad: self.cycles_bad.load(Ordering::SeqCst),
        }
    }
}

tokio::task_local! {
    /// The cell of the class the current task is the task of.
    static CLASS_CELL: &'static ClassCell;
}

/// Run `fut`, a class task's whole future, with `cell` visible to it and to
/// nothing else.
pub async fn scope<F: std::future::Future>(cell: &'static ClassCell, fut: F) -> F::Output {
    CLASS_CELL.scope(cell, fut).await
}

fn with<R>(f: impl FnOnce(&'static ClassCell) -> R) -> Option<R> {
    CLASS_CELL.try_with(|cell| f(cell)).ok()
}

/// Run `f` on the current class's cell, if there is one.
pub fn with_cell(f: impl FnOnce(&'static ClassCell)) {
    with(f);
}

/// Beat the current class, if there is one.
pub fn beat() {
    with(ClassCell::beat);
}

/// Switch the current class's phase.
pub fn phase(phase: Phase) {
    with(|cell| cell.set_phase(phase));
}

/// A local step succeeded.
pub fn ok() {
    with(ClassCell::ok);
}

/// A local step failed.
pub fn local_fault() {
    with(ClassCell::local_fault);
}

/// A remote step failed.
pub fn remote_fault() {
    with(ClassCell::remote_fault);
}

/// A configuration fault of this node.
pub fn config_fault() {
    with(ClassCell::config_fault);
}

/// A cycle completed.
pub fn cycle(bad: bool) {
    with(|cell| cell.cycle(bad));
}

/// A remote attempt begins: the phase is `Remote` with `since` now, so every
/// remote wait is bounded by a timer of ours **per attempt**.
pub fn remote_attempt() {
    with(|cell| cell.set_phase(Phase::Remote));
}

/// A remote attempt ended, whichever way: a beat, and the phase back to local.
pub fn remote_done() {
    with(|cell| cell.set_phase(Phase::Local));
}

/// A commit by the class: a local success.
pub(crate) fn commit_ok() {
    with(ClassCell::ok);
}

/// Run `wait`, the wait for the writer gate, with the class's phase switched to
/// `WriterGate` and back to what it was **with `since = now`** afterwards. Gate
/// time is therefore never counted in any phase.
pub(crate) fn around_gate<R>(wait: impl FnOnce() -> R) -> R {
    match with(|cell| (cell, cell.phase())) {
        None => wait(),
        Some((cell, before)) => {
            cell.set_phase(Phase::WriterGate);
            let held = wait();
            cell.set_phase(before);
            held
        }
    }
}

// ---------------------------------------------------------------------------
// KIMMY_TEST_FAIL_STEP
// ---------------------------------------------------------------------------

const NO_FAIL: u64 = u64::MAX;

static FAIL_AFTER_MS: [AtomicU64; 3] =
    [AtomicU64::new(NO_FAIL), AtomicU64::new(NO_FAIL), AtomicU64::new(NO_FAIL)];
static FAIL_ARMED_AT: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

fn slot(class: Class) -> usize {
    match class {
        Class::Ttl => 0,
        Class::Webhooks => 1,
        Class::Embeddings => 2,
    }
}

/// Ask for the class's local step to fail, `after` the node has begun serving
/// (`KIMMY_TEST_FAIL_STEP=<class>[:<after_secs>]`). Does nothing until
/// [`arm_test_fail_steps`].
pub fn set_test_fail_step(class: Class, after: Duration) {
    FAIL_AFTER_MS[slot(class)]
        .store(u64::try_from(after.as_millis()).unwrap_or(NO_FAIL - 1), Ordering::SeqCst);
}

/// The node is serving: the switch's delays count from here.
pub fn arm_test_fail_steps() {
    let _ = FAIL_ARMED_AT.set(std::time::Instant::now());
}

thread_local! {
    /// A test's own switch, for the thread it runs on: a crate test that must fail
    /// a step cannot use the process-wide one, which every test in the binary
    /// would see.
    static FAIL_ON_THIS_THREAD: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

/// Fail `class`'s local step on this thread only, for a test.
pub fn test_fail_step_on_this_thread(class: Class, on: bool) {
    FAIL_ON_THIS_THREAD.with(|bits| {
        let bit = 1 << slot(class);
        bits.set(if on { bits.get() | bit } else { bits.get() & !bit });
    });
}

/// Whether the class's local step should fail now **without calling the
/// backend**, so it can never trip the storage latch (ADR-188). The caller
/// returns `StorageError::Transaction("test fail step")`.
pub fn test_fail_step(class: Class) -> bool {
    if FAIL_ON_THIS_THREAD.with(|bits| bits.get() & (1 << slot(class)) != 0) {
        return true;
    }
    let after = FAIL_AFTER_MS[slot(class)].load(Ordering::SeqCst);
    if after == NO_FAIL {
        return false;
    }
    FAIL_ARMED_AT.get().is_some_and(|armed| armed.elapsed().as_millis() >= u128::from(after))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A clock a test moves by hand.
    struct Manual(AtomicU64);

    impl Manual {
        fn at(ms: u64) -> Arc<Self> {
            Arc::new(Self(AtomicU64::new(ms)))
        }

        fn advance(&self, ms: u64) {
            self.0.fetch_add(ms, Ordering::SeqCst);
        }
    }

    impl StepClock for Manual {
        fn now_ms(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn cell(clock: &Arc<Manual>) -> &'static ClassCell {
        ClassCell::leak(Arc::clone(clock) as Arc<dyn StepClock>)
    }

    /// A phase and when it began are one word, and a beat moves only its own age.
    #[test]
    fn the_phase_and_its_start_are_read_together_and_ages_follow_the_clock() {
        let clock = Manual::at(1_000);
        let cell = cell(&clock);
        clock.advance(10_000);
        cell.set_phase(Phase::Remote);
        clock.advance(7_000);
        assert_eq!(cell.reading().since_beat, Duration::from_secs(7), "a phase change is a beat");
        cell.beat();
        clock.advance(2_000);
        let reading = cell.reading();
        assert_eq!(reading.phase, Phase::Remote);
        assert_eq!(reading.since_phase, Duration::from_secs(9), "since the phase began");
        assert_eq!(reading.since_beat, Duration::from_secs(2), "since the beat");
    }

    #[test]
    fn counters_move_with_their_events() {
        let clock = Manual::at(0);
        let cell = cell(&clock);
        cell.ok();
        cell.local_fault();
        cell.remote_fault();
        cell.config_fault();
        cell.cycle(false);
        cell.cycle(true);
        cell.set_owned(4);
        let r = cell.reading();
        assert_eq!(
            (
                r.local_ok,
                r.local_fault,
                r.remote_fault,
                r.config_fault,
                r.cycles,
                r.cycles_bad,
                r.owned
            ),
            (1, 1, 1, 1, 2, 1, 4)
        );
        assert!(cell.owned_ttl().is_none(), "no pass has completed");
        cell.set_owned_ttl(vec![CollectionId(7)]);
        assert_eq!(cell.owned_ttl().unwrap().as_slice(), [CollectionId(7)]);
    }

    /// Only the class task sees its cell: a function called outside a scope is a
    /// no-op, and so is another task's.
    #[tokio::test(flavor = "current_thread")]
    async fn only_the_class_task_sees_its_cell() {
        let clock = Manual::at(0);
        let cell = cell(&clock);
        ok();
        assert_eq!(cell.local_oks(), 0, "outside a class task nothing is counted");
        scope(cell, async {
            ok();
            // Another task on the same thread, while this one is suspended.
            tokio::spawn(async { ok() }).await.unwrap();
            tokio::task::yield_now().await;
            ok();
        })
        .await;
        assert_eq!(cell.local_oks(), 2, "the class task's two, and not the other task's");
    }

    /// The gate is a phase of its own and gate time is counted in no other: the
    /// phase goes back with `since` re-stamped.
    #[tokio::test(flavor = "current_thread")]
    async fn waiting_for_the_gate_is_its_own_phase_and_restamps_the_phase_it_left() {
        let clock = Manual::at(0);
        let cell = cell(&clock);
        scope(cell, async {
            phase(Phase::Local);
            clock.advance(5_000);
            let during = around_gate(|| {
                clock.advance(100_000);
                cell.phase()
            });
            assert_eq!(during, Phase::WriterGate);
        })
        .await;
        let r = cell.reading();
        assert_eq!(r.phase, Phase::Local, "back to what it was");
        assert_eq!(
            r.since_phase,
            Duration::ZERO,
            "with its age re-stamped: the wait is not Local's"
        );
    }

    /// A9: a request's commit runs on the same worker thread while the class task
    /// is suspended at an `.await`, and the class's counters and phase do not move;
    /// the class's own commit is a local success.
    #[tokio::test(flavor = "current_thread")]
    async fn a_commit_by_the_class_counts_and_another_tasks_commit_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(crate::Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let coll = engine.create_collection("app", "things").unwrap();
        let clock = Manual::at(0);
        let cell = cell(&clock);
        scope(cell, async {
            phase(Phase::Local);
            // Suspended here, another task commits on this very thread.
            let other = {
                let engine = Arc::clone(&engine);
                let coll = coll.clone();
                tokio::spawn(async move { engine.insert(&coll, bson::doc! { "_id": 1 }).unwrap() })
            };
            other.await.unwrap();
            assert_eq!(cell.local_oks(), 0, "another task's commit is not the class's");
            assert_eq!(cell.phase(), Phase::Local, "and did not move its phase");
            engine.insert(&coll, bson::doc! { "_id": 2 }).unwrap();
        })
        .await;
        assert_eq!(cell.local_oks(), 1, "the class's own commit is one local success");
        assert_eq!(cell.phase(), Phase::Local, "the gate came and went");
    }

    #[test]
    fn a_test_fail_step_waits_to_be_armed() {
        set_test_fail_step(Class::Webhooks, Duration::ZERO);
        assert!(!test_fail_step(Class::Webhooks), "not armed: the node is not serving");
        assert!(!test_fail_step(Class::Ttl), "another class is not asked");
        arm_test_fail_steps();
        assert!(test_fail_step(Class::Webhooks));
        FAIL_AFTER_MS[slot(Class::Webhooks)].store(NO_FAIL, Ordering::SeqCst);
    }
}
