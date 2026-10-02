//! The thread that feeds an [`Evaluator`] (ADR-213).
//!
//! **A dedicated OS thread, never a runtime task.** It decides every E, takes **no
//! lock the runtime takes** and holds no engine: it reads atomics (the heartbeat
//! cells and the probe's marks) and one [`PeerView`] load. A stalled worker holding
//! a `Members` lock therefore cannot block it, and cannot void the very tick that
//! would judge the stall.
//!
//! **The control** is the thread's own step: each wake it records the gap since the
//! previous wake over [`EVAL_STEP`], work included ([`ControlTracker`]). Each step's
//! deadline is **the previous wake plus the step**, never a fixed grid, so a pause
//! gives one long step and never a burst of zero-length ones. A tick is exactly
//! `steps_per_tick` steps; the evaluator never catches up missed ticks.
//!
//! **The freeze.** [`EvaluatorHandle::freeze`], called at the stop's first signal,
//! makes the thread issue no verdict, no transition and no publication from then on
//! and unparks it so it exits at once. The stop does not join it.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use kimmy_core::CollectionId;
use tracing::{info, warn};

use super::{
    ClassSample, Config, ControlTracker, EVAL_STEP, Evaluator, Event, Published, Sample,
    TickMeasure,
};
use crate::facts::PerClass;
use crate::membership::PeerView;

/// A monotonic clock the thread waits on, which a test replaces.
pub trait Clock: Send + Sync + 'static {
    /// Time since the clock's own origin.
    fn now(&self) -> Duration;
    /// Block until `now() >= deadline` or [`Self::wake`]; may return early, and the
    /// caller loops.
    fn wait_until(&self, deadline: Duration);
    /// End a wait at once.
    fn wake(&self);
}

/// The real clock: `park_timeout` until the deadline.
pub struct RealClock {
    origin: Instant,
}

impl RealClock {
    pub fn new() -> Self {
        Self { origin: Instant::now() }
    }

    /// The instant this clock reads zero at.
    pub fn origin(&self) -> Instant {
        self.origin
    }
}

impl Default for RealClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for RealClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }

    fn wait_until(&self, deadline: Duration) {
        let now = self.now();
        if now < deadline {
            std::thread::park_timeout(deadline - now);
        }
    }

    fn wake(&self) {}
}

/// A clock a test moves by hand.
#[derive(Default)]
pub struct ManualClock {
    state: Mutex<(Duration, bool)>,
    changed: Condvar,
}

impl ManualClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn advance(&self, by: Duration) {
        self.state.lock().expect("manual clock").0 += by;
        self.changed.notify_all();
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Duration {
        self.state.lock().expect("manual clock").0
    }

    fn wait_until(&self, deadline: Duration) {
        let mut state = self.state.lock().expect("manual clock");
        while state.0 < deadline && !state.1 {
            state = self.changed.wait(state).expect("manual clock");
        }
    }

    fn wake(&self) {
        self.state.lock().expect("manual clock").1 = true;
        self.changed.notify_all();
    }
}

/// What the driver reads each tick. Every method is atomics or an `ArcSwap` load:
/// none may take a lock a runtime task takes.
pub trait Inputs: Send + 'static {
    /// The probe's wake counter.
    fn probe_wakes(&self) -> u64;
    /// The probe's worst lateness since the last call, reset to zero.
    fn probe_take_worst(&self) -> Duration;
    fn classes(&self) -> PerClass<ClassSample>;
    /// The collections this member's TTL pass owned at its last completed pass.
    fn owned_ttl(&self) -> Option<Vec<CollectionId>>;
    fn ttl_holders(&self) -> Vec<CollectionId>;
    fn stopping(&self) -> bool {
        false
    }
    fn view(&self) -> Arc<PeerView>;
    /// What `KIMMY_TEST_KILL_TASK` asks of this thread, if anything: read each
    /// tick, so a panic lands at the next tick and a stall stops the ticking.
    fn kill_switch(&self) -> Option<KillSwitch> {
        None
    }
}

/// What the test switch asks of the evaluator thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KillSwitch {
    /// Panic at the next tick, which the supervisor reads as a death.
    Panic,
    /// Stop ticking (and so publishing) while the thread lives on: the facts
    /// source then advertises `unknown` after two ticks (A12).
    Stall,
}

/// An evaluator ready to run, with the instant its clock reads zero at.
pub struct Prepared {
    evaluator: Evaluator,
    origin: Instant,
}

/// Build the evaluator and **publish its start decision**: with probation every
/// class is `stalled` and the yield bits are set before the first tick, so the
/// very first block this process sends already yields. Called before anything
/// sends a block; the thread that runs it may start after.
pub fn prepare(config: Config, clock: &dyn Clock, published: &Published) -> Prepared {
    let now = Instant::now();
    let origin = now.checked_sub(clock.now()).unwrap_or(now);
    let (evaluator, initial) = Evaluator::start(config, now);
    published.store_start(&initial, clock.now().as_millis() as u64);
    log_events(&initial.events);
    Prepared { evaluator, origin }
}

struct Shared {
    frozen: AtomicBool,
    steps: AtomicU64,
    /// The thread has read its clock and fixed its first deadline.
    ready: AtomicBool,
}

/// A running evaluator thread, as the stop and the tests see it.
#[derive(Clone)]
pub struct EvaluatorHandle {
    shared: Arc<Shared>,
    published: Arc<Published>,
    clock: Arc<dyn Clock>,
    thread: Arc<Mutex<Option<std::thread::Thread>>>,
}

impl EvaluatorHandle {
    pub fn new(clock: Arc<dyn Clock>, published: Arc<Published>) -> Self {
        Self {
            shared: Arc::new(Shared {
                frozen: AtomicBool::new(false),
                steps: AtomicU64::new(0),
                ready: AtomicBool::new(false),
            }),
            published,
            clock,
            thread: Arc::new(Mutex::new(None)),
        }
    }

    pub fn published(&self) -> &Arc<Published> {
        &self.published
    }

    /// Control steps taken since start.
    pub fn steps(&self) -> u64 {
        self.shared.steps.load(Ordering::SeqCst)
    }

    /// The thread has read the clock and fixed its first deadline: a test that moves a
    /// manual clock waits for this first, or its first step is lost.
    pub fn is_ready(&self) -> bool {
        self.shared.ready.load(Ordering::SeqCst)
    }

    pub fn is_frozen(&self) -> bool {
        self.shared.frozen.load(Ordering::SeqCst)
    }

    /// Freeze: no verdict, no transition, no publication from now on; the thread
    /// exits at its next wake, which this makes immediate. **Not a join**: the
    /// thread holds no engine and no lock, so the stop pays nothing for it.
    pub fn freeze(&self) {
        self.shared.frozen.store(true, Ordering::SeqCst);
        self.clock.wake();
        if let Some(thread) = self.thread.lock().expect("thread slot").as_ref() {
            thread.unpark();
        }
    }
}

/// The thread's body: runs until frozen. Under `kimmy_task::supervise_thread` in
/// kimmyd, where a panic ends the process.
pub fn run(prepared: Prepared, inputs: impl Inputs, handle: &EvaluatorHandle) {
    let Prepared { mut evaluator, origin } = prepared;
    let steps_per_tick = evaluator.config().timings.steps_per_tick();
    let clock = &handle.clock;
    let published = &handle.published;
    *handle.thread.lock().expect("thread slot") = Some(std::thread::current());
    let start = clock.now();
    let mut tracker = ControlTracker::new(start);
    let mut last_wakes = inputs.probe_wakes();
    let mut next = start + EVAL_STEP;
    let mut stall_said = false;
    handle.shared.ready.store(true, Ordering::SeqCst);
    loop {
        while clock.now() < next && !handle.is_frozen() {
            clock.wait_until(next);
        }
        if handle.is_frozen() {
            return;
        }
        let at = clock.now();
        // The next step is measured from this wake, never from a grid.
        next = at + EVAL_STEP;
        tracker.wake(at);
        handle.shared.steps.fetch_add(1, Ordering::SeqCst);
        if tracker.steps() < steps_per_tick {
            continue;
        }
        let worst = inputs.probe_take_worst();
        let wakes = inputs.probe_wakes();
        let woke = wakes != last_wakes;
        last_wakes = wakes;
        let measure: TickMeasure = tracker.take(woke, worst);
        // A stop that began while this tick was measured: no verdict.
        if handle.is_frozen() {
            return;
        }
        match inputs.kill_switch() {
            Some(KillSwitch::Panic) => {
                panic!("KIMMY_TEST_KILL_TASK asked yield_evaluator to panic")
            }
            Some(KillSwitch::Stall) => {
                if !stall_said {
                    stall_said = true;
                    warn!(task = "yield_evaluator", secs = "until the stop", "test stall began");
                }
                continue;
            }
            None => {}
        }
        let mut sample = Sample::new(measure, inputs.classes(), inputs.view());
        sample.stopping = inputs.stopping();
        sample.owned_ttl = inputs.owned_ttl();
        sample.ttl_holders = inputs.ttl_holders();
        let decision = evaluator.tick(&sample, origin + at);
        // The stop may have frozen the decision while this tick ran: what it
        // decided is never published, and the frozen decision stands.
        if handle.is_frozen() {
            return;
        }
        published.store(&decision, at.as_millis() as u64);
        log_events(&decision.events);
        // A class that could not be yielded for want of a target says, once, why each
        // live peer is not one: what a `no_target` was, when it needs explaining.
        for event in &decision.events {
            if let Event::Suppressed { class, reason: super::Suppression::NoTarget } = event {
                warn!(
                    class = class.label(),
                    peers = ?super::explain_no_target(&sample, *class, origin + at),
                    "no live peer is a target for the class"
                );
            }
        }
    }
}

/// Start the thread, named `yield-evaluator`: the convenience tests use.
pub fn spawn_evaluator(
    config: Config,
    inputs: impl Inputs,
    clock: Arc<dyn Clock>,
    published: Arc<Published>,
) -> io::Result<EvaluatorHandle> {
    let prepared = prepare(config, clock.as_ref(), &published);
    let handle = EvaluatorHandle::new(clock, published);
    let body = handle.clone();
    let builder = std::thread::Builder::new().name("yield-evaluator".into());
    // UNSUPERVISED: a convenience for tests; the daemon runs `run` on its own supervised thread, where a panic ends the process
    builder.spawn(move || run(prepared, inputs, &body))?;
    Ok(handle)
}

fn log_events(events: &[Event]) {
    for event in events {
        match event {
            Event::Stalled { class, cause, bad, of } => info!(
                class = class.label(),
                cause = cause.label(),
                bad,
                of,
                "a class of owned work is stalled on this member"
            ),
            Event::Yielded { class, cause, bad, of, targets } => warn!(
                class = class.label(),
                cause = cause.label(),
                evidence = %format!("{bad}/{of}"),
                targets = ?targets,
                "this member yields a class of owned work: it cannot do it, and a peer can"
            ),
            Event::Reclaimed { class, r_ticks, step, yielded } => info!(
                class = class.label(),
                r_ticks,
                backoff_step = step,
                yielded,
                "this member owns a class of work again after good ticks"
            ),
            Event::Withdrew { class, reason } => warn!(
                class = class.label(),
                reason = reason.label(),
                "this member took a yielded class back while it is still stalled"
            ),
            Event::Suppressed { class, reason } => warn!(
                class = class.label(),
                reason = reason.label(),
                "a stalled class could not be yielded, so this member keeps owning it"
            ),
        }
    }
}
