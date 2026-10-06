//! A member that cannot do a class of work yields it, judged by its own
//! heartbeats against its peers' (ADR-213).
//!
//! This module is the **pure** half: the [`Evaluator`] state machine, which is
//! fed one [`Sample`] per tick and an injected `now`, and answers with a
//! [`Decision`]. It reads no clock, takes no lock and owns no thread. The thread
//! that feeds it is [`driver`], and the lock-free view of the peers it reads is
//! [`crate::membership::PeerView`].
//!
//! # What it decides
//!
//! Per class (`ttl`, `webhooks`, `embeddings`) and per **non-void** tick it turns
//! the class's heartbeat counters into at most one evidence item (good or bad),
//! keeps a ring of them, and derives the class state `ok` / `idle` / `suspect` /
//! `stalled`. A `stalled` class yields only to a **target**, under a **cap**, and
//! never while the shared-fault **latch** is set; it leaves by **reclaim** after R
//! good ticks, with R doubling for repeated yields. The runtime is judged from a
//! control and a subject (see [`ControlTracker`]); a pause of the whole process
//! delays both and voids the tick instead of reading as a stall.
//!
//! Nothing here is advice to a running node until the thread that owns an
//! evaluator is started: an `Evaluator` that is never ticked changes nothing.

pub mod driver;
pub mod probation;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod thread_tests;

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use kimmy_core::{CollectionId, NodeId};

use crate::facts::{ClassState, Facts, OwnerClass, PerClass, StallCause, Yielding, may_own};
use crate::membership::PeerView;

// ---------------------------------------------------------------------------
// Constants (ADR-213, section 6.2). Every one is an expression over the code's
// own constants where it is tied to one, and asserted.
// ---------------------------------------------------------------------------

/// The evaluator's step, equal to the runtime stall probe's period.
pub const EVAL_STEP: Duration = Duration::from_millis(250);
/// Steps in one tick.
pub const STEPS_PER_TICK: u32 = 20;
/// E: the evaluator's tick. Equal to the default sync interval and to the worker's
/// ownership tick, and spans two dispatcher ticks.
pub const EVAL_TICK: Duration =
    Duration::from_millis(EVAL_STEP.as_millis() as u64 * STEPS_PER_TICK as u64);
/// S: the probe's worst lateness in a tick above which the runtime is stalled.
pub const RUNTIME_STALL: Duration = Duration::from_secs(1);
/// V: the control's worst step gap over [`EVAL_STEP`] above which a tick is void.
pub const VOID_GAP: Duration = Duration::from_millis(500);
/// The margin the probe's and the control's wake-up jitter may use.
pub const JITTER_MARGIN: Duration = Duration::from_millis(250);
/// Consecutive on-time control steps, in non-void ticks, with no probe wake that
/// make the runtime stalled: about 5 s of real time at any scale.
pub const NO_WAKE_STEPS: u32 = 20;
/// Non-void ticks of no stalled verdict that make a member `responsive`.
pub const RESPONSIVE_TICKS: usize = 6;

/// The longest a `Local` step may go without a beat.
pub const LOCAL_BOUND: Duration = Duration::from_secs(30);
/// W: the longest one hold of the writer may last before it is read as a wedge
/// (ADR-220). Twice [`LOCAL_BOUND`], and at least twelve times the warn line the
/// storage engine logs a long hold at, so a hold that is merely long is never
/// one. It does not apply to a holder whose work scales with the data it works
/// on (`WriterHolder::is_unbounded`): the evaluator is never given those.
pub const WRITER_WEDGE: Duration = Duration::from_secs(2 * LOCAL_BOUND.as_secs());

/// What a `Remote` wait's bound adds to our own timer: the timer firing, the
/// response handled, the in-client retry pause.
pub const REMOTE_GRACE: Duration = Duration::from_secs(15);

/// The numbers this module's bounds are tied to in other crates. Each of those
/// crates asserts that its own constant equals the one here, so a change there
/// that would loosen a bound fails to compile.
pub mod tied {
    use std::time::Duration;

    /// `kimmy_api::dispatch::TICK`.
    pub const DISPATCH_TICK: Duration = Duration::from_secs(2);
    /// `kimmy_api::dispatch::DELIVERY_TIMEOUT`.
    pub const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);
    /// `kimmy_vector`'s worker `OWNERSHIP_TICK`.
    pub const OWNERSHIP_TICK: Duration = Duration::from_secs(5);
    /// `kimmy_vector`'s worker `DEFERRAL_TICK`.
    pub const DEFERRAL_TICK: Duration = Duration::from_secs(5);
    /// `kimmy_vector`'s worker `RETRY_DELAY`.
    pub const RETRY_DELAY: Duration = Duration::from_secs(5);
    /// `kimmy_vector`'s provider `REQUEST_TIMEOUT`, which bounds one HTTP attempt,
    /// connect and DNS included.
    pub const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60);
}

/// `Waiting` webhooks: 15 dispatcher ticks.
pub const WEBHOOKS_WAITING: Duration =
    Duration::from_millis(tied::DISPATCH_TICK.as_millis() as u64 * 15);
/// `Remote` webhooks: one delivery's deadline plus grace.
pub const WEBHOOKS_REMOTE: Duration =
    Duration::from_millis((tied::DELIVERY_TIMEOUT.as_millis() + REMOTE_GRACE.as_millis()) as u64);
/// `Waiting` embeddings: 6 ownership ticks.
pub const EMBEDDINGS_WAITING: Duration =
    Duration::from_millis(tied::OWNERSHIP_TICK.as_millis() as u64 * 6);
/// `Remote` embeddings (HTTP): one attempt's timeout plus grace.
pub const EMBEDDINGS_REMOTE: Duration =
    Duration::from_millis((tied::ATTEMPT_TIMEOUT.as_millis() + REMOTE_GRACE.as_millis()) as u64);
/// The cycle of each class, which the evidence window follows: the dispatcher's
/// tick, and the worker's longer of its ownership tick and retry delay.
pub const WEBHOOKS_CYCLE: Duration = tied::DISPATCH_TICK;
pub const EMBEDDINGS_CYCLE: Duration =
    if tied::OWNERSHIP_TICK.as_millis() > tied::RETRY_DELAY.as_millis() {
        tied::OWNERSHIP_TICK
    } else {
        tied::RETRY_DELAY
    };

/// R: reclaim after this many consecutive good ticks, doubled for each new yield
/// of the class within [`BACKOFF_WINDOW`] of its last reclaim, up to
/// [`RECLAIM_MAX_TICKS`].
pub const RECLAIM_TICKS: u32 = 24;
pub const RECLAIM_MAX_TICKS: u32 = 384;
/// The window in which a new yield doubles R, and after which the back-off resets.
pub const BACKOFF_WINDOW: Duration = Duration::from_secs(30 * 60);
/// How long the shared-fault latch holds after fewer than two members are `stalled`.
pub const LATCH_HOLD: Duration = Duration::from_secs(30 * 60);
/// A start after a run shorter than this, following an unclean end, is probation.
pub const SHORT_RUN: Duration = Duration::from_secs(10 * 60);
/// The most free space, below which an unclean start is probation: this or 5% of the
/// filesystem, whichever is less.
pub const LOW_SPACE_CAP: u64 = 1 << 30;
/// The member count past which a missing evaluator tick advertises `unknown`: this
/// many ticks.
pub const STALE_TICKS: u32 = 2;

const _: () = {
    // E equals the default sync interval, so slowing sync never slows detection.
    assert!(EVAL_TICK.as_millis() == crate::peers::DEFAULT_SYNC_INTERVAL.as_millis());
    assert!(EVAL_TICK.as_millis() == tied::OWNERSHIP_TICK.as_millis());
    assert!(EVAL_TICK.as_millis() == tied::DEFERRAL_TICK.as_millis());
    // E spans two dispatcher ticks (and a bit).
    assert!(EVAL_TICK.as_millis() >= 2 * tied::DISPATCH_TICK.as_millis());
    // V + the step + the jitter margin fit inside S: a pause long enough to read as
    // a stall on the probe always shows on the control as well.
    assert!(
        VOID_GAP.as_millis() + EVAL_STEP.as_millis() + JITTER_MARGIN.as_millis()
            <= RUNTIME_STALL.as_millis()
    );
    // A Local bound is at least six ticks.
    assert!(LOCAL_BOUND.as_millis() >= 6 * EVAL_TICK.as_millis());
    // A wedge is longer than any step a class waits on, and than the line the
    // storage engine warns at by a wide margin.
    assert!(WRITER_WEDGE.as_millis() > LOCAL_BOUND.as_millis());
    assert!(WRITER_WEDGE.as_millis() >= 12 * kimmy_storage::WRITER_HOLD_WARN.as_millis());
    // Waiting embeddings covers six deferral ticks and six retry delays.
    assert!(EMBEDDINGS_WAITING.as_millis() >= 6 * tied::DEFERRAL_TICK.as_millis());
    assert!(EMBEDDINGS_WAITING.as_millis() >= 6 * tied::RETRY_DELAY.as_millis());
    // A Remote wait outlasts its own timer.
    assert!(WEBHOOKS_REMOTE.as_millis() > tied::DELIVERY_TIMEOUT.as_millis());
    assert!(EMBEDDINGS_REMOTE.as_millis() > tied::ATTEMPT_TIMEOUT.as_millis());
    assert!(NO_WAKE_STEPS as u128 * EVAL_STEP.as_millis() == EVAL_TICK.as_millis());
    assert!(RECLAIM_MAX_TICKS == RECLAIM_TICKS * 16);
    assert!(RESPONSIVE_TICKS >= 1);
};

/// N of M bad items in the window make a class `stalled`.
pub const fn n_of_m(class: OwnerClass) -> (usize, usize) {
    match class {
        OwnerClass::Ttl => (3, 5),
        OwnerClass::Webhooks | OwnerClass::Embeddings => (6, 12),
    }
}

/// The least tick `E`, the latch hold and the back-off window, scaled by one divisor
/// (the `KIMMY_TEST_YIELD_SCALE` switch). R stays in ticks; the physical constants
/// (S, V, the step) and the code-tied bounds are not scaled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timings {
    pub e: Duration,
    pub latch_hold: Duration,
    pub backoff_window: Duration,
}

impl Timings {
    pub const fn standard() -> Self {
        Self { e: EVAL_TICK, latch_hold: LATCH_HOLD, backoff_window: BACKOFF_WINDOW }
    }

    /// Divided by `k` (at least one control step per tick is kept).
    pub fn scaled(k: u32) -> Self {
        let k = k.max(1);
        let std = Self::standard();
        Self {
            e: (std.e / k).max(EVAL_STEP),
            latch_hold: std.latch_hold / k,
            backoff_window: std.backoff_window / k,
        }
    }

    /// Control steps per tick.
    pub fn steps_per_tick(&self) -> u32 {
        (self.e.as_millis() / EVAL_STEP.as_millis()).max(1) as u32
    }
}

/// A class's phase, as its heartbeat records it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Waiting,
    Local,
    Remote,
    /// Waiting for the writer gate: never overdue.
    WriterGate,
}

/// How long each phase may go without a beat before the class is overdue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bounds {
    pub waiting: Duration,
    pub local: Duration,
    /// `None` where the class has no `Remote` phase.
    pub remote: Option<Duration>,
}

impl Bounds {
    /// The bounds of `class`, from the code's own constants. `ttl_interval` is
    /// configuration; `e` is the (possibly scaled) tick.
    pub fn of(class: OwnerClass, ttl_interval: Duration, e: Duration) -> Self {
        match class {
            OwnerClass::Ttl => {
                Self { waiting: ttl_interval + e * 12, local: LOCAL_BOUND, remote: None }
            }
            OwnerClass::Webhooks => Self {
                waiting: WEBHOOKS_WAITING,
                local: LOCAL_BOUND,
                remote: Some(WEBHOOKS_REMOTE),
            },
            OwnerClass::Embeddings => Self {
                waiting: EMBEDDINGS_WAITING,
                local: LOCAL_BOUND,
                remote: Some(EMBEDDINGS_REMOTE),
            },
        }
    }

    pub fn of_phase(&self, phase: Phase) -> Option<Duration> {
        match phase {
            Phase::Waiting => Some(self.waiting),
            Phase::Local => Some(self.local),
            Phase::Remote => self.remote,
            Phase::WriterGate => None,
        }
    }
}

/// The evidence window of `class`: M ticks of the longer of E and the class's cycle.
pub fn window_of(class: OwnerClass, ttl_interval: Duration, e: Duration) -> Duration {
    let (_, m) = n_of_m(class);
    let cycle = match class {
        OwnerClass::Ttl => ttl_interval,
        OwnerClass::Webhooks => WEBHOOKS_CYCLE,
        OwnerClass::Embeddings => EMBEDDINGS_CYCLE,
    };
    e.max(cycle) * m as u32
}

/// R in ticks for a back-off step: 24, 48, 96, 192, 384.
pub fn reclaim_ticks(step: u32) -> u32 {
    RECLAIM_TICKS.saturating_mul(1u32.checked_shl(step).unwrap_or(u32::MAX)).min(RECLAIM_MAX_TICKS)
}

// ---------------------------------------------------------------------------
// The runtime's measure.
// ---------------------------------------------------------------------------

/// What the control and the subject measured in one tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TickMeasure {
    /// Control steps in the tick.
    pub steps: u32,
    /// L_eval: the worst gap between consecutive control wakes over [`EVAL_STEP`].
    pub control_worst_gap: Duration,
    /// The probe's worst lateness in the tick.
    pub probe_worst: Duration,
    /// The probe woke at least once in the tick.
    pub probe_woke: bool,
}

impl TickMeasure {
    /// A quiet tick of `steps` steps.
    pub fn quiet(steps: u32) -> Self {
        Self {
            steps,
            control_worst_gap: Duration::ZERO,
            probe_worst: Duration::ZERO,
            probe_woke: true,
        }
    }
}

/// Builds a [`TickMeasure`] from the control's wake times: **the gap between
/// consecutive wakes**, which includes the evaluator's own work, so a pause that
/// lands between parks or a log write that blocks shows in the control.
#[derive(Clone, Debug)]
pub struct ControlTracker {
    last_wake: Duration,
    worst: Duration,
    steps: u32,
}

impl ControlTracker {
    /// `start` is the clock reading the first step is measured from.
    pub fn new(start: Duration) -> Self {
        Self { last_wake: start, worst: Duration::ZERO, steps: 0 }
    }

    /// The control woke at `at`.
    pub fn wake(&mut self, at: Duration) {
        let gap = at.saturating_sub(self.last_wake).saturating_sub(EVAL_STEP);
        self.worst = self.worst.max(gap);
        self.last_wake = at;
        self.steps += 1;
    }

    pub fn steps(&self) -> u32 {
        self.steps
    }

    /// Close the tick with the probe's reading and start the next.
    pub fn take(&mut self, probe_woke: bool, probe_worst: Duration) -> TickMeasure {
        let measure = TickMeasure {
            steps: self.steps,
            control_worst_gap: self.worst,
            probe_worst,
            probe_woke,
        };
        self.worst = Duration::ZERO;
        self.steps = 0;
        measure
    }
}

// ---------------------------------------------------------------------------
// Samples and decisions.
// ---------------------------------------------------------------------------

/// One class's heartbeat, read at the tick. The counters are cumulative; the
/// evaluator reads their deltas.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClassSample {
    /// The class's owned count at its last owner check.
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
    /// Catching up (ADR-202): the class is gated and judged neutral.
    pub gated: bool,
    /// The operator switched the class off (`ttl_disabled`, `embeddings_disabled`).
    pub switched_off: bool,
}

impl ClassSample {
    /// A class that owns `owned`, waiting, just beaten, with nothing counted.
    pub fn at_rest(owned: u64) -> Self {
        Self {
            owned,
            phase: Phase::Waiting,
            since_beat: Duration::ZERO,
            since_phase: Duration::ZERO,
            local_ok: 0,
            local_fault: 0,
            remote_fault: 0,
            config_fault: 0,
            cycles: 0,
            cycles_bad: 0,
            gated: false,
            switched_off: false,
        }
    }
}

/// Everything one tick reads.
#[derive(Clone, Debug)]
pub struct Sample {
    pub measure: TickMeasure,
    pub classes: PerClass<ClassSample>,
    /// The member is stopping: every class is neutral.
    pub stopping: bool,
    /// The live set and every held block, from one atomic load.
    pub view: Arc<PeerView>,
    /// The collections this member's TTL pass owned at its last completed pass, or
    /// `None` before the first pass.
    pub owned_ttl: Option<Vec<CollectionId>>,
    /// The collections this member's own block lists as TTL-indexed: the target
    /// rule before the first pass.
    pub ttl_holders: Vec<CollectionId>,
    /// The hold that has the writer now, when it is one whose length is judged
    /// (ADR-220): never a build or an index drop. `None` when the writer is free,
    /// or held by one of those.
    pub writer_hold: Option<WriterHoldSample>,
}

/// The hold of the writer the tick found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriterHoldSample {
    /// How long ago the hold began.
    pub age: Duration,
    /// What it is doing: the holder's label.
    pub holder: &'static str,
}

impl Sample {
    pub fn new(measure: TickMeasure, classes: PerClass<ClassSample>, view: Arc<PeerView>) -> Self {
        Self {
            measure,
            classes,
            stopping: false,
            view,
            owned_ttl: Some(Vec::new()),
            ttl_holders: Vec::new(),
            writer_hold: None,
        }
    }
}

/// How the runtime was judged in one tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeVerdict {
    /// The control was late, or the tick before it was: nothing is judged.
    Void,
    Stalled,
    Fine,
}

/// What a class's tick counted as.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Verdict {
    Good,
    Bad,
    /// Owns nothing, and nothing is wrong: counts only towards reclaim.
    Idle,
    Neutral,
    Void,
}

impl Verdict {
    pub const ALL: [Self; 5] = [Self::Good, Self::Bad, Self::Idle, Self::Neutral, Self::Void];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Good => "good",
            Self::Bad => "bad",
            Self::Idle => "idle",
            Self::Neutral => "neutral",
            Self::Void => "void",
        }
    }

    pub const fn slot(self) -> usize {
        self as usize
    }
}

/// Why a `stalled` class is not yielding, in the order they are tried.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Suppression {
    SwitchedOff,
    SharedFault,
    NoTarget,
    Cap,
}

impl Suppression {
    pub const ALL: [Self; 4] = [Self::SwitchedOff, Self::SharedFault, Self::NoTarget, Self::Cap];

    pub const fn label(self) -> &'static str {
        match self {
            Self::SwitchedOff => "switched_off",
            Self::SharedFault => "shared_fault",
            Self::NoTarget => "no_target",
            Self::Cap => "cap",
        }
    }

    pub const fn slot(self) -> usize {
        self as usize
    }
}

/// Why a yielded class took its work back while still `stalled`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WithdrawReason {
    /// Two members are `stalled` locally in the class: the fault follows the data.
    SharedFault,
    /// More members yield the class than the cap allows, and this one is past it
    /// in node-id order.
    TieBreak,
}

impl WithdrawReason {
    pub const fn label(self) -> &'static str {
        match self {
            Self::SharedFault => "shared_fault",
            Self::TieBreak => "tie_break",
        }
    }
}

/// A transition, for the log and the counters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The class became `stalled`.
    Stalled {
        class: OwnerClass,
        cause: StallCause,
        bad: usize,
        of: usize,
    },
    /// The class set its yield bit.
    Yielded {
        class: OwnerClass,
        cause: StallCause,
        bad: usize,
        of: usize,
        targets: Vec<NodeId>,
    },
    /// R good ticks: the yield ended and the class owns again at once.
    Reclaimed {
        class: OwnerClass,
        r_ticks: u32,
        step: u32,
        yielded: bool,
    },
    Withdrew {
        class: OwnerClass,
        reason: WithdrawReason,
    },
    /// A `stalled` class could not yield, said at most once per
    /// [`crate::health::WARN_INTERVAL`] per class.
    Suppressed {
        class: OwnerClass,
        reason: Suppression,
    },
    /// The writer's hold passed [`WRITER_WEDGE`]: said once per wedge (ADR-220).
    WriterWedged {
        holder: &'static str,
        age: Duration,
    },
    /// The wedge ended: the writer is free, or held by something else.
    WriterFree,
}

/// One class's result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClassDecision {
    pub state: ClassState,
    /// Set while `stalled`.
    pub cause: Option<StallCause>,
    /// The advertised yield bit.
    pub yielding: bool,
    pub verdict: Verdict,
    pub suppressed: Option<Suppression>,
    pub owned: u64,
    /// The shared-fault latch is set for the class.
    pub latched: bool,
}

/// What a tick decided.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decision {
    pub runtime: RuntimeVerdict,
    pub responsive: bool,
    /// Fewer than [`RESPONSIVE_TICKS`] non-void ticks have been judged since the
    /// start: `responsive` is false because nothing has been shown yet, and not
    /// because the runtime stalled (ADR-220).
    pub warming: bool,
    /// The writer's hold is past [`WRITER_WEDGE`] (ADR-220). A void tick leaves it
    /// as it was.
    pub writer_wedged: bool,
    /// How old the hold the last judged tick found was: zero when the writer was
    /// free, or held by a holder that is not judged.
    pub writer_hold_age: Duration,
    pub classes: PerClass<ClassDecision>,
    pub events: Vec<Event>,
}

impl Decision {
    /// The advertised yield bits.
    pub fn yielding(&self) -> Yielding {
        Yielding {
            ttl: self.classes.ttl.yielding,
            webhooks: self.classes.webhooks.yielding,
            embeddings: self.classes.embeddings.yielding,
        }
    }
}

// ---------------------------------------------------------------------------
// The evaluator.
// ---------------------------------------------------------------------------

/// What an evaluator is built from.
#[derive(Clone, Debug)]
pub struct Config {
    /// This member, for the tie-break and the cap.
    pub me: NodeId,
    /// `KIMMY_OWNERSHIP_YIELD` is not `off`. Off, the evaluator still judges and
    /// advertises, and never sets its own bits.
    pub enabled: bool,
    pub timings: Timings,
    /// `storage.ttl_interval_secs`: the TTL bound and window follow it.
    pub ttl_interval: Duration,
    /// The start follows an unclean end: every class starts `stalled` and yields
    /// from the first block, with the target condition waived.
    pub probation: bool,
    /// The classes the operator has switched off (and any whose worker does not
    /// beat yet): **probation skips them**. A class that is switched off judges
    /// every tick neutral, and a neutral tick neither counts towards reclaim nor
    /// breaks it, so a class that started `stalled` would never leave.
    pub off: PerClass<bool>,
}

impl Config {
    pub fn new(me: NodeId) -> Self {
        Self {
            me,
            enabled: true,
            timings: Timings::standard(),
            ttl_interval: Duration::from_secs(60),
            probation: false,
            off: PerClass::all(false),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Local,
    Runtime,
    Probation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Item {
    Good,
    Bad(Kind),
}

#[derive(Clone, Copy, Debug, Default)]
struct Seen {
    local_ok: u64,
    local_fault: u64,
    cycles: u64,
    cycles_bad: u64,
}

struct Track {
    class: OwnerClass,
    bounds: Bounds,
    window: Duration,
    ring: VecDeque<(Instant, Item)>,
    seen: Seen,
    stalled: bool,
    cause: StallCause,
    yielded: bool,
    /// The episode began with probation: the target condition stays waived.
    probation_episode: bool,
    good_run: u32,
    /// Back-off step, and whether this episode has applied it.
    step: u32,
    step_applied: bool,
    last_reclaim: Option<Instant>,
    latch: bool,
    latch_below_since: Option<Instant>,
    last_suppress_event: Option<Instant>,
    state: ClassState,
    suppressed: Option<Suppression>,
    verdict: Verdict,
    owned: u64,
}

impl Track {
    fn new(class: OwnerClass, config: &Config) -> Self {
        Self {
            class,
            bounds: Bounds::of(class, config.ttl_interval, config.timings.e),
            window: window_of(class, config.ttl_interval, config.timings.e),
            ring: VecDeque::new(),
            seen: Seen::default(),
            stalled: false,
            cause: StallCause::Unknown,
            yielded: false,
            probation_episode: false,
            good_run: 0,
            step: 0,
            step_applied: false,
            last_reclaim: None,
            latch: false,
            latch_below_since: None,
            last_suppress_event: None,
            state: ClassState::Idle,
            suppressed: None,
            verdict: Verdict::Neutral,
            owned: 0,
        }
    }

    fn counts(&self, now: Instant) -> (usize, usize) {
        let mut bad = 0;
        let mut local = 0;
        for (at, item) in &self.ring {
            if now.saturating_duration_since(*at) >= self.window {
                continue;
            }
            if let Item::Bad(kind) = item {
                bad += 1;
                if *kind == Kind::Local {
                    local += 1;
                }
            }
        }
        (bad, local)
    }

    fn decision(&self) -> ClassDecision {
        ClassDecision {
            state: self.state,
            cause: self.stalled.then_some(self.cause),
            yielding: self.yielded,
            verdict: self.verdict,
            suppressed: self.suppressed,
            owned: self.owned,
            latched: self.latch,
        }
    }

    fn r_ticks(&self) -> u32 {
        reclaim_ticks(if self.step_applied { self.step } else { 0 })
    }
}

/// The yield predicate's state machine. See the module documentation.
pub struct Evaluator {
    config: Config,
    tracks: [Track; 3],
    /// Verdicts of the last non-void ticks, newest last, at most
    /// [`RESPONSIVE_TICKS`].
    recent: VecDeque<bool>,
    non_void_ticks: u64,
    prev_late: bool,
    /// Consecutive on-time control steps, in non-void ticks, with no probe wake.
    no_wake_run: u32,
    responsive: bool,
    writer_wedged: bool,
    writer_hold_age: Duration,
    last: Option<Decision>,
}

fn idx(class: OwnerClass) -> usize {
    match class {
        OwnerClass::Ttl => 0,
        OwnerClass::Webhooks => 1,
        OwnerClass::Embeddings => 2,
    }
}

impl Evaluator {
    /// A new evaluator, and what it publishes **before its first tick**. With
    /// probation every class starts `stalled` (cause `probation`), its window
    /// pre-filled with N bad items, and with the switch on it yields from the
    /// first block, the target condition waived and the cap and the switch still
    /// applying (with no peer view yet the cap counts this member alone).
    pub fn start(config: Config, now: Instant) -> (Self, Decision) {
        let tracks = OwnerClass::ALL.map(|class| Track::new(class, &config));
        let mut me = Self {
            config,
            tracks,
            recent: VecDeque::new(),
            non_void_ticks: 0,
            prev_late: false,
            no_wake_run: 0,
            responsive: false,
            writer_wedged: false,
            writer_hold_age: Duration::ZERO,
            last: None,
        };
        let mut events = Vec::new();
        if me.config.probation {
            let enabled = me.config.enabled;
            let off = me.config.off;
            for track in &mut me.tracks {
                if off.of(track.class) {
                    continue;
                }
                let (n, _) = n_of_m(track.class);
                for _ in 0..n {
                    track.ring.push_back((now, Item::Bad(Kind::Probation)));
                }
                track.stalled = true;
                track.cause = StallCause::Probation;
                track.probation_episode = true;
                track.state = ClassState::Stalled;
                if enabled {
                    // The cap with no peer view counts this member alone, and no
                    // latch has been set: the first block already yields.
                    track.yielded = true;
                    track.step_applied = true;
                    events.push(Event::Yielded {
                        class: track.class,
                        cause: StallCause::Probation,
                        bad: n,
                        of: n,
                        targets: Vec::new(),
                    });
                } else {
                    track.suppressed = Some(Suppression::SwitchedOff);
                }
            }
        }
        let decision = me.snapshot(RuntimeVerdict::Void, events);
        me.last = Some(decision.clone());
        (me, decision)
    }

    fn snapshot(&self, runtime: RuntimeVerdict, events: Vec<Event>) -> Decision {
        let [ttl, webhooks, embeddings] = &self.tracks;
        Decision {
            runtime,
            responsive: self.responsive,
            warming: self.non_void_ticks < RESPONSIVE_TICKS as u64,
            writer_wedged: self.writer_wedged,
            writer_hold_age: self.writer_hold_age,
            classes: PerClass {
                ttl: ttl.decision(),
                webhooks: webhooks.decision(),
                embeddings: embeddings.decision(),
            },
            events,
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The runtime's verdict for one tick, updating the control's running state.
    fn judge_runtime(&mut self, measure: &TickMeasure) -> RuntimeVerdict {
        let late = measure.control_worst_gap > VOID_GAP;
        let void = late || self.prev_late;
        self.prev_late = late;
        if void {
            // A pause lands its lateness on the probe's first wake after resume,
            // which can fall in the next tick: both are void.
            self.no_wake_run = 0;
            return RuntimeVerdict::Void;
        }
        if measure.probe_woke {
            self.no_wake_run = 0;
        } else {
            self.no_wake_run += measure.steps;
        }
        if measure.probe_worst > RUNTIME_STALL || self.no_wake_run >= NO_WAKE_STEPS {
            RuntimeVerdict::Stalled
        } else {
            RuntimeVerdict::Fine
        }
    }

    /// One tick. **Void ticks judge nothing and change nothing**; the verdict of
    /// every class is `void` and its state, bits and counters stand as they were.
    pub fn tick(&mut self, sample: &Sample, now: Instant) -> Decision {
        let runtime = self.judge_runtime(&sample.measure);
        if runtime == RuntimeVerdict::Void {
            for track in &mut self.tracks {
                track.verdict = Verdict::Void;
            }
            let decision = self.snapshot(runtime, Vec::new());
            self.last = Some(decision.clone());
            return decision;
        }
        self.non_void_ticks += 1;
        if self.recent.len() == RESPONSIVE_TICKS {
            self.recent.pop_front();
        }
        self.recent.push_back(runtime != RuntimeVerdict::Stalled);
        self.responsive =
            self.non_void_ticks >= RESPONSIVE_TICKS as u64 && self.recent.iter().all(|fine| *fine);

        let mut events = Vec::new();
        // The writer's hold is judged on every non-void tick, before and apart
        // from the classes (ADR-220): it is a fact about the member, and a Repair
        // hold wedges exactly while the classes are gated at the writer.
        let wedged_now = sample.writer_hold.filter(|hold| hold.age > WRITER_WEDGE);
        self.writer_hold_age = sample.writer_hold.map_or(Duration::ZERO, |hold| hold.age);
        match (self.writer_wedged, wedged_now) {
            (false, Some(hold)) => {
                events.push(Event::WriterWedged { holder: hold.holder, age: hold.age });
            }
            (true, None) => events.push(Event::WriterFree),
            _ => {}
        }
        self.writer_wedged = wedged_now.is_some();
        for class in OwnerClass::ALL {
            self.tick_class(class, sample, runtime, now, &mut events);
        }
        let decision = self.snapshot(runtime, events);
        self.last = Some(decision.clone());
        decision
    }

    fn tick_class(
        &mut self,
        class: OwnerClass,
        sample: &Sample,
        runtime: RuntimeVerdict,
        now: Instant,
        events: &mut Vec<Event>,
    ) {
        let cs = sample.classes.of(class);
        let responsive = self.responsive;
        let enabled = self.config.enabled;
        let me = self.config.me;
        let track = &mut self.tracks[idx(class)];
        let (n, m) = n_of_m(class);

        // --- evidence ---
        let d_ok = cs.local_ok.saturating_sub(track.seen.local_ok);
        let d_fault = cs.local_fault.saturating_sub(track.seen.local_fault);
        let d_cycles = cs.cycles.saturating_sub(track.seen.cycles);
        let d_bad_cycles = cs.cycles_bad.saturating_sub(track.seen.cycles_bad);
        track.seen = Seen {
            local_ok: cs.local_ok,
            local_fault: cs.local_fault,
            cycles: cs.cycles,
            cycles_bad: cs.cycles_bad,
        };
        track.owned = cs.owned;

        let neutral = cs.gated || cs.switched_off || sample.stopping;
        let overdue = !neutral
            && track
                .bounds
                .of_phase(cs.phase)
                .is_some_and(|bound| cs.since_beat.min(cs.since_phase) > bound);
        let bad_local = overdue || d_bad_cycles > 0;
        let bad_runtime = runtime == RuntimeVerdict::Stalled && cs.owned > 0;
        // A wedged writer is the member's own fault (a local cause, which the
        // shared-fault latch counts), and a bad item only for a class that owns
        // work, as a runtime stall is; for a class that owns nothing the fact is
        // the member's `writer_wedged`, which refuses it as a target (ADR-220).
        let bad_writer = self.writer_wedged && cs.owned > 0;
        let verdict = if neutral {
            Verdict::Neutral
        } else if bad_local || bad_runtime || bad_writer {
            Verdict::Bad
        } else if cs.owned > 0 && (d_ok > 0 || d_cycles > 0) {
            Verdict::Good
        } else if cs.owned == 0 && d_fault == 0 && responsive {
            Verdict::Idle
        } else {
            Verdict::Neutral
        };
        track.verdict = verdict;
        match verdict {
            Verdict::Bad => {
                let kind = if bad_local || bad_writer { Kind::Local } else { Kind::Runtime };
                track.ring.push_back((now, Item::Bad(kind)));
            }
            Verdict::Good => track.ring.push_back((now, Item::Good)),
            _ => {}
        }
        while track.ring.len() > m {
            track.ring.pop_front();
        }

        // --- entering, and the class state ---
        let (bad, bad_local_count) = track.counts(now);
        if !track.stalled && verdict == Verdict::Bad && bad >= n {
            track.stalled = true;
            track.cause =
                if bad_local_count >= n { StallCause::Local } else { StallCause::Runtime };
            track.good_run = 0;
            track.step_applied = false;
            track.probation_episode = false;
            events.push(Event::Stalled { class, cause: track.cause, bad, of: m });
        }
        // Within an episode the cause is upgraded to `local`, never downgraded.
        if track.stalled && track.cause != StallCause::Local && bad_local_count >= n {
            track.cause = StallCause::Local;
        }
        track.state = if track.stalled {
            ClassState::Stalled
        } else if overdue || bad >= 1 {
            ClassState::Suspect
        } else if cs.owned > 0 {
            ClassState::Ok
        } else {
            ClassState::Idle
        };

        // --- the shared-fault latch: members `stalled` with a local cause ---
        let view = &sample.view;
        let mut local_stalled = usize::from(track.stalled && track.cause == StallCause::Local);
        for node in &view.live {
            if let Some((held, true)) = view.live_block(node, now)
                && held.facts.class_state.is_some_and(|s| s.of(class) == ClassState::Stalled)
                && held.facts.class_cause.is_some_and(|c| c.of(class) == StallCause::Local)
            {
                local_stalled += 1;
            }
        }
        if local_stalled >= 2 {
            track.latch = true;
            track.latch_below_since = None;
        } else if track.latch {
            let since = *track.latch_below_since.get_or_insert(now);
            if now.saturating_duration_since(since) >= self.config.timings.latch_hold {
                track.latch = false;
                track.latch_below_since = None;
            }
        }

        // --- the yield bit ---
        let cap = cap_of(view);
        let mut peers_yielding: BTreeSet<NodeId> = view
            .live
            .iter()
            .filter(|node| view.blocks.get(*node).is_some_and(|held| held.facts.yielding.of(class)))
            .copied()
            .collect();
        peers_yielding.remove(&me);
        let mut withdrew = false;
        if track.yielded {
            let reason = if track.latch {
                Some(WithdrawReason::SharedFault)
            } else {
                let mut all = peers_yielding.clone();
                all.insert(me);
                let position = all.iter().position(|node| *node == me).unwrap_or(0);
                (all.len() > cap && position >= cap).then_some(WithdrawReason::TieBreak)
            };
            if let Some(reason) = reason {
                track.yielded = false;
                withdrew = true;
                events.push(Event::Withdrew { class, reason });
            }
        }
        track.suppressed = None;
        if track.stalled && !track.yielded && !neutral && !withdrew {
            let reason = if !enabled {
                Some(Suppression::SwitchedOff)
            } else if track.latch {
                Some(Suppression::SharedFault)
            } else if !track.probation_episode && !has_target(sample, class, now) {
                Some(Suppression::NoTarget)
            } else if peers_yielding.len() + 1 > cap {
                Some(Suppression::Cap)
            } else {
                None
            };
            match reason {
                None => {
                    track.yielded = true;
                    if !track.step_applied {
                        track.step_applied = true;
                        track.step = match track.last_reclaim {
                            Some(at)
                                if now.saturating_duration_since(at)
                                    < self.config.timings.backoff_window =>
                            {
                                (track.step + 1).min(4)
                            }
                            _ => 0,
                        };
                    }
                    let targets = targets_of(sample, class, now);
                    events.push(Event::Yielded { class, cause: track.cause, bad, of: m, targets });
                }
                Some(reason) => {
                    track.suppressed = Some(reason);
                    let due = track.last_suppress_event.is_none_or(|at| {
                        now.saturating_duration_since(at) >= crate::health::WARN_INTERVAL
                    });
                    if due {
                        track.last_suppress_event = Some(now);
                        events.push(Event::Suppressed { class, reason });
                    }
                }
            }
        }

        // --- reclaim ---
        if track.stalled {
            match verdict {
                // A tick under a wedged writer is not good for reclaim whatever the class
                // owns (ADR-220): a yielded class owns nothing, so the wedge adds it no
                // bad item and it reads idle, and idle ticks counted here would reclaim
                // it into the wedge it left. The R good ticks start at the let-go.
                _ if self.writer_wedged => track.good_run = 0,
                Verdict::Good | Verdict::Idle => track.good_run += 1,
                Verdict::Bad => track.good_run = 0,
                _ => {}
            }
            let r = track.r_ticks();
            if track.good_run >= r {
                let was_yielded = track.yielded;
                events.push(Event::Reclaimed {
                    class,
                    r_ticks: r,
                    step: track.step,
                    yielded: was_yielded,
                });
                track.stalled = false;
                track.yielded = false;
                track.probation_episode = false;
                track.good_run = 0;
                track.cause = StallCause::Unknown;
                if was_yielded {
                    track.last_reclaim = Some(now);
                }
                let (bad, _) = track.counts(now);
                track.state = if bad >= 1 {
                    ClassState::Suspect
                } else if cs.owned > 0 {
                    ClassState::Ok
                } else {
                    ClassState::Idle
                };
            }
        }
    }
}

/// `max(1, ⌊(n−1)/2⌋)`, `n` counting this member.
pub fn cap_of(view: &PeerView) -> usize {
    (view.live.len() / 2).max(1)
}

/// Whether a live peer is a target for `class`: a lease-fresh block from its
/// current boot, not catching up, not yielding the class, `ok` or `idle` in it
/// (never `suspect`, `stalled`, `unknown`, or absent), `responsive`, and one that
/// may own it (for TTL: it lists a collection this member owns, or before this
/// member's first pass, one in its own list).
pub fn is_target(
    view: &PeerView,
    node: &NodeId,
    sample: &Sample,
    class: OwnerClass,
    now: Instant,
) -> bool {
    target_refusal(view, node, sample, class, now).is_none()
}

/// The collections a TTL peer must list to be a target: what this member's last
/// completed pass **owned**, or, before its first pass (or after a pass that ran
/// while the member was catching up and so recorded nothing), the collections its
/// own block lists. A pass that ran ungated and owned nothing is `Some([])`, which
/// means this member owns nothing, so no peer is asked to list anything and the
/// class does not yield.
pub fn ttl_scope(sample: &Sample) -> &[CollectionId] {
    sample.owned_ttl.as_deref().unwrap_or(&sample.ttl_holders)
}

/// Why `node` is **not** a target for `class`, or `None` when it is: the one
/// predicate [`is_target`] answers from, so the reason logged for a missing target
/// can never differ from the rule that found none.
pub fn target_refusal(
    view: &PeerView,
    node: &NodeId,
    sample: &Sample,
    class: OwnerClass,
    now: Instant,
) -> Option<&'static str> {
    if !view.live.contains(node) {
        return Some("not live");
    }
    let Some(held) = view.blocks.get(node) else { return Some("no block heard") };
    if now.saturating_duration_since(held.received) > view.lease {
        return Some("block past its lease");
    }
    let facts: &Facts = &held.facts;
    if facts.catching_up {
        return Some("catching up");
    }
    // A member whose writer is wedged can do no write, whatever its classes own:
    // a class it owns nothing in reads `idle` and would pass the check below
    // (ADR-220).
    if facts.writer_wedged == Some(true) {
        return Some("writer wedged");
    }
    if facts.yielding.of(class) {
        return Some("already yields the class");
    }
    let responsive = facts.responsive == Some(true);
    match facts.class_state.map(|states| states.of(class)) {
        Some(ClassState::Ok | ClassState::Idle) if responsive => {}
        Some(ClassState::Ok | ClassState::Idle) => return Some("runtime not responsive"),
        Some(ClassState::Suspect) => return Some("class suspect"),
        Some(ClassState::Stalled) => return Some("class stalled"),
        Some(ClassState::Unknown) => return Some("class state unknown"),
        None => return Some("no class state in its block"),
    }
    let may = match class {
        OwnerClass::Ttl => ttl_scope(sample).iter().any(|collection| {
            may_own(Some(facts), OwnerClass::Ttl, Some(*collection), None, false, true)
        }),
        _ => may_own(Some(facts), class, None, None, false, true),
    };
    (!may).then_some("does not hold what this member owns")
}

/// For each live peer, why it is not a target for `class`, as a line to log when
/// no peer is one.
pub fn explain_no_target(sample: &Sample, class: OwnerClass, now: Instant) -> Vec<String> {
    sample
        .view
        .live
        .iter()
        .map(|node| {
            let why =
                target_refusal(&sample.view, node, sample, class, now).unwrap_or("is a target");
            // For a TTL class the two sides of the comparison, which is what a
            // "does not hold" needs to be read against.
            let detail = if class == OwnerClass::Ttl {
                let scope = ttl_scope(sample);
                let theirs: Vec<_> = sample
                    .view
                    .blocks
                    .get(node)
                    .map(|b| b.facts.ttl.iter().map(|held| held.collection).collect())
                    .unwrap_or_default();
                format!(
                    " [this member's scope: {scope:?}{}; its block lists: {theirs:?}]",
                    if sample.owned_ttl.is_some() { " (owned)" } else { " (holders)" }
                )
            } else {
                String::new()
            };
            format!("{node:?}: {why}{detail}")
        })
        .collect()
}

fn targets_of(sample: &Sample, class: OwnerClass, now: Instant) -> Vec<NodeId> {
    sample
        .view
        .live
        .iter()
        .filter(|node| is_target(&sample.view, node, sample, class, now))
        .copied()
        .collect()
}

fn has_target(sample: &Sample, class: OwnerClass, now: Instant) -> bool {
    sample.view.live.iter().any(|node| is_target(&sample.view, node, sample, class, now))
}

/// The probe's marks the evaluator reads (ADR-213): its subject. The runtime's
/// 250 ms stall probe records each wake here as well as in the scrape's own mark,
/// and **these are atomics only**: the evaluator takes no lock a runtime task
/// takes. `wakes` is a counter because a `fetch_max` of lateness cannot tell no
/// wake from an on-time one.
#[derive(Debug, Default)]
pub struct ProbeMarks {
    wakes: AtomicU64,
    worst_us: AtomicU64,
}

impl ProbeMarks {
    /// The probe woke, `late` after it should have.
    pub fn record(&self, late: Duration) {
        self.worst_us
            .fetch_max(u64::try_from(late.as_micros()).unwrap_or(u64::MAX), Ordering::Relaxed);
        self.wakes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn wakes(&self) -> u64 {
        self.wakes.load(Ordering::Relaxed)
    }

    /// The worst lateness since the last call, which resets it.
    pub fn take_worst(&self) -> Duration {
        Duration::from_micros(self.worst_us.swap(0, Ordering::Relaxed))
    }
}

// ---------------------------------------------------------------------------
// What the evaluator publishes.
// ---------------------------------------------------------------------------

fn state_code(state: ClassState) -> u8 {
    match state {
        ClassState::Ok => 0,
        ClassState::Idle => 1,
        ClassState::Suspect => 2,
        ClassState::Stalled => 3,
        ClassState::Unknown => 4,
    }
}

fn state_of(code: u8) -> ClassState {
    match code {
        0 => ClassState::Ok,
        1 => ClassState::Idle,
        2 => ClassState::Suspect,
        3 => ClassState::Stalled,
        _ => ClassState::Unknown,
    }
}

fn cause_code(cause: Option<StallCause>) -> u8 {
    match cause {
        None => 0,
        Some(StallCause::Local) => 1,
        Some(StallCause::Runtime) => 2,
        Some(StallCause::Probation) => 3,
        Some(StallCause::Unknown) => 4,
    }
}

fn cause_of(code: u8) -> StallCause {
    match code {
        1 => StallCause::Local,
        2 => StallCause::Runtime,
        3 => StallCause::Probation,
        _ => StallCause::Unknown,
    }
}

/// What an evaluator tick published, in atomics: the facts source reads it, and
/// nothing a runtime task takes a lock on stands between the evaluator and the
/// block (ADR-213).
pub struct Published {
    states: [AtomicU8; 3],
    causes: [AtomicU8; 3],
    yields: [AtomicU8; 3],
    responsive: AtomicU8,
    /// No tick has judged the runtime six times yet, so `responsive` is not
    /// false for a stall but for want of evidence (ADR-220).
    warming: AtomicU8,
    /// The writer's hold is past [`WRITER_WEDGE`] (ADR-220).
    writer_wedged: AtomicU8,
    /// The age, in milliseconds, of the hold the last judged tick found: zero
    /// when the writer was free or held by a holder that is not judged. A void
    /// tick leaves it as it was.
    writer_hold_age_ms: AtomicU64,
    /// Milliseconds on the driver's clock of the last tick, plus one so zero means
    /// never.
    last_tick_ms: AtomicU64,
    /// Bumped whenever a published state, cause, bit or the responsive flag changes.
    version: AtomicU64,
    ticks: AtomicU64,
    observations: [[AtomicU64; 5]; 3],
    transitions: [[AtomicU64; 3]; 3],
    suppressed: [[AtomicU64; 4]; 3],
}

/// What was last published, unmasked by the staleness rule: for `/metrics` and the
/// topology, which report what the evaluator last decided and say separately how
/// long ago that was.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Current {
    pub state: PerClass<ClassState>,
    pub cause: PerClass<Option<StallCause>>,
    pub yielding: PerClass<bool>,
    pub responsive: bool,
    /// The runtime stalled within the last judged ticks: `responsive` is false
    /// for that, and not because the member has not been judged long enough yet.
    pub runtime_stalled: bool,
    pub writer_wedged: bool,
    pub writer_hold_age: Duration,
}

/// What the block advertises, after the staleness rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Advertised {
    pub class_state: PerClass<ClassState>,
    pub class_cause: PerClass<StallCause>,
    /// `None` while the member is warming up as well as when the evaluator is
    /// stale (ADR-220): `Some(false)` says the runtime stalled.
    pub responsive: Option<bool>,
    pub writer_wedged: Option<bool>,
    pub yielding: Yielding,
}

/// A transition's direction, for `kimmy_yield_transitions_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Yield,
    Reclaim,
    Withdraw,
}

impl Direction {
    pub const ALL: [Self; 3] = [Self::Yield, Self::Reclaim, Self::Withdraw];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Yield => "yield",
            Self::Reclaim => "reclaim",
            Self::Withdraw => "withdraw",
        }
    }
}

impl Default for Published {
    fn default() -> Self {
        Self {
            states: Default::default(),
            causes: Default::default(),
            yields: Default::default(),
            responsive: AtomicU8::new(0),
            warming: AtomicU8::new(1),
            writer_wedged: AtomicU8::new(0),
            writer_hold_age_ms: AtomicU64::new(0),
            last_tick_ms: AtomicU64::new(0),
            version: AtomicU64::new(0),
            ticks: AtomicU64::new(0),
            observations: Default::default(),
            transitions: Default::default(),
            suppressed: Default::default(),
        }
    }
}

impl Published {
    /// Publish `decision`, taken at `at_ms` on the driver's clock. The version moves
    /// only when something advertised changed.
    pub fn store(&self, decision: &Decision, at_ms: u64) {
        let mut changed = false;
        for class in OwnerClass::ALL {
            let i = idx(class);
            let d = decision.classes.of(class);
            changed |=
                self.states[i].swap(state_code(d.state), Ordering::SeqCst) != state_code(d.state);
            changed |=
                self.causes[i].swap(cause_code(d.cause), Ordering::SeqCst) != cause_code(d.cause);
            changed |=
                self.yields[i].swap(u8::from(d.yielding), Ordering::SeqCst) != u8::from(d.yielding);
            self.observations[i][d.verdict.slot()].fetch_add(1, Ordering::Relaxed);
            for reason in Suppression::ALL {
                // A gauge: 1 while this reason is the one in force.
                self.suppressed[i][reason.slot()]
                    .store(u64::from(d.suppressed == Some(reason)), Ordering::Relaxed);
            }
        }
        changed |= self.responsive.swap(u8::from(decision.responsive), Ordering::SeqCst)
            != u8::from(decision.responsive);
        changed |= self.warming.swap(u8::from(decision.warming), Ordering::SeqCst)
            != u8::from(decision.warming);
        changed |= self.writer_wedged.swap(u8::from(decision.writer_wedged), Ordering::SeqCst)
            != u8::from(decision.writer_wedged);
        self.writer_hold_age_ms.store(
            u64::try_from(decision.writer_hold_age.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        for event in &decision.events {
            let (class, direction) = match event {
                Event::Yielded { class, .. } => (*class, Direction::Yield),
                Event::Reclaimed { class, yielded: true, .. } => (*class, Direction::Reclaim),
                Event::Withdrew { class, .. } => (*class, Direction::Withdraw),
                _ => continue,
            };
            self.transitions[idx(class)][direction as usize].fetch_add(1, Ordering::Relaxed);
        }
        if changed {
            self.version.fetch_add(1, Ordering::SeqCst);
        }
        self.ticks.fetch_add(1, Ordering::SeqCst);
        self.last_tick_ms.store(at_ms + 1, Ordering::SeqCst);
    }

    /// Publish the start decision, which counts as a tick time but not as a tick.
    pub fn store_start(&self, decision: &Decision, at_ms: u64) {
        self.store(decision, at_ms);
        self.ticks.store(0, Ordering::SeqCst);
        for per_class in &self.observations {
            for count in per_class {
                count.store(0, Ordering::Relaxed);
            }
        }
    }

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    /// What the evaluator last published.
    pub fn current(&self) -> Current {
        let load = |codes: &[AtomicU8; 3]| [0, 1, 2].map(|i| codes[i].load(Ordering::SeqCst));
        let [s0, s1, s2] = load(&self.states);
        let [c0, c1, c2] = load(&self.causes);
        let [y0, y1, y2] = load(&self.yields);
        let cause = |code: u8| (code != 0).then(|| cause_of(code));
        Current {
            state: PerClass { ttl: state_of(s0), webhooks: state_of(s1), embeddings: state_of(s2) },
            cause: PerClass { ttl: cause(c0), webhooks: cause(c1), embeddings: cause(c2) },
            yielding: PerClass { ttl: y0 != 0, webhooks: y1 != 0, embeddings: y2 != 0 },
            responsive: self.responsive.load(Ordering::SeqCst) != 0,
            runtime_stalled: self.responsive.load(Ordering::SeqCst) == 0
                && self.warming.load(Ordering::SeqCst) == 0,
            writer_wedged: self.writer_wedged.load(Ordering::SeqCst) != 0,
            writer_hold_age: Duration::from_millis(self.writer_hold_age_ms.load(Ordering::Relaxed)),
        }
    }

    /// Milliseconds on the driver's clock of the last tick (or the start), or
    /// `None` before either.
    pub fn last_tick_ms(&self) -> Option<u64> {
        self.last_tick_ms.load(Ordering::SeqCst).checked_sub(1)
    }

    /// Evaluator ticks since start: `kimmy_yield_evaluator_ticks_total`.
    pub fn ticks(&self) -> u64 {
        self.ticks.load(Ordering::SeqCst)
    }

    pub fn observations(&self, class: OwnerClass, verdict: Verdict) -> u64 {
        self.observations[idx(class)][verdict.slot()].load(Ordering::Relaxed)
    }

    pub fn transitions(&self, class: OwnerClass, direction: Direction) -> u64 {
        self.transitions[idx(class)][direction as usize].load(Ordering::Relaxed)
    }

    pub fn suppressed(&self, class: OwnerClass, reason: Suppression) -> u64 {
        self.suppressed[idx(class)][reason.slot()].load(Ordering::Relaxed)
    }

    /// The block's evaluator fields at `now_ms` on the driver's clock. **If the
    /// evaluator has not ticked for [`STALE_TICKS`]·E, every class advertises
    /// `unknown` and `responsive` is `None`, and the yield bits stay as they are**:
    /// silence is not evidence of recovery, and a wedged evaluator's member is
    /// never a target (ADR-213).
    pub fn advertised(&self, now_ms: u64, e: Duration) -> Advertised {
        let yielding = Yielding {
            ttl: self.yields[0].load(Ordering::SeqCst) != 0,
            webhooks: self.yields[1].load(Ordering::SeqCst) != 0,
            embeddings: self.yields[2].load(Ordering::SeqCst) != 0,
        };
        let last = self.last_tick_ms.load(Ordering::SeqCst);
        let age = now_ms.saturating_sub(last.saturating_sub(1));
        let stale = last == 0 || u128::from(age) > e.as_millis() * u128::from(STALE_TICKS);
        if stale {
            return Advertised {
                class_state: PerClass::all(ClassState::Unknown),
                class_cause: PerClass::all(StallCause::Unknown),
                responsive: None,
                writer_wedged: None,
                yielding,
            };
        }
        let read = |codes: &[AtomicU8; 3]| [0, 1, 2].map(|i| codes[i].load(Ordering::SeqCst));
        let [s0, s1, s2] = read(&self.states);
        let [c0, c1, c2] = read(&self.causes);
        Advertised {
            class_state: PerClass {
                ttl: state_of(s0),
                webhooks: state_of(s1),
                embeddings: state_of(s2),
            },
            class_cause: PerClass {
                ttl: cause_of(c0),
                webhooks: cause_of(c1),
                embeddings: cause_of(c2),
            },
            // Warming up says nothing: the runtime has not been judged long
            // enough to be called responsive, and has not stalled (ADR-220). A
            // peer reads `Some(false)` as a stall, and still refuses `None` as a
            // target.
            responsive: if self.warming.load(Ordering::SeqCst) != 0 {
                None
            } else {
                Some(self.responsive.load(Ordering::SeqCst) != 0)
            },
            writer_wedged: Some(self.writer_wedged.load(Ordering::SeqCst) != 0),
            yielding,
        }
    }

    /// Set `facts`' evaluator fields from [`Self::advertised`].
    pub fn apply_to(&self, facts: &mut Facts, now_ms: u64, e: Duration) {
        let advertised = self.advertised(now_ms, e);
        facts.class_state = Some(advertised.class_state);
        facts.class_cause = Some(advertised.class_cause);
        facts.responsive = advertised.responsive;
        facts.writer_wedged = advertised.writer_wedged;
        facts.yielding = advertised.yielding;
    }
}
