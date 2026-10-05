//! The daemon's half of ownership yielding (ADR-213).
//!
//! The decision logic is `kimmy_cluster::yielding`, and the heartbeat cells are
//! `kimmy_storage::class_step`; this wires them to a running node:
//!
//! - **the off switch**, `KIMMY_OWNERSHIP_YIELD`, environment only, with no
//!   configuration key (a new key under `deny_unknown_fields` breaks a rollback to
//!   0.43): unset or `on` is on, `off` is off, case-insensitive, and anything else
//!   refuses the start;
//! - **the four test switches**, all environment only, announced at `WARN` at
//!   start and armed only after the node serves: `KIMMY_TEST_YIELD_SCALE`,
//!   `KIMMY_TEST_FAIL_STEP`, `KIMMY_TEST_STALL_RUNTIME`, and the `stall` and
//!   evaluator modes of `KIMMY_TEST_KILL_TASK` (parsed in `kimmy_task`);
//! - **the evaluator's inputs**: atomics and one view load, never a lock a runtime
//!   task takes, never the engine;
//! - **the facts hook**, which puts the evaluator's published decision into this
//!   member's block, and keeps the list of TTL-indexed collections the evaluator
//!   needs for its target rule, so it never reads the store.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use kimmy_cluster::yielding::driver::{Clock, Inputs, KillSwitch, RealClock};
use kimmy_cluster::yielding::{ClassSample, Phase, ProbeMarks, Published, WriterHoldSample};
use kimmy_cluster::{Facts, PeerView, PerClass};
use kimmy_core::CollectionId;
use kimmy_storage::class_step::{self, Class, ClassCell, MonotonicClock};
use tracing::{info, warn};

/// Whether the embedding worker beats its heartbeat. It does: the drive loop's
/// `Waiting`, each scan row and batch, each provider attempt and each commit beat
/// its cell. Were this false the class would be judged neutral, since a class whose
/// cell nothing beats reads as overdue for ever and would make every member stall
/// in it.
pub const EMBEDDINGS_INSTRUMENTED: bool = true;

/// What `KIMMY_OWNERSHIP_YIELD` says, or why the start is refused.
pub fn parse_ownership_yield(value: Option<&str>) -> Result<bool, String> {
    match value.map(str::trim) {
        None | Some("") => Ok(true),
        Some(v) if v.eq_ignore_ascii_case("on") => Ok(true),
        Some(v) if v.eq_ignore_ascii_case("off") => Ok(false),
        Some(other) => Err(format!(
            "KIMMY_OWNERSHIP_YIELD={other:?} is not a value: use `on` (the default) or `off`, \
             case-insensitive. A value that is not understood is refused rather than read as on, \
             so a typo cannot silently leave yielding on"
        )),
    }
}

/// `KIMMY_OWNERSHIP_YIELD`, read from the environment.
pub fn ownership_yield_from_env() -> Result<bool, String> {
    parse_ownership_yield(std::env::var("KIMMY_OWNERSHIP_YIELD").ok().as_deref())
}

/// The most `KIMMY_TEST_STALL_RUNTIME` may stall for: below foca's 4.8 s
/// suspicion, so the member is not declared down by its own test.
pub const STALL_RUNTIME_MAX_MS: u64 = 3_000;

/// `KIMMY_TEST_STALL_RUNTIME=<ms>/<period_s>`: stall every runtime worker for
/// `ms` once every `period`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StallRuntime {
    pub ms: u64,
    pub period: Duration,
}

/// `KIMMY_TEST_HOLD_WRITER=<ms>,<holder>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HoldWriter {
    pub ms: u64,
    pub holder: kimmy_storage::WriterHolder,
}

/// The longest `KIMMY_TEST_HOLD_WRITER` may hold for: an hour.
pub const HOLD_WRITER_MAX_MS: u64 = 3_600_000;

fn parse_hold_writer(value: &str) -> Result<HoldWriter, String> {
    let bad = || {
        let holders: Vec<&str> =
            kimmy_storage::WriterHolder::ALL.iter().map(|h| h.label()).collect();
        format!(
            "KIMMY_TEST_HOLD_WRITER={value:?} is not <ms>,<holder>, with ms from 1 to \
             {HOLD_WRITER_MAX_MS} and a holder one of {}",
            holders.join(", ")
        )
    };
    let (ms, holder) = value.split_once(',').ok_or_else(bad)?;
    let ms: u64 = ms.trim().parse().map_err(|_| bad())?;
    let holder = kimmy_storage::WriterHolder::ALL
        .into_iter()
        .find(|h| h.label() == holder.trim())
        .ok_or_else(bad)?;
    if ms == 0 || ms > HOLD_WRITER_MAX_MS {
        return Err(bad());
    }
    Ok(HoldWriter { ms, holder })
}

fn parse_stall_runtime(value: &str) -> Result<StallRuntime, String> {
    let bad = || {
        format!(
            "KIMMY_TEST_STALL_RUNTIME={value:?} is not <ms>/<period_s>, with ms at most \
             {STALL_RUNTIME_MAX_MS} (below SWIM's suspicion) and a period of at least one second"
        )
    };
    let (ms, period) = value.split_once('/').ok_or_else(bad)?;
    let ms: u64 = ms.trim().parse().map_err(|_| bad())?;
    let period: u64 = period.trim().parse().map_err(|_| bad())?;
    if ms == 0 || ms > STALL_RUNTIME_MAX_MS || period == 0 {
        return Err(bad());
    }
    Ok(StallRuntime { ms, period: Duration::from_secs(period) })
}

/// `KIMMY_TEST_YIELD_SCALE`, `KIMMY_TEST_FAIL_STEP` and `KIMMY_TEST_STALL_RUNTIME`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TestSwitches {
    /// Divides E, the latch hold and the back-off window; R stays in ticks.
    pub scale: Option<u32>,
    pub stall_runtime: Option<StallRuntime>,
    pub fail_steps: Vec<(Class, Duration)>,
    /// `KIMMY_TEST_REFUSE_SYNC=1`: no replication contact completes, either way.
    pub refuse_sync: bool,
    /// `KIMMY_TEST_HOLD_WRITER=<ms>,<holder>`: take the writer as `holder` and hold
    /// it for `ms` once the node serves (ADR-220).
    pub hold_writer: Option<HoldWriter>,
    /// What each switch said, for the announcement: `(variable, value, note)`.
    pub notes: Vec<(&'static str, String, String)>,
}

/// `<class>[:<after_secs>]`, comma-separated. Entries that do not read are named
/// and ignored, as `KIMMY_TEST_YIELD_SCALE`'s and `KIMMY_TEST_KILL_TASK`'s are; a bad
/// `KIMMY_TEST_STALL_RUNTIME` or `KIMMY_TEST_REFUSE_SYNC` refuses the start instead.
fn parse_fail_steps(value: &str) -> (Vec<(Class, Duration)>, Vec<String>) {
    let mut steps = Vec::new();
    let mut complaints = Vec::new();
    for entry in value.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (class, after) = match entry.split_once(':') {
            Some((class, after)) => (class, after.parse::<u64>().ok()),
            None => (entry, Some(0)),
        };
        let class = Class::ALL.into_iter().find(|c| c.label() == class);
        match (class, after) {
            (Some(class), Some(after)) => steps.push((class, Duration::from_secs(after))),
            _ => complaints.push(format!(
                "{entry} -- malformed, so nothing will happen; expected \
                 <ttl|webhooks|embeddings>[:<after_secs>]"
            )),
        }
    }
    (steps, complaints)
}

impl TestSwitches {
    /// Read the switches from the environment. A `KIMMY_TEST_STALL_RUNTIME` above
    /// the cap **refuses the start**; the others that do not read are announced
    /// and ignored.
    pub fn from_env() -> Result<Self, String> {
        Self::read(|name| std::env::var(name).ok())
    }

    pub(crate) fn read(var: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let mut switches = Self::default();
        if let Some(value) = var("KIMMY_TEST_YIELD_SCALE") {
            match value.trim().parse::<u32>() {
                Ok(k) if k >= 1 => {
                    switches.scale = Some(k);
                    switches.notes.push((
                        "KIMMY_TEST_YIELD_SCALE",
                        value,
                        "divides the evaluator's tick, the latch hold and the back-off window"
                            .into(),
                    ));
                }
                _ => switches.notes.push((
                    "KIMMY_TEST_YIELD_SCALE",
                    value,
                    "not a whole number of at least 1, so nothing will happen".into(),
                )),
            }
        }
        if let Some(value) = var("KIMMY_TEST_STALL_RUNTIME") {
            let stall = parse_stall_runtime(&value)?;
            switches.stall_runtime = Some(stall);
            switches.notes.push((
                "KIMMY_TEST_STALL_RUNTIME",
                value,
                "blocks every runtime worker on purpose once the node serves".into(),
            ));
        }
        if let Some(value) = var("KIMMY_TEST_HOLD_WRITER") {
            let hold = parse_hold_writer(&value)?;
            switches.hold_writer = Some(hold);
            switches.notes.push((
                "KIMMY_TEST_HOLD_WRITER",
                value,
                "takes the single writer on purpose once the node serves, and holds it".into(),
            ));
        }
        if let Some(value) = var("KIMMY_TEST_FAIL_STEP") {
            let (steps, complaints) = parse_fail_steps(&value);
            let note = if complaints.is_empty() {
                "fails a class's local step on purpose once the node serves, without calling the \
                 backend"
                    .to_string()
            } else {
                complaints.join("; ")
            };
            switches.fail_steps = steps;
            switches.notes.push(("KIMMY_TEST_FAIL_STEP", value, note));
        }
        if let Some(value) = var("KIMMY_TEST_REFUSE_SYNC") {
            // Refused rather than ignored, like a bad runtime stall: a typo must
            // not leave a test believing a peer refuses when it does not.
            if value != "1" {
                return Err(format!(
                    "KIMMY_TEST_REFUSE_SYNC={value:?} is not a value: the only one is `1`"
                ));
            }
            switches.refuse_sync = true;
            switches.notes.push((
                "KIMMY_TEST_REFUSE_SYNC",
                value,
                "makes this member complete no replication contact in either direction once it serves: \
                 its listener accepts and drops peers, and it dials none (SWIM and the client API are \
                 not affected)"
                    .into(),
            ));
        }
        Ok(switches)
    }

    /// Say at `WARN`, once at start, which test switches are set: they are in the
    /// shipped binary, so none may sit on unnoticed in a deployment.
    pub fn announce(&self) {
        for (name, value, note) in &self.notes {
            warn!(
                switch = *name,
                value = %value,
                "a test switch is set that {note}; unset it outside a test"
            );
        }
    }

    /// The node serves: arm what waits for it. The fail steps count their delay from
    /// here, and the runtime stall begins. `stop` ends the stall at the stop's first
    /// signal.
    pub fn arm(&self, stop: kimmy_task::Shutdown, engine: &Arc<kimmy_storage::Engine>) {
        for (class, after) in &self.fail_steps {
            class_step::set_test_fail_step(*class, *after);
        }
        if !self.fail_steps.is_empty() {
            class_step::arm_test_fail_steps();
        }
        if self.refuse_sync {
            info!("test switch armed: the replication listener now drops every peer connection");
            kimmy_cluster::transport::set_test_refuse_sync(true);
        }
        if let Some(hold) = self.hold_writer {
            info!(
                ms = hold.ms,
                holder = hold.holder.label(),
                "test switch armed: the writer is held now that the node serves"
            );
            hold_writer(hold, Arc::clone(engine));
        }
        if let Some(stall) = self.stall_runtime {
            info!(
                ms = stall.ms,
                period_secs = stall.period.as_secs(),
                "test switch armed: the runtime stall begins now that the node serves"
            );
            inject_stalls(stall, stop);
        }
    }
}

/// Take the writer as `hold.holder` from a plain OS thread and keep it for
/// `hold.ms`: the wedge `KIMMY_TEST_HOLD_WRITER` stands for. The engine is moved
/// into the thread, so it is held for the length of the hold; a stop that arrives
/// during it waits on the writer as it would behind any hold.
fn hold_writer(hold: HoldWriter, engine: Arc<kimmy_storage::Engine>) {
    // UNSUPERVISED: a test switch's holder, ended by its own deadline; the wedge it causes is the point of it, and nothing restarts it
    let spawned = std::thread::Builder::new().name("test-hold-writer".into()).spawn(move || {
        let guard = engine.hold_writer(hold.holder);
        std::thread::sleep(Duration::from_millis(hold.ms));
        drop(guard);
        info!(holder = hold.holder.label(), "test switch ended: the writer is let go");
    });
    if let Err(e) = spawned {
        warn!(error = %e, "could not start the writer-holding thread, so nothing will happen");
    }
}

/// Block every runtime worker for `stall.ms`, once every `stall.period`, from a
/// plain OS thread that hands the runtime tasks through its injection queue (never
/// a worker's own queue, which a worker would run in order).
///
/// Each task sleeps in 50 ms slices until **a common absolute deadline**, so no
/// barrier is needed and a worker that picks up two finds the second already due.
/// The thread keeps spawning one task per worker every 10 ms until the deadline, so
/// any worker that frees up picks one up. It ends at the stop's first signal.
fn inject_stalls(stall: StallRuntime, stop: kimmy_task::Shutdown) {
    let handle = tokio::runtime::Handle::current();
    let workers = handle.metrics().num_workers().max(1);
    // UNSUPERVISED: a test switch's injector, ended by the stop; the stall it causes is the point of it, and nothing restarts it
    let spawned = std::thread::Builder::new().name("test-stall-runtime".into()).spawn(move || {
        loop {
            let deadline = std::time::Instant::now() + Duration::from_millis(stall.ms);
            while std::time::Instant::now() < deadline && !stop.has_begun() {
                for _ in 0..workers {
                    let stop = stop.clone();
                    // UNSUPERVISED: a task that blocks a worker on purpose for the test switch and ends at its deadline
                    handle.spawn(async move {
                        while std::time::Instant::now() < deadline && !stop.has_begun() {
                            std::thread::sleep(Duration::from_millis(50));
                        }
                    });
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            // Until the next period starts.
            let next = deadline + stall.period.saturating_sub(Duration::from_millis(stall.ms));
            while std::time::Instant::now() < next {
                if stop.has_begun() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            if stop.has_begun() {
                return;
            }
        }
    });
    if let Err(e) = spawned {
        warn!(error = %e, "could not start the runtime-stall injector, so nothing will happen");
    }
}

/// What `KIMMY_TEST_KILL_TASK` asks of the evaluator thread: it can panic or stall;
/// a return or an error makes no sense for a thread and is ignored (and said at the
/// start, by the switch's own parser).
pub fn kill_switch_of(kill: kimmy_task::Kill) -> Option<KillSwitch> {
    match kill {
        kimmy_task::Kill::Panic => Some(KillSwitch::Panic),
        kimmy_task::Kill::Stall(_) => Some(KillSwitch::Stall),
        _ => None,
    }
}

/// The three classes' heartbeat cells, which live for the process.
pub struct Cells(pub [&'static ClassCell; 3]);

impl Cells {
    pub fn new() -> Self {
        let clock = Arc::new(MonotonicClock::new());
        Self(
            [0, 1, 2]
                .map(|_| ClassCell::leak(Arc::clone(&clock) as Arc<dyn class_step::StepClock>)),
        )
    }

    pub fn ttl(&self) -> &'static ClassCell {
        self.0[0]
    }

    pub fn webhooks(&self) -> &'static ClassCell {
        self.0[1]
    }

    /// The embedding class's cell, which the worker's task runs under.
    pub fn embeddings(&self) -> &'static ClassCell {
        self.0[2]
    }
}

impl Default for Cells {
    fn default() -> Self {
        Self::new()
    }
}

fn phase_of(phase: class_step::Phase) -> Phase {
    match phase {
        class_step::Phase::Waiting => Phase::Waiting,
        class_step::Phase::Local => Phase::Local,
        class_step::Phase::Remote => Phase::Remote,
        class_step::Phase::WriterGate => Phase::WriterGate,
    }
}

/// What the evaluator thread reads each tick: atomics, one view load, and nothing
/// that takes a lock a runtime task takes or touches the engine.
pub struct YieldInputs {
    pub cells: [&'static ClassCell; 3],
    pub marks: Arc<ProbeMarks>,
    pub members: Option<kimmy_cluster::Members>,
    pub catch_up: Option<Arc<kimmy_cluster::catchup::CatchUp>>,
    pub ttl_disabled: bool,
    pub embeddings_disabled: bool,
    /// The stop has begun: every class is neutral.
    pub stopping: Arc<AtomicBool>,
    /// The collections this member's own block lists as TTL-indexed, kept by the
    /// facts hook: the target rule before the first TTL pass.
    pub ttl_holders: Arc<ArcSwap<Vec<CollectionId>>>,
    /// Which hold has the writer, and since when: the engine's word, read without
    /// the engine (ADR-220).
    pub writer_hold: Arc<kimmy_storage::writer_hold::WriterHoldWord>,
}

impl Inputs for YieldInputs {
    fn probe_wakes(&self) -> u64 {
        self.marks.wakes()
    }

    fn probe_take_worst(&self) -> Duration {
        self.marks.take_worst()
    }

    fn classes(&self) -> PerClass<ClassSample> {
        let gated = self.catch_up.as_ref().is_some_and(|c| c.is_set());
        let sample = |i: usize, switched_off: bool| {
            let r = self.cells[i].reading();
            ClassSample {
                owned: r.owned,
                phase: phase_of(r.phase),
                since_beat: r.since_beat,
                since_phase: r.since_phase,
                local_ok: r.local_ok,
                local_fault: r.local_fault,
                remote_fault: r.remote_fault,
                config_fault: r.config_fault,
                cycles: r.cycles,
                cycles_bad: r.cycles_bad,
                gated,
                switched_off,
            }
        };
        PerClass {
            ttl: sample(0, self.ttl_disabled),
            webhooks: sample(1, false),
            embeddings: sample(2, self.embeddings_disabled || !EMBEDDINGS_INSTRUMENTED),
        }
    }

    fn owned_ttl(&self) -> Option<Vec<CollectionId>> {
        self.cells[0].owned_ttl().map(|owned| owned.as_ref().clone())
    }

    fn ttl_holders(&self) -> Vec<CollectionId> {
        self.ttl_holders.load().as_ref().clone()
    }

    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    fn view(&self) -> Arc<PeerView> {
        self.members.as_ref().map_or_else(Arc::default, kimmy_cluster::Members::view)
    }

    /// The hold that has the writer now, unless its holder has no bound (an index
    /// build or drop, which scale with the data): those are never judged
    /// (ADR-220).
    fn writer_hold(&self) -> Option<WriterHoldSample> {
        let hold = self.writer_hold.reading()?;
        (!hold.holder.is_unbounded())
            .then(|| WriterHoldSample { age: hold.age(), holder: hold.holder.label() })
    }

    fn kill_switch(&self) -> Option<KillSwitch> {
        kill_switch_of(kimmy_task::kill_for("yield_evaluator")?)
    }
}

/// What puts the evaluator's decision into this member's block, and keeps the
/// TTL-indexed collection list for the evaluator.
#[derive(Clone)]
pub struct FactsHook {
    pub published: Arc<Published>,
    pub clock: Arc<RealClock>,
    /// The evaluator's tick, which the staleness rule is a multiple of.
    pub e: Duration,
    pub ttl_holders: Arc<ArcSwap<Vec<CollectionId>>>,
}

impl FactsHook {
    /// Called with every block the facts source builds: fills `class_state`,
    /// `class_cause`, `responsive` and `yielding` from what the evaluator last
    /// published (every class `unknown` and `responsive` none if it has not ticked
    /// for two ticks, the bits kept), and remembers the TTL list.
    pub fn apply(&self, facts: &mut Facts) {
        self.ttl_holders.store(Arc::new(facts.ttl.iter().map(|held| held.collection).collect()));
        self.published.apply_to(facts, self.clock.now().as_millis() as u64, self.e);
    }
}

/// Say, once, what this start's evaluator does.
pub fn announce(enabled: bool, probation: bool, timings: &kimmy_cluster::yielding::Timings) {
    info!(
        enabled,
        probation,
        tick_ms = timings.e.as_millis() as u64,
        "ownership yielding: {}",
        if enabled {
            "on (KIMMY_OWNERSHIP_YIELD=off turns it off)"
        } else {
            "OFF by KIMMY_OWNERSHIP_YIELD=off: this member judges and advertises but never sets \
             its own yield bits, and still honours its peers'"
        }
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(vars: &[(&str, &str)]) -> Result<TestSwitches, String> {
        TestSwitches::read(|name| {
            vars.iter().find(|(k, _)| *k == name).map(|(_, v)| (*v).to_string())
        })
    }

    /// Unset or `on` is on and `off` is off, in any case; any other value refuses.
    #[test]
    fn the_off_switch_reads_on_off_and_refuses_everything_else() {
        assert_eq!(parse_ownership_yield(None), Ok(true));
        assert_eq!(parse_ownership_yield(Some("")), Ok(true));
        for on in ["on", "ON", " On "] {
            assert_eq!(parse_ownership_yield(Some(on)), Ok(true), "{on}");
        }
        for off in ["off", "OFF", "Off"] {
            assert_eq!(parse_ownership_yield(Some(off)), Ok(false), "{off}");
        }
        for bad in ["0", "false", "no", "of", "offf", "disabled"] {
            let refused = parse_ownership_yield(Some(bad)).expect_err(bad);
            assert!(
                refused.contains("KIMMY_OWNERSHIP_YIELD") && refused.contains("off"),
                "{refused}"
            );
        }
    }

    #[test]
    fn the_runtime_stall_is_capped_below_swims_suspicion_and_must_be_well_formed() {
        assert_eq!(
            read(&[("KIMMY_TEST_STALL_RUNTIME", "1500/2")]).unwrap().stall_runtime,
            Some(StallRuntime { ms: 1500, period: Duration::from_secs(2) })
        );
        assert!(read(&[("KIMMY_TEST_STALL_RUNTIME", "3000/5")]).is_ok(), "the cap is inclusive");
        for bad in ["3001/5", "0/5", "1500/0", "1500", "x/2", "1500/x", "-1/2", "/"] {
            assert!(
                read(&[("KIMMY_TEST_STALL_RUNTIME", bad)]).is_err(),
                "{bad} must refuse the start"
            );
        }
    }

    /// `KIMMY_TEST_REFUSE_SYNC` reads only as `1`: any other value refuses the start
    /// (an unset one is off), so a typo cannot leave a test believing a peer refuses.
    #[test]
    fn the_refuse_sync_switch_reads_only_one_and_refuses_the_start_otherwise() {
        assert!(!read(&[]).unwrap().refuse_sync);
        let on = read(&[("KIMMY_TEST_REFUSE_SYNC", "1")]).unwrap();
        assert!(on.refuse_sync);
        assert!(on.notes.iter().any(|(name, _, _)| *name == "KIMMY_TEST_REFUSE_SYNC"));
        for bad in ["0", "true", "", "yes", "2", " 1", "1 ", " 1 ", "01"] {
            assert!(
                read(&[("KIMMY_TEST_REFUSE_SYNC", bad)]).is_err(),
                "{bad:?} must refuse the start"
            );
        }
    }

    #[test]
    fn the_scale_and_the_fail_steps_read_and_the_rest_is_said_and_ignored() {
        let switches = read(&[
            ("KIMMY_TEST_YIELD_SCALE", "5"),
            ("KIMMY_TEST_FAIL_STEP", "webhooks:90, ttl,embeddings:3"),
        ])
        .unwrap();
        assert_eq!(switches.scale, Some(5));
        assert_eq!(
            switches.fail_steps,
            vec![
                (Class::Webhooks, Duration::from_secs(90)),
                (Class::Ttl, Duration::ZERO),
                (Class::Embeddings, Duration::from_secs(3)),
            ]
        );
        for bad in ["0", "x", "-3", ""] {
            let switches = read(&[("KIMMY_TEST_YIELD_SCALE", bad)]).unwrap();
            assert_eq!(switches.scale, None, "{bad:?}");
            assert!(switches.notes[0].2.contains("nothing will happen"));
        }
        let switches = read(&[("KIMMY_TEST_FAIL_STEP", "webhooks:x,nothing,ttl:2")]).unwrap();
        assert_eq!(switches.fail_steps, vec![(Class::Ttl, Duration::from_secs(2))]);
        assert!(
            switches.notes[0].2.contains("webhooks:x") && switches.notes[0].2.contains("nothing")
        );
        assert!(read(&[]).unwrap().notes.is_empty(), "nothing set, nothing said");
    }

    /// A panic request reaches the evaluator as a panic, a stall as
    /// a stall, and nothing else as anything.
    #[test]
    fn the_kill_modes_map_to_what_the_evaluator_can_do() {
        use kimmy_task::Kill;
        assert_eq!(kill_switch_of(Kill::Panic), Some(KillSwitch::Panic));
        assert_eq!(kill_switch_of(Kill::Stall(None)), Some(KillSwitch::Stall));
        assert_eq!(
            kill_switch_of(Kill::Stall(Some(Duration::from_secs(3)))),
            Some(KillSwitch::Stall)
        );
        assert_eq!(kill_switch_of(Kill::Return), None);
        assert_eq!(kill_switch_of(Kill::Error), None);
    }

    fn inputs(cells: &Cells) -> YieldInputs {
        YieldInputs {
            cells: cells.0,
            marks: Arc::default(),
            members: None,
            catch_up: None,
            ttl_disabled: false,
            embeddings_disabled: false,
            stopping: Arc::default(),
            ttl_holders: Arc::default(),
            writer_hold: Arc::default(),
        }
    }

    /// What the evaluator reads is the cells, field by field, class by class: a
    /// class's counters under its own name, the phases mapped, the stop and the
    /// probe through their atomics. The embeddings class is neutral until the worker
    /// beats its cell, or it would read as overdue for ever.
    #[test]
    fn the_inputs_read_each_class_from_its_own_cell() {
        let cells = Cells::new();
        let mut inputs = inputs(&cells);
        cells.ttl().ok();
        cells.ttl().ok();
        cells.ttl().set_phase(class_step::Phase::Remote);
        cells.webhooks().local_fault();
        cells.webhooks().cycle(true);
        cells.webhooks().set_owned(7);
        cells.webhooks().set_phase(class_step::Phase::WriterGate);
        cells.ttl().set_owned_ttl(vec![CollectionId(3)]);
        let classes = inputs.classes();
        assert_eq!((classes.ttl.local_ok, classes.ttl.phase), (2, Phase::Remote));
        assert_eq!(
            (classes.webhooks.local_fault, classes.webhooks.cycles_bad, classes.webhooks.owned),
            (1, 1, 7)
        );
        assert_eq!(classes.webhooks.phase, Phase::WriterGate);
        assert_eq!(
            (classes.webhooks.local_ok, classes.ttl.local_fault),
            (0, 0),
            "not each other's"
        );
        assert!(!classes.ttl.switched_off && !classes.webhooks.switched_off);
        assert_eq!(
            classes.embeddings.switched_off,
            !EMBEDDINGS_INSTRUMENTED || inputs.embeddings_disabled,
            "neutral until the worker beats"
        );
        assert_eq!(inputs.owned_ttl(), Some(vec![CollectionId(3)]));
        // The operator's switches and the stop.
        inputs.ttl_disabled = true;
        inputs.embeddings_disabled = true;
        let classes = inputs.classes();
        assert!(classes.ttl.switched_off && classes.embeddings.switched_off);
        assert!(!inputs.stopping());
        inputs.stopping.store(true, Ordering::SeqCst);
        assert!(inputs.stopping());
        // The probe's marks are atomics.
        inputs.marks.record(Duration::from_millis(40));
        inputs.marks.record(Duration::from_millis(10));
        assert_eq!(inputs.probe_wakes(), 2);
        assert_eq!(inputs.probe_take_worst(), Duration::from_millis(40));
        assert_eq!(inputs.probe_take_worst(), Duration::ZERO, "taken once");
        // With no clustering there is no one to yield to.
        assert!(inputs.view().live.is_empty());
        assert_eq!(inputs.kill_switch(), None, "nothing armed");
    }

    /// The evaluator is given the hold that has the writer, with its age and the
    /// holder's label, unless the holder has no bound: an index build, an index
    /// drop and a rewind are never judged, however long they last (ADR-220).
    #[test]
    fn the_inputs_read_the_writers_hold_and_skip_the_holders_that_have_no_bound() {
        use kimmy_storage::WriterHolder;
        let cells = Cells::new();
        let inputs = inputs(&cells);
        assert_eq!(inputs.writer_hold(), None, "free");
        for holder in WriterHolder::ALL {
            // The word is the engine's; a bare one of its own is published through
            // an engine, as the daemon's is.
            let dir = tempfile::tempdir().unwrap();
            let engine = kimmy_storage::Engine::open(&dir.path().join("kimmy.redb")).unwrap();
            let inputs =
                YieldInputs { writer_hold: engine.writer_hold_word(), ..self::inputs(&cells) };
            let hold = engine.hold_writer(holder);
            std::thread::sleep(Duration::from_millis(20));
            let read = inputs.writer_hold();
            match holder {
                WriterHolder::IndexBuild | WriterHolder::IndexDrop | WriterHolder::Rewind => {
                    assert_eq!(read, None, "{} is never judged", holder.label());
                }
                _ => {
                    let read = read.unwrap_or_else(|| panic!("{} is judged", holder.label()));
                    assert_eq!(read.holder, holder.label());
                    assert!(read.age >= Duration::from_millis(20), "{:?}", read.age);
                }
            }
            drop(hold);
            assert_eq!(inputs.writer_hold(), None, "{}: let go", holder.label());
        }
    }

    /// `KIMMY_TEST_HOLD_WRITER` is `<ms>,<holder>` with a holder by its label, and
    /// anything else refuses the start, so a typo cannot leave a test believing the
    /// writer is held.
    #[test]
    fn the_hold_writer_switch_reads_a_duration_and_a_holder_and_refuses_the_rest() {
        let ok = read(&[("KIMMY_TEST_HOLD_WRITER", "70000, replication")]).unwrap();
        assert_eq!(
            ok.hold_writer,
            Some(HoldWriter { ms: 70_000, holder: kimmy_storage::WriterHolder::Replication })
        );
        assert!(ok.notes.iter().any(|(name, _, _)| *name == "KIMMY_TEST_HOLD_WRITER"));
        for holder in kimmy_storage::WriterHolder::ALL {
            let value = format!("1,{}", holder.label());
            assert_eq!(
                read(&[("KIMMY_TEST_HOLD_WRITER", &value)]).unwrap().hold_writer.map(|h| h.holder),
                Some(holder),
                "{value}"
            );
        }
        assert!(
            read(&[("KIMMY_TEST_HOLD_WRITER", &format!("{HOLD_WRITER_MAX_MS},write"))]).is_ok()
        );
        for bad in [
            "",
            "5000",
            ",write",
            "write",
            "x,write",
            "0,write",
            "-5,write",
            "3600001,write",
            "5000,",
            "5000,nobody",
            "5000,Write",
            "5000,write,extra",
        ] {
            let refused = read(&[("KIMMY_TEST_HOLD_WRITER", bad)]).expect_err(bad);
            assert!(refused.contains("KIMMY_TEST_HOLD_WRITER"), "{refused}");
        }
        assert_eq!(read(&[]).unwrap().hold_writer, None);
    }

    /// A class that is catching up is gated, whichever class it is.
    #[test]
    fn a_member_that_is_catching_up_gates_every_class() {
        let dir = tempfile::tempdir().unwrap();
        let catch_up = kimmy_cluster::catchup::CatchUp::open(dir.path(), Duration::from_secs(120));
        let cells = Cells::new();
        let mut inputs = inputs(&cells);
        inputs.catch_up = Some(Arc::clone(&catch_up));
        assert!(!inputs.classes().webhooks.gated);
        catch_up.mark(kimmy_cluster::CatchUpReason::SeededEmpty).unwrap();
        let classes = inputs.classes();
        assert!(classes.ttl.gated && classes.webhooks.gated && classes.embeddings.gated);
    }

    /// The hook puts the evaluator's decision into the block and remembers the TTL
    /// list; with no tick for two ticks every class reads unknown and the bits stay.
    #[test]
    fn the_facts_hook_fills_the_block_from_what_the_evaluator_published() {
        use kimmy_cluster::yielding::{Config, Evaluator};
        let published = Arc::new(Published::default());
        let mut config = Config::new(kimmy_core::NodeId::from_bytes([1; 16]));
        config.probation = true;
        let (_, start) = Evaluator::start(config, std::time::Instant::now());
        let clock = Arc::new(RealClock::new());
        published.store_start(&start, clock.now().as_millis() as u64);
        let hook = FactsHook {
            published: Arc::clone(&published),
            clock,
            e: Duration::from_secs(5),
            ttl_holders: Arc::default(),
        };
        let mut block = Facts::default().with_ttl(vec![kimmy_cluster::TtlHeld {
            collection: CollectionId(9),
            digest: vec![1; 8],
        }]);
        hook.apply(&mut block);
        assert!(block.yielding.ttl && block.yielding.webhooks && block.yielding.embeddings);
        assert_eq!(
            block.class_cause.unwrap().webhooks,
            kimmy_cluster::StallCause::Probation,
            "a probation start yields from its first block"
        );
        assert_eq!(block.class_state.unwrap().ttl, kimmy_cluster::ClassState::Stalled);
        assert_eq!(block.writer_wedged, Some(false), "the flag is in every block that has ticked");
        assert_eq!(hook.ttl_holders.load().as_slice(), [CollectionId(9)]);
        // An evaluator that never ticked is told by its age.
        let wedged = FactsHook { e: Duration::ZERO, ..hook.clone() };
        std::thread::sleep(Duration::from_millis(5));
        let mut later = Facts::default();
        wedged.apply(&mut later);
        assert_eq!(later.class_state.unwrap().ttl, kimmy_cluster::ClassState::Unknown);
        assert_eq!(later.responsive, None);
        assert_eq!(later.writer_wedged, None, "a silent evaluator claims nothing of the writer");
        assert!(later.yielding.ttl, "the bits stay: silence is not evidence of recovery");
    }
}
