//! The evaluator thread against a real `Members` (ADR-213): it reads the view and
//! never a lock, takes a long pause as one long step, and is silent once frozen.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use kimmy_core::NodeId;

use super::driver::{
    Clock, EvaluatorHandle, Inputs, KillSwitch, ManualClock, prepare, run, spawn_evaluator,
};
use super::{ClassSample, Config as EvalConfig, EVAL_STEP, Phase, Published, Timings, Verdict};
use crate::facts::{ClassState, Facts, OwnerClass, PerClass};
use crate::membership::{Members, PeerView};

fn n(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 16])
}

fn a(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

/// What the thread reads: the cells are atomics the test moves, the probe is a
/// counter, and the view is `Members`'.
struct Fake {
    members: Members,
    probe_wakes: Arc<AtomicU64>,
    overdue: Arc<AtomicBool>,
    /// A handle to freeze from inside the tick: the stop's first signal arriving
    /// while a tick is being decided.
    freeze_in_tick: Arc<std::sync::Mutex<Option<EvaluatorHandle>>>,
    /// What the test switch asks of the thread.
    kill: Option<KillSwitch>,
}

impl Inputs for Fake {
    fn probe_wakes(&self) -> u64 {
        self.probe_wakes.load(Ordering::SeqCst)
    }

    fn probe_take_worst(&self) -> Duration {
        Duration::ZERO
    }

    fn classes(&self) -> PerClass<ClassSample> {
        let mut webhooks = ClassSample::at_rest(1);
        if self.overdue.load(Ordering::SeqCst) {
            webhooks.phase = Phase::Waiting;
            webhooks.since_beat = Duration::from_secs(3600);
            webhooks.since_phase = Duration::from_secs(3600);
        }
        PerClass { ttl: ClassSample::at_rest(0), webhooks, embeddings: ClassSample::at_rest(0) }
    }

    fn owned_ttl(&self) -> Option<Vec<kimmy_core::CollectionId>> {
        Some(Vec::new())
    }

    fn ttl_holders(&self) -> Vec<kimmy_core::CollectionId> {
        Vec::new()
    }

    fn kill_switch(&self) -> Option<KillSwitch> {
        self.kill
    }

    fn view(&self) -> Arc<PeerView> {
        if let Some(handle) = self.freeze_in_tick.lock().unwrap().take() {
            handle.freeze();
        }
        self.members.view()
    }
}

/// Wait, with a ceiling that fails only on a hang.
fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !done() {
        assert!(Instant::now() < deadline, "hung waiting for {what}");
        std::thread::sleep(Duration::from_millis(2));
    }
}

struct Driven {
    members: Members,
    clock: Arc<ManualClock>,
    handle: EvaluatorHandle,
    probe: Arc<AtomicU64>,
    overdue: Arc<AtomicBool>,
    freeze_in_tick: Arc<std::sync::Mutex<Option<EvaluatorHandle>>>,
}

impl Driven {
    fn start() -> Self {
        let members = Members::default();
        let clock = Arc::new(ManualClock::new());
        let probe = Arc::new(AtomicU64::new(0));
        let overdue = Arc::new(AtomicBool::new(false));
        let freeze_in_tick = Arc::new(std::sync::Mutex::new(None));
        let mut config = EvalConfig::new(n(9));
        config.timings = Timings::scaled(5);
        let handle = spawn_evaluator(
            config,
            Fake {
                members: members.clone(),
                probe_wakes: Arc::clone(&probe),
                overdue: Arc::clone(&overdue),
                freeze_in_tick: Arc::clone(&freeze_in_tick),
                kill: None,
            },
            Arc::clone(&clock) as Arc<dyn Clock>,
            Arc::new(Published::default()),
        )
        .unwrap();
        until("the evaluator thread to start", || handle.is_ready());
        Self { members, clock, handle, probe, overdue, freeze_in_tick }
    }

    /// Advance one control step and wait for the thread to take it. The probe
    /// wakes with it unless `probe_wakes` is false.
    fn step(&self, probe_wakes: bool) {
        let target = self.handle.steps() + 1;
        if probe_wakes {
            self.probe.fetch_add(1, Ordering::SeqCst);
        }
        self.clock.advance(EVAL_STEP);
        until("a control step", || self.handle.steps() >= target);
    }

    fn steps(&self, n: usize, probe_wakes: bool) {
        for _ in 0..n {
            self.step(probe_wakes);
        }
    }
}

/// B6.1: a thread holds `Members`' write lock while the evaluator ticks, and its
/// ticks keep advancing: it reads the view, never a lock.
#[test]
fn the_evaluator_ticks_while_members_write_lock_is_held() {
    let driven = Driven::start();
    let held = driven.members.write_lock_for_test();
    let before = driven.handle.published().ticks();
    // Four steps a tick at this scale: ten ticks.
    driven.steps(40, true);
    until("ticks to advance behind the lock", || driven.handle.published().ticks() >= before + 10);
    drop(held);
    driven.handle.freeze();
}

/// A 20 s gap is one long step and never a burst of catch-up ticks.
#[test]
fn a_long_pause_is_one_step_not_a_burst() {
    let driven = Driven::start();
    driven.steps(4, true);
    until("the first tick", || driven.handle.published().ticks() == 1);
    let steps = driven.handle.steps();
    driven.clock.advance(Duration::from_secs(20));
    until("the long step", || driven.handle.steps() > steps);
    assert_eq!(driven.handle.steps(), steps + 1, "one wake for twenty seconds");
    assert_eq!(driven.handle.published().ticks(), 1, "and no tick was made up");
    driven.handle.freeze();
}

/// B9: frozen at the stop's first signal, the thread issues no verdict, no
/// transition and no publication, however long the clock then runs without probe
/// wakes: a ring at N - 1 cannot enter during a stop.
#[test]
fn a_frozen_evaluator_publishes_nothing_and_enters_nothing() {
    let driven = Driven::start();
    // Five bad ticks (webhooks needs six): overdue, with the probe waking.
    driven.overdue.store(true, Ordering::SeqCst);
    driven.steps(4 * 5, true);
    until("five ticks", || driven.handle.published().ticks() == 5);
    assert_ne!(
        driven.handle.published().observations(OwnerClass::Webhooks, Verdict::Bad),
        0,
        "the condition is really present"
    );
    let version = driven.handle.published().version();
    let ticks = driven.handle.published().ticks();
    driven.handle.freeze();
    // Three E with no probe wake and the class still overdue: both would count.
    let steps = driven.handle.steps();
    for _ in 0..12 {
        driven.clock.advance(EVAL_STEP);
        // Give a thread that is still running the chance to take the step.
        std::thread::sleep(Duration::from_millis(5));
    }
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(driven.handle.steps(), steps, "the thread took no further step");
    assert_eq!(driven.handle.published().version(), version, "nothing was published");
    assert_eq!(driven.handle.published().ticks(), ticks);
    assert_eq!(
        driven.handle.published().advertised(1_000_000, Timings::scaled(5).e).class_state.webhooks,
        ClassState::Unknown,
        "and it is long past its last tick"
    );
}

/// A `Panic` kill switch really panics the thread, at the next
/// tick, which is what lets the supervisor read it as a death; the control is the
/// same thread with no switch, which runs on and is not dead.
#[test]
fn a_panic_switch_panics_the_evaluator_thread() {
    for (kill, panics) in [(None, false), (Some(KillSwitch::Panic), true)] {
        let clock = Arc::new(ManualClock::new());
        let published = Arc::new(Published::default());
        let mut config = EvalConfig::new(n(9));
        config.timings = Timings::scaled(5);
        let fake = Fake {
            members: Members::default(),
            probe_wakes: Arc::new(AtomicU64::new(0)),
            overdue: Arc::new(AtomicBool::new(false)),
            freeze_in_tick: Arc::new(std::sync::Mutex::new(None)),
            kill,
        };
        let probe = Arc::clone(&fake.probe_wakes);
        let prepared = prepare(config, clock.as_ref(), &published);
        let handle = EvaluatorHandle::new(Arc::clone(&clock) as Arc<dyn Clock>, published);
        let body = handle.clone();
        let join = std::thread::spawn(move || run(prepared, fake, &body));
        until("the thread to start", || handle.is_ready());
        for _ in 0..8 {
            let target = handle.steps() + 1;
            probe.fetch_add(1, Ordering::SeqCst);
            clock.advance(EVAL_STEP);
            if panics && join.is_finished() {
                break;
            }
            until("a step", || handle.steps() >= target || join.is_finished());
        }
        if panics {
            until("the panic", || join.is_finished());
            assert!(join.join().is_err(), "the thread panicked");
        } else {
            assert!(!join.is_finished(), "no switch, no death");
            handle.freeze();
            assert!(join.join().is_ok());
        }
    }
}

/// A freeze that lands while a tick is being decided keeps that
/// tick from being published, and the decision before it stands.
#[test]
fn a_freeze_during_a_tick_keeps_that_tick_unpublished() {
    let driven = Driven::start();
    driven.steps(4, true);
    until("the first tick", || driven.handle.published().ticks() == 1);
    let version = driven.handle.published().version();
    *driven.freeze_in_tick.lock().unwrap() = Some(driven.handle.clone());
    // The next tick's inputs are read, the stop's signal lands, the tick ends.
    driven.steps(4, true);
    until("the freeze to land in the tick", || driven.handle.is_frozen());
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(driven.handle.published().ticks(), 1, "the second tick was not published");
    assert_eq!(driven.handle.published().version(), version);
}

/// The thread publishes a yield and a stall once the evidence is there: the real
/// driver, the real view, a peer that is a target.
#[test]
fn the_driver_yields_to_a_target_it_read_from_the_view() {
    let driven = Driven::start();
    driven.members.insert_for_test(a(7001), n(1));
    driven.members.record_peer_facts_for_test(
        n(1),
        Facts {
            boot: vec![1; 16],
            class_state: Some(PerClass::all(ClassState::Ok)),
            responsive: Some(true),
            ..Facts::default()
        },
        Duration::ZERO,
    );
    driven.steps(4 * 6, true);
    driven.overdue.store(true, Ordering::SeqCst);
    // The peer's block must stay fresh against the manual clock: re-record it.
    for _ in 0..8 {
        driven.members.record_peer_facts_for_test(
            n(1),
            Facts {
                boot: vec![1; 16],
                class_state: Some(PerClass::all(ClassState::Ok)),
                responsive: Some(true),
                ..Facts::default()
            },
            Duration::ZERO,
        );
        driven.steps(4, true);
    }
    until("the yield", || {
        driven.handle.published().transitions(OwnerClass::Webhooks, super::Direction::Yield) == 1
    });
    let advertised = driven
        .handle
        .published()
        .advertised(driven.clock.now().as_millis() as u64, Timings::scaled(5).e);
    assert!(advertised.yielding.webhooks);
    assert_eq!(advertised.class_state.webhooks, ClassState::Stalled);
    driven.handle.freeze();
}
