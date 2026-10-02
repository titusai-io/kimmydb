//! The pure tests of the yield predicate (ADR-213): the evaluator is fed samples
//! with an injected `now`, so nothing here sleeps, reads a clock or starts a
//! thread.

use std::collections::BTreeMap;

use super::*;
use crate::membership::ViewBlock;

fn node(n: u8) -> NodeId {
    NodeId::from_bytes([n; 16])
}

fn base() -> Instant {
    // Far enough from the clock's origin to take a peer block's age from.
    Instant::now() + Duration::from_secs(100_000)
}

const W: OwnerClass = OwnerClass::Webhooks;
const E: OwnerClass = OwnerClass::Embeddings;
const T: OwnerClass = OwnerClass::Ttl;

fn peer_facts(state: ClassState, responsive: bool) -> Facts {
    Facts {
        boot: vec![7; 16],
        class_state: Some(PerClass::all(state)),
        class_cause: None,
        responsive: Some(responsive),
        ..Facts::default()
    }
}

fn stalled_peer(cause: StallCause) -> Facts {
    Facts {
        class_state: Some(PerClass::all(ClassState::Stalled)),
        class_cause: Some(PerClass::all(cause)),
        ..peer_facts(ClassState::Stalled, true)
    }
}

fn yielding_peer() -> Facts {
    Facts {
        yielding: Yielding { ttl: true, webhooks: true, embeddings: true },
        ..peer_facts(ClassState::Stalled, true)
    }
}

struct Peer {
    facts: Option<Facts>,
    received: Instant,
    /// Heard again at every tick, as a live peer is; `age` pins it instead.
    renewed: bool,
}

/// An evaluator and the world it is fed: one sample per tick, `E` apart.
struct Rig {
    ev: Evaluator,
    now: Instant,
    cs: PerClass<ClassSample>,
    peers: BTreeMap<NodeId, Peer>,
    measure: TickMeasure,
    owned_ttl: Option<Vec<CollectionId>>,
    ttl_holders: Vec<CollectionId>,
    stopping: bool,
    e: Duration,
    steps: u32,
    last: Decision,
}

impl Rig {
    fn new() -> Self {
        Self::with(Config::new(node(5)))
    }

    fn with(config: Config) -> Self {
        let now = base();
        let e = config.timings.e;
        let steps = config.timings.steps_per_tick();
        let (ev, last) = Evaluator::start(config, now);
        Self {
            ev,
            now,
            cs: PerClass::all(ClassSample::at_rest(0)),
            peers: BTreeMap::new(),
            measure: TickMeasure::quiet(steps),
            owned_ttl: Some(Vec::new()),
            ttl_holders: Vec::new(),
            stopping: false,
            e,
            steps,
            last,
        }
    }

    fn peer(&mut self, n: u8, facts: Facts) {
        self.peers.insert(node(n), Peer { facts: Some(facts), received: self.now, renewed: true });
    }

    /// A live peer this member holds no block from.
    fn unheard(&mut self, n: u8) {
        self.peers.insert(node(n), Peer { facts: None, received: self.now, renewed: true });
    }

    /// The peer's block was received `secs` ago.
    fn age(&mut self, n: u8, secs: u64) {
        let peer = self.peers.get_mut(&node(n)).unwrap();
        peer.received = self.now - Duration::from_secs(secs);
        peer.renewed = false;
    }

    fn drop_peer(&mut self, n: u8) {
        self.peers.remove(&node(n));
    }

    fn view(&self) -> Arc<PeerView> {
        let mut view = PeerView {
            version: 1,
            lease: Duration::from_secs(15),
            live: self.peers.keys().copied().collect(),
            blocks: BTreeMap::new(),
        };
        for (n, peer) in &self.peers {
            if let Some(facts) = &peer.facts {
                view.blocks.insert(
                    *n,
                    ViewBlock {
                        facts: Arc::new(facts.clone()),
                        generation: Some(1),
                        received: peer.received,
                    },
                );
            }
        }
        Arc::new(view)
    }

    fn owned(&mut self, class: OwnerClass, owned: u64) {
        let cs = self.cs_mut(class);
        cs.owned = owned;
    }

    fn cs_mut(&mut self, class: OwnerClass) -> &mut ClassSample {
        match class {
            OwnerClass::Ttl => &mut self.cs.ttl,
            OwnerClass::Webhooks => &mut self.cs.webhooks,
            OwnerClass::Embeddings => &mut self.cs.embeddings,
        }
    }

    /// The class has not beaten for an hour, in `phase`.
    fn overdue(&mut self, class: OwnerClass, phase: Phase) {
        let cs = self.cs_mut(class);
        cs.phase = phase;
        cs.since_beat = Duration::from_secs(3600);
        cs.since_phase = Duration::from_secs(3600);
    }

    fn healthy(&mut self, class: OwnerClass) {
        let cs = self.cs_mut(class);
        cs.phase = Phase::Waiting;
        cs.since_beat = Duration::ZERO;
        cs.since_phase = Duration::ZERO;
    }

    /// One more local success for the class: a good tick if it owns something.
    fn work(&mut self, class: OwnerClass) {
        let cs = self.cs_mut(class);
        cs.local_ok += 1;
        cs.cycles += 1;
    }

    fn fail_cycle(&mut self, class: OwnerClass) {
        let cs = self.cs_mut(class);
        cs.local_fault += 1;
        cs.cycles += 1;
        cs.cycles_bad += 1;
    }

    fn tick(&mut self) -> Decision {
        self.now += self.e;
        let measure = self.measure;
        self.tick_with(measure)
    }

    fn tick_with(&mut self, measure: TickMeasure) -> Decision {
        for peer in self.peers.values_mut().filter(|peer| peer.renewed) {
            peer.received = self.now;
        }
        let mut sample = Sample::new(measure, self.cs, self.view());
        sample.stopping = self.stopping;
        sample.owned_ttl = self.owned_ttl.clone();
        sample.ttl_holders = self.ttl_holders.clone();
        self.last = self.ev.tick(&sample, self.now);
        self.last.clone()
    }

    fn ticks(&mut self, n: usize) -> Decision {
        let mut last = self.last.clone();
        for _ in 0..n {
            last = self.tick();
        }
        last
    }

    /// Enough quiet ticks that the member is `responsive`.
    fn responsive(&mut self) {
        self.ticks(RESPONSIVE_TICKS);
        assert!(self.last.responsive);
    }

    /// Every tick until `pred` holds, at most `limit`.
    fn until(&mut self, limit: usize, pred: impl Fn(&Decision) -> bool) -> usize {
        for i in 1..=limit {
            let d = self.tick();
            if pred(&d) {
                return i;
            }
        }
        panic!("not reached in {limit} ticks: {:?}", self.last)
    }

    fn state(&self, class: OwnerClass) -> ClassState {
        self.last.classes.of(class).state
    }

    fn yielding(&self, class: OwnerClass) -> bool {
        self.last.classes.of(class).yielding
    }

    fn suppressed(&self, class: OwnerClass) -> Option<Suppression> {
        self.last.classes.of(class).suppressed
    }
}

fn events_of(decision: &Decision, pred: impl Fn(&Event) -> bool) -> usize {
    decision.events.iter().filter(|event| pred(event)).count()
}

/// A rig whose webhooks class owns one subscription and is overdue, with one
/// healthy responsive peer, driven until it has yielded.
fn yielded_rig() -> Rig {
    let mut rig = Rig::new();
    rig.responsive();
    rig.peer(1, peer_facts(ClassState::Ok, true));
    rig.owned(W, 1);
    rig.overdue(W, Phase::Waiting);
    rig.until(20, |d| d.classes.webhooks.yielding);
    rig
}

// ---------------------------------------------------------------------------
// N of M, windows, evidence.
// ---------------------------------------------------------------------------

/// Webhooks and embeddings need 6 bad items of the last 12, ttl 3 of 5: one short
/// is `suspect`, and the next is `stalled`.
#[test]
fn n_of_m_edges_per_class() {
    for (class, n) in [(W, 6), (E, 6), (T, 3)] {
        let mut rig = Rig::new();
        rig.owned(class, 1);
        rig.overdue(class, Phase::Waiting);
        for i in 1..n {
            rig.tick();
            assert_eq!(rig.state(class), ClassState::Suspect, "{class:?} after {i} bad items");
        }
        let d = rig.tick();
        assert_eq!(rig.state(class), ClassState::Stalled, "{class:?} at {n} bad items");
        assert_eq!(
            events_of(&d, |e| matches!(e, Event::Stalled { class: c, .. } if *c == class)),
            1
        );
    }
}

/// A bad item older than the window is shed: an idle class does not stay
/// `suspect` for ever, and five old bad items plus one new one are not six.
#[test]
fn the_window_sheds_old_bad_items() {
    let mut rig = Rig::new();
    rig.owned(W, 1);
    rig.overdue(W, Phase::Waiting);
    rig.ticks(5);
    assert_eq!(rig.state(W), ClassState::Suspect);
    // The class recovers and is quiet for longer than its 60 s window.
    rig.healthy(W);
    rig.ticks(13);
    assert_eq!(rig.state(W), ClassState::Ok, "the old items are shed");
    // One bad item now is one, not six.
    rig.overdue(W, Phase::Waiting);
    rig.tick();
    assert_eq!(rig.state(W), ClassState::Suspect);
}

/// A failed ttl pass is one bad item, not one per tick until the next pass.
#[test]
fn a_failed_ttl_pass_is_one_item() {
    let mut rig = Rig::new();
    rig.owned(T, 1);
    rig.fail_cycle(T);
    rig.tick();
    assert_eq!(rig.state(T), ClassState::Suspect);
    // The counters do not move again: nothing else is counted.
    rig.ticks(10);
    assert_eq!(rig.state(T), ClassState::Suspect, "one item stays one item");
    // Three failed passes are three items.
    rig.fail_cycle(T);
    rig.tick();
    rig.fail_cycle(T);
    rig.tick();
    assert_eq!(rig.state(T), ClassState::Stalled);
}

/// A class that is overdue is `suspect` at once, owning or not, and a class that
/// is gated, switched off or stopping judges nothing.
#[test]
fn overdue_is_suspect_at_once_and_neutral_classes_judge_nothing() {
    let mut rig = Rig::new();
    rig.overdue(W, Phase::Local);
    let d = rig.tick();
    assert_eq!(d.classes.webhooks.state, ClassState::Suspect, "overdue, owning nothing");
    assert_eq!(d.classes.webhooks.verdict, Verdict::Bad);

    for gate in 0..3 {
        let mut rig = Rig::new();
        rig.owned(W, 1);
        rig.overdue(W, Phase::Waiting);
        match gate {
            0 => rig.cs.webhooks.gated = true,
            1 => rig.cs.webhooks.switched_off = true,
            _ => rig.stopping = true,
        }
        let d = rig.ticks(20);
        assert_eq!(d.classes.webhooks.verdict, Verdict::Neutral, "gate {gate}");
        assert_ne!(d.classes.webhooks.state, ClassState::Stalled, "gate {gate}");
    }
}

/// The bound of each phase, per class: past it is bad, within it is not, a
/// `Remote` wait past its bound is a local fault, `WriterGate` is never overdue,
/// and a phase re-stamped by a fresh attempt is not overdue however long ago the
/// beat was.
#[test]
fn overdue_per_phase() {
    let probe = |class: OwnerClass, phase: Phase, since: u64, since_phase: u64| {
        let mut rig = Rig::new();
        rig.owned(class, 1);
        let cs = rig.cs_mut(class);
        cs.phase = phase;
        cs.since_beat = Duration::from_secs(since);
        cs.since_phase = Duration::from_secs(since_phase);
        rig.tick().classes.of(class).verdict
    };
    // Webhooks: Waiting 30 s, Local 30 s, Remote 25 s.
    assert_eq!(probe(W, Phase::Waiting, 31, 31), Verdict::Bad);
    assert_eq!(probe(W, Phase::Waiting, 29, 29), Verdict::Neutral);
    assert_eq!(probe(W, Phase::Local, 31, 31), Verdict::Bad);
    assert_eq!(probe(W, Phase::Local, 29, 29), Verdict::Neutral);
    assert_eq!(probe(W, Phase::Remote, 26, 26), Verdict::Bad, "a Remote past its bound is local");
    assert_eq!(probe(W, Phase::Remote, 24, 24), Verdict::Neutral);
    // Embeddings: Waiting 30 s, Remote 75 s.
    assert_eq!(probe(E, Phase::Waiting, 31, 31), Verdict::Bad);
    assert_eq!(probe(E, Phase::Remote, 74, 74), Verdict::Neutral);
    assert_eq!(probe(E, Phase::Remote, 76, 76), Verdict::Bad);
    // Ttl: Waiting is ttl_interval + 12 E (120 s), Local 30 s, no Remote phase.
    assert_eq!(probe(T, Phase::Waiting, 119, 119), Verdict::Neutral);
    assert_eq!(probe(T, Phase::Waiting, 121, 121), Verdict::Bad);
    assert_eq!(probe(T, Phase::Local, 31, 31), Verdict::Bad);
    assert_eq!(probe(T, Phase::Remote, 9_999, 9_999), Verdict::Neutral, "ttl has no Remote bound");
    // The gate is never overdue.
    for class in OwnerClass::ALL {
        assert_eq!(probe(class, Phase::WriterGate, 9_999, 9_999), Verdict::Neutral, "{class:?}");
    }
    // A beat long ago, but the phase was re-stamped a moment ago (a fresh attempt).
    assert_eq!(probe(E, Phase::Remote, 9_999, 1), Verdict::Neutral);
    // And the other way: a recent beat in a phase entered long ago.
    assert_eq!(probe(W, Phase::Local, 1, 9_999), Verdict::Neutral);
}

/// A running class that completes work is good, one that owns nothing and is
/// responsive is idle-alive, and one that owns something and does nothing counts
/// as neither.
#[test]
fn good_idle_and_neutral_ticks() {
    let mut rig = Rig::new();
    rig.owned(W, 1);
    rig.work(W);
    assert_eq!(rig.tick().classes.webhooks.verdict, Verdict::Good);
    assert_eq!(rig.tick().classes.webhooks.verdict, Verdict::Neutral, "no progress, no fault");
    rig.owned(W, 0);
    assert_eq!(
        rig.tick().classes.webhooks.verdict,
        Verdict::Neutral,
        "idle-alive needs a responsive member, and none has run six ticks yet"
    );
    rig.ticks(RESPONSIVE_TICKS);
    assert_eq!(rig.tick().classes.webhooks.verdict, Verdict::Idle);
}

// ---------------------------------------------------------------------------
// The runtime.
// ---------------------------------------------------------------------------

fn measure(gap_ms: u64, probe_ms: u64, woke: bool) -> TickMeasure {
    TickMeasure {
        steps: STEPS_PER_TICK,
        control_worst_gap: Duration::from_millis(gap_ms),
        probe_worst: Duration::from_millis(probe_ms),
        probe_woke: woke,
    }
}

/// A void tick judges nothing, and so does the tick after it.
#[test]
fn a_void_tick_and_the_one_after_judge_nothing() {
    let mut rig = Rig::new();
    rig.owned(W, 1);
    rig.overdue(W, Phase::Waiting);
    let late = rig.tick_with(measure(600, 0, true));
    assert_eq!(late.runtime, RuntimeVerdict::Void);
    assert_eq!(late.classes.webhooks.verdict, Verdict::Void);
    assert_eq!(late.classes.webhooks.state, ClassState::Idle, "nothing was judged");
    let after = rig.tick_with(measure(0, 5_000, true));
    assert_eq!(after.runtime, RuntimeVerdict::Void, "the tick after a void one is void too");
    assert_eq!(after.classes.webhooks.verdict, Verdict::Void);
    // A probe that read 5 s late is judged once the control has been on time.
    let judged = rig.tick_with(measure(0, 5_000, true));
    assert_eq!(judged.runtime, RuntimeVerdict::Stalled);
    assert_eq!(judged.classes.webhooks.verdict, Verdict::Bad);
}

/// A void tick does not discard the deltas: what a class did across it is read by
/// the next tick that judges.
#[test]
fn a_void_tick_keeps_the_counters_for_the_next_judgement() {
    let mut rig = Rig::new();
    rig.owned(W, 1);
    rig.tick();
    rig.work(W);
    rig.tick_with(measure(900, 0, true));
    rig.tick_with(measure(0, 0, true));
    let judged = rig.tick();
    assert_eq!(judged.classes.webhooks.verdict, Verdict::Good, "the work done in the void tick");
}

/// A tick whose probe lateness is the whole story: over S is stalled, at S is not.
#[test]
fn the_probe_lateness_over_s_is_a_runtime_stall() {
    let mut rig = Rig::new();
    assert_eq!(rig.tick_with(measure(0, 1_000, true)).runtime, RuntimeVerdict::Fine);
    assert_eq!(rig.tick_with(measure(0, 1_001, true)).runtime, RuntimeVerdict::Stalled);
    // The control's own lateness below V says nothing about the runtime.
    assert_eq!(rig.tick_with(measure(400, 0, true)).runtime, RuntimeVerdict::Fine);
}

/// No probe wake is a stall only after 20 on-time control steps in non-void ticks,
/// and never in a void tick.
#[test]
fn no_wake_is_judged_only_in_a_tick_that_is_not_void() {
    let mut rig = Rig::new();
    assert_eq!(rig.tick_with(measure(0, 0, false)).runtime, RuntimeVerdict::Stalled);
    // The same tick with a late control: void, and nothing was a stall.
    let mut rig = Rig::new();
    assert_eq!(rig.tick_with(measure(700, 0, false)).runtime, RuntimeVerdict::Void);
    assert_eq!(rig.tick_with(measure(0, 0, false)).runtime, RuntimeVerdict::Void);
    // A void tick resets the running count.
    let mut rig = Rig::with({
        let mut c = Config::new(node(5));
        c.timings = Timings::scaled(5);
        c
    });
    let four = TickMeasure { steps: 4, ..measure(0, 0, false) };
    for _ in 0..4 {
        assert_eq!(rig.tick_with(four).runtime, RuntimeVerdict::Fine);
    }
    let late = TickMeasure { control_worst_gap: Duration::from_millis(700), ..four };
    assert_eq!(rig.tick_with(late).runtime, RuntimeVerdict::Void);
    assert_eq!(rig.tick_with(four).runtime, RuntimeVerdict::Void, "the tick after");
    for _ in 0..4 {
        assert_eq!(rig.tick_with(four).runtime, RuntimeVerdict::Fine, "the count started again");
    }
    assert_eq!(rig.tick_with(four).runtime, RuntimeVerdict::Stalled);
}

/// At a scale (E of one second, four steps a tick) a stall of three quarters of a
/// second, which wakes the probe in a tick and not the next, is not a stall: the
/// no-wake rule counts 20 on-time steps whatever the scale.
#[test]
fn no_wake_needs_twenty_steps_at_any_scale() {
    let mut config = Config::new(node(5));
    config.timings = Timings::scaled(5);
    assert_eq!(config.timings.steps_per_tick(), 4);
    let mut rig = Rig::with(config);
    let four = TickMeasure { steps: 4, ..measure(0, 0, false) };
    for i in 0..4 {
        assert_eq!(rig.tick_with(four).runtime, RuntimeVerdict::Fine, "tick {i}");
    }
    // A wake in between starts the count again.
    let woke = TickMeasure { probe_woke: true, ..four };
    assert_eq!(rig.tick_with(woke).runtime, RuntimeVerdict::Fine);
    for _ in 0..4 {
        assert_eq!(rig.tick_with(four).runtime, RuntimeVerdict::Fine);
    }
    assert_eq!(rig.tick_with(four).runtime, RuntimeVerdict::Stalled, "twenty steps");
}

/// The sweep: a whole-process pause of 0.5 to 2 s, at every phase offset of the
/// probe and of the control (10 ms steps), and at every place in the tick the late
/// wake can fall (including the tick's last step, with the probe's late wake read by
/// the next tick), is never judged a stall.
#[test]
fn a_pause_of_half_to_two_seconds_is_never_judged_stalled() {
    let ms = Duration::from_millis;
    let mut judged = 0u64;
    for pause in (500..=2_000u64).step_by(10) {
        for probe_phase in (0..250u64).step_by(10) {
            for control_phase in (0..250u64).step_by(10) {
                for (k, probe_in_next) in [(0usize, false), (9, false), (19, false), (19, true)] {
                    let _ = k;
                    let mut rig = Rig::new();
                    rig.owned(W, 1);
                    let control = ms(pause - control_phase);
                    let probe = ms(pause - probe_phase);
                    let ticks = [
                        TickMeasure::quiet(STEPS_PER_TICK),
                        TickMeasure {
                            control_worst_gap: control,
                            probe_worst: if probe_in_next { Duration::ZERO } else { probe },
                            ..TickMeasure::quiet(STEPS_PER_TICK)
                        },
                        TickMeasure {
                            probe_worst: if probe_in_next { probe } else { Duration::ZERO },
                            ..TickMeasure::quiet(STEPS_PER_TICK)
                        },
                        TickMeasure::quiet(STEPS_PER_TICK),
                    ];
                    for tick in ticks {
                        let d = rig.tick_with(tick);
                        judged += 1;
                        assert_ne!(
                            d.runtime,
                            RuntimeVerdict::Stalled,
                            "pause {pause} ms, probe phase {probe_phase}, control phase \
                             {control_phase}, probe in next tick {probe_in_next}"
                        );
                    }
                }
            }
        }
    }
    assert!(judged > 100_000, "the sweep ran: {judged}");
}

/// The control's tracker measures the gap between consecutive wakes over the step,
/// and a long gap is one long step, never a burst.
#[test]
fn the_control_measures_gaps_between_wakes() {
    let ms = Duration::from_millis;
    let mut tracker = ControlTracker::new(ms(0));
    tracker.wake(ms(250));
    tracker.wake(ms(500));
    tracker.wake(ms(1_250)); // 500 ms late
    tracker.wake(ms(1_500));
    let m = tracker.take(true, ms(10));
    assert_eq!((m.steps, m.control_worst_gap, m.probe_worst), (4, ms(500), ms(10)));
    // The next tick starts clean, from the last wake.
    tracker.wake(ms(1_750));
    let m = tracker.take(false, Duration::ZERO);
    assert_eq!((m.steps, m.control_worst_gap, m.probe_woke), (1, Duration::ZERO, false));
    // A 20 s pause is one step with one long gap.
    tracker.wake(ms(21_750));
    let m = tracker.take(true, Duration::ZERO);
    assert_eq!((m.steps, m.control_worst_gap), (1, ms(19_750)));
}

/// Responsive is quick to false and slow to true: six non-void ticks without a
/// stalled verdict, none before.
#[test]
fn responsive_is_quick_to_false_and_slow_to_true() {
    let mut rig = Rig::new();
    for i in 1..RESPONSIVE_TICKS {
        assert!(!rig.tick().responsive, "tick {i}");
    }
    assert!(rig.tick().responsive);
    assert!(!rig.tick_with(measure(0, 2_000, true)).responsive, "one stalled tick");
    for i in 1..RESPONSIVE_TICKS {
        assert!(!rig.tick().responsive, "{i} ticks after");
    }
    assert!(rig.tick().responsive, "six clean ticks after");
    // Void ticks neither count towards it nor break it.
    rig.tick_with(measure(900, 0, true));
    assert!(rig.last.responsive);
}

/// The runtime stalled while a class owns work is a bad item for that class, with a
/// runtime cause; a class that owns nothing is not touched. Local items alone
/// reaching N upgrade the cause.
#[test]
fn a_runtime_stall_is_evidence_against_a_class_that_owns_work_only() {
    let mut rig = Rig::new();
    rig.owned(W, 1);
    let d = rig.tick_with(measure(0, 3_000, true));
    assert_eq!(d.classes.webhooks.verdict, Verdict::Bad);
    assert_ne!(d.classes.embeddings.verdict, Verdict::Bad, "owns nothing");
    for _ in 0..5 {
        rig.tick_with(measure(0, 3_000, true));
    }
    let d = rig.last.classes.webhooks;
    assert_eq!((d.state, d.cause), (ClassState::Stalled, Some(StallCause::Runtime)));
    // Local items reach N inside the episode: the cause is upgraded, never downgraded.
    rig.overdue(W, Phase::Waiting);
    for _ in 0..6 {
        rig.tick();
    }
    assert_eq!(rig.last.classes.webhooks.cause, Some(StallCause::Local));
    rig.healthy(W);
    rig.ticks(3);
    assert_eq!(rig.last.classes.webhooks.cause, Some(StallCause::Local), "never downgraded");
}

/// The cause of an episode is decided at entry: `local` only when local items alone
/// reach N, so a mix is `runtime`.
#[test]
fn a_mix_of_local_and_runtime_items_enters_as_runtime() {
    let mut rig = Rig::new();
    rig.owned(W, 1);
    rig.overdue(W, Phase::Waiting);
    rig.ticks(3);
    rig.healthy(W);
    for _ in 0..3 {
        rig.tick_with(measure(0, 3_000, true));
    }
    let d = rig.last.classes.webhooks;
    assert_eq!((d.state, d.cause), (ClassState::Stalled, Some(StallCause::Runtime)));
}

// ---------------------------------------------------------------------------
// The target predicate.
// ---------------------------------------------------------------------------

/// `suppressed` for a stalled webhooks class with `peer` as the only other member.
fn webhooks_stalled_beside(peer: Option<Facts>, age: u64) -> Rig {
    let mut rig = Rig::new();
    rig.responsive();
    if let Some(facts) = peer {
        rig.peer(1, facts);
        if age > 0 {
            rig.age(1, age);
        }
    }
    rig.owned(W, 1);
    rig.overdue(W, Phase::Waiting);
    rig.ticks(8);
    assert_eq!(rig.state(W), ClassState::Stalled);
    rig
}

#[test]
fn the_target_predicate() {
    // An ok or idle, responsive, fresh peer is a target: the class yields.
    for state in [ClassState::Ok, ClassState::Idle] {
        let rig = webhooks_stalled_beside(Some(peer_facts(state, true)), 0);
        assert!(rig.yielding(W), "{state:?}");
    }
    let refused = |facts: Option<Facts>, age: u64| {
        let rig = webhooks_stalled_beside(facts, age);
        assert!(!rig.yielding(W));
        assert_eq!(rig.suppressed(W), Some(Suppression::NoTarget));
    };
    // Not responsive, whether ok or idle.
    refused(Some(peer_facts(ClassState::Ok, false)), 0);
    refused(Some(peer_facts(ClassState::Idle, false)), 0);
    refused(Some(Facts { responsive: None, ..peer_facts(ClassState::Ok, true) }), 0);
    // Suspect, stalled or unknown: never.
    refused(Some(peer_facts(ClassState::Suspect, true)), 0);
    refused(Some(peer_facts(ClassState::Stalled, true)), 0);
    refused(Some(peer_facts(ClassState::Unknown, true)), 0);
    // A 0.43 peer: no class_state at all.
    refused(Some(Facts { class_state: None, ..peer_facts(ClassState::Ok, true) }), 0);
    // A stale block: past the 15 s lease.
    refused(Some(peer_facts(ClassState::Ok, true)), 16);
    // Catching up, or yielding the class.
    refused(Some(Facts { catching_up: true, ..peer_facts(ClassState::Ok, true) }), 0);
    refused(
        Some(Facts {
            yielding: Yielding { webhooks: true, ..Yielding::default() },
            ..peer_facts(ClassState::Ok, true)
        }),
        0,
    );
    // No peers at all.
    refused(None, 0);
}

/// The target must pass `may_own` for the class: embeddings off, ttl off.
#[test]
fn a_target_must_be_able_to_own_the_class() {
    let mut rig = Rig::new();
    rig.responsive();
    rig.peer(1, Facts { embeddings_disabled: true, ..peer_facts(ClassState::Ok, true) });
    rig.owned(E, 1);
    rig.overdue(E, Phase::Waiting);
    rig.ticks(8);
    assert_eq!(rig.state(E), ClassState::Stalled);
    assert_eq!(rig.suppressed(E), Some(Suppression::NoTarget), "its embedding worker is off");
    rig.peer(1, peer_facts(ClassState::Ok, true));
    rig.tick();
    assert!(rig.yielding(E));
}

/// The TTL target rule: the peer must list a collection this member owns after its
/// first pass, or one in this member's own holder list before it.
#[test]
fn the_ttl_target_rule_before_and_after_the_first_pass() {
    let held = |c: u64| crate::facts::TtlHeld { collection: CollectionId(c), digest: vec![1; 8] };
    let peer = |lists: &[u64]| {
        peer_facts(ClassState::Ok, true).with_ttl(lists.iter().map(|c| held(*c)).collect())
    };
    let stalled = |owned_ttl: Option<Vec<CollectionId>>, holders: Vec<CollectionId>, p: Facts| {
        let mut rig = Rig::new();
        rig.responsive();
        rig.peer(1, p);
        rig.owned(T, 1);
        rig.owned_ttl = owned_ttl;
        rig.ttl_holders = holders;
        rig.overdue(T, Phase::Waiting);
        rig.ticks(6);
        assert_eq!(rig.state(T), ClassState::Stalled);
        rig.yielding(T)
    };
    // After the first pass: this member owned collection 1.
    assert!(stalled(
        Some(vec![CollectionId(1)]),
        vec![CollectionId(1), CollectionId(2)],
        peer(&[1])
    ));
    assert!(
        !stalled(Some(vec![CollectionId(1)]), vec![CollectionId(1), CollectionId(2)], peer(&[2])),
        "the peer lists only a collection this member did not own"
    );
    assert!(!stalled(Some(vec![]), vec![CollectionId(1)], peer(&[1])), "it owned nothing");
    assert!(!stalled(Some(vec![]), vec![CollectionId(1)], peer(&[3])), "it owned nothing");
    // Before it: the member's own holder list.
    assert!(stalled(None, vec![CollectionId(2)], peer(&[2, 3])));
    assert!(!stalled(None, vec![CollectionId(2)], peer(&[3])));
    // A peer with ttl off, or a listing it truncated, is no holder.
    assert!(!stalled(None, vec![CollectionId(2)], Facts { ttl_disabled: true, ..peer(&[2]) }));
}

/// A member whose last pass ran ungated and owned nothing does not yield its
/// expiry when that task wedges, even though a peer lists the very collection it
/// holds: it owns nothing, so there is nothing to hand over, and the class says
/// `no_target` and why. The control is the same member with a pass that owned the
/// collection, which yields to the same peer.
#[test]
fn a_member_that_owns_nothing_does_not_yield_expiry_and_says_why() {
    let held = |c: u64| crate::facts::TtlHeld { collection: CollectionId(c), digest: vec![1; 8] };
    let run = |owned_ttl: Option<Vec<CollectionId>>, owned: u64| {
        let mut rig = Rig::new();
        rig.responsive();
        rig.peer(1, peer_facts(ClassState::Ok, true).with_ttl(vec![held(1)]));
        rig.owned(T, owned);
        rig.owned_ttl = owned_ttl;
        rig.ttl_holders = vec![CollectionId(1)];
        rig.overdue(T, Phase::Waiting);
        rig.ticks(6);
        assert_eq!(
            rig.state(T),
            ClassState::Stalled,
            "an overdue class is bad whether or not it owns"
        );
        rig
    };
    let rig = run(Some(vec![]), 0);
    assert!(!rig.yielding(T), "it owns nothing, so there is nothing to hand over");
    assert_eq!(rig.suppressed(T), Some(Suppression::NoTarget));
    let mut sample = Sample::new(rig.measure, rig.cs, rig.view());
    sample.owned_ttl = Some(vec![]);
    sample.ttl_holders = vec![CollectionId(1)];
    let said = explain_no_target(&sample, OwnerClass::Ttl, rig.now);
    assert!(
        said.iter().all(|line| line.contains("this member's scope: [] (owned)")),
        "the WARN names the empty scope it judged by: {said:?}"
    );
    let rig = run(Some(vec![CollectionId(1)]), 1);
    assert!(rig.yielding(T), "control: it owned the collection the peer lists");
}

// ---------------------------------------------------------------------------
// Yielding, the cap, the tie-break, the latch.
// ---------------------------------------------------------------------------

#[test]
fn a_stalled_class_yields_to_a_target_and_says_so() {
    let mut rig = Rig::new();
    rig.responsive();
    rig.peer(1, peer_facts(ClassState::Ok, true));
    rig.owned(W, 1);
    rig.overdue(W, Phase::Waiting);
    let mut yielded = None;
    for _ in 0..8 {
        let d = rig.tick();
        for event in &d.events {
            if let Event::Yielded { class, cause, targets, .. } = event {
                yielded = Some((*class, *cause, targets.clone()));
            }
        }
    }
    assert_eq!(yielded, Some((W, StallCause::Local, vec![node(1)])));
    assert!(rig.yielding(W));
    assert!(!rig.yielding(E) && !rig.yielding(T), "only the class that is stalled");
    assert_eq!(rig.last.yielding(), Yielding { webhooks: true, ..Yielding::default() });
}

/// The cap counts every yielder of the class, fresh **or stale**, and
/// bounds them at `max(1, floor((n - 1) / 2))`: 1 at n = 2, 3 and 4, 2 at n = 5.
#[test]
fn the_cap_counts_stale_yielders() {
    // n = 3 (two peers): the cap is 1, and a peer already yielding fills it.
    let mut rig = Rig::new();
    rig.responsive();
    rig.peer(1, peer_facts(ClassState::Ok, true));
    rig.peer(2, yielding_peer());
    rig.owned(W, 1);
    rig.overdue(W, Phase::Waiting);
    rig.ticks(8);
    assert!(!rig.yielding(W));
    assert_eq!(rig.suppressed(W), Some(Suppression::Cap));
    // The same with the yielder's block long stale: still counted. Its block is
    // stale, and peer 1 stays fresh.
    rig.age(2, 600);
    rig.peer(1, peer_facts(ClassState::Ok, true));
    rig.ticks(3);
    assert!(!rig.yielding(W), "a stale yielding bit is still honoured by `candidates`");
    assert_eq!(rig.suppressed(W), Some(Suppression::Cap));
    // n = 5 (four peers): the cap is 2, so one yielder leaves room for one more.
    let mut rig = Rig::new();
    rig.responsive();
    rig.peer(1, peer_facts(ClassState::Ok, true));
    rig.peer(2, yielding_peer());
    rig.peer(3, peer_facts(ClassState::Ok, true));
    rig.peer(4, peer_facts(ClassState::Ok, true));
    rig.owned(W, 1);
    rig.overdue(W, Phase::Waiting);
    rig.ticks(8);
    assert!(rig.yielding(W), "two yielders at n = 5");
    // A third is not.
    let mut rig = Rig::new();
    rig.responsive();
    rig.peer(1, peer_facts(ClassState::Ok, true));
    rig.peer(2, yielding_peer());
    rig.peer(3, yielding_peer());
    rig.peer(4, peer_facts(ClassState::Ok, true));
    rig.owned(W, 1);
    rig.overdue(W, Phase::Waiting);
    rig.ticks(8);
    assert!(!rig.yielding(W));
    assert_eq!(rig.suppressed(W), Some(Suppression::Cap));
}

/// Over the cap, the yielders past it in ascending node-id order withdraw, so the
/// outcome is deterministic once views converge: the lowest id stays.
#[test]
fn the_tie_break_keeps_the_lowest_node_id() {
    // This member is node 5, and node 7 also yields: node 5 is first, and stays.
    let mut rig = yielded_rig();
    rig.peer(7, yielding_peer());
    let d = rig.ticks(2);
    assert!(rig.yielding(W), "{d:?}");
    // Node 2 yields as well: node 5 is now second of two, past the cap of 1.
    rig.peer(2, yielding_peer());
    let d = rig.tick();
    assert!(!rig.yielding(W), "node 5 is past the cap");
    assert_eq!(
        events_of(&d, |e| matches!(e, Event::Withdrew { reason: WithdrawReason::TieBreak, .. })),
        1
    );
    assert_eq!(rig.state(W), ClassState::Stalled, "withdrawn, not recovered");
    assert_eq!(rig.suppressed(W), None, "the tick that withdraws does not yield again");
    rig.tick();
    assert_eq!(rig.suppressed(W), Some(Suppression::Cap), "and the cap now holds it");
}

/// Two members `stalled` with a local cause set the latch: nobody yields, and a
/// yielder withdraws at once.
#[test]
fn the_latch_stops_a_yield_and_withdraws_one() {
    // Not yet yielded: a peer that is stalled locally too.
    let mut rig = Rig::new();
    rig.responsive();
    rig.peer(1, peer_facts(ClassState::Ok, true));
    rig.peer(2, stalled_peer(StallCause::Local));
    rig.owned(W, 1);
    rig.overdue(W, Phase::Waiting);
    rig.ticks(8);
    assert_eq!(rig.state(W), ClassState::Stalled);
    assert!(!rig.yielding(W));
    assert_eq!(rig.suppressed(W), Some(Suppression::SharedFault));
    assert!(rig.last.classes.webhooks.latched);
    // A yielder: the second stalled member appears after it yielded.
    let mut rig = yielded_rig();
    rig.peer(2, stalled_peer(StallCause::Local));
    let d = rig.tick();
    assert!(!rig.yielding(W));
    assert_eq!(
        events_of(&d, |e| matches!(e, Event::Withdrew { reason: WithdrawReason::SharedFault, .. })),
        1
    );
    assert_eq!(rig.state(W), ClassState::Stalled, "still stalled, owning again");
}

/// The latch holds for 30 minutes after fewer than two are stalled, then clears.
#[test]
fn the_latch_holds_thirty_minutes_then_clears() {
    let mut rig = Rig::new();
    rig.responsive();
    rig.peer(1, peer_facts(ClassState::Ok, true));
    rig.peer(2, stalled_peer(StallCause::Local));
    rig.owned(W, 1);
    rig.overdue(W, Phase::Waiting);
    rig.ticks(8);
    assert!(rig.last.classes.webhooks.latched);
    // The other member recovers.
    rig.peer(2, peer_facts(ClassState::Ok, true));
    // 29 minutes later it is still set (360 ticks of 5 s is 30 minutes).
    rig.ticks(340);
    assert!(rig.last.classes.webhooks.latched);
    assert!(!rig.yielding(W));
    rig.ticks(30);
    assert!(!rig.last.classes.webhooks.latched, "cleared after 30 minutes");
    // And a class that is still stalled now yields.
    rig.ticks(2);
    assert!(rig.yielding(W));
}

/// A stale `stalled` peer does not count: only lease-fresh blocks do.
#[test]
fn a_stale_stalled_peer_does_not_latch() {
    let mut rig = Rig::new();
    rig.responsive();
    rig.peer(1, peer_facts(ClassState::Ok, true));
    rig.peer(2, stalled_peer(StallCause::Local));
    rig.age(2, 600);
    rig.owned(W, 1);
    rig.overdue(W, Phase::Waiting);
    rig.ticks(8);
    assert!(!rig.last.classes.webhooks.latched);
    assert!(rig.yielding(W));
}

/// Runtime and probation stalls never latch: a yielder with one peer
/// `stalled(runtime)` and one `stalled(probation)` keeps its yield.
#[test]
fn runtime_and_probation_stalls_do_not_latch() {
    let mut rig = yielded_rig();
    rig.peer(2, stalled_peer(StallCause::Runtime));
    rig.peer(3, stalled_peer(StallCause::Probation));
    rig.peer(4, stalled_peer(StallCause::Unknown));
    rig.ticks(6);
    assert!(rig.yielding(W));
    assert!(!rig.last.classes.webhooks.latched);
    // And this member's own cause counts too: a runtime stall here plus a local
    // one there is one local stall, not two.
    let mut rig = Rig::new();
    rig.responsive();
    rig.peer(1, peer_facts(ClassState::Ok, true));
    rig.peer(2, stalled_peer(StallCause::Local));
    rig.owned(W, 1);
    for _ in 0..8 {
        rig.tick_with(measure(0, 3_000, true));
    }
    assert_eq!(rig.last.classes.webhooks.cause, Some(StallCause::Runtime));
    assert!(!rig.last.classes.webhooks.latched, "one local stall, and ours is a runtime one");
}

/// While yielded the target is required only to enter: it going suspect,
/// unresponsive or unknown, or leaving, does not end the yield.
#[test]
fn a_yielded_class_does_not_recheck_its_target() {
    for how in 0..4 {
        let mut rig = yielded_rig();
        match how {
            0 => rig.peer(1, peer_facts(ClassState::Suspect, true)),
            1 => rig.peer(1, peer_facts(ClassState::Ok, false)),
            2 => rig.peer(1, peer_facts(ClassState::Unknown, true)),
            _ => rig.drop_peer(1),
        }
        rig.ticks(10);
        assert!(rig.yielding(W), "case {how}");
    }
}

/// The first failing reason, in the order the switch, the latch, the target, the cap.
#[test]
fn the_suppression_reasons_come_in_order() {
    let mk = |enabled: bool, latch: bool, target: bool, cap: bool| {
        let mut config = Config::new(node(5));
        config.enabled = enabled;
        let mut rig = Rig::with(config);
        rig.responsive();
        if target {
            rig.peer(1, peer_facts(ClassState::Ok, true));
        } else {
            rig.peer(1, peer_facts(ClassState::Suspect, true));
        }
        if latch {
            rig.peer(2, stalled_peer(StallCause::Local));
        }
        if cap {
            rig.peer(3, yielding_peer());
        }
        rig.owned(W, 1);
        rig.overdue(W, Phase::Waiting);
        rig.ticks(8);
        rig.suppressed(W)
    };
    assert_eq!(mk(false, true, false, true), Some(Suppression::SwitchedOff));
    assert_eq!(mk(true, true, false, true), Some(Suppression::SharedFault));
    assert_eq!(mk(true, false, false, true), Some(Suppression::NoTarget));
    assert_eq!(mk(true, false, true, true), Some(Suppression::Cap));
    assert_eq!(mk(true, false, true, false), None);
}

/// A suppression is said once per `WARN_INTERVAL` per class.
#[test]
fn a_suppression_is_said_once_per_interval() {
    let mut rig = Rig::new();
    rig.responsive();
    rig.owned(W, 1);
    rig.overdue(W, Phase::Waiting);
    let mut said = 0;
    let mut said_at = Vec::new();
    for i in 0..120 {
        let d = rig.tick();
        let n = events_of(&d, |e| matches!(e, Event::Suppressed { class: W, .. }));
        said += n;
        if n > 0 {
            said_at.push(i);
        }
    }
    // 120 ticks of 5 s is 600 s: at the first stall and about 300 s later.
    assert_eq!(said, 2, "{said_at:?}");
}

/// The off switch: judged and advertised, never a bit, and the reason says why.
#[test]
fn the_off_switch_never_sets_a_bit() {
    let mut config = Config::new(node(5));
    config.enabled = false;
    let mut rig = Rig::with(config);
    rig.responsive();
    rig.peer(1, peer_facts(ClassState::Ok, true));
    rig.owned(W, 1);
    rig.overdue(W, Phase::Waiting);
    rig.ticks(30);
    assert_eq!(rig.state(W), ClassState::Stalled, "still judged and advertised");
    assert!(!rig.yielding(W));
    assert_eq!(rig.suppressed(W), Some(Suppression::SwitchedOff));
    assert_eq!(rig.last.yielding(), Yielding::default());
}

// ---------------------------------------------------------------------------
// Reclaim.
// ---------------------------------------------------------------------------

/// The episode: stalled, yielded, then idle and responsive for R ticks.
fn reclaim_after(rig: &mut Rig) -> (usize, u32, u32) {
    rig.owned(W, 0);
    rig.healthy(W);
    for i in 1..=500 {
        let d = rig.tick();
        for event in &d.events {
            if let Event::Reclaimed { r_ticks, step, yielded: true, .. } = event {
                assert!(!rig.yielding(W), "leaving clears the bit at once");
                return (i, *r_ticks, *step);
            }
        }
    }
    panic!("never reclaimed");
}

/// R good ticks reclaim: exactly 24 at first, the state leaves `stalled`,
/// and the class owns again at once.
#[test]
fn reclaim_takes_r_ticks_and_re_owns_at_once() {
    let mut rig = yielded_rig();
    let (ticks, r, step) = reclaim_after(&mut rig);
    assert_eq!((ticks, r, step), (24, 24, 0));
    assert_ne!(rig.state(W), ClassState::Stalled);
    assert!(!rig.yielding(W));
}

/// A bad tick resets the run; void and neutral ticks neither count nor break it.
#[test]
fn reclaim_counts_only_consecutive_good_or_idle_ticks() {
    let mut rig = yielded_rig();
    rig.owned(W, 0);
    rig.healthy(W);
    rig.ticks(20);
    assert!(rig.state(W) == ClassState::Stalled);
    // Void ticks in the middle neither count nor break.
    rig.tick_with(measure(900, 0, true));
    rig.tick_with(measure(0, 0, true));
    rig.ticks(1);
    // A neutral tick (the class is gated for a moment) neither counts nor breaks.
    rig.cs.webhooks.gated = true;
    assert_eq!(rig.tick().classes.webhooks.verdict, Verdict::Neutral);
    rig.cs.webhooks.gated = false;
    rig.ticks(2);
    assert_eq!(
        rig.state(W),
        ClassState::Stalled,
        "20 + 3 idle ticks, the void and the neutral ones not counted"
    );
    rig.tick();
    assert_ne!(rig.state(W), ClassState::Stalled, "the 24th idle tick");
    // And a bad one resets the run.
    let mut rig = yielded_rig();
    rig.owned(W, 0);
    rig.healthy(W);
    rig.ticks(20);
    rig.overdue(W, Phase::Waiting);
    rig.tick();
    rig.healthy(W);
    rig.ticks(20);
    assert_eq!(rig.state(W), ClassState::Stalled, "restarted at zero after the bad tick");
    rig.ticks(4);
    assert_ne!(rig.state(W), ClassState::Stalled);
}

/// Idle-alive needs a zero local-fault delta: a class that owns nothing and
/// keeps faulting never reclaims.
#[test]
fn idle_alive_needs_no_local_faults() {
    let mut rig = yielded_rig();
    rig.owned(W, 0);
    rig.healthy(W);
    for _ in 0..60 {
        rig.cs.webhooks.local_fault += 1;
        rig.tick();
    }
    assert_eq!(rig.state(W), ClassState::Stalled, "faults while owning nothing are not idle");
    rig.ticks(24);
    assert_ne!(rig.state(W), ClassState::Stalled);
}

/// Idle-alive needs a responsive member too.
#[test]
fn idle_alive_needs_a_responsive_member() {
    let mut rig = yielded_rig();
    rig.owned(W, 0);
    rig.healthy(W);
    rig.ticks(3);
    // The runtime stalls on every tick for 40 ticks: no progress towards reclaim.
    for _ in 0..40 {
        rig.tick_with(measure(0, 3_000, true));
    }
    assert_eq!(rig.state(W), ClassState::Stalled);
}

/// R doubles for each new yield within 30 minutes of the last reclaim, up to 384,
/// resets after 30 minutes, and starts again with a process.
#[test]
fn the_back_off_doubles_caps_and_resets() {
    let mut rig = yielded_rig();
    let mut seen = Vec::new();
    seen.push(reclaim_after(&mut rig).1);
    for _ in 0..5 {
        // Stall and yield again, at once.
        rig.owned(W, 1);
        rig.overdue(W, Phase::Waiting);
        rig.until(20, |d| d.classes.webhooks.yielding);
        seen.push(reclaim_after(&mut rig).1);
    }
    assert_eq!(seen, vec![24, 48, 96, 192, 384, 384], "doubled each time, capped at 32 minutes");

    // After 30 minutes with no yield the back-off resets.
    rig.ticks(361);
    rig.owned(W, 1);
    rig.overdue(W, Phase::Waiting);
    rig.until(20, |d| d.classes.webhooks.yielding);
    assert_eq!(reclaim_after(&mut rig).1, 24);

    // A restart is a new evaluator: it starts at the base.
    let mut fresh = yielded_rig();
    assert_eq!(reclaim_after(&mut fresh).1, 24);
}

/// A reclaim that left a bad item in the window is `suspect`, and another bad item
/// brings the class straight back to `stalled`.
#[test]
fn a_reclaim_leaves_the_state_the_window_says() {
    let mut rig = Rig::new();
    rig.responsive();
    rig.peer(
        1,
        peer_facts(ClassState::Ok, true).with_ttl(vec![crate::facts::TtlHeld {
            collection: CollectionId(1),
            digest: vec![1; 8],
        }]),
    );
    rig.owned_ttl = Some(vec![CollectionId(1)]);
    rig.owned(T, 1);
    rig.overdue(T, Phase::Waiting);
    rig.until(20, |d| d.classes.ttl.yielding);
    rig.owned(T, 0);
    rig.healthy(T);
    let (ticks, _, _) = {
        let mut out = None;
        for i in 1..=100 {
            let d = rig.tick();
            if events_of(&d, |e| matches!(e, Event::Reclaimed { class: T, .. })) > 0 {
                out = Some((i, 0, 0));
                break;
            }
        }
        out.unwrap()
    };
    assert_eq!(ticks, 24);
    assert_eq!(rig.state(T), ClassState::Suspect, "ttl's 300 s window still holds bad items");
}

// ---------------------------------------------------------------------------
// Evaluator state is recomputed every tick.
// ---------------------------------------------------------------------------

/// The state follows the samples: the first tick's verdict is not kept.
#[test]
fn the_state_is_recomputed_every_tick() {
    let mut rig = Rig::new();
    rig.owned(W, 1);
    rig.work(W);
    assert_eq!(rig.tick().classes.webhooks.state, ClassState::Ok);
    rig.owned(W, 0);
    rig.responsive();
    assert_eq!(rig.state(W), ClassState::Idle);
    rig.overdue(W, Phase::Waiting);
    assert_eq!(rig.tick().classes.webhooks.state, ClassState::Suspect);
    // And the evaluator's published copy follows it.
    assert_ne!(rig.last.classes.webhooks.state, ClassState::Ok);
}

// ---------------------------------------------------------------------------
// Probation.
// ---------------------------------------------------------------------------

fn probation_config(me: u8) -> Config {
    let mut config = Config::new(node(me));
    config.probation = true;
    config
}

/// A probation start publishes `stalled(probation)` and the yield bits **before its
/// first tick**, with the target waived: no peer is known, and it yields all the same.
#[test]
fn probation_yields_from_its_first_block_without_a_target() {
    let (_, start) = Evaluator::start(probation_config(5), base());
    for class in OwnerClass::ALL {
        let d = start.classes.of(class);
        assert_eq!(
            (d.state, d.cause, d.yielding),
            (ClassState::Stalled, Some(StallCause::Probation), true)
        );
    }
    assert_eq!(start.yielding(), Yielding { ttl: true, webhooks: true, embeddings: true });
    assert_eq!(events_of(&start, |e| matches!(e, Event::Yielded { .. })), 3);
    // It stays yielded through ordinary ticks with no target in sight.
    let mut rig = Rig::with(probation_config(5));
    rig.peer(1, peer_facts(ClassState::Suspect, false));
    rig.ticks(10);
    for class in OwnerClass::ALL {
        assert!(rig.yielding(class), "{class:?}");
    }
    // It leaves by the ordinary reclaim: 24 idle-alive ticks.
    let (ticks, r, _) = reclaim_after(&mut rig);
    assert_eq!(r, 24);
    assert!(ticks <= 24, "after the ticks already run, and not later than R: {ticks}");
}

/// The reason a peer is not a target is the predicate's own (`target_refusal`), one
/// line per live peer: what a `no_target` WARN prints. Each way of not being a target
/// has its own words, and a peer that is one says so.
#[test]
fn every_way_of_not_being_a_target_is_named() {
    let mut rig = Rig::new();
    rig.peer(1, peer_facts(ClassState::Stalled, true));
    rig.peer(2, peer_facts(ClassState::Suspect, true));
    rig.peer(3, peer_facts(ClassState::Ok, false));
    rig.peer(4, Facts { catching_up: true, ..peer_facts(ClassState::Ok, true) });
    rig.peer(5, yielding_peer());
    rig.unheard(6);
    rig.peer(7, peer_facts(ClassState::Ok, true));
    let mut sample = Sample::new(rig.measure, rig.cs, rig.view());
    sample.owned_ttl = None;
    let said = explain_no_target(&sample, OwnerClass::Webhooks, rig.now);
    let want = [
        (1, "class stalled"),
        (2, "class suspect"),
        (3, "runtime not responsive"),
        (4, "catching up"),
        (5, "already yields the class"),
        (6, "no block heard"),
        (7, "is a target"),
    ];
    for (n, why) in want {
        let line = said.iter().find(|l| l.contains(&format!("{:?}", node(n)))).unwrap();
        assert!(line.ends_with(why), "{line} should end with {why}");
    }
}

/// A class the operator has switched off does not start in probation: it judges
/// every tick neutral, which never counts towards reclaim, so it would never leave.
#[test]
fn probation_skips_a_class_that_is_switched_off() {
    let mut config = probation_config(5);
    config.off = PerClass { ttl: false, webhooks: false, embeddings: true };
    let (_, start) = Evaluator::start(config, base());
    assert_eq!(start.yielding(), Yielding { ttl: true, webhooks: true, embeddings: false });
    assert_eq!(start.classes.embeddings.state, ClassState::Idle);
    assert_eq!(start.classes.embeddings.cause, None);
    assert_eq!(start.classes.ttl.state, ClassState::Stalled);
    assert_eq!(events_of(&start, |e| matches!(e, Event::Yielded { .. })), 2);
}

/// With the switch off a probation start advertises `stalled` and sets no bit.
#[test]
fn probation_with_the_switch_off_sets_no_bit() {
    let mut config = probation_config(5);
    config.enabled = false;
    let (_, start) = Evaluator::start(config, base());
    assert_eq!(start.yielding(), Yielding::default());
    assert_eq!(start.classes.webhooks.state, ClassState::Stalled);
    assert_eq!(start.classes.webhooks.suppressed, Some(Suppression::SwitchedOff));
}

/// The cap and the tie-break still apply to probation: at n <= 4 a probation member
/// with a node id above a yielder's takes its yield back, and one below stays.
#[test]
fn the_tie_break_applies_to_a_probation_yield() {
    // This member is node 5; node 3 already yields.
    let mut rig = Rig::with(probation_config(5));
    rig.peer(3, yielding_peer());
    let d = rig.tick();
    assert!(!rig.yielding(W), "{d:?}");
    assert_eq!(
        events_of(&d, |e| matches!(e, Event::Withdrew { reason: WithdrawReason::TieBreak, .. })),
        3
    );
    // This member is node 2: it stays.
    let mut rig = Rig::with(probation_config(2));
    rig.peer(3, yielding_peer());
    rig.ticks(3);
    assert!(rig.yielding(W));
}

/// The waiver of the target condition lasts the **episode**: a probation
/// member that lost the tie-break and withdrew yields again when the winner's block
/// is gone, with no target anywhere in the cluster. The start's own waiver is
/// no help here, since the bits were already set once and withdrawn.
#[test]
fn a_withdrawn_probation_yield_returns_without_a_target() {
    let mut rig = Rig::with(probation_config(5));
    rig.peer(3, yielding_peer());
    rig.tick();
    assert!(!rig.yielding(W), "the control: it lost the tie-break and withdrew");
    // The winner's block is never heard again; no peer is a target.
    rig.unheard(3);
    rig.ticks(3);
    assert!(rig.yielding(W), "yields again with no target: {:?}", rig.last.classes.webhooks);
    assert_eq!(rig.last.classes.webhooks.suppressed, None);
}

/// The latch counts only local stalls, so probation members restarting around a
/// yielder do not make it withdraw; a probation episode that goes on to local
/// evidence does count.
#[test]
fn a_probation_episode_counts_for_the_latch_only_once_it_is_local() {
    let mut rig = Rig::with(probation_config(1));
    rig.responsive();
    rig.peer(2, stalled_peer(StallCause::Local));
    rig.ticks(3);
    assert!(!rig.last.classes.webhooks.latched, "a probation stall alone is no local stall");
    rig.owned(W, 1);
    rig.overdue(W, Phase::Waiting);
    rig.ticks(8);
    assert_eq!(rig.last.classes.webhooks.cause, Some(StallCause::Local));
    assert!(rig.last.classes.webhooks.latched);
}

mod probation_table {
    use super::super::probation::*;
    use super::super::{LOW_SPACE_CAP, SHORT_RUN};
    use std::time::Duration;

    const NOW: u64 = 10_000_000_000;
    const GIB: u64 = 1 << 30;

    fn inputs(end: PreviousEnd) -> Inputs {
        Inputs {
            end,
            free_available: Some(100 * GIB),
            fs_size: Some(1000 * GIB),
            last_start: Some(LastStart {
                at_ms: NOW - 3_600_000,
                inherited: "shutdown".into(),
                written_by: "0.44.0".into(),
            }),
            sidecar_written_by: Some("0.44.0".into()),
            now_ms: NOW,
            last_write_secs_ago: Some(5),
        }
    }

    fn unclean() -> PreviousEnd {
        PreviousEnd::Unclean { before: None }
    }

    #[test]
    fn a_failed_run_is_probation_unconditionally() {
        for end in [PreviousEnd::StorageFailed, PreviousEnd::TaskDied] {
            let mut input = inputs(end);
            input.last_start = None;
            assert!(decide(&input).probation);
        }
    }

    #[test]
    fn clean_ends_and_errors_never_are() {
        for end in [
            PreviousEnd::FirstStart,
            PreviousEnd::Shutdown,
            PreviousEnd::Error,
            PreviousEnd::Restore,
            PreviousEnd::Unreadable,
        ] {
            let mut input = inputs(end);
            input.free_available = Some(0);
            input.last_start.as_mut().unwrap().at_ms = NOW - 1000;
            assert!(!decide(&input).probation);
        }
    }

    /// A 137 after a long run on a disk with space, following a clean start: no.
    #[test]
    fn an_ordinary_roll_is_not_probation() {
        let d = decide(&inputs(unclean()));
        assert!(!d.probation, "{d:?}");
        assert_eq!(d.run_length, Some(Duration::from_secs(3600)));
        assert!(!decide(&inputs(PreviousEnd::StorageNotClosed)).probation);
    }

    /// An ENOSPC-sized free count corroborates `unclean`.
    #[test]
    fn low_free_space_corroborates() {
        let mut input = inputs(unclean());
        input.free_available = Some(8 << 20);
        let d = decide(&input);
        assert!(d.probation);
        assert_eq!(d.reasons, vec!["low free space"]);
        // The threshold is the lesser of 1 GiB and 5% of the filesystem.
        assert!(low_space(Some(GIB - 1), Some(1000 * GIB)));
        assert!(!low_space(Some(GIB), Some(1000 * GIB)));
        assert!(low_space(Some(99 << 20), Some(2 * GIB)), "5% of 2 GiB is about 102 MiB");
        assert!(!low_space(Some(103 << 20), Some(2 * GIB)));
        assert!(!low_space(None, Some(GIB)), "unknown is not low");
        assert_eq!(LOW_SPACE_CAP, GIB);
    }

    /// A previous run shorter than 10 minutes, measured from the record's start.
    #[test]
    fn a_short_previous_run_corroborates() {
        let mut input = inputs(unclean());
        input.last_start.as_mut().unwrap().at_ms = NOW - 60_000;
        assert!(decide(&input).probation);
        input.last_start.as_mut().unwrap().at_ms = NOW - SHORT_RUN.as_millis() as u64;
        assert!(!decide(&input).probation, "exactly ten minutes is not short");
    }

    /// An idle member: its store was last written long ago, and its run started
    /// recently. The run is as short as the record says, whatever the file's mtime,
    /// and the other way round: a recent write does not make a long run short.
    #[test]
    fn the_run_is_measured_from_the_record_not_from_the_last_write() {
        let mut input = inputs(unclean());
        input.last_start.as_mut().unwrap().at_ms = NOW - 60_000;
        input.last_write_secs_ago = Some(7 * 86_400);
        assert!(decide(&input).probation, "a recent start is short whatever was last written");
        let mut input = inputs(unclean());
        input.last_write_secs_ago = Some(2);
        assert!(!decide(&input).probation, "a long run with a recent write is not short");
    }

    /// A wall clock stepped back, or a record that is missing or unusable, never
    /// reads as short.
    #[test]
    fn an_unknown_run_length_is_never_short() {
        let mut input = inputs(unclean());
        input.last_start.as_mut().unwrap().at_ms = NOW + 5_000;
        let d = decide(&input);
        assert!(!d.probation && d.run_length.is_none());
        let mut input = inputs(unclean());
        input.last_start = None;
        assert!(!decide(&input).probation);
    }

    /// A second non-clean end in a row: the record's `inherited`, or the `before`
    /// an unclean verdict carries.
    #[test]
    fn a_second_non_clean_end_corroborates() {
        for name in ["unclean", "storage_not_closed", "storage_failed", "task_died"] {
            let mut input = inputs(unclean());
            input.last_start.as_mut().unwrap().inherited = name.into();
            assert!(decide(&input).probation, "inherited {name}");
        }
        for name in ["shutdown", "error", "restore", "first_start", "unreadable"] {
            let mut input = inputs(unclean());
            input.last_start.as_mut().unwrap().inherited = name.into();
            assert!(!decide(&input).probation, "inherited {name}");
        }
        let input = inputs(PreviousEnd::Unclean { before: Some("task_died".into()) });
        assert!(decide(&input).probation, "the before carried in the verdict");
        let input = inputs(PreviousEnd::Unclean { before: Some("shutdown".into()) });
        assert!(!decide(&input).probation);
    }

    /// A record written by another build is not used: after 0.44 -> 0.43 ->
    /// 0.44 the sidecar says 0.43 and the record says 0.44, and neither (b) nor (c)
    /// may fire from it.
    #[test]
    fn a_record_the_sidecar_does_not_vouch_for_is_not_used() {
        let mut input = inputs(unclean());
        input.last_start.as_mut().unwrap().inherited = "unclean".into();
        input.last_start.as_mut().unwrap().at_ms = NOW - 60_000;
        assert!(decide(&input).probation, "matching sidecar: both fire");
        input.sidecar_written_by = Some("0.43.0".into());
        let d = decide(&input);
        assert!(!d.probation && !d.last_start_used, "{d:?}");
        input.sidecar_written_by = None;
        assert!(!decide(&input).probation, "a missing sidecar vouches for nothing");
    }
}

// ---------------------------------------------------------------------------
// What the facts source advertises.
// ---------------------------------------------------------------------------

/// The facts source's staleness rule (A12): with no tick for two ticks every class
/// advertises `unknown` and `responsive` is `None`, the yield bits stay.
#[test]
fn a_wedged_evaluator_advertises_unknown_and_keeps_its_bits() {
    let rig = yielded_rig();
    let published = Published::default();
    published.store_start(&rig.last, 1_000);
    published.store(&rig.last, 6_000);
    let e = EVAL_TICK;
    let fresh = published.advertised(6_000 + 9_000, e);
    assert_eq!(fresh.class_state.webhooks, ClassState::Stalled);
    assert_eq!(fresh.responsive, Some(true));
    assert!(fresh.yielding.webhooks);
    let stale = published.advertised(6_000 + 10_001, e);
    assert_eq!(stale.class_state, PerClass::all(ClassState::Unknown));
    assert_eq!(stale.class_cause, PerClass::all(StallCause::Unknown));
    assert_eq!(stale.responsive, None);
    assert!(stale.yielding.webhooks, "silence is not evidence of recovery");
    // Before any tick at all.
    let never = Published::default();
    assert_eq!(never.advertised(0, e).responsive, None);
    // A published block carries it.
    let mut facts = Facts::default();
    published.apply_to(&mut facts, 6_000, e);
    assert_eq!(facts.class_state.unwrap().webhooks, ClassState::Stalled);
    assert_eq!(facts.class_cause.unwrap().webhooks, StallCause::Local);
    assert!(facts.yielding.webhooks);
}

/// The published counters follow the decisions: observations, transitions and the
/// version, which moves only when something advertised moved.
#[test]
fn the_published_counters_follow_the_decisions() {
    let mut rig = Rig::new();
    let published = Published::default();
    published.store_start(&rig.last, 0);
    let v0 = published.version();
    rig.responsive();
    rig.peer(1, peer_facts(ClassState::Ok, true));
    rig.owned(W, 1);
    rig.work(W);
    let d = rig.tick();
    published.store(&d, 5_000);
    assert_eq!(published.observations(W, Verdict::Good), 1);
    assert_eq!(published.ticks(), 1);
    assert_eq!(published.version(), v0 + 1, "the first tick turned responsive on... or Ok");
    let v1 = published.version();
    rig.work(W);
    published.store(&rig.tick(), 10_000);
    assert_eq!(published.version(), v1, "nothing advertised moved");
    rig.overdue(W, Phase::Waiting);
    for i in 0..8 {
        published.store(&rig.tick(), 15_000 + i * 5_000);
    }
    assert_eq!(published.transitions(W, Direction::Yield), 1);
    assert!(published.observations(W, Verdict::Bad) >= 6);
    assert!(published.version() > v1);
}

/// The constants are the ones the design states.
#[test]
fn the_constants_are_the_designs() {
    assert_eq!(EVAL_STEP, Duration::from_millis(250));
    assert_eq!(EVAL_TICK, Duration::from_secs(5));
    assert_eq!(RUNTIME_STALL, Duration::from_secs(1));
    assert_eq!(VOID_GAP, Duration::from_millis(500));
    assert_eq!(WEBHOOKS_WAITING, Duration::from_secs(30));
    assert_eq!(WEBHOOKS_REMOTE, Duration::from_secs(25));
    assert_eq!(EMBEDDINGS_WAITING, Duration::from_secs(30));
    assert_eq!(EMBEDDINGS_REMOTE, Duration::from_secs(75));
    assert_eq!(Bounds::of(T, Duration::from_secs(60), EVAL_TICK).waiting, Duration::from_secs(120));
    assert_eq!((n_of_m(W), n_of_m(E), n_of_m(T)), ((6, 12), (6, 12), (3, 5)));
    assert_eq!(window_of(W, Duration::from_secs(60), EVAL_TICK), Duration::from_secs(60));
    assert_eq!(window_of(E, Duration::from_secs(60), EVAL_TICK), Duration::from_secs(60));
    assert_eq!(window_of(T, Duration::from_secs(60), EVAL_TICK), Duration::from_secs(300));
    // At a scale of 5 the window follows the class's cycle, not the shrunken tick.
    let e = Timings::scaled(5).e;
    assert_eq!(e, Duration::from_secs(1));
    assert_eq!(window_of(W, Duration::from_secs(2), e), Duration::from_secs(24));
    assert_eq!(window_of(E, Duration::from_secs(2), e), Duration::from_secs(60));
    assert_eq!(window_of(T, Duration::from_secs(2), e), Duration::from_secs(10));
    assert_eq!(Bounds::of(T, Duration::from_secs(2), e).waiting, Duration::from_secs(14));
    assert_eq!(Timings::scaled(5).latch_hold, Duration::from_secs(360));
    assert_eq!((0..6).map(reclaim_ticks).collect::<Vec<_>>(), vec![24, 48, 96, 192, 384, 384]);
}
