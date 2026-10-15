use super::*;

fn node(n: u128) -> NodeId {
    NodeId::from_bytes(n.to_be_bytes())
}

fn key(peer: u128, collection: u64) -> Key {
    Key { peer: node(peer), collection: CollectionId(collection), kind: LedgerKind::Digest }
}

const MIN: Duration = Duration::from_secs(60);

fn mins(n: u64) -> Duration {
    MIN * n as u32
}

fn ledger() -> RepairLedger {
    // A 5-minute rotation: three periods are 15 minutes, so the 30-minute floor
    // is the deadline.
    RepairLedger::new(Params { rotation: mins(5) })
}

/// Plan, start and end a repair that ran to its end, at `now`.
fn run(l: &mut RepairLedger, k: Key, now: Duration, progress: bool) {
    assert_eq!(l.may_plan(k, now), MayPlan::Yes, "planned at {now:?}");
    l.started(k, now);
    l.ended(k, now, End::Finished { progress });
}

#[test]
fn a_new_pair_may_plan_and_a_pair_in_flight_or_awaiting_its_probe_may_not() {
    let mut l = ledger();
    let k = key(1, 1);
    assert_eq!(l.may_plan(k, mins(0)), MayPlan::Yes);
    l.started(k, mins(0));
    assert_eq!(l.may_plan(k, mins(1)), MayPlan::Busy, "in flight");
    l.ended(k, mins(2), End::Finished { progress: false });
    assert_eq!(l.may_plan(k, mins(3)), MayPlan::Busy, "awaiting its probe (R5)");
    // The pair next to it is its own.
    assert_eq!(l.may_plan(key(2, 1), mins(3)), MayPlan::Yes);
    assert_eq!(l.may_plan(key(1, 2), mins(3)), MayPlan::Yes);
    let mut other_kind = k;
    other_kind.kind = LedgerKind::Definition;
    assert_eq!(l.may_plan(other_kind, mins(3)), MayPlan::Yes);
}

#[test]
fn a_transient_end_changes_nothing_and_does_not_hold_planning() {
    let mut l = ledger();
    let k = key(1, 1);
    // Climb to level 2 first, so a transient end has something to leave alone.
    for step in 0..2u64 {
        let at = mins(100 * (step + 1));
        run(&mut l, k, at, false);
        l.judge(k, at, Judged::Ineffective);
    }
    let before = l.entries[&k];
    assert_eq!(before.level, 2);
    let at = before.not_before + mins(1);
    l.started(k, at);
    for t in [
        Transient::PeerError,
        Transient::Stopped,
        Transient::PurgePending,
        Transient::Abandoned,
        Transient::Dropped,
        Transient::WholeDatabaseTookOver,
        Transient::Gone,
    ] {
        l.ended(k, at, End::Transient(t));
        let after = l.entries[&k];
        assert_eq!(after.level, 2, "{t:?} leaves the level");
        assert_eq!(after.not_before, before.not_before, "{t:?} leaves the back-off");
        assert_eq!(after.unjudged_run, 0, "{t:?} neither counts nor resets");
        assert_eq!(l.may_plan(k, at), MayPlan::Yes, "{t:?} returns the phase to idle");
        l.started(k, at);
    }
}

#[test]
fn only_a_compared_probe_judges_and_a_match_is_effective() {
    let mut l = ledger();
    let k = key(1, 1);
    run(&mut l, k, mins(0), false);
    // Nothing but a judgement leaves the awaiting phase before the deadline.
    assert!(l.expire(mins(29)).is_empty());
    assert_eq!(l.may_plan(k, mins(29)), MayPlan::Busy);
    assert_eq!(l.judge(k, mins(10), Judged::Effective), Some(Verdict::Effective));
    assert_eq!(l.may_plan(k, mins(10)), MayPlan::Yes);
    assert_eq!(l.entries[&k].level, 0);
    // A second judgement of the same end is nothing.
    assert_eq!(l.judge(k, mins(11), Judged::Ineffective), None);
    assert_eq!(l.entries[&k].level, 0);
}

#[test]
fn ineffective_repairs_back_off_by_the_table_and_stop_at_the_cap() {
    let mut l = ledger();
    let k = key(1, 1);
    let mut now = mins(0);
    for (i, back) in BACKOFF.iter().enumerate() {
        run(&mut l, k, now, false);
        let v = l.judge(k, now, Judged::Ineffective).unwrap();
        assert_eq!(v, Verdict::Ineffective { level: i as u8 + 1, not_before: now + *back });
        assert_eq!(l.may_plan(k, now + *back - mins(1)), MayPlan::BackOff(now + *back));
        now += *back;
    }
    // At the cap the back-off stays at 6 h.
    run(&mut l, k, now, false);
    let v = l.judge(k, now, Judged::Ineffective).unwrap();
    assert_eq!(v, Verdict::Ineffective { level: MAX_LEVEL, not_before: now + BACKOFF[4] });
}

#[test]
fn an_effective_repair_resets_the_level_and_the_back_off() {
    let mut l = ledger();
    let k = key(1, 1);
    run(&mut l, k, mins(0), false);
    l.judge(k, mins(0), Judged::Ineffective);
    run(&mut l, k, mins(5), false);
    l.judge(k, mins(5), Judged::Ineffective);
    assert_eq!(l.entries[&k].level, 2);
    run(&mut l, k, mins(30), false);
    l.judge(k, mins(31), Judged::Effective);
    assert_eq!(l.entries[&k].level, 0);
    assert_eq!(l.may_plan(k, mins(31)), MayPlan::Yes);
}

#[test]
fn a_repair_that_made_progress_lowers_the_level_to_one() {
    let mut l = ledger();
    let k = key(1, 1);
    let mut now = mins(0);
    for _ in 0..4 {
        run(&mut l, k, now, false);
        l.judge(k, now, Judged::Ineffective);
        now = l.entries[&k].not_before;
    }
    assert_eq!(l.entries[&k].level, 4);
    run(&mut l, k, now, true);
    let v = l.judge(k, now, Judged::Ineffective).unwrap();
    assert_eq!(v, Verdict::Ineffective { level: 1, not_before: now + BACKOFF[0] });
}

#[test]
fn an_unjudged_repair_is_transient_and_every_second_one_in_a_row_climbs() {
    let mut l = ledger();
    let k = key(1, 1);
    let mut now = mins(0);
    // First unjudged: transient.
    run(&mut l, k, now, false);
    now += mins(31);
    assert_eq!(l.expire(now), vec![(k, Verdict::Transient)]);
    assert_eq!(l.entries[&k].level, 0);
    assert_eq!(l.may_plan(k, now), MayPlan::Yes);
    // Second in a row: one ineffective step, from the end of that repair.
    let ended = now;
    run(&mut l, k, now, false);
    now += mins(31);
    let out = l.expire(now);
    assert_eq!(
        out,
        vec![(k, Verdict::UnjudgedClimb { level: 1, not_before: (ended + BACKOFF[0]).max(now) })]
    );
    // Third: transient again, fourth: climbs again.
    let nb = l.entries[&k].not_before;
    now = nb;
    run(&mut l, k, now, false);
    now += mins(31);
    assert_eq!(l.expire(now), vec![(k, Verdict::Transient)]);
    run(&mut l, k, now, false);
    now += mins(31);
    assert!(matches!(l.expire(now)[0].1, Verdict::UnjudgedClimb { level: 2, .. }));
}

#[test]
fn a_judged_outcome_resets_the_unjudged_run_and_a_transient_end_does_not() {
    let mut l = ledger();
    let k = key(1, 1);
    run(&mut l, k, mins(0), false);
    l.expire(mins(31));
    assert_eq!(l.entries[&k].unjudged_run, 1);
    // A transient end between neither increments nor resets.
    l.started(k, mins(31));
    l.ended(k, mins(31), End::Transient(Transient::PeerError));
    assert_eq!(l.entries[&k].unjudged_run, 1);
    // The next unjudged one is the second: it climbs.
    run(&mut l, k, mins(32), false);
    assert!(matches!(l.expire(mins(70))[0].1, Verdict::UnjudgedClimb { .. }));
    // A judged outcome resets the count.
    let at = l.entries[&k].not_before;
    run(&mut l, k, at, false);
    l.judge(k, at, Judged::Effective);
    assert_eq!(l.entries[&k].unjudged_run, 0);
}

#[test]
fn an_unjudged_progress_repair_lowers_the_level_then_adds_the_unjudged_step() {
    let mut l = ledger();
    let k = key(1, 1);
    let mut now = mins(0);
    for _ in 0..3 {
        run(&mut l, k, now, false);
        l.judge(k, now, Judged::Ineffective);
        now = l.entries[&k].not_before;
    }
    assert_eq!(l.entries[&k].level, 3);
    // Two unjudged progress repairs: the first lowers to 1 and is transient,
    // the second lowers (already 1) and adds a step: level 2, not 4.
    run(&mut l, k, now, true);
    now += mins(31);
    assert_eq!(l.expire(now), vec![(k, Verdict::Transient)]);
    assert_eq!(l.entries[&k].level, 1);
    run(&mut l, k, now, true);
    now += mins(31);
    assert!(matches!(l.expire(now)[0].1, Verdict::UnjudgedClimb { level: 2, .. }));
}

#[test]
fn the_deadline_is_three_rotation_periods_or_thirty_minutes_whichever_is_longer() {
    let mut l = RepairLedger::new(Params { rotation: mins(20) });
    let k = key(1, 1);
    run(&mut l, k, mins(0), false);
    assert!(l.expire(mins(59)).is_empty(), "three periods are an hour");
    assert_eq!(l.expire(mins(60)).len(), 1);
    let mut l = ledger();
    run(&mut l, k, mins(0), false);
    assert!(l.expire(mins(29)).is_empty());
    assert_eq!(l.expire(mins(30)).len(), 1, "the 30-minute floor");
}

#[test]
fn a_clear_within_the_deadline_is_recorded_as_effective_and_after_it_is_nothing() {
    let mut l = ledger();
    let k = key(1, 1);
    run(&mut l, k, mins(0), false);
    assert_eq!(l.cleared(k, mins(20)), Some(Verdict::Effective));
    assert_eq!(l.may_plan(k, mins(20)), MayPlan::Yes);
    // It armed the pair: the next confirmation is an episode.
    assert!(l.confirmed(k, mins(25)).episode);
    // Past the deadline the repair is unjudged, and a clear is nothing.
    run(&mut l, k, mins(40), false);
    assert_eq!(l.cleared(k, mins(80)), None);
}

#[test]
fn a_recurrence_after_an_effective_repair_is_an_episode_and_a_standing_finding_is_not() {
    let mut l = ledger();
    let k = key(1, 1);
    // A standing finding confirmed every contact is no episode.
    for t in 0..5 {
        assert!(!l.confirmed(k, mins(t)).episode);
    }
    run(&mut l, k, mins(5), false);
    l.judge(k, mins(6), Judged::Effective);
    assert!(l.confirmed(k, mins(8)).episode, "clear and recur");
    assert!(!l.confirmed(k, mins(9)).episode, "once");
}

/// A partition that heals on five members: each peer's one legitimate effective
/// repair is one episode, not four.
#[test]
fn episodes_of_different_peers_for_one_collection_are_one() {
    let mut l = ledger();
    for peer in 1..=4u128 {
        let k = key(peer, 7);
        run(&mut l, k, mins(0), false);
        l.judge(k, mins(1), Judged::Effective);
    }
    let mut counted = 0;
    for peer in 1..=4u128 {
        if l.confirmed(key(peer, 7), mins(10)).episode {
            counted += 1;
        }
    }
    assert_eq!(counted, 1);
    assert!(!l.is_stopped(CollectionId(7), mins(10)));
}

fn recur(l: &mut RepairLedger, peer: u128, collection: u64, at: Duration) -> Confirmed {
    let k = key(peer, collection);
    // The repair must be plannable: it is judged effective and the finding
    // recurs a few minutes later.
    run(l, k, at, false);
    l.judge(k, at, Judged::Effective);
    l.confirmed(k, at + mins(1))
}

#[test]
fn more_than_three_episodes_in_an_hour_stop_the_collection_and_the_stop_expires() {
    let mut l = ledger();
    // One peer, episodes 16 minutes apart (past the 15-minute de-duplication).
    let mut stopped_at = None;
    for i in 0..4u64 {
        let c = recur(&mut l, 1, 3, mins(i * 16));
        assert!(c.episode, "episode {i}");
        if c.newly_stopped {
            stopped_at = Some(mins(i * 16 + 1));
        }
    }
    // 4 episodes in 49 minutes: the fourth tripped it.
    let stopped_at = stopped_at.expect("stopped on the fourth");
    let k = key(1, 3);
    assert!(matches!(l.may_plan(k, stopped_at + mins(1)), MayPlan::Stopped(_)));
    // Another collection is untouched.
    assert_eq!(l.may_plan(key(1, 4), stopped_at), MayPlan::Yes);
    // It expires after six hours even if findings keep being confirmed.
    let mut t = stopped_at;
    while t < stopped_at + mins(6 * 60 - 2) {
        t += mins(30);
        l.confirmed(k, t);
        if t < stopped_at + mins(6 * 60 - 1) {
            assert!(l.is_stopped(CollectionId(3), t), "still stopped at +{:?}", t - stopped_at);
        }
    }
    assert!(!l.is_stopped(CollectionId(3), stopped_at + STOP_FOR));
    assert_eq!(l.may_plan(k, stopped_at + STOP_FOR), MayPlan::Yes);
}

#[test]
fn a_stopped_collection_is_rearmed_by_an_hour_without_a_confirmed_finding() {
    let mut l = ledger();
    let mut stopped_at = None;
    for i in 0..4u64 {
        let c = recur(&mut l, 1, 3, mins(i * 16));
        if c.newly_stopped {
            stopped_at = Some(mins(i * 16 + 1));
        }
    }
    let stopped_at = stopped_at.unwrap();
    assert!(l.is_stopped(CollectionId(3), stopped_at + mins(59)));
    assert!(!l.is_stopped(CollectionId(3), stopped_at + mins(61)), "an hour clear re-arms");
    // Re-armed with its episodes forgotten, and a recurrence stops it again.
    let mut again = false;
    for i in 0..4u64 {
        again |= recur(&mut l, 1, 3, stopped_at + mins(120 + i * 16)).newly_stopped;
    }
    assert!(again, "stopped again, so the ERROR repeats");
}

#[test]
fn three_episodes_in_an_hour_do_not_stop_and_old_episodes_fall_out_of_the_window() {
    let mut l = ledger();
    for i in 0..3u64 {
        assert!(!recur(&mut l, 1, 3, mins(i * 16)).newly_stopped);
    }
    // A fourth an hour after the first: the first has left the window.
    assert!(!recur(&mut l, 1, 3, mins(61)).newly_stopped);
    assert!(!l.is_stopped(CollectionId(3), mins(62)));
}

#[test]
fn retain_drops_removed_peers_and_dropped_collections_and_their_repairs() {
    let mut l = ledger();
    let keep = key(1, 1);
    for k in [keep, key(2, 1), key(1, 2)] {
        run(&mut l, k, mins(0), false);
        l.judge(k, mins(0), Judged::Effective);
    }
    run(&mut l, key(3, 1), mins(0), false); // still in flight, then pruned
    let held: BTreeSet<_> = [CollectionId(1)].into();
    let members: BTreeSet<_> = [node(1), node(3)].into();
    l.retain(&held, Some(&members));
    assert_eq!(l.entries.keys().copied().collect::<Vec<_>>(), vec![keep, key(3, 1)]);
    assert_eq!(l.armed.len(), 1);
    assert!(l.collections.is_empty() || l.collections.keys().all(|c| held.contains(c)));
    // A dropped collection's stop goes with it.
    let members: BTreeSet<_> = [node(1)].into();
    l.retain(&BTreeSet::new(), Some(&members));
    assert!(l.is_empty());
}

#[test]
fn a_late_event_for_a_pair_that_never_started_invents_no_state() {
    let mut l = ledger();
    let k = key(1, 1);
    l.ended(k, mins(1), End::Finished { progress: true });
    assert_eq!(l.judge(k, mins(2), Judged::Ineffective), None);
    assert_eq!(l.cleared(k, mins(3)), None);
    assert!(l.expire(mins(99)).is_empty());
    assert!(l.is_empty());
}

/// The model test (§4): random event sequences against a reference that states
/// the invariants, not the implementation.
mod model {
    use super::*;
    use proptest::prelude::*;

    #[derive(Clone, Debug)]
    enum Ev {
        /// Time passes.
        Tick(u16),
        /// Try to plan and start a repair for pair `p`.
        Start(u8),
        /// The in-flight repair for pair `p` ends.
        End(u8, EndKind),
        /// A compared probe for pair `p`.
        Probe(u8, bool),
        /// A contact without a compared probe: nothing reaches the ledger.
        Contact,
        /// The finding for pair `p` is confirmed.
        Confirm(u8),
        /// The finding for pair `p` clears.
        Clear(u8),
        /// Peer `p` is removed, or collection `c` dropped (R7).
        Prune(u8),
        /// A restart: everything forgotten (R6).
        Restart,
    }

    #[derive(Clone, Copy, Debug)]
    enum EndKind {
        Transient,
        Plain,
        Progress,
    }

    fn ev() -> impl Strategy<Value = Ev> {
        prop_oneof![
            4 => (1u16..400).prop_map(Ev::Tick),
            5 => (0u8..6).prop_map(Ev::Start),
            5 => (0u8..6, prop_oneof![Just(EndKind::Transient), Just(EndKind::Plain), Just(EndKind::Progress)])
                .prop_map(|(p, e)| Ev::End(p, e)),
            4 => (0u8..6, any::<bool>()).prop_map(|(p, m)| Ev::Probe(p, m)),
            2 => Just(Ev::Contact),
            3 => (0u8..6).prop_map(Ev::Confirm),
            2 => (0u8..6).prop_map(Ev::Clear),
            1 => (0u8..6).prop_map(Ev::Prune),
            1 => Just(Ev::Restart),
        ]
    }

    /// Pairs: 3 peers x 2 collections.
    fn pair(p: u8) -> Key {
        key(u128::from(p % 3) + 1, u64::from(p / 3) + 1)
    }

    /// The reference: the per-key state the rules imply, written as a replay of
    /// the key's own history instead of the ledger's fields.
    #[derive(Default, Clone)]
    struct Hist {
        /// `Some(true)` effective, `Some(false)` ineffective, per judged repair,
        /// with the progress flag; unjudged ends are `None`.
        outcomes: Vec<Outcome>,
        in_flight: bool,
        awaiting: Option<(Duration, Duration, bool)>,
        starts: Vec<Duration>,
    }

    #[derive(Clone, Copy)]
    enum Outcome {
        Effective,
        Ineffective { progress: bool, ended: Duration },
        Unjudged { progress: bool, ended: Duration, deadline: Duration },
    }

    impl Hist {
        /// The level and back-off the history implies.
        fn state(&self, now: Duration) -> (u8, Duration) {
            let mut level = 0u8;
            let mut not_before = Duration::ZERO;
            let mut run = 0u32;
            for o in &self.outcomes {
                match *o {
                    Outcome::Effective => {
                        level = 0;
                        not_before = Duration::ZERO;
                        run = 0;
                    }
                    Outcome::Ineffective { progress, ended } => {
                        level = if progress { 1 } else { (level + 1).min(5) };
                        not_before = ended + BACKOFF[usize::from(level) - 1];
                        run = 0;
                    }
                    Outcome::Unjudged { progress, ended, deadline } => {
                        run += 1;
                        if progress {
                            level = level.min(1);
                        }
                        if run % 2 == 0 {
                            level = (level + 1).min(5);
                            not_before = (ended + BACKOFF[usize::from(level) - 1]).max(deadline);
                        }
                    }
                }
            }
            let _ = now;
            (level, not_before)
        }
    }

    fn run_model(events: Vec<Ev>) {
        let params = Params { rotation: mins(5) };
        let mut l = RepairLedger::new(params);
        let mut hist: BTreeMap<Key, Hist> = BTreeMap::new();
        let mut now = mins(1);
        let mut last_end: BTreeMap<Key, Duration> = BTreeMap::new();
        for e in events {
            match e {
                Ev::Tick(m) => {
                    now += mins(u64::from(m));
                    // The driver expires on every tick.
                    let expired = l.expire(now);
                    for (k, _) in expired {
                        let h = hist.get_mut(&k).expect("expired a key it never saw");
                        let (ended, deadline, progress) =
                            h.awaiting.take().expect("expired a repair that was not awaiting");
                        assert!(now >= deadline, "expired before its deadline");
                        h.outcomes.push(Outcome::Unjudged { progress, ended, deadline: now });
                    }
                }
                Ev::Start(p) => {
                    let k = pair(p);
                    let h = hist.entry(k).or_default();
                    // The planner's cooldown: not within 5 minutes of the last end.
                    let cooled = last_end.get(&k).is_none_or(|t| now >= *t + mins(5));
                    let may = l.may_plan(k, now);
                    // The reference says when planning is allowed.
                    let (_, not_before) = h.state(now);
                    let stopped = l.is_stopped(k.collection, now);
                    let expected_yes =
                        !h.in_flight && h.awaiting.is_none() && now >= not_before && !stopped;
                    assert_eq!(may == MayPlan::Yes, expected_yes, "may_plan at {now:?}: {may:?}");
                    if may == MayPlan::Yes && cooled {
                        l.started(k, now);
                        h.in_flight = true;
                        h.starts.push(now);
                    }
                }
                Ev::End(p, kind) => {
                    let k = pair(p);
                    let Some(h) = hist.get_mut(&k) else { continue };
                    if !h.in_flight {
                        // A late end: invents nothing.
                        l.ended(k, now, End::Finished { progress: true });
                        continue;
                    }
                    h.in_flight = false;
                    last_end.insert(k, now);
                    match kind {
                        EndKind::Transient => {
                            l.ended(k, now, End::Transient(Transient::PeerError));
                        }
                        EndKind::Plain | EndKind::Progress => {
                            let progress = matches!(kind, EndKind::Progress);
                            l.ended(k, now, End::Finished { progress });
                            h.awaiting = Some((now, now + mins(30), progress));
                        }
                    }
                }
                Ev::Probe(p, matched) => {
                    let k = pair(p);
                    let judged = if matched { Judged::Effective } else { Judged::Ineffective };
                    let got = l.judge(k, now, judged);
                    let h = hist.entry(k).or_default();
                    match h.awaiting.take() {
                        Some((ended, _, progress)) => {
                            assert!(got.is_some(), "an awaiting repair was not judged");
                            h.outcomes.push(if matched {
                                Outcome::Effective
                            } else {
                                Outcome::Ineffective { progress, ended }
                            });
                        }
                        None => assert!(got.is_none(), "a judgement with nothing awaiting"),
                    }
                }
                Ev::Contact => {}
                Ev::Confirm(p) => {
                    l.confirmed(pair(p), now);
                }
                Ev::Clear(p) => {
                    let k = pair(p);
                    let got = l.cleared(k, now);
                    let h = hist.entry(k).or_default();
                    if let Some((_, deadline, _)) = h.awaiting
                        && now <= deadline
                    {
                        assert!(got.is_some());
                        h.awaiting = None;
                        h.outcomes.push(Outcome::Effective);
                    } else {
                        assert!(got.is_none());
                    }
                }
                Ev::Prune(p) => {
                    let gone_peer = u128::from(p % 3) + 1;
                    let held: BTreeSet<_> = (1..=2).map(CollectionId).collect();
                    let members: BTreeSet<_> =
                        (1..=3u128).filter(|n| *n != gone_peer).map(node).collect();
                    l.retain(&held, Some(&members));
                    hist.retain(|k, _| k.peer != node(gone_peer));
                    last_end.retain(|k, _| k.peer != node(gone_peer));
                    // No entry survives its peer.
                    assert!(l.entries.keys().all(|k| k.peer != node(gone_peer)));
                }
                Ev::Restart => {
                    l = RepairLedger::new(params);
                    hist.clear();
                    last_end.clear();
                }
            }
            // Invariants after every event.
            for (k, h) in &hist {
                let (level, not_before) = h.state(now);
                if let Some(entry) = l.entries.get(k) {
                    assert_eq!(entry.level, level, "level of {k:?} at {now:?}");
                    assert!(entry.level <= MAX_LEVEL);
                    assert_eq!(entry.not_before, not_before, "back-off of {k:?}");
                    let in_flight = matches!(entry.phase, Phase::InFlight);
                    assert_eq!(in_flight, h.in_flight, "in flight");
                    assert_eq!(
                        matches!(entry.phase, Phase::Awaiting { .. }),
                        h.awaiting.is_some(),
                        "awaiting"
                    );
                } else {
                    assert!(h.outcomes.is_empty() && !h.in_flight && h.awaiting.is_none());
                }
                // Repairs per hour are bounded under any event order: the cooldown
                // (5 minutes) is the floor between two starts of a pair.
                let recent = h.starts.iter().filter(|t| now.saturating_sub(**t) < mins(60)).count();
                assert!(recent <= 12, "{recent} starts of {k:?} in an hour");
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 2000, ..ProptestConfig::default() })]
        #[test]
        fn the_ledger_agrees_with_a_reference_that_replays_each_pairs_history(
            events in proptest::collection::vec(ev(), 1..220)
        ) {
            run_model(events);
        }
    }
}
