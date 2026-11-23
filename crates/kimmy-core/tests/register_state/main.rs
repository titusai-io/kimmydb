//! The register of a logical name as a pure function (ADR-272, decisions 3, 5, 7, 8, 9): `kimmy_core::register_state`.
//!
//! Four kinds of test. The rules one by one. The model's answers for generated scenarios (`table.rs`, from
//! `swapmodel6g.py`, its sha in the table's header), so that the product's function and the model cannot drift. Every order of
//! arrival of a few events, checking that the visible event, the burials and the late writes only move forward and end the
//! same whatever the order (decision 5's monotone claim). And the two named invariants of decision 5, each its own assertion:
//! the old physical is never buried unless the new one is alive, and a binding whose physical is buried is never visible.

mod table;

use kimmy_core::register_state::{
    Birth, Burial, MAX_OUT_DEADLINE_MS, OUT_DEADLINE_SKEW_MS, Put, Ready, Refusal, Timeline,
    Visible, covers_in_position, valid_binding,
};
use kimmy_core::{Epoch, Hlc, NodeId, RegisterEvent, StageInfo, Stamp, VersionVector};

use table::{CASES, Ready as TReady, Vis};

fn st(hlc: u64, origin: usize) -> Stamp {
    Stamp::new(Hlc::new(hlc, 0), NodeId::from_bytes([origin as u8; 16]), Epoch::ZERO)
}

fn bind(physical: &str) -> RegisterEvent {
    RegisterEvent::Bind { physical: physical.into() }
}

fn create(physical: &str) -> RegisterEvent {
    RegisterEvent::Create { physical: physical.into() }
}

/// A readiness lookup from a list; a physical not listed is buried (never held here).
fn ready<'a>(held: &'a [(&'a str, Ready)]) -> impl Fn(&str) -> Ready + 'a {
    move |p| held.iter().find(|(n, _)| *n == p).map_or(Ready::Buried, |(_, r)| *r)
}

/// The creation every test stage was made at, and a deadline far above any stamp the tests bind at.
fn a_stage() -> (Stamp, StageInfo) {
    (st(1, 0), StageInfo { deadline: Hlc::new(1_000_000, 0) })
}

/// A timeline of events that are all valid: each binding has a stage with a deadline above it.
fn timeline(events: &[(u64, usize, RegisterEvent)]) -> Timeline {
    let mut t = Timeline::new("orders");
    let (creation, info) = a_stage();
    for (hlc, origin, event) in events {
        let put = t.insert(st(*hlc, *origin), event.clone(), Some((&creation, &info)));
        assert!(matches!(put, Put::Stored | Put::Duplicate), "{put:?}");
    }
    t
}

// ---------------------------------------------------------------------------------------------------------------------
// the stage's deadline (decision 9, as ruled): clamps, never a refusal

#[test]
fn a_deadline_below_the_creation_reads_as_the_creation() {
    let creation = st(1_000, 0);
    for deadline in [0, 1, 999] {
        let stage = StageInfo { deadline: Hlc::new(deadline, 0) };
        assert_eq!(stage.effective_deadline(&creation), creation.hlc, "deadline {deadline}");
    }
    // exactly the creation's hlc, and one past it, are what they say
    let at = StageInfo { deadline: Hlc::new(1_000, 0) };
    assert_eq!(at.effective_deadline(&creation), Hlc::new(1_000, 0));
    let above = StageInfo { deadline: Hlc::new(1_001, 0) };
    assert_eq!(above.effective_deadline(&creation), Hlc::new(1_001, 0));
}

#[test]
fn a_deadline_past_the_cap_reads_as_the_cap() {
    let creation = st(1_000, 0);
    let cap = Hlc::new(1_000 + MAX_OUT_DEADLINE_MS + OUT_DEADLINE_SKEW_MS, 0);
    let at = StageInfo { deadline: cap };
    assert_eq!(at.effective_deadline(&creation), cap, "exactly the cap is taken as it is");
    let past = StageInfo { deadline: Hlc::new(cap.wall_ms + 1, 0) };
    assert_eq!(past.effective_deadline(&creation), cap, "one millisecond past it reads as the cap");
    let far = StageInfo { deadline: Hlc::new(u64::MAX >> 20, 0) };
    assert_eq!(
        far.effective_deadline(&creation),
        cap,
        "a deadline of the year 2500 reads as the cap"
    );
    // a counter above the cap's millisecond is past the cap too
    let counted = StageInfo { deadline: Hlc::new(cap.wall_ms, 1) };
    assert_eq!(counted.effective_deadline(&creation), cap);
}

#[test]
fn a_binding_is_valid_at_its_stages_effective_deadline_and_not_a_tick_above_it() {
    let creation = st(1_000, 0);
    // an ordinary deadline
    let stage = StageInfo { deadline: Hlc::new(1_030, 0) };
    assert!(valid_binding(&st(1_030, 1), &creation, &stage), "at the deadline");
    assert!(!valid_binding(&st(1_031, 1), &creation, &stage), "one past it");
    assert!(valid_binding(&st(1_001, 1), &creation, &stage), "well inside");
    // a deadline below the creation: only a binding at the creation's hlc is valid
    let backwards = StageInfo { deadline: Hlc::new(10, 0) };
    assert!(valid_binding(&st(1_000, 1), &creation, &backwards), "at the creation's hlc");
    assert!(!valid_binding(&st(1_001, 1), &creation, &backwards), "a tick above it");
    // a far deadline: valid up to the cap, not past it
    let far = StageInfo { deadline: Hlc::new(u64::MAX >> 20, 0) };
    let cap = 1_000 + MAX_OUT_DEADLINE_MS + OUT_DEADLINE_SKEW_MS;
    assert!(valid_binding(&st(cap, 1), &creation, &far), "at the cap");
    assert!(!valid_binding(&st(cap + 1, 1), &creation, &far), "one past the cap");
}

// ---------------------------------------------------------------------------------------------------------------------
// coverage (decision 3): the witnessed vector is a per-origin maximum

#[test]
fn an_entry_is_covered_when_its_origin_is_witnessed_up_to_it() {
    let mut witnessed = VersionVector::new();
    let entry = st(100, 1);
    assert!(!covers_in_position(&witnessed, &entry), "nothing witnessed");
    witnessed.observe(st(99, 1));
    assert!(!covers_in_position(&witnessed, &entry), "one below");
    witnessed.observe(st(100, 1));
    assert!(covers_in_position(&witnessed, &entry), "exactly up to it");
    witnessed.observe(st(150, 1));
    assert!(covers_in_position(&witnessed, &entry), "past it");
    assert!(
        !covers_in_position(&witnessed, &st(100, 2)),
        "another origin's witness does not cover it"
    );
    let other_epoch = Stamp::new(Hlc::new(100, 0), NodeId::from_bytes([1; 16]), Epoch::from_u64(7));
    assert!(
        !covers_in_position(&witnessed, &other_epoch),
        "nor does another epoch of the same node"
    );
}

// ---------------------------------------------------------------------------------------------------------------------
// the timeline

#[test]
fn a_timeline_is_a_set_of_events_ordered_by_stamp() {
    let mut t = Timeline::new("orders");
    let (creation, info) = a_stage();
    let stage = Some((&creation, &info));
    assert_eq!(t.insert(st(30, 0), bind("a"), stage), Put::Stored);
    assert_eq!(t.insert(st(10, 0), create("b"), None), Put::Stored);
    assert_eq!(t.insert(st(20, 1), RegisterEvent::LogicalDrop, None), Put::Stored);
    assert_eq!(
        t.insert(st(20, 1), RegisterEvent::LogicalDrop, None),
        Put::Duplicate,
        "the same event again changes nothing"
    );
    let stamps: Vec<Stamp> = t.events().iter().map(|(s, _)| *s).collect();
    assert_eq!(stamps, [st(10, 0), st(20, 1), st(30, 0)]);
    // two origins at one hlc order by origin
    assert_eq!(t.insert(st(20, 0), bind("c"), stage), Put::Stored);
    let stamps: Vec<Stamp> = t.events().iter().map(|(s, _)| *s).collect();
    assert_eq!(stamps, [st(10, 0), st(20, 0), st(20, 1), st(30, 0)]);
}

#[test]
fn a_physical_is_born_with_the_earliest_event_that_names_it() {
    let t =
        timeline(&[(40, 0, bind("a")), (10, 0, create("a")), (20, 0, RegisterEvent::LogicalDrop)]);
    assert_eq!(t.birth("a"), Birth::At(st(10, 0)), "the earliest event naming it");
    assert_eq!(
        t.birth("orders"),
        Birth::Identity,
        "the name itself is the identity, bound at minus infinity"
    );
    assert_eq!(t.birth("nobody"), Birth::Unknown);
}

#[test]
fn with_no_event_the_identity_is_served_and_nothing_is_buried() {
    let t = Timeline::new("orders");
    let held = [("orders", Ready::Alive)];
    assert_eq!(t.visible(ready(&held)), Visible::Identity);
    assert!(t.burials(&Visible::Identity, ready(&held)).is_empty());
    assert_eq!(t.superseded_at("orders"), None);
    assert!(!t.is_late("orders", &st(1_000, 0)));
}

#[test]
fn the_newest_ready_event_is_visible() {
    let t = timeline(&[(10, 0, bind("a")), (20, 0, bind("b")), (30, 1, create("c"))]);
    let all =
        [("orders", Ready::Alive), ("a", Ready::Alive), ("b", Ready::Alive), ("c", Ready::Alive)];
    assert_eq!(t.visible(ready(&all)), Visible::Created { physical: "c".into(), stamp: st(30, 1) });
    let no_c = [("orders", Ready::Alive), ("a", Ready::Alive), ("b", Ready::Alive)];
    assert_eq!(
        t.visible(ready(&no_c)),
        Visible::Bound { physical: "b".into(), stamp: st(20, 0) },
        "an unheld physical is skipped"
    );
    let missing_c =
        [("orders", Ready::Alive), ("a", Ready::Alive), ("b", Ready::Alive), ("c", Ready::Missing)];
    assert_eq!(
        t.visible(ready(&missing_c)),
        Visible::Bound { physical: "b".into(), stamp: st(20, 0) },
        "an incomplete physical is skipped"
    );
    let only_a =
        [("orders", Ready::Alive), ("a", Ready::Alive), ("b", Ready::Buried), ("c", Ready::Buried)];
    assert_eq!(
        t.visible(ready(&only_a)),
        Visible::Bound { physical: "a".into(), stamp: st(10, 0) },
        "buried ones are skipped"
    );
    let none = [("orders", Ready::Alive)];
    assert_eq!(
        t.visible(ready(&none)),
        Visible::Identity,
        "nothing ready: the identity keeps serving"
    );
}

#[test]
fn a_logical_drop_is_always_applicable_and_a_later_binding_overrides_it() {
    let t = timeline(&[(10, 0, bind("a")), (20, 0, RegisterEvent::LogicalDrop)]);
    let held = [("orders", Ready::Alive), ("a", Ready::Alive)];
    assert_eq!(t.visible(ready(&held)), Visible::Dropped { stamp: st(20, 0) });
    assert_eq!(
        Visible::Dropped { stamp: st(20, 0) }.physical("orders"),
        None,
        "the name is absent"
    );
    let later =
        timeline(&[(10, 0, bind("a")), (20, 0, RegisterEvent::LogicalDrop), (25, 0, create("r"))]);
    let held = [("orders", Ready::Alive), ("a", Ready::Alive), ("r", Ready::Alive)];
    assert_eq!(
        later.visible(ready(&held)),
        Visible::Created { physical: "r".into(), stamp: st(25, 0) }
    );
}

#[test]
fn the_visible_event_buries_the_physicals_it_leaves_behind_and_only_those_held_and_named() {
    let t = timeline(&[(10, 0, bind("a")), (20, 0, bind("b"))]);
    let held = [
        ("orders", Ready::Alive),
        ("a", Ready::Alive),
        ("b", Ready::Alive),
        ("stranger", Ready::Alive),
    ];
    let visible = t.visible(ready(&held));
    assert_eq!(visible, Visible::Bound { physical: "b".into(), stamp: st(20, 0) });
    let mut buried = t.burials(&visible, ready(&held));
    buried.sort_by(|x, y| x.physical.cmp(&y.physical));
    assert_eq!(
        buried,
        [
            Burial { physical: "a".into(), at: st(20, 0) },
            Burial { physical: "orders".into(), at: st(20, 0) }
        ],
        "the identity and the older stage; not the served one, and not a physical no event names"
    );
    // a physical already buried is not buried again, and a missing one still is
    let held = [("orders", Ready::Buried), ("a", Ready::Missing), ("b", Ready::Alive)];
    let buried = t.burials(&visible, ready(&held));
    assert_eq!(buried, [Burial { physical: "a".into(), at: st(20, 0) }]);
}

#[test]
fn the_served_physical_is_never_buried_even_when_two_events_name_it() {
    // a stage bound twice (a writer that binds again): the physical is born at its earliest event, and the newer binding of it
    // is the visible one; the physical being served, it is not buried by the event that serves it
    let t = timeline(&[(10, 0, bind("a")), (20, 0, bind("a"))]);
    let held = [("orders", Ready::Alive), ("a", Ready::Alive)];
    let visible = t.visible(ready(&held));
    assert_eq!(visible, Visible::Bound { physical: "a".into(), stamp: st(20, 0) });
    assert_eq!(
        t.burials(&visible, ready(&held)),
        [Burial { physical: "orders".into(), at: st(20, 0) }],
        "the identity is buried, the served physical is not"
    );
}

#[test]
fn a_logical_drop_buries_the_identity_and_every_older_physical() {
    let t = timeline(&[(10, 0, bind("a")), (20, 0, RegisterEvent::LogicalDrop)]);
    let held = [("orders", Ready::Alive), ("a", Ready::Alive)];
    let visible = t.visible(ready(&held));
    let mut buried = t.burials(&visible, ready(&held));
    buried.sort_by(|x, y| x.physical.cmp(&y.physical));
    assert_eq!(
        buried,
        [
            Burial { physical: "a".into(), at: st(20, 0) },
            Burial { physical: "orders".into(), at: st(20, 0) }
        ]
    );
}

// ---------------------------------------------------------------------------------------------------------------------
// burial by a later event (decision 5, cause `doomed`): a stage with no binding in effect whose deadline lies strictly below the
// visible event

/// Bindings `a` at 10 and `b` at 20 are in effect; the stages `s` (deadline 15) and `t` (deadline 20) have no binding.
fn doom_case() -> (Timeline, Visible) {
    let t = timeline(&[(10, 0, bind("a")), (20, 0, bind("b"))]);
    let held = [("orders", Ready::Alive), ("a", Ready::Alive), ("b", Ready::Alive)];
    let visible = t.visible(ready(&held));
    (t, visible)
}

#[test]
fn a_stage_with_no_binding_whose_deadline_is_strictly_below_the_visible_event_is_doomed() {
    let (t, visible) = doom_case();
    let held = [("b", Ready::Alive), ("s", Ready::Alive), ("t", Ready::Alive)];
    let stages = [("s", Hlc::new(15, 0)), ("t", Hlc::new(20, 0))];
    let doomed = t.doomed(&visible, ready(&held), &stages);
    assert_eq!(
        doomed,
        [Burial { physical: "s".into(), at: st(20, 0) }],
        "s (deadline 15) is doomed by the event at 20, stamped by it; t (deadline 20) is not: a binding at the deadline is valid"
    );
}

#[test]
fn a_stage_that_has_a_binding_in_effect_is_buried_by_the_loser_rule_and_never_doomed() {
    let (t, visible) = doom_case();
    let held = [("orders", Ready::Alive), ("a", Ready::Alive), ("b", Ready::Alive)];
    // `a` is a stage too, with a deadline far below the visible event: its binding is in effect, so decision 5's loser rule buries it
    let stages = [("a", Hlc::new(12, 0)), ("b", Hlc::new(12, 0))];
    assert!(
        t.doomed(&visible, ready(&held), &stages).is_empty(),
        "D4 before DOOM: neither a bound stage nor the served one"
    );
    let buried = t.burials(&visible, ready(&held));
    assert!(buried.iter().any(|b| b.physical == "a"), "the loser rule buries it: {buried:?}");
}

#[test]
fn a_doomed_stage_is_one_held_here_alive_or_missing_and_never_the_served_physical() {
    let (t, visible) = doom_case();
    let stages = [("s", Hlc::new(15, 0)), ("m", Hlc::new(15, 0)), ("gone", Hlc::new(15, 0))];
    let held = [("b", Ready::Alive), ("s", Ready::Alive), ("m", Ready::Missing)];
    let mut doomed = t.doomed(&visible, ready(&held), &stages);
    doomed.sort_by(|x, y| x.physical.cmp(&y.physical));
    assert_eq!(
        doomed,
        [
            Burial { physical: "m".into(), at: st(20, 0) },
            Burial { physical: "s".into(), at: st(20, 0) }
        ],
        "a missing stage is buried too; one not held here (buried or never held) is not buried again"
    );
    // the served physical is never doomed, whatever its deadline
    let served = [("b", Hlc::new(1, 0))];
    assert!(t.doomed(&visible, ready(&held), &served).is_empty());
}

#[test]
fn nothing_is_doomed_while_the_identity_is_served() {
    let t = timeline(&[]);
    let held = [("orders", Ready::Alive), ("s", Ready::Alive)];
    let visible = t.visible(ready(&held));
    assert_eq!(visible, Visible::Identity);
    assert!(t.doomed(&visible, ready(&held), &[("s", Hlc::new(1, 0))]).is_empty());
}

#[test]
fn a_logical_drop_and_a_recreation_doom_a_stage_below_them_as_a_binding_does() {
    for event in [RegisterEvent::LogicalDrop, create("r")] {
        let t = timeline(&[(20, 0, event.clone())]);
        let held = [("orders", Ready::Alive), ("r", Ready::Alive), ("s", Ready::Alive)];
        let visible = t.visible(ready(&held));
        let doomed = t.doomed(&visible, ready(&held), &[("s", Hlc::new(19, 0))]);
        assert_eq!(doomed, [Burial { physical: "s".into(), at: st(20, 0) }], "{event:?}");
    }
}

#[test]
fn a_stage_born_after_the_visible_event_is_not_buried() {
    // b was bound at 30; a drop at 20 is visible only if b is not ready: b stays (born after the drop)
    let t = timeline(&[(20, 0, RegisterEvent::LogicalDrop), (30, 0, bind("b"))]);
    let held = [("orders", Ready::Alive), ("b", Ready::Missing)];
    let visible = t.visible(ready(&held));
    assert_eq!(visible, Visible::Dropped { stamp: st(20, 0) });
    let buried = t.burials(&visible, ready(&held));
    assert_eq!(
        buried,
        [Burial { physical: "orders".into(), at: st(20, 0) }],
        "b was born at 30, above the drop"
    );
}

#[test]
fn a_write_is_late_when_an_event_supersedes_its_physical_between_the_birth_and_the_write() {
    let t = timeline(&[(10, 0, bind("a")), (20, 0, bind("b"))]);
    assert_eq!(
        t.superseded_at("a"),
        Some(st(20, 0)),
        "b's binding names another physical, above a's birth"
    );
    assert_eq!(
        t.superseded_at("orders"),
        Some(st(10, 0)),
        "the identity is superseded by the first event"
    );
    assert_eq!(t.superseded_at("b"), None, "nothing names another physical after b");
    assert!(
        !t.is_late("a", &st(20, 0)),
        "a write at the superseding stamp is not late: the event is not below it"
    );
    assert!(t.is_late("a", &st(21, 0)));
    assert!(!t.is_late("a", &st(15, 0)), "a write before the event is the old physical's own");
    assert!(!t.is_late("b", &st(1_000, 0)));
    assert_eq!(t.superseded_at("nobody"), None, "no birth known: nothing to be late against");
}

#[test]
fn a_logical_drop_supersedes_the_physical_it_names_and_every_older_one() {
    let t = timeline(&[(10, 0, bind("a")), (20, 1, RegisterEvent::LogicalDrop)]);
    assert_eq!(t.superseded_at("a"), Some(st(20, 1)));
    assert!(t.is_late("a", &st(21, 0)));
    let after = timeline(&[(20, 1, RegisterEvent::LogicalDrop), (30, 0, create("r"))]);
    assert_eq!(
        after.superseded_at("r"),
        None,
        "a recreation born after the drop is not superseded by it"
    );
}

#[test]
fn a_member_refuses_writes_while_an_event_supersedes_what_it_serves_known_or_parked() {
    let known = timeline(&[(10, 0, bind("a"))]);
    let parked = Timeline::new("orders");
    assert_eq!(known.refusal(&parked, "orders"), Some(Refusal { event: st(10, 0), parked: false }));
    assert_eq!(
        known.refusal(&parked, "a"),
        None,
        "serving the bound physical: nothing supersedes it"
    );
    // an event ahead of its position is known and refuses too, once none is in effect
    let mut ahead = Timeline::new("orders");
    let (creation, info) = a_stage();
    ahead.insert(st(40, 2), bind("b"), Some((&creation, &info)));
    let none = Timeline::new("orders");
    assert_eq!(none.refusal(&ahead, "orders"), Some(Refusal { event: st(40, 2), parked: true }));
    // an event in effect is named before a parked one
    assert_eq!(known.refusal(&ahead, "orders"), Some(Refusal { event: st(10, 0), parked: false }));
    assert_eq!(none.refusal(&none, "orders"), None);
}

/// The stage of the tests below: created at 100 with a deadline of 130.
fn a_short_stage() -> (Stamp, StageInfo) {
    (st(100, 0), StageInfo { deadline: Hlc::new(130, 0) })
}

#[test]
fn an_invalid_binding_is_never_stored_never_visible_and_buries_nothing() {
    let (creation, info) = a_short_stage();
    let stage = Some((&creation, &info));
    let mut t = Timeline::new("orders");
    assert_eq!(
        t.insert(st(131, 1), bind("s"), stage),
        Put::InvalidBinding,
        "one past the deadline"
    );
    assert_eq!(t.insert(st(900, 1), bind("s"), stage), Put::InvalidBinding, "far past it");
    assert!(t.events().is_empty(), "nothing was stored");
    assert_eq!(
        t.birth("s"),
        Birth::Unknown,
        "the invalid binding does not even give the stage a birth"
    );
    let held = [("orders", Ready::Alive), ("s", Ready::Alive)];
    assert_eq!(
        t.visible(ready(&held)),
        Visible::Identity,
        "an invalid binding is never visible, whatever the stage's readiness"
    );
    assert!(t.burials(&t.visible(ready(&held)), ready(&held)).is_empty(), "and buries nothing");
    assert_eq!(
        t.superseded_at("orders"),
        None,
        "nor supersedes the identity: no write is late for it"
    );
    // at the deadline it is valid, and it is then visible
    assert_eq!(t.insert(st(130, 1), bind("s"), stage), Put::Stored, "at the deadline");
    assert_eq!(t.visible(ready(&held)), Visible::Bound { physical: "s".into(), stamp: st(130, 1) });
    // a deadline above the creation's cap is clamped, and a binding past the clamp is invalid too
    let far = StageInfo { deadline: Hlc::new(u64::MAX >> 20, 0) };
    let cap = 100 + MAX_OUT_DEADLINE_MS + OUT_DEADLINE_SKEW_MS;
    let mut u = Timeline::new("orders");
    assert_eq!(u.insert(st(cap + 1, 1), bind("s"), Some((&creation, &far))), Put::InvalidBinding);
    assert_eq!(u.insert(st(cap, 1), bind("s"), Some((&creation, &far))), Put::Stored);
}

#[test]
fn a_binding_of_a_stage_whose_creation_is_unknown_is_refused_and_not_stored() {
    let mut t = Timeline::new("orders");
    assert_eq!(t.insert(st(50, 0), bind("s"), None), Put::UnknownStage);
    assert!(t.events().is_empty());
    // a recreation and a logical drop need no stage
    assert_eq!(t.insert(st(60, 0), create("r"), None), Put::Stored);
    assert_eq!(t.insert(st(70, 0), RegisterEvent::LogicalDrop, None), Put::Stored);
}

// ---------------------------------------------------------------------------------------------------------------------
// the model's answers

fn ready_of(r: TReady) -> Ready {
    match r {
        TReady::Alive => Ready::Alive,
        TReady::Missing => Ready::Missing,
        TReady::Buried => Ready::Buried,
    }
}

/// The timeline of a table case: the events in effect, a binding only if valid at its stage's deadline (the creation's own
/// stamp is the stage's first event; the table gives its hlc and the stage's deadline).
fn case_timeline(case: &table::Case) -> Timeline {
    case_timeline_counting(case).0
}

/// The same, and how many bindings the timeline refused as invalid.
fn case_timeline_counting(case: &table::Case) -> (Timeline, usize) {
    let mut t = Timeline::new("L");
    let mut invalid = 0;
    for &(kind, origin, hlc, physical) in case.events {
        let stamp = st(hlc, origin);
        let put = match kind {
            "bind" => {
                let &(_, creation, deadline) = case
                    .stages
                    .iter()
                    .find(|(s, _, _)| *s == physical)
                    .expect("a binding names a stage of the case");
                let (creation, info) =
                    (st(creation, 0), StageInfo { deadline: Hlc::new(deadline, 0) });
                t.insert(stamp, bind(physical), Some((&creation, &info)))
            }
            "lcreate" => t.insert(stamp, create(physical), None),
            "ldrop" => t.insert(stamp, RegisterEvent::LogicalDrop, None),
            other => panic!("{other}"),
        };
        match put {
            Put::Stored => {}
            Put::InvalidBinding => invalid += 1,
            other => panic!("{other:?}"),
        }
    }
    (t, invalid)
}

fn visible_of(v: &Visible) -> Vis {
    let origin = |s: &Stamp| s.node.to_bytes()[0] as usize;
    match v {
        Visible::Identity => Vis::Identity,
        Visible::Bound { physical, stamp } => {
            Vis::Bind(leak(physical), stamp.hlc.wall_ms, origin(stamp))
        }
        Visible::Created { physical, stamp } => {
            Vis::Create(leak(physical), stamp.hlc.wall_ms, origin(stamp))
        }
        Visible::Dropped { stamp } => Vis::Drop(stamp.hlc.wall_ms, origin(stamp)),
    }
}

fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

#[test]
fn the_pure_function_answers_every_case_of_the_model_table_as_the_model_does() {
    assert_eq!(table::MODEL_SHA.len(), 16, "the table names the model it was generated from");
    assert!(CASES.len() >= 400, "{} cases", CASES.len());
    for (i, case) in CASES.iter().enumerate() {
        let t = case_timeline(case);
        let held: Vec<(&str, Ready)> =
            case.physicals.iter().map(|(p, r)| (*p, ready_of(*r))).collect();
        let visible = t.visible(ready(&held));
        assert_eq!(visible_of(&visible), case.visible, "case {i}: visible");
        let mut buried: Vec<(String, u64, usize)> = t
            .burials(&visible, ready(&held))
            .into_iter()
            .map(|b| (b.physical, b.at.hlc.wall_ms, b.at.node.to_bytes()[0] as usize))
            .collect();
        buried.sort();
        let mut want: Vec<(String, u64, usize)> =
            case.burials.iter().map(|&(p, h, o)| (p.to_string(), h, o)).collect();
        want.sort();
        assert_eq!(buried, want, "case {i}: burials");
        // the stages the burial by a later event buries: every stage of the case is one this member knows the creation of
        let stages: Vec<(&str, Hlc)> = case
            .stages
            .iter()
            .map(|&(s, created, deadline)| {
                let info = StageInfo { deadline: Hlc::new(deadline, 0) };
                (s, info.effective_deadline(&st(created, 0)))
            })
            .collect();
        let mut doomed: Vec<(String, u64, usize)> = t
            .doomed(&visible, ready(&held), &stages)
            .into_iter()
            .map(|b| (b.physical, b.at.hlc.wall_ms, b.at.node.to_bytes()[0] as usize))
            .collect();
        doomed.sort();
        let mut want: Vec<(String, u64, usize)> =
            case.doomed.iter().map(|&(p, h, o)| (p.to_string(), h, o)).collect();
        want.sort();
        assert_eq!(doomed, want, "case {i}: doomed");
        for &(physical, superseded) in case.superseded {
            let got =
                t.superseded_at(physical).map(|s| (s.hlc.wall_ms, s.node.to_bytes()[0] as usize));
            assert_eq!(got, superseded, "case {i}: superseded_at({physical})");
        }
    }
}

#[test]
fn the_table_exercises_every_branch_it_is_meant_to() {
    let count = |f: &dyn Fn(&table::Case) -> bool| CASES.iter().filter(|c| f(c)).count();
    assert!(count(&|c| c.visible == Vis::Identity) >= 50, "identity");
    assert!(count(&|c| matches!(c.visible, Vis::Bind(..))) >= 50, "a binding visible");
    assert!(count(&|c| matches!(c.visible, Vis::Create(..))) >= 40, "a recreation visible");
    assert!(count(&|c| matches!(c.visible, Vis::Drop(..))) >= 40, "a logical drop visible");
    assert!(count(&|c| !c.burials.is_empty()) >= 150, "a burial");
    assert!(count(&|c| !c.doomed.is_empty()) >= 100, "a burial by a later event");
    assert!(
        count(&|c| !c.doomed.is_empty() && !c.burials.is_empty()) >= 20,
        "both causes in one case"
    );
    assert!(
        count(&|c| c.superseded.iter().any(|(_, s)| s.is_some())) >= 100,
        "a superseded physical"
    );
    // a binding skipped because its physical was missing or buried, with something older visible
    let skipped = |c: &table::Case| {
        c.events.iter().any(|&(k, _, _, p)| {
            (k == "bind" || k == "lcreate")
                && c.physicals.iter().any(|&(n, r)| n == p && r != TReady::Alive)
        })
    };
    assert!(count(&skipped) >= 50, "an event whose physical is not ready");
    // an invalid binding (above its stage's deadline)
    let invalid = |c: &table::Case| {
        c.events
            .iter()
            .any(|&(k, _, h, p)| k == "bind" && c.stages.iter().any(|&(s, _, d)| s == p && h > d))
    };
    assert!(count(&invalid) >= 50, "a binding above its stage's deadline");
    // and the pure layer refuses exactly those, whatever happens to them next
    let refused: usize = CASES.iter().map(|c| case_timeline_counting(c).1).sum();
    let above: usize = CASES
        .iter()
        .map(|c| {
            c.events
                .iter()
                .filter(|&&(k, _, h, p)| {
                    k == "bind" && c.stages.iter().any(|&(s, _, d)| s == p && h > d)
                })
                .count()
        })
        .sum();
    assert_eq!(refused, above, "every binding above its stage's deadline is refused as invalid");
    assert!(refused >= 300, "{refused} invalid bindings in the table");
}

// ---------------------------------------------------------------------------------------------------------------------
// every order of arrival

/// A small deterministic generator.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) % n
    }
}

fn permutations(n: usize) -> Vec<Vec<usize>> {
    fn go(prefix: &mut Vec<usize>, left: &mut Vec<usize>, out: &mut Vec<Vec<usize>>) {
        if left.is_empty() {
            out.push(prefix.clone());
            return;
        }
        for i in 0..left.len() {
            let x = left.remove(i);
            prefix.push(x);
            go(prefix, left, out);
            prefix.pop();
            left.insert(i, x);
        }
    }
    let mut out = Vec::new();
    go(&mut Vec::new(), &mut (0..n).collect(), &mut out);
    out
}

/// What a member holds of the name's physicals at the end, as a function of the events delivered: every physical is held
/// (alive) once its event has arrived, except those the scenario says arrive incomplete, which become alive one delivery later.
#[derive(Clone)]
struct Scenario {
    events: Vec<(u64, usize, RegisterEvent)>,
    /// Physicals that are `Missing` when their event arrives and become `Alive` one delivery later.
    slow: Vec<&'static str>,
    writes: Vec<(&'static str, Stamp)>,
}

struct Run {
    visible: Vec<Visible>,
    /// every physical buried so far, accumulated over the run, with the stamp that buried it
    buried: Vec<Burial>,
    late: Vec<Vec<bool>>,
    superseded: Vec<Vec<Option<Stamp>>>,
}

fn run(scenario: &Scenario, order: &[usize]) -> Run {
    let mut t = Timeline::new("L");
    let mut held: Vec<(String, Ready)> = vec![("L".into(), Ready::Alive)];
    let mut out = Run { visible: vec![], buried: vec![], late: vec![], superseded: vec![] };
    let mut pending_alive: Vec<String> = vec![];
    let physicals = ["L", "a", "b", "c", "r"];
    for &i in order {
        let (hlc, origin, event) = &scenario.events[i];
        // the physicals that were incomplete become alive one delivery later
        for p in pending_alive.drain(..) {
            if let Some(h) = held.iter_mut().find(|(n, r)| *n == p && *r == Ready::Missing) {
                h.1 = Ready::Alive;
            }
        }
        if let Some(p) = event.physical() {
            let slow = scenario.slow.contains(&p);
            if !held.iter().any(|(n, _)| n == p) {
                held.push((p.to_string(), if slow { Ready::Missing } else { Ready::Alive }));
                if slow {
                    pending_alive.push(p.to_string());
                }
            }
        }
        let (creation, info) = a_stage();
        t.insert(st(*hlc, *origin), event.clone(), Some((&creation, &info)));
        let lookup = |p: &str| held.iter().find(|(n, _)| n == p).map_or(Ready::Buried, |(_, r)| *r);
        let visible = t.visible(lookup);
        for b in t.burials(&visible, lookup) {
            if let Some(h) = held.iter_mut().find(|(n, _)| *n == b.physical) {
                h.1 = Ready::Buried;
            }
            out.buried.push(b);
        }
        out.late.push(scenario.writes.iter().map(|(p, w)| t.is_late(p, w)).collect());
        out.superseded.push(physicals.iter().map(|p| t.superseded_at(p)).collect());
        out.visible.push(visible);
    }
    // deliveries have all arrived: let the incomplete ones complete, and judge once more
    for p in pending_alive.drain(..) {
        if let Some(h) = held.iter_mut().find(|(n, r)| *n == p && *r == Ready::Missing) {
            h.1 = Ready::Alive;
        }
    }
    let lookup = |p: &str| held.iter().find(|(n, _)| n == p).map_or(Ready::Buried, |(_, r)| *r);
    let visible = t.visible(lookup);
    for b in t.burials(&visible, lookup) {
        if let Some(h) = held.iter_mut().find(|(n, _)| *n == b.physical) {
            h.1 = Ready::Buried;
        }
        out.buried.push(b);
    }
    out.visible.push(visible);
    out
}

#[test]
fn every_order_of_arrival_ends_alike_and_the_visible_event_the_burials_and_the_late_writes_only_move_forward()
 {
    let mut rng = Lcg(0x5eed);
    let names = ["a", "b", "c", "r"];
    let mut scenarios = 0;
    for _ in 0..120 {
        let n = 2 + rng.next(4) as usize; // 2 to 5 events
        let mut events: Vec<(u64, usize, RegisterEvent)> = Vec::new();
        let mut used: Vec<&str> = Vec::new();
        for k in 0..n {
            let hlc = 10 + rng.next(60);
            let origin = rng.next(3) as usize;
            let event = match rng.next(4) {
                0 => RegisterEvent::LogicalDrop,
                1 => {
                    let p = names[3];
                    if used.contains(&p) {
                        RegisterEvent::LogicalDrop
                    } else {
                        used.push(p);
                        create(p)
                    }
                }
                _ => {
                    let p = names[(rng.next(3)) as usize];
                    if used.contains(&p) {
                        RegisterEvent::LogicalDrop
                    } else {
                        used.push(p);
                        bind(p)
                    }
                }
            };
            // distinct stamps
            if events.iter().any(|(h, o, _)| *h == hlc && *o == origin) {
                continue;
            }
            let _ = k;
            events.push((hlc, origin, event));
        }
        if events.len() < 2 {
            continue;
        }
        let slow: Vec<&'static str> = names.iter().copied().filter(|_| rng.next(3) == 0).collect();
        let writes: Vec<(&'static str, Stamp)> = (0..3)
            .map(|_| {
                (
                    ["L", "a", "b", "c", "r"][rng.next(5) as usize],
                    st(10 + rng.next(80), rng.next(3) as usize),
                )
            })
            .collect();
        let scenario = Scenario { events, slow, writes };
        scenarios += 1;
        let perms = permutations(scenario.events.len());
        let reference = run(&scenario, &perms[0]);
        for order in &perms {
            let r = run(&scenario, order);
            // the end: the same visible event and the same set of buried physicals, whatever the order
            assert_eq!(
                r.visible.last(),
                reference.visible.last(),
                "visible at the end, order {order:?}"
            );
            let set = |run: &Run| {
                let mut v: Vec<String> = run.buried.iter().map(|b| b.physical.clone()).collect();
                v.sort();
                v.dedup();
                v
            };
            // the physicals buried: buried exactly once each
            assert_eq!(r.buried.len(), set(&r).len(), "a physical is buried once, order {order:?}");
            // the same set whatever the order, EXCEPT that a physical whose own event arrives after a newer visible
            // event is judged when it arrives: it is in the same final set, which is what is compared
            assert_eq!(set(&r), set(&reference), "buried set, order {order:?}");
            // monotone: the visible event only moves to newer stamps once things are ready, the late set only grows
            let mut stamp: Option<Stamp> = None;
            for v in &r.visible {
                let now = v.stamp().copied();
                if let (Some(then), Some(now)) = (stamp, now) {
                    assert!(
                        now >= then,
                        "the visible event moved back from {then:?} to {now:?}, order {order:?}"
                    );
                }
                if now.is_some() {
                    stamp = now;
                }
            }
            for w in 0..scenario.writes.len() {
                let mut was = false;
                for step in &r.late {
                    assert!(!was || step[w], "a late write became not late, order {order:?}");
                    was = step[w];
                }
            }
            for p in 0..5 {
                let mut was: Option<Stamp> = None;
                for step in &r.superseded {
                    if let (Some(then), Some(now)) = (was, step[p]) {
                        assert!(
                            now <= then,
                            "the earliest superseding event only moves earlier, order {order:?}"
                        );
                    }
                    assert!(
                        was.is_none() || step[p].is_some(),
                        "a superseded physical stopped being superseded"
                    );
                    was = step[p].or(was);
                }
            }
            // the late verdicts at the end are the same
            assert_eq!(r.late.last(), reference.late.last(), "late at the end, order {order:?}");
            assert_eq!(
                r.superseded.last(),
                reference.superseded.last(),
                "superseded at the end, order {order:?}"
            );
        }
    }
    assert!(scenarios >= 60, "{scenarios} scenarios were run");
}

// ---------------------------------------------------------------------------------------------------------------------
// the two named invariants of decision 5

/// Every case of the table and every scenario of the order test: the invariants are properties of the function, so they hold
/// wherever it is called.
fn all_answers() -> Vec<(Timeline, Vec<(String, Ready)>)> {
    let mut out = Vec::new();
    for case in CASES {
        let held = case.physicals.iter().map(|(p, r)| (p.to_string(), ready_of(*r))).collect();
        out.push((case_timeline(case), held));
    }
    // every readiness of up to four physicals under a fixed timeline
    let t = timeline(&[
        (10, 0, bind("a")),
        (20, 1, bind("b")),
        (30, 0, create("c")),
        (40, 2, RegisterEvent::LogicalDrop),
    ]);
    for mask in 0..3u32.pow(4) {
        let pick = |i: u32| match (mask / 3u32.pow(i)) % 3 {
            0 => Ready::Alive,
            1 => Ready::Missing,
            _ => Ready::Buried,
        };
        out.push((
            t.clone(),
            vec![
                ("orders".into(), Ready::Alive),
                ("a".into(), pick(0)),
                ("b".into(), pick(1)),
                ("c".into(), pick(2)),
                ("d".into(), pick(3)),
            ],
        ));
    }
    out
}

#[test]
fn the_old_physical_is_never_buried_unless_the_new_one_is_alive() {
    let mut buried_something = 0;
    for (t, held) in all_answers() {
        let lookup = |p: &str| held.iter().find(|(n, _)| n == p).map_or(Ready::Buried, |(_, r)| *r);
        let visible = t.visible(lookup);
        let burials = t.burials(&visible, lookup);
        if burials.is_empty() {
            continue;
        }
        buried_something += 1;
        // a logical drop leaves no new physical to wait for: the name is absent
        if let Some(new) = visible.physical(t.identity()) {
            assert_eq!(
                lookup(new),
                Ready::Alive,
                "{burials:?} were buried for {visible:?}, which is not alive here"
            );
        }
        assert!(
            !matches!(visible, Visible::Identity),
            "the identity serving buries nothing: {burials:?}"
        );
    }
    assert!(buried_something >= 100, "the invariant was exercised {buried_something} times");
}

#[test]
fn a_binding_whose_physical_is_buried_is_never_visible() {
    let mut skipped = 0;
    for (t, held) in all_answers() {
        let lookup = |p: &str| held.iter().find(|(n, _)| n == p).map_or(Ready::Buried, |(_, r)| *r);
        let visible = t.visible(lookup);
        if let Visible::Bound { physical, .. } | Visible::Created { physical, .. } = &visible {
            assert_ne!(
                lookup(physical),
                Ready::Buried,
                "{visible:?} is a binding or a recreation of a buried physical"
            );
        }
        // the skipped ones: a binding or a recreation newer than the visible event, whose physical is buried
        let newer_buried = t.events().iter().any(|(s, e)| {
            visible.stamp().is_none_or(|v| s > v)
                && e.physical().is_some_and(|p| lookup(p) == Ready::Buried)
        });
        skipped += usize::from(newer_buried);
    }
    assert!(skipped >= 30, "the invariant was exercised {skipped} times");
}
