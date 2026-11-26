//! D13 containment, the decisions that are pure functions (ADR-274): the bound on a claimed epoch,
//! the guard's three cases, the halt marker's rule and the `--replace` flag. The tests commit of
//! unit 14a: each is red on its designed assertion against the scaffold in `containment.rs`. Test
//! numbers are those of `D13-prebuild.md`.

use std::time::Duration;

use kimmy_core::{Epoch, Hlc, NodeId};

use crate::containment::*;

const CLOCK: u64 = 1_000_000_000_000;
const SKEW: u64 = 300_000;

fn epoch_starting(wall_ms: u64) -> Epoch {
    Epoch::new(Hlc::new(wall_ms, 0), [5; 8])
}

fn node(n: u8) -> NodeId {
    NodeId::from_bytes([n; 16])
}

// -- the bound (tests 6 and 15; findings 4, F; gap 4)
// -------------------------------------------------------------------

/// Both sides of the boundary: an epoch exactly at the bound is believed, one millisecond beyond is
/// not (the bound is inclusive, as U1's is).
#[test]
fn a_claim_at_the_bound_is_believed_and_one_millisecond_beyond_it_is_not() {
    assert!(claim_within_bound(CLOCK, SKEW, &epoch_starting(CLOCK + SKEW)), "exactly at the bound");
    assert!(
        !claim_within_bound(CLOCK, SKEW, &epoch_starting(CLOCK + SKEW + 1)),
        "one millisecond beyond the bound is refused"
    );
}

#[test]
fn a_claim_far_beyond_the_bound_is_refused_and_a_past_one_is_not() {
    assert!(
        !claim_within_bound(CLOCK, SKEW, &epoch_starting(CLOCK + 10 * SKEW)),
        "a start in the far future"
    );
    assert!(
        claim_within_bound(CLOCK, SKEW, &epoch_starting(CLOCK - 10 * SKEW)),
        "a start in the past"
    );
    assert!(
        claim_within_bound(CLOCK, SKEW, &Epoch::ZERO),
        "the smallest epoch is within any bound"
    );
}

/// `Epoch::MAX` is a sentinel on the wire (a replay position carries it in a stamp) and can never
/// be exceeded: it is refused as a claim, whatever the clock.
#[test]
fn the_maximum_epoch_is_never_believed() {
    assert!(!claim_within_bound(CLOCK, SKEW, &Epoch::MAX));
    assert!(!claim_within_bound(u64::MAX / 2, SKEW, &Epoch::MAX));
}

/// A member whose wall clock is behind its own logical clock (it took peers' stamps within the
/// bound) holds a claim against the logical one, or it refuses every legitimate new epoch (gap 4).
/// The boundary is HLC-inclusive (J-15).
#[test]
fn the_bound_reads_the_later_of_the_wall_clock_and_the_logical_clock() {
    assert_eq!(bound_clock_ms(1_000, 5_000), 5_000, "the logical clock leads: it is the clock");
    assert_eq!(bound_clock_ms(5_000, 1_000), 5_000, "the wall clock leads: it is the clock");
    let behind = bound_clock_ms(CLOCK - 120_000, CLOCK);
    assert!(
        claim_within_bound(behind, SKEW, &epoch_starting(CLOCK + SKEW)),
        "a legitimate new epoch, above the wall clock and inside the logical bound, is believed"
    );
}

// -- the guard (test 9, with the window and the three cases ruled on 2026-11-25)
// ------------------------------------------

const SUSPECT_TO_DOWN: Duration = Duration::from_millis(4_800);

#[test]
fn the_window_is_twice_the_suspicion_to_down_time() {
    assert_eq!(guard_window(SUSPECT_TO_DOWN), Duration::from_millis(9_600));
}

/// (i) no seed configured, and a seed that answers nothing: nobody to ask, so no wait (9h).
#[test]
fn the_guard_passes_at_once_with_no_seed_or_no_seed_reachable() {
    assert_eq!(guard_step(false, false, false, false, SUSPECT_TO_DOWN), GuardStep::Pass, "no seed");
    assert_eq!(
        guard_step(true, false, false, false, SUSPECT_TO_DOWN),
        GuardStep::Pass,
        "none answers"
    );
}

/// (ii) a seed answers and no member list has arrived: wait for the first one, bounded by the seed
/// timeout.
#[test]
fn the_guard_waits_for_the_first_member_list_but_not_for_ever() {
    assert_eq!(
        guard_step(true, true, false, false, SUSPECT_TO_DOWN),
        GuardStep::AwaitMemberList(SEED_TIMEOUT)
    );
    assert!(
        SEED_TIMEOUT > guard_window(SUSPECT_TO_DOWN),
        "a slow first list is not read as silence"
    );
}

/// (iii) another address lists this node id: probe it and wait the window. The list without it
/// passes.
#[test]
fn the_guard_probes_an_address_that_lists_this_node_and_passes_when_none_does() {
    assert_eq!(
        guard_step(true, true, true, true, SUSPECT_TO_DOWN),
        GuardStep::Probe(guard_window(SUSPECT_TO_DOWN))
    );
    assert_eq!(
        guard_step(true, true, true, false, SUSPECT_TO_DOWN),
        GuardStep::Pass,
        "the list does not name it"
    );
}

// -- the halt marker (test 17, ruling 7)
// ---------------------------------------------------------------------------------

#[test]
fn a_marker_that_names_a_newer_epoch_halts_any_store_of_that_node_below_it() {
    let (halted, newer) = (epoch_starting(1_000), epoch_starting(2_000));
    let marker = Marker { node: node(1), halted, told: Some(newer) };
    assert!(marker_halts(&marker, node(1), halted), "the store it names");
    assert!(
        marker_halts(&marker, node(1), epoch_starting(500)),
        "an OLDER copy put back under the marker (an 'epoch differs, ignore' rule would let it run)"
    );
    assert!(marker_halts(&marker, node(1), epoch_starting(1_999)), "any epoch below the newer one");
}

#[test]
fn a_marker_does_not_halt_a_store_at_or_above_the_newer_epoch() {
    let (halted, newer) = (epoch_starting(1_000), epoch_starting(2_000));
    let marker = Marker { node: node(1), halted, told: Some(newer) };
    assert!(!marker_halts(&marker, node(1), newer), "the store on the newer epoch itself");
    assert!(
        !marker_halts(&marker, node(1), epoch_starting(2_001)),
        "a re-restore, above the newer epoch by the floor"
    );
}

#[test]
fn a_guard_only_marker_halts_only_the_store_on_its_epoch() {
    let halted = epoch_starting(1_000);
    let marker = Marker { node: node(1), halted, told: None };
    assert!(marker_halts(&marker, node(1), halted), "the store on the epoch it names");
    assert!(
        !marker_halts(&marker, node(1), epoch_starting(1_001)),
        "another epoch is a different store"
    );
    assert!(
        !marker_halts(&marker, node(1), epoch_starting(999)),
        "a lower one too: there is no newer epoch to be below"
    );
}

#[test]
fn a_marker_of_another_node_is_ignored() {
    let (halted, newer) = (epoch_starting(1_000), epoch_starting(2_000));
    assert!(!marker_halts(&Marker { node: node(1), halted, told: Some(newer) }, node(2), halted));
    assert!(!marker_halts(&Marker { node: node(1), halted, told: None }, node(2), halted));
}

// -- `--replace` (ruling 2)
// -----------------------------------------------------------------------------------------------

#[test]
fn replace_is_honoured_only_for_the_store_on_the_epoch_it_names() {
    let minted = epoch_starting(1_000);
    assert!(replace_honoured(Some(minted), minted), "the restore's own store");
    assert!(
        !replace_honoured(Some(minted), epoch_starting(2_000)),
        "a later restore into the directory is not authorized"
    );
    assert!(!replace_honoured(None, minted), "no flag, no replace");
}
