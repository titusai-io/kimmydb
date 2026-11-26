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
    assert!(
        !claim_within_bound(u64::MAX, SKEW, &Epoch::MAX),
        "not even at the last clock, where the bound saturates and its start is no longer beyond it"
    );
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

// -- the halt marker (test 17, ruling 7 as the model row made it exact)
// ----------------------------

fn a_guard_halt(epoch: Epoch) -> GuardHalt {
    GuardHalt { epoch, address: "10.1.2.3:7900".parse().unwrap() }
}

#[test]
fn a_marker_halts_any_store_of_its_node_below_the_floor() {
    let (halted, floor) = (epoch_starting(1_000), epoch_starting(2_000));
    let marker = Marker { node: node(1), floor, guard: None };
    assert!(marker_halts(&marker, node(1), halted), "the store that was told");
    assert!(
        marker_halts(&marker, node(1), epoch_starting(500)),
        "an OLDER copy put back under the marker (an 'epoch differs, ignore' rule would let it run)"
    );
    assert!(marker_halts(&marker, node(1), epoch_starting(1_999)), "any epoch below the floor");
}

#[test]
fn a_marker_does_not_halt_a_store_at_or_above_its_floor() {
    let marker = Marker { node: node(1), floor: epoch_starting(2_000), guard: None };
    assert!(!marker_halts(&marker, node(1), epoch_starting(2_000)), "on the floor itself");
    assert!(
        !marker_halts(&marker, node(1), epoch_starting(2_001)),
        "a re-restore, above the floor by the mint"
    );
}

/// A guard's halt halts only the store on the epoch it halted: the host that restored and was
/// halted for a live host at that address, and no other store of the node.
#[test]
fn a_guard_halt_halts_only_the_store_on_its_epoch() {
    let halted = epoch_starting(1_000);
    let marker = Marker { node: node(1), floor: Epoch::ZERO, guard: Some(a_guard_halt(halted)) };
    assert!(marker_halts(&marker, node(1), halted), "the store on the epoch the guard halted");
    assert!(!marker_halts(&marker, node(1), epoch_starting(1_001)), "another epoch");
    assert!(!marker_halts(&marker, node(1), epoch_starting(999)), "no floor to be below");
}

#[test]
fn a_marker_of_another_node_is_ignored() {
    let marker = Marker {
        node: node(1),
        floor: epoch_starting(2_000),
        guard: Some(a_guard_halt(epoch_starting(1_000))),
    };
    assert!(!marker_halts(&marker, node(2), epoch_starting(1_000)));
    assert!(!marker_halts(&marker, node(2), epoch_starting(500)));
}

/// A tell raises the floor and never lowers it, and a guard halt already recorded is kept.
#[test]
fn a_tell_raises_the_floor_and_never_lowers_it() {
    let first = marker_told(None, node(1), epoch_starting(2_000));
    assert_eq!(first.floor, epoch_starting(2_000), "the first tell sets the floor");
    let higher = marker_told(Some(&first), node(1), epoch_starting(3_000));
    assert_eq!(higher.floor, epoch_starting(3_000));
    let lower = marker_told(Some(&higher), node(1), epoch_starting(2_500));
    assert_eq!(lower.floor, epoch_starting(3_000), "an older tell does not lower it");
}

/// Row 4c, mutant `floor-lost-at-guard`: a later halt that has only the guard's reason keeps the
/// floor an earlier tell set, or a recovery that the guard halted mints below it.
#[test]
fn a_later_guard_halt_keeps_the_floor() {
    let told = marker_told(None, node(1), epoch_starting(2_000));
    let guarded = marker_guard_halted(Some(&told), node(1), a_guard_halt(epoch_starting(2_100)));
    assert_eq!(guarded.floor, epoch_starting(2_000), "the floor survives the guard's halt");
    assert_eq!(guarded.guard, Some(a_guard_halt(epoch_starting(2_100))));
    let again = marker_told(Some(&guarded), node(1), epoch_starting(1_500));
    assert_eq!(again.guard, guarded.guard, "and a tell keeps the guard's halt");
}

// -- a tell is bounded on the told side, and a superseded peer runs no round (model row, finding 3)
// --

#[test]
fn a_tell_is_acted_on_only_when_it_is_newer_and_within_the_told_hosts_bound() {
    let own = epoch_starting(CLOCK - 5_000);
    assert!(tell_acted_on(CLOCK, SKEW, own, epoch_starting(CLOCK)), "newer and in the bound");
    assert!(
        tell_acted_on(CLOCK, SKEW, own, epoch_starting(CLOCK + SKEW)),
        "exactly at the bound is believed, as a claim is"
    );
    assert!(
        !tell_acted_on(CLOCK, SKEW, own, epoch_starting(CLOCK + SKEW + 1)),
        "a tell beyond the told host's bound is not acted on (mutant `tell-unbounded`)"
    );
    assert!(!tell_acted_on(CLOCK, SKEW, own, Epoch::MAX), "Epoch::MAX is never acted on");
    assert!(!tell_acted_on(CLOCK, SKEW, own, own), "an equal epoch is not newer");
    assert!(
        !tell_acted_on(CLOCK, SKEW, own, epoch_starting(CLOCK - 10_000)),
        "an older one neither"
    );
}

/// A member that knows its peer is on an older epoch of its node runs no round with it, whether or
/// not the tell was acted on (mutant `round-with-superseded`): this keeps the cluster's side
/// contained when the told host's clock is too far behind to act.
#[test]
fn no_round_runs_with_a_peer_on_an_epoch_older_than_the_newest_known() {
    let (older, newest) = (epoch_starting(1_000), epoch_starting(2_000));
    assert!(!runs_round(newest, older), "the peer is superseded");
    assert!(runs_round(newest, newest), "the peer is on the newest epoch");
    assert!(runs_round(older, newest), "the peer is newer than anything known");
    assert!(runs_round(Epoch::ZERO, older), "nothing is known of the node");
}

// -- the relayed rows (test 14; the model row's finding 4)
// ---------------------------------------------------------------

/// What a relayed row does: a newer epoch of the host's own node, within the bound, halts it; a
/// newer epoch of another node, within the bound, is recorded; everything else is dropped. The
/// bound is the claim's, inclusive.
#[test]
fn a_relayed_row_halts_records_or_is_dropped() {
    let own = (node(1), epoch_starting(CLOCK - 5_000));
    let newer = epoch_starting(CLOCK);
    assert_eq!(relayed_row(CLOCK, SKEW, own, (node(1), newer)), RelayedRow::Halt);
    assert_eq!(
        relayed_row(CLOCK, SKEW, own, (node(1), epoch_starting(CLOCK + SKEW))),
        RelayedRow::Halt,
        "exactly at the bound"
    );
    assert_eq!(relayed_row(CLOCK, SKEW, own, (node(2), newer)), RelayedRow::Record);
    for (what, row) in [
        ("own node, equal", (node(1), own.1)),
        ("own node, older", (node(1), epoch_starting(CLOCK - 10_000))),
        ("own node, no epoch", (node(1), Epoch::ZERO)),
        ("own node, beyond the bound", (node(1), epoch_starting(CLOCK + SKEW + 1))),
        ("own node, the maximum", (node(1), Epoch::MAX)),
        ("another node, beyond the bound", (node(2), epoch_starting(CLOCK + SKEW + 1))),
        ("another node, the maximum", (node(2), Epoch::MAX)),
        ("another node, no epoch", (node(2), Epoch::ZERO)),
    ] {
        assert_eq!(relayed_row(CLOCK, SKEW, own, row), RelayedRow::Drop, "{what}");
    }
}

// -- the recovery restore (tests 4 and 17: which case each is)
// --------------------------------------

/// The recovery makes a third host of the node: the newer host, if it is up, halts it through the
/// guard unless `--replace` was honoured, and with it the newer host is then told and halts. With
/// the newer host stopped, the recovered host proceeds.
#[test]
fn a_recovery_restore_wins_only_with_replace_or_when_the_newer_host_is_stopped() {
    assert_eq!(
        recovery_outcome(true, false),
        Recovery { recovered_host_halts: true, newer_host_halts: false },
        "row 4c: the newer host is up and no --replace: the guard halts the recovered host"
    );
    assert_eq!(
        recovery_outcome(true, true),
        Recovery { recovered_host_halts: false, newer_host_halts: true },
        "row 4b: --replace: the recovered host proceeds and the host that halted it is told"
    );
    assert_eq!(
        recovery_outcome(false, false),
        Recovery { recovered_host_halts: false, newer_host_halts: false },
        "the newer host is stopped: nothing answers the guard"
    );
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

// -- halt takes the writer gate, which is not re-entrant (hardproblems' review) ------------

/// `Engine::halt` waits for the writer, so a thread that holds a write transaction and calls it
/// would wait for itself. The cluster crate opens no write transaction of its own, and a tell is
/// acted on only from the async functions that read the frame (the accept loop, the handshakes,
/// a sync round's first reply, a push's reply, an exchange): a write transaction holds a guard that
/// is not `Send`, so none can be alive across the `.await` that read it. This pins both.
#[test]
fn a_tell_is_acted_on_only_where_no_write_transaction_is_held() {
    const ALLOWED: &[&str] = &[
        "accept_handshake",
        "open_handshake_claimed",
        "serve_peer_counting",
        "sync_round",
        "push_over",
        "frames",
    ];
    let mut found = 0;
    let mut seen = std::collections::BTreeSet::new();
    for (file, source) in [
        ("transport.rs", include_str!("transport.rs")),
        ("confirm.rs", include_str!("confirm.rs")),
        ("exchange.rs", include_str!("exchange.rs")),
    ] {
        // The production code ends where the file's own test module begins: transport.rs has
        // smaller test modules above most of its code, so the first `#[cfg(test)]` is not the end.
        let end = source.find("#[cfg(test)]\nmod tests {").expect("the file's test module");
        let production = &source[..end];
        assert!(!production.contains("begin_write"), "{file} opens a write transaction");
        let lines: Vec<&str> = production.lines().collect();
        for (n, line) in lines.iter().enumerate() {
            if line.trim_start().starts_with("//") || !line.contains("act_on_tell(") {
                continue;
            }
            let enclosing = lines[..=n]
                .iter()
                .rev()
                .find_map(|l| {
                    let head = l.trim_start();
                    let at = head.find("fn ")?;
                    head[..at]
                        .split_whitespace()
                        .all(|w| w.starts_with("pub") || ["async", "const"].contains(&w))
                        .then(|| head[at + 3..].split(['(', '<']).next().unwrap_or("").to_string())
                })
                .unwrap_or_default();
            assert!(ALLOWED.contains(&enclosing.as_str()), "{file}:{}: {enclosing}", n + 1);
            seen.insert(enclosing);
            found += 1;
        }
    }
    assert!(found >= 5, "premise: the scan found the call sites: {found}");
    let allowed: std::collections::BTreeSet<String> =
        ALLOWED.iter().map(ToString::to_string).collect();
    assert_eq!(seen, allowed, "every allowed function still acts on a tell, and no other does");
}
