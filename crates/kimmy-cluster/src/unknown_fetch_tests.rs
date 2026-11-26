//! w4 of unit P's pending write (Q6-build-order.md, "The pending write"): the fetch of a collection a member cannot make is exempt from the repair cooldown. An unknown-collection stop is planned on the
//! strength of the stop itself (`plan_repair(peer, collection, Repair::Snapshot)`, transport.rs, the `(None, Some(unknown))` arm), and `plan_repair` refuses a collection whose repair ran against that peer
//! and has not cooled (`REPAIR_COOLDOWN_ROUNDS` = 60 contacts, about 5 minutes at the default tick). A collection dropped and remade twice inside five minutes, with the member stopped at a write each time,
//! would wait out the cooldown for the second fetch. Expected: the second fetch is planned AT ONCE. Today RED: `plan_repair` answers false while cooling.
//! The planner's own entry point is what the fix changes (an unknown-collection fetch is planned through its own call, or `plan_repair` takes the reason): adapt the calls then; the assertions stay.
//! NOT COMMITTED: pending, with the other unit P tests; it goes in crates/kimmy-cluster/src/unknown_fetch_tests.rs with `#[cfg(test)] mod unknown_fetch_tests;` in lib.rs.

use kimmy_core::{CollectionId, NodeId};

use crate::{PeerStalls, REPAIR_COOLDOWN_ROUNDS, Repair};

fn node(n: u8) -> NodeId {
    NodeId::from_bytes([n; 16])
}

/// The first fetch of the collection from a peer is planned, run and finished; the collection is dropped and remade, and the member is stopped at a write again `rounds` contacts later: the second fetch.
fn second_fetch_planned(rounds: u32) -> bool {
    let peer = node(1);
    let collection = CollectionId(7);
    let mut stalls = PeerStalls::new();
    assert!(stalls.plan_repair(peer, collection, Repair::Snapshot), "the first fetch is planned");
    assert_eq!(stalls.repair_due(peer, false), Some((collection, Repair::Snapshot)), "and runs");
    stalls.repair_finished(peer);
    for _ in 0..rounds {
        stalls.tick_opened();
        assert_eq!(stalls.repair_due(peer, false), None);
    }
    stalls.plan_repair(peer, collection, Repair::Snapshot)
}

#[test]
fn w4_a_second_unknown_collection_fetch_is_planned_at_once_not_after_the_cooldown() {
    let waited: Vec<String> = [0, 1, 5, REPAIR_COOLDOWN_ROUNDS - 1]
        .into_iter()
        .filter(|&rounds| !second_fetch_planned(rounds))
        .map(|rounds| format!("{rounds} contacts after the first fetch finished"))
        .collect();
    assert!(
        waited.is_empty(),
        "w4: the second fetch of a collection the member cannot make was refused ({}): it waited for the {REPAIR_COOLDOWN_ROUNDS}-contact cooldown",
        waited.join("; ")
    );
}
