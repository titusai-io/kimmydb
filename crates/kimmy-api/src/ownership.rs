//! Which node delivers a given subscription.
//!
//! # Not a leader
//!
//! `owner(subscription, members)` is a **pure function** of the subscription id
//! and the live member set. Every node computes it independently and gets the
//! same answer; there is no vote, no term, no consensus and no cluster-wide
//! coordinator. Different subscriptions land on different nodes, so the work
//! spreads, and a transient disagreement about membership produces a *duplicate
//! delivery* rather than a split brain.
//!
//! # Rendezvous hashing, not modulo
//!
//! `hash(subscription, member)` per member, highest wins. The obvious
//! alternative — `hash(subscription) % members.len()` — remaps almost every
//! subscription whenever the member count changes, so one node leaving would
//! shuffle the entire cluster's assignments. Rendezvous moves only the
//! subscriptions that belonged to the departed node, which is the property that
//! makes failover cheap.
//!
//! # Why a node dying does not lose events
//!
//! The owner is derived from the *live* set, which SWIM maintains
//! ([ADR-037](../../../docs/decisions.md)). When a node dies it leaves that
//! set, every surviving node recomputes, and exactly one of them becomes the
//! new owner — then resumes from replicated progress rather than from the
//! beginning. Nothing is lost, and nothing needed to elect anything.
//!
//! # Node ids, not addresses
//!
//! This hashed `SocketAddr` until M8. An address is where a node *is*, not
//! which node it is: moving a node to a new address — a pod rescheduled onto a
//! different IP, a port changed, a host renumbered — reshuffled its
//! subscriptions as though it had left and a stranger had joined, for a node
//! that never went anywhere.
//!
//! SWIM gossips identities already, so the node id rides along as part of
//! [`kimmy_cluster::Member`] and costs no second channel and no mapping to
//! keep in step. A node id is durable — it lives inside the database file, so
//! it survives restarts and moves with a restore ([ADR-051](../../../docs/decisions.md)).

use std::collections::BTreeSet;

use kimmy_core::NodeId;

/// FNV-1a, so the mapping is stable across processes and releases.
///
/// A `DefaultHasher` is explicitly not guaranteed stable between Rust versions,
/// and an ownership function that changed under a compiler upgrade would
/// reshuffle every subscription in the cluster on a rolling restart.
fn hash(subscription: &str, member: &NodeId) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x100_0000_01b3;

    let mut h = OFFSET;
    // The hyphenated form, which `NodeId` fixes deliberately rather than
    // inheriting from `Uuid`'s serde. Hashing the *chosen* representation is
    // what keeps this answer the same everywhere, so the bytes cannot drift
    // with a format's idea of whether it is human-readable.
    let member = member.to_string();
    // A separator, for the same reason `CollectionId::derive` has one: without
    // it ("ab", "c") and ("a", "bc") hash the same bytes.
    for byte in subscription.as_bytes().iter().chain(b"\0").chain(member.as_bytes()) {
        h ^= u64::from(*byte);
        h = h.wrapping_mul(PRIME);
    }
    h
}

/// The member that owns this subscription, or `None` when there are none.
///
/// Ties break on the node id, so two members hashing identically still produce
/// one answer rather than depending on iteration order.
pub fn owner(subscription: &str, members: &BTreeSet<NodeId>) -> Option<NodeId> {
    members
        .iter()
        .map(|member| (hash(subscription, member), member))
        .max_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)))
        .map(|(_, member)| *member)
}

/// Whether this node should deliver `subscription`.
///
/// The candidate set is the live members **plus this node**. That is not
/// belt-and-braces — it is the fix for a bug the cluster harness caught on
/// its first run: the live set SWIM maintains contains *peers only*, never
/// the node holding it, so an owner computed over it can never be `me`.
/// Every node stood down for every subscription, and **no webhook was ever
/// delivered in any clustered deployment** — while single-node worked
/// (empty set, own everything), which is why nothing else caught it.
///
/// With clustering off the set is empty and the union is just `me`: a single
/// node owns everything, with no special case needed.
///
/// The trade this makes: a node SWIM has declared dead still considers
/// itself a candidate, so a flapping node can deliver alongside its
/// replacement until it rejoins or stops. That is a duplicate — which
/// at-least-once already promises receivers — where the alternative was
/// silence.
pub fn owns(subscription: &str, me: NodeId, members: &BTreeSet<NodeId>) -> bool {
    let mut candidates = members.clone();
    candidates.insert(me);
    owner(subscription, &candidates) == Some(me)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A node id from a single repeated byte, so tests read as `node(1)`
    /// rather than as a wall of hex.
    fn node(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    fn members(bytes: &[u8]) -> BTreeSet<NodeId> {
        bytes.iter().map(|b| node(*b)).collect()
    }

    #[test]
    fn every_node_computes_the_same_owner() {
        // The property that makes this work without coordination: three nodes
        // holding the same member set must all name the same owner, or two of
        // them deliver and one does not.
        let set = members(&[1, 2, 3]);
        for subscription in ["wh_a", "wh_b", "wh_c", "wh_d"] {
            let answers: BTreeSet<_> = (0..5).map(|_| owner(subscription, &set).unwrap()).collect();
            assert_eq!(answers.len(), 1, "{subscription} produced {answers:?}");
        }
    }

    #[test]
    fn exactly_one_node_owns_each_subscription() {
        let set = members(&[1, 2, 3]);
        for subscription in ["wh_a", "wh_b", "wh_c"] {
            let owners: Vec<_> = set.iter().filter(|m| owns(subscription, **m, &set)).collect();
            assert_eq!(owners.len(), 1, "{subscription} owned by {owners:?}");
        }
    }

    #[test]
    fn work_spreads_across_the_cluster() {
        // If every subscription hashed to one node, that node would do all the
        // delivering and the other two would idle.
        let set = members(&[1, 2, 3]);
        let used: BTreeSet<_> = (0..200).filter_map(|i| owner(&format!("wh_{i}"), &set)).collect();
        assert_eq!(used.len(), 3, "all three should own some subscriptions, got {used:?}");
    }

    #[test]
    fn a_node_leaving_moves_only_what_it_owned() {
        // The reason for rendezvous rather than modulo. With `hash % len`,
        // removing one of three members remaps roughly two thirds of all
        // subscriptions; here it must move only the departed node's share.
        let before = members(&[1, 2, 3]);
        let gone = node(3);
        let after: BTreeSet<_> = before.iter().copied().filter(|m| *m != gone).collect();

        let mut moved = 0;
        let mut owned_by_gone = 0;
        for i in 0..300 {
            let s = format!("wh_{i}");
            let was = owner(&s, &before).unwrap();
            let now = owner(&s, &after).unwrap();
            if was == gone {
                owned_by_gone += 1;
            } else if was != now {
                moved += 1;
            }
        }
        assert!(owned_by_gone > 50, "the departed node should have owned a fair share");
        assert_eq!(
            moved, 0,
            "no subscription owned by a surviving node may move when a different node leaves"
        );
    }

    #[test]
    fn a_dead_owners_subscriptions_are_taken_over() {
        // The design-review question, as a test: when a node dies, does
        // someone deliver?
        let before = members(&[1, 2, 3]);
        let subscription = "wh_orders";
        let dead = owner(subscription, &before).unwrap();

        let after: BTreeSet<_> = before.iter().copied().filter(|m| *m != dead).collect();
        let survivor = owner(subscription, &after).expect("someone must take it over");

        assert_ne!(survivor, dead);
        assert!(after.contains(&survivor));
        assert!(
            after.iter().filter(|m| owns(subscription, **m, &after)).count() == 1,
            "exactly one survivor takes it, not both"
        );
    }

    #[test]
    fn a_single_node_owns_everything() {
        // Clustering off, or SWIM not yet populated. A node that waited for a
        // member set it will never have would deliver nothing.
        let me = node(1);
        assert!(owns("wh_a", me, &BTreeSet::new()), "an empty member set must not stall delivery");
        assert!(owns("wh_a", me, &members(&[1])));
    }

    #[test]
    fn peer_only_views_still_elect_exactly_one_owner() {
        // What production actually looks like, and what the original tests
        // never modelled: SWIM's live set holds *peers*, so each node sees the
        // other two and never itself. `owns` must union `me` in, or an owner
        // can never be the node computing it — the bug that left every
        // clustered webhook undelivered until the harness caught it. Three
        // peer-only views must still agree on exactly one owner.
        let all = [1u8, 2, 3];
        for subscription in ["wh_a", "wh_b", "wh_c", "wh_d"] {
            let mut owners = 0;
            for me in all {
                let peers: BTreeSet<NodeId> =
                    all.iter().filter(|b| **b != me).map(|b| node(*b)).collect();
                if owns(subscription, node(me), &peers) {
                    owners += 1;
                }
            }
            assert_eq!(owners, 1, "{subscription}: peer-only views must elect exactly one owner");
        }
    }

    #[test]
    fn re_addressing_a_node_does_not_move_its_subscriptions() {
        // The whole point of task 10, driven through the real member set rather
        // than asserted about this function alone: a node moves to a new
        // address — a pod rescheduled, a port changed — and every assignment
        // must be unchanged. Before M8 the address was the hash input, so this
        // was the disruption of a node leaving and a stranger joining, for a
        // node that never went anywhere.
        let live = kimmy_cluster::Members::default();
        live.insert_for_test("10.0.0.1:7900".parse().unwrap(), node(1));
        live.insert_for_test("10.0.0.2:7900".parse().unwrap(), node(2));
        live.insert_for_test("10.0.0.3:7900".parse().unwrap(), node(3));

        let before: Vec<_> =
            (0..200).map(|i| owner(&format!("wh_{i}"), &live.node_ids())).collect();

        // node(2) is rescheduled onto a different address.
        live.remove_for_test(&"10.0.0.2:7900".parse().unwrap());
        live.insert_for_test("10.9.9.9:7911".parse().unwrap(), node(2));

        let after: Vec<_> = (0..200).map(|i| owner(&format!("wh_{i}"), &live.node_ids())).collect();

        assert_eq!(before, after, "an address change must not move a single subscription");
        assert!(
            before.iter().any(|o| *o == Some(node(2))),
            "the moved node must actually own some subscriptions, or this proves nothing"
        );
    }

    #[test]
    fn the_mapping_is_pinned() {
        // Ownership must not change under a compiler upgrade: a `DefaultHasher`
        // is explicitly not stable between Rust versions, and a rolling restart
        // that reshuffled every subscription would deliver a burst of
        // duplicates for no reason. Pinned to catch an accidental swap.
        //
        // Cross-checked against an independent FNV-1a implementation over
        // `"wh_a" || 0x00 || "01010101-0101-0101-0101-010101010101"` rather than
        // recorded from this one, so the value pins the algorithm — and the
        // *representation* of a `NodeId`, which is the hyphenated string form
        // fixed in `kimmy_core::ids` rather than whatever a format would pick.
        assert_eq!(hash("wh_a", &node(1)), 0xd7f5_9b66_b282_926a);
    }
}
