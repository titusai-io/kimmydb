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
//! # Addresses, not node ids
//!
//! `Members` holds `SocketAddr`, and hashing those keeps this a pure function
//! of what SWIM already publishes. The cost is that re-addressing a node
//! reshuffles its subscriptions, which is the same disruption as that node
//! leaving and a new one joining — acceptable, and cheaper than maintaining an
//! address-to-node-id mapping purely to hash it.

use std::collections::BTreeSet;
use std::net::SocketAddr;

/// FNV-1a, so the mapping is stable across processes and releases.
///
/// A `DefaultHasher` is explicitly not guaranteed stable between Rust versions,
/// and an ownership function that changed under a compiler upgrade would
/// reshuffle every subscription in the cluster on a rolling restart.
fn hash(subscription: &str, member: &SocketAddr) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x100_0000_01b3;

    let mut h = OFFSET;
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
/// Ties break on the address, so two members hashing identically still produce
/// one answer rather than depending on iteration order.
pub fn owner(subscription: &str, members: &BTreeSet<SocketAddr>) -> Option<SocketAddr> {
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
pub fn owns(subscription: &str, me: SocketAddr, members: &BTreeSet<SocketAddr>) -> bool {
    let mut candidates = members.clone();
    candidates.insert(me);
    owner(subscription, &candidates) == Some(me)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn members(addrs: &[&str]) -> BTreeSet<SocketAddr> {
        addrs.iter().map(|a| a.parse().unwrap()).collect()
    }

    fn addr(a: &str) -> SocketAddr {
        a.parse().unwrap()
    }

    #[test]
    fn every_node_computes_the_same_owner() {
        // The property that makes this work without coordination: three nodes
        // holding the same member set must all name the same owner, or two of
        // them deliver and one does not.
        let set = members(&["10.0.0.1:7900", "10.0.0.2:7900", "10.0.0.3:7900"]);
        for subscription in ["wh_a", "wh_b", "wh_c", "wh_d"] {
            let answers: BTreeSet<_> = (0..5).map(|_| owner(subscription, &set).unwrap()).collect();
            assert_eq!(answers.len(), 1, "{subscription} produced {answers:?}");
        }
    }

    #[test]
    fn exactly_one_node_owns_each_subscription() {
        let set = members(&["10.0.0.1:7900", "10.0.0.2:7900", "10.0.0.3:7900"]);
        for subscription in ["wh_a", "wh_b", "wh_c"] {
            let owners: Vec<_> = set.iter().filter(|m| owns(subscription, **m, &set)).collect();
            assert_eq!(owners.len(), 1, "{subscription} owned by {owners:?}");
        }
    }

    #[test]
    fn work_spreads_across_the_cluster() {
        // If every subscription hashed to one node, that node would do all the
        // delivering and the other two would idle.
        let set = members(&["10.0.0.1:7900", "10.0.0.2:7900", "10.0.0.3:7900"]);
        let used: BTreeSet<_> = (0..200).filter_map(|i| owner(&format!("wh_{i}"), &set)).collect();
        assert_eq!(used.len(), 3, "all three should own some subscriptions, got {used:?}");
    }

    #[test]
    fn a_node_leaving_moves_only_what_it_owned() {
        // The reason for rendezvous rather than modulo. With `hash % len`,
        // removing one of three members remaps roughly two thirds of all
        // subscriptions; here it must move only the departed node's share.
        let before = members(&["10.0.0.1:7900", "10.0.0.2:7900", "10.0.0.3:7900"]);
        let gone = addr("10.0.0.3:7900");
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
        let before = members(&["10.0.0.1:7900", "10.0.0.2:7900", "10.0.0.3:7900"]);
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
        let me = addr("127.0.0.1:7900");
        assert!(owns("wh_a", me, &BTreeSet::new()), "an empty member set must not stall delivery");
        assert!(owns("wh_a", me, &members(&["127.0.0.1:7900"])));
    }

    #[test]
    fn peer_only_views_still_elect_exactly_one_owner() {
        // What production actually looks like, and what the original tests
        // never modelled: SWIM's live set holds *peers*, so each node sees the
        // other two and never itself. `owns` must union `me` in, or an owner
        // can never be the node computing it — the bug that left every
        // clustered webhook undelivered until the harness caught it. Three
        // peer-only views must still agree on exactly one owner.
        let all = ["10.0.0.1:7900", "10.0.0.2:7900", "10.0.0.3:7900"];
        for subscription in ["wh_a", "wh_b", "wh_c", "wh_d"] {
            let mut owners = 0;
            for me in all {
                let peers: BTreeSet<SocketAddr> =
                    all.iter().filter(|a| **a != me).map(|a| a.parse().unwrap()).collect();
                if owns(subscription, addr(me), &peers) {
                    owners += 1;
                }
            }
            assert_eq!(owners, 1, "{subscription}: peer-only views must elect exactly one owner");
        }
    }

    #[test]
    fn the_mapping_is_pinned() {
        // Ownership must not change under a compiler upgrade: a `DefaultHasher`
        // is explicitly not stable between Rust versions, and a rolling restart
        // that reshuffled every subscription would deliver a burst of
        // duplicates for no reason. Pinned to catch an accidental swap.
        //
        // Cross-checked against an independent FNV-1a implementation rather
        // than recorded from this one, so the value pins the algorithm and not
        // whatever this code happens to produce.
        assert_eq!(hash("wh_a", &addr("10.0.0.1:7900")), 0x7457_8f08_134a_f3a8);
    }
}
