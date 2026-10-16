//! Who may persist a `Decimal128` id or partial-filter bound (ADR-238): the
//! operator's declaration, `cluster.decimal128`, and the live-peer net over
//! [`crate::Facts::decimal128`].
//!
//! `kimmy-storage` asks a predicate at its three local chokepoints and has no
//! view of membership; this is the predicate. It answers yes only when
//!
//! 1. the key is set, which states that every member runs 0.51 and has finished
//!    its migration, and no member will run an older build again; and
//! 2. with clustering on, every peer SWIM lists live advertises
//!    `Facts.decimal128 = Some(true)`. A live peer not heard from yet fails
//!    closed. A peer that is down or migrating is not live and is not asked,
//!    which is the hazard the operator's procedure covers: the net only
//!    catches what this member can see.
//!
//! With clustering off there are no peers, and the key alone decides (its
//! default is `true` there). With clustering on and `cluster.membership` off,
//! this member exchanges no blocks with its peers and cannot see what they
//! can hold, so a gated creation is refused whatever the key says.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use kimmy_core::Decimal128Closed;

use crate::membership::Members;

/// How often a peer that cannot hold a `Decimal128` id is named again in the
/// WARN, while this member holds one (ADR-238).
pub const DECIMAL128_WARN_INTERVAL: Duration = Duration::from_secs(600);

/// The declaration and the membership view the predicate reads. Built at the
/// node's start, before anything serves; the member set is handed over once the
/// cluster is up.
pub struct Decimal128Gate {
    declared: bool,
    clustered: bool,
    membership: bool,
    members: OnceLock<Members>,
}

impl Decimal128Gate {
    /// `declared` is `cluster.decimal128` as configured (its default already
    /// applied), `clustered` is `cluster.enabled` and `membership` is
    /// `cluster.membership`.
    pub fn new(declared: bool, clustered: bool, membership: bool) -> Arc<Self> {
        Arc::new(Self { declared, clustered, membership, members: OnceLock::new() })
    }

    /// The live member set, once the cluster is up. Once: a second call is
    /// ignored.
    pub fn set_members(&self, members: Members) {
        let _ = self.members.set(members);
    }

    /// Whether a locally originated creation may persist a `Decimal128` now,
    /// and if not, why. One atomic load of the membership view: it is asked
    /// under the storage writer.
    pub fn check(&self) -> Result<(), Decimal128Closed> {
        if !self.declared {
            return Err(Decimal128Closed::Undeclared);
        }
        if !self.clustered {
            return Ok(());
        }
        if !self.membership {
            return Err(Decimal128Closed::Unseen(
                "cluster.membership is off, so this member hears nothing its peers say about \
                 themselves and cannot see that every one of them can hold one",
            ));
        }
        let Some(members) = self.members.get() else {
            return Err(Decimal128Closed::Unseen(
                "this member's membership has not started yet, so it cannot see that every \
                 peer can hold one",
            ));
        };
        let blockers = members.decimal128_blockers();
        if blockers.is_empty() {
            Ok(())
        } else {
            Err(Decimal128Closed::Peers(blockers.iter().map(ToString::to_string).collect()))
        }
    }

    /// The predicate to install on the engine
    /// (`kimmy_storage::Engine::set_decimal128_gate`).
    pub fn predicate(self: &Arc<Self>) -> kimmy_storage::Decimal128Predicate {
        let gate = Arc::clone(self);
        Arc::new(move || gate.check())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Facts;
    use kimmy_core::NodeId;
    use std::net::SocketAddr;
    use std::time::Instant;

    fn node(n: u8) -> NodeId {
        NodeId::from_bytes([n; 16])
    }

    fn addr(n: u8) -> SocketAddr {
        format!("10.0.0.{n}:7900").parse().unwrap()
    }

    fn can(value: Option<bool>) -> Facts {
        Facts { boot: vec![1; 16], decimal128: value, ..Facts::default() }
    }

    fn gate_over(members: &Members) -> Arc<Decimal128Gate> {
        let gate = Decimal128Gate::new(true, true, true);
        gate.set_members(members.clone());
        gate
    }

    #[test]
    fn the_key_decides_first() {
        let members = Members::default();
        let gate = Decimal128Gate::new(false, true, true);
        gate.set_members(members);
        assert_eq!(gate.check(), Err(Decimal128Closed::Undeclared), "a key that is false refuses");
        // Clustering off: the key alone, and no peer to ask.
        assert_eq!(Decimal128Gate::new(true, false, false).check(), Ok(()));
        assert_eq!(
            Decimal128Gate::new(false, false, false).check(),
            Err(Decimal128Closed::Undeclared)
        );
    }

    #[test]
    fn the_net_refuses_with_an_older_or_unknown_live_peer_and_accepts_without() {
        let members = Members::default();
        let gate = gate_over(&members);
        assert_eq!(gate.check(), Ok(()), "a member alone holds only what it can hold");

        members.insert_for_test(addr(2), node(2));
        // Live and not yet heard from: fails closed.
        assert_eq!(gate.check(), Err(Decimal128Closed::Peers(vec![node(2).to_string()])));
        // An older build: its block says nothing of the capability.
        members.record_peer_facts_for_test(node(2), can(None), Duration::ZERO);
        assert_eq!(gate.check(), Err(Decimal128Closed::Peers(vec![node(2).to_string()])));
        members.record_peer_facts_for_test(node(2), can(Some(false)), Duration::ZERO);
        assert!(gate.check().is_err(), "anything but Some(true) is cannot");
        members.record_peer_facts_for_test(node(2), can(Some(true)), Duration::ZERO);
        assert_eq!(gate.check(), Ok(()));

        // A second live peer, older: it alone is named.
        members.insert_for_test(addr(3), node(3));
        members.record_peer_facts_for_test(node(3), can(None), Duration::ZERO);
        assert_eq!(gate.check(), Err(Decimal128Closed::Peers(vec![node(3).to_string()])));
    }

    #[test]
    fn a_peer_that_is_not_live_is_not_asked() {
        // Down, or migrating its store (it binds no socket until it has): not in the
        // live set, so it neither blocks nor counts. The operator's procedure is what
        // covers it.
        let members = Members::default();
        let gate = gate_over(&members);
        members.insert_for_test(addr(2), node(2));
        members.record_peer_facts_for_test(node(2), can(None), Duration::ZERO);
        assert!(gate.check().is_err());
        members.remove_for_test(&addr(2));
        assert_eq!(gate.check(), Ok(()), "a peer SWIM declared down is not asked");
    }

    #[test]
    fn without_a_membership_view_the_net_fails_closed() {
        let gate = Decimal128Gate::new(true, true, true);
        assert!(matches!(gate.check(), Err(Decimal128Closed::Unseen(_))), "not started yet");
        let gate = Decimal128Gate::new(true, true, false);
        gate.set_members(Members::default());
        assert!(matches!(gate.check(), Err(Decimal128Closed::Unseen(_))), "membership off");
    }

    #[test]
    fn the_predicate_answers_as_the_gate_does() {
        let members = Members::default();
        let gate = gate_over(&members);
        let predicate = gate.predicate();
        assert_eq!(predicate(), Ok(()));
        members.insert_for_test(addr(2), node(2));
        assert!(predicate().is_err(), "read live, not captured at install");
    }

    #[test]
    fn the_warn_names_each_blocking_peer_once_then_every_ten_minutes() {
        let members = Members::default();
        members.insert_for_test(addr(2), node(2));
        members.insert_for_test(addr(3), node(3));
        // Node 2 runs an older build: its block says nothing of the capability.
        members.record_peer_facts_for_test(node(2), can(None), Duration::ZERO);
        members.record_peer_facts_for_test(node(3), can(Some(true)), Duration::ZERO);
        let start = Instant::now();
        assert_eq!(members.decimal128_warn_due(start), vec![node(2)], "only the peer that cannot");
        assert!(members.decimal128_warn_due(start + Duration::from_secs(1)).is_empty(), "once");
        // Ten minutes as written in the design, not as the constant says.
        let ten_minutes = Duration::from_secs(600);
        assert_eq!(DECIMAL128_WARN_INTERVAL, ten_minutes);
        for early in [300, 599] {
            assert!(
                members.decimal128_warn_due(start + Duration::from_secs(early)).is_empty(),
                "not before ten minutes: {early} s"
            );
        }
        let again = start + ten_minutes;
        assert_eq!(members.decimal128_warn_due(again), vec![node(2)], "again at ten minutes");

        // A peer that upgrades stops being named, and one that later blocks again is
        // named at once.
        members.record_peer_facts_for_test(node(2), can(Some(true)), Duration::ZERO);
        assert!(members.decimal128_warn_due(again + Duration::from_secs(1)).is_empty());
        members.record_peer_facts_for_test(node(2), can(None), Duration::ZERO);
        assert_eq!(members.decimal128_warn_due(again + Duration::from_secs(2)), vec![node(2)]);
    }

    #[test]
    fn a_peer_not_heard_from_is_named_only_after_a_lease() {
        // It blocks the gate at once, failing closed, but is named in the WARN only
        // once it has been unheard for longer than a lease, as the unheard-peer WARN
        // names one: a peer not contacted yet is not a peer that cannot hold one.
        let members = Members::default();
        let gate = gate_over(&members);
        members.insert_for_test(addr(4), node(4));
        assert!(gate.check().is_err(), "the gate fails closed at once");
        let now = Instant::now();
        assert!(members.decimal128_warn_due(now).is_empty(), "not named within its lease");
        let lease = members.lease();
        assert_eq!(
            members.decimal128_warn_due(now + lease + Duration::from_secs(1)),
            vec![node(4)]
        );
    }

    #[test]
    fn the_warn_waits_for_the_marker_and_reads_it_only_when_a_peer_blocks() {
        let members = Members::default();
        let read = std::cell::Cell::new(0);
        let marker = |present: bool| {
            let read = &read;
            move || {
                read.set(read.get() + 1);
                present
            }
        };
        let now = Instant::now();
        assert!(members.decimal128_warnings(marker(true), now).is_empty(), "no peer");
        assert_eq!(read.get(), 0, "no peer blocks, so the store is not read");

        members.insert_for_test(addr(2), node(2));
        members.record_peer_facts_for_test(node(2), can(None), Duration::ZERO);
        assert!(members.decimal128_warnings(marker(false), now).is_empty(), "no marker, no WARN");
        assert_eq!(read.get(), 1);
        assert_eq!(members.decimal128_warnings(marker(true), now), vec![node(2)]);
        assert!(
            members.decimal128_warnings(marker(true), now + Duration::from_secs(1)).is_empty(),
            "the cadence of decimal128_warn_due"
        );
    }
}
