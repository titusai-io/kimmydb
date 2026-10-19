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
//! The net's two causes are told apart ([`crate::PeerNet`]): a held block that
//! says the peer cannot hold one is [`Decimal128Closed::Peers`], which the API
//! answers `400`, and lasts a roll; a live peer not heard from yet, with no
//! held block saying no, is [`Decimal128Closed::Unheard`], answered `503` with
//! `retry: wait` and a `Retry-After` of 1, since its block
//! arrives by an immediate exchange or at the next contact.
//!
//! With clustering off there are no peers, and the key alone decides (its
//! default is `true` there). With clustering on and `cluster.membership` off,
//! this member exchanges no blocks with its peers and cannot see what they
//! can hold, so a gated creation is refused whatever the key says.

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use kimmy_core::Decimal128Closed;

use crate::facts::PeerNet;
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
        self.check_at(Instant::now())
    }

    /// [`Self::check`] as of `now`, which decides whether a live peer not heard
    /// from is still within its lease.
    pub fn check_at(&self, now: Instant) -> Result<(), Decimal128Closed> {
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
        let names =
            |peers: Vec<kimmy_core::NodeId>| peers.iter().map(ToString::to_string).collect();
        match crate::facts::decimal128_net(&members.view(), now) {
            PeerNet::Open => Ok(()),
            PeerNet::Refused(peers) => Err(Decimal128Closed::Peers(names(peers))),
            PeerNet::Unheard(peers) => Err(Decimal128Closed::Unheard {
                peers: names(peers),
                retry_after_secs: crate::membership::UNHEARD_RETRY_AFTER_SECS,
            }),
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
        // Live and not yet heard from: fails closed, and transient.
        assert_eq!(
            gate.check(),
            Err(Decimal128Closed::Unheard {
                peers: vec![node(2).to_string()],
                retry_after_secs: crate::membership::UNHEARD_RETRY_AFTER_SECS,
            })
        );
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

    /// The two causes apart: a peer not heard from is `Unheard`, transient, with
    /// a second to wait; a held block that says no is `Peers` and
    /// carries none; and with both at once the block that says no decides, naming
    /// only its member.
    #[test]
    fn a_peer_not_heard_from_is_transient_and_a_peer_that_says_no_decides() {
        let members = Members::default();
        members.configure_lease(Duration::from_secs(3), 2);
        let gate = gate_over(&members);
        members.insert_for_test(addr(2), node(2));
        let closed = gate.check().unwrap_err();
        assert_eq!(
            closed,
            Decimal128Closed::Unheard { peers: vec![node(2).to_string()], retry_after_secs: 1 }
        );
        assert_eq!(closed.retry_after_secs(), Some(1), "a fixed second");

        // A second live peer whose block says no: it decides, whoever is unheard.
        members.insert_for_test(addr(3), node(3));
        members.record_peer_facts_for_test(node(3), can(None), Duration::ZERO);
        let closed = gate.check().unwrap_err();
        assert_eq!(closed, Decimal128Closed::Peers(vec![node(3).to_string()]));
        assert_eq!(closed.retry_after_secs(), None, "lasts until it is upgraded");

        // It upgrades: only the unheard peer is left, and the refusal is transient again.
        members.record_peer_facts_for_test(node(3), can(Some(true)), Duration::ZERO);
        assert!(matches!(gate.check(), Err(Decimal128Closed::Unheard { .. })));
        members.record_peer_facts_for_test(node(2), can(Some(true)), Duration::ZERO);
        assert_eq!(gate.check(), Ok(()));
    }

    /// A block dropped on a rename or a rejoin leaves the peer live and unheard:
    /// `Unheard` again, not a refusal that lasts.
    #[test]
    fn a_block_dropped_while_the_peer_stays_live_is_unheard_again() {
        let members = Members::default();
        let gate = gate_over(&members);
        members.insert_for_test(addr(2), node(2));
        members.record_peer_facts_for_test(node(2), can(Some(true)), Duration::ZERO);
        assert_eq!(gate.check(), Ok(()));
        members.forget_peer_for_test(node(2));
        assert!(
            matches!(gate.check(), Err(Decimal128Closed::Unheard { ref peers, .. }) if peers == &[node(2).to_string()]),
            "{:?}",
            gate.check()
        );
    }

    /// "Not heard" lasts the unheard-peer WARN's lease, on its clock: `Unheard`
    /// within it, `Peers` once it has run with no block, and `Peers` at once for
    /// a block that does not decode.
    #[test]
    fn a_peer_not_heard_from_refuses_after_its_lease_and_an_undecodable_one_at_once() {
        let members = Members::default();
        members.configure_lease(Duration::from_secs(2), 3);
        let gate = gate_over(&members);
        members.insert_for_test(addr(2), node(2));
        let view = members.view();
        let within = view.unheard_since[&node(2)] + view.unheard_lease;
        assert!(matches!(gate.check_at(within), Err(Decimal128Closed::Unheard { .. })));
        let after = within + Duration::from_millis(1);
        assert_eq!(gate.check_at(after), Err(Decimal128Closed::Peers(vec![node(2).to_string()])));

        members.insert_for_test(addr(3), node(3));
        members.record_peer_facts_for_test(node(2), can(Some(true)), Duration::ZERO);
        assert!(matches!(gate.check(), Err(Decimal128Closed::Unheard { .. })));
        members.note_undecodable_for_test(node(3));
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
