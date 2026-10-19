//! Whether this member may take an `Idempotency-Key`: the operator's switch,
//! `cluster.idempotency_keys`, and the live-peer net over
//! [`crate::Facts::idempotency_records`].
//!
//! A keyed write is safe only where every member that can relay it carries its
//! record with it. A member of an older release serves every origin's entries
//! from its own oplog without the record, and a member that pulls a keyed write
//! through it skips the entry in every later window, so it would hold the
//! write's effect without the record for good, and apply a retry on top of it.
//! The API asks this predicate before it looks a key up (a layer over the route
//! table, and the MCP write tools); `kimmy-api` has no view of membership, so
//! the predicate is injected the way the `Decimal128` gate's is
//! ([`crate::decimal128`]). It answers yes only when
//!
//! 1. the switch is set, which states that every member runs 0.52 and none will
//!    run an older release again; and
//! 2. with clustering on, every peer SWIM lists live advertises
//!    `Facts.idempotency_records = Some(true)`. A live peer not heard from yet
//!    fails closed. A peer that is down, or migrating its store (it binds no
//!    socket until it has), is not live and is not asked, which is the hazard
//!    the operator's procedure covers: the net only catches what this member
//!    can see.
//!
//! The net's two causes are told apart ([`crate::PeerNet`]), since a client
//! acts on them differently: a held block that says the peer does not carry
//! records ([`IdempotencyClosed::Peers`], `400`) lasts a roll, while a live peer
//! not heard from yet, with no held block saying no
//! ([`IdempotencyClosed::Unheard`], `503`, `retry: wait`, `Retry-After: 1`),
//! is heard by an immediate exchange or at its next contact: after any restart, and after
//! a SWIM rename or rejoin drops the block.
//!
//! With clustering off there are no peers, and the switch alone decides (its
//! default is `true` there). With clustering on and `cluster.membership` off,
//! this member exchanges no blocks with its peers and cannot see whether they
//! carry records, so a key is refused whatever the switch says.
//!
//! The gate governs **local acceptance only**: records, and keyed writes from
//! other members, are always stored and applied.

use std::fmt;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use crate::facts::PeerNet;
use crate::membership::Members;

/// The question the API asks before it takes an `Idempotency-Key`: is this
/// member allowed to now? Called on every keyed request, so the node's answer is
/// one atomic load of the membership view and no lock.
pub type IdempotencyPredicate = Arc<dyn Fn() -> Result<(), IdempotencyClosed> + Send + Sync>;

/// Why the gate is closed. Its `Display` is the refusal a client reads: it names
/// the header and the cause, the switch when it is off and otherwise the live
/// member that does not carry records, or that has not been heard from yet.
///
/// Every cause but one lasts until the operator or the roll changes something,
/// and the API answers it `400`. [`Self::Unheard`] ends by itself, within about
/// a contact, and is answered `503` with `retry: wait` and the
/// `Retry-After` it carries ([`Self::retry_after_secs`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdempotencyClosed {
    /// `cluster.idempotency_keys` is false: the operator has not declared that
    /// every member runs 0.52.
    Undeclared,
    /// The switch is set, and these live peers (node ids, sorted) do not say
    /// they carry records: a held block says otherwise or nothing of it (a build
    /// older than 0.52), their last block did not decode, or no block of theirs
    /// has been held for more than a lease since they came up, were renamed or
    /// this member rejoined. It lasts until they change.
    Peers(Vec<String>),
    /// The switch is set, and this member cannot see what its peers say about
    /// themselves, for the reason given.
    Unseen(&'static str),
    /// The switch is set and no live peer refuses, but these live peers (node
    /// ids, sorted) have not been heard from yet, within a lease of coming up,
    /// being renamed or this member's rejoin. **Transient**: a block arrives
    /// by an immediate exchange or the next contact, about
    /// `retry_after_secs` (a fixed second) later, and once the lease has run with none the peer counts among
    /// [`Self::Peers`].
    Unheard { peers: Vec<String>, retry_after_secs: u64 },
}

impl IdempotencyClosed {
    /// The seconds to wait before sending the request again, for the one cause
    /// that ends by itself ([`Self::Unheard`]); `None` for a refusal that lasts.
    pub fn retry_after_secs(&self) -> Option<u64> {
        match self {
            Self::Unheard { retry_after_secs, .. } => Some(*retry_after_secs),
            Self::Undeclared | Self::Peers(_) | Self::Unseen(_) => None,
        }
    }
}

impl fmt::Display for IdempotencyClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const WHAT: &str = "this member does not take an Idempotency-Key";
        match self {
            Self::Undeclared => write!(
                f,
                "{WHAT} until cluster.idempotency_keys is set. Set it only once every member \
                 runs 0.52 or later, as a configuration change of its own and never in the \
                 change that upgrades the binary: a member of an older release relays a keyed \
                 write without its record, and a retry could then apply it twice. Send the \
                 request without the header, or wait for the operator"
            ),
            Self::Peers(peers) => write!(
                f,
                "{WHAT} while a live member does not carry idempotency records: \
                 cluster.idempotency_keys is set, but the live member(s) {} do not say they do (a \
                 build older than 0.52, a block this member cannot read, or no block for longer \
                 than a lease); it is taken again once every live member does",
                peers.join(", ")
            ),
            Self::Unseen(why) => {
                write!(f, "{WHAT}: cluster.idempotency_keys is set, but {why}")
            }
            Self::Unheard { peers, .. } => write!(
                f,
                "{WHAT} yet: cluster.idempotency_keys is set, but this member has not yet heard \
                 from the live member(s) {} whether they carry idempotency records (a member \
                 that came up or rejoined a moment ago; its facts arrive by an immediate \
                 exchange, or the next replication contact). This is transient: nothing was looked \
                 up or written; send the same request again, with the same key, after \
                 Retry-After",
                peers.join(", ")
            ),
        }
    }
}

/// The switch and the membership view the predicate reads. Built at the node's
/// start, before anything serves; the member set is handed over once the
/// cluster is up.
pub struct IdempotencyGate {
    switch: bool,
    clustered: bool,
    membership: bool,
    members: OnceLock<Members>,
}

impl IdempotencyGate {
    /// `switch` is `cluster.idempotency_keys` as configured (its default
    /// already applied), `clustered` is `cluster.enabled` and `membership` is
    /// `cluster.membership`.
    pub fn new(switch: bool, clustered: bool, membership: bool) -> Arc<Self> {
        Arc::new(Self { switch, clustered, membership, members: OnceLock::new() })
    }

    /// The live member set, once the cluster is up. Once: a second call is
    /// ignored.
    pub fn set_members(&self, members: Members) {
        let _ = self.members.set(members);
    }

    /// Whether this member may take a key now, and if not, why. The switch is
    /// asked first, so a refusal names it whenever it is off, whatever the
    /// peers say.
    pub fn check(&self) -> Result<(), IdempotencyClosed> {
        self.check_at(Instant::now())
    }

    /// [`Self::check`] as of `now`, which decides whether a live peer not heard
    /// from is still within its lease.
    pub fn check_at(&self, now: Instant) -> Result<(), IdempotencyClosed> {
        if !self.switch {
            return Err(IdempotencyClosed::Undeclared);
        }
        if !self.clustered {
            return Ok(());
        }
        if !self.membership {
            return Err(IdempotencyClosed::Unseen(
                "cluster.membership is off, so this member hears nothing its peers say about \
                 themselves and cannot see that every one of them carries idempotency records",
            ));
        }
        let Some(members) = self.members.get() else {
            return Err(IdempotencyClosed::Unseen(
                "this member's membership has not started yet, so it cannot see that every \
                 peer carries idempotency records",
            ));
        };
        let names =
            |peers: Vec<kimmy_core::NodeId>| peers.iter().map(ToString::to_string).collect();
        match crate::facts::idempotency_records_net(&members.view(), now) {
            PeerNet::Open => Ok(()),
            PeerNet::Refused(peers) => Err(IdempotencyClosed::Peers(names(peers))),
            PeerNet::Unheard(peers) => Err(IdempotencyClosed::Unheard {
                peers: names(peers),
                retry_after_secs: crate::membership::UNHEARD_RETRY_AFTER_SECS,
            }),
        }
    }

    /// The predicate to install on the API state
    /// (`kimmy_api::AppState::set_idempotency_gate`). It reads the gate live,
    /// not a copy taken at install.
    pub fn predicate(self: &Arc<Self>) -> IdempotencyPredicate {
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
    use std::time::Duration;

    fn node(n: u8) -> NodeId {
        NodeId::from_bytes([n; 16])
    }

    fn addr(n: u8) -> SocketAddr {
        format!("10.0.0.{n}:7900").parse().unwrap()
    }

    fn carries(value: Option<bool>) -> Facts {
        Facts { boot: vec![1; 16], idempotency_records: value, ..Facts::default() }
    }

    fn gate_over(members: &Members) -> Arc<IdempotencyGate> {
        let gate = IdempotencyGate::new(true, true, true);
        gate.set_members(members.clone());
        gate
    }

    #[test]
    fn the_switch_decides_first_and_is_named() {
        let members = Members::default();
        let gate = IdempotencyGate::new(false, true, true);
        gate.set_members(members.clone());
        // Even with a peer that would block: the switch is the cause named.
        members.insert_for_test(addr(2), node(2));
        assert_eq!(gate.check(), Err(IdempotencyClosed::Undeclared), "a switch that is off");
        let text = IdempotencyClosed::Undeclared.to_string();
        assert!(text.contains("Idempotency-Key"), "names the header: {text}");
        assert!(text.contains("cluster.idempotency_keys"), "names the switch: {text}");
        // Clustering off: the switch alone, and no peer to ask.
        assert_eq!(IdempotencyGate::new(true, false, false).check(), Ok(()));
        assert_eq!(
            IdempotencyGate::new(false, false, false).check(),
            Err(IdempotencyClosed::Undeclared)
        );
    }

    #[test]
    fn the_net_refuses_with_an_older_or_unknown_live_peer_and_accepts_without() {
        let members = Members::default();
        let gate = gate_over(&members);
        assert_eq!(gate.check(), Ok(()), "a member alone relays nothing without records");

        members.insert_for_test(addr(2), node(2));
        // Live and not yet heard from: fails closed, and transient.
        assert_eq!(
            gate.check(),
            Err(IdempotencyClosed::Unheard {
                peers: vec![node(2).to_string()],
                retry_after_secs: crate::membership::UNHEARD_RETRY_AFTER_SECS,
            })
        );
        // An older build: its block says nothing of records.
        members.record_peer_facts_for_test(node(2), carries(None), Duration::ZERO);
        assert_eq!(gate.check(), Err(IdempotencyClosed::Peers(vec![node(2).to_string()])));
        members.record_peer_facts_for_test(node(2), carries(Some(false)), Duration::ZERO);
        assert!(gate.check().is_err(), "anything but Some(true) is does not carry");
        // A block that says decimal128 but not records is still an older peer here.
        let decimal_only = Facts { decimal128: Some(true), ..carries(None) };
        members.record_peer_facts_for_test(node(2), decimal_only, Duration::ZERO);
        assert!(gate.check().is_err(), "another capability is not this one");
        members.record_peer_facts_for_test(node(2), carries(Some(true)), Duration::ZERO);
        assert_eq!(gate.check(), Ok(()));

        // A second live peer, older: it alone is named, and by its node id.
        members.insert_for_test(addr(3), node(3));
        members.record_peer_facts_for_test(node(3), carries(None), Duration::ZERO);
        let closed = gate.check().unwrap_err();
        assert_eq!(closed, IdempotencyClosed::Peers(vec![node(3).to_string()]));
        let text = closed.to_string();
        assert!(text.contains(&node(3).to_string()), "names the member: {text}");
        assert!(!text.contains(&node(2).to_string()), "and only that one: {text}");
        assert!(text.contains("Idempotency-Key"), "names the header: {text}");
    }

    /// The net's two causes apart. A live peer not heard from is `Unheard`,
    /// transient, waiting a fixed second and saying so; a held block that says no is `Peers`, which
    /// carries no wait; and with both at once the block that says no decides,
    /// naming only its member.
    #[test]
    fn a_peer_not_heard_from_is_transient_and_a_peer_that_says_no_decides() {
        let members = Members::default();
        members.configure_lease(Duration::from_secs(2), 3);
        let gate = gate_over(&members);
        members.insert_for_test(addr(2), node(2));
        let closed = gate.check().unwrap_err();
        assert_eq!(
            closed,
            IdempotencyClosed::Unheard { peers: vec![node(2).to_string()], retry_after_secs: 1 }
        );
        assert_eq!(closed.retry_after_secs(), Some(1), "a fixed second");
        let text = closed.to_string();
        for said in ["Idempotency-Key", "not yet heard", "transient", "same key", "Retry-After"] {
            assert!(text.contains(said), "says {said:?}: {text}");
        }
        assert!(text.contains(&node(2).to_string()), "names the member: {text}");

        // A second live peer whose block says no: it decides, whoever is unheard.
        members.insert_for_test(addr(3), node(3));
        members.record_peer_facts_for_test(node(3), carries(None), Duration::ZERO);
        let closed = gate.check().unwrap_err();
        assert_eq!(closed, IdempotencyClosed::Peers(vec![node(3).to_string()]));
        assert_eq!(closed.retry_after_secs(), None, "lasts until it is upgraded");
        let text = closed.to_string();
        assert!(!text.contains("heard") && !text.contains("transient"), "{text}");

        // It upgrades: only the unheard peer is left, and the refusal is transient.
        members.record_peer_facts_for_test(node(3), carries(Some(true)), Duration::ZERO);
        assert!(matches!(gate.check(), Err(IdempotencyClosed::Unheard { .. })));
        members.record_peer_facts_for_test(node(2), carries(Some(true)), Duration::ZERO);
        assert_eq!(gate.check(), Ok(()));
    }

    /// The wait is a fixed second, whatever the sync interval.
    #[test]
    fn the_wait_is_a_fixed_second_whatever_the_sync_interval() {
        let members = Members::default();
        let gate = gate_over(&members);
        members.insert_for_test(addr(2), node(2));
        for interval in [None, Some(200), Some(1_500), Some(5_000)] {
            if let Some(ms) = interval {
                members.configure_lease(Duration::from_millis(ms), 3);
            }
            let closed = gate.check().unwrap_err();
            assert_eq!(closed.retry_after_secs(), Some(1), "{interval:?}");
        }
    }

    /// A block dropped while the peer stays live (its SWIM rename, or this
    /// member's own rejoin) makes the refusal `Unheard` again, never `Peers`:
    /// what the restart window after a roll's restart was (the gate window).
    #[test]
    fn a_block_dropped_on_a_rename_or_a_rejoin_is_unheard_again() {
        let members = Members::default();
        let gate = gate_over(&members);
        members.insert_for_test(addr(2), node(2));
        members.insert_for_test(addr(3), node(3));
        for peer in [node(2), node(3)] {
            members.record_peer_facts_for_test(peer, carries(Some(true)), Duration::ZERO);
        }
        assert_eq!(gate.check(), Ok(()));

        members.forget_peer_for_test(node(2));
        let unheard = |peers: Vec<NodeId>| {
            Err(IdempotencyClosed::Unheard {
                peers: peers.iter().map(ToString::to_string).collect(),
                retry_after_secs: crate::membership::UNHEARD_RETRY_AFTER_SECS,
            })
        };
        assert_eq!(gate.check(), unheard(vec![node(2)]), "a rename");
        members.record_peer_facts_for_test(node(2), carries(Some(true)), Duration::ZERO);
        assert_eq!(gate.check(), Ok(()), "heard again");

        members.rejoin_for_test();
        assert_eq!(gate.check(), unheard(vec![node(2), node(3)]), "this member's rejoin");
    }

    /// "Not heard" lasts a lease, the unheard-peer WARN's, on its clock: within
    /// it the refusal is the transient `Unheard`; once it has run with no block
    /// the peer counts as one that does not carry records, `Peers`, which carries
    /// no wait. Never a `503` for longer than the lease.
    #[test]
    fn a_peer_not_heard_from_is_unheard_within_its_lease_and_refuses_after_it() {
        let members = Members::default();
        members.configure_lease(Duration::from_secs(2), 3);
        let gate = gate_over(&members);
        members.insert_for_test(addr(2), node(2));
        let view = members.view();
        let since = view.unheard_since[&node(2)];
        let within = since + view.unheard_lease;
        assert!(matches!(gate.check_at(within), Err(IdempotencyClosed::Unheard { .. })));
        let after = within + Duration::from_millis(1);
        let closed = gate.check_at(after).unwrap_err();
        assert_eq!(closed, IdempotencyClosed::Peers(vec![node(2).to_string()]));
        assert_eq!(closed.retry_after_secs(), None, "no Retry-After past the lease");
        assert_eq!(members.unheard_beyond_lease(after), vec![node(2)], "as the WARN names it");
        // Heard: its block decides.
        members.record_peer_facts_for_test(node(2), carries(Some(true)), Duration::ZERO);
        assert_eq!(gate.check_at(after), Ok(()));
    }

    /// A block that does not decode was heard and cannot vouch for records: the
    /// refusal is `Peers` at once, inside the lease.
    #[test]
    fn a_peer_whose_block_does_not_decode_refuses_at_once() {
        let members = Members::default();
        let gate = gate_over(&members);
        members.insert_for_test(addr(2), node(2));
        assert!(matches!(gate.check(), Err(IdempotencyClosed::Unheard { .. })));
        members.note_undecodable_for_test(node(2));
        assert_eq!(gate.check(), Err(IdempotencyClosed::Peers(vec![node(2).to_string()])));
        members.record_peer_facts_for_test(node(2), carries(Some(true)), Duration::ZERO);
        assert_eq!(gate.check(), Ok(()), "a block that decodes decides");
    }

    #[test]
    fn a_peer_that_is_not_live_is_not_asked() {
        // Down, or migrating its store: not in the live set, so it neither blocks
        // nor counts. The operator's procedure is what covers it.
        let members = Members::default();
        let gate = gate_over(&members);
        members.insert_for_test(addr(2), node(2));
        members.record_peer_facts_for_test(node(2), carries(None), Duration::ZERO);
        assert!(gate.check().is_err());
        members.remove_for_test(&addr(2));
        assert_eq!(gate.check(), Ok(()), "a peer SWIM declared down is not asked");
    }

    #[test]
    fn without_a_membership_view_the_net_fails_closed() {
        let gate = IdempotencyGate::new(true, true, true);
        assert!(matches!(gate.check(), Err(IdempotencyClosed::Unseen(_))), "not started yet");
        let gate = IdempotencyGate::new(true, true, false);
        gate.set_members(Members::default());
        let closed = gate.check().unwrap_err();
        assert!(matches!(closed, IdempotencyClosed::Unseen(_)), "membership off");
        assert!(closed.to_string().contains("cluster.membership"), "{closed}");
    }

    #[test]
    fn the_predicate_answers_as_the_gate_does_and_reads_it_live() {
        let members = Members::default();
        let gate = gate_over(&members);
        let predicate = gate.predicate();
        assert_eq!(predicate(), Ok(()));
        members.insert_for_test(addr(2), node(2));
        assert!(predicate().is_err(), "read live, not captured at install");
        members.record_peer_facts_for_test(node(2), carries(Some(true)), Duration::ZERO);
        assert_eq!(predicate(), Ok(()));
    }
}
