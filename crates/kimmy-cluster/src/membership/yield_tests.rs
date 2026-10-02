//! What `Members` does with yielding (ADR-213): the effective-yield mask, the read
//! map and the echo, the order blocks are held in, the unheard-peer rule, the
//! member's own `Defunct` and `Rejoin`, the lock-free view, and the evaluator thread
//! that reads it.

use foca::Runtime;

use super::*;
use crate::facts::Yielding;
use crate::protocol::Echo;

fn n(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 16])
}

fn a(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

const ME_BOOT: u8 = 9;

/// A member set with `peers` live, a lease of 15 s at three peers, and an own block
/// whose yielding the test sets.
struct Rig {
    members: Members,
    yielding: Arc<Mutex<Yielding>>,
    extra_ttl: Arc<Mutex<bool>>,
    clock: Mutex<Instant>,
}

impl Rig {
    fn new(peers: &[u8]) -> Self {
        let members = Members::default();
        members.configure_lease(Duration::from_secs(5), 3);
        for p in peers {
            members.insert_for_test(a(7000 + u16::from(*p)), n(*p));
        }
        let yielding = Arc::new(Mutex::new(Yielding::default()));
        let extra_ttl = Arc::new(Mutex::new(false));
        members.set_facts_source({
            let (yielding, extra_ttl) = (Arc::clone(&yielding), Arc::clone(&extra_ttl));
            Arc::new(move || {
                let mut facts = Facts {
                    boot: vec![ME_BOOT; 16],
                    started_ms: Some(1_000),
                    yielding: *yielding.lock(),
                    ..Facts::default()
                };
                if *extra_ttl.lock() {
                    facts = facts.with_ttl(vec![crate::facts::TtlHeld {
                        collection: kimmy_core::CollectionId(1),
                        digest: vec![1; 8],
                    }]);
                }
                facts
            })
        });
        Self { members, yielding, extra_ttl, clock: Mutex::new(Instant::now()) }
    }

    fn later(&self, by: Duration) -> Instant {
        let mut clock = self.clock.lock();
        *clock += by;
        *clock
    }

    /// The own block, rebuilt from the source.
    fn block(&self) -> (Arc<Facts>, u64) {
        self.members.local_facts_at(self.later(Duration::from_secs(2))).unwrap()
    }

    fn set_yield(&self, webhooks: bool) {
        *self.yielding.lock() = Yielding { webhooks, ..Yielding::default() };
        self.block();
    }

    /// Whether the webhooks yield is effective, asked a lease after the live set
    /// last changed (and the block rebuilt as of then).
    fn effective(&self) -> bool {
        let at = self.later(Duration::from_secs(600));
        self.members.local_facts_effective_at(at).unwrap().yielding.webhooks
    }

    fn echo(&self, peer: u8, generation: u64) {
        let echo = Echo { boot: vec![ME_BOOT; 16], generation };
        self.members.note_echo(n(peer), Some(&echo));
    }

    fn unconfirmed(&self) -> Vec<NodeId> {
        self.members.unconfirmed_peers(OwnerClass::Webhooks)
    }
}

fn block(boot: u8) -> Facts {
    Facts { boot: vec![boot; 16], ..Facts::default() }
}

fn hear(members: &Members, peer: u8, facts: Facts, generation: Option<u64>, seq: u64) -> bool {
    members.record_peer_facts(n(peer), Arc::new(facts), generation, seq, Instant::now())
}

// ---------------------------------------------------------------------------
// The effective-yield mask and per-class confirmation.
// ---------------------------------------------------------------------------

/// Until every live peer has echoed the generation that set the bit, this
/// member keeps owning the class, though its advertised block says it yields.
#[test]
fn a_yield_is_effective_only_once_every_live_peer_has_echoed_it() {
    let rig = Rig::new(&[1, 2]);
    rig.set_yield(true);
    let (advertised, generation) = rig.block();
    assert!(advertised.yielding.webhooks, "advertised at once");
    assert!(!rig.effective(), "but not effective: nobody has echoed it");
    // The own candidacy follows the effective bits.
    let me = n(9);
    let effective =
        rig.members.local_facts_effective_at(rig.later(Duration::from_secs(1))).unwrap();
    for p in [1, 2] {
        hear(&rig.members, p, block(p), Some(1), crate::facts::next_decoded_seq());
    }
    assert!(
        rig.members.candidates(OwnerClass::Webhooks, None, me, &effective, false).contains(&me),
        "this member is still a candidate"
    );
    rig.echo(1, generation);
    assert!(!rig.effective(), "one of two peers");
    assert_eq!(rig.unconfirmed(), vec![n(2)]);
    rig.echo(2, generation);
    assert!(rig.effective(), "both echoed");
    assert!(rig.unconfirmed().is_empty());
    let effective =
        rig.members.local_facts_effective_at(rig.later(Duration::from_secs(1))).unwrap();
    assert!(
        !rig.members.candidates(OwnerClass::Webhooks, None, me, &effective, false).contains(&me),
        "and now it is not"
    );
    // Only the yielded class is masked.
    assert!(rig.members.unconfirmed_peers(OwnerClass::Ttl).is_empty());
}

/// An unrelated change to the block (a TTL index created) bumps its generation
/// and does not re-open the confirmation: a peer confirmed the generation that set
/// the bit.
#[test]
fn an_unrelated_rebuild_does_not_reopen_a_confirmation() {
    let rig = Rig::new(&[1]);
    rig.set_yield(true);
    let (_, generation) = rig.block();
    rig.echo(1, generation);
    assert!(rig.effective());
    *rig.extra_ttl.lock() = true;
    let (_, later_generation) = rig.block();
    assert!(later_generation > generation, "the block changed");
    assert!(rig.effective(), "and the yield is still confirmed");
    assert!(rig.unconfirmed().is_empty());
}

/// Clearing the bit re-owns at once, and setting it again needs a new confirmation
/// at the generation that set it.
#[test]
fn a_reclaim_re_owns_at_once_and_a_new_yield_needs_a_new_echo() {
    let rig = Rig::new(&[1]);
    rig.set_yield(true);
    let (_, first) = rig.block();
    rig.echo(1, first);
    assert!(rig.effective());
    rig.set_yield(false);
    assert!(!rig.effective(), "cleared: owns again at once");
    // A peer's echo going away changes nothing: no bit is set, so nothing is waited on.
    rig.members.note_echo(n(1), None);
    assert!(rig.unconfirmed().is_empty(), "nothing is waited on");
    rig.set_yield(true);
    let (_, second) = rig.block();
    assert!(second > first);
    assert!(!rig.effective(), "the old echo does not confirm the new yield");
    assert_eq!(rig.unconfirmed(), vec![n(1)]);
    rig.echo(1, second);
    assert!(rig.effective());
}

/// A peer that sends no echo (0.43), or one whose echo names another boot or
/// nothing, never confirms; and a peer that joined later has read nothing.
#[test]
fn a_peer_that_does_not_echo_never_confirms() {
    let rig = Rig::new(&[1, 2, 3]);
    rig.set_yield(true);
    let (_, generation) = rig.block();
    // 1 echoes the right thing; 2 sends no echo; 3 echoes another boot at a higher generation.
    rig.echo(1, generation);
    rig.members.note_echo(n(2), None);
    rig.members.note_echo(n(3), Some(&Echo { boot: vec![0x0F; 16], generation: 500 }));
    assert_eq!(rig.unconfirmed(), vec![n(2), n(3)]);
    assert!(!rig.effective());
    // A peer that comes up later has echoed nothing: the yield re-opens.
    rig.echo(2, generation);
    rig.echo(3, generation);
    assert!(rig.effective());
    rig.members.insert_for_test(a(7004), n(4));
    assert_eq!(rig.unconfirmed(), vec![n(4)]);
    assert!(!rig.effective());
}

/// An echo records the highest generation and never lowers it (a delayed older
/// frame arriving late is not a retraction), and a new boot of the peer forgets it.
#[test]
fn an_echo_keeps_the_highest_generation_and_a_new_boot_forgets_it() {
    let rig = Rig::new(&[1]);
    // The block changes before it yields, so the yield's generation is above 1.
    *rig.extra_ttl.lock() = true;
    rig.block();
    rig.set_yield(true);
    let (_, generation) = rig.block();
    assert!(generation >= 2, "the yield is not the first block");
    rig.echo(1, generation + 5);
    rig.echo(1, generation - 1);
    assert!(rig.unconfirmed().is_empty(), "the older echo, arriving late, did not lower it");
    hear(&rig.members, 1, block(1), Some(1), crate::facts::next_decoded_seq());
    hear(
        &rig.members,
        1,
        Facts { started_ms: Some(5_000), ..block(2) },
        Some(1),
        crate::facts::next_decoded_seq(),
    );
    assert_eq!(rig.unconfirmed(), vec![n(1)], "a restarted peer has read nothing");
}

/// A peer that restarted and now echoes that it holds nothing loses its confirmation
/// at once, even before its own new block has been heard (the boot change that
/// forgets echoes would otherwise be the only thing to do it).
#[test]
fn an_echo_of_nothing_withdraws_a_confirmation_without_a_new_block() {
    let rig = Rig::new(&[1]);
    rig.set_yield(true);
    let (_, generation) = rig.block();
    rig.echo(1, generation);
    assert!(rig.unconfirmed().is_empty(), "the control: it had confirmed");
    rig.members.note_echo(n(1), Some(&Echo::default()));
    assert_eq!(rig.unconfirmed(), vec![n(1)], "it now holds nothing");
    rig.echo(1, generation);
    assert!(rig.unconfirmed().is_empty(), "and confirms again when it reads the block");
    rig.members.note_echo(n(1), Some(&Echo { boot: vec![0x42; 16], generation: 99 }));
    assert_eq!(rig.unconfirmed(), vec![n(1)], "an echo of another boot confirms nothing");
}

/// The lease escape is across boots only. A held block of the
/// same boot past its lease is not replaced by a later-decoded block of a lower
/// generation.
#[test]
fn the_lease_escape_never_lowers_a_generation_within_a_boot() {
    let members = Members::default();
    members.configure_lease(Duration::from_secs(5), 3);
    members.insert_for_test(a(7001), n(1));
    let held = crate::facts::next_decoded_seq();
    let long_ago = Instant::now().checked_sub(Duration::from_secs(600)).unwrap();
    let yields = Facts { yielding: Yielding { webhooks: true, ..Yielding::default() }, ..block(1) };
    assert!(members.record_peer_facts(n(1), Arc::new(yields), Some(5), held, long_ago));
    let later = crate::facts::next_decoded_seq();
    assert!(!hear(&members, 1, block(1), Some(4), later), "same boot, older generation");
    assert!(members.view().blocks[&n(1)].facts.yielding.webhooks, "the held block stands");
    // The control: the same later block from another boot does take the stale slot.
    assert!(hear(&members, 1, block(2), Some(1), crate::facts::next_decoded_seq()));
    assert_eq!(held_boot(&members, 1), vec![2; 16]);
}

/// C9: a yield becomes effective only after the live set has held still for a lease;
/// any member up, down or renamed starts the lease again.
#[test]
fn a_yield_waits_for_the_live_set_to_hold_still_for_a_lease() {
    let rig = Rig::new(&[1]);
    rig.set_yield(true);
    let (_, generation) = rig.block();
    rig.echo(1, generation);
    let lease = rig.members.lease();
    assert_eq!(lease, Duration::from_secs(15));
    // Just after the change: still waiting.
    let just_after = Instant::now() + Duration::from_secs(5);
    assert!(
        !rig.members.local_facts_effective_at(just_after).unwrap().yielding.webhooks,
        "five seconds after the live set changed"
    );
    let a_lease_on = Instant::now() + lease + Duration::from_secs(1);
    assert!(rig.members.local_facts_effective_at(a_lease_on).unwrap().yielding.webhooks);
    // A member coming up restarts the lease, whatever it has echoed.
    rig.members.insert_for_test(a(7002), n(2));
    rig.echo(2, generation);
    let two_leases_less = Instant::now() + Duration::from_secs(5);
    assert!(!rig.members.local_facts_effective_at(two_leases_less).unwrap().yielding.webhooks);
    let after = Instant::now() + rig.members.lease() + Duration::from_secs(1);
    assert!(rig.members.local_facts_effective_at(after).unwrap().yielding.webhooks);
}

// ---------------------------------------------------------------------------
// The order blocks are held in (6.1.2 and 6.2).
// ---------------------------------------------------------------------------

fn held_boot(members: &Members, peer: u8) -> Vec<u8> {
    members.view().blocks[&n(peer)].facts.boot.clone()
}

/// A lower generation from the same boot is ignored, an equal or higher one
/// replaces, and a new boot replaces.
#[test]
fn a_lower_generation_from_the_same_boot_is_ignored_and_a_new_boot_replaces() {
    let members = Members::default();
    members.insert_for_test(a(7001), n(1));
    let seq = crate::facts::next_decoded_seq;
    let high = Facts { catching_up: true, ..block(1) };
    assert!(hear(&members, 1, high, Some(7), seq()));
    assert!(!hear(&members, 1, block(1), Some(3), seq()), "older, from the same process");
    assert!(members.view().blocks[&n(1)].facts.catching_up, "the later generation stands");
    assert!(hear(&members, 1, block(1), Some(7), seq()), "an equal one refreshes");
    assert!(!members.view().blocks[&n(1)].facts.catching_up);
    // A 0.43 sender has no generation: it overwrites, as it always did.
    assert!(hear(&members, 1, Facts { catching_up: true, ..block(1) }, None, seq()));
    // A new boot replaces whatever generation it carries.
    assert!(hear(&members, 1, block(2), Some(1), seq()));
    assert_eq!(held_boot(&members, 1), vec![2; 16]);
}

/// A block decoded earlier never replaces one decoded later, whatever its boot or
/// generation, and an ignored block does not refresh `received`.
#[test]
fn an_earlier_decoded_block_never_replaces_a_later_one() {
    let members = Members::default();
    members.insert_for_test(a(7001), n(1));
    let early = crate::facts::next_decoded_seq();
    let late = crate::facts::next_decoded_seq();
    let long_ago = Instant::now().checked_sub(Duration::from_secs(5)).unwrap();
    assert!(members.record_peer_facts(
        n(1),
        Arc::new(Facts { catching_up: true, ..block(1) }),
        Some(2),
        late,
        long_ago
    ));
    // Same boot, higher generation, decoded earlier.
    assert!(!hear(&members, 1, block(1), Some(9), early));
    // Another boot, decoded earlier.
    assert!(!hear(&members, 1, Facts { started_ms: Some(9_999_999), ..block(2) }, Some(1), early));
    let view = members.view();
    assert!(view.blocks[&n(1)].facts.catching_up);
    assert_eq!(view.blocks[&n(1)].received, long_ago, "ignored blocks refresh nothing");
}

/// Across boots the sender's own start time orders them: a dead process's frame
/// decoded last has a lower `started_ms` and is ignored; a later start wins; a
/// missing `started_ms` skips that half.
#[test]
fn across_boots_the_sender_start_time_orders_them() {
    let members = Members::default();
    members.insert_for_test(a(7001), n(1));
    let seq = crate::facts::next_decoded_seq;
    let new = Facts { started_ms: Some(2_000), ..block(2) };
    assert!(hear(&members, 1, new, Some(1), seq()));
    // The old process's frame, decoded after the new one's, from before it started.
    assert!(!hear(&members, 1, Facts { started_ms: Some(1_000), ..block(1) }, Some(99), seq()));
    assert_eq!(held_boot(&members, 1), vec![2; 16]);
    // A later process replaces it.
    assert!(hear(&members, 1, Facts { started_ms: Some(3_000), ..block(3) }, Some(1), seq()));
    assert_eq!(held_boot(&members, 1), vec![3; 16]);
    // A 0.43 sender has no start time.
    assert!(hear(&members, 1, block(4), None, seq()));
    assert_eq!(held_boot(&members, 1), vec![4; 16]);
}

/// 6.2: a held, confirmed yielding block aged past its lease is not replaced by an
/// earlier-decoded pre-yield block of the same boot, while a later-decoded
/// block replaces it even from a clock that stepped back (the escape).
#[test]
fn a_stale_held_block_is_not_replaced_by_an_earlier_decoded_one() {
    let members = Members::default();
    members.configure_lease(Duration::from_secs(5), 3);
    members.insert_for_test(a(7001), n(1));
    let pre = crate::facts::next_decoded_seq();
    let yielding = crate::facts::next_decoded_seq();
    let long_ago = Instant::now().checked_sub(Duration::from_secs(600)).unwrap();
    let yields = Facts { yielding: Yielding { webhooks: true, ..Yielding::default() }, ..block(1) };
    assert!(members.record_peer_facts(n(1), Arc::new(yields), Some(5), yielding, long_ago));
    // The held block is far past its lease. The old contact's block arrives.
    assert!(
        !hear(&members, 1, block(1), Some(4), pre),
        "same boot, earlier decoded, lower generation"
    );
    assert!(
        !hear(&members, 1, block(1), Some(9), pre),
        "same boot, earlier decoded, higher generation"
    );
    assert!(!hear(&members, 1, Facts { started_ms: Some(1), ..block(2) }, Some(1), pre));
    assert!(members.view().blocks[&n(1)].facts.yielding.webhooks, "the yielding block stays");
    // A later-decoded block of a process whose clock stepped back replaces the stale one.
    let old = Facts { started_ms: Some(1), ..block(7) };
    assert!(hear(&members, 1, old, Some(1), crate::facts::next_decoded_seq()), "the escape");
    assert_eq!(held_boot(&members, 1), vec![7; 16]);
}

/// A slot dropped on SWIM down, a rename or this member's own `Defunct` refuses any
/// block decoded before the drop.
#[test]
fn a_dropped_slot_refuses_a_block_decoded_before_it() {
    let members = Members::default();
    members.insert_for_test(a(7001), n(1));
    members.insert_for_test(a(7002), n(2));
    let early = crate::facts::next_decoded_seq();
    assert!(hear(&members, 1, block(1), Some(1), crate::facts::next_decoded_seq()));
    members.remove_for_test(&a(7001));
    members.insert_for_test(a(7001), n(1));
    assert!(
        !hear(&members, 1, block(1), Some(1), early),
        "decoded before the member was declared down"
    );
    assert!(hear(&members, 1, block(1), Some(1), crate::facts::next_decoded_seq()));
    // This member's own Defunct drops every slot, one never heard included.
    let before_defunct = crate::facts::next_decoded_seq();
    let mut collector =
        Collector { outgoing: Vec::new(), timers: Vec::new(), members: members.clone() };
    collector.notify(Notification::Defunct);
    assert!(!hear(&members, 1, block(1), Some(2), before_defunct));
    assert!(!hear(&members, 2, block(2), Some(2), before_defunct));
    assert!(hear(&members, 2, block(2), Some(2), crate::facts::next_decoded_seq()));
}

/// Decision 7: a member that has seen no change in its live set counts the lease
/// from the first time ownership asks, not from a moment that has already passed.
#[test]
fn the_stable_timer_of_a_member_with_no_change_starts_at_the_first_question() {
    let rig = Rig::new(&[]);
    rig.set_yield(true);
    let lease = rig.members.lease();
    let first = Instant::now() + Duration::from_secs(1000);
    assert!(!rig.members.local_facts_effective_at(first).unwrap().yielding.webhooks);
    let almost = first + lease - Duration::from_millis(1);
    assert!(!rig.members.local_facts_effective_at(almost).unwrap().yielding.webhooks);
    let after = first + lease;
    assert!(rig.members.local_facts_effective_at(after).unwrap().yielding.webhooks);
}

/// The drop floors are pruned. One is gone once its slot holds a
/// later block, and one for a peer that left and has been quiet for ten leases is
/// forgotten; a floor that still protects a live, silent slot stays.
#[test]
fn the_drop_floors_are_pruned() {
    // Alone in the process the sequence is still zero, where a floor protects nothing.
    let _ = crate::facts::next_decoded_seq();
    let members = Members::default();
    members.configure_lease(Duration::from_secs(5), 3);
    members.insert_for_test(a(7001), n(1));
    members.insert_for_test(a(7002), n(2));
    members.insert_for_test(a(7003), n(3));
    for p in [1, 2, 3] {
        members.remove_for_test(&a(7000 + u16::from(p)));
        members.insert_for_test(a(7000 + u16::from(p)), n(p));
    }
    assert_eq!(members.drop_floors_for_test(), 3, "one per dropped slot");
    // Peer 1 speaks again: its floor is redundant now.
    assert!(hear(&members, 1, block(1), Some(1), crate::facts::next_decoded_seq()));
    assert_eq!(members.drop_floors_for_test(), 2, "the slot that holds a later block");
    // Peer 3 leaves for good; ten leases on, hearing from a live peer prunes it.
    members.remove_for_test(&a(7003));
    let lease = members.lease();
    let later = Instant::now() + lease * 11;
    assert!(members.record_peer_facts(
        n(1),
        Arc::new(block(1)),
        Some(2),
        crate::facts::next_decoded_seq(),
        later
    ));
    assert_eq!(members.drop_floors_for_test(), 1, "only the live, silent peer's floor stays");
    assert!(
        !hear(&members, 2, block(2), Some(1), 1),
        "and it still refuses what was decoded before the drop"
    );
    // This member's own Defunct sets a floor above every drop floor, which then
    // say nothing more: they are pruned at the next block.
    let mut collector =
        Collector { outgoing: Vec::new(), timers: Vec::new(), members: members.clone() };
    collector.notify(Notification::Defunct);
    assert!(hear(&members, 1, block(1), Some(3), crate::facts::next_decoded_seq()));
    assert_eq!(members.drop_floors_for_test(), 0, "covered by the global floor");
}

// ---------------------------------------------------------------------------
// A2: own Defunct and Rejoin; unheard peers.
// ---------------------------------------------------------------------------

/// After its own `Defunct` or `Rejoin` a member's read map and its peer blocks are
/// empty and it owns again.
#[test]
fn own_defunct_and_rejoin_clear_the_read_map_and_the_peer_blocks() {
    for rejoin in [false, true] {
        let rig = Rig::new(&[1, 2]);
        rig.set_yield(true);
        let (_, generation) = rig.block();
        for p in [1, 2] {
            hear(&rig.members, p, block(p), Some(1), crate::facts::next_decoded_seq());
            rig.echo(p, generation);
        }
        assert!(rig.effective(), "confirmed by both");
        let mut collector =
            Collector { outgoing: Vec::new(), timers: Vec::new(), members: rig.members.clone() };
        let identity = Member::identified(a(7900), n(9));
        if rejoin {
            collector.notify(Notification::Rejoin(&identity));
        } else {
            collector.notify(Notification::Defunct);
        }
        assert_eq!(rig.unconfirmed(), vec![n(1), n(2)], "rejoin={rejoin}: the read map is empty");
        assert!(!rig.effective(), "rejoin={rejoin}: it owns again");
        let states = rig.members.peer_states();
        assert!(
            states.values().all(|state| *state == PeerState::Unknown),
            "rejoin={rejoin}: every peer block is dropped: {states:?}"
        );
        assert!(rig.members.view().blocks.is_empty());
    }
}

/// A peer that is not heard from is not a candidate for webhooks or embeddings,
/// and the fallback pass does not add it either; heard again, it is.
#[test]
fn an_unheard_peer_is_not_a_candidate_even_in_the_fallback() {
    let members = Members::default();
    members.insert_for_test(a(7001), n(1));
    let catching = Facts { catching_up: true, ..block(9) };
    for class in [OwnerClass::Webhooks, OwnerClass::Embeddings] {
        let set = members.candidates(class, None, n(9), &catching, false);
        assert!(set.is_empty(), "{class:?}: me is catching up and the peer is unheard: {set:?}");
    }
    let me = block(9);
    for class in [OwnerClass::Webhooks, OwnerClass::Embeddings] {
        let set = members.candidates(class, None, n(9), &me, false);
        assert_eq!(set, BTreeSet::from([n(9)]), "{class:?}");
    }
    // A peer that yields, with me yielding as well: the fallback gives the work to
    // the heard one and never to the unheard one.
    members.insert_for_test(a(7002), n(2));
    let yields = Facts { yielding: Yielding { webhooks: true, ..Yielding::default() }, ..block(2) };
    hear(&members, 2, yields.clone(), Some(1), crate::facts::next_decoded_seq());
    let mine = Facts { yielding: Yielding { webhooks: true, ..Yielding::default() }, ..block(9) };
    let set = members.candidates(OwnerClass::Webhooks, None, n(9), &mine, false);
    assert_eq!(set, BTreeSet::from([n(2), n(9)]), "the fallback ignores yielding, not hearing");
    hear(&members, 1, block(1), Some(1), crate::facts::next_decoded_seq());
    let set = members.candidates(OwnerClass::Webhooks, None, n(9), &me, false);
    assert!(set.contains(&n(1)));
}

/// A peer SWIM brings down and up again is unheard at once, so it is not a
/// candidate until a block arrives.
#[test]
fn a_peer_declared_down_and_up_is_unheard_until_a_block_arrives() {
    let members = Members::default();
    members.insert_for_test(a(7001), n(1));
    hear(&members, 1, block(1), Some(1), crate::facts::next_decoded_seq());
    let me = block(9);
    assert!(members.candidates(OwnerClass::Webhooks, None, n(9), &me, false).contains(&n(1)));
    members.remove_for_test(&a(7001));
    members.insert_for_test(a(7001), n(1));
    assert!(!members.candidates(OwnerClass::Webhooks, None, n(9), &me, false).contains(&n(1)));
    hear(&members, 1, block(1), Some(1), crate::facts::next_decoded_seq());
    assert!(members.candidates(OwnerClass::Webhooks, None, n(9), &me, false).contains(&n(1)));
}

/// A live peer that has sent nothing for more than a lease is named, once per
/// interval.
#[test]
fn a_peer_unheard_for_more_than_a_lease_is_named() {
    let members = Members::default();
    members.configure_lease(Duration::from_secs(5), 3);
    members.insert_for_test(a(7001), n(1));
    members.insert_for_test(a(7002), n(2));
    hear(&members, 2, block(2), Some(1), crate::facts::next_decoded_seq());
    assert!(members.unheard_beyond_lease(Instant::now()).is_empty(), "not yet a lease");
    let later = Instant::now() + Duration::from_secs(60);
    assert_eq!(members.unheard_beyond_lease(later), vec![n(1)]);
    assert!(members.note_unheard(n(1), later));
    assert!(!members.note_unheard(n(1), later + Duration::from_secs(10)));
}

/// The record of who was named does not outlive the peer: one that has left, and
/// was named longer ago than the interval, is dropped the next time anyone is named.
#[test]
fn the_record_of_named_peers_is_pruned_to_live_ones_and_recent_ones() {
    let members = Members::default();
    members.configure_lease(Duration::from_secs(5), 3);
    members.insert_for_test(a(7001), n(1));
    members.insert_for_test(a(7002), n(2));
    let t0 = Instant::now();
    assert!(members.note_unheard(n(1), t0));
    assert!(members.note_unheard(n(9), t0), "a peer that is not live is named too");
    assert_eq!(members.0.unheard_said.lock().len(), 2);
    // Inside the interval nothing is dropped, live or not.
    assert!(members.note_unheard(n(2), t0 + Duration::from_secs(10)));
    assert_eq!(members.0.unheard_said.lock().len(), 3);
    // Past it, the peer that is not live goes, and the live ones stay.
    assert!(members.note_unheard(n(2), t0 + crate::health::WARN_INTERVAL * 2));
    let kept: Vec<NodeId> = members.0.unheard_said.lock().keys().copied().collect();
    assert_eq!(kept, vec![n(1), n(2)], "{kept:?}");
}

/// A peer that confirmed, and then sends a frame with no echo (a 0.43 sender, or an
/// echo that did not decode) on the same boot and no new block, has stopped
/// confirming: the yield is unconfirmed again and the member owns the class.
#[test]
fn a_peer_that_confirmed_and_then_sends_no_echo_is_unconfirmed_again() {
    let rig = Rig::new(&[1]);
    rig.set_yield(true);
    let (_, generation) = rig.block();
    rig.echo(1, generation);
    assert!(rig.effective(), "control: it confirmed");
    assert!(rig.unconfirmed().is_empty());
    rig.members.note_echo(n(1), None);
    assert_eq!(rig.unconfirmed(), vec![n(1)]);
    assert!(!rig.effective(), "and the member owns the class again");
}

// ---------------------------------------------------------------------------
// ADR-191: generations.
// ---------------------------------------------------------------------------

/// No transition moves `generations`: not a block recorded, not a yield, not a
/// confirmation, not this member's own `Defunct`; only SWIM's `insert` does.
#[test]
fn no_yield_transition_moves_the_address_generations() {
    let rig = Rig::new(&[1]);
    let addr = a(7001);
    let before = rig.members.generation(&addr).unwrap();
    hear(&rig.members, 1, block(1), Some(1), crate::facts::next_decoded_seq());
    rig.set_yield(true);
    let (_, generation) = rig.block();
    rig.echo(1, generation);
    assert!(rig.effective());
    rig.set_yield(false);
    rig.members.note_echo(n(1), None);
    hear(&rig.members, 1, block(2), Some(1), crate::facts::next_decoded_seq());
    let mut collector =
        Collector { outgoing: Vec::new(), timers: Vec::new(), members: rig.members.clone() };
    collector.notify(Notification::Defunct);
    assert_eq!(rig.members.generation(&addr), Some(before), "nothing moved it");
    rig.members.insert_for_test(addr, n(1));
    assert!(rig.members.generation(&addr).unwrap() > before, "SWIM bringing it up does");
}

// ---------------------------------------------------------------------------
// The view.
// ---------------------------------------------------------------------------

/// Every change to the live set or to a held block publishes a view, in order.
#[test]
fn the_view_follows_every_change() {
    let members = Members::default();
    let v0 = members.view().version;
    members.configure_lease(Duration::from_secs(5), 3);
    members.insert_for_test(a(7001), n(1));
    let v = members.view();
    assert!(v.version > v0 && v.live == BTreeSet::from([n(1)]) && v.blocks.is_empty());
    assert_eq!(v.lease, Duration::from_secs(15));
    hear(
        &members,
        1,
        Facts { catching_up: true, ..block(1) },
        Some(4),
        crate::facts::next_decoded_seq(),
    );
    let w = members.view();
    assert!(w.version > v.version);
    assert_eq!(w.blocks[&n(1)].generation, Some(4));
    assert!(w.blocks[&n(1)].facts.catching_up);
    let (held, fresh) = w.live_block(&n(1), Instant::now()).unwrap();
    assert!(fresh && held.facts.catching_up);
    assert!(w.live_block(&n(2), Instant::now()).is_none(), "not live");
    members.remove_for_test(&a(7001));
    let x = members.view();
    assert!(x.version > w.version && x.live.is_empty() && x.blocks.is_empty());
}

/// Writers racing leave the last view equal to the tables: an older view is never
/// stored last (the view is built under the table's write lock).
#[test]
fn racing_writers_leave_the_view_equal_to_the_tables() {
    let members = Members::default();
    for p in 1..=4u8 {
        members.insert_for_test(a(7000 + u16::from(p)), n(p));
    }
    let threads: Vec<_> = (0..4u8)
        .map(|t| {
            let members = members.clone();
            // UNSUPERVISED: a test thread joined below, whose panic fails the test through the join
            std::thread::spawn(move || {
                for i in 0..200u64 {
                    let peer = 1 + (i % 4) as u8;
                    members.record_peer_facts(
                        n(peer),
                        Arc::new(Facts { boot: vec![t; 16], ..Facts::default() }),
                        Some(i),
                        crate::facts::next_decoded_seq(),
                        Instant::now(),
                    );
                    if i % 50 == 0 {
                        members.insert_for_test(a(7000 + u16::from(peer)), n(peer));
                    }
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let view = members.view();
    assert_eq!(view.live, members.node_ids());
    let table = members.0.peer_facts.read();
    assert_eq!(view.blocks.keys().collect::<Vec<_>>(), table.held.keys().collect::<Vec<_>>());
    for (peer, held) in &table.held {
        assert_eq!(view.blocks[peer].generation, held.generation);
        assert_eq!(view.blocks[peer].facts.boot, held.facts.boot);
    }
}
