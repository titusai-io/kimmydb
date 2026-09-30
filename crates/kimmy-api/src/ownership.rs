//! Which node delivers a given subscription, expires a given collection, or
//! embeds one.
//!
//! # Not a leader
//!
//! `owner(key, candidates)` is a **pure function** of the key (a subscription
//! id, or a collection's expiry or embedding key) and a candidate set. The
//! candidates are the live members that may own the class by what each last
//! said about itself on the replication contact, plus this node by its own
//! block ([`Owners`], ADR-201): a member that is catching up, or has the work
//! switched off, or (for expiry) is not known to hold the collection's TTL
//! index, is not one. Every node computes it independently and gets the same
//! answer once their views of the members agree; there is no vote, no term,
//! no consensus and no cluster-wide coordinator. Different keys land on
//! different nodes, so the work spreads, and a transient disagreement about
//! membership or about what a member said produces a *duplicate delivery*
//! rather than a split brain.
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

/// Whether `me` is the owner of `key` among `candidates`, a set that is already
/// filtered by what the members say about themselves (ADR-201) and so already
/// holds `me` when `me` may own the class. **An empty set has no owner**: unlike
/// [`owns`] it does not add `me`, because a member that may not own the class
/// (it is catching up, or does not hold the index) must not be put back.
pub fn owns_among(key: &str, me: NodeId, candidates: &BTreeSet<NodeId>) -> bool {
    owner(key, candidates) == Some(me)
}

/// How a collection's expiry stands, from this member's view (ADR-201).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TtlState {
    /// This member owns it.
    Owned,
    /// Another member owns it.
    OwedElsewhere,
    /// No member can expire it: none is known to hold the index and be able to
    /// (every holder has expiry switched off, or none is known to hold it).
    UnownedNoHolder,
    /// Every member known to hold the index is catching up. A delay, never a
    /// loss: a stale expiry beats a refreshed copy under last-writer-wins, so
    /// the work waits.
    UnownedCatchingUp,
}

impl TtlState {
    pub const ALL: [Self; 4] =
        [Self::Owned, Self::OwedElsewhere, Self::UnownedNoHolder, Self::UnownedCatchingUp];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Owned => "owned",
            Self::OwedElsewhere => "owed_elsewhere",
            Self::UnownedNoHolder => "unowned_no_holder",
            Self::UnownedCatchingUp => "unowned_catching_up",
        }
    }

    pub const fn slot(self) -> usize {
        self as usize
    }
}

/// This member's view of every TTL collection it holds an index on.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TtlView {
    /// Collections per [`TtlState`], in [`TtlState::ALL`] order.
    pub counts: [u64; TtlState::ALL.len()],
    /// The collections nobody can expire, `db.collection`, with why.
    pub unowned: Vec<(String, TtlState)>,
}

#[derive(Clone)]
enum View {
    /// No clustering: this node owns everything.
    Alone,
    /// The live member set and what its members say about themselves.
    Members(kimmy_cluster::Members),
    /// A bare member set, as before facts: for tests that fix who is live.
    Set(BTreeSet<NodeId>),
}

/// Who may own what (ADR-201): the rendezvous function over the candidates each
/// class allows, from the live members and what they last said.
///
/// Three consumers ask, each for its own class: expiry per collection, webhooks
/// per subscription, embeddings per collection. Cheap to clone and to ask; the
/// candidate set is computed per question from the member table, never cached.
#[derive(Clone)]
pub struct Owners {
    me: NodeId,
    view: View,
    /// Expiry is switched off here (`storage.ttl_interval_secs = 0`), so this
    /// member expires nothing. With clustering its own block says so too; this
    /// is what says it without clustering, where there is no block.
    expiry_off: bool,
}

impl Owners {
    /// With clustering (`Some`) or without (`None`, which owns everything).
    pub fn over(me: NodeId, members: Option<kimmy_cluster::Members>) -> Self {
        Self { me, view: members.map_or(View::Alone, View::Members), expiry_off: false }
    }

    /// Over a fixed live set and nothing said by anyone, as ownership was before
    /// members said anything.
    pub fn over_set(me: NodeId, live: BTreeSet<NodeId>) -> Self {
        Self { me, view: View::Set(live), expiry_off: false }
    }

    /// The same view, for a member whose expiry is switched off: it is never a
    /// candidate for a collection's expiry, clustered or not, so a collection
    /// no other member can expire is `unowned_no_holder` from here. Without
    /// clustering that is every TTL collection, since nobody else exists to
    /// expire them (ADR-201).
    #[must_use]
    pub fn with_expiry_off(mut self, off: bool) -> Self {
        self.expiry_off = off;
        self
    }

    fn mine(&self, members: &kimmy_cluster::Members) -> std::sync::Arc<kimmy_cluster::Facts> {
        members.local_facts().map(|(facts, _)| facts).unwrap_or_default()
    }

    fn owns(
        &self,
        class: kimmy_cluster::OwnerClass,
        key: &str,
        collection: Option<kimmy_core::CollectionId>,
        holds_ttl: bool,
    ) -> bool {
        // A member with expiry off is never a candidate for it: in its own
        // view, as in its block's.
        let holds_ttl = holds_ttl && !self.expiry_off;
        if class == kimmy_cluster::OwnerClass::Ttl && !holds_ttl {
            return false;
        }
        match &self.view {
            View::Alone => true,
            View::Set(live) => owns(key, self.me, live),
            View::Members(members) => {
                let mine = self.mine(members);
                let candidates = members.candidates(class, collection, self.me, &mine, holds_ttl);
                owns_among(key, self.me, &candidates)
            }
        }
    }

    /// Whether this member delivers the subscription.
    pub fn owns_subscription(&self, id: &str) -> bool {
        self.owns(kimmy_cluster::OwnerClass::Webhooks, id, None, false)
    }

    /// Whether this member embeds the collection (`db/collection`, the worker's
    /// key).
    pub fn owns_embedding(&self, key: &str) -> bool {
        self.owns(kimmy_cluster::OwnerClass::Embeddings, key, None, false)
    }

    /// Whether this member expires the collection: it holds a TTL index on it,
    /// and is the top-ranked of the members known to.
    pub fn owns_ttl(&self, coll: &kimmy_storage::CollectionMeta) -> bool {
        let holds = kimmy_storage::ttl_indexes(coll).next().is_some();
        self.owns(
            kimmy_cluster::OwnerClass::Ttl,
            &crate::expiry::key(&coll.db, &coll.name),
            Some(coll.id),
            holds,
        )
    }

    /// How each TTL collection among `collections` stands from here.
    pub fn ttl_view(&self, collections: &[kimmy_storage::CollectionMeta]) -> TtlView {
        let mut view = TtlView::default();
        for coll in collections {
            if kimmy_storage::ttl_indexes(coll).next().is_none() {
                continue;
            }
            let state = match &self.view {
                // Alone, with expiry off, nobody can expire it: there is no
                // other member to.
                View::Alone if self.expiry_off => TtlState::UnownedNoHolder,
                View::Alone | View::Set(_) => {
                    if self.owns_ttl(coll) {
                        TtlState::Owned
                    } else {
                        TtlState::OwedElsewhere
                    }
                }
                View::Members(members) => {
                    let mine = self.mine(members);
                    let candidates = members.candidates(
                        kimmy_cluster::OwnerClass::Ttl,
                        Some(coll.id),
                        self.me,
                        &mine,
                        !self.expiry_off,
                    );
                    let key = crate::expiry::key(&coll.db, &coll.name);
                    if candidates.is_empty() {
                        // This member counts among the holders that are
                        // catching up only if it could expire once caught up.
                        let me_waiting = mine.catching_up && !mine.ttl_disabled && !self.expiry_off;
                        if me_waiting || members.holder_catching_up(coll.id) {
                            TtlState::UnownedCatchingUp
                        } else {
                            TtlState::UnownedNoHolder
                        }
                    } else if owns_among(&key, self.me, &candidates) {
                        TtlState::Owned
                    } else {
                        TtlState::OwedElsewhere
                    }
                }
            };
            view.counts[state.slot()] += 1;
            if matches!(state, TtlState::UnownedNoHolder | TtlState::UnownedCatchingUp) {
                view.unowned.push((format!("{}.{}", coll.db, coll.name), state));
            }
        }
        view
    }
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

    // -- What members say about themselves (ADR-201) ----------------------------

    use kimmy_cluster::{Facts, Members, TtlHeld};

    fn addr(n: u8) -> std::net::SocketAddr {
        format!("127.0.0.1:{}", 7000 + u16::from(n)).parse().unwrap()
    }

    /// A member's view: the others live, each having said `said`, and itself
    /// described by `mine`.
    fn view(me: u8, others: &[(u8, Facts)], mine: Facts) -> Owners {
        let members = Members::default();
        for (n, said) in others {
            members.insert_for_test(addr(*n), node(*n));
            members.record_peer_facts_for_test(node(*n), said.clone(), std::time::Duration::ZERO);
        }
        members.set_facts_source(std::sync::Arc::new(move || mine.clone()));
        Owners::over(node(me), Some(members))
    }

    fn block() -> Facts {
        Facts { boot: vec![1; 16], ..Facts::default() }
    }

    #[test]
    fn an_empty_candidate_set_has_no_owner_and_does_not_put_me_back() {
        assert!(!owns_among("wh_a", node(1), &BTreeSet::new()));
        assert!(owns_among("wh_a", node(1), &members(&[1])));
    }

    /// A member that is catching up delivers nothing, and its peers between them
    /// deliver everything, each subscription once.
    #[test]
    fn a_catching_up_member_owns_no_subscription_and_the_others_own_each_once() {
        let gated = Facts { catching_up: true, ..block() };
        // Member 1 is the gated one; 2 and 3 have read its block.
        let one = view(1, &[(2, block()), (3, block())], gated.clone());
        let two = view(2, &[(1, gated.clone()), (3, block())], block());
        let three = view(3, &[(1, gated), (2, block())], block());
        for id in (0..40).map(|i| format!("wh_{i}")) {
            assert!(!one.owns_subscription(&id), "{id}: the catching-up member delivers nothing");
            let owners = [&two, &three].iter().filter(|o| o.owns_subscription(&id)).count();
            assert_eq!(owners, 1, "{id}");
        }
    }

    /// A member whose embedding worker is off never owns an embedding, and one
    /// whose peers are all off owns none.
    #[test]
    fn a_member_with_its_embedding_worker_off_owns_no_embedding() {
        let off = Facts { embeddings_disabled: true, ..block() };
        let one = view(1, &[(2, block())], off.clone());
        let two = view(2, &[(1, off)], block());
        for key in (0..30).map(|i| format!("db/c{i}")) {
            assert!(!one.owns_embedding(&key));
            assert!(two.owns_embedding(&key), "{key}: the only member that can");
        }
    }

    /// Without clustering, or with a member set nobody has spoken in, ownership
    /// is what it was.
    #[test]
    fn with_no_clustering_this_node_owns_everything_and_unheard_peers_count_as_before() {
        let alone = Owners::over(node(1), None);
        assert!(alone.owns_subscription("wh_a") && alone.owns_embedding("db/c"));
        // Peers that have said nothing (older builds): as the bare set did.
        let members_set = members(&[2, 3]);
        let unheard = Members::default();
        for n in [2u8, 3] {
            unheard.insert_for_test(addr(n), node(n));
        }
        let with_facts = Owners::over(node(1), Some(unheard));
        let bare = Owners::over_set(node(1), members_set);
        for i in 0..40 {
            let id = format!("wh_{i}");
            assert_eq!(with_facts.owns_subscription(&id), bare.owns_subscription(&id), "{id}");
        }
    }

    fn ttl_block(collection: u64) -> Facts {
        block().with_ttl(vec![TtlHeld {
            collection: kimmy_core::CollectionId(collection),
            digest: vec![1; 8],
        }])
    }

    /// A store holding `app.sessions` with a TTL index, and its registry.
    fn ttl_registry() -> (Vec<kimmy_storage::CollectionMeta>, u64, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = kimmy_storage::Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        engine.create_collection("app", "sessions").unwrap();
        engine
            .create_index_with(
                "app",
                "sessions",
                vec![kimmy_storage::IndexField::ascending("seen")],
                false,
                Default::default(),
                Some("ttl_seen".into()),
                Some(60),
                None,
            )
            .unwrap();
        let id = engine.get_collection("app", "sessions").unwrap().id.0;
        (engine.all_collections().unwrap(), id, dir)
    }

    /// A member with expiry off, whose one peer holds the index: the collection
    /// waits when that holder says it is catching up, and nobody can expire it
    /// when the holder has expiry off too, catching up or not, since it would not
    /// expire it once caught up. The two states are what tell an operator "wait"
    /// from "fix the configuration".
    #[test]
    fn a_holder_catching_up_is_a_wait_and_one_with_expiry_off_is_no_holder() {
        let (registry, id, _dir) = ttl_registry();
        let mine = Facts { ttl_disabled: true, ..ttl_block(id) };
        let seen = |said: Facts| view(1, &[(2, said)], mine.clone()).ttl_view(&registry);

        let waiting = seen(Facts { catching_up: true, ..ttl_block(id) });
        assert_eq!(waiting.counts, [0, 0, 0, 1], "{waiting:?}");
        assert_eq!(waiting.unowned, vec![("app.sessions".into(), TtlState::UnownedCatchingUp)]);

        let off = seen(Facts { ttl_disabled: true, ..ttl_block(id) });
        assert_eq!(off.counts, [0, 0, 1, 0], "{off:?}");
        let off_and_waiting =
            seen(Facts { ttl_disabled: true, catching_up: true, ..ttl_block(id) });
        assert_eq!(off_and_waiting.counts, [0, 0, 1, 0], "{off_and_waiting:?}");

        let able = seen(ttl_block(id));
        assert_eq!(able.counts, [0, 1, 0, 0], "the able holder owns it: {able:?}");
    }

    /// Without clustering, a member with expiry off can expire nothing and nobody
    /// else exists to: every TTL collection it holds is `unowned_no_holder`, and
    /// it owns none of them. `Owners::over(me, None)` alone reads them owned.
    #[test]
    fn a_lone_member_with_expiry_off_owns_no_ttl_collection() {
        let (registry, _, _dir) = ttl_registry();
        let alone = Owners::over(node(1), None);
        assert_eq!(alone.ttl_view(&registry).counts, [1, 0, 0, 0]);
        let off = Owners::over(node(1), None).with_expiry_off(true);
        let seen = off.ttl_view(&registry);
        assert_eq!(seen.counts, [0, 0, 1, 0], "{seen:?}");
        assert_eq!(seen.unowned, vec![("app.sessions".into(), TtlState::UnownedNoHolder)]);
        assert!(!off.owns_ttl(&registry[0]));
        assert!(off.owns_subscription("wh_a") && off.owns_embedding("app/c"), "TTL only");
    }
}
