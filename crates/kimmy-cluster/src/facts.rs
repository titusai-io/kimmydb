//! What each member says about itself on the sync contact, and who may own what
//! because of it (ADR-201).
//!
//! Ownership is a pure function of a key and a candidate set (`kimmy-api`'s
//! `ownership`). SWIM says which members are up and nothing else, so a member
//! that is up and cannot do a class of work, or does not hold the index the
//! work is on, kept its share: a collection's expiry could be owned by a member
//! with no TTL index, and then no member expired it. [`Facts`] is a small block
//! each member sends on the replication contact it already makes, and
//! [`Members`](crate::membership::Members) keeps what it hears, so the
//! candidate set can be filtered by it.
//!
//! **Compatibility is argued from the codec.** A [`crate::protocol::Message`]
//! is a length-prefixed BSON document and a decoder ignores keys it does not
//! know, so an older build reads a reply carrying `facts` and drops the key, and
//! a build that reads a reply without it sees `None`. Every field carries
//! `serde(default)`, so a block from a build with fewer fields decodes. An
//! absent block is *unknown*, never a fault.
//!
//! **Digests and ids go on the wire as `Binary` or as a collection id**, never
//! as a `u64`: BSON refuses an integer above `i64::MAX`, and a hash is uniformly
//! distributed, so half of them would fail the whole reply to serialise.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kimmy_core::{CollectionId, NodeId};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

/// The most collections one block names. A member holding TTL indexes on more
/// says so in [`Facts::ttl_truncated`], and a collection missing from a
/// truncated list is *not known* to be held, so it does not make its member a
/// candidate to anyone else.
pub const MAX_TTL_COLLECTIONS: usize = 256;

/// A class of work whose owner is chosen by the rendezvous function.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OwnerClass {
    /// Expiring a collection's documents by its TTL index.
    Ttl,
    /// Delivering a webhook subscription's events.
    Webhooks,
    /// Embedding a collection's documents.
    Embeddings,
}

impl OwnerClass {
    pub const ALL: [Self; 3] = [Self::Ttl, Self::Webhooks, Self::Embeddings];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Ttl => "ttl",
            Self::Webhooks => "webhooks",
            Self::Embeddings => "embeddings",
        }
    }
}

/// Which classes this member has stopped owning because it cannot do them.
/// Defined and honoured by a reader in 0.43.0; a 0.43.0 sender always sends
/// `false`, and nothing yields until the predicate that sets these ships.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Yielding {
    #[serde(default)]
    pub ttl: bool,
    #[serde(default)]
    pub webhooks: bool,
    #[serde(default)]
    pub embeddings: bool,
}

impl Yielding {
    pub const fn of(&self, class: OwnerClass) -> bool {
        match class {
            OwnerClass::Ttl => self.ttl,
            OwnerClass::Webhooks => self.webhooks,
            OwnerClass::Embeddings => self.embeddings,
        }
    }

    pub const fn any(&self) -> bool {
        self.ttl || self.webhooks || self.embeddings
    }
}

/// One collection this member holds at least one TTL index on, and a
/// fixed-size digest of those indexes' definitions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtlHeld {
    pub collection: CollectionId,
    #[serde(with = "serde_bytes")]
    pub digest: Vec<u8>,
}

/// What a member says about itself. See the module documentation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Facts {
    /// A random id, new at every process start. SWIM's incarnation does not
    /// change on a restart, so it cannot say that a member's earlier block
    /// belonged to a process that no longer exists; a receiver that sees a new
    /// boot id discards that member's earlier state.
    #[serde(default, with = "serde_bytes")]
    pub boot: Vec<u8>,
    /// The catching-up marker is set (a fresh, restored or snapshot-catching-up
    /// store). Never overridden for any class.
    #[serde(default)]
    pub catching_up: bool,
    #[serde(default)]
    pub yielding: Yielding,
    /// Operator choices, which are not faults and so are not yielding: expiry is
    /// off here (`ttl_interval_secs = 0`), or the embedding worker is
    /// (`vector.worker_enabled = false`).
    #[serde(default)]
    pub ttl_disabled: bool,
    #[serde(default)]
    pub embeddings_disabled: bool,
    /// Every collection this member holds a TTL index on, up to
    /// [`MAX_TTL_COLLECTIONS`], in collection-id order.
    #[serde(default)]
    pub ttl: Vec<TtlHeld>,
    #[serde(default)]
    pub ttl_truncated: bool,
}

impl Facts {
    /// Whether this block lists `collection` among the TTL indexes it holds.
    pub fn holds_ttl(&self, collection: CollectionId) -> bool {
        self.ttl.iter().any(|held| held.collection == collection)
    }

    /// Order and truncate the TTL list as it goes on the wire.
    pub fn with_ttl(mut self, mut held: Vec<TtlHeld>) -> Self {
        held.sort_by_key(|h| h.collection);
        held.dedup_by_key(|h| h.collection);
        self.ttl_truncated = held.len() > MAX_TTL_COLLECTIONS;
        held.truncate(MAX_TTL_COLLECTIONS);
        self.ttl = held;
        self
    }
}

/// How a member's own block is produced: a closure the daemon supplies, which
/// reads the store's schema and the configuration.
pub type FactsSource = Arc<dyn Fn() -> Facts + Send + Sync>;

/// This member's block, cached: rebuilt when the source says something else,
/// and at most once a second, so a serve reply costs one `Arc` clone and the
/// schema is not read per contact. Also which peers have read the current
/// block, for [`crate::membership::Members::unconfirmed_peers`].
pub(crate) struct LocalFacts {
    source: Option<FactsSource>,
    state: Mutex<LocalState>,
}

struct LocalState {
    built: Option<Instant>,
    /// The next build's ticket, and the ticket of the build whose block is
    /// current: a build that started earlier never overwrites one that started
    /// later.
    next_ticket: u64,
    published_ticket: u64,
    current: Arc<Facts>,
    /// Bumped whenever the block changes.
    generation: u64,
    /// The generation each peer last read.
    read: BTreeMap<NodeId, u64>,
}

/// The longest a cached block is reused.
const REBUILD_AFTER: Duration = Duration::from_secs(1);

impl Default for LocalFacts {
    fn default() -> Self {
        Self {
            source: None,
            state: Mutex::new(LocalState {
                built: None,
                next_ticket: 1,
                published_ticket: 0,
                current: Arc::default(),
                generation: 0,
                read: BTreeMap::new(),
            }),
        }
    }
}

impl LocalFacts {
    pub(crate) fn with_source(source: FactsSource) -> Self {
        Self { source: Some(source), ..Self::default() }
    }

    pub(crate) fn has_source(&self) -> bool {
        self.source.is_some()
    }

    /// The block to send now with its generation, or `None` when this member
    /// has no source (a test, or a build that does not send one).
    ///
    /// **The source runs outside the lock**: it reads the store's schema, and a
    /// serve task must not wait behind another's read. Two callers that find the
    /// cache stale may both build; each takes a ticket first, and a build never
    /// publishes over one that started after it, so an older read of the schema
    /// cannot replace a newer one.
    ///
    /// **And off the async worker** (ADR-153, ADR-199): the callers are the serve
    /// and sync arms and every owner check (the webhook dispatcher's and the
    /// embedding worker's), all on the runtime, and a rebuild is a read of the
    /// whole collection registry. A reuse of the cached block reads nothing.
    pub(crate) fn current(&self, now: Instant) -> Option<(Arc<Facts>, u64)> {
        let source = self.source.as_ref()?;
        let ticket = {
            let mut state = self.state.lock();
            let stale =
                state.built.is_none_or(|at| now.saturating_duration_since(at) >= REBUILD_AFTER);
            stale.then(|| {
                let ticket = state.next_ticket;
                state.next_ticket += 1;
                ticket
            })
        };
        let fresh = ticket.map(|ticket| (ticket, kimmy_storage::blocking(|| source())));
        let mut state = self.state.lock();
        if let Some((ticket, fresh)) = fresh
            && ticket > state.published_ticket
        {
            state.published_ticket = ticket;
            state.built = Some(now);
            if *state.current != fresh || state.generation == 0 {
                state.current = Arc::new(fresh);
                state.generation += 1;
            }
        }
        Some((Arc::clone(&state.current), state.generation))
    }

    /// Forget what `peer` has read: it is a new process, or SWIM declared it
    /// down, and what the old one read says nothing of this one.
    pub(crate) fn forget(&self, peer: NodeId) {
        self.state.lock().read.remove(&peer);
    }

    /// Keep only the read records of peers still in `live`.
    pub(crate) fn retain(&self, live: &BTreeSet<NodeId>) {
        self.state.lock().read.retain(|peer, _| live.contains(peer));
    }

    /// `peer` was sent block `generation`.
    pub(crate) fn note_read(&self, peer: NodeId, generation: u64) {
        self.state.lock().read.insert(peer, generation);
    }

    /// The peers among `live` that have not read the current block.
    pub(crate) fn unread_by(&self, live: &BTreeSet<NodeId>) -> Vec<NodeId> {
        let state = self.state.lock();
        live.iter()
            .filter(|peer| state.read.get(*peer) != Some(&state.generation))
            .copied()
            .collect()
    }
}

/// A block a peer sent, and when.
#[derive(Clone, Debug)]
pub(crate) struct PeerFacts {
    pub facts: Arc<Facts>,
    pub received: Instant,
}

/// How this member sees one live peer, for the gauge and the topology.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PeerState {
    /// A block, within its lease, that leaves the peer able to own.
    Eligible,
    /// A block whose `catching_up` is set.
    IneligibleCatchingUp,
    /// A block that yields at least one class.
    IneligibleYielding,
    /// The peer has never sent a block: an older version, or one not yet heard.
    Unknown,
    /// A block past its lease with no fresh one. It keeps saying what it last
    /// said: not hearing from a member is not evidence that it recovered.
    Stale,
}

impl PeerState {
    pub const ALL: [Self; 5] = [
        Self::Eligible,
        Self::IneligibleCatchingUp,
        Self::IneligibleYielding,
        Self::Unknown,
        Self::Stale,
    ];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Eligible => "eligible",
            Self::IneligibleCatchingUp => "ineligible_catching_up",
            Self::IneligibleYielding => "ineligible_yielding",
            Self::Unknown => "unknown",
            Self::Stale => "stale",
        }
    }

    pub const fn slot(self) -> usize {
        self as usize
    }
}

/// Whether one member, described by `facts` (`None` when it has never sent a
/// block), may own `class` (for TTL, `collection`), with `holds_ttl` the
/// caller's own knowledge when the member is this one.
///
/// **`catching_up` is never overridden.** `ignore_yielding` is the fallback for
/// an empty candidate set: an owner that is slow is better than none.
///
/// **A TTL listing is positive knowledge only while its block is fresh**
/// (`fresh`): a holder that dropped the index while its contacts fail must not
/// stay the owner for as long as it is stale. `catching_up` and `yielding` are
/// the other way round and stay set while stale, since silence is not evidence
/// of recovery.
pub(crate) fn may_own(
    facts: Option<&Facts>,
    class: OwnerClass,
    collection: Option<CollectionId>,
    holds_ttl: Option<bool>,
    ignore_yielding: bool,
    fresh: bool,
) -> bool {
    let Some(facts) = facts else {
        // Never sent a block: an older member. It owns as it always did for the
        // classes with no holder rule, and is not *known* to hold a TTL index.
        return class != OwnerClass::Ttl;
    };
    if facts.catching_up || (!ignore_yielding && facts.yielding.of(class)) {
        return false;
    }
    match class {
        OwnerClass::Ttl => {
            !facts.ttl_disabled
                && holds_ttl
                    .unwrap_or_else(|| fresh && collection.is_some_and(|c| facts.holds_ttl(c)))
        }
        OwnerClass::Webhooks => true,
        OwnerClass::Embeddings => !facts.embeddings_disabled,
    }
}

/// Read a `facts` key leniently: a block that does not decode is *no block* (and
/// a debug line), never a failed `Vectors` or `AskVersions`, which would fail the
/// round for a field that is only advice.
///
/// **It is counted and said**, or a peer whose blocks never decode would look
/// exactly like an older peer that sends none: the count is
/// [`facts_undecodable_total`], and the read that decoded the frame reports it
/// (`protocol::read_frame_noting_facts`), which is the only place that knows
/// which peer sent it.
pub(crate) fn lenient<'de, D>(deserializer: D) -> Result<Option<Arc<Facts>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<bson::Bson>::deserialize(deserializer)?;
    Ok(raw.and_then(|raw| match bson::deserialize_from_bson::<Facts>(raw) {
        Ok(facts) => Some(Arc::new(facts)),
        Err(error) => {
            tracing::debug!(%error, "ignoring a facts block that does not decode");
            UNDECODABLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            LAST_UNDECODABLE.with(|flag| flag.set(true));
            None
        }
    }))
}

/// Blocks that arrived and did not decode, since start.
static UNDECODABLE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

thread_local! {
    /// Set by [`lenient`] when the frame being decoded on this thread carried a
    /// block that did not decode; taken by the read's caller. The decode and the
    /// caller's next line run in one poll, so nothing else decodes between.
    static LAST_UNDECODABLE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// How many blocks have arrived that did not decode, since start
/// (`kimmy_ownership_facts_undecodable_total`).
pub fn facts_undecodable_total() -> u64 {
    UNDECODABLE.load(std::sync::atomic::Ordering::Relaxed)
}

/// Whether the frame just read on this thread carried a block that did not
/// decode; clears the flag. Called before a read (to discard a stale one) and
/// after it.
pub(crate) fn take_undecodable() -> bool {
    LAST_UNDECODABLE.with(|flag| flag.replace(false))
}

/// Says once per interval, per class and peer, that a yielding member is still
/// waiting on a peer that has not read its block.
#[derive(Default)]
pub struct UnconfirmedWarn {
    last: std::collections::HashMap<(OwnerClass, NodeId), Instant>,
}

impl UnconfirmedWarn {
    /// Whether to say it now for `class` and `peer`.
    pub fn due(&mut self, class: OwnerClass, peer: NodeId, now: Instant, every: Duration) -> bool {
        match self.last.get(&(class, peer)) {
            Some(at) if now.saturating_duration_since(*at) < every => false,
            _ => {
                self.last.insert((class, peer), now);
                true
            }
        }
    }

    /// Forget what is no longer waited on, so the map follows the cluster.
    pub fn retain(&mut self, waiting: &[(OwnerClass, NodeId)]) {
        self.last.retain(|key, _| waiting.contains(key));
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;
    use crate::membership::Members;

    fn node(n: u8) -> NodeId {
        NodeId::from_bytes([n; 16])
    }

    fn addr(n: u8) -> SocketAddr {
        format!("127.0.0.1:{}", 7000 + u16::from(n)).parse().unwrap()
    }

    fn cluster(peers: &[u8]) -> Members {
        let members = Members::default();
        members.configure_lease(Duration::from_secs(5), 3);
        for n in peers {
            members.insert_for_test(addr(*n), node(*n));
        }
        members
    }

    fn held(collection: u64) -> TtlHeld {
        TtlHeld { collection: CollectionId(collection), digest: vec![7; 8] }
    }

    fn block() -> Facts {
        Facts { boot: vec![1; 16], ..Facts::default() }
    }

    fn hear(members: &Members, n: u8, facts: Facts) {
        members.record_peer_facts(node(n), Arc::new(facts), Instant::now());
    }

    fn set(
        members: &Members,
        class: OwnerClass,
        collection: Option<CollectionId>,
        me: u8,
        mine: &Facts,
        me_holds: bool,
    ) -> Vec<u8> {
        members
            .candidates(class, collection, node(me), mine, me_holds)
            .into_iter()
            .map(|n| n.to_bytes()[0])
            .collect()
    }

    /// The wire block survives BSON with a digest and a collection id above
    /// `i64::MAX`, which is what serialising them as `u64` would have refused.
    #[test]
    fn a_block_with_large_ids_and_digests_round_trips() {
        let facts = block().with_ttl(vec![
            TtlHeld { collection: CollectionId(u64::MAX - 3), digest: vec![0xFF; 8] },
            held(5),
        ]);
        let bytes = bson::serialize_to_vec(&facts).unwrap();
        let back: Facts = bson::deserialize_from_slice(&bytes).unwrap();
        assert_eq!(back, facts);
        assert_eq!(back.ttl[0].collection, CollectionId(5), "in collection-id order");
    }

    /// A block from a build with fewer fields decodes, each absent field at its
    /// default, and an empty document is the default block.
    #[test]
    fn a_block_with_fewer_fields_decodes() {
        let older = bson::doc! { "catching_up": true };
        let back: Facts =
            bson::deserialize_from_slice(&bson::serialize_to_vec(&older).unwrap()).unwrap();
        assert!(back.catching_up && back.ttl.is_empty() && !back.yielding.any());
        let empty: Facts =
            bson::deserialize_from_slice(&bson::serialize_to_vec(&bson::doc! {}).unwrap()).unwrap();
        assert_eq!(empty, Facts::default());
    }

    /// The list is capped, and a cap is said.
    #[test]
    fn a_long_ttl_list_is_truncated_and_says_so() {
        let many: Vec<TtlHeld> = (0..(MAX_TTL_COLLECTIONS as u64 + 10)).map(held).collect();
        let facts = Facts::default().with_ttl(many);
        assert_eq!(facts.ttl.len(), MAX_TTL_COLLECTIONS);
        assert!(facts.ttl_truncated);
        assert!(!Facts::default().with_ttl(vec![held(1)]).ttl_truncated);
    }

    /// The lease is at least `ceil(peers / fanout) + 2` sync intervals, derived
    /// from the live member count, not a constant.
    #[test]
    fn the_lease_follows_the_member_count_and_the_fanout() {
        let unconfigured = Members::default();
        assert!(unconfigured.lease() >= Duration::from_secs(10), "a lease is never zero");
        let members = cluster(&[]);
        assert_eq!(members.lease(), Duration::from_secs(10), "no peers: two intervals");
        let members = cluster(&[1, 2, 3]);
        assert_eq!(members.lease(), Duration::from_secs(15), "three peers at fanout 3");
        let members = cluster(&[1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(members.lease(), Duration::from_secs(25), "seven peers: three rounds and two");
    }

    /// A peer that has never sent a block is unknown; one that has is eligible;
    /// past the lease it is stale, and stays as it last said.
    #[test]
    fn a_stale_block_keeps_saying_what_it_last_said() {
        let members = cluster(&[1, 2]);
        assert_eq!(members.peer_states()[&node(1)], PeerState::Unknown);
        // Heard long ago: well past any lease.
        let then = Instant::now().checked_sub(Duration::from_secs(600)).unwrap();
        members.record_peer_facts(node(1), Arc::new(Facts { catching_up: true, ..block() }), then);
        members.record_peer_facts(node(2), Arc::new(block()), then);
        let states = members.peer_states();
        assert_eq!(states[&node(1)], PeerState::Stale);
        assert_eq!(states[&node(2)], PeerState::Stale);
        // Stale is not eligible again: the catching-up bit is still honoured.
        let mine = block();
        assert_eq!(set(&members, OwnerClass::Webhooks, None, 9, &mine, false), vec![2, 9]);
    }

    /// A fresh block replaces the earlier one whatever process sent it, and a
    /// peer SWIM brings up again has its block dropped: it has not spoken yet.
    #[test]
    fn a_new_block_and_a_new_incarnation_replace_the_old_state() {
        let members = cluster(&[1]);
        hear(&members, 1, Facts { catching_up: true, ..block() });
        assert_eq!(members.peer_states()[&node(1)], PeerState::IneligibleCatchingUp);
        hear(&members, 1, Facts { boot: vec![2; 16], ..Facts::default() });
        assert_eq!(members.peer_states()[&node(1)], PeerState::Eligible, "a new boot, a new state");
        hear(
            &members,
            1,
            Facts { yielding: Yielding { ttl: true, ..Yielding::default() }, ..block() },
        );
        assert_eq!(members.peer_states()[&node(1)], PeerState::IneligibleYielding);
        members.remove_for_test(&addr(1));
        members.insert_for_test(addr(1), node(1));
        assert_eq!(
            members.peer_states()[&node(1)],
            PeerState::Unknown,
            "down and up again: unheard"
        );
    }

    /// `catching_up` is never overridden, in the fallback either: if every
    /// candidate is catching up the class is delayed, not given to one.
    #[test]
    fn catching_up_is_never_overridden() {
        let members = cluster(&[1, 2]);
        hear(&members, 1, Facts { catching_up: true, ..block() });
        hear(&members, 2, Facts { catching_up: true, ..block() });
        let gated = Facts { catching_up: true, ..block() };
        for class in [OwnerClass::Webhooks, OwnerClass::Embeddings] {
            assert!(set(&members, class, None, 9, &gated, false).is_empty(), "{class:?}");
        }
        let ok = block();
        assert_eq!(set(&members, OwnerClass::Webhooks, None, 9, &ok, false), vec![9]);
    }

    /// A class every candidate yields is tried again ignoring the yielding:
    /// an owner that is slow beats none.
    #[test]
    fn a_class_everyone_yields_falls_back_to_the_yielding_members() {
        let members = cluster(&[1]);
        let yields =
            Facts { yielding: Yielding { webhooks: true, ..Yielding::default() }, ..block() };
        hear(&members, 1, yields.clone());
        assert_eq!(set(&members, OwnerClass::Webhooks, None, 9, &yields, false), vec![1, 9]);
        let ok = block();
        assert_eq!(set(&members, OwnerClass::Webhooks, None, 9, &ok, false), vec![9]);
        assert_eq!(
            set(&members, OwnerClass::Embeddings, None, 9, &yields, false),
            vec![1, 9],
            "only the yielded class is affected"
        );
    }

    /// TTL: only positive knowledge makes a candidate. A member that lacks the
    /// index, one whose list was truncated, one with expiry off and one that has
    /// never sent a block are not holders.
    #[test]
    fn a_ttl_candidate_is_a_member_known_to_hold_the_index() {
        let c = CollectionId(42);
        let members = cluster(&[1, 2, 3, 4, 5]);
        hear(&members, 1, block().with_ttl(vec![held(42)]));
        hear(&members, 2, block().with_ttl(vec![held(7)]));
        hear(&members, 3, Facts { ttl_truncated: true, ..block() });
        hear(&members, 4, Facts { ttl_disabled: true, ..block().with_ttl(vec![held(42)]) });
        // 5 has sent nothing.
        let mine = block();
        assert_eq!(set(&members, OwnerClass::Ttl, Some(c), 9, &mine, true), vec![1, 9]);
        assert_eq!(set(&members, OwnerClass::Ttl, Some(c), 9, &mine, false), vec![1]);
        assert!(
            set(&members, OwnerClass::Ttl, Some(CollectionId(99)), 9, &mine, false).is_empty(),
            "no holder anywhere: nobody owns it, and that is the truth"
        );
        let off = Facts { ttl_disabled: true, ..block() };
        assert_eq!(
            set(&members, OwnerClass::Ttl, Some(c), 9, &off, true),
            vec![1],
            "expiry off here"
        );
    }

    /// Embeddings skip a member whose worker is off, and webhooks count a peer
    /// that has sent nothing.
    #[test]
    fn the_embedding_worker_off_and_an_older_peer_are_handled_as_stated() {
        let members = cluster(&[1, 2]);
        hear(&members, 1, Facts { embeddings_disabled: true, ..block() });
        let mine = block();
        assert_eq!(set(&members, OwnerClass::Embeddings, None, 9, &mine, false), vec![2, 9]);
        assert_eq!(set(&members, OwnerClass::Webhooks, None, 9, &mine, false), vec![1, 2, 9]);
    }

    /// With no peers the member owns everything it may, as a single node does.
    #[test]
    fn a_member_with_no_peers_owns_as_it_did() {
        let members = cluster(&[]);
        let mine = block();
        for class in [OwnerClass::Webhooks, OwnerClass::Embeddings] {
            assert_eq!(set(&members, class, None, 9, &mine, false), vec![9]);
        }
        assert_eq!(set(&members, OwnerClass::Ttl, Some(CollectionId(1)), 9, &mine, true), vec![9]);
    }

    /// The gauge's counts, per state.
    #[test]
    fn the_peer_counts_are_by_state() {
        let members = cluster(&[1, 2, 3, 4]);
        hear(&members, 1, block());
        hear(&members, 2, Facts { catching_up: true, ..block() });
        hear(
            &members,
            3,
            Facts { yielding: Yielding { ttl: true, ..Yielding::default() }, ..block() },
        );
        let counts = members.peer_state_counts();
        assert_eq!(counts[PeerState::Eligible.slot()], 1);
        assert_eq!(counts[PeerState::IneligibleCatchingUp.slot()], 1);
        assert_eq!(counts[PeerState::IneligibleYielding.slot()], 1);
        assert_eq!(counts[PeerState::Unknown.slot()], 1);
        assert_eq!(counts[PeerState::Stale.slot()], 0);
    }

    /// A yielding member is waiting on the peers that have not read the block
    /// that says so, and on nobody when it does not yield.
    #[test]
    fn a_yielding_member_waits_on_the_peers_that_have_not_read_its_block() {
        let yielding = Arc::new(parking_lot::Mutex::new(false));
        let members = cluster(&[1, 2]);
        members.set_facts_source({
            let yielding = Arc::clone(&yielding);
            Arc::new(move || Facts {
                yielding: Yielding { ttl: *yielding.lock(), ..Yielding::default() },
                ..block()
            })
        });
        assert!(members.unconfirmed_peers(OwnerClass::Ttl).is_empty(), "nothing yields");
        *yielding.lock() = true;
        // The cached block is at most a second old.
        std::thread::sleep(Duration::from_millis(1_100));
        assert_eq!(members.unconfirmed_peers(OwnerClass::Ttl).len(), 2);
        let (_, generation) = members.local_facts().unwrap();
        members.note_read_by(node(1), generation);
        assert_eq!(members.unconfirmed_peers(OwnerClass::Ttl), vec![node(2)]);
        assert!(
            members.unconfirmed_peers(OwnerClass::Webhooks).is_empty(),
            "only the yielded class"
        );
    }

    /// A TTL listing is positive knowledge only while its block is fresh: a
    /// holder that dropped the index while its contacts fail does not stay the
    /// owner for as long as it is stale. `catching_up` is the other way round.
    #[test]
    fn a_stale_ttl_listing_is_not_a_holder_but_a_stale_catching_up_bit_still_excludes() {
        let c = CollectionId(42);
        let members = cluster(&[1, 2, 3]);
        let long_ago = Instant::now().checked_sub(Duration::from_secs(600)).unwrap();
        members.record_peer_facts(node(1), Arc::new(block().with_ttl(vec![held(42)])), long_ago);
        hear(&members, 2, block().with_ttl(vec![held(42)]));
        members.record_peer_facts(
            node(3),
            Arc::new(Facts { catching_up: true, ..block() }),
            long_ago,
        );
        let mine = block();
        assert_eq!(
            set(&members, OwnerClass::Ttl, Some(c), 9, &mine, false),
            vec![2],
            "the stale holder is not a candidate, the fresh one is"
        );
        assert_eq!(
            set(&members, OwnerClass::Webhooks, None, 9, &mine, false),
            vec![1, 2, 9],
            "a stale peer that said nothing against it still counts; the stale catching-up one does not"
        );
    }

    /// A block that arrives before SWIM's insert survives the insert, and a peer
    /// SWIM declares down has its block and its read record dropped.
    #[test]
    fn a_block_before_swims_insert_is_kept_and_a_declared_down_peer_is_forgotten() {
        let members = cluster(&[]);
        hear(&members, 1, Facts { catching_up: true, ..block() });
        members.insert_for_test(addr(1), node(1));
        assert_eq!(
            members.peer_states()[&node(1)],
            PeerState::IneligibleCatchingUp,
            "the block that beat the insert is kept"
        );
        members.remove_for_test(&addr(1));
        members.insert_for_test(addr(1), node(1));
        assert_eq!(members.peer_states()[&node(1)], PeerState::Unknown, "declared down: dropped");
    }

    /// What a peer read of this member's block is forgotten when the peer
    /// restarts or is declared down, and pruned for peers that left.
    #[test]
    fn the_read_records_follow_the_peers() {
        let members = cluster(&[1, 2]);
        members.set_facts_source(Arc::new(|| Facts {
            yielding: Yielding { ttl: true, ..Yielding::default() },
            ..block()
        }));
        let (_, generation) = members.local_facts().unwrap();
        hear(&members, 1, block());
        hear(&members, 2, block());
        members.note_read_by(node(1), generation);
        members.note_read_by(node(2), generation);
        assert!(members.unconfirmed_peers(OwnerClass::Ttl).is_empty());
        // Peer 1 restarts: a new boot id, so what the old process read counts for
        // nothing.
        hear(&members, 1, Facts { boot: vec![2; 16], ..Facts::default() });
        assert_eq!(members.unconfirmed_peers(OwnerClass::Ttl), vec![node(1)]);
        // Peer 2 is declared down and comes back: unread again.
        members.remove_for_test(&addr(2));
        members.insert_for_test(addr(2), node(2));
        assert_eq!(members.unconfirmed_peers(OwnerClass::Ttl), vec![node(1), node(2)]);
    }

    /// The unconfirmed-peer warning is said once per interval per class and peer.
    #[test]
    fn the_unconfirmed_warning_is_rate_limited_per_class_and_peer() {
        let mut warned = UnconfirmedWarn::default();
        let every = Duration::from_secs(300);
        let t0 = Instant::now();
        assert!(warned.due(OwnerClass::Ttl, node(1), t0, every));
        assert!(!warned.due(OwnerClass::Ttl, node(1), t0 + Duration::from_secs(10), every));
        assert!(warned.due(OwnerClass::Ttl, node(2), t0, every), "another peer");
        assert!(warned.due(OwnerClass::Webhooks, node(1), t0, every), "another class");
        assert!(warned.due(OwnerClass::Ttl, node(1), t0 + every, every), "the interval passed");
        warned.retain(&[(OwnerClass::Ttl, node(2))]);
        assert!(
            warned.due(OwnerClass::Ttl, node(1), t0 + every, every),
            "forgotten once not waited on"
        );
    }

    /// Two builds that overlap: the one that started first and finished last must
    /// not overwrite the block the later one published.
    #[test]
    fn an_earlier_build_never_overwrites_a_later_one() {
        let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let local = Arc::new(LocalFacts::with_source(Arc::new({
            let calls = Arc::clone(&calls);
            move || {
                let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n == 0 {
                    // The first build is the slow one, and reads the older schema.
                    std::thread::sleep(Duration::from_millis(300));
                    Facts { boot: vec![1; 16], ..Facts::default() }
                } else {
                    Facts { boot: vec![2; 16], ..Facts::default() }
                }
            }
        })));
        let slow = {
            let local = Arc::clone(&local);
            std::thread::spawn(move || local.current(Instant::now()))
        };
        std::thread::sleep(Duration::from_millis(80));
        let (later, _) = local.current(Instant::now()).unwrap();
        assert_eq!(later.boot, vec![2; 16], "the second build finished first");
        slow.join().unwrap();
        let (now, _) = local.current(Instant::now()).unwrap();
        assert_eq!(now.boot, vec![2; 16], "the older build did not replace it");
    }

    /// An undecodable block is counted, flagged for the connection that read it,
    /// and said once per interval per peer.
    #[test]
    fn an_undecodable_block_is_counted_flagged_and_said_once_per_interval() {
        let before = facts_undecodable_total();
        take_undecodable();
        let doc = bson::doc! { "facts": { "ttl": "not a list" } };
        #[derive(serde::Deserialize)]
        struct Holder {
            #[serde(default, deserialize_with = "lenient")]
            facts: Option<Arc<Facts>>,
        }
        let held: Holder =
            bson::deserialize_from_slice(&bson::serialize_to_vec(&doc).unwrap()).unwrap();
        assert!(held.facts.is_none());
        assert_eq!(facts_undecodable_total(), before + 1);
        assert!(take_undecodable(), "flagged for the read's caller");
        assert!(!take_undecodable(), "and only once");

        let members = cluster(&[1]);
        let t0 = Instant::now();
        assert!(members.note_undecodable(node(1), t0));
        assert!(!members.note_undecodable(node(1), t0 + Duration::from_secs(10)));
        assert!(members.note_undecodable(node(2), t0), "another peer");
        assert!(members.note_undecodable(node(1), t0 + crate::health::WARN_INTERVAL));
    }
}
