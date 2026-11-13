//! Version vectors: how far each origin has been observed.
//!
//! A version vector answers "what have you got that I haven't?" without either
//! side sending its whole log. Each node tracks the highest [`Hlc`] it holds
//! *per originating origin*, an [`Origin`] being a node and one of its epochs, and
//! two peers exchanging vectors can each work out what to ask for.
//!
//! The key is the origin, never the node alone: a restored node writes under a
//! fresh epoch, and a vector keyed by node would let the new epoch's writes stand
//! on the old epoch's coverage. There is deliberately no lookup by node.
//!
//! This is not a vector clock and does not track causality. It is a summary of
//! coverage — conflict resolution is last-writer-wins on the stamp
//! ([ADR-004](../../../docs/decisions.md)), decided per document, and does not
//! consult this at all.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::hlc::{Hlc, Stamp};
use crate::origin::Origin;

/// The highest stamp held from each originating origin.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionVector(#[serde(with = "keyed_by_wire_string")] BTreeMap<Origin, Hlc>);

/// The map's keys always cross as the origin's wire string (`<node uuid>/<36 hex>`, U5), in every
/// format: a BSON document key is a string whatever the writer reports about itself, and
/// [`Origin`] alone writes bytes where the format is not human-readable.
mod keyed_by_wire_string {
    use std::collections::BTreeMap;

    use serde::de::{Deserialize, Deserializer, Error as _};
    use serde::ser::{Serialize, SerializeMap, Serializer};

    use super::{Hlc, Origin};

    struct Key<'a>(&'a Origin);

    impl Serialize for Key<'_> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.collect_str(self.0)
        }
    }

    pub(super) fn serialize<S: Serializer>(
        map: &BTreeMap<Origin, Hlc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut out = serializer.serialize_map(Some(map.len()))?;
        for (origin, hlc) in map {
            out.serialize_entry(&Key(origin), hlc)?;
        }
        out.end()
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<Origin, Hlc>, D::Error> {
        let raw = BTreeMap::<String, Hlc>::deserialize(deserializer)?;
        raw.into_iter()
            .map(|(key, hlc)| Ok((key.parse::<Origin>().map_err(D::Error::custom)?, hlc)))
            .collect()
    }
}

impl VersionVector {
    pub fn new() -> Self {
        Self::default()
    }

    /// The highest HLC held from `origin`, or [`Hlc::ZERO`] if none. An origin never seen
    /// reads as zero: its epoch's start HLC is **not** read here (see [`Self::behind`]).
    pub fn get(&self, origin: Origin) -> Hlc {
        self.0.get(&origin).copied().unwrap_or(Hlc::ZERO)
    }

    /// Record that `stamp` is held. Keeps the maximum per origin.
    pub fn observe(&mut self, stamp: Stamp) {
        let slot = self.0.entry(stamp.origin()).or_insert(Hlc::ZERO);
        if stamp.hlc > *slot {
            *slot = stamp.hlc;
        }
    }

    pub fn insert(&mut self, origin: Origin, hlc: Hlc) {
        self.0.insert(origin, hlc);
    }

    pub fn iter(&self) -> impl Iterator<Item = (Origin, Hlc)> + '_ {
        self.0.iter().map(|(origin, hlc)| (*origin, *hlc))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// The lowest HLC a peer must send from for this node to catch up to
    /// `theirs`, or `None` if nothing is missing.
    ///
    /// # Why one threshold rather than a range per origin
    ///
    /// The oplog is keyed by `(hlc, node, epoch)`, so it sorts by time first — asking
    /// for "everything from origin O after H" would be a full scan and filter.
    /// Asking for "everything after H", where H is the lowest point we are
    /// behind at, is a single range read.
    ///
    /// The cost is over-fetching entries we already hold. That is deliberately
    /// cheap: applying one is idempotent — `apply_remote` requires the incoming
    /// stamp to *strictly* win, so a re-delivered entry is compared and
    /// discarded without touching the document or republishing an event.
    ///
    /// # The start-HLC rule (an optimisation only)
    ///
    /// An origin this vector has never witnessed contributes the start HLC of its epoch
    /// to the threshold instead of zero: no entry of an epoch is older than its start,
    /// so a fresh epoch anywhere would otherwise drag the threshold to zero and make the
    /// next window the whole oplog. This is the **only** place the start HLC is read. A
    /// coverage claim ([`Self::get`], [`Self::observe`], [`Self::covers`], `merge`, `meet`,
    /// and `coverage_up_to` in storage) never reads it, so it cannot raise coverage:
    /// with the rule off the same entries are applied and the same vectors result, only
    /// the first window is larger.
    pub fn behind(&self, theirs: &VersionVector) -> Option<Hlc> {
        theirs
            .iter()
            .filter(|(origin, their_max)| *their_max > self.get(*origin))
            .map(|(origin, _)| {
                self.0.get(&origin).copied().unwrap_or_else(|| origin.epoch.start_hlc())
            })
            .min()
    }

    /// Raise every entry to the maximum of the two vectors.
    ///
    /// Used when a snapshot hands over coverage wholesale. Merging rather than
    /// replacing matters: the receiver may hold writes of its own that the
    /// sender has never seen, and adopting the sender's vector outright would
    /// claim to have forgotten them.
    pub fn merge(&mut self, other: &VersionVector) {
        for (origin, hlc) in other.iter() {
            let slot = self.0.entry(origin).or_insert(Hlc::ZERO);
            if hlc > *slot {
                *slot = hlc;
            }
        }
    }

    /// The highest position both vectors have reached, per origin: the cut at
    /// which two members can be compared (B3). An origin only one of them has
    /// heard of is at zero in the other, so it is absent here.
    pub fn meet(&self, other: &VersionVector) -> VersionVector {
        self.0
            .iter()
            .filter_map(|(origin, hlc)| {
                let low = (*hlc).min(other.get(*origin));
                (low > Hlc::ZERO).then_some((*origin, low))
            })
            .collect()
    }

    /// Whether this vector covers everything `theirs` reports.
    pub fn covers(&self, theirs: &VersionVector) -> bool {
        theirs.iter().all(|(origin, their_max)| their_max <= self.get(origin))
    }
}

impl FromIterator<(Origin, Hlc)> for VersionVector {
    fn from_iter<T: IntoIterator<Item = (Origin, Hlc)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Epoch;
    use crate::ids::NodeId;

    /// A fresh origin: a new node at epoch zero.
    fn node() -> Origin {
        Origin::new(NodeId::generate(), Epoch::ZERO)
    }

    fn st(hlc: Hlc, origin: Origin) -> Stamp {
        Stamp::new(hlc, origin.node, origin.epoch)
    }

    #[test]
    fn an_unseen_node_reads_as_zero() {
        assert_eq!(VersionVector::new().get(node()), Hlc::ZERO);
    }

    #[test]
    fn observing_keeps_the_maximum() {
        let n = node();
        let mut v = VersionVector::new();
        v.observe(st(Hlc::new(10, 0), n));
        v.observe(st(Hlc::new(5, 0), n));
        assert_eq!(v.get(n), Hlc::new(10, 0), "an older stamp must not lower the mark");
    }

    #[test]
    fn the_meet_is_the_lower_position_per_origin_and_drops_an_origin_one_side_lacks() {
        let (a, b, c) = (node(), node(), node());
        let mut mine = VersionVector::new();
        mine.insert(a, Hlc::new(10, 0));
        mine.insert(b, Hlc::new(4, 0));
        mine.insert(c, Hlc::new(9, 0));
        let mut theirs = VersionVector::new();
        theirs.insert(a, Hlc::new(7, 0));
        theirs.insert(b, Hlc::new(8, 0));
        let cut = mine.meet(&theirs);
        assert_eq!(cut.get(a), Hlc::new(7, 0));
        assert_eq!(cut.get(b), Hlc::new(4, 0));
        assert_eq!(cut.get(c), Hlc::ZERO, "an origin the other side lacks is at zero");
        assert_eq!(cut, theirs.meet(&mine), "symmetric");
    }

    #[test]
    fn a_vector_that_covers_another_needs_nothing() {
        let n = node();
        let mut mine = VersionVector::new();
        mine.observe(st(Hlc::new(10, 0), n));
        let mut theirs = VersionVector::new();
        theirs.observe(st(Hlc::new(7, 0), n));

        assert_eq!(mine.behind(&theirs), None);
        assert!(mine.covers(&theirs));
    }

    #[test]
    fn behind_is_pinned_by_the_lowest_trailing_origin() {
        // One threshold, not a range per origin: the resume point is this
        // vector's *own* position at whichever trailing origin it holds least
        // of — even when it is far ahead everywhere else. Coverage that never
        // moves for one origin therefore never moves the threshold at all,
        // which is what `coverage_up_to` in storage exists to prevent
        // (ADR-082). This test pins the semantics that fix compensates for.
        let stuck = node();
        let busy = node();
        let mut mine = VersionVector::new();
        mine.observe(st(Hlc::new(1_000_000, 0), busy));
        let mut theirs = VersionVector::new();
        theirs.observe(st(Hlc::new(50, 0), stuck));
        theirs.observe(st(Hlc::new(2_000_000, 0), busy));

        assert_eq!(mine.behind(&theirs), Some(Hlc::ZERO), "pinned at the unseen origin's floor");
        mine.observe(st(Hlc::new(5_000_000, 0), busy));
        assert_eq!(mine.behind(&theirs), Some(Hlc::ZERO), "and no progress elsewhere moves it");
    }

    #[test]
    fn being_behind_reports_where_to_resume_from() {
        let n = node();
        let mut mine = VersionVector::new();
        mine.observe(st(Hlc::new(4, 0), n));
        let mut theirs = VersionVector::new();
        theirs.observe(st(Hlc::new(9, 0), n));

        assert_eq!(mine.behind(&theirs), Some(Hlc::new(4, 0)));
    }

    #[test]
    fn a_node_never_seen_resumes_from_zero() {
        // Otherwise a peer joining a running cluster would silently skip
        // everything written before it arrived.
        let mut theirs = VersionVector::new();
        theirs.observe(st(Hlc::new(9, 0), node()));

        assert_eq!(VersionVector::new().behind(&theirs), Some(Hlc::ZERO));
    }

    #[test]
    fn the_threshold_is_the_lowest_across_deficient_nodes() {
        // One request has to cover every node we are behind on, so it must
        // start at the earliest of them — starting later would silently skip
        // whatever the further-behind node was missing.
        let (a, b) = (node(), node());
        let mut mine = VersionVector::new();
        mine.observe(st(Hlc::new(100, 0), a));
        mine.observe(st(Hlc::new(3, 0), b));

        let mut theirs = VersionVector::new();
        theirs.observe(st(Hlc::new(200, 0), a));
        theirs.observe(st(Hlc::new(50, 0), b));

        assert_eq!(mine.behind(&theirs), Some(Hlc::new(3, 0)));
    }

    #[test]
    fn a_node_we_are_ahead_on_does_not_drag_the_threshold_down() {
        // We hold more of `a` than they do, so `a` must not pull the request
        // back to a point we have already covered.
        let (a, b) = (node(), node());
        let mut mine = VersionVector::new();
        mine.observe(st(Hlc::new(5, 0), a));
        mine.observe(st(Hlc::new(40, 0), b));

        let mut theirs = VersionVector::new();
        theirs.observe(st(Hlc::new(1, 0), a));
        theirs.observe(st(Hlc::new(90, 0), b));

        assert_eq!(mine.behind(&theirs), Some(Hlc::new(40, 0)));
    }

    #[test]
    fn being_behind_is_not_symmetric() {
        // Each side asks the other for what it lacks; both can be true at once,
        // which is what makes a single exchange converge in both directions.
        let (a, b) = (node(), node());
        let mut x = VersionVector::new();
        x.observe(st(Hlc::new(10, 0), a));
        let mut y = VersionVector::new();
        y.observe(st(Hlc::new(10, 0), b));

        assert!(x.behind(&y).is_some());
        assert!(y.behind(&x).is_some());
    }

    #[test]
    fn merging_takes_the_higher_of_each_entry() {
        let (a, b) = (node(), node());
        let mut mine = VersionVector::new();
        mine.observe(st(Hlc::new(10, 0), a));
        mine.observe(st(Hlc::new(99, 0), b));

        let mut theirs = VersionVector::new();
        theirs.observe(st(Hlc::new(50, 0), a));
        theirs.observe(st(Hlc::new(7, 0), b));

        mine.merge(&theirs);

        assert_eq!(mine.get(a), Hlc::new(50, 0), "the higher of the two must win");
        assert_eq!(mine.get(b), Hlc::new(99, 0), "and merging must never lower one");
    }

    #[test]
    fn merging_keeps_writes_the_other_side_never_saw() {
        // A snapshot hands over the sender's coverage. Adopting it outright
        // would claim the receiver had forgotten its own writes.
        let (mine_node, theirs_node) = (node(), node());
        let mut mine = VersionVector::new();
        mine.observe(st(Hlc::new(5, 0), mine_node));

        let mut theirs = VersionVector::new();
        theirs.observe(st(Hlc::new(80, 0), theirs_node));

        mine.merge(&theirs);

        assert_eq!(mine.get(mine_node), Hlc::new(5, 0), "own writes must survive the merge");
        assert_eq!(mine.get(theirs_node), Hlc::new(80, 0));
    }

    #[test]
    fn a_vector_round_trips_through_json() {
        let mut v = VersionVector::new();
        v.observe(st(Hlc::new(3, 1), node()));
        let text = serde_json::to_string(&v).unwrap();
        assert_eq!(serde_json::from_str::<VersionVector>(&text).unwrap(), v);
    }

    #[test]
    fn a_populated_vector_round_trips_through_bson() {
        // BSON is what crosses the wire between nodes, and it is stricter than
        // JSON: document keys must be strings, and it disagrees with itself
        // about whether it is a human-readable format. An *empty* vector round
        // trips under any encoding, which is why this one is deliberately not.
        let mut v = VersionVector::new();
        v.observe(st(Hlc::new(3, 1), node()));
        v.observe(st(Hlc::new(90, 0), node()));

        let bytes = bson::serialize_to_vec(&v).unwrap();
        assert_eq!(bson::deserialize_from_slice::<VersionVector>(&bytes).unwrap(), v);
    }

    #[test]
    fn a_node_id_round_trips_through_bson() {
        let id = NodeId::generate();
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Wrapper {
            node: NodeId,
        }
        let bytes = bson::serialize_to_vec(&Wrapper { node: id }).unwrap();
        assert_eq!(bson::deserialize_from_slice::<Wrapper>(&bytes).unwrap().node, id);
    }

    /// A restored node's new epoch is a different origin: nothing the old epoch covered is
    /// covered for it, the V4m1 silent miss that keying by node alone would let through.
    #[test]
    fn two_epochs_of_one_node_are_two_origins() {
        let id = NodeId::generate();
        let (old, new) = (Origin::new(id, Epoch::from_u64(1)), Origin::new(id, Epoch::from_u64(2)));
        let mut mine = VersionVector::new();
        mine.observe(st(Hlc::new(100, 0), old));
        let mut theirs = VersionVector::new();
        theirs.observe(st(Hlc::new(50, 0), new));

        assert_eq!(mine.get(old), Hlc::new(100, 0));
        assert_eq!(mine.get(new), Hlc::ZERO, "the old epoch's coverage is not the new one's");
        assert!(!mine.covers(&theirs));
        assert_eq!(mine.len(), 1);
        mine.observe(st(Hlc::new(60, 0), new));
        assert_eq!(mine.len(), 2);
        assert!(mine.covers(&theirs));
        // order: node, then epoch
        let order: Vec<_> = mine.iter().map(|(o, _)| o).collect();
        assert_eq!(order, vec![old, new]);
    }

    fn epoch_started_at(wall_ms: u64) -> Epoch {
        Epoch::new(Hlc::new(wall_ms, 0), [7; 8])
    }

    /// The start-HLC rule: an origin never witnessed contributes its epoch's start to the
    /// threshold, so a fresh epoch does not drag the next window down to zero.
    #[test]
    fn a_never_witnessed_origin_resumes_from_its_epochs_start() {
        let fresh = Origin::new(NodeId::generate(), epoch_started_at(5_000));
        let mut theirs = VersionVector::new();
        theirs.observe(st(Hlc::new(9_000, 3), fresh));

        assert_eq!(VersionVector::new().behind(&theirs), Some(Hlc::new(5_000, 0)));

        // an origin that has an entry resumes from its own position, whatever its start
        let mut mine = VersionVector::new();
        mine.observe(st(Hlc::new(6_000, 0), fresh));
        assert_eq!(mine.behind(&theirs), Some(Hlc::new(6_000, 0)));

        // the lowest of several still wins, the start counted as that origin's position
        let low = Origin::new(NodeId::generate(), epoch_started_at(2_000));
        theirs.observe(st(Hlc::new(8_000, 0), low));
        assert_eq!(VersionVector::new().behind(&theirs), Some(Hlc::new(2_000, 0)));

        // an epoch with no start (zero, or built from an integer) is the old zero floor
        let none = Origin::new(NodeId::generate(), Epoch::from_u64(4));
        theirs.observe(st(Hlc::new(1, 0), none));
        assert_eq!(VersionVector::new().behind(&theirs), Some(Hlc::ZERO));
    }

    /// No coverage test reads the start HLC: with the rule in force every coverage answer is
    /// the one a vector with no such rule would give. A coverage claim raised by a start HLC
    /// would skip honest entries below it.
    #[test]
    fn no_coverage_test_reads_the_start_hlc() {
        let started = Origin::new(NodeId::generate(), epoch_started_at(5_000));
        let blank = VersionVector::new();

        // `get` of a never-witnessed origin is zero, not its start
        assert_eq!(blank.get(started), Hlc::ZERO);

        // an advertised position below the start is still behind, and covers says so
        let mut theirs = VersionVector::new();
        theirs.insert(started, Hlc::new(100, 0));
        assert!(!blank.covers(&theirs), "covers does not credit the start");
        assert!(blank.behind(&theirs).is_some());

        // `observe` and `merge` raise to the stamp or the vector, never to the start
        let mut seen = VersionVector::new();
        seen.observe(st(Hlc::new(10, 0), started));
        assert_eq!(seen.get(started), Hlc::new(10, 0));
        let mut merged = VersionVector::new();
        merged.merge(&theirs);
        assert_eq!(merged.get(started), Hlc::new(100, 0));

        // `meet` drops an origin the other side lacks; it does not credit the start
        assert!(blank.meet(&theirs).is_empty());
        assert!(theirs.meet(&blank).is_empty());

        // the same answers with an epoch that has no start: the rule is invisible to coverage
        let plain = Origin::new(started.node, Epoch::from_u64(9));
        let mut theirs_plain = VersionVector::new();
        theirs_plain.insert(plain, Hlc::new(100, 0));
        assert_eq!(blank.covers(&theirs), blank.covers(&theirs_plain));
        assert_eq!(blank.get(started), blank.get(plain));
    }

    #[test]
    fn a_vector_of_epochs_round_trips_through_bson_and_json_keyed_by_the_wire_string() {
        let id = NodeId::generate();
        let mut v = VersionVector::new();
        v.insert(Origin::new(id, Epoch::from_u64(1)), Hlc::new(3, 1));
        v.insert(Origin::new(id, epoch_started_at(77)), Hlc::new(90, 0));
        let bytes = bson::serialize_to_vec(&v).unwrap();
        assert_eq!(bson::deserialize_from_slice::<VersionVector>(&bytes).unwrap(), v);
        let doc: bson::Document = bson::deserialize_from_slice(&bytes).unwrap();
        for (origin, _) in v.iter() {
            assert!(doc.contains_key(origin.to_string()), "keyed by {origin}");
        }
        let text = serde_json::to_string(&v).unwrap();
        assert_eq!(serde_json::from_str::<VersionVector>(&text).unwrap(), v);
        // a key that is not an origin's wire form is refused, not read as another origin
        assert!(serde_json::from_str::<VersionVector>(&format!("{{\"{id}\": {{}}}}")).is_err());
    }

    /// Switching the start-HLC rule off changes the size of the first window and nothing else.
    /// A small pull loop over a log: the requester asks from `behind()`'s threshold, takes the
    /// entries at or above it, and raises its vector by what it took (and, once the window
    /// reaches the tail, to the sender's vector). Run once with the epochs the engine mints (a
    /// start HLC) and once with the same origins under start-less epochs: the final vectors
    /// agree, the entries taken agree, and only the first window differs.
    #[test]
    fn the_start_hlc_rule_changes_the_first_window_and_not_the_final_vectors() {
        let node = NodeId::from_bytes([3; 16]);
        // an old epoch with a long history, and a restore's new epoch started at 9_000
        let old = (Origin::new(node, Epoch::from_u64(1)), Origin::new(node, Epoch::from_u64(2)));
        let started = Origin::new(node, Epoch::new(Hlc::new(9_000, 0), [4; 8]));
        let log = |new: Origin, old: Origin| -> Vec<Stamp> {
            let mut log: Vec<Stamp> = (1..=50u64)
                .map(|n| Stamp::new(Hlc::new(n * 100, 0), old.node, old.epoch))
                .collect();
            log.extend((1..=3u64).map(|n| Stamp::new(Hlc::new(9_000 + n, 0), new.node, new.epoch)));
            log.sort();
            log
        };
        let pull = |new: Origin, old: Origin| -> (VersionVector, Vec<Stamp>, usize) {
            let log = log(new, old);
            let mut theirs = VersionVector::new();
            for stamp in &log {
                theirs.observe(*stamp);
            }
            // the requester already holds the old epoch's history
            let mut mine = VersionVector::new();
            for stamp in log.iter().filter(|s| s.origin() == old) {
                mine.observe(*stamp);
            }
            let (mut taken, mut first) = (Vec::new(), None);
            while let Some(from) = mine.behind(&theirs) {
                let window: Vec<Stamp> = log.iter().filter(|s| s.hlc >= from).copied().collect();
                first.get_or_insert(window.len());
                for stamp in &window {
                    mine.observe(*stamp);
                    taken.push(*stamp);
                }
                mine.merge(&theirs);
            }
            (mine, taken, first.unwrap_or(0))
        };
        let (with_rule, taken_with, first_with) = pull(started, old.0);
        // the same origins, the new one with an epoch that carries no start
        let unstarted = Origin::new(node, Epoch::from_u64(7));
        let (without, taken_without, first_without) = pull(unstarted, old.0);

        assert_eq!(first_with, 3, "the rule asks from the epoch's start: the three new entries");
        assert_eq!(first_without, 53, "without it the threshold is zero: the whole log");
        // the same entries (as stamps of the same hlc) were taken, and the vectors agree
        let hlcs = |taken: &[Stamp]| -> std::collections::BTreeSet<Hlc> {
            taken.iter().filter(|s| s.hlc.wall_ms >= 9_000).map(|s| s.hlc).collect()
        };
        assert_eq!(hlcs(&taken_with), hlcs(&taken_without));
        let positions = |v: &VersionVector| -> Vec<Hlc> { v.iter().map(|(_, h)| h).collect() };
        assert_eq!(positions(&with_rule), positions(&without));
        assert_eq!(with_rule.get(started), Hlc::new(9_003, 0));
        assert_eq!(without.get(unstarted), Hlc::new(9_003, 0));
    }
}
