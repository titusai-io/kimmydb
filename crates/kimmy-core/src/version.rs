//! Version vectors: how far each node has been observed.
//!
//! A version vector answers "what have you got that I haven't?" without either
//! side sending its whole log. Each node tracks the highest [`Hlc`] it holds
//! *per originating node*, and two peers exchanging vectors can each work out
//! what to ask for.
//!
//! This is not a vector clock and does not track causality. It is a summary of
//! coverage — conflict resolution is last-writer-wins on the stamp
//! ([ADR-004](../../../docs/decisions.md)), decided per document, and does not
//! consult this at all.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::hlc::{Hlc, Stamp};
use crate::ids::NodeId;

/// The highest stamp held from each originating node.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionVector(BTreeMap<NodeId, Hlc>);

impl VersionVector {
    pub fn new() -> Self {
        Self::default()
    }

    /// The highest HLC held from `node`, or [`Hlc::ZERO`] if none.
    pub fn get(&self, node: NodeId) -> Hlc {
        self.0.get(&node).copied().unwrap_or(Hlc::ZERO)
    }

    /// Record that `stamp` is held. Keeps the maximum per node.
    pub fn observe(&mut self, stamp: Stamp) {
        let slot = self.0.entry(stamp.node).or_insert(Hlc::ZERO);
        if stamp.hlc > *slot {
            *slot = stamp.hlc;
        }
    }

    pub fn insert(&mut self, node: NodeId, hlc: Hlc) {
        self.0.insert(node, hlc);
    }

    pub fn iter(&self) -> impl Iterator<Item = (NodeId, Hlc)> + '_ {
        self.0.iter().map(|(node, hlc)| (*node, *hlc))
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
    /// # Why one threshold rather than a range per node
    ///
    /// The oplog is keyed by `(hlc, node)`, so it sorts by time first — asking
    /// for "everything from node N after H" would be a full scan and filter.
    /// Asking for "everything after H", where H is the lowest point we are
    /// behind at, is a single range read.
    ///
    /// The cost is over-fetching entries we already hold. That is deliberately
    /// cheap: applying one is idempotent — `apply_remote` requires the incoming
    /// stamp to *strictly* win, so a re-delivered entry is compared and
    /// discarded without touching the document or republishing an event.
    pub fn behind(&self, theirs: &VersionVector) -> Option<Hlc> {
        theirs
            .iter()
            .filter(|(node, their_max)| *their_max > self.get(*node))
            .map(|(node, _)| self.get(node))
            .min()
    }

    /// Raise every entry to the maximum of the two vectors.
    ///
    /// Used when a snapshot hands over coverage wholesale. Merging rather than
    /// replacing matters: the receiver may hold writes of its own that the
    /// sender has never seen, and adopting the sender's vector outright would
    /// claim to have forgotten them.
    pub fn merge(&mut self, other: &VersionVector) {
        for (node, hlc) in other.iter() {
            let slot = self.0.entry(node).or_insert(Hlc::ZERO);
            if hlc > *slot {
                *slot = hlc;
            }
        }
    }

    /// Whether this vector covers everything `theirs` reports.
    pub fn covers(&self, theirs: &VersionVector) -> bool {
        self.behind(theirs).is_none()
    }
}

impl FromIterator<(NodeId, Hlc)> for VersionVector {
    fn from_iter<T: IntoIterator<Item = (NodeId, Hlc)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node() -> NodeId {
        NodeId::generate()
    }

    #[test]
    fn an_unseen_node_reads_as_zero() {
        assert_eq!(VersionVector::new().get(node()), Hlc::ZERO);
    }

    #[test]
    fn observing_keeps_the_maximum() {
        let n = node();
        let mut v = VersionVector::new();
        v.observe(Stamp::new(Hlc::new(10, 0), n));
        v.observe(Stamp::new(Hlc::new(5, 0), n));
        assert_eq!(v.get(n), Hlc::new(10, 0), "an older stamp must not lower the mark");
    }

    #[test]
    fn a_vector_that_covers_another_needs_nothing() {
        let n = node();
        let mut mine = VersionVector::new();
        mine.observe(Stamp::new(Hlc::new(10, 0), n));
        let mut theirs = VersionVector::new();
        theirs.observe(Stamp::new(Hlc::new(7, 0), n));

        assert_eq!(mine.behind(&theirs), None);
        assert!(mine.covers(&theirs));
    }

    #[test]
    fn being_behind_reports_where_to_resume_from() {
        let n = node();
        let mut mine = VersionVector::new();
        mine.observe(Stamp::new(Hlc::new(4, 0), n));
        let mut theirs = VersionVector::new();
        theirs.observe(Stamp::new(Hlc::new(9, 0), n));

        assert_eq!(mine.behind(&theirs), Some(Hlc::new(4, 0)));
    }

    #[test]
    fn a_node_never_seen_resumes_from_zero() {
        // Otherwise a peer joining a running cluster would silently skip
        // everything written before it arrived.
        let mut theirs = VersionVector::new();
        theirs.observe(Stamp::new(Hlc::new(9, 0), node()));

        assert_eq!(VersionVector::new().behind(&theirs), Some(Hlc::ZERO));
    }

    #[test]
    fn the_threshold_is_the_lowest_across_deficient_nodes() {
        // One request has to cover every node we are behind on, so it must
        // start at the earliest of them — starting later would silently skip
        // whatever the further-behind node was missing.
        let (a, b) = (node(), node());
        let mut mine = VersionVector::new();
        mine.observe(Stamp::new(Hlc::new(100, 0), a));
        mine.observe(Stamp::new(Hlc::new(3, 0), b));

        let mut theirs = VersionVector::new();
        theirs.observe(Stamp::new(Hlc::new(200, 0), a));
        theirs.observe(Stamp::new(Hlc::new(50, 0), b));

        assert_eq!(mine.behind(&theirs), Some(Hlc::new(3, 0)));
    }

    #[test]
    fn a_node_we_are_ahead_on_does_not_drag_the_threshold_down() {
        // We hold more of `a` than they do, so `a` must not pull the request
        // back to a point we have already covered.
        let (a, b) = (node(), node());
        let mut mine = VersionVector::new();
        mine.observe(Stamp::new(Hlc::new(5, 0), a));
        mine.observe(Stamp::new(Hlc::new(40, 0), b));

        let mut theirs = VersionVector::new();
        theirs.observe(Stamp::new(Hlc::new(1, 0), a));
        theirs.observe(Stamp::new(Hlc::new(90, 0), b));

        assert_eq!(mine.behind(&theirs), Some(Hlc::new(40, 0)));
    }

    #[test]
    fn being_behind_is_not_symmetric() {
        // Each side asks the other for what it lacks; both can be true at once,
        // which is what makes a single exchange converge in both directions.
        let (a, b) = (node(), node());
        let mut x = VersionVector::new();
        x.observe(Stamp::new(Hlc::new(10, 0), a));
        let mut y = VersionVector::new();
        y.observe(Stamp::new(Hlc::new(10, 0), b));

        assert!(x.behind(&y).is_some());
        assert!(y.behind(&x).is_some());
    }

    #[test]
    fn merging_takes_the_higher_of_each_entry() {
        let (a, b) = (node(), node());
        let mut mine = VersionVector::new();
        mine.observe(Stamp::new(Hlc::new(10, 0), a));
        mine.observe(Stamp::new(Hlc::new(99, 0), b));

        let mut theirs = VersionVector::new();
        theirs.observe(Stamp::new(Hlc::new(50, 0), a));
        theirs.observe(Stamp::new(Hlc::new(7, 0), b));

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
        mine.observe(Stamp::new(Hlc::new(5, 0), mine_node));

        let mut theirs = VersionVector::new();
        theirs.observe(Stamp::new(Hlc::new(80, 0), theirs_node));

        mine.merge(&theirs);

        assert_eq!(mine.get(mine_node), Hlc::new(5, 0), "own writes must survive the merge");
        assert_eq!(mine.get(theirs_node), Hlc::new(80, 0));
    }

    #[test]
    fn a_vector_round_trips_through_json() {
        let mut v = VersionVector::new();
        v.observe(Stamp::new(Hlc::new(3, 1), node()));
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
        v.observe(Stamp::new(Hlc::new(3, 1), node()));
        v.observe(Stamp::new(Hlc::new(90, 0), node()));

        let bytes = bson::serialize_to_vec(&v).unwrap();
        assert_eq!(bson::deserialize_from_slice::<VersionVector>(&bytes).unwrap(), v);
    }

    #[test]
    fn a_node_id_round_trips_through_bson() {
        let id = node();
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Wrapper {
            node: NodeId,
        }
        let bytes = bson::serialize_to_vec(&Wrapper { node: id }).unwrap();
        assert_eq!(bson::deserialize_from_slice::<Wrapper>(&bytes).unwrap().node, id);
    }
}
