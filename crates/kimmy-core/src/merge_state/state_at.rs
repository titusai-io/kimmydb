//! A document's state at a point in time (the field rewind, design q7-step3-rewind-design.md).
//!
//! An oplog entry of a `field` collection carries the writer's whole state of the document at that
//! write. The state at a target `T` is the join of the states of the entries stamped at or before it,
//! folded **in ascending stamp order** `(hlc, node, epoch)` from the empty state, each through the
//! incarnation floor first. The order is the definition and not a convenience: the join is not
//! associative once the spread of a history passes the retention `R` (a life the horizon dropped
//! returns when an early state that still lists it is joined late), so a fold that depended on arrival
//! order would give two members that hold the same entries different states. In ascending order the
//! accumulator holds only stamps below the entry it joins, so no entry is joined into a state later
//! than itself and equal entry sets give equal states on every member.
//!
//! **Loss entries.** A loss entry is applied at its key position through [`MergeState::receive_loss_floored`], the rule
//! the live apply uses (the floor, then the horizon, the known key, the cut and the cap, all by the options given), so
//! the state at a target is what the live apply of the same entries, in key order, gives. One that meets a state with no
//! life and no delete is parked, as the live apply parks it, and applied after the next state entry that gives the
//! fold a row. Nothing here reads a clock: a record the live apply expired by the member's wall clock while it was
//! parked may be kept here (an allowed extra), and only the state's own horizon drops a record. The records of a
//! state this function returns are those the joins derive plus the loss entries at or below the target.

use crate::hlc::Hlc;

use std::cmp::Ordering;

use super::join::{JoinOptions, join};
use super::state::MergeState;
use super::types::{LossKey, LossValue, MStamp};

/// An oplog entry of a `field` document as the fold reads it: the state a write carries, or a loss record.
#[derive(Clone, Debug)]
pub enum StateAtEntry<'a> {
    /// The merge state the entry carries.
    Meta(&'a MergeState),
    /// A loss-record entry: the record, applied at the entry's own position.
    Loss(LossKey, LossValue),
}

impl<'a> From<&'a MergeState> for StateAtEntry<'a> {
    fn from(state: &'a MergeState) -> Self {
        Self::Meta(state)
    }
}

impl StateAtEntry<'_> {
    /// The order of entries that share a stamp: a state before a record, then records by key.
    fn order(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Meta(_), Self::Meta(_)) => Ordering::Equal,
            (Self::Meta(_), Self::Loss(..)) => Ordering::Less,
            (Self::Loss(..), Self::Meta(_)) => Ordering::Greater,
            (Self::Loss(a, _), Self::Loss(b, _)) => a.cmp(b),
        }
    }
}

/// The state of a document at `target`: the fold, in ascending `MStamp` order, of the entries stamped at or below it
/// from the empty state. A state entry is passed through [`MergeState::drop_at_or_below`] at `floor` first when there
/// is one (the collection's incarnation floor, the one filter the apply path uses) and joined; a loss entry goes
/// through [`MergeState::receive_loss_floored`] with the same floor.
///
/// `o` must be the options the live apply of the collection uses (`loss_cap` and `loss_cut` included): the cap and the
/// cut change which records a state holds. `entries` are in any order; an entry above `target` is not read, an entry
/// listed twice (the same stamp, and for a loss entry the same key) is read once, and a state and a loss entry that
/// share a stamp are both read, the state first. The empty input gives the empty state.
///
/// No cell and no version-vector entry of the result is stamped above `target`: an entry's state holds only
/// stamps its writer had seen, and a writer's clock is above every stamp it has seen. A loss entry moves no version
/// vector.
///
/// A debug build asserts that no state entry is already covered by the accumulator (its version vector at
/// or below the accumulator's for every writer): in ascending order the accumulator holds only lower stamps,
/// none of which can have seen the entry's own, so a covered entry means the history is not causally closed.
pub fn state_at<'a, I>(entries: I, target: Hlc, floor: Option<Hlc>, o: &JoinOptions) -> MergeState
where
    I: IntoIterator<Item = (MStamp, StateAtEntry<'a>)>,
{
    let mut chosen: Vec<(MStamp, StateAtEntry<'a>)> =
        entries.into_iter().filter(|(stamp, _)| stamp.hlc <= target).collect();
    chosen.sort_by(|(a, x), (b, y)| a.cmp(b).then_with(|| x.order(y)));
    chosen.dedup_by(|(b, y), (a, x)| a == b && x.order(y) == Ordering::Equal);
    let mut acc = MergeState::default();
    // The records that met a state with no life and no delete: applied after the next state that gives the fold a row.
    let mut parked: Vec<(LossKey, LossValue)> = Vec::new();
    for (stamp, entry) in chosen {
        match entry {
            StateAtEntry::Meta(meta) => {
                let mut state = meta.clone();
                if let Some(floor) = floor {
                    state.drop_at_or_below(floor);
                }
                debug_assert!(
                    state.vv.is_empty()
                        || state
                            .vv
                            .iter()
                            .any(|(writer, hlc)| acc.vv.get(writer).is_none_or(|held| held < hlc)),
                    "the entry at {stamp:?} carries a state the fold already covers: the history is not causally closed"
                );
                acc = join(&acc, &state, o).0;
                if !acc.is_bare() {
                    for (key, val) in parked.drain(..) {
                        acc.receive_loss_floored(key, val, floor, o);
                    }
                }
            }
            StateAtEntry::Loss(key, val) if acc.is_bare() => parked.push((key, val)),
            StateAtEntry::Loss(key, val) => {
                acc.receive_loss_floored(key, val, floor, o);
            }
        }
    }
    acc
}
