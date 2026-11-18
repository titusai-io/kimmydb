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
//! **Not here: loss entries.** No loss-entry kind exists yet (Step 4). When Step 4 adds one it MUST extend this
//! function to apply a loss entry at its key position through a `MergeState` form of `Writer::receive_loss`,
//! extend the rewind plan to collect them, and re-run the restore-twice test (L26) and the property test of
//! design 5.2(iii). The records of a state this function returns are those the joins derive.

use crate::hlc::Hlc;

use super::join::{JoinOptions, join};
use super::state::MergeState;
use super::types::MStamp;

/// The state of a document at `target`: the fold, in ascending `MStamp` order, of the states of the entries
/// stamped at or below it, each passed through [`MergeState::drop_at_or_below`] at `floor` first when there
/// is one (the collection's incarnation floor, the one filter the apply path uses).
///
/// `entries` pairs an entry's stamp with the state it carries, in any order; an entry above `target` is not
/// read, and an entry listed twice under one stamp is joined once. The empty input gives the empty state.
///
/// No cell and no version-vector entry of the result is stamped above `target`: an entry's state holds only
/// stamps its writer had seen, and a writer's clock is above every stamp it has seen.
///
/// A debug build asserts that no entry's state is already covered by the accumulator (its version vector at
/// or below the accumulator's for every writer): in ascending order the accumulator holds only lower stamps,
/// none of which can have seen the entry's own, so a covered entry means the history is not causally closed.
pub fn state_at<'a, I>(entries: I, target: Hlc, floor: Option<Hlc>, o: &JoinOptions) -> MergeState
where
    I: IntoIterator<Item = (MStamp, &'a MergeState)>,
{
    let mut chosen: Vec<(MStamp, &MergeState)> =
        entries.into_iter().filter(|(stamp, _)| stamp.hlc <= target).collect();
    chosen.sort_by_key(|(stamp, _)| *stamp);
    chosen.dedup_by_key(|(stamp, _)| *stamp);
    let mut acc = MergeState::default();
    for (stamp, meta) in chosen {
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
    }
    acc
}
