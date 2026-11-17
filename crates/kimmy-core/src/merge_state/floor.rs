//! The incarnation floor as an operation on a state.
//!
//! A collection that is dropped and created again stands over a floor: the stamp of the drop. What a
//! document write stamped at or below it belongs to the life before and is history; what is stamped
//! after it is kept, however much of the old life it saw. A `field` document's state is cumulative,
//! so one state can hold both: [`MergeState::drop_at_or_below`] keeps the cells stamped above the
//! floor and drops the rest, and the judgement of a whole state by its newest stamp is left to the
//! caller's cheap pre-check.
//!
//! The cell is the unit. A register is judged by its stamp. A counter slot is one cell too, judged by
//! the stamp of its last increment: a slot stamped above the floor is a write after the drop and is
//! kept whole, as a `document` record written after the drop is kept whole, the fields it saw
//! included.
//!
//! Two members can meet this at different times: one holds a life dead by a delete at or below the floor
//! before it knew the floor, another only ever saw the states already thinned. The first drops the delete
//! and the cells it killed when it learns the floor and holds nothing of the life; the second holds the
//! life alive, with the cells stamped after the floor. They converge on exchange, and what wins is the
//! life ALIVE with the cells after the floor: a delete at or below the floor is history, and a write
//! after the floor is kept (ruling (a)), so a delete cannot kill it. A delete above the floor still
//! kills every life that had not seen it. The divergence is transient and resolves to alive.
//!
//! This is not an operation of the reference model; it is bounded by the properties the tests state
//! (idempotent, leaves the version vector alone, leaves a state `decode` accepts).

use std::collections::BTreeSet;

use super::ord::{FieldOrd, OrdAt};
use super::state::MergeState;
use super::types::{MStamp, Path};
use crate::hlc::Hlc;

impl MergeState {
    /// Drop everything of the state stamped at or below `floor` (`Hlc`, not strict: a tie goes to the
    /// drop, ADR-236). `true` when it dropped anything.
    ///
    /// - A register and a counter slot go when their stamp is at or below the floor, except the register of
    ///   `_id` (and its place in the order) of a life that stays: it is the document's identity, not a cell of the
    ///   life before, and is no reason to keep a life.
    /// - A delete goes when its stamp is at or below the floor: a delete of the life before must not
    ///   kill a life the drop did not leave it. A surviving life's `seen` still names it, which
    ///   `decode` accepts (every stamp there is covered by the version vector, and nothing relates
    ///   `seen` to the deletes held).
    /// - A life goes when it was created at or below the floor and none of its cells is left, with
    ///   its mark in `dead`. A life created at or below the floor that a write after the floor
    ///   reached stays, with the cells above the floor.
    /// - The order state is judged by the stamps it holds, as the cells are: a candidate position, a
    ///   void mark or a level mark stamped at or below the floor goes, and one above it stays, so a
    ///   field a write after the floor added to a life made before it keeps its place. An entry left
    ///   with nothing goes, and so does the order state of a life that is gone.
    /// - A loss record goes when the write it records is at or below the floor.
    /// - The version vector stays: it says what the state has seen, and the filter does not unsee it.
    pub fn drop_at_or_below(&mut self, floor: Hlc) -> bool {
        let before = self.sizes();
        // `_id` is the document's identity, equal in every life and fixed by the key, and not a cell of the
        // life before: it stays with every life that stays, and goes with a life that goes. It is not a
        // reason to keep a life by itself.
        let identity = Path::flat("_id").expect("`_id` is a path");
        self.regs.retain(|(_, path), reg| reg.stamp.hlc > floor || *path == identity);
        self.slots.retain(|_, slot| slot.stamp.hlc > floor);
        self.deletes.retain(|stamp, _| stamp.hlc > floor);
        let held: BTreeSet<MStamp> = self
            .regs
            .iter()
            .filter(|(_, r)| r.stamp.hlc > floor)
            .map(|(k, _)| k.0)
            .chain(self.slots.keys().map(|k| k.0))
            .collect();
        self.lives.retain(|life, _| life.hlc > floor || held.contains(life));
        self.regs.retain(|(life, _), _| self.lives.contains_key(life));
        self.dead.retain(|life| self.lives.contains_key(life));
        self.gens.retain(|key, _| self.lives.contains_key(&key.0));
        self.ords.retain(|(life, _), _| self.lives.contains_key(life));
        for ((_, at), ord) in self.ords.iter_mut() {
            if *at != OrdAt::Field(identity.clone()) {
                thin(ord, floor);
            }
        }
        self.ords.retain(|_, ord| !ord.is_empty());
        self.losses.retain(|key, _| key.stamp.hlc > floor);
        self.sizes() != before
    }

    /// Whether the state holds no life and no delete: what remains of a state that was all history.
    pub fn is_bare(&self) -> bool {
        self.lives.is_empty() && self.deletes.is_empty()
    }

    fn sizes(&self) -> [usize; 8] {
        [
            self.lives.len(),
            self.deletes.len(),
            self.dead.len(),
            self.regs.len(),
            self.slots.len(),
            self.gens.len(),
            self.ords.len(),
            self.losses.len(),
        ]
    }
}

/// The order state of one path without what is stamped at or below the floor. A candidate is above
/// the floor or gone, as `last` is the newest of them, so no standing first outlives it; a void mark
/// at or below the floor voids only candidates below it, which are gone already.
fn thin(ord: &mut FieldOrd, floor: Hlc) {
    ord.firsts.retain(|_, cand| cand.0.hlc > floor);
    if ord.last.is_some_and(|cand| cand.0.hlc <= floor) {
        ord.last = None;
    }
    if ord.floor.is_some_and(|f| f.stamp().hlc <= floor) {
        ord.floor = None;
    }
    if ord.lvl.is_some_and(|l| l.hlc <= floor) {
        ord.lvl = None;
    }
}
