//! What a state records, read by stamp.
//!
//! The snapshot pull judges a row of a `field` document by what its state holds, not by the stamp of the
//! record, which is only the newest write: [`MergeState::alive_lives`] says which lives a row would bring
//! back to life, and [`MergeState::write_stamps`] names the writes it holds, as the cut of a repair is
//! counted against.

use super::state::MergeState;
use super::types::MStamp;

impl MergeState {
    /// The stamps of the lives of the state that are not marked dead: the lives a read can render and a
    /// member that takes the state in brings back.
    pub fn alive_lives(&self) -> impl Iterator<Item = MStamp> + '_ {
        self.lives.keys().filter(|life| !self.dead.contains(*life)).copied()
    }

    /// The stamp of every write the state records: its lives, its deletes, its registers and its counter
    /// slots, a stamp more than once where several cells carry it. The version vector is not read: it says
    /// what the state has seen, which is more than it holds once the floor has dropped cells, and the
    /// question is what a member that takes the state in would be given.
    pub fn write_stamps(&self) -> impl Iterator<Item = MStamp> + '_ {
        self.lives
            .keys()
            .chain(self.deletes.keys())
            .chain(self.regs.values().map(|reg| &reg.stamp))
            .chain(self.slots.values().map(|slot| &slot.stamp))
            .copied()
    }
}
