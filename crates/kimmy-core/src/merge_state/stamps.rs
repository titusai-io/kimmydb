//! What a state records, read by stamp.
//!
//! The snapshot pull judges a row of a `field` document by what its state holds, not by the stamp of the
//! record, which is only the newest write: [`MergeState::alive_lives`] says which lives a row would bring
//! back to life.

use super::state::MergeState;
use super::types::MStamp;

impl MergeState {
    /// The stamps of the lives of the state that are not marked dead: the lives a read can render and a
    /// member that takes the state in brings back.
    pub fn alive_lives(&self) -> impl Iterator<Item = MStamp> + '_ {
        self.lives.keys().filter(|life| !self.dead.contains(*life)).copied()
    }
}
