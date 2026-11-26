//! D13 containment (ADR-274): the newest epoch of each node this member has heard of, and the halt
//! of a host that is told a newer epoch of its own node exists.
//!
//! **This file is the scaffold of the tests commit and nothing else**: each function compiles and
//! does nothing (a table that records no row, a halt that halts nothing), so that every test in
//! `containment_tests.rs` and in the cluster crate is red on its designed assertion and none on a
//! missing symbol. The code commits replace each body, and none of these stubs survives to the last
//! commit (a source scan asserts it).

use kimmy_core::{Epoch, NodeId};

use crate::{Engine, Result};

/// What a halted host knows of why: the epoch it halts (its own), and the newer epoch of its node
/// it was told of. `told` is `None` for a halt by the mistaken-restore guard, which has no newer
/// epoch to name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Halt {
    pub halted: Epoch,
    pub told: Option<Epoch>,
}

impl Engine {
    /// The newest epoch of `node` this member has recorded: the grow-only table's row, or the
    /// node's newest origin in the version vectors, whichever is newer. `Epoch::ZERO` for a node it
    /// has never heard of.
    pub fn newest_epoch(&self, _node: NodeId) -> Result<Epoch> {
        Ok(Epoch::ZERO)
    }

    /// Record that `epoch` is an epoch of `node`: the row becomes the larger of the two, and never
    /// falls.
    pub fn note_epoch(&self, _node: NodeId, _epoch: Epoch) -> Result<()> {
        Ok(())
    }

    /// Halt this host: it was told that `told`, a newer epoch of its own node, exists. The mirror
    /// is set first, then the writer gate is taken, then the halt row (naming this member's own
    /// epoch) is committed durably.
    pub fn halt(&self, _told: Epoch) -> Result<()> {
        Ok(())
    }

    /// The halt this store holds for its own epoch, if any: `None` when it was never halted, and
    /// `None` when the row names another epoch (a backup of a halted host, restored).
    pub fn halted(&self) -> Option<Halt> {
        None
    }
}
