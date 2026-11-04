//! The identity of a writer incarnation: [`Epoch`] and [`Origin`].
//!
//! An [`Origin`] is a node and one of its epochs. Everything that orders, covers or keys writes
//! by "who wrote them" does so by origin, never by node alone: a restored node takes a fresh
//! epoch, and its new writes must not be mistaken for the writes the restore threw away.

use crate::hlc::Hlc;
use crate::ids::NodeId;

/// The incarnation part of a writer identity: 18 bytes, in production the
/// 10-byte HLC at the writer's start followed by 8 random bytes. A restored
/// node takes a fresh epoch, so its new writes never collide with the writes
/// the restore threw away. Tests build epochs from integers.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Epoch(pub [u8; Epoch::LEN]);

impl Epoch {
    pub const LEN: usize = 18;

    /// A start HLC followed by 8 random bytes.
    pub fn new(start: Hlc, random: [u8; 8]) -> Self {
        let mut b = [0u8; Self::LEN];
        b[..10].copy_from_slice(&start.to_bytes());
        b[10..].copy_from_slice(&random);
        Self(b)
    }

    /// A small epoch for tests and models: `n` in the low 8 bytes.
    pub fn from_u64(n: u64) -> Self {
        let mut b = [0u8; Self::LEN];
        b[10..].copy_from_slice(&n.to_be_bytes());
        Self(b)
    }
}

/// One writer incarnation: the node and its epoch. Part of every ordering.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Origin {
    pub node: NodeId,
    pub epoch: Epoch,
}
