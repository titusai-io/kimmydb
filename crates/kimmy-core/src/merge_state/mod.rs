//! Q7 field-level merge: the replicated state, its join, the local write
//! operations, the visible document, the hidden figure and the state hash.
//!
//! This is a port of the reviewed reference model (`model5.py` and the
//! `Node.finish` normalisation of `pullmodel5.py`). It is pure: no I/O, no
//! async, no global state. Field paths are flat strings. Nothing here is wired
//! into storage, the API or the oplog.
//!
//! The pieces:
//!
//! * [`MergeState`] holds lives, deletes, dead lives, registers, counter slots,
//!   generation marks, the version vector and the loss records.
//! * [`join()`] merges two states and reports the loss records that are new
//!   relative to its first argument. It is the only way two states combine.
//! * [`Writer`] is one writer incarnation (a [`WriterId`]): it applies local
//!   writes to its own state, normalising the state through a join with the
//!   empty state exactly as the model's `Node.finish` does, and absorbs other
//!   states with [`Writer::receive`].
//! * [`MergeState::canonical_bytes`] and [`MergeState::state_hash`] give a
//!   deterministic encoding and a 128-bit digest; [`MergeState::encode`] and
//!   [`MergeState::decode`] round-trip a whole state.
//!
//! Retention (`R`) and the integer bound are carried by [`JoinOptions`]; every
//! read method that depends on them takes the options.

mod codec;
mod join;
mod state;
mod types;
mod writer;

#[cfg(test)]
mod prop_tests;
#[cfg(test)]
mod tests;

pub use codec::DecodeError;
pub use join::{JoinOptions, join};
pub use state::{Gen, Life, LifeKind, MergeState, ObsKey, Reg, Slot, Vv};
pub use types::{Epoch, LossKey, LossKind, LossValue, MStamp, Value, WriterId};
pub use writer::{Received, WriteKind, WriteRecord, Writer, Written};
