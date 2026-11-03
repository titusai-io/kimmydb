//! Q7 field-level merge: the replicated state, its join, the local write
//! operations, the visible document, the hidden figure and the state hash.
//!
//! This is a port of the reviewed reference model (`model5.py` and the
//! `Node.finish` normalisation of `pullmodel5.py`). It is pure: no I/O, no
//! async, no global state. Field paths are segment lists ([`Path`]), opaque keys at this stage. Nothing here is wired
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
//!   Each write takes its stamp from the writer's own clock (`write_set(.., skew)`) or, for
//!   the engine, from outside (`write_set_stamped(.., &mut || stamp)`): the stamp must be
//!   the writer's own and above everything it has used or seen ([`StampError`]).
//! * [`MergeState::canonical_bytes`] and [`MergeState::state_hash`] give a
//!   deterministic encoding and a 128-bit digest; [`MergeState::encode`] and
//!   [`MergeState::decode`] round-trip a whole state. `compact.rs` is the compact
//!   form (`QMS` version 2) that will replace them; nothing hashes or stores it yet.
//!
//! Retention (`R`) and the integer bound are carried by [`JoinOptions`]; every
//! read method that depends on them takes the options.
//!
//! Two bounds apply to a counter. The integer bound (Q11, [`JoinOptions::int_bound`], the Int64
//! range unless lowered) is what a client sees: a total saturates there, and a local `$inc` that
//! would pass it is refused. [`MAX_TOTAL`] (2^119) is a theoretical bound on the `i128` totals
//! underneath, which keeps the read side's arithmetic exact for any 64 writers; it needs more
//! than 2^56 maximal increments to reach, `decode` and `receive_loss` refuse a total past it,
//! and `write_inc` refuses an `$inc` that would carry a writer's slot past it.

mod codec;
mod compact;
mod join;
mod state;
mod types;
mod writer;

#[cfg(test)]
mod compact_tests;
#[cfg(test)]
mod prop_tests;
#[cfg(test)]
mod tests;

pub use codec::DecodeError;
pub use join::{InvalidIntBound, JoinOptions, join};
pub use state::{Gen, Life, LifeKind, MAX_TOTAL, MergeState, ObsKey, Reg, Slot, Vv};
pub use types::{
    Epoch, InvalidPath, LossAt, LossKey, LossKind, LossValue, MStamp, Path, Value, WriterId,
};
pub use writer::{Received, StampError, WriteKind, WriteRecord, Writer, Written};

// Production signatures. These items compile in EVERY build (with and without the
// `merge-mutants` feature, test or not): the mutation switches are private fields of
// `JoinOptions` read inside unchanged function bodies, never a different signature or a
// different set of public fields.
// A change to any of these types is a deliberate API change.
#[allow(clippy::type_complexity)]
const _: () = {
    use std::borrow::Cow;
    use std::collections::{BTreeMap, BTreeSet};
    let _: fn(&str) -> Result<Path, InvalidPath> = Path::flat;
    let _: fn(&Path) -> &[String] = Path::segments;
    let _: fn(&LossAt) -> Option<&Path> = LossAt::field;
    let _: fn(LossKind, Path, MStamp) -> LossKey = LossKey::field;
    let _: fn(LossKind, MStamp) -> LossKey = LossKey::document;
    let _: fn(&MergeState, &MergeState, &JoinOptions) -> (MergeState, BTreeSet<LossKey>) = join;
    let _: fn(u64, i64) -> Result<JoinOptions, InvalidIntBound> = JoinOptions::new;
    let _: fn(&mut JoinOptions, i64) -> Result<(), InvalidIntBound> = JoinOptions::set_int_bound;
    let _: fn(&JoinOptions) -> i64 = JoinOptions::int_bound;
    let _: for<'a> fn(&'a MergeState, &Path, &JoinOptions) -> Option<Cow<'a, Gen>> =
        MergeState::gen_of;
    let _: for<'a> fn(&'a MergeState, &Path, &JoinOptions) -> Option<&'a Reg> = MergeState::winner;
    let _: for<'a> fn(&'a MergeState, &Path, &JoinOptions) -> BTreeMap<ObsKey, &'a Slot> =
        MergeState::counted;
    let _: fn(&MergeState, &Path, &JoinOptions) -> i128 = MergeState::hidden;
    let _: fn(&MergeState, &JoinOptions) -> BTreeMap<Path, i128> = MergeState::hidden_all;
    let _: fn(&MergeState, &Path, &JoinOptions) -> Option<Value> = MergeState::value;
    let _: fn(&MergeState, &JoinOptions) -> Option<BTreeMap<Path, Value>> = MergeState::visible;
    let _: fn(&MergeState, &JoinOptions) -> Vec<MStamp> = MergeState::alive;
    let _: fn(&MergeState, &JoinOptions) -> crate::hlc::Hlc = MergeState::horizon;
    let _: fn(&MergeState) -> Vec<u8> = MergeState::canonical_bytes;
    let _: fn(&MergeState) -> Vec<u8> = MergeState::compact_bytes;
    let _: fn(&MergeState) -> [u8; 16] = MergeState::compact_hash;
    let _: fn(&MergeState) -> Vec<u8> = MergeState::encode_compact;
    let _: fn(&[u8]) -> Result<MergeState, DecodeError> = MergeState::decode_compact;
    let _: fn(&MergeState) -> [u8; 16] = MergeState::state_hash;
    let _: fn(&MergeState) -> Vec<u8> = MergeState::encode;
    let _: fn(&[u8]) -> Result<MergeState, DecodeError> = MergeState::decode;
    let _: fn(&mut Writer, &[(Path, Value)], u64) -> Option<Written> = Writer::insert;
    let _: fn(&mut Writer, &Path, i64, u64) -> Option<Written> = Writer::upsert_inc;
    let _: fn(&mut Writer, &[(Path, Value)], &Path, &[(Path, Value)], u64) -> Option<Written> =
        Writer::upsert_soi_create;
    let _: fn(&mut Writer, MStamp, &Path, i64, u64) -> Option<Written> = Writer::upsert_inc_on;
    let _: fn(
        &mut Writer,
        &[(Path, Value)],
        &Path,
        i64,
        &[(Path, Value)],
        u64,
    ) -> Option<(Written, Written)> = Writer::upsert_soi;
    let _: fn(&mut Writer, &Path, Value, u64) -> Option<Written> = Writer::write_set;
    let _: fn(&mut Writer, &Path, u64) -> Option<Written> = Writer::write_unset;
    let _: fn(&mut Writer, &Path, i64, u64) -> Option<Written> = Writer::write_inc;
    let _: fn(&mut Writer, &[(Path, Value)], u64) -> Option<Written> = Writer::write_replace;
    let _: fn(&mut Writer, u64) -> Option<Written> = Writer::write_delete;
    let _: fn(&mut Writer, MStamp) -> Result<MStamp, StampError> = Writer::tick_to;
    type Mint<'a> = &'a mut dyn FnMut() -> MStamp;
    let _: fn(&mut Writer, &[(Path, Value)], Mint) -> Result<Option<Written>, StampError> =
        Writer::insert_stamped;
    let _: fn(&mut Writer, &Path, i64, Mint) -> Result<Option<Written>, StampError> =
        Writer::upsert_inc_stamped;
    let _: fn(
        &mut Writer,
        &[(Path, Value)],
        &Path,
        &[(Path, Value)],
        Mint,
    ) -> Result<Option<Written>, StampError> = Writer::upsert_soi_create_stamped;
    let _: fn(&mut Writer, MStamp, &Path, i64, Mint) -> Result<Option<Written>, StampError> =
        Writer::upsert_inc_on_stamped;
    let _: fn(
        &mut Writer,
        &[(Path, Value)],
        &Path,
        i64,
        &[(Path, Value)],
        Mint,
    ) -> Result<Option<(Written, Written)>, StampError> = Writer::upsert_soi_stamped;
    let _: fn(&mut Writer, &Path, Value, Mint) -> Result<Option<Written>, StampError> =
        Writer::write_set_stamped;
    let _: fn(&mut Writer, &Path, Mint) -> Result<Option<Written>, StampError> =
        Writer::write_unset_stamped;
    let _: fn(&mut Writer, &Path, i64, Mint) -> Result<Option<Written>, StampError> =
        Writer::write_inc_stamped;
    let _: fn(&mut Writer, &[(Path, Value)], Mint) -> Result<Option<Written>, StampError> =
        Writer::write_replace_stamped;
    let _: fn(&mut Writer, Mint) -> Result<Option<Written>, StampError> =
        Writer::write_delete_stamped;
    let _: fn(&mut Writer, &MergeState) -> Received = Writer::receive_report;
    let _: fn(&mut Writer, &MergeState) -> Vec<LossKey> = Writer::receive;
    let _: fn(&mut Writer, LossKey, LossValue) -> bool = Writer::receive_loss;
    let _: fn(&Writer) -> Option<BTreeMap<Path, Value>> = Writer::visible;
    let _: fn(&Writer) -> BTreeMap<Path, i128> = Writer::hidden_all;
};
