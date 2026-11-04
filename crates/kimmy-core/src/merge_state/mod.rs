//! Q7 field-level merge: the replicated state, its join, the local write
//! operations, the visible document, the hidden figure and the state hash.
//!
//! This is a port of the reviewed reference model (`model5.py` and the
//! `Node.finish` normalisation of `pullmodel5.py`). It is pure: no I/O, no
//! async, no global state. Field paths are segment lists ([`Path`]), never dotted strings. Nothing here is wired
//! into storage, the API or the oplog.
//!
//! The pieces:
//!
//! * Nested documents: a cell is keyed by `(life, segment path)`. A sub-document is a [`Value::Map`]
//!   marker at its path plus a cell for each leaf (an array is one atomic value). The read side
//!   applies the recursive type rule ([`MergeState::visible`]); a write diffs a sub-document
//!   against what the writer sees and writes only the leaves that changed; a write below a scalar
//!   or an array is refused; the join reports `typeClash` and `arrayClash`. NOT built here, by
//!   ruling: the implicit map marker a leaf write would leave on absent parents (so an emptied
//!   implicit map vanishes), a map-creating `$inc` and the multi-operator update, which the
//!   next model round revises.
//! * The field order (`ord.rs`, option A): [`MergeState::ords`] holds, per `(life, path)`, a
//!   [`FieldOrd`] (floor, per-writer firsts, last, level floor), joined with the cells by
//!   [`ord_join`]; [`MergeState::ordered_document`] reads the document in that order. The state,
//!   its join, the read and the compact codec's section 10 are built; the [`Writer`] does NOT
//!   feed the order state yet (a state a writer builds has none), so the order of a document is
//!   tested against the reference model on states assembled from its writes.
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
//! Counters are typed (`counter.rs`): `Int32`, `Int64` and `Decimal128` amounts merge (doubles
//! are not counters), the type of a merged cell is the join of its parts, the parts are added in
//! a canonical order, and a merge that crosses a bound saturates and reports the excess
//! ([`MergeState::excess`]). A counter has at most [`MAX_CONTRIBUTORS`] contributing writers and a document at most
//! [`MAX_WRITERS`] writers: a LOCAL write that would pass either is refused, a received state
//! past either is kept and reported ([`MergeState::over_caps`]).
//!
//! Two bounds apply to a counter. The integer bound (Q11, [`JoinOptions::int_bound`], the Int64
//! range unless lowered) is what a client sees: a total saturates there, and a local `$inc` that
//! would pass it is refused. [`MAX_TOTAL`] (2^119) is a theoretical bound on the `i128` totals
//! underneath, which keeps the read side's arithmetic exact for any 64 writers; it needs more
//! than 2^56 maximal increments to reach, `decode` and `receive_loss` refuse a total past it,
//! and `write_inc` refuses an `$inc` that would carry a writer's slot past it.

mod codec;
mod compact;
mod counter;
mod join;
mod ord;
mod state;
mod types;
#[cfg(test)]
mod work;
mod writer;

#[cfg(test)]
mod compact_tests;
#[cfg(test)]
mod counter_tests;
#[cfg(test)]
mod d7_equivalence_tests;
#[cfg(test)]
mod hand_built;
#[cfg(test)]
mod nested_tests;
#[cfg(test)]
mod ord_codec_tests;
#[cfg(test)]
mod ord_tests;
#[cfg(test)]
mod prop_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod work_tests;

pub use codec::{DecodeError, V1CannotHold};
pub use counter::Num;
pub use join::{InvalidIntBound, JoinOptions, join};
pub use ord::{
    Cand, FieldOrd, Floor, OrdAt, OrdEvent, OrdMode, OrderedValue, ord_events, ord_join,
};
pub use state::{
    Excess, Gen, Life, LifeKind, MAX_CONTRIBUTORS, MAX_TOTAL, MAX_WRITERS, MergeState, ObsKey,
    OverCaps, Reg, Slot, Vv,
};
pub use types::{
    Epoch, InvalidPath, InvalidValue, LossAt, LossKey, LossKind, LossValue, MStamp, Path, Value,
    WriterId,
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
    let _: fn(&Path, &str) -> Result<Path, InvalidPath> = Path::child;
    let _: fn(&Path) -> Option<Path> = Path::parent;
    let _: fn(&Path) -> &str = Path::name;
    let _: fn(&Path, &Path) -> bool = Path::is_below;
    let _: fn(&Value) -> bool = Value::is_plain;
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
    let _: fn(&MergeState, &JoinOptions) -> Option<Vec<(String, OrderedValue)>> =
        MergeState::ordered_document;
    let _: fn(&[(String, OrderedValue)]) -> Vec<Path> = OrderedValue::paths;
    let _: fn(&FieldOrd, &FieldOrd, &JoinOptions) -> FieldOrd = ord_join;
    let _: fn(&[(OrdAt, OrdMode)], &JoinOptions) -> Vec<OrdEvent> = ord_events;
    let _: fn(&Floor) -> MStamp = Floor::stamp;
    let _: fn(&FieldOrd, &Cand, &JoinOptions) -> bool = FieldOrd::valid;
    let _: fn(&FieldOrd, &JoinOptions) -> bool = FieldOrd::is_normal;
    let _: fn(&FieldOrd) -> bool = FieldOrd::is_empty;
    let _: fn(&MergeState, &JoinOptions) -> Vec<MStamp> = MergeState::alive;
    let _: fn(&MergeState) -> BTreeSet<WriterId> = MergeState::writers;
    let _: fn(&MergeState, &Path, &JoinOptions) -> BTreeSet<WriterId> = MergeState::contributors;
    let _: fn(&MergeState, &JoinOptions) -> OverCaps = MergeState::over_caps;
    let _: fn(&MergeState, &JoinOptions) -> BTreeMap<Path, Excess> = MergeState::excess;
    let _: fn(&MergeState, &JoinOptions) -> crate::hlc::Hlc = MergeState::horizon;
    let _: fn(&MergeState) -> Result<Vec<u8>, V1CannotHold> = MergeState::try_canonical_bytes;
    let _: fn(&MergeState) -> Result<Vec<u8>, V1CannotHold> = MergeState::try_encode;
    let _: fn(&MergeState) -> bool = MergeState::v1_can_hold;
    let _: fn(&MergeState) -> Result<[u8; 16], V1CannotHold> = MergeState::try_state_hash;
    let _: fn(
        MStamp,
        Value,
        BTreeMap<ObsKey, (MStamp, Num)>,
        Option<Gen>,
        bool,
    ) -> Result<Reg, InvalidValue> = Reg::new;
    let _: fn(&Value) -> Result<(), InvalidValue> = Value::check_register;
    let _: fn(&Value) -> Result<(), InvalidValue> = Value::check_input;
    let _: fn(&MergeState) -> Vec<u8> = MergeState::compact_bytes;
    let _: fn(&MergeState) -> [u8; 16] = MergeState::compact_hash;
    let _: fn(&MergeState) -> Vec<u8> = MergeState::encode_compact;
    let _: fn(&[u8]) -> Result<MergeState, DecodeError> = MergeState::decode_compact;
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
    let _: fn(&mut Writer, &Path, &Value, u64) -> Option<Written> = Writer::write_inc_value;
    let _: fn(&Value) -> Option<Num> = Num::from_value;
    let _: fn(&Num) -> Option<i128> = Num::int;
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
    let _: fn(&mut Writer, &Path, &Value, Mint) -> Result<Option<Written>, StampError> =
        Writer::write_inc_value_stamped;
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
