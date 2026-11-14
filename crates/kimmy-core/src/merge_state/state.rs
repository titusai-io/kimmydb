//! The merge state and its read side (the model's `State`).

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use super::codec::DecodeError;
use super::counter::Num;
use super::join::JoinOptions;
use super::ord::{FieldOrd, OrdAt};
use super::types::{Epoch, InvalidValue, LossKey, LossValue, MStamp, Path, Value, WriterId};
use crate::hlc::Hlc;
use crate::ids::NodeId;

/// A version vector: the newest HLC seen per writer. A state's own vector COVERS every stamp the
/// state holds as a life, a delete, a register or a counter slot (`vv[stamp.writer] >=
/// stamp.hlc`: a writer has seen what it wrote, and what it received): the join decides that two
/// cells are concurrent by this, and a writer stamps above its vector. `decode` refuses a state
/// whose vector does not (see [`MergeState::check_decoded`]).
pub type Vv = BTreeMap<WriterId, Hlc>;

/// Key of an observed or counted slot: `(life, writer, generation)`.
pub type ObsKey = (MStamp, WriterId, Option<MStamp>);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LifeKind {
    Insert,
    Upsert,
}

/// A life of the document: one creation. `seen` is the set of delete stamps the
/// creator held; `order` the field order of the creating write.
#[derive(Clone, PartialEq, Debug)]
pub struct Life {
    pub seen: BTreeSet<MStamp>,
    pub order: Vec<Path>,
    pub kind: LifeKind,
}

/// The generation a base starts or belongs to: the starter's stamp and the totals the starter
/// had seen.
///
/// Only a NON-numeric base starts a generation (`Writer::setreg`, the model's `_setreg`), with a
/// `seen` over the slots alive at that write. A numeric `$set` or an `$unset` copies the winner's
/// generation, `seen` keys that name no slot any more included, and a register of the path in
/// another alive life keeps its copy until that life ends. Such a key is a subset of the
/// generation-start keys, so it is bounded by the slots alive at the start, and it is dead for
/// every read: `hidden` reads `seen` only for slots of alive lives.
///
/// `vv` (the starter's version vector) is memory for the V4M1 mutant of the test harness and
/// nothing else: it is kept only in a test or `merge-mutants` build, is empty in a production
/// build, and is not encoded, hashed or compared (two generations are equal when `stamp` and
/// `seen` are).
#[derive(Clone, Debug)]
pub struct Gen {
    pub stamp: MStamp,
    pub vv: Vv,
    pub seen: BTreeMap<ObsKey, Num>,
}

impl Gen {
    /// A generation started by the write `stamp`, with the totals it had seen.
    pub(crate) fn start(stamp: MStamp, vv: &Vv, seen: BTreeMap<ObsKey, Num>) -> Self {
        Gen { stamp, vv: mutant_vv(vv), seen }
    }
}

impl PartialEq for Gen {
    fn eq(&self, other: &Self) -> bool {
        self.stamp == other.stamp && self.seen == other.seen
    }
}

/// The version vector the mutants of the harness read (see [`Gen`] and
/// [`MergeState::gens`]): kept in a test or `merge-mutants` build, empty otherwise.
#[cfg(any(test, feature = "merge-mutants"))]
fn mutant_vv(vv: &Vv) -> Vv {
    vv.clone()
}
#[cfg(not(any(test, feature = "merge-mutants")))]
fn mutant_vv(_: &Vv) -> Vv {
    Vv::new()
}

/// A register cell: higher stamp wins. `generation` the generation of this base, `soi` marks
/// `$setOnInsert` creation cells. What a base write had counted of each slot is not kept here:
/// it is the slot's `absorbed` pair ([`Slot`]).
#[derive(PartialEq, Debug)]
pub struct Reg {
    pub stamp: MStamp,
    pub value: Value,
    pub generation: Option<Gen>,
    pub soi: bool,
}

// What `derive(Clone)` writes, by hand so that a test can count the registers a join or a write
// copies (see `work`).
impl Clone for Reg {
    fn clone(&self) -> Self {
        #[cfg(test)]
        super::work::bump(super::work::Counter::RegClones, 1);
        Reg {
            stamp: self.stamp,
            value: self.value.clone(),
            generation: self.generation.clone(),
            soi: self.soi,
        }
    }
}

impl Reg {
    /// A register. The value must be one a register can hold: a top-level `Document` is refused
    /// ([`Value::check_register`]).
    pub fn new(
        stamp: MStamp,
        value: Value,
        generation: Option<Gen>,
        soi: bool,
    ) -> Result<Self, InvalidValue> {
        value.check_register()?;
        Ok(Reg { stamp, value, generation, soi })
    }
}

/// The largest magnitude of a counter total (a slot's, its absorbed one, a generation's `seen`
/// entry or a loss record's): 2^119.
///
/// A THEORETICAL bound, there only so that the `i128` arithmetic of the read side is exact.
/// A visible value saturates at the Int64 bound, but the totals underneath are `i128` sums that
/// a writer's own `$inc`s can drive past it (a refused `$inc` is judged on the saturated value,
/// not on the sum). Each term the read side adds is a total minus the total a base observed, so
/// under 2^120 in magnitude, and 64 writers' terms sum to under 2^126, inside `i128`. Each
/// `$inc` moves a slot by less than 2^63, so an honest writer needs more than 2^56 maximal
/// increments to reach the bound: it is not reachable in practice, and no client sees a refusal
/// at it.
///
/// It is enforced where totals come from outside, in [`MergeState::decode`] (a slot, an absorbed
/// total, a `seen` entry, a loss record) and [`super::Writer::receive_loss`], which refuse a larger
/// one, so that a member never stores a state it cannot decode; and where they grow, in
/// [`super::Writer::write_inc`], which refuses an `$inc` that would pass it. The read side also
/// adds with saturation, as a backstop for a state assembled by hand through the public fields.
pub const MAX_TOTAL: i128 = 1 << 119;

// 64 writers' terms, each under twice the bound, sum inside i128.
const _: () = assert!(MAX_TOTAL.checked_mul(128).is_some());

/// The most writers that may contribute to one counter (design-ruled, Q9): a local `$inc` by a
/// writer that is not one of them yet is refused (a `400`) at this many. A state received with
/// more is kept and reported ([`MergeState::over_caps`]), never refused (Q8).
pub const MAX_CONTRIBUTORS: usize = 64;

/// The most writers (incarnations: a node and an epoch, so every restore adds one) a document
/// may mention (design-ruled, Q8; the compact form's writer table indexes them in one byte): a
/// local write by a writer the document does not mention yet is refused at this many. A state
/// received with more is kept and reported ([`MergeState::over_caps`]), never refused.
pub const MAX_WRITERS: usize = 256;

/// What the saturation of a merged counter cut off (Q11). Derived at read time, never stored.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Excess {
    /// An integer total past the integer bound: the signed amount past it (the visible value is
    /// the bound; the exact sum underneath is kept, so a later opposite increment brings the
    /// value back).
    Int(i128),
    /// A decimal sum that left the format at some step: the visible value is the largest finite
    /// `Decimal128` of the sign (`negative`) at that step. The amount is not reported: the sum
    /// is rounded at every step, and what lay beyond the format has no exact `Decimal128`.
    Decimal128 { negative: bool },
}

/// What a state holds past the caps (Q8: an over-cap result is stored and reported, and a local
/// write is refused only if it would add to what is over).
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct OverCaps {
    /// The number of writers the document mentions, when it is past [`MAX_WRITERS`].
    pub writers: Option<usize>,
    /// Each counter with more than [`MAX_CONTRIBUTORS`] contributors, and how many it has.
    pub counters: BTreeMap<Path, usize>,
}

/// A counter slot: one writer's running total in one generation.
///
/// `absorbed` is the highest `(stamp, total)` of THIS slot that any base write on the path had
/// held when it wrote (see [`Writer`](super::Writer): at every base write the writer folds every
/// slot of the path it holds in an alive life, of any generation). The increments up to that
/// pair are in a base's value, so the read side counts only what the slot gained since
/// ([`MergeState::value`]); the stamp tells a slot that has not moved since from one that moved
/// and came back to the same total (the B1 rule: the field is present when the slot moved). It is
/// keyed by the slot's own key, joined by the higher absorbed stamp, and dropped with the slot's
/// life. An honest pair is never above the slot (`absorbed.0 <= stamp`) and equals it in the total
/// when the stamps are equal.
#[derive(Clone, PartialEq, Debug)]
pub struct Slot {
    pub stamp: MStamp,
    pub total: Num,
    pub absorbed: Option<(MStamp, Num)>,
}

/// The replicated merge state. Grow-only components are joined by union, cells
/// by the higher stamp, the version vector by max; see [`super::join()`].
#[derive(Clone, Debug, Default)]
pub struct MergeState {
    pub lives: BTreeMap<MStamp, Life>,
    /// Delete stamp -> the version vector of the deleting writer.
    pub deletes: BTreeMap<MStamp, Vv>,
    /// Lives known dead; dropped once their stamp passes the horizon. Every dead life is also a
    /// key of `lives` (the join marks dead only lives it lists, and drops a life and its mark
    /// together): `decode` refuses a state where one is not, see [`MergeState::check_decoded`].
    pub dead: BTreeSet<MStamp>,
    pub regs: BTreeMap<(MStamp, Path), Reg>,
    pub slots: BTreeMap<(MStamp, Path, WriterId, Option<MStamp>), Slot>,
    /// Newest non-numeric base per cell, with its writer's version vector. NOTE(model):
    /// vestigial in the model (kept for its V3M3 mutant, read nowhere else). Mutant memory,
    /// like [`Gen::vv`]: filled only in a test or `merge-mutants` build, joined and dropped
    /// like cells, but never encoded, hashed or compared.
    pub gens: BTreeMap<(MStamp, Path), (MStamp, Vv)>,
    pub vv: Vv,
    pub losses: BTreeMap<LossKey, LossValue>,
    /// The order state: for each `(life, path)` a write named, the writes that could give the
    /// path its place in the document (see `ord.rs`). Empty for a state no order feed has
    /// touched. A key is a life and a path, like a register's; the root of a replace is
    /// [`OrdAt::Root`]. Joined like cells, dropped with a life that is not alive.
    pub ords: BTreeMap<(MStamp, OrdAt), FieldOrd>,
}

impl PartialEq for MergeState {
    /// Equality of the replicated state: everything that is encoded. `gens` (mutant memory)
    /// is left out.
    fn eq(&self, other: &Self) -> bool {
        let MergeState { lives, deletes, dead, regs, slots, gens: _, vv, losses, ords } = self;
        *lives == other.lives
            && *deletes == other.deletes
            && *dead == other.dead
            && *regs == other.regs
            && *slots == other.slots
            && *vv == other.vv
            && *losses == other.losses
            && *ords == other.ords
    }
}

impl MergeState {
    /// What `decode` and `decode_compact` require of the state they read, after the bytes have
    /// been parsed: the structural invariants of a state that the join relies on and that ONE
    /// state can show. A state that fails is not one any writer builds, and the join is not
    /// commutative or idempotent on it (a peer that could send one would split the replicas by
    /// the order of its receives).
    ///
    /// INVENTORY of what the join (`join.rs`), the reads (`state.rs`) and the writer
    /// (`writer.rs`) rely on, and whether `decode` checks it:
    ///
    /// 1. `dead` is a subset of `lives`. CHECKED here ([`DecodeError::DeadLifeNotListed`]).
    ///    The join keeps the lives of its first argument and adds one of the second only if it
    ///    is not dead in the first, so an unlisted dead life makes `lives` depend on the order.
    /// 2. The version vector covers every stamp an honest state's vector covers, loss records
    ///    excepted. CHECKED here ([`DecodeError::VersionVectorBehind`]). The stamps are: every
    ///    life, delete, register and slot stamp; the absorbed stamp of a slot; the generation
    ///    stamp of a slot key; the stamp of a register's generation; the life and generation
    ///    stamps of a generation's `seen` keys; the delete stamps a life has seen (`Life.seen`);
    ///    and the entries of the version vector of each delete. Clash and `typeClash` detection
    ///    take two cells as concurrent when the other side's vector does not cover them, so a
    ///    state that does not cover its own stamps finds a clash with itself (the join is not
    ///    idempotent), and a writer restored from it could mint a stamp it already used. The
    ///    join compares the others only by equality or order, never by coverage, so nothing
    ///    relies on them; they are checked because an honest vector covers them and a restored
    ///    writer must mint above them (the D2 deferral, closed with the counter cell). Loss
    ///    stamps are NOT included, and cannot be: a loss entry is received without its write
    ///    (`Writer::receive_loss` does not move the vector).
    /// 3. A counter total is at most `MAX_TOTAL` in magnitude (slots, absorbed
    ///    totals, `seen`, loss totals): checked by the readers ([`DecodeError::TotalOutOfRange`]).
    /// 4. A register holds a legal value (no top-level document, no marker in an array, nesting
    ///    within the BSON depth), a path has at least one segment and no NUL byte, strings are
    ///    UTF-8, every map and set is in strictly increasing key order, and the compact tables
    ///    are exactly what is used: checked by the readers (`BadTag`, `BadPath`, `BadString`,
    ///    `TooDeep`, `NotCanonical`, `BadIndex`).
    /// 5. Cells (registers, slots) belong to an alive life, as the join leaves them. NOT
    ///    checked, and not needed: the reads take the alive lives first and the join drops the
    ///    other cells (and is commutative on a state that has them); a normalised state is
    ///    only what makes `join(s, s) == s`, which no receive relies on.
    /// 6. A write is a register write or an increment at a path, never both: a register and a
    ///    slot do not share `(stamp, path)` (the loss record keyed by them would hold a value
    ///    or a total by the order of the join). NOT closable by one state: one state can show
    ///    the clash (the pinned states of the property tests do), but the two cells can also be
    ///    in two states, so a check of one state would be a partial guard; honest stamping
    ///    guarantees it.
    /// 7. One stamp is one write: what a stamp carries (a register's value and
    ///    generation, a slot's total, a life's kind, `seen` and field order, a delete's vector,
    ///    a loss record's value, and an absorbed stamp's total: one absorbed stamp under one key
    ///    carries one total across states, since the pair's join tie keeps the first argument's)
    ///    is the same in every state that holds it, since the join keeps
    ///    the first argument's copy. CROSS-STATE: not checkable by `decode`; honest stamping
    ///    (`Writer::tick`, strictly above the clock and the vector, `StampError`) guarantees it.
    /// 8. The caps (`MAX_WRITERS`, `MAX_CONTRIBUTORS`): a received state past them is kept and
    ///    reported (`over_caps`), by design (Q8). NOT checked.
    /// 9. No index or arithmetic in the join or the reads can fail on a state with 1 to 2:
    ///    every `lives[l]` is over a key drawn from `lives`, `sub_ret` clamps, sums saturate.
    ///
    /// 10. The order state (`ords`). Every entry is NORMAL: no candidate (`firsts`, `last`) is
    ///     one its floor voids. CHECKED here ([`DecodeError::OrdNotNormal`]): it is needed for
    ///     COMMUTATIVITY and idempotence, not only for the reads. The join keeps an entry only
    ///     `s` has as it is and normalises one only `t` has, so `join(s, t)` differs from
    ///     `join(t, s)` on a non-normal entry; and the join of an entry with itself drops what
    ///     the floor voids, so a state that holds one is not idempotent either. Every state a
    ///     writer or a join makes is normal.
    /// 11. An order entry belongs to a life `lives` lists. CHECKED here
    ///     ([`DecodeError::OrdLifeNotListed`]); every state a join leaves has it (the entries
    ///     of a life that is not alive go with it, and a life leaves `lives` only dead). The
    ///     reads would not notice (they take the alive lives first, and an unlisted life is not
    ///     alive), but the join drops the entry, so the state is not one it can produce; the
    ///     cells (item 5) are NOT checked the same way, because a hand-built state has them
    ///     and no reader needs it of them. (Not needed for the laws either: the join drops the
    ///     entries of a life that is not alive symmetrically; it is a check of the shape.)
    /// 12. The version vector covers the stamps an order entry holds (floor, `firsts`, `last`,
    ///     `lvl`), as item 2 does for cells: CHECKED ([`DecodeError::VersionVectorBehind`]).
    ///     The join does not read the vector for the order (not needed for the laws), but a
    ///     writer restored from the state mints above its vector, and a stamp it then reused
    ///     would meet its own floor.
    /// 13. No entry is EMPTY (no floor, no `firsts`, no `last`, no `lvl`). CHECKED here
    ///     ([`DecodeError::OrdEntryEmpty`]). An empty entry and no entry are the same state, so
    ///     the join removes an empty entry (of either side) and the constructors never make
    ///     one; a state that holds one is not idempotent under join (the join drops it), and
    ///     the compact form could spell the one state two ways if the decoder took both.
    ///     (A voided entry cannot become empty: voiding needs a floor, which stays.)
    ///
    /// THE REACHABILITY INVARIANT of an order entry. The join of order entries is commutative and
    /// idempotent on any normal entries but associative only on entries of histories within
    /// retention (ADR-085; a test pins the counterexample, and another the honest one beyond
    /// retention). The clauses below say what an entry looks like in any state such a history
    /// reaches, derived from the model's rules (`_events`,
    /// `_ord_apply`, `ord_join`) and held by every honest source in the tests: the model's
    /// runs (the 457 entries of its join-law probe and the 120 states of its scenarios), the
    /// causal simulation, and the property tests' runs with the feed. Each says whether one
    /// state can show it. Items 10 to 13 above are the first four.
    ///
    /// 14. Standing candidates carry what a write gives them. CHECKED, items 15 to 20, by
    ///     [`FieldOrd::shape_error`]:
    /// 15. `firsts[w]` is a stamp of `w` ([`DecodeError::OrdFirstNotByItsWriter`]). The feed sets
    ///     it from the writer's own stamp, and the join keeps the writer as the key.
    /// 16. A standing first implies a `last`, and no first is after it
    ///     ([`DecodeError::OrdLastBehindAFirst`]). `last` is the newest write the entry knows
    ///     (a writer stamps above everything it has seen, the join takes the maximum), so no
    ///     first is later; a floor that voids `last` voids every earlier first. (The converse
    ///     is false and not a clause: a floor may void a first and leave `last`, the model's
    ///     approximate case.)
    /// 17. A `u` floor's `prev` is below its stamp's HLC ([`DecodeError::OrdPrevNotBelowStamp`]):
    ///     `prev` is the writer's highest HLC before the write, and the clock ticks above it.
    /// 18. The root of a replace holds a level mark and nothing else
    ///     ([`DecodeError::OrdRootNotALevel`]): a replace names the root only as `("", "L")`.
    /// 19. One write names a path once, so candidates under one stamp share one position
    ///     ([`DecodeError::OrdPositionsDisagree`]).
    /// 20. A standing level mark at a path (not the root) implies a `last` at or after it
    ///     ([`DecodeError::OrdLevelWithoutLast`]): a document-valued write names the path and
    ///     marks it in one write, and a candidate that stands under the floor stays in `last`.
    ///
    /// 21. NOT checked, one state could show it but honest states break it or no law needs it: an
    ///     entry's path having a cell (a no-op unset in an update leaves a floor on a path that
    ///     has none: the hybrid floor); candidates against the cells' stamps (a cell is
    ///     overwritten by a newer write, and the counter cell is not frozen); an entry against the
    ///     entry of its parent (a write the unsetter never saw survives below an unset parent);
    ///     small positions (a write may name any number of siblings); `last` against an
    ///     unvoided first of another writer (a floor may void one and not the other).
    /// 22. NOT checkable from one state, the cross-state clause: for one writer W and one key,
    ///     any two states whose entries hold W's work agree on W's history. If one holds
    ///     `firsts[W] = s` and another `firsts[W] = t` with s earlier than t, the second must
    ///     carry a floor that voids `s`: W's state at `t` held `s` (W wrote it first) or a floor
    ///     over it, and every state that holds W's stamp `t` holds W's whole state at `t`. The
    ///     impossible pair is the entry `{first W at 5}` of one state and the entry `{first W at
    ///     9, no floor over 5}` of another: each alone is the entry of an honest state (W's first
    ///     write to the path was 5, or was 9), together they say W's write at 5 both happened and
    ///     did not. It is "one stamp is one write" for the order state (the second state denies
    ///     the write at stamp 5 that the first holds), guaranteed by honest stamping and by
    ///     the full-state transfer, and not closable by decode. The join groups such entries
    ///     differently and does not agree with itself (`ord_tests`). Beyond retention an HONEST
    ///     history reaches the same pair (the model's seed 1421: a delete and its dead mark are
    ///     collected when the horizon jumps by more than R, an old entry that is re-sent brings the
    ///     life back without the entry of the path, and the writer then writes a first with no
    ///     floor over its earlier one): the boundary of the claim is "within retention", the same as
    ///     the state's own, not "a causal history" (`ord_codec_tests`).
    ///
    ///     The same boundary holds for the redundancy of a register's old `obs` against the
    ///     absorbed pairs: the register obs is dead while a dead life cannot come back (within the
    ///     retention claim). A resurrected life beyond retention (ADR-085), or Q3 without the
    ///     covered skip, would bring back slots that a surviving register's obs had discounted,
    ///     and the Rust, which keeps no obs, would count them.
    ///
    /// 23. NOT relied on, so NOT checked: nothing else is assumed of an entry: the join and the read
    ///     are defined for any values, and the laws hold as stated above.
    ///
    /// THE ABSORBED PAIR of a slot (the counter cell). A slot is `(stamp, total)` and an optional
    /// `absorbed` `(stamp, total)`: the highest state of this very slot that a base write had
    /// held. Six clauses are checked (items 24 to 29); 30 is the version 1 form and 31 says what else is NOT checked:
    ///
    /// 24. The absorbed stamp is not above the slot's ([`DecodeError::AbsorbedAboveSlot`]). A base
    ///     write absorbs what its writer holds, and a slot only moves forward, so the pair is the
    ///     slot's own past or its present. The join takes the slot with the higher stamp and the
    ///     pair with the higher absorbed stamp independently; the clause is what keeps the pair
    ///     from naming a state of the slot that no copy of the slot has reached.
    /// 25. At equal stamps the totals are equal ([`DecodeError::AbsorbedTotalMismatch`]): one
    ///     stamp is one write (item 7), applied inside one slot. The read side takes the
    ///     difference of the two totals, so a pair that disagrees with its own slot at the same
    ///     stamp would read as an increment nobody made.
    ///
    /// 26. The absorbed stamp is by the writer of the slot's key
    ///     ([`DecodeError::AbsorbedWriterMismatch`]). The pair is a past state of that very
    ///     slot, and a slot is one writer's running total in one generation, so every stamp it
    ///     ever had is that writer's. The compact form spells the absorbed stamp in full (an HLC
    ///     and a writer index), so a pair by another writer can be encoded and is refused here.
    ///     The check follows the two above it: a stamp above the slot's, or equal to it with
    ///     another total, is reported as that.
    ///
    /// 27. The slot's own stamp is by the writer of the slot's key
    ///     ([`DecodeError::SlotWriterMismatch`]). The same reason as item 26 applies to the slot
    ///     itself: a slot is one writer's running total, and every write that moved it is that
    ///     writer's, so its stamp is. Without this, a pair by the key's writer under a slot
    ///     stamped by another writer would be accepted, though item 26 calls the pair a past state
    ///     of the very slot. Checked first of the slot's clauses.
    /// 28. The absorbed total's type is not wider than the slot's
    ///     ([`DecodeError::AbsorbedWiderThanSlot`]), in the order of the labels `Int32 < Int64 <
    ///     Decimal128`. THE HONEST RELATION, derived from the writer: the slot's total is the
    ///     writer's running sum, `sum_checked(old total, operand)` (`Writer::inc_plan`), and
    ///     `Num::combine` labels an integer sum `Int32` only when both parts are, otherwise
    ///     `Int64`, and a sum with a `Decimal128` part a `Decimal128`; a visible `Int64` only
    ///     widens the label (`Num::widened`, `Int32` to `Int64`); nothing converts a decimal back.
    ///     So the LABEL of the slot (the variant as spelled, not `Num::kind`, which promotes an
    ///     `Int32` amount outside the `i32` range, and so can fall again) never drops across the
    ///     writes of its writer. The pair is a past state of that slot (item 24), so its label
    ///     is at most the slot's; at the slot's own stamp the totals are equal (item 25) and so
    ///     are the labels. Neither the join nor the reads need it (an Int64 slot with a
    ///     `Decimal128` pair reads as a `Decimal128`, a defined value), it is the shape of the
    ///     writers' states, and the compact form spells both labels, so a state can show it.
    ///     (The reference model has one integer type and no decimal arithmetic, see its
    ///     INTERFACE.md: the relation is the Rust counter's, not the model's.)
    ///
    /// 29. A `Decimal128` total is a word the Writer writes ([`DecodeError::NotCanonicalDecimal`]), for the
    ///     slot, its absorbed pair, a generation's `seen` and a loss total alike (the one compact reader,
    ///     `num_payload`). THE DERIVATION. A decimal enters a typed total in three ways, and each ends in
    ///     `pack_raw`: (a) a writer's first amount at a path, `Num::first_amount` (`Writer::inc_plan`,
    ///     `None => op.first_amount()`), is the product's stored FORM of the operand (ADR-252, `update.rs`
    ///     `decimal_arithmetic` ends in `pack_raw`): the canonical encoding `dec_bytes(dec_raw(operand))`, so a NaN
    ///     of any sign, payload or signalling bit is the plain quiet NaN, a coefficient above 10^34 - 1 or the
    ///     large-coefficient form a canonical zero, an infinity stays one (its payload dropped), and the cohort
    ///     member is KEPT (`-0`, `0E2015`, `1E+2` as spelled): not a recomputation `0 + operand`, which would move
    ///     the exponent to the smaller one and make a read over a decimal base differ from the product's
    ///     `base + operand`; (b) every later sum is `sum_checked`, `Num::add`, `dec_bytes` = `pack_raw`; (c) a
    ///     base write copies the slot into the pair (`Writer::fold`), a generation's `seen` copies slots
    ///     (`Writer::seen`), and the loss totals copy slots (`join` parts (2), `receive_loss` takes a peer's
    ///     only if canonical). `$set` stores a decimal in a REGISTER as given, as the product does; a register
    ///     value is not a total and is not checked. So the totals of honest histories are exactly the words
    ///     `pack_raw` writes: a coefficient of at most 10^34 - 1 in the normal form with the exponent in range (the
    ///     largest biased exponent of the normal form, 12287, is the largest in range, so an out-of-range
    ///     exponent can only be spelled in the large-coefficient form), the plain quiet NaN, and the two
    ///     infinities with no payload. `Num::is_canonical` is `dec_bytes(dec_raw(b)) == b`. Refused: a NaN
    ///     that is not the plain quiet NaN (sign, payload, signalling), an infinity with a payload, a
    ///     coefficient above 10^34 - 1, the large-coefficient form. Every other canonical word is accepted, and a
    ///     writer produces them: a positive exponent (a sum of more than 34 digits is rounded and its exponent
    ///     rises, `9999999999999999999999999999999999 + 1` is `1.000...E+34`; the test
    ///     `an_honest_total_with_a_positive_exponent_is_accepted_for_the_slot_the_pair_seen_and_the_loss`), and `-0`
    ///     (a first amount keeps the cohort member of its operand). The totals are still compared byte for byte (a pair that differs from its
    ///     slot at the slot's own stamp is `AbsorbedTotalMismatch`). Tests:
    ///     `the_writer_canonicalises_nan_infinite_and_non_canonical_decimal_operands` (the Writer side: the
    ///     tripwire, it fails if the writer stores a word decode refuses),
    ///     `a_decimal_total_no_writer_produces_is_refused_wherever_a_total_is_spelled` and
    ///     `a_canonical_decimal_total_is_accepted_as_spelled_for_the_pair_as_for_the_slot` (the decode side).
    ///
    /// The version 1 form only (`decode`, not `decode_compact`):
    ///
    /// 30. A register's `obs` place is always empty ([`DecodeError::NotCanonical`]). A version 1
    ///     register still has a count where the `obs` map used to be; what a base had counted is
    ///     the slots' absorbed pairs now, which version 1 cannot spell, so a non-empty count is
    ///     not the encoding of any state. Checked by the version 1 reader, not by `check_decoded`
    ///     (a decoded state has no `obs` to show it).
    ///
    /// 31. NOT checked, NOT relied on: other single-state shapes that honest writers keep and that nothing
    ///     reads as an error. Two are known: a slot whose key's generation stamp is above the slot's own
    ///     stamp and above its pair's, and a register whose generation stamp is above the register's own
    ///     stamp. `check_decoded` accepts both. The list is not complete and is not meant to be: the join
    ///     and the reads are defined for any values (item 23), the hand-built Closed mode draws generations
    ///     freely, and a clause is added only when a state it refuses cannot be produced and something
    ///     would read it wrongly.
    ///
    /// The mutant memory (`gens`, `Gen::vv`) is not encoded and not checked.
    pub(super) fn check_decoded(&self) -> Result<(), DecodeError> {
        if self.dead.iter().any(|l| !self.lives.contains_key(l)) {
            return Err(DecodeError::DeadLifeNotListed);
        }
        for (k, s) in &self.slots {
            if s.stamp.writer != k.2 {
                return Err(DecodeError::SlotWriterMismatch);
            }
            if let Some((st, total)) = &s.absorbed {
                if *st > s.stamp {
                    return Err(DecodeError::AbsorbedAboveSlot);
                }
                if *st == s.stamp && *total != s.total {
                    return Err(DecodeError::AbsorbedTotalMismatch);
                }
                if st.writer != k.2 {
                    return Err(DecodeError::AbsorbedWriterMismatch);
                }
                if total.label() > s.total.label() {
                    return Err(DecodeError::AbsorbedWiderThanSlot);
                }
            }
        }
        let covered = |st: &MStamp| self.vv.get(&st.writer).is_some_and(|h| *h >= st.hlc);
        let gens = self.regs.values().filter_map(|r| r.generation.as_ref());
        let stamps = self
            .lives
            .keys()
            .chain(self.deletes.keys())
            .chain(self.lives.values().flat_map(|l| l.seen.iter()))
            .chain(self.regs.values().map(|r| &r.stamp))
            .chain(self.slots.values().map(|s| &s.stamp))
            .chain(self.slots.values().filter_map(|s| s.absorbed.as_ref().map(|a| &a.0)))
            .chain(self.slots.keys().filter_map(|k| k.3.as_ref()))
            .chain(gens.clone().map(|g| &g.stamp))
            .chain(gens.flat_map(|g| g.seen.keys().flat_map(|k| [&k.0].into_iter().chain(&k.2))));
        if stamps.into_iter().any(|st| !covered(st)) {
            return Err(DecodeError::VersionVectorBehind);
        }
        let vector_covered =
            |v: &Vv| v.iter().all(|(w, h)| self.vv.get(w).is_some_and(|x| *x >= *h));
        if !self.deletes.values().all(vector_covered) {
            return Err(DecodeError::VersionVectorBehind);
        }
        let o = JoinOptions::default();
        for ((l, at), x) in &self.ords {
            if !self.lives.contains_key(l) {
                return Err(DecodeError::OrdLifeNotListed);
            }
            if x.is_empty() {
                return Err(DecodeError::OrdEntryEmpty);
            }
            if !x.is_normal(&o) {
                return Err(DecodeError::OrdNotNormal);
            }
            let held = x
                .floor
                .map(|f| f.stamp())
                .into_iter()
                .chain(x.firsts.values().map(|c| c.0))
                .chain(x.last.map(|c| c.0))
                .chain(x.lvl);
            if held.into_iter().any(|st| !covered(&st)) {
                return Err(DecodeError::VersionVectorBehind);
            }
            if let Some(e) = x.shape_error(at) {
                return Err(e);
            }
        }
        Ok(())
    }

    /// Record the newest non-numeric base of a cell for the V3M3 mutant. The callers are
    /// compiled only in a test or `merge-mutants` build.
    #[cfg(any(test, feature = "merge-mutants"))]
    pub(crate) fn note_base(&mut self, cell: (MStamp, Path), stamp: MStamp, vv: &Vv) {
        self.gens.insert(cell, (stamp, vv.clone()));
    }
}

/// The model's `cov`: is `stamp` covered by `vv`?
pub(crate) fn cov(vv: &Vv, stamp: &MStamp) -> bool {
    vv.get(&stamp.writer).copied().unwrap_or(Hlc::ZERO) >= stamp.hlc
}

/// `h - ret`, clamped so that a "negative" value is below every stamp.
pub(crate) fn sub_ret(h: Hlc, ret: u64) -> Hlc {
    if h.wall_ms < ret { Hlc::ZERO } else { Hlc::new(h.wall_ms - ret, h.counter) }
}

impl MergeState {
    #[cfg(any(test, feature = "merge-mutants"))]
    /// Every counter amount the state holds: slot totals, their absorbed totals, the `seen`
    /// entries of the generations of the registers, and the lost totals.
    pub(crate) fn totals(&self) -> impl Iterator<Item = &Num> {
        let regs =
            self.regs.values().flat_map(|r| r.generation.iter().flat_map(|g| g.seen.values()));
        let absorbed = self.slots.values().filter_map(|s| s.absorbed.as_ref().map(|a| &a.1));
        let lost = self.losses.values().filter_map(|v| match v {
            LossValue::Total(n) => Some(n),
            _ => None,
        });
        self.slots.values().map(|s| &s.total).chain(absorbed).chain(regs).chain(lost)
    }

    pub(crate) fn max_vv(&self) -> Hlc {
        self.vv.values().copied().max().unwrap_or(Hlc::ZERO)
    }

    /// `H(state) = max(vv) - R`, a pure function of the state.
    pub fn horizon(&self, o: &JoinOptions) -> Hlc {
        sub_ret(self.max_vv(), o.retention)
    }

    pub(crate) fn alive_set(&self, ret: u64) -> BTreeSet<MStamp> {
        self.lives
            .iter()
            .filter(|(l, v)| {
                !self.dead.contains(l)
                    && self
                        .deletes
                        .keys()
                        .all(|d| d.hlc < sub_ret(l.hlc, ret) || v.seen.contains(d))
            })
            .map(|(l, _)| *l)
            .collect()
    }

    /// The alive lives, in stamp order. A life is alive iff it is not dead and
    /// every delete (inside retention of the life) is in its `seen`.
    pub fn alive(&self, o: &JoinOptions) -> Vec<MStamp> {
        self.alive_set(o.retention).into_iter().collect()
    }

    /// Is some alive life an explicit insert (any insert, re-inserts included)?
    fn insert_alive(&self, al: &BTreeSet<MStamp>) -> bool {
        al.iter().any(|a| self.lives.get(a).is_some_and(|v| v.kind == LifeKind::Insert))
    }

    /// V6m2: the creation cells of an upsert-created life are dropped when an
    /// explicit insert is alive (any insert, re-inserts included). `insert_alive` is that
    /// answer, asked for the first time it is needed and kept by the caller.
    fn suppressed(
        &self,
        life: &MStamp,
        r: &Reg,
        al: &BTreeSet<MStamp>,
        o: &JoinOptions,
        insert_alive: &mut Option<bool>,
    ) -> bool {
        match self.lives.get(life) {
            Some(l) if l.kind == LifeKind::Upsert && r.soi && o.suppress_soi() => {
                *insert_alive.get_or_insert_with(|| self.insert_alive(al))
            }
            _ => false,
        }
    }

    /// The alive registers of `p` in life order, as the read side picks among them: the first of
    /// the newest stamp wins, a suppressed creation cell never does. One lookup per alive life,
    /// not a pass over every register.
    pub(crate) fn winner_in(
        &self,
        p: &Path,
        al: &BTreeSet<MStamp>,
        o: &JoinOptions,
    ) -> Option<&Reg> {
        let first = al.first()?;
        let mut key = (*first, p.clone());
        let mut insert_alive = None;
        let mut best: Option<&Reg> = None;
        for l in al {
            key.0 = *l;
            #[cfg(test)]
            super::work::bump(super::work::Counter::Visits, 1);
            if let Some(r) = self.regs.get(&key)
                && !self.suppressed(l, r, al, o, &mut insert_alive)
                && best.is_none_or(|b| r.stamp > b.stamp)
            {
                best = Some(r);
            }
        }
        best
    }

    /// The counter slots of `p` in the alive lives, in key order: one range per alive life.
    pub(super) fn slots_at<'a>(
        &'a self,
        p: &Path,
        al: &BTreeSet<MStamp>,
    ) -> Vec<(&'a SlotKey, &'a Slot)> {
        let mut out = Vec::new();
        let Some(first) = al.first() else { return out };
        // the smallest writer, and no generation (which sorts first)
        let least = WriterId { node: NodeId::from_bytes([0; 16]), epoch: Epoch([0; Epoch::LEN]) };
        let mut from = (*first, p.clone(), least, None);
        for l in al {
            from.0 = *l;
            #[cfg(test)]
            super::work::bump(super::work::Counter::Visits, 1);
            out.extend(self.slots.range(&from..).take_while(|(k, _)| k.0 == *l && k.1 == *p));
        }
        out
    }

    /// The winning register of `p` among the alive lives.
    pub fn winner(&self, p: &Path, o: &JoinOptions) -> Option<&Reg> {
        self.winner_in(p, &self.alive_set(o.retention), o)
    }

    /// V3M3: the generation of the winning base.
    pub fn gen_of(&self, p: &Path, o: &JoinOptions) -> Option<Cow<'_, Gen>> {
        let al = self.alive_set(o.retention);
        self.gen_in(p, &al, self.winner_in(p, &al, o), o)
    }

    /// The generation `win` (the winner of `p`) belongs to. Under the V3M3
    /// mutant switch, the v3 rule instead: the newest non-numeric base among
    /// the alive lives, even when it lost.
    pub(super) fn gen_in<'a>(
        &'a self,
        p: &Path,
        al: &BTreeSet<MStamp>,
        win: Option<&'a Reg>,
        o: &JoinOptions,
    ) -> Option<Cow<'a, Gen>> {
        if !o.v3_generation() {
            return win.and_then(|w| w.generation.as_ref()).map(Cow::Borrowed);
        }
        self.gens
            .iter()
            .filter(|((l, q), _)| q == p && al.contains(l))
            .map(|(_, g)| g)
            .max_by_key(|g| g.0)
            .map(|(st, vv)| Cow::Owned(Gen::start(*st, vv, BTreeMap::new())))
    }

    /// Slots of the winning base's generation and of NEWER generations (their
    /// starter lost: the increments are kept). Slots of OLDER generations are
    /// overwritten (hidden).
    pub fn counted(&self, p: &Path, o: &JoinOptions) -> BTreeMap<ObsKey, &Slot> {
        let al = self.alive_set(o.retention);
        let win = self.winner_in(p, &al, o);
        self.counted_in(p, &al, win, o)
    }

    pub(super) fn counted_in(
        &self,
        p: &Path,
        al: &BTreeSet<MStamp>,
        win: Option<&Reg>,
        o: &JoinOptions,
    ) -> BTreeMap<ObsKey, &Slot> {
        let g = self.gen_in(p, al, win, o).map(|g| g.stamp);
        counted_among(g, self.slots_at(p, al)).collect()
    }

    /// The increments the winning generation overwrote, as state derived from
    /// the current merge: the NET SUM (signed), over the slots of older
    /// generations, of the part the starter of the winning generation had not
    /// seen (slot total minus the total the starter saw).
    pub fn hidden(&self, p: &Path, o: &JoinOptions) -> i128 {
        let al = self.alive_set(o.retention);
        self.hidden_in(p, &al, o)
    }

    fn hidden_in(&self, p: &Path, al: &BTreeSet<MStamp>, o: &JoinOptions) -> i128 {
        let win = self.winner_in(p, al, o);
        self.hidden_with(p, al, win, self.slots_at(p, al), o)
    }

    /// [`MergeState::hidden`] of `p` from its winner and its alive slots.
    fn hidden_with<'a>(
        &self,
        p: &Path,
        al: &BTreeSet<MStamp>,
        win: Option<&Reg>,
        slots: impl IntoIterator<Item = (&'a SlotKey, &'a Slot)>,
        o: &JoinOptions,
    ) -> i128 {
        let Some(g) = self.gen_in(p, al, win, o) else {
            return 0;
        };
        // An integer figure: the amounts of the other types are not reported (provisional, see
        // the module docs of `counter`).
        slots
            .into_iter()
            .filter(|((_, _, _, generation), _)| generation.is_none_or(|x| x < g.stamp))
            .filter_map(|((l, _, w, generation), s)| {
                let seen = g.seen.get(&(*l, *w, *generation)).and_then(Num::int).unwrap_or(0);
                s.total.int().map(|t| t.saturating_sub(seen))
            })
            .fold(0i128, i128::saturating_add)
    }

    /// The hidden figure of every path that has a nonzero one.
    pub fn hidden_all(&self, o: &JoinOptions) -> BTreeMap<Path, i128> {
        let al = self.alive_set(o.retention);
        self.index(&al, o)
            .into_iter()
            .filter(|(_, c)| !c.slots.is_empty())
            .filter_map(|(p, c)| {
                let h = self.hidden_with(p, &al, c.win, c.slots.iter().copied(), o);
                (h != 0).then(|| (p.clone(), h))
            })
            .collect()
    }

    /// The writers that contribute to the counter at `p`: those with a slot the read side counts
    /// (the winning base's generation and the newer ones), in the alive lives.
    pub fn contributors(&self, p: &Path, o: &JoinOptions) -> BTreeSet<WriterId> {
        self.counted(p, o).keys().map(|k| k.1).collect()
    }

    /// The caps the state is past, if any. Never an error: a merge can exceed a cap that each
    /// side kept, and the state is stored whole.
    pub fn over_caps(&self, o: &JoinOptions) -> OverCaps {
        let writers = self.writers().len();
        let al = self.alive_set(o.retention);
        OverCaps {
            writers: (writers > MAX_WRITERS).then_some(writers),
            counters: self
                .index(&al, o)
                .into_iter()
                .filter(|(_, c)| !c.slots.is_empty())
                .filter_map(|(p, c)| {
                    let g = self.gen_in(p, &al, c.win, o).map(|g| g.stamp);
                    let n = counted_among(g, c.slots.iter().copied())
                        .map(|(k, _)| k.1)
                        .collect::<BTreeSet<_>>()
                        .len();
                    (n > MAX_CONTRIBUTORS).then(|| (p.clone(), n))
                })
                .collect(),
        }
    }

    /// The visible value of one path (`None` when absent).
    pub fn value(&self, p: &Path, o: &JoinOptions) -> Option<Value> {
        let al = self.alive_set(o.retention);
        self.value_in(p, &al, o)
    }

    pub(crate) fn value_in(
        &self,
        p: &Path,
        al: &BTreeSet<MStamp>,
        o: &JoinOptions,
    ) -> Option<Value> {
        self.cell_in(p, al, o).map(|c| c.0)
    }

    /// The value of a path and what its saturation cut off, if anything.
    fn cell_in(
        &self,
        p: &Path,
        al: &BTreeSet<MStamp>,
        o: &JoinOptions,
    ) -> Option<(Value, Option<Excess>)> {
        let win = self.winner_in(p, al, o);
        self.cell_with(p, al, win, self.slots_at(p, al), o)
    }

    /// [`MergeState::cell_in`] from the winner of `p` and its alive slots.
    fn cell_with<'a>(
        &self,
        p: &Path,
        al: &BTreeSet<MStamp>,
        win: Option<&Reg>,
        slots: impl IntoIterator<Item = (&'a SlotKey, &'a Slot)>,
        o: &JoinOptions,
    ) -> Option<(Value, Option<Excess>)> {
        // A base that is not a counter amount (a string, a double, a map marker, ...) is the
        // value: doubles are not counters.
        let base = match win.map(|w| &w.value) {
            Some(v) if !matches!(v, Value::Unset) => match Num::from_value(v) {
                Some(n) => Some(n),
                None => return Some((v.clone(), None)),
            },
            _ => None,
        };
        // What each slot gained since a base write absorbed it (the increments no base write on
        // the path had seen). B1: the field is present when the slot moved since its absorbed
        // stamp, even if its increments net to 0 (a counter back at 0 is still there).
        let mut cut = None;
        let mut terms: Vec<(MStamp, ObsKey, Num)> = Vec::new();
        let g = self.gen_in(p, al, win, o).map(|g| g.stamp);
        for (k, s) in counted_among(g, slots) {
            let ob = s.absorbed.as_ref();
            let d = ob.map_or(s.total, |x| {
                if o.no_decimal_saturation() {
                    s.total.sub(&x.1)
                } else {
                    s.total.sub_sat(&x.1, &mut cut)
                }
            });
            if !d.is_zero() || (!o.value_ignores_slot_stamp() && ob.is_none_or(|x| x.0 != s.stamp))
            {
                terms.push((s.stamp, k, d));
            }
        }
        // The canonical order (see `counter`): the base, then the slots by writer (node, epoch),
        // then life and generation.
        if o.sum_in_stamp_order() {
            terms.sort_by_key(|t| (t.0, t.1));
        } else {
            terms.sort_by_key(|t| (t.1.1, t.1.0, t.1.2));
        }
        if base.is_none() && terms.is_empty() {
            return None;
        }
        let kind = terms.iter().map(|t| t.2.kind()).chain(base.map(|b| b.kind())).max()?;
        let mut acc = base.map_or(Num::zero(kind), |b| b.to_kind(kind));
        for (_, _, d) in &terms {
            acc = if o.no_decimal_saturation() {
                acc.add(&d.to_kind(kind))
            } else {
                acc.add_sat(&d.to_kind(kind), &mut cut)
            };
        }
        // Q11: a merge that crosses either bound saturates: an integer at the integer bound, a
        // decimal at the largest finite value of its sign.
        let excess = match acc.int_excess(o.int_bound()) {
            0 => cut.map(|negative| Excess::Decimal128 { negative }),
            e => Some(Excess::Int(e)),
        };
        if o.no_saturation()
            && let Some(v) = acc.int()
        {
            return Some((Value::Int64(v as i64), excess));
        }
        Some((acc.into_value(o.int_bound(), o.int32_saturates()), excess))
    }

    /// What the saturation of a merged counter cut off, per path: the paths whose visible value
    /// stopped at a bound (Q11). A pure function of the state, like [`MergeState::hidden`]:
    /// nothing is stored, and no join law, hash or byte of either codec depends on it.
    pub fn excess(&self, o: &JoinOptions) -> BTreeMap<Path, Excess> {
        let al = self.alive_set(o.retention);
        self.index(&al, o)
            .into_iter()
            .filter(|(_, c)| !c.slots.is_empty())
            .filter_map(|(p, c)| {
                let cell = self.cell_with(p, &al, c.win, c.slots.iter().copied(), o)?;
                cell.1.map(|e| (p.clone(), e))
            })
            .collect()
    }

    /// The visible document, or `None` when no life is alive: every present path, in path order
    /// (a map before the paths below it), with the value of a leaf and [`Value::Map`] at a map.
    /// A document with only flat fields has no map entry, so for it this is the map of its
    /// fields.
    ///
    /// The TYPE RULE (D4), at every depth: a path is a map when a map marker is its winning
    /// register, or when it has a visible child that is newer than its own scalar or array (a
    /// path with no cell of its own is a map too, if a child is visible); otherwise it is its
    /// register's value. Recency is decided per child: a scalar or array at `p` hides only the
    /// children that are older than it, and a newer child makes `p` a map showing just the
    /// newer children. A map marker never hides a child.
    pub fn visible(&self, o: &JoinOptions) -> Option<BTreeMap<Path, Value>> {
        let al = self.alive_set(o.retention);
        if al.is_empty() {
            return None;
        }
        let idx = self.index(&al, o);
        // every path with a cell, and every path above one
        let mut above: BTreeSet<Path> = BTreeSet::new();
        for p in idx.keys() {
            above.extend(p.ancestors().filter(|a| !idx.contains_key(a)));
        }
        let mut all: Vec<&Path> = idx.keys().copied().chain(above.iter()).collect();
        all.sort_unstable();
        // the children of a path, by the segments of the path (a slice is not a new path)
        let mut kids: BTreeMap<&[String], Vec<&Path>> = BTreeMap::new();
        let mut tops = Vec::new();
        for p in all {
            let segs = p.segments();
            match segs.split_last() {
                Some((_, par)) if !par.is_empty() => kids.entry(par).or_default().push(p),
                _ => tops.push(p),
            }
        }
        let cx = Tree { st: self, al: &al, o, idx: &idx, kids: &kids };
        let mut out = BTreeMap::new();
        for t in tops {
            if let Some(n) = cx.node(t) {
                n.emit(t, &mut out);
            }
        }
        Some(out)
    }

    /// Everything the read side needs of each path that has a cell in an alive life, from one
    /// pass over the registers and one over the slots: the newest stamp, the winning register
    /// and the slots. A path with a register that is suppressed and no other cell is absent.
    fn index<'a>(
        &'a self,
        al: &BTreeSet<MStamp>,
        o: &JoinOptions,
    ) -> BTreeMap<&'a Path, PathCells<'a>> {
        #[cfg(test)]
        super::work::bump(super::work::Counter::Visits, self.regs.len() + self.slots.len());
        let mut idx: BTreeMap<&Path, PathCells> = BTreeMap::new();
        let mut insert_alive = None;
        for ((l, p), r) in &self.regs {
            if al.contains(l) && !self.suppressed(l, r, al, o, &mut insert_alive) {
                let c = idx.entry(p).or_insert_with(|| PathCells::new(r.stamp));
                c.newest = c.newest.max(r.stamp);
                if c.win.is_none_or(|b| r.stamp > b.stamp) {
                    c.win = Some(r);
                }
            }
        }
        for (k, sl) in &self.slots {
            if al.contains(&k.0) {
                let c = idx.entry(&k.1).or_insert_with(|| PathCells::new(sl.stamp));
                c.newest = c.newest.max(sl.stamp);
                c.slots.push((k, sl));
            }
        }
        idx
    }
}

/// A counter slot's key: `(life, path, writer, generation)`.
type SlotKey = (MStamp, Path, WriterId, Option<MStamp>);

/// The cells of one path in the alive lives, see [`MergeState::index`].
struct PathCells<'a> {
    /// The newest stamp among them.
    newest: MStamp,
    /// The winning register, `None` when the path has only slots.
    win: Option<&'a Reg>,
    /// The slots, in key order.
    slots: Vec<(&'a SlotKey, &'a Slot)>,
}

impl PathCells<'_> {
    fn new(newest: MStamp) -> Self {
        PathCells { newest, win: None, slots: Vec::new() }
    }
}

/// The slots of the winning base's generation `g` and of NEWER generations, as [`ObsKey`] and
/// slot, from the slots of one path.
fn counted_among<'a>(
    g: Option<MStamp>,
    slots: impl IntoIterator<Item = (&'a SlotKey, &'a Slot)>,
) -> impl Iterator<Item = (ObsKey, &'a Slot)> {
    slots
        .into_iter()
        .filter(move |((_, _, _, generation), _)| {
            *generation == g || (generation.is_some() && (g.is_none() || *generation > g))
        })
        .map(|((l, _, w, generation), s)| ((*l, *w, *generation), s))
}

/// What the type rule decided for one path.
struct Node<'a> {
    /// The newest stamp among the cells of the visible subtree.
    newest: MStamp,
    shape: Shape<'a>,
}

enum Shape<'a> {
    Leaf(Value),
    Map(BTreeMap<&'a Path, Node<'a>>),
}

impl Node<'_> {
    fn emit(self, at: &Path, out: &mut BTreeMap<Path, Value>) {
        match self.shape {
            Shape::Leaf(v) => {
                out.insert(at.clone(), v);
            }
            Shape::Map(kids) => {
                out.insert(at.clone(), Value::Map);
                for (c, n) in kids {
                    n.emit(c, out);
                }
            }
        }
    }
}

/// The inputs of the type rule, for the recursion of [`MergeState::visible`].
struct Tree<'a> {
    st: &'a MergeState,
    al: &'a BTreeSet<MStamp>,
    o: &'a JoinOptions,
    idx: &'a BTreeMap<&'a Path, PathCells<'a>>,
    kids: &'a BTreeMap<&'a [String], Vec<&'a Path>>,
}

impl<'a> Tree<'a> {
    /// The node of `q`, `None` when `q` is absent.
    fn node(&self, q: &'a Path) -> Option<Node<'a>> {
        let ks: BTreeMap<&Path, Node> = self
            .kids
            .get(q.segments())
            .into_iter()
            .flatten()
            .filter_map(|c| self.node(c).map(|n| (*c, n)))
            .collect();
        let cells = self.idx.get(q);
        let own = cells.and_then(|c| {
            self.st.cell_with(q, self.al, c.win, c.slots.iter().copied(), self.o).map(|c| c.0)
        });
        let own_n = cells.map(|c| c.newest);
        let top = |v: &BTreeMap<&Path, Node>| v.values().map(|n| n.newest).max();
        if ks.is_empty() {
            let v = own?;
            let shape = if v.is_marker() { Shape::Map(BTreeMap::new()) } else { Shape::Leaf(v) };
            return Some(Node { newest: own_n?, shape });
        }
        match own {
            // a scalar, number or array at q: only the children newer than its register show
            Some(v) if !v.is_marker() => {
                let reg = cells.and_then(|c| c.win);
                let rs = match reg {
                    Some(r) if r.value.is_plain() => r.stamp,
                    _ => own_n?,
                };
                let shown: BTreeMap<&Path, Node> = if self.o.type_ignores_recency() {
                    BTreeMap::new()
                } else {
                    ks.into_iter().filter(|(_, n)| n.newest > rs).collect()
                };
                if shown.is_empty() {
                    return Some(Node { newest: own_n?, shape: Shape::Leaf(v) });
                }
                let newest = top(&shown).into_iter().chain(own_n).max()?;
                Some(Node { newest, shape: Shape::Map(shown) })
            }
            Some(_) if self.o.marker_hides() => {
                Some(Node { newest: own_n?, shape: Shape::Map(BTreeMap::new()) })
            }
            // a map marker, an unset or no cell at all: a map of every visible child
            _ => {
                let newest = top(&ks).into_iter().chain(own_n).max()?;
                Some(Node { newest, shape: Shape::Map(ks) })
            }
        }
    }
}
