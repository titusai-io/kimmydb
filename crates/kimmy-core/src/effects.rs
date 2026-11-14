//! The effects of one update, as the merge layer takes them (Q7 Step 2, interface only).
//!
//! `kimmy-query` computes an update's effects from the operators and the document it sees; the
//! storage layer hands them to [`Writer::write_update`] under the `field` merge policy. A diff of
//! before and after cannot tell `$inc 5` from `$set old+5` (one adds to a counter slot, the other
//! writes a register), so the effects are produced by the operator, not recovered afterwards.
//!
//! # What is reused
//!
//! [`UpdateOp`] (in `merge_state::writer`) already is the effect kind: `Set(path, value)`,
//! `Unset(path)`, `Inc(path, operand)`, with the writer's own overlap, below-scalar and counter
//! checks and its [`UpdateRefused`] error. This module does not duplicate it. [`Effects`] is the
//! ordered list of them for ONE update, [`Outcome`] names the three ways a write ends, and
//! [`UpsertEffects`] is the shape of an upsert. `kimmy-query` already depends on `kimmy-core`
//! (`crates/kimmy-query/Cargo.toml`), and `kimmy-core` depends on neither `kimmy-query` nor
//! `kimmy-storage`, so S2b can name every type here.
//!
//! # Invariants
//!
//! 1. A no-op yields no effects and no write: an update whose operators change nothing produces an
//!    empty [`Effects`] (or effects the writer finds unchanged, [`Outcome::Unchanged`]); neither
//!    mints a stamp, writes a record or an entry (`modified: false`, ADR-208).
//! 2. Effects are per LEAF path. A path is a [`Path`] of segments, never a dotted string split on
//!    `.`: a pipeline `$setField` can name a field `a.b` or `$x`, which is a one-segment path.
//! 3. One stamp per update. The list is one write; the paths are pairwise disjoint (none equal to
//!    or below another) or the whole update is [`UpdateRefused::Overlap`], atomically.
//! 4. Arrays are atomic: every array operator sends the whole new array as one `Set`. Any operator
//!    whose path CROSSES an array (a numeric index, `$`, `$[]`, `$[id]`, nested arrays) becomes
//!    ONE `Set` of the OUTERMOST array: all such operators of one update are combined by the
//!    product into that one final value. `$inc` on an element is a register write (the array is
//!    rewritten); `$unset` of an element writes `null` (as the reference does). So `$set {"a.0": 1,
//!    "a.1": 2}` is `Set(a, [1, 2, ..])`; expressed as two leaf `Set`s of `a.0` and `a.1` it is
//!    `BelowScalar`, and `Set(a)` beside `Set(a.0)` is `Overlap`.
//! 5. Order is the order of the operators in the update, and positions within the list are
//!    deterministic: the same update on the same document yields the same list.
//! 6. `Set(path, Value::Unset)` is an `Unset`; the writer normalises it.
//! 7. A refusal changes nothing (no stamp, no clock move): the whole update is refused.
//! 8. A [`Path`]'s `Display` is LOSSY (it joins the segments with `.`, so `a.b` as one segment and
//!    `a`, `b` as two print alike): never use it as a key (for example when grouping operators).
//!    Compare and sort [`Path`]s themselves.
//!
//! # Operator by effect
//!
//! "Product" is `kimmy-query` (S2b); "writer" is `merge_state::Writer`. A refusal named
//! `UpdateRefused::..` comes from the writer (HTTP 400); a product refusal comes from
//! `kimmy-query` before any effect is sent (also 400).
//!
//! | Operator | Effect(s) | Notes and refusals |
//! |---|---|---|
//! | `$set p v` | `Set(p, v)` | a document `v` is expanded by the writer, only changed leaves are written; below a scalar or array: `BelowScalar`; illegal value: `BadValue`/`BadPath` |
//! | `$unset p` | `Unset(p)` | absent path: no write of its own |
//! | `$inc p n` | `Inc(p, n)` | counter slot on an Int32/Int64/Decimal128 base, register write on a Double base (the writer decides); refusals `IncOperandNotANumber`, `IncTargetNotANumber`, `IncTargetIsAContainer`, `IncDoubleOnCounter`, `IncOutOfRange`, `TooManyContributors`; absent target: a counter is created for an Int32, Int64 or Decimal128 operand, but a Double operand creates a Double REGISTER (not a counter) |
//! | `$mul`, `$bit` | `Set(p, computed)` | register write (a base write); computed by the product |
//! | `$min`, `$max` | `Set(p, v)` only if it changes, else no effect | register write; "changes" is the reference's numeric comparison, see "No-op test" |
//! | `$currentDate` | `Set(p, now)` | ONE `now` per update, from the product clock as today's `apply(update, doc, now_ms)`; NOT derived from the stamp (the `document` policy stays byte-identical to 0.59) |
//! | `$push`, `$addToSet`, `$pull`, `$pullAll`, `$pop` | `Set(p, whole_array)` | arrays atomic; no effect if the array is unchanged |
//! | positional `$`, `$[]`, `$[id]` (with `arrayFilters`), or any path through an array (`a.0`, `a.0.b`, nested arrays) | ONE `Set(outermost_array, whole_array)` per array | the product resolves the position and combines every operator of the update on that array into its final value (invariant 4); `$inc` on an element is a register write; `$unset` of an element writes `null` |
//! | `$rename a b` | `Unset(a)` + `Set(b, value)` (source recorded); NO effects when the source is absent (a no-op, as in the reference) | disjoint paths required (else `Overlap`); `a` or any path below it that the WRITER sees as a counter is `RenameOfCounter(a)` (400), see below; a target that is a counter is just a `Set` (folds), allowed |
//! | `$setOnInsert` | none on update; `UpsertEffects::set_on_insert` on an insert | no-op on an existing document |
//! | pipeline update (ADR-265) | `Set(top_field, Document)` per CHANGED top-level field (or `Unset(top_field)`), one list, never an ancestor and a descendant together | not `write_replace` (a replace reorders fields); the writer's diff writes only the changed leaves and unsets only what it saw; each path is a [`Path`] of one segment per name (S2b item below) |
//! | replace-style update | not an effect list | stays the replace path of the storage layer |
//! | any operator on a missing document | `UpdateRefused::NoLife` | not a 400: matched 0 (see "Outcome mapping"); an upsert goes through [`UpsertEffects`] |
//! | any two operators on overlapping paths | `UpdateRefused::Overlap` | whole update refused |
//! | a new writer past the caps | `UpdateRefused::WriterCap` | whole update refused |
//!
//! # Counters: `$rename`, and `$mul`/`$min`/`$max` over a counter
//!
//! A path is a COUNTER when, in the writer's state, its visible cell in an alive life is an
//! `Int32`, `Int64` or `Decimal128` (its base and/or slots, design D5/NB1); a `Double` is not.
//! Only the writer's state knows this, so it is decided at the writer
//! ([`Writer::sees_counter`], read-only), never inferred from the `Document` by the product.
//! `$rename` is leaf effects (D4): renaming a sub-document renames every counter cell under it and
//! the slots cannot move, so the refusal applies when the source path OR ANY DESCENDANT is a
//! counter cell in an alive life ([`Writer::sees_counter_at_or_under`]). An array holding ints is
//! an atomic register, not a counter. The TARGET side is unchanged: a rename onto a counter path is
//! a `Set`, which folds, and is allowed.
//! The check lives IN THE WRITER ([`Writer::write_update_renaming`] and its `_stamped` form take
//! the ops plus the rename sources and refuse first); [`Effects::apply`] and
//! [`Effects::apply_stamped`] call them. A rename enters an [`Effects`] only through
//! [`Effects::rename`], and the ops list is not exposed, so a counter rename cannot be written
//! around the check: S2a MUST use `apply*`, never `write_update` with the ops. The whole update is
//! refused with [`UpdateRefused::RenameOfCounter`] (400) BEFORE writing or moving the clock when a
//! rename source is a counter. Every Int32/Int64/Decimal128 cell counts, whether or not it was
//! ever incremented (a deviation entry records this; the owner may narrow it). Race (U2, as the Q7 design words it): a counter field THIS member
//! sees as a counter cannot be renamed (400); a concurrent first `$inc` elsewhere races the rename
//! and its increments reappear on the source path (U2), documented, not "always a 400".
//!
//! `$mul`, `$min`, `$max` (and `$bit`) over a counter path are a register write, a base write:
//! `Set(path, computed)`. The fold rule applies (it folds every slot of the path that it holds in
//! an alive life); increments it has not seen add on top (design line 117). Nothing new in the API.
//!
//! # Upserts
//!
//! [`UpsertEffects`] is `{ from_filter, set_on_insert, set, inc }` (the effects when no document
//! matches). Rulings:
//!
//! * (a) Creation works with no `$inc`: `$set` only, `$setOnInsert` only, and `{}` (an empty
//!   upsert) all create the document. The creation entry must accept an empty `inc` list (S2a).
//! * (b) The filter's equality fields (any path whose FIRST segment is `_id`, `{"_id.x": 1}`
//!   included, is left out: the existing upsert `_id` logic owns `_id`) go in `from_filter`, TAGGED
//!   as creation cells like `$setOnInsert`. They are the LOWEST layer: a `$set` or `$setOnInsert`
//!   on the same path overrides them, and that is NOT an overlap.
//! * (b1) Two checks, in two places. [`UpsertEffects::check`] runs at PARSE time on every update
//!   (matched or not): the overlap among `set_on_insert`, `set` and `inc`. [`UpsertEffects::check_creation`]
//!   (also run first by `creation_cells`) runs ONLY on the creation path: `from_filter` paths that
//!   overlap EACH OTHER (the same path twice, or an ancestor and a descendant) are
//!   [`UpdateRefused::Overlap`] (this matches the reference, so it is NOT a deviation); and an
//!   `$inc` on a path that a filter leaf also writes, or below or above one, is
//!   [`UpdateRefused::Overlap`] (the design's overlap refusal: the creation wrote the path, and
//!   giving a creation register a slot of its own write is a merge-model change, out of scope).
//!   Both are compared on the COMPOSED LEAVES, not on the paths as written, so `{"a.b": 1}` and
//!   `{a: {b: 1}}` behave alike. The reference accepts an upsert that increments a filter field
//!   (`{a: 5}` with `$inc a 1` gives 6) when no document matches, so that case is a deviation
//!   (`docs/deviations.md`, "upsert-filter-field-incremented-q7"); a MATCHED upsert is an ordinary
//!   update and is never refused for it (`updateOne({v: 3}, {$inc: {v: 1}}, {upsert: true})`).
//! * (b2) Composition, before the writer ([`UpsertEffects::creation_cells`]): the layers compose
//!   in order, filter first, then `$setOnInsert` and `$set` on top, into one cell per TOP-LEVEL
//!   FIELD whose value is the whole composed subtree (a `Document` or a scalar), so the writer's
//!   `expand` writes the map markers exactly as for an insert (`$setOnInsert {a: {x: 1}}` is one
//!   cell `a` = `{x: 1}`). A higher-layer path EQUAL to or an ANCESTOR of a filter path overrides
//!   it, IN PLACE, and the filter leaves below it are dropped (filter `{"a.b": 1}` with `$set
//!   {a: {c: 2}}` gives `a: {c: 2}`); a higher-layer path BELOW a filter document merges into it
//!   (filter `{a: {x: 1}}` with `$set {"a.y": 2}` gives `a: {x: 1, y: 2}`). A cell is tagged
//!   `Creation` when its subtree came ONLY from the filter and/or `$setOnInsert`, `Plain` when any
//!   part came from `$set` (so `$set` data is never dropped by a later insert; a mixed
//!   filter-and-`$set` subtree is `Plain`, the filter part surviving being the safe side; `$setOnInsert`
//!   with `$set` in one subtree is already an `Overlap`). A higher-layer path below a non-document
//!   filter value (filter `{a: 5}`, `$set {"a.y": 2}`) is [`UpdateRefused::BelowScalar`], as the
//!   reference cannot create a field in a non-document; an empty-document filter value gives way.
//!   An `$unset` (`Set(p, Value::Unset)`) in a higher layer removes what is there and is a NO-OP,
//!   with no refusal, when the path is absent or below a scalar (`{a: 5}` with `$unset a.y`).
//!   `$inc` is not a cell. Field ORDER: filter fields in filter order; an override keeps its
//!   field in place; new fields are appended in the order the caller lists them, `set_on_insert`
//!   first, then `set`. The caller (S2b) lists the new fields of an operator update in
//!   lexicographic order (the reference's order since 5.0, as ruled) and a pipeline update in
//!   pipeline order. NOT checked against a recording: `reference-6.0.5` holds no operator-update or
//!   upsert recording (its only upsert text is an expectation comment in `pipeline-updates.js`:
//!   filter fields, then `n`, `label` in written order), so the field order of an upsert, and the
//!   order of ordinary-update effects (the operator order given), are by ruling and S2b must
//!   confirm them against the reference.
//! * (c) Creation values of `$mul`, `$min`, `$max`, `$currentDate`, `$push`, `$addToSet` and `$bit`
//!   are computed by S2b and go in `set`.
//! * (d) One write or two, from the existing code (`merge_state/writer.rs`): `upsert_inc` is ONE
//!   write (creation and slot under one stamp, `insert = true`); `upsert_soi` is TWO sequential
//!   writes (the creation, then the increment, each with its own stamp, the second dropped if the
//!   first is refused). So an upsert carrying `$setOnInsert` is two writes (design); an upsert with
//!   `$inc` and neither `$setOnInsert` nor `$set` is the one-write `upsert_inc`; one with `$set`
//!   (or a filter field) and `$inc` but no `$setOnInsert` has no one-write entry today and takes
//!   the `upsert_soi` two-write shape with empty `fields`. No new semantics here.
//! * (e) The overlap check runs at PARSE time on every update and upsert, matched or not (the
//!   reference refuses at parse): `$inc` plus `$setOnInsert` (or `$set`) on the same path, or any
//!   overlap among the three lists `set_on_insert`, `set`, `inc` (a path equal to or below another),
//!   is [`UpdateRefused::Overlap`] ([`UpsertEffects::check`]); there is no splitting logic.
//!   Several `$inc` on DISTINCT paths are allowed.
//!
//! # S2a work item (not implemented here)
//!
//! `Writer::upsert_soi*` takes exactly one `(inc_path, n)`. S2a generalises the two-write upsert to
//! a list of `(path, n)` at ONE stamp for the increments, validated as a whole together with the
//! creation before anything is written or the clock moves. One write = one stamp, as
//! `write_update` already does, so no model rerun; extend the Rust oracle and property generator
//! to multi-`$inc` upserts. It also accepts an empty `inc` list (a), and the `from_filter` layer
//! (b).
//!
//! The same item widens `upsert_soi*` and `upsert_inc_on` from `i64` to typed operands (as
//! [`UpdateOp::Inc`] already is: Int32, Int64, Double or Decimal128; the 2026-11-03 ruling already
//! says to extend `upsert_inc` to Decimal128 operands). On a counter, an Int or Decimal operand is
//! a counter increment and a Double is a 400 (`IncDoubleOnCounter`); on a Double base it is a
//! register write; on creation the operand's type sets the cell's type. Not implemented here.
//!
//! # Outcome mapping (for S2a)
//!
//! | Writer result | [`Outcome`] | Response |
//! |---|---|---|
//! | `Ok(Some)` | `Modified` | 200, `modified: true`, the stamp |
//! | `Ok(None)`, or empty effects with a life | `Unchanged` | 200, `modified: false` |
//! | `NoLife` | `Refused(NoLife)` | NOT a 400: no match (matched 0), then the upsert path or `n: 0`. It takes precedence over `Unchanged`: empty effects and no life is `NoLife` |
//! | `Overlap`, `BelowScalar`, `BadValue`, `BadPath`, `RenameOfCounter`, `IncOperandNotANumber`, `IncTargetNotANumber`, `IncTargetIsAContainer`, `IncDoubleOnCounter`, `IncOutOfRange`, `TooManyContributors` (the 65th contributor) | `Refused` | 400 |
//! | `WriterCap` (the 257th writer; 256 allowed) | `Refused(WriterCap)` | 400; goes in `docs/deviations.md` as an S2a item with the item-9 refusals |
//!
//! # S2b item: pipeline updates and field order
//!
//! After `$replaceWith` or `$project` the product emits `Set(top_field, Document)` per changed
//! top-level field and relies on the writer's diff. Check the resulting FIELD ORDER against
//! ADR-265 and the reference recordings, with a test.
//!
//! # No-op test
//!
//! By representation, as for every write ([`Value::same`]): doubles compare by bit pattern
//! (`-0.0` is not `0.0`; a NaN over the same NaN is a no-op), a `Decimal128` by its 16 bytes, an
//! `Int` never equals a `Double`, a type change is a write, `Null` is distinct from unset.
//!
//! That is the test of the WRITER's diff. Whether `$min`, `$max` and `$addToSet` change anything is
//! decided by the product BEFORE emitting an effect, by the reference's own comparison (numeric
//! comparison across types; set membership), NOT by `Value::same`: `$max {n: 5.0}` over `Int 5`
//! changes nothing and yields no effect.
//!
//! # Questions: resolved by ruling, and still open
//!
//! * RESOLVED: `$rename` of a counter is decided at the writer, see above.
//! * RESOLVED: `$mul`/`$min`/`$max` over a counter is `Set(computed)`, see above.
//! * RESOLVED: upserts refuse overlap, several `$inc` on distinct paths; generalisation is S2a's.
//! * RESOLVED: pipeline no-op is by representation (`Value::same`).
//! * RESOLVED: `$currentDate` is one product-clock `now` per update.
//! * RESOLVED: `$rename` of a sub-document holding counters is refused (source or any descendant).
//! * RESOLVED: upsert `$inc` operands stay `Value` (Int32, Int64, Double, Decimal128); the bridge
//!   to the writer is S2a's (next section).
//! * RESOLVED: bypass of the rename check (it is in the writer), `$rename` of a missing source (no
//!   effects), arrays (one outermost `Set`), the upsert contract (a) to (e), the outcome mapping.
//! * RESOLVED: `from_filter` overlap among itself is an `Overlap` on the creation path (as the
//!   reference); the layers compose filter, then `$setOnInsert`, then `$set`, see (b1), (b2).
//! * RESOLVED: a higher-layer path below a NON-document filter value is `BelowScalar` (400, as the
//!   reference); an empty-document filter value gives way to what is set below it.
//! * RESOLVED (withdrawn ruling): `$inc` on a filter field is not `{a: 5}` gives 6 but an
//!   `Overlap` refusal, recorded as a deviation.
//! * OPEN: the field order of upserts and of ordinary-update effects has no recording, see (b2).

pub use crate::merge_state::{MStamp, Path, UpdateOp, UpdateRefused, Value, Written};
use crate::merge_state::{UpdateError, Writer};

/// The effects of one update: an ordered list of per-leaf [`UpdateOp`]s that make ONE write with
/// ONE stamp. See the module docs for the operator table and the invariants.
#[derive(Clone, Debug, Default)]
pub struct Effects {
    ops: Vec<UpdateOp>,
    /// The source paths of the renames recorded by [`Effects::rename`], in order.
    renames: Vec<Path>,
}

impl Effects {
    /// No effects: an update that changes nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Effects from a ready list, kept in order. A rename cannot come in this way: only
    /// [`Effects::rename`] records one.
    pub fn from_ops(ops: Vec<UpdateOp>) -> Self {
        Self { ops, renames: Vec::new() }
    }

    /// Append an effect, after the ones already there.
    pub fn push(&mut self, op: UpdateOp) -> &mut Self {
        self.ops.push(op);
        self
    }

    /// Append `Set(path, value)`: `$set`, and the computed result of every register operator.
    pub fn set(&mut self, path: Path, value: Value) -> &mut Self {
        self.push(UpdateOp::Set(path, value))
    }

    /// Append `Unset(path)`.
    pub fn unset(&mut self, path: Path) -> &mut Self {
        self.push(UpdateOp::Unset(path))
    }

    /// Append `Inc(path, by)`: the operand as written (never `old + by`).
    pub fn inc(&mut self, path: Path, by: Value) -> &mut Self {
        self.push(UpdateOp::Inc(path, by))
    }

    /// Append `$rename from to`: `Unset(from)` then `Set(to, value)`, where `value` is what the
    /// product read at `from`; `Value::Unset` means the source is absent, and a rename of an absent
    /// source is a no-op: NO effects (a `Set(to, Unset)` would delete the target). The source is
    /// recorded ([`Effects::renames`]) so that `apply` refuses it at the writer when it is, or
    /// holds, a counter ([`UpdateRefused::RenameOfCounter`]).
    pub fn rename(&mut self, from: Path, to: Path, value: Value) -> &mut Self {
        if value == Value::Unset {
            return self;
        }
        self.renames.push(from.clone());
        self.unset(from).set(to, value)
    }

    /// The sources of the recorded renames, in order.
    pub fn renames(&self) -> &[Path] {
        &self.renames
    }

    /// No effect at all: nothing to write, no stamp.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Write the effects as one update with the writer's own clock, through
    /// [`Writer::write_update_renaming`] (the rename check is the writer's). Empty effects write
    /// nothing: [`Outcome::Unchanged`], or `Refused(NoLife)` when no life exists (that takes
    /// precedence).
    pub fn apply(&self, writer: &mut Writer, skew: u64) -> Outcome {
        if self.is_empty() {
            return Self::empty_outcome(writer);
        }
        writer.write_update_renaming(&self.ops, &self.renames, skew).into()
    }

    /// [`Effects::apply`] with the stamp taken from `stamp` (not called for a refusal or a no-op);
    /// a rejected stamp is the `Err`.
    pub fn apply_stamped(
        &self,
        writer: &mut Writer,
        stamp: &mut dyn FnMut() -> MStamp,
    ) -> Result<Outcome, UpdateError> {
        if self.is_empty() {
            return Ok(Self::empty_outcome(writer));
        }
        match writer.write_update_renaming_stamped(&self.ops, &self.renames, stamp) {
            Err(UpdateError::Refused(r)) => Ok(Outcome::Refused(r)),
            Err(e) => Err(e),
            Ok(w) => Ok(Outcome::from(Ok(w))),
        }
    }

    fn empty_outcome(writer: &Writer) -> Outcome {
        if writer.alive().is_empty() {
            Outcome::Refused(UpdateRefused::NoLife)
        } else {
            Outcome::Unchanged
        }
    }
}

impl FromIterator<UpdateOp> for Effects {
    fn from_iter<I: IntoIterator<Item = UpdateOp>>(it: I) -> Self {
        Self { ops: it.into_iter().collect(), renames: Vec::new() }
    }
}

/// How an update ends at the writer: `Ok(Some)`, `Ok(None)` and `Err` of
/// `Result<Option<Written>, UpdateRefused>`, named.
#[derive(Debug, PartialEq)]
pub enum Outcome {
    /// One write, one stamp: the response carries the stamp (`modified: true`).
    Modified(Box<Written>),
    /// Nothing changed: no stamp, no entry (`modified: false`, a 200).
    Unchanged,
    /// Refused, nothing changed: HTTP 400 with the refusal, except `NoLife` (no match, see "Outcome
    /// mapping" in the module docs).
    Refused(UpdateRefused),
}

impl Outcome {
    /// The `modified` flag of the response.
    pub fn modified(&self) -> bool {
        matches!(self, Outcome::Modified(_))
    }

    /// The refusal, when it is one.
    pub fn refusal(&self) -> Option<&UpdateRefused> {
        match self {
            Outcome::Refused(r) => Some(r),
            _ => None,
        }
    }
}

impl From<Result<Option<Written>, UpdateRefused>> for Outcome {
    fn from(r: Result<Option<Written>, UpdateRefused>) -> Self {
        match r {
            Ok(Some(w)) => Outcome::Modified(Box::new(w)),
            Ok(None) => Outcome::Unchanged,
            Err(e) => Outcome::Refused(e),
        }
    }
}

/// The effects of an upsert (`update` with `upsert: true`) when no document matches; the contract
/// is in the module docs, "Upserts". `from_filter` are the filter's equality fields (not `_id`),
/// written as creation cells like `$setOnInsert` and the lowest layer; `set_on_insert` the
/// `$setOnInsert` fields; `set` the `$set` fields and the computed values of the register
/// operators; `inc` the `$inc` operands (an `Int32`, `Int64`, `Decimal128` or `Double`, as
/// [`UpdateOp::Inc`]). On an existing document `from_filter` and `set_on_insert` are ignored and
/// `set`/`inc` are an ordinary update. Several `$inc` on distinct paths are allowed; the storage
/// side (S2a) generalises `Writer::upsert_soi*` (module docs, "S2a work item").
#[derive(Clone, Debug, Default)]
pub struct UpsertEffects {
    pub from_filter: Vec<(Path, Value)>,
    pub set_on_insert: Vec<(Path, Value)>,
    pub set: Vec<(Path, Value)>,
    pub inc: Vec<(Path, Value)>,
}

/// Who wrote a creation cell: a cell whose subtree came only from the filter and/or
/// `$setOnInsert` is a creation cell (it can be dropped by a later merge); any `$set` part makes
/// it `Plain`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellTag {
    /// Only from the filter and/or `$setOnInsert`: tagged as a creation cell.
    Creation,
    /// Some part from `$set`: untagged.
    Plain,
}

/// One TOP-LEVEL field of the composed creation: `path` has one segment, `value` is the whole
/// composed subtree (a `Document` or a scalar), which the writer expands as for an insert.
#[derive(Clone, Debug, PartialEq)]
pub struct CreationCell {
    pub path: Path,
    pub value: Value,
    pub tag: CellTag,
}

impl UpsertEffects {
    /// The PARSE-time check, run on every update and upsert, matched or not: refuse any two paths
    /// of `set_on_insert`, `set` and `inc`, in or across the lists, where one is equal to or below
    /// the other ([`UpdateRefused::Overlap`], the first pair found, in sorted order). It never
    /// looks at `from_filter`: see [`UpsertEffects::check_creation`].
    pub fn check(&self) -> Result<(), UpdateRefused> {
        let mut all: Vec<&Path> =
            self.set_on_insert.iter().chain(&self.set).chain(&self.inc).map(|(p, _)| p).collect();
        first_overlap(&mut all)
    }

    /// The checks of the CREATION path only (no document matched): `from_filter` leaves that
    /// overlap each other, and an `$inc` equal to, below or above a filter leaf, are
    /// [`UpdateRefused::Overlap`], compared on the composed leaves (so `{"a.b": 1}` and
    /// `{a: {b: 1}}` behave alike). Filter paths starting with `_id` are left out.
    pub fn check_creation(&self) -> Result<(), UpdateRefused> {
        let mut filter = Vec::new();
        for (p, v) in self.from_filter.iter().filter(|(p, _)| !is_id(p)) {
            leaf_paths(p, v, &mut filter)?;
        }
        let mut sorted: Vec<&Path> = filter.iter().collect();
        first_overlap(&mut sorted)?;
        for f in &filter {
            if let Some((q, _)) =
                self.inc.iter().find(|(q, _)| q == f || q.is_below(f) || f.is_below(q))
            {
                return Err(UpdateRefused::Overlap(f.clone(), q.clone()));
            }
        }
        Ok(())
    }

    /// The creation as one cell per top-level field, in a deterministic order: the composition of
    /// "Upserts" (b2). Runs [`UpsertEffects::check`] and [`UpsertEffects::check_creation`] first.
    /// `inc` is not part of the cells.
    pub fn creation_cells(&self) -> Result<Vec<CreationCell>, UpdateRefused> {
        self.check()?;
        self.check_creation()?;
        let mut root: Vec<(String, Node)> = Vec::new();
        for (p, v) in self.from_filter.iter().filter(|(p, _)| !is_id(p)) {
            put(&mut root, p, p.segments(), expand(v, CellTag::Creation))?;
        }
        let higher = self
            .set_on_insert
            .iter()
            .map(|(p, v)| (p, v, CellTag::Creation))
            .chain(self.set.iter().map(|(p, v)| (p, v, CellTag::Plain)));
        for (q, v, tag) in higher {
            if *v == Value::Unset {
                unset(&mut root, q.segments());
            } else {
                put(&mut root, q, q.segments(), expand(v, tag))?;
            }
        }
        root.into_iter()
            .map(|(name, node)| {
                let path = Path::flat(&name).map_err(UpdateRefused::BadPath)?;
                let tag = if node.all_creation() { CellTag::Creation } else { CellTag::Plain };
                Ok(CreationCell { path, value: node.into_value(), tag })
            })
            .collect()
    }
}

/// A composed subtree: a leaf value with the tag of the layer that wrote it, or a map.
enum Node {
    Leaf(Value, CellTag),
    Map(Vec<(String, Node)>),
}

impl Node {
    fn all_creation(&self) -> bool {
        match self {
            Node::Leaf(_, t) => *t == CellTag::Creation,
            Node::Map(f) => f.iter().all(|(_, n)| n.all_creation()),
        }
    }

    fn into_value(self) -> Value {
        match self {
            Node::Leaf(v, _) => v,
            Node::Map(f) => {
                Value::Document(f.into_iter().map(|(k, n)| (k, n.into_value())).collect())
            }
        }
    }
}

/// A value as a subtree: a non-empty document is a map, everything else (scalars, arrays, an empty
/// document) is a leaf.
fn expand(v: &Value, tag: CellTag) -> Node {
    match v {
        Value::Document(f) if !f.is_empty() => {
            Node::Map(f.iter().map(|(k, fv)| (k.clone(), expand(fv, tag))).collect())
        }
        _ => Node::Leaf(v.clone(), tag),
    }
}

/// Put `node` at `segs` below `fields`: an existing field is replaced IN PLACE, a new one is
/// appended; a missing map is created, an empty-document leaf gives way, any other leaf on the way
/// is `BelowScalar(full)`.
fn put(
    fields: &mut Vec<(String, Node)>,
    full: &Path,
    segs: &[String],
    node: Node,
) -> Result<(), UpdateRefused> {
    let (first, rest) = segs.split_first().expect("a path has a segment");
    let at = fields.iter().position(|(k, _)| k == first);
    if rest.is_empty() {
        match at {
            Some(i) => fields[i].1 = node,
            None => fields.push((first.clone(), node)),
        }
        return Ok(());
    }
    let i = match at {
        Some(i) => i,
        None => {
            fields.push((first.clone(), Node::Map(Vec::new())));
            fields.len() - 1
        }
    };
    if matches!(&fields[i].1, Node::Leaf(Value::Document(d), _) if d.is_empty()) {
        fields[i].1 = Node::Map(Vec::new());
    }
    match &mut fields[i].1 {
        Node::Map(inner) => put(inner, full, rest, node),
        Node::Leaf(..) => Err(UpdateRefused::BelowScalar(full.clone())),
    }
}

/// Remove the field at `segs`; a no-op (no refusal) when it is absent or below a leaf.
fn unset(fields: &mut Vec<(String, Node)>, segs: &[String]) {
    let Some((first, rest)) = segs.split_first() else { return };
    let Some(i) = fields.iter().position(|(k, _)| k == first) else { return };
    if rest.is_empty() {
        fields.remove(i);
    } else if let Node::Map(inner) = &mut fields[i].1 {
        unset(inner, rest);
    }
}

/// Any path whose first segment is `_id`: the upsert's `_id` logic owns it.
fn is_id(p: &Path) -> bool {
    p.segments().first().is_some_and(|s| s == "_id")
}

/// The first pair, in sorted order, where one path is equal to or below the other.
fn first_overlap(paths: &mut [&Path]) -> Result<(), UpdateRefused> {
    // sorted, a path is followed by its extensions, so an overlap is between neighbours
    paths.sort();
    for w in paths.windows(2) {
        if w[0] == w[1] || w[1].is_below(w[0]) {
            return Err(UpdateRefused::Overlap(w[0].clone(), w[1].clone()));
        }
    }
    Ok(())
}

/// The leaf paths of `v` at `p` (a non-empty document is expanded; everything else is a leaf).
fn leaf_paths(p: &Path, v: &Value, out: &mut Vec<Path>) -> Result<(), UpdateRefused> {
    match v {
        Value::Document(fields) if !fields.is_empty() => {
            for (name, fv) in fields {
                let child = Path::new(p.segments().iter().cloned().chain([name.clone()]))
                    .map_err(UpdateRefused::BadPath)?;
                leaf_paths(&child, fv, out)?;
            }
        }
        Value::Unset => {}
        _ => out.push(p.clone()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::Hlc;
    use crate::ids::NodeId;
    use crate::merge_state::{Epoch, JoinOptions, StampError, WriterId};

    fn p(x: &str) -> Path {
        Path::flat(x).unwrap()
    }
    fn np(segs: &[&str]) -> Path {
        Path::new(segs.iter().copied()).unwrap()
    }
    fn i(n: i64) -> Value {
        Value::Int64(n)
    }
    fn st(x: &str) -> Value {
        Value::String(x.into())
    }
    fn wid(b: u8) -> WriterId {
        WriterId { node: NodeId::from_bytes([b; 16]), epoch: Epoch::from_u64(0) }
    }
    fn writer_of(b: u8) -> Writer {
        Writer::new(wid(b), JoinOptions::default())
    }
    fn writer() -> Writer {
        let mut w = writer_of(7);
        w.insert(&[(p("x"), i(1)), (p("n"), i(10))], 0).unwrap();
        w
    }
    /// A refusal leaves the state and the clock as they were.
    fn untouched(w: &Writer, before: &(crate::merge_state::MergeState, Hlc)) {
        assert_eq!(w.state, before.0);
        assert_eq!(w.clock, before.1);
    }
    fn snap(w: &Writer) -> (crate::merge_state::MergeState, Hlc) {
        (w.state.clone(), w.clock)
    }

    #[test]
    fn effects_keep_operator_order_and_are_deterministic() {
        let build = || {
            let mut e = Effects::new();
            e.set(p("b"), i(1)).inc(p("n"), i(5)).unset(p("a"));
            e
        };
        let (a, b) = (build(), build());
        assert_eq!(a.len(), 3);
        assert!(matches!(a.ops[0], UpdateOp::Set(..)));
        assert!(matches!(a.ops[1], UpdateOp::Inc(..)));
        assert!(matches!(a.ops[2], UpdateOp::Unset(..)));
        assert_eq!(format!("{:?}", a.ops), format!("{:?}", b.ops));
    }

    #[test]
    fn rename_is_unset_then_set_on_disjoint_paths() {
        let mut e = Effects::new();
        e.rename(p("a"), p("b"), i(3));
        match e.ops.as_slice() {
            [UpdateOp::Unset(from), UpdateOp::Set(to, Value::Int64(3))] => {
                assert_eq!((from, to), (&p("a"), &p("b")));
            }
            o => panic!("{o:?}"),
        }
        assert_eq!(e.renames(), &[p("a")]);
    }

    #[test]
    fn a_rename_of_a_missing_source_has_no_effects_and_keeps_the_target() {
        let mut w = writer();
        w.write_set(&p("t"), st("keep"), 0).unwrap();
        let before = snap(&w);
        let mut e = Effects::new();
        e.rename(p("gone"), p("t"), Value::Unset);
        assert!(e.is_empty());
        assert!(e.renames().is_empty());
        assert_eq!(e.apply(&mut w, 0), Outcome::Unchanged);
        untouched(&w, &before);
        assert_eq!(w.visible().unwrap()[&p("t")], st("keep"));
    }

    #[test]
    fn a_dotted_name_is_one_segment() {
        let path = Path::new(["a.b"]).unwrap();
        assert_eq!(path.segments().len(), 1);
        let mut e = Effects::new();
        e.set(path.clone(), i(1));
        assert!(matches!(&e.ops[0], UpdateOp::Set(q, _) if *q == path));
    }

    #[test]
    fn empty_effects_write_nothing_and_no_life_wins() {
        let mut w = writer();
        let before = snap(&w);
        let o = Effects::new().apply(&mut w, 0);
        assert_eq!(o, Outcome::Unchanged);
        assert!(!o.modified());
        untouched(&w, &before);
        // no life: NoLife takes precedence over Unchanged, plain and stamped
        let mut empty = writer_of(1);
        assert_eq!(Effects::new().apply(&mut empty, 0), Outcome::Refused(UpdateRefused::NoLife));
        let mut called = false;
        let mut stamp = || {
            called = true;
            unreachable!()
        };
        let o = Effects::new().apply_stamped(&mut empty, &mut stamp).unwrap();
        assert_eq!(o, Outcome::Refused(UpdateRefused::NoLife));
        assert!(!called);
    }

    #[test]
    fn outcomes_map_modified_unchanged_and_refused() {
        let mut w = writer();
        let mut e = Effects::new();
        e.inc(p("n"), i(5)).set(p("y"), i(2));
        assert!(e.apply(&mut w, 0).modified());
        // the same value again is a no-op, not a refusal
        let again = Effects::from_ops(vec![UpdateOp::Set(p("y"), i(2))]).apply(&mut w, 0);
        assert_eq!(again, Outcome::Unchanged);
        // overlapping paths refuse the whole update, the clock does not move
        let before = snap(&w);
        let mut bad = Effects::new();
        bad.set(p("y"), i(3)).set(np(&["y", "z"]), i(1));
        let o = bad.apply(&mut w, 0);
        assert!(matches!(o.refusal(), Some(UpdateRefused::Overlap(..))));
        untouched(&w, &before);
        // a double against an integer counter is a refusal
        let o = Effects::from_ops(vec![UpdateOp::Inc(p("n"), Value::Double(1.5))]).apply(&mut w, 0);
        assert!(matches!(o.refusal(), Some(UpdateRefused::IncDoubleOnCounter(_))));
        untouched(&w, &before);
    }

    #[test]
    fn a_counter_rename_is_refused_before_any_write_or_clock_move() {
        let mut w = writer(); // n is an Int64 counter
        let before = snap(&w);
        let mut e = Effects::new();
        e.rename(p("n"), p("m"), i(10));
        assert_eq!(e.len(), 2);
        let o = e.apply(&mut w, 0);
        assert_eq!(o.refusal(), Some(&UpdateRefused::RenameOfCounter(p("n"))));
        untouched(&w, &before);
        // stamped: refused, the stamp closure is never called
        let mut called = false;
        let mut stamp = || {
            called = true;
            unreachable!()
        };
        let o = e.apply_stamped(&mut w, &mut stamp).unwrap();
        assert_eq!(o.refusal(), Some(&UpdateRefused::RenameOfCounter(p("n"))));
        assert!(!called);
        untouched(&w, &before);
        // a string, a double: renamed
        w.write_set(&p("s"), st("v"), 0).unwrap();
        w.write_set(&p("d"), Value::Double(1.5), 0).unwrap();
        let mut r = Effects::new();
        r.rename(p("s"), p("s2"), st("v"));
        assert!(r.apply(&mut w, 0).modified());
        let mut r = Effects::new();
        r.rename(p("d"), p("d2"), Value::Double(1.5));
        assert!(r.apply(&mut w, 0).modified());
    }

    #[test]
    fn apply_stamped_passes_a_stamp_error_through_and_refusals_never_call_it() {
        let mut w = writer();
        let before = snap(&w);
        let other = MStamp { hlc: Hlc::new(1, 0), writer: wid(9) };
        let mut stamp = || other;
        let mut e = Effects::new();
        e.set(p("y"), i(2));
        let r = e.apply_stamped(&mut w, &mut stamp);
        assert!(matches!(
            r,
            Err(UpdateError::Stamp(StampError::OtherWriter { stamp, .. })) if stamp == wid(9)
        ));
        untouched(&w, &before);
        // an overlap is refused before the closure is asked for a stamp
        let mut called = false;
        let mut stamp = || {
            called = true;
            other
        };
        let mut bad = Effects::new();
        bad.set(p("y"), i(1)).set(np(&["y", "z"]), i(1));
        let o = bad.apply_stamped(&mut w, &mut stamp).unwrap();
        assert!(matches!(o.refusal(), Some(UpdateRefused::Overlap(..))));
        assert!(!called);
    }

    #[test]
    fn rename_of_a_sub_document_with_a_counter_is_refused_and_onto_a_counter_is_a_set() {
        let mut w = writer();
        let doc = Value::Document(vec![("c".into(), i(1)), ("t".into(), st("v"))]);
        w.write_set(&p("sub"), doc.clone(), 0).unwrap();
        let before = snap(&w);
        let mut e = Effects::new();
        e.rename(p("sub"), p("moved"), doc);
        let o = e.apply(&mut w, 0);
        assert_eq!(o.refusal(), Some(&UpdateRefused::RenameOfCounter(p("sub"))));
        untouched(&w, &before);
        // onto a counter path: allowed (a Set folds), the source is a string
        w.write_set(&p("s"), st("v"), 0).unwrap();
        let mut r = Effects::new();
        r.rename(p("s"), p("n"), st("v"));
        assert!(r.apply(&mut w, 0).modified());
        assert_eq!(w.visible().unwrap()[&p("n")], st("v"));
    }

    #[test]
    fn a_one_segment_dotted_field_holding_an_int_does_not_block_renaming_its_prefix() {
        let mut w = writer_of(3);
        w.insert(&[(p("a"), st("s")), (np(&["a.b"]), i(1))], 0).unwrap();
        let mut e = Effects::new();
        e.rename(p("a"), p("z"), st("s"));
        assert!(e.apply(&mut w, 0).modified());
    }

    #[test]
    fn a_counter_in_a_dead_or_superseded_life_does_not_block_a_rename() {
        // Every join drops the cells of a dead life, so such cells exist only in a state that is
        // not normalised: mark the life dead by hand, keeping its cells, so that a check that
        // ignores life state would see the counter.
        let mut w = writer();
        let life = *w.state.lives.keys().next().unwrap();
        w.state.dead.insert(life);
        assert!(w.state.regs.keys().any(|(_, k)| *k == p("n")), "the dead life still holds n");
        assert!(!w.sees_counter_at_or_under(&p("n")));
        // deleted and re-inserted without `n`: the rename is not blocked
        let mut w = writer();
        w.write_delete(0).unwrap();
        w.insert(&[(p("x"), i(2))], 0).unwrap();
        assert!(!w.sees_counter_at_or_under(&p("n")));
        let mut e = Effects::new();
        e.rename(p("n"), p("m"), i(10));
        assert!(e.apply(&mut w, 0).modified());
    }

    #[test]
    fn u2_unseen_increments_that_reappear_make_the_rename_a_refusal() {
        let mut w2 = writer();
        let mut w1 = writer_of(8);
        w1.receive(&w2.state);
        // w1 makes the first increment of `u`; w2 has not seen it and sees no `u`
        w1.write_inc(&p("u"), 1, 0).unwrap();
        let mut gone = Effects::new();
        gone.rename(p("u"), p("v"), Value::Unset);
        assert_eq!(gone.apply(&mut w2, 0), Outcome::Unchanged);
        // the increment reaches w2: it now sees a counter at `u`
        w2.receive(&w1.state);
        let before = snap(&w2);
        let mut e = Effects::new();
        e.rename(p("u"), p("v"), i(1));
        assert_eq!(e.apply(&mut w2, 0).refusal(), Some(&UpdateRefused::RenameOfCounter(p("u"))));
        untouched(&w2, &before);
    }

    #[test]
    fn paths_through_an_array_are_one_set_of_the_outermost_array() {
        let mut w = writer();
        w.write_set(&p("a"), Value::Array(vec![i(5), i(6)]), 0).unwrap();
        // `$set {"a.0": 1, "a.1": 2}` as one Set of the array: accepted
        let mut ok = w.clone();
        let mut one = Effects::new();
        one.set(p("a"), Value::Array(vec![i(1), i(2)]));
        assert!(one.apply(&mut ok, 0).modified());
        assert_eq!(ok.visible().unwrap()[&p("a")], Value::Array(vec![i(1), i(2)]));
        // two leaf Sets are below an array
        let before = snap(&w);
        let mut naive = Effects::new();
        naive.set(np(&["a", "0"]), i(1)).set(np(&["a", "1"]), i(2));
        let o = naive.apply(&mut w, 0);
        assert!(matches!(o.refusal(), Some(UpdateRefused::BelowScalar(_))));
        // the array and an element together are an overlap
        let mut both = Effects::new();
        both.set(p("a"), Value::Array(vec![i(1)])).set(np(&["a", "0"]), i(1));
        assert!(matches!(both.apply(&mut w, 0).refusal(), Some(UpdateRefused::Overlap(..))));
        untouched(&w, &before);
    }

    #[test]
    fn upsert_effects_shape_and_overlap_refusal() {
        let u = UpsertEffects {
            from_filter: vec![(p("f"), i(5))],
            set_on_insert: vec![(p("a"), i(1))],
            set: vec![(p("b"), i(2))],
            inc: vec![(p("c"), i(1)), (p("d"), Value::Double(0.5))],
        };
        assert!(u.check().is_ok(), "several $inc on distinct paths are allowed");
        assert!(UpsertEffects::default().check().is_ok(), "an empty upsert is valid");
        let same = UpsertEffects { inc: vec![(p("a"), i(1))], ..u.clone() };
        assert!(matches!(same.check(), Err(UpdateRefused::Overlap(..))));
        let below = UpsertEffects { set: vec![(np(&["c", "x"]), i(1))], ..u.clone() };
        assert!(matches!(below.check(), Err(UpdateRefused::Overlap(..))));
        let twice = UpsertEffects { inc: vec![(p("c"), i(1)), (p("c"), i(2))], ..u.clone() };
        assert!(matches!(twice.check(), Err(UpdateRefused::Overlap(..))));
        // the filter layer is the lowest: a $set or a $setOnInsert on its path is fine, and an
        // $inc on an unrelated path
        let over = UpsertEffects {
            set: vec![(p("f"), i(9))],
            set_on_insert: vec![(p("g"), i(1))],
            from_filter: vec![(p("f"), i(5)), (p("g"), i(5)), (p("h"), i(5))],
            inc: vec![(p("k"), i(1))],
        };
        assert!(over.check().is_ok());
    }

    #[test]
    fn the_no_op_test_is_by_representation() {
        let same = |a: Option<&Value>, b: &Value| Value::same(a, b);
        assert!(!same(Some(&Value::Double(0.0)), &Value::Double(-0.0)));
        let nan = Value::Double(f64::NAN);
        assert!(same(Some(&nan), &nan));
        assert!(!same(Some(&Value::Decimal128([1; 16])), &Value::Decimal128([2; 16])));
        assert!(!same(Some(&i(2)), &Value::Double(2.0)));
        assert!(!same(Some(&Value::Int32(2)), &i(2)));
        assert!(!same(None, &Value::Null));
        assert!(same(None, &Value::Unset));
    }

    fn cell(name: &str, value: Value, tag: CellTag) -> CreationCell {
        CreationCell { path: p(name), value, tag }
    }
    fn filter(f: Vec<(Path, Value)>) -> UpsertEffects {
        UpsertEffects { from_filter: f, ..Default::default() }
    }
    fn dc(f: Vec<(&str, Value)>) -> Value {
        Value::Document(f.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
    }

    #[test]
    fn composition_is_one_cell_per_top_level_field_with_its_tag() {
        let u = UpsertEffects {
            from_filter: vec![(np(&["a", "b"]), i(1)), (p("d"), dc(vec![("x", i(1))]))],
            set_on_insert: vec![(p("s"), i(2)), (p("m"), dc(vec![("x", i(1))]))],
            set: vec![(p("t"), i(3))],
            inc: vec![],
        };
        assert_eq!(
            u.creation_cells().unwrap(),
            vec![
                cell("a", dc(vec![("b", i(1))]), CellTag::Creation),
                cell("d", dc(vec![("x", i(1))]), CellTag::Creation),
                cell("s", i(2), CellTag::Creation),
                cell("m", dc(vec![("x", i(1))]), CellTag::Creation),
                cell("t", i(3), CellTag::Plain),
            ]
        );
        assert!(UpsertEffects::default().creation_cells().unwrap().is_empty());
        // $setOnInsert {a: {x: 1}}: one cell a = Document, Creation
        let soi = UpsertEffects {
            set_on_insert: vec![(p("a"), dc(vec![("x", i(1))]))],
            ..Default::default()
        };
        assert_eq!(
            soi.creation_cells().unwrap(),
            vec![cell("a", dc(vec![("x", i(1))]), CellTag::Creation)]
        );
    }

    #[test]
    fn a_higher_layer_equal_to_or_above_a_filter_path_overrides_it_in_place() {
        let eq = UpsertEffects { set: vec![(p("f"), i(9))], ..filter(vec![(p("f"), i(5))]) };
        assert_eq!(eq.creation_cells().unwrap(), vec![cell("f", i(9), CellTag::Plain)]);
        let soi =
            UpsertEffects { set_on_insert: vec![(p("f"), i(8))], ..filter(vec![(p("f"), i(5))]) };
        assert_eq!(soi.creation_cells().unwrap(), vec![cell("f", i(8), CellTag::Creation)]);
        // filter {"a.b": 1} with $set {a: {c: 2}} gives a: {c: 2}
        let anc = UpsertEffects {
            set: vec![(p("a"), dc(vec![("c", i(2))]))],
            ..filter(vec![(np(&["a", "b"]), i(1))])
        };
        assert_eq!(
            anc.creation_cells().unwrap(),
            vec![cell("a", dc(vec![("c", i(2))]), CellTag::Plain)]
        );
    }

    #[test]
    fn a_higher_layer_below_a_filter_document_merges_into_it() {
        // filter {a: {x: 1}} with $set {"a.y": 2}: one cell a = {x: 1, y: 2}, Plain
        let u = UpsertEffects {
            set: vec![(np(&["a", "y"]), i(2))],
            ..filter(vec![(p("a"), dc(vec![("x", i(1))]))])
        };
        assert_eq!(
            u.creation_cells().unwrap(),
            vec![cell("a", dc(vec![("x", i(1)), ("y", i(2))]), CellTag::Plain)]
        );
        // an empty filter document gives way
        let e = UpsertEffects {
            set: vec![(np(&["a", "y"]), i(2))],
            ..filter(vec![(p("a"), dc(vec![]))])
        };
        assert_eq!(
            e.creation_cells().unwrap(),
            vec![cell("a", dc(vec![("y", i(2))]), CellTag::Plain)]
        );
        // below a non-document filter value is refused
        let bad =
            UpsertEffects { set: vec![(np(&["a", "y"]), i(2))], ..filter(vec![(p("a"), i(5))]) };
        assert_eq!(bad.creation_cells(), Err(UpdateRefused::BelowScalar(np(&["a", "y"]))));
    }

    #[test]
    fn an_unset_through_a_scalar_is_a_no_op_and_an_unset_of_a_filter_field_removes_it() {
        let through = UpsertEffects {
            set: vec![(np(&["a", "y"]), Value::Unset)],
            ..filter(vec![(p("a"), i(5))])
        };
        assert_eq!(through.creation_cells().unwrap(), vec![cell("a", i(5), CellTag::Creation)]);
        let missing = UpsertEffects {
            set: vec![(np(&["q", "y"]), Value::Unset)],
            ..filter(vec![(p("a"), i(5))])
        };
        assert_eq!(missing.creation_cells().unwrap(), vec![cell("a", i(5), CellTag::Creation)]);
        let removes = UpsertEffects {
            set: vec![(p("a"), Value::Unset)],
            ..filter(vec![(p("a"), i(5)), (p("b"), i(6))])
        };
        assert_eq!(removes.creation_cells().unwrap(), vec![cell("b", i(6), CellTag::Creation)]);
    }

    #[test]
    fn the_order_is_filter_order_overrides_in_place_and_new_fields_as_listed() {
        let u = UpsertEffects {
            from_filter: vec![(p("z"), i(1)), (p("a"), i(2)), (p("m"), i(3))],
            set_on_insert: vec![(p("c"), i(4))],
            set: vec![(p("a"), i(9)), (p("b"), i(5))],
            inc: vec![],
        };
        let names: Vec<String> =
            u.creation_cells().unwrap().iter().map(|c| c.path.segments()[0].clone()).collect();
        assert_eq!(names, ["z", "a", "m", "c", "b"]);
        assert_eq!(u.creation_cells().unwrap()[1].value, i(9));
    }

    #[test]
    fn any_filter_path_starting_with_id_is_left_out() {
        let u = filter(vec![(p("_id"), i(1)), (np(&["_id", "x"]), i(1)), (p("a"), i(2))]);
        assert!(u.check_creation().is_ok(), "_id and _id.x do not even overlap here");
        assert_eq!(u.creation_cells().unwrap(), vec![cell("a", i(2), CellTag::Creation)]);
    }

    #[test]
    fn filter_paths_that_overlap_each_other_are_refused_on_creation_only() {
        let twice = filter(vec![(p("a"), i(1)), (p("a"), i(2))]);
        assert!(matches!(twice.check_creation(), Err(UpdateRefused::Overlap(..))));
        assert!(twice.check().is_ok(), "the parse-time check never looks at the filter");
        let nested = filter(vec![(p("a"), i(1)), (np(&["a", "b"]), i(2))]);
        assert!(matches!(nested.creation_cells(), Err(UpdateRefused::Overlap(..))));
        // compared on the composed leaves: a: {b: 1} with "a.b" is the same path twice
        let spelled = filter(vec![(p("a"), dc(vec![("b", i(1))])), (np(&["a", "b"]), i(2))]);
        assert!(matches!(spelled.check_creation(), Err(UpdateRefused::Overlap(..))));
        let disjoint = filter(vec![(np(&["a", "b"]), i(1)), (np(&["a", "c"]), i(2))]);
        assert!(disjoint.check_creation().is_ok());
    }

    #[test]
    fn an_inc_on_a_path_the_filter_writes_is_refused_on_creation_but_not_when_matched() {
        // {a: 5} with $inc a 1: refused on the creation path (the reference gives 6; a deviation)
        let on = UpsertEffects { inc: vec![(p("a"), i(1))], ..filter(vec![(p("a"), i(5))]) };
        assert_eq!(on.check_creation(), Err(UpdateRefused::Overlap(p("a"), p("a"))));
        assert!(on.creation_cells().is_err());
        // a MATCHED upsert is an ordinary update: the parse-time check accepts it
        assert!(on.check().is_ok());
        // both spellings of the same filter behave alike: refused on the leaf, allowed beside it
        for f in [vec![(np(&["a", "b"]), i(1))], vec![(p("a"), dc(vec![("b", i(1))]))]] {
            let hit = UpsertEffects { inc: vec![(np(&["a", "b"]), i(1))], ..filter(f.clone()) };
            assert!(matches!(hit.check_creation(), Err(UpdateRefused::Overlap(..))));
            let beside = UpsertEffects { inc: vec![(np(&["a", "c"]), i(1))], ..filter(f.clone()) };
            assert!(beside.check_creation().is_ok());
            // above a filter leaf
            let above = UpsertEffects { inc: vec![(p("a"), i(1))], ..filter(f) };
            assert!(matches!(above.check_creation(), Err(UpdateRefused::Overlap(..))));
        }
        // below a scalar leaf, and an unrelated path
        let below =
            UpsertEffects { inc: vec![(np(&["a", "n"]), i(1))], ..filter(vec![(p("a"), i(5))]) };
        assert!(matches!(below.check_creation(), Err(UpdateRefused::Overlap(..))));
        let free = UpsertEffects {
            inc: vec![(p("n"), i(1)), (np(&["a", "c"]), i(1))],
            ..filter(vec![(np(&["a", "b"]), i(1)), (p("f"), i(2))])
        };
        assert!(free.check_creation().is_ok());
        assert_eq!(free.creation_cells().unwrap().len(), 2);
    }
}
