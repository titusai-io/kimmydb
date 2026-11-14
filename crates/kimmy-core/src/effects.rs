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
//! * (b) The filter's equality fields (not `_id`, which is the key) go in `from_filter`, TAGGED as
//!   creation cells like `$setOnInsert`. They are the LOWEST layer: a `$set` or `$setOnInsert` on
//!   the same path overrides them, and that is NOT an overlap ([`UpsertEffects::check`] does not
//!   refuse it); an `$inc` on a filter field increments the filter value (`{a: 5}` with `$inc a 1`
//!   gives 6, by the two-write shape). (Recorded on the Q7 design page as an amendment.)
//! * (b2) Composition, before the writer, into NON-overlapping leaf cells
//!   ([`UpsertEffects::creation_cells`]): the layers compose in order, filter first, then
//!   `$setOnInsert` and `$set` on top. A higher-layer path EQUAL to or an ANCESTOR of a filter path
//!   overrides it and the filter leaves below it are dropped (filter `{"a.b": 1}` with `$set
//!   {a: {c: 2}}` gives `a: {c: 2}`); a higher-layer path BELOW a filter document merges into it
//!   (filter `{a: {x: 1}}` with `$set {"a.y": 2}` gives `a: {x: 1, y: 2}`). Tagging is per leaf:
//!   leaves from the filter or from `$setOnInsert` are creation cells, leaves from `$set` are not.
//!   `_id` is the key, not a cell, and is dropped. `from_filter` paths that overlap EACH OTHER (the
//!   same path twice, or an ancestor and a descendant such as a filter on `a` and `a.b`) are
//!   refused as [`UpdateRefused::Overlap`] at parse time: this matches the reference, so it is NOT a
//!   deviation. A higher-layer path below a non-document filter value (filter `{a: 5}`, `$set
//!   {"a.y": 2}`) is [`UpdateRefused::BelowScalar`], as the reference cannot create a field in a
//!   non-document. The `$inc` of a filter field is not a cell: the creation holds the filter value
//!   and the increment follows (`{a: 5}` with `$inc a 1` gives 6). NOTE for S2a: today
//!   `Writer::upsert_inc_on` refuses an increment on a path the creation wrote as a register, so
//!   the slot must be allowed to land on a creation register's base (the S2a work item).
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
//! * RESOLVED: `from_filter` overlap among itself is a parse-time `Overlap` (as the reference);
//!   the layers compose filter, then `$setOnInsert`, then `$set`, see (b2).
//! * OPEN: a higher-layer path below a NON-document filter value is `BelowScalar` here (as the
//!   reference's error); not separately ruled.

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

/// Who wrote a creation cell: a filter or `$setOnInsert` leaf is a creation cell (it can be
/// dropped by a later merge), a `$set` leaf is not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellTag {
    /// From the filter or `$setOnInsert`: tagged as a creation cell.
    Creation,
    /// From `$set`: untagged.
    Plain,
}

/// One leaf of the composed creation: a path, its value and its tag.
#[derive(Clone, Debug, PartialEq)]
pub struct CreationCell {
    pub path: Path,
    pub value: Value,
    pub tag: CellTag,
}

impl UpsertEffects {
    /// The parse-time check, run on every upsert, matched or not: refuse any two paths, in or
    /// across `set_on_insert`, `set` and `inc`, where one is equal to or below the other
    /// ([`UpdateRefused::Overlap`], the first pair found, in sorted order); and any two
    /// `from_filter` paths (without `_id`) where one is equal to or below the other. `from_filter`
    /// is otherwise the lowest layer and is NOT checked against the others: a `$set` or
    /// `$setOnInsert` on a filter path overrides it, and an `$inc` on one increments it.
    pub fn check(&self) -> Result<(), UpdateRefused> {
        let mut all: Vec<&Path> =
            self.set_on_insert.iter().chain(&self.set).chain(&self.inc).map(|(p, _)| p).collect();
        first_overlap(&mut all)?;
        let mut filter: Vec<&Path> =
            self.from_filter.iter().map(|(p, _)| p).filter(|p| !is_id(p)).collect();
        first_overlap(&mut filter)
    }

    /// The creation as non-overlapping leaf cells, in a deterministic order (the surviving filter
    /// leaves, then `$setOnInsert`, then `$set`, each in the order given): the composition of
    /// "Upserts" (b2). Runs [`UpsertEffects::check`] first. A document value is expanded into its
    /// leaves (an empty document, an array and every scalar are leaves); a `Value::Unset` makes no
    /// cell but still overrides the filter below it. `inc` is not part of the cells.
    pub fn creation_cells(&self) -> Result<Vec<CreationCell>, UpdateRefused> {
        self.check()?;
        let mut cells = Vec::new();
        for (p, v) in self.from_filter.iter().filter(|(p, _)| !is_id(p)) {
            leaves(p, v, CellTag::Creation, &mut cells)?;
        }
        let higher = self
            .set_on_insert
            .iter()
            .map(|(p, v)| (p, v, CellTag::Creation))
            .chain(self.set.iter().map(|(p, v)| (p, v, CellTag::Plain)));
        for (q, v, tag) in higher {
            let mut keep = Vec::with_capacity(cells.len());
            for c in cells {
                let below_or_equal = c.path == *q || c.path.is_below(q);
                let ancestor = q.is_below(&c.path);
                if c.tag == CellTag::Creation && below_or_equal {
                    continue; // overridden (only filter and soi cells exist before this layer)
                }
                if ancestor && !matches!(&c.value, Value::Document(f) if f.is_empty()) {
                    return Err(UpdateRefused::BelowScalar(q.clone()));
                }
                if ancestor {
                    continue; // an empty filter document gives way to what is set below it
                }
                keep.push(c);
            }
            cells = keep;
            leaves(q, v, tag, &mut cells)?;
        }
        Ok(cells)
    }
}

fn is_id(p: &Path) -> bool {
    p.segments().first().is_some_and(|s| s == "_id") && p.segments().len() == 1
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

/// Expand `v` at `p` into leaf cells.
fn leaves(
    p: &Path,
    v: &Value,
    tag: CellTag,
    out: &mut Vec<CreationCell>,
) -> Result<(), UpdateRefused> {
    match v {
        Value::Unset => Ok(()),
        Value::Document(fields) if !fields.is_empty() => {
            for (name, fv) in fields {
                let child = Path::new(p.segments().iter().cloned().chain([name.clone()]))
                    .map_err(UpdateRefused::BadPath)?;
                leaves(&child, fv, tag, out)?;
            }
            Ok(())
        }
        _ => {
            out.push(CreationCell { path: p.clone(), value: v.clone(), tag });
            Ok(())
        }
    }
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
    fn a_counter_in_a_dead_life_does_not_block_a_rename() {
        let mut w = writer();
        assert!(w.sees_counter_at_or_under(&p("n")));
        w.write_delete(0).unwrap();
        w.insert(&[(p("n"), st("fresh"))], 0).unwrap();
        let mut e = Effects::new();
        e.rename(p("n"), p("m"), st("fresh"));
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
        // the filter layer is the lowest: a $set, a $setOnInsert or an $inc on its path is fine
        let over = UpsertEffects {
            set: vec![(p("f"), i(9))],
            set_on_insert: vec![(p("g"), i(1))],
            from_filter: vec![(p("f"), i(5)), (p("g"), i(5)), (p("h"), i(5))],
            inc: vec![(p("h"), i(1))],
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

    fn cell(path: Path, value: Value, tag: CellTag) -> CreationCell {
        CreationCell { path, value, tag }
    }
    fn filter(f: Vec<(Path, Value)>) -> UpsertEffects {
        UpsertEffects { from_filter: f, ..Default::default() }
    }
    fn dc(f: Vec<(&str, Value)>) -> Value {
        Value::Document(f.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
    }

    #[test]
    fn composition_filter_alone_and_soi_and_set_tags() {
        let u = UpsertEffects {
            from_filter: vec![(np(&["a", "b"]), i(1)), (p("d"), dc(vec![("x", i(1))]))],
            set_on_insert: vec![(p("s"), i(2))],
            set: vec![(p("t"), i(3))],
            inc: vec![],
        };
        assert_eq!(
            u.creation_cells().unwrap(),
            vec![
                cell(np(&["a", "b"]), i(1), CellTag::Creation),
                cell(np(&["d", "x"]), i(1), CellTag::Creation),
                cell(p("s"), i(2), CellTag::Creation),
                cell(p("t"), i(3), CellTag::Plain),
            ]
        );
        assert!(UpsertEffects::default().creation_cells().unwrap().is_empty());
    }

    #[test]
    fn a_higher_layer_equal_to_or_above_a_filter_path_overrides_it() {
        let eq = UpsertEffects { set: vec![(p("f"), i(9))], ..filter(vec![(p("f"), i(5))]) };
        assert_eq!(eq.creation_cells().unwrap(), vec![cell(p("f"), i(9), CellTag::Plain)]);
        let soi =
            UpsertEffects { set_on_insert: vec![(p("f"), i(8))], ..filter(vec![(p("f"), i(5))]) };
        assert_eq!(soi.creation_cells().unwrap(), vec![cell(p("f"), i(8), CellTag::Creation)]);
        // filter {"a.b": 1} with $set {a: {c: 2}} gives a: {c: 2}
        let anc = UpsertEffects {
            set: vec![(p("a"), dc(vec![("c", i(2))]))],
            ..filter(vec![(np(&["a", "b"]), i(1))])
        };
        assert_eq!(
            anc.creation_cells().unwrap(),
            vec![cell(np(&["a", "c"]), i(2), CellTag::Plain)]
        );
    }

    #[test]
    fn a_higher_layer_below_a_filter_document_merges_into_it() {
        // filter {a: {x: 1}} with $set {"a.y": 2} gives a: {x: 1, y: 2}
        let u = UpsertEffects {
            set: vec![(np(&["a", "y"]), i(2))],
            ..filter(vec![(p("a"), dc(vec![("x", i(1))]))])
        };
        assert_eq!(
            u.creation_cells().unwrap(),
            vec![
                cell(np(&["a", "x"]), i(1), CellTag::Creation),
                cell(np(&["a", "y"]), i(2), CellTag::Plain),
            ]
        );
        // an empty filter document gives way
        let e = UpsertEffects {
            set: vec![(np(&["a", "y"]), i(2))],
            ..filter(vec![(p("a"), dc(vec![]))])
        };
        assert_eq!(e.creation_cells().unwrap(), vec![cell(np(&["a", "y"]), i(2), CellTag::Plain)]);
        // below a non-document filter value is refused
        let bad =
            UpsertEffects { set: vec![(np(&["a", "y"]), i(2))], ..filter(vec![(p("a"), i(5))]) };
        assert_eq!(bad.creation_cells(), Err(UpdateRefused::BelowScalar(np(&["a", "y"]))));
    }

    #[test]
    fn the_id_is_the_key_and_not_a_cell() {
        let u = filter(vec![(p("_id"), i(1)), (p("a"), i(2))]);
        assert_eq!(u.creation_cells().unwrap(), vec![cell(p("a"), i(2), CellTag::Creation)]);
    }

    #[test]
    fn filter_paths_that_overlap_each_other_are_refused() {
        let twice = filter(vec![(p("a"), i(1)), (p("a"), i(2))]);
        assert!(matches!(twice.check(), Err(UpdateRefused::Overlap(..))));
        let nested = filter(vec![(p("a"), i(1)), (np(&["a", "b"]), i(2))]);
        assert!(matches!(nested.creation_cells(), Err(UpdateRefused::Overlap(..))));
        let disjoint = filter(vec![(np(&["a", "b"]), i(1)), (np(&["a", "c"]), i(2))]);
        assert!(disjoint.check().is_ok());
    }

    #[test]
    fn an_inc_on_a_filter_field_leaves_the_filter_value_for_the_increment_to_follow() {
        // {a: 5} with $inc a 1: the creation holds a = 5, the increment (inc list) follows -> 6
        let u = UpsertEffects { inc: vec![(p("a"), i(1))], ..filter(vec![(p("a"), i(5))]) };
        assert!(u.check().is_ok());
        assert_eq!(u.creation_cells().unwrap(), vec![cell(p("a"), i(5), CellTag::Creation)]);
        assert_eq!(u.inc, vec![(p("a"), i(1))]);
    }
}
