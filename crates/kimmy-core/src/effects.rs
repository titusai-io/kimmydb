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
//! 4. Arrays are atomic: every array operator sends the whole new array as one `Set`.
//! 5. Order is the order of the operators in the update, and positions within the list are
//!    deterministic: the same update on the same document yields the same list.
//! 6. `Set(path, Value::Unset)` is an `Unset`; the writer normalises it.
//! 7. A refusal changes nothing (no stamp, no clock move): the whole update is refused.
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
//! | `$inc p n` | `Inc(p, n)` | counter slot on an Int32/Int64/Decimal128 base, register write on a Double base (the writer decides); refusals `IncOperandNotANumber`, `IncTargetNotANumber`, `IncTargetIsAContainer`, `IncDoubleOnCounter`, `IncOutOfRange`, `TooManyContributors`; absent target: counter created |
//! | `$mul`, `$bit` | `Set(p, computed)` | register write (a base write); computed by the product |
//! | `$min`, `$max` | `Set(p, v)` only if it changes, else no effect | register write |
//! | `$currentDate` | `Set(p, now)` | ONE `now` per update, from the product clock as today's `apply(update, doc, now_ms)`; NOT derived from the stamp (the `document` policy stays byte-identical to 0.59) |
//! | `$push`, `$addToSet`, `$pull`, `$pullAll`, `$pop` | `Set(p, whole_array)` | arrays atomic; no effect if the array is unchanged |
//! | positional `$`, `$[]`, `$[id]` (with `arrayFilters`) | `Set(array_path, whole_array)` | the product resolves the position; the array is the leaf |
//! | `$rename a b` | `Unset(a)` + `Set(b, value)` (source recorded) | disjoint paths required (else `Overlap`); a path the WRITER sees as a counter at `a` is `RenameOfCounter(a)` (400), see below |
//! | `$setOnInsert` | none on update; `UpsertEffects::set_on_insert` on an insert | no-op on an existing document |
//! | pipeline update (ADR-265) | per-leaf `Set`/`Unset` of what differs, one list | not `write_replace` (a replace reorders fields); each written path is a [`Path`] of one segment per name |
//! | replace-style update | not an effect list | stays the replace path of the storage layer |
//! | any operator on a missing document | `UpdateRefused::NoLife` | upsert goes through [`UpsertEffects`] |
//! | any two operators on overlapping paths | `UpdateRefused::Overlap` | whole update refused |
//! | a new writer past the caps | `UpdateRefused::WriterCap` | whole update refused |
//!
//! # Counters: `$rename`, and `$mul`/`$min`/`$max` over a counter
//!
//! A path is a COUNTER when, in the writer's state, its visible cell in an alive life is an
//! `Int32`, `Int64` or `Decimal128` (its base and/or slots, design D5/NB1); a `Double` is not.
//! Only the writer's state knows this, so it is decided at the writer
//! ([`Writer::sees_counter`], read-only), never inferred from the `Document` by the product.
//! [`Effects::apply`] and [`Effects::apply_stamped`] refuse the whole update with
//! [`UpdateRefused::RenameOfCounter`] (400) BEFORE writing or moving the clock when a recorded
//! rename source is a counter. Race (U2, as the Q7 design words it): a counter field THIS member
//! sees as a counter cannot be renamed (400); a concurrent first `$inc` elsewhere races the rename
//! and its increments reappear on the source path (U2), documented, not "always a 400".
//!
//! `$mul`, `$min`, `$max` (and `$bit`) over a counter path are a register write, a base write:
//! `Set(path, computed)`. The fold rule applies (it folds every slot of the path that it holds in
//! an alive life); increments it has not seen add on top (design line 117). Nothing new in the API.
//!
//! # Upserts
//!
//! [`UpsertEffects`] is `{ set_on_insert, set, inc }`. `$inc` plus `$setOnInsert` (or `$set`) on the
//! same path, or any overlap among the three lists (a path equal to or below another), is refused
//! up front as [`UpdateRefused::Overlap`] ([`UpsertEffects::check`]); there is no splitting logic.
//! Several `$inc` on DISTINCT paths are allowed.
//!
//! # S2a work item (not implemented here)
//!
//! `Writer::upsert_soi*` takes exactly one `(inc_path, n)`. S2a generalises the two-write upsert to
//! a list of `(path, n)` at ONE stamp for the increments, validated as a whole together with the
//! creation before anything is written or the clock moves. One write = one stamp, as
//! `write_update` already does, so no model rerun; extend the Rust oracle and property generator
//! to multi-`$inc` upserts.
//!
//! # No-op test
//!
//! By representation, as for every write ([`Value::same`]): doubles compare by bit pattern
//! (`-0.0` is not `0.0`; a NaN over the same NaN is a no-op), a `Decimal128` by its 16 bytes, an
//! `Int` never equals a `Double`, a type change is a write, `Null` is distinct from unset.
//!
//! # Questions: resolved by ruling, and still open
//!
//! * RESOLVED: `$rename` of a counter is decided at the writer, see above.
//! * RESOLVED: `$mul`/`$min`/`$max` over a counter is `Set(computed)`, see above.
//! * RESOLVED: upserts refuse overlap, several `$inc` on distinct paths; generalisation is S2a's.
//! * RESOLVED: pipeline no-op is by representation (`Value::same`).
//! * RESOLVED: `$currentDate` is one product-clock `now` per update.
//! * OPEN: `$rename` of a sub-document that holds counters: only the source path's own cell is
//!   checked by [`Writer::sees_counter`].

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

    /// Effects from a ready list, kept in order.
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
    /// product read at `from`. The source is recorded ([`Effects::renames`]) so that `apply`
    /// can refuse a counter at `from` at the writer ([`UpdateRefused::RenameOfCounter`]).
    pub fn rename(&mut self, from: Path, to: Path, value: Value) -> &mut Self {
        self.renames.push(from.clone());
        self.unset(from).set(to, value)
    }

    /// The sources of the recorded renames, in order.
    pub fn renames(&self) -> &[Path] {
        &self.renames
    }

    /// The first recorded rename source that the writer sees as a counter, as the refusal.
    fn rename_refusal(&self, writer: &Writer) -> Option<UpdateRefused> {
        self.renames
            .iter()
            .find(|p| writer.sees_counter(p))
            .map(|p| UpdateRefused::RenameOfCounter(p.clone()))
    }

    /// No effect at all: nothing to write, no stamp.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// The list in order, exactly what [`Writer::write_update`] takes.
    pub fn as_ops(&self) -> &[UpdateOp] {
        &self.ops
    }

    pub fn into_ops(self) -> Vec<UpdateOp> {
        self.ops
    }

    /// Write the effects as one update with the writer's own clock. Empty effects write nothing
    /// and return [`Outcome::Unchanged`] without touching the writer.
    pub fn apply(&self, writer: &mut Writer, skew: u64) -> Outcome {
        if self.is_empty() {
            return Outcome::Unchanged;
        }
        if let Some(r) = self.rename_refusal(writer) {
            return Outcome::Refused(r);
        }
        writer.write_update(&self.ops, skew).into()
    }

    /// [`Effects::apply`] with the stamp taken from `stamp`; a rejected stamp is the `Err`.
    pub fn apply_stamped(
        &self,
        writer: &mut Writer,
        stamp: &mut dyn FnMut() -> MStamp,
    ) -> Result<Outcome, UpdateError> {
        if self.is_empty() {
            return Ok(Outcome::Unchanged);
        }
        if let Some(r) = self.rename_refusal(writer) {
            return Ok(Outcome::Refused(r));
        }
        match writer.write_update_stamped(&self.ops, stamp) {
            Err(UpdateError::Refused(r)) => Ok(Outcome::Refused(r)),
            Err(e) => Err(e),
            Ok(w) => Ok(Outcome::from(Ok(w))),
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
    /// Nothing changed: no stamp, no entry (`modified: false`).
    Unchanged,
    /// Refused, nothing changed: HTTP 400 with the refusal.
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

/// The effects of an upsert (`update` with `upsert: true`) when no document matches.
/// `set_on_insert` are the `$setOnInsert` fields (written tagged by the creation), `set` the
/// `$set` fields and `inc` the `$inc` operands (an `Int32`, `Int64`, `Decimal128` or `Double`, as
/// [`UpdateOp::Inc`]). The three lists must be pairwise disjoint, within and across: see
/// [`UpsertEffects::check`]. On an existing document `set_on_insert` is ignored and `set`/`inc`
/// are an ordinary update. Several `$inc` on distinct paths are allowed; the storage side (S2a)
/// must generalise `Writer::upsert_soi*` to them (module docs, "S2a work item").
#[derive(Clone, Debug, Default)]
pub struct UpsertEffects {
    pub set_on_insert: Vec<(Path, Value)>,
    pub set: Vec<(Path, Value)>,
    pub inc: Vec<(Path, Value)>,
}

impl UpsertEffects {
    /// Refuse up front any two paths, in one list or across the three, where one is equal to or
    /// below the other: [`UpdateRefused::Overlap`] (the first pair found, in sorted order).
    pub fn check(&self) -> Result<(), UpdateRefused> {
        let mut all: Vec<&Path> =
            self.set_on_insert.iter().chain(&self.set).chain(&self.inc).map(|(p, _)| p).collect();
        // sorted, a path is followed by its extensions, so an overlap is between neighbours
        all.sort();
        for w in all.windows(2) {
            if w[0] == w[1] || w[1].is_below(w[0]) {
                return Err(UpdateRefused::Overlap(w[0].clone(), w[1].clone()));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::NodeId;
    use crate::merge_state::{Epoch, JoinOptions};

    fn p(x: &str) -> Path {
        Path::flat(x).unwrap()
    }
    fn i(n: i64) -> Value {
        Value::Int64(n)
    }
    fn writer() -> Writer {
        let id = crate::merge_state::WriterId {
            node: NodeId::from_bytes([7; 16]),
            epoch: Epoch::from_u64(0),
        };
        let mut w = Writer::new(id, JoinOptions::default());
        w.insert(&[(p("x"), i(1)), (p("n"), i(10))], 0).unwrap();
        w
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
        assert!(matches!(a.as_ops()[0], UpdateOp::Set(..)));
        assert!(matches!(a.as_ops()[1], UpdateOp::Inc(..)));
        assert!(matches!(a.as_ops()[2], UpdateOp::Unset(..)));
        assert_eq!(format!("{:?}", a.as_ops()), format!("{:?}", b.as_ops()));
    }

    #[test]
    fn rename_is_unset_then_set_on_disjoint_paths() {
        let mut e = Effects::new();
        e.rename(p("a"), p("b"), i(3));
        match e.as_ops() {
            [UpdateOp::Unset(from), UpdateOp::Set(to, Value::Int64(3))] => {
                assert_eq!((from, to), (&p("a"), &p("b")));
            }
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn a_dotted_name_is_one_segment() {
        let path = Path::new(["a.b"]).unwrap();
        assert_eq!(path.segments().len(), 1);
        let mut e = Effects::new();
        e.set(path.clone(), i(1));
        assert!(matches!(&e.as_ops()[0], UpdateOp::Set(q, _) if *q == path));
    }

    #[test]
    fn empty_effects_write_nothing() {
        let mut w = writer();
        let before = w.state.clone();
        let o = Effects::new().apply(&mut w, 0);
        assert_eq!(o, Outcome::Unchanged);
        assert!(!o.modified());
        assert_eq!(w.state, before);
    }

    #[test]
    fn outcomes_map_modified_unchanged_and_refused() {
        let mut w = writer();
        let mut e = Effects::new();
        e.inc(p("n"), i(5)).set(p("y"), i(2));
        let o = e.apply(&mut w, 0);
        assert!(o.modified());
        // the same value again is a no-op, not a refusal
        let again = Effects::from_ops(vec![UpdateOp::Set(p("y"), i(2))]).apply(&mut w, 0);
        assert_eq!(again, Outcome::Unchanged);
        // overlapping paths refuse the whole update
        let mut bad = Effects::new();
        bad.set(p("y"), i(3)).set(Path::new(["y", "z"]).unwrap(), i(1));
        let o = bad.apply(&mut w, 0);
        assert!(matches!(o.refusal(), Some(UpdateRefused::Overlap(..))));
        // a double against an integer counter is a refusal
        let o = Effects::from_ops(vec![UpdateOp::Inc(p("n"), Value::Double(1.5))]).apply(&mut w, 0);
        assert!(matches!(o.refusal(), Some(UpdateRefused::IncDoubleOnCounter(_))));
    }

    #[test]
    fn rename_records_its_source_and_a_counter_source_is_refused_before_any_write() {
        let mut w = writer(); // n is an Int64 counter, x too
        let before = w.state.clone();
        let mut e = Effects::new();
        e.rename(p("n"), p("m"), i(10));
        assert_eq!(e.renames(), &[p("n")]);
        assert_eq!(e.len(), 2);
        let o = e.apply(&mut w, 0);
        assert_eq!(o.refusal(), Some(&UpdateRefused::RenameOfCounter(p("n"))));
        assert_eq!(w.state, before, "no write, no clock move");
        // a string or a double, and a missing source, are renamed
        w.write_set(&p("s"), Value::String("v".into()), 0).unwrap();
        w.write_set(&p("d"), Value::Double(1.5), 0).unwrap();
        let mut r = Effects::new();
        r.rename(p("s"), p("s2"), Value::String("v".into()));
        assert!(r.apply(&mut w, 0).modified());
        let mut r = Effects::new();
        r.rename(p("d"), p("d2"), Value::Double(1.5));
        assert!(r.apply(&mut w, 0).modified());
        let mut r = Effects::new();
        r.rename(p("gone"), p("g2"), i(1));
        assert!(!matches!(r.apply(&mut w, 0), Outcome::Refused(UpdateRefused::RenameOfCounter(_))));
    }

    #[test]
    fn upsert_effects_shape_and_overlap_refusal() {
        let u = UpsertEffects {
            set_on_insert: vec![(p("a"), i(1))],
            set: vec![(p("b"), i(2))],
            inc: vec![(p("c"), i(1)), (p("d"), Value::Double(0.5))],
        };
        assert!(u.check().is_ok(), "several $inc on distinct paths are allowed");
        let same = UpsertEffects { inc: vec![(p("a"), i(1))], ..u.clone() };
        assert!(matches!(same.check(), Err(UpdateRefused::Overlap(..))));
        let below =
            UpsertEffects { set: vec![(Path::new(["c", "x"]).unwrap(), i(1))], ..u.clone() };
        assert!(matches!(below.check(), Err(UpdateRefused::Overlap(..))));
        let twice = UpsertEffects { inc: vec![(p("c"), i(1)), (p("c"), i(2))], ..u };
        assert!(matches!(twice.check(), Err(UpdateRefused::Overlap(..))));
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
}
