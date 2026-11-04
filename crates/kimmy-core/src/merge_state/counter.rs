//! Counter amounts: the typed numbers a counter slot, an observed total, a generation's seen
//! totals and a lost slot total hold, and the arithmetic on them.
//!
//! A counter is a numeric base (or none) plus one running total per writer and generation (a
//! slot); the visible value is the base plus, for each slot, what it gained since the base
//! observed it ([`MergeState::value`](super::MergeState::value)). A [`Num`] is an amount with
//! its BSON type, so that `Int32`, `Int64` and `Decimal128` counters merge.
//!
//! DOUBLES ARE NOT COUNTERS (Q6). A `Double` is a plain register: it starts a generation, and
//! `$inc` of a double base writes the computed value as a register, so concurrent increments of
//! a double do not sum (the newer write wins). A double operand against a counter is refused.
//! There is no `Double` amount.
//!
//! # Types
//!
//! The three types are ordered `Int32 < Int64 < Decimal128`, production's rule of `$inc`
//! (kimmy-query `arithmetic`) read as a join. Two `Int32`s stay `Int32` while the total fits;
//! any `Int64` makes it `Int64`; a `Decimal128` makes it `Decimal128`.
//!
//! * A LOCAL `$inc` follows production ([`inc_result`]): the result type comes from the writer's
//!   visible value and the operand, an `Int64` past the integer bound is refused, a `Decimal128`
//!   result that overflows from finite inputs is refused, and so is a `Double` operand against
//!   an `Int32`, `Int64` or `Decimal128` counter (the 400 of Q9; this one predicate decides it).
//! * A MERGED cell has no one to refuse. Its type is the JOIN of the types of its parts: the
//!   base and every slot that contributes. Its value is promoted to `Int64` when the total does
//!   not fit an `i32`. It never narrows through a write: a writer whose increment made the
//!   visible value `Int64` marks its slot `Int64`, so a total that later dips back under the
//!   `i32` range stays `Int64` in that writer's slot. THE ONE DEVIATION: a total that crossed
//!   the range only in a merge, and dips back by a merge, narrows again, because no write saw it.
//! * Integer totals are exact (`i128`, bounded by [`MAX_TOTAL`](super::MAX_TOTAL)); a
//!   `Decimal128` total is a running sum rounded once per addition to 34 digits (ADR-252, the
//!   production `add`).
//! * A merge that crosses a bound SATURATES (Q11): the visible `Int64` stops at the integer
//!   bound, a `Decimal128` at the largest finite value of its sign (never an infinity). Nothing
//!   is stored: [`MergeState::excess`](super::MergeState::excess) derives what was cut off, as
//!   `hidden` is derived. An integer keeps its exact `i128` sum underneath, so a later opposite
//!   increment brings the value back; a decimal saturates at each addition (the sum is
//!   rounded at each step anyway), so a later opposite term is added to the saturated
//!   value and the excess cut off earlier is not recovered.
//!
//! # The canonical order
//!
//! A decimal sum is not associative, so the read side adds the parts of a cell in ONE order,
//! the design's: the base first, then each contributing slot in ascending order of its writer
//! `(node, epoch)`, ties (one writer with slots in several lives or generations) broken by
//! `(life, generation)`. The order is a function of the state alone, so replicas that hold equal
//! states read byte-identical values whatever order the states arrived in. What a slot adds is
//! `total - observed` in the slot's own type, then converted up to the cell's type (an integer
//! to a `Decimal128` is exact up to 34 digits and rounded once above).
//!
//! # Step 2 checklist
//!
//! * `upsert_inc` takes an `i64` today. Extend it to `Decimal128` operands as production allows,
//!   then add the multi-life Decimal case that discriminates the canonical order (writer
//!   `(node, epoch)`, then life and generation) from the key order (life, writer, generation):
//!   `decimal_order_scenario` has one life, so it cannot tell the two apart.

use crate::decimal::arith;
use crate::decimal::{MAX_COEFFICIENT, MAX_EXPONENT, Raw, decode_raw_bits, pack_raw};

use super::types::Value;

/// The type of a counter amount, in the order of the join.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(super) enum Kind {
    Int32,
    Int64,
    Decimal128,
}

/// A counter amount with its type.
#[derive(Clone, Copy, Debug)]
pub enum Num {
    /// An integer whose type is `Int32` while it fits an `i32`.
    Int32(i128),
    Int64(i128),
    /// The 16 bytes of a BSON Decimal128.
    Decimal128([u8; 16]),
}

impl PartialEq for Num {
    /// Byte for byte, like the encoding: `Int32(1)` is not `Int64(1)`.
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Num::Int32(a), Num::Int32(b)) | (Num::Int64(a), Num::Int64(b)) => a == b,
            (Num::Decimal128(a), Num::Decimal128(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Num {}

/// An integer amount of the `Int64` type: what every counter total was before the other types.
impl From<i128> for Num {
    fn from(v: i128) -> Self {
        Num::Int64(v)
    }
}

fn dec_raw(b: &[u8; 16]) -> Raw {
    decode_raw_bits(u128::from_le_bytes(*b))
}

fn dec_bytes(r: Raw) -> [u8; 16] {
    pack_raw(r).bytes()
}

/// An integer as a decimal: exact up to 34 digits, rounded once above.
fn int_raw(v: i128) -> Raw {
    let mut s = arith::ExactSum::new();
    s.add_integers(v, 1);
    s.finish_sum()
}

/// The largest finite `Decimal128` of the sign.
fn max_finite(negative: bool) -> Raw {
    Raw::Finite { negative, coefficient: MAX_COEFFICIENT, exponent: MAX_EXPONENT }
}

impl Num {
    /// The type, with the promotion: an `Int32` amount outside the `i32` range is an `Int64`.
    pub(super) fn kind(&self) -> Kind {
        match self {
            Num::Int32(v) if i32::try_from(*v).is_ok() => Kind::Int32,
            Num::Int32(_) | Num::Int64(_) => Kind::Int64,
            Num::Decimal128(_) => Kind::Decimal128,
        }
    }

    /// The type LABEL, as written (and as the compact form spells it): unlike [`Num::kind`], an
    /// `Int32` amount outside the `i32` range stays an `Int32`. A slot's label only widens
    /// (`Int32 < Int64 < Decimal128`) across the writes of its writer, see `check_decoded`.
    pub(super) fn label(&self) -> Kind {
        match self {
            Num::Int32(_) => Kind::Int32,
            Num::Int64(_) => Kind::Int64,
            Num::Decimal128(_) => Kind::Decimal128,
        }
    }

    /// The integer, for an integer amount.
    pub fn int(&self) -> Option<i128> {
        match self {
            Num::Int32(v) | Num::Int64(v) => Some(*v),
            Num::Decimal128(_) => None,
        }
    }

    /// Does the amount equal zero (either sign, any decimal quantum)?
    pub(super) fn is_zero(&self) -> bool {
        match self {
            Num::Int32(v) | Num::Int64(v) => *v == 0,
            Num::Decimal128(b) => dec_raw(b).is_zero(),
        }
    }

    /// The amount of a counter value, `None` for any other value (a `Double` included).
    pub fn from_value(v: &Value) -> Option<Num> {
        Some(match v {
            Value::Int32(i) => Num::Int32(i128::from(*i)),
            Value::Int64(i) => Num::Int64(i128::from(*i)),
            Value::Decimal128(b) => Num::Decimal128(*b),
            _ => return None,
        })
    }

    /// How far an integer amount is past `[-bound - 1, bound]` (signed, 0 inside it).
    pub(super) fn int_excess(&self, bound: i64) -> i128 {
        let b = i128::from(bound);
        self.int().map_or(0, |v| v - v.clamp(-b - 1, b))
    }

    /// The value of a cell whose amount this is. An integer saturates to `[-bound - 1, bound]`
    /// (Q11); an `Int32` that does not fit an `i32` is an `Int64`.
    pub(super) fn into_value(self, bound: i64, saturate_i32: bool) -> Value {
        let b = i128::from(bound);
        match self {
            Num::Int32(v) | Num::Int64(v) => {
                let t = v.clamp(-b - 1, b);
                match (self.kind(), i32::try_from(t)) {
                    (Kind::Int32, Ok(i)) => Value::Int32(i),
                    // the mutant: an Int32 that overflows saturates instead of being promoted
                    _ if saturate_i32 && matches!(self, Num::Int32(_)) => {
                        Value::Int32(t.clamp(i128::from(i32::MIN), i128::from(i32::MAX)) as i32)
                    }
                    _ => Value::Int64(t as i64),
                }
            }
            Num::Decimal128(b) => Value::Decimal128(b),
        }
    }

    fn as_raw(&self) -> Raw {
        match self {
            Num::Int32(v) | Num::Int64(v) => int_raw(*v),
            Num::Decimal128(b) => dec_raw(b),
        }
    }

    /// A writer's FIRST amount at a path in the product's stored FORM (ADR-252): a canonical encoding of the
    /// operand, the word `pack_raw` writes for it (a NaN of any payload is the plain quiet NaN, a non-canonical
    /// coefficient or the large-coefficient form a canonical zero, an infinity stays one with its payload dropped),
    /// with the cohort member kept (`-0` stays `-0`, `0E2015` stays `0E2015`, `1E2` stays `1E+2`). It is NOT a
    /// recomputation `0 + operand`: the read adds each slot to the register base, and a sum with a zero would move the
    /// cohort exponent to the smaller one (`1E2` base plus `1E2` would read `200`, the product stores `2E+2`). An
    /// integer is unchanged.
    pub(super) fn first_amount(self) -> Num {
        match self {
            Num::Decimal128(b) => Num::Decimal128(dec_bytes(dec_raw(&b))),
            n => n,
        }
    }

    /// Is this a total the Writer can produce, for a `Decimal128`: the bytes `pack_raw` writes
    /// (`dec_bytes(dec_raw(bytes)) == bytes`)? A NaN other than the plain quiet NaN, an infinity with a
    /// payload, a coefficient above 10^34 - 1 and the large-coefficient form are not. Every other total is.
    pub(super) fn is_canonical(&self) -> bool {
        match self {
            Num::Decimal128(b) => dec_bytes(dec_raw(b)) == *b,
            _ => true,
        }
    }

    /// Relabel an integer amount as an `Int64` (it never narrows again); a decimal as it is.
    pub(super) fn widened(self) -> Num {
        match self {
            Num::Int32(v) => Num::Int64(v),
            n => n,
        }
    }

    /// `self + other` in the join of the two types.
    pub(super) fn add(&self, other: &Num) -> Num {
        self.combine(other, false, None)
    }

    /// `self - other` in the join of the two types, without saturation.
    pub(super) fn sub(&self, other: &Num) -> Num {
        self.combine(other, true, None)
    }

    /// `self + other` for the read side: a decimal that overflows from finite inputs saturates
    /// at the largest finite value of its sign, and `saturated` holds that sign (`true`: negative).
    pub(super) fn add_sat(&self, other: &Num, saturated: &mut Option<bool>) -> Num {
        self.combine(other, false, Some(saturated))
    }

    /// `self - other`, saturating as [`Num::add_sat`].
    pub(super) fn sub_sat(&self, other: &Num, saturated: &mut Option<bool>) -> Num {
        self.combine(other, true, Some(saturated))
    }

    fn combine(&self, other: &Num, minus: bool, saturated: Option<&mut Option<bool>>) -> Num {
        let (ka, kb) = (self.kind(), other.kind());
        // the label of an integer result: Int32 only when both parts are
        let label = |v: i128| {
            if matches!(self, Num::Int32(_)) && matches!(other, Num::Int32(_)) {
                Num::Int32(v)
            } else {
                Num::Int64(v)
            }
        };
        match ka.max(kb) {
            Kind::Int32 | Kind::Int64 => {
                let (a, b) = (self.int().unwrap_or(0), other.int().unwrap_or(0));
                label(if minus { a.saturating_sub(b) } else { a.saturating_add(b) })
            }
            Kind::Decimal128 => {
                let (a, b) = (self.as_raw(), other.as_raw());
                let mut r = if minus { arith::sub(&a, &b) } else { arith::add(&a, &b) };
                let inf = |x: &Raw| matches!(x, Raw::Inf { .. });
                if let (Some(flag), Raw::Inf { negative }) = (saturated, r)
                    && !inf(&a)
                    && !inf(&b)
                {
                    *flag = Some(negative);
                    r = max_finite(negative);
                }
                Num::Decimal128(dec_bytes(r))
            }
        }
    }

    /// The zero of the type `k` (the start of a sum).
    pub(super) fn zero(k: Kind) -> Num {
        match k {
            Kind::Int32 => Num::Int32(0),
            Kind::Int64 => Num::Int64(0),
            Kind::Decimal128 => Num::Decimal128(dec_bytes(Raw::Finite {
                negative: false,
                coefficient: 0,
                exponent: 0,
            })),
        }
    }

    /// `self` converted up to the type `k` (a type below is unchanged).
    pub(super) fn to_kind(self, k: Kind) -> Num {
        match (k, self) {
            (Kind::Int64, Num::Int32(v)) => Num::Int64(v),
            (Kind::Decimal128, n) if !matches!(n, Num::Decimal128(_)) => {
                Num::Decimal128(dec_bytes(n.as_raw()))
            }
            (_, n) => n,
        }
    }

    /// Is this amount, a decimal result, an infinity that finite inputs cannot have produced?
    fn decimal_overflowed(result: &Num, a: &Num, b: &Num) -> bool {
        let inf = |n: &Num| matches!(n.as_raw(), Raw::Inf { .. });
        matches!(result, Num::Decimal128(_)) && inf(result) && !inf(a) && !inf(b)
    }
}

/// What a local `$inc` does.
#[derive(Clone, PartialEq, Debug)]
pub(super) enum Inc {
    /// A counter: the writer's slot grows by the operand; the document would hold this amount.
    Slot(Num),
    /// A register write of this value (a double base, or a double operand on an absent field).
    Register(Value),
}

/// Production's `$inc` (kimmy-query `arithmetic`) on the writer's visible value `cur` (`None`:
/// the field is absent, which counts as the `Int32` zero) and the operand `op`, or `None` for a
/// refusal (the 400 of a client): an integer result past the integer `bound` (Q11), a decimal
/// that overflows from finite inputs, a non-numeric stored value or operand, and a `Double`
/// operand against an `Int32`, `Int64` or `Decimal128` counter (Q9). Doubles are not counters
/// (Q6): `$inc` of a double base, or of an absent field by a double, is a register write of the
/// computed value.
pub(super) fn inc_result(cur: Option<&Value>, op: &Value, bound: i64) -> Option<Inc> {
    if let Value::Double(x) = op {
        return match cur {
            None => Some(Inc::Register(Value::Double(0.0 + x))),
            Some(Value::Double(d)) => Some(Inc::Register(Value::Double(d + x))),
            // THE predicate: a double operand on a counter is refused
            Some(_) => None,
        };
    }
    let o = Num::from_value(op)?;
    if let Some(Value::Double(d)) = cur {
        // a double base: the result is a double, or a decimal when the operand is one
        return Some(Inc::Register(match o {
            Num::Int32(v) | Num::Int64(v) => Value::Double(d + v as f64),
            Num::Decimal128(_) => {
                let stored = Num::Decimal128(dec_bytes(arith::from_binary64(d.to_bits()).0));
                Value::Decimal128(match sum_checked(&stored, &o)? {
                    Num::Decimal128(b) => b,
                    _ => return None,
                })
            }
        }));
    }
    let a = match cur {
        None => Num::Int32(0),
        Some(v) => Num::from_value(v)?,
    };
    let (ka, ko) = (a.kind(), o.kind());
    if ka.max(ko) <= Kind::Int64 {
        let t = a.int()?.checked_add(o.int()?)?;
        let b = i128::from(bound);
        if !(-b - 1..=b).contains(&t) {
            return None;
        }
        return Some(Inc::Slot(
            if ka == Kind::Int32 && ko == Kind::Int32 && i32::try_from(t).is_ok() {
                Num::Int32(t)
            } else {
                Num::Int64(t)
            },
        ));
    }
    sum_checked(&a, &o).map(Inc::Slot)
}

/// `a + b` for a writer: `None` when a decimal sum overflows from finite inputs.
pub(super) fn sum_checked(a: &Num, b: &Num) -> Option<Num> {
    let r = a.add(b);
    (!Num::decimal_overflowed(&r, a, b)).then_some(r)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Num {
        let d: bson::Decimal128 = s.parse().unwrap();
        Num::Decimal128(d.bytes())
    }

    fn dv(s: &str) -> Value {
        Value::Decimal128(s.parse::<bson::Decimal128>().unwrap().bytes())
    }

    fn text(n: &Num) -> String {
        match n {
            Num::Decimal128(b) => bson::Decimal128::from_bytes(*b).to_string(),
            n => format!("{n:?}"),
        }
    }

    #[test]
    fn the_type_is_the_join_and_an_int32_that_outgrows_i32_is_an_int64() {
        let (i32a, i64a, x) = (Num::Int32(5), Num::Int64(5), dec("0.5"));
        assert_eq!(i32a.add(&i32a), Num::Int32(10));
        assert_eq!(i32a.add(&i64a), Num::Int64(10));
        assert_eq!(i64a.add(&x), dec("5.5"));
        assert_eq!(i32a.add(&x), dec("5.5"));
        let big = Num::Int32(i128::from(i32::MAX)).add(&Num::Int32(1));
        assert_eq!(big.kind(), Kind::Int64);
        assert_eq!(big.into_value(i64::MAX, false), Value::Int64(i64::from(i32::MAX) + 1));
        assert_eq!(Num::Int32(7).into_value(i64::MAX, false), Value::Int32(7));
    }

    #[test]
    fn production_rule_for_a_local_inc() {
        let b = i64::MAX;
        let slot = |cur: Option<Value>, op: Value| match inc_result(cur.as_ref(), &op, b) {
            Some(Inc::Slot(n)) => Some(n),
            _ => None,
        };
        let (i32v, i64v) = (Value::Int32, Value::Int64);
        assert_eq!(slot(None, i32v(7)), Some(Num::Int32(7)));
        assert_eq!(slot(None, i64v(7)), Some(Num::Int64(7)));
        assert_eq!(slot(Some(i32v(5)), i32v(1)), Some(Num::Int32(6)));
        assert_eq!(slot(Some(i32v(i32::MAX)), i32v(1)), Some(Num::Int64(i128::from(i32::MAX) + 1)));
        assert_eq!(slot(Some(i32v(5)), i64v(1)), Some(Num::Int64(6)));
        assert_eq!(slot(Some(i64v(i64::MAX)), i32v(1)), None, "past the bound");
        assert_eq!(text(&slot(Some(i32v(1)), dv("2.30")).unwrap()), "3.30");
    }

    /// Doubles are not counters (Q6/Q9): a double operand against an Int32, Int64 or Decimal128
    /// counter is refused, by the one predicate; a double base, or an absent field, takes a
    /// register write of the computed value.
    #[test]
    fn a_double_operand_on_a_counter_is_refused_and_a_double_base_is_a_register() {
        let b = i64::MAX;
        let d = Value::Double(0.5);
        for cur in [Value::Int32(1), Value::Int64(1), dv("1.5")] {
            assert_eq!(inc_result(Some(&cur), &d, b), None, "{cur:?}");
        }
        assert_eq!(
            inc_result(Some(&Value::Double(2.5)), &Value::Int64(1), b),
            Some(Inc::Register(Value::Double(3.5)))
        );
        assert_eq!(
            inc_result(Some(&Value::Double(2.5)), &d, b),
            Some(Inc::Register(Value::Double(3.0)))
        );
        assert_eq!(inc_result(None, &d, b), Some(Inc::Register(Value::Double(0.5))));
        // a decimal operand on a double base is a decimal register
        match inc_result(Some(&Value::Double(0.5)), &dv("0.25"), b) {
            Some(Inc::Register(Value::Decimal128(x))) => {
                assert_eq!(bson::Decimal128::from_bytes(x).to_string(), "0.75");
            }
            r => panic!("{r:?}"),
        }
    }

    #[test]
    fn a_decimal_sum_that_overflows_saturates_on_the_read_side_and_is_refused_for_a_writer() {
        let max = dec("9.999999999999999999999999999999999E+6144");
        let mut sat = None;
        assert_eq!(max.add_sat(&max, &mut sat), max);
        assert_eq!(sat, Some(false));
        let neg = dec("-9.999999999999999999999999999999999E+6144");
        let mut sat = None;
        assert_eq!(neg.add_sat(&neg, &mut sat), neg);
        assert_eq!(sat, Some(true));
        let mut sat = None;
        assert_eq!(dec("1").add_sat(&dec("2"), &mut sat), dec("3"));
        assert_eq!(sat, None);
        assert!(sum_checked(&max, &max).is_none());
    }
}
