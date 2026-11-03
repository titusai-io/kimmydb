//! Counter amounts: the typed numbers a counter slot, an observed total, a generation's seen
//! totals and a lost slot total hold, and the arithmetic on them.
//!
//! A counter is a numeric base (or none) plus one running total per writer and generation (a
//! slot); the visible value is the base plus, for each slot, what it gained since the base
//! observed it ([`MergeState::value`](super::MergeState::value)). Until now every amount was an
//! `Int64` in an `i128`. A [`Num`] is an amount with its BSON type, so that `Int32`, `Double`
//! and `Decimal128` counters merge too.
//!
//! # Types
//!
//! The four types are ordered `Int32 < Int64 < Double` and `Int64 < Decimal128`, and a
//! `Decimal128` is above a `Double`: the order is the production rule of `$inc` (kimmy-query
//! `arithmetic`), read as a join. Two `Int32`s stay `Int32` while the total fits; any `Int64`
//! makes it `Int64`; a `Decimal128` makes it `Decimal128`; otherwise a `Double` is involved
//! and the result is a `Double`.
//!
//! * A LOCAL `$inc` follows production exactly ([`inc_result`]): the result type comes from the
//!   writer's visible value and the operand, an `Int64` past the integer bound is refused, a
//!   `Decimal128` result that overflows from finite inputs is refused, and so is an operand
//!   double that is not exactly a decimal when the stored value is one.
//! * A MERGED cell has no one to refuse. Its type is the JOIN of the types of its parts: the
//!   base and every slot that contributes. Its value is promoted to `Int64` when the total
//!   does not fit an `i32`. It never narrows again through a write: a writer whose increment
//!   made the visible value `Int64` marks its slot `Int64`, so a total that later dips back
//!   under the `i32` range stays `Int64` in that writer's slot. THE ONE DEVIATION: a total that
//!   crossed the range only in a merge, and dips back by a merge, narrows again, because no
//!   write saw it.
//! * Integer totals are exact (`i128`, bounded by [`MAX_TOTAL`](super::MAX_TOTAL)); a `Double`
//!   total is a running IEEE sum, a `Decimal128` total a running sum rounded once per addition
//!   to 34 digits (ADR-252, the production `add`).
//!
//! # The canonical order
//!
//! Neither a `Double` sum nor a `Decimal128` sum is associative, so the read side adds the
//! parts of a cell in ONE order: the base first, then each contributing slot in ascending
//! order of `(the slot's stamp, its key)`, where the stamp is `(hlc, node, epoch)` and the key
//! is `(life, writer, generation)`. The order is a function of the state alone, so replicas
//! that hold equal states read byte-identical values whatever order the states arrived in.
//! What a slot adds is `total - observed` in the slot's own type, then converted up to the
//! cell's type (an `Int` to a `Double` rounds to nearest; an `Int` to a `Decimal128` is exact up
//! to 34 digits and rounded once above; a `Double` to a `Decimal128` is ADR-252's R1, its exact
//! value rounded once).

use crate::decimal::arith;
use crate::decimal::{Raw, decode_raw_bits, pack_raw};

use super::types::Value;

/// The type of a counter amount, in the order of the join.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(super) enum Kind {
    Int32,
    Int64,
    Double,
    Decimal128,
}

/// A counter amount with its type.
#[derive(Clone, Copy, Debug)]
pub enum Num {
    /// An integer whose type is `Int32` while it fits an `i32`.
    Int32(i128),
    Int64(i128),
    Double(f64),
    /// The 16 bytes of a BSON Decimal128.
    Decimal128([u8; 16]),
}

impl PartialEq for Num {
    /// Bit for bit, like the encoding: `Int32(1)` is not `Int64(1)`, `0.0` is not `-0.0`.
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Num::Int32(a), Num::Int32(b)) | (Num::Int64(a), Num::Int64(b)) => a == b,
            (Num::Double(a), Num::Double(b)) => a.to_bits() == b.to_bits(),
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

impl Num {
    /// The type, with the promotion: an `Int32` amount outside the `i32` range is an `Int64`.
    pub(super) fn kind(&self) -> Kind {
        match self {
            Num::Int32(v) if i32::try_from(*v).is_ok() => Kind::Int32,
            Num::Int32(_) | Num::Int64(_) => Kind::Int64,
            Num::Double(_) => Kind::Double,
            Num::Decimal128(_) => Kind::Decimal128,
        }
    }

    /// The integer, for an integer amount.
    pub fn int(&self) -> Option<i128> {
        match self {
            Num::Int32(v) | Num::Int64(v) => Some(*v),
            _ => None,
        }
    }

    /// Does the amount equal zero (either sign, any decimal quantum)?
    pub(super) fn is_zero(&self) -> bool {
        match self {
            Num::Int32(v) | Num::Int64(v) => *v == 0,
            Num::Double(d) => *d == 0.0,
            Num::Decimal128(b) => dec_raw(b).is_zero(),
        }
    }

    /// The amount of a counter value, `None` for any other value.
    pub fn from_value(v: &Value) -> Option<Num> {
        Some(match v {
            Value::Int32(i) => Num::Int32(i128::from(*i)),
            Value::Int64(i) => Num::Int64(i128::from(*i)),
            Value::Double(d) => Num::Double(*d),
            Value::Decimal128(b) => Num::Decimal128(*b),
            _ => return None,
        })
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
            Num::Double(d) => Value::Double(d),
            Num::Decimal128(b) => Value::Decimal128(b),
        }
    }

    fn as_f64(&self) -> f64 {
        match self {
            Num::Int32(v) | Num::Int64(v) => *v as f64,
            Num::Double(d) => *d,
            // never read: a decimal is not converted down
            Num::Decimal128(b) => f64::from_bits(arith::to_binary64(&dec_raw(b))),
        }
    }

    fn as_raw(&self) -> Raw {
        match self {
            Num::Int32(v) | Num::Int64(v) => int_raw(*v),
            Num::Double(d) => arith::from_binary64(d.to_bits()).0,
            Num::Decimal128(b) => dec_raw(b),
        }
    }

    /// Relabel an integer amount as an `Int64` (it never narrows again); other types as they are.
    pub(super) fn widened(self) -> Num {
        match self {
            Num::Int32(v) => Num::Int64(v),
            n => n,
        }
    }

    /// `self + other` in the join of the two types.
    pub(super) fn add(&self, other: &Num) -> Num {
        self.combine(other, false)
    }

    /// `self - other` in the join of the two types.
    pub(super) fn sub(&self, other: &Num) -> Num {
        self.combine(other, true)
    }

    fn combine(&self, other: &Num, minus: bool) -> Num {
        let (ka, kb) = (self.kind(), other.kind());
        // the label of an integer result: Int32 only when both parts are
        let label = |v: i128| {
            if self.is_int32_label() && other.is_int32_label() {
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
            Kind::Double => {
                let (a, b) = (self.as_f64(), other.as_f64());
                Num::Double(if minus { a - b } else { a + b })
            }
            Kind::Decimal128 => {
                let (a, b) = (self.as_raw(), other.as_raw());
                Num::Decimal128(dec_bytes(if minus {
                    arith::sub(&a, &b)
                } else {
                    arith::add(&a, &b)
                }))
            }
        }
    }

    fn is_int32_label(&self) -> bool {
        matches!(self, Num::Int32(_))
    }

    /// The zero of the type `k` (the start of a sum).
    pub(super) fn zero(k: Kind) -> Num {
        match k {
            Kind::Int32 => Num::Int32(0),
            Kind::Int64 => Num::Int64(0),
            Kind::Double => Num::Double(0.0),
            Kind::Decimal128 => Num::Decimal128(dec_bytes(Raw::Finite {
                negative: false,
                coefficient: 0,
                exponent: 0,
            })),
        }
    }

    /// `self` converted up to the type `k` (a type below is unchanged). A decimal that is
    /// reached by a `Double` is ADR-252's R1.
    pub(super) fn to_kind(self, k: Kind) -> Num {
        match (k, self) {
            (Kind::Int64, Num::Int32(v)) => Num::Int64(v),
            (Kind::Double, n @ (Num::Int32(_) | Num::Int64(_))) => Num::Double(n.as_f64()),
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

/// Production's `$inc` (kimmy-query `arithmetic`) on the writer's visible value `cur` (`None`:
/// the field is absent, which counts as the `Int32` zero) and the operand `op`: the amount the
/// document would hold, or `None` for a refusal (the 400 of a client): an integer result past
/// the integer `bound` (Q11), a decimal that overflows from finite inputs, an operand double
/// that is not exactly a decimal when the stored value is one.
pub(super) fn inc_result(cur: Option<Num>, op: &Num, bound: i64) -> Option<Num> {
    let a = cur.unwrap_or(Num::Int32(0));
    let (ka, ko) = (a.kind(), op.kind());
    if ka.max(ko) <= Kind::Int64 {
        let t = a.int()?.checked_add(op.int()?)?;
        let b = i128::from(bound);
        if !(-b - 1..=b).contains(&t) {
            return None;
        }
        return Some(if ka == Kind::Int32 && ko == Kind::Int32 && i32::try_from(t).is_ok() {
            Num::Int32(t)
        } else {
            Num::Int64(t)
        });
    }
    sum_checked(&a, op)
}

/// `a + b` for a writer: `None` when a decimal sum overflows from finite inputs, or when a
/// double that is not exactly a decimal meets a decimal (`a` is the stored side).
pub(super) fn sum_checked(a: &Num, b: &Num) -> Option<Num> {
    if let (Num::Decimal128(_), Num::Double(d)) = (a, b) {
        let (_, inexact) = arith::from_binary64(d.to_bits());
        if inexact {
            return None;
        }
    }
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

    fn text(n: &Num) -> String {
        match n {
            Num::Decimal128(b) => bson::Decimal128::from_bytes(*b).to_string(),
            n => format!("{n:?}"),
        }
    }

    #[test]
    fn the_type_is_the_join_and_an_int32_that_outgrows_i32_is_an_int64() {
        let (i32a, i64a, d, x) = (Num::Int32(5), Num::Int64(5), Num::Double(0.5), dec("0.5"));
        assert_eq!(i32a.add(&i32a), Num::Int32(10));
        assert_eq!(i32a.add(&i64a), Num::Int64(10));
        assert_eq!(i32a.add(&d), Num::Double(5.5));
        assert_eq!(i64a.add(&x), dec("5.5"));
        assert_eq!(d.add(&x), dec("1.0"));
        let big = Num::Int32(i128::from(i32::MAX)).add(&Num::Int32(1));
        assert_eq!(big.kind(), Kind::Int64);
        assert_eq!(big.into_value(i64::MAX, false), Value::Int64(i64::from(i32::MAX) + 1));
        assert_eq!(Num::Int32(7).into_value(i64::MAX, false), Value::Int32(7));
    }

    #[test]
    fn production_rule_for_a_local_inc() {
        let b = i64::MAX;
        let r = |cur: Option<Num>, op: Num| inc_result(cur, &op, b);
        assert_eq!(r(None, Num::Int32(7)), Some(Num::Int32(7)));
        assert_eq!(r(None, Num::Int64(7)), Some(Num::Int64(7)));
        assert_eq!(r(Some(Num::Int32(5)), Num::Int32(1)), Some(Num::Int32(6)));
        assert_eq!(
            r(Some(Num::Int32(i128::from(i32::MAX))), Num::Int32(1)),
            Some(Num::Int64(i128::from(i32::MAX) + 1))
        );
        assert_eq!(r(Some(Num::Int32(5)), Num::Int64(1)), Some(Num::Int64(6)));
        assert_eq!(r(Some(Num::Int64(5)), Num::Double(0.5)), Some(Num::Double(5.5)));
        assert_eq!(r(Some(Num::Int64(i128::from(b))), Num::Int32(1)), None, "past the bound");
        // an inexact double meeting a stored decimal is refused; 0.5 is exact
        assert_eq!(r(Some(dec("19.99")), Num::Double(0.01)), None);
        assert_eq!(text(&r(Some(dec("19.99")), Num::Double(0.5)).unwrap()), "20.49");
        assert_eq!(text(&r(Some(Num::Int32(1)), dec("2.30")).unwrap()), "3.30");
    }
}
