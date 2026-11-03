//! Identity, stamp and value types of the merge state.

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
pub struct WriterId {
    pub node: NodeId,
    pub epoch: Epoch,
}

/// The stamp of a write, ordered by `(hlc, node, epoch)`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct MStamp {
    pub hlc: Hlc,
    pub writer: WriterId,
}

/// A cell value: a BSON value other than an embedded document. `Null` is the model's `None`:
/// an unset field. Numeric cells (`Null`, `Int64`) take part in counting; every other variant
/// is a base that starts a generation. An array is ONE atomic value (a register); an embedded
/// document is not a `Value` (it is a map path, a later stage).
///
/// The variants keep the BSON types apart: `Int32(2)`, `Int64(2)`, `Double(2.0)` and
/// `Decimal128` of two are four different values. A `Decimal128` is its 16 bytes, as BSON
/// stores it.
#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Int32(i32),
    Int64(i64),
    Double(f64),
    /// The 16 bytes of a BSON Decimal128, compared and encoded as they are.
    Decimal128([u8; 16]),
    String(String),
    Bool(bool),
    /// A BSON date: milliseconds since the Unix epoch.
    Date(i64),
    ObjectId([u8; 12]),
    Binary {
        subtype: u8,
        bytes: Vec<u8>,
    },
    /// A BSON timestamp: seconds and an ordinal.
    Timestamp {
        time: u32,
        increment: u32,
    },
    Regex {
        pattern: String,
        options: String,
    },
    /// An array, atomic: the whole array is the register value.
    Array(Vec<Value>),
}

impl Value {
    /// The model's `numeric()`: `None` or an integer. Only an `Int64` counts: `Int32` and
    /// `Decimal128` counters are not modelled yet, so they are plain register values.
    pub fn is_numeric(&self) -> bool {
        matches!(self, Value::Null | Value::Int64(_))
    }
}

impl Value {
    /// The model's `same(a, b)` (Q4, ruled): the write diff compares value AND type,
    /// so `$set 2` over a stored `2.0` is a write, and so is an `Int32` over an `Int64`
    /// of the same number. A missing field (`None`) is the same as `Null`. Doubles
    /// compare as numbers (`==`: `0.0` is `-0.0`, NaN is not NaN), as Python does; an
    /// array compares element by element in the same way, type for type.
    pub fn same(current: Option<&Value>, new: &Value) -> bool {
        Self::same_value(current.unwrap_or(&Value::Null), new)
    }

    fn same_value(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Double(a), Value::Double(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(a, b)| Self::same_value(a, b))
            }
            (a, b) => a == b,
        }
    }
}

// Structural equality of states: values of different types are never equal and
// doubles compare by bit pattern, so that equality agrees with the encoding.
// (The write diff uses `Value::same`, which follows the model.) There is no order on
// values: a register is decided by its stamp, never by comparing two values.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Int32(a), Value::Int32(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Double(a), Value::Double(b)) => a.to_bits() == b.to_bits(),
            (Value::Decimal128(a), Value::Decimal128(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::ObjectId(a), Value::ObjectId(b)) => a == b,
            (Value::Binary { subtype: s, bytes: a }, Value::Binary { subtype: t, bytes: b }) => {
                s == t && a == b
            }
            (
                Value::Timestamp { time: t, increment: i },
                Value::Timestamp { time: u, increment: j },
            ) => t == u && i == j,
            (Value::Regex { pattern: p, options: o }, Value::Regex { pattern: q, options: r }) => {
                p == q && o == r
            }
            (Value::Array(a), Value::Array(b)) => a == b,
            _ => false,
        }
    }
}

/// The kind of a loss record.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum LossKind {
    Clash,
    DeleteWon,
    InsertLost,
    /// Declared for the retired-writer rule; not produced in P0.
    RetiredWriter,
}

/// The key of a loss record: kind, path (`"_"` for a whole document) and the
/// stamp of the lost write. Ordered so that it can key a `BTreeMap`.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct LossKey {
    pub kind: LossKind,
    pub path: String,
    pub stamp: MStamp,
}

/// What a loss record keeps of the lost write.
#[derive(Clone, PartialEq, Debug)]
pub enum LossValue {
    /// A lost register value.
    Value(Value),
    /// A lost counter slot total.
    Total(i128),
    /// The field order of a lost whole document (`insertLost` on `"_"`).
    Fields(Vec<String>),
}
