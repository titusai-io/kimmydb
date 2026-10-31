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

/// A flat cell value. `Null` is the model's `None`: an unset field. Numeric
/// cells (`Null`, `Int`) take part in counting; `Double` and `Str` are bases
/// that start a generation.
#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Int(i64),
    Double(f64),
    Str(String),
}

impl Value {
    /// The model's `numeric()`: `None` or an integer.
    pub fn is_numeric(&self) -> bool {
        matches!(self, Value::Null | Value::Int(_))
    }
}

// NOTE(model): Python's `==` makes `2 == 2.0`; here values of different types
// are never equal, and doubles compare by bit pattern (so the encoding and
// equality agree, and NaN equals itself). The only observable difference is a
// `$set` of `2` over a stored `2.0`, which is now a write instead of a no-op.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Double(a), Value::Double(b)) => a.to_bits() == b.to_bits(),
            (Value::Str(a), Value::Str(b)) => a == b,
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
