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

/// A refused path: no segments, or a segment with a NUL byte.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("merge state: invalid path: {0}")]
pub struct InvalidPath(pub &'static str);

/// A field path: the names from the document root to a field, as a list of segments, never as
/// one dotted string. A stored document may hold a field named `a.b`, `$x` or the empty string
/// (insert checks no key), which no dotted path expression can reach, so a segment is any
/// string without a NUL byte (a BSON field name cannot hold one); a path has at least one
/// segment. Paths order by their segments, each compared as UTF-8 bytes, so a path sorts before
/// every path it is a prefix of.
///
/// In this stage a path is an opaque key of the state: no meaning is given to a path being
/// below another. A flat field is the path of one segment.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Path(Vec<String>);

impl Path {
    /// A path of the given segments.
    pub fn new<I, S>(segments: I) -> Result<Self, InvalidPath>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let segments: Vec<String> = segments.into_iter().map(Into::into).collect();
        if segments.is_empty() {
            return Err(InvalidPath("no segments"));
        }
        if segments.iter().any(|s| s.contains('\0')) {
            return Err(InvalidPath("a NUL byte in a segment"));
        }
        Ok(Self(segments))
    }

    /// The path of one segment: a top-level field.
    pub fn flat(name: &str) -> Result<Self, InvalidPath> {
        Self::new([name])
    }

    pub fn segments(&self) -> &[String] {
        &self.0
    }
}

/// The segments joined by `.`, for messages and for tools that name a path by a dotted string.
/// Not a key: two different paths can print alike.
impl std::fmt::Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.join("."))
    }
}

/// A cell value: a BSON value, a map marker, or `Unset`. `Null` is the BSON
/// null, a value like any other: `$set x null` stores it and the field is present. `Unset` is
/// the model's `None`: the absence of a value, what a register holds after an `$unset` (the
/// field is not in the document). Numeric cells (`Unset`, `Int64`) take part in counting; every
/// other variant is a base that starts a generation, so `$inc` on a null is refused.
///
/// Three variants are not plain scalars:
///
/// * An `Array` is ONE atomic value (a register): it is never merged element by element, and two
///   concurrent arrays at one path are an `arrayClash`.
/// * A `Document` is an ordered list of named fields and is legal ONLY inside an `Array` (at any
///   depth; arrays in arrays are fine). There it is part of the atomic value: compared
///   structurally, type for type, field order included. Outside an array a document is not a
///   value of the state at all: it is a map, merged field by field, written as a [`Value::Map`]
///   marker at its path and a cell for each leaf. So a `Document` handed to a write is expanded
///   by the writer, and the state never holds one as a register ([`Value::check_register`]
///   refuses it; so do `Reg::new` and both decoders).
/// * `Map` is the explicit map marker (the model's `MAP`): the register value at a map path
///   that a write of a sub-document puts there, beside the cells of its leaves. It is never a
///   value a caller hands in, and never inside an array.
///
/// The variants keep the BSON types apart: `Int32(2)`, `Int64(2)`, `Double(2.0)` and
/// `Decimal128` of two are four different values. A `Decimal128` is its 16 bytes, as BSON
/// stores it.
#[derive(Clone, Debug)]
pub enum Value {
    Unset,
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
    /// A document: named fields in order. Legal only inside an `Array`; see above.
    Document(Vec<(String, Value)>),
    /// The explicit map marker. See above.
    Map,
}

/// A value that is not legal where it was put.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum InvalidValue {
    /// A document as a register: only inside an array is a document a value.
    #[error("merge state: a document is a value only inside an array")]
    DocumentAsRegister,
    /// The map marker inside an array or a document, or handed to a write.
    #[error("merge state: the map marker is not a value a caller can write or an array can hold")]
    MapMarker,
    /// Arrays and documents nested past the BSON depth limit.
    #[error("merge state: a value is nested too deep")]
    TooDeep,
    /// A written document names a field twice, or a field name has a NUL byte.
    #[error("merge state: a document field name is repeated or has a NUL byte")]
    BadFieldName,
}

impl Value {
    /// May this value be a register of the state? Not a top-level `Document`; an array's
    /// elements may be anything but the map marker (documents among them, at any depth).
    pub fn check_register(&self) -> Result<(), InvalidValue> {
        match self {
            Value::Document(_) => Err(InvalidValue::DocumentAsRegister),
            Value::Map => Ok(()),
            v => v.check_element(0),
        }
    }

    /// May this value be inside an array (`depth` levels of arrays and documents down)?
    fn check_element(&self, depth: usize) -> Result<(), InvalidValue> {
        match self {
            Value::Map => Err(InvalidValue::MapMarker),
            Value::Array(a) => {
                if depth >= super::codec::MAX_VALUE_DEPTH {
                    return Err(InvalidValue::TooDeep);
                }
                a.iter().try_for_each(|x| x.check_element(depth + 1))
            }
            Value::Document(f) => {
                if depth >= super::codec::MAX_VALUE_DEPTH {
                    return Err(InvalidValue::TooDeep);
                }
                f.iter().try_for_each(|(_, x)| x.check_element(depth + 1))
            }
            _ => Ok(()),
        }
    }

    /// May a caller hand this value to a write? Anything `check_register` takes, and a
    /// `Document` at any level of a sub-document (the writer expands it into a map): its field
    /// names must be distinct and free of NUL bytes, and no map marker.
    pub fn check_input(&self) -> Result<(), InvalidValue> {
        match self {
            Value::Document(f) => {
                let mut names: Vec<&str> = f.iter().map(|(n, _)| n.as_str()).collect();
                names.sort_unstable();
                if names.iter().any(|n| n.contains('\0')) || names.windows(2).any(|w| w[0] == w[1])
                {
                    return Err(InvalidValue::BadFieldName);
                }
                f.iter().try_for_each(|(_, x)| x.check_input())
            }
            v => v.check_register(),
        }
    }

    /// Does this value hold a `Document` anywhere? The version 1 form cannot spell one.
    pub(super) fn contains_document(&self) -> bool {
        match self {
            Value::Document(_) => true,
            Value::Array(a) => a.iter().any(Value::contains_document),
            _ => false,
        }
    }

    /// The model's `numeric()`: `None` (`Unset`) or an integer. Only an `Int64` counts: `Int32`
    /// and `Decimal128` counters are not modelled yet, so they are plain register values.
    pub fn is_numeric(&self) -> bool {
        matches!(self, Value::Unset | Value::Int64(_))
    }
}

impl Value {
    /// The model's `same(a, b)` (Q4, ruled): the write diff compares value AND type,
    /// so `$set 2` over a stored `2.0` is a write, and so is an `Int32` over an `Int64`
    /// of the same number. A missing field (`None`) is the same as `Unset`, not as `Null`: `$set x
    /// null` over a missing field is a write. Doubles
    /// compare as numbers (`==`: `0.0` is `-0.0`, NaN is not NaN), as Python does; an
    /// array compares element by element in the same way, type for type.
    pub fn same(current: Option<&Value>, new: &Value) -> bool {
        Self::same_value(current.unwrap_or(&Value::Unset), new)
    }

    fn same_value(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Double(a), Value::Double(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(a, b)| Self::same_value(a, b))
            }
            (Value::Document(a), Value::Document(b)) => {
                a.len() == b.len()
                    && a.iter().zip(b).all(|(a, b)| a.0 == b.0 && Self::same_value(&a.1, &b.1))
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
            (Value::Unset, Value::Unset) | (Value::Null, Value::Null) => true,
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
            (Value::Document(a), Value::Document(b)) => a == b,
            (Value::Map, Value::Map) => true,
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
    /// A scalar write against a map (or the reverse) at one path: declared for the nested
    /// stage; nothing produces it yet.
    TypeClash,
    /// Two concurrent array registers at one path: declared for the nested stage; nothing
    /// produces it yet.
    ArrayClash,
}

/// What a loss record is about: the whole document (`insertLost` of a superseded insert), or one
/// field. They are different things even when a field is literally named `_`, which is the name
/// the version 1 spelling and the reference model give the whole document: that is why this is
/// a variant and not a [`Path`].
///
/// Ordering: by the path the version 1 form writes (the whole document as the one segment `_`),
/// the whole document first among equals. That keeps the order of the version 1 bytes exactly
/// what it was when a loss was keyed by a path alone.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum LossAt {
    Document,
    Field(Path),
}

impl LossAt {
    /// The field this record is about, `None` for the whole document.
    pub fn field(&self) -> Option<&Path> {
        match self {
            LossAt::Document => None,
            LossAt::Field(p) => Some(p),
        }
    }

    /// The path the version 1 form writes for this record. A field literally named `_` has the
    /// same one, which is a known collision of that form (see `codec.rs`).
    pub(super) fn v1_path(&self) -> Path {
        match self {
            LossAt::Document => Path(vec!["_".to_string()]),
            LossAt::Field(p) => p.clone(),
        }
    }
}

/// Where the whole document (the segment `_`) sorts against the field `q`: by that path, and
/// before a field literally named `_`.
fn document_against(q: &Path) -> std::cmp::Ordering {
    std::iter::once("_").cmp(q.segments().iter().map(String::as_str)).then(std::cmp::Ordering::Less)
}

impl Ord for LossAt {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (LossAt::Document, LossAt::Document) => std::cmp::Ordering::Equal,
            (LossAt::Document, LossAt::Field(q)) => document_against(q),
            (LossAt::Field(p), LossAt::Document) => document_against(p).reverse(),
            (LossAt::Field(p), LossAt::Field(q)) => p.cmp(q),
        }
    }
}

impl PartialOrd for LossAt {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<Path> for LossAt {
    fn from(p: Path) -> Self {
        LossAt::Field(p)
    }
}

/// For messages: the field's dotted path, or `_` for the whole document. Not a key (a field
/// named `_` prints alike).
impl std::fmt::Display for LossAt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LossAt::Document => f.write_str("_"),
            LossAt::Field(p) => p.fmt(f),
        }
    }
}

/// The key of a loss record: kind, what it is about and the stamp of the lost write. Ordered so
/// that it can key a `BTreeMap`.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct LossKey {
    pub kind: LossKind,
    pub at: LossAt,
    pub stamp: MStamp,
}

impl LossKey {
    /// A record about the field `path`.
    pub fn field(kind: LossKind, path: Path, stamp: MStamp) -> Self {
        Self { kind, at: LossAt::Field(path), stamp }
    }

    /// A record about the whole document.
    pub fn document(kind: LossKind, stamp: MStamp) -> Self {
        Self { kind, at: LossAt::Document, stamp }
    }
}

/// What a loss record keeps of the lost write.
#[derive(Clone, PartialEq, Debug)]
pub enum LossValue {
    /// A lost register value.
    Value(Value),
    /// A lost counter slot total.
    Total(i128),
    /// The field order of a lost whole document (`insertLost` on `"_"`).
    Fields(Vec<Path>),
}
