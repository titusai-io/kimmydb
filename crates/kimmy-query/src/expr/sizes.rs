//! The size limits of the expression layer: a string or an array an expression
//! builds is at most 16 MiB, the reference's limit on a string and the size of a
//! document (a string must be shorter than that, as the reference's is). A value over it is refused with `Error::ValueTooLarge` (`413`), and
//! the length is worked out before the value is built wherever it can be, so an
//! expression that asks for a terabyte allocates nothing.
//!
//! An array is counted as BSON, the way it would encode: five bytes of framing and,
//! for each element, its type byte, its decimal index with a terminator, and its
//! payload. Every function takes the limit as a parameter, so a test reaches it
//! with a small one.

use bson::Bson;
use kimmy_core::{Error, Result};

/// The longest string an expression may build: one byte short of 16 MiB. The
/// reference's string builder passes 16,777,215 bytes and refuses 16,777,216
/// (`Tried to create string longer than 16MB`), so a string of exactly 16 MiB is
/// over the limit.
pub(crate) const MAX_STRING_BYTES: usize = kimmy_core::limits::MAX_DOCUMENT_BYTES - 1;

/// The most an array an expression builds may hold, as BSON: 16 MiB.
pub(crate) const MAX_ARRAY_BYTES: usize = kimmy_core::limits::MAX_DOCUMENT_BYTES;

const STRING_LIMIT: &str = "16 MiB string limit";
const ARRAY_LIMIT: &str = "16 MiB array limit";

/// Refuse a string of `bytes` bytes, over `max`, that `what` is about to build.
pub(crate) fn check_string_len_within(what: &'static str, bytes: usize, max: usize) -> Result<()> {
    if bytes > max {
        return Err(Error::ValueTooLarge {
            what,
            bytes,
            max,
            limit: STRING_LIMIT.into(),
            remedy: "build a shorter string, or $limit or $substrBytes the input",
        });
    }
    Ok(())
}

/// The decimal digits of an array index.
fn digits(n: usize) -> usize {
    n.checked_ilog10().map_or(1, |d| d as usize + 1)
}

/// What the value is as the payload of a BSON element (its name and type byte
/// excluded), saturating.
pub(crate) fn bson_bytes(value: &Bson) -> usize {
    match value {
        Bson::Double(_) | Bson::Int64(_) | Bson::DateTime(_) | Bson::Timestamp(_) => 8,
        Bson::Int32(_) => 4,
        Bson::Decimal128(_) => 16,
        Bson::Boolean(_) => 1,
        Bson::ObjectId(_) => 12,
        Bson::Null | Bson::Undefined | Bson::MinKey | Bson::MaxKey => 0,
        Bson::String(s) | Bson::Symbol(s) | Bson::JavaScriptCode(s) => {
            5usize.saturating_add(s.len())
        }
        Bson::Binary(b) => 5usize.saturating_add(b.bytes.len()),
        Bson::RegularExpression(r) => {
            2usize.saturating_add(r.pattern.as_str().len()).saturating_add(r.options.as_str().len())
        }
        Bson::DbPointer(_) => 17,
        Bson::JavaScriptCodeWithScope(c) => {
            let scope = c.scope.iter().fold(5usize, |sum, (k, v)| {
                sum.saturating_add(2 + k.len()).saturating_add(bson_bytes(v))
            });
            9usize.saturating_add(c.code.len()).saturating_add(scope)
        }
        Bson::Document(d) => d.iter().fold(5usize, |sum, (k, v)| {
            sum.saturating_add(2 + k.len()).saturating_add(bson_bytes(v))
        }),
        Bson::Array(items) => items.iter().enumerate().fold(5usize, |sum, (i, v)| {
            sum.saturating_add(2 + digits(i)).saturating_add(bson_bytes(v))
        }),
    }
}

/// The BSON size of a document: what it encodes to, worked out without encoding it.
pub(crate) fn document_bytes(doc: &bson::Document) -> usize {
    doc.iter().fold(5usize, |sum, (k, v)| {
        sum.saturating_add(2 + k.len()).saturating_add(bson_bytes(v))
    })
}

/// The bytes an array of an expression is growing to, element by element. `add`
/// counts an element and refuses at once when the array is over the limit.
pub(crate) struct ArrayBudget {
    what: &'static str,
    max: usize,
    count: usize,
    bytes: usize,
}

impl ArrayBudget {
    /// A budget for the array `what` builds, within `max` bytes.
    pub(crate) fn new(what: &'static str, max: usize) -> Self {
        Self { what, max, count: 0, bytes: 5 }
    }

    /// Count `value` as the next element.
    pub(crate) fn add(&mut self, value: &Bson) -> Result<()> {
        self.add_payload(bson_bytes(value))
    }

    /// Count an element whose payload is `payload` bytes.
    pub(crate) fn add_payload(&mut self, payload: usize) -> Result<()> {
        self.bytes = self.bytes.saturating_add(2 + digits(self.count)).saturating_add(payload);
        self.count += 1;
        if self.bytes > self.max {
            return Err(Error::ValueTooLarge {
                what: self.what,
                bytes: self.bytes,
                max: self.max,
                limit: ARRAY_LIMIT.into(),
                remedy: "build a smaller array, or $slice, $filter or $limit the input",
            });
        }
        Ok(())
    }
}

/// Refuse `items` as an array of `what` when it is over `max` bytes.
pub(crate) fn check_array_within(what: &'static str, items: &[Bson], max: usize) -> Result<()> {
    let mut budget = ArrayBudget::new(what, max);
    items.iter().try_for_each(|item| budget.add(item))
}
