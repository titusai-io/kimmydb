//! The size limits of the expression layer: a string, an array or a document an
//! expression builds is at most 16 MiB, the reference's limit on a string and the
//! size of a document (a string must be shorter than that, as the reference's
//! is). A value over it is refused with `Error::ValueTooLarge` (`413`), and the
//! length is worked out before the value is built wherever it can be, so an
//! expression that asks for a terabyte allocates nothing.
//!
//! An array or a document is counted as BSON, the way it would encode: five bytes
//! of framing and, for each element, its type byte, its name (an array's is its
//! decimal index) with a terminator, and its payload. A builder keeps a running
//! total and stops at the first element that takes it over the limit, so it never
//! holds more than the limit and one element. Every function takes the limit as a
//! parameter, so a test reaches it with a small one.

use bson::{Bson, Document};
use kimmy_core::{Error, Result};

/// The longest string an expression may build: one byte short of 16 MiB. The
/// reference's string builder passes 16,777,215 bytes and refuses 16,777,216
/// (`Tried to create string longer than 16MB`), so a string of exactly 16 MiB is
/// over the limit.
pub(crate) const MAX_STRING_BYTES: usize = kimmy_core::limits::MAX_DOCUMENT_BYTES - 1;

/// The most an array an expression builds may hold, as BSON: 16 MiB.
pub(crate) const MAX_ARRAY_BYTES: usize = kimmy_core::limits::MAX_DOCUMENT_BYTES;

/// A string of exactly 16 MiB is over the limit, so the limit is named by its
/// length, which reads true at every size over it.
const STRING_LIMIT: &str = "string limit of 16777215 bytes (16 MiB less one)";
const ARRAY_LIMIT: &str = "16 MiB array limit";
const DOCUMENT_LIMIT: &str = "16 MiB document limit";

/// Refuse a string of `bytes` bytes, over `max`, that `what` is about to build.
pub(crate) fn check_string_len_within(what: &'static str, bytes: usize, max: usize) -> Result<()> {
    if bytes > max {
        return Err(Error::ValueTooLarge {
            what,
            bytes,
            max,
            limit: STRING_LIMIT,
            remedy: "build a shorter string, or $limit or $substrBytes the input",
        });
    }
    Ok(())
}

/// The decimal digits of an array index.
fn digits(n: usize) -> usize {
    n.checked_ilog10().map_or(1, |d| d as usize + 1)
}

/// What the document is as a BSON document, saturating.
pub(crate) fn document_bytes(d: &Document) -> usize {
    d.iter().fold(5usize, |sum, (k, v)| {
        sum.saturating_add(2usize.saturating_add(k.len())).saturating_add(bson_bytes(v))
    })
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
        Bson::Document(d) => document_bytes(d),
        Bson::Array(items) => items.iter().enumerate().fold(5usize, |sum, (i, v)| {
            sum.saturating_add(2 + digits(i)).saturating_add(bson_bytes(v))
        }),
    }
}

/// The bytes an array of an expression is growing to, element by element. `add`
/// counts an element and refuses at once when the array is over the limit.
#[derive(Clone, Debug)]
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

    /// Refuse a total of `bytes` bytes, over the limit.
    fn check_total(&mut self, bytes: usize) -> Result<()> {
        self.bytes = bytes;
        if self.bytes > self.max {
            return Err(Error::ValueTooLarge {
                what: self.what,
                bytes: self.bytes,
                max: self.max,
                limit: ARRAY_LIMIT,
                remedy: "build a smaller array, or $slice, $filter or $limit the input",
            });
        }
        Ok(())
    }

    /// Count `value` as the next element.
    pub(crate) fn add(&mut self, value: &Bson) -> Result<()> {
        self.add_payload(bson_bytes(value))
    }

    /// Count an element whose payload is `payload` bytes.
    pub(crate) fn add_payload(&mut self, payload: usize) -> Result<()> {
        let bytes = self.bytes.saturating_add(2 + digits(self.count)).saturating_add(payload);
        self.count += 1;
        self.check_total(bytes)
    }
}

/// Refuse `items` as an array of `what` when it is over `max` bytes.
pub(crate) fn check_array_within(what: &'static str, items: &[Bson], max: usize) -> Result<()> {
    let mut budget = ArrayBudget::new(what, max);
    items.iter().try_for_each(|item| budget.add(item))
}

/// The bytes a document of an expression is growing to, field by field, for a
/// builder that sets fields one at a time. A field that replaces another is
/// counted for the difference, so a merge of a thousand documents that all set one
/// key counts that key once.
#[derive(Clone, Debug)]
pub(crate) struct DocBudget {
    what: &'static str,
    max: usize,
    bytes: usize,
}

impl DocBudget {
    /// A budget for the document `what` builds, from empty, within `max` bytes.
    pub(crate) fn new(what: &'static str, max: usize) -> Self {
        Self::starting_at(what, max, 5)
    }

    /// A budget for the document `what` builds, which already holds `bytes` bytes.
    pub(crate) fn starting_at(what: &'static str, max: usize, bytes: usize) -> Self {
        Self { what, max, bytes }
    }

    /// A budget for the document `what` builds, which is `doc` so far. Refuses
    /// when `doc` is over `max` already.
    pub(crate) fn of_document(what: &'static str, max: usize, doc: &Document) -> Result<Self> {
        let budget = Self::starting_at(what, max, document_bytes(doc));
        budget.check(budget.bytes)?;
        Ok(budget)
    }

    /// Count the field `key` set to `value`, which replaces `old` (a value's
    /// payload size) if the document has the key. Refuses before the value is
    /// stored.
    pub(crate) fn set(&mut self, key: &str, old: Option<usize>, value: &Bson) -> Result<()> {
        self.set_payload(key, old, bson_bytes(value))
    }

    /// [`Self::set`] for a value whose payload is `payload` bytes.
    pub(crate) fn set_payload(
        &mut self,
        key: &str,
        old: Option<usize>,
        payload: usize,
    ) -> Result<()> {
        let kept = self.bytes.saturating_sub(old.map_or(0, |o| 2 + key.len() + o));
        let bytes = kept.saturating_add(2 + key.len()).saturating_add(payload);
        self.check(bytes)?;
        self.bytes = bytes;
        Ok(())
    }

    /// Refuse a total of `bytes` bytes when it is over the limit.
    fn check(&self, bytes: usize) -> Result<()> {
        check_document_bytes(self.what, bytes, self.max)
    }

    /// Count the removal of the field `key`, whose value was `old` bytes.
    pub(crate) fn remove(&mut self, key: &str, old: usize) {
        self.bytes = self.bytes.saturating_sub(2 + key.len() + old);
    }
}

/// Refuse an array of `bytes` bytes, over `max`, that `what` is about to build:
/// for a builder that works the size out from its arguments and builds nothing
/// until it is known.
pub(crate) fn check_array_bytes(what: &'static str, bytes: usize, max: usize) -> Result<()> {
    ArrayBudget::new(what, max).check_total(bytes)
}

/// What the element names of an array of `count` elements take: for each, its
/// type byte, its decimal index and a terminator. Worked out by the number of
/// digits the index has, so it takes twenty steps at the most, and saturates.
pub(crate) fn index_framing(count: u64) -> usize {
    let mut bytes = 0u128;
    let mut low = 0u128;
    let mut digits = 1u32;
    let count = u128::from(count);
    while low < count && digits <= 20 {
        let high = 10u128.pow(digits).min(count);
        bytes += (high - low) * (2 + u128::from(digits));
        low = high;
        digits += 1;
    }
    usize::try_from(bytes).unwrap_or(usize::MAX)
}

/// An array of `count` elements whose payloads total `payload` bytes, as BSON.
pub(crate) fn array_bytes(count: usize, payload: usize) -> usize {
    5usize.saturating_add(index_framing(count as u64)).saturating_add(payload)
}

/// The bytes of an array that gives elements back as well as taking them (the
/// `n` best rows of a `$topN`, the last `n` values of a `$lastN`): the count and
/// the payload total are kept, and the size is worked out from them, exactly as
/// the array would encode. The size is checked once the group has kept what it
/// keeps, so a row that is taken and dropped in the same step does not count.
#[derive(Clone, Debug)]
pub(crate) struct HeldArray {
    what: &'static str,
    max: usize,
    count: usize,
    payload: usize,
}

impl HeldArray {
    /// A budget for the array `what` answers, within `max` bytes.
    pub(crate) fn new(what: &'static str, max: usize) -> Self {
        Self { what, max, count: 0, payload: 0 }
    }

    /// Take an element of `payload` bytes.
    pub(crate) fn take(&mut self, payload: usize) {
        self.count += 1;
        self.payload = self.payload.saturating_add(payload);
    }

    /// Give back an element of `payload` bytes.
    pub(crate) fn give_back(&mut self, payload: usize) {
        self.count = self.count.saturating_sub(1);
        self.payload = self.payload.saturating_sub(payload);
    }

    /// Refuse the array as it stands when it is over the limit.
    pub(crate) fn check(&self) -> Result<()> {
        let bytes = array_bytes(self.count, self.payload);
        if bytes > self.max {
            return Err(Error::ValueTooLarge {
                what: self.what,
                bytes,
                max: self.max,
                limit: ARRAY_LIMIT,
                remedy: "build a smaller array, or lower n, or $limit or $project the input",
            });
        }
        Ok(())
    }
}

/// Refuse a document of `bytes` bytes, over `max`, that `what` is about to build or
/// has just built.
pub(crate) fn check_document_bytes(what: &'static str, bytes: usize, max: usize) -> Result<()> {
    if bytes > max {
        return Err(Error::ValueTooLarge {
            what,
            bytes,
            max,
            limit: DOCUMENT_LIMIT,
            remedy: "build a smaller document, or leave out the fields that make it large",
        });
    }
    Ok(())
}
