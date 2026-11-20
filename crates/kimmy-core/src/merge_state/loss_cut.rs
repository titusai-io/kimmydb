//! The cut of a loss record's value and the cap of a document's records (Step 4, design 3.1 to 3.4).
//!
//! **The cut.** A value a record keeps is at most `cut` bytes of its compact encoding (the loss value's tag byte
//! included); a longer one is cut to what fits and becomes a `Cut*` variant, whose flag is part of the value. The cut is a
//! function of the value and `cut` only, so two members cut one value to equal bytes: a string to the longest prefix that
//! ends on a char boundary, binary data to a prefix, an array to its leading whole elements, a regular expression's pattern
//! like a string (its options stay), a field order to its leading paths. A counter total and the fixed-size values (numbers,
//! dates, ids, the markers, at most 17 bytes) are never cut. A received record whose value is over the cut is refused, not
//! cut again: a second cut would give one key two values on two members.
//!
//! **The cap.** The records of a document are the `K` greatest by [`LossKey::rank_cmp`], after the horizon filter.
//! `cap(cap(A) ∪ cap(B)) = cap(A ∪ B)` (a top-`K` over a total order), and the horizon removes a prefix of the order, so the
//! cap commutes with it (claims 1 and 2 of the design note); the tests of `loss_cap_tests` run both as properties.

use std::collections::BTreeMap;

use super::compact::loss_value_len;
use super::types::{LossKey, LossValue, Path, Value};

// A test-only record of every record a join makes or a state receives on this thread, so that a harness can name `U`, the
// records of a run before any cap (the claims of the design note are about `cap(F_h(U))`). Off until `sink_begin`.
#[cfg(any(test, feature = "merge-mutants"))]
thread_local! {
    static SINK: std::cell::RefCell<Option<BTreeMap<LossKey, LossValue>>> =
        const { std::cell::RefCell::new(None) };
}

/// Start collecting (and forget what was collected).
#[cfg(any(test, feature = "merge-mutants"))]
pub fn sink_begin() {
    SINK.with(|s| *s.borrow_mut() = Some(BTreeMap::new()));
}

/// Stop collecting and return what was.
#[cfg(any(test, feature = "merge-mutants"))]
pub fn sink_take() -> BTreeMap<LossKey, LossValue> {
    SINK.with(|s| s.borrow_mut().take().unwrap_or_default())
}

#[cfg(any(test, feature = "merge-mutants"))]
pub(super) fn sink_note(k: &LossKey, v: &LossValue) {
    SINK.with(|s| {
        if let Some(m) = s.borrow_mut().as_mut() {
            m.entry(k.clone()).or_insert_with(|| v.clone());
        }
    });
}

#[cfg(not(any(test, feature = "merge-mutants")))]
#[inline(always)]
pub(super) fn sink_note(_: &LossKey, _: &LossValue) {}

/// The bytes of the compact encoding of `v`, as the cut counts them.
pub fn encoded_len(v: &LossValue) -> usize {
    loss_value_len(v)
}

/// Whether the record value fits the cut: a total always does (it is a number), anything else by its encoding.
pub fn within_cut(v: &LossValue, cut: usize) -> bool {
    matches!(v, LossValue::Total(_)) || encoded_len(v) <= cut
}

/// A lost register value as a record keeps it under `cut`: whole when it fits, else cut (see the module).
pub fn cut_value(v: &Value, cut: usize) -> LossValue {
    let whole = LossValue::Value(v.clone());
    if encoded_len(&whole) <= cut {
        return whole;
    }
    let fits = |x: Value| encoded_len(&LossValue::CutValue(x)) <= cut;
    let shrunk = match v {
        Value::String(s) => Value::String(string_prefix(s, |p| fits(Value::String(p.to_string())))),
        Value::Binary { subtype, bytes } => {
            let mut n = bytes.len().min(cut);
            while n > 0 && !fits(Value::Binary { subtype: *subtype, bytes: bytes[..n].to_vec() }) {
                n -= 1;
            }
            Value::Binary { subtype: *subtype, bytes: bytes[..n].to_vec() }
        }
        Value::Regex { pattern, options } => {
            let options = if fits(Value::Regex { pattern: String::new(), options: options.clone() })
            {
                options.clone()
            } else {
                String::new()
            };
            let pattern = string_prefix(pattern, |p| {
                fits(Value::Regex { pattern: p.to_string(), options: options.clone() })
            });
            Value::Regex { pattern, options }
        }
        Value::Array(items) => {
            let mut kept = 0;
            while kept < items.len() && fits(Value::Array(items[..=kept].to_vec())) {
                kept += 1;
            }
            Value::Array(items[..kept].to_vec())
        }
        // Everything else is at most 17 bytes and a cut is at least 64 (the unchecked tiny cuts of a test build
        // keep it whole).
        other => return LossValue::Value(other.clone()),
    };
    LossValue::CutValue(shrunk)
}

/// A lost document's field order under `cut`: whole when it fits, else its leading paths that do.
pub fn cut_fields(fields: &[Path], cut: usize) -> LossValue {
    if encoded_len(&LossValue::Fields(fields.to_vec())) <= cut {
        return LossValue::Fields(fields.to_vec());
    }
    let mut kept = 0;
    while kept < fields.len() && encoded_len(&LossValue::CutFields(fields[..=kept].to_vec())) <= cut
    {
        kept += 1;
    }
    LossValue::CutFields(fields[..kept].to_vec())
}

/// The longest prefix of `s`, on a char boundary, for which `fits` holds (the empty string if none does).
fn string_prefix(s: &str, fits: impl Fn(&str) -> bool) -> String {
    let mut n = s.len().min(super::types::MAX_LOSS_CUT);
    loop {
        while n > 0 && !s.is_char_boundary(n) {
            n -= 1;
        }
        if n == 0 || fits(&s[..n]) {
            return s[..n].to_string();
        }
        n -= 1;
    }
}

/// Drop all but the `cap` greatest records by rank; the keys dropped, lowest first.
pub fn cap_losses(losses: &mut BTreeMap<LossKey, LossValue>, cap: usize) -> Vec<LossKey> {
    if losses.len() <= cap {
        return Vec::new();
    }
    let mut keys: Vec<&LossKey> = losses.keys().collect();
    keys.sort_by(|a, b| a.rank_cmp(b));
    let dropped: Vec<LossKey> = keys[..keys.len() - cap].iter().map(|k| (*k).clone()).collect();
    for k in &dropped {
        losses.remove(k);
    }
    dropped
}
