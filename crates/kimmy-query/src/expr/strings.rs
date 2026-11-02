//! The string operators of ADR-260: `$trim`, `$ltrim`, `$rtrim`, `$regexMatch`,
//! `$regexFind`, `$regexFindAll`, `$replaceOne`, `$replaceAll`, and the
//! positional `$indexOfCP`, `$indexOfBytes`, `$strcasecmp`, `$strLenBytes` and
//! `$substrBytes`.
//!
//! Every rule below is the reference's: null and a missing field give null (or
//! `false`, or `[]`, as each operator says) where the reference does, a value
//! of the wrong type is an `InvalidQuery` naming the operator, and an operator
//! that takes named arguments refuses a name it does not know.
//!
//! The regex operators use the engine, the pattern budget and the documented
//! differences of the `$regex` filter (`kimmy_core::regex`): no backreferences
//! or lookaround, the flags `i`, `m`, `s`, `x` and `u`. A pattern that is a constant
//! is compiled once, when the expression is parsed, and charged to the
//! enclosing pipeline's budget; one computed from the document is compiled
//! when it is evaluated, the last one kept for the next document.

use std::cell::RefCell;

use bson::{Bson, Document};
use kimmy_core::deadline;
use kimmy_core::regex::{RegexMatch, RegexPattern};
use kimmy_core::{Error, Result};

use super::{Declared, Expr, MAX_STRING_BYTES, Op, Scope, check_string_len_within, type_name};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Both,
    Left,
    Right,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegexKind {
    Match,
    Find,
    FindAll,
}

impl RegexKind {
    fn name(self) -> &'static str {
        match self {
            RegexKind::Match => "$regexMatch",
            RegexKind::Find => "$regexFind",
            RegexKind::FindAll => "$regexFindAll",
        }
    }
}

/// A string operator with named arguments.
#[derive(Clone, Debug, PartialEq)]
pub enum StrExpr {
    /// `{$trim|$ltrim|$rtrim: {input, chars}}`.
    Trim { side: Side, input: Expr, chars: Option<Expr> },
    /// `{$regexMatch|$regexFind|$regexFindAll: {input, regex, options}}`.
    /// `compiled` is the pattern when `regex` and `options` are constants.
    Regex {
        kind: RegexKind,
        input: Expr,
        regex: Expr,
        options: Option<Expr>,
        compiled: Option<RegexPattern>,
    },
    /// `{$replaceOne|$replaceAll: {input, find, replacement}}`.
    Replace { all: bool, input: Expr, find: Expr, replacement: Expr },
}

fn nullish(v: &Bson) -> bool {
    matches!(v, Bson::Null | Bson::Undefined)
}

impl StrExpr {
    /// The sub-expressions, for the walks that ask what an expression reads.
    pub(super) fn children(&self) -> Vec<&Expr> {
        match self {
            StrExpr::Trim { input, chars, .. } => {
                std::iter::once(input).chain(chars.as_ref()).collect()
            }
            StrExpr::Regex { input, regex, options, .. } => {
                [input, regex].into_iter().chain(options.as_ref()).collect()
            }
            StrExpr::Replace { input, find, replacement, .. } => vec![input, find, replacement],
        }
    }

    pub(super) fn parse(name: &str, raw: &Bson, declared: &mut Declared) -> Result<Expr> {
        match name {
            "$trim" | "$ltrim" | "$rtrim" => {
                let spec = Expr::named_spec(name, raw, &["input", "chars"])?;
                let input = Expr::required(name, spec, "input")?;
                let side = match name {
                    "$trim" => Side::Both,
                    "$ltrim" => Side::Left,
                    _ => Side::Right,
                };
                let chars = spec.get("chars").map(|c| Expr::parse_in(c, declared)).transpose()?;
                Ok(Expr::Str(Box::new(StrExpr::Trim {
                    side,
                    input: Expr::parse_in(input, declared)?,
                    chars,
                })))
            }
            "$regexMatch" | "$regexFind" | "$regexFindAll" => {
                let kind = match name {
                    "$regexMatch" => RegexKind::Match,
                    "$regexFind" => RegexKind::Find,
                    _ => RegexKind::FindAll,
                };
                let spec = Expr::named_spec(name, raw, &["input", "regex", "options"])?;
                let input = Expr::parse_in(Expr::required(name, spec, "input")?, declared)?;
                let regex = Expr::parse_in(Expr::required(name, spec, "regex")?, declared)?;
                let options =
                    spec.get("options").map(|o| Expr::parse_in(o, declared)).transpose()?;
                // Constants are compiled now: a bad pattern is refused when the
                // pipeline is parsed, and counts against its budget.
                let compiled = match (&regex, options.as_ref()) {
                    (Expr::Literal(re), None) => constant_pattern(kind, re, &Bson::Null)?,
                    (Expr::Literal(re), Some(Expr::Literal(o))) => constant_pattern(kind, re, o)?,
                    _ => None,
                };
                Ok(Expr::Str(Box::new(StrExpr::Regex { kind, input, regex, options, compiled })))
            }
            "$replaceOne" | "$replaceAll" => {
                let spec = Expr::named_spec(name, raw, &["input", "find", "replacement"])?;
                let mut get = |key: &str| -> Result<Expr> {
                    Expr::parse_in(Expr::required(name, spec, key)?, declared)
                };
                Ok(Expr::Str(Box::new(StrExpr::Replace {
                    all: name == "$replaceAll",
                    input: get("input")?,
                    find: get("find")?,
                    replacement: get("replacement")?,
                })))
            }
            _ => unreachable!("dispatched by name"),
        }
    }

    pub(super) fn eval(&self, scope: &Scope<'_>) -> Result<Bson> {
        match self {
            StrExpr::Trim { side, input, chars } => {
                let name = match side {
                    Side::Both => "$trim",
                    Side::Left => "$ltrim",
                    Side::Right => "$rtrim",
                };
                let input = input.eval_in(scope)?;
                if nullish(&input) {
                    return Ok(Bson::Null);
                }
                let Bson::String(input) = &input else {
                    return Err(Error::InvalidQuery(format!(
                        "{name} requires its input to be a string, got {} instead",
                        type_name(&input)
                    )));
                };
                let chars = match chars {
                    None => None,
                    Some(c) => match c.eval_in(scope)? {
                        v if nullish(&v) => return Ok(Bson::Null),
                        Bson::String(s) => Some(s),
                        other => {
                            return Err(Error::InvalidQuery(format!(
                                "{name} requires 'chars' to be a string, got {} instead",
                                type_name(&other)
                            )));
                        }
                    },
                };
                // The scan is a pass over the text, a test against the set for each character.
                super::charge_text(input.len())?;
                Ok(Bson::String(trim(input, chars.as_deref(), *side).to_string()))
            }
            StrExpr::Regex { kind, input, regex, options, compiled } => {
                eval_regex(*kind, input, regex, options.as_ref(), compiled.as_ref(), scope)
            }
            StrExpr::Replace { all, input, find, replacement } => {
                let name = if *all { "$replaceAll" } else { "$replaceOne" };
                let values = [
                    ("input", input.eval_in(scope)?),
                    ("find", find.eval_in(scope)?),
                    ("replacement", replacement.eval_in(scope)?),
                ];
                // Type errors come before the null answer, as the reference's do.
                for (what, v) in &values {
                    if !nullish(v) && !matches!(v, Bson::String(_)) {
                        return Err(Error::InvalidQuery(format!(
                            "{name} requires that '{what}' be a string, found: {}",
                            type_name(v)
                        )));
                    }
                }
                let [(_, Bson::String(input)), (_, Bson::String(find)), (_, Bson::String(with))] =
                    &values
                else {
                    return Ok(Bson::Null);
                };
                // The scan for `find` is a pass over the text, and each match writes `with`.
                super::charge_text(input.len().saturating_add(with.len()))?;
                replace(input, find, with, *all, MAX_STRING_BYTES).map(Bson::String)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// $trim
// ---------------------------------------------------------------------------

/// What `$trim` strips when `chars` is not given: the reference's list, which is
/// narrower than Unicode's `White_Space` (it has no U+0085, U+2028, U+2029,
/// U+202F, U+205F or U+3000) and wider than ASCII (it has the null character).
fn is_default_trim(c: char) -> bool {
    matches!(c,
        '\u{0}' | '\u{20}' | '\u{9}'..='\u{D}' | '\u{A0}' | '\u{1680}' | '\u{2000}'..='\u{200A}')
}

fn trim<'a>(s: &'a str, chars: Option<&str>, side: Side) -> &'a str {
    let strip = |c: char| match chars {
        None => is_default_trim(c),
        Some(set) => set.contains(c),
    };
    match side {
        Side::Both => s.trim_matches(strip),
        Side::Left => s.trim_start_matches(strip),
        Side::Right => s.trim_end_matches(strip),
    }
}

// ---------------------------------------------------------------------------
// $replaceOne / $replaceAll
// ---------------------------------------------------------------------------

/// The length in bytes of what [`replace`] builds, `None` when it does not fit a
/// `usize`. Counted from the match count, before anything is built.
fn replaced_len(input: &str, find: &str, with: &str, all: bool) -> Option<usize> {
    let matches = if find.is_empty() {
        // Before every code point and at the end: "abc" has four places, and
        // `$replaceOne` takes the first.
        if all { input.chars().count().checked_add(1)? } else { 1 }
    } else if all {
        input.matches(find).count()
    } else {
        usize::from(input.contains(find))
    };
    // Each match swaps `find` for `with`. The matches are disjoint, so what they
    // remove (`matches * find.len()`) is at most the input and cannot overflow.
    let removed = if find.is_empty() { 0 } else { matches * find.len() };
    input.len().checked_add(matches.checked_mul(with.len())?)?.checked_sub(removed)
}

/// `$replaceOne` and `$replaceAll`, refused (`ValueTooLarge`) when the result
/// would be longer than `max` bytes. The length is worked out first, with checked
/// arithmetic, so that nothing is allocated for an answer that is refused: an
/// empty `find` over a megabyte with a megabyte `replacement` asks for a terabyte.
pub(super) fn replace(
    input: &str,
    find: &str,
    with: &str,
    all: bool,
    max: usize,
) -> Result<String> {
    let len = replaced_len(input, find, with, all).unwrap_or(usize::MAX);
    check_string_len_within(
        if all { "the string $replaceAll builds" } else { "the string $replaceOne builds" },
        len,
        max,
    )?;
    if !all {
        return Ok(input.replacen(find, with, 1));
    }
    if find.is_empty() {
        // Matches between every code point and at both ends: "abc" with "-"
        // gives "-a-b-c-". The reference matches between bytes and so builds
        // invalid UTF-8 from non-ASCII text, which it cannot return.
        let mut out = String::with_capacity(len);
        out.push_str(with);
        for c in input.chars() {
            out.push(c);
            out.push_str(with);
        }
        return Ok(out);
    }
    Ok(input.replace(find, with))
}

// ---------------------------------------------------------------------------
// $regexMatch / $regexFind / $regexFindAll
// ---------------------------------------------------------------------------

/// The pattern and flags a `regex` and an `options` value come to, or `None`
/// when `regex` is null (the operator then answers its null result). Refuses a
/// value of the wrong type, and flags given both in a regex value and in
/// `options`.
fn resolve(kind: RegexKind, regex: &Bson, options: &Bson) -> Result<Option<(String, String)>> {
    let name = kind.name();
    let options = match options {
        v if nullish(v) => None,
        Bson::String(s) => Some(s.as_str()),
        other => {
            return Err(Error::InvalidQuery(format!(
                "{name} needs 'options' to be of type string, found {}",
                type_name(other)
            )));
        }
    };
    let (pattern, inline) = match regex {
        v if nullish(v) => return Ok(None),
        Bson::String(s) => (s.clone(), ""),
        Bson::RegularExpression(re) => (re.pattern.to_string(), re.options.as_str()),
        other => {
            return Err(Error::InvalidQuery(format!(
                "{name} needs 'regex' to be of type string or regex, found {}",
                type_name(other)
            )));
        }
    };
    if !inline.is_empty() && options.is_some_and(|o| !o.is_empty()) {
        return Err(Error::InvalidQuery(format!(
            "{name} found regex option(s) specified in both 'regex' and 'options' fields"
        )));
    }
    let flags = options.unwrap_or(inline);
    if pattern.contains('\0') || flags.contains('\0') {
        return Err(Error::InvalidQuery(format!(
            "{name}: regular expression cannot contain an embedded null byte"
        )));
    }
    Ok(Some((pattern, flags.to_string())))
}

fn constant_pattern(kind: RegexKind, regex: &Bson, options: &Bson) -> Result<Option<RegexPattern>> {
    resolve(kind, regex, options)?
        .map(|(pattern, flags)| RegexPattern::new(&pattern, &flags))
        .transpose()
}

thread_local! {
    /// The last pattern compiled for a computed `regex`, so a column of one
    /// pattern costs one compile. One entry, not a map: each entry may hold a
    /// megabyte of automaton, and this lives as long as the thread.
    static LAST_PATTERN: RefCell<Option<RegexPattern>> = const { RefCell::new(None) };
}

fn dynamic_pattern(pattern: &str, flags: &str) -> Result<RegexPattern> {
    LAST_PATTERN.with(|last| {
        let mut last = last.borrow_mut();
        if let Some(p) = last.as_ref().filter(|p| p.pattern() == pattern && p.options() == flags) {
            return Ok(p.clone());
        }
        let compiled = RegexPattern::new(pattern, flags)?;
        *last = Some(compiled.clone());
        Ok(compiled)
    })
}

fn eval_regex(
    kind: RegexKind,
    input: &Expr,
    regex: &Expr,
    options: Option<&Expr>,
    compiled: Option<&RegexPattern>,
    scope: &Scope<'_>,
) -> Result<Bson> {
    let name = kind.name();
    let none = || match kind {
        RegexKind::Match => Bson::Boolean(false),
        RegexKind::Find => Bson::Null,
        RegexKind::FindAll => Bson::Array(Vec::new()),
    };
    let input = input.eval_in(scope)?;
    let regex = regex.eval_in(scope)?;
    let options = match options {
        Some(o) => o.eval_in(scope)?,
        None => Bson::Null,
    };
    // The reference checks the types of all three before it answers null.
    if !nullish(&input) && !matches!(input, Bson::String(_)) {
        return Err(Error::InvalidQuery(format!(
            "{name} needs 'input' to be of type string, found {}",
            type_name(&input)
        )));
    }
    let resolved = resolve(kind, &regex, &options)?;
    let (Bson::String(text), Some((pattern, flags))) = (&input, resolved) else {
        return Ok(none());
    };
    let owned;
    let pattern = match compiled {
        Some(p) => p,
        None => {
            owned = dynamic_pattern(&pattern, &flags)?;
            &owned
        }
    };
    // The text the pattern is about to scan counts toward the deadline (ADR-266), by sixteens
    // of bytes, before the scan: the regex engine cannot be interrupted from inside.
    deadline::tick_n(text.len() / 16)?;
    Ok(match kind {
        RegexKind::Match => Bson::Boolean(pattern.is_match(text)),
        RegexKind::Find => match pattern.first_match(text) {
            Some(m) => match_document(text, &m, text[..m.start].chars().count()),
            None => Bson::Null,
        },
        RegexKind::FindAll => find_all_documents(pattern, text, FIND_ALL_MAX_BYTES)?,
    })
}

/// The most a `$regexFindAll` answer may hold, as BSON: 64 MiB, the reference's
/// limit on its output buffer. A pattern that matches everywhere over a long
/// string would otherwise build one document per code point.
const FIND_ALL_MAX_BYTES: usize = 64 << 20;

/// Every match of `pattern` in `text` as `{match, idx, captures}` documents, in an
/// array, refused (`ValueTooLarge`) as soon as the documents so far, counted as
/// BSON, pass `max_bytes`. The matches are read one at a time, so a refused
/// answer never holds more than that, and `idx` is counted from the previous
/// match rather than from the start, so the whole is linear in the text.
pub(super) fn find_all_documents(
    pattern: &RegexPattern,
    text: &str,
    max_bytes: usize,
) -> Result<Bson> {
    let mut found = Vec::new();
    let mut bytes = 0usize;
    // The match last seen: its byte offset and its code point index.
    let (mut at_byte, mut at_cp) = (0usize, 0usize);
    pattern.try_for_each_match(text, |m| {
        // Once for each match found (ADR-266): a pattern that matches everywhere over a
        // long string is one match per code point.
        deadline::tick()?;
        at_cp += text[at_byte..m.start].chars().count();
        at_byte = m.start;
        bytes = bytes.saturating_add(match_bytes(text, m, found.len()));
        if bytes > max_bytes {
            return Err(Error::ValueTooLarge {
                what: "the $regexFindAll output",
                bytes,
                max: max_bytes,
                limit: "64 MiB limit".into(),
                remedy: "match a narrower pattern, or $limit or $substrBytes the input",
            });
        }
        found.push(match_document(text, m, at_cp));
        Ok(())
    })?;
    Ok(Bson::Array(found))
}

/// What the match is as an element of the answer's array, in BSON bytes: the
/// element's type byte, its decimal index and terminator, then the document
/// `{match, idx, captures}`.
pub(super) fn match_bytes(text: &str, m: &RegexMatch, index: usize) -> usize {
    let string = |len: usize| 4 + len + 1;
    let digits = |n: usize| n.checked_ilog10().map_or(1, |d| d as usize + 1);
    let captures: usize = m
        .groups
        .iter()
        .enumerate()
        .map(|(i, g)| 1 + digits(i) + 1 + g.map_or(0, |(a, b)| string(text[a..b].len())))
        .sum();
    let document = 4
        + (1 + "match".len() + 1 + string(m.end - m.start))
        // `idx` is an `Int32`: a code point index in a string no document can hold
        // more than 2^31 of.
        + (1 + "idx".len() + 1 + 4)
        + (1 + "captures".len() + 1 + 4 + captures + 1)
        + 1;
    1 + digits(index) + 1 + document
}

/// `{match, idx, captures}`: `idx` is the code point index `cp` of the match,
/// `captures` holds one string per group, null for a group that took no part.
pub(super) fn match_document(text: &str, m: &RegexMatch, cp: usize) -> Bson {
    let slice = |(a, b): (usize, usize)| Bson::String(text[a..b].to_string());
    let mut doc = Document::new();
    doc.insert("match", slice((m.start, m.end)));
    doc.insert("idx", super::count_bson(cp));
    doc.insert(
        "captures",
        Bson::Array(m.groups.iter().map(|g| g.map_or(Bson::Null, slice)).collect()),
    );
    Bson::Document(doc)
}

// ---------------------------------------------------------------------------
// The positional operators
// ---------------------------------------------------------------------------

/// The reference's `coerceToString`, which `$strcasecmp` and `$substrBytes` apply
/// to their string arguments: a string as it is, null and a missing field as the
/// empty string, a number as its rendering. An integer is its digits; a double is
/// printed as C's `%g` does, with six significant digits (see [`format_g`]), which
/// is not what `$toString` prints. Every other type is refused here, where the
/// reference also renders a date, a timestamp, code and a symbol.
fn coerce(op: Op, v: &Bson) -> Result<String> {
    match v {
        Bson::String(s) => Ok(s.clone()),
        v if nullish(v) => Ok(String::new()),
        Bson::Double(d) => Ok(format_g(*d)),
        Bson::Int32(_) | Bson::Int64(_) | Bson::Decimal128(_) => {
            match super::convert(v, super::ConvertTo::String)? {
                Bson::String(s) => Ok(s),
                _ => unreachable!("converting to a string gives a string"),
            }
        }
        other => Err(Error::InvalidQuery(format!(
            "{} can't convert {} to a string",
            op.name(),
            type_name(other)
        ))),
    }
}

/// A double as C's `printf("%g")` prints it: six significant digits, no trailing
/// zeros, an exponent when it is below -4 or at least 6 (`1e+21`, `1.23457e+11`,
/// `1e-05`), and `nan`, `inf` and `-inf`.
fn format_g(d: f64) -> String {
    if d.is_nan() {
        return "nan".to_string();
    }
    if d.is_infinite() {
        return if d < 0.0 { "-inf" } else { "inf" }.to_string();
    }
    if d == 0.0 {
        return if d.is_sign_negative() { "-0" } else { "0" }.to_string();
    }
    let trim = |s: &str| -> String {
        if s.contains('.') {
            s.trim_end_matches('0').trim_end_matches('.').to_string()
        } else {
            s.to_string()
        }
    };
    // The exponent is that of the value rounded to six digits, as `%e` has it.
    let scientific = format!("{d:.5e}");
    let (mantissa, exponent) = scientific.split_once('e').expect("`e` notation has an exponent");
    let exponent: i32 = exponent.parse().expect("an integer exponent");
    if (-4..6).contains(&exponent) {
        trim(&format!("{d:.*}", (5 - exponent) as usize))
    } else {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{}e{sign}{:02}", trim(mantissa), exponent.abs())
    }
}

pub(super) fn str_len_bytes(arg: &Bson) -> Result<Bson> {
    match arg {
        Bson::String(s) => Ok(super::count_bson(s.len())),
        other => Err(Error::InvalidQuery(format!(
            "$strLenBytes requires a string argument, found: {}",
            type_name(other)
        ))),
    }
}

/// ASCII case folding only, as the reference does: `é` and `É` differ.
pub(super) fn strcasecmp(a: &Bson, b: &Bson) -> Result<Bson> {
    let a = coerce(Op::Strcasecmp, a)?.to_ascii_uppercase();
    let b = coerce(Op::Strcasecmp, b)?.to_ascii_uppercase();
    Ok(Bson::Int32(match a.as_bytes().cmp(b.as_bytes()) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }))
}

/// `$substrBytes`: byte offsets that must fall between code points. A start below
/// zero is refused, and so is a start or length that is not a number that fits a
/// 64-bit integer (a NaN, an infinity, a double or decimal out of range); a
/// negative length reads to the end of the string.
pub(super) fn substr_bytes(args: &[Bson]) -> Result<Bson> {
    let op = Op::SubstrBytes;
    let s = coerce(op, &args[0])?;
    let out_of_range = |shown: String| {
        Error::InvalidQuery(format!(
            "$substrBytes: Can't coerce out of range value {shown} to long"
        ))
    };
    let number = |v: &Bson, what: &str| -> Result<i64> {
        match v {
            Bson::Int32(n) => Ok(i64::from(*n)),
            Bson::Int64(n) => Ok(*n),
            // Truncated toward zero.
            Bson::Double(d) if (i64::MIN as f64..-(i64::MIN as f64)).contains(d) => Ok(*d as i64),
            Bson::Double(d) => Err(out_of_range(format_g(*d))),
            Bson::Decimal128(d) => match kimmy_core::decimal::integer_part(d) {
                Some(part) if part.in_range => Ok(part.value),
                _ => Err(out_of_range(d.to_string().to_lowercase())),
            },
            other => Err(Error::InvalidQuery(format!(
                "$substrBytes: {what} must be a numeric type (is BSON type {})",
                type_name(other)
            ))),
        }
    };
    let start = number(&args[1], "starting index")?;
    let length = number(&args[2], "length")?;
    let Ok(start) = usize::try_from(start) else {
        return Err(Error::InvalidQuery(format!(
            "$substrBytes: starting index must be non-negative (got: {start})"
        )));
    };
    let bytes = s.as_bytes();
    // A negative length is "to the end".
    let end = match usize::try_from(length) {
        Ok(n) => start.saturating_add(n),
        Err(_) => usize::MAX,
    };
    let continuation = |i: usize| bytes.get(i).is_some_and(|b| b & 0xC0 == 0x80);
    if continuation(start) {
        return Err(Error::InvalidQuery(
            "$substrBytes: Invalid range, starting index is a UTF-8 continuation byte".into(),
        ));
    }
    if continuation(end) {
        return Err(Error::InvalidQuery(
            "$substrBytes: Invalid range, ending index is a UTF-8 continuation byte".into(),
        ));
    }
    if start >= bytes.len() {
        return Ok(Bson::String(String::new()));
    }
    let end = end.min(bytes.len());
    Ok(Bson::String(s[start..end].to_string()))
}

/// The optional `start` and `end` of the `$indexOf` string operators.
fn bound(op: Op, v: Option<&Bson>, what: &str, default: usize) -> Result<usize> {
    let Some(v) = v else { return Ok(default) };
    let n = match v {
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
            super::as_integer(v, &format!("{} {what} index", op.name()))?
        }
        other => {
            return Err(Error::InvalidQuery(format!(
                "{} requires an integral {what} index, found a value of type: {}",
                op.name(),
                type_name(other)
            )));
        }
    };
    usize::try_from(n).map_err(|_| {
        Error::InvalidQuery(format!(
            "{} requires a nonnegative {what} index, found: {n}",
            op.name()
        ))
    })
}

/// `$indexOfCP` and `$indexOfBytes`: `[string, substring, start?, end?]`. A null
/// or missing string is null; a substring must be a string.
///
/// The two read `end` differently, as the reference does. `$indexOfBytes` searches
/// the bytes `[start, end)` and finds the substring only when it lies wholly
/// inside them. `$indexOfCP` bounds where a match may *start*: it tries each
/// position from `start` up to, but not including, `end` or the end of the
/// string, and the substring may run on past `end`. So `['abcabc', 'bc', 0, 2]`
/// is -1 for bytes and 1 for code points, and an empty substring is found at
/// `start` only if `start` is such a position: `['abc', '', 2]` is 2 and
/// `['abc', '', 3]` and `['abc', '', 1, 1]` are -1 (the empty string finds the
/// empty substring at 0, the one exception).
pub(super) fn index_of(op: Op, args: &[Bson]) -> Result<Bson> {
    let name = op.name();
    let s = match &args[0] {
        v if nullish(v) => return Ok(Bson::Null),
        Bson::String(s) => s,
        other => {
            return Err(Error::InvalidQuery(format!(
                "{name} requires a string as the first argument, found: {}",
                type_name(other)
            )));
        }
    };
    let Bson::String(token) = &args[1] else {
        return Err(Error::InvalidQuery(format!(
            "{name} requires a string as the second argument, found: {}",
            type_name(&args[1])
        )));
    };
    let start = bound(op, args.get(2), "starting", 0)?;
    let end = bound(op, args.get(3), "ending", usize::MAX)?;

    if op == Op::IndexOfBytes {
        // A start past the end (or past `end`) finds nothing, even for "".
        if start > s.len() || start > end.min(s.len()) {
            return Ok(Bson::Int32(-1));
        }
        let hay = &s.as_bytes()[start..end.min(s.len())];
        let found = if token.is_empty() {
            Some(0)
        } else {
            hay.windows(token.len()).position(|w| w == token.as_bytes())
        };
        return Ok(found.map_or(Bson::Int32(-1), |at| super::count_bson(start + at)));
    }

    // Code points: translate the bounds to bytes, search, translate the answer back.
    let to_byte = |i: usize| s.char_indices().nth(i).map_or(s.len(), |(b, _)| b);
    if start > s.chars().count() {
        return Ok(Bson::Int32(-1));
    }
    let (lo, hi) = (to_byte(start), to_byte(end));
    let at = if token.is_empty() {
        (lo < hi || (s.is_empty() && start == 0)).then_some(lo)
    } else {
        s[lo..].find(token.as_str()).map(|at| lo + at).filter(|at| *at < hi)
    };
    Ok(at.map_or(Bson::Int32(-1), |at| super::count_bson(s[..at].chars().count())))
}
