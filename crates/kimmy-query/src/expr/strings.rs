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
//! or lookaround, the flags `i`, `m`, `s` and `x`. A pattern that is a constant
//! is compiled once, when the expression is parsed, and charged to the
//! enclosing pipeline's budget; one computed from the document is compiled
//! when it is evaluated, the last one kept for the next document.

use std::cell::RefCell;

use bson::{Bson, Document};
use kimmy_core::regex::{RegexMatch, RegexPattern};
use kimmy_core::{Error, Result};

use super::{Declared, Expr, Op, Scope, type_name};

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
                Ok(Bson::String(replace(input, find, with, *all)))
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

fn replace(input: &str, find: &str, with: &str, all: bool) -> String {
    if !all {
        return input.replacen(find, with, 1);
    }
    if find.is_empty() {
        // Matches between every code point and at both ends: "abc" with "-"
        // gives "-a-b-c-".
        let mut out = String::with_capacity(input.len() + with.len());
        out.push_str(with);
        for c in input.chars() {
            out.push(c);
            out.push_str(with);
        }
        return out;
    }
    input.replace(find, with)
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
    Ok(match kind {
        RegexKind::Match => Bson::Boolean(pattern.is_match(text)),
        RegexKind::Find => match pattern.first_match(text) {
            Some(m) => match_document(text, &m),
            None => Bson::Null,
        },
        RegexKind::FindAll => {
            // Code point indexes for every match in one pass over the string.
            Bson::Array(pattern.find_all(text).iter().map(|m| match_document(text, m)).collect())
        }
    })
}

/// `{match, idx, captures}`: `idx` counts code points, `captures` holds one
/// string per group, null for a group that took no part.
fn match_document(text: &str, m: &RegexMatch) -> Bson {
    let slice = |(a, b): (usize, usize)| Bson::String(text[a..b].to_string());
    let mut doc = Document::new();
    doc.insert("match", slice((m.start, m.end)));
    doc.insert("idx", Bson::Int64(text[..m.start].chars().count() as i64));
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
/// empty string, a number as its decimal rendering. Every other type is refused
/// here, where the reference also renders a date, a timestamp, code and a symbol.
fn coerce(op: Op, v: &Bson) -> Result<String> {
    match v {
        Bson::String(s) => Ok(s.clone()),
        v if nullish(v) => Ok(String::new()),
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
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

pub(super) fn str_len_bytes(arg: &Bson) -> Result<Bson> {
    match arg {
        Bson::String(s) => Ok(Bson::Int64(s.len() as i64)),
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

/// `$substrBytes`: byte offsets that must fall between code points.
pub(super) fn substr_bytes(args: &[Bson]) -> Result<Bson> {
    let op = Op::SubstrBytes;
    let s = coerce(op, &args[0])?;
    let number = |v: &Bson, what: &str| -> Result<i64> {
        match v {
            Bson::Int32(n) => Ok(i64::from(*n)),
            Bson::Int64(n) => Ok(*n),
            // Truncated toward zero; a NaN is 0.
            Bson::Double(d) => Ok(*d as i64),
            Bson::Decimal128(d) => {
                Ok(kimmy_core::decimal::integer_part(d).map_or(0, |part| part.value))
            }
            other => Err(Error::InvalidQuery(format!(
                "$substrBytes: {what} must be a numeric type (is BSON type {})",
                type_name(other)
            ))),
        }
    };
    let start = number(&args[1], "starting index")?;
    let length = number(&args[2], "length")?;
    let bytes = s.as_bytes();
    // A negative start is past the end (the reference reads it as unsigned).
    let Ok(start) = usize::try_from(start) else {
        return Ok(Bson::String(String::new()));
    };
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

/// `$indexOfCP` and `$indexOfBytes`: `[string, substring, start?, end?]`. The
/// search covers `[start, end)` and returns -1 when the substring is not wholly
/// inside it. A null or missing string is null; a substring must be a string.
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

    // Work in bytes; for code points, translate the bounds in and the answer out.
    let cp = op == Op::IndexOfCp;
    let to_byte = |i: usize| -> usize {
        if !cp {
            return i.min(s.len());
        }
        s.char_indices().nth(i).map_or(s.len(), |(b, _)| b)
    };
    let (lo, hi) = (to_byte(start), to_byte(end));
    // A start past the end (or past `end`) finds nothing, even for "".
    let past = if cp { start > s.chars().count() } else { start > s.len() };
    if past || lo > hi {
        return Ok(Bson::Int64(-1));
    }
    let hay = &s.as_bytes()[lo..hi];
    let found = if token.is_empty() {
        Some(0)
    } else {
        hay.windows(token.len()).position(|w| w == token.as_bytes())
    };
    Ok(Bson::Int64(match found {
        None => -1,
        Some(at) if cp => s[..lo + at].chars().count() as i64,
        Some(at) => (lo + at) as i64,
    }))
}
