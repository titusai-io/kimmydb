//! Update operators.
//!
//! An update document is either a set of `$`-prefixed operators or a whole
//! replacement document — never a mix, because the two mean very different
//! things and guessing would silently discard fields.

use bson::{Bson, Document};
use kimmy_core::cmp::canonical_cmp;
use kimmy_core::{Error, Result};
use std::cmp::Ordering;
use std::collections::BTreeSet;

use crate::filter::{self, Filter};
use crate::path;
use crate::shape::{self, SortKey};

/// What an update document asks for.
#[derive(Clone, Debug, PartialEq)]
pub enum Update {
    /// Replace the document wholesale, preserving `_id`.
    Replace(Document),
    /// Apply operators in order.
    Operators(Vec<Operation>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Operation {
    pub path: String,
    pub kind: OpKind,
    /// The `arrayFilters` entries this path's `$[<identifier>]` segments name,
    /// resolved at parse time so that applying the operation needs nothing
    /// but the document. Empty for a path with no named positional segment.
    pub array_filters: Vec<ArrayFilter>,
}

/// One entry of a request's `arrayFilters`: the identifier that `$[<identifier>]`
/// segments refer to it by, and what an array element must satisfy to be
/// addressed through it.
#[derive(Clone, Debug, PartialEq)]
pub struct ArrayFilter {
    pub identifier: String,
    /// The filter with its identifier prefix removed, evaluated per element by
    /// [`filter::matches_element`].
    pub filter: Filter,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OpKind {
    Set(Bson),
    /// Set only when an upsert inserts; a no-op on an existing document.
    SetOnInsert(Bson),
    Unset,
    Inc(Bson),
    Mul(Bson),
    /// Set only if the new value is smaller than the current one.
    Min(Bson),
    /// Set only if the new value is larger than the current one.
    Max(Bson),
    Push(Bson),
    /// Append several values (`$push` with `$each`), then reshape the array.
    PushEach(PushEach),
    /// Append only values not already present.
    AddToSet(Vec<Bson>),
    /// Remove every element equal to this value.
    Pull(Bson),
    /// Remove every element equal to any of these values.
    PullAll(Vec<Bson>),
    /// Remove the first (`-1`) or last (`1`) element.
    Pop(i32),
    Rename(String),
    CurrentDate,
}

impl OpKind {
    /// The operator's name as it appears in an update document.
    fn name(&self) -> &'static str {
        match self {
            OpKind::Set(_) => "$set",
            OpKind::SetOnInsert(_) => "$setOnInsert",
            OpKind::Unset => "$unset",
            OpKind::Inc(_) => "$inc",
            OpKind::Mul(_) => "$mul",
            OpKind::Min(_) => "$min",
            OpKind::Max(_) => "$max",
            OpKind::Push(_) | OpKind::PushEach(_) => "$push",
            OpKind::AddToSet(_) => "$addToSet",
            OpKind::Pull(_) => "$pull",
            OpKind::PullAll(_) => "$pullAll",
            OpKind::Pop(_) => "$pop",
            OpKind::Rename(_) => "$rename",
            OpKind::CurrentDate => "$currentDate",
        }
    }
}

/// `$push` with `$each` and its modifiers.
///
/// Applied in MongoDB's order — insert at `position`, then `sort`, then
/// `slice` — which is what makes `{$each: [x], $sort: {t: 1}, $slice: -100}`
/// a capped, ordered history in one operator.
#[derive(Clone, Debug, PartialEq)]
pub struct PushEach {
    pub values: Vec<Bson>,
    /// Where the values go; `None` appends. Negative counts from the end.
    pub position: Option<i64>,
    pub sort: Option<ArraySort>,
    /// Keep the first `n` (positive) or last `n` (negative) elements; `0`
    /// empties the array.
    pub slice: Option<i64>,
}

/// How `$push`'s `$sort` orders the array.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArraySort {
    /// Whole elements, in canonical order.
    Whole { descending: bool },
    /// Elements are documents; order them by these fields. An element that
    /// is not a document sorts as though every field were missing.
    ByFields(Vec<SortKey>),
}

impl ArraySort {
    fn sort(&self, items: &mut [Bson]) {
        match self {
            ArraySort::Whole { descending } => items.sort_by(|a, b| {
                let ordering = canonical_cmp(a, b);
                if *descending { ordering.reverse() } else { ordering }
            }),
            ArraySort::ByFields(keys) => {
                let empty = Document::new();
                fn as_document<'a>(value: &'a Bson, empty: &'a Document) -> &'a Document {
                    match value {
                        Bson::Document(doc) => doc,
                        _ => empty,
                    }
                }
                items.sort_by(|a, b| {
                    shape::compare(keys, as_document(a, &empty), as_document(b, &empty))
                });
            }
        }
    }
}

/// The primary key field, which updates may not move.
const ID_FIELD: &str = "_id";

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

pub fn parse(doc: &Document) -> Result<Update> {
    parse_with_filters(doc, &[])
}

/// Parse an update together with the request's `arrayFilters`.
///
/// A path may contain `$[]` (every element) and `$[<identifier>]` (the
/// elements a filter selects) segments. Each filter document names exactly
/// one identifier — every top-level field starts with it — and is evaluated
/// against each element with that prefix removed: `{"line.qty": {$gt: 5}}`
/// tests `{qty: {$gt: 5}}` against each element, and a bare `{"line": {$gt:
/// 5}}` tests the element itself, which is how an array of scalars is
/// addressed. Every identifier a path uses must have a filter, and every
/// filter must be used by some path: a filter nothing refers to is nearly
/// always a misspelt identifier, and applying the update regardless would
/// change elements the caller never selected.
pub fn parse_with_filters(doc: &Document, array_filters: &[Document]) -> Result<Update> {
    let has_operators = doc.keys().any(|k| k.starts_with('$'));
    let has_plain = doc.keys().any(|k| !k.starts_with('$'));

    if has_operators && has_plain {
        return Err(Error::InvalidUpdate(
            "an update cannot mix operators with replacement fields".into(),
        ));
    }
    if !has_operators {
        if !array_filters.is_empty() {
            return Err(Error::InvalidUpdate(
                "arrayFilters apply to update operators, not to a replacement document".into(),
            ));
        }
        return Ok(Update::Replace(doc.clone()));
    }

    let filters = parse_array_filters(array_filters)?;
    let mut used = BTreeSet::new();

    let mut operations = Vec::new();
    for (key, value) in doc {
        let op = &key[1..];
        let Bson::Document(targets) = value else {
            return Err(Error::InvalidUpdate(format!("${op} requires a document")));
        };
        for (target_path, arg) in targets {
            if target_path == ID_FIELD {
                return Err(Error::InvalidUpdate("_id is immutable and cannot be updated".into()));
            }
            let kind = parse_op(op, arg)?;
            let identifiers = positional_identifiers(target_path)?;
            if let OpKind::Rename(target) = &kind
                && (has_positional(target_path) || has_positional(target))
            {
                // A rename names two fixed places; "wherever the filter
                // matches" is not a place a value can be moved from or to.
                return Err(Error::InvalidUpdate(
                    "$rename cannot use positional segments in its source or destination".into(),
                ));
            }
            let mut op_filters = Vec::new();
            for identifier in identifiers {
                let Some(found) = filters.iter().find(|f| f.identifier == identifier) else {
                    return Err(Error::InvalidUpdate(format!(
                        "the update path {target_path:?} uses identifier {identifier:?}, but \
                         arrayFilters has no filter for it"
                    )));
                };
                used.insert(identifier);
                op_filters.push(found.clone());
            }
            operations.push(Operation {
                path: target_path.clone(),
                kind,
                array_filters: op_filters,
            });
        }
    }

    if let Some(unused) = filters.iter().find(|f| !used.contains(&f.identifier)) {
        return Err(Error::InvalidUpdate(format!(
            "arrayFilters names identifier {:?}, which no update path uses",
            unused.identifier
        )));
    }

    reject_set_on_insert_conflicts(&operations)?;
    Ok(Update::Operators(operations))
}

/// Whether a path has any `$[...]` segment.
fn has_positional(path: &str) -> bool {
    path::segments(path).iter().any(|segment| segment.starts_with("$["))
}

/// Check a path's positional segments and return the identifiers they name,
/// in order of first use and without repeats. `$[]` names none.
fn positional_identifiers(path: &str) -> Result<Vec<String>> {
    let mut identifiers: Vec<String> = Vec::new();
    for (position, segment) in path::segments(path).iter().enumerate() {
        if *segment == "$" {
            return Err(Error::InvalidUpdate(format!(
                "the `$` positional operator is not supported (in path {path:?}); use \
                 `$[<identifier>]` with arrayFilters, or `$[]` for every element"
            )));
        }
        let Some(inner) = segment.strip_prefix("$[") else {
            continue;
        };
        let Some(identifier) = inner.strip_suffix(']') else {
            return Err(Error::InvalidUpdate(format!(
                "malformed positional segment {segment:?} in path {path:?}"
            )));
        };
        if position == 0 {
            return Err(Error::InvalidUpdate(format!(
                "path {path:?} begins with a positional segment, but a document is not an array"
            )));
        }
        if identifier.is_empty() {
            continue;
        }
        check_identifier(identifier)?;
        if !identifiers.iter().any(|known| known == identifier) {
            identifiers.push(identifier.to_string());
        }
    }
    Ok(identifiers)
}

/// MongoDB's rule for an identifier: a lowercase letter, then letters and
/// digits. Kept so that a filter written for MongoDB is accepted unchanged and
/// one written here is accepted there.
fn check_identifier(identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    let well_formed = chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_alphanumeric());
    if well_formed {
        Ok(())
    } else {
        Err(Error::InvalidUpdate(format!(
            "array filter identifier {identifier:?} must start with a lowercase letter and \
             contain only letters and digits"
        )))
    }
}

/// Parse the request's `arrayFilters` documents.
fn parse_array_filters(docs: &[Document]) -> Result<Vec<ArrayFilter>> {
    let mut out: Vec<ArrayFilter> = Vec::new();
    for doc in docs {
        let mut identifier = None;
        let stripped = strip_identifier(doc, &mut identifier)?;
        let Some(identifier) = identifier else {
            return Err(Error::InvalidUpdate(format!(
                "an arrayFilters entry must name an identifier in every field, as in \
                 {{\"line.qty\": ...}}; got {doc}"
            )));
        };
        check_identifier(&identifier)?;
        if out.iter().any(|f| f.identifier == identifier) {
            return Err(Error::InvalidUpdate(format!(
                "arrayFilters has more than one filter for identifier {identifier:?}"
            )));
        }
        let filter = filter::parse(&stripped)?;
        out.push(ArrayFilter { identifier, filter });
    }
    Ok(out)
}

/// Rewrite a filter document with the identifier prefix removed from every
/// field, recording the identifier and refusing a second one. `{"line.qty":
/// 1}` becomes `{"qty": 1}`; a bare `{"line": 1}` becomes `{"": 1}`, the empty
/// path that [`filter::matches_element`] reads as the element itself.
/// `$and`/`$or`/`$nor` are rewritten through; no other `$`-operator has a
/// meaning at the top of an array filter.
fn strip_identifier(doc: &Document, identifier: &mut Option<String>) -> Result<Document> {
    let mut out = Document::new();
    for (key, value) in doc {
        if let Some(op) = key.strip_prefix('$') {
            if !matches!(op, "and" | "or" | "nor") {
                return Err(Error::InvalidUpdate(format!(
                    "an arrayFilters entry takes field conditions and $and/$or/$nor, not ${op}"
                )));
            }
            let Bson::Array(branches) = value else {
                return Err(Error::InvalidUpdate(format!("${op} requires an array")));
            };
            let mut rewritten = Vec::with_capacity(branches.len());
            for branch in branches {
                let Bson::Document(branch) = branch else {
                    return Err(Error::InvalidUpdate(format!("${op} requires documents")));
                };
                rewritten.push(Bson::Document(strip_identifier(branch, identifier)?));
            }
            out.insert(key.clone(), Bson::Array(rewritten));
            continue;
        }
        let (prefix, rest) = key.split_once('.').unwrap_or((key.as_str(), ""));
        match identifier {
            Some(seen) if seen != prefix => {
                return Err(Error::InvalidUpdate(format!(
                    "an arrayFilters entry names one identifier, but this one names both \
                     {seen:?} and {prefix:?}"
                )));
            }
            Some(_) => {}
            None => *identifier = Some(prefix.to_string()),
        }
        out.insert(rest, value.clone());
    }
    Ok(out)
}

/// Refuse a `$setOnInsert` that shares a path — or a prefix of one — with any
/// other write in the update, as MongoDB does.
///
/// The two would disagree about the inserted document depending on the order
/// they ran in, and an update that means different things on insert and on
/// match is exactly the kind of thing that should fail loudly at parse time.
fn reject_set_on_insert_conflicts(operations: &[Operation]) -> Result<()> {
    let written_paths = |op: &Operation| -> Vec<String> {
        match &op.kind {
            // A rename writes its destination as well as clearing its source.
            OpKind::Rename(target) => vec![op.path.clone(), target.clone()],
            _ => vec![op.path.clone()],
        }
    };
    for (i, a) in operations.iter().enumerate() {
        if !matches!(a.kind, OpKind::SetOnInsert(_)) {
            continue;
        }
        for (j, b) in operations.iter().enumerate() {
            if i == j {
                continue;
            }
            for path in written_paths(b) {
                if paths_overlap(&a.path, &path) {
                    return Err(Error::InvalidUpdate(format!(
                        "$setOnInsert on {:?} conflicts with {} on {:?}",
                        a.path,
                        b.kind.name(),
                        path
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Whether two dot paths name the same field or one lies inside the other.
fn paths_overlap(a: &str, b: &str) -> bool {
    a == b
        || a.strip_prefix(b).is_some_and(|rest| rest.starts_with('.'))
        || b.strip_prefix(a).is_some_and(|rest| rest.starts_with('.'))
}

fn parse_op(op: &str, arg: &Bson) -> Result<OpKind> {
    let numeric = |arg: &Bson| -> Result<Bson> {
        match arg {
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => Ok(arg.clone()),
            _ => Err(Error::InvalidUpdate(format!("${op} requires a number"))),
        }
    };

    Ok(match op {
        "set" => OpKind::Set(arg.clone()),
        "setOnInsert" => OpKind::SetOnInsert(arg.clone()),
        "unset" => OpKind::Unset,
        "inc" => OpKind::Inc(numeric(arg)?),
        "mul" => OpKind::Mul(numeric(arg)?),
        "min" => OpKind::Min(arg.clone()),
        "max" => OpKind::Max(arg.clone()),
        "push" => match modifier_document(arg) {
            Some(modifiers) => OpKind::PushEach(parse_push_each(modifiers)?),
            None => OpKind::Push(arg.clone()),
        },
        "addToSet" => match modifier_document(arg) {
            Some(modifiers) => OpKind::AddToSet(parse_add_to_set_each(modifiers)?),
            None => OpKind::AddToSet(vec![arg.clone()]),
        },
        "pull" => OpKind::Pull(arg.clone()),
        "pullAll" => match arg {
            Bson::Array(values) => OpKind::PullAll(values.clone()),
            _ => return Err(Error::InvalidUpdate("$pullAll requires an array of values".into())),
        },
        "pop" => match arg {
            Bson::Int32(1) | Bson::Int64(1) => OpKind::Pop(1),
            Bson::Int32(-1) | Bson::Int64(-1) => OpKind::Pop(-1),
            _ => return Err(Error::InvalidUpdate("$pop requires 1 or -1".into())),
        },
        "rename" => match arg {
            Bson::String(target) => OpKind::Rename(target.clone()),
            _ => return Err(Error::InvalidUpdate("$rename requires a field name".into())),
        },
        "currentDate" => OpKind::CurrentDate,
        other => return Err(Error::UnsupportedOperator(format!("${other}"))),
    })
}

/// The argument as a modifier document, if that is what it is.
///
/// A document with any `$`-prefixed key is read as modifiers rather than as a
/// value to push: pushing `{$each: [1]}` literally is never what a caller
/// meant, and reading it as a value would hide a typo in a modifier name.
fn modifier_document(arg: &Bson) -> Option<&Document> {
    match arg {
        Bson::Document(doc) if doc.keys().any(|k| k.starts_with('$')) => Some(doc),
        _ => None,
    }
}

/// The values of a modifier document's `$each`, which every modifier needs.
fn each_values(op: &str, modifiers: &Document) -> Result<Vec<Bson>> {
    match modifiers.get("$each") {
        Some(Bson::Array(items)) => Ok(items.clone()),
        Some(_) => Err(Error::InvalidUpdate(format!("${op}: $each requires an array"))),
        None => Err(Error::InvalidUpdate(format!(
            "${op}: a modifier document requires $each (an array of values)"
        ))),
    }
}

/// Parse `{$each, $position, $sort, $slice}` for `$push`.
fn parse_push_each(modifiers: &Document) -> Result<PushEach> {
    let mut each = PushEach {
        values: each_values("push", modifiers)?,
        position: None,
        sort: None,
        slice: None,
    };
    for (key, value) in modifiers {
        match key.as_str() {
            "$each" => {}
            "$position" => each.position = Some(integer("$push: $position", value)?),
            "$slice" => each.slice = Some(integer("$push: $slice", value)?),
            "$sort" => each.sort = Some(parse_array_sort(value)?),
            other => {
                return Err(Error::InvalidUpdate(format!("$push: unrecognized clause {other:?}")));
            }
        }
    }
    Ok(each)
}

/// Parse `{$each}` for `$addToSet`, which takes no other modifier: a set has
/// no order to sort or position in, and no end to slice from.
fn parse_add_to_set_each(modifiers: &Document) -> Result<Vec<Bson>> {
    if let Some(other) = modifiers.keys().find(|k| *k != "$each") {
        return Err(Error::InvalidUpdate(format!("$addToSet: unrecognized clause {other:?}")));
    }
    each_values("addToSet", modifiers)
}

/// `1`, `-1`, or a `{field: direction}` document.
fn parse_array_sort(value: &Bson) -> Result<ArraySort> {
    match value {
        Bson::Int32(1) | Bson::Int64(1) | Bson::Double(1.0) => {
            Ok(ArraySort::Whole { descending: false })
        }
        Bson::Int32(-1) | Bson::Int64(-1) | Bson::Double(-1.0) => {
            Ok(ArraySort::Whole { descending: true })
        }
        Bson::Document(spec) if !spec.is_empty() => {
            let keys = shape::parse_sort(spec).map_err(|e| match e {
                Error::InvalidQuery(msg) => Error::InvalidUpdate(format!("$push: $sort: {msg}")),
                other => other,
            })?;
            Ok(ArraySort::ByFields(keys))
        }
        _ => Err(Error::InvalidUpdate(
            "$push: $sort requires 1, -1, or a {field: direction} document".into(),
        )),
    }
}

/// A whole number, in any of the numeric BSON types JSON may have produced.
fn integer(what: &str, value: &Bson) -> Result<i64> {
    match value {
        Bson::Int32(n) => Ok(i64::from(*n)),
        Bson::Int64(n) => Ok(*n),
        // JSON has one number type, so `-3` may arrive as `-3.0`.
        Bson::Double(d) if d.fract() == 0.0 && d.abs() < 9_007_199_254_740_992.0 => Ok(*d as i64),
        _ => Err(Error::InvalidUpdate(format!("{what} requires an integer"))),
    }
}

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------

/// Apply an update to an existing document in place.
///
/// `$setOnInsert` is skipped: it speaks only to the document an upsert creates,
/// for which [`apply_on_insert`] is the entry point.
///
/// `now_ms` is passed in rather than read from the clock so that `$currentDate`
/// stays deterministic in tests and consistent with the write's own timestamp.
pub fn apply(update: &Update, doc: &mut Document, now_ms: i64) -> Result<()> {
    apply_to(update, doc, now_ms, Target::Existing)
}

/// Apply an update to the document an upsert is about to insert.
///
/// `doc` arrives already seeded from the filter's equalities. `$setOnInsert`
/// runs first, then every other operator, so `{$setOnInsert: {created: t},
/// $inc: {n: 1}}` creates `{created: t, n: 1}` and later matches only count.
pub fn apply_on_insert(update: &Update, doc: &mut Document, now_ms: i64) -> Result<()> {
    apply_to(update, doc, now_ms, Target::Inserted)
}

/// Which kind of document an update is being applied to.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Target {
    Existing,
    Inserted,
}

fn apply_to(update: &Update, doc: &mut Document, now_ms: i64, target: Target) -> Result<()> {
    let operations = match update {
        Update::Replace(replacement) => {
            // `_id` belongs to the document's identity, not its contents.
            let id = doc.get(ID_FIELD).cloned();
            *doc = replacement.clone();
            match id {
                Some(id) => {
                    doc.insert(ID_FIELD, id);
                }
                None => {
                    doc.remove(ID_FIELD);
                }
            }
            return Ok(());
        }
        Update::Operators(ops) => ops,
    };

    // `$setOnInsert` first and only for an inserted document, then everything
    // else — each through `apply_expanded`, so a positional path is expanded
    // whichever of the two passes carries it.
    let on_insert = |op: &&Operation| matches!(op.kind, OpKind::SetOnInsert(_));
    if target == Target::Inserted {
        for op in operations.iter().filter(on_insert) {
            apply_expanded(op, doc, now_ms)?;
        }
    }
    for op in operations.iter().filter(|op| !on_insert(op)) {
        apply_expanded(op, doc, now_ms)?;
    }
    Ok(())
}

/// One operation, applied through however many concrete paths its positional
/// segments name.
///
/// The segments are resolved against the document as it is before the
/// operation touches it, and each concrete index path is then applied in turn.
/// Positions stay valid across those applications because `$unset` leaves a
/// null hole rather than shifting the elements after it.
fn apply_expanded(op: &Operation, doc: &mut Document, now_ms: i64) -> Result<()> {
    if !has_positional(&op.path) {
        return apply_one(op, doc, now_ms);
    }
    for concrete in expand_positional(&op.path, &op.array_filters, doc)? {
        let at = Operation { path: concrete, kind: op.kind.clone(), array_filters: Vec::new() };
        apply_one(&at, doc, now_ms)?;
    }
    Ok(())
}

/// The concrete index paths a positional path names in `doc`, in element
/// order: `items.$[line].qty` over a document whose second and fourth line
/// items satisfy `line` becomes `["items.1.qty", "items.3.qty"]`. An empty
/// result means no element was selected, and the operation leaves the
/// document as it found it.
fn expand_positional(path: &str, filters: &[ArrayFilter], doc: &Document) -> Result<Vec<String>> {
    let segments = path::segments(path);
    let (head, rest) = segments.split_first().expect("non-empty path");
    let mut prefix = vec![(*head).to_string()];
    let mut out = Vec::new();
    expand_into(doc.get(*head), rest, filters, path, &mut prefix, &mut out)?;
    Ok(out)
}

fn expand_into(
    value: Option<&Bson>,
    segments: &[&str],
    filters: &[ArrayFilter],
    path: &str,
    prefix: &mut Vec<String>,
    out: &mut Vec<String>,
) -> Result<()> {
    let Some((head, rest)) = segments.split_first() else {
        out.push(prefix.join("."));
        return Ok(());
    };

    if let Some(identifier) = head.strip_prefix("$[").and_then(|s| s.strip_suffix(']')) {
        // A positional segment has nothing to select from unless there is an
        // array here. Creating one, or treating a scalar as a one-element
        // array, would write somewhere the caller did not point at.
        let Some(Bson::Array(items)) = value else {
            let at = prefix.join(".");
            return Err(Error::InvalidUpdate(match value {
                None => format!("the path {at:?} must exist to apply {head:?} in {path:?}"),
                Some(_) => {
                    format!("the path {at:?} must be an array to apply {head:?} in {path:?}")
                }
            }));
        };
        let selector = match identifier {
            "" => None,
            named => Some(
                &filters
                    .iter()
                    .find(|f| f.identifier == named)
                    .ok_or_else(|| {
                        Error::InvalidUpdate(format!("no array filter for identifier {named:?}"))
                    })?
                    .filter,
            ),
        };
        for (index, item) in items.iter().enumerate() {
            if selector.is_none_or(|f| filter::matches_element(f, item)) {
                prefix.push(index.to_string());
                expand_into(Some(item), rest, filters, path, prefix, out)?;
                prefix.pop();
            }
        }
        return Ok(());
    }

    // A plain segment descends when it can; when it cannot, the remaining
    // segments are carried along as they are, and the operator decides what
    // an absent path means (`$set` creates it, `$pull` ignores it).
    let next = match value {
        Some(Bson::Document(child)) => child.get(*head),
        Some(Bson::Array(items)) => head.parse::<usize>().ok().and_then(|i| items.get(i)),
        _ => None,
    };
    prefix.push((*head).to_string());
    expand_into(next, rest, filters, path, prefix, out)?;
    prefix.pop();
    Ok(())
}

fn apply_one(op: &Operation, doc: &mut Document, now_ms: i64) -> Result<()> {
    let current = path::resolve(doc, &op.path).first().cloned().cloned();

    let invalid = |msg: String| Error::InvalidUpdate(msg);
    let set = |doc: &mut Document, value: Bson| -> Result<()> {
        path::set(doc, &op.path, value).map_err(invalid)
    };

    match &op.kind {
        // Reached only from `apply_on_insert`, which is what makes it a set.
        OpKind::Set(value) | OpKind::SetOnInsert(value) => set(doc, value.clone())?,

        OpKind::Unset => {
            path::unset(doc, &op.path);
        }

        OpKind::Inc(delta) => {
            // An absent field starts at zero, so `$inc` on a new counter works.
            let base = current.unwrap_or(Bson::Int32(0));
            set(doc, arithmetic(&base, delta, Arith::Add, &op.path)?)?;
        }

        OpKind::Mul(factor) => {
            let base = current.unwrap_or(Bson::Int32(0));
            set(doc, arithmetic(&base, factor, Arith::Mul, &op.path)?)?;
        }

        OpKind::Min(candidate) => {
            let replace = match &current {
                Some(existing) => canonical_cmp(candidate, existing) == Ordering::Less,
                // A missing field takes the value outright.
                None => true,
            };
            if replace {
                set(doc, candidate.clone())?;
            }
        }

        OpKind::Max(candidate) => {
            let replace = match &current {
                Some(existing) => canonical_cmp(candidate, existing) == Ordering::Greater,
                None => true,
            };
            if replace {
                set(doc, candidate.clone())?;
            }
        }

        OpKind::Push(value) => {
            let mut items = as_array(&current, &op.path)?;
            items.push(value.clone());
            set(doc, Bson::Array(items))?;
        }

        OpKind::PushEach(each) => {
            let mut items = as_array(&current, &op.path)?;
            // A position past either end clamps to that end, as in Mongo.
            let at = match each.position {
                None => items.len(),
                Some(p) if p >= 0 => usize::try_from(p).unwrap_or(usize::MAX).min(items.len()),
                Some(p) => items
                    .len()
                    .saturating_sub(usize::try_from(p.unsigned_abs()).unwrap_or(usize::MAX)),
            };
            items.splice(at..at, each.values.iter().cloned());
            if let Some(sort) = &each.sort {
                sort.sort(&mut items);
            }
            if let Some(n) = each.slice {
                slice(&mut items, n);
            }
            set(doc, Bson::Array(items))?;
        }

        OpKind::AddToSet(values) => {
            let mut items = as_array(&current, &op.path)?;
            for value in values {
                let present =
                    items.iter().any(|item| canonical_cmp(item, value) == Ordering::Equal);
                if !present {
                    items.push(value.clone());
                }
            }
            set(doc, Bson::Array(items))?;
        }

        OpKind::Pull(value) => {
            // Pulling from a missing field is a no-op, not an error.
            let Some(Bson::Array(items)) = current else {
                return Ok(());
            };
            let kept: Vec<Bson> = items
                .into_iter()
                .filter(|item| canonical_cmp(item, value) != Ordering::Equal)
                .collect();
            set(doc, Bson::Array(kept))?;
        }

        OpKind::PullAll(values) => {
            // Each listed value is matched as `$pull` matches a literal —
            // canonical equality, so `2` removes `2.0`. Pulling from a missing
            // field is a no-op; pulling from a scalar is an error, because the
            // caller believes the field is an array and it is not.
            let items = match current {
                None => return Ok(()),
                Some(Bson::Array(items)) => items,
                Some(_) => {
                    return Err(invalid(format!(
                        "cannot apply $pullAll to non-array field {:?}",
                        op.path
                    )));
                }
            };
            let kept: Vec<Bson> = items
                .into_iter()
                .filter(|item| {
                    !values.iter().any(|value| canonical_cmp(item, value) == Ordering::Equal)
                })
                .collect();
            set(doc, Bson::Array(kept))?;
        }

        OpKind::Pop(direction) => {
            let Some(Bson::Array(mut items)) = current else {
                return Ok(());
            };
            if !items.is_empty() {
                if *direction == 1 {
                    items.pop();
                } else {
                    items.remove(0);
                }
            }
            set(doc, Bson::Array(items))?;
        }

        OpKind::Rename(target) => {
            if target == ID_FIELD {
                return Err(invalid("_id is immutable and cannot be renamed onto".into()));
            }
            // Renaming a missing field is a no-op, matching Mongo.
            if let Some(value) = current {
                path::unset(doc, &op.path);
                path::set(doc, target, value).map_err(invalid)?;
            }
        }

        OpKind::CurrentDate => {
            set(doc, Bson::DateTime(bson::DateTime::from_millis(now_ms)))?;
        }
    }
    Ok(())
}

enum Arith {
    Add,
    Mul,
}

/// Apply arithmetic, preserving integer types where the result still fits.
fn arithmetic(base: &Bson, operand: &Bson, op: Arith, path: &str) -> Result<Bson> {
    let both_int = matches!(base, Bson::Int32(_) | Bson::Int64(_))
        && matches!(operand, Bson::Int32(_) | Bson::Int64(_));

    if both_int {
        let a = as_i64(base).expect("checked int");
        let b = as_i64(operand).expect("checked int");
        let result = match op {
            Arith::Add => a.checked_add(b),
            Arith::Mul => a.checked_mul(b),
        };
        // On overflow, widening to a double loses precision silently; refusing
        // is the honest outcome.
        return match result {
            Some(v) => Ok(Bson::Int64(v)),
            None => Err(Error::InvalidUpdate(format!(
                "arithmetic on field {path:?} overflowed a 64-bit integer"
            ))),
        };
    }

    let a = as_f64(base).ok_or_else(|| {
        Error::InvalidUpdate(format!("cannot apply arithmetic to non-numeric field {path:?}"))
    })?;
    let b = as_f64(operand).ok_or_else(|| {
        Error::InvalidUpdate("cannot apply arithmetic with a non-numeric operand".to_string())
    })?;
    Ok(Bson::Double(match op {
        Arith::Add => a + b,
        Arith::Mul => a * b,
    }))
}

fn as_i64(value: &Bson) -> Option<i64> {
    match value {
        Bson::Int32(v) => Some(i64::from(*v)),
        Bson::Int64(v) => Some(*v),
        _ => None,
    }
}

fn as_f64(value: &Bson) -> Option<f64> {
    match value {
        Bson::Int32(v) => Some(f64::from(*v)),
        Bson::Int64(v) => Some(*v as f64),
        Bson::Double(v) => Some(*v),
        _ => None,
    }
}

/// Keep the first `n` elements, or the last `-n` when `n` is negative.
fn slice(items: &mut Vec<Bson>, n: i64) {
    if n >= 0 {
        items.truncate(usize::try_from(n).unwrap_or(usize::MAX));
    } else {
        let keep = usize::try_from(n.unsigned_abs()).unwrap_or(usize::MAX);
        if keep < items.len() {
            items.drain(..items.len() - keep);
        }
    }
}

/// Interpret the current value as an array for the array operators.
fn as_array(current: &Option<Bson>, path: &str) -> Result<Vec<Bson>> {
    match current {
        Some(Bson::Array(items)) => Ok(items.clone()),
        // A missing field becomes a new array, which is what makes `$push` to a
        // fresh field work.
        None => Ok(Vec::new()),
        Some(_) => Err(Error::InvalidUpdate(format!(
            "cannot apply an array operator to non-array field {path:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    const NOW: i64 = 1_700_000_000_000;

    fn applied(update: Document, mut doc: Document) -> Document {
        let parsed = parse(&update).unwrap_or_else(|e| panic!("parse failed: {e}"));
        apply(&parsed, &mut doc, NOW).unwrap_or_else(|e| panic!("apply failed: {e}"));
        doc
    }

    fn apply_err(update: Document, mut doc: Document) -> String {
        let parsed = match parse(&update) {
            Ok(p) => p,
            Err(e) => return e.to_string(),
        };
        apply(&parsed, &mut doc, NOW).unwrap_err().to_string()
    }

    #[test]
    fn set_writes_scalars_and_nested_paths() {
        assert_eq!(applied(doc! { "$set": { "a": 2 } }, doc! { "a": 1 }), doc! { "a": 2 });
        assert_eq!(applied(doc! { "$set": { "a.b": 1 } }, doc! {}), doc! { "a": { "b": 1 } });
    }

    #[test]
    fn unset_removes_a_field() {
        assert_eq!(
            applied(doc! { "$unset": { "a": "" } }, doc! { "a": 1, "b": 2 }),
            doc! { "b": 2 }
        );
    }

    #[test]
    fn inc_starts_a_missing_field_from_zero() {
        assert_eq!(applied(doc! { "$inc": { "n": 5 } }, doc! {}), doc! { "n": 5i64 });
        assert_eq!(applied(doc! { "$inc": { "n": 5 } }, doc! { "n": 1 }), doc! { "n": 6i64 });
        assert_eq!(applied(doc! { "$inc": { "n": -2 } }, doc! { "n": 1 }), doc! { "n": -1i64 });
    }

    #[test]
    fn arithmetic_keeps_integers_integral() {
        // Silently widening to a double would lose precision on large ids.
        let out = applied(doc! { "$inc": { "n": 1 } }, doc! { "n": 9_007_199_254_740_992i64 });
        assert_eq!(out.get_i64("n").unwrap(), 9_007_199_254_740_993);
    }

    #[test]
    fn arithmetic_promotes_to_double_when_either_side_is_one() {
        assert_eq!(applied(doc! { "$inc": { "n": 0.5 } }, doc! { "n": 1 }), doc! { "n": 1.5 });
    }

    #[test]
    fn integer_overflow_is_refused_rather_than_silently_widened() {
        let err = apply_err(doc! { "$inc": { "n": 1 } }, doc! { "n": i64::MAX });
        assert!(err.contains("overflow"), "unhelpful error: {err}");
    }

    #[test]
    fn arithmetic_on_a_non_numeric_field_is_an_error() {
        let err = apply_err(doc! { "$inc": { "s": 1 } }, doc! { "s": "text" });
        assert!(err.contains("non-numeric"), "unhelpful error: {err}");
    }

    #[test]
    fn mul_multiplies() {
        assert_eq!(applied(doc! { "$mul": { "n": 3 } }, doc! { "n": 4 }), doc! { "n": 12i64 });
        // A missing field is treated as zero, as in Mongo.
        assert_eq!(applied(doc! { "$mul": { "n": 3 } }, doc! {}), doc! { "n": 0i64 });
    }

    #[test]
    fn min_and_max_only_move_in_one_direction() {
        assert_eq!(applied(doc! { "$min": { "n": 1 } }, doc! { "n": 5 }), doc! { "n": 1 });
        assert_eq!(applied(doc! { "$min": { "n": 9 } }, doc! { "n": 5 }), doc! { "n": 5 });
        assert_eq!(applied(doc! { "$max": { "n": 9 } }, doc! { "n": 5 }), doc! { "n": 9 });
        assert_eq!(applied(doc! { "$max": { "n": 1 } }, doc! { "n": 5 }), doc! { "n": 5 });
        // A missing field takes the value outright.
        assert_eq!(applied(doc! { "$min": { "n": 3 } }, doc! {}), doc! { "n": 3 });
    }

    #[test]
    fn push_appends_and_creates_missing_arrays() {
        assert_eq!(applied(doc! { "$push": { "a": 2 } }, doc! { "a": [1] }), doc! { "a": [1, 2] });
        assert_eq!(applied(doc! { "$push": { "a": 1 } }, doc! {}), doc! { "a": [1] });
    }

    #[test]
    fn push_each_appends_several() {
        assert_eq!(
            applied(doc! { "$push": { "a": { "$each": [2, 3] } } }, doc! { "a": [1] }),
            doc! { "a": [1, 2, 3] }
        );
    }

    #[test]
    fn push_slice_keeps_one_end_or_empties() {
        let push = |slice: i32| doc! { "$push": { "a": { "$each": [4, 5], "$slice": slice } } };
        let base = doc! { "a": [1, 2, 3] };
        assert_eq!(applied(push(2), base.clone()), doc! { "a": [1, 2] });
        assert_eq!(applied(push(-2), base.clone()), doc! { "a": [4, 5] });
        assert_eq!(applied(push(0), base.clone()), doc! { "a": [] });
        // A slice wider than the array keeps everything.
        assert_eq!(applied(push(10), base.clone()), doc! { "a": [1, 2, 3, 4, 5] });
        assert_eq!(applied(push(-10), base), doc! { "a": [1, 2, 3, 4, 5] });
        // JSON has one number type, so an integral double is an integer.
        assert_eq!(
            applied(
                doc! { "$push": { "a": { "$each": [], "$slice": -1.0 } } },
                doc! { "a": [1, 2] }
            ),
            doc! { "a": [2] }
        );
    }

    #[test]
    fn push_sort_orders_scalars_and_subdocuments() {
        assert_eq!(
            applied(doc! { "$push": { "a": { "$each": [2], "$sort": 1 } } }, doc! { "a": [3, 1] }),
            doc! { "a": [1, 2, 3] }
        );
        assert_eq!(
            applied(doc! { "$push": { "a": { "$each": [2], "$sort": -1 } } }, doc! { "a": [3, 1] }),
            doc! { "a": [3, 2, 1] }
        );
        // Documents sort by the named field, with a missing field as null —
        // so a stray scalar element sorts first rather than failing.
        let out = applied(
            doc! { "$push": { "a": { "$each": [{ "s": 5 }, 9], "$sort": { "s": -1 } } } },
            doc! { "a": [{ "s": 7 }, { "s": 3 }] },
        );
        assert_eq!(out, doc! { "a": [{ "s": 7 }, { "s": 5 }, { "s": 3 }, 9] });
        // Several keys, dotted paths, mixed direction.
        let out = applied(
            doc! { "$push": { "a": { "$each": [], "$sort": { "g": 1, "v.n": -1 } } } },
            doc! { "a": [{ "g": 2, "v": { "n": 1 } }, { "g": 1, "v": { "n": 1 } }, { "g": 1, "v": { "n": 2 } }] },
        );
        assert_eq!(
            out,
            doc! { "a": [{ "g": 1, "v": { "n": 2 } }, { "g": 1, "v": { "n": 1 } }, { "g": 2, "v": { "n": 1 } }] }
        );
        // Sorting is by canonical order, so 2 and 2.0 tie and 10 follows 9.
        assert_eq!(
            applied(
                doc! { "$push": { "a": { "$each": [10], "$sort": 1 } } },
                doc! { "a": [9, 2.0, 2] }
            ),
            doc! { "a": [2.0, 2, 9, 10] }
        );
    }

    #[test]
    fn push_position_inserts_from_either_end() {
        let push =
            |position: i32| doc! { "$push": { "a": { "$each": [9, 8], "$position": position } } };
        let base = doc! { "a": [1, 2, 3] };
        assert_eq!(applied(push(0), base.clone()), doc! { "a": [9, 8, 1, 2, 3] });
        assert_eq!(applied(push(1), base.clone()), doc! { "a": [1, 9, 8, 2, 3] });
        assert_eq!(applied(push(-1), base.clone()), doc! { "a": [1, 2, 9, 8, 3] });
        // Beyond either end clamps rather than erroring.
        assert_eq!(applied(push(50), base.clone()), doc! { "a": [1, 2, 3, 9, 8] });
        assert_eq!(applied(push(-50), base.clone()), doc! { "a": [9, 8, 1, 2, 3] });
        // A missing field is an empty array; every position is its start.
        assert_eq!(applied(push(2), doc! {}), doc! { "a": [9, 8] });
    }

    #[test]
    fn push_modifiers_apply_as_position_then_sort_then_slice() {
        // The capped-history idiom: append, order, keep the newest three.
        let out = applied(
            doc! { "$push": { "h": {
                "$each": [{ "t": 5, "e": "e" }, { "t": 2, "e": "b" }],
                "$sort": { "t": 1 },
                "$slice": -3,
            } } },
            doc! { "h": [{ "t": 1, "e": "a" }, { "t": 3, "e": "c" }, { "t": 4, "e": "d" }] },
        );
        assert_eq!(
            out,
            doc! { "h": [{ "t": 3, "e": "c" }, { "t": 4, "e": "d" }, { "t": 5, "e": "e" }] }
        );
        // Position, then sort, then slice — the position is observable only
        // through the slice when there is no sort, and not at all with one.
        assert_eq!(
            applied(
                doc! { "$push": { "a": { "$each": [0], "$position": 0, "$slice": 2 } } },
                doc! { "a": [1, 2, 3] }
            ),
            doc! { "a": [0, 1] }
        );
        assert_eq!(
            applied(
                doc! { "$push": { "a": { "$each": [0], "$position": 0, "$sort": -1, "$slice": 2 } } },
                doc! { "a": [1, 2, 3] }
            ),
            doc! { "a": [3, 2] }
        );
        // An empty $each still sorts and slices what is there.
        assert_eq!(
            applied(
                doc! { "$push": { "a": { "$each": [], "$sort": 1, "$slice": 2 } } },
                doc! { "a": [3, 1, 2] }
            ),
            doc! { "a": [1, 2] }
        );
    }

    #[test]
    fn push_modifiers_require_each_and_reject_strangers() {
        let err = apply_err(doc! { "$push": { "a": { "$slice": -3 } } }, doc! {});
        assert!(err.contains("requires $each"), "unhelpful error: {err}");
        let err = apply_err(doc! { "$push": { "a": { "$each": 1 } } }, doc! {});
        assert!(err.contains("$each requires an array"), "unhelpful error: {err}");
        let err = apply_err(doc! { "$push": { "a": { "$each": [1], "$slise": 1 } } }, doc! {});
        assert!(err.contains("unrecognized clause \"$slise\""), "unhelpful error: {err}");
        // A plain field alongside modifiers is neither a value nor a modifier.
        let err = apply_err(doc! { "$push": { "a": { "$each": [1], "x": 1 } } }, doc! {});
        assert!(err.contains("unrecognized clause \"x\""), "unhelpful error: {err}");
        for bad in [
            doc! { "$push": { "a": { "$each": [1], "$slice": 1.5 } } },
            doc! { "$push": { "a": { "$each": [1], "$slice": "3" } } },
            doc! { "$push": { "a": { "$each": [1], "$position": true } } },
            doc! { "$push": { "a": { "$each": [1], "$sort": 2 } } },
            doc! { "$push": { "a": { "$each": [1], "$sort": {} } } },
            doc! { "$push": { "a": { "$each": [1], "$sort": { "t": "asc" } } } },
        ] {
            assert!(parse(&bad).is_err(), "accepted {bad}");
        }
        // A document without modifiers is still a value to push.
        assert_eq!(
            applied(doc! { "$push": { "a": { "x": 1 } } }, doc! { "a": [] }),
            doc! { "a": [{ "x": 1 }] }
        );
    }

    #[test]
    fn add_to_set_takes_each_and_nothing_else() {
        assert_eq!(
            applied(doc! { "$addToSet": { "a": { "$each": [2, 3] } } }, doc! { "a": [1, 2] }),
            doc! { "a": [1, 2, 3] }
        );
        let err = apply_err(doc! { "$addToSet": { "a": { "$each": [1], "$slice": 1 } } }, doc! {});
        assert!(err.contains("unrecognized clause \"$slice\""), "unhelpful error: {err}");
    }

    #[test]
    fn push_onto_a_non_array_is_an_error() {
        let err = apply_err(doc! { "$push": { "a": 1 } }, doc! { "a": "scalar" });
        assert!(err.contains("non-array"), "unhelpful error: {err}");
    }

    #[test]
    fn add_to_set_skips_duplicates() {
        assert_eq!(
            applied(doc! { "$addToSet": { "a": 2 } }, doc! { "a": [1, 2] }),
            doc! { "a": [1, 2] }
        );
        assert_eq!(
            applied(doc! { "$addToSet": { "a": 3 } }, doc! { "a": [1, 2] }),
            doc! { "a": [1, 2, 3] }
        );
        // Equality is numeric, so 2 and 2.0 are the same member.
        assert_eq!(
            applied(doc! { "$addToSet": { "a": 2.0 } }, doc! { "a": [1, 2] }),
            doc! { "a": [1, 2] }
        );
    }

    #[test]
    fn pull_removes_every_matching_element() {
        assert_eq!(
            applied(doc! { "$pull": { "a": 2 } }, doc! { "a": [1, 2, 3, 2] }),
            doc! { "a": [1, 3] }
        );
        // Pulling from a missing field is a no-op.
        assert_eq!(applied(doc! { "$pull": { "a": 2 } }, doc! { "b": 1 }), doc! { "b": 1 });
    }

    #[test]
    fn pull_all_removes_every_listed_value() {
        assert_eq!(
            applied(doc! { "$pullAll": { "a": [1, 3] } }, doc! { "a": [1, 2, 3, 1, 4, 3] }),
            doc! { "a": [2, 4] }
        );
        // Equality is canonical, so 2 removes 2.0 and a document matches whole.
        assert_eq!(
            applied(doc! { "$pullAll": { "a": [2, {"k": 1}] } }, doc! { "a": [2.0, {"k": 1}, 5] }),
            doc! { "a": [5] }
        );
        // A value that is not present changes nothing.
        assert_eq!(
            applied(doc! { "$pullAll": { "a": [9] } }, doc! { "a": [1, 2] }),
            doc! { "a": [1, 2] }
        );
    }

    #[test]
    fn pull_all_from_a_missing_field_is_a_no_op() {
        assert_eq!(applied(doc! { "$pullAll": { "a": [1] } }, doc! { "b": 1 }), doc! { "b": 1 });
    }

    #[test]
    fn pull_all_from_a_non_array_is_an_error() {
        let err = apply_err(doc! { "$pullAll": { "a": [1] } }, doc! { "a": 1 });
        assert!(err.contains("non-array"), "unhelpful error: {err}");
    }

    #[test]
    fn pull_all_requires_an_array_argument() {
        // `{$pullAll: {a: 1}}` is the shape of a `$pull`; accepting it would
        // make the two operators interchangeable by accident.
        let err = parse(&doc! { "$pullAll": { "a": 1 } }).unwrap_err().to_string();
        assert!(err.contains("array"), "unhelpful error: {err}");
    }

    #[test]
    fn pop_removes_from_either_end() {
        assert_eq!(
            applied(doc! { "$pop": { "a": 1 } }, doc! { "a": [1, 2, 3] }),
            doc! { "a": [1, 2] }
        );
        assert_eq!(
            applied(doc! { "$pop": { "a": -1 } }, doc! { "a": [1, 2, 3] }),
            doc! { "a": [2, 3] }
        );
        // Popping an empty array is a no-op rather than an error.
        assert_eq!(applied(doc! { "$pop": { "a": 1 } }, doc! { "a": [] }), doc! { "a": [] });
    }

    #[test]
    fn pop_rejects_directions_other_than_one() {
        assert!(parse(&doc! { "$pop": { "a": 2 } }).is_err());
    }

    #[test]
    fn rename_moves_a_field() {
        assert_eq!(applied(doc! { "$rename": { "a": "b" } }, doc! { "a": 1 }), doc! { "b": 1 });
        // Renaming a missing field is a no-op.
        assert_eq!(applied(doc! { "$rename": { "a": "b" } }, doc! { "c": 1 }), doc! { "c": 1 });
    }

    #[test]
    fn current_date_uses_the_supplied_timestamp() {
        let out = applied(doc! { "$currentDate": { "at": true } }, doc! {});
        assert_eq!(out.get_datetime("at").unwrap().timestamp_millis(), NOW);
    }

    #[test]
    fn several_operators_apply_together() {
        let out = applied(
            doc! { "$set": { "a": 1 }, "$inc": { "n": 1 }, "$push": { "tags": "x" } },
            doc! { "n": 5, "tags": [] },
        );
        assert_eq!(out.get_i32("a").unwrap(), 1);
        assert_eq!(out.get_i64("n").unwrap(), 6);
        assert_eq!(out.get_array("tags").unwrap().len(), 1);
    }

    // -----------------------------------------------------------------------
    // Replacement form and _id protection
    // -----------------------------------------------------------------------

    #[test]
    fn a_document_without_operators_is_a_replacement() {
        assert_eq!(
            applied(doc! { "x": 1 }, doc! { "_id": 7, "a": 1, "b": 2 }),
            doc! { "x": 1, "_id": 7 }
        );
    }

    #[test]
    fn a_replacement_preserves_the_existing_id() {
        // Even when the replacement names a different one: _id is identity,
        // not content, and a replace must not relocate the document.
        let out = applied(doc! { "_id": 999, "x": 1 }, doc! { "_id": 7, "a": 1 });
        assert_eq!(out.get_i32("_id").unwrap(), 7);
    }

    #[test]
    fn mixing_operators_and_replacement_fields_is_rejected() {
        let err = parse(&doc! { "$set": { "a": 1 }, "b": 2 }).unwrap_err().to_string();
        assert!(err.contains("cannot mix"), "unhelpful error: {err}");
    }

    #[test]
    fn operators_may_not_touch_id() {
        assert!(parse(&doc! { "$set": { "_id": 1 } }).is_err());
        assert!(parse(&doc! { "$unset": { "_id": "" } }).is_err());
        assert!(parse(&doc! { "$rename": { "a": "_id" } }).is_ok_and(|u| {
            let mut d = doc! { "_id": 1, "a": 2 };
            apply(&u, &mut d, NOW).is_err()
        }));
    }

    #[test]
    fn unknown_operators_are_rejected() {
        assert!(parse(&doc! { "$frobnicate": { "a": 1 } }).is_err());
    }

    #[test]
    fn operators_require_a_document_argument() {
        assert!(parse(&doc! { "$set": 1 }).is_err());
    }

    // -----------------------------------------------------------------------
    // Positional paths: `$[]` and `$[<identifier>]` with arrayFilters
    // -----------------------------------------------------------------------

    fn applied_with(update: Document, filters: Vec<Document>, mut doc: Document) -> Document {
        let parsed =
            parse_with_filters(&update, &filters).unwrap_or_else(|e| panic!("parse failed: {e}"));
        apply(&parsed, &mut doc, NOW).unwrap_or_else(|e| panic!("apply failed: {e}"));
        doc
    }

    fn apply_err_with(update: Document, filters: Vec<Document>, mut doc: Document) -> String {
        let parsed = match parse_with_filters(&update, &filters) {
            Ok(p) => p,
            Err(e) => return e.to_string(),
        };
        apply(&parsed, &mut doc, NOW).unwrap_err().to_string()
    }

    fn order() -> Document {
        doc! {
            "_id": 1,
            "items": [
                { "sku": "a", "qty": 1, "shipped": false },
                { "sku": "b", "qty": 5, "shipped": false },
                { "sku": "c", "qty": 9, "shipped": false },
            ]
        }
    }

    /// The `shipped` flag of each line item, in order.
    fn shipped(order: &Document) -> Vec<bool> {
        order
            .get_array("items")
            .unwrap()
            .iter()
            .map(|line| line.as_document().unwrap().get_bool("shipped").unwrap())
            .collect()
    }

    #[test]
    fn a_filtered_identifier_reaches_one_element() {
        // The line-item case the feature exists for: mark one line shipped
        // without rewriting the document.
        let out = applied_with(
            doc! { "$set": { "items.$[line].shipped": true } },
            vec![doc! { "line.sku": "b" }],
            order(),
        );
        assert_eq!(shipped(&out), vec![false, true, false]);
    }

    #[test]
    fn every_matching_element_is_updated() {
        let out = applied_with(
            doc! { "$set": { "items.$[line].shipped": true } },
            vec![doc! { "line.qty": { "$gt": 2 } }],
            order(),
        );
        assert_eq!(shipped(&out), vec![false, true, true]);
    }

    #[test]
    fn a_filter_may_combine_several_conditions_on_the_element() {
        // Two fields of the same element, as `$elemMatch` would test them.
        let out = applied_with(
            doc! { "$set": { "items.$[line].shipped": true } },
            vec![doc! { "line.qty": { "$gt": 2 }, "line.sku": "c" }],
            order(),
        );
        assert_eq!(shipped(&out), vec![false, false, true]);

        // And through `$or`, which is rewritten branch by branch.
        let out = applied_with(
            doc! { "$set": { "items.$[line].shipped": true } },
            vec![doc! { "$or": [ { "line.sku": "a" }, { "line.qty": 9 } ] }],
            order(),
        );
        assert_eq!(shipped(&out), vec![true, false, true]);
    }

    #[test]
    fn a_bare_identifier_addresses_scalar_elements() {
        assert_eq!(
            applied_with(
                doc! { "$inc": { "grades.$[g]": 10 } },
                vec![doc! { "g": { "$gte": 80 } }],
                doc! { "grades": [70, 80, 90] },
            ),
            doc! { "grades": [70, 90i64, 100i64] }
        );
        // Equality against the element itself.
        assert_eq!(
            applied_with(
                doc! { "$set": { "tags.$[t]": "B" } },
                vec![doc! { "t": "b" }],
                doc! { "tags": ["a", "b", "b"] },
            ),
            doc! { "tags": ["a", "B", "B"] }
        );
    }

    #[test]
    fn all_positional_updates_every_element() {
        assert_eq!(
            applied_with(
                doc! { "$inc": { "grades.$[]": 5 } },
                vec![],
                doc! { "grades": [1, 2, 3] },
            ),
            doc! { "grades": [6i64, 7i64, 8i64] }
        );
        let out = applied_with(doc! { "$set": { "items.$[].shipped": true } }, vec![], order());
        assert_eq!(shipped(&out), vec![true, true, true]);
    }

    #[test]
    fn no_matching_element_leaves_the_document_alone() {
        // Not an error: "ship the lines with sku z" on an order without one
        // is a legitimate no-op. The write still counts as `modified`, which
        // is the register's documented meaning of that field.
        let out = applied_with(
            doc! { "$set": { "items.$[line].shipped": true } },
            vec![doc! { "line.sku": "z" }],
            order(),
        );
        assert_eq!(out, order());
    }

    #[test]
    fn nested_identifiers_address_inner_arrays() {
        let doc = doc! {
            "orders": [
                { "id": 1, "items": [ { "sku": "a", "qty": 1 }, { "sku": "b", "qty": 2 } ] },
                { "id": 2, "items": [ { "sku": "a", "qty": 3 }, { "sku": "b", "qty": 4 } ] },
            ]
        };
        let out = applied_with(
            doc! { "$set": { "orders.$[o].items.$[i].qty": 0 } },
            vec![doc! { "o.id": 2 }, doc! { "i.sku": "b" }],
            doc,
        );
        assert_eq!(
            out,
            doc! {
                "orders": [
                    { "id": 1, "items": [ { "sku": "a", "qty": 1 }, { "sku": "b", "qty": 2 } ] },
                    { "id": 2, "items": [ { "sku": "a", "qty": 3 }, { "sku": "b", "qty": 0 } ] },
                ]
            }
        );
    }

    #[test]
    fn inc_mul_min_max_and_current_date_work_through_a_positional_path() {
        let out = applied_with(
            doc! {
                "$inc": { "items.$[line].qty": 1 },
                "$mul": { "items.$[line].price": 2 },
                "$max": { "items.$[line].seen": 7 },
                "$currentDate": { "items.$[line].at": true },
            },
            vec![doc! { "line.sku": "b" }],
            doc! { "items": [ { "sku": "a", "qty": 1 }, { "sku": "b", "qty": 5, "price": 3 } ] },
        );
        let line = out.get_array("items").unwrap()[1].as_document().unwrap();
        assert_eq!(line.get_i64("qty").unwrap(), 6);
        assert_eq!(line.get_i64("price").unwrap(), 6);
        assert_eq!(line.get_i32("seen").unwrap(), 7);
        assert_eq!(line.get_datetime("at").unwrap().timestamp_millis(), NOW);
        // The unselected line is exactly as it was.
        assert_eq!(
            out.get_array("items").unwrap()[0],
            Bson::Document(doc! { "sku": "a", "qty": 1 })
        );
    }

    #[test]
    fn unset_of_a_matched_element_leaves_a_null_hole() {
        // Compacting would renumber the elements after it, so an element that
        // is unset becomes null, as in MongoDB.
        assert_eq!(
            applied_with(
                doc! { "$unset": { "grades.$[g]": "" } },
                vec![doc! { "g": { "$lt": 60 } }],
                doc! { "grades": [90, 50, 70] },
            ),
            doc! { "grades": [90, Bson::Null, 70] }
        );
        // Unsetting a field *of* a matched element removes the field.
        let out = applied_with(
            doc! { "$unset": { "items.$[line].shipped": "" } },
            vec![doc! { "line.sku": "a" }],
            order(),
        );
        assert_eq!(
            out.get_array("items").unwrap()[0],
            Bson::Document(doc! { "sku": "a", "qty": 1 })
        );
    }

    #[test]
    fn array_operators_apply_inside_a_matched_subdocument() {
        let doc = doc! {
            "items": [
                { "sku": "a", "tags": ["x", "y"] },
                { "sku": "b", "tags": ["x", "z"] },
            ]
        };
        let out = applied_with(
            doc! {
                "$pull": { "items.$[line].tags": "x" },
                "$push": { "items.$[line].tags": "w" },
                "$addToSet": { "items.$[line].tags": "z" },
            },
            vec![doc! { "line.sku": "b" }],
            doc,
        );
        assert_eq!(
            out,
            doc! {
                "items": [
                    { "sku": "a", "tags": ["x", "y"] },
                    { "sku": "b", "tags": ["z", "w"] },
                ]
            }
        );
        // `$pop` and `$push` with `$each` go through the same path.
        let out = applied_with(
            doc! { "$pop": { "items.$[].tags": -1 } },
            vec![],
            doc! { "items": [ { "tags": [1, 2] }, { "tags": [3, 4] } ] },
        );
        assert_eq!(out, doc! { "items": [ { "tags": [2] }, { "tags": [4] } ] });
    }

    #[test]
    fn a_positional_segment_needs_an_array_to_select_from() {
        let err = apply_err_with(
            doc! { "$set": { "items.$[line].shipped": true } },
            vec![doc! { "line.sku": "a" }],
            doc! { "items": "not an array" },
        );
        assert!(err.contains("must be an array"), "unhelpful error: {err}");

        let err = apply_err_with(
            doc! { "$set": { "items.$[].shipped": true } },
            vec![],
            doc! { "other": 1 },
        );
        assert!(err.contains("must exist"), "unhelpful error: {err}");

        // The same rule one level down: an element without the inner array.
        let err = apply_err_with(
            doc! { "$set": { "orders.$[].items.$[].qty": 0 } },
            vec![],
            doc! { "orders": [ { "items": [ { "qty": 1 } ] }, { "id": 2 } ] },
        );
        assert!(err.contains("\"orders.1.items\" must exist"), "unhelpful error: {err}");
    }

    #[test]
    fn an_identifier_without_a_filter_is_rejected() {
        let err =
            apply_err_with(doc! { "$set": { "items.$[line].shipped": true } }, vec![], order());
        assert!(err.contains("no filter for it"), "unhelpful error: {err}");
    }

    #[test]
    fn a_filter_without_an_identifier_is_rejected() {
        let err = apply_err_with(
            doc! { "$set": { "items.$[line].shipped": true } },
            vec![doc! { "line.sku": "a" }, doc! { "other.sku": "b" }],
            order(),
        );
        assert!(err.contains("no update path uses"), "unhelpful error: {err}");
        // Including on a path without positional segments at all.
        let err = apply_err_with(doc! { "$set": { "n": 1 } }, vec![doc! { "x.y": 1 }], order());
        assert!(err.contains("no update path uses"), "unhelpful error: {err}");
    }

    #[test]
    fn a_filter_document_names_exactly_one_identifier() {
        let err = apply_err_with(
            doc! { "$set": { "items.$[a].shipped": true, "items.$[b].shipped": true } },
            vec![doc! { "a.sku": "a", "b.sku": "b" }],
            order(),
        );
        assert!(err.contains("names both"), "unhelpful error: {err}");

        let err = apply_err_with(
            doc! { "$set": { "items.$[a].shipped": true } },
            vec![doc! { "a.sku": "a" }, doc! { "a.sku": "b" }],
            order(),
        );
        assert!(err.contains("more than one filter"), "unhelpful error: {err}");

        let err = apply_err_with(
            doc! { "$set": { "items.$[a].shipped": true } },
            vec![doc! { "$and": [] }],
            order(),
        );
        assert!(err.contains("must name an identifier"), "unhelpful error: {err}");
    }

    #[test]
    fn identifiers_follow_the_naming_rule() {
        for bad in ["Line", "1st", "a-b"] {
            let path = format!("items.$[{bad}].shipped");
            let err = apply_err_with(doc! { "$set": { path: true } }, vec![], order());
            assert!(err.contains("lowercase letter"), "{bad:?}: unhelpful error: {err}");
        }
        let err = apply_err_with(doc! { "$set": { "items.$[line": true } }, vec![], order());
        assert!(err.contains("malformed"), "unhelpful error: {err}");
    }

    #[test]
    fn a_path_cannot_begin_with_a_positional_segment() {
        let err = apply_err_with(doc! { "$set": { "$[].x": 1 } }, vec![], order());
        assert!(err.contains("not an array"), "unhelpful error: {err}");
    }

    #[test]
    fn the_dollar_positional_operator_is_refused_with_a_pointer() {
        let err = apply_err_with(doc! { "$set": { "items.$.shipped": true } }, vec![], order());
        assert!(err.contains("$[<identifier>]"), "unhelpful error: {err}");
    }

    #[test]
    fn rename_refuses_positional_paths() {
        let err = apply_err_with(doc! { "$rename": { "items.$[].sku": "code" } }, vec![], order());
        assert!(err.contains("$rename"), "unhelpful error: {err}");
        let err = apply_err_with(doc! { "$rename": { "a": "items.$[].a" } }, vec![], order());
        assert!(err.contains("$rename"), "unhelpful error: {err}");
    }

    #[test]
    fn array_filters_do_not_apply_to_a_replacement() {
        let err = apply_err_with(doc! { "x": 1 }, vec![doc! { "a.b": 1 }], order());
        assert!(err.contains("replacement"), "unhelpful error: {err}");
    }

    #[test]
    fn only_logical_operators_are_accepted_at_the_top_of_a_filter() {
        let err = apply_err_with(
            doc! { "$set": { "items.$[line].shipped": true } },
            vec![doc! { "$where": "1" }],
            order(),
        );
        assert!(err.contains("$where"), "unhelpful error: {err}");
    }

    #[test]
    fn a_dotted_condition_on_a_scalar_element_sees_an_absent_field() {
        // `{"e.x": {$exists: false}}` selects scalars, which have no `x`;
        // `{"e.x": {$gt: 0}}` selects nothing among them.
        assert_eq!(
            applied_with(
                doc! { "$set": { "mixed.$[e]": 0 } },
                vec![doc! { "e.x": { "$exists": false } }],
                doc! { "mixed": [ 7, { "x": 1 } ] },
            ),
            doc! { "mixed": [ 0, { "x": 1 } ] }
        );
        assert_eq!(
            applied_with(
                doc! { "$set": { "mixed.$[e]": 0 } },
                vec![doc! { "e.x": { "$gt": 0 } }],
                doc! { "mixed": [ 7, { "x": 1 } ] },
            ),
            doc! { "mixed": [ 7, 0 ] }
        );
    }

    /// `$expr` reached the filter language (ADR-106) after array filters were
    /// written, so it is worth saying which of the two won: an arrayFilters
    /// entry still takes field conditions and `$and`/`$or`/`$nor` and nothing
    /// else, at the top or inside a branch. MongoDB allows `$expr` here; that
    /// is recorded in `docs/deviations.md` rather than quietly half-supported.
    #[test]
    fn an_array_filter_does_not_take_expr() {
        let update = doc! { "$set": { "items.$[line].qty": 0 } };
        let err = parse_with_filters(&update, &[doc! { "$expr": { "$gt": ["$line.qty", 5] } }])
            .expect_err("$expr is not an array-filter condition");
        assert!(err.to_string().contains("$expr"), "the error should name it: {err}");
        assert!(
            parse_with_filters(
                &update,
                &[doc! { "$or": [ { "$expr": { "$gt": ["$line.qty", 5] } } ] }]
            )
            .is_err(),
            "a branch is not a way around the restriction"
        );
    }

    // -----------------------------------------------------------------------
    // $setOnInsert
    // -----------------------------------------------------------------------

    fn inserted(update: Document, mut seed: Document) -> Document {
        let parsed = parse(&update).unwrap_or_else(|e| panic!("parse failed: {e}"));
        apply_on_insert(&parsed, &mut seed, NOW).unwrap_or_else(|e| panic!("apply failed: {e}"));
        seed
    }

    #[test]
    fn set_on_insert_writes_only_the_inserted_document() {
        let update = doc! { "$setOnInsert": { "created_at": 100 }, "$inc": { "n": 1 } };
        // The upsert path: the filter's seed, then $setOnInsert, then the rest.
        assert_eq!(
            inserted(update.clone(), doc! { "_id": "hits" }),
            doc! { "_id": "hits", "created_at": 100, "n": 1i64 }
        );
        // The match path: the field is left exactly as it was — present or not.
        assert_eq!(
            applied(update.clone(), doc! { "_id": "hits", "created_at": 1, "n": 1 }),
            doc! { "_id": "hits", "created_at": 1, "n": 2i64 }
        );
        assert_eq!(applied(update, doc! { "n": 1 }), doc! { "n": 2i64 });
        // Alone, on an existing document, it is a no-op rather than an error.
        assert_eq!(applied(doc! { "$setOnInsert": { "a": 1 } }, doc! { "b": 2 }), doc! { "b": 2 });
    }

    #[test]
    fn set_on_insert_reaches_nested_and_dotted_paths() {
        assert_eq!(
            inserted(doc! { "$setOnInsert": { "meta.created": 1, "tags": ["new"] } }, doc! {}),
            doc! { "meta": { "created": 1 }, "tags": ["new"] }
        );
        // Nested into a seeded subdocument rather than replacing it.
        assert_eq!(
            inserted(doc! { "$setOnInsert": { "meta.created": 1 } }, doc! { "meta": { "k": 0 } }),
            doc! { "meta": { "k": 0, "created": 1 } }
        );
        assert_eq!(
            applied(doc! { "$setOnInsert": { "meta.created": 1 } }, doc! { "meta": { "k": 0 } }),
            doc! { "meta": { "k": 0 } }
        );
    }

    #[test]
    fn set_on_insert_runs_before_the_other_operators() {
        // $inc sees the value $setOnInsert put there, on insert only.
        assert_eq!(
            inserted(doc! { "$setOnInsert": { "n": 10 }, "$inc": { "m": 1 } }, doc! {}),
            doc! { "n": 10, "m": 1i64 }
        );
    }

    #[test]
    fn set_on_insert_may_not_share_a_path_with_another_operator() {
        for bad in [
            doc! { "$setOnInsert": { "a": 1 }, "$set": { "a": 2 } },
            doc! { "$set": { "a": 2 }, "$setOnInsert": { "a": 1 } },
            doc! { "$setOnInsert": { "a": 1 }, "$inc": { "a": 2 } },
            doc! { "$setOnInsert": { "a": 1 }, "$unset": { "a": "" } },
            // A prefix in either direction is the same field.
            doc! { "$setOnInsert": { "a": { "b": 1 } }, "$set": { "a.b": 2 } },
            doc! { "$setOnInsert": { "a.b": 1 }, "$set": { "a": {} } },
            doc! { "$setOnInsert": { "a": 1, "a.b": 2 } },
            // A rename writes its destination.
            doc! { "$setOnInsert": { "b": 1 }, "$rename": { "a": "b" } },
        ] {
            let err = parse(&bad).unwrap_err().to_string();
            assert!(err.contains("conflicts"), "{bad}: {err}");
        }
        // Sharing a prefix of the *name* is not sharing a path.
        assert!(parse(&doc! { "$setOnInsert": { "ab": 1 }, "$set": { "a": 2 } }).is_ok());
        assert!(parse(&doc! { "$setOnInsert": { "a.b": 1 }, "$set": { "a.c": 2 } }).is_ok());
    }

    #[test]
    fn set_on_insert_may_not_touch_id() {
        assert!(parse(&doc! { "$setOnInsert": { "_id": 1 } }).is_err());
    }
}
