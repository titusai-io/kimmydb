//! Update operators.
//!
//! An update document is either a set of `$`-prefixed operators or a whole
//! replacement document — never a mix, because the two mean very different
//! things and guessing would silently discard fields.

use bson::{Bson, Document};
use kimmy_core::cmp::canonical_cmp;
use kimmy_core::{Error, Result};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};

use crate::aggregate::group_key;
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
    /// Refused, rather than sorted, when an element holds a `Decimal128`
    /// where the order would read it: the canonical order ranks one equal to
    /// every other number, so it has no position among them to be given.
    fn sort(&self, items: &mut [Bson]) -> Result<()> {
        match self {
            ArraySort::Whole { descending } => {
                if items.iter().any(kimmy_core::holds_decimal128) {
                    return Err(Error::InvalidUpdate(
                        "$push $sort cannot order an element holding a Decimal128, which cannot \
                         be compared: it has no exact key encoding in this engine and ranks equal \
                         to every other number; store a double or a long instead"
                            .into(),
                    ));
                }
                items.sort_by(|a, b| {
                    let ordering = canonical_cmp(a, b);
                    if *descending { ordering.reverse() } else { ordering }
                });
            }
            ArraySort::ByFields(keys) => {
                let empty = Document::new();
                fn as_document<'a>(value: &'a Bson, empty: &'a Document) -> &'a Document {
                    match value {
                        Bson::Document(doc) => doc,
                        _ => empty,
                    }
                }
                for item in items.iter() {
                    if let Some(why) = shape::unsortable(keys, as_document(item, &empty)) {
                        return Err(Error::InvalidUpdate(format!("$push $sort {why}")));
                    }
                }
                items.sort_by(|a, b| {
                    shape::compare(keys, as_document(a, &empty), as_document(b, &empty))
                });
            }
        }
        Ok(())
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
            if touches_id(target_path) {
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

    reject_conflicting_paths(&operations)?;
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
///
/// Their `$regex` conditions share one budget, the one a single filter has:
/// every entry is held for the whole update.
fn parse_array_filters(docs: &[Document]) -> Result<Vec<ArrayFilter>> {
    let _budget = filter::RegexBudget::open();
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

/// Every path an operation writes: its own, and for a `$rename` its
/// destination as well, since a rename clears one field and writes another.
fn written_paths(op: &Operation) -> Vec<&str> {
    match &op.kind {
        OpKind::Rename(target) => vec![op.path.as_str(), target.as_str()],
        _ => vec![op.path.as_str()],
    }
}

/// Refuse an update in which two writes share a path, or one writes inside
/// the other (ADR-205).
///
/// Applied one after the other, the two would leave a result that depends on
/// which ran first, and the order is the order of the keys in the request
/// body, which a client's JSON encoder may not preserve. A request whose
/// meaning depends on an accident of encoding is one the server cannot
/// honour, so it is refused at parse time rather than answered. Every pair of
/// operations is checked, including two paths under one operator, and the
/// error names the two in the order they were written.
///
/// A positional segment is compared as written: `items.$[a].qty` and
/// `items.$[b].qty` do not overlap here, because which elements each reaches
/// depends on the document. [`apply_to`] checks the concrete paths they
/// expand to, in each document. An index segment is compared by its value, so
/// `a.1` and `a.01` are one path.
fn reject_conflicting_paths(operations: &[Operation]) -> Result<()> {
    let mut claims = Claims::new(operations, "");
    for (i, op) in operations.iter().enumerate() {
        for path in written_paths(op) {
            claims.claim(i, path)?;
        }
    }
    Ok(())
}

/// The paths an update's operations write, each with the operation that
/// writes it, for finding two that overlap in time proportional to the paths
/// and their depth rather than to every pair of them.
///
/// Paths are claimed in operation order. Each is looked up as a path another
/// operation wrote, as a path that lies inside one another operation wrote
/// (through `inner`, every proper prefix of every path claimed), and through
/// each of its own proper prefixes, as lying inside one. Two paths of the same
/// operation are not compared: a positional operation's paths reach distinct
/// elements, and a `$rename` whose source and destination overlap is not this
/// rule's business.
struct Claims<'a> {
    operations: &'a [Operation],
    /// How the refusal says where the conflict was found.
    where_: &'static str,
    /// Each canonical path claimed, and the operation that claimed it first.
    exact: HashMap<String, usize>,
    /// Each proper prefix of a canonical path claimed, with the first
    /// operation and path that had it.
    inner: HashMap<String, (usize, String)>,
    /// Map lookups made, which a test holds to a bound.
    probes: usize,
}

impl<'a> Claims<'a> {
    fn new(operations: &'a [Operation], where_: &'static str) -> Self {
        Claims { operations, where_, exact: HashMap::new(), inner: HashMap::new(), probes: 0 }
    }

    /// Refuse operation `i` writing `path` when another operation wrote it, a
    /// path inside it, or a path it lies inside; otherwise record it.
    fn claim(&mut self, i: usize, path: &str) -> Result<()> {
        let path = canonical(path);
        let prefixes: Vec<&str> = path.match_indices('.').map(|(at, _)| &path[..at]).collect();
        self.probes += 2 + prefixes.len();
        if let Some(&j) = self.exact.get(&path)
            && j != i
        {
            return Err(self.conflict(j, i, &path, &path));
        }
        if let Some((j, longer)) = self.inner.get(&path)
            && *j != i
        {
            return Err(self.conflict(*j, i, longer, &path));
        }
        for prefix in &prefixes {
            if let Some(&j) = self.exact.get(*prefix)
                && j != i
            {
                return Err(self.conflict(j, i, prefix, &path));
            }
        }
        for prefix in prefixes {
            self.inner.entry(prefix.to_string()).or_insert_with(|| (i, path.clone()));
        }
        self.exact.entry(path).or_insert(i);
        Ok(())
    }

    /// The refusal for operation `earlier`'s write at `theirs` and operation
    /// `later`'s at `mine`, which overlap.
    fn conflict(&self, earlier: usize, later: usize, theirs: &str, mine: &str) -> Error {
        conflict(&self.operations[earlier], &self.operations[later], theirs, mine, self.where_)
    }
}

/// A path with every segment that addresses an array element by number in
/// its decimal form: `a.01` and `a.+1` are `a.1`, because the path language
/// reads all three as the element at index 1 (`usize::from_str`), and two
/// writes to it are one path written twice. A positional segment is left as
/// it is. Over a document whose `a` holds an object, `01` and `1` would be
/// two keys; they are still read as one path here, so such a pair is refused
/// rather than told apart. The first segment is left as written: it names a
/// field of the document itself, which is never an array, so `01` and `1`
/// there are always two fields.
fn canonical(path: &str) -> String {
    path::segments(path)
        .iter()
        .enumerate()
        .map(|(at, segment)| match segment.parse::<usize>() {
            Ok(index) if at > 0 => index.to_string(),
            _ => (*segment).to_string(),
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// The refusal for two writes that overlap at `first` and `second`, which
/// name the field the conflict is at and the path inside it, in either order.
fn conflict(
    earlier: &Operation,
    later: &Operation,
    first: &str,
    second: &str,
    where_: &str,
) -> Error {
    let (at, updating) =
        if first.len() <= second.len() { (first, second) } else { (second, first) };
    Error::InvalidUpdate(format!(
        "{} on {:?} conflicts with {} on {:?}{where_}: updating the path {updating:?} would \
         create a conflict at {at:?}; an update may write each path once",
        earlier.kind.name(),
        earlier.path,
        later.kind.name(),
        later.path,
    ))
}

/// Whether a target path is `_id` or anything beneath it.
///
/// The exact match alone was not enough. `_id` holds a scalar in almost every
/// document, and `path::set` on `"_id.x"` replaces a scalar with a document to
/// make room — so `{$set: {"_id.x": 1}}` turned `_id: 7` into `_id: {x: 1}`,
/// past a check that only knew the literal name. That relocates the document
/// out from under every index entry and oplog record that named it, silently.
/// Found by the fuzz harness's `_id`-is-preserved assertion (ADR-111).
fn touches_id(path: &str) -> bool {
    path == ID_FIELD || path.strip_prefix(ID_FIELD).is_some_and(|rest| rest.starts_with('.'))
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
        // These four compare their operand against what is stored — `$min`
        // and `$max` against the value, `$pull` and `$pullAll` against every
        // element — and the canonical order ranks a Decimal128 equal to every
        // other number. Let through, a `$pull` of one would empty an array of
        // its numbers, silently. `$set` and `$push` take any value; these only
        // what they can compare. Checked over the whole operand, so a
        // Decimal128 inside a condition document counts.
        //
        // `$addToSet` is not among them: a set's members are identified by
        // their group key, with a Decimal128 by its bytes, so no comparison
        // decides membership and a Decimal128 operand is well defined
        // (ADR-186).
        "min" | "max" | "pull" | "pullAll" if kimmy_core::holds_decimal128(arg) => {
            return Err(Error::InvalidUpdate(format!(
                "${op} cannot compare a Decimal128 operand: it has no exact key encoding in this \
                 engine and ranks equal to every other number; use $set, or a double or a long"
            )));
        }
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
        other => {
            return Err(Error::UnsupportedOperator { operator: format!("${other}"), reason: None });
        }
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

    // Two phases. Every operation's positional paths are expanded against
    // the document as it was before the update, and every concrete path is
    // claimed, before anything is written: which elements an array filter
    // selects cannot depend on what an operation written before it did, and
    // an update refused for two writes that meet leaves the document as it
    // found it (ADR-205).
    let applies =
        |op: &Operation| target == Target::Inserted || !matches!(op.kind, OpKind::SetOnInsert(_));
    let concrete = concrete_paths(operations, doc, applies)?;
    if operations.iter().any(|op| has_positional(&op.path)) {
        claim_concrete(operations, &concrete)?;
    }

    // Then `$setOnInsert`, only for an inserted document, and everything else.
    let on_insert = |op: &Operation| matches!(op.kind, OpKind::SetOnInsert(_));
    let passes: [&dyn Fn(&Operation) -> bool; 2] = [&on_insert, &|op| !on_insert(op)];
    for pass in passes {
        for (op, paths) in operations.iter().zip(&concrete) {
            if applies(op) && pass(op) {
                apply_at(op, paths, doc, now_ms)?;
            }
        }
    }
    Ok(())
}

/// Each operation's concrete paths in `doc`, in operation order: its own path
/// when it has no positional segment, the paths its positional segments
/// select otherwise, and none for an operation that does not apply.
fn concrete_paths(
    operations: &[Operation],
    doc: &Document,
    applies: impl Fn(&Operation) -> bool,
) -> Result<Vec<Vec<String>>> {
    operations
        .iter()
        .map(|op| {
            if !applies(op) {
                Ok(Vec::new())
            } else if has_positional(&op.path) {
                expand_positional(&op.path, &op.array_filters, doc)
            } else {
                Ok(vec![op.path.clone()])
            }
        })
        .collect()
}

/// Refuse the update when two operations' concrete paths overlap.
///
/// The parser has refused every pair whose paths overlap as written. What it
/// cannot see is two positional paths, or a positional and a plain one, that
/// reach the same element: `items.$[a].qty` and `items.$[].qty` both reach
/// every element `a` selects, and `items.1.qty` and `items.$[].qty` meet at
/// the second. Run only when some operation has a positional path; returns
/// the lookups made.
fn claim_concrete(operations: &[Operation], concrete: &[Vec<String>]) -> Result<usize> {
    let mut claims = Claims::new(operations, " in this document");
    for (i, (op, paths)) in operations.iter().zip(concrete).enumerate() {
        for path in paths {
            claims.claim(i, path)?;
        }
        if let OpKind::Rename(target) = &op.kind
            && !paths.is_empty()
        {
            claims.claim(i, target)?;
        }
    }
    Ok(claims.probes)
}

/// One operation, applied through the concrete paths phase one found for it.
///
/// Positions stay valid across those applications because `$unset` leaves a
/// null hole rather than shifting the elements after it.
fn apply_at(op: &Operation, paths: &[String], doc: &mut Document, now_ms: i64) -> Result<()> {
    if !has_positional(&op.path) {
        return apply_one(op, doc, now_ms);
    }
    for path in paths {
        let at = Operation { path: path.clone(), kind: op.kind.clone(), array_filters: Vec::new() };
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
    expand_into(doc.get(*head), rest, filters, path, &mut prefix, &mut out, doc.get(ID_FIELD))?;
    Ok(out)
}

fn expand_into(
    value: Option<&Bson>,
    segments: &[&str],
    filters: &[ArrayFilter],
    path: &str,
    prefix: &mut Vec<String>,
    out: &mut Vec<String>,
    // The `_id` of the document being updated, for the message when an
    // array filter's `$expr` cannot be evaluated.
    id: Option<&Bson>,
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
            let selected = match selector {
                None => true,
                Some(f) => filter::matches_element(f, item, id)?,
            };
            if selected {
                prefix.push(index.to_string());
                expand_into(Some(item), rest, filters, path, prefix, out, id)?;
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
    expand_into(next, rest, filters, path, prefix, out, id)?;
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
                sort.sort(&mut items)?;
            }
            if let Some(n) = each.slice {
                slice(&mut items, n);
            }
            set(doc, Bson::Array(items))?;
        }

        OpKind::AddToSet(values) => {
            let mut items = as_array(&current, &op.path)?;
            // Members are identified as `$group` identifies a bucket, not by
            // the canonical order: a stored `Decimal128` ranks equal to every
            // number there, so it would swallow the add (ADR-186).
            let mut keys: HashSet<Vec<u8>> = items.iter().map(group_key).collect();
            for value in values {
                if keys.insert(group_key(value)) {
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
            if touches_id(target) {
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
    fn the_array_filters_of_one_update_hold_their_regex_patterns_under_one_budget() {
        // Three entries of one `\w{15}` each: about 2.4 MiB held for the whole
        // update, past the 2 MiB one filter may hold; two fit.
        let entry = |id: &str| doc! { format!("{id}.s"): { "$regex": format!(r"\w{{15}}{id}") } };
        let two = doc! { "$set": { "a.$[x].s": 1, "b.$[y].s": 1 } };
        parse_with_filters(&two, &[entry("x"), entry("y")]).unwrap();
        let three = doc! { "$set": { "a.$[x].s": 1, "b.$[y].s": 1, "c.$[z].s": 1 } };
        let err = parse_with_filters(&three, &[entry("x"), entry("y"), entry("z")])
            .unwrap_err()
            .to_string();
        assert!(err.contains("more than 2 MiB together"), "{err}");
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
    fn operators_may_not_reach_under_id_either() {
        // `_id.x` is not `_id`, so the exact check let it through — and
        // `path::set` replaces a scalar `_id` with a document to make room for
        // `x`, which changes the document's identity. Every operator goes
        // through the same check, so `$unset` and `$rename` are pinned too.
        assert!(parse(&doc! { "$set": { "_id.x": 1 } }).is_err());
        assert!(parse(&doc! { "$unset": { "_id.x": "" } }).is_err());
        assert!(parse(&doc! { "$inc": { "_id.0": 1 } }).is_err());
        assert!(parse(&doc! { "$rename": { "a": "_id.x" } }).is_ok_and(|u| {
            let mut d = doc! { "_id": 1, "a": 2 };
            apply(&u, &mut d, NOW).is_err()
        }));
        // A field that merely starts with the letters is an ordinary field.
        assert!(parse(&doc! { "$set": { "_identity": 1 } }).is_ok());
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
        let filters = || vec![doc! { "line.sku": "b" }];
        let out = applied_with(doc! { "$pull": { "items.$[line].tags": "x" } }, filters(), doc);
        let out = applied_with(doc! { "$push": { "items.$[line].tags": "w" } }, filters(), out);
        let out = applied_with(doc! { "$addToSet": { "items.$[line].tags": "z" } }, filters(), out);
        assert_eq!(
            out,
            doc! {
                "items": [
                    { "sku": "a", "tags": ["x", "y"] },
                    { "sku": "b", "tags": ["z", "w"] },
                ]
            }
        );
        // The three in one update write one path three times, and are refused.
        let err = parse_with_filters(
            &doc! {
                "$pull": { "items.$[line].tags": "x" },
                "$push": { "items.$[line].tags": "w" },
            },
            &filters(),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("would create a conflict"), "{err}");
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

    /// The refusal for an update whose paths overlap, as the parser gives it.
    fn conflict_err(update: Document) -> String {
        match parse(&update) {
            Ok(parsed) => panic!("{update} parsed as {parsed:?}, expected a conflict"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn two_operators_may_not_write_one_path_in_either_order() {
        // Applied in the order written, each pair below left one result for
        // one key order and another for the other: `$set` then `$inc` on
        // `a: 0` left 6, the reverse left 1.
        for (first, second) in [("$set", "$inc"), ("$set", "$mul"), ("$min", "$max")] {
            for (a, b) in [(first, second), (second, first)] {
                let update = doc! { a: { "a": 1 }, b: { "a": 5 } };
                let err = conflict_err(update.clone());
                assert_eq!(
                    err,
                    format!(
                        "invalid update: {a} on \"a\" conflicts with {b} on \"a\": updating the \
                         path \"a\" would create a conflict at \"a\"; an update may write each \
                         path once"
                    ),
                    "{update}"
                );
            }
        }
        let err = conflict_err(doc! { "$push": { "tags": "x" }, "$pull": { "tags": "y" } });
        assert!(err.contains("$push on \"tags\" conflicts with $pull on \"tags\""), "{err}");
        let err = conflict_err(doc! { "$currentDate": { "t": true }, "$unset": { "t": "" } });
        assert!(err.contains("$currentDate") && err.contains("$unset"), "{err}");
    }

    #[test]
    fn a_path_inside_another_written_path_is_a_conflict() {
        let err = conflict_err(doc! { "$set": { "a": 1 }, "$unset": { "a.b": "" } });
        assert!(
            err.contains("$set on \"a\" conflicts with $unset on \"a.b\"")
                && err.contains("updating the path \"a.b\" would create a conflict at \"a\""),
            "{err}"
        );
        let err = conflict_err(doc! { "$inc": { "a.b": 1 }, "$set": { "a": {} } });
        assert!(
            err.contains("updating the path \"a.b\" would create a conflict at \"a\""),
            "{err}"
        );
        // Two paths under one operator are two writes too.
        let err = conflict_err(doc! { "$set": { "a": { "b": 1 }, "a.b": 2 } });
        assert!(err.contains("$set on \"a\" conflicts with $set on \"a.b\""), "{err}");
    }

    #[test]
    fn a_rename_writes_both_its_source_and_its_destination() {
        let err = conflict_err(doc! { "$rename": { "a": "b" }, "$set": { "b": 1 } });
        assert!(err.contains("$rename on \"a\" conflicts with $set on \"b\""), "{err}");
        let err = conflict_err(doc! { "$set": { "a.x": 1 }, "$rename": { "a": "b" } });
        assert!(err.contains("$set on \"a.x\" conflicts with $rename on \"a\""), "{err}");
        let err = conflict_err(doc! { "$rename": { "a": "c", "b": "c.d" } });
        assert!(err.contains("$rename on \"a\" conflicts with $rename on \"b\""), "{err}");
    }

    #[test]
    fn a_rename_whose_source_lies_under_its_destination_still_meets_an_earlier_write() {
        // The `$set` claims `a.b` first; the rename's own source `a.c` must not
        // take the place of that claim inside `a`, or its destination `a`
        // would no longer meet `a.b`.
        let err = conflict_err(doc! { "$set": { "a.b": 1 }, "$rename": { "a.c": "a" } });
        assert!(err.contains("$set on \"a.b\" conflicts with $rename"), "{err}");
        assert!(err.contains("would create a conflict at \"a\""), "{err}");
    }

    #[test]
    fn a_rename_that_overlaps_only_its_own_path_is_not_a_conflict() {
        for spec in [
            doc! { "$rename": { "a": "a.b" } },
            doc! { "$rename": { "a.b": "a" } },
            doc! { "$rename": { "a": "a" } },
        ] {
            assert!(parse_with_filters(&spec, &[]).is_ok(), "{spec}");
        }
    }

    #[test]
    fn a_numeric_top_level_field_is_compared_as_written() {
        // The document itself is never an array, so `01` and `1` are two fields.
        assert_eq!(
            applied(doc! { "$set": { "01": 1, "1": 2 } }, doc! {}),
            doc! { "01": 1, "1": 2 }
        );
        let err = conflict_err(doc! { "$set": { "a.01": 1, "a.1": 2 } });
        assert!(err.contains("would create a conflict at \"a.1\""), "{err}");
    }

    #[test]
    fn writes_to_separate_paths_still_apply_together() {
        assert_eq!(
            applied(doc! { "$set": { "a": 1 }, "$inc": { "b": 5 } }, doc! { "a": 0, "b": 1 }),
            doc! { "a": 1, "b": 6i64 }
        );
        assert_eq!(
            applied(
                doc! { "$set": { "a.b": 1 }, "$unset": { "a.c": "" } },
                doc! { "a": { "c": 2 } }
            ),
            doc! { "a": { "b": 1 } }
        );
        // A shared prefix of the name is not a shared path.
        assert_eq!(
            applied(doc! { "$set": { "ab": 1 }, "$inc": { "a": 1 } }, doc! {}),
            doc! { "ab": 1, "a": 1i64 }
        );
        assert_eq!(
            applied(doc! { "$rename": { "a": "b" }, "$set": { "c": 1 } }, doc! { "a": 7 }),
            doc! { "b": 7, "c": 1 }
        );
    }

    #[test]
    fn positional_paths_conflict_at_parse_when_they_overlap_as_written() {
        let filters = vec![doc! { "l.sku": "b" }];
        for update in [
            doc! { "$set": { "items.$[l].qty": 0 }, "$inc": { "items.$[l].qty": 1 } },
            doc! { "$set": { "items.$[l]": {} }, "$inc": { "items.$[l].qty": 1 } },
            doc! { "$set": { "items": [] }, "$inc": { "items.$[l].qty": 1 } },
        ] {
            let err = parse_with_filters(&update, &filters).unwrap_err().to_string();
            assert!(err.contains("would create a conflict"), "{update}: {err}");
        }
        let err = parse(&doc! { "$set": { "items.$[]": 0 }, "$unset": { "items.$[].qty": "" } })
            .unwrap_err()
            .to_string();
        assert!(err.contains("would create a conflict at \"items.$[]\""), "{err}");
    }

    #[test]
    fn positional_paths_that_reach_one_element_conflict_in_that_document() {
        // `l` selects the second line, which `$[]` reaches too.
        let err = apply_err_with(
            doc! { "$set": { "items.$[l].qty": 0 }, "$inc": { "items.$[].qty": 1 } },
            vec![doc! { "l.sku": "b" }],
            order(),
        );
        assert_eq!(
            err,
            "invalid update: $set on \"items.$[l].qty\" conflicts with $inc on \
             \"items.$[].qty\" in this document: updating the path \"items.1.qty\" would \
             create a conflict at \"items.1.qty\"; an update may write each path once"
        );
        // Written the other way round, the refusal names them that way round.
        let err = apply_err_with(
            doc! { "$inc": { "items.$[].qty": 1 }, "$set": { "items.$[l].qty": 0 } },
            vec![doc! { "l.sku": "b" }],
            order(),
        );
        assert!(err.contains("$inc on \"items.$[].qty\" conflicts with $set"), "{err}");
        // A plain index path meets a positional one at that element.
        let err = apply_err_with(
            doc! { "$set": { "items.2.qty": 0 }, "$inc": { "items.$[].qty": 1 } },
            vec![],
            order(),
        );
        assert!(err.contains("would create a conflict at \"items.2.qty\""), "{err}");
        // A whole element against a field inside it.
        let err = apply_err_with(
            doc! { "$set": { "items.$[l]": {} }, "$unset": { "items.$[].qty": "" } },
            vec![doc! { "l.qty": { "$gt": 4 } }],
            order(),
        );
        assert!(
            err.contains(
                "updating the path \"items.1.qty\" would create a conflict at \"items.1\""
            ),
            "{err}"
        );
        // Two filters that both select the third line.
        let err = apply_err_with(
            doc! { "$set": { "items.$[a].shipped": true }, "$unset": { "items.$[b].shipped": "" } },
            vec![doc! { "a.qty": { "$gt": 4 } }, doc! { "b.sku": "c" }],
            order(),
        );
        assert!(err.contains("would create a conflict at \"items.2.shipped\""), "{err}");
    }

    #[test]
    fn positional_paths_that_reach_separate_elements_apply_together() {
        // The same update as above, on a document where `l` selects nothing.
        let out = applied_with(
            doc! { "$set": { "items.$[l].qty": 0 }, "$inc": { "items.$[].qty": 1 } },
            vec![doc! { "l.sku": "z" }],
            order(),
        );
        let qty: Vec<i64> = out
            .get_array("items")
            .unwrap()
            .iter()
            .map(|line| line.as_document().unwrap().get_i64("qty").unwrap())
            .collect();
        assert_eq!(qty, vec![2, 6, 10]);
        // Two filters that select different lines, on one field.
        let out = applied_with(
            doc! { "$set": { "items.$[a].shipped": true }, "$unset": { "items.$[b].shipped": "" } },
            vec![doc! { "a.sku": "a" }, doc! { "b.sku": "c" }],
            order(),
        );
        let lines = out.get_array("items").unwrap();
        assert!(lines[0].as_document().unwrap().get_bool("shipped").unwrap());
        assert!(!lines[2].as_document().unwrap().contains_key("shipped"));
        // A positional path and a plain one beside it.
        let out = applied_with(
            doc! { "$set": { "items.$[].shipped": true, "total": 15 } },
            vec![],
            order(),
        );
        assert_eq!(shipped(&out), vec![true, true, true]);
        assert_eq!(out.get_i32("total").unwrap(), 15);
    }

    #[test]
    fn set_on_insert_is_checked_on_insert_against_the_paths_it_reaches() {
        // On a match `$setOnInsert` writes nothing, so nothing conflicts; on
        // an insert it writes, and its element is checked like any other.
        let update = parse_with_filters(
            &doc! { "$setOnInsert": { "items.$[l].qty": 0 }, "$inc": { "items.$[].qty": 1 } },
            &[doc! { "l.sku": "b" }],
        )
        .unwrap();
        let mut matched = order();
        apply(&update, &mut matched, NOW).unwrap();
        let mut seeded = order();
        let err = apply_on_insert(&update, &mut seeded, NOW).unwrap_err().to_string();
        assert!(err.contains("$setOnInsert on \"items.$[l].qty\" conflicts with $inc"), "{err}");
    }

    #[test]
    fn array_filters_select_against_the_document_before_the_update() {
        // Applied operation by operation, `l` was evaluated after `$inc` had
        // made the line's qty 3, selected it, and the pair conflicted; the
        // other order evaluated `l` first, selected nothing, and applied.
        let filters = || vec![doc! { "m.qty": 2 }, doc! { "l.qty": 3 }];
        let start = || doc! { "items": [ { "qty": 2 } ] };
        let one = applied_with(
            doc! { "$inc": { "items.$[m].qty": 1 }, "$set": { "items.$[l].qty": 0 } },
            filters(),
            start(),
        );
        let other = applied_with(
            doc! { "$set": { "items.$[l].qty": 0 }, "$inc": { "items.$[m].qty": 1 } },
            filters(),
            start(),
        );
        assert_eq!(one, doc! { "items": [ { "qty": 3i64 } ] });
        assert_eq!(one, other);

        // With no conflict at all: `m` reads the sku as it was, so the line
        // whose sku the same update renames to "b" is not selected, in
        // either order.
        let filters = || vec![doc! { "m.sku": "b" }];
        let start = || doc! { "items": [ { "sku": "a", "qty": 1 } ] };
        let one = applied_with(
            doc! { "$set": { "items.0.sku": "b" }, "$inc": { "items.$[m].qty": 1 } },
            filters(),
            start(),
        );
        let other = applied_with(
            doc! { "$inc": { "items.$[m].qty": 1 }, "$set": { "items.0.sku": "b" } },
            filters(),
            start(),
        );
        assert_eq!(one, doc! { "items": [ { "sku": "b", "qty": 1 } ] });
        assert_eq!(one, other);
    }

    #[test]
    fn an_update_refused_in_a_document_leaves_it_as_it_was() {
        // Through the public entry point: every path is claimed before any
        // is written, so the `$set` and `$inc` written before the `$unset`
        // that meets `$inc` are not applied either.
        let update = parse_with_filters(
            &doc! {
                "$set": { "total": 15 },
                "$inc": { "items.$[].qty": 1 },
                "$unset": { "items.1.qty": "" },
            },
            &[],
        )
        .unwrap();
        let mut doc = order();
        let err = apply(&update, &mut doc, NOW).unwrap_err().to_string();
        assert!(err.contains("would create a conflict at \"items.1.qty\""), "{err}");
        assert_eq!(doc, order(), "nothing was written");
        let mut doc = order();
        apply_on_insert(&update, &mut doc, NOW).unwrap_err();
        assert_eq!(doc, order(), "nothing was written on insert either");
    }

    #[test]
    fn an_index_segment_is_one_path_however_it_is_spelled() {
        // The path language reads `01` and `+1` as index 1, so each was a
        // second write to `a.1`, and the result followed the key order.
        for spelling in ["a.01", "a.+1", "a.001"] {
            for update in [
                doc! { "$set": { "a.1": 5 }, "$inc": { spelling: 1 } },
                doc! { "$inc": { spelling: 1 }, "$set": { "a.1": 5 } },
            ] {
                let err = conflict_err(update.clone());
                assert!(err.contains("would create a conflict at \"a.1\""), "{update}: {err}");
            }
        }
        let err = conflict_err(doc! { "$set": { "a": [] }, "$inc": { "a.01.n": 1 } });
        assert!(err.contains("updating the path \"a.1.n\" would create a conflict at \"a\""));
        // Against a positional path, in the document.
        let err = apply_err_with(
            doc! { "$inc": { "items.$[].qty": 1 }, "$set": { "items.01.qty": 0 } },
            vec![],
            order(),
        );
        assert!(err.contains("in this document") && err.contains("at \"items.1.qty\""), "{err}");
        // Spelled that way alone, it is not refused, and different indexes
        // whose spellings share a prefix are different paths.
        assert_eq!(
            applied(doc! { "$set": { "a.01": 5 } }, doc! { "a": [10, 20] }),
            doc! {
                "a": [10, 5]
            }
        );
        assert_eq!(
            applied(doc! { "$set": { "a.1": 5 }, "$inc": { "a.10": 1 } }, doc! { "a": [10, 20] })
                .get_array("a")
                .unwrap()[1],
            Bson::Int32(5)
        );
    }

    #[test]
    fn a_rename_onto_an_element_conflicts_with_a_positional_path_reaching_it() {
        let mut start = order();
        start.insert("x", 7);
        let err = apply_err_with(
            doc! { "$rename": { "x": "items.1.qty" }, "$inc": { "items.$[].qty": 1 } },
            vec![],
            start.clone(),
        );
        assert!(
            err.contains(
                "$rename on \"x\" conflicts with $inc on \"items.$[].qty\" in this document"
            ) && err.contains("at \"items.1.qty\""),
            "{err}"
        );
        // Onto a field no positional path reaches, it applies.
        let out = applied_with(
            doc! { "$rename": { "x": "items.1.note" }, "$inc": { "items.$[].qty": 1 } },
            vec![],
            start,
        );
        assert_eq!(
            out.get_array("items").unwrap()[1].as_document().unwrap().get_i32("note").unwrap(),
            7
        );
    }

    #[test]
    fn a_conflict_found_on_insert_names_the_pair_in_written_order() {
        // `$setOnInsert` runs first on an insert, but the refusal names the
        // two operations in the order the update wrote them.
        let update = parse_with_filters(
            &doc! { "$inc": { "items.$[].qty": 1 }, "$setOnInsert": { "items.$[l].qty": 0 } },
            &[doc! { "l.sku": "b" }],
        )
        .unwrap();
        let mut seeded = order();
        let err = apply_on_insert(&update, &mut seeded, NOW).unwrap_err().to_string();
        assert!(err.contains("$inc on \"items.$[].qty\" conflicts with $setOnInsert"), "{err}");
    }

    #[test]
    fn claiming_concrete_paths_costs_a_bounded_number_of_lookups_per_path() {
        // Three operations over every element of a 20,000-element array:
        // 60,000 concrete paths. Compared pairwise, that is 1.8e9
        // comparisons; claimed, each path costs its own lookup, one into the
        // paths it may lie inside, and one per prefix (`items`, `items.N`).
        let n = 20_000;
        let items: Vec<Bson> = (0..n).map(|i| Bson::Document(doc! { "k": i })).collect();
        let doc = doc! { "items": items };
        let update = parse(
            &doc! { "$set": { "items.$[].a": 1 }, "$inc": { "items.$[].b": 1 }, "$unset": { "items.$[].k": "" } },
        )
        .unwrap();
        let Update::Operators(operations) = &update else { unreachable!() };
        let concrete = concrete_paths(operations, &doc, |_| true).unwrap();
        let paths: usize = concrete.iter().map(Vec::len).sum();
        assert_eq!(paths, 3 * n as usize);
        let probes = claim_concrete(operations, &concrete).unwrap();
        assert_eq!(probes, 4 * paths);
        // And a conflict at the last element is still found.
        let update = parse_with_filters(
            &doc! { "$set": { "items.$[].a": 1 }, "$inc": { "items.$[l].a": 1 } },
            &[doc! { "l.k": n - 1 }],
        )
        .unwrap();
        let mut target = doc.clone();
        let err = apply(&update, &mut target, NOW).unwrap_err().to_string();
        assert!(err.contains(&format!("at \"items.{}.a\"", n - 1)), "{err}");
    }

    #[test]
    fn set_on_insert_may_not_touch_id() {
        assert!(parse(&doc! { "$setOnInsert": { "_id": 1 } }).is_err());
    }
}

#[cfg(test)]
mod decimal128 {
    use super::*;
    use bson::doc;

    fn dec(text: &str) -> Bson {
        Bson::Decimal128(text.parse().unwrap())
    }

    #[test]
    fn set_stores_a_decimal128_and_the_comparing_operators_refuse_one() {
        // `$set` takes any value: a Decimal128 is stored intact, which is
        // the promise the JSON edge now keeps. `$min` and `$max` compare,
        // and the canonical order has nothing to compare a Decimal128 with.
        let mut d = doc! { "_id": 1 };
        let set = parse(&doc! { "$set": { "amount": dec("9.99") } }).unwrap();
        apply(&set, &mut d, 0).unwrap();
        assert_eq!(d.get("amount"), Some(&dec("9.99")));

        for op in ["$min", "$max"] {
            let msg = parse(&doc! { op: { "amount": dec("1") } }).unwrap_err().to_string();
            assert!(msg.contains(op) && msg.contains("Decimal128"), "{op}: {msg}");
            let msg = parse(&doc! { op: { "amount": { "n": dec("1") } } }).unwrap_err().to_string();
            assert!(msg.contains("Decimal128"), "{op}, nested: {msg}");
        }
        assert!(parse(&doc! { "$max": { "amount": 1.5 } }).is_ok());
    }

    #[test]
    fn the_pull_operators_refuse_an_operand_they_cannot_compare() {
        // Each compares its operand against every element; let through, a
        // `$pull` of a Decimal128 emptied `[1, 2.5, "s", 3]` down to `["s"]`,
        // silently.
        for update in [
            doc! { "$pull": { "xs": dec("1.5") } },
            doc! { "$pull": { "xs": { "$gt": dec("1.5") } } },
            doc! { "$pull": { "xs": { "n": dec("1.5") } } },
            doc! { "$pullAll": { "xs": [1, dec("0")] } },
        ] {
            let msg = parse(&update).expect_err("refused").to_string();
            let op = update.keys().next().unwrap();
            assert!(msg.contains(op) && msg.contains("Decimal128"), "{update}: {msg}");
            assert!(msg.contains("ranks equal to every other number"), "{update}: {msg}");
        }
        // The same operators over a double behave as ever.
        let mut d = doc! { "_id": 1, "xs": [1, 2.5, "s", 3] };
        let pull = parse(&doc! { "$pull": { "xs": 2.5 } }).unwrap();
        apply(&pull, &mut d, 0).unwrap();
        assert_eq!(d.get_array("xs").unwrap().len(), 3);
        let add = parse(&doc! { "$addToSet": { "xs": { "$each": [3, 9.9] } } }).unwrap();
        apply(&add, &mut d, 0).unwrap();
        assert_eq!(d.get_array("xs").unwrap().len(), 4);
    }

    #[test]
    fn add_to_set_identifies_members_as_group_does_against_stored_elements() {
        // A stored `Decimal128` ranks equal to every number in the canonical
        // order, so the 5 was judged present and dropped (ADR-186).
        let mut d = doc! { "_id": 1, "t": [dec("1")] };
        let add = parse(&doc! { "$addToSet": { "t": 5 } }).unwrap();
        apply(&add, &mut d, 0).unwrap();
        assert_eq!(d.get_array("t").unwrap(), &vec![dec("1"), Bson::Int32(5)]);
        // One number in another width is still the same member.
        let mut d = doc! { "_id": 1, "t": [1] };
        let add = parse(&doc! { "$addToSet": { "t": 1.0 } }).unwrap();
        apply(&add, &mut d, 0).unwrap();
        assert_eq!(d.get_array("t").unwrap(), &vec![Bson::Int32(1)]);
        // Members of one `$each` are de-duplicated against each other too, and
        // the first of each kind stays: the integer 2, not the double.
        let mut d = doc! { "_id": 1, "t": [] };
        let add = parse(&doc! { "$addToSet": { "t": { "$each": [f64::NAN, f64::NAN, 2, 2.0] } } })
            .unwrap();
        apply(&add, &mut d, 0).unwrap();
        let t = d.get_array("t").unwrap();
        assert_eq!(t.len(), 2, "{t:?}");
        assert!(matches!(t[0], Bson::Double(n) if n.is_nan()), "{t:?}");
        assert_eq!(t[1], Bson::Int32(2), "{t:?}");
        // Duplicates already stored are never rewritten.
        let mut d = doc! { "_id": 1, "t": [1, 1.0] };
        let add = parse(&doc! { "$addToSet": { "t": 2 } }).unwrap();
        apply(&add, &mut d, 0).unwrap();
        assert_eq!(
            d.get_array("t").unwrap(),
            &vec![Bson::Int32(1), Bson::Double(1.0), Bson::Int32(2)]
        );
    }

    #[test]
    fn add_to_set_takes_a_decimal128_operand_and_identifies_it_by_its_bytes() {
        let add = |update: Document, stored: Vec<Bson>| {
            let mut d = doc! { "_id": 1, "t": stored };
            let parsed = parse(&update).unwrap_or_else(|e| panic!("refused {update}: {e}"));
            apply(&parsed, &mut d, 0).unwrap();
            d.get_array("t").unwrap().clone()
        };
        let one = || doc! { "$addToSet": { "t": dec("1") } };
        // A number is not a `Decimal128`, whichever is stored.
        assert_eq!(add(one(), vec![Bson::Int32(1)]), vec![Bson::Int32(1), dec("1")]);
        // The same bytes are the same member.
        assert_eq!(add(one(), vec![dec("1")]), vec![dec("1")]);
        // Different bytes are two members.
        assert_eq!(add(one(), vec![dec("1.0")]), vec![dec("1.0"), dec("1")]);
        // Inside `$each`, and in a document or array operand.
        let each = doc! { "$addToSet": { "t": { "$each": [dec("1"), dec("1"), 1] } } };
        assert_eq!(add(each, vec![]), vec![dec("1"), Bson::Int32(1)]);
        let nested = doc! { "$addToSet": { "t": { "n": dec("1") } } };
        assert_eq!(add(nested, vec![]), vec![Bson::Document(doc! { "n": dec("1") })]);
        let array = doc! { "$addToSet": { "t": [dec("1")] } };
        assert_eq!(add(array, vec![]), vec![Bson::Array(vec![dec("1")])]);
    }

    #[test]
    fn push_sort_refuses_an_element_it_cannot_place() {
        // Whole elements, and documents sorted by a field: either way the
        // element has no position among the numbers, and the update says so
        // rather than leaving it somewhere.
        let mut d = doc! { "_id": 1, "xs": [3, dec("2")], "ds": [{ "n": 3 }, { "n": dec("2") }] };
        let whole = parse(&doc! { "$push": { "xs": { "$each": [1], "$sort": 1 } } }).unwrap();
        let msg = apply(&whole, &mut d.clone(), 0).unwrap_err().to_string();
        assert!(msg.contains("$push $sort") && msg.contains("Decimal128"), "{msg}");
        let by_field =
            parse(&doc! { "$push": { "ds": { "$each": [{ "n": 1 }], "$sort": { "n": 1 } } } })
                .unwrap();
        let msg = apply(&by_field, &mut d.clone(), 0).unwrap_err().to_string();
        assert!(msg.contains("$push $sort") && msg.contains("Decimal128"), "{msg}");
        // The same pushes without a sort append as ever.
        let plain = parse(&doc! { "$push": { "xs": { "$each": [1] } } }).unwrap();
        apply(&plain, &mut d, 0).unwrap();
        assert_eq!(d.get_array("xs").unwrap().len(), 3);
    }
}
