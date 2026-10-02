//! Sorting and projection — the two things that shape a result set after
//! filtering has decided its membership.

use bson::{Bson, Document};
use kimmy_core::cmp::{canonical_cmp, holds_decimal128};
use kimmy_core::{Error, Result};
use std::cmp::Ordering;
use std::collections::HashMap;

use crate::path;

/// The `_id` field, which projection treats specially.
pub(crate) const ID_FIELD: &str = "_id";

// ---------------------------------------------------------------------------
// Sort
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortKey {
    pub path: String,
    pub descending: bool,
}

/// Parse a sort specification like `{age: -1, name: 1}`.
pub fn parse_sort(doc: &Document) -> Result<Vec<SortKey>> {
    doc.iter()
        .map(|(path, direction)| {
            let descending = match direction {
                Bson::Int32(1) | Bson::Int64(1) | Bson::Double(1.0) => false,
                Bson::Int32(-1) | Bson::Int64(-1) | Bson::Double(-1.0) => true,
                _ => {
                    return Err(Error::InvalidQuery(format!(
                        "sort direction for {path:?} must be 1 or -1"
                    )));
                }
            };
            Ok(SortKey { path: path.clone(), descending })
        })
        .collect()
}

/// Compare two documents by a sort specification.
///
/// A document missing the sort field sorts as though it held `null`, which is
/// what puts absent values at one end rather than in arbitrary positions.
pub fn compare(keys: &[SortKey], a: &Document, b: &Document) -> Ordering {
    for key in keys {
        let va = sort_value(a, &key.path);
        let vb = sort_value(b, &key.path);
        let ordering = canonical_cmp(&va, &vb);
        if ordering != Ordering::Equal {
            return if key.descending { ordering.reverse() } else { ordering };
        }
    }
    Ordering::Equal
}

/// The values a document sorts by, one per key, in `keys` order.
///
/// What [`compare`] reads out of a document, taken once and kept — so a
/// bounded sort can rank a match without holding the document it came from
/// (ADR-150). A missing path yields `Null` here exactly as it does there, so
/// the vector is always as long as `keys` and the two comparisons line up
/// position for position.
pub fn sort_keys(keys: &[SortKey], doc: &Document) -> Vec<Bson> {
    keys.iter().map(|key| sort_value(doc, &key.path)).collect()
}

/// [`compare`], over values already taken by [`sort_keys`].
///
/// The same ordering, arrived at from the same values: `canonical_cmp` on each
/// key in turn, reversed where the key is descending, `Equal` when every key
/// agrees. A vector shorter than `keys` — which nothing this crate produces
/// can be — compares as though the missing keys were absent from both sides.
pub fn compare_keys(keys: &[SortKey], a: &[Bson], b: &[Bson]) -> Ordering {
    for (i, key) in keys.iter().enumerate() {
        let (Some(va), Some(vb)) = (a.get(i), b.get(i)) else { break };
        let ordering = canonical_cmp(va, vb);
        if ordering != Ordering::Equal {
            return if key.descending { ordering.reverse() } else { ordering };
        }
    }
    Ordering::Equal
}

/// The value a document sorts by for one key.
///
/// When a path resolves to several values — because it passes through an array
/// — the smallest of them is the one the document sorts by, whichever
/// direction was asked for. Direction is not visible here: [`compare`] applies
/// `descending` by reversing the comparison of two minima that were already
/// chosen, so a descending sort orders documents by their smallest element
/// rather than their largest. Mongo takes the largest for a descending sort,
/// so the two orderings differ where a sort key passes through an array.
fn sort_value(doc: &Document, path: &str) -> Bson {
    let values = path::resolve(doc, path);
    if values.is_empty() {
        return Bson::Null;
    }
    // Flatten a terminal array so that sorting by an array field orders by its
    // elements rather than by the array as a whole.
    let mut candidates: Vec<&Bson> = Vec::new();
    for value in &values {
        match value {
            Bson::Array(items) if !items.is_empty() => candidates.extend(items.iter()),
            other => candidates.push(other),
        }
    }
    candidates.into_iter().min_by(|a, b| canonical_cmp(a, b)).cloned().unwrap_or(Bson::Null)
}

/// Sort documents in place.
///
/// The caller has asked [`refuse_unsortable`] about every document first;
/// this comparator has no way to refuse one.
pub fn sort(keys: &[SortKey], docs: &mut [Document]) {
    if keys.is_empty() {
        return;
    }
    docs.sort_by(|a, b| compare(keys, a, b));
}

/// Why `keys` cannot order `doc`, if they cannot.
///
/// `canonical_cmp` ranks a `Decimal128` equal to every other number, which is
/// not an order a sort can use: the document would land somewhere among the
/// numbers that depended on which of them it happened to be compared with.
/// Every place that sorts documents by keys asks this before it compares, so
/// the refusal names the document and the path rather than placing it
/// nowhere in particular. The path is resolved the way [`compare`] resolves
/// it, so a Decimal128 inside an array or a document at the path counts.
pub fn unsortable(keys: &[SortKey], doc: &Document) -> Option<String> {
    let key = keys
        .iter()
        .find(|key| path::resolve(doc, &key.path).iter().any(|v| holds_decimal128(v)))?;
    let id = doc.get(ID_FIELD).map_or_else(|| "?".to_string(), Bson::to_string);
    Some(format!(
        "cannot sort by {:?}: document {id} holds a Decimal128 there, which cannot be compared: \
         it has no exact key encoding in this engine and ranks equal to every other number; \
         store a double or a long instead",
        key.path
    ))
}

/// [`unsortable`], as the refusal a query returns.
pub fn refuse_unsortable(keys: &[SortKey], doc: &Document) -> Result<()> {
    unsortable(keys, doc).map_or(Ok(()), |why| Err(Error::InvalidQuery(why)))
}

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Projection {
    /// Keep only these paths.
    Include(ProjectionPaths),
    /// Keep everything except these paths.
    Exclude(ProjectionPaths),
}

/// A projection's paths, as the tree [`project`] walks.
///
/// The tree is built once, when the projection is made, and every document
/// the request shapes walks the same one: building it per document cost a
/// projection naming hundreds of fields more than the shaping itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionPaths {
    tree: PathTree,
}

impl From<Vec<String>> for ProjectionPaths {
    fn from(listed: Vec<String>) -> Self {
        ProjectionPaths { tree: PathTree::of(&listed) }
    }
}

/// Parse a projection like `{name: 1, age: 1}` or `{secret: 0}`.
///
/// Inclusion and exclusion cannot be mixed, because the result would be
/// ambiguous about what to do with unnamed fields. `_id` is the documented
/// exception: it may be excluded alongside inclusions.
pub fn parse_projection(doc: &Document) -> Result<Option<Projection>> {
    if doc.is_empty() {
        return Ok(None);
    }

    let mut include = Vec::new();
    let mut exclude = Vec::new();

    for (path, flag) in doc {
        let keep = match flag {
            Bson::Int32(0) | Bson::Int64(0) | Bson::Double(0.0) | Bson::Boolean(false) => false,
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Boolean(true) => true,
            // `{items: {$slice: 3}}`, `{items: {$elemMatch: {...}}}`: a
            // projection operator, which this projection language does not
            // have. Named, so the refusal says what was asked for rather than
            // leaving a reader to work out why an object is not "0 or 1".
            Bson::Document(spec) if spec.keys().any(|k| k.starts_with('$')) => {
                let operator = spec.keys().find(|k| k.starts_with('$')).expect("checked");
                return Err(Error::InvalidQuery(format!(
                    "projection operator {operator} is not supported for {path:?}; \
                     a projection value must be 0 or 1"
                )));
            }
            _ => {
                return Err(Error::InvalidQuery(format!(
                    "projection value for {path:?} must be 0 or 1"
                )));
            }
        };
        if keep {
            include.push(path.clone());
        } else {
            exclude.push(path.clone());
        }
    }

    // `_id: 0` alongside inclusions is the one legal mix.
    let excludes_only_id = exclude.len() == 1 && exclude[0] == ID_FIELD;
    if !include.is_empty() && !exclude.is_empty() && !excludes_only_id {
        return Err(Error::InvalidQuery(
            "a projection cannot mix inclusion and exclusion (except excluding _id)".into(),
        ));
    }

    if !include.is_empty() {
        // `_id` is included by default unless explicitly excluded.
        if !excludes_only_id && !include.iter().any(|p| p == ID_FIELD) {
            include.push(ID_FIELD.to_string());
        }
        return Ok(Some(Projection::Include(include.into())));
    }
    Ok(Some(Projection::Exclude(exclude.into())))
}

/// Apply a projection, returning the reshaped document.
///
/// Both kinds walk the document and the projection's paths together, segment
/// by segment, and **a path that meets an array applies its remaining segments
/// to every element** — the reading a path has in a filter, where `p._id`
/// reaches every element's `_id`. The array is kept, in its own order, so a
/// projection never changes an array into something else:
///
/// - an inclusion reduces each element that is a document to the included
///   sub-paths, as `{}` when it holds none of them; drops each element that is
///   not a document, which has no such sub-path; and reduces an element that is
///   itself an array the same way, recursively;
/// - an exclusion removes the sub-path from each element that is a document,
///   recurses into one that is an array, and leaves every other element as it
///   was.
///
/// A segment is a field name throughout, a numeric one too: `a.0` names a
/// field called `0` in each element of `a`, never the element at position 0.
/// Several paths into one array (`p._id`, `p.name`) are one walk, so each
/// element carries all of them. Where one path is a prefix of another
/// (`a` and `a.b`), the shorter one decides: `a` is included, or removed,
/// whole.
///
/// Outside arrays an inclusion keeps a sub-document only when it holds at least
/// one included path, so `{"a.b": 1}` over `{a: {c: 1}}` leaves `a` out; an
/// element of an array is kept as `{}` instead, so the array keeps one entry
/// per document it held. An inclusion's fields come out in the order the
/// document holds them, at every level, not the order the specification names
/// them (`_id`, which the parser appends, stays where the document has it).
pub fn project(projection: Option<&Projection>, doc: &Document) -> Document {
    match projection {
        None => doc.clone(),
        Some(Projection::Exclude(paths)) => excluded(doc, &paths.tree),
        Some(Projection::Include(paths)) => include_fields(doc, &paths.tree),
    }
}

/// A projection's paths as a tree of segments, so that every path through one
/// field is applied in a single walk of it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PathTree {
    /// A path ends here: the field is included, or removed, whole, and any
    /// longer path through it adds nothing.
    whole: bool,
    /// The next segments of the paths that continue past this one, in the
    /// order the specification first names them.
    fields: Vec<(String, PathTree)>,
    /// Where each of `fields` is, by name, once there are more than
    /// [`SCAN_UP_TO`] of them; below that a scan comparing names is cheaper
    /// than hashing one.
    index: Option<HashMap<String, usize>>,
    /// How many of `fields` are `whole`, which decides how an exclusion
    /// removes them (see [`excluded`]).
    wholes: usize,
}

impl PathTree {
    fn of(paths: &[String]) -> Self {
        let mut root = PathTree::default();
        for p in paths {
            let mut node = &mut root;
            for seg in path::segments(p) {
                let at = match node.fields.iter().position(|(name, _)| name == seg) {
                    Some(at) => at,
                    None => {
                        node.fields.push((seg.to_string(), PathTree::default()));
                        node.fields.len() - 1
                    }
                };
                node = &mut node.fields[at].1;
            }
            node.whole = true;
        }
        root.finish();
        root
    }

    /// Count each level's whole fields and index the wide levels, once the
    /// tree holds every path.
    fn finish(&mut self) {
        self.wholes = self.fields.iter().filter(|(_, node)| node.whole).count();
        if self.fields.len() > SCAN_UP_TO {
            self.index = Some(
                self.fields.iter().enumerate().map(|(at, (name, _))| (name.clone(), at)).collect(),
            );
        }
        for (_, node) in &mut self.fields {
            node.finish();
        }
    }

    fn field(&self, name: &str) -> Option<&PathTree> {
        match &self.index {
            Some(index) => index.get(name).map(|&at| &self.fields[at].1),
            None => self.fields.iter().find(|(field, _)| field == name).map(|(_, node)| node),
        }
    }
}

/// The fields of `doc` that `tree` includes, in `doc`'s order.
fn include_fields(doc: &Document, tree: &PathTree) -> Document {
    let mut out = Document::new();
    // A document holds each name once, so once every field the tree names
    // has been met, the rest of the document holds nothing to include. When
    // the tree names few fields of a wide document, asking the document for
    // each is cheaper than walking it: none present is an empty answer, one
    // is the answer with no order to keep, and otherwise the walk stops at
    // the last one present rather than the last one named.
    let mut unmet = tree.fields.len();
    if unmet * 4 < doc.len() {
        let mut present = tree.fields.iter().filter(|(name, _)| doc.contains_key(name));
        match (present.next(), present.next()) {
            (None, _) => return out,
            (Some((name, node)), None) => {
                let value = doc.get(name).expect("present");
                include_field(&mut out, name, value, node);
                return out;
            }
            (Some(_), Some(_)) => unmet = 2 + present.count(),
        }
    }
    for (key, value) in doc {
        if unmet == 0 {
            break;
        }
        let Some(node) = tree.field(key) else { continue };
        unmet -= 1;
        include_field(&mut out, key, value, node);
    }
    out
}

/// Add to `out` what `node` includes of the field `key`, holding `value`.
fn include_field(out: &mut Document, key: &str, value: &Bson, node: &PathTree) {
    if node.whole {
        out.insert(key, value.clone());
        return;
    }
    match value {
        Bson::Document(inner) => {
            let picked = include_fields(inner, node);
            if !picked.is_empty() {
                out.insert(key, picked);
            }
        }
        Bson::Array(items) => {
            out.insert(key, include_elements(items, node));
        }
        // A scalar has no field for the rest of the path to name.
        _ => {}
    }
}

/// An array that an inclusion's path continues through, each element reduced
/// to what the rest of the path includes.
fn include_elements(items: &[Bson], node: &PathTree) -> Bson {
    Bson::Array(
        items
            .iter()
            .filter_map(|item| match item {
                Bson::Document(inner) => Some(Bson::Document(include_fields(inner, node))),
                Bson::Array(inner) => Some(include_elements(inner, node)),
                _ => None,
            })
            .collect(),
    )
}

/// How many fields a level of a [`PathTree`] may name before it is indexed
/// by name rather than scanned.
const SCAN_UP_TO: usize = 8;

/// Whether an exclusion removes most of a document's fields, so that building
/// the fields that stay is cheaper than removing the ones that go.
///
/// A document keeps its fields in order, so removing one shifts every field
/// after it, and removing many from a wide document costs their product;
/// building costs one insertion, and one hash of the name, per field kept.
/// Removal is the cheaper while the fields that go are few, or sit near the
/// end, which is where the cut-over is put: at more than half.
fn mostly_removed(doc: &Document, tree: &PathTree) -> bool {
    tree.wholes * 2 > doc.len()
}

/// `doc` without every path `tree` excludes.
fn excluded(doc: &Document, tree: &PathTree) -> Document {
    if !mostly_removed(doc, tree) {
        let mut out = doc.clone();
        exclude_fields(&mut out, tree);
        return out;
    }
    let mut out = Document::new();
    for (key, value) in doc {
        match tree.field(key) {
            None => {
                out.insert(key.clone(), value.clone());
            }
            Some(node) if node.whole => {}
            Some(node) => {
                let value = match value {
                    Bson::Document(inner) => Bson::Document(excluded(inner, node)),
                    Bson::Array(items) => {
                        let mut items = items.clone();
                        exclude_elements(&mut items, node);
                        Bson::Array(items)
                    }
                    other => other.clone(),
                };
                out.insert(key.clone(), value);
            }
        }
    }
    out
}

/// Remove from `doc` every path `tree` excludes, in place.
fn exclude_fields(doc: &mut Document, tree: &PathTree) {
    if mostly_removed(doc, tree) {
        *doc = std::mem::take(doc)
            .into_iter()
            .filter(|(key, _)| !tree.field(key).is_some_and(|node| node.whole))
            .collect();
    } else if tree.wholes > 0 {
        for (name, node) in &tree.fields {
            if node.whole {
                doc.remove(name);
            }
        }
    }
    for (name, node) in &tree.fields {
        if node.whole {
            continue;
        }
        match doc.get_mut(name) {
            Some(Bson::Document(inner)) => exclude_fields(inner, node),
            Some(Bson::Array(items)) => exclude_elements(items, node),
            _ => {}
        }
    }
}

/// An array that an exclusion's path continues through: the rest of the path
/// is removed from each element that can hold it.
fn exclude_elements(items: &mut [Bson], node: &PathTree) {
    for item in items {
        match item {
            Bson::Document(inner) => exclude_fields(inner, node),
            Bson::Array(inner) => exclude_elements(inner, node),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    fn sorted(spec: Document, mut docs: Vec<Document>) -> Vec<Document> {
        let keys = parse_sort(&spec).unwrap();
        sort(&keys, &mut docs);
        docs
    }

    fn projected(spec: Document, doc: Document) -> Document {
        let p = parse_projection(&spec).unwrap();
        project(p.as_ref(), &doc)
    }

    fn keys(doc: &Document) -> Vec<&str> {
        doc.keys().map(String::as_str).collect()
    }

    #[test]
    fn an_inclusion_projection_keeps_the_document_field_order() {
        // The specification names the fields in the opposite order to the
        // document, and leaves `_id` implicit, which the parser appends last.
        let doc = doc! { "_id": 1, "zeta": 1, "alpha": { "y": 2, "x": 1 }, "mid": 3 };
        let out = projected(doc! { "alpha": 1, "zeta": 1 }, doc.clone());
        assert_eq!(keys(&out), ["_id", "zeta", "alpha"]);

        // Reaching into a sub-document keeps that level's order too.
        let out = projected(doc! { "alpha.x": 1, "alpha.y": 1, "zeta": 1 }, doc.clone());
        assert_eq!(keys(&out), ["_id", "zeta", "alpha"]);
        assert_eq!(keys(out.get_document("alpha").unwrap()), ["y", "x"]);

        // An explicit `_id: 0` removes it rather than moving it.
        let out = projected(doc! { "mid": 1, "zeta": 1, "_id": 0 }, doc);
        assert_eq!(keys(&out), ["zeta", "mid"]);
    }

    #[test]
    fn sorts_ascending_and_descending() {
        let docs = vec![doc! { "n": 3 }, doc! { "n": 1 }, doc! { "n": 2 }];
        assert_eq!(
            sorted(doc! { "n": 1 }, docs.clone()),
            vec![doc! { "n": 1 }, doc! { "n": 2 }, doc! { "n": 3 }]
        );
        assert_eq!(
            sorted(doc! { "n": -1 }, docs),
            vec![doc! { "n": 3 }, doc! { "n": 2 }, doc! { "n": 1 }]
        );
    }

    #[test]
    fn sorts_by_several_keys_in_order() {
        let docs = vec![doc! { "a": 1, "b": 2 }, doc! { "a": 1, "b": 1 }, doc! { "a": 0, "b": 9 }];
        assert_eq!(
            sorted(doc! { "a": 1, "b": 1 }, docs),
            vec![doc! { "a": 0, "b": 9 }, doc! { "a": 1, "b": 1 }, doc! { "a": 1, "b": 2 }]
        );
    }

    #[test]
    fn a_missing_field_sorts_as_null() {
        // Absent values must land at one end rather than in arbitrary places.
        let docs = vec![doc! { "n": 1 }, doc! { "other": 1 }];
        let out = sorted(doc! { "n": 1 }, docs);
        assert!(out[0].contains_key("other"), "null sorts before numbers");
    }

    #[test]
    fn sorts_across_numeric_types() {
        let docs = vec![doc! { "n": 2i64 }, doc! { "n": 1.5 }, doc! { "n": 1i32 }];
        let out = sorted(doc! { "n": 1 }, docs);
        assert_eq!(out[0].get_i32("n").unwrap(), 1);
        assert_eq!(out[2].get_i64("n").unwrap(), 2);
    }

    #[test]
    fn sorting_by_an_array_field_uses_its_elements() {
        let docs = vec![doc! { "a": [5, 6] }, doc! { "a": [1, 9] }];
        let out = sorted(doc! { "a": 1 }, docs);
        assert_eq!(out[0].get_array("a").unwrap()[0], Bson::Int32(1));
    }

    #[test]
    fn taken_sort_keys_order_exactly_as_the_documents_do() {
        // The property the bounded sort rests on (ADR-150): a window that
        // holds `sort_keys` instead of documents must rank them the way
        // `compare` ranked the documents — every rule included. Missing
        // fields, a path through an array (smallest element, in both
        // directions), a path into a sub-document, mixed numeric types, and
        // the `_id` key the executor appends to make the order total.
        let docs = vec![
            doc! { "_id": 1, "a": [5, 6], "m": { "r": 2 } },
            doc! { "_id": 2, "a": [1, 9], "m": { "r": 2 } },
            doc! { "_id": 3, "a": 1i64, "m": { "r": 1.5 } },
            doc! { "_id": 4, "m": { "r": 2 } },
            doc! { "_id": 5, "a": [1, 9], "m": { "r": 2 } },
            doc! { "_id": 6, "a": Bson::Null },
            doc! { "_id": 7, "a": "x", "m": { "r": 2 } },
            // An empty array has no element to reduce to, so it is compared
            // as the array itself, at the rank arrays hold — a rule of its
            // own in query-language.md, and one a taken key must carry too.
            doc! { "_id": 8, "a": [] },
        ];
        for spec in [
            doc! { "a": 1, "_id": 1 },
            doc! { "a": -1, "_id": 1 },
            doc! { "m.r": 1, "a": -1, "_id": 1 },
            doc! { "missing": 1, "_id": -1 },
        ] {
            let order = parse_sort(&spec).unwrap();
            let taken: Vec<Vec<Bson>> = docs.iter().map(|d| sort_keys(&order, d)).collect();
            for (i, a) in docs.iter().enumerate() {
                for (j, b) in docs.iter().enumerate() {
                    assert_eq!(
                        compare(&order, a, b),
                        compare_keys(&order, &taken[i], &taken[j]),
                        "{spec} disagreed on {a} vs {b}"
                    );
                }
            }
            // And the order is total once `_id` is a key, which is what lets
            // a heap under it return one page rather than an arrival-order one.
            let mut by_doc = docs.clone();
            by_doc.sort_by(|a, b| compare(&order, a, b));
            let mut by_key: Vec<(Vec<Bson>, &Document)> =
                docs.iter().map(|d| (sort_keys(&order, d), d)).collect();
            by_key.sort_by(|(a, _), (b, _)| compare_keys(&order, a, b));
            assert_eq!(
                by_doc.iter().collect::<Vec<_>>(),
                by_key.into_iter().map(|(_, d)| d).collect::<Vec<_>>(),
                "{spec} sorted differently through taken keys"
            );
        }
    }

    #[test]
    fn invalid_sort_directions_are_rejected() {
        assert!(parse_sort(&doc! { "n": 2 }).is_err());
        assert!(parse_sort(&doc! { "n": "asc" }).is_err());
    }

    #[test]
    fn inclusion_keeps_named_fields_and_id() {
        let out = projected(doc! { "a": 1 }, doc! { "_id": 1, "a": 2, "b": 3 });
        assert_eq!(out, doc! { "a": 2, "_id": 1 });
    }

    #[test]
    fn inclusion_can_drop_id_explicitly() {
        let out = projected(doc! { "a": 1, "_id": 0 }, doc! { "_id": 1, "a": 2, "b": 3 });
        assert_eq!(out, doc! { "a": 2 });
    }

    #[test]
    fn exclusion_keeps_everything_else() {
        let out = projected(doc! { "b": 0 }, doc! { "_id": 1, "a": 2, "b": 3 });
        assert_eq!(out, doc! { "_id": 1, "a": 2 });
    }

    #[test]
    fn projection_reaches_nested_paths() {
        let out = projected(
            doc! { "a.b": 1, "_id": 0 },
            doc! { "_id": 1, "a": { "b": 2, "c": 3 }, "d": 4 },
        );
        assert_eq!(out, doc! { "a": { "b": 2 } });
    }

    #[test]
    fn mixing_inclusion_and_exclusion_is_rejected() {
        // The result would be ambiguous about unnamed fields.
        assert!(parse_projection(&doc! { "a": 1, "b": 0 }).is_err());
        // ...except for _id, which is the documented exception.
        assert!(parse_projection(&doc! { "a": 1, "_id": 0 }).is_ok());
    }

    #[test]
    fn a_projection_operator_is_refused_by_name() {
        // A client porting `$slice` or `$elemMatch` should be told which
        // operator is missing, not that an object "must be 0 or 1".
        let err = parse_projection(&doc! { "items": { "$slice": 3 } }).unwrap_err().to_string();
        assert!(err.contains("projection operator $slice is not supported"), "{err}");
        assert!(err.contains("\"items\""), "names the path: {err}");
        assert!(err.contains("must be 0 or 1"), "and still states the rule: {err}");

        let err = parse_projection(&doc! { "items": { "$elemMatch": { "qty": 1 } } })
            .unwrap_err()
            .to_string();
        assert!(err.contains("projection operator $elemMatch is not supported"), "{err}");

        // A value that is neither a flag nor an operator keeps the plain
        // message: there is no operator to name.
        let err = parse_projection(&doc! { "items": "yes" }).unwrap_err().to_string();
        assert_eq!(err, "invalid query: projection value for \"items\" must be 0 or 1");
        let err = parse_projection(&doc! { "items": { "nested": 1 } }).unwrap_err().to_string();
        assert!(err.contains("must be 0 or 1") && !err.contains("operator"), "{err}");
    }

    #[test]
    fn projection_values_are_read_as_flags_not_as_the_literals_0_and_1() {
        // Any non-zero number or `true` includes; 0, 0.0 and `false` exclude;
        // everything else is refused. Documented in query-language.md, so a
        // change here is a change there.
        assert_eq!(
            parse_projection(&doc! { "a": 2, "b": true, "c": 1.5 }).unwrap(),
            Some(Projection::Include(
                vec!["a".into(), "b".into(), "c".into(), "_id".into()].into()
            ))
        );
        assert_eq!(
            parse_projection(&doc! { "a": 0.0, "b": false }).unwrap(),
            Some(Projection::Exclude(vec!["a".into(), "b".into()].into()))
        );
        for bad in [doc! { "a": "1" }, doc! { "a": Bson::Null }, doc! { "a": [1] }] {
            assert!(parse_projection(&bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_dotted_exclusion_beside_an_inclusion_is_still_a_mix() {
        // A path through an array is an ordinary path to the parser: only
        // `_id: 0` may sit beside inclusions, not `p._id: 0`, in either order.
        for spec in [doc! { "a": 1, "p._id": 0 }, doc! { "p._id": 0, "a": 1 }] {
            let err = parse_projection(&spec).unwrap_err().to_string();
            assert!(err.contains("cannot mix inclusion and exclusion"), "{spec}: {err}");
        }
        assert!(parse_projection(&doc! { "p._id": 1, "_id": 0 }).is_ok());
    }

    #[test]
    fn the_id_exception_is_one_directional() {
        // `_id: 0` may sit beside inclusions; `_id: 1` beside exclusions is
        // still a mix and is refused.
        assert!(parse_projection(&doc! { "_id": 0, "note": 1 }).is_ok());
        let err = parse_projection(&doc! { "_id": 1, "note": 0 }).unwrap_err().to_string();
        assert!(err.contains("cannot mix inclusion and exclusion"), "{err}");
    }

    #[test]
    fn an_empty_projection_is_no_projection() {
        assert_eq!(parse_projection(&doc! {}).unwrap(), None);
        assert_eq!(projected(doc! {}, doc! { "a": 1 }), doc! { "a": 1 });
    }

    #[test]
    fn projecting_a_missing_field_simply_omits_it() {
        let out = projected(doc! { "zzz": 1, "_id": 0 }, doc! { "a": 1 });
        assert_eq!(out, doc! {});
    }

    /// `assert_eq!` on two documents ignores field order, and the order of an
    /// element's fields is part of what these tests claim, so they compare
    /// the rendered text, which keeps it.
    #[track_caller]
    fn same(got: Document, want: Document) {
        assert_eq!(got.to_string(), want.to_string());
    }

    #[test]
    fn an_inclusion_through_an_array_keeps_the_array_and_reduces_every_element() {
        // The worked example: every element, as an array, not the first one
        // as a document.
        let stored = doc! { "_id": 7, "p": [{ "_id": 1, "name": "a" }, { "_id": 2, "name": "b" }] };
        same(
            projected(doc! { "p._id": 1 }, stored.clone()),
            doc! {
                "_id": 7, "p": [{ "_id": 1 }, { "_id": 2 }]
            },
        );
        // Several paths into one array are one array, each element carrying
        // all of them in the element's own order, whatever order they are
        // named in.
        same(
            projected(doc! { "p.name": 1, "p._id": 1 }, stored.clone()),
            doc! {
                "_id": 7, "p": [{ "_id": 1, "name": "a" }, { "_id": 2, "name": "b" }]
            },
        );
        // `_id` excluded, and a top-level field beside the dotted path.
        let with_name = doc! { "_id": 7, "name": "n", "p": stored.get_array("p").unwrap().clone() };
        same(
            projected(doc! { "p.name": 1, "name": 1, "_id": 0 }, with_name),
            doc! {
                "name": "n", "p": [{ "name": "a" }, { "name": "b" }]
            },
        );
    }

    #[test]
    fn an_inclusion_through_an_array_drops_scalars_and_keeps_an_element_missing_the_field() {
        let stored = doc! {
            "_id": 1,
            "p": [{ "x": 1, "y": 2 }, 5, "s", Bson::Null, { "y": 3 }, { "x": [1, 2] }]
        };
        same(
            projected(doc! { "p.x": 1, "_id": 0 }, stored),
            doc! {
                "p": [{ "x": 1 }, {}, { "x": [1, 2] }]
            },
        );
        // An array of scalars alone keeps its place, empty.
        same(projected(doc! { "p.x": 1, "_id": 0 }, doc! { "p": [1, 2, 3] }), doc! { "p": [] });
        same(projected(doc! { "p.x": 1, "_id": 0 }, doc! { "p": [] }), doc! { "p": [] });
        // A scalar where the path continues has no field to give, as before.
        same(projected(doc! { "p.x": 1, "_id": 0 }, doc! { "p": 5 }), doc! {});
    }

    #[test]
    fn an_inclusion_through_two_levels_of_array_keeps_both() {
        let stored = doc! { "a": [
            { "b": [{ "c": 1, "d": 2 }, { "d": 3 }, 7], "e": 1 },
            { "b": { "c": 5, "e": 6 } },
            { "b": { "e": 6 } },
            3,
            { "x": 1 },
        ] };
        same(
            projected(doc! { "a.b.c": 1, "_id": 0 }, stored),
            doc! { "a": [
                { "b": [{ "c": 1 }, {}] },
                { "b": { "c": 5 } },
                // Outside an array a sub-document holding none of the paths is
                // left out, as it always was; the element itself stays.
                {},
                {},
            ] },
        );
    }

    #[test]
    fn a_nested_array_is_reduced_the_same_way() {
        let stored =
            doc! { "p": [[{ "k": 1, "v": 1 }, 5, [{ "k": 2, "v": 2 }]], { "k": 3, "v": 3 }, 4] };
        same(
            projected(doc! { "p.k": 1, "_id": 0 }, stored),
            doc! {
                "p": [[{ "k": 1 }, [{ "k": 2 }]], { "k": 3 }]
            },
        );
    }

    #[test]
    fn a_shorter_path_includes_the_field_whole() {
        let stored = doc! { "_id": 1, "p": [{ "x": 1, "y": 2 }, 5] };
        for spec in [doc! { "p": 1, "p.x": 1 }, doc! { "p.x": 1, "p": 1 }] {
            same(projected(spec, stored.clone()), stored.clone());
        }
        let mut gone = stored.clone();
        gone.remove("p");
        for spec in [doc! { "p": 0, "p.x": 0 }, doc! { "p.x": 0, "p": 0 }] {
            same(projected(spec, stored.clone()), gone.clone());
        }
    }

    #[test]
    fn a_few_fields_named_over_a_wide_document_are_found_wherever_they_are() {
        // A tree naming under a quarter of a document's fields asks for each
        // rather than walking (`include_fields`): none present, one, or
        // several, at the start, the end, through a sub-document or an array.
        let mut stored = doc! { "_id": 7 };
        for f in 0..20 {
            stored.insert(format!("f{f}"), f);
        }
        stored.insert("sub", doc! { "y": 1, "x": 2 });
        stored.insert("arr", vec![Bson::Document(doc! { "x": 1, "y": 2 }), Bson::Int32(3)]);
        let cases = [
            (doc! { "missing": 1 }, doc! { "_id": 7 }),
            (doc! { "missing": 1, "_id": 0 }, doc! {}),
            (doc! { "sub.x": 1, "_id": 0 }, doc! { "sub": { "x": 2 } }),
            (doc! { "sub.zz": 1, "_id": 0 }, doc! {}),
            (doc! { "arr.x": 1, "_id": 0 }, doc! { "arr": [{ "x": 1 }] }),
            (doc! { "f19": 1 }, doc! { "_id": 7, "f19": 19 }),
            (doc! { "f19": 1, "_id": 0 }, doc! { "f19": 19 }),
            (doc! { "f19": 1, "missing": 1, "f0": 1 }, doc! { "_id": 7, "f0": 0, "f19": 19 }),
            (doc! { "arr.y": 1, "f3": 1, "_id": 0 }, doc! { "f3": 3, "arr": [{ "y": 2 }] }),
        ];
        for (spec, want) in cases {
            same(projected(spec, stored.clone()), want);
        }
    }

    #[test]
    fn a_level_naming_many_fields_finds_each_by_name() {
        // Past `SCAN_UP_TO` names a level is indexed rather than scanned; the
        // answer is the same, through an array as at the top.
        let names: Vec<String> = (0..12).map(|f| format!("f{f}")).collect();
        let mut element = Document::new();
        let mut spec = doc! { "_id": 0 };
        let mut want_element = Document::new();
        for (at, name) in names.iter().enumerate() {
            element.insert(name.clone(), at as i32);
            element.insert(format!("other{at}"), at as i32);
            if at % 2 == 0 {
                spec.insert(format!("p.{name}"), 1);
                want_element.insert(name.clone(), at as i32);
            }
        }
        // Six named at this level by `p.*`, plus names that are absent.
        for absent in 0..6 {
            spec.insert(format!("p.none{absent}"), 1);
        }
        let stored = doc! { "_id": 1, "p": [element.clone(), 5, element] };
        same(projected(spec, stored), doc! { "p": [want_element.clone(), want_element] });
    }

    #[test]
    fn a_numeric_segment_names_a_field_in_each_element_not_a_position() {
        let scalars = doc! { "a": [10, 20, 30] };
        same(projected(doc! { "a.0": 1, "_id": 0 }, scalars.clone()), doc! { "a": [] });
        same(projected(doc! { "a.1": 0 }, scalars.clone()), scalars);
        let named = doc! { "a": [{ "0": "x", "1": "y" }, { "1": "z" }] };
        same(
            projected(doc! { "a.0": 1, "_id": 0 }, named.clone()),
            doc! {
                "a": [{ "0": "x" }, {}]
            },
        );
        same(projected(doc! { "a.1": 0 }, named), doc! { "a": [{ "0": "x" }, {}] });
        // In a document it is a field name, as it always was.
        same(
            projected(doc! { "a.0": 1, "_id": 0 }, doc! { "a": { "0": 1, "1": 2 } }),
            doc! {
                "a": { "0": 1 }
            },
        );
    }

    #[test]
    fn a_dollar_segment_is_a_field_name_through_an_array_too() {
        // Not the positional operator (query-language.md): over an array of
        // documents it reaches a field called `$` in each, and finds none.
        let stored = doc! { "items": [{ "q": 1 }, { "q": 2 }, 3] };
        same(projected(doc! { "items.$": 1, "_id": 0 }, stored), doc! { "items": [{}, {}] });
    }

    #[test]
    fn an_exclusion_through_an_array_removes_the_path_from_every_element() {
        let stored = doc! {
            "_id": 7,
            "p": [{ "_id": 1, "name": "a" }, 5, { "_id": 2, "name": "b" }, { "other": 1 }]
        };
        // Scalars and an element without the field are left as they were.
        same(
            projected(doc! { "p.name": 0 }, stored.clone()),
            doc! {
                "_id": 7, "p": [{ "_id": 1 }, 5, { "_id": 2 }, { "other": 1 }]
            },
        );
        same(
            projected(doc! { "p.name": 0, "p._id": 0 }, stored),
            doc! {
                "_id": 7, "p": [{}, 5, {}, { "other": 1 }]
            },
        );
    }

    #[test]
    fn an_exclusion_that_removes_most_of_a_document_answers_the_same() {
        // More than half of a level's fields removed whole switches it from
        // removing them to building what stays (`mostly_removed`), at the top
        // (`_id`, `a`, `b` of four), from a copy (`x`, `y` of an element's
        // three) and by removal below that (`r` of two). Every level must
        // answer what the rules say: `a` goes whole beside `a.k`, the path
        // still reaches into `p`, and `q` stays because `p.q.r` reaches past it.
        let stored = doc! {
            "_id": 1, "a": { "k": 1 }, "b": 2,
            "p": [{ "x": 1, "y": 2, "q": { "r": 1, "s": 2 } }, 3]
        };
        let spec = doc! {
            "_id": 0, "a": 0, "a.k": 0, "b": 0, "p.x": 0, "p.y": 0, "p.q.r": 0
        };
        same(projected(spec, stored), doc! { "p": [{ "q": { "s": 2 } }, 3] });
    }

    #[test]
    fn an_exclusion_through_two_levels_and_a_nested_array() {
        let stored = doc! { "a": [
            { "b": [{ "c": 1, "d": 2 }, 7, [{ "c": 3, "d": 4 }]], "e": 1 },
            { "b": { "c": 5, "e": 6 } },
            3,
        ] };
        same(
            projected(doc! { "a.b.c": 0 }, stored),
            doc! { "a": [
                { "b": [{ "d": 2 }, 7, [{ "d": 4 }]], "e": 1 },
                { "b": { "e": 6 } },
                3,
            ] },
        );
    }
}

#[cfg(test)]
mod decimal128 {
    use super::*;
    use bson::doc;

    #[test]
    fn a_document_holding_a_decimal128_at_a_sort_path_is_refused_by_name() {
        // Wherever the sort would read it: at the path, inside an array at
        // the path, or in a document the path descends into.
        let keys = parse_sort(&doc! { "qty": 1, "meta.rank": -1 }).unwrap();
        let d = Bson::Decimal128("2".parse().unwrap());
        for held in [
            doc! { "_id": 7, "qty": d.clone() },
            doc! { "_id": 7, "qty": [1, d.clone()] },
            doc! { "_id": 7, "meta": { "rank": d.clone() } },
            doc! { "_id": 7, "meta": [{ "rank": d.clone() }] },
        ] {
            let why =
                unsortable(&keys, &held).unwrap_or_else(|| panic!("{held} should be refused"));
            assert!(why.contains("cannot sort by"), "{why}");
            assert!(why.contains("document 7"), "names the document: {why}");
            assert!(why.contains("Decimal128"), "{why}");
            assert!(refuse_unsortable(&keys, &held).is_err());
        }
        // Elsewhere in the document it is nobody's business.
        let elsewhere = doc! { "_id": 8, "qty": 1, "meta": { "rank": 2 }, "price": d.clone() };
        assert_eq!(unsortable(&keys, &elsewhere), None);
        assert!(refuse_unsortable(&keys, &elsewhere).is_ok());
        let no_id = doc! { "qty": d };
        assert!(unsortable(&keys, &no_id).unwrap().contains("document ?"));
    }
}
