//! Sorting and projection — the two things that shape a result set after
//! filtering has decided its membership.

use bson::{Bson, Document};
use kimmy_core::cmp::canonical_cmp;
use kimmy_core::{Error, Result};
use std::cmp::Ordering;

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

/// The value a document sorts by for one key.
///
/// When a path resolves to several values — because it passes through an array
/// — Mongo sorts ascending by the smallest and descending by the largest. Since
/// the direction is applied by the caller, taking the minimum here and
/// reversing gives the wrong end, so both extremes are handled by the caller
/// through `descending`.
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
pub fn sort(keys: &[SortKey], docs: &mut [Document]) {
    if keys.is_empty() {
        return;
    }
    docs.sort_by(|a, b| compare(keys, a, b));
}

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Projection {
    /// Keep only these paths.
    Include(Vec<String>),
    /// Keep everything except these paths.
    Exclude(Vec<String>),
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
        return Ok(Some(Projection::Include(include)));
    }
    Ok(Some(Projection::Exclude(exclude)))
}

/// Apply a projection, returning the reshaped document.
pub fn project(projection: Option<&Projection>, doc: &Document) -> Document {
    match projection {
        None => doc.clone(),
        Some(Projection::Exclude(paths)) => {
            let mut out = doc.clone();
            for p in paths {
                path::unset(&mut out, p);
            }
            out
        }
        Some(Projection::Include(paths)) => {
            let mut out = Document::new();
            for p in paths {
                // Take the first match: a projection names one destination, so
                // fanning an array traversal out would change the shape.
                if let Some(value) = path::resolve(doc, p).first() {
                    // Ignore errors: a path that cannot be written (e.g. into
                    // an array without an index) simply is not projected.
                    let _ = path::set(&mut out, p, (*value).clone());
                }
            }
            out
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
    fn an_empty_projection_is_no_projection() {
        assert_eq!(parse_projection(&doc! {}).unwrap(), None);
        assert_eq!(projected(doc! {}, doc! { "a": 1 }), doc! { "a": 1 });
    }

    #[test]
    fn projecting_a_missing_field_simply_omits_it() {
        let out = projected(doc! { "zzz": 1, "_id": 0 }, doc! { "a": 1 });
        assert_eq!(out, doc! {});
    }
}
