//! Sorting and projection — the two things that shape a result set after
//! filtering has decided its membership.

use bson::{Bson, Document};
use kimmy_core::cmp::{canonical_cmp, holds_decimal128};
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
            let mut picked = Document::new();
            for p in paths {
                // Take the first match: a projection names one destination, so
                // fanning an array traversal out would change the shape.
                if let Some(value) = path::resolve(doc, p).first() {
                    // Ignore errors: a path that cannot be written (e.g. into
                    // an array without an index) simply is not projected.
                    let _ = path::set(&mut picked, p, (*value).clone());
                }
            }
            in_document_order(picked, doc)
        }
    }
}

/// Reorder an inclusion projection's output to the source document's field
/// order, at every level the projection reached into.
///
/// Walking the projection's paths builds the output in *specification* order —
/// `{alpha: 1, zeta: 1}` over `{_id, zeta, alpha}` gave `{alpha, zeta, _id}`,
/// with `_id` last because the parser appends the implicit `_id` to the list.
/// That was invisible while the JSON boundary sorted every object (ADR-120)
/// and wrong once it stopped: MongoDB returns projected fields in the order
/// the document holds them, `_id` first, and `docs/compatibility.md` now says
/// a client may rely on stored order. The picking is left as it was and the
/// result is reordered afterwards, so the array and dotted-path rules above
/// are untouched; a field that was picked but has no counterpart at this
/// level of the source (a path set through an array, say) keeps its place at
/// the end.
fn in_document_order(mut picked: Document, source: &Document) -> Document {
    let mut out = Document::new();
    for (key, original) in source {
        let Some(value) = picked.remove(key) else { continue };
        let value = match (value, original) {
            (Bson::Document(inner), Bson::Document(from)) => {
                Bson::Document(in_document_order(inner, from))
            }
            (value, _) => value,
        };
        out.insert(key.clone(), value);
    }
    out.extend(picked);
    out
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
            Some(Projection::Include(vec!["a".into(), "b".into(), "c".into(), "_id".into()]))
        );
        assert_eq!(
            parse_projection(&doc! { "a": 0.0, "b": false }).unwrap(),
            Some(Projection::Exclude(vec!["a".into(), "b".into()]))
        );
        for bad in [doc! { "a": "1" }, doc! { "a": Bson::Null }, doc! { "a": [1] }] {
            assert!(parse_projection(&bad).is_err(), "{bad:?}");
        }
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
