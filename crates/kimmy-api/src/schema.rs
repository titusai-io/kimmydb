//! Sampled schema inference.
//!
//! A schemaless store is hostile to an agent that has never seen the data: it
//! can list collections but has no idea what is *in* one, so it guesses field
//! names and gets empty results. Sampling a few documents and reporting the
//! paths, their BSON types, and how often each appears turns that guessing into
//! a lookup.
//!
//! This is **inference, not a schema**. Nothing here is enforced, and a field
//! absent from the sample may still exist — which is why presence is reported
//! as a fraction of the sample rather than asserted as "optional".

use std::collections::{BTreeMap, BTreeSet};

use bson::{Bson, Document};
use kimmy_auth::Action;
use serde_json::{Value, json};

use crate::error::ApiError;
use crate::exec::{authorize, index_to_json};
use crate::json::document_to_json;
use crate::state::{Auth, SharedState};

/// How many documents to look at by default.
///
/// Large enough to catch optional fields that appear in a minority of
/// documents, small enough that describing a collection is not itself a scan
/// worth worrying about.
pub const DEFAULT_SAMPLE: usize = 100;
pub const MAX_SAMPLE: usize = 1_000;

/// Nesting depth beyond which paths are not expanded.
///
/// Without a bound, one deeply nested document would produce a field list
/// longer than the documents it describes — and an agent reading it would spend
/// its context on structure it will never query.
const MAX_DEPTH: usize = 6;

/// A field observed in the sample.
struct FieldStats {
    /// BSON type names seen at this path, in a stable order.
    types: BTreeMap<&'static str, usize>,
    /// Documents in which the path was present.
    present: usize,
    /// One example value, for a path whose name does not explain itself.
    example: Option<Bson>,
}

pub fn describe_collection(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    sample_size: Option<usize>,
    include_examples: bool,
) -> Result<Value, ApiError> {
    let meta = authorize(state, auth, Action::Read, db, coll)?;
    let limit = sample_size.unwrap_or(DEFAULT_SAMPLE).clamp(1, MAX_SAMPLE);

    let mut fields: BTreeMap<String, FieldStats> = BTreeMap::new();
    let mut sampled = 0usize;

    state.engine.for_each_doc(&meta, |_, doc| {
        observe_document(&doc, &mut fields);
        sampled += 1;
        Ok(sampled < limit)
    })?;

    let total = state.engine.count(&meta)?;

    let described: Vec<Value> = fields
        .iter()
        .map(|(path, stats)| {
            let mut types: Vec<&str> = stats.types.keys().copied().collect();
            // Most-common type first: an agent scanning the list should see the
            // usual shape before the exception.
            types.sort_by_key(|t| std::cmp::Reverse(stats.types[t]));

            let mut field = json!({
                "path": path,
                "types": types,
                // Fraction of *sampled* documents, not of the collection.
                "presence": presence(stats.present, sampled),
            });
            if include_examples && let Some(example) = &stats.example {
                field["example"] = crate::json::bson_to_json(example);
            }
            field
        })
        .collect();

    Ok(json!({
        "database": db,
        "collection": meta.name,
        "documentCount": total,
        "sampled": sampled,
        "fields": described,
        "indexes": meta.indexes.iter().map(index_to_json).collect::<Vec<_>>(),
        "vector": meta.vector,
    }))
}

/// Round presence to three decimals.
///
/// The raw ratio carries false precision — `0.9900000000000001` invites an
/// agent to treat sampling noise as signal.
fn presence(present: usize, sampled: usize) -> f64 {
    if sampled == 0 {
        return 0.0;
    }
    ((present as f64 / sampled as f64) * 1000.0).round() / 1000.0
}

/// Fold one document into the running statistics.
///
/// Presence counts *documents*, not occurrences, so the paths seen in this
/// document are collected first and counted once each. Incrementing as we walk
/// would count an array of three elements as three occurrences of `tags[]` and
/// report a presence above 1.0 — which happened, and is meaningless.
fn observe_document(doc: &Document, fields: &mut BTreeMap<String, FieldStats>) {
    let mut seen = BTreeSet::new();
    observe(doc, "", fields, 0, &mut seen);
    for path in seen {
        if let Some(stats) = fields.get_mut(&path) {
            stats.present += 1;
        }
    }
}

/// Walk a document, recording every path it contains.
fn observe(
    doc: &Document,
    prefix: &str,
    fields: &mut BTreeMap<String, FieldStats>,
    depth: usize,
    seen: &mut BTreeSet<String>,
) {
    for (key, value) in doc {
        let path = if prefix.is_empty() { key.clone() } else { format!("{prefix}.{key}") };
        record(&path, value, fields, seen);

        if depth >= MAX_DEPTH {
            continue;
        }
        match value {
            Bson::Document(sub) => observe(sub, &path, fields, depth + 1, seen),
            // Array elements are recorded under `path[]`, mirroring how a
            // multikey index treats them: a query on `tags` matches an element
            // of the array, not the array itself, so the element type is what a
            // caller actually needs to know.
            Bson::Array(items) => {
                let element_path = format!("{path}[]");
                for item in items {
                    record(&element_path, item, fields, seen);
                    if let Bson::Document(sub) = item {
                        observe(sub, &element_path, fields, depth + 1, seen);
                    }
                }
            }
            _ => {}
        }
    }
}

fn record(
    path: &str,
    value: &Bson,
    fields: &mut BTreeMap<String, FieldStats>,
    seen: &mut BTreeSet<String>,
) {
    let entry = fields.entry(path.to_string()).or_insert_with(|| FieldStats {
        types: BTreeMap::new(),
        present: 0,
        example: None,
    });
    *entry.types.entry(type_name(value)).or_insert(0) += 1;
    if entry.example.is_none() && is_illustrative(value) {
        entry.example = Some(value.clone());
    }
    seen.insert(path.to_string());
}

/// Whether a value is worth showing as an example.
///
/// A nested document or array would duplicate what the field list already says,
/// and a long string is mostly context cost — the point of an example is to
/// show the *format* of a value, which a truncated one still does.
fn is_illustrative(value: &Bson) -> bool {
    match value {
        Bson::Document(_) | Bson::Array(_) | Bson::Null => false,
        Bson::String(s) => s.len() <= 120,
        _ => true,
    }
}

/// The name a caller would write in a `$type` query.
fn type_name(value: &Bson) -> &'static str {
    match value {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::Binary(_) => "binData",
        Bson::ObjectId(_) => "objectId",
        Bson::Boolean(_) => "bool",
        Bson::DateTime(_) => "date",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::Int32(_) => "int",
        Bson::Int64(_) => "long",
        Bson::Timestamp(_) => "timestamp",
        Bson::Decimal128(_) => "decimal",
        Bson::MinKey => "minKey",
        Bson::MaxKey => "maxKey",
        _ => "unknown",
    }
}

/// Render a few whole documents alongside the inferred schema.
///
/// Types tell an agent what to filter on; examples tell it what the values look
/// like. Both are cheap here and expensive to obtain by trial and error.
pub fn sample_documents(
    state: &SharedState,
    auth: &Auth,
    db: &str,
    coll: &str,
    limit: usize,
) -> Result<Vec<Value>, ApiError> {
    let meta = authorize(state, auth, Action::Read, db, coll)?;
    let mut out = Vec::new();
    state.engine.for_each_doc(&meta, |_, doc| {
        out.push(document_to_json(&doc));
        Ok(out.len() < limit)
    })?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn observed(docs: &[Document]) -> BTreeMap<String, FieldStats> {
        let mut fields = BTreeMap::new();
        for doc in docs {
            observe_document(doc, &mut fields);
        }
        fields
    }

    #[test]
    fn nested_documents_become_dotted_paths() {
        let fields = observed(&[doc! { "customer": { "name": "ada", "id": 7 } }]);
        assert!(fields.contains_key("customer"));
        assert!(fields.contains_key("customer.name"));
        assert!(fields.contains_key("customer.id"));
        assert_eq!(fields["customer.name"].types.keys().collect::<Vec<_>>(), vec![&"string"]);
        assert_eq!(fields["customer.id"].types.keys().collect::<Vec<_>>(), vec![&"int"]);
    }

    #[test]
    fn array_elements_are_reported_under_a_bracket_path() {
        // A query on `tags` matches an *element*, so the element type is the
        // one a caller needs; reporting only "array" would be useless.
        let fields = observed(&[doc! { "tags": ["a", "b"] }]);
        assert_eq!(fields["tags"].types.keys().collect::<Vec<_>>(), vec![&"array"]);
        assert_eq!(fields["tags[]"].types.keys().collect::<Vec<_>>(), vec![&"string"]);
    }

    #[test]
    fn documents_inside_arrays_are_expanded() {
        let fields = observed(&[doc! { "items": [{ "sku": "x", "qty": 2 }] }]);
        assert!(fields.contains_key("items[].sku"));
        assert!(fields.contains_key("items[].qty"));
    }

    #[test]
    fn a_field_with_mixed_types_reports_all_of_them() {
        let fields = observed(&[doc! { "v": 1 }, doc! { "v": "one" }]);
        assert_eq!(fields["v"].types.len(), 2);
        assert_eq!(fields["v"].present, 2);
    }

    #[test]
    fn presence_counts_only_the_documents_that_had_the_field() {
        let fields = observed(&[doc! { "a": 1 }, doc! { "a": 2, "b": 3 }]);
        assert_eq!(presence(fields["a"].present, 2), 1.0);
        assert_eq!(presence(fields["b"].present, 2), 0.5);
    }

    #[test]
    fn presence_counts_documents_not_array_elements() {
        // Counting per element made `tags[]` report a presence of 2.0 across
        // three documents — a fraction above 1.0, which means nothing. A
        // document either contains the path or does not.
        let docs =
            [doc! { "tags": ["a", "b"] }, doc! { "tags": ["c", "d"] }, doc! { "tags": ["e", "f"] }];
        let fields = observed(&docs);
        assert_eq!(fields["tags[]"].present, 3);
        assert_eq!(presence(fields["tags[]"].present, 3), 1.0);
    }

    #[test]
    fn a_path_present_in_some_documents_stays_a_fraction() {
        let docs = [doc! { "tags": ["a", "b", "c"] }, doc! { "other": 1 }];
        let fields = observed(&docs);
        assert_eq!(presence(fields["tags[]"].present, 2), 0.5);
    }

    #[test]
    fn type_counts_still_see_every_occurrence() {
        // Presence is per document, but the *type* histogram must keep counting
        // occurrences — it is what orders the reported types by how usual they
        // are, and one document with nine strings and one integer is not a
        // fifty-fifty split.
        let fields = observed(&[doc! { "v": ["a", "b", 1] }]);
        assert_eq!(fields["v[]"].types["string"], 2);
        assert_eq!(fields["v[]"].types["int"], 1);
        assert_eq!(fields["v[]"].present, 1);
    }

    #[test]
    fn recursion_is_bounded() {
        // A self-similar document must not produce an unbounded field list.
        let mut doc = doc! { "leaf": 1 };
        for _ in 0..50 {
            doc = doc! { "n": doc };
        }
        let fields = observed(&[doc]);
        let deepest = fields.keys().map(|k| k.matches('.').count()).max().unwrap();
        assert!(deepest <= MAX_DEPTH + 1, "depth {deepest} escaped the bound");
    }

    #[test]
    fn examples_skip_containers_and_long_strings() {
        assert!(!is_illustrative(&Bson::Document(doc! {})));
        assert!(!is_illustrative(&Bson::Array(vec![])));
        assert!(!is_illustrative(&Bson::Null));
        assert!(is_illustrative(&Bson::String("short".into())));
        assert!(!is_illustrative(&Bson::String("x".repeat(200))));
        assert!(is_illustrative(&Bson::Int32(1)));
    }

    #[test]
    fn presence_is_not_falsely_precise() {
        // 99/100 must not read as 0.9900000000000001.
        assert_eq!(presence(99, 100), 0.99);
        assert_eq!(presence(1, 3), 0.333);
        assert_eq!(presence(0, 0), 0.0);
    }
}
