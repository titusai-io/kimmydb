//! Dot-path resolution with MongoDB array semantics.
//!
//! The rule that surprises people, and that the rest of the query layer is
//! built around: a path traverses *into* arrays implicitly. Given
//! `{items: [{sku: "a"}, {sku: "b"}]}`, the path `items.sku` resolves to both
//! `"a"` and `"b"`, and a filter matches if *any* of them matches.
//!
//! A numeric segment is ambiguous — `a.0` could mean "index 0" or "the field
//! named 0" — so both interpretations are resolved, matching Mongo.

use bson::{Bson, Document};

/// Split a dot path into its segments.
pub fn segments(path: &str) -> Vec<&str> {
    path.split('.').collect()
}

/// All values reachable at `path`, following arrays implicitly.
///
/// An empty result means the path is absent, which callers must distinguish
/// from a path present with value `null` — `$exists` and `$eq: null` depend on
/// the difference.
pub fn resolve<'a>(doc: &'a Document, path: &str) -> Vec<&'a Bson> {
    let segs = segments(path);
    let mut out = Vec::new();
    resolve_in_document(doc, &segs, &mut out);
    out
}

fn resolve_in_document<'a>(doc: &'a Document, segs: &[&str], out: &mut Vec<&'a Bson>) {
    let Some((head, rest)) = segs.split_first() else {
        return;
    };
    if let Some(value) = doc.get(*head) {
        descend(value, rest, out);
    }
}

fn descend<'a>(value: &'a Bson, segs: &[&str], out: &mut Vec<&'a Bson>) {
    if segs.is_empty() {
        out.push(value);
        return;
    }
    let (head, rest) = segs.split_first().expect("non-empty");

    match value {
        Bson::Document(doc) => {
            if let Some(next) = doc.get(*head) {
                descend(next, rest, out);
            }
        }
        Bson::Array(items) => {
            // A numeric segment may address a position...
            if let Ok(index) = head.parse::<usize>()
                && let Some(item) = items.get(index)
            {
                descend(item, rest, out);
            }
            // ...and independently, the segment may name a field inside each
            // element. Both are valid readings, and Mongo honours both.
            for item in items {
                if let Bson::Document(doc) = item
                    && let Some(next) = doc.get(*head)
                {
                    descend(next, rest, out);
                }
            }
        }
        _ => {}
    }
}

/// Set a value at a dot path, creating intermediate documents as needed.
///
/// Traverses existing arrays only by numeric index; it will not fan a write out
/// across array elements, because a write must have exactly one destination.
pub fn set(doc: &mut Document, path: &str, value: Bson) -> Result<(), String> {
    let segs = segments(path);
    set_in_document(doc, &segs, value)
}

fn set_in_document(doc: &mut Document, segs: &[&str], value: Bson) -> Result<(), String> {
    let (head, rest) = segs.split_first().expect("non-empty path");
    if rest.is_empty() {
        doc.insert(*head, value);
        return Ok(());
    }

    // Vivify a missing or non-traversable intermediate as a document, which is
    // what Mongo's $set does.
    let needs_new = !matches!(doc.get(*head), Some(Bson::Document(_)) | Some(Bson::Array(_)));
    if needs_new {
        doc.insert(*head, Bson::Document(Document::new()));
    }

    match doc.get_mut(*head).expect("just ensured present") {
        Bson::Document(child) => set_in_document(child, rest, value),
        Bson::Array(items) => {
            let (index_seg, tail) = rest.split_first().expect("rest is non-empty");
            let index: usize = index_seg.parse().map_err(|_| {
                format!("cannot traverse array with non-numeric segment {index_seg:?}")
            })?;
            while items.len() <= index {
                items.push(Bson::Null);
            }
            if tail.is_empty() {
                items[index] = value;
                return Ok(());
            }
            if !matches!(items[index], Bson::Document(_)) {
                items[index] = Bson::Document(Document::new());
            }
            match &mut items[index] {
                Bson::Document(child) => set_in_document(child, tail, value),
                _ => unreachable!("just ensured a document"),
            }
        }
        _ => unreachable!("just ensured a container"),
    }
}

/// Remove the value at a dot path. Returns whether anything was removed.
pub fn unset(doc: &mut Document, path: &str) -> bool {
    let segs = segments(path);
    unset_in_document(doc, &segs)
}

fn unset_in_document(doc: &mut Document, segs: &[&str]) -> bool {
    let (head, rest) = segs.split_first().expect("non-empty path");
    if rest.is_empty() {
        return doc.remove(*head).is_some();
    }
    match doc.get_mut(*head) {
        Some(Bson::Document(child)) => unset_in_document(child, rest),
        Some(Bson::Array(items)) => {
            let (index_seg, tail) = rest.split_first().expect("rest is non-empty");
            let Ok(index) = index_seg.parse::<usize>() else {
                return false;
            };
            match items.get_mut(index) {
                // Mongo leaves a null hole rather than shifting the array, so
                // that other paths into it keep their indices.
                Some(slot) if tail.is_empty() => {
                    *slot = Bson::Null;
                    true
                }
                Some(Bson::Document(child)) => unset_in_document(child, tail),
                _ => false,
            }
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    fn values(doc: &Document, path: &str) -> Vec<Bson> {
        resolve(doc, path).into_iter().cloned().collect()
    }

    #[test]
    fn resolves_a_top_level_field() {
        let d = doc! { "a": 1 };
        assert_eq!(values(&d, "a"), vec![Bson::Int32(1)]);
    }

    #[test]
    fn resolves_a_nested_field() {
        let d = doc! { "a": { "b": { "c": 7 } } };
        assert_eq!(values(&d, "a.b.c"), vec![Bson::Int32(7)]);
    }

    #[test]
    fn a_missing_path_resolves_to_nothing() {
        let d = doc! { "a": 1 };
        assert!(values(&d, "b").is_empty());
        assert!(values(&d, "a.b").is_empty());
        assert!(values(&d, "a.b.c.d").is_empty());
    }

    #[test]
    fn an_explicit_null_is_distinct_from_a_missing_field() {
        // $exists and `$eq: null` both hinge on this distinction.
        let d = doc! { "a": Bson::Null };
        assert_eq!(values(&d, "a"), vec![Bson::Null]);
        assert!(values(&d, "b").is_empty());
    }

    #[test]
    fn paths_traverse_into_arrays_of_documents() {
        let d = doc! { "items": [ { "sku": "a" }, { "sku": "b" } ] };
        assert_eq!(
            values(&d, "items.sku"),
            vec![Bson::String("a".into()), Bson::String("b".into())]
        );
    }

    #[test]
    fn a_terminal_array_resolves_to_the_array_itself() {
        // Element-wise matching is the comparison layer's job, not the path's.
        let d = doc! { "tags": ["x", "y"] };
        assert_eq!(values(&d, "tags").len(), 1);
        assert!(matches!(values(&d, "tags")[0], Bson::Array(_)));
    }

    #[test]
    fn numeric_segments_address_positions_and_field_names_alike() {
        let by_index = doc! { "a": [10, 20] };
        assert_eq!(values(&by_index, "a.0"), vec![Bson::Int32(10)]);

        // The same segment can also name a field inside array elements.
        let by_name = doc! { "a": [ { "0": "named" } ] };
        assert!(values(&by_name, "a.0").contains(&Bson::String("named".into())));
    }

    #[test]
    fn nested_arrays_flatten_across_levels() {
        let d = doc! { "a": [ { "b": [ { "c": 1 }, { "c": 2 } ] } ] };
        assert_eq!(values(&d, "a.b.c"), vec![Bson::Int32(1), Bson::Int32(2)]);
    }

    #[test]
    fn set_creates_missing_intermediates() {
        let mut d = doc! {};
        set(&mut d, "a.b.c", Bson::Int32(1)).unwrap();
        assert_eq!(d, doc! { "a": { "b": { "c": 1 } } });
    }

    #[test]
    fn set_overwrites_a_scalar_intermediate() {
        let mut d = doc! { "a": 5 };
        set(&mut d, "a.b", Bson::Int32(1)).unwrap();
        assert_eq!(d, doc! { "a": { "b": 1 } });
    }

    #[test]
    fn set_writes_into_arrays_by_index() {
        let mut d = doc! { "a": [1, 2, 3] };
        set(&mut d, "a.1", Bson::Int32(99)).unwrap();
        assert_eq!(d, doc! { "a": [1, 99, 3] });
    }

    #[test]
    fn set_pads_an_array_when_writing_past_its_end() {
        let mut d = doc! { "a": [1] };
        set(&mut d, "a.3", Bson::Int32(9)).unwrap();
        assert_eq!(d, doc! { "a": [1, Bson::Null, Bson::Null, 9] });
    }

    #[test]
    fn set_rejects_a_non_numeric_segment_into_an_array() {
        // A write must have exactly one destination; fanning out across
        // elements would be ambiguous.
        let mut d = doc! { "a": [ { "b": 1 } ] };
        assert!(set(&mut d, "a.b", Bson::Int32(2)).is_err());
    }

    #[test]
    fn unset_removes_fields_and_reports_whether_it_did() {
        let mut d = doc! { "a": { "b": 1, "c": 2 } };
        assert!(unset(&mut d, "a.b"));
        assert_eq!(d, doc! { "a": { "c": 2 } });
        assert!(!unset(&mut d, "a.zzz"));
        assert!(!unset(&mut d, "nope.deep"));
    }

    #[test]
    fn unset_leaves_a_null_hole_in_an_array() {
        // Compacting would shift every later element, silently changing what
        // other paths into the array refer to.
        let mut d = doc! { "a": [1, 2, 3] };
        assert!(unset(&mut d, "a.1"));
        assert_eq!(d, doc! { "a": [1, Bson::Null, 3] });
    }
}
