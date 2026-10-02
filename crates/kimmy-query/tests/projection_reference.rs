//! `shape::project` against a reference projector written from the rules in
//! query-language.md ("Sort and projection") and ADR-214 alone, compared
//! exactly, field order included (the rendered text keeps it).
//!
//! The reference is deliberately naive: it carries the remaining segments of
//! every path down each field, where the engine builds a tree once and
//! chooses between removing fields and rebuilding a document by how many go.
//! Agreement over generated documents and paths, numeric segments and `_id`
//! among them, is what shows the faster walk answers what the rules say.

use bson::{Bson, Document};
use kimmy_query::shape;
use proptest::prelude::*;

const KEYS: [&str; 5] = ["a", "b", "0", "1", "_id"];

// The reference.

fn split(paths: &[Vec<String>], key: &str) -> Option<Vec<Vec<String>>> {
    let rests: Vec<Vec<String>> =
        paths.iter().filter(|p| p[0] == key).map(|p| p[1..].to_vec()).collect();
    if rests.is_empty() { None } else { Some(rests) }
}

fn ref_inc_doc(doc: &Document, paths: &[Vec<String>]) -> Document {
    let mut out = Document::new();
    for (k, v) in doc {
        let Some(rests) = split(paths, k) else { continue };
        if rests.iter().any(|r| r.is_empty()) {
            out.insert(k.clone(), v.clone());
            continue;
        }
        match v {
            Bson::Document(d) => {
                let r = ref_inc_doc(d, &rests);
                if !r.is_empty() {
                    out.insert(k.clone(), r);
                }
            }
            Bson::Array(items) => {
                out.insert(k.clone(), ref_inc_arr(items, &rests));
            }
            _ => {}
        }
    }
    out
}

fn ref_inc_arr(items: &[Bson], paths: &[Vec<String>]) -> Bson {
    let mut out = Vec::new();
    for it in items {
        match it {
            Bson::Document(d) => out.push(Bson::Document(ref_inc_doc(d, paths))),
            Bson::Array(a) => out.push(ref_inc_arr(a, paths)),
            _ => {}
        }
    }
    Bson::Array(out)
}

fn ref_exc_doc(doc: &Document, paths: &[Vec<String>]) -> Document {
    let mut out = Document::new();
    for (k, v) in doc {
        let Some(rests) = split(paths, k) else {
            out.insert(k.clone(), v.clone());
            continue;
        };
        if rests.iter().any(|r| r.is_empty()) {
            continue;
        }
        let nv = match v {
            Bson::Document(d) => Bson::Document(ref_exc_doc(d, &rests)),
            Bson::Array(a) => ref_exc_arr(a, &rests),
            other => other.clone(),
        };
        out.insert(k.clone(), nv);
    }
    out
}

fn ref_exc_arr(items: &[Bson], paths: &[Vec<String>]) -> Bson {
    Bson::Array(
        items
            .iter()
            .map(|it| match it {
                Bson::Document(d) => Bson::Document(ref_exc_doc(d, paths)),
                Bson::Array(a) => ref_exc_arr(a, paths),
                other => other.clone(),
            })
            .collect(),
    )
}

fn segs(p: &str) -> Vec<String> {
    p.split('.').map(str::to_string).collect()
}

/// Reference reading of a (valid) spec: inclusion adds `_id` unless `_id: 0`.
fn reference(spec: &Document, doc: &Document) -> Document {
    let truthy = |v: &Bson| !matches!(v, Bson::Int32(0));
    let inc: Vec<&String> = spec.iter().filter(|(_, v)| truthy(v)).map(|(k, _)| k).collect();
    if inc.is_empty() {
        let ps: Vec<Vec<String>> = spec.keys().map(|k| segs(k)).collect();
        return ref_exc_doc(doc, &ps);
    }
    let mut ps: Vec<Vec<String>> = inc.iter().map(|k| segs(k)).collect();
    let id_off = spec.get("_id").is_some_and(|v| !truthy(v));
    if !id_off {
        ps.push(vec!["_id".into()]);
    }
    ref_inc_doc(doc, &ps)
}

// Generators.

fn value() -> impl Strategy<Value = Bson> {
    let leaf = prop_oneof![
        (0i32..3).prop_map(Bson::Int32),
        Just(Bson::String("s".into())),
        Just(Bson::Null),
    ];
    leaf.prop_recursive(5, 64, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Bson::Array),
            fields(inner).prop_map(Bson::Document),
        ]
    })
}

fn fields(inner: impl Strategy<Value = Bson>) -> impl Strategy<Value = Document> {
    prop::collection::vec((prop::sample::select(KEYS.to_vec()), inner), 0..5)
        .prop_map(|pairs| pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

fn path() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::sample::select(KEYS.to_vec()), 1..4).prop_map(|s| s.join("."))
}

/// Top-level names for wide documents: enough that a projection can name few
/// of a document's fields, or more than a level scans before it indexes.
const WIDE: [&str; 26] = [
    "_id", "0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9", "k10", "k11", "k12", "k13",
    "k14", "k15", "k16", "k17", "k18", "k19", "k20", "k21", "k22", "k23", "k24",
];

fn wide_document() -> impl Strategy<Value = Document> {
    prop::collection::vec((prop::sample::select(WIDE.to_vec()), value()), 0..40)
        .prop_map(|pairs| pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

fn wide_path() -> impl Strategy<Value = String> {
    (
        prop::sample::select(WIDE.to_vec()),
        prop::collection::vec(prop::sample::select(KEYS.to_vec()), 0..3),
    )
        .prop_map(|(first, rest)| std::iter::once(first).chain(rest).collect::<Vec<_>>().join("."))
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 800, ..ProptestConfig::default() })]

    #[test]
    fn a_wide_document_matches_reference(
        doc in wide_document(),
        paths in prop::collection::vec(wide_path(), 1..14),
        flag in prop_oneof![Just(0), Just(1), Just(2)],
    ) {
        // 0 excludes; 1 includes; 2 includes with `_id: 0`.
        let mut spec = Document::new();
        for p in &paths { spec.insert(p.clone(), i32::from(flag != 0)); }
        if flag == 2 { spec.insert("_id", 0); }
        let Ok(projection) = shape::parse_projection(&spec) else { return Ok(()); };
        let got = shape::project(projection.as_ref(), &doc);
        let want = reference(&spec, &doc);
        prop_assert_eq!(got.to_string(), want.to_string(), "\nspec {}\ndoc  {}", spec, doc);
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 5000, ..ProptestConfig::default() })]

    #[test]
    fn inclusion_matches_reference(
        doc in fields(value()),
        paths in prop::collection::vec(path(), 1..5),
        drop_id in any::<bool>(),
    ) {
        let mut spec = Document::new();
        for p in &paths { spec.insert(p.clone(), 1); }
        if drop_id { spec.insert("_id", 0); }
        let Ok(projection) = shape::parse_projection(&spec) else { return Ok(()); };
        let got = shape::project(projection.as_ref(), &doc);
        let want = reference(&spec, &doc);
        prop_assert_eq!(got.to_string(), want.to_string(), "\nspec {}\ndoc  {}", spec, doc);
    }

    #[test]
    fn exclusion_matches_reference(
        doc in fields(value()),
        paths in prop::collection::vec(path(), 1..5),
    ) {
        let mut spec = Document::new();
        for p in &paths { spec.insert(p.clone(), 0); }
        let projection = shape::parse_projection(&spec).unwrap();
        let got = shape::project(projection.as_ref(), &doc);
        let want = reference(&spec, &doc);
        prop_assert_eq!(got.to_string(), want.to_string(), "\nspec {}\ndoc  {}", spec, doc);
    }
}
