//! A property test for projections through arrays (ADR-214): whatever the
//! document and the paths, a projection's answer is a sub-document of the
//! stored one, element by element, and it reads back what was asked of it.
//!
//! - An inclusion's answer, read at any included path, gives every value the
//!   stored document gives there, in the same order: the first element alone
//!   is not every value, and neither is an array turned into a document.
//! - An exclusion's answer gives nothing at an excluded path, and the stored
//!   values at every path that neither contains an excluded one nor lies
//!   inside one.
//!
//! "Read" here is a projection's reading of a path: every segment a field
//! name, numeric ones included, and an array entered element by element. It
//! is `path::resolve` without the positional reading a filter adds for a
//! numeric segment, which a projection does not have.

use bson::{Bson, Document};
use kimmy_query::shape;

use proptest::prelude::*;

const KEYS: [&str; 5] = ["a", "b", "c", "0", "1"];

fn value() -> impl Strategy<Value = Bson> {
    let leaf = prop_oneof![
        (0i32..4).prop_map(Bson::Int32),
        Just(Bson::String("s".into())),
        Just(Bson::Null),
    ];
    leaf.prop_recursive(4, 40, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Bson::Array),
            fields(inner).prop_map(Bson::Document),
        ]
    })
}

fn fields(inner: impl Strategy<Value = Bson>) -> impl Strategy<Value = Document> {
    prop::collection::vec((prop::sample::select(KEYS.to_vec()), inner), 0..4)
        .prop_map(|pairs| pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

fn document() -> impl Strategy<Value = Document> {
    (0i32..100, fields(value())).prop_map(|(id, rest)| {
        let mut d = bson::doc! { "_id": id };
        d.extend(rest);
        d
    })
}

fn paths() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(prop::collection::vec(prop::sample::select(KEYS.to_vec()), 1..4), 1..4)
        .prop_map(|ps| ps.into_iter().map(|segs| segs.join(".")).collect())
}

fn spec(paths: &[String], flag: i32) -> Document {
    paths.iter().map(|p| (p.clone(), Bson::Int32(flag))).collect()
}

/// `small` is `big` with fields and elements left out and nothing else: every
/// field of a document is one `big` holds, in `big`'s order, every array is a
/// subsequence of `big`'s elements, and a value is never another type.
fn is_sub(small: &Bson, big: &Bson) -> bool {
    match (small, big) {
        (Bson::Document(s), Bson::Document(b)) => is_sub_document(s, b),
        (Bson::Array(s), Bson::Array(b)) => {
            let mut rest = b.iter();
            s.iter().all(|item| rest.any(|candidate| is_sub(item, candidate)))
        }
        (s, b) => s == b,
    }
}

fn is_sub_document(small: &Document, big: &Document) -> bool {
    let mut order = big.keys();
    small.iter().all(|(k, v)| order.any(|bk| bk == k) && big.get(k).is_some_and(|bv| is_sub(v, bv)))
}

fn read(doc: &Document, p: &str) -> Vec<Bson> {
    let segs: Vec<&str> = p.split('.').collect();
    let mut out = Vec::new();
    if let Some(value) = doc.get(segs[0]) {
        descend(value, &segs[1..], &mut out);
    }
    out
}

fn descend(value: &Bson, segs: &[&str], out: &mut Vec<Bson>) {
    let Some((head, rest)) = segs.split_first() else {
        out.push(value.clone());
        return;
    };
    match value {
        Bson::Document(doc) => {
            if let Some(next) = doc.get(*head) {
                descend(next, rest, out);
            }
        }
        Bson::Array(items) => {
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

/// Whether `a` is `b` or a path inside it, segment by segment.
fn within(a: &str, b: &str) -> bool {
    a == b || a.starts_with(&format!("{b}."))
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 4000, ..ProptestConfig::default() })]

    #[test]
    fn an_inclusion_is_a_sub_document_that_reads_back_every_included_value(
        doc in document(),
        included in paths(),
        drop_id in any::<bool>(),
    ) {
        let mut asked = spec(&included, 1);
        if drop_id {
            asked.insert("_id", 0);
        }
        let projection = shape::parse_projection(&asked).unwrap();
        let out = shape::project(projection.as_ref(), &doc);
        prop_assert!(is_sub_document(&out, &doc), "\nspec {asked}\ndoc  {doc}\nout  {out}");
        for p in &included {
            prop_assert_eq!(read(&out, p), read(&doc, p), "\nat {}\ndoc {}\nout {}", p, doc, out);
        }
        prop_assert_eq!(out.contains_key("_id"), !drop_id);
    }

    #[test]
    fn an_exclusion_is_a_sub_document_with_nothing_left_at_an_excluded_path(
        doc in document(),
        excluded in paths(),
        probes in paths(),
    ) {
        let asked = spec(&excluded, 0);
        let projection = shape::parse_projection(&asked).unwrap();
        let out = shape::project(projection.as_ref(), &doc);
        prop_assert!(is_sub_document(&out, &doc), "\nspec {asked}\ndoc  {doc}\nout  {out}");
        for p in &excluded {
            prop_assert!(read(&out, p).is_empty(), "\nat {}\ndoc {}\nout {}", p, doc, out);
        }
        for q in &probes {
            if excluded.iter().any(|p| within(q, p) || within(p, q)) {
                continue;
            }
            prop_assert_eq!(read(&out, q), read(&doc, q), "\nat {}\ndoc {}\nout {}", q, doc, out);
        }
        prop_assert_eq!(out.get("_id"), doc.get("_id"));
    }
}
