//! The membership of a partial index is the filter's own answer, over random documents and paths
//! (the mirror of the keys test in `index.rs`): for documents with nested arrays, arrays of
//! arrays, digit-named fields beside positions and leading zeros, and for dotted paths through
//! them, `PartialFilter::selects` holds exactly when `find`'s matcher selects the document, for
//! each predicate a partial index can carry. A filter the partial index refuses to carry is not
//! a row.

use bson::{Bson, Document, doc};
use proptest::prelude::*;

const NAMES: [&str; 8] = ["a", "b", "c", "0", "1", "00", "01", "99999999999"];

fn value(depth: u32) -> BoxedStrategy<Bson> {
    let leaf = prop_oneof![
        (0..4i32).prop_map(Bson::Int32),
        Just(Bson::Null),
        Just(Bson::String("s".into())),
    ];
    if depth == 0 {
        return leaf.boxed();
    }
    let inner = value(depth - 1);
    prop_oneof![
        3 => leaf,
        2 => prop::collection::vec(inner.clone(), 0..4).prop_map(Bson::Array),
        2 => prop::collection::vec((prop::sample::select(&NAMES[..]), inner), 0..4)
            .prop_map(|fields| Bson::Document(fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect())),
    ]
    .boxed()
}

fn document() -> impl Strategy<Value = Document> {
    prop::collection::vec((prop::sample::select(&NAMES[..]), value(3)), 0..4)
        .prop_map(|fields| fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

fn dotted_path() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::sample::select(&NAMES[..]), 1..4)
        .prop_map(|segments| segments.join("."))
}

fn predicate() -> impl Strategy<Value = Bson> {
    prop_oneof![
        (0..4i32).prop_map(Bson::Int32),
        Just(Bson::Document(doc! { "$exists": true })),
        (0..4i32).prop_map(|n| Bson::Document(doc! { "$gt": n })),
        (0..4i32).prop_map(|n| Bson::Document(doc! { "$lte": n })),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(3_000))]

    #[test]
    fn a_partial_index_holds_what_the_filters_read_selects(
        d in document(),
        path in dotted_path(),
        want in predicate(),
    ) {
        let mut filter = Document::new();
        filter.insert(path, want);
        let Ok(partial) = kimmy_core::PartialFilter::parse(&filter) else { return Ok(()) };
        let parsed = kimmy_query::filter::parse(&filter).unwrap();
        let scan = kimmy_query::filter::matches(&parsed, &d).unwrap();
        prop_assert_eq!(partial.selects(&d), scan, "{} over {}", filter, d);
    }
}
