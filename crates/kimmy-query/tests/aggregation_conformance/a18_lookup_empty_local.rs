//! A18: in `$lookup` a local `[]` joins as `null`; a foreign `[]` is met only by a local `[[]]`
//! (q7s4g:627, 628; q7s4a:253). A document with no local field, and a foreign document with no
//! `foreignField`, stay under the documented deviation: they join nothing.

use bson::{Bson, Document, doc};
use kimmy_core::cmp::canonical_cmp;
use kimmy_query::aggregate::{join_values, lookup_keys};
use std::cmp::Ordering;

/// The foreign `k` of q7s4g:627: ids 1 to 9.
fn foreign() -> Vec<Document> {
    vec![
        doc! { "_id": 1, "k": [] },
        doc! { "_id": 2, "k": Bson::Null },
        doc! { "_id": 3 },
        doc! { "_id": 4, "k": 1 },
        doc! { "_id": 5, "k": [[]] },
        doc! { "_id": 6, "k": [Bson::Null] },
        doc! { "_id": 7, "k": [1, 2] },
        doc! { "_id": 8, "k": [[1]] },
        doc! { "_id": 9, "k": 2 },
    ]
}

/// The local `k` of q7s4g:627, and the foreign ids it joins.
fn locals() -> Vec<(Document, Vec<i32>)> {
    vec![
        // The reference also joins 3 (missing) for these three: the documented deviation.
        (doc! { "_id": 1, "k": [] }, vec![2, 6]),
        (doc! { "_id": 2, "k": Bson::Null }, vec![2, 6]),
        (doc! { "_id": 3 }, vec![]),
        (doc! { "_id": 4, "k": 1 }, vec![4, 7]),
        (doc! { "_id": 5, "k": [[]] }, vec![1, 5]),
        (doc! { "_id": 6, "k": [Bson::Null] }, vec![2, 6]),
        (doc! { "_id": 7, "k": [1, 2] }, vec![4, 7, 9]),
        (doc! { "_id": 8, "k": [[1]] }, vec![8]),
    ]
}

#[test]
fn an_empty_local_array_joins_as_null_and_a_foreign_one_only_by_a_nested_empty() {
    let foreign = foreign();
    for (local, want) in locals() {
        let wanted = lookup_keys(std::slice::from_ref(&local), "k");
        let joined: Vec<i32> = foreign
            .iter()
            .filter(|doc| {
                join_values(doc, "k")
                    .iter()
                    .any(|key| wanted.iter().any(|w| canonical_cmp(w, key) == Ordering::Equal))
            })
            .map(|doc| doc.get_i32("_id").unwrap())
            .collect();
        assert_eq!(joined, want, "{local}");
    }
}

#[test]
fn the_keys_of_an_empty_local_array_are_the_null() {
    // q7s4a:253, id 3 (`arr: []`): it joined nothing before, as `[]` was its own key.
    assert_eq!(lookup_keys(&[doc! { "arr": [] }], "arr"), vec![Bson::Null]);
    // A nested empty array is still a key and an element: `[[]]` and `[]`.
    assert_eq!(
        lookup_keys(&[doc! { "arr": [[]] }], "arr"),
        vec![Bson::Array(vec![Bson::Array(vec![])]), Bson::Array(vec![])]
    );
    // Through a path: each element's empty array joins as null.
    assert_eq!(
        lookup_keys(&[doc! { "a": [{ "b": [] }, { "b": 1 }] }], "a.b"),
        vec![Bson::Null, Bson::Int32(1)]
    );
}
