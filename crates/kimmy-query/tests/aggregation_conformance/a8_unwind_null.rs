//! A8: `$unwind` with `preserveNullAndEmptyArrays` keeps a null as `null` (q7s4a:233, 235, 237:
//! ids 5 hold `arr: null` and `n: null`). An empty array loses the field, a missing path stays
//! missing and a scalar passes as itself.

use super::common::run;
use bson::{Bson, Document, doc};

fn input() -> Vec<Document> {
    vec![
        doc! { "_id": 1, "arr": [1, 2], "n": 1 },
        doc! { "_id": 3, "arr": [], "n": 3 },
        doc! { "_id": 5, "arr": Bson::Null, "n": Bson::Null },
        doc! { "_id": 6, "n": 5 },
    ]
}

fn unwind(path: &str, index: Option<&str>) -> Vec<Document> {
    let mut spec = doc! { "path": path, "preserveNullAndEmptyArrays": true };
    if let Some(index) = index {
        spec.insert("includeArrayIndex", index);
    }
    run(&[doc! { "$unwind": spec }, doc! { "$sort": { "_id": 1 } }], input()).unwrap()
}

#[test]
fn a_null_is_kept_as_null_an_empty_array_loses_its_field() {
    // a:233
    let out = unwind("$arr", None);
    assert_eq!(
        out,
        vec![
            doc! { "_id": 1, "arr": 1, "n": 1 },
            doc! { "_id": 1, "arr": 2, "n": 1 },
            doc! { "_id": 3, "n": 3 },
            doc! { "_id": 5, "arr": Bson::Null, "n": Bson::Null },
            doc! { "_id": 6, "n": 5 },
        ]
    );
}

#[test]
fn the_index_of_a_kept_null_is_null() {
    // a:235 (`includeArrayIndex`: null on a row no element made)
    let out = unwind("$arr", Some("i"));
    assert_eq!(
        out[3],
        doc! { "_id": 5, "arr": Bson::Null, "n": Bson::Null, "i": Bson::Null },
        "{out:?}"
    );
    assert_eq!(out[2], doc! { "_id": 3, "n": 3, "i": Bson::Null });
    assert_eq!(out[0].get_i64("i").unwrap(), 0);
}

#[test]
fn a_scalar_and_a_null_at_a_scalar_path_pass_as_themselves() {
    // a:237 (path `n`): id 5 stays `n: null`, the scalars stay scalars.
    let out = unwind("$n", None);
    assert_eq!(out.len(), 4, "{out:?}");
    assert_eq!(out[2], doc! { "_id": 5, "arr": Bson::Null, "n": Bson::Null });
    assert_eq!(out[3], doc! { "_id": 6, "n": 5 });
}

#[test]
fn without_the_option_a_null_is_dropped_as_before() {
    let out = run(&[doc! { "$unwind": "$arr" }], input()).unwrap();
    let ids: Vec<i32> = out.iter().map(|d| d.get_i32("_id").unwrap()).collect();
    assert_eq!(ids, vec![1, 1]);
}
