//! A5: an `_id` object of exactly one field reads a missing value as null, so `{k: null}` and `{}`
//! are one group, whichever is scanned first (q7s4g:106, 114). Two fields or more, and a field
//! that is itself an object, keep a missing value apart (q7s4g:107, 110, 112, 113).

use super::common::{json, run};
use bson::{Bson, Document, doc};

/// The six documents of q7s4g:106: `v` null, missing, 5, null, missing and 5.
fn fixture(missing_first: bool) -> Vec<Document> {
    let mut docs = vec![
        doc! { "_id": 11, "v": Bson::Null, "w": 1 },
        doc! { "_id": 12, "w": 1 },
        doc! { "_id": 13, "v": 5, "w": 1 },
        doc! { "_id": 14, "v": Bson::Null },
        doc! { "_id": 15 },
        doc! { "_id": 16, "v": 5, "w": Bson::Null },
    ];
    if missing_first {
        docs.swap(0, 1);
        docs.swap(3, 4);
    }
    docs
}

/// The groups `{$group: {_id: <key>, c: {$sum: 1}}}` makes, as `(_id, c)` in any order.
fn groups(key: &str, docs: Vec<Document>) -> Vec<(Bson, i32)> {
    let key = json(key);
    let out = run(&[doc! { "$group": { "_id": key, "c": { "$sum": 1 } } }], docs).unwrap();
    out.iter().map(|d| (d.get("_id").cloned().unwrap(), d.get_i32("c").unwrap())).collect()
}

fn same(mut got: Vec<(Bson, i32)>, mut want: Vec<(Bson, i32)>) {
    let key = |p: &(Bson, i32)| format!("{:?}", p);
    got.sort_by_key(key);
    want.sort_by_key(key);
    assert_eq!(got, want);
}

fn row(id: &str, c: i32) -> (Bson, i32) {
    (json(id), c)
}

#[test]
fn a_one_field_object_reads_missing_as_null_in_either_scan_order() {
    // g:106 (null first) and g:114 (missing first): `{k: null}` four times, `{k: 5}` twice.
    for missing_first in [false, true] {
        same(
            groups(r#"{"k": "$v"}"#, fixture(missing_first)),
            vec![row(r#"{"k": null}"#, 4), row(r#"{"k": 5}"#, 2)],
        );
    }
    // g:111: the value of the one field is an expression that answers a value.
    same(
        groups(r#"{"k": {"$ifNull": ["$v", "none"]}}"#, fixture(false)),
        vec![row(r#"{"k": "none"}"#, 4), row(r#"{"k": 5}"#, 2)],
    );
}

#[test]
fn two_fields_or_a_nested_object_keep_a_missing_value_apart() {
    // g:107: six groups, a missing field left out of the key.
    same(
        groups(r#"{"k": "$v", "j": "$w"}"#, fixture(false)),
        vec![
            row(r#"{"k": null, "j": 1}"#, 1),
            row(r#"{"j": 1}"#, 1),
            row(r#"{"k": 5, "j": 1}"#, 1),
            row(r#"{"k": null}"#, 1),
            row("{}", 1),
            row(r#"{"k": 5, "j": null}"#, 1),
        ],
    );
    // g:110, 112, 113.
    same(
        groups(r#"{"k": "$v", "j": {"$literal": 1}}"#, fixture(false)),
        vec![
            row(r#"{"k": null, "j": 1}"#, 2),
            row(r#"{"j": 1}"#, 2),
            row(r#"{"k": 5, "j": 1}"#, 2),
        ],
    );
    same(
        groups(r#"{"k": "$v", "k2": "$v"}"#, fixture(false)),
        vec![row(r#"{"k": null, "k2": null}"#, 2), row("{}", 2), row(r#"{"k": 5, "k2": 5}"#, 2)],
    );
    same(
        groups(r#"{"a": {"k": "$v"}}"#, fixture(false)),
        vec![row(r#"{"a": {"k": null}}"#, 2), row(r#"{"a": {}}"#, 2), row(r#"{"a": {"k": 5}}"#, 2)],
    );
}

#[test]
fn a_plain_key_and_an_array_key_read_missing_as_null() {
    // g:108 and g:109, which agreed already.
    same(groups(r#""$v""#, fixture(false)), vec![row("null", 4), row("5", 2)]);
    same(groups(r#"["$v"]"#, fixture(false)), vec![row("[null]", 4), row("[5]", 2)]);
}
