use super::common::{Case, assert_recorded, dec, pick, re};
use bson::{Bson, Document, doc};
use kimmy_query::filter;
use kimmy_query::seed::implied_equalities;

fn docs_q7s3a0001() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(2), "v": Bson::Int32(2) },
        doc! { "_id": Bson::Int32(3), "v": Bson::Array(vec![Bson::Int32(1)]) },
        doc! { "_id": Bson::Int32(4), "v": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
        doc! { "_id": Bson::Int32(5), "v": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])]) },
        doc! { "_id": Bson::Int32(6), "v": Bson::Null },
        doc! { "_id": Bson::Int32(7) },
        doc! { "_id": Bson::Int32(8), "v": Bson::String("x".to_string()) },
        doc! { "_id": Bson::Int32(9), "v": Bson::Array(vec![Bson::Int32(1), Bson::Int32(1)]) },
        doc! { "_id": Bson::Int32(10), "v": Bson::Array(vec![Bson::Int32(1), Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])]) },
    ]
}
fn docs_q7s3a0023() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "arr": Bson::Array(vec![Bson::Document(doc! { "a": Bson::Int32(1) }), Bson::Document(doc! { "a": Bson::Int32(2) })]) },
        doc! { "_id": Bson::Int32(2), "arr": Bson::Array(vec![Bson::Document(doc! { "a": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) })]) },
        doc! { "_id": Bson::Int32(3), "arr": Bson::Array(vec![Bson::Document(doc! { "a": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(4), "arr": Bson::Array(vec![Bson::Document(doc! { "a": Bson::Int32(2) }), Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "a": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(5), "arr": Bson::Array(vec![]) },
        doc! { "_id": Bson::Int32(6) },
        doc! { "_id": Bson::Int32(7), "arr": Bson::Document(doc! { "a": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }) },
        doc! { "_id": Bson::Int32(8), "arr": Bson::Array(vec![Bson::Document(doc! { "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])]) })]) },
    ]
}
fn docs_q7s3b0140() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Int32(5) },
        doc! { "_id": Bson::Int32(2), "a": Bson::Null },
        doc! { "_id": Bson::Int32(3) },
        doc! { "_id": Bson::Int32(4), "a": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]) },
        doc! { "_id": Bson::Int32(5), "a": Bson::Array(vec![]) },
        doc! { "_id": Bson::Int32(6), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]), Bson::Array(vec![Bson::Int32(3)])]) },
        doc! { "_id": Bson::Int32(7), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]) },
        doc! { "_id": Bson::Int32(8), "a": Bson::Document(doc! { "b": Bson::Int32(1), "c": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }) },
        doc! { "_id": Bson::Int32(9), "a": Bson::String("abc".to_string()) },
        doc! { "_id": Bson::Int32(10), "a": Bson::Array(vec![Bson::Null]) },
        doc! { "_id": Bson::Int32(11), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Null }), Bson::Document(doc! { "c": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(12), "a": Bson::Double(7.5_f64) },
        doc! { "_id": Bson::Int32(13), "a": Bson::Array(vec![Bson::Int32(5), Bson::String("x".to_string()), Bson::Null]) },
        doc! { "_id": Bson::Int32(14), "a": Bson::Document(doc! { "b": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(2) })]) }) },
        doc! { "_id": Bson::Int32(15), "a": Bson::Boolean(true) },
        doc! { "_id": Bson::Int32(16), "a": Bson::DateTime(bson::DateTime::from_millis(0)) },
        doc! { "_id": Bson::Int32(17), "a": Bson::Array(vec![Bson::Int32(1), Bson::Array(vec![Bson::Int32(2), Bson::Array(vec![Bson::Int32(3)])])]) },
        doc! { "_id": Bson::Int32(18), "a": Bson::String("ABC".to_string()) },
        doc! { "_id": Bson::Int32(19), "a": Bson::Int64(8) },
        doc! { "_id": Bson::Int32(20), "a": dec("9") },
        doc! { "_id": Bson::Int32(21), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }), Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(3)]) })]) },
        doc! { "_id": Bson::Int32(22), "a": Bson::Document(Document::new()) },
        doc! { "_id": Bson::Int32(23), "a": Bson::Array(vec![Bson::Array(vec![])]) },
        doc! { "_id": Bson::Int32(24), "a": re("abc", "") },
    ]
}
fn cases_r8_all() -> Vec<Case> {
    vec![
        Case {
            label: "q7s3a0001",
            line: 2,
            docs: docs_q7s3a0001,
            filter: doc! { "v": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(1), Bson::Int32(1)]) }) },
            ids: Some(vec![1, 3, 4, 9, 10]),
        },
        Case {
            label: "q7s3a0003",
            line: 32,
            docs: docs_q7s3a0001,
            filter: doc! { "v": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(1)]) }) },
            ids: Some(vec![1, 3, 4, 9, 10]),
        },
        Case {
            label: "q7s3a0005",
            line: 62,
            docs: docs_q7s3a0001,
            filter: doc! { "v": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }) },
            ids: Some(vec![4]),
        },
        Case {
            label: "q7s3a0007",
            line: 92,
            docs: docs_q7s3a0001,
            filter: doc! { "v": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(2), Bson::Int32(1)]) }) },
            ids: Some(vec![4]),
        },
        Case {
            label: "q7s3a0009",
            line: 122,
            docs: docs_q7s3a0001,
            filter: doc! { "v": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])]) }) },
            ids: Some(vec![4, 5, 10]),
        },
        Case {
            label: "q7s3a0011",
            line: 152,
            docs: docs_q7s3a0001,
            filter: doc! { "v": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Null]) }) },
            ids: Some(vec![6, 7]),
        },
        Case {
            label: "q7s3a0013",
            line: 182,
            docs: docs_q7s3a0001,
            filter: doc! { "v": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::String("x".to_string())]) }) },
            ids: Some(vec![8]),
        },
        Case {
            label: "q7s3a0015",
            line: 212,
            docs: docs_q7s3a0001,
            filter: doc! { "v": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(1), Bson::Null]) }) },
            ids: Some(vec![]),
        },
        Case {
            label: "q7s3a0017",
            line: 242,
            docs: docs_q7s3a0001,
            filter: doc! { "v": Bson::Document(doc! { "$all": Bson::Array(vec![]) }) },
            ids: Some(vec![]),
        },
        Case {
            label: "q7s3a0019",
            line: 272,
            docs: docs_q7s3a0001,
            filter: doc! { "v": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(1), Bson::Int32(1), Bson::Int32(1)]) }) },
            ids: Some(vec![1, 3, 4, 9, 10]),
        },
        Case {
            label: "q7s3a0021",
            line: 302,
            docs: docs_q7s3a0001,
            filter: doc! { "v": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Document(doc! { "$eq": Bson::Int32(1) })]) }) },
            ids: None,
        },
        Case {
            label: "q7s3a0023",
            line: 332,
            docs: docs_q7s3a0023,
            filter: doc! { "arr.a": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }) },
            ids: Some(vec![1, 2, 4, 7]),
        },
        Case {
            label: "q7s3a0025",
            line: 358,
            docs: docs_q7s3a0023,
            filter: doc! { "arr.a": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(1)]) }) },
            ids: Some(vec![1, 2, 3, 4, 7]),
        },
        Case {
            label: "q7s3a0027",
            line: 384,
            docs: docs_q7s3a0023,
            filter: doc! { "arr.a": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(2), Bson::Int32(2)]) }) },
            ids: Some(vec![1, 2, 4, 7]),
        },
        Case {
            label: "q7s3a0029",
            line: 410,
            docs: docs_q7s3a0023,
            filter: doc! { "arr.a": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])]) }) },
            ids: Some(vec![2, 7, 8]),
        },
        Case {
            label: "q7s3a0031",
            line: 436,
            docs: docs_q7s3a0023,
            filter: doc! { "arr.a": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(1), Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])]) }) },
            ids: Some(vec![2, 7]),
        },
        Case {
            label: "q7s3a0033",
            line: 462,
            docs: docs_q7s3a0023,
            filter: doc! { "arr.0.a": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }) },
            ids: Some(vec![2]),
        },
        Case {
            label: "q7s3a0035",
            line: 488,
            docs: docs_q7s3a0023,
            filter: doc! { "arr.a": Bson::Document(doc! { "$all": Bson::Array(vec![]) }) },
            ids: Some(vec![]),
        },
        Case {
            label: "q7s3b0140",
            line: 4035,
            docs: docs_q7s3b0140,
            filter: doc! { "a": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }) },
            ids: Some(vec![4]),
        },
        Case {
            label: "q7s3b0141",
            line: 4064,
            docs: docs_q7s3b0140,
            filter: doc! { "a": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(1)]) }) },
            ids: Some(vec![4, 17]),
        },
        Case {
            label: "q7s3b0142",
            line: 4093,
            docs: docs_q7s3b0140,
            filter: doc! { "a": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(5)]) }) },
            ids: Some(vec![1, 13]),
        },
        Case {
            label: "q7s3b0143",
            line: 4122,
            docs: docs_q7s3b0140,
            filter: doc! { "a": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])]) }) },
            ids: Some(vec![6]),
        },
        Case {
            label: "q7s3b0144",
            line: 4151,
            docs: docs_q7s3b0140,
            filter: doc! { "a": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::String("x".to_string()), Bson::Null]) }) },
            ids: Some(vec![13]),
        },
        Case {
            label: "q7s3b0145",
            line: 4180,
            docs: docs_q7s3b0140,
            filter: doc! { "a": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Null]) }) },
            ids: Some(vec![2, 3, 10, 13]),
        },
        Case {
            label: "q7s3b0146",
            line: 4209,
            docs: docs_q7s3b0140,
            filter: doc! { "a": Bson::Document(doc! { "$all": Bson::Array(vec![]) }) },
            ids: Some(vec![]),
        },
        Case {
            label: "q7s3b0147",
            line: 4238,
            docs: docs_q7s3b0140,
            filter: doc! { "a": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) })]) }) },
            ids: Some(vec![7]),
        },
        Case {
            label: "q7s3b0148",
            line: 4267,
            docs: docs_q7s3b0140,
            filter: doc! { "a": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Array(vec![])]) }) },
            ids: Some(vec![5, 23]),
        },
        Case {
            label: "q7s3b0149",
            line: 4296,
            docs: docs_q7s3b0140,
            filter: doc! { "a": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::String("abc".to_string())]) }) },
            ids: Some(vec![9]),
        },
        Case {
            label: "q7s3b0150",
            line: 4325,
            docs: docs_q7s3b0140,
            filter: doc! { "a.b": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }) },
            ids: Some(vec![7, 21]),
        },
        Case {
            label: "q7s3b0151",
            line: 4354,
            docs: docs_q7s3b0140,
            filter: doc! { "a.b": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(1), Bson::Int32(3)]) }) },
            ids: Some(vec![21]),
        },
        Case {
            label: "q7s3b0152",
            line: 4383,
            docs: docs_q7s3b0140,
            filter: doc! { "a": Bson::Document(doc! { "$all": Bson::Int32(5) }) },
            ids: None,
        },
        Case {
            label: "q7s3b0153",
            line: 4412,
            docs: docs_q7s3b0140,
            filter: doc! { "a": Bson::Document(doc! { "$not": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }) }) },
            ids: Some(vec![
                1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            ]),
        },
        Case {
            label: "q7s3b0154",
            line: 4441,
            docs: docs_q7s3b0140,
            filter: doc! { "a": Bson::Document(doc! { "$all": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3), Bson::Int32(3)]) }) },
            ids: Some(vec![4]),
        },
    ]
}

/// The whole sweep of `$all` over lists, nested paths and arrays of documents: q7s3a0001 to
/// a0035 (q7s3a.out:2 to 488) over its three document sets, and q7s3b0140 to b0154
/// (q7s3b.out:4035 to 4441) over the 24 documents of set b. Each is answered as the
/// `$and` of an equality per member.
#[test]
fn all_is_an_and_of_equalities_as_recorded() {
    assert_recorded("$all", cases_r8_all());
}

/// The cases that were answered differently: a scalar against two members, a whole array, `null`
/// against a missing field, and members held by different elements of an array of documents.
#[test]
fn all_of_a_scalar_a_whole_array_null_and_spread_members() {
    let cases = pick(
        cases_r8_all(),
        &[
            "q7s3a0001", // v:1 against [1, 1]
            "q7s3a0009", // [[1,2]] against v:[1,2]
            "q7s3a0011", // [null] against a missing v
            "q7s3a0019", // [1, 1, 1]
            "q7s3a0023", // arr.a [1, 2] over [{a:1},{a:2}]
            "q7s3a0027", // arr.a [2, 2]
            "q7s3a0031", // arr.a [1, [1,2]]
            "q7s3b0148", // [[]] against []
            "q7s3b0151", // a.b [1, 3] over [{b:[1,2]},{b:[3]}]
        ],
    );
    assert_recorded("$all, the cases that differed", cases);
}

/// A `$all` of two or more values as an upsert filter is refused, as the path matched twice, and
/// of one value it seeds that value (q7s3a0041 to a0046, q7s3a.out:540 to 568, code 54).
#[test]
fn all_of_two_values_is_the_path_matched_twice_in_an_upsert() {
    for query in [doc! { "v": { "$all": [1, 2] } }, doc! { "v": { "$all": [1, 1] } }] {
        let parsed = filter::parse(&query).unwrap();
        let mut out = Document::new();
        let err = implied_equalities(&parsed, &mut out, &mut Vec::new()).unwrap_err();
        assert!(err.contains("path 'v' is matched twice"), "{query}: {err}");
    }
    let parsed = filter::parse(&doc! { "v": { "$all": [1] } }).unwrap();
    let mut out = Document::new();
    implied_equalities(&parsed, &mut out, &mut Vec::new()).unwrap();
    assert_eq!(out, doc! { "v": 1 });
}
