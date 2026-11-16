use bson::{Bson, Document, doc};
use kimmy_query::{filter, shape};

use super::common::{bin, dec, re};

fn docs_q7s3c0001() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Int32(0), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(2), "v": Bson::Int32(5), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(3), "v": Bson::Int32(-5), "k": Bson::Int32(2) },
        doc! { "_id": Bson::Int32(4), "v": Bson::Int64(5), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(5), "v": Bson::Double(5.5_f64), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(6), "v": Bson::Double(-0.0_f64), "k": Bson::Int32(2) },
        doc! { "_id": Bson::Int32(7), "v": Bson::Double(f64::NAN), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(8), "v": Bson::Double(f64::INFINITY), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(9), "v": Bson::Double(f64::NEG_INFINITY), "k": Bson::Int32(2) },
        doc! { "_id": Bson::Int32(10), "v": dec("5"), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(11), "v": dec("NaN"), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(12), "v": Bson::String("5".to_string()), "k": Bson::Int32(2) },
        doc! { "_id": Bson::Int32(13), "v": Bson::String("abc".to_string()), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(14), "v": Bson::String("ABC".to_string()), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(15), "v": Bson::String("".to_string()), "k": Bson::Int32(2) },
        doc! { "_id": Bson::Int32(16), "v": Bson::Boolean(true), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(17), "v": Bson::Boolean(false), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(18), "v": Bson::Null, "k": Bson::Int32(2) },
        doc! { "_id": Bson::Int32(19), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(20), "v": Bson::DateTime(bson::DateTime::from_millis(0)), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(21), "v": Bson::ObjectId(bson::oid::ObjectId::parse_str("000000000000000000000001").unwrap()), "k": Bson::Int32(2) },
        doc! { "_id": Bson::Int32(22), "v": bin(&[1, 2, 3], 0x00), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(23), "v": re("a", ""), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(24), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }), "k": Bson::Int32(2) },
        doc! { "_id": Bson::Int32(25), "v": Bson::Document(doc! { "b": Bson::Int32(2), "a": Bson::Int32(1) }), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(26), "v": Bson::Document(doc! { "a": Bson::Int32(1) }), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(27), "v": Bson::Document(Document::new()), "k": Bson::Int32(2) },
        doc! { "_id": Bson::Int32(28), "v": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(29), "v": Bson::Array(vec![Bson::Int32(5)]), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(30), "v": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)])]), "k": Bson::Int32(2) },
        doc! { "_id": Bson::Int32(31), "v": Bson::Array(vec![Bson::Document(doc! { "a": Bson::Int32(1) })]), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(32), "v": Bson::Array(vec![]), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(33), "v": Bson::Array(vec![Bson::Null]), "k": Bson::Int32(2) },
        doc! { "_id": Bson::Int32(34), "v": Bson::Array(vec![Bson::Int32(5), Bson::String("abc".to_string())]), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(35), "v": Bson::MinKey, "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(36), "v": Bson::MaxKey, "k": Bson::Int32(2) },
        doc! { "_id": Bson::Int32(37), "v": Bson::Timestamp(bson::Timestamp { time: 1, increment: 1 }), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(38), "v": Bson::Array(vec![Bson::Int32(9), Bson::Int32(0)]), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(39), "v": Bson::Array(vec![Bson::Int32(3)]), "k": Bson::Int32(2) },
        doc! { "_id": Bson::Int32(40), "v": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])]), "k": Bson::Int32(0) },
    ]
}

fn docs_q7s3f0274() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Array(vec![]) })]), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Array(vec![])]), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Array(vec![]), Bson::Int32(1)]), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(4), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)]), Bson::Array(vec![])]), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(5), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Array(vec![]) }), Bson::Document(doc! { "b": Bson::Int32(1) })]), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(6), "a": Bson::Array(vec![]), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(7), "a": Bson::Array(vec![Bson::Int32(2)]), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(8), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(9), "a": Bson::Null, "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(10), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(3)]) })]), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(11), "a": Bson::Array(vec![Bson::Array(vec![]), Bson::Array(vec![])]), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(12), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(0)])]), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(13), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Array(vec![Bson::Array(vec![])]) })]), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(14), "a": Bson::Array(vec![Bson::Null, Bson::Array(vec![])]), "k": Bson::Int32(1) },
    ]
}

/// One recorded sorted find: the documents, the filter, the sort and the ids it answered in order.
struct SortCase {
    label: &'static str,
    line: u32,
    docs: fn() -> Vec<Document>,
    filter: Document,
    sort: Document,
    ids: Vec<i32>,
}

fn cases_sorts() -> Vec<SortCase> {
    vec![
        SortCase {
            label: "q7s3c0001",
            line: 4,
            docs: docs_q7s3c0001,
            filter: Document::new(),
            sort: doc! { "v": Bson::Int32(1) },
            ids: vec![
                35, 32, 18, 19, 33, 7, 11, 9, 3, 1, 6, 38, 28, 39, 2, 4, 10, 29, 34, 5, 8, 15, 12,
                14, 13, 27, 26, 31, 24, 25, 40, 30, 22, 21, 17, 16, 20, 37, 23, 36,
            ],
        },
        SortCase {
            label: "q7s3c0002",
            line: 50,
            docs: docs_q7s3c0001,
            filter: Document::new(),
            sort: doc! { "v": Bson::Int32(-1) },
            ids: vec![
                36, 23, 37, 20, 16, 17, 21, 22, 30, 40, 25, 24, 26, 31, 27, 13, 34, 14, 12, 15, 8,
                38, 5, 2, 4, 10, 29, 39, 28, 1, 6, 3, 9, 7, 11, 18, 19, 33, 32, 35,
            ],
        },
        SortCase {
            label: "q7s3c0003",
            line: 96,
            docs: docs_q7s3c0001,
            filter: Document::new(),
            sort: doc! { "v": Bson::Int32(1), "_id": Bson::Int32(1) },
            ids: vec![
                35, 32, 18, 19, 33, 7, 11, 9, 3, 1, 6, 38, 28, 39, 2, 4, 10, 29, 34, 5, 8, 15, 12,
                14, 13, 27, 26, 31, 24, 25, 40, 30, 22, 21, 17, 16, 20, 37, 23, 36,
            ],
        },
        SortCase {
            label: "q7s3c0004",
            line: 142,
            docs: docs_q7s3c0001,
            filter: Document::new(),
            sort: doc! { "v": Bson::Int32(-1), "_id": Bson::Int32(-1) },
            ids: vec![
                36, 23, 37, 20, 16, 17, 21, 22, 30, 40, 25, 24, 31, 26, 27, 34, 13, 14, 12, 15, 8,
                38, 5, 29, 10, 4, 2, 39, 28, 6, 1, 3, 9, 11, 7, 33, 19, 18, 32, 35,
            ],
        },
        SortCase {
            label: "q7s3c0025",
            line: 1108,
            docs: docs_q7s3c0001,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::String("array".to_string()) }) },
            sort: doc! { "v": Bson::Int32(1), "_id": Bson::Int32(1) },
            ids: vec![32, 33, 38, 28, 39, 29, 34, 31, 40, 30],
        },
        SortCase {
            label: "q7s3c0026",
            line: 1154,
            docs: docs_q7s3c0001,
            filter: doc! { "v": Bson::Document(doc! { "$type": Bson::String("array".to_string()) }) },
            sort: doc! { "v": Bson::Int32(-1), "_id": Bson::Int32(1) },
            ids: vec![30, 40, 31, 34, 38, 29, 39, 28, 33, 32],
        },
        SortCase {
            label: "q7s3f0274",
            line: 4863,
            docs: docs_q7s3f0274,
            filter: Document::new(),
            sort: doc! { "a.b": Bson::Int32(1), "_id": Bson::Int32(1) },
            ids: vec![1, 5, 2, 3, 4, 6, 7, 8, 9, 11, 12, 14, 10, 13],
        },
        SortCase {
            label: "q7s3f0275",
            line: 4882,
            docs: docs_q7s3f0274,
            filter: Document::new(),
            sort: doc! { "a.b": Bson::Int32(-1), "_id": Bson::Int32(1) },
            ids: vec![13, 10, 5, 2, 3, 4, 6, 7, 8, 9, 11, 12, 14, 1],
        },
        SortCase {
            label: "q7s3f0276",
            line: 4901,
            docs: docs_q7s3f0274,
            filter: Document::new(),
            sort: doc! { "a": Bson::Int32(1), "_id": Bson::Int32(1) },
            ids: vec![6, 8, 9, 14, 3, 7, 5, 1, 10, 13, 2, 4, 11, 12],
        },
        SortCase {
            label: "q7s3f0277",
            line: 4920,
            docs: docs_q7s3f0274,
            filter: Document::new(),
            sort: doc! { "a": Bson::Int32(-1), "_id": Bson::Int32(1) },
            ids: vec![4, 12, 2, 3, 11, 14, 13, 10, 1, 5, 7, 8, 9, 6],
        },
        SortCase {
            label: "q7s3f0278",
            line: 4939,
            docs: docs_q7s3f0274,
            filter: Document::new(),
            sort: doc! { "a": Bson::Int32(1), "k": Bson::Int32(1), "_id": Bson::Int32(1) },
            ids: vec![6, 9, 8, 14, 3, 7, 5, 1, 10, 13, 11, 2, 4, 12],
        },
        SortCase {
            label: "q7s3f0279",
            line: 4958,
            docs: docs_q7s3f0274,
            filter: Document::new(),
            sort: doc! { "k": Bson::Int32(1), "a": Bson::Int32(1), "_id": Bson::Int32(1) },
            ids: vec![9, 3, 7, 5, 1, 13, 11, 6, 8, 14, 10, 2, 4, 12],
        },
        SortCase {
            label: "q7s3f0280",
            line: 4977,
            docs: docs_q7s3f0274,
            filter: Document::new(),
            sort: doc! { "k": Bson::Int32(-1), "a": Bson::Int32(-1), "_id": Bson::Int32(1) },
            ids: vec![4, 12, 2, 14, 10, 8, 6, 3, 11, 13, 1, 5, 7, 9],
        },
        SortCase {
            label: "q7s3f0281",
            line: 4996,
            docs: docs_q7s3f0274,
            filter: Document::new(),
            sort: doc! { "a.b": Bson::Int32(1), "k": Bson::Int32(-1), "_id": Bson::Int32(1) },
            ids: vec![1, 5, 2, 4, 6, 8, 12, 14, 3, 7, 9, 11, 10, 13],
        },
    ]
}

/// What the sort of the case's documents, filtered, answers: the ids in order, by the sort
/// every route uses ([`shape::sort`]), and by the sort that can be stopped by a deadline, which
/// must give the same order.
fn answered(case: &SortCase) -> Vec<i32> {
    let parsed = filter::parse(&case.filter).expect("a filter");
    let docs: Vec<Document> =
        (case.docs)().into_iter().filter(|d| filter::matches(&parsed, d).unwrap()).collect();
    let keys = shape::parse_sort(&case.sort).expect("a sort");
    let mut plain = docs.clone();
    shape::sort(&keys, &mut plain);
    let stoppable = shape::sort_until_deadline(&keys, docs).expect("a sort");
    // By the ids they return: the documents can hold a NaN, which is not equal to itself.
    let ids = |docs: &[Document]| -> Vec<i32> {
        docs.iter().map(|d| d.get_i32("_id").expect("an Int32 _id")).collect()
    };
    assert_eq!(ids(&plain), ids(&stoppable), "{}: the two sorts differ", case.label);
    ids(&plain)
}

fn assert_sorted(labels: &[&str]) {
    let mut checked = 0;
    for case in cases_sorts().iter().filter(|c| labels.contains(&c.label)) {
        assert_eq!(
            answered(case),
            case.ids,
            "{} (line {}): sort {}, filter {}",
            case.label,
            case.line,
            case.sort,
            case.filter
        );
        checked += 1;
    }
    assert_eq!(checked, labels.len(), "a label is not in the table: {labels:?}");
}

/// The 40 mixed values of q7s3c0001 to c0004 (q7s3c.out:4 to 188): `[]` sorts first ascending,
/// below `null`, a missing field and `[null]` and above the one `MinKey`, and in descending
/// order it is second to last, just above the `MinKey`. It sorted among the arrays, after
/// `{b:2,a:1}` and before `[[1,2]]`, in both directions.
#[test]
fn an_empty_array_sorts_between_minkey_and_null() {
    assert_sorted(&["q7s3c0001", "q7s3c0002", "q7s3c0003", "q7s3c0004"]);
}

/// The same over the arrays alone (q7s3c0025 and c0026, q7s3c.out:1108 and 1154): `{v: {$type:
/// "array"}}`, sorted both ways.
#[test]
fn an_empty_array_sorts_first_among_arrays() {
    assert_sorted(&["q7s3c0025", "q7s3c0026"]);
}

/// The 14 documents of q7s3f0274 to f0281 (q7s3f.out:4863 to 5014), `[]` at every depth: an
/// empty array that is the value a path reaches (`a: []`, `a.b` over `a: [{b: []}]`) is the
/// lowest key but `MinKey`, one that is an element of the array reached (`a: [[]]`,
/// `[[1], []]`, `[null, []]`) is the array `[]` among the arrays, and the least of them. Both
/// directions, by a path through documents, and as a second key.
#[test]
fn an_empty_array_at_any_depth_sorts_as_recorded() {
    assert_sorted(&[
        "q7s3f0274",
        "q7s3f0275",
        "q7s3f0276",
        "q7s3f0277",
        "q7s3f0278",
        "q7s3f0279",
        "q7s3f0280",
        "q7s3f0281",
    ]);
}

/// The sort keys the pipeline's `$sort` and `$setWindowFields` take once and compare by
/// [`shape::compare_keys`] order a collection as [`shape::compare`] does.
#[test]
fn the_keys_taken_once_order_as_the_documents_do() {
    for case in cases_sorts() {
        let keys = shape::parse_sort(&case.sort).unwrap();
        let docs = (case.docs)();
        let keyed: Vec<Vec<Bson>> =
            docs.iter().map(|d| shape::sort_keys(&keys, d).unwrap()).collect();
        for (i, a) in docs.iter().enumerate() {
            for (j, b) in docs.iter().enumerate() {
                assert_eq!(
                    shape::compare(&keys, a, b),
                    shape::compare_keys(&keys, &keyed[i], &keyed[j]),
                    "{}: documents {i} and {j}",
                    case.label
                );
            }
        }
    }
}
