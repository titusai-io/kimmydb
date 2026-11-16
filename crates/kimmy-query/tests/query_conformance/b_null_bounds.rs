use super::common::{Case, dec, re};
use bson::{Bson, Document, doc};

fn docs_q7s3b0074() -> Vec<Document> {
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
pub fn cases_r4_b() -> Vec<Case> {
    vec![
        Case {
            label: "q7s3b0074",
            line: 2120,
            docs: docs_q7s3b0074,
            filter: doc! { "a": Bson::Document(doc! { "$gt": Bson::Null }) },
            ids: Some(vec![]),
        },
        Case {
            label: "q7s3b0075",
            line: 2149,
            docs: docs_q7s3b0074,
            filter: doc! { "a": Bson::Document(doc! { "$gte": Bson::Null }) },
            ids: Some(vec![2, 3, 10, 13]),
        },
        Case {
            label: "q7s3b0076",
            line: 2178,
            docs: docs_q7s3b0074,
            filter: doc! { "a": Bson::Document(doc! { "$lt": Bson::Null }) },
            ids: Some(vec![]),
        },
        Case {
            label: "q7s3b0077",
            line: 2207,
            docs: docs_q7s3b0074,
            filter: doc! { "a": Bson::Document(doc! { "$lte": Bson::Null }) },
            ids: Some(vec![2, 3, 10, 13]),
        },
    ]
}
