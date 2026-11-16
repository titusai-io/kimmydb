//! Generated from the recordings q7s3g, q7s3h and q7s3j (reference 6.0.5): documents compared with
//! documents, by the sort, `$cmp`, `$max`, `$min`, `$group` and the update operators. The label is
//! the case's, the line its place in the set's `.out`.

use super::common::dec;
use super::resolver_sorts::{Outcome, Rec, Run};
use bson::{Bson, Document, doc};

fn docs_b5a06ac6() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(5), Bson::Document(doc! { "c": Bson::Int32(9) })]) },
        doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)]), Bson::Document(doc! { "c": Bson::Int32(9) })]) },
        doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::String("x".to_string()), Bson::Document(doc! { "c": Bson::Int32(9) })]) },
        doc! { "_id": Bson::Int32(4), "a": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(5), "a": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(2) }), Bson::Int32(0)]) },
        doc! { "_id": Bson::Int32(6), "a": Bson::Array(vec![Bson::Boolean(true), Bson::Document(doc! { "c": Bson::Int32(3) })]) },
        doc! { "_id": Bson::Int32(7), "a": Bson::Array(vec![Bson::Null, Bson::Document(doc! { "c": Bson::Int32(5) })]) },
        doc! { "_id": Bson::Int32(8), "a": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Int32(7)]) },
        doc! { "_id": Bson::Int32(9), "a": Bson::Array(vec![Bson::Int32(5), Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Array(vec![Bson::Int32(2)])]) },
        doc! { "_id": Bson::Int32(10), "a": Bson::Array(vec![Bson::Double(f64::NAN), Bson::Document(doc! { "c": Bson::Int32(4) })]) },
        doc! { "_id": Bson::Int32(11), "a": Bson::Array(vec![Bson::Document(doc! { "c": Bson::String("z".to_string()) }), Bson::String("x".to_string())]) },
        doc! { "_id": Bson::Int32(12), "a": Bson::Array(vec![Bson::DateTime(bson::DateTime::from_millis(0)), Bson::Document(doc! { "c": Bson::Int32(6) })]) },
        doc! { "_id": Bson::Int32(13), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(1)]) },
        doc! { "_id": Bson::Int32(14), "a": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Array(vec![Bson::Int32(8), Bson::Int32(2)]) })]) },
        doc! { "_id": Bson::Int32(15) },
    ]
}

fn docs_304ffe11() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(2), "v": Bson::Document(doc! { "b": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(3), "v": Bson::Document(doc! { "c": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(4), "v": Bson::Document(doc! { "A": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(5), "v": Bson::Document(doc! { "aa": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(6), "v": Bson::Document(doc! { "": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(7), "v": Bson::Document(doc! { "1": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(8), "v": Bson::Document(doc! { "0": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(9), "v": Bson::Document(doc! { "_": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(10), "v": Bson::Document(doc! { "a": Bson::Int32(2) }) },
        doc! { "_id": Bson::Int32(11), "v": Bson::Document(doc! { "b": Bson::Int32(0) }) },
        doc! { "_id": Bson::Int32(12), "v": Bson::Document(doc! { "B": Bson::Int32(1) }) },
    ]
}

fn docs_56ebc6f4() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(2), "v": Bson::Document(doc! { "b": Bson::String("x".to_string()) }) },
        doc! { "_id": Bson::Int32(3), "v": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
        doc! { "_id": Bson::Int32(4), "v": Bson::Document(doc! { "b": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(5), "v": Bson::Document(doc! { "a": Bson::Null }) },
        doc! { "_id": Bson::Int32(6), "v": Bson::Document(doc! { "b": Bson::Null }) },
        doc! { "_id": Bson::Int32(7), "v": Bson::Document(doc! { "c": Bson::Boolean(true) }) },
        doc! { "_id": Bson::Int32(8), "v": Bson::Document(doc! { "a": Bson::Boolean(true) }) },
        doc! { "_id": Bson::Int32(9), "v": Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(1)]) }) },
        doc! { "_id": Bson::Int32(10), "v": Bson::Document(doc! { "a": Bson::Array(vec![Bson::Int32(1)]) }) },
    ]
}

fn docs_616832a4() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(Document::new()) },
        doc! { "_id": Bson::Int32(2), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(3), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
        doc! { "_id": Bson::Int32(4), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2), "c": Bson::Int32(3) }) },
        doc! { "_id": Bson::Int32(5), "v": Bson::Document(doc! { "b": Bson::Int32(2) }) },
        doc! { "_id": Bson::Int32(6), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(7), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(3) }) },
        doc! { "_id": Bson::Int32(8), "v": Bson::Document(doc! { "a": Bson::Int32(0), "b": Bson::Int32(9) }) },
        doc! { "_id": Bson::Int32(9), "v": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(0) }) },
        doc! { "_id": Bson::Int32(10), "v": Bson::Document(doc! { "a": Bson::Int32(2) }) },
    ]
}

fn docs_bcc9146d() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
        doc! { "_id": Bson::Int32(2), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(2) }) }) },
        doc! { "_id": Bson::Int32(3), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
        doc! { "_id": Bson::Int32(4), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1), "c": Bson::Int32(1) }) }) },
        doc! { "_id": Bson::Int32(5), "v": Bson::Document(doc! { "a": Bson::Document(Document::new()) }) },
        doc! { "_id": Bson::Int32(6), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Document(doc! { "c": Bson::Int32(1) }) }) }) },
        doc! { "_id": Bson::Int32(7), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::String("x".to_string()) }) }) },
        doc! { "_id": Bson::Int32(8), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Null }) }) },
        doc! { "_id": Bson::Int32(9), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(10), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
        doc! { "_id": Bson::Int32(11), "v": Bson::Document(doc! { "b": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
    ]
}

fn docs_c14a5782() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
        doc! { "_id": Bson::Int32(2), "v": Bson::Document(doc! { "b": Bson::Int32(2), "a": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(3), "v": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) },
        doc! { "_id": Bson::Int32(4), "v": Bson::Document(doc! { "b": Bson::Int32(2), "c": Bson::Int32(3), "a": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(5), "v": Bson::Document(doc! { "c": Bson::Int32(3), "b": Bson::Int32(2), "a": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(6), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2), "c": Bson::Int32(3) }) },
        doc! { "_id": Bson::Int32(7), "v": Bson::Document(doc! { "a": Bson::Int32(2), "b": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(8), "v": Bson::Document(doc! { "b": Bson::Int32(1), "a": Bson::Int32(2) }) },
    ]
}

fn docs_f31a0c95() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(2), "v": Bson::Document(doc! { "a": Bson::Double(1.0) }) },
        doc! { "_id": Bson::Int32(3), "v": Bson::Document(doc! { "a": Bson::Int64(1) }) },
        doc! { "_id": Bson::Int32(4), "v": Bson::Document(doc! { "a": dec("1") }) },
        doc! { "_id": Bson::Int32(5), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
        doc! { "_id": Bson::Int32(6), "v": Bson::Document(doc! { "a": Bson::Double(1.0), "b": Bson::Double(2.0) }) },
        doc! { "_id": Bson::Int32(7), "v": Bson::Document(doc! { "a": Bson::Int64(1), "b": Bson::Int64(2) }) },
        doc! { "_id": Bson::Int32(8), "v": Bson::Document(doc! { "a": dec("1"), "b": dec("2") }) },
        doc! { "_id": Bson::Int32(9), "v": Bson::Document(doc! { "a": Bson::Double(1.5) }) },
        doc! { "_id": Bson::Int32(10), "v": Bson::Document(doc! { "a": Bson::Double(f64::NAN) }) },
        doc! { "_id": Bson::Int32(11), "v": Bson::Document(doc! { "a": Bson::Double(-0.0) }) },
        doc! { "_id": Bson::Int32(12), "v": Bson::Document(doc! { "a": Bson::Int32(0) }) },
    ]
}

fn docs_6f59fc4e() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Int32(1)]) },
        doc! { "_id": Bson::Int32(2), "v": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
        doc! { "_id": Bson::Int32(3), "v": Bson::Array(vec![Bson::Int32(2)]) },
        doc! { "_id": Bson::Int32(4), "v": Bson::Array(vec![Bson::String("a".to_string())]) },
        doc! { "_id": Bson::Int32(5), "v": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)])]) },
        doc! { "_id": Bson::Int32(6), "v": Bson::Array(vec![]) },
        doc! { "_id": Bson::Int32(7), "v": Bson::Array(vec![Bson::Null]) },
        doc! { "_id": Bson::Int32(8), "v": Bson::Array(vec![Bson::Int32(1), Bson::String("a".to_string())]) },
        doc! { "_id": Bson::Int32(9), "v": Bson::Array(vec![Bson::String("a".to_string()), Bson::Int32(1)]) },
        doc! { "_id": Bson::Int32(10), "v": Bson::Array(vec![Bson::Double(1.5)]) },
        doc! { "_id": Bson::Int32(11), "v": Bson::Array(vec![Bson::Array(vec![])]) },
        doc! { "_id": Bson::Int32(12), "v": Bson::Array(vec![Bson::Int32(1), Bson::Int32(1)]) },
        doc! { "_id": Bson::Int32(13), "v": Bson::Array(vec![Bson::Document(doc! { "a": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(14), "v": Bson::Array(vec![Bson::Boolean(true)]) },
        doc! { "_id": Bson::Int32(15), "v": Bson::Array(vec![Bson::Int32(0), Bson::Int32(5)]) },
        doc! { "_id": Bson::Int32(16), "v": Bson::Array(vec![Bson::Int32(1), Bson::Null]) },
    ]
}

fn docs_a8e7b32f() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
    ]
}

fn docs_19cba0b4() -> Vec<Document> {
    vec![doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }]
}

fn docs_0f0bfae5() -> Vec<Document> {
    vec![doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Null }) }]
}

fn docs_0fb61f90() -> Vec<Document> {
    vec![doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Boolean(true) }) }]
}

fn docs_13c1907a() -> Vec<Document> {
    vec![doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::Int32(1) }) }]
}

fn docs_e817904d() -> Vec<Document> {
    vec![doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "c": Bson::Int32(1) }) }]
}

fn docs_ab128755() -> Vec<Document> {
    vec![doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "A": Bson::Int32(1) }) }]
}

fn docs_fbffaf9c() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::String("x".to_string()) }) },
    ]
}

fn docs_280ac96a() -> Vec<Document> {
    vec![doc! { "_id": Bson::Int32(1), "v": Bson::Document(Document::new()) }]
}

fn docs_66696d16() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
    ]
}

fn docs_ddcb3f0e() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2), "c": Bson::Int32(3) }) },
    ]
}

fn docs_d4f65a75() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(2) }) }) },
    ]
}

fn docs_e8fa1b73() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
    ]
}

fn docs_f66da303() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
    ]
}

fn docs_1911c5e3() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1), "c": Bson::Int32(1) }) }) },
    ]
}

fn docs_1d5615d4() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::Int32(2), "a": Bson::Int32(1) }) },
    ]
}

fn docs_8c9e3418() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) },
    ]
}

fn docs_3f703491() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::Int32(2), "c": Bson::Int32(3), "a": Bson::Int32(1) }) },
    ]
}

fn docs_7c45de81() -> Vec<Document> {
    vec![doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Double(1.0) }) }]
}

fn docs_ed655150() -> Vec<Document> {
    vec![doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int64(1) }) }]
}

fn docs_5bd6abdf() -> Vec<Document> {
    vec![doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": dec("1") }) }]
}

fn docs_2cc6a2c9() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Int32(1)]) },
        doc! { "_id": Bson::Int32(2), "v": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
        doc! { "_id": Bson::Int32(3), "v": Bson::Array(vec![Bson::Int32(2)]) },
        doc! { "_id": Bson::Int32(4), "v": Bson::Array(vec![Bson::String("a".to_string())]) },
        doc! { "_id": Bson::Int32(5), "v": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)])]) },
        doc! { "_id": Bson::Int32(6), "v": Bson::Array(vec![]) },
        doc! { "_id": Bson::Int32(7), "v": Bson::Array(vec![Bson::Null]) },
        doc! { "_id": Bson::Int32(8), "v": Bson::Array(vec![Bson::Int32(1), Bson::String("a".to_string())]) },
        doc! { "_id": Bson::Int32(9), "v": Bson::Array(vec![Bson::String("a".to_string()), Bson::Int32(1)]) },
        doc! { "_id": Bson::Int32(10), "v": Bson::Array(vec![Bson::Double(1.5)]) },
        doc! { "_id": Bson::Int32(11), "v": Bson::Array(vec![Bson::Array(vec![])]) },
        doc! { "_id": Bson::Int32(12), "v": Bson::Array(vec![Bson::Int32(1), Bson::Int32(1)]) },
    ]
}

fn docs_727e0e56() -> Vec<Document> {
    vec![doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }]
}

fn docs_1fc43d25() -> Vec<Document> {
    vec![doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Int32(1)]) }]
}

fn docs_ca63c1b9() -> Vec<Document> {
    vec![doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Int32(2)]) }]
}

fn docs_b54803e8() -> Vec<Document> {
    vec![doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::String("a".to_string())]) }]
}

pub fn cases_document_order() -> Vec<Rec> {
    vec![
        Rec {
            label: "q7s3g0098",
            line: 818,
            docs: docs_b5a06ac6,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![12, 6, 9, 2, 14, 11, 8, 1, 3, 7, 10, 5, 4, 13, 15]),
        },
        Rec {
            label: "q7s3g0099",
            line: 838,
            docs: docs_b5a06ac6,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![7, 15, 10, 1, 9, 13, 3, 4, 5, 8, 11, 14, 2, 6, 12]),
        },
        Rec {
            label: "q7s3h0015",
            line: 243,
            docs: docs_304ffe11,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "v": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![6, 8, 7, 4, 12, 9, 1, 10, 5, 11, 2, 3]),
        },
        Rec {
            label: "q7s3h0016",
            line: 260,
            docs: docs_304ffe11,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "v": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![3, 2, 11, 5, 10, 1, 9, 12, 4, 7, 8, 6]),
        },
        Rec {
            label: "q7s3h0029",
            line: 481,
            docs: docs_56ebc6f4,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "v": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![5, 6, 1, 4, 3, 2, 10, 9, 8, 7]),
        },
        Rec {
            label: "q7s3h0030",
            line: 496,
            docs: docs_56ebc6f4,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "v": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![7, 8, 9, 10, 2, 3, 4, 1, 6, 5]),
        },
        Rec {
            label: "q7s3h0043",
            line: 691,
            docs: docs_616832a4,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "v": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 8, 2, 6, 3, 4, 7, 9, 10, 5]),
        },
        Rec {
            label: "q7s3h0044",
            line: 706,
            docs: docs_616832a4,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "v": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![5, 10, 9, 7, 4, 3, 6, 2, 8, 1]),
        },
        Rec {
            label: "q7s3h0057",
            line: 901,
            docs: docs_bcc9146d,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "v": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![9, 5, 8, 10, 1, 4, 2, 3, 7, 6, 11]),
        },
        Rec {
            label: "q7s3h0058",
            line: 917,
            docs: docs_bcc9146d,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "v": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![11, 6, 7, 3, 2, 4, 1, 10, 8, 5, 9]),
        },
        Rec {
            label: "q7s3h0071",
            line: 1125,
            docs: docs_c14a5782,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "v": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 6, 3, 7, 8, 2, 4, 5]),
        },
        Rec {
            label: "q7s3h0072",
            line: 1138,
            docs: docs_c14a5782,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "v": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![5, 4, 2, 8, 7, 3, 6, 1]),
        },
        Rec {
            label: "q7s3h0085",
            line: 1307,
            docs: docs_f31a0c95,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "v": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        },
        Rec {
            label: "q7s3h0086",
            line: 1324,
            docs: docs_f31a0c95,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "v": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![9, 5, 6, 7, 8, 1, 2, 3, 4, 11, 12, 10]),
        },
        Rec {
            label: "q7s3h0099",
            line: 1545,
            docs: docs_6f59fc4e,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "v": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![6, 7, 16, 15, 1, 2, 8, 9, 12, 10, 3, 4, 13, 11, 5, 14]),
        },
        Rec {
            label: "q7s3h0100",
            line: 1566,
            docs: docs_6f59fc4e,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "v": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![14, 5, 11, 13, 4, 8, 9, 15, 2, 3, 10, 1, 12, 16, 7, 6]),
        },
        Rec {
            label: "q7s3j0214",
            line: 1497,
            docs: docs_a8e7b32f,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
            ]),
        },
        Rec {
            label: "q7s3j0215",
            line: 1503,
            docs: docs_a8e7b32f,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0220",
            line: 1577,
            docs: docs_a8e7b32f,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Null }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
            ]),
        },
        Rec {
            label: "q7s3j0221",
            line: 1583,
            docs: docs_a8e7b32f,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Null }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Null }) },
            ]),
        },
        Rec {
            label: "q7s3j0222",
            line: 1589,
            docs: docs_19cba0b4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
            ]),
        },
        Rec {
            label: "q7s3j0223",
            line: 1595,
            docs: docs_19cba0b4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0224",
            line: 1601,
            docs: docs_a8e7b32f,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Null }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
            ]),
        },
        Rec {
            label: "q7s3j0225",
            line: 1607,
            docs: docs_a8e7b32f,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Null }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Null }) },
            ]),
        },
        Rec {
            label: "q7s3j0226",
            line: 1613,
            docs: docs_0f0bfae5,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0227",
            line: 1619,
            docs: docs_0f0bfae5,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Null }) },
            ]),
        },
        Rec {
            label: "q7s3j0228",
            line: 1625,
            docs: docs_0fb61f90,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Array(vec![Bson::Int32(1)]) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Boolean(true) }) },
            ]),
        },
        Rec {
            label: "q7s3j0229",
            line: 1631,
            docs: docs_0fb61f90,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Array(vec![Bson::Int32(1)]) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Array(vec![Bson::Int32(1)]) }) },
            ]),
        },
        Rec {
            label: "q7s3j0230",
            line: 1637,
            docs: docs_304ffe11,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "c": Bson::Document(doc! { "$cmp": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(2), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(3), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(4), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(5), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(6), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(7), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(8), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(9), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(10), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(11), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(12), "c": Bson::Int32(-1) },
            ]),
        },
        Rec {
            label: "q7s3j0231",
            line: 1654,
            docs: docs_304ffe11,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$max": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "b": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "c": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "aa": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(11), "m": Bson::Document(doc! { "b": Bson::Int32(0) }) },
                doc! { "_id": Bson::Int32(12), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0232",
            line: 1671,
            docs: docs_304ffe11,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$min": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "A": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "1": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "0": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "_": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(11), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(12), "m": Bson::Document(doc! { "B": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0233",
            line: 1688,
            docs: docs_304ffe11,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "v": Bson::Document(doc! { "$ne": Bson::Document(doc! { "a": Bson::Int32(1) }) }) }) },
                doc! { "$group": Bson::Document(doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "$max": Bson::String("$v".to_string()) }), "mn": Bson::Document(doc! { "$min": Bson::String("$v".to_string()) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "c": Bson::Int32(1) }), "mn": Bson::Document(doc! { "": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0234",
            line: 1705,
            docs: docs_13c1907a,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0235",
            line: 1711,
            docs: docs_13c1907a,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0236",
            line: 1717,
            docs: docs_304ffe11,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "c": Bson::Document(doc! { "$cmp": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "c": Bson::Int32(1) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(2), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(3), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(4), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(5), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(6), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(7), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(8), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(9), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(10), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(11), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(12), "c": Bson::Int32(-1) },
            ]),
        },
        Rec {
            label: "q7s3j0237",
            line: 1734,
            docs: docs_304ffe11,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$max": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "c": Bson::Int32(1) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "c": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "c": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "c": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "c": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "c": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "c": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "c": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "c": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "c": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "c": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(11), "m": Bson::Document(doc! { "c": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(12), "m": Bson::Document(doc! { "c": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0238",
            line: 1751,
            docs: docs_304ffe11,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$min": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "c": Bson::Int32(1) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "b": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "c": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "A": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "aa": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "1": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "0": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "_": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(11), "m": Bson::Document(doc! { "b": Bson::Int32(0) }) },
                doc! { "_id": Bson::Int32(12), "m": Bson::Document(doc! { "B": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0239",
            line: 1768,
            docs: docs_304ffe11,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "v": Bson::Document(doc! { "$ne": Bson::Document(doc! { "c": Bson::Int32(1) }) }) }) },
                doc! { "$group": Bson::Document(doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "$max": Bson::String("$v".to_string()) }), "mn": Bson::Document(doc! { "$min": Bson::String("$v".to_string()) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "b": Bson::Int32(1) }), "mn": Bson::Document(doc! { "": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0240",
            line: 1785,
            docs: docs_13c1907a,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "c": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0241",
            line: 1791,
            docs: docs_13c1907a,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0242",
            line: 1797,
            docs: docs_19cba0b4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0243",
            line: 1803,
            docs: docs_19cba0b4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0244",
            line: 1809,
            docs: docs_13c1907a,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "c": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0245",
            line: 1815,
            docs: docs_13c1907a,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0246",
            line: 1821,
            docs: docs_e817904d,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "c": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0247",
            line: 1827,
            docs: docs_e817904d,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0248",
            line: 1833,
            docs: docs_ab128755,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "aa": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "aa": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0249",
            line: 1839,
            docs: docs_ab128755,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "aa": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "A": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0250",
            line: 1845,
            docs: docs_56ebc6f4,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "c": Bson::Document(doc! { "$cmp": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(2), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(3), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(4), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(5), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(6), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(7), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(8), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(9), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(10), "c": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3j0251",
            line: 1860,
            docs: docs_56ebc6f4,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$max": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "b": Bson::String("x".to_string()) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "b": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "c": Bson::Boolean(true) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": Bson::Boolean(true) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(1)]) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Array(vec![Bson::Int32(1)]) }) },
            ]),
        },
        Rec {
            label: "q7s3j0252",
            line: 1875,
            docs: docs_56ebc6f4,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$min": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "a": Bson::Null }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "b": Bson::Null }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0253",
            line: 1890,
            docs: docs_56ebc6f4,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "v": Bson::Document(doc! { "$ne": Bson::Document(doc! { "a": Bson::Int32(1) }) }) }) },
                doc! { "$group": Bson::Document(doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "$max": Bson::String("$v".to_string()) }), "mn": Bson::Document(doc! { "$min": Bson::String("$v".to_string()) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "c": Bson::Boolean(true) }), "mn": Bson::Document(doc! { "a": Bson::Null }) },
            ]),
        },
        Rec {
            label: "q7s3j0254",
            line: 1905,
            docs: docs_fbffaf9c,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::String("x".to_string()) }) },
            ]),
        },
        Rec {
            label: "q7s3j0255",
            line: 1911,
            docs: docs_fbffaf9c,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0256",
            line: 1917,
            docs: docs_56ebc6f4,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "c": Bson::Document(doc! { "$cmp": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::String("x".to_string()) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(2), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(3), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(4), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(5), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(6), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(7), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(8), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(9), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(10), "c": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3j0257",
            line: 1932,
            docs: docs_56ebc6f4,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$max": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::String("x".to_string()) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "b": Bson::String("x".to_string()) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "c": Bson::Boolean(true) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": Bson::Boolean(true) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(1)]) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Array(vec![Bson::Int32(1)]) }) },
            ]),
        },
        Rec {
            label: "q7s3j0258",
            line: 1947,
            docs: docs_56ebc6f4,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$min": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::String("x".to_string()) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "b": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "a": Bson::Null }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "b": Bson::Null }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
            ]),
        },
        Rec {
            label: "q7s3j0259",
            line: 1962,
            docs: docs_56ebc6f4,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "v": Bson::Document(doc! { "$ne": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) }) }) },
                doc! { "$group": Bson::Document(doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "$max": Bson::String("$v".to_string()) }), "mn": Bson::Document(doc! { "$min": Bson::String("$v".to_string()) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "c": Bson::Boolean(true) }), "mn": Bson::Document(doc! { "a": Bson::Null }) },
            ]),
        },
        Rec {
            label: "q7s3j0260",
            line: 1977,
            docs: docs_fbffaf9c,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::String("x".to_string()) }) },
            ]),
        },
        Rec {
            label: "q7s3j0261",
            line: 1983,
            docs: docs_fbffaf9c,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
            ]),
        },
        Rec {
            label: "q7s3j0262",
            line: 1989,
            docs: docs_19cba0b4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "b": Bson::String("x".to_string()) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::String("x".to_string()) }) },
            ]),
        },
        Rec {
            label: "q7s3j0263",
            line: 1995,
            docs: docs_19cba0b4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "b": Bson::String("x".to_string()) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0264",
            line: 2001,
            docs: docs_fbffaf9c,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::String("x".to_string()) }) },
            ]),
        },
        Rec {
            label: "q7s3j0265",
            line: 2007,
            docs: docs_fbffaf9c,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
            ]),
        },
        Rec {
            label: "q7s3j0266",
            line: 2013,
            docs: docs_a8e7b32f,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::String("x".to_string()) }) },
            ]),
        },
        Rec {
            label: "q7s3j0267",
            line: 2019,
            docs: docs_a8e7b32f,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0268",
            line: 2025,
            docs: docs_13c1907a,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Null }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0269",
            line: 2031,
            docs: docs_13c1907a,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Null }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Null }) },
            ]),
        },
        Rec {
            label: "q7s3j0270",
            line: 2037,
            docs: docs_616832a4,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "c": Bson::Document(doc! { "$cmp": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(Document::new())]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(2), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(3), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(4), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(5), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(6), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(7), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(8), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(9), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(10), "c": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3j0271",
            line: 2052,
            docs: docs_616832a4,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$max": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(Document::new())]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(Document::new()) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2), "c": Bson::Int32(3) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(3) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": Bson::Int32(0), "b": Bson::Int32(9) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(0) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0272",
            line: 2067,
            docs: docs_616832a4,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$min": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(Document::new())]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(Document::new()) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(Document::new()) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(Document::new()) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(Document::new()) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(Document::new()) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(Document::new()) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(Document::new()) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(Document::new()) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(Document::new()) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(Document::new()) },
            ]),
        },
        Rec {
            label: "q7s3j0273",
            line: 2082,
            docs: docs_616832a4,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "v": Bson::Document(doc! { "$ne": Bson::Document(Document::new()) }) }) },
                doc! { "$group": Bson::Document(doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "$max": Bson::String("$v".to_string()) }), "mn": Bson::Document(doc! { "$min": Bson::String("$v".to_string()) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "b": Bson::Int32(2) }), "mn": Bson::Document(doc! { "a": Bson::Int32(0), "b": Bson::Int32(9) }) },
            ]),
        },
        Rec {
            label: "q7s3j0274",
            line: 2097,
            docs: docs_19cba0b4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(Document::new()) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0275",
            line: 2103,
            docs: docs_19cba0b4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(Document::new()) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(Document::new()) },
            ]),
        },
        Rec {
            label: "q7s3j0276",
            line: 2109,
            docs: docs_616832a4,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "c": Bson::Document(doc! { "$cmp": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(2), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(3), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(4), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(5), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(6), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(7), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(8), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(9), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(10), "c": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3j0277",
            line: 2124,
            docs: docs_616832a4,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$max": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2), "c": Bson::Int32(3) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(3) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(0) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0278",
            line: 2139,
            docs: docs_616832a4,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$min": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(Document::new()) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": Bson::Int32(0), "b": Bson::Int32(9) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0279",
            line: 2154,
            docs: docs_616832a4,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "v": Bson::Document(doc! { "$ne": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) }) }) },
                doc! { "$group": Bson::Document(doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "$max": Bson::String("$v".to_string()) }), "mn": Bson::Document(doc! { "$min": Bson::String("$v".to_string()) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "b": Bson::Int32(2) }), "mn": Bson::Document(Document::new()) },
            ]),
        },
        Rec {
            label: "q7s3j0280",
            line: 2169,
            docs: docs_19cba0b4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0281",
            line: 2175,
            docs: docs_19cba0b4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0282",
            line: 2181,
            docs: docs_280ac96a,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0283",
            line: 2187,
            docs: docs_280ac96a,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(Document::new()) },
            ]),
        },
        Rec {
            label: "q7s3j0284",
            line: 2193,
            docs: docs_19cba0b4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0285",
            line: 2199,
            docs: docs_19cba0b4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0286",
            line: 2205,
            docs: docs_66696d16,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(Document::new()) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0287",
            line: 2211,
            docs: docs_66696d16,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(Document::new()) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(Document::new()) },
            ]),
        },
        Rec {
            label: "q7s3j0288",
            line: 2217,
            docs: docs_ddcb3f0e,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "b": Bson::Int32(2) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0289",
            line: 2223,
            docs: docs_ddcb3f0e,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "b": Bson::Int32(2) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2), "c": Bson::Int32(3) }) },
            ]),
        },
        Rec {
            label: "q7s3j0290",
            line: 2229,
            docs: docs_bcc9146d,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "c": Bson::Document(doc! { "$cmp": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(2), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(3), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(4), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(5), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(6), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(7), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(8), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(9), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(10), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(11), "c": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3j0291",
            line: 2245,
            docs: docs_bcc9146d,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$max": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(2) }) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1), "c": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Document(doc! { "c": Bson::Int32(1) }) }) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::String("x".to_string()) }) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(11), "m": Bson::Document(doc! { "b": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            ]),
        },
        Rec {
            label: "q7s3j0292",
            line: 2261,
            docs: docs_bcc9146d,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$min": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "a": Bson::Document(Document::new()) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Null }) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(11), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
            ]),
        },
        Rec {
            label: "q7s3j0293",
            line: 2277,
            docs: docs_bcc9146d,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "v": Bson::Document(doc! { "$ne": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) }) }) },
                doc! { "$group": Bson::Document(doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "$max": Bson::String("$v".to_string()) }), "mn": Bson::Document(doc! { "$min": Bson::String("$v".to_string()) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "b": Bson::Document(doc! { "a": Bson::Int32(1) }) }), "mn": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0294",
            line: 2293,
            docs: docs_d4f65a75,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(2) }) }) },
            ]),
        },
        Rec {
            label: "q7s3j0295",
            line: 2299,
            docs: docs_d4f65a75,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
            ]),
        },
        Rec {
            label: "q7s3j0296",
            line: 2305,
            docs: docs_bcc9146d,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "c": Bson::Document(doc! { "$cmp": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(2), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(3), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(4), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(5), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(6), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(7), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(8), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(9), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(10), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(11), "c": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3j0297",
            line: 2321,
            docs: docs_bcc9146d,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$max": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Document(doc! { "c": Bson::Int32(1) }) }) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::String("x".to_string()) }) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(11), "m": Bson::Document(doc! { "b": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            ]),
        },
        Rec {
            label: "q7s3j0298",
            line: 2337,
            docs: docs_bcc9146d,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$min": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(2) }) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1), "c": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "a": Bson::Document(Document::new()) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Null }) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
                doc! { "_id": Bson::Int32(11), "m": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
            ]),
        },
        Rec {
            label: "q7s3j0299",
            line: 2353,
            docs: docs_bcc9146d,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "v": Bson::Document(doc! { "$ne": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) }) }) },
                doc! { "$group": Bson::Document(doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "$max": Bson::String("$v".to_string()) }), "mn": Bson::Document(doc! { "$min": Bson::String("$v".to_string()) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "b": Bson::Document(doc! { "a": Bson::Int32(1) }) }), "mn": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0300",
            line: 2369,
            docs: docs_d4f65a75,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
            ]),
        },
        Rec {
            label: "q7s3j0301",
            line: 2375,
            docs: docs_d4f65a75,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(2) }) }) },
            ]),
        },
        Rec {
            label: "q7s3j0302",
            line: 2381,
            docs: docs_e8fa1b73,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(2) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(2) }) }) },
            ]),
        },
        Rec {
            label: "q7s3j0303",
            line: 2387,
            docs: docs_e8fa1b73,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(2) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
            ]),
        },
        Rec {
            label: "q7s3j0304",
            line: 2393,
            docs: docs_d4f65a75,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
            ]),
        },
        Rec {
            label: "q7s3j0305",
            line: 2399,
            docs: docs_d4f65a75,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(2) }) }) },
            ]),
        },
        Rec {
            label: "q7s3j0306",
            line: 2405,
            docs: docs_f66da303,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "c": Bson::Int32(1) }) }) },
            ]),
        },
        Rec {
            label: "q7s3j0307",
            line: 2411,
            docs: docs_f66da303,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1) }) }) },
            ]),
        },
        Rec {
            label: "q7s3j0308",
            line: 2417,
            docs: docs_1911c5e3,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Document(Document::new()) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(doc! { "b": Bson::Int32(1), "c": Bson::Int32(1) }) }) },
            ]),
        },
        Rec {
            label: "q7s3j0309",
            line: 2423,
            docs: docs_1911c5e3,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Document(Document::new()) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Document(Document::new()) }) },
            ]),
        },
        Rec {
            label: "q7s3j0310",
            line: 2429,
            docs: docs_c14a5782,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "c": Bson::Document(doc! { "$cmp": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(2), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(3), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(4), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(5), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(6), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(7), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(8), "c": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3j0311",
            line: 2442,
            docs: docs_c14a5782,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$max": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "b": Bson::Int32(2), "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "b": Bson::Int32(2), "c": Bson::Int32(3), "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "c": Bson::Int32(3), "b": Bson::Int32(2), "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2), "c": Bson::Int32(3) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Int32(2), "b": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "b": Bson::Int32(1), "a": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0312",
            line: 2455,
            docs: docs_c14a5782,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$min": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0313",
            line: 2468,
            docs: docs_c14a5782,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "v": Bson::Document(doc! { "$ne": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) }) }) },
                doc! { "$group": Bson::Document(doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "$max": Bson::String("$v".to_string()) }), "mn": Bson::Document(doc! { "$min": Bson::String("$v".to_string()) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "c": Bson::Int32(3), "b": Bson::Int32(2), "a": Bson::Int32(1) }), "mn": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2), "c": Bson::Int32(3) }) },
            ]),
        },
        Rec {
            label: "q7s3j0314",
            line: 2481,
            docs: docs_1d5615d4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::Int32(2), "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0315",
            line: 2487,
            docs: docs_1d5615d4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0316",
            line: 2493,
            docs: docs_c14a5782,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "c": Bson::Document(doc! { "$cmp": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(2), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(3), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(4), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(5), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(6), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(7), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(8), "c": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3j0317",
            line: 2506,
            docs: docs_c14a5782,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$max": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "b": Bson::Int32(2), "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "b": Bson::Int32(2), "c": Bson::Int32(3), "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "c": Bson::Int32(3), "b": Bson::Int32(2), "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Int32(2), "b": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "b": Bson::Int32(1), "a": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0318",
            line: 2519,
            docs: docs_c14a5782,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$min": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2), "c": Bson::Int32(3) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0319",
            line: 2532,
            docs: docs_c14a5782,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "v": Bson::Document(doc! { "$ne": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) }) }) },
                doc! { "$group": Bson::Document(doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "$max": Bson::String("$v".to_string()) }), "mn": Bson::Document(doc! { "$min": Bson::String("$v".to_string()) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "c": Bson::Int32(3), "b": Bson::Int32(2), "a": Bson::Int32(1) }), "mn": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0320",
            line: 2545,
            docs: docs_1d5615d4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::Int32(2), "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0321",
            line: 2551,
            docs: docs_1d5615d4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0322",
            line: 2557,
            docs: docs_66696d16,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "b": Bson::Int32(2), "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::Int32(2), "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0323",
            line: 2563,
            docs: docs_66696d16,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "b": Bson::Int32(2), "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0324",
            line: 2569,
            docs: docs_1d5615d4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::Int32(2), "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0325",
            line: 2575,
            docs: docs_1d5615d4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0326",
            line: 2581,
            docs: docs_8c9e3418,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1), "c": Bson::Int32(3), "b": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0327",
            line: 2587,
            docs: docs_8c9e3418,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0328",
            line: 2593,
            docs: docs_3f703491,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "c": Bson::Int32(3), "b": Bson::Int32(2), "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "c": Bson::Int32(3), "b": Bson::Int32(2), "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0329",
            line: 2599,
            docs: docs_3f703491,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "c": Bson::Int32(3), "b": Bson::Int32(2), "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "b": Bson::Int32(2), "c": Bson::Int32(3), "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0330",
            line: 2605,
            docs: docs_f31a0c95,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "c": Bson::Document(doc! { "$cmp": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(2), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(3), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(4), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(5), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(6), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(7), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(8), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(9), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(10), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(11), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(12), "c": Bson::Int32(-1) },
            ]),
        },
        Rec {
            label: "q7s3j0331",
            line: 2622,
            docs: docs_f31a0c95,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$max": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "a": Bson::Double(1.0) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Int64(1) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "a": dec("1") }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Double(1.0), "b": Bson::Double(2.0) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Int64(1), "b": Bson::Int64(2) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": dec("1"), "b": dec("2") }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "a": Bson::Double(1.5) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(11), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(12), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0332",
            line: 2639,
            docs: docs_f31a0c95,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$min": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int32(1) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "a": Bson::Double(1.0) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Int64(1) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "a": dec("1") }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Double(f64::NAN) }) },
                doc! { "_id": Bson::Int32(11), "m": Bson::Document(doc! { "a": Bson::Double(-0.0) }) },
                doc! { "_id": Bson::Int32(12), "m": Bson::Document(doc! { "a": Bson::Int32(0) }) },
            ]),
        },
        Rec {
            label: "q7s3j0333",
            line: 2656,
            docs: docs_f31a0c95,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "v": Bson::Document(doc! { "$ne": Bson::Document(doc! { "a": Bson::Int32(1) }) }) }) },
                doc! { "$group": Bson::Document(doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "$max": Bson::String("$v".to_string()) }), "mn": Bson::Document(doc! { "$min": Bson::String("$v".to_string()) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "a": Bson::Double(1.5) }), "mn": Bson::Document(doc! { "a": Bson::Double(f64::NAN) }) },
            ]),
        },
        Rec {
            label: "q7s3j0334",
            line: 2673,
            docs: docs_7c45de81,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Double(1.0) }) },
            ]),
        },
        Rec {
            label: "q7s3j0335",
            line: 2679,
            docs: docs_7c45de81,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Double(1.0) }) },
            ]),
        },
        Rec {
            label: "q7s3j0336",
            line: 2685,
            docs: docs_f31a0c95,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "c": Bson::Document(doc! { "$cmp": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int64(1) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(2), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(3), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(4), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(5), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(6), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(7), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(8), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(9), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(10), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(11), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(12), "c": Bson::Int32(-1) },
            ]),
        },
        Rec {
            label: "q7s3j0337",
            line: 2702,
            docs: docs_f31a0c95,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$max": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int64(1) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "a": Bson::Double(1.0) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Int64(1) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "a": dec("1") }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Double(1.0), "b": Bson::Double(2.0) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Int64(1), "b": Bson::Int64(2) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": dec("1"), "b": dec("2") }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "a": Bson::Double(1.5) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Int64(1) }) },
                doc! { "_id": Bson::Int32(11), "m": Bson::Document(doc! { "a": Bson::Int64(1) }) },
                doc! { "_id": Bson::Int32(12), "m": Bson::Document(doc! { "a": Bson::Int64(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0338",
            line: 2719,
            docs: docs_f31a0c95,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$min": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Document(doc! { "a": Bson::Int64(1) })]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Document(doc! { "a": Bson::Double(1.0) }) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Document(doc! { "a": Bson::Int64(1) }) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Document(doc! { "a": dec("1") }) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Document(doc! { "a": Bson::Int64(1) }) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Document(doc! { "a": Bson::Int64(1) }) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Document(doc! { "a": Bson::Int64(1) }) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Document(doc! { "a": Bson::Int64(1) }) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Document(doc! { "a": Bson::Int64(1) }) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Document(doc! { "a": Bson::Double(f64::NAN) }) },
                doc! { "_id": Bson::Int32(11), "m": Bson::Document(doc! { "a": Bson::Double(-0.0) }) },
                doc! { "_id": Bson::Int32(12), "m": Bson::Document(doc! { "a": Bson::Int32(0) }) },
            ]),
        },
        Rec {
            label: "q7s3j0339",
            line: 2736,
            docs: docs_f31a0c95,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "v": Bson::Document(doc! { "$ne": Bson::Document(doc! { "a": Bson::Int64(1) }) }) }) },
                doc! { "$group": Bson::Document(doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "$max": Bson::String("$v".to_string()) }), "mn": Bson::Document(doc! { "$min": Bson::String("$v".to_string()) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "a": Bson::Double(1.5) }), "mn": Bson::Document(doc! { "a": Bson::Double(f64::NAN) }) },
            ]),
        },
        Rec {
            label: "q7s3j0340",
            line: 2753,
            docs: docs_7c45de81,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int64(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Double(1.0) }) },
            ]),
        },
        Rec {
            label: "q7s3j0341",
            line: 2759,
            docs: docs_7c45de81,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int64(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Double(1.0) }) },
            ]),
        },
        Rec {
            label: "q7s3j0342",
            line: 2765,
            docs: docs_19cba0b4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Double(1.0) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0343",
            line: 2771,
            docs: docs_19cba0b4,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Double(1.0) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0344",
            line: 2777,
            docs: docs_7c45de81,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int64(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Double(1.0) }) },
            ]),
        },
        Rec {
            label: "q7s3j0345",
            line: 2783,
            docs: docs_7c45de81,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int64(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Double(1.0) }) },
            ]),
        },
        Rec {
            label: "q7s3j0346",
            line: 2789,
            docs: docs_ed655150,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int64(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0347",
            line: 2795,
            docs: docs_ed655150,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int64(1) }) },
            ]),
        },
        Rec {
            label: "q7s3j0348",
            line: 2801,
            docs: docs_5bd6abdf,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) },
            ]),
        },
        Rec {
            label: "q7s3j0349",
            line: 2807,
            docs: docs_5bd6abdf,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Document(doc! { "a": Bson::Int32(1), "b": Bson::Int32(2) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Document(doc! { "a": dec("1") }) },
            ]),
        },
        Rec {
            label: "q7s3j0350",
            line: 2813,
            docs: docs_2cc6a2c9,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "c": Bson::Document(doc! { "$cmp": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Array(vec![Bson::Int32(1)])]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(2), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(3), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(4), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(5), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(6), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(7), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(8), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(9), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(10), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(11), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(12), "c": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3j0351",
            line: 2830,
            docs: docs_2cc6a2c9,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$max": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Array(vec![Bson::Int32(1)])]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Array(vec![Bson::Int32(1)]) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Array(vec![Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Array(vec![Bson::String("a".to_string())]) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)])]) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Array(vec![Bson::Int32(1)]) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Array(vec![Bson::Int32(1)]) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Array(vec![Bson::Int32(1), Bson::String("a".to_string())]) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Array(vec![Bson::String("a".to_string()), Bson::Int32(1)]) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Array(vec![Bson::Double(1.5)]) },
                doc! { "_id": Bson::Int32(11), "m": Bson::Array(vec![Bson::Array(vec![])]) },
                doc! { "_id": Bson::Int32(12), "m": Bson::Array(vec![Bson::Int32(1), Bson::Int32(1)]) },
            ]),
        },
        Rec {
            label: "q7s3j0352",
            line: 2847,
            docs: docs_2cc6a2c9,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$min": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Array(vec![Bson::Int32(1)])]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Array(vec![Bson::Int32(1)]) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Array(vec![Bson::Int32(1)]) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Array(vec![Bson::Int32(1)]) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Array(vec![Bson::Int32(1)]) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Array(vec![Bson::Int32(1)]) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Array(vec![]) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Array(vec![Bson::Null]) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Array(vec![Bson::Int32(1)]) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Array(vec![Bson::Int32(1)]) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Array(vec![Bson::Int32(1)]) },
                doc! { "_id": Bson::Int32(11), "m": Bson::Array(vec![Bson::Int32(1)]) },
                doc! { "_id": Bson::Int32(12), "m": Bson::Array(vec![Bson::Int32(1)]) },
            ]),
        },
        Rec {
            label: "q7s3j0353",
            line: 2864,
            docs: docs_2cc6a2c9,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "v": Bson::Document(doc! { "$ne": Bson::Array(vec![Bson::Int32(1)]) }) }) },
                doc! { "$group": Bson::Document(doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "$max": Bson::String("$v".to_string()) }), "mn": Bson::Document(doc! { "$min": Bson::String("$v".to_string()) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Null, "mx": Bson::Array(vec![Bson::Array(vec![])]), "mn": Bson::Array(vec![]) },
            ]),
        },
        Rec {
            label: "q7s3j0354",
            line: 2881,
            docs: docs_727e0e56,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Array(vec![Bson::Int32(1)]) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
            ]),
        },
        Rec {
            label: "q7s3j0355",
            line: 2887,
            docs: docs_727e0e56,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Array(vec![Bson::Int32(1)]) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Int32(1)]) },
            ]),
        },
        Rec {
            label: "q7s3j0356",
            line: 2893,
            docs: docs_2cc6a2c9,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "c": Bson::Document(doc! { "$cmp": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Array(vec![Bson::Int32(2)])]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(2), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(3), "c": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(4), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(5), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(6), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(7), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(8), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(9), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(10), "c": Bson::Int32(-1) },
                doc! { "_id": Bson::Int32(11), "c": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(12), "c": Bson::Int32(-1) },
            ]),
        },
        Rec {
            label: "q7s3j0357",
            line: 2910,
            docs: docs_2cc6a2c9,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$max": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Array(vec![Bson::Int32(2)])]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Array(vec![Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Array(vec![Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Array(vec![Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Array(vec![Bson::String("a".to_string())]) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)])]) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Array(vec![Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Array(vec![Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Array(vec![Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Array(vec![Bson::String("a".to_string()), Bson::Int32(1)]) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Array(vec![Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(11), "m": Bson::Array(vec![Bson::Array(vec![])]) },
                doc! { "_id": Bson::Int32(12), "m": Bson::Array(vec![Bson::Int32(2)]) },
            ]),
        },
        Rec {
            label: "q7s3j0358",
            line: 2927,
            docs: docs_2cc6a2c9,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "m": Bson::Document(doc! { "$min": Bson::Array(vec![Bson::String("$v".to_string()), Bson::Array(vec![Bson::Int32(2)])]) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "m": Bson::Array(vec![Bson::Int32(1)]) },
                doc! { "_id": Bson::Int32(2), "m": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(3), "m": Bson::Array(vec![Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(4), "m": Bson::Array(vec![Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(5), "m": Bson::Array(vec![Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(6), "m": Bson::Array(vec![]) },
                doc! { "_id": Bson::Int32(7), "m": Bson::Array(vec![Bson::Null]) },
                doc! { "_id": Bson::Int32(8), "m": Bson::Array(vec![Bson::Int32(1), Bson::String("a".to_string())]) },
                doc! { "_id": Bson::Int32(9), "m": Bson::Array(vec![Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(10), "m": Bson::Array(vec![Bson::Double(1.5)]) },
                doc! { "_id": Bson::Int32(11), "m": Bson::Array(vec![Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(12), "m": Bson::Array(vec![Bson::Int32(1), Bson::Int32(1)]) },
            ]),
        },
        Rec {
            label: "q7s3j0359",
            line: 2944,
            docs: docs_2cc6a2c9,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "v": Bson::Document(doc! { "$ne": Bson::Array(vec![Bson::Int32(2)]) }) }) },
                doc! { "$group": Bson::Document(doc! { "_id": Bson::Null, "mx": Bson::Document(doc! { "$max": Bson::String("$v".to_string()) }), "mn": Bson::Document(doc! { "$min": Bson::String("$v".to_string()) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Null, "mx": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)])]), "mn": Bson::Array(vec![]) },
            ]),
        },
        Rec {
            label: "q7s3j0360",
            line: 2961,
            docs: docs_727e0e56,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Array(vec![Bson::Int32(2)]) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Int32(2)]) },
            ]),
        },
        Rec {
            label: "q7s3j0361",
            line: 2967,
            docs: docs_727e0e56,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Array(vec![Bson::Int32(2)]) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
            ]),
        },
        Rec {
            label: "q7s3j0362",
            line: 2973,
            docs: docs_1fc43d25,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
            ]),
        },
        Rec {
            label: "q7s3j0363",
            line: 2979,
            docs: docs_1fc43d25,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Int32(1)]) },
            ]),
        },
        Rec {
            label: "q7s3j0364",
            line: 2985,
            docs: docs_727e0e56,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Array(vec![Bson::Int32(2)]) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Int32(2)]) },
            ]),
        },
        Rec {
            label: "q7s3j0365",
            line: 2991,
            docs: docs_727e0e56,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Array(vec![Bson::Int32(2)]) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
            ]),
        },
        Rec {
            label: "q7s3j0366",
            line: 2997,
            docs: docs_ca63c1b9,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Array(vec![Bson::Int32(1)]) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Int32(2)]) },
            ]),
        },
        Rec {
            label: "q7s3j0367",
            line: 3003,
            docs: docs_ca63c1b9,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Array(vec![Bson::Int32(1)]) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Int32(1)]) },
            ]),
        },
        Rec {
            label: "q7s3j0368",
            line: 3009,
            docs: docs_b54803e8,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$max": Bson::Document(doc! { "v": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)])]) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)])]) },
            ]),
        },
        Rec {
            label: "q7s3j0369",
            line: 3015,
            docs: docs_b54803e8,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$min": Bson::Document(doc! { "v": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)])]) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::String("a".to_string())]) },
            ]),
        },
    ]
}
