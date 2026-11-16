//! Rebuilt by `tests/data/resolver_sort_cases_gen.py`; its docstring has the command.
//! Generated from the recordings q7s3f, q7s3g, q7s3i and q7s3j (reference 6.0.5): the sorts
//! over arrays, 16746, `$sortArray` and `$push`'s `$sort`. The label is the case's, the line
//! its place in the set's `.out`.

use super::resolver_sorts::{Outcome, Rec, Run};
use bson::{Bson, Document, doc};

fn docs_2a0623ea() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Null }), Bson::Document(doc! { "c": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(5) }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
        doc! { "_id": Bson::Int32(4), "a": Bson::Array(vec![]) },
        doc! { "_id": Bson::Int32(5), "a": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null })]) },
        doc! { "_id": Bson::Int32(6), "a": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Null })]) },
        doc! { "_id": Bson::Int32(7), "a": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Array(vec![]) })]) },
        doc! { "_id": Bson::Int32(8), "a": Bson::Document(doc! { "c": Bson::Int32(1) }) },
        doc! { "_id": Bson::Int32(9), "a": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(3) }), Bson::Document(doc! { "c": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(10) },
        doc! { "_id": Bson::Int32(11), "a": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(2) }), Bson::Document(doc! { "d": Bson::Int32(0) })]) },
        doc! { "_id": Bson::Int32(12), "a": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(7) })]) },
        doc! { "_id": Bson::Int32(13), "a": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Array(vec![Bson::Int32(5), Bson::Int32(1)]) }), Bson::Document(doc! { "c": Bson::Int32(4) })]) },
        doc! { "_id": Bson::Int32(14), "a": Bson::Array(vec![Bson::Null, Bson::Document(doc! { "c": Bson::Int32(9) })]) },
        doc! { "_id": Bson::Int32(15), "a": Bson::Array(vec![Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) })])]) },
        doc! { "_id": Bson::Int32(16), "a": Bson::Document(doc! { "c": Bson::Null }) },
    ]
}

fn docs_723f9d3b() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6), Bson::Int32(7)]) },
        doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]), Bson::Array(vec![Bson::Int32(3), Bson::Int32(4)])]) },
        doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Int32(1) }), Bson::Document(doc! { "x": Bson::Int32(2) })]) },
        doc! { "_id": Bson::Int32(4), "a": Bson::Document(doc! { "0": Bson::String("x".to_string()), "1": Bson::String("y".to_string()), "01": Bson::String("z".to_string()), "00": Bson::String("w".to_string()) }) },
        doc! { "_id": Bson::Int32(5), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(9) })]) },
        doc! { "_id": Bson::Int32(6), "a": Bson::Array(vec![]) },
        doc! { "_id": Bson::Int32(7) },
        doc! { "_id": Bson::Int32(8), "a": Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]) }) },
        doc! { "_id": Bson::Int32(9), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)])]) },
        doc! { "_id": Bson::Int32(10), "a": Bson::Document(doc! { "0": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }) },
        doc! { "_id": Bson::Int32(11), "a": Bson::String("str".to_string()) },
    ]
}

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

fn docs_34fbfc25() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(9) })]) },
        doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "0": Bson::Int32(2) })]) },
        doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Document(doc! { "0": Bson::Int32(9) })]) },
        doc! { "_id": Bson::Int32(4), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]) },
        doc! { "_id": Bson::Int32(5), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(6) },
        doc! { "_id": Bson::Int32(7), "a": Bson::Document(doc! { "0": Bson::Int32(3) }) },
        doc! { "_id": Bson::Int32(8), "a": Bson::Array(vec![Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(1) })])]) },
    ]
}

fn docs_4c307f5d() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]) },
        doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]) },
        doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Array(vec![Bson::Int32(1)])]) },
        doc! { "_id": Bson::Int32(4) },
        doc! { "_id": Bson::Int32(5), "a": Bson::Document(doc! { "b": Bson::Int32(1) }) },
    ]
}

fn docs_2cbcf973() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(9) })]), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "0": Bson::Int32(2) })]), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Document(doc! { "0": Bson::Int32(9) })]), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(4), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(5), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) })]), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(6), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(7), "a": Bson::Document(doc! { "0": Bson::Int32(3) }), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(8), "a": Bson::Array(vec![Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(1) })])]), "k": Bson::Int32(1) },
    ]
}

fn docs_68d0945c() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(9) })]), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "0": Bson::Int32(2) })]), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Document(doc! { "0": Bson::Int32(9) })]), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(4), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(5), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) })]), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(6), "k": Bson::Int32(0) },
    ]
}

fn docs_b4264950() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]), "k": Bson::Int32(1) },
        doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]), "k": Bson::Int32(0) },
        doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Array(vec![Bson::Int32(1)])]), "k": Bson::Int32(1) },
    ]
}

fn docs_0544cc41() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(9) })]) },
    ]
}

fn docs_75d68306() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "0": Bson::Int32(2) })]) },
    ]
}

fn docs_8a1694a6() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) })]) },
    ]
}

fn docs_f3d7c6d1() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(2)]), Bson::Document(doc! { "0": Bson::Int32(3) })]) },
    ]
}

fn docs_265faa42() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "1": Bson::Int32(5) })]) },
        doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Document(doc! { "1": Bson::Int32(5) }), Bson::Document(doc! { "b": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]) },
        doc! { "_id": Bson::Int32(4) },
        doc! { "_id": Bson::Int32(5), "a": Bson::Array(vec![Bson::Document(doc! { "99999999999": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(6), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) })]) },
    ]
}

fn docs_1feb0573() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(1) }), Bson::Int32(5)]) },
        doc! { "_id": Bson::Int32(2), "x": Bson::Array(vec![Bson::Int32(5), Bson::Int32(3)]) },
        doc! { "_id": Bson::Int32(3), "x": Bson::Array(vec![Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) })]), Bson::Array(vec![Bson::Int32(1)])]) },
        doc! { "_id": Bson::Int32(4), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Int32(1)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) })]) },
        doc! { "_id": Bson::Int32(5), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(1)]) })]) },
    ]
}

fn docs_d3304414() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Int32(1)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) })]) },
    ]
}

fn docs_aade5ac6() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(1)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) })]) },
    ]
}

/// The number of cases `cases_sorts` holds, written by the generator.
pub const CASES_SORTS_COUNT: usize = 172;

pub fn cases_sorts() -> Vec<Rec> {
    vec![
        Rec {
            label: "q7s3f0027",
            line: 548,
            docs: docs_2a0623ea,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.c": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![7, 1, 2, 3, 4, 5, 6, 10, 11, 12, 14, 15, 16, 8, 9, 13]),
        },
        Rec {
            label: "q7s3f0028",
            line: 569,
            docs: docs_2a0623ea,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.c": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![14, 2, 13, 9, 11, 1, 6, 8, 3, 4, 5, 10, 12, 15, 16, 7]),
        },
        Rec {
            label: "q7s3f0029",
            line: 590,
            docs: docs_2a0623ea,
            run: Run::Find { filter: Document::new(), sort: doc! { "a.c": Bson::Int32(1) } },
            outcome: Outcome::Ids(vec![7, 1, 2, 3, 4, 5, 6, 10, 11, 12, 14, 15, 16, 8, 9, 13]),
        },
        Rec {
            label: "q7s3f0030",
            line: 611,
            docs: docs_2a0623ea,
            run: Run::Find { filter: Document::new(), sort: doc! { "a.c": Bson::Int32(-1) } },
            outcome: Outcome::Ids(vec![14, 2, 13, 9, 11, 1, 6, 8, 3, 4, 5, 10, 12, 15, 16, 7]),
        },
        Rec {
            label: "q7s3f0031",
            line: 632,
            docs: docs_2a0623ea,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.d": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 12]),
        },
        Rec {
            label: "q7s3f0032",
            line: 653,
            docs: docs_2a0623ea,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.d": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![12, 2, 11, 1, 3, 4, 5, 6, 7, 8, 9, 10, 13, 14, 15, 16]),
        },
        Rec {
            label: "q7s3f0033",
            line: 674,
            docs: docs_2a0623ea,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.b": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
        },
        Rec {
            label: "q7s3f0034",
            line: 695,
            docs: docs_2a0623ea,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.b": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
        },
        Rec {
            label: "q7s3f0035",
            line: 716,
            docs: docs_2a0623ea,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.c": Bson::Int32(1), "a.d": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![7, 1, 3, 4, 5, 6, 10, 14, 15, 16, 11, 2, 12, 8, 9, 13]),
        },
        Rec {
            label: "q7s3f0036",
            line: 737,
            docs: docs_2a0623ea,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.c": Bson::Int32(-1), "a.d": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![14, 2, 13, 9, 11, 1, 6, 8, 12, 3, 4, 5, 10, 15, 16, 7]),
        },
        Rec {
            label: "q7s3f0037",
            line: 758,
            docs: docs_2a0623ea,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0.c": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![7, 1, 3, 4, 5, 8, 10, 12, 14, 16, 6, 13, 15, 11, 9, 2]),
        },
        Rec {
            label: "q7s3f0038",
            line: 779,
            docs: docs_2a0623ea,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0.c": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![2, 13, 9, 11, 6, 15, 1, 3, 4, 5, 8, 10, 12, 14, 16, 7]),
        },
        Rec {
            label: "q7s3f0039",
            line: 800,
            docs: docs_2a0623ea,
            run: Run::Find {
                filter: doc! { "a.c": Bson::Document(doc! { "$exists": Bson::Boolean(true) }) },
                sort: doc! { "a.c": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![7, 1, 2, 5, 6, 11, 14, 16, 8, 9, 13]),
        },
        Rec {
            label: "q7s3f0040",
            line: 821,
            docs: docs_2a0623ea,
            run: Run::Find {
                filter: doc! { "a.c": Bson::Document(doc! { "$gte": Bson::Int32(1) }) },
                sort: doc! { "a.c": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![14, 2, 13, 9, 11, 1, 6, 8]),
        },
        Rec {
            label: "q7s3f0046",
            line: 922,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3f0047",
            line: 938,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3f0053",
            line: 1034,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.01": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 4]),
        },
        Rec {
            label: "q7s3f0054",
            line: 1050,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.01": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![4, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11]),
        },
        Rec {
            label: "q7s3f0060",
            line: 1146,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.1": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![5, 6, 7, 8, 9, 10, 11, 1, 4, 3, 2]),
        },
        Rec {
            label: "q7s3f0061",
            line: 1162,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.1": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![2, 3, 4, 1, 5, 6, 7, 8, 9, 10, 11]),
        },
        Rec {
            label: "q7s3f0067",
            line: 1258,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.00": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 4]),
        },
        Rec {
            label: "q7s3f0068",
            line: 1274,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.00": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![4, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11]),
        },
        Rec {
            label: "q7s3f0074",
            line: 1370,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.+0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        },
        Rec {
            label: "q7s3f0075",
            line: 1386,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.+0": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        },
        Rec {
            label: "q7s3f0081",
            line: 1482,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.-1": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        },
        Rec {
            label: "q7s3f0082",
            line: 1498,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.-1": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        },
        Rec {
            label: "q7s3f0088",
            line: 1594,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.2": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 1]),
        },
        Rec {
            label: "q7s3f0089",
            line: 1610,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.2": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        },
        Rec {
            label: "q7s3f0095",
            line: 1706,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0.0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3f0096",
            line: 1722,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0.0": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3f0102",
            line: 1818,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0.x": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3f0103",
            line: 1834,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0.x": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3f0109",
            line: 1930,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.1.0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2]),
        },
        Rec {
            label: "q7s3f0110",
            line: 1946,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.1.0": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![2, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        },
        Rec {
            label: "q7s3f0116",
            line: 2042,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0.1": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3f0117",
            line: 2058,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0.1": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3f0123",
            line: 2154,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.01.0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        },
        Rec {
            label: "q7s3f0124",
            line: 2170,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.01.0": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        },
        Rec {
            label: "q7s3f0130",
            line: 2266,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.b.0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 8]),
        },
        Rec {
            label: "q7s3f0131",
            line: 2282,
            docs: docs_723f9d3b,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.b.0": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![8, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11]),
        },
        Rec {
            label: "q7s3g0093",
            line: 718,
            docs: docs_b5a06ac6,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.c": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 4, 14]),
        },
        Rec {
            label: "q7s3g0094",
            line: 738,
            docs: docs_b5a06ac6,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.c": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![11, 1, 2, 3, 14, 12, 7, 10, 6, 5, 4, 9, 8, 13, 15]),
        },
        Rec {
            label: "q7s3g0095",
            line: 758,
            docs: docs_b5a06ac6,
            run: Run::Find { filter: Document::new(), sort: doc! { "a.c": Bson::Int32(1) } },
            outcome: Outcome::Ids(vec![1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 4, 14]),
        },
        Rec {
            label: "q7s3g0096",
            line: 778,
            docs: docs_b5a06ac6,
            run: Run::Find { filter: Document::new(), sort: doc! { "a.c": Bson::Int32(-1) } },
            outcome: Outcome::Ids(vec![11, 1, 2, 3, 14, 12, 7, 10, 6, 5, 4, 9, 8, 13, 15]),
        },
        Rec {
            label: "q7s3g0097",
            line: 798,
            docs: docs_b5a06ac6,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![7, 15, 10, 5, 13, 1, 9, 8, 3, 11, 4, 6, 12, 2, 14]),
        },
        Rec {
            label: "q7s3g0100",
            line: 858,
            docs: docs_b5a06ac6,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.1.c": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![4, 5, 8, 11, 13, 14, 15, 9, 6, 10, 7, 12, 1, 2, 3]),
        },
        Rec {
            label: "q7s3g0105",
            line: 958,
            docs: docs_34fbfc25,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3g0106",
            line: 971,
            docs: docs_34fbfc25,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3g0107",
            line: 984,
            docs: docs_4c307f5d,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![4, 5, 1, 2, 3]),
        },
        Rec {
            label: "q7s3g0108",
            line: 994,
            docs: docs_34fbfc25,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.1": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 5, 6, 7, 8, 4, 2, 3]),
        },
        Rec {
            label: "q7s3g0109",
            line: 1007,
            docs: docs_34fbfc25,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.1": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![3, 2, 4, 1, 5, 6, 7, 8]),
        },
        Rec {
            label: "q7s3g0110",
            line: 1020,
            docs: docs_4c307f5d,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.1": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![4, 5, 1, 2, 3]),
        },
        Rec {
            label: "q7s3g0111",
            line: 1030,
            docs: docs_34fbfc25,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0.0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3g0112",
            line: 1043,
            docs: docs_34fbfc25,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0.0": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3g0113",
            line: 1056,
            docs: docs_4c307f5d,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0.0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 4, 5, 3]),
        },
        Rec {
            label: "q7s3g0114",
            line: 1066,
            docs: docs_34fbfc25,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.b.0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 8]),
        },
        Rec {
            label: "q7s3g0115",
            line: 1079,
            docs: docs_34fbfc25,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.b.0": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 8]),
        },
        Rec {
            label: "q7s3g0116",
            line: 1092,
            docs: docs_4c307f5d,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.b.0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5]),
        },
        Rec {
            label: "q7s3g0117",
            line: 1102,
            docs: docs_34fbfc25,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0.b": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3g0118",
            line: 1115,
            docs: docs_34fbfc25,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0.b": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3g0119",
            line: 1128,
            docs: docs_4c307f5d,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0.b": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 3, 4, 5, 2]),
        },
        Rec {
            label: "q7s3g0120",
            line: 1138,
            docs: docs_34fbfc25,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.00": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 8]),
        },
        Rec {
            label: "q7s3g0121",
            line: 1151,
            docs: docs_34fbfc25,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.00": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 8]),
        },
        Rec {
            label: "q7s3g0122",
            line: 1164,
            docs: docs_4c307f5d,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.00": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5]),
        },
        Rec {
            label: "q7s3g0123",
            line: 1174,
            docs: docs_34fbfc25,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.01": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 8]),
        },
        Rec {
            label: "q7s3g0124",
            line: 1187,
            docs: docs_34fbfc25,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.01": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6, 7, 8]),
        },
        Rec {
            label: "q7s3g0125",
            line: 1200,
            docs: docs_4c307f5d,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.01": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5]),
        },
        Rec {
            label: "q7s3g0126",
            line: 1210,
            docs: docs_2cbcf973,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "k": Bson::Int32(1), "a.0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3g0127",
            line: 1223,
            docs: docs_2cbcf973,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0": Bson::Int32(1), "k": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3g0130",
            line: 1262,
            docs: docs_34fbfc25,
            run: Run::Find {
                filter: doc! { "a.0": Bson::Int32(9) },
                sort: doc! { "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 3]),
        },
        Rec {
            label: "q7s3g0131",
            line: 1275,
            docs: docs_34fbfc25,
            run: Run::Find {
                filter: doc! { "_id": Bson::Document(doc! { "$in": Bson::Array(vec![Bson::Int32(4), Bson::Int32(5)]) }) },
                sort: doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![4, 5]),
        },
        Rec {
            label: "q7s3i0034",
            line: 261,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$sort": Bson::Document(doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3i0035",
            line: 272,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$sort": Bson::Document(doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]), "k": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]), "k": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Array(vec![Bson::Int32(1)])]), "k": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3i0036",
            line: 280,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$sort": Bson::Document(doc! { "a.0": Bson::Int32(-1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3i0037",
            line: 291,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$sort": Bson::Document(doc! { "a.0": Bson::Int32(-1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Array(vec![Bson::Int32(1)])]), "k": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]), "k": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]), "k": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3i0038",
            line: 299,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$sort": Bson::Document(doc! { "a.b": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(9) })]), "k": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "0": Bson::Int32(2) })]), "k": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Document(doc! { "0": Bson::Int32(9) })]), "k": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(4), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]), "k": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(6), "k": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(5), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) })]), "k": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3i0039",
            line: 310,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$sort": Bson::Document(doc! { "a.b": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]), "k": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Array(vec![Bson::Int32(1)])]), "k": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]), "k": Bson::Int32(0) },
            ]),
        },
        Rec {
            label: "q7s3i0040",
            line: 318,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$sort": Bson::Document(doc! { "k": Bson::Int32(1), "a.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3i0041",
            line: 329,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$sort": Bson::Document(doc! { "k": Bson::Int32(1), "a.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]), "k": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]), "k": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Array(vec![Bson::Int32(1)])]), "k": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3i0042",
            line: 337,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "k": Bson::Int32(0) }) },
                doc! { "$sort": Bson::Document(doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3i0043",
            line: 348,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "k": Bson::Int32(0) }) },
                doc! { "$sort": Bson::Document(doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]), "k": Bson::Int32(0) },
            ]),
        },
        Rec {
            label: "q7s3i0044",
            line: 356,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "_id": Bson::Document(doc! { "$gte": Bson::Int32(4) }) }) },
                doc! { "$sort": Bson::Document(doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(6), "k": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(4), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]), "k": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(5), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) })]), "k": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3i0045",
            line: 367,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$match": Bson::Document(doc! { "_id": Bson::Document(doc! { "$gte": Bson::Int32(4) }) }) },
                doc! { "$sort": Bson::Document(doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![]),
        },
        Rec {
            label: "q7s3i0046",
            line: 375,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "$sort": Bson::Document(doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3i0047",
            line: 386,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "a": Bson::Int32(1) }) },
                doc! { "$sort": Bson::Document(doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]) },
                doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Array(vec![Bson::Int32(1)])]) },
            ]),
        },
        Rec {
            label: "q7s3i0048",
            line: 394,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$addFields": Bson::Document(doc! { "x": Bson::String("$a".to_string()) }) },
                doc! { "$sort": Bson::Document(doc! { "x.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3i0049",
            line: 405,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$addFields": Bson::Document(doc! { "x": Bson::String("$a".to_string()) }) },
                doc! { "$sort": Bson::Document(doc! { "x.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]), "k": Bson::Int32(1), "x": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]) },
                doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]), "k": Bson::Int32(0), "x": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Array(vec![Bson::Int32(1)])]), "k": Bson::Int32(1), "x": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Array(vec![Bson::Int32(1)])]) },
            ]),
        },
        Rec {
            label: "q7s3i0050",
            line: 413,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "s": Bson::Document(doc! { "$sortArray": Bson::Document(doc! { "input": Bson::String("$a".to_string()), "sortBy": Bson::Document(doc! { "0": Bson::Int32(1) }) }) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "s": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(9) })]) },
                doc! { "_id": Bson::Int32(2), "s": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "0": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(3), "s": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Document(doc! { "0": Bson::Int32(9) })]) },
                doc! { "_id": Bson::Int32(4), "s": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]) },
                doc! { "_id": Bson::Int32(5), "s": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "s": Bson::Null },
            ]),
        },
        Rec {
            label: "q7s3i0051",
            line: 424,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "s": Bson::Document(doc! { "$sortArray": Bson::Document(doc! { "input": Bson::String("$a".to_string()), "sortBy": Bson::Document(doc! { "0": Bson::Int32(1) }) }) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "s": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]) },
                doc! { "_id": Bson::Int32(2), "s": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(3), "s": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Array(vec![Bson::Int32(1)])]) },
            ]),
        },
        Rec {
            label: "q7s3i0052",
            line: 432,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "s": Bson::Document(doc! { "$sortArray": Bson::Document(doc! { "input": Bson::String("$a".to_string()), "sortBy": Bson::Document(doc! { "b": Bson::Int32(1) }) }) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "s": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(9) })]) },
                doc! { "_id": Bson::Int32(2), "s": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Document(doc! { "b": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "s": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Document(doc! { "0": Bson::Int32(9) })]) },
                doc! { "_id": Bson::Int32(4), "s": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]) },
                doc! { "_id": Bson::Int32(5), "s": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "s": Bson::Null },
            ]),
        },
        Rec {
            label: "q7s3i0053",
            line: 443,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "s": Bson::Document(doc! { "$sortArray": Bson::Document(doc! { "input": Bson::String("$a".to_string()), "sortBy": Bson::Document(doc! { "b": Bson::Int32(1) }) }) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "s": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]) },
                doc! { "_id": Bson::Int32(2), "s": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(3), "s": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Array(vec![Bson::Int32(1)])]) },
            ]),
        },
        Rec {
            label: "q7s3i0054",
            line: 451,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "s": Bson::Document(doc! { "$sortArray": Bson::Document(doc! { "input": Bson::String("$a".to_string()), "sortBy": Bson::Int32(1) }) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "s": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(9) })]) },
                doc! { "_id": Bson::Int32(2), "s": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Document(doc! { "b": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "s": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(9) }), Bson::Array(vec![Bson::Int32(5)])]) },
                doc! { "_id": Bson::Int32(4), "s": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]) },
                doc! { "_id": Bson::Int32(5), "s": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "s": Bson::Null },
            ]),
        },
        Rec {
            label: "q7s3i0055",
            line: 462,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "s": Bson::Document(doc! { "$sortArray": Bson::Document(doc! { "input": Bson::String("$a".to_string()), "sortBy": Bson::Int32(1) }) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "s": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]) },
                doc! { "_id": Bson::Int32(2), "s": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(3), "s": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)]), Bson::Array(vec![Bson::Int32(5)])]) },
            ]),
        },
        Rec {
            label: "q7s3i0056",
            line: 470,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "s": Bson::Document(doc! { "$sortArray": Bson::Document(doc! { "input": Bson::String("$a".to_string()), "sortBy": Bson::Int32(-1) }) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "s": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(9) })]) },
                doc! { "_id": Bson::Int32(2), "s": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "0": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(3), "s": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Document(doc! { "0": Bson::Int32(9) })]) },
                doc! { "_id": Bson::Int32(4), "s": Bson::Array(vec![Bson::Int32(6), Bson::Int32(5)]) },
                doc! { "_id": Bson::Int32(5), "s": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "s": Bson::Null },
            ]),
        },
        Rec {
            label: "q7s3i0057",
            line: 481,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "s": Bson::Document(doc! { "$sortArray": Bson::Document(doc! { "input": Bson::String("$a".to_string()), "sortBy": Bson::Int32(-1) }) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "s": Bson::Array(vec![Bson::Int32(6), Bson::Int32(5)]) },
                doc! { "_id": Bson::Int32(2), "s": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(2) }), Bson::Document(doc! { "b": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "s": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Array(vec![Bson::Int32(1)])]) },
            ]),
        },
        Rec {
            label: "q7s3i0058",
            line: 489,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$group": Bson::Document(doc! { "_id": Bson::String("$k".to_string()), "v": Bson::Document(doc! { "$push": Bson::String("$a".to_string()) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(0), "v": Bson::Array(vec![Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "0": Bson::Int32(2) })]), Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)])]) },
                doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(9) })]), Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Document(doc! { "0": Bson::Int32(9) })]), Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) })])]) },
            ]),
        },
        Rec {
            label: "q7s3i0059",
            line: 500,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$group": Bson::Document(doc! { "_id": Bson::String("$k".to_string()), "v": Bson::Document(doc! { "$push": Bson::String("$a".to_string()) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(0), "v": Bson::Array(vec![Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })])]) },
                doc! { "_id": Bson::Int32(1), "v": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]), Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Array(vec![Bson::Int32(1)])])]) },
            ]),
        },
        Rec {
            label: "q7s3i0060",
            line: 508,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$group": Bson::Document(doc! { "_id": Bson::String("$k".to_string()), "t": Bson::Document(doc! { "$top": Bson::Document(doc! { "sortBy": Bson::Document(doc! { "a.0": Bson::Int32(1) }), "output": Bson::String("$_id".to_string()) }) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(0), "t": Bson::Int32(4) },
                doc! { "_id": Bson::Int32(1), "t": Bson::Int32(5) },
            ]),
        },
        Rec {
            label: "q7s3i0061",
            line: 519,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$group": Bson::Document(doc! { "_id": Bson::String("$k".to_string()), "t": Bson::Document(doc! { "$top": Bson::Document(doc! { "sortBy": Bson::Document(doc! { "a.0": Bson::Int32(1) }), "output": Bson::String("$_id".to_string()) }) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(0), "t": Bson::Int32(2) },
                doc! { "_id": Bson::Int32(1), "t": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3i0062",
            line: 527,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$group": Bson::Document(doc! { "_id": Bson::String("$k".to_string()), "t": Bson::Document(doc! { "$bottom": Bson::Document(doc! { "sortBy": Bson::Document(doc! { "a.0": Bson::Int32(1) }), "output": Bson::String("$_id".to_string()) }) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(0), "t": Bson::Int32(2) },
                doc! { "_id": Bson::Int32(1), "t": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3i0063",
            line: 538,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$group": Bson::Document(doc! { "_id": Bson::String("$k".to_string()), "t": Bson::Document(doc! { "$bottom": Bson::Document(doc! { "sortBy": Bson::Document(doc! { "a.0": Bson::Int32(1) }), "output": Bson::String("$_id".to_string()) }) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(0), "t": Bson::Int32(2) },
                doc! { "_id": Bson::Int32(1), "t": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3i0064",
            line: 546,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$group": Bson::Document(doc! { "_id": Bson::String("$k".to_string()), "t": Bson::Document(doc! { "$top": Bson::Document(doc! { "sortBy": Bson::Document(doc! { "_id": Bson::Int32(1) }), "output": Bson::String("$a".to_string()) }) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(0), "t": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "0": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(1), "t": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(9) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0065",
            line: 557,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$group": Bson::Document(doc! { "_id": Bson::String("$k".to_string()), "t": Bson::Document(doc! { "$top": Bson::Document(doc! { "sortBy": Bson::Document(doc! { "_id": Bson::Int32(1) }), "output": Bson::String("$a".to_string()) }) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(0), "t": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(1), "t": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]) },
            ]),
        },
        Rec {
            label: "q7s3i0066",
            line: 565,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$group": Bson::Document(doc! { "_id": Bson::String("$k".to_string()), "t": Bson::Document(doc! { "$topN": Bson::Document(doc! { "n": Bson::Int32(2), "sortBy": Bson::Document(doc! { "a.0": Bson::Int32(-1) }), "output": Bson::String("$_id".to_string()) }) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(0), "t": Bson::Array(vec![Bson::Int32(2), Bson::Int32(6)]) },
                doc! { "_id": Bson::Int32(1), "t": Bson::Array(vec![Bson::Int32(1), Bson::Int32(3)]) },
            ]),
        },
        Rec {
            label: "q7s3i0067",
            line: 576,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$group": Bson::Document(doc! { "_id": Bson::String("$k".to_string()), "t": Bson::Document(doc! { "$topN": Bson::Document(doc! { "n": Bson::Int32(2), "sortBy": Bson::Document(doc! { "a.0": Bson::Int32(-1) }), "output": Bson::String("$_id".to_string()) }) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(0), "t": Bson::Array(vec![Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(1), "t": Bson::Array(vec![Bson::Int32(1), Bson::Int32(3)]) },
            ]),
        },
        Rec {
            label: "q7s3i0068",
            line: 584,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$sort": Bson::Document(doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
                doc! { "$group": Bson::Document(doc! { "_id": Bson::String("$k".to_string()), "f": Bson::Document(doc! { "$first": Bson::String("$_id".to_string()) }) }) },
            ]),
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3i0069",
            line: 595,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$sort": Bson::Document(doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
                doc! { "$group": Bson::Document(doc! { "_id": Bson::String("$k".to_string()), "f": Bson::Document(doc! { "$first": Bson::String("$_id".to_string()) }) }) },
            ]),
            outcome: Outcome::DocsAsSet(vec![
                doc! { "_id": Bson::Int32(0), "f": Bson::Int32(2) },
                doc! { "_id": Bson::Int32(1), "f": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3i0070",
            line: 603,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$bucketAuto": Bson::Document(doc! { "groupBy": Bson::String("$a.0".to_string()), "buckets": Bson::Int32(2) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Document(doc! { "min": Bson::Null, "max": Bson::Array(vec![Bson::Int32(2)]) }), "count": Bson::Int32(3) },
                doc! { "_id": Bson::Document(doc! { "min": Bson::Array(vec![Bson::Int32(2)]), "max": Bson::Array(vec![Bson::Int32(9)]) }), "count": Bson::Int32(3) },
            ]),
        },
        Rec {
            label: "q7s3i0071",
            line: 614,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$bucketAuto": Bson::Document(doc! { "groupBy": Bson::String("$a.0".to_string()), "buckets": Bson::Int32(2) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Document(doc! { "min": Bson::Array(vec![]), "max": Bson::Array(vec![]) }), "count": Bson::Int32(3) },
            ]),
        },
        Rec {
            label: "q7s3i0072",
            line: 622,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$setWindowFields": Bson::Document(doc! { "sortBy": Bson::Document(doc! { "a.0": Bson::Int32(1) }), "output": Bson::Document(doc! { "r": Bson::Document(doc! { "$documentNumber": Bson::Document(Document::new()) }) }) }) },
            ]),
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3i0073",
            line: 633,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$setWindowFields": Bson::Document(doc! { "sortBy": Bson::Document(doc! { "a.0": Bson::Int32(1) }), "output": Bson::Document(doc! { "r": Bson::Document(doc! { "$documentNumber": Bson::Document(Document::new()) }) }) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(5), Bson::Int32(6)]), "k": Bson::Int32(1), "r": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(2), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(2) })]), "k": Bson::Int32(0), "r": Bson::Int32(2) },
                doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(5)]), Bson::Array(vec![Bson::Int32(1)])]), "k": Bson::Int32(1), "r": Bson::Int32(3) },
            ]),
        },
        Rec {
            label: "q7s3i0074",
            line: 641,
            docs: docs_68d0945c,
            run: Run::Pipeline(vec![
                doc! { "$unwind": Bson::String("$a".to_string()) },
                doc! { "$sort": Bson::Document(doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(2), "a": Bson::Document(doc! { "b": Bson::Int32(1) }), "k": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(4), "a": Bson::Int32(5), "k": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(4), "a": Bson::Int32(6), "k": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(5), "a": Bson::Document(doc! { "b": Bson::Int32(1) }), "k": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(2), "a": Bson::Document(doc! { "0": Bson::Int32(2) }), "k": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Int32(5)]), "k": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(1), "a": Bson::Document(doc! { "0": Bson::Int32(9) }), "k": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(3), "a": Bson::Document(doc! { "0": Bson::Int32(9) }), "k": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3i0075",
            line: 652,
            docs: docs_b4264950,
            run: Run::Pipeline(vec![
                doc! { "$unwind": Bson::String("$a".to_string()) },
                doc! { "$sort": Bson::Document(doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Int32(5), "k": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(1), "a": Bson::Int32(6), "k": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(2), "a": Bson::Document(doc! { "b": Bson::Int32(1) }), "k": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(2), "a": Bson::Document(doc! { "b": Bson::Int32(2) }), "k": Bson::Int32(0) },
                doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Int32(1)]), "k": Bson::Int32(1) },
                doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::Int32(5)]), "k": Bson::Int32(1) },
            ]),
        },
        Rec {
            label: "q7s3i0076",
            line: 660,
            docs: docs_0544cc41,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(0) })]), "$sort": Bson::Document(doc! { "0": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(0) }), Bson::Document(doc! { "0": Bson::Int32(9) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0077",
            line: 666,
            docs: docs_75d68306,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(0) })]), "$sort": Bson::Document(doc! { "0": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(0) }), Bson::Document(doc! { "0": Bson::Int32(2) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0078",
            line: 672,
            docs: docs_8a1694a6,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(0) })]), "$sort": Bson::Document(doc! { "0": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "0": Bson::Int32(0) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0079",
            line: 678,
            docs: docs_f3d7c6d1,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)])]), "$sort": Bson::Document(doc! { "0": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(2)]), Bson::Array(vec![Bson::Int32(1)]), Bson::Document(doc! { "0": Bson::Int32(3) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0080",
            line: 684,
            docs: docs_0544cc41,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(0) })]), "$sort": Bson::Document(doc! { "b": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(9) }), Bson::Document(doc! { "b": Bson::Int32(0) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0081",
            line: 690,
            docs: docs_75d68306,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(0) })]), "$sort": Bson::Document(doc! { "b": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Document(doc! { "b": Bson::Int32(0) }), Bson::Document(doc! { "b": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0082",
            line: 696,
            docs: docs_8a1694a6,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(0) })]), "$sort": Bson::Document(doc! { "b": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(0) }), Bson::Document(doc! { "b": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0083",
            line: 702,
            docs: docs_f3d7c6d1,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)])]), "$sort": Bson::Document(doc! { "b": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(2)]), Bson::Document(doc! { "0": Bson::Int32(3) }), Bson::Array(vec![Bson::Int32(1)])]) },
            ]),
        },
        Rec {
            label: "q7s3i0084",
            line: 708,
            docs: docs_0544cc41,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(0) })]), "$sort": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(9) }), Bson::Document(doc! { "b": Bson::Int32(0) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0085",
            line: 714,
            docs: docs_75d68306,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(0) })]), "$sort": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Document(doc! { "b": Bson::Int32(0) }), Bson::Document(doc! { "b": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0086",
            line: 720,
            docs: docs_8a1694a6,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(0) })]), "$sort": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(0) }), Bson::Document(doc! { "b": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0087",
            line: 726,
            docs: docs_f3d7c6d1,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)])]), "$sort": Bson::Int32(1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(3) }), Bson::Array(vec![Bson::Int32(1)]), Bson::Array(vec![Bson::Int32(2)])]) },
            ]),
        },
        Rec {
            label: "q7s3i0088",
            line: 732,
            docs: docs_0544cc41,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(0) })]), "$sort": Bson::Int32(-1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(0) }), Bson::Document(doc! { "0": Bson::Int32(9) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0089",
            line: 738,
            docs: docs_75d68306,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(0) })]), "$sort": Bson::Int32(-1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "b": Bson::Int32(0) }), Bson::Document(doc! { "0": Bson::Int32(2) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0090",
            line: 744,
            docs: docs_8a1694a6,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(0) })]), "$sort": Bson::Int32(-1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Document(doc! { "0": Bson::Int32(0) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0091",
            line: 750,
            docs: docs_f3d7c6d1,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)])]), "$sort": Bson::Int32(-1) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(2)]), Bson::Array(vec![Bson::Int32(1)]), Bson::Document(doc! { "0": Bson::Int32(3) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0092",
            line: 756,
            docs: docs_0544cc41,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(0) })]), "$sort": Bson::Document(doc! { "0": Bson::Int32(-1), "b": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(9) }), Bson::Document(doc! { "b": Bson::Int32(0) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0093",
            line: 762,
            docs: docs_75d68306,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(0) })]), "$sort": Bson::Document(doc! { "0": Bson::Int32(-1), "b": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Document(doc! { "b": Bson::Int32(0) }), Bson::Document(doc! { "b": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0094",
            line: 768,
            docs: docs_8a1694a6,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(0) })]), "$sort": Bson::Document(doc! { "0": Bson::Int32(-1), "b": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(0) }), Bson::Document(doc! { "b": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0095",
            line: 774,
            docs: docs_f3d7c6d1,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "a": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)])]), "$sort": Bson::Document(doc! { "0": Bson::Int32(-1), "b": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(3) }), Bson::Array(vec![Bson::Int32(2)]), Bson::Array(vec![Bson::Int32(1)])]) },
            ]),
        },
        Rec {
            label: "q7s3j0151",
            line: 902,
            docs: docs_265faa42,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.1": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3j0152",
            line: 913,
            docs: docs_265faa42,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.1": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3j0155",
            line: 946,
            docs: docs_265faa42,
            run: Run::Find {
                filter: doc! { "a.1": Bson::Int32(5) },
                sort: doc! { "a.1": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3j0156",
            line: 957,
            docs: docs_265faa42,
            run: Run::Find {
                filter: doc! { "a.1": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) },
                sort: doc! { "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![4, 5, 6]),
        },
        Rec {
            label: "q7s3j0157",
            line: 968,
            docs: docs_265faa42,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.99999999999": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 6, 5]),
        },
        Rec {
            label: "q7s3j0158",
            line: 979,
            docs: docs_265faa42,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.99999999999": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![5, 1, 2, 3, 4, 6]),
        },
        Rec {
            label: "q7s3j0161",
            line: 1012,
            docs: docs_265faa42,
            run: Run::Find {
                filter: doc! { "a.99999999999": Bson::Int32(5) },
                sort: doc! { "a.99999999999": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![]),
        },
        Rec {
            label: "q7s3j0162",
            line: 1023,
            docs: docs_265faa42,
            run: Run::Find {
                filter: doc! { "a.99999999999": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) },
                sort: doc! { "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 6]),
        },
        Rec {
            label: "q7s3j0163",
            line: 1034,
            docs: docs_265faa42,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![4, 3, 1, 2, 5, 6]),
        },
        Rec {
            label: "q7s3j0164",
            line: 1045,
            docs: docs_265faa42,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.0": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![6, 5, 1, 2, 3, 4]),
        },
        Rec {
            label: "q7s3j0167",
            line: 1078,
            docs: docs_265faa42,
            run: Run::Find {
                filter: doc! { "a.0": Bson::Int32(5) },
                sort: doc! { "a.0": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![3]),
        },
        Rec {
            label: "q7s3j0168",
            line: 1089,
            docs: docs_265faa42,
            run: Run::Find {
                filter: doc! { "a.0": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) },
                sort: doc! { "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![4]),
        },
        Rec {
            label: "q7s3j0169",
            line: 1100,
            docs: docs_265faa42,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.2": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6]),
        },
        Rec {
            label: "q7s3j0170",
            line: 1111,
            docs: docs_265faa42,
            run: Run::Find {
                filter: Document::new(),
                sort: doc! { "a.2": Bson::Int32(-1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6]),
        },
        Rec {
            label: "q7s3j0173",
            line: 1144,
            docs: docs_265faa42,
            run: Run::Find {
                filter: doc! { "a.2": Bson::Int32(5) },
                sort: doc! { "a.2": Bson::Int32(1), "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![]),
        },
        Rec {
            label: "q7s3j0174",
            line: 1155,
            docs: docs_265faa42,
            run: Run::Find {
                filter: doc! { "a.2": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) },
                sort: doc! { "_id": Bson::Int32(1) },
            },
            outcome: Outcome::Ids(vec![1, 2, 3, 4, 5, 6]),
        },
        Rec {
            label: "q7s3j0187",
            line: 1260,
            docs: docs_1feb0573,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "s": Bson::Document(doc! { "$sortArray": Bson::Document(doc! { "input": Bson::String("$x".to_string()), "sortBy": Bson::Document(doc! { "0": Bson::Int32(1) }) }) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "s": Bson::Array(vec![Bson::Int32(5), Bson::Document(doc! { "0": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "s": Bson::Array(vec![Bson::Int32(5), Bson::Int32(3)]) },
                doc! { "_id": Bson::Int32(3), "s": Bson::Array(vec![Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) })]), Bson::Array(vec![Bson::Int32(1)])]) },
                doc! { "_id": Bson::Int32(4), "s": Bson::Null },
                doc! { "_id": Bson::Int32(5), "s": Bson::Null },
            ]),
        },
        Rec {
            label: "q7s3j0188",
            line: 1270,
            docs: docs_1feb0573,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "s": Bson::Document(doc! { "$sortArray": Bson::Document(doc! { "input": Bson::String("$x".to_string()), "sortBy": Bson::Int32(1) }) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "s": Bson::Array(vec![Bson::Int32(5), Bson::Document(doc! { "0": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "s": Bson::Array(vec![Bson::Int32(3), Bson::Int32(5)]) },
                doc! { "_id": Bson::Int32(3), "s": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)]), Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) })])]) },
                doc! { "_id": Bson::Int32(4), "s": Bson::Null },
                doc! { "_id": Bson::Int32(5), "s": Bson::Null },
            ]),
        },
        Rec {
            label: "q7s3j0189",
            line: 1280,
            docs: docs_1feb0573,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "s": Bson::Document(doc! { "$sortArray": Bson::Document(doc! { "input": Bson::String("$y".to_string()), "sortBy": Bson::Document(doc! { "x.0": Bson::Int32(1) }) }) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "s": Bson::Null },
                doc! { "_id": Bson::Int32(2), "s": Bson::Null },
                doc! { "_id": Bson::Int32(3), "s": Bson::Null },
                doc! { "_id": Bson::Int32(4), "s": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Int32(1)]) })]) },
                doc! { "_id": Bson::Int32(5), "s": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(1)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0190",
            line: 1290,
            docs: docs_1feb0573,
            run: Run::Pipeline(vec![
                doc! { "$project": Bson::Document(doc! { "s": Bson::Document(doc! { "$sortArray": Bson::Document(doc! { "input": Bson::String("$y".to_string()), "sortBy": Bson::Document(doc! { "x.0": Bson::Int32(-1) }) }) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "s": Bson::Null },
                doc! { "_id": Bson::Int32(2), "s": Bson::Null },
                doc! { "_id": Bson::Int32(3), "s": Bson::Null },
                doc! { "_id": Bson::Int32(4), "s": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Int32(1)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) })]) },
                doc! { "_id": Bson::Int32(5), "s": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(1)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0191",
            line: 1300,
            docs: docs_1feb0573,
            run: Run::Pipeline(vec![
                doc! { "$sort": Bson::Document(doc! { "x.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3j0192",
            line: 1310,
            docs: docs_1feb0573,
            run: Run::Pipeline(vec![
                doc! { "$sort": Bson::Document(doc! { "y.x.0": Bson::Int32(1), "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Refused(16746),
        },
        Rec {
            label: "q7s3j0193",
            line: 1320,
            docs: docs_1feb0573,
            run: Run::Pipeline(vec![
                doc! { "$group": Bson::Document(doc! { "_id": Bson::Null, "t": Bson::Document(doc! { "$top": Bson::Document(doc! { "sortBy": Bson::Document(doc! { "x.0": Bson::Int32(1) }), "output": Bson::String("$_id".to_string()) }) }) }) },
                doc! { "$sort": Bson::Document(doc! { "_id": Bson::Int32(1) }) },
            ]),
            outcome: Outcome::Docs(vec![doc! { "_id": Bson::Null, "t": Bson::Int32(2) }]),
        },
        Rec {
            label: "q7s3j0194",
            line: 1330,
            docs: docs_d3304414,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "y": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(1) }), Bson::Int32(5)]) })]), "$sort": Bson::Document(doc! { "x.0": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(1) }), Bson::Int32(5)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Int32(1)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0195",
            line: 1336,
            docs: docs_d3304414,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "y": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) })]), "$sort": Bson::Document(doc! { "x.0": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Int32(1)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0196",
            line: 1342,
            docs: docs_aade5ac6,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "y": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(2)]) })]), "$sort": Bson::Document(doc! { "x.0": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(1)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(2)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0197",
            line: 1348,
            docs: docs_d3304414,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "y": Bson::Document(doc! { "$each": Bson::Array(vec![]), "$sort": Bson::Document(doc! { "x.0": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Int32(1)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0198",
            line: 1354,
            docs: docs_d3304414,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "y": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(1) }), Bson::Int32(5)]) })]), "$sort": Bson::Document(doc! { "x.0": Bson::Int32(-1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Int32(1)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(1) }), Bson::Int32(5)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0199",
            line: 1360,
            docs: docs_d3304414,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "y": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) })]), "$sort": Bson::Document(doc! { "x.0": Bson::Int32(-1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Int32(1)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0200",
            line: 1366,
            docs: docs_aade5ac6,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "y": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(2)]) })]), "$sort": Bson::Document(doc! { "x.0": Bson::Int32(-1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(2)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(1)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0201",
            line: 1372,
            docs: docs_d3304414,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "y": Bson::Document(doc! { "$each": Bson::Array(vec![]), "$sort": Bson::Document(doc! { "x.0": Bson::Int32(-1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Int32(1)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0202",
            line: 1378,
            docs: docs_d3304414,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "y": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(1) }), Bson::Int32(5)]) })]), "$sort": Bson::Document(doc! { "x": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(1) }), Bson::Int32(5)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Int32(1)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0203",
            line: 1384,
            docs: docs_d3304414,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "y": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) })]), "$sort": Bson::Document(doc! { "x": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Int32(1)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0204",
            line: 1390,
            docs: docs_aade5ac6,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "y": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(2)]) })]), "$sort": Bson::Document(doc! { "x": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(1)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(2)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0205",
            line: 1396,
            docs: docs_d3304414,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "y": Bson::Document(doc! { "$each": Bson::Array(vec![]), "$sort": Bson::Document(doc! { "x": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Int32(1)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0206",
            line: 1402,
            docs: docs_d3304414,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "y": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(1) }), Bson::Int32(5)]) })]), "$sort": Bson::Document(doc! { "x.1": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Int32(1)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(1) }), Bson::Int32(5)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0207",
            line: 1408,
            docs: docs_d3304414,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "y": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) })]), "$sort": Bson::Document(doc! { "x.1": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Int32(1)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0208",
            line: 1414,
            docs: docs_aade5ac6,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "y": Bson::Document(doc! { "$each": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(2)]) })]), "$sort": Bson::Document(doc! { "x.1": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(1)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(2)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3j0209",
            line: 1420,
            docs: docs_d3304414,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$push": Bson::Document(doc! { "y": Bson::Document(doc! { "$each": Bson::Array(vec![]), "$sort": Bson::Document(doc! { "x.1": Bson::Int32(1) }) }) }) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "y": Bson::Array(vec![Bson::Document(doc! { "x": Bson::Array(vec![Bson::Int32(3)]) }), Bson::Document(doc! { "x": Bson::Array(vec![Bson::Document(doc! { "0": Bson::Int32(2) }), Bson::Int32(1)]) })]) },
            ]),
        },
    ]
}
