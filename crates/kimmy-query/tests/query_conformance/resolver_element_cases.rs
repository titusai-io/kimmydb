//! Rebuilt by `tests/data/resolver_element_cases_gen.py`; its docstring has the command.
//! Generated from the recordings q7s3b, q7s3g and q7s3i (reference 6.0.5): `$elemMatch`,
//! `arrayFilters` and the positional `$` of a projection and of an update. The label is the
//! case's, the line its place in the set's `.out`.

use super::resolver_elements::{Outcome, Rec, Run};
use bson::{Bson, Document, doc};

fn docs_8834cdc0() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]), Bson::Array(vec![Bson::Int32(3)])]) },
    ]
}

fn docs_246cbcfb() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Null]) },
    ]
}

fn docs_66fdcd86() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]), Bson::Int32(3)]) },
    ]
}

fn docs_1c85af32() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Array(vec![Bson::Int32(2), Bson::Int32(3)])]), Bson::Array(vec![Bson::Int32(4)])]) },
    ]
}

fn docs_fbd3d1ba() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }), Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(3)]) })]) },
    ]
}

fn docs_456503cf() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Null })]) },
        doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(2) })]) },
        doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(2) }), Bson::Document(doc! { "c": Bson::Null }), Bson::Document(doc! { "d": Bson::Int32(3) })]) },
        doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
        doc! { "_id": Bson::Int32(7) },
    ]
}

fn docs_afa18611() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(0) }), Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(2), "d": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(2) })]) },
        doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
        doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
    ]
}

/// The number of cases `cases_elements` holds, written by the generator.
pub const CASES_ELEMENTS_COUNT: usize = 92;

pub fn cases_elements() -> Vec<Rec> {
    vec![
        Rec {
            label: "q7s3g0174",
            line: 1736,
            docs: docs_8834cdc0,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Document(doc! { "$gt": Bson::Int32(1) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(0), Bson::Int32(0)]) },
            ]),
        },
        Rec {
            label: "q7s3g0175",
            line: 1742,
            docs: docs_8834cdc0,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x.0": Bson::Document(doc! { "$gt": Bson::Int32(0) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(0), Bson::Int32(0)]) },
            ]),
        },
        Rec {
            label: "q7s3g0176",
            line: 1748,
            docs: docs_8834cdc0,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x.b": Bson::Document(doc! { "$exists": Bson::Boolean(true) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]), Bson::Array(vec![Bson::Int32(3)])]) },
            ]),
        },
        Rec {
            label: "q7s3g0177",
            line: 1754,
            docs: docs_8834cdc0,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x": Bson::Document(doc! { "$type": Bson::String("array".to_string()) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(0), Bson::Int32(0)]) },
            ]),
        },
        Rec {
            label: "q7s3g0178",
            line: 1760,
            docs: docs_8834cdc0,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.0.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Document(doc! { "$gt": Bson::Int32(1) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(0)]), Bson::Array(vec![Bson::Int32(3)])]) },
            ]),
        },
        Rec {
            label: "q7s3g0179",
            line: 1766,
            docs: docs_8834cdc0,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "$gt": Bson::Int32(1) }) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(0), Bson::Int32(0)]) },
            ]),
        },
        Rec {
            label: "q7s3g0180",
            line: 1772,
            docs: docs_8834cdc0,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Null }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]), Bson::Array(vec![Bson::Int32(3)])]) },
            ]),
        },
        Rec {
            label: "q7s3g0181",
            line: 1778,
            docs: docs_8834cdc0,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]), Bson::Array(vec![Bson::Int32(3)])]) },
            ]),
        },
        Rec {
            label: "q7s3g0182",
            line: 1784,
            docs: docs_8834cdc0,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Document(doc! { "$size": Bson::Int32(2) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(0), Bson::Array(vec![Bson::Int32(3)])]) },
            ]),
        },
        Rec {
            label: "q7s3g0201",
            line: 1898,
            docs: docs_246cbcfb,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Document(doc! { "$gt": Bson::Int32(1) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Null]) },
            ]),
        },
        Rec {
            label: "q7s3g0202",
            line: 1904,
            docs: docs_246cbcfb,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x.0": Bson::Document(doc! { "$gt": Bson::Int32(0) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Null]) },
            ]),
        },
        Rec {
            label: "q7s3g0203",
            line: 1910,
            docs: docs_246cbcfb,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x.b": Bson::Document(doc! { "$exists": Bson::Boolean(true) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(0), Bson::Null]) },
            ]),
        },
        Rec {
            label: "q7s3g0204",
            line: 1916,
            docs: docs_246cbcfb,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x": Bson::Document(doc! { "$type": Bson::String("array".to_string()) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Null]) },
            ]),
        },
        Rec {
            label: "q7s3g0206",
            line: 1928,
            docs: docs_246cbcfb,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "$gt": Bson::Int32(1) }) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Null]) },
            ]),
        },
        Rec {
            label: "q7s3g0207",
            line: 1934,
            docs: docs_246cbcfb,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Null }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Int32(0)]) },
            ]),
        },
        Rec {
            label: "q7s3g0208",
            line: 1940,
            docs: docs_246cbcfb,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Null]) },
            ]),
        },
        Rec {
            label: "q7s3g0209",
            line: 1946,
            docs: docs_246cbcfb,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Document(doc! { "$size": Bson::Int32(2) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Int32(1) }), Bson::Null]) },
            ]),
        },
        Rec {
            label: "q7s3g0228",
            line: 2060,
            docs: docs_66fdcd86,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Document(doc! { "$gt": Bson::Int32(1) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(0), Bson::Int32(0)]) },
            ]),
        },
        Rec {
            label: "q7s3g0229",
            line: 2066,
            docs: docs_66fdcd86,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x.0": Bson::Document(doc! { "$gt": Bson::Int32(0) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(0), Bson::Int32(3)]) },
            ]),
        },
        Rec {
            label: "q7s3g0230",
            line: 2072,
            docs: docs_66fdcd86,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x.b": Bson::Document(doc! { "$exists": Bson::Boolean(true) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]), Bson::Int32(3)]) },
            ]),
        },
        Rec {
            label: "q7s3g0231",
            line: 2078,
            docs: docs_66fdcd86,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x": Bson::Document(doc! { "$type": Bson::String("array".to_string()) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(0), Bson::Int32(3)]) },
            ]),
        },
        Rec {
            label: "q7s3g0232",
            line: 2084,
            docs: docs_66fdcd86,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.0.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Document(doc! { "$gt": Bson::Int32(1) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(0)]), Bson::Int32(3)]) },
            ]),
        },
        Rec {
            label: "q7s3g0233",
            line: 2090,
            docs: docs_66fdcd86,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "$gt": Bson::Int32(1) }) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(0), Bson::Int32(3)]) },
            ]),
        },
        Rec {
            label: "q7s3g0234",
            line: 2096,
            docs: docs_66fdcd86,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Null }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]), Bson::Int32(3)]) },
            ]),
        },
        Rec {
            label: "q7s3g0235",
            line: 2102,
            docs: docs_66fdcd86,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]), Bson::Int32(3)]) },
            ]),
        },
        Rec {
            label: "q7s3g0236",
            line: 2108,
            docs: docs_66fdcd86,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Document(doc! { "$size": Bson::Int32(2) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(0), Bson::Int32(3)]) },
            ]),
        },
        Rec {
            label: "q7s3g0255",
            line: 2222,
            docs: docs_1c85af32,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Document(doc! { "$gt": Bson::Int32(1) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Array(vec![Bson::Int32(2), Bson::Int32(3)])]), Bson::Int32(0)]) },
            ]),
        },
        Rec {
            label: "q7s3g0256",
            line: 2228,
            docs: docs_1c85af32,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x.0": Bson::Document(doc! { "$gt": Bson::Int32(0) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(0), Bson::Int32(0)]) },
            ]),
        },
        Rec {
            label: "q7s3g0257",
            line: 2234,
            docs: docs_1c85af32,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x.b": Bson::Document(doc! { "$exists": Bson::Boolean(true) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Array(vec![Bson::Int32(2), Bson::Int32(3)])]), Bson::Array(vec![Bson::Int32(4)])]) },
            ]),
        },
        Rec {
            label: "q7s3g0258",
            line: 2240,
            docs: docs_1c85af32,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x": Bson::Document(doc! { "$type": Bson::String("array".to_string()) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(0), Bson::Int32(0)]) },
            ]),
        },
        Rec {
            label: "q7s3g0259",
            line: 2246,
            docs: docs_1c85af32,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.0.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Document(doc! { "$gt": Bson::Int32(1) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(0)]), Bson::Array(vec![Bson::Int32(4)])]) },
            ]),
        },
        Rec {
            label: "q7s3g0260",
            line: 2252,
            docs: docs_1c85af32,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "$gt": Bson::Int32(1) }) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Array(vec![Bson::Int32(2), Bson::Int32(3)])]), Bson::Int32(0)]) },
            ]),
        },
        Rec {
            label: "q7s3g0261",
            line: 2258,
            docs: docs_1c85af32,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Null }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Array(vec![Bson::Int32(2), Bson::Int32(3)])]), Bson::Array(vec![Bson::Int32(4)])]) },
            ]),
        },
        Rec {
            label: "q7s3g0262",
            line: 2264,
            docs: docs_1c85af32,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Array(vec![Bson::Int32(1), Bson::Array(vec![Bson::Int32(2), Bson::Int32(3)])]), Bson::Array(vec![Bson::Int32(4)])]) },
            ]),
        },
        Rec {
            label: "q7s3g0263",
            line: 2270,
            docs: docs_1c85af32,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Document(doc! { "$size": Bson::Int32(2) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(0), Bson::Array(vec![Bson::Int32(4)])]) },
            ]),
        },
        Rec {
            label: "q7s3g0282",
            line: 2384,
            docs: docs_fbd3d1ba,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Document(doc! { "$gt": Bson::Int32(1) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }), Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(3)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3g0283",
            line: 2390,
            docs: docs_fbd3d1ba,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x.0": Bson::Document(doc! { "$gt": Bson::Int32(0) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }), Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(3)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3g0284",
            line: 2396,
            docs: docs_fbd3d1ba,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x.b": Bson::Document(doc! { "$exists": Bson::Boolean(true) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Int32(0), Bson::Int32(0)]) },
            ]),
        },
        Rec {
            label: "q7s3g0285",
            line: 2402,
            docs: docs_fbd3d1ba,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x": Bson::Document(doc! { "$type": Bson::String("array".to_string()) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }), Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(3)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3g0287",
            line: 2414,
            docs: docs_fbd3d1ba,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "$gt": Bson::Int32(1) }) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }), Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(3)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3g0288",
            line: 2420,
            docs: docs_fbd3d1ba,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Null }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }), Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(3)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3g0289",
            line: 2426,
            docs: docs_fbd3d1ba,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![
                    doc! { "x": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) },
                ],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }), Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(3)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3g0290",
            line: 2432,
            docs: docs_fbd3d1ba,
            run: Run::Update {
                query: doc! { "_id": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "a.$[x]": Bson::Int32(0) }) },
                array_filters: vec![doc! { "x": Bson::Document(doc! { "$size": Bson::Int32(2) }) }],
                multi: false,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) }), Bson::Document(doc! { "b": Bson::Array(vec![Bson::Int32(3)]) })]) },
            ]),
        },
        Rec {
            label: "q7s3g0291",
            line: 2438,
            docs: docs_456503cf,
            run: Run::Find {
                filter: doc! { "items.c": Bson::Null },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null })]) },
                doc! { "_id": Bson::Int32(7) },
            ]),
        },
        Rec {
            label: "q7s3g0292",
            line: 2450,
            docs: docs_456503cf,
            run: Run::Find {
                filter: doc! { "items.c": Bson::Null },
                projection: doc! { "items.$": Bson::Int32(1), "_id": Bson::Int32(0) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null })]) },
                Document::new(),
            ]),
        },
        Rec {
            label: "q7s3g0296",
            line: 2498,
            docs: docs_456503cf,
            run: Run::Find {
                filter: doc! { "items.c": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(51246),
        },
        Rec {
            label: "q7s3g0297",
            line: 2510,
            docs: docs_456503cf,
            run: Run::Find {
                filter: doc! { "items.c": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) },
                projection: doc! { "items.$": Bson::Int32(1), "_id": Bson::Int32(0) },
            },
            outcome: Outcome::Refused(51246),
        },
        Rec {
            label: "q7s3g0301",
            line: 2558,
            docs: docs_456503cf,
            run: Run::Find {
                filter: doc! { "items.c": Bson::Document(doc! { "$in": Bson::Array(vec![Bson::Null]) }) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null })]) },
                doc! { "_id": Bson::Int32(7) },
            ]),
        },
        Rec {
            label: "q7s3g0302",
            line: 2570,
            docs: docs_456503cf,
            run: Run::Find {
                filter: doc! { "items.c": Bson::Document(doc! { "$in": Bson::Array(vec![Bson::Null]) }) },
                projection: doc! { "items.$": Bson::Int32(1), "_id": Bson::Int32(0) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null })]) },
                Document::new(),
            ]),
        },
        Rec {
            label: "q7s3g0306",
            line: 2618,
            docs: docs_456503cf,
            run: Run::Find {
                filter: doc! { "items.c": Bson::Document(doc! { "$ne": Bson::Int32(1) }) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(51246),
        },
        Rec {
            label: "q7s3g0307",
            line: 2630,
            docs: docs_456503cf,
            run: Run::Find {
                filter: doc! { "items.c": Bson::Document(doc! { "$ne": Bson::Int32(1) }) },
                projection: doc! { "items.$": Bson::Int32(1), "_id": Bson::Int32(0) },
            },
            outcome: Outcome::Refused(51246),
        },
        Rec {
            label: "q7s3g0311",
            line: 2678,
            docs: docs_456503cf,
            run: Run::Find {
                filter: doc! { "items": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "c": Bson::Null }) }) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null })]) },
            ]),
        },
        Rec {
            label: "q7s3g0312",
            line: 2690,
            docs: docs_456503cf,
            run: Run::Find {
                filter: doc! { "items": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "c": Bson::Null }) }) },
                projection: doc! { "items.$": Bson::Int32(1), "_id": Bson::Int32(0) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null })]) },
            ]),
        },
        Rec {
            label: "q7s3g0313",
            line: 2702,
            docs: docs_456503cf,
            run: Run::Update {
                query: doc! { "items": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "c": Bson::Null }) }) },
                update: doc! { "$set": Bson::Document(doc! { "items.$.z": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Null })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(2) }), Bson::Document(doc! { "c": Bson::Null, "z": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(3) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(7) },
            ]),
        },
        Rec {
            label: "q7s3g0314",
            line: 2714,
            docs: docs_456503cf,
            run: Run::Update {
                query: doc! { "items": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "c": Bson::Null }) }) },
                update: doc! { "$unset": Bson::Document(doc! { "items.$.d": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(Document::new())]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(Document::new()), Bson::Document(doc! { "c": Bson::Null })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(Document::new()), Bson::Document(doc! { "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(2) }), Bson::Document(doc! { "c": Bson::Null }), Bson::Document(doc! { "d": Bson::Int32(3) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(7) },
            ]),
        },
        Rec {
            label: "q7s3g0315",
            line: 2726,
            docs: docs_456503cf,
            run: Run::Update {
                query: doc! { "items": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "c": Bson::Null }) }) },
                update: doc! { "$set": Bson::Document(doc! { "items.$": Bson::Int32(0) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Int32(0)]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Int32(0), Bson::Document(doc! { "c": Bson::Null })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Int32(0), Bson::Document(doc! { "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(2) }), Bson::Int32(0), Bson::Document(doc! { "d": Bson::Int32(3) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(7) },
            ]),
        },
        Rec {
            label: "q7s3g0316",
            line: 2738,
            docs: docs_456503cf,
            run: Run::Find {
                filter: doc! { "items": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "c": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) }) }) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(3) })]) },
            ]),
        },
        Rec {
            label: "q7s3g0317",
            line: 2750,
            docs: docs_456503cf,
            run: Run::Find {
                filter: doc! { "items": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "c": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) }) }) },
                projection: doc! { "items.$": Bson::Int32(1), "_id": Bson::Int32(0) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(3) })]) },
            ]),
        },
        Rec {
            label: "q7s3g0318",
            line: 2762,
            docs: docs_456503cf,
            run: Run::Update {
                query: doc! { "items": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "c": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) }) }) },
                update: doc! { "$set": Bson::Document(doc! { "items.$.z": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Null })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(2) }), Bson::Document(doc! { "c": Bson::Null }), Bson::Document(doc! { "d": Bson::Int32(3), "z": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(7) },
            ]),
        },
        Rec {
            label: "q7s3g0319",
            line: 2774,
            docs: docs_456503cf,
            run: Run::Update {
                query: doc! { "items": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "c": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) }) }) },
                update: doc! { "$unset": Bson::Document(doc! { "items.$.d": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(Document::new())]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(Document::new()), Bson::Document(doc! { "c": Bson::Null })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(Document::new()), Bson::Document(doc! { "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(2) }), Bson::Document(doc! { "c": Bson::Null }), Bson::Document(Document::new())]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(7) },
            ]),
        },
        Rec {
            label: "q7s3g0320",
            line: 2786,
            docs: docs_456503cf,
            run: Run::Update {
                query: doc! { "items": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "c": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) }) }) },
                update: doc! { "$set": Bson::Document(doc! { "items.$": Bson::Int32(0) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Int32(0)]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Int32(0), Bson::Document(doc! { "c": Bson::Null })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Int32(0), Bson::Document(doc! { "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(2) }), Bson::Document(doc! { "c": Bson::Null }), Bson::Int32(0)]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(7) },
            ]),
        },
        Rec {
            label: "q7s3g0321",
            line: 2798,
            docs: docs_456503cf,
            run: Run::Find {
                filter: doc! { "items.c": Bson::Document(doc! { "$gte": Bson::Null }) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null })]) },
                doc! { "_id": Bson::Int32(7) },
            ]),
        },
        Rec {
            label: "q7s3g0322",
            line: 2810,
            docs: docs_456503cf,
            run: Run::Find {
                filter: doc! { "items.c": Bson::Document(doc! { "$gte": Bson::Null }) },
                projection: doc! { "items.$": Bson::Int32(1), "_id": Bson::Int32(0) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null })]) },
                Document::new(),
            ]),
        },
        Rec {
            label: "q7s3g0326",
            line: 2858,
            docs: docs_456503cf,
            run: Run::Find {
                filter: doc! { "items.d": Bson::Document(doc! { "$exists": Bson::Boolean(true) }), "items.c": Bson::Null },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null })]) },
            ]),
        },
        Rec {
            label: "q7s3g0327",
            line: 2870,
            docs: docs_456503cf,
            run: Run::Find {
                filter: doc! { "items.d": Bson::Document(doc! { "$exists": Bson::Boolean(true) }), "items.c": Bson::Null },
                projection: doc! { "items.$": Bson::Int32(1), "_id": Bson::Int32(0) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null })]) },
            ]),
        },
        Rec {
            label: "q7s3g0328",
            line: 2882,
            docs: docs_456503cf,
            run: Run::Update {
                query: doc! { "items.d": Bson::Document(doc! { "$exists": Bson::Boolean(true) }), "items.c": Bson::Null },
                update: doc! { "$set": Bson::Document(doc! { "items.$.z": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Null })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(2) }), Bson::Document(doc! { "c": Bson::Null }), Bson::Document(doc! { "d": Bson::Int32(3), "z": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(7) },
            ]),
        },
        Rec {
            label: "q7s3g0329",
            line: 2894,
            docs: docs_456503cf,
            run: Run::Update {
                query: doc! { "items.d": Bson::Document(doc! { "$exists": Bson::Boolean(true) }), "items.c": Bson::Null },
                update: doc! { "$unset": Bson::Document(doc! { "items.$.d": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(Document::new())]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(Document::new()), Bson::Document(doc! { "c": Bson::Null })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(Document::new()), Bson::Document(doc! { "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(2) }), Bson::Document(doc! { "c": Bson::Null }), Bson::Document(Document::new())]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(7) },
            ]),
        },
        Rec {
            label: "q7s3g0330",
            line: 2906,
            docs: docs_456503cf,
            run: Run::Update {
                query: doc! { "items.d": Bson::Document(doc! { "$exists": Bson::Boolean(true) }), "items.c": Bson::Null },
                update: doc! { "$set": Bson::Document(doc! { "items.$": Bson::Int32(0) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Int32(0)]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Int32(0), Bson::Document(doc! { "c": Bson::Null })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Int32(0), Bson::Document(doc! { "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(2) }), Bson::Document(doc! { "c": Bson::Null }), Bson::Int32(0)]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]) },
                doc! { "_id": Bson::Int32(7) },
            ]),
        },
        Rec {
            label: "q7s3i0161",
            line: 1170,
            docs: docs_afa18611,
            run: Run::Find {
                filter: doc! { "items.c": Bson::Int32(1), "items.d": Bson::Int32(1) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0162",
            line: 1181,
            docs: docs_afa18611,
            run: Run::Update {
                query: doc! { "items.c": Bson::Int32(1), "items.d": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "items.$.z": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(0) }), Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(2), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1), "z": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0163",
            line: 1192,
            docs: docs_afa18611,
            run: Run::Find {
                filter: doc! { "items.d": Bson::Int32(1), "items.c": Bson::Int32(1) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(0) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0164",
            line: 1203,
            docs: docs_afa18611,
            run: Run::Update {
                query: doc! { "items.d": Bson::Int32(1), "items.c": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "items.$.z": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(0) }), Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(2), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1), "z": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0165",
            line: 1214,
            docs: docs_afa18611,
            run: Run::Find {
                filter: doc! { "items.c": Bson::Null, "items.d": Bson::Int32(1) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0166",
            line: 1225,
            docs: docs_afa18611,
            run: Run::Update {
                query: doc! { "items.c": Bson::Null, "items.d": Bson::Int32(1) },
                update: doc! { "$set": Bson::Document(doc! { "items.$.z": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(0) }), Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(2), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null }), Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0167",
            line: 1236,
            docs: docs_afa18611,
            run: Run::Find {
                filter: doc! { "items.d": Bson::Int32(1), "items.c": Bson::Null },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0168",
            line: 1247,
            docs: docs_afa18611,
            run: Run::Update {
                query: doc! { "items.d": Bson::Int32(1), "items.c": Bson::Null },
                update: doc! { "$set": Bson::Document(doc! { "items.$.z": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(0) }), Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(2), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null }), Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0169",
            line: 1258,
            docs: docs_afa18611,
            run: Run::Find {
                filter: doc! { "$and": Bson::Array(vec![Bson::Document(doc! { "items.c": Bson::Int32(1) }), Bson::Document(doc! { "items.d": Bson::Int32(1) })]) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0170",
            line: 1269,
            docs: docs_afa18611,
            run: Run::Update {
                query: doc! { "$and": Bson::Array(vec![Bson::Document(doc! { "items.c": Bson::Int32(1) }), Bson::Document(doc! { "items.d": Bson::Int32(1) })]) },
                update: doc! { "$set": Bson::Document(doc! { "items.$.z": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(0) }), Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(2), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1), "z": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0171",
            line: 1280,
            docs: docs_afa18611,
            run: Run::Find {
                filter: doc! { "$and": Bson::Array(vec![Bson::Document(doc! { "items.d": Bson::Int32(1) }), Bson::Document(doc! { "items.c": Bson::Int32(1) })]) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(0) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0172",
            line: 1291,
            docs: docs_afa18611,
            run: Run::Update {
                query: doc! { "$and": Bson::Array(vec![Bson::Document(doc! { "items.d": Bson::Int32(1) }), Bson::Document(doc! { "items.c": Bson::Int32(1) })]) },
                update: doc! { "$set": Bson::Document(doc! { "items.$.z": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(0) }), Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(2), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1), "z": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0173",
            line: 1302,
            docs: docs_afa18611,
            run: Run::Find {
                filter: doc! { "$or": Bson::Array(vec![Bson::Document(doc! { "items.c": Bson::Int32(2) }), Bson::Document(doc! { "items.d": Bson::Int32(1) })]) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(51246),
        },
        Rec {
            label: "q7s3i0175",
            line: 1324,
            docs: docs_afa18611,
            run: Run::Find {
                filter: doc! { "$or": Bson::Array(vec![Bson::Document(doc! { "items.d": Bson::Int32(1) }), Bson::Document(doc! { "items.c": Bson::Int32(2) })]) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Refused(51246),
        },
        Rec {
            label: "q7s3i0177",
            line: 1346,
            docs: docs_afa18611,
            run: Run::Find {
                filter: doc! { "items": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) }) }) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0178",
            line: 1357,
            docs: docs_afa18611,
            run: Run::Update {
                query: doc! { "items": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) }) }) },
                update: doc! { "$set": Bson::Document(doc! { "items.$.z": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(0) }), Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(2), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1), "z": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0179",
            line: 1368,
            docs: docs_afa18611,
            run: Run::Find {
                filter: doc! { "items.c": Bson::Document(doc! { "$gt": Bson::Int32(0) }), "items.d": Bson::Document(doc! { "$gt": Bson::Int32(0) }) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0180",
            line: 1379,
            docs: docs_afa18611,
            run: Run::Update {
                query: doc! { "items.c": Bson::Document(doc! { "$gt": Bson::Int32(0) }), "items.d": Bson::Document(doc! { "$gt": Bson::Int32(0) }) },
                update: doc! { "$set": Bson::Document(doc! { "items.$.z": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(0) }), Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(2), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1), "z": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0181",
            line: 1390,
            docs: docs_afa18611,
            run: Run::Find {
                filter: doc! { "items.c": Bson::Document(doc! { "$exists": Bson::Boolean(true) }), "items.d": Bson::Document(doc! { "$exists": Bson::Boolean(true) }) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(0) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0182",
            line: 1401,
            docs: docs_afa18611,
            run: Run::Update {
                query: doc! { "items.c": Bson::Document(doc! { "$exists": Bson::Boolean(true) }), "items.d": Bson::Document(doc! { "$exists": Bson::Boolean(true) }) },
                update: doc! { "$set": Bson::Document(doc! { "items.$.z": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(0), "z": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(2), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null }), Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1), "z": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0183",
            line: 1412,
            docs: docs_afa18611,
            run: Run::Find {
                filter: doc! { "items.d": Bson::Document(doc! { "$exists": Bson::Boolean(true) }), "items.c": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0184",
            line: 1423,
            docs: docs_afa18611,
            run: Run::Update {
                query: doc! { "items.d": Bson::Document(doc! { "$exists": Bson::Boolean(true) }), "items.c": Bson::Document(doc! { "$exists": Bson::Boolean(false) }) },
                update: doc! { "$set": Bson::Document(doc! { "items.$.z": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(0) }), Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(2), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0185",
            line: 1434,
            docs: docs_afa18611,
            run: Run::Find {
                filter: doc! { "items.c": Bson::Int32(1), "items": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "d": Bson::Int32(1) }) }) },
                projection: doc! { "items.$": Bson::Int32(1) },
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
            ]),
        },
        Rec {
            label: "q7s3i0186",
            line: 1445,
            docs: docs_afa18611,
            run: Run::Update {
                query: doc! { "items.c": Bson::Int32(1), "items": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "d": Bson::Int32(1) }) }) },
                update: doc! { "$set": Bson::Document(doc! { "items.$.z": Bson::Int32(1) }) },
                array_filters: vec![],
                multi: true,
            },
            outcome: Outcome::Docs(vec![
                doc! { "_id": Bson::Int32(1), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(0), "z": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(2), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(2), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "z": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(3), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Null }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(4), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(2) })]) },
                doc! { "_id": Bson::Int32(5), "items": Bson::Array(vec![Bson::Document(doc! { "c": Bson::Int32(1), "z": Bson::Int32(1) }), Bson::Document(doc! { "c": Bson::Int32(1), "d": Bson::Int32(1) })]) },
                doc! { "_id": Bson::Int32(6), "items": Bson::Array(vec![Bson::Document(doc! { "d": Bson::Int32(1) }), Bson::Document(doc! { "d": Bson::Int32(1) })]) },
            ]),
        },
    ]
}
