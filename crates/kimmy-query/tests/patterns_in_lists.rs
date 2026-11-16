//! A regular expression among the members of `$in`, `$nin` and `$all`, and `$all`
//! with `$elemMatch` members, against the reference's recorded behaviour.
//!
//! Every case names its label and the line of `q7s2r.out` it is recorded at (the
//! breadth recording of the Q7 step 2 probe set, reference 6.0.5). The documents
//! are the sixteen of the recording's first section, and an id list is the order
//! the reference answered in. `$ne` with a regular expression is refused by the
//! reference, and here.

use bson::{Bson, Document, doc};
use kimmy_query::filter::{self, matches};
use kimmy_query::seed::implied_equalities;

/// One row of a find table: the label, the document count, the filter and the ids it answers.
type FindCases<'a> = Vec<(&'a str, usize, Document, Vec<i32>)>;
/// One row of a seed table: the label, two document counts, two filters and the verdict.
type SeedCases<'a> = Vec<(&'a str, usize, Document, usize, Document, Option<bool>)>;

/// A regular expression as a client sends it.
fn re(pattern: &str, options: &str) -> Bson {
    Bson::RegularExpression(bson::Regex {
        pattern: pattern.to_string().try_into().expect("a pattern"),
        options: options.to_string().try_into().expect("options"),
    })
}

/// The recording's sixteen documents (`before` of r0001).
fn sixteen() -> Vec<Document> {
    vec![
        doc! { "_id": 1, "v": "apple" },
        doc! { "_id": 2, "v": "banana" },
        doc! { "_id": 3, "v": ["apple", "cherry"] },
        doc! { "_id": 4, "v": ["banana"] },
        doc! { "_id": 5, "v": 5 },
        doc! { "_id": 6, "v": Bson::Null },
        doc! { "_id": 7 },
        doc! { "_id": 8, "v": { "a": "apple" } },
        doc! { "_id": 9, "v": [5, "x"] },
        doc! { "_id": 10, "v": re("apple", "") },
        doc! { "_id": 11, "v": [re("ap", ""), "q"] },
        doc! { "_id": 12, "v": "Apple" },
        doc! { "_id": 13, "v": Bson::Array(vec![]) },
        doc! { "_id": 14, "v": [{ "a": 1, "b": 2 }] },
        doc! { "_id": 15, "v": "apple pie" },
        doc! { "_id": 16, "v": ["x", ["apple"]] },
    ]
}

/// The recording's eleven documents of the `$all` and `$elemMatch` section (`before` of r0049).
fn eleven() -> Vec<Document> {
    vec![
        doc! { "_id": 21, "arr": [{ "a": 1, "b": 2 }, { "a": 2, "b": 3 }] },
        doc! { "_id": 22, "arr": [{ "a": 1 }, { "a": 1 }] },
        doc! { "_id": 23, "arr": [1, 2, 3] },
        doc! { "_id": 24, "arr": [[1, 2], [3]] },
        doc! { "_id": 25 },
        doc! { "_id": 26, "arr": Bson::Array(vec![]) },
        doc! { "_id": 27, "arr": { "a": 1 } },
        doc! { "_id": 28, "arr": 5 },
        doc! { "_id": 29, "arr": [{ "a": 1, "b": [1, 2] }] },
        doc! { "_id": 30, "arr": [2, { "a": 1 }] },
        doc! { "_id": 31, "arr": [[{ "a": 1 }]] },
    ]
}

/// The ids a filter selects among `docs`, in order.
fn ids_over(docs: &[Document], query: &Document) -> Vec<i32> {
    let parsed = filter::parse(query).unwrap_or_else(|e| panic!("{query}: refused: {e}"));
    docs.iter()
        .filter(|d| matches(&parsed, d).unwrap_or_else(|e| panic!("{query}: {e}")))
        .map(|d| d.get_i32("_id").unwrap())
        .collect()
}

fn ids(query: &Document) -> Vec<i32> {
    ids_over(&sixteen(), query)
}

/// What an upsert with this filter seeds, as the document's text.
fn seeded(query: &Document) -> String {
    let parsed = filter::parse(query).expect("a filter");
    let mut out = Document::new();
    implied_equalities(&parsed, &mut out, &mut Vec::new()).expect("a seed");
    out.to_string()
}

fn refused(query: &Document) -> String {
    match filter::parse(query) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("{query} must be refused"),
    }
}

// ---------------------------------------------------------------------------
// find: `$in` and `$nin` with a regular expression
// ---------------------------------------------------------------------------

/// r0001 to r0047: the filters of the first section that this change reads, with the
/// ids the reference answered.
#[test]
fn r_find_in_and_nin_with_a_regex_answer_as_recorded() {
    let cases: FindCases<'_> = vec![
        ("r0001", 2, doc! { "v": re("^a", "") }, vec![1, 3, 15]),
        ("r0002", 23, doc! { "v": { "$eq": re("^a", "") } }, vec![]),
        ("r0003", 44, doc! { "v": { "$regex": "^a" } }, vec![1, 3, 15]),
        ("r0004", 65, doc! { "v": { "$regex": "^a", "$options": "i" } }, vec![1, 3, 12, 15]),
        ("r0005", 86, doc! { "v": re("^a", "i") }, vec![1, 3, 12, 15]),
        ("r0006", 107, doc! { "v": { "$in": [re("^a", "")] } }, vec![1, 3, 15]),
        ("r0007", 128, doc! { "v": { "$in": [re("^a", ""), re("^b", "")] } }, vec![1, 2, 3, 4, 15]),
        ("r0008", 149, doc! { "v": { "$in": [re("^a", ""), "banana"] } }, vec![1, 2, 3, 4, 15]),
        ("r0009", 170, doc! { "v": { "$in": ["banana", re("^a", "")] } }, vec![1, 2, 3, 4, 15]),
        ("r0010", 191, doc! { "v": { "$in": [re("^a", ""), 5] } }, vec![1, 3, 5, 9, 15]),
        ("r0011", 212, doc! { "v": { "$in": [re("^a", ""), Bson::Null] } }, vec![1, 3, 6, 7, 15]),
        ("r0012", 233, doc! { "v": { "$in": [re("^a", "i")] } }, vec![1, 3, 12, 15]),
        (
            "r0013",
            254,
            doc! { "v": { "$in": [re("^A", "i"), re("^b", "")] } },
            vec![1, 2, 3, 4, 12, 15],
        ),
        ("r0014", 275, doc! { "v": { "$in": [re("apple", "")] } }, vec![1, 3, 10, 15]),
        ("r0015", 296, doc! { "v": { "$in": [re("^a", "m")] } }, vec![1, 3, 15]),
        ("r0016", 317, doc! { "v": { "$in": [re("a p", "x")] } }, vec![1, 3, 15]),
        (
            "r0017",
            338,
            doc! { "v": { "$in": [re(".", "s")] } },
            vec![1, 2, 3, 4, 9, 11, 12, 15, 16],
        ),
        ("r0018", 359, doc! { "v": { "$in": [re("^$", "")] } }, vec![]),
        ("r0019", 380, doc! { "v": { "$in": [re("^a", ""), re("^a", "")] } }, vec![1, 3, 15]),
        (
            "r0020",
            401,
            doc! { "v": { "$in": [re("^a", ""), { "a": "apple" }] } },
            vec![1, 3, 8, 15],
        ),
        (
            "r0021",
            422,
            doc! { "v": { "$in": [re("^a", ""), ["apple", "cherry"]] } },
            vec![1, 3, 15],
        ),
        ("r0022", 443, doc! { "v": { "$in": [[re("^a", "")]] } }, vec![]),
        (
            "r0023",
            464,
            doc! { "v": { "$nin": [re("^a", "")] } },
            vec![2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16],
        ),
        (
            "r0024",
            485,
            doc! { "v": { "$nin": [re("^a", ""), re("^b", "")] } },
            vec![5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16],
        ),
        (
            "r0025",
            506,
            doc! { "v": { "$nin": [re("^a", ""), "banana"] } },
            vec![5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16],
        ),
        (
            "r0026",
            527,
            doc! { "v": { "$nin": [re("^a", ""), Bson::Null] } },
            vec![2, 4, 5, 8, 9, 10, 11, 12, 13, 14, 16],
        ),
        (
            "r0027",
            548,
            doc! { "v": { "$nin": [re("^a", "i")] } },
            vec![2, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 16],
        ),
        (
            "r0035",
            716,
            doc! { "v": { "$not": re("^a", "") } },
            vec![2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16],
        ),
        (
            "r0036",
            737,
            doc! { "v": { "$not": { "$regex": "^a" } } },
            vec![2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16],
        ),
        (
            "r0037",
            758,
            doc! { "v": { "$not": re("^a", "i") } },
            vec![2, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 16],
        ),
        (
            "r0038",
            779,
            doc! { "v": { "$not": { "$in": [re("^a", "")] } } },
            vec![2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16],
        ),
        ("r0041", 842, doc! { "$or": [{ "v": re("^a", "") }, { "v": 5 }] }, vec![1, 3, 5, 9, 15]),
        (
            "r0042",
            863,
            doc! { "$and": [{ "v": { "$in": [re("^a", "")] } }, { "v": { "$in": [re("e$", "")] } }] },
            vec![1, 3, 15],
        ),
        ("r0043", 884, doc! { "v.a": { "$in": [re("^a", "")] } }, vec![8]),
        ("r0044", 905, doc! { "v.0": { "$in": [re("^a", "")] } }, vec![3]),
        ("r0045", 926, doc! { "v": { "$in": [] } }, vec![]),
        (
            "r0046",
            947,
            doc! { "v": { "$nin": [] } },
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        ),
        ("r0047", 968, doc! { "v": { "$in": [re("^a", "")] }, "w": 1 }, vec![]),
    ];
    for (label, line, query, want) in cases {
        assert_eq!(ids(&query), want, "{label} (q7s2r.out line {line}): {query}");
    }
}

/// r0033 and r0034 (q7s2r.out lines 674 and 686, code 2 "Can't have regex as arg to $ne."): `$ne`
/// with a regex stays refused, as every other write of it does.
#[test]
fn r_ne_with_a_regex_stays_refused() {
    for query in [doc! { "v": { "$ne": re("^a", "") } }, doc! { "v": { "$ne": re("^a", "i") } }] {
        assert!(refused(&query).contains("$ne cannot take a regular expression"), "{query}");
    }
}

/// r0048 (q7s2r.out line 989): an invalid pattern in the list is a refusal (the reference's
/// code 51091; here a 400 naming the fault).
#[test]
fn r_an_invalid_pattern_in_a_list_is_refused() {
    for op in ["$in", "$nin"] {
        let message = refused(&doc! { "v": { op: [re("(", "")] } });
        assert!(message.contains('('), "{op}: {message}");
    }
    // The refusals of `$regex` hold for a member too: a possessive quantifier. (The options
    // `z` and `g` cannot be recorded from the client: UNRECORDED, left as they are.)
    refused(&doc! { "v": { "$in": [re("a*+a", "")] } });
}

/// The three shapes a client can send a regex in read as the literal does: the literal, the
/// document of `$regex` and `$options`, and the Extended JSON document. A stored document
/// shaped exactly `{$regex: ..}` cannot be found through `$in`, only through `$eq`.
#[test]
fn r_every_shape_a_regex_arrives_in_is_a_pattern() {
    let shapes = |pattern: &str, options: &str| -> Vec<Bson> {
        vec![
            re(pattern, options),
            Bson::Document(doc! { "$regex": pattern, "$options": options }),
            Bson::Document(
                doc! { "$regularExpression": { "pattern": pattern, "options": options } },
            ),
        ]
    };
    for shape in shapes("^A", "i") {
        assert_eq!(ids(&doc! { "v": { "$in": [shape.clone(), "banana"] } }), [1, 2, 3, 4, 12, 15]);
        assert_eq!(ids(&doc! { "v": { "$nin": [shape.clone()] } }).len(), 12);
    }
    // A flag on the literal wins over a sibling `$options`, as under `$regex`.
    let both = Bson::Document(doc! { "$regex": re("^A", "i"), "$options": "" });
    assert_eq!(ids(&doc! { "v": { "$in": [both] } }), [1, 3, 12, 15]);
    let stored = [doc! { "_id": 1, "v": { "$regex": "^a" } }, doc! { "_id": 2, "v": "apple" }];
    let found = ids_over(&stored, &doc! { "v": { "$in": [{ "$regex": "^a" }] } });
    assert_eq!(found, [2], "the pattern, and not the stored document of that shape");
    assert_eq!(ids_over(&stored, &doc! { "v": { "$eq": { "$regex": "^a" } } }), [1]);
    // A document that is not a regex's shape and has a `$`-led key is no value of an `$in`
    // (q7s3f0299), and is still found by `$eq` of the whole document.
    let other = [doc! { "_id": 1, "v": { "$regex": "x", "kind": 1 } }];
    refused(&doc! { "v": { "$in": [{ "$regex": "x", "kind": 1 }] } });
    assert_eq!(ids_over(&other, &doc! { "v": { "$eq": { "$regex": "x", "kind": 1 } } }), [1]);
}

/// `$nin` is the exact complement of `$in` over the sixteen documents, for every list the
/// recording tried (r0006 to r0027), missing and null documents included unless null is listed.
#[test]
fn r_nin_is_the_complement_of_in() {
    let lists: Vec<Vec<Bson>> = vec![
        vec![re("^a", "")],
        vec![re("^a", ""), re("^b", "")],
        vec![re("^a", ""), "banana".into()],
        vec![re("^a", ""), Bson::Null],
        vec![re("^a", "i")],
        vec![re("^a", ""), 5.into()],
    ];
    for list in lists {
        let inside = ids(&doc! { "v": { "$in": list.clone() } });
        let outside = ids(&doc! { "v": { "$nin": list.clone() } });
        let all: Vec<i32> = (1..=16).collect();
        let mut both: Vec<i32> = inside.iter().chain(&outside).copied().collect();
        both.sort_unstable();
        assert_eq!(both, all, "{list:?}");
        assert!(inside.iter().all(|i| !outside.contains(i)), "{list:?}");
    }
}

// ---------------------------------------------------------------------------
// the filter of a multi-update and of an upsert
// ---------------------------------------------------------------------------

/// r0072 to r0116 (q7s2r.out lines 1378 to 1880): the same filters as the filter of a multi
/// update (what it matched, `n`) and of an upsert (what it seeds when nothing matches, and
/// whether it matched the one document `{_id: 1, v: 99}`).
#[test]
fn r_the_same_filters_as_an_update_filter_and_an_upsert_filter() {
    let ninety_nine = [doc! { "_id": 1, "v": 99 }];
    let cases: SeedCases<'_> = vec![
        ("r0072", 1378, doc! { "v": re("^a", "") }, 3, doc! {}, Some(false)),
        ("r0075", 1412, doc! { "v": { "$in": [re("^a", "")] } }, 3, doc! {}, Some(false)),
        (
            "r0078",
            1446,
            doc! { "v": { "$in": [re("^a", ""), re("^b", "")] } },
            5,
            doc! {},
            Some(false),
        ),
        ("r0081", 1480, doc! { "v": { "$in": [re("^a", ""), "banana"] } }, 5, doc! {}, Some(false)),
        ("r0084", 1514, doc! { "v": { "$in": [re("^a", ""), 5] } }, 5, doc! {}, Some(false)),
        ("r0087", 1548, doc! { "v": { "$nin": [re("^a", "")] } }, 13, doc! {}, Some(true)),
        (
            "r0090",
            1581,
            doc! { "v": { "$nin": [re("^a", ""), "banana"] } },
            11,
            doc! {},
            Some(true),
        ),
        ("r0093", 1614, doc! { "v": { "$nin": [re("^a", "i")] } }, 12, doc! {}, Some(true)),
        ("r0105", 1747, doc! { "v": { "$not": re("^a", "") } }, 13, doc! {}, Some(true)),
        ("r0108", 1780, doc! { "v": { "$not": re("^a", "i") } }, 12, doc! {}, Some(true)),
        (
            "r0111",
            1813,
            doc! { "v": { "$not": { "$in": [re("^a", "")] } } },
            13,
            doc! {},
            Some(true),
        ),
        (
            "r0114",
            1846,
            doc! { "$or": [{ "v": re("^a", "") }, { "v": 5 }] },
            5,
            doc! {},
            Some(false),
        ),
    ];
    for (label, line, query, matched, seed, hits_99) in cases {
        assert_eq!(ids(&query).len(), matched, "{label} (q7s2r.out line {line}): {query}");
        assert_eq!(seeded(&query), seed.to_string(), "{label} seeds (line {line}): {query}");
        if let Some(hit) = hits_99 {
            assert_eq!(!ids_over(&ninety_nine, &query).is_empty(), hit, "{label} {query}");
        }
    }
}

// ---------------------------------------------------------------------------
// `$all` with a regular expression
// ---------------------------------------------------------------------------

/// r0028 to r0032 (q7s2r.out lines 569 to 653): each regex of an `$all` list is a pattern the
/// field must match, as a value member is a value the field must hold, whether the field is a
/// string or an array.
#[test]
fn r_find_all_with_a_regex_answers_as_recorded() {
    let cases: FindCases<'_> = vec![
        ("r0028", 569, doc! { "v": { "$all": [re("^a", "")] } }, vec![1, 3, 15]),
        ("r0029", 590, doc! { "v": { "$all": [re("^a", ""), re("^c", "")] } }, vec![3]),
        ("r0030", 611, doc! { "v": { "$all": [re("^a", ""), "cherry"] } }, vec![3]),
        ("r0031", 632, doc! { "v": { "$all": [re("^a", "i")] } }, vec![1, 3, 12, 15]),
        ("r0032", 653, doc! { "v": { "$all": [re("^a", ""), re("^a", "")] } }, vec![1, 3, 15]),
    ];
    for (label, line, query, want) in cases {
        assert_eq!(ids(&query), want, "{label} (q7s2r.out line {line}): {query}");
    }
}

/// r0070 (q7s2r.out line 1346): an `$all` of values reads each value over every value the path
/// resolves to, as two equalities on the path would: `arr.a` over an array of documents holds
/// 1 and 2 when two of its elements do.
#[test]
fn r_all_reads_each_member_over_every_value_of_a_path() {
    assert_eq!(ids_over(&eleven(), &doc! { "arr.a": { "$all": [1, 2] } }), [21]);
    // An array field holds every member it lists, and a scalar holds the member it is.
    let docs = [
        doc! { "_id": 1, "a": [1, 2, 3] },
        doc! { "_id": 2, "a": 1 },
        doc! { "_id": 3, "a": [1] },
        doc! { "_id": 4 },
    ];
    assert_eq!(ids_over(&docs, &doc! { "a": { "$all": [1, 3] } }), [1]);
    assert_eq!(ids_over(&docs, &doc! { "a": { "$all": [1] } }), [1, 2, 3]);
    assert_eq!(ids_over(&docs, &doc! { "a": { "$all": [9] } }), Vec::<i32>::new());
}

/// r0096 to r0101 (q7s2r.out lines 1647 to 1715): the same `$all` forms as the filter of a multi
/// update and of an upsert. A pattern seeds nothing, a value beside it seeds as a value does
/// (`$all: [/^a/, "cherry"]` seeds `v: "cherry"`), and two values are still the path matched
/// twice. `$ne` of a regex is refused in all three (r0102 to r0104, code 2).
#[test]
fn r_all_with_a_regex_as_an_update_filter_and_an_upsert_filter() {
    let ninety_nine = [doc! { "_id": 1, "v": 99 }];
    let cases: SeedCases<'_> = vec![
        ("r0096", 1647, doc! { "v": { "$all": [re("^a", "")] } }, 3, doc! {}, Some(false)),
        (
            "r0099",
            1681,
            doc! { "v": { "$all": [re("^a", ""), "cherry"] } },
            1,
            doc! { "v": "cherry" },
            Some(false),
        ),
    ];
    for (label, line, query, matched, seed, hits_99) in cases {
        assert_eq!(ids(&query).len(), matched, "{label} (q7s2r.out line {line}): {query}");
        assert_eq!(seeded(&query), seed.to_string(), "{label} seeds (line {line}): {query}");
        if let Some(hit) = hits_99 {
            assert_eq!(!ids_over(&ninety_nine, &query).is_empty(), hit, "{label} {query}");
        }
    }
    for query in [
        doc! { "v": { "$all": ["a", "b", re("^c", "")] } },
        doc! { "$or": [{ "v": { "$all": ["a", "b", re("^c", "")] } }] },
    ] {
        let parsed = filter::parse(&query).unwrap();
        let err = implied_equalities(&parsed, &mut Document::new(), &mut Vec::new()).unwrap_err();
        assert!(err.contains("path 'v' is matched twice"), "{query}: {err}");
    }
}

// ---------------------------------------------------------------------------
// `$all` with `$elemMatch` members
// ---------------------------------------------------------------------------

/// r0039 and r0040 (q7s2r.out lines 800 and 821): an `$elemMatch` of scalar conditions reads an
/// element that is an array as one value, so `["x", ["apple"]]` does not match `$regex: "^a"` on
/// the strings inside its inner array.
#[test]
fn r_an_elem_match_does_not_look_into_an_element_that_is_an_array() {
    let cases: FindCases<'_> = vec![
        ("r0039", 800, doc! { "v": { "$elemMatch": { "$in": [re("^a", "")] } } }, vec![3]),
        ("r0040", 821, doc! { "v": { "$elemMatch": { "$regex": "^a" } } }, vec![3]),
    ];
    for (label, line, query, want) in cases {
        assert_eq!(ids(&query), want, "{label} (q7s2r.out line {line}): {query}");
    }
}

/// r0049 to r0071 (q7s2r.out lines 1010 to 1362): an `$all` of `$elemMatch` documents holds when the
/// field has an element for each, each by an element of its own, over arrays of documents, of
/// scalars and of arrays, a missing field and a field that is not an array. `{$elemMatch: {}}` holds
/// for a document or an array element and for no scalar (r0059).
#[test]
fn r_find_all_of_elem_matches_answers_as_recorded() {
    let cases: FindCases<'_> = vec![
        (
            "r0049",
            1010,
            doc! { "arr": { "$all": [{ "$elemMatch": { "a": 1 } }] } },
            vec![21, 22, 29, 30],
        ),
        (
            "r0050",
            1026,
            doc! { "arr": { "$all": [{ "$elemMatch": { "a": 1 } }, { "$elemMatch": { "a": 2 } }] } },
            vec![21],
        ),
        (
            "r0051",
            1042,
            doc! { "arr": { "$all": [{ "$elemMatch": { "a": 1, "b": 2 } }, { "$elemMatch": { "a": 2 } }] } },
            vec![21],
        ),
        ("r0052", 1058, doc! { "arr": { "$all": [{ "$elemMatch": { "$gt": 1 } }] } }, vec![23, 30]),
        (
            "r0053",
            1074,
            doc! { "arr": { "$all": [{ "$elemMatch": { "$gt": 1 } }, { "$elemMatch": { "$lt": 3 } }] } },
            vec![23, 30],
        ),
        (
            "r0057",
            1138,
            doc! { "arr": { "$all": [{ "$elemMatch": { "$elemMatch": { "$gt": 2 } } }] } },
            vec![24],
        ),
        ("r0058", 1154, doc! { "arr": { "$all": [{ "$elemMatch": { "$size": 2 } }] } }, vec![24]),
        (
            "r0059",
            1170,
            doc! { "arr": { "$all": [{ "$elemMatch": {  } }] } },
            vec![21, 22, 24, 29, 30, 31],
        ),
        (
            "r0060",
            1186,
            doc! { "arr": { "$all": [{ "$elemMatch": { "a": { "$exists": true } } }] } },
            vec![21, 22, 29, 30],
        ),
        (
            "r0061",
            1202,
            doc! { "arr": { "$all": [{ "$elemMatch": { "b": { "$all": [1, 2] } } }] } },
            vec![29],
        ),
        (
            "r0062",
            1218,
            doc! { "arr": { "$all": [{ "$elemMatch": { "a": re("^1", "") } }] } },
            vec![],
        ),
        (
            "r0064",
            1250,
            doc! { "arr": { "$all": [{ "$elemMatch": { "a": 1 } }, { "$elemMatch": { "a": 1 } }] } },
            vec![21, 22, 29, 30],
        ),
        (
            "r0065",
            1266,
            doc! { "arr": { "$all": [{ "$elemMatch": { "a": 1 } }, { "$elemMatch": { "a": 1, "b": { "$exists": false } } }] } },
            vec![22, 30],
        ),
        ("r0066", 1282, doc! { "arr": { "$all": [{ "$elemMatch": { "a": 9 } }] } }, vec![]),
        (
            "r0068",
            1314,
            doc! { "arr": { "$not": { "$all": [{ "$elemMatch": { "a": 1 } }] } } },
            vec![23, 24, 25, 26, 27, 28, 31],
        ),
        ("r0069", 1330, doc! { "arr": { "$elemMatch": { "a": 1 } } }, vec![21, 22, 29, 30]),
        (
            "r0071",
            1362,
            doc! { "arr": { "$all": [{ "$elemMatch": { "a": 1 } }] }, "arr.1": 2 },
            vec![],
        ),
    ];
    for (label, line, query, want) in cases {
        assert_eq!(ids_over(&eleven(), &query), want, "{label} (q7s2r.out line {line}): {query}");
    }
}

/// r0054 to r0056 and r0063 (q7s2r.out lines 1090 to 1234, code 2 `BadValue`): an `$all` that
/// starts with an `$elemMatch` and holds anything else (a value, a document, a regex) is "has to
/// be consistent"; one that holds a value first and an `$elemMatch` later is "no $ expressions in
/// $all". Here a 400, with the reference's text.
#[test]
fn r_an_all_that_mixes_elem_matches_with_other_members_is_refused() {
    let em = || doc! { "$elemMatch": { "a": 1 } };
    for (label, line, query, message) in [
        (
            "r0054",
            1090,
            doc! { "arr": { "$all": [{ "$elemMatch": { "$gt": 1 } }, 2] } },
            "$all/$elemMatch has to be consistent",
        ),
        (
            "r0055",
            1106,
            doc! { "arr": { "$all": [1, { "$elemMatch": { "$gt": 2 } }] } },
            "no $ expressions in $all",
        ),
        (
            "r0056",
            1122,
            doc! { "arr": { "$all": [em(), { "a": 1 }] } },
            "$all/$elemMatch has to be consistent",
        ),
        (
            "r0063",
            1234,
            doc! { "arr": { "$all": [em(), re("x", "")] } },
            "$all/$elemMatch has to be consistent",
        ),
    ] {
        let got = refused(&query);
        assert!(got.contains(message), "{label} (q7s2r.out line {line}): {query}: {got}");
    }
    // A bad body is the `$elemMatch`'s own refusal.
    refused(&doc! { "arr": { "$all": [{ "$elemMatch": 5 }] } });
}

/// r0117 to r0134 (q7s2r.out lines 1880 to 2060): the same forms as the filter of a multi update
/// (what it matches) and of an upsert (it seeds nothing from an `$elemMatch`, however many).
/// r0125 and r0126, the mixed list, are the refusal above, as a write error.
#[test]
fn r_all_of_elem_matches_as_an_update_filter_and_an_upsert_filter() {
    let cases: SeedCases<'_> = vec![
        (
            "r0117",
            1880,
            doc! { "arr": { "$all": [{ "$elemMatch": { "a": 1 } }] } },
            4,
            doc! {},
            None,
        ),
        (
            "r0119",
            1902,
            doc! { "arr": { "$all": [{ "$elemMatch": { "a": 1 } }, { "$elemMatch": { "a": 2 } }] } },
            1,
            doc! {},
            None,
        ),
        (
            "r0121",
            1924,
            doc! { "arr": { "$all": [{ "$elemMatch": { "a": 1, "b": 2 } }, { "$elemMatch": { "a": 2 } }] } },
            1,
            doc! {},
            None,
        ),
        (
            "r0123",
            1946,
            doc! { "arr": { "$all": [{ "$elemMatch": { "$gt": 1 } }] } },
            2,
            doc! {},
            None,
        ),
        (
            "r0127",
            1989,
            doc! { "arr": { "$all": [{ "$elemMatch": { "$elemMatch": { "$gt": 2 } } }] } },
            1,
            doc! {},
            None,
        ),
        (
            "r0129",
            2011,
            doc! { "arr": { "$all": [{ "$elemMatch": { "$size": 2 } }] } },
            1,
            doc! {},
            None,
        ),
        (
            "r0131",
            2033,
            doc! { "arr": { "$all": [{ "$elemMatch": { "a": { "$exists": true } } }] } },
            4,
            doc! {},
            None,
        ),
        (
            "r0133",
            2055,
            doc! { "arr": { "$all": [{ "$elemMatch": { "a": 9 } }] } },
            0,
            doc! {},
            None,
        ),
    ];
    for (label, line, query, matched, seed, _) in cases {
        assert_eq!(
            ids_over(&eleven(), &query).len(),
            matched,
            "{label} (q7s2r.out line {line}): {query}"
        );
        assert_eq!(seeded(&query), seed.to_string(), "{label} seeds (line {line}): {query}");
    }
}

/// r0067, r0135 and r0136 (q7s2r.out lines 1298, 2077 and 2093): an empty `$all` matches no
/// document, as a find filter, as the filter of a multi update, and as the filter of an upsert,
/// which seeds nothing from it.
#[test]
fn r_an_empty_all_matches_nothing() {
    let query = doc! { "arr": { "$all": [] } };
    assert_eq!(ids_over(&eleven(), &query), Vec::<i32>::new(), "r0067 (line 1298)");
    assert_eq!(ids_over(&eleven(), &query).len(), 0, "r0135 (line 2077), n 0");
    assert_eq!(seeded(&query), doc! {}.to_string(), "r0136 (line 2093)");
}
