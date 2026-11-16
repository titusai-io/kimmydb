use super::common::{Case, assert_recorded, pick};
use bson::{Bson, Document, doc};

fn docs_q7s3f0286() -> Vec<Document> {
    vec![
        doc! { "_id": Bson::Int32(1), "a": Bson::String("abc".to_string()) },
        doc! { "_id": Bson::Int32(2), "a": Bson::String("ABC".to_string()) },
        doc! { "_id": Bson::Int32(3), "a": Bson::Array(vec![Bson::String("abc".to_string()), Bson::String("x".to_string())]) },
        doc! { "_id": Bson::Int32(4), "a": Bson::Int32(5) },
        doc! { "_id": Bson::Int32(5) },
        doc! { "_id": Bson::Int32(6), "a": Bson::Array(vec![Bson::Document(doc! { "b": Bson::String("abc".to_string()) })]) },
        doc! { "_id": Bson::Int32(7), "a": Bson::Null },
    ]
}
fn cases_r15() -> Vec<Case> {
    vec![
        Case {
            label: "q7s3f0286",
            line: 5091,
            docs: docs_q7s3f0286,
            filter: doc! { "a": Bson::Document(doc! { "$options": Bson::String("i".to_string()) }) },
            ids: None,
        },
        Case {
            label: "q7s3f0287",
            line: 5103,
            docs: docs_q7s3f0286,
            filter: doc! { "a": Bson::Document(doc! { "$not": Bson::Document(doc! { "$options": Bson::String("i".to_string()) }) }) },
            ids: None,
        },
        Case {
            label: "q7s3f0288",
            line: 5115,
            docs: docs_q7s3f0286,
            filter: doc! { "a": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "$options": Bson::String("i".to_string()) }) }) },
            ids: None,
        },
        Case {
            label: "q7s3f0289",
            line: 5127,
            docs: docs_q7s3f0286,
            filter: doc! { "a": Bson::Document(doc! { "$not": Bson::Document(doc! { "$options": Bson::String("i".to_string()), "$regex": Bson::String("abc".to_string()) }) }) },
            ids: Some(vec![4, 5, 6, 7]),
        },
        Case {
            label: "q7s3f0290",
            line: 5139,
            docs: docs_q7s3f0286,
            filter: doc! { "a": Bson::Document(doc! { "$elemMatch": Bson::Document(doc! { "$options": Bson::String("i".to_string()), "$regex": Bson::String("^A".to_string()) }) }) },
            ids: Some(vec![3]),
        },
        Case {
            label: "q7s3f0291",
            line: 5151,
            docs: docs_q7s3f0286,
            filter: doc! { "$and": Bson::Array(vec![Bson::Document(doc! { "a": Bson::Document(doc! { "$options": Bson::String("i".to_string()) }) })]) },
            ids: None,
        },
        Case {
            label: "q7s3f0292",
            line: 5163,
            docs: docs_q7s3f0286,
            filter: doc! { "$or": Bson::Array(vec![Bson::Document(doc! { "a": Bson::Document(doc! { "$options": Bson::String("i".to_string()) }) }), Bson::Document(doc! { "_id": Bson::Int32(1) })]) },
            ids: None,
        },
        Case {
            label: "q7s3f0293",
            line: 5175,
            docs: docs_q7s3f0286,
            filter: doc! { "$nor": Bson::Array(vec![Bson::Document(doc! { "a": Bson::Document(doc! { "$options": Bson::String("i".to_string()) }) })]) },
            ids: None,
        },
        Case {
            label: "q7s3f0294",
            line: 5187,
            docs: docs_q7s3f0286,
            filter: doc! { "a": Bson::Document(doc! { "$options": Bson::String("".to_string()) }) },
            ids: None,
        },
        Case {
            label: "q7s3f0295",
            line: 5199,
            docs: docs_q7s3f0286,
            filter: doc! { "a": Bson::Document(doc! { "$options": Bson::Int32(5) }) },
            ids: None,
        },
        Case {
            label: "q7s3f0296",
            line: 5211,
            docs: docs_q7s3f0286,
            filter: doc! { "a": Bson::Document(doc! { "$options": Bson::String("i".to_string()), "$gt": Bson::Int32(1) }) },
            ids: None,
        },
        Case {
            label: "q7s3f0297",
            line: 5223,
            docs: docs_q7s3f0286,
            filter: doc! { "a": Bson::Document(doc! { "$options": Bson::String("i".to_string()), "$exists": Bson::Boolean(true) }) },
            ids: None,
        },
        Case {
            label: "q7s3f0298",
            line: 5235,
            docs: docs_q7s3f0286,
            filter: doc! { "a.b": Bson::Document(doc! { "$options": Bson::String("i".to_string()) }) },
            ids: None,
        },
        Case {
            label: "q7s3f0300",
            line: 5259,
            docs: docs_q7s3f0286,
            filter: doc! { "a": Bson::Document(doc! { "$not": Bson::Document(doc! { "$regex": Bson::String("abc".to_string()), "$options": Bson::String("i".to_string()) }) }) },
            ids: Some(vec![4, 5, 6, 7]),
        },
    ]
}

/// The `$options` of a field's own condition: alone, with a bad flag's type, empty, beside
/// `$gt` and `$exists`, and on a dotted path (q7s3f.jsonl lines 286, 294 to 298): code 2,
/// "$options needs a $regex".
#[test]
fn a_lone_options_is_refused_in_a_field_condition() {
    let cases = pick(
        cases_r15(),
        &["q7s3f0286", "q7s3f0294", "q7s3f0295", "q7s3f0296", "q7s3f0297", "q7s3f0298"],
    );
    assert!(cases.iter().all(|c| c.ids.is_none()));
    assert_recorded("a field's condition", cases);
}

/// Under `$not` and inside an `$elemMatch` (q7s3f.jsonl lines 287 and 288).
#[test]
fn a_lone_options_is_refused_under_not_and_inside_an_elem_match() {
    assert_recorded("$not and $elemMatch", pick(cases_r15(), &["q7s3f0287", "q7s3f0288"]));
}

/// Inside `$and`, `$or` and `$nor` (q7s3f.jsonl lines 291 to 293).
#[test]
fn a_lone_options_is_refused_inside_and_or_and_nor() {
    assert_recorded("$and $or $nor", pick(cases_r15(), &["q7s3f0291", "q7s3f0292", "q7s3f0293"]));
}

/// Beside a `$regex` it is the pattern's flags, under `$not` and `$elemMatch` too (q7s3f.jsonl
/// lines 289, 290 and 300).
#[test]
fn an_options_beside_a_regex_is_the_patterns_flags() {
    let cases = pick(cases_r15(), &["q7s3f0289", "q7s3f0290", "q7s3f0300"]);
    assert!(cases.iter().all(|c| c.ids.is_some()));
    assert_recorded("$options beside $regex", cases);
}
