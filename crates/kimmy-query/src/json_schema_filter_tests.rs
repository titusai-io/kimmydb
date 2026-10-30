//! `$jsonSchema` as a filter: the engine itself lives in `kimmy_core::json_schema`
//! (ADR-257), and what is tested here is how a filter holds it.

use bson::{Bson, Document, doc};

use crate::filter::{Filter, matches, parse, parse_condition_document};

fn hits(filter: Document, d: Document) -> bool {
    matches(&parse(&filter).expect("parses"), &d).expect("evaluates")
}

#[test]
fn a_top_level_schema_matches_the_documents_it_accepts() {
    let f = doc! {"$jsonSchema": {"required": ["a"], "properties": {"a": {"bsonType": "int"}}}};
    assert!(hits(f.clone(), doc! {"a": 1}));
    assert!(!hits(f.clone(), doc! {"a": "x"}));
    assert!(!hits(f, doc! {}));
    assert!(matches!(parse(&doc! {"$jsonSchema": {}}).unwrap(), Filter::JsonSchema(_)));
}

#[test]
fn it_combines_with_other_clauses_and_the_logical_operators() {
    let schema = || doc! {"required": ["a"]};
    assert!(hits(doc! {"$jsonSchema": schema(), "b": 2}, doc! {"a": 1, "b": 2}));
    assert!(!hits(doc! {"$jsonSchema": schema(), "b": 2}, doc! {"a": 1, "b": 3}));
    assert!(hits(doc! {"$and": [{"$jsonSchema": schema()}, {"b": 2}]}, doc! {"a": 1, "b": 2}));
    assert!(hits(doc! {"$or": [{"$jsonSchema": schema()}, {"b": 2}]}, doc! {"b": 2}));
    // $nor finds the documents that break the schema.
    let bad = doc! {"$nor": [{"$jsonSchema": schema()}]};
    assert!(hits(bad.clone(), doc! {"b": 1}));
    assert!(!hits(bad, doc! {"a": 1}));
}

#[test]
fn it_tests_the_document_elements_of_an_elem_match() {
    let f = doc! {"l": {"$elemMatch": {"$jsonSchema": {"required": ["q"]}}}};
    assert!(hits(f.clone(), doc! {"l": [{"x": 1}, {"q": 1}]}));
    assert!(!hits(f.clone(), doc! {"l": [{"x": 1}, 5]}));
    assert!(!hits(f, doc! {"l": [1, 2]}));
}

#[test]
fn it_tests_the_document_elements_of_a_pull_condition() {
    let f = parse_condition_document(&doc! {"$jsonSchema": {"required": ["q"]}}).unwrap();
    assert!(crate::filter::matches_pull_element(&f, &bson::bson!({"q": 1})).unwrap());
    assert!(!crate::filter::matches_pull_element(&f, &bson::bson!({"x": 1})).unwrap());
    assert!(!crate::filter::matches_pull_element(&f, &bson::Bson::Int32(1)).unwrap());
}

#[test]
fn a_field_level_schema_is_refused() {
    for f in [
        doc! {"a": {"$jsonSchema": {}}},
        doc! {"a": {"$not": {"$jsonSchema": {}}}},
        doc! {"a": {"$gt": 1, "$jsonSchema": {}}},
        doc! {"$or": [{"a": {"$jsonSchema": {}}}]},
    ] {
        let e = parse(&f).expect_err("refused");
        assert!(matches!(e, kimmy_core::Error::InvalidQuery(_)), "{e:?}");
        assert!(e.to_string().contains("$jsonSchema"), "{e}");
    }
}

#[test]
fn a_bad_schema_is_a_bad_filter() {
    for f in [
        doc! {"$jsonSchema": 5},
        doc! {"$jsonSchema": "x"},
        doc! {"$jsonSchema": {"format": "email"}},
        doc! {"$nor": [{"$jsonSchema": {"properties": {"a": {"$ref": "#"}}}}]},
        doc! {"l": {"$elemMatch": {"$jsonSchema": {"default": 1}}}},
    ] {
        let e = parse(&f).expect_err("refused");
        assert!(matches!(e, kimmy_core::Error::InvalidQuery(_)), "{e:?}");
    }
}

#[test]
fn partial_index_filters_do_not_take_it() {
    use kimmy_core::PartialFilter;
    assert!(PartialFilter::parse(&doc! {"$jsonSchema": {}}).is_err());
    assert!(PartialFilter::parse(&doc! {"a": {"$jsonSchema": {}}}).is_err());
}

#[test]
fn the_filter_clones_and_compares() {
    let a = parse(&doc! {"$jsonSchema": {"required": ["a"]}}).unwrap();
    assert_eq!(a.clone(), a);
    assert_ne!(a, parse(&doc! {"$jsonSchema": {"required": ["b"]}}).unwrap());
}

#[test]
fn a_filters_regex_budget_is_the_one_a_schemas_patterns_are_charged_to() {
    let mut filter = Vec::new();
    for _ in 0..31 {
        filter.push(Bson::Document(doc! {"s": {"$regex": "a"}}));
    }
    let too_many = doc! {"$and": filter, "$jsonSchema": {"patternProperties": {"a": {}, "b": {}}}};
    assert!(parse(&too_many).is_err());
}

#[test]
fn array_filters_refuse_a_schema_in_both_forms() {
    for filters in [
        vec![doc! {"$jsonSchema": {"required": ["q"]}}],
        vec![doc! {"e": {"$jsonSchema": {"required": ["q"]}}}],
        vec![doc! {"$and": [{"e": {"$jsonSchema": {}}}]}],
    ] {
        let update = doc! {"$set": {"l.$[e].ok": true}};
        let err = crate::update::parse_with_filters(&update, &filters)
            .expect_err(&format!("{filters:?} is refused"));
        assert!(
            matches!(err, kimmy_core::Error::InvalidUpdate(_) | kimmy_core::Error::InvalidQuery(_)),
            "{err:?}"
        );
    }
}
