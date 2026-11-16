//! A6: a malformed field name or specification is refused at parse (q7s4d, q7s4a: 22 cases,
//! codes 51272, 40352, 40353, 16410, 15998, 16872, 40176, 40235, 40236, 40160, 40158, 15976).

use super::common::{refused, run};
use bson::{Document, doc};
use kimmy_query::update_pipeline::UpdatePipeline;

fn stage(name: &str, spec: impl Into<bson::Bson>) -> Vec<Document> {
    let spec: bson::Bson = spec.into();
    vec![doc! { name: spec }]
}

#[test]
fn a_project_with_no_field_is_refused() {
    // d:17, a:26 (51272)
    assert!(refused(&stage("$project", doc! {})).contains("at least one field"));
}

#[test]
fn a_project_name_that_is_no_path_is_refused() {
    // d:20 "$a" (16410), d:21 "a." (40353), d:22 "" (40352); the shapes of the query branch's
    // R26 (q7s3i 187 to 228) for "a..b" and ".a" (15998).
    for name in ["$a", "a.", "", "a..b", ".a", "a.$a"] {
        let message = refused(&stage("$project", doc! { name: 1 }));
        assert!(message.contains("$project field"), "{name:?}: {message}");
        // The exclusion form and a computed one read the name the same way.
        refused(&stage("$project", doc! { name: 0 }));
        refused(&stage("$project", doc! { name: "$x" }));
    }
}

#[test]
fn a_project_name_that_reads_as_before_is_accepted() {
    // `a.$ref` and `a.b$` are names (q7s3i 208 to 219); a plain dotted path and `_id`.
    for name in ["a.$ref", "a.b$", "a.b", "_id", "a.0"] {
        kimmy_query::aggregate::parse(&stage("$project", doc! { name: 1 }))
            .unwrap_or_else(|e| panic!("{name:?}: {e}"));
    }
}

#[test]
fn a_field_path_that_is_no_path_in_an_expression_is_refused() {
    // d:28 "$" (16872), d:30 "$a." (40353), d:31 "$.a" (15998). Where ours left the field out
    // or wrote the string "$".
    for value in ["$", "$a.", "$.a"] {
        refused(&stage("$project", doc! { "a": value }));
        refused(&stage("$addFields", doc! { "a": value }));
        refused(&stage("$group", doc! { "_id": value }));
    }
    // UNRECORDED in an expression: an empty segment inside and a segment that starts with $.
    refused(&stage("$addFields", doc! { "a": "$a..b" }));
    refused(&stage("$addFields", doc! { "a": "$a.$b" }));
    // A variable and a plain path are not touched.
    run(&stage("$addFields", doc! { "a": "$$ROOT.x", "b": "$x.y" }), vec![doc! { "_id": 1 }])
        .unwrap();
}

#[test]
fn an_add_fields_name_that_is_no_path_is_refused() {
    // d:32 "$a" (16410), d:33 "a..b" (15998): ours wrote `a: {"": {b: 1}}`.
    for stage_name in ["$addFields", "$set"] {
        for name in ["$a", "a..b", "a.", "", ".a"] {
            let message = refused(&stage(stage_name, doc! { name: 1 }));
            assert!(message.contains("field"), "{stage_name} {name:?}: {message}");
        }
    }
}

#[test]
fn a_set_that_names_a_path_and_one_inside_it_is_refused() {
    // d:35 `$set {a: 1, "a.b": 2}` (40176), whichever comes first.
    for spec in
        [doc! { "a": 1, "a.b": 2 }, doc! { "a.b": 2, "a": 1 }, doc! { "a.b": 1, "a.b.c": 2 }]
    {
        let message = refused(&stage("$set", spec.clone()));
        assert!(message.contains("conflicting paths"), "{spec}: {message}");
        refused(&stage("$addFields", spec));
    }
    // Names that only share a prefix are not nested, and two paths of one parent are fine.
    let out =
        run(&stage("$set", doc! { "a": 1, "ab": 2, "c.d": 3, "c.e": 4 }), vec![doc! { "_id": 1 }])
            .unwrap();
    assert_eq!(out, vec![doc! { "_id": 1, "a": 1, "ab": 2, "c": { "d": 3, "e": 4 } }]);
}

#[test]
fn a_pipeline_update_refuses_what_a_stage_refuses() {
    // The parsers are shared: `$set {"a..b": 1}` would store `a: {"": {b: 1}}`, and
    // `$project {"a.": 1}` would store `{_id}` alone.
    for stage in [
        doc! { "$set": { "a..b": 1 } },
        doc! { "$set": { "a": 1, "a.b": 2 } },
        doc! { "$project": { "a.": 1 } },
        doc! { "$project": { "": 1 } },
        doc! { "$project": {} },
    ] {
        let pipeline = [bson::Bson::Document(stage.clone())];
        assert!(UpdatePipeline::parse(&pipeline).is_err(), "{stage} must be refused");
    }
}

#[test]
fn a_sort_with_no_key_or_a_key_that_is_no_path_is_refused() {
    // a:214 `{}` (15976), d:52 "" (40352), d:53 "a." (40353), d:57 "$a" (16410).
    assert!(refused(&stage("$sort", doc! {})).contains("at least one sort key"));
    for name in ["", "a.", "$a", "a..b"] {
        let message = refused(&stage("$sort", doc! { name: 1 }));
        assert!(message.contains("$sort key"), "{name:?}: {message}");
    }
    // Inside a $facet branch and a $lookup pipeline too.
    refused(&stage("$facet", doc! { "f": [{ "$sort": {} }] }));
    // A find's sort is not changed: an empty one is no order.
    assert!(kimmy_query::shape::parse_sort(&doc! {}).unwrap().is_empty());
}

#[test]
fn an_unwind_path_that_is_no_path_is_refused() {
    // d:63 "$a." (40353): ours answered no documents. d:62 "$" (40352) was refused already.
    refused(&stage("$unwind", "$a."));
    refused(&stage("$unwind", doc! { "path": "$a..b" }));
    refused(&stage("$unwind", "$"));
}

#[test]
fn a_group_output_name_with_a_dot_or_a_dollar_is_refused() {
    // d:40 "a.b" (40235), d:41 "$a" (40236).
    let message = refused(&stage("$group", doc! { "_id": null, "a.b": { "$sum": 1 } }));
    assert!(message.contains("cannot contain '.'"), "{message}");
    let message = refused(&stage("$group", doc! { "_id": null, "$a": { "$sum": 1 } }));
    assert!(message.contains("operator name"), "{message}");
    run(&stage("$group", doc! { "_id": null, "a": { "$sum": 1 } }), vec![doc! { "_id": 1 }])
        .unwrap();
}

#[test]
fn a_count_name_with_a_dot_or_a_dollar_is_refused() {
    // d:77, a:274 "a.b" (40160); d:78, a:275 "$a" and "$c" (40158).
    assert!(refused(&stage("$count", "a.b")).contains("cannot contain '.'"));
    assert!(refused(&stage("$count", "$c")).contains("$-prefixed"));
    let out = run(&stage("$count", "n"), vec![doc! { "_id": 1 }]).unwrap();
    assert_eq!(out, vec![doc! { "n": 1 }]);
}

#[test]
fn a_nested_document_and_a_dotted_key_of_one_parent_do_not_conflict() {
    // q7s4g 5, 6 (`$addFields`), 20, 21 (`$set`), 44, 45 (a pipeline update): `{o: {z: 1}}` is
    // `o.z`, which sits beside `o.w` in either order; the reference applies them.
    for spec in [doc! { "o": { "z": 1 }, "o.w": 2 }, doc! { "o.w": 2, "o": { "z": 1 } }] {
        for name in ["$addFields", "$set"] {
            kimmy_query::aggregate::parse(&stage(name, spec.clone()))
                .unwrap_or_else(|e| panic!("{name} {spec}: {e}"));
        }
        let pipeline = [bson::Bson::Document(doc! { "$set": spec.clone() })];
        UpdatePipeline::parse(&pipeline).unwrap_or_else(|e| panic!("{spec}: {e}"));
    }
    // Leaves that are one inside the other still conflict: `o` is a leaf and `o.w` another, and
    // so are `o.z` and `o.z.y`.
    for spec in [
        doc! { "o": 1, "o.w": 2 },
        doc! { "o": { "z": 1 }, "o.z.y": 2 },
        doc! { "o.z.y": 2, "o": { "z": 1 } },
    ] {
        let message = refused(&stage("$set", spec.clone()));
        assert!(message.contains("conflicting paths"), "{spec}: {message}");
    }
}
