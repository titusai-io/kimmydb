//! Pipeline-style updates (ADR-265): what the stages do to one document and what is refused.

use bson::{Bson, Document, doc};
use kimmy_core::Error;

use crate::update::{self, Update};
use crate::update_pipeline::UpdatePipeline;

fn stages(docs: Vec<Document>) -> Vec<Bson> {
    docs.into_iter().map(Bson::Document).collect()
}

fn apply(pipeline: Vec<Document>, mut stored: Document) -> kimmy_core::Result<Document> {
    let update = update::parse_pipeline(&stages(pipeline), &[])?;
    update::apply(&update, &mut stored, 0)?;
    Ok(stored)
}

fn refused(pipeline: Vec<Document>) -> String {
    UpdatePipeline::parse(&stages(pipeline)).expect_err("must be refused").to_string()
}

// ---------------------------------------------------------------------------
// The three stage families
// ---------------------------------------------------------------------------

#[test]
fn set_computes_a_field_from_others_and_keeps_the_rest() {
    let out = apply(
        vec![doc! {"$set": {"total": {"$add": ["$a", "$b"]}, "label": {"$concat": ["n", "$k"]}}}],
        doc! {"_id": 1, "a": 2, "b": 3, "k": "x"},
    )
    .unwrap();
    assert_eq!(out, doc! {"_id": 1, "a": 2, "b": 3, "k": "x", "total": 5, "label": "nx"});
    // `$addFields` is the same stage under its other name.
    let again = apply(
        vec![doc! {"$addFields": {"total": {"$add": ["$a", "$b"]}, "label": {"$concat": ["n", "$k"]}}}],
        doc! {"_id": 1, "a": 2, "b": 3, "k": "x"},
    )
    .unwrap();
    assert_eq!(out, again);
}

#[test]
fn a_stored_integer_result_has_the_references_width() {
    // ADR-268: the width of an expression's integer result is the type that is stored.
    let stored = || doc! {"_id": 1, "a": 2, "b": 3, "big": i32::MAX, "l": 5i64};
    let n = |expr: Bson| {
        apply(vec![doc! {"$set": {"n": expr}}], stored()).unwrap().get("n").cloned().unwrap()
    };
    assert_eq!(n(bson::bson!({"$add": ["$a", "$b"]})), Bson::Int32(5));
    assert_eq!(n(bson::bson!({"$multiply": ["$a", "$b"]})), Bson::Int32(6));
    assert_eq!(n(bson::bson!({"$add": ["$big", 1]})), Bson::Int64(2_147_483_648));
    assert_eq!(n(bson::bson!({"$add": ["$a", "$l"]})), Bson::Int64(7));
    assert_eq!(n(bson::bson!({"$add": ["$a", 0.5]})), Bson::Double(2.5));
    assert_eq!(n(bson::bson!({"$mod": ["$b", "$a"]})), Bson::Int32(1));
    assert_eq!(n(bson::bson!({"$size": [["$a", "$b"]]})), Bson::Int32(2));
}

#[test]
fn a_result_equal_to_the_stored_int32_leaves_the_document_byte_equal() {
    // The reference keeps the `Int32` and counts no change; an `Int64` would be written.
    let stored = doc! {"_id": 1, "a": 2, "b": 3, "total": 5};
    let out =
        apply(vec![doc! {"$set": {"total": {"$add": ["$a", "$b"]}}}], stored.clone()).unwrap();
    assert_eq!(bson::serialize_to_vec(&out).unwrap(), bson::serialize_to_vec(&stored).unwrap());
}

#[test]
fn stages_run_in_order_and_each_sees_the_one_before() {
    let out = apply(
        vec![
            doc! {"$set": {"a": {"$add": ["$a", 1]}}},
            doc! {"$set": {"b": {"$multiply": ["$a", 10]}}},
            doc! {"$unset": "a"},
        ],
        doc! {"_id": 1, "a": 4},
    )
    .unwrap();
    assert_eq!(out, doc! {"_id": 1, "b": 50});
}

#[test]
fn array_fields_can_be_computed_over() {
    let out = apply(
        vec![doc! {"$set": {
            "n": {"$size": "$items"},
            "sum": {"$reduce": {"input": "$items", "initialValue": 0, "in": {"$add": ["$$value", "$$this.q"]}}},
            "big": {"$filter": {"input": "$items", "as": "i", "cond": {"$gte": ["$$i.q", 5]}}},
            "items": {"$map": {"input": "$items", "as": "i", "in": {"sku": "$$i.sku", "q": {"$add": ["$$i.q", 1]}}}},
        }}],
        doc! {"_id": 1, "items": [{"sku": "a", "q": 5}, {"sku": "b", "q": 2}]},
    )
    .unwrap();
    assert_eq!(
        out.get_array("items").unwrap(),
        &vec![Bson::Document(doc! {"sku": "a", "q": 6}), Bson::Document(doc! {"sku": "b", "q": 3}),]
    );
    // `n`, `sum` and `big` were computed from the array as it was before this stage.
    assert_eq!(out.get("n"), Some(&Bson::Int32(2)));
    assert_eq!(out.get("sum"), Some(&Bson::Int32(7)));
    assert_eq!(out.get_array("big").unwrap(), &vec![Bson::Document(doc! {"sku": "a", "q": 5})]);
}

#[test]
fn root_current_and_remove_read_as_they_do_in_set() {
    let out = apply(
        vec![doc! {"$set": {
            "copy": "$$ROOT",
            "seen": "$$CURRENT.a",
            "gone": "$$REMOVE",
            "b": {"$cond": [{"$gt": ["$a", 0]}, "$b", "$$REMOVE"]},
        }}],
        doc! {"_id": 1, "a": 1, "b": 2, "gone": 9},
    )
    .unwrap();
    assert_eq!(
        out,
        doc! {"_id": 1, "a": 1, "b": 2, "copy": {"_id": 1, "a": 1, "b": 2, "gone": 9}, "seen": 1}
    );
}

#[test]
fn null_and_missing_propagate_as_in_an_aggregation() {
    let out = apply(
        vec![doc! {"$set": {
            "sum": {"$add": ["$a", "$nope"]},
            "up": {"$toUpper": "$nope"},
            "dflt": {"$ifNull": ["$nope", "d"]},
            "path": "$nope",
        }}],
        doc! {"_id": 1, "a": 1},
    )
    .unwrap();
    // A missing field in `$add` is null; a field set from a missing path is left out, as
    // the reference's `$addFields` does: `path` is not added.
    assert_eq!(out, doc! {"_id": 1, "a": 1, "sum": Bson::Null, "up": "", "dflt": "d"});
}

#[test]
fn a_missing_path_removes_an_existing_field_and_adds_nothing() {
    // Reference 6.0.5: `$set: {p: "$nope"}` over `{p: 5, q: 1}` is `{q: 1}`.
    let stored = doc! {"_id": 1, "p": 5, "q": 1, "o": {"p": 9, "r": 1}, "items": [1, 2]};
    assert_eq!(
        apply(vec![doc! {"$set": {"p": "$nope"}}], stored.clone()).unwrap(),
        doc! {"_id": 1, "q": 1, "o": {"p": 9, "r": 1}, "items": [1, 2]}
    );
    // A dotted target leaves the embedded document without the field.
    assert_eq!(
        apply(vec![doc! {"$set": {"o.p": "$nope"}}], stored.clone()).unwrap(),
        doc! {"_id": 1, "p": 5, "q": 1, "o": {"r": 1}, "items": [1, 2]}
    );
    // Through `$$ROOT` and `$$CURRENT` paths, and a variable's path.
    for expr in ["$$ROOT.nope", "$$CURRENT.nope", "$nope.deeper", "$o.nope"] {
        assert_eq!(
            apply(vec![doc! {"$set": {"p": expr}}], stored.clone()).unwrap(),
            doc! {"_id": 1, "q": 1, "o": {"p": 9, "r": 1}, "items": [1, 2]},
            "{expr}"
        );
    }
    let out = apply(
        vec![doc! {"$set": {"p": {"$let": {"vars": {"v": {"a": 1}}, "in": "$$v.nope"}}}}],
        stored.clone(),
    )
    .unwrap();
    assert!(out.get("p").is_none(), "{out:?}");
    // A missing value in an object expression omits the key, in an array it is null.
    let out = apply(
        vec![doc! {"$set": {"obj": {"a": "$nope", "b": 1}, "arr": ["$nope", 2]}}],
        stored.clone(),
    )
    .unwrap();
    assert_eq!(out.get_document("obj").unwrap(), &doc! {"b": 1});
    assert_eq!(out.get_array("arr").unwrap(), &vec![Bson::Null, Bson::Int32(2)]);
    // Null is a value: it is stored.
    let nulled = apply(vec![doc! {"$set": {"p": Bson::Null}}], stored.clone()).unwrap();
    assert_eq!(nulled.get("p"), Some(&Bson::Null));
    // A field that exists with a null value is not missing.
    let stored = doc! {"_id": 1, "n": Bson::Null};
    let out = apply(vec![doc! {"$set": {"copy": "$n"}}], stored).unwrap();
    assert_eq!(out.get("copy"), Some(&Bson::Null));
}

#[test]
fn an_expression_error_is_the_aggregations() {
    let err = apply(vec![doc! {"$set": {"x": {"$add": ["$s", 1]}}}], doc! {"_id": 1, "s": "text"})
        .unwrap_err();
    let agg = crate::aggregate::parse(&[doc! {"$set": {"x": {"$add": ["$s", 1]}}}])
        .and_then(|st| {
            crate::aggregate::apply(&st[0], vec![doc! {"_id": 1, "s": "text"}], &Default::default())
        })
        .unwrap_err();
    assert_eq!(err.to_string(), agg.to_string());
    // A parse error likewise.
    let message = refused(vec![doc! {"$set": {"x": {"$nope": 1}}}]);
    assert!(message.contains("$nope"), "{message}");
}

#[test]
fn unset_removes_paths_and_project_includes_or_excludes() {
    let stored = doc! {"_id": 1, "a": 1, "b": {"c": 2, "d": 3}, "e": 4};
    assert_eq!(
        apply(vec![doc! {"$unset": ["a", "b.c"]}], stored.clone()).unwrap(),
        doc! {"_id": 1, "b": {"d": 3}, "e": 4}
    );
    assert_eq!(
        apply(vec![doc! {"$unset": "e"}], stored.clone()).unwrap(),
        doc! {"_id": 1, "a": 1, "b": {"c": 2, "d": 3}}
    );
    // Inclusion keeps `_id` as it does in an aggregation.
    assert_eq!(
        apply(vec![doc! {"$project": {"a": 1, "b.d": 1}}], stored.clone()).unwrap(),
        doc! {"_id": 1, "a": 1, "b": {"d": 3}}
    );
    assert_eq!(
        apply(vec![doc! {"$project": {"a": 0, "e": 0}}], stored.clone()).unwrap(),
        doc! {"_id": 1, "b": {"c": 2, "d": 3}}
    );
    // And computes, then keeps `_id`.
    assert_eq!(
        apply(vec![doc! {"$project": {"a": 1, "twice": {"$multiply": ["$a", 2]}}}], stored)
            .unwrap(),
        doc! {"_id": 1, "a": 1, "twice": 2}
    );
}

#[test]
fn replace_with_and_replace_root_replace_the_document_and_keep_the_id() {
    let stored = doc! {"_id": 7, "a": {"x": 1}, "b": 2};
    // Without an `_id` in the result, the stored one is kept, first.
    for stage in [doc! {"$replaceWith": "$a"}, doc! {"$replaceRoot": {"newRoot": "$a"}}] {
        assert_eq!(apply(vec![stage], stored.clone()).unwrap(), doc! {"_id": 7, "x": 1});
    }
    assert_eq!(
        apply(vec![doc! {"$replaceWith": {"n": {"$add": ["$b", 1]}}}], stored.clone()).unwrap(),
        doc! {"_id": 7, "n": 3}
    );
    // With the same `_id`, in any position, the document is stored with `_id` first.
    assert_eq!(
        apply(vec![doc! {"$replaceWith": {"y": 1, "_id": "$_id"}}], stored.clone()).unwrap(),
        doc! {"_id": 7, "y": 1}
    );
    // A result that is not a document is the aggregation's error.
    let err = apply(vec![doc! {"$replaceWith": "$b"}], stored.clone()).unwrap_err().to_string();
    assert!(err.contains("$replaceRoot needs a document"), "{err}");
    let err = apply(vec![doc! {"$replaceWith": "$nope"}], stored).unwrap_err().to_string();
    assert!(err.contains("$replaceRoot needs a document"), "{err}");
}

#[test]
fn a_replace_with_later_stages_keeps_working_on_the_new_document() {
    let out = apply(
        vec![doc! {"$replaceWith": {"v": "$a"}}, doc! {"$set": {"w": {"$add": ["$v", 1]}}}],
        doc! {"_id": 1, "a": 1},
    )
    .unwrap();
    assert_eq!(out, doc! {"_id": 1, "v": 1, "w": 2});
}

// ---------------------------------------------------------------------------
// `_id`
// ---------------------------------------------------------------------------

#[test]
fn id_cannot_be_changed_to_another_documents_id() {
    let stored = doc! {"_id": 1, "a": 1};
    for pipeline in [
        vec![doc! {"$set": {"_id": 2}}],
        vec![doc! {"$replaceWith": {"_id": 2, "a": 1}}],
        vec![doc! {"$set": {"_id": "1"}}],
        vec![doc! {"$set": {"_id": Bson::Null}}],
        vec![doc! {"$set": {"x": 1}}, doc! {"$set": {"_id": 9}}],
        // A double is not an id, whatever its value.
        vec![doc! {"$set": {"_id": 1.0}}],
    ] {
        let err = apply(pipeline.clone(), stored.clone()).expect_err("must be refused");
        assert!(
            matches!(&err, Error::InvalidUpdate(m) if m == "_id is immutable and cannot be updated"),
            "{pipeline:?}: {err:?}"
        );
    }
    // Setting it to what it is is not a change.
    assert_eq!(
        apply(vec![doc! {"$set": {"_id": "$_id", "a": 2}}], stored).unwrap(),
        doc! {"_id": 1, "a": 2}
    );
}

#[test]
fn a_pipeline_that_removes_id_gets_the_stored_one_back_first() {
    // The reference judges the final result: a result without `_id` is given the stored one.
    let stored = doc! {"_id": 1, "a": 2, "b": 3};
    for (pipeline, want) in [
        (vec![doc! {"$unset": "_id"}], doc! {"_id": 1, "a": 2, "b": 3}),
        (vec![doc! {"$project": {"_id": 0, "a": 1}}], doc! {"_id": 1, "a": 2}),
        (vec![doc! {"$project": {"_id": 0}}], doc! {"_id": 1, "a": 2, "b": 3}),
        (vec![doc! {"$set": {"_id": "$$REMOVE", "c": 4}}], doc! {"_id": 1, "a": 2, "b": 3, "c": 4}),
        (vec![doc! {"$replaceWith": {"z": 1}}], doc! {"_id": 1, "z": 1}),
        // Taken out and put back last: the same document, `_id` first.
        (
            vec![
                doc! {"$set": {"keep": "$_id"}},
                doc! {"$unset": "_id"},
                doc! {"$set": {"_id": "$keep"}},
                doc! {"$unset": "keep"},
            ],
            doc! {"_id": 1, "a": 2, "b": 3},
        ),
    ] {
        assert_eq!(apply(pipeline.clone(), stored.clone()).unwrap(), want, "{pipeline:?}");
    }
}

#[test]
fn between_stages_id_is_whatever_the_stages_made_it() {
    // After `$replaceWith {a: "$a"}` there is no `_id` for the next stage to read.
    let out = apply(
        vec![
            doc! {"$replaceWith": {"a": "$a"}},
            doc! {"$set": {"x": "$_id", "t": {"$type": "$_id"}}},
        ],
        doc! {"_id": 1, "a": 2, "b": 3},
    )
    .unwrap();
    // `x` is not set from a missing field and `$type` says so; `_id` is restored at the end.
    assert_eq!(out, doc! {"_id": 1, "a": 2, "t": "missing"});
    let out = apply(
        vec![doc! {"$unset": "_id"}, doc! {"$set": {"seen": {"$type": "$_id"}}}],
        doc! {"_id": 1},
    )
    .unwrap();
    assert_eq!(out, doc! {"_id": 1, "seen": "missing"});
}

#[test]
fn id_changed_to_the_same_id_of_another_type_is_stored_as_it_is() {
    let out =
        apply(vec![doc! {"$set": {"_id": {"$toLong": "$_id"}}}], doc! {"_id": 1, "a": 1}).unwrap();
    assert_eq!(out.get("_id"), Some(&Bson::Int64(1)));
    let out =
        apply(vec![doc! {"$set": {"_id": {"$toDecimal": "$_id"}}}], doc! {"_id": 1i64, "a": 1})
            .unwrap();
    assert!(matches!(out.get("_id"), Some(Bson::Decimal128(_))));
    assert_eq!(out.keys().next().map(String::as_str), Some("_id"));
}

#[test]
fn an_empty_pipeline_is_a_no_op() {
    let stored = doc! {"_id": 1, "a": 1};
    assert_eq!(apply(vec![], stored.clone()).unwrap(), stored);
    let mut seed = doc! {"k": "e"};
    let update = update::parse_pipeline(&[], &[]).unwrap();
    update::apply_on_insert(&update, &mut seed, 0).unwrap();
    assert_eq!(seed, doc! {"k": "e"});
}

#[test]
fn a_document_without_an_id_may_not_gain_one() {
    // The seed of an upsert before it has been given one.
    let mut seed = doc! {"a": 1};
    let update = update::parse_pipeline(&stages(vec![doc! {"$set": {"_id": 5}}]), &[]).unwrap();
    assert!(update::apply_on_insert(&update, &mut seed, 0).is_err());
    let update = update::parse_pipeline(&stages(vec![doc! {"$set": {"b": 2}}]), &[]).unwrap();
    let mut seed = doc! {"a": 1};
    update::apply_on_insert(&update, &mut seed, 0).unwrap();
    assert_eq!(seed, doc! {"a": 1, "b": 2});
}

#[test]
fn the_id_is_stored_first() {
    // `$replaceWith` of a document that lists `_id` last.
    let out = apply(vec![doc! {"$replaceWith": {"a": 1, "_id": "$_id"}}], doc! {"_id": 3, "z": 0})
        .unwrap();
    assert_eq!(out.keys().collect::<Vec<_>>(), ["_id", "a"]);
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

#[test]
fn only_the_three_stage_families_are_allowed() {
    for stage in [
        doc! {"$match": {"a": 1}},
        doc! {"$group": {"_id": null}},
        doc! {"$sort": {"a": 1}},
        doc! {"$limit": 1},
        doc! {"$unwind": "$a"},
        doc! {"$lookup": {"from": "x", "localField": "a", "foreignField": "a", "as": "j"}},
        doc! {"$out": "x"},
        doc! {"$merge": {"into": "x"}},
        doc! {"$count": "n"},
        doc! {"$facet": {"f": []}},
        doc! {"$redact": "$$KEEP"},
        doc! {"$sample": {"size": 1}},
        doc! {"$unionWith": "x"},
        doc! {"$sortByCount": "$a"},
        doc! {"$skip": 1},
        doc! {"$nope": 1},
    ] {
        let name = stage.keys().next().unwrap().clone();
        let message = refused(vec![doc! {"$set": {"a": 1}}, stage]);
        assert!(
            message.starts_with("invalid update: a pipeline update allows only $addFields")
                && message.ends_with(&format!("found {name}")),
            "{message}"
        );
    }
}

#[test]
fn a_non_document_and_a_bad_stage_shape_are_refused() {
    for element in [Bson::Int32(1), Bson::String("$set".into()), Bson::Null, Bson::Array(vec![])] {
        let message =
            UpdatePipeline::parse(std::slice::from_ref(&element)).unwrap_err().to_string();
        assert!(
            message.starts_with("invalid update: each stage of a pipeline update is a document"),
            "{element:?}: {message}"
        );
    }
    let message = refused(vec![doc! {}]);
    assert!(message.contains("exactly one key"), "{message}");
    let message = refused(vec![doc! {"$set": {"a": 1}, "$unset": "b"}]);
    assert!(message.contains("exactly one key"), "{message}");
    // The stage's own operand rules are the aggregation's.
    assert!(refused(vec![doc! {"$set": 1}]).contains("$set takes a document"));
    assert!(refused(vec![doc! {"$unset": []}]).contains("$unset"));
    assert!(refused(vec![doc! {"$replaceWith": {"$nope": 1}}]).contains("$nope"));
}

#[test]
fn array_filters_are_refused_with_a_pipeline() {
    let filters = [doc! {"line.sku": "b"}];
    let err = update::parse_pipeline(&stages(vec![doc! {"$set": {"a": 1}}]), &filters).unwrap_err();
    assert!(
        err.to_string().starts_with(
            "invalid update: arrayFilters apply to update operators, not to a pipeline update"
        ),
        "{err}"
    );
}

#[test]
fn a_pipeline_update_has_no_matched_position() {
    let parsed = update::parse_pipeline(&stages(vec![doc! {"$set": {"a": 1}}]), &[]).unwrap();
    assert!(!parsed.uses_matched_position());
    assert_eq!(parsed.first_write_outside_matched("items"), Some("the update pipeline".into()));
}

// ---------------------------------------------------------------------------
// `$$NOW`
// ---------------------------------------------------------------------------

#[test]
fn now_is_one_date_for_the_whole_pipeline() {
    let update = update::parse_pipeline(
        &stages(vec![
            doc! {"$set": {"a": "$$NOW"}},
            doc! {"$set": {"b": "$$NOW", "same": {"$eq": ["$a", "$$NOW"]}}},
        ]),
        &[],
    )
    .unwrap();
    let mut doc = doc! {"_id": 1};
    update::apply(&update, &mut doc, 0).unwrap();
    assert_eq!(doc.get("a"), doc.get("b"));
    assert!(matches!(doc.get("a"), Some(Bson::DateTime(_))));
    assert_eq!(doc.get("same"), Some(&Bson::Boolean(true)));
    // And one date across the documents of a request: applied twice, the same date.
    let mut other = doc! {"_id": 2};
    update::apply(&update, &mut other, 0).unwrap();
    assert_eq!(doc.get("a"), other.get("a"));
}

#[test]
fn a_pipeline_update_is_not_a_replacement_or_operators() {
    let parsed = update::parse_pipeline(&stages(vec![doc! {"$set": {"a": 1}}]), &[]).unwrap();
    assert!(matches!(parsed, Update::Pipeline(_)));
    assert_eq!(
        parsed,
        update::parse_pipeline(&stages(vec![doc! {"$set": {"a": 1}}]), &[]).unwrap()
    );
    assert_ne!(
        parsed,
        update::parse_pipeline(&stages(vec![doc! {"$set": {"a": 2}}]), &[]).unwrap()
    );
}

#[test]
fn an_update_pipeline_is_held_to_a_thousand_stages() {
    let sets = |n: usize| vec![doc! {"$set": {"a": 1}}; n];
    // A thousand apply; one more is refused before any stage is read, in the words of an
    // update (the aggregation's say "a pipeline", and name `$facet` and sub-pipelines).
    let out = apply(sets(1_000), doc! {"_id": 1}).unwrap();
    assert_eq!(out, doc! {"_id": 1, "a": 1});
    assert_eq!(
        refused(sets(1_001)),
        "invalid update: an update pipeline has 1001 stages, over the limit of 1000"
    );
}
