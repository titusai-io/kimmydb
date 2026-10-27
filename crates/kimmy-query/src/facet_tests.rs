//! `$facet` (ADR-254): what it answers, and what it refuses.

use bson::{Bson, Document, doc};

use crate::aggregate::{self, Limits, Stage};

fn run(pipeline: &[Document], input: Vec<Document>) -> kimmy_core::Result<Vec<Document>> {
    run_with(pipeline, input, &Limits::default())
}

fn run_with(
    pipeline: &[Document],
    input: Vec<Document>,
    limits: &Limits,
) -> kimmy_core::Result<Vec<Document>> {
    let mut docs = input;
    for stage in aggregate::parse(pipeline)? {
        docs = aggregate::apply(&stage, docs, limits)?;
    }
    Ok(docs)
}

fn refused(pipeline: &[Document]) -> String {
    aggregate::parse(pipeline).expect_err("must be refused").to_string()
}

fn people() -> Vec<Document> {
    vec![
        doc! {"_id": 1, "city": "oslo", "age": 30},
        doc! {"_id": 2, "city": "oslo", "age": 40},
        doc! {"_id": 3, "city": "rome", "age": 50},
    ]
}

#[test]
fn every_facet_runs_over_the_same_input_and_the_answer_is_one_document() {
    let out = run(
        &[doc! {"$facet": {
            "total": [{"$count": "n"}],
            "byCity": [
                {"$group": {"_id": "$city", "n": {"$sum": 1}}},
                {"$sort": {"_id": 1}},
            ],
            "oldest": [{"$sort": {"age": -1}}, {"$limit": 1}, {"$project": {"age": 1}}],
        }}],
        people(),
    )
    .unwrap();
    assert_eq!(
        out,
        vec![doc! {
            "total": [{"n": 3i64}],
            "byCity": [{"_id": "oslo", "n": 2i64}, {"_id": "rome", "n": 1i64}],
            "oldest": [{"_id": 3, "age": 50}],
        }]
    );
}

#[test]
fn a_facet_does_not_change_what_the_next_one_sees() {
    let out = run(
        &[doc! {"$facet": {"one": [{"$limit": 1}], "all": [{"$count": "n"}], "none": [{"$match": {"age": 0}}]}}],
        people(),
    )
    .unwrap();
    assert_eq!(out[0].get_array("one").unwrap().len(), 1);
    assert_eq!(out[0].get_array("all").unwrap(), &vec![Bson::Document(doc! {"n": 3i64})]);
    assert!(out[0].get_array("none").unwrap().is_empty());
}

#[test]
fn an_empty_facet_pipeline_answers_the_input_and_an_empty_input_still_answers_a_document() {
    let out = run(&[doc! {"$facet": {"same": []}}], people()).unwrap();
    assert_eq!(out[0].get_array("same").unwrap().len(), 3);
    let out = run(&[doc! {"$facet": {"a": [{"$count": "n"}], "b": []}}], vec![]).unwrap();
    // `$count` of nothing is a zero, as it is outside a facet.
    assert_eq!(out, vec![doc! {"a": [{"n": 0i64}], "b": []}]);
}

#[test]
fn stages_after_a_facet_read_its_one_document() {
    let out = run(
        &[
            doc! {"$facet": {"byCity": [{"$group": {"_id": "$city", "n": {"$sum": 1}}}]}},
            doc! {"$unwind": "$byCity"},
            doc! {"$replaceRoot": {"newRoot": "$byCity"}},
            doc! {"$sort": {"_id": 1}},
        ],
        people(),
    )
    .unwrap();
    assert_eq!(out, vec![doc! {"_id": "oslo", "n": 2i64}, doc! {"_id": "rome", "n": 1i64}]);
    // And a facet is a stage like any other in the middle of a pipeline.
    let out = run(
        &[doc! {"$match": {"city": "oslo"}}, doc! {"$facet": {"n": [{"$count": "n"}]}}],
        people(),
    )
    .unwrap();
    assert_eq!(out, vec![doc! {"n": [{"n": 2i64}]}]);
}

#[test]
fn now_is_the_same_date_in_every_facet() {
    let stages = aggregate::parse(&[doc! {"$facet": {
        "a": [{"$addFields": {"t": "$$NOW"}}],
        "b": [{"$addFields": {"t": "$$NOW"}}],
    }}])
    .unwrap();
    let out = aggregate::apply(&stages[0], vec![doc! {"_id": 1}], &Limits::default()).unwrap();
    let t = |name: &str| {
        out[0].get_array(name).unwrap()[0].as_document().unwrap().get_datetime("t").copied()
    };
    assert_eq!(t("a").unwrap(), t("b").unwrap());
}

#[test]
fn a_facet_that_fails_fails_the_stage() {
    let err = run(
        &[doc! {"$facet": {"ok": [{"$count": "n"}], "bad": [{"$addFields": {"x": {"$add": ["$city", 1]}}}]}}],
        people(),
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("$add needs numbers"), "{err}");
}

#[test]
fn a_malformed_facet_is_refused_at_parse() {
    assert!(refused(&[doc! {"$facet": {}}]).contains("at least one facet"));
    assert!(refused(&[doc! {"$facet": 1}]).contains("takes a document"));
    assert!(refused(&[doc! {"$facet": {"a": 1}}]).contains("an array of pipeline stages"));
    assert!(
        refused(&[doc! {"$facet": {"a": {"$count": "n"}}}]).contains("an array of pipeline stages")
    );
    assert!(
        refused(&[doc! {"$facet": {"a": [1]}}]).contains("each $facet `a` stage is a document")
    );
    for name in ["", "$a", "a.b"] {
        let message = refused(&[doc! {"$facet": {name: []}}]);
        assert!(message.contains("becomes a field of the answer"), "{name:?}: {message}");
    }
    assert!(
        refused(&[doc! {"$facet": {"a": [{"$nope": 1}]}}])
            .contains("$nope is not a pipeline stage")
    );
    // A stage that is wrong is refused with the stage's own message, inside a facet too.
    assert!(refused(&[doc! {"$facet": {"a": [{"$limit": -1}]}}]).contains("$limit"));
    // Closed on both sides: more than the limit of facets.
    let mut many = Document::new();
    for i in 0..=aggregate::MAX_FACETS {
        many.insert(format!("f{i}"), Bson::Array(vec![]));
    }
    assert!(refused(&[doc! {"$facet": many}]).contains("over the limit"));
}

#[test]
fn a_facet_cannot_hold_a_facet_or_a_vector_search() {
    let nested = refused(&[doc! {"$facet": {"a": [{"$facet": {"b": []}}]}}]);
    assert_eq!(nested, "invalid query: $facet cannot be used inside a $facet (in `a`)");
    let vector = refused(&[doc! {"$facet": {"a": [{"$vectorSearch": {"query": "x"}}]}}]);
    assert_eq!(vector, "invalid query: $vectorSearch cannot be used inside a $facet (in `a`)");
    // Nor in a facet of a `$lookup` sub-pipeline.
    let deep = refused(&[doc! {"$lookup": {"from": "x", "pipeline": [
        {"$facet": {"a": [{"$facet": {"b": []}}]}}
    ], "as": "j"}}]);
    assert!(deep.contains("$facet cannot be used inside a $facet"), "{deep}");
}

#[test]
fn the_outputs_together_may_not_pass_the_document_limit() {
    let limits = Limits { max_documents: 5 };
    // Three and three: each is under the ceiling and together they are over.
    let err = run_with(
        &[doc! {"$facet": {"a": [{"$limit": 3}], "b": [{"$limit": 3}]}}],
        (0..10).map(|i| doc! {"_id": i}).collect(),
        &limits,
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("$facet produced 6 documents, over the pipeline limit of 5"), "{err}");
    // Exactly at the ceiling is fine.
    let out = run_with(
        &[doc! {"$facet": {"a": [{"$limit": 3}], "b": [{"$limit": 2}]}}],
        (0..10).map(|i| doc! {"_id": i}).collect(),
        &limits,
    )
    .unwrap();
    assert_eq!(out[0].get_array("a").unwrap().len(), 3);
}

#[test]
fn an_answer_over_the_document_size_cap_is_refused_not_cut_short() {
    let big = "x".repeat(6 * 1024 * 1024);
    let input: Vec<Document> = (0..3).map(|i| doc! {"_id": i, "pad": big.clone()}).collect();
    let err = run(&[doc! {"$facet": {"all": []}}], input).unwrap_err();
    assert!(matches!(err, kimmy_core::Error::AnswerTooLarge { .. }), "{err:?}");
    let message = err.to_string();
    assert!(
        message.contains("$facet answer") && message.contains("$limit and $project"),
        "{message}"
    );
    // The same data trimmed by a facet that keeps one is fine.
    let input: Vec<Document> = (0..3).map(|i| doc! {"_id": i, "pad": big.clone()}).collect();
    let out =
        run(&[doc! {"$facet": {"one": [{"$limit": 1}], "n": [{"$count": "n"}]}}], input).unwrap();
    assert_eq!(out[0].get_array("one").unwrap().len(), 1);
}

#[test]
fn a_facet_that_holds_a_lookup_needs_storage_and_the_pure_pipeline_refuses_it() {
    let stages = aggregate::parse(&[doc! {"$facet": {
        "joined": [{"$lookup": {"from": "o", "localField": "a", "foreignField": "b", "as": "j"}}],
        "plain": [],
    }}])
    .unwrap();
    assert!(stages[0].needs_storage());
    let err = aggregate::apply(&stages[0], vec![doc! {"a": 1}], &Limits::default()).unwrap_err();
    assert!(err.to_string().contains("must be run by the executor"), "{err}");
    let plain = aggregate::parse(&[doc! {"$facet": {"a": []}}]).unwrap();
    assert!(!plain[0].needs_storage());
    assert!(matches!(&plain[0], Stage::Facet(f) if f.len() == 1));
}

#[test]
fn a_pipeline_with_facets_reads_the_clock_once() {
    let before = crate::expr::clock_reads();
    let stages = aggregate::parse(&[
        doc! {"$match": {"$expr": {"$lt": ["$at", "$$NOW"]}}},
        doc! {"$facet": {
            "a": [{"$addFields": {"t": "$$NOW"}}],
            "b": [{"$match": {"$expr": {"$gt": ["$$NOW", "$at"]}}}, {"$lookup": {
                "from": "x", "pipeline": [{"$addFields": {"deep": "$$NOW"}}], "as": "j"}}],
        }},
    ])
    .unwrap();
    assert_eq!(crate::expr::clock_reads() - before, 1);
    drop(stages);
}

#[test]
fn an_answer_over_the_cap_is_refused_before_a_later_facet_runs() {
    // One facet emits one document holding everything: a document count of 1 and
    // 18 MiB. The facet after it would fail, and must never be reached.
    let big = "x".repeat(6 * 1024 * 1024);
    let input: Vec<Document> = (0..3).map(|i| doc! {"_id": i, "pad": big.clone()}).collect();
    let err = run(
        &[doc! {"$facet": {
            "everything": [{"$group": {"_id": null, "all": {"$push": "$$ROOT"}}}],
            "never": [{"$addFields": {"x": {"$add": ["$_id", "text"]}}}],
        }}],
        input,
    )
    .unwrap_err();
    assert!(matches!(err, kimmy_core::Error::AnswerTooLarge { .. }), "{err:?}");
    // The same facets, with the failing one first, report that failure instead.
    let input: Vec<Document> = (0..3).map(|i| doc! {"_id": i, "pad": big.clone()}).collect();
    let err = run(
        &[doc! {"$facet": {
            "first": [{"$addFields": {"x": {"$add": ["$_id", "text"]}}}],
            "everything": [{"$group": {"_id": null, "all": {"$push": "$$ROOT"}}}],
        }}],
        input,
    )
    .unwrap_err();
    assert!(err.to_string().contains("$add needs numbers"), "{err}");
}

#[test]
fn the_size_counted_as_the_facets_finish_is_the_size_the_answer_encodes_to() {
    // `{a: [{p: "x…"}]}` encodes to the padding plus 29 bytes: the answer's header and
    // terminator (5), the element `a` (4), its array (5), the element `0` of it (3) and
    // the document `{p: …}` (padding + 13). So the padding that makes the answer exactly
    // the cap is accepted, and one byte more is refused.
    let cap = kimmy_core::limits::MAX_DOCUMENT_BYTES;
    let facet =
        |padding: usize| run(&[doc! {"$facet": {"a": []}}], vec![doc! {"p": "x".repeat(padding)}]);
    let out = facet(cap - 29).expect("exactly the cap is allowed");
    assert_eq!(bson::serialize_to_vec(&out[0]).unwrap().len(), cap);
    let err = facet(cap - 28).expect_err("one byte over");
    assert!(
        matches!(err, kimmy_core::Error::AnswerTooLarge { bytes, .. } if bytes == cap + 1),
        "{err:?}"
    );
}

#[test]
fn a_facet_inside_a_lookup_inside_a_facet_is_refused_too() {
    for inner in [doc! {"$facet": {"b": []}}, doc! {"$vectorSearch": {"query": "x"}}] {
        let message = refused(&[doc! {"$facet": {"a": [{"$lookup": {
        "from": "x", "pipeline": [inner.clone()], "as": "j"}}]}}]);
        assert!(
            message.contains("inside a $facet") || message.contains("$lookup sub-pipeline"),
            "{inner}: {message}"
        );
    }
    // A lookup in a facet with neither is fine, and a facet in a lookup outside one is too.
    assert!(
        aggregate::parse(&[doc! {"$facet": {"a": [{"$lookup": {
        "from": "x", "pipeline": [{"$limit": 1}], "as": "j"}}]}}])
        .is_ok()
    );
    assert!(
        aggregate::parse(&[doc! {"$lookup": {
        "from": "x", "pipeline": [{"$facet": {"b": []}}], "as": "j"}}])
        .is_ok()
    );
}
