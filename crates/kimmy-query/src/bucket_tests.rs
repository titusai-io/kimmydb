//! `$bucket` and `$bucketAuto` (ADR-254): what they answer, and what they refuse.

use bson::{Bson, Document, doc};

use crate::aggregate::{self, Limits};

fn run(stage: Document, input: Vec<Document>) -> kimmy_core::Result<Vec<Document>> {
    let stages = aggregate::parse(&[stage])?;
    aggregate::apply(&stages[0], input, &Limits::default())
}

fn refused(stage: Document) -> String {
    aggregate::parse(&[stage]).expect_err("must be refused").to_string()
}

fn prices(values: &[i32]) -> Vec<Document> {
    values.iter().enumerate().map(|(i, p)| doc! {"_id": i as i32, "price": *p}).collect()
}

// ---------------------------------------------------------------------------
// $bucket
// ---------------------------------------------------------------------------

#[test]
fn bucket_counts_each_range_and_names_it_by_its_lower_boundary() {
    let out = run(
        doc! {"$bucket": {"groupBy": "$price", "boundaries": [0, 10, 20, 100]}},
        prices(&[0, 5, 9, 10, 19, 20, 99]),
    )
    .unwrap();
    assert_eq!(
        out,
        vec![
            doc! {"_id": 0, "count": 3i64},
            doc! {"_id": 10, "count": 2i64},
            doc! {"_id": 20, "count": 2i64},
        ]
    );
}

#[test]
fn a_range_includes_its_lower_boundary_and_excludes_its_upper() {
    // The documents each range took, by `_id`: 9 and 10 are on either side of the boundary
    // 10, and 19 and 20 of 20, so a lower bound that excluded or an upper bound that
    // included would move one of them.
    let out = run(
        doc! {"$bucket": {"groupBy": "$price", "boundaries": [0, 10, 20], "default": "other",
        "output": {"ids": {"$push": "$_id"}}}},
        prices(&[0, 9, 10, 19, 20, 25, -1]),
    )
    .unwrap();
    assert_eq!(
        out,
        vec![
            doc! {"_id": 0, "ids": [0, 1]},
            doc! {"_id": 10, "ids": [2, 3]},
            doc! {"_id": "other", "ids": [4, 5, 6]},
        ]
    );
}

#[test]
fn only_buckets_that_received_a_document_are_answered_in_boundary_order() {
    let out = run(
        doc! {"$bucket": {"groupBy": "$price", "boundaries": [0, 10, 20, 30, 40]}},
        prices(&[35, 5]),
    )
    .unwrap();
    assert_eq!(out, vec![doc! {"_id": 0, "count": 1i64}, doc! {"_id": 30, "count": 1i64}]);
    assert_eq!(
        run(doc! {"$bucket": {"groupBy": "$price", "boundaries": [0, 10]}}, vec![]).unwrap(),
        Vec::<Document>::new()
    );
}

#[test]
fn the_default_bucket_takes_every_value_no_range_holds_and_comes_last() {
    let mut input = prices(&[-5, 5, 50]);
    input.push(doc! {"_id": 90});
    input.push(doc! {"_id": 91, "price": "text"});
    input.push(doc! {"_id": 92, "price": Bson::Null});
    let out = run(
        doc! {"$bucket": {"groupBy": "$price", "boundaries": [0, 10], "default": "none"}},
        input,
    )
    .unwrap();
    assert_eq!(out, vec![doc! {"_id": 0, "count": 1i64}, doc! {"_id": "none", "count": 5i64}]);
}

#[test]
fn a_value_in_no_bucket_is_refused_when_there_is_no_default() {
    for (value, shown) in [
        (Bson::Int32(-5), "the value -5"),
        (Bson::Int32(10), "the value 10"),
        (Bson::String("x".into()), "the value \"x\""),
        (Bson::Null, "null or a missing value"),
    ] {
        let err = run(
            doc! {"$bucket": {"groupBy": "$price", "boundaries": [0, 10]}},
            vec![doc! {"price": value}],
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains(&format!("$bucket: {shown} falls in no bucket and there is no `default`")),
            "{err}"
        );
    }
    let err = run(doc! {"$bucket": {"groupBy": "$missing", "boundaries": [0, 10]}}, prices(&[1]))
        .unwrap_err()
        .to_string();
    assert!(err.contains("null or a missing value"), "{err}");
}

#[test]
fn output_replaces_the_count_and_takes_any_accumulator() {
    let out = run(
        doc! {"$bucket": {"groupBy": "$price", "boundaries": [0, 10, 100], "output": {
            "total": {"$sum": "$price"}, "ids": {"$push": "$_id"}, "top": {"$max": "$price"}
        }}},
        prices(&[1, 2, 50]),
    )
    .unwrap();
    assert_eq!(
        out,
        vec![
            doc! {"_id": 0, "total": 3i64, "ids": [0, 1], "top": 2},
            doc! {"_id": 10, "total": 50i64, "ids": [2], "top": 50},
        ]
    );
}

#[test]
fn boundaries_of_any_one_kind_work_and_numbers_of_every_type_are_one_kind() {
    // Mixed numeric types are one series.
    let out = run(
        doc! {"$bucket": {"groupBy": "$price", "boundaries": [0, 10i64, 20.5, 100]}},
        prices(&[5, 15, 30]),
    )
    .unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(out[1].get("_id"), Some(&Bson::Int64(10)));
    // Dates and strings.
    let a = bson::DateTime::from_millis(1_000);
    let b = bson::DateTime::from_millis(2_000);
    let out = run(
        doc! {"$bucket": {"groupBy": "$at", "boundaries": [a, b]}},
        vec![doc! {"at": bson::DateTime::from_millis(1_500)}],
    )
    .unwrap();
    assert_eq!(out, vec![doc! {"_id": a, "count": 1i64}]);
    let out = run(
        doc! {"$bucket": {"groupBy": "$name", "boundaries": ["a", "m", "z"]}},
        vec![doc! {"name": "bob"}, doc! {"name": "tom"}],
    )
    .unwrap();
    assert_eq!(out, vec![doc! {"_id": "a", "count": 1i64}, doc! {"_id": "m", "count": 1i64}]);
    // A value of another kind than the boundaries' is in no range.
    let err = run(doc! {"$bucket": {"groupBy": "$price", "boundaries": ["a", "z"]}}, prices(&[5]))
        .unwrap_err()
        .to_string();
    assert!(err.contains("falls in no bucket"), "{err}");
}

#[test]
fn a_decimal_value_is_compared_by_its_value() {
    let dec = |t: &str| Bson::Decimal128(kimmy_core::decimal::parse(t).unwrap());
    let out = run(
        doc! {"$bucket": {"groupBy": "$price", "boundaries": [0, 10], "default": "high"}},
        vec![doc! {"price": dec("9.99")}, doc! {"price": dec("0.00")}, doc! {"price": dec("10.0")}],
    )
    .unwrap();
    assert_eq!(out, vec![doc! {"_id": 0, "count": 2i64}, doc! {"_id": "high", "count": 1i64}]);
}

#[test]
fn a_malformed_bucket_is_refused_at_parse() {
    let bad = |spec: Document| refused(doc! {"$bucket": spec});
    assert!(bad(doc! {"boundaries": [0, 1]}).contains("needs a `groupBy`"));
    assert!(bad(doc! {"groupBy": "$p"}).contains("needs `boundaries`"));
    assert!(bad(doc! {"groupBy": "$p", "boundaries": 5}).contains("needs `boundaries`"));
    assert!(bad(doc! {"groupBy": "$p", "boundaries": [5]}).contains("at least two values"));
    assert!(bad(doc! {"groupBy": "$p", "boundaries": [0, 10, 5]}).contains("ascending order"));
    assert!(bad(doc! {"groupBy": "$p", "boundaries": [0, 10, 10]}).contains("without repeats"));
    assert!(bad(doc! {"groupBy": "$p", "boundaries": [0, "z"]}).contains("one type"));
    assert!(
        bad(doc! {"groupBy": "$p", "boundaries": [0, Bson::Null]})
            .contains("found null at position 1")
    );
    assert!(bad(doc! {"groupBy": "$p", "boundaries": [0, [1]]}).contains("found an array"));
    assert!(bad(doc! {"groupBy": "$p", "boundaries": [0, f64::NAN]}).contains("a NaN"));
    assert!(bad(doc! {"groupBy": "$p", "boundaries": ["a", "$b"]}).contains("begins with $"));
    assert!(
        bad(doc! {"groupBy": "$p", "boundaries": [0, 10], "default": 5}).contains("not inside")
    );
    assert!(
        bad(doc! {"groupBy": "$p", "boundaries": [0, 10], "default": 0}).contains("not inside")
    );
    assert!(
        bad(doc! {"groupBy": "$p", "boundaries": [0, 10], "output": {}})
            .contains("with at least one")
    );
    assert!(
        bad(doc! {"groupBy": "$p", "boundaries": [0, 10], "output": 1})
            .contains("with at least one")
    );
    assert!(
        bad(doc! {"groupBy": "$p", "boundaries": [0, 10], "output": {"n": 1}})
            .contains("accumulator document")
    );
    assert!(
        bad(doc! {"groupBy": "$p", "boundaries": [0, 10], "output": {"n": {"$nope": 1}}})
            .contains("not an accumulator")
    );
    assert!(bad(doc! {"groupBy": "$p", "boundaries": [0, 10], "buckets": 3}).contains("buckets"));
    // The default may sit at or above the last boundary, or below the first.
    for default in [Bson::Int32(10), Bson::Int32(-1), Bson::String("x".into()), Bson::Null] {
        assert!(
            aggregate::parse(&[
                doc! {"$bucket": {"groupBy": "$p", "boundaries": [0, 10], "default": default}}
            ])
            .is_ok()
        );
    }
    assert!(refused(doc! {"$bucket": 1}).contains("takes a document"));
}

// ---------------------------------------------------------------------------
// $bucketAuto
// ---------------------------------------------------------------------------

fn auto(buckets: i32, values: &[i32]) -> Vec<Document> {
    run(doc! {"$bucketAuto": {"groupBy": "$price", "buckets": buckets}}, prices(values)).unwrap()
}

fn range(out: &Document) -> (i32, i32, i64) {
    let id = out.get_document("_id").unwrap();
    (id.get_i32("min").unwrap(), id.get_i32("max").unwrap(), out.get_i64("count").unwrap())
}

#[test]
fn bucket_auto_cuts_a_sorted_run_into_even_buckets() {
    let out = auto(3, &[9, 1, 5, 3, 7, 2, 8, 4, 6]);
    assert_eq!(out.iter().map(range).collect::<Vec<_>>(), vec![(1, 4, 3), (4, 7, 3), (7, 9, 3)]);
    // The last bucket's max is its own largest value, the others' the next bucket's min.
    let out = auto(2, &[1, 2, 3, 4, 5]);
    assert_eq!(out.iter().map(range).collect::<Vec<_>>(), vec![(1, 4, 3), (4, 5, 2)]);
}

#[test]
fn bucket_auto_never_splits_a_value_and_makes_fewer_buckets_when_values_repeat() {
    let out = auto(3, &[1, 1, 1, 1, 2, 3]);
    // Two documents would be the even size; the four ones stay together.
    assert_eq!(out.iter().map(range).collect::<Vec<_>>(), vec![(1, 2, 4), (2, 3, 1), (3, 3, 1)]);
    let out = auto(4, &[7, 7, 7]);
    assert_eq!(out.iter().map(range).collect::<Vec<_>>(), vec![(7, 7, 3)]);
    // More buckets than documents: one each.
    let out = auto(10, &[3, 1, 2]);
    assert_eq!(out.iter().map(range).collect::<Vec<_>>(), vec![(1, 2, 1), (2, 3, 1), (3, 3, 1)]);
    // One bucket is the whole run, and an empty input answers nothing.
    assert_eq!(auto(1, &[5, 1, 3]).iter().map(range).collect::<Vec<_>>(), vec![(1, 5, 3)]);
    assert!(auto(3, &[]).is_empty());
}

#[test]
fn bucket_auto_takes_output_and_orders_a_bucket_by_the_sorted_value() {
    let out = run(
        doc! {"$bucketAuto": {"groupBy": "$price", "buckets": 2, "output": {
            "ids": {"$push": "$_id"}, "avg": {"$avg": "$price"}
        }}},
        prices(&[30, 10, 40, 20]),
    )
    .unwrap();
    assert_eq!(
        out,
        vec![
            doc! {"_id": {"min": 10, "max": 30}, "ids": [1, 3], "avg": 15.0},
            doc! {"_id": {"min": 30, "max": 40}, "ids": [0, 2], "avg": 35.0},
        ]
    );
}

#[test]
fn bucket_auto_orders_other_kinds_by_the_order_sort_uses() {
    let out = run(
        doc! {"$bucketAuto": {"groupBy": "$name", "buckets": 2}},
        vec![
            doc! {"name": "tom"},
            doc! {"name": "amy"},
            doc! {"name": "bob"},
            doc! {"name": "zed"},
        ],
    )
    .unwrap();
    assert_eq!(out[0].get_document("_id").unwrap(), &doc! {"min": "amy", "max": "tom"});
    assert_eq!(out[1].get_document("_id").unwrap(), &doc! {"min": "tom", "max": "zed"});
}

#[test]
fn bucket_auto_refuses_granularity_and_a_malformed_spec() {
    let bad = |spec: Document| refused(doc! {"$bucketAuto": spec});
    let message = bad(doc! {"groupBy": "$p", "buckets": 3, "granularity": "R5"});
    assert!(message.contains("`granularity` is not supported"), "{message}");
    assert!(bad(doc! {"buckets": 3}).contains("needs a `groupBy`"));
    assert!(bad(doc! {"groupBy": "$p"}).contains("needs `buckets`"));
    for buckets in
        [Bson::Int32(0), Bson::Int32(-2), Bson::Double(1.5), Bson::String("3".into()), Bson::Null]
    {
        let message = bad(doc! {"groupBy": "$p", "buckets": buckets.clone()});
        assert!(
            message.contains("`buckets` is a whole number of at least 1"),
            "{buckets:?}: {message}"
        );
    }
    assert!(bad(doc! {"groupBy": "$p", "buckets": 2, "boundaries": [1, 2]}).contains("boundaries"));
    assert!(bad(doc! {"groupBy": "$p", "buckets": 2, "output": {}}).contains("with at least one"));
    // A whole double is a whole number.
    assert!(aggregate::parse(&[doc! {"$bucketAuto": {"groupBy": "$p", "buckets": 3.0}}]).is_ok());
}

#[test]
fn the_bucket_count_is_held_to_the_pipeline_ceiling() {
    let limits = Limits { max_documents: 3 };
    let stages =
        aggregate::parse(&[doc! {"$bucketAuto": {"groupBy": "$price", "buckets": 10}}]).unwrap();
    let err =
        aggregate::apply(&stages[0], prices(&[1, 2, 3, 4, 5]), &limits).unwrap_err().to_string();
    // Counted as the buckets are made: the fourth is the first over.
    assert!(
        err.contains("$bucketAuto produced 4 documents, over the pipeline limit of 3"),
        "{err}"
    );
    // Asking for many buckets of few documents is not what is counted.
    let wide = aggregate::apply(&stages[0], prices(&[1, 1, 1, 1, 1]), &limits).unwrap();
    assert_eq!(wide.len(), 1);
    let stages = aggregate::parse(&[
        doc! {"$bucket": {"groupBy": "$price", "boundaries": [0, 1, 2, 3, 4, 5, 6], "default": 9}},
    ])
    .unwrap();
    let err =
        aggregate::apply(&stages[0], prices(&[0, 1, 2, 3, 4]), &limits).unwrap_err().to_string();
    assert!(err.contains("$bucket produced 4 documents"), "{err}");
}

#[test]
fn a_bucket_stage_inside_a_facet_runs() {
    let out = run(
        doc! {"$facet": {
            "hist": [{"$bucket": {"groupBy": "$price", "boundaries": [0, 10, 100]}}],
            "auto": [{"$bucketAuto": {"groupBy": "$price", "buckets": 2}}],
        }},
        prices(&[1, 2, 50, 60]),
    )
    .unwrap();
    assert_eq!(out[0].get_array("hist").unwrap().len(), 2);
    assert_eq!(out[0].get_array("auto").unwrap().len(), 2);
}

#[test]
fn an_output_field_named_id_is_refused_in_both_stages() {
    for stage in ["$bucket", "$bucketAuto"] {
        let mut spec = doc! {"groupBy": "$p", "output": {"_id": {"$sum": 1}}};
        if stage == "$bucket" {
            spec.insert("boundaries", vec![0, 10]);
        } else {
            spec.insert("buckets", 2);
        }
        let message = refused(doc! {stage: spec});
        assert!(
            message.contains("cannot name `_id`") && message.contains(stage),
            "{stage}: {message}"
        );
    }
}

#[test]
fn a_default_beginning_with_a_dollar_is_refused_as_a_boundary_is() {
    let message =
        refused(doc! {"$bucket": {"groupBy": "$p", "boundaries": [0, 10], "default": "$other"}});
    assert!(message.contains("`default` is a constant"), "{message}");
    assert!(
        aggregate::parse(&[
            doc! {"$bucket": {"groupBy": "$p", "boundaries": [0, 10], "default": "other"}}
        ])
        .is_ok()
    );
}

#[test]
fn the_buckets_error_names_a_number_by_its_value_and_anything_else_by_its_type() {
    let message = |b: Bson| refused(doc! {"$bucketAuto": {"groupBy": "$p", "buckets": b}});
    assert!(message(Bson::Int32(0)).contains("found 0"));
    assert!(message(Bson::Int32(-2)).contains("found -2"));
    assert!(message(Bson::String("3".into())).contains("found a string"));
    assert!(message(Bson::Double(1.5)).contains("found a double"));
}
