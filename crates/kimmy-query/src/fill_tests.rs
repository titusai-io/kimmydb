//! `$fill` (ADR-264): what it fills, and what it refuses.

use bson::{Bson, Document, doc};

use crate::aggregate::{self, Limits, Stage};

fn run(spec: Document, input: Vec<Document>) -> kimmy_core::Result<Vec<Document>> {
    let stages = aggregate::parse(&[doc! {"$fill": spec}])?;
    aggregate::apply(&stages[0], input, &Limits::default())
}

fn refused(spec: Document) -> String {
    aggregate::parse(&[doc! {"$fill": spec}]).expect_err("must be refused").to_string()
}

fn col(out: &[Document], field: &str) -> Vec<Bson> {
    out.iter().map(|d| d.get(field).cloned().unwrap_or(Bson::Undefined)).collect()
}

fn d(text: &str) -> bson::DateTime {
    bson::DateTime::parse_rfc3339_str(text).unwrap()
}

// ---------------------------------------------------------------------------
// value
// ---------------------------------------------------------------------------

#[test]
fn a_value_fills_null_and_missing_and_nothing_else() {
    let out = run(
        doc! {"output": {"v": {"value": 0}}},
        vec![
            doc! {"_id": 1, "v": 5},
            doc! {"_id": 2, "v": Bson::Null},
            doc! {"_id": 3},
            doc! {"_id": 4, "v": 0},
            doc! {"_id": 5, "v": ""},
            doc! {"_id": 6, "v": false},
        ],
    )
    .unwrap();
    assert_eq!(
        col(&out, "v"),
        vec![
            Bson::Int32(5),
            Bson::Int32(0),
            Bson::Int32(0),
            Bson::Int32(0),
            Bson::String(String::new()),
            Bson::Boolean(false),
        ]
    );
}

#[test]
fn a_value_is_an_expression_over_the_document_and_needs_no_order() {
    let input = vec![
        doc! {"_id": 3, "a": 4, "b": Bson::Null},
        doc! {"_id": 1, "a": 1, "b": 7},
        doc! {"_id": 2, "a": 2},
    ];
    let out = run(doc! {"output": {"b": {"value": {"$multiply": ["$a", 10]}}}}, input).unwrap();
    // The order of the input is kept.
    assert_eq!(col(&out, "_id"), vec![Bson::Int32(3), Bson::Int32(1), Bson::Int32(2)]);
    assert_eq!(col(&out, "b"), vec![Bson::Int64(40), Bson::Int32(7), Bson::Int64(20)]);
}

#[test]
fn values_are_read_from_the_document_before_any_is_filled() {
    let out =
        run(doc! {"output": {"a": {"value": "$b"}, "b": {"value": 9}}}, vec![doc! {"_id": 1}])
            .unwrap();
    // `a` reads `b` as it was, not as it is filled: `b` was missing then, and a
    // field path that reaches nothing leaves the field out (the rule of `$addFields`),
    // so `a` stays absent while `b` is filled.
    assert_eq!(out, vec![doc! {"_id": 1, "b": 9}]);
}

#[test]
fn a_value_may_fill_a_dotted_field() {
    let out = run(
        doc! {"output": {"p.q": {"value": 1}}},
        vec![doc! {"p": {"q": Bson::Null, "r": 2}}, doc! {}, doc! {"p": {"q": 5}}],
    )
    .unwrap();
    assert_eq!(out, vec![doc! {"p": {"q": 1, "r": 2}}, doc! {"p": {"q": 1}}, doc! {"p": {"q": 5}}]);
}

#[test]
fn a_value_may_read_a_variable_of_a_lookup() {
    let stages = aggregate::parse(&[doc! {"$lookup": {
        "from": "x", "let": {"d": "$k"}, "pipeline": [{"$fill": {"output": {"v": {"value": "$$d"}}}}], "as": "y"
    }}])
    .unwrap();
    let Stage::Lookup { join: aggregate::Join::Pipeline { stages: inner, .. }, .. } = &stages[0]
    else {
        panic!()
    };
    let three = Bson::Int32(3);
    let out = aggregate::apply_with_vars(
        &inner[0],
        vec![doc! {"_id": 1}],
        &Limits::default(),
        &[("d", &three)],
    )
    .unwrap();
    assert_eq!(out, vec![doc! {"_id": 1, "v": 3}]);
}

// ---------------------------------------------------------------------------
// locf
// ---------------------------------------------------------------------------

fn days() -> Vec<Document> {
    vec![
        doc! {"_id": 1, "day": 1, "v": Bson::Null},
        doc! {"_id": 2, "day": 2, "v": 10},
        doc! {"_id": 3, "day": 3},
        doc! {"_id": 4, "day": 4, "v": Bson::Null},
        doc! {"_id": 5, "day": 5, "v": 40},
        doc! {"_id": 6, "day": 6},
    ]
}

#[test]
fn locf_carries_the_last_value_forward_and_leaves_a_leading_gap_null() {
    let out =
        run(doc! {"sortBy": {"day": 1}, "output": {"v": {"method": "locf"}}}, days()).unwrap();
    assert_eq!(
        col(&out, "v"),
        vec![
            Bson::Null,
            Bson::Int32(10),
            Bson::Int32(10),
            Bson::Int32(10),
            Bson::Int32(40),
            Bson::Int32(40)
        ]
    );
}

#[test]
fn locf_reads_the_sort_order_and_the_documents_leave_in_it() {
    let mut input = days();
    input.reverse();
    let out = run(doc! {"sortBy": {"day": 1}, "output": {"v": {"method": "locf"}}}, input.clone())
        .unwrap();
    assert_eq!(col(&out, "_id"), (1..=6).map(Bson::Int32).collect::<Vec<_>>());
    assert_eq!(col(&out, "v")[5], Bson::Int32(40));
    // Descending carries from the other end: 6 and 5 first.
    let out =
        run(doc! {"sortBy": {"day": -1}, "output": {"v": {"method": "locf"}}}, input).unwrap();
    assert_eq!(col(&out, "_id"), (1..=6).rev().map(Bson::Int32).collect::<Vec<_>>());
    assert_eq!(
        col(&out, "v"),
        vec![
            Bson::Null,
            Bson::Int32(40),
            Bson::Int32(40),
            Bson::Int32(40),
            Bson::Int32(10),
            Bson::Int32(10)
        ]
    );
}

#[test]
fn locf_restarts_in_each_partition() {
    let input = vec![
        doc! {"_id": 1, "g": "a", "day": 1, "v": 1},
        doc! {"_id": 2, "g": "b", "day": 1},
        doc! {"_id": 3, "g": "a", "day": 2},
        doc! {"_id": 4, "g": "b", "day": 2, "v": 4},
        doc! {"_id": 5, "g": "b", "day": 3},
    ];
    for partition in [doc! {"partitionBy": {"g": "$g"}}, doc! {"partitionByFields": ["g"]}] {
        let mut spec = doc! {"sortBy": {"day": 1}, "output": {"v": {"method": "locf"}}};
        spec.extend(partition);
        let out = run(spec, input.clone()).unwrap();
        // Partition a, then b.
        assert_eq!(
            col(&out, "_id"),
            vec![Bson::Int32(1), Bson::Int32(3), Bson::Int32(2), Bson::Int32(4), Bson::Int32(5)]
        );
        assert_eq!(
            col(&out, "v"),
            vec![Bson::Int32(1), Bson::Int32(1), Bson::Null, Bson::Int32(4), Bson::Int32(4)]
        );
    }
}

#[test]
fn the_partitions_leave_in_ascending_order_of_their_values_not_in_the_order_they_arrive() {
    let input = vec![
        doc! {"_id": 1, "g": "y", "day": 1, "v": 1},
        doc! {"_id": 2, "g": "x", "day": 1, "v": 2},
        doc! {"_id": 3, "g": "y", "day": 2},
        doc! {"_id": 4, "g": "x", "day": 2},
    ];
    for partition in [doc! {"partitionBy": {"g": "$g"}}, doc! {"partitionByFields": ["g"]}] {
        let mut spec = doc! {"sortBy": {"day": 1}, "output": {"v": {"method": "locf"}}};
        spec.extend(partition);
        let out = run(spec, input.clone()).unwrap();
        assert_eq!(
            col(&out, "_id"),
            vec![Bson::Int32(2), Bson::Int32(4), Bson::Int32(1), Bson::Int32(3)]
        );
        assert_eq!(
            col(&out, "v"),
            vec![Bson::Int32(2), Bson::Int32(2), Bson::Int32(1), Bson::Int32(1)]
        );
    }
}

#[test]
fn locf_without_a_sort_runs_in_the_order_the_documents_arrive() {
    let out = run(
        doc! {"output": {"v": {"method": "locf"}}},
        vec![
            doc! {"_id": 1, "v": 1},
            doc! {"_id": 2},
            doc! {"_id": 3, "v": 5},
            doc! {"_id": 4, "v": Bson::Null},
        ],
    )
    .unwrap();
    assert_eq!(
        col(&out, "_id"),
        vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3), Bson::Int32(4)]
    );
    assert_eq!(
        col(&out, "v"),
        vec![Bson::Int32(1), Bson::Int32(1), Bson::Int32(5), Bson::Int32(5)]
    );
    // An empty `sortBy` is no sort.
    let out =
        run(doc! {"sortBy": {}, "output": {"v": {"method": "locf"}}}, vec![doc! {"v": 1}, doc! {}])
            .unwrap();
    assert_eq!(col(&out, "v"), vec![Bson::Int32(1), Bson::Int32(1)]);
}

// ---------------------------------------------------------------------------
// linear
// ---------------------------------------------------------------------------

#[test]
fn linear_interpolates_between_neighbours_on_the_sort_value() {
    let out = run(
        doc! {"sortBy": {"day": 1}, "output": {"v": {"method": "linear"}}},
        vec![
            doc! {"day": 1},
            doc! {"day": 2, "v": 10},
            doc! {"day": 3},
            doc! {"day": 5},
            doc! {"day": 6, "v": 40},
            doc! {"day": 7, "v": Bson::Null},
        ],
    )
    .unwrap();
    // 10 at day 2 and 40 at day 6 is 7.5 a day: day 3 is 17.5, day 5 is 32.5. The ends stay null.
    assert_eq!(
        col(&out, "v"),
        vec![
            Bson::Null,
            Bson::Int32(10),
            Bson::Double(17.5),
            Bson::Double(32.5),
            Bson::Int32(40),
            Bson::Null
        ]
    );
}

#[test]
fn linear_works_on_dates_and_descending_sorts() {
    let out = run(
        doc! {"sortBy": {"t": 1}, "output": {"v": {"method": "linear"}}},
        vec![
            doc! {"t": d("2023-01-01T00:00:00Z"), "v": 0},
            doc! {"t": d("2023-01-01T01:00:00Z")},
            doc! {"t": d("2023-01-01T04:00:00Z"), "v": 40},
        ],
    )
    .unwrap();
    assert_eq!(col(&out, "v"), vec![Bson::Int32(0), Bson::Double(10.0), Bson::Int32(40)]);
    let out = run(
        doc! {"sortBy": {"x": -1}, "output": {"v": {"method": "linear"}}},
        vec![doc! {"x": 0, "v": 0}, doc! {"x": 1}, doc! {"x": 4, "v": 40}],
    )
    .unwrap();
    // Sorted 4, 1, 0: the middle is a quarter of the way from 40 to 0... 1 lies 3/4 of the way
    // from x = 4 to x = 0, so 40 + (0 - 40) * 3/4 = 10.
    assert_eq!(col(&out, "v"), vec![Bson::Int32(40), Bson::Double(10.0), Bson::Int32(0)]);
}

#[test]
fn linear_refuses_a_value_that_is_not_a_number_and_a_sort_that_is_not_one_field() {
    let e = run(
        doc! {"sortBy": {"x": 1}, "output": {"v": {"method": "linear"}}},
        vec![doc! {"x": 1, "v": "a"}, doc! {"x": 2}],
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("needs numbers"), "{e}");
    let e = run(
        doc! {"sortBy": {"x": 1}, "output": {"v": {"method": "linear"}}},
        vec![doc! {"x": "a", "v": 1}],
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("number or a date"), "{e}");
    let e = refused(doc! {"sortBy": {"x": 1, "y": 1}, "output": {"v": {"method": "linear"}}});
    assert!(e.contains("exactly one field"), "{e}");
}

// ---------------------------------------------------------------------------
// Mixing, and refusals
// ---------------------------------------------------------------------------

#[test]
fn methods_and_values_mix_across_fields() {
    let out = run(
        doc! {"sortBy": {"day": 1}, "output": {"v": {"method": "locf"}, "w": {"value": -1}}},
        vec![
            doc! {"day": 2, "v": Bson::Null, "w": Bson::Null},
            doc! {"day": 1, "v": 3, "w": 8},
            doc! {"day": 3, "w": Bson::Null},
        ],
    )
    .unwrap();
    // Sorted by day (the method's stage sorts); v carries 3; w is -1 where it was null.
    assert_eq!(col(&out, "day"), vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]);
    assert_eq!(col(&out, "v"), vec![Bson::Int32(3), Bson::Int32(3), Bson::Int32(3)]);
    assert_eq!(col(&out, "w"), vec![Bson::Int32(8), Bson::Int32(-1), Bson::Int32(-1)]);
}

#[test]
fn a_sort_by_and_a_partition_with_only_values_change_nothing_about_the_order() {
    let input = vec![doc! {"_id": 2, "g": 1}, doc! {"_id": 1, "g": 2}];
    let out = run(
        doc! {"partitionBy": {"g": "$g"}, "sortBy": {"_id": 1}, "output": {"v": {"value": 0}}},
        input,
    )
    .unwrap();
    assert_eq!(col(&out, "_id"), vec![Bson::Int32(2), Bson::Int32(1)]);
}

#[test]
fn the_stage_refuses_what_it_does_not_know() {
    assert!(
        refused(doc! {"output": {"v": {"value": 0}}, "extra": 1}).contains("does not take `extra`")
    );
    assert!(refused(doc! {}).contains("needs `output`"));
    assert!(refused(doc! {"output": {}}).contains("at least one field"));
    assert!(refused(doc! {"output": 1}).contains("`output` must be a document"));
    assert!(refused(doc! {"output": {"v": 1}}).contains("{value: ..} or {method: ..}"));
    assert!(refused(doc! {"output": {"v": {}}}).contains("needs `value` or `method`"));
    assert!(
        refused(doc! {"output": {"v": {"value": 0, "method": "locf"}}})
            .contains("one of `value` and `method`")
    );
    assert!(
        refused(doc! {"output": {"v": {"value": 0, "other": 1}}}).contains("does not take `other`")
    );
    assert!(
        refused(doc! {"sortBy": {"d": 1}, "output": {"v": {"method": "cubic"}}})
            .contains("\"locf\" or \"linear\"")
    );
    assert!(
        refused(doc! {"sortBy": {"d": 1}, "output": {"v": {"method": 1}}})
            .contains("\"locf\" or \"linear\"")
    );
    let e = refused(doc! {"output": {"v": {"method": "linear"}}});
    assert!(e.contains("exactly one field"), "{e}");
    // The reference wants an object for `partitionBy`, not a field name.
    let e = refused(doc! {"partitionBy": "$g", "output": {"v": {"value": 0}}});
    assert!(e.contains("object expression"), "{e}");
    assert!(
        refused(
            doc! {"partitionBy": {"g": "$g"}, "partitionByFields": ["g"], "output": {"v": {"value": 0}}}
        )
        .contains("one of `partitionBy` and `partitionByFields`")
    );
    assert!(
        refused(doc! {"partitionByFields": "g", "output": {"v": {"value": 0}}})
            .contains("array of field paths")
    );
    assert!(
        refused(doc! {"partitionByFields": ["$g"], "output": {"v": {"value": 0}}})
            .contains("without a leading `$`")
    );
    assert!(
        refused(doc! {"partitionByFields": [1], "output": {"v": {"value": 0}}})
            .contains("field paths")
    );
    assert!(
        refused(doc! {"sortBy": {"d": 3}, "output": {"v": {"value": 0}}})
            .contains("must be 1 or -1")
    );
    assert!(
        refused(doc! {"output": {"$v": {"value": 0}}}).contains("cannot be empty, start with `$`")
    );
    assert!(refused(doc! {"output": {"v": {"value": {"$nope": 1}}}}).contains("$nope"));
}

#[test]
fn the_stage_runs_inside_a_facet() {
    let stages = aggregate::parse(&[doc! {"$facet": {"f": [
        {"$fill": {"sortBy": {"day": 1}, "output": {"v": {"method": "locf"}}}},
        {"$project": {"_id": 0, "v": 1}},
    ]}}])
    .unwrap();
    let out = aggregate::apply(&stages[0], days(), &Limits::default()).unwrap();
    assert_eq!(out[0].get_array("f").unwrap().len(), 6);
    let fill = aggregate::parse(&[doc! {"$fill": {"output": {"v": {"value": 0}}}}]).unwrap();
    assert_eq!(fill[0].name(), "$fill");
    assert!(!fill[0].needs_storage());
}
