//! `$densify` (ADR-264): what it generates, and what it refuses.

use bson::{Bson, Document, doc};
use kimmy_core::Error;

use crate::aggregate::{self, Limits, Stage};
use crate::densify;

fn run(spec: Document, input: Vec<Document>) -> kimmy_core::Result<Vec<Document>> {
    run_with(spec, input, &Limits::default())
}

fn run_with(
    spec: Document,
    input: Vec<Document>,
    limits: &Limits,
) -> kimmy_core::Result<Vec<Document>> {
    let stages = aggregate::parse(&[doc! {"$densify": spec}])?;
    aggregate::apply(&stages[0], input, limits)
}

fn refused(spec: Document) -> String {
    aggregate::parse(&[doc! {"$densify": spec}]).expect_err("must be refused").to_string()
}

fn refused_run(spec: Document, input: Vec<Document>) -> String {
    run(spec, input).expect_err("must be refused at run time").to_string()
}

/// Limits whose byte budget is `cap`.
fn capped(cap: usize) -> Limits {
    Limits { buffer: crate::budget::BufferBudget::new(cap), ..Default::default() }
}

fn d(text: &str) -> bson::DateTime {
    bson::DateTime::parse_rfc3339_str(text).unwrap()
}

/// Documents holding only `a`.
fn a(values: &[i32]) -> Vec<Document> {
    values.iter().map(|v| doc! {"a": *v}).collect()
}

fn col(out: &[Document], field: &str) -> Vec<Bson> {
    out.iter().map(|d| d.get(field).cloned().unwrap_or(Bson::Null)).collect()
}

fn ints(values: &[i32]) -> Vec<Bson> {
    values.iter().map(|v| Bson::Int32(*v)).collect()
}

fn full(step: i32) -> Document {
    doc! {"field": "a", "range": {"step": step, "bounds": "full"}}
}

// ---------------------------------------------------------------------------
// Numbers
// ---------------------------------------------------------------------------

#[test]
fn gaps_are_filled_at_every_step_with_documents_holding_only_the_field() {
    let out = run(
        full(1),
        vec![doc! {"_id": 1, "a": 1, "x": "keep"}, doc! {"_id": 2, "a": 4, "x": "keep"}],
    )
    .unwrap();
    assert_eq!(
        out,
        vec![
            doc! {"_id": 1, "a": 1, "x": "keep"},
            doc! {"a": 2},
            doc! {"a": 3},
            doc! {"_id": 2, "a": 4, "x": "keep"},
        ]
    );
}

#[test]
fn the_grid_starts_at_the_range_start_and_a_document_off_it_does_not_move_it() {
    // Full bounds start at the lowest value: 1, 3, 5, 7 are the grid of step 2, and the
    // documents at 4 and 8 are not on it.
    assert_eq!(col(&run(full(2), a(&[1, 4, 8])).unwrap(), "a"), ints(&[1, 3, 4, 5, 7, 8]));
    // [1, 4] with step 2: 3 is on the grid, 4 is a document.
    assert_eq!(col(&run(full(2), a(&[1, 4])).unwrap(), "a"), ints(&[1, 3, 4]));
    // Explicit bounds start at `lo`: 0, 3, 6, 9.
    let spec = |lo: i32, hi: i32, step: i32| {
        doc! {"field": "a", "range": {"step": step, "bounds": [lo, hi]}}
    };
    assert_eq!(
        col(&run(spec(0, 10, 3), a(&[1, 4, 8])).unwrap(), "a"),
        ints(&[0, 1, 3, 4, 6, 8, 9])
    );
    // Nothing is made where a document already is, or where the grid has no point.
    assert_eq!(col(&run(full(3), a(&[1, 3])).unwrap(), "a"), ints(&[1, 3]));
    assert_eq!(col(&run(full(2), a(&[1, 3, 5])).unwrap(), "a"), ints(&[1, 3, 5]));
    assert_eq!(col(&run(full(1), a(&[7])).unwrap(), "a"), ints(&[7]));
}

#[test]
fn the_input_need_not_be_sorted_and_equal_values_keep_their_order() {
    let out = run(
        full(1),
        vec![
            doc! {"_id": 1, "a": 5},
            doc! {"_id": 2, "a": 1},
            doc! {"_id": 3, "a": 3},
            doc! {"_id": 4, "a": 3},
        ],
    )
    .unwrap();
    assert_eq!(col(&out, "a"), ints(&[1, 2, 3, 3, 4, 5]));
    assert_eq!(col(&out, "_id")[2..4], [Bson::Int32(3), Bson::Int32(4)]);
}

#[test]
fn explicit_bounds_run_from_the_lower_included_to_the_upper_excluded() {
    let spec = |lo: i32, hi: i32, step: i32| {
        doc! {"field": "a", "range": {"step": step, "bounds": [lo, hi]}}
    };
    // The grid is 0, 2, 4 below 5; -1 is below the range and 7 above it.
    assert_eq!(col(&run(spec(0, 5, 2), a(&[7, 3, -1])).unwrap(), "a"), ints(&[-1, 0, 2, 3, 4, 7]));
    // A document on the lower bound.
    assert_eq!(col(&run(spec(0, 3, 1), a(&[0])).unwrap(), "a"), ints(&[0, 1, 2]));
    // A range with no document in it.
    assert_eq!(col(&run(spec(10, 13, 1), a(&[0, 20])).unwrap(), "a"), ints(&[0, 10, 11, 12, 20]));
    // A document on the upper bound is outside, and nothing is made at it.
    assert_eq!(col(&run(spec(0, 3, 1), a(&[3])).unwrap(), "a"), ints(&[0, 1, 2, 3]));
    // Bounds inside the documents.
    assert_eq!(col(&run(spec(2, 6, 1), a(&[1, 4, 8])).unwrap(), "a"), ints(&[1, 2, 3, 4, 5, 8]));
    // `[lo, lo]` is the point `lo`.
    assert_eq!(col(&run(spec(5, 5, 1), a(&[1, 4, 8])).unwrap(), "a"), ints(&[1, 4, 5, 8]));
    assert_eq!(col(&run(spec(4, 4, 1), a(&[4])).unwrap(), "a"), ints(&[4]));
}

#[test]
fn a_document_without_the_field_or_with_a_null_one_passes_through_ahead_of_the_rest() {
    let out = run(
        full(1),
        vec![
            doc! {"_id": 1, "a": 1},
            doc! {"_id": 2},
            doc! {"_id": 3, "a": Bson::Null},
            doc! {"_id": 4, "a": 3},
        ],
    )
    .unwrap();
    assert_eq!(
        out,
        vec![
            doc! {"_id": 2},
            doc! {"_id": 3, "a": Bson::Null},
            doc! {"_id": 1, "a": 1},
            doc! {"a": 2},
            doc! {"_id": 4, "a": 3},
        ]
    );
    // They are no part of the series for any bounds, and a series of none is only them.
    let out =
        run(doc! {"field": "a", "range": {"step": 1, "bounds": [0, 3]}}, vec![doc! {"_id": 1}])
            .unwrap();
    assert_eq!(out, vec![doc! {"_id": 1}]);
}

#[test]
fn no_input_makes_nothing() {
    assert!(run(full(1), vec![]).unwrap().is_empty());
    assert!(
        run(doc! {"field": "a", "range": {"step": 1, "bounds": [0, 5]}}, vec![])
            .unwrap()
            .is_empty()
    );
}

#[test]
fn generated_values_take_the_type_of_an_add() {
    // Int32 and a whole step stay Int32 until they no longer fit, then Int64.
    let out = run(full(1), vec![doc! {"a": 2147483646}, doc! {"a": 2147483649_i64}]).unwrap();
    assert_eq!(
        col(&out, "a"),
        vec![
            Bson::Int32(2147483646),
            Bson::Int32(2147483647),
            Bson::Int64(2147483648),
            Bson::Int64(2147483649)
        ]
    );
    // A fractional step makes doubles, whatever the field is.
    let out =
        run(doc! {"field": "a", "range": {"step": 0.5, "bounds": "full"}}, a(&[1, 2])).unwrap();
    assert_eq!(col(&out, "a"), vec![Bson::Int32(1), Bson::Double(1.5), Bson::Int32(2)]);
    let out =
        run(doc! {"field": "a", "range": {"step": 2.0, "bounds": "full"}}, a(&[1, 4, 8])).unwrap();
    assert_eq!(
        col(&out, "a"),
        vec![
            Bson::Int32(1),
            Bson::Double(3.0),
            Bson::Int32(4),
            Bson::Double(5.0),
            Bson::Double(7.0),
            Bson::Int32(8)
        ]
    );
    // A double field with a whole step.
    let out = run(full(1), vec![doc! {"a": 1.5}, doc! {"a": 3.5}]).unwrap();
    assert_eq!(col(&out, "a"), vec![Bson::Double(1.5), Bson::Double(2.5), Bson::Double(3.5)]);
    // A long field with an int step.
    let out = run(full(1), vec![doc! {"a": 1_i64}, doc! {"a": 3_i64}]).unwrap();
    assert_eq!(col(&out, "a"), vec![Bson::Int64(1), Bson::Int64(2), Bson::Int64(3)]);
    // A long step makes longs, whatever the field is.
    let out = run(doc! {"field": "a", "range": {"step": 3_i64, "bounds": "full"}}, a(&[1, 4, 8]))
        .unwrap();
    assert_eq!(
        col(&out, "a"),
        vec![Bson::Int32(1), Bson::Int32(4), Bson::Int64(7), Bson::Int32(8)]
    );
    // The range start fixes the type: a long start and an int step make longs.
    let out = run(full(1), vec![doc! {"a": 1_i64}, doc! {"a": 3.5}]).unwrap();
    assert_eq!(
        col(&out, "a"),
        vec![Bson::Int64(1), Bson::Int64(2), Bson::Int64(3), Bson::Double(3.5)]
    );
}

#[test]
fn explicit_bounds_and_the_step_are_of_one_numeric_type_and_make_that_type() {
    let long = |lo: i64, hi: i64, step: i64| {
        doc! {"field": "a", "range": {"step": step, "bounds": [lo, hi]}}
    };
    let out = run(long(0, 9, 3), a(&[1, 4, 8])).unwrap();
    assert_eq!(
        col(&out, "a"),
        vec![
            Bson::Int64(0),
            Bson::Int32(1),
            Bson::Int64(3),
            Bson::Int32(4),
            Bson::Int64(6),
            Bson::Int32(8)
        ]
    );
    for (bounds, step) in [
        (Bson::Array(vec![0.5.into(), 3.0.into()]), Bson::Int32(1)),
        (Bson::Array(vec![0.into(), 3.into()]), Bson::Int64(1)),
        (Bson::Array(vec![0.into(), Bson::Int64(3)]), Bson::Int32(1)),
        (Bson::Array(vec![0.into(), 3.into()]), Bson::Double(1.0)),
    ] {
        let e =
            refused(doc! {"field": "a", "range": {"step": step.clone(), "bounds": bounds.clone()}});
        assert!(e.contains("same numeric type"), "{bounds} {step}: {e}");
    }
}

#[test]
fn a_fractional_step_adds_up_as_it_goes() {
    let out =
        run(doc! {"field": "a", "range": {"step": 0.1, "bounds": "full"}}, a(&[0, 1])).unwrap();
    let generated: Vec<f64> = out.iter().filter_map(|d| d.get_f64("a").ok()).collect();
    // Each is the one before plus 0.1, in doubles: 0.1 + 0.1 + 0.1 is 0.30000000000000004 and
    // the sum runs a little short of 1.0 after nine steps and is still below it after ten.
    let mut expected = Vec::new();
    let mut x = 0.0f64;
    for _ in 0..10 {
        x += 0.1;
        expected.push(x);
    }
    assert_eq!(generated, expected);
    assert_eq!(generated[2], 0.30000000000000004);
    assert_eq!(generated[9], 0.9999999999999999);
}

#[test]
fn a_step_too_small_to_move_the_grid_is_refused_not_stopped() {
    let e = run(
        doc! {"field": "a", "range": {"step": 1e-300, "bounds": "full"}},
        vec![doc! {"a": 1.0}, doc! {"a": 2.0}],
    )
    .unwrap_err();
    assert!(matches!(e, Error::Limit(_)), "{e:?}");
    assert!(e.to_string().contains("too small"), "{e}");
}

#[test]
fn a_nested_field_is_densified_in_place() {
    let out = run(
        doc! {"field": "p.t", "range": {"step": 1, "bounds": "full"}},
        vec![doc! {"p": {"t": 1}}, doc! {"p": {"t": 3}}],
    )
    .unwrap();
    assert_eq!(out, vec![doc! {"p": {"t": 1}}, doc! {"p": {"t": 2}}, doc! {"p": {"t": 3}}]);
}

// ---------------------------------------------------------------------------
// Partitions
// ---------------------------------------------------------------------------

fn partitioned() -> Vec<Document> {
    vec![
        doc! {"_id": 1, "v": 1, "a": 1},
        doc! {"_id": 2, "v": 1, "a": 3},
        doc! {"_id": 3, "v": 2, "a": 2},
    ]
}

#[test]
fn full_bounds_give_one_sequence_in_order_of_the_field_with_the_partitions_interleaved() {
    let out = run(
        doc! {"field": "a", "partitionByFields": ["v"], "range": {"step": 1, "bounds": "full"}},
        partitioned(),
    )
    .unwrap();
    // In order of `a`: id 1 (v 1, a 1); id 3 (v 2, a 2) is the first of its partition, which
    // starts at the lowest value, 1, so 1 is made before it; id 2 (v 1, a 3) has 2 made before
    // it; then partition 2 runs on to the highest value, 3. The generated documents carry the
    // partition field first.
    assert_eq!(
        out,
        vec![
            doc! {"_id": 1, "v": 1, "a": 1},
            doc! {"v": 2, "a": 1},
            doc! {"_id": 3, "v": 2, "a": 2},
            doc! {"v": 1, "a": 2},
            doc! {"_id": 2, "v": 1, "a": 3},
            doc! {"v": 2, "a": 3},
        ]
    );
}

#[test]
fn partition_bounds_densify_each_partition_over_its_own_range_one_partition_after_another() {
    let out = run(
        doc! {"field": "a", "partitionByFields": ["v"], "range": {"step": 1, "bounds": "partition"}},
        partitioned(),
    )
    .unwrap();
    assert_eq!(
        out,
        vec![
            doc! {"_id": 1, "v": 1, "a": 1},
            doc! {"v": 1, "a": 2},
            doc! {"_id": 2, "v": 1, "a": 3},
            doc! {"_id": 3, "v": 2, "a": 2},
        ]
    );
}

#[test]
fn explicit_bounds_apply_to_every_partition_one_after_another() {
    let out = run(
        doc! {"field": "a", "partitionByFields": ["v"], "range": {"step": 1, "bounds": [1, 4]}},
        partitioned(),
    )
    .unwrap();
    assert_eq!(col(&out, "a"), ints(&[1, 2, 3, 1, 2, 3]));
    assert_eq!(col(&out, "v"), ints(&[1, 1, 1, 2, 2, 2]));
}

/// The reference's own example: partitions `b`, `a`, `c`, `null` and none, arriving in that
/// order, with ids that say which document is which.
fn many_partitions() -> Vec<Document> {
    vec![
        doc! {"_id": 1, "g": "b", "a": 1},
        doc! {"_id": 2, "g": "a", "a": 3},
        doc! {"_id": 3, "g": "b", "a": 4},
        doc! {"_id": 4, "g": "c", "a": 2},
        doc! {"_id": 5, "g": Bson::Null, "a": 2},
        doc! {"_id": 6, "a": 3},
    ]
}

#[test]
fn partition_bounds_leave_the_partitions_in_ascending_order_null_before_missing() {
    let out = run(
        doc! {"field": "a", "partitionByFields": ["g"], "range": {"step": 1, "bounds": "partition"}},
        many_partitions(),
    )
    .unwrap();
    assert_eq!(
        out,
        vec![
            doc! {"_id": 5, "g": Bson::Null, "a": 2},
            doc! {"_id": 6, "a": 3},
            doc! {"_id": 2, "g": "a", "a": 3},
            doc! {"_id": 1, "g": "b", "a": 1},
            doc! {"g": "b", "a": 2},
            doc! {"g": "b", "a": 3},
            doc! {"_id": 3, "g": "b", "a": 4},
            doc! {"_id": 4, "g": "c", "a": 2},
        ]
    );
}

#[test]
fn full_bounds_with_many_partitions_start_each_at_the_lowest_value_in_order_of_the_field() {
    let out = run(
        doc! {"field": "a", "partitionByFields": ["g"], "range": {"step": 1, "bounds": "full"}},
        many_partitions(),
    )
    .unwrap();
    let id = |d: &Document| d.get("_id").and_then(Bson::as_i32);
    let key = |d: &Document| (d.get("g").cloned(), d.get_i32("a").unwrap(), id(d));
    // The first eleven are in the reference's order: `b` starts the sequence; `c`, `null`, `a`
    // and the partition with no `g` each begin at 1 when their first document comes; `b` fills
    // 2 and 3 before its document at 4.
    let head: Vec<_> = out[..14].iter().map(key).collect();
    let g = |s: &str| Some(Bson::String(s.into()));
    assert_eq!(
        head,
        vec![
            (g("b"), 1, Some(1)),
            (g("c"), 1, None),
            (g("c"), 2, Some(4)),
            (Some(Bson::Null), 1, None),
            (Some(Bson::Null), 2, Some(5)),
            (g("a"), 1, None),
            (g("a"), 2, None),
            (g("a"), 3, Some(2)),
            (None, 1, None),
            (None, 2, None),
            (None, 3, Some(6)),
            (g("b"), 2, None),
            (g("b"), 3, None),
            (g("b"), 4, Some(3)),
        ]
    );
    // Then each partition's grid runs on to the highest value, 4, partition after partition
    // in ascending order of the partitions: null, none, a, c. (The reference ends in a hash order.)
    let tail: Vec<_> = out[14..].iter().map(key).collect();
    assert_eq!(
        tail,
        vec![
            (Some(Bson::Null), 3, None),
            (Some(Bson::Null), 4, None),
            (None, 4, None),
            (g("a"), 4, None),
            (g("c"), 3, None),
            (g("c"), 4, None),
        ]
    );
}

#[test]
fn a_partition_field_may_be_a_dotted_path_and_one_named_twice_is_one() {
    let out = run(
        doc! {"field": "a", "partitionByFields": ["m.k", "m.k"], "range": {"step": 1, "bounds": "partition"}},
        vec![doc! {"m": {"k": 1}, "a": 1}, doc! {"m": {"k": 1}, "a": 3}],
    )
    .unwrap();
    assert_eq!(out[1], doc! {"m": {"k": 1}, "a": 2});
    assert_eq!(out.len(), 3);
}

// ---------------------------------------------------------------------------
// Dates
// ---------------------------------------------------------------------------

fn t(values: &[&str]) -> Vec<Document> {
    values.iter().map(|v| doc! {"t": d(v)}).collect()
}

fn times(out: &[Document]) -> Vec<bson::DateTime> {
    out.iter().map(|doc| doc.get_datetime("t").unwrap().to_owned()).collect()
}

#[test]
fn dates_are_stepped_by_the_unit() {
    let spec = |unit: &str, step: i32| {
        doc! {"field": "t", "range": {"step": step, "unit": unit, "bounds": "full"}}
    };
    let out = run(spec("day", 1), t(&["2023-01-01T00:00:00Z", "2023-01-04T00:00:00Z"])).unwrap();
    assert_eq!(
        times(&out),
        vec![
            d("2023-01-01T00:00:00Z"),
            d("2023-01-02T00:00:00Z"),
            d("2023-01-03T00:00:00Z"),
            d("2023-01-04T00:00:00Z")
        ]
    );
    let out = run(spec("hour", 6), t(&["2023-01-01T00:00:00Z", "2023-01-01T13:00:00Z"])).unwrap();
    assert_eq!(
        times(&out),
        vec![
            d("2023-01-01T00:00:00Z"),
            d("2023-01-01T06:00:00Z"),
            d("2023-01-01T12:00:00Z"),
            d("2023-01-01T13:00:00Z")
        ]
    );
    let out = run(spec("week", 1), t(&["2023-01-01T00:00:00Z", "2023-01-20T00:00:00Z"])).unwrap();
    assert_eq!(times(&out).len(), 4);
    assert_eq!(times(&out)[2], d("2023-01-15T00:00:00Z"));
    let out = run(spec("millisecond", 250), t(&["2023-01-01T00:00:00Z", "2023-01-01T00:00:01Z"]))
        .unwrap();
    assert_eq!(times(&out).len(), 5);
}

#[test]
fn a_month_is_added_to_the_month_before_so_the_day_drifts_down_and_stays() {
    // 31 January, 28 February, 28 March, 28 April: the reference adds a month to the
    // previous point, and a short month takes the day down with it.
    let out = run(
        doc! {"field": "t", "range": {"step": 1, "unit": "month", "bounds": "full"}},
        t(&["2023-01-31T00:00:00Z", "2023-04-30T00:00:00Z"]),
    )
    .unwrap();
    assert_eq!(
        times(&out),
        vec![
            d("2023-01-31T00:00:00Z"),
            d("2023-02-28T00:00:00Z"),
            d("2023-03-28T00:00:00Z"),
            d("2023-04-28T00:00:00Z"),
            d("2023-04-30T00:00:00Z")
        ]
    );
    let out = run(
        doc! {"field": "t", "range": {"step": 1, "unit": "quarter", "bounds": "full"}},
        t(&["2023-01-15T00:00:00Z", "2023-12-31T00:00:00Z"]),
    )
    .unwrap();
    assert_eq!(
        times(&out),
        vec![
            d("2023-01-15T00:00:00Z"),
            d("2023-04-15T00:00:00Z"),
            d("2023-07-15T00:00:00Z"),
            d("2023-10-15T00:00:00Z"),
            d("2023-12-31T00:00:00Z")
        ]
    );
    let out = run(
        doc! {"field": "t", "range": {"step": 1, "unit": "year", "bounds": "full"}},
        t(&["2020-02-29T00:00:00Z", "2023-03-01T00:00:00Z"]),
    )
    .unwrap();
    assert_eq!(
        times(&out),
        vec![
            d("2020-02-29T00:00:00Z"),
            d("2021-02-28T00:00:00Z"),
            d("2022-02-28T00:00:00Z"),
            d("2023-02-28T00:00:00Z"),
            d("2023-03-01T00:00:00Z")
        ]
    );
    // From the 31st every two months: 31 March, 31 May; no short month in between.
    let out = run(
        doc! {"field": "t", "range": {"step": 2, "unit": "month", "bounds": "full"}},
        t(&["2023-01-31T00:00:00Z", "2023-06-30T00:00:00Z"]),
    )
    .unwrap();
    assert_eq!(
        times(&out),
        vec![
            d("2023-01-31T00:00:00Z"),
            d("2023-03-31T00:00:00Z"),
            d("2023-05-31T00:00:00Z"),
            d("2023-06-30T00:00:00Z")
        ]
    );
}

#[test]
fn date_bounds_run_from_the_lower_included_to_the_upper_excluded() {
    let out = run(
        doc! {"field": "t", "range": {"step": 1, "unit": "day", "bounds": [d("2023-01-01T00:00:00Z"), d("2023-01-04T00:00:00Z")]}},
        t(&["2023-01-02T00:00:00Z"]),
    )
    .unwrap();
    assert_eq!(
        times(&out),
        vec![d("2023-01-01T00:00:00Z"), d("2023-01-02T00:00:00Z"), d("2023-01-03T00:00:00Z")]
    );
    // From the 31st to 1 July by months: 28 February to 28 June, then the document on 30 June.
    let out = run(
        doc! {"field": "t", "range": {"step": 1, "unit": "month", "bounds": [d("2023-01-31T00:00:00Z"), d("2023-07-01T00:00:00Z")]}},
        t(&["2023-01-31T00:00:00Z", "2023-06-30T00:00:00Z"]),
    )
    .unwrap();
    assert_eq!(
        times(&out),
        vec![
            d("2023-01-31T00:00:00Z"),
            d("2023-02-28T00:00:00Z"),
            d("2023-03-28T00:00:00Z"),
            d("2023-04-28T00:00:00Z"),
            d("2023-05-28T00:00:00Z"),
            d("2023-06-28T00:00:00Z"),
            d("2023-06-30T00:00:00Z")
        ]
    );
}

#[test]
fn a_date_step_that_overflows_is_an_error() {
    let e = run(
        doc! {"field": "t", "range": {"step": i64::MAX, "unit": "year", "bounds": "full"}},
        t(&["2023-01-31T00:00:00Z", "2023-06-30T00:00:00Z"]),
    )
    .unwrap_err();
    assert!(e.to_string().to_lowercase().contains("overflow"), "{e}");
}

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

#[test]
fn the_documents_a_range_would_make_are_counted_against_the_limit_before_any_is_made() {
    let started = std::time::Instant::now();
    let err = run(full(1), a(&[0, 2_000_000_000])).unwrap_err();
    assert!(matches!(err, Error::Limit(_)), "{err:?}");
    assert!(err.to_string().contains("$densify would produce"), "{err}");
    // A step of nothing like the limit's reach.
    let err = run(
        doc! {"field": "a", "range": {"step": 0.000001, "bounds": "full"}},
        vec![doc! {"a": 0}, doc! {"a": 1_000_000_000}],
    )
    .unwrap_err();
    assert!(matches!(err, Error::Limit(_)), "{err:?}");
    let err = run(
        doc! {"field": "t", "range": {"step": 1, "unit": "millisecond", "bounds": "full"}},
        t(&["1970-01-01T00:00:00Z", "2100-01-01T00:00:00Z"]),
    )
    .unwrap_err();
    assert!(matches!(err, Error::Limit(_)), "{err:?}");
    assert!(started.elapsed().as_secs() < 20, "counting must not build documents");
}

#[test]
fn the_limit_is_on_the_input_and_the_generated_documents_together_and_exact() {
    let limits = Limits { max_documents: 5, ..Default::default() };
    // 1 and 5 make 2, 3, 4: five in all.
    assert_eq!(run_with(full(1), a(&[1, 5]), &limits).unwrap().len(), 5);
    // 1 and 6 make four: six in all.
    let err = run_with(full(1), a(&[1, 6]), &limits).unwrap_err();
    assert!(matches!(err, Error::Limit(_)), "{err:?}");
    // Several gaps and partitions add up.
    let err = run_with(
        doc! {"field": "a", "partitionByFields": ["v"], "range": {"step": 1, "bounds": "full"}},
        vec![doc! {"v": 1, "a": 1}, doc! {"v": 2, "a": 4}],
        &Limits { max_documents: 7, ..Default::default() },
    )
    .unwrap_err();
    // Each partition makes the three it lacks: 2 + 6 = 8 documents.
    assert!(matches!(err, Error::Limit(_)), "{err:?}");
}

#[test]
fn the_bytes_buffered_and_generated_are_held_to_the_byte_budget() {
    let spec = match aggregate::parse(&[doc! {"$densify": full(1)}]).unwrap().remove(0) {
        Stage::Densify(spec) => *spec,
        other => panic!("{other:?}"),
    };
    let size = |d: &Document| bson::serialize_to_vec(d).unwrap().len();
    let input = a(&[0, 10]);
    let held: usize = input.iter().map(size).sum();
    // Room for the input and the nine documents it makes (each the size of an input).
    let out = densify::apply(&spec, input.clone(), &capped(held + 9 * size(&input[0]))).unwrap();
    assert_eq!(out.len(), 11);
    let err =
        densify::apply(&spec, input.clone(), &capped(held + 9 * size(&input[0]) - 1)).unwrap_err();
    assert!(
        matches!(
            err,
            Error::ValueTooLarge { what: "the documents $densify buffers and generates", .. }
        ),
        "{err:?}"
    );
    // The input alone over the budget.
    let err = densify::apply(&spec, input, &capped(8)).unwrap_err();
    assert!(matches!(err, Error::ValueTooLarge { .. }), "{err:?}");
}

#[test]
fn the_stage_refuses_what_it_does_not_know() {
    let range = |r: Document| doc! {"field": "a", "range": r};
    assert!(refused(doc! {"range": {"step": 1, "bounds": "full"}}).contains("needs `field`"));
    assert!(refused(doc! {"field": "a"}).contains("needs `range`"));
    assert!(
        refused(doc! {"field": 1, "range": {"step": 1, "bounds": "full"}})
            .contains("`field` must be a string")
    );
    assert!(
        refused(doc! {"field": "$a", "range": {"step": 1, "bounds": "full"}})
            .contains("without a leading `$`")
    );
    assert!(
        refused(doc! {"field": "", "range": {"step": 1, "bounds": "full"}}).contains("field path")
    );
    assert!(
        refused(doc! {"field": "a..b", "range": {"step": 1, "bounds": "full"}})
            .contains("field path")
    );
    assert!(refused(doc! {"field": "a", "range": 1}).contains("`range` must be a document"));
    assert!(
        refused(doc! {"field": "a", "range": {"step": 1, "bounds": "full"}, "extra": 1})
            .contains("does not take `extra`")
    );
    assert!(
        refused(range(doc! {"step": 1, "bounds": "full", "extra": 1}))
            .contains("does not take `extra`")
    );
    assert!(refused(range(doc! {"bounds": "full"})).contains("needs `step`"));
    assert!(refused(range(doc! {"step": 1})).contains("needs `bounds`"));
    assert!(refused(range(doc! {"step": 0, "bounds": "full"})).contains("greater than 0"));
    assert!(refused(range(doc! {"step": -1, "bounds": "full"})).contains("greater than 0"));
    assert!(refused(range(doc! {"step": 0.0, "bounds": "full"})).contains("greater than 0"));
    assert!(refused(range(doc! {"step": f64::NAN, "bounds": "full"})).contains("greater than 0"));
    assert!(
        refused(range(doc! {"step": f64::INFINITY, "bounds": "full"})).contains("greater than 0")
    );
    assert!(refused(range(doc! {"step": "1", "bounds": "full"})).contains("must be a number"));
    assert!(
        refused(range(doc! {"step": 1.5, "unit": "day", "bounds": "full"}))
            .contains("whole number")
    );
    assert!(
        refused(range(doc! {"step": 1, "unit": "fortnight", "bounds": "full"}))
            .contains("`unit` must be one of")
    );
    assert!(
        refused(range(doc! {"step": 1, "unit": 3, "bounds": "full"}))
            .contains("`unit` must be a string")
    );
    assert!(
        refused(range(doc! {"step": 1, "bounds": "all"}))
            .contains("\"full\", \"partition\" or [lo, hi]")
    );
    assert!(
        refused(range(doc! {"step": 1, "bounds": 5}))
            .contains("\"full\", \"partition\" or [lo, hi]")
    );
    assert!(
        refused(range(doc! {"step": 1, "bounds": [1]}))
            .contains("\"full\", \"partition\" or [lo, hi]")
    );
    assert!(
        refused(range(doc! {"step": 1, "bounds": "partition"}))
            .contains("needs `partitionByFields`")
    );
}

#[test]
fn bounds_and_partition_fields_are_validated() {
    let with_bounds = |b: Bson, unit: Option<&str>| {
        let mut r = doc! {"step": 1, "bounds": b};
        if let Some(u) = unit {
            r.insert("unit", u);
        }
        refused(doc! {"field": "a", "range": r})
    };
    assert!(with_bounds(Bson::Array(vec![5.into(), 1.into()]), None).contains("must not be above"));
    assert!(
        with_bounds(Bson::Array(vec![1.into(), "x".into()]), None)
            .contains("two numbers or two dates")
    );
    assert!(
        with_bounds(Bson::Array(vec![1.into(), d("2023-01-01T00:00:00Z").into()]), None)
            .contains("two numbers or two dates")
    );
    assert!(
        with_bounds(
            Bson::Array(vec![d("2023-01-01T00:00:00Z").into(), d("2023-01-02T00:00:00Z").into()]),
            None
        )
        .contains("needs a `unit`")
    );
    assert!(
        with_bounds(Bson::Array(vec![1.into(), 5.into()]), Some("day"))
            .contains("`unit` is for dates")
    );
    assert!(
        with_bounds(Bson::Array(vec![f64::INFINITY.into(), f64::INFINITY.into()]), None)
            .contains("finite")
    );
    let pf = |p: Bson| {
        refused(doc! {"field": "a", "partitionByFields": p, "range": {"step": 1, "bounds": "full"}})
    };
    assert!(pf(Bson::String("v".into())).contains("array of field paths"));
    assert!(pf(Bson::Array(vec![1.into()])).contains("holds field paths"));
    assert!(pf(Bson::Array(vec!["$v".into()])).contains("without a leading `$`"));
    assert!(pf(Bson::Array(vec!["a".into()])).contains("cannot hold the densified field"));
    assert!(pf(Bson::Array(vec!["a.b".into()])).contains("cannot hold the densified field"));
}

#[test]
fn a_field_that_is_not_a_number_or_a_date_in_every_document_is_refused() {
    let e = refused_run(full(1), vec![doc! {"a": "x"}]);
    assert!(e.contains("found a string"), "{e}");
    let e = refused_run(full(1), vec![doc! {"a": f64::NAN}]);
    assert!(e.contains("non-finite"), "{e}");
    let e = refused_run(full(1), vec![doc! {"a": [1, 2]}]);
    assert!(e.contains("array"), "{e}");
    let dec = Bson::Decimal128("1".parse().unwrap());
    let e = refused_run(full(1), vec![doc! {"a": dec}]);
    assert!(e.contains("Decimal128 (not supported)"), "{e}");
    let e = refused_run(
        doc! {"field": "a", "range": {"step": 1, "unit": "day", "bounds": "full"}},
        vec![doc! {"a": 1}, doc! {"a": d("2023-01-01T00:00:00Z")}],
    );
    assert!(e.contains("all numbers or all dates"), "{e}");
}

#[test]
fn the_unit_must_match_the_kind_of_the_field() {
    let e = refused_run(
        doc! {"field": "a", "range": {"step": 1, "unit": "day", "bounds": "full"}},
        vec![doc! {"a": 1}],
    );
    assert!(e.contains("`unit` is for a date field"), "{e}");
    let e = refused_run(
        full(1),
        t(&["2023-01-01T00:00:00Z"])
            .into_iter()
            .map(|d| doc! {"a": d.get_datetime("t").copied().unwrap()})
            .collect(),
    );
    assert!(e.contains("needs a `unit`"), "{e}");
    let e = refused_run(
        doc! {"field": "a", "range": {"step": 1, "bounds": [0, 5]}},
        vec![doc! {"a": d("2023-01-01T00:00:00Z")}],
    );
    assert!(e.contains("needs a `unit`") || e.contains("one kind"), "{e}");
    let e = refused_run(
        doc! {"field": "t", "range": {"step": 1, "unit": "day", "bounds": [d("2023-01-01T00:00:00Z"), d("2023-01-04T00:00:00Z")]}},
        vec![doc! {"t": 5}],
    );
    assert!(e.contains("`unit` is for a date field") || e.contains("one kind"), "{e}");
}

// ---------------------------------------------------------------------------
// In a pipeline
// ---------------------------------------------------------------------------

#[test]
fn the_stage_runs_inside_a_facet_and_a_lookup_sub_pipeline_parses() {
    let stages = aggregate::parse(&[doc! {"$facet": {"f": [
        {"$densify": full(1)},
        {"$count": "n"},
    ]}}])
    .unwrap();
    let out = aggregate::apply(&stages[0], a(&[1, 5]), &Limits::default()).unwrap();
    assert_eq!(out, vec![doc! {"f": [{"n": 5i64}]}]);
    let stages = aggregate::parse(&[doc! {"$lookup": {
        "from": "x", "pipeline": [{"$densify": full(1)}], "as": "y"
    }}])
    .unwrap();
    assert!(
        matches!(&stages[0], Stage::Lookup { join: aggregate::Join::Pipeline { stages, .. }, .. } if matches!(stages[0], Stage::Densify(_)))
    );
    assert_eq!(stages[0].name(), "$lookup");
    let densify = aggregate::parse(&[doc! {"$densify": full(1)}]).unwrap();
    assert_eq!(densify[0].name(), "$densify");
    assert!(!densify[0].needs_storage());
}

#[test]
fn the_count_over_all_gaps_and_partitions_is_decided_before_any_document_is_generated() {
    // Each partition's gap alone (3 documents) is inside the limit of 7 with its two inputs;
    // together they are not. The byte cap would stop the first generated document, so a
    // `Limit` and not an `AnswerTooLarge` shows that nothing was generated first.
    let spec = match aggregate::parse(&[doc! {"$densify": {
    "field": "a", "partitionByFields": ["v"], "range": {"step": 1, "bounds": "full"}}}])
    .unwrap()
    .remove(0)
    {
        Stage::Densify(spec) => *spec,
        other => panic!("{other:?}"),
    };
    let input = vec![doc! {"v": 1, "a": 1}, doc! {"v": 2, "a": 4}];
    let held: usize = input.iter().map(|d| bson::serialize_to_vec(d).unwrap().len()).sum();
    let err =
        densify::apply(&spec, input, &Limits { max_documents: 7, ..capped(held + 1) }).unwrap_err();
    assert!(matches!(err, Error::Limit(_)), "{err:?}");
}
